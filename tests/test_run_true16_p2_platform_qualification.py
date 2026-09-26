#!/usr/bin/env python3
"""Small fake-simulator tests for staged P2 platform qualification."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


LIMER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIMER / "tools"))

import run_true16_p2_platform_qualification as platform  # noqa: E402
import validate_p2_workload_runtime as runtime  # noqa: E402


class DelayedProcessMapsAuthority(platform.runner.SealedRuntimeAuthority):
    """Test seam that makes a genuinely short ELF disappear before /proc/maps."""

    def verify_process(self, binding, pid):
        time.sleep(0.1)
        return super().verify_process(binding, pid)


class PlatformQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.topology = self._write(
            "topology.txt",
            "2 1 0 1 1 TEST_GPU\n1\n0 1 100Gbps 0.001ms 0\n",
        )
        self.link_map = self.root / "link_map.csv"
        with self.link_map.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=runtime.LINK_MAP_COLUMNS)
            writer.writeheader()
            writer.writerow(
                {
                    "link_id": "L0-1",
                    "src_node": 0,
                    "dst_node": 1,
                    "src_type": "HOST",
                    "dst_type": "SWITCH",
                    "src_port": 2,
                    "dst_port": 1,
                    "link_class": "ACCESS",
                    "bandwidth_bps": 100000000000,
                    "delay_ns": 1000,
                }
            )
        self.config = self._write(
            "config.conf",
            "\n".join(
                f"{key} /shared/{key.lower()}"
                for key in platform.runner.RUNTIME_CONFIG_PATHS
            )
            + "\n",
        )
        self.workloads = tuple(
            self._write(f"q{index}.txt", f"workload-{index}\n") for index in range(1, 5)
        )
        self.binary_scripts: dict[Path, Path] = {}
        self.runtime_build = self.root / "runtime-build"
        self.runtime_build.mkdir()
        self.runtime_library = self._compile_runtime_library()
        self.binary = self._fake_binary(fail_stage="")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

    def _compile_runtime_library(self) -> Path:
        source = self.runtime_build / "fixture.c"
        library = self.runtime_build / "libns3-platform-fixture.so"
        source.write_text(
            "int platform_fixture_value(void) { return 42; }\n",
            encoding="ascii",
        )
        subprocess.run(
            [
                "gcc",
                "-shared",
                "-fPIC",
                "-Wl,-soname,libns3-platform-fixture.so",
                str(source),
                "-o",
                str(library),
            ],
            check=True,
            capture_output=True,
        )
        return library

    def _compile_launcher(self, name: str, *, short_lived: bool = False) -> Path:
        source = self.runtime_build / f"{name}.c"
        binary = self.runtime_build / name
        if short_lived:
            body = "return platform_fixture_value() == 42 ? 0 : 7;"
        else:
            body = r"""
  if (platform_fixture_value() != 42) return 7;
  usleep(250000);
  const char* script = getenv("PLATFORM_TEST_SIMULATOR_SCRIPT");
  if (script == NULL) return 9;
  pid_t child = fork();
  if (child < 0) return 10;
  if (child == 0) {
    execl("/usr/bin/python3", "python3", script, (char*)NULL);
    _exit(11);
  }
  int status = 0;
  if (waitpid(child, &status, 0) < 0) return 12;
  if (!WIFEXITED(status)) return 13;
  return WEXITSTATUS(status);
"""
        source.write_text(
            """
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
extern int platform_fixture_value(void);
int main(void) {
"""
            + body
            + "\n}\n",
            encoding="ascii",
        )
        subprocess.run(
            [
                "gcc",
                str(source),
                f"-L{self.runtime_build}",
                "-lns3-platform-fixture",
                f"-Wl,-rpath,{self.runtime_build}",
                "-o",
                str(binary),
            ],
            check=True,
            capture_output=True,
        )
        return binary

    def _fake_binary(self, *, fail_stage: str) -> Path:
        script = self.root / f"fake-{fail_stage or 'pass'}.py"
        columns = {
            "switch_telemetry.csv": runtime.SWITCH_COLUMNS,
            "nic_telemetry.csv": runtime.NIC_COLUMNS,
            "collective_telemetry.csv": runtime.COLLECTIVE_FLOW_COLUMNS,
        }
        script.write_text(
            f"""#!/usr/bin/env python3
import csv, os, sys
from pathlib import Path
out = Path(os.environ['LIMER_TELEMETRY_DIR'])
stage = os.environ['LIMER_QUALIFICATION_STAGE']
if stage == {fail_stage!r}:
    raise SystemExit(7)
run_id = os.environ['LIMER_RUN_ID']
(out / 'link_map.csv').write_bytes((out / 'inputs/link_map.csv').read_bytes())
with (out / 'ecmp_route_candidates.csv').open('w', newline='') as stream:
    writer = csv.writer(stream)
    writer.writerow([
        'run_id', 'node_id', 'node_type', 'destination_node_id',
        'destination_ip', 'candidate_index', 'candidate_count',
        'egress_port_id', 'next_hop_node_id', 'status'])
    writer.writerow([run_id, 1, 'SWITCH', 0, '11.0.0.1', 0, 1, 1, 0,
                     'INSTALLED'])
columns = {columns!r}
for filename, fields in columns.items():
    with (out / filename).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        row = {{field: '' for field in fields}}
        row['run_id'] = run_id
        if 'timestamp_ns' in row: row['timestamp_ns'] = '1'
        writer.writerow(row)
tx = {runtime.COLLECTIVE_TRANSACTION_COLUMNS!r}
with (out / 'collective_transaction.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=tx); writer.writeheader()
    layers = int(os.environ['LIMER_EXPECTED_LAYERS'])
    message = os.environ['LIMER_EXPECTED_MESSAGE_BYTES']
    if stage != 'Q4':
        for seq in range(layers):
            common = {{'run_id': run_id, 'collective_seq': seq, 'attempt': 0,
                      'layer_num': seq, 'message_size_bytes': message,
                      'world_size': 16}}
            for rank in range(16):
                writer.writerow({{**common, 'timestamp_ns': 1, 'event': 'START',
                                 'rank_id': rank, 'ready_ranks': rank + 1,
                                 'status': 'in_flight'}})
            for rank in range(16):
                writer.writerow({{**common, 'timestamp_ns': 2, 'event': 'LOCAL_READY',
                                 'rank_id': rank, 'ready_ranks': rank + 1,
                                 'status': 'ready'}})
            writer.writerow({{**common, 'timestamp_ns': 3, 'event': 'COMMIT',
                             'rank_id': '', 'ready_ranks': 16,
                             'result_digest': 'digest', 'status': 'exactly_once'}})
life = {runtime.LIFECYCLE_COLUMNS!r}
with (out / 'run_lifecycle.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=life); writer.writeheader()
    if stage == 'Q4':
        finish = os.environ['LIMER_OBSERVATION_STOP_NS']
        writer.writerow({{'run_id': run_id, 'event': 'observation_horizon',
                         'scheduled_ns': finish, 'actual_ns': finish,
                         'finished_ranks': 0, 'world_size': 16,
                         'status': 'OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE'}})
    else:
        writer.writerow({{'run_id': run_id, 'event': 'finish_barrier',
                         'scheduled_ns': '', 'actual_ns': 1000 + layers,
                         'finished_ranks': 16, 'world_size': 16,
                         'status': 'WORKLOAD_COMPLETE'}})
sport_fields = {platform.sport_validator.RAW_COLUMNS!r}
with (out / 'training_source_port_allocator.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=sport_fields); writer.writeheader()
    q4 = stage == 'Q4'
    allocations = 39153 if q4 else 32
    writer.writerow({{'run_id': run_id, 'interval_first': 10000,
                     'interval_end_exclusive': 49152, 'capacity': 39152,
                     'allocations': allocations, 'releases': allocations,
                     'reuses': 1 if q4 else 0, 'active_at_stop': 0,
                     'peak_active': 8, 'pair_count': 16,
                     'pairs_with_reuse': 1 if q4 else 0,
                     'max_pair_allocations': 39153 if q4 else 2,
                     'max_pair_reuses': 1 if q4 else 0,
                     'external_conflicts': 0, 'exhaustions': 0,
                     'invariant_errors': 0, 'min_allocated_port': 10000,
                     'max_allocated_port': 49151 if q4 else 10003,
                     'status': 'PASS'}})
""",
            encoding="utf-8",
        )
        launcher = self._compile_launcher(f"simulator-{fail_stage or 'pass'}")
        self.binary_scripts[launcher] = script
        return launcher

    def _static(self, path: Path, *, profile: str) -> dict:
        index = int(path.stem[1:])
        layers = (1, 10, 32, 550)[index - 1]
        return {
            "status": "PASS",
            "qualification_profile": profile,
            "world_size": 16,
            "layer_count": layers,
            "collective_bytes_per_layer": 65536,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "duration_estimate": {
                "corpus_max_virtual_finish_ns": platform.HORIZON_NS,
            },
        }

    @staticmethod
    def _runtime_pass(**kwargs) -> dict:
        return {
            "schema_version": runtime.SCHEMA_VERSION,
            "status": "PASS",
            "run_id": kwargs["run"]["run_id"],
            "checks": [{"name": "small-validator", "status": "PASS"}],
            "source_artifacts": {},
            "errors": [],
        }

    def _run(
        self,
        out: Path,
        *,
        binary: Path | None = None,
        workers: int = 1,
        runtime_authority=None,
    ):
        selected_binary = binary or self.binary
        fixture_environment = {}
        if selected_binary in self.binary_scripts:
            fixture_environment["PLATFORM_TEST_SIMULATOR_SCRIPT"] = str(
                self.binary_scripts[selected_binary]
            )
        with (
            patch.dict(os.environ, fixture_environment),
            patch.object(
                platform.workload, "validate_workload", side_effect=self._static
            ),
            patch.object(
                platform.runtime,
                "validate_runtime_qualification",
                side_effect=self._runtime_pass,
            ),
            patch.object(
                platform.runner, "_validate_topology", return_value={"status": "PASS"}
            ),
            patch.object(
                platform.runner,
                "_validate_link_map_against_topology",
                return_value=None,
            ),
        ):
            return platform.qualify_platform(
                binary=selected_binary,
                topology=self.topology,
                link_map=self.link_map,
                config=self.config,
                out_root=out,
                workloads=self.workloads,
                worker_threads=workers,
                wall_timeout_s=2,
                _runtime_authority=runtime_authority,
            )

    def test_four_stages_pass_and_record_worker_and_controls(self) -> None:
        out = self.root / "out"
        with patch.dict(
            os.environ,
            {
                "LIMER_STALE": "bad",
                "AS_STALE": "bad",
                "NS_STALE": "bad",
                "LD_LIBRARY_PATH": "/ambient/not-the-bundle",
                "LD_PRELOAD": str(self.runtime_library),
                "LD_AUDIT": "/ambient/not-an-auditor.so",
            },
        ):
            result = self._run(out, workers=3)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["completed_stage_count"], 4)
        self.assertEqual(result["schema_version"], platform.SCHEMA)
        harness_binding = result["harness_source_binding"]
        self.assertEqual(
            harness_binding["schema_version"], platform.HARNESS_SOURCE_SCHEMA
        )
        self.assertGreaterEqual(harness_binding["source_count"], 10)
        bundle_parent = out / platform.runtime_bundle.BUNDLE_ROOT_NAME
        bundles = list(bundle_parent.iterdir())
        self.assertEqual(len(bundles), 1)
        identities = set()
        for index in range(1, 5):
            manifest = platform.validate_sealed_stage(out / "stages" / f"Q{index}")
            self.assertEqual(manifest["worker_threads"], 3)
            self.assertEqual(manifest["schema_version"], platform.STAGE_SCHEMA)
            self.assertEqual(
                manifest["harness_source_binding"], harness_binding
            )
            identities.add(manifest["runtime_closure"]["identity_sha256"])
            self.assertEqual(
                manifest["command"][0],
                str(bundles[0] / platform.runtime_bundle.EXECUTABLE_RELATIVE),
            )
            self.assertEqual(manifest["command"][2], "3")
            self.assertIsNone(manifest["simulator"]["execution_copy"])
            route = manifest["ecmp_route_candidate_evidence"]
            self.assertEqual(route["status"], "PASS")
            self.assertEqual(
                route["raw_sha256"],
                platform.runner.sha256_file(
                    out / "stages" / f"Q{index}" / "ecmp_route_candidates.csv"
                ),
            )
            allocator = manifest["training_source_port_allocator_evidence"]
            self.assertEqual(allocator["status"], "PASS")
            self.assertEqual(
                allocator["profile"],
                "Q4_REUSE_REQUIRED" if index == 4 else "GENERAL",
            )
            allocator_report = json.loads(
                (
                    out
                    / "stages"
                    / f"Q{index}"
                    / platform.sport_validator.REPORT_FILENAME
                ).read_text()
            )
            self.assertEqual(allocator_report["status"], "PASS")
            self.assertEqual(
                allocator_report["metrics"]["reuses"], 1 if index == 4 else 0
            )
            self.assertEqual(
                route["validation_sha256"],
                platform.runner.sha256_file(
                    out
                    / "stages"
                    / f"Q{index}"
                    / "ecmp_route_candidate_validation.json"
                ),
            )
            self.assertFalse(
                (out / "stages" / f"Q{index}" / "inputs/simulator_binary").exists()
            )
            execution = manifest["runtime_execution"]
            self.assertEqual(execution["status"], "PASS")
            self.assertEqual(
                execution["process_mapping_verification"]["status"], "PASS"
            )
            self.assertGreater(
                execution["process_mapping_verification"][
                    "project_dependency_count"
                ],
                0,
            )
            self.assertEqual(
                execution["loader_environment"]["LD_LIBRARY_PATH"],
                str(bundles[0] / "lib"),
            )
            self.assertIsNone(execution["loader_environment"]["LD_PRELOAD"])
            self.assertTrue(
                {"LD_AUDIT", "LD_LIBRARY_PATH", "LD_PRELOAD"}.issubset(
                    execution["loader_environment"][
                        "cleared_inherited_loader_variables"
                    ]
                )
            )
            env = manifest["environment_overrides"]
            self.assertEqual(env["LIMER_TELEMETRY_INTERVAL_US"], "1000")
            self.assertEqual(env["LIMER_HARD_EVENT_DETECTOR_ENABLE"], "0")
            self.assertEqual(env["LIMER_RECOVERY_ACTION_ENABLE"], "0")
            self.assertEqual(env["LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE"], "0")
            self.assertTrue(
                {"LIMER_STALE", "AS_STALE", "NS_STALE"}.issubset(
                    manifest["cleared_inherited_control_variables"]
                )
            )
        self.assertEqual(len(identities), 1)
        self.assertEqual(
            identities, {result["runtime_closure_identity_sha256"]}
        )
        self.assertNotIn(
            "LIMER_OBSERVATION_STOP_NS",
            platform.validate_sealed_stage(out / "stages/Q1")["environment_overrides"],
        )
        self.assertEqual(
            platform.validate_sealed_stage(out / "stages/Q4")["environment_overrides"][
                "LIMER_OBSERVATION_STOP_NS"
            ],
            str(platform.HORIZON_NS),
        )

    def test_fail_stop_does_not_launch_later_stages(self) -> None:
        out = self.root / "failed"
        result = self._run(out, binary=self._fake_binary(fail_stage="Q2"))
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["completed_stage_count"], 2)
        self.assertTrue((out / "stages/Q2").is_dir())
        self.assertFalse((out / "stages/Q3").exists())
        self.assertFalse((out / "stages/Q4").exists())

    def test_harness_binding_detects_byte_and_module_set_drift(self) -> None:
        source_root = self.root / "harness-sources"
        source_root.mkdir()
        first = self._write("harness-sources/first.py", "VALUE = 1\n")
        second = self._write("harness-sources/second.py", "VALUE = 2\n")
        binding = platform._build_harness_source_binding(
            source_root, (first, second)
        )
        platform._validate_harness_source_binding(binding)
        platform._verify_harness_source_binding(
            binding,
            phase="synthetic_stable",
            _source_root=source_root,
            _source_paths=(first, second),
        )

        first.write_text("VALUE = 9\n", encoding="utf-8")
        with self.assertRaisesRegex(
            platform.QualificationError,
            r"Python harness source drift at synthetic_byte_drift: .*first.py",
        ):
            platform._verify_harness_source_binding(
                binding,
                phase="synthetic_byte_drift",
                _source_root=source_root,
                _source_paths=(first, second),
            )

        first.write_text("VALUE = 1\n", encoding="utf-8")
        third = self._write("harness-sources/third.py", "VALUE = 3\n")
        with self.assertRaisesRegex(
            platform.QualificationError,
            r"Python harness source drift at synthetic_module_drift: .*third.py",
        ):
            platform._verify_harness_source_binding(
                binding,
                phase="synthetic_module_drift",
                _source_root=source_root,
                _source_paths=(first, second, third),
            )

    def test_harness_drift_fail_stops_before_next_stage(self) -> None:
        out = self.root / "harness-drift"
        original = platform._verify_harness_source_binding

        def verify(binding, *, phase, **kwargs):
            if phase == "Q2_before_stage":
                raise platform.QualificationError(
                    "Python harness source drift at Q2_before_stage: synthetic"
                )
            return original(binding, phase=phase, **kwargs)

        with patch.object(
            platform, "_verify_harness_source_binding", side_effect=verify
        ):
            with self.assertRaisesRegex(
                platform.QualificationError, "drift at Q2_before_stage"
            ):
                self._run(out)
        self.assertTrue((out / "stages/Q1/run_manifest.json").is_file())
        self.assertFalse((out / "stages/Q2").exists())
        self.assertFalse((out / "qualification.json").exists())

    def test_tamper_breaks_stage_seal(self) -> None:
        out = self.root / "tamper"
        self._run(out)
        with (out / "stages/Q1/switch_telemetry.csv").open("a") as stream:
            stream.write("tamper\n")
        with self.assertRaisesRegex(platform.QualificationError, "inventory"):
            platform.validate_sealed_stage(out / "stages/Q1")

    def test_resealed_forged_route_report_is_independently_rejected(self) -> None:
        out = self.root / "route-report-forgery"
        self._run(out)
        stage = out / "stages/Q1"
        report_path = stage / "ecmp_route_candidate_validation.json"
        report = json.loads(report_path.read_text())
        report["evidence"]["forged_but_still_pass"] = True
        material = dict(report)
        material.pop("report_sha256")
        report["report_sha256"] = platform.runner.canonical_hash(material)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        manifest = dict(platform._load_stage_manifest(stage))
        route = dict(manifest["ecmp_route_candidate_evidence"])
        route["validation_sha256"] = platform.runner.sha256_file(report_path)
        route["report_sha256"] = report["report_sha256"]
        manifest["ecmp_route_candidate_evidence"] = route
        for artifact in manifest["artifacts"]:
            if artifact["path"] == "ecmp_route_candidate_validation.json":
                artifact["sha256"] = route["validation_sha256"]
                artifact["size_bytes"] = report_path.stat().st_size
        manifest["artifact_set_sha256"] = platform.runner.canonical_hash(
            manifest["artifacts"]
        )
        platform._write_seal(stage, manifest)
        with self.assertRaisesRegex(
            platform.QualificationError, "independent raw-evidence recomputation"
        ):
            platform.validate_sealed_stage(stage)

    def test_resealed_forged_allocator_report_is_independently_rejected(self) -> None:
        out = self.root / "allocator-report-forgery"
        self._run(out)
        stage = out / "stages/Q4"
        report_path = stage / platform.sport_validator.REPORT_FILENAME
        report = json.loads(report_path.read_text())
        report["metrics"]["reuses"] = 999
        material = dict(report)
        material.pop("report_sha256")
        report["report_sha256"] = platform.sport_validator.canonical_hash(material)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        manifest = dict(platform._load_stage_manifest(stage))
        allocator = dict(manifest["training_source_port_allocator_evidence"])
        allocator["validation_sha256"] = platform.runner.sha256_file(report_path)
        allocator["report_sha256"] = report["report_sha256"]
        manifest["training_source_port_allocator_evidence"] = allocator
        for artifact in manifest["artifacts"]:
            if artifact["path"] == platform.sport_validator.REPORT_FILENAME:
                artifact["sha256"] = allocator["validation_sha256"]
                artifact["size_bytes"] = report_path.stat().st_size
        manifest["artifact_set_sha256"] = platform.runner.canonical_hash(
            manifest["artifacts"]
        )
        platform._write_seal(stage, manifest)
        with self.assertRaisesRegex(
            platform.QualificationError, "independent raw-evidence recomputation"
        ):
            platform.validate_sealed_stage(stage)

    def test_legacy_v1_is_not_reinterpreted_as_v2_allocator_contract(self) -> None:
        out = self.root / "legacy-stage"
        self._run(out)
        stage = out / "stages/Q1"
        manifest = dict(platform._load_stage_manifest(stage))
        manifest["schema_version"] = platform.LEGACY_STAGE_SCHEMA
        manifest.pop("training_source_port_allocator_evidence")
        for filename in (
            platform.sport_validator.RAW_FILENAME,
            platform.sport_validator.REPORT_FILENAME,
        ):
            (stage / filename).unlink()
        manifest["artifacts"] = [
            artifact
            for artifact in manifest["artifacts"]
            if artifact["path"]
            not in {
                platform.sport_validator.RAW_FILENAME,
                platform.sport_validator.REPORT_FILENAME,
            }
        ]
        manifest["artifact_set_sha256"] = platform.runner.canonical_hash(
            manifest["artifacts"]
        )
        platform._write_seal(stage, manifest)
        self.assertEqual(platform.validate_sealed_stage(stage)["status"], "PASS")

        manifest = dict(platform._load_stage_manifest(stage))
        manifest["schema_version"] = platform.STAGE_SCHEMA
        platform._write_seal(stage, manifest)
        with self.assertRaisesRegex(
            platform.QualificationError, "lacks training source-port"
        ):
            platform.validate_sealed_stage(stage)

    def test_v2_allocator_stage_is_not_reinterpreted_as_v3_harness_contract(
        self,
    ) -> None:
        out = self.root / "legacy-v2-stage"
        self._run(out)
        stage = out / "stages/Q1"
        manifest = dict(platform._load_stage_manifest(stage))
        manifest["schema_version"] = platform.ALLOCATOR_STAGE_SCHEMA
        manifest.pop("harness_source_binding")
        platform._write_seal(stage, manifest)
        self.assertEqual(platform.validate_sealed_stage(stage)["status"], "PASS")

        manifest = dict(platform._load_stage_manifest(stage))
        manifest["schema_version"] = platform.STAGE_SCHEMA
        platform._write_seal(stage, manifest)
        with self.assertRaisesRegex(
            platform.QualificationError, "harness source binding is missing"
        ):
            platform.validate_sealed_stage(stage)

    def test_reuse_refuses_nonempty_output(self) -> None:
        out = self.root / "reuse"
        self._run(out)
        with self.assertRaisesRegex(platform.QualificationError, "non-empty"):
            self._run(out)

    def test_shared_runtime_bundle_tamper_is_rejected(self) -> None:
        out = self.root / "binary-tamper"
        self._run(out)
        identity = platform.validate_sealed_stage(out / "stages/Q1")[
            "runtime_closure"
        ]["identity_sha256"]
        archived = (
            out
            / platform.runtime_bundle.BUNDLE_ROOT_NAME
            / identity
            / platform.runtime_bundle.EXECUTABLE_RELATIVE
        )
        archived.chmod(0o755)
        archived.write_bytes(archived.read_bytes() + b"tamper\n")
        with self.assertRaisesRegex(platform.QualificationError, "bundle"):
            platform.validate_sealed_stage(out / "stages/Q1")

    def test_short_lived_real_elf_fails_closed_on_process_maps(self) -> None:
        out = self.root / "short-lived"
        short_binary = self._compile_launcher("simulator-short", short_lived=True)
        result = self._run(
            out,
            binary=short_binary,
            runtime_authority=DelayedProcessMapsAuthority(),
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["completed_stage_count"], 1)
        manifest = platform.validate_sealed_stage(out / "stages/Q1")
        mapping = manifest["runtime_execution"]["process_mapping_verification"]
        self.assertEqual(mapping["status"], "FAIL")
        self.assertIn("process", mapping["error"].lower())
        self.assertFalse((out / "stages/Q2").exists())

    def test_production_path_rejects_non_elf_script(self) -> None:
        script = self._write("not-elf.py", "#!/usr/bin/env python3\n")
        script.chmod(0o755)
        with self.assertRaisesRegex(platform.QualificationError, "not an ELF"):
            self._run(self.root / "script-output", binary=script)

    def test_sibling_stage_identity_divergence_is_rejected(self) -> None:
        out = self.root / "mixed-identity"
        self._run(out)
        q2 = out / "stages/Q2"
        manifest = platform._load_stage_manifest(q2)
        changed = dict(manifest)
        changed["runtime_closure"] = dict(changed["runtime_closure"])
        changed["runtime_closure"]["identity_sha256"] = "0" * 64
        platform._write_seal(q2, changed)
        with self.assertRaisesRegex(platform.QualificationError, "do not share"):
            platform.validate_sealed_stage(out / "stages/Q1")


if __name__ == "__main__":
    unittest.main()
