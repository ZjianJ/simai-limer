#!/usr/bin/env python3
"""Fail-closed publication tests for the true-16 P2 finalizer."""

from __future__ import annotations

import copy
import csv
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
sys.path.insert(0, str(TOOLS))

import finalize_true16_p2_corpus as finalizer  # noqa: E402
import generate_true16_p2_corpus as generator  # noqa: E402
import run_true16_p2_corpus as runner  # noqa: E402
import validate_ecmp_route_candidates as route_validator  # noqa: E402


class FakeRuntimeAuthority:
    """Explicit non-ELF runtime seam; production CLI cannot select this."""

    TEST_SEAM = "finalizer-explicit-non-elf-runtime-authority"

    @staticmethod
    def _identity(source_binary: Path) -> str:
        return runner.canonical_hash(
            {
                "test_seam": FakeRuntimeAuthority.TEST_SEAM,
                "source_sha256": runner.sha256_file(source_binary),
            }
        )

    @staticmethod
    def _record(
        execution_root: Path,
        source_binary: Path,
        identity: str,
        manifest_path: Path,
        executable: Path,
    ) -> dict:
        root = execution_root.resolve()
        bundle_root = manifest_path.parent
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return {
            "schema_version": "limer.simulator-runtime-bundle.v1",
            "identity_sha256": identity,
            "execution_root": str(root),
            "bundle_path": bundle_root.relative_to(root).as_posix(),
            "bundle_manifest_path": manifest_path.relative_to(root).as_posix(),
            "bundle_manifest_sha256": runner.sha256_file(manifest_path),
            "bundle_manifest_seal_path": (
                bundle_root / "runtime_bundle_manifest.sha256"
            ).relative_to(root).as_posix(),
            "bundle_artifact_set_sha256": manifest["artifact_set_sha256"],
            "bundle_executable_path": executable.relative_to(root).as_posix(),
            "bundle_executable_sha256": runner.sha256_file(executable),
            "bundle_executable_size_bytes": executable.stat().st_size,
            "source_executable_path": str(source_binary.resolve()),
            "project_dependency_count": 1,
            "system_dependency_count": 0,
            "loader_isolation": manifest["loader_contract"],
            "test_seam": FakeRuntimeAuthority.TEST_SEAM,
        }

    @staticmethod
    def _validate_binding(binding: runner.SimulatorRuntimeBinding) -> None:
        if (
            not binding.executable.is_file()
            or runner.sha256_file(binding.executable)
            != binding.record["bundle_executable_sha256"]
        ):
            raise runner.RunnerError("fake sealed runtime executable changed")
        if (
            not binding.manifest_path.is_file()
            or runner.sha256_file(binding.manifest_path)
            != binding.manifest_sha256
        ):
            raise runner.RunnerError("fake sealed runtime manifest changed")
        seal = binding.bundle_root / "runtime_bundle_manifest.sha256"
        fields = seal.read_text(encoding="ascii").strip().split()
        if fields != [binding.manifest_sha256, "runtime_bundle_manifest.json"]:
            raise runner.RunnerError("fake sealed runtime manifest seal changed")

    def prepare(
        self, execution_root: Path, source_binary: Path
    ) -> runner.SimulatorRuntimeBinding:
        root = execution_root.resolve()
        identity = self._identity(source_binary)
        bundle_root = root / "runtime-bundles" / identity
        executable = bundle_root / "bin" / "simulator_binary"
        library_path = bundle_root / "lib"
        manifest_path = bundle_root / "runtime_bundle_manifest.json"
        seal_path = bundle_root / "runtime_bundle_manifest.sha256"
        if not bundle_root.exists():
            executable.parent.mkdir(parents=True)
            library_path.mkdir()
            shutil.copyfile(source_binary, executable)
            executable.chmod(0o555)
            artifact = {
                "path": "bin/simulator_binary",
                "sha256": runner.sha256_file(executable),
                "size_bytes": executable.stat().st_size,
            }
            manifest = {
                "schema_version": "limer.simulator-runtime-bundle.v1",
                "status": "SEALED",
                "identity_sha256": identity,
                "executable": {
                    "source_path": str(source_binary.resolve()),
                    "execution_path": "bin/simulator_binary",
                    "sha256": artifact["sha256"],
                    "size_bytes": artifact["size_bytes"],
                },
                "project_dependencies": [{"soname": "libns3-test.so"}],
                "system_dependencies": [],
                "loader_contract": {
                    "clear_inherited_prefix": "LD_",
                    "LD_LIBRARY_PATH": "lib",
                    "LD_PRELOAD": None,
                },
                "artifacts": [artifact],
                "artifact_set_sha256": runner.canonical_hash([artifact]),
                "test_seam": self.TEST_SEAM,
            }
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            seal_path.write_text(
                f"{runner.sha256_file(manifest_path)}  "
                "runtime_bundle_manifest.json\n",
                encoding="ascii",
            )
        record = self._record(
            root, source_binary, identity, manifest_path, executable
        )
        loader = {
            "status": "PASS",
            "runtime_bundle_identity_sha256": identity,
            "project_dependency_count": 1,
            "project_dependencies": [{"soname": "libns3-test.so"}],
            "system_dependency_count": 0,
            "system_dependencies": [],
            "phase": "explicit_test_seam",
            "test_seam": self.TEST_SEAM,
        }
        binding = runner.SimulatorRuntimeBinding(
            execution_root=root,
            bundle_root=bundle_root,
            executable=executable,
            identity_sha256=identity,
            manifest_path=manifest_path,
            manifest_sha256=runner.sha256_file(manifest_path),
            record=record,
            loader_preflight=loader,
            bundle=None,
            source_closure=None,
        )
        self._validate_binding(binding)
        return binding

    def discover_current(self, source_binary: Path) -> finalizer.CurrentSimulatorRuntime:
        source = source_binary.resolve(strict=True)
        identity = self._identity(source)
        source_ref = {
            "identity_sha256": identity,
            "executable": {
                "path": str(source),
                "sha256": runner.sha256_file(source),
                "size_bytes": source.stat().st_size,
            },
            "project_dependencies": [
                {
                    "soname": "libns3-test.so",
                    "path": "explicit-test-seam",
                    "sha256": "0" * 64,
                    "size_bytes": 0,
                }
            ],
            "system_dependencies": [],
            "virtual_dependencies": [],
            "identity_material_sha256": identity,
            "test_seam": self.TEST_SEAM,
        }
        return finalizer.CurrentSimulatorRuntime(identity, source_ref, None)

    def validate_record(
        self,
        execution_root: Path,
        recorded: dict,
        current: finalizer.CurrentSimulatorRuntime,
    ) -> finalizer.AuditedRuntimeBinding:
        root = execution_root.resolve(strict=True)
        identity = current.identity_sha256
        if recorded.get("identity_sha256") != identity:
            raise finalizer.RuntimeAuditError(
                "RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR",
                "fake run closure differs from current fake source closure",
            )
        bundle_root = root / "runtime-bundles" / identity
        manifest_path = bundle_root / "runtime_bundle_manifest.json"
        seal_path = bundle_root / "runtime_bundle_manifest.sha256"
        executable = bundle_root / "bin" / "simulator_binary"
        source = Path(recorded.get("source_executable_path", ""))
        if not source.is_file():
            raise finalizer.RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "fake runtime source path is invalid",
            )
        try:
            binding = self.prepare(root, source)
        except (runner.RunnerError, OSError, ValueError, json.JSONDecodeError) as exc:
            raise finalizer.RuntimeAuditError(
                "INVALID_RUNTIME_BUNDLE", str(exc)
            ) from exc
        expected = self._record(root, source, identity, manifest_path, executable)
        if dict(recorded) != expected:
            raise finalizer.RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "fake run authority differs from sealed bundle",
            )
        try:
            self._validate_binding(binding)
        except runner.RunnerError as exc:
            raise finalizer.RuntimeAuditError(
                "INVALID_RUNTIME_BUNDLE", str(exc)
            ) from exc
        return finalizer.AuditedRuntimeBinding(
            binding=binding,
            evidence={
                "status": "PASS",
                "identity_sha256": identity,
                "authority_record_sha256": runner.canonical_hash(expected),
                "bundle_path": str(bundle_root),
                "bundle_manifest": finalizer._file_ref(manifest_path),
                "bundle_manifest_seal": finalizer._file_ref(seal_path),
                "bundle_artifact_set_sha256": json.loads(
                    manifest_path.read_text(encoding="utf-8")
                )["artifact_set_sha256"],
                "loader_resolution_identity_sha256": identity,
                "project_dependency_count": 1,
                "system_dependency_count": 0,
                "shared_bundle_outside_runs": True,
                "test_seam": self.TEST_SEAM,
            },
        )

    def verify_current(self, current: finalizer.CurrentSimulatorRuntime) -> None:
        source = Path(current.source_evidence["executable"]["path"])
        if runner.sha256_file(source) != current.source_evidence["executable"]["sha256"]:
            raise finalizer.RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_CHANGED",
                "fake source executable changed",
            )

    def verify_before_run(self, binding: runner.SimulatorRuntimeBinding) -> dict:
        self._validate_binding(binding)
        return {
            "status": "PASS",
            "phase": "before_run",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
            "test_seam": self.TEST_SEAM,
        }

    def execution_environment(
        self,
        binding: runner.SimulatorRuntimeBinding,
        base_environment: dict,
        overrides: dict,
    ) -> tuple[dict, dict]:
        self._validate_binding(binding)
        environment = {
            key: value
            for key, value in base_environment.items()
            if not key.startswith("LD_")
        }
        environment.update(overrides)
        library_path = str(binding.bundle_root / "lib")
        environment["LD_LIBRARY_PATH"] = library_path
        return environment, {
            "runtime_bundle_identity_sha256": binding.identity_sha256,
            "runtime_bundle_path": str(binding.bundle_root),
            "cleared_inherited_loader_variables": sorted(
                key for key in base_environment if key.startswith("LD_")
            ),
            "LD_LIBRARY_PATH": library_path,
            "LD_PRELOAD": None,
            "test_seam": self.TEST_SEAM,
        }

    def verify_process(
        self, binding: runner.SimulatorRuntimeBinding, pid: int
    ) -> dict:
        self._validate_binding(binding)
        return {
            "status": "PASS",
            "pid": pid,
            "runtime_bundle_identity_sha256": binding.identity_sha256,
            "project_dependency_count": 1,
            "project_dependencies": [{"soname": "libns3-test.so"}],
            "system_dependency_count": 0,
            "system_dependencies": [],
            "test_seam": self.TEST_SEAM,
        }

    def verify_after_run(self, binding: runner.SimulatorRuntimeBinding) -> dict:
        self._validate_binding(binding)
        return {
            "status": "PASS",
            "phase": "after_run",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
            "test_seam": self.TEST_SEAM,
        }

    def verify_for_reuse(self, binding: runner.SimulatorRuntimeBinding) -> dict:
        self._validate_binding(binding)
        return {
            "status": "PASS",
            "phase": "reuse",
            "bundle_integrity_status": "PASS",
            "source_closure_reopened": False,
            "test_seam": self.TEST_SEAM,
        }


class P2FinalizerTest(unittest.TestCase):
    """Use real corpus/runner validators; only the expensive runtime model is stubbed."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.shared = tempfile.TemporaryDirectory()
        cls.shared_root = Path(cls.shared.name)
        cls.full_root = cls.shared_root / "prepared"
        cls.full_corpus = generator.generate_corpus(
            link_map_path=LIMER / "results/true16_hard_fault_e2e/healthy/link_map.csv",
            contract_path=LIMER / "configs/experiment_contract.yaml",
            topology_path=(
                LIMER / "results/true16_hard_fault_e2e/topology/"
                "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
            ),
            workload_path=(
                LIMER / "configs/microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
            ),
            simulator_config_path=LIMER / "configs/SimAI.baseline.conf",
            out_dir=cls.full_root,
            seed=20260827,
        )
        cls.full_manifest = cls.full_root / "corpus_manifest.json"
        cls.min_manifest, cls.min_run = cls._make_minimal_corpus()
        cls.binary = cls._write_fake_simulator(
            str(cls.min_run["mechanism"]["mechanism_id"])
        )
        cls.route_template = cls._write_route_template()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.shared.cleanup()

    @classmethod
    def _make_minimal_corpus(cls) -> tuple[Path, dict]:
        manifest = copy.deepcopy(cls.full_corpus)
        run = copy.deepcopy(
            next(item for item in manifest["runs"] if item["run_role"] == "healthy")
        )
        manifest["runs"] = [run]
        manifest["schedule_set_sha256"] = runner.canonical_hash(
            [runner.schedule_identity_tuple(run)]
        )
        split_source = json.loads(
            (cls.full_root / manifest["split_manifest"]["path"]).read_text()
        )
        split_source["entries"] = [
            entry
            for entry in split_source["entries"]
            if entry["run_id"] == run["run_id"]
        ]
        identity = {
            "identity_schema": manifest["identity_schema"],
            "contract_sha256": manifest["input_artifacts"]["contract"]["sha256"],
            "link_map_sha256": manifest["input_artifacts"]["link_map"]["sha256"],
            "topology_sha256": manifest["input_artifacts"]["topology"]["sha256"],
            "workload_sha256": manifest["input_artifacts"]["workload"]["sha256"],
            "simulator_config_sha256": manifest["input_artifacts"]["simulator_config"][
                "sha256"
            ],
            "seed": manifest["generation_seed"],
            "holdouts": manifest["split_manifest"]["paired_holdout_gpu_ids"],
            "runs": [runner.run_identity_entry(run)],
        }
        corpus_id = "p2-" + runner.canonical_hash(identity)[:24]
        manifest["corpus_id"] = corpus_id
        split_source["corpus_id"] = corpus_id
        split_path = cls.full_root / "split_manifest.minimal.json"
        split_path.write_text(
            json.dumps(split_source, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest["split_manifest"].update(
            {
                "path": split_path.relative_to(cls.full_root).as_posix(),
                "sha256": runner.sha256_file(split_path),
            }
        )
        qualification = manifest["workload_qualification"]
        qualification["planned_minimum_virtual_finish_ns"] = run["virtual_finish_ns"]
        qualification["planned_maximum_virtual_finish_ns"] = run["virtual_finish_ns"]
        qualification["recovery_evaluation_run_count"] = int(
            run.get("recovery_evaluation") is True
        )
        path = cls.full_root / "corpus_manifest.minimal.json"
        path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path, run

    @classmethod
    def _write_fake_simulator(cls, mechanism_id: str) -> Path:
        path = cls.shared_root / "fake_simulator.py"
        quoted_mechanism = json.dumps(mechanism_id)
        path.write_text(
            f"""#!/usr/bin/env python3
import csv
import hashlib
import json
import os
from pathlib import Path

out = Path(os.environ['LIMER_TELEMETRY_DIR'])
run_id = os.environ['LIMER_RUN_ID']
finish = int(os.environ['LIMER_OBSERVATION_STOP_NS'])
link_source = next((out / 'inputs').glob('link_map*'))
(out / 'link_map.csv').write_bytes(link_source.read_bytes())
with Path(os.environ['FAKE_ROUTE_TEMPLATE_PATH']).open(newline='') as stream:
    route_reader = csv.DictReader(stream)
    route_columns = route_reader.fieldnames
    route_rows = list(route_reader)
with (out / 'ecmp_route_candidates.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=route_columns)
    writer.writeheader()
    for route_row in route_rows:
        route_row['run_id'] = run_id
        writer.writerow(route_row)
(out / 'switch_telemetry.csv').write_text(
    'run_id,timestamp_ns,switch_id,port_id,link_id,direction,'
    'configured_bandwidth_bps,link_state,tx_bytes,queue_bytes,'
    'observed_throughput_bps,dropped_packets,link_errors,'
    'recovered_packets,flap_count,last_link_down_ns,last_link_up_ns,'
    'cumulative_link_down_ns\\n'
    f'{{run_id}},1,20,1,L0-20,tx,100000000000,up,1000,0,'
    '8000000,0,0,0,0,0,0,0\\n', encoding='utf-8')
(out / 'nic_telemetry.csv').write_text(
    'run_id,timestamp_ns,node_id,nic_id,link_id,'
    'configured_bandwidth_bps,link_state,tx_bytes,queue_bytes,'
    'effective_throughput_bps,rx_dropped_packets,link_errors,'
    'recovered_packets,flap_count,last_link_down_ns,last_link_up_ns,'
    'cumulative_link_down_ns\\n'
    f'{{run_id}},1,0,2,L0-20,100000000000,up,1000,0,'
    '8000000,0,0,0,0,0,0,0\\n', encoding='utf-8')
(out / 'collective_telemetry.csv').write_text(
    'run_id,start_time_ns,finish_time_ns,duration_ns,status\\n'
    f'{{run_id}},0,1,1,ok\\n', encoding='utf-8')
(out / 'collective_transaction.csv').write_text(
    'run_id,timestamp_ns,collective_seq,attempt,layer_num,'
    'message_size_bytes,event,rank_id,world_size,ready_ranks,'
    'result_digest,status\\n'
    f'{{run_id}},1,0,0,0,65536,COMMIT,,16,16,digest,exactly_once\\n',
    encoding='utf-8')
(out / 'fault_application_telemetry.csv').write_text(
    'run_id,fault_id,fault_type,target_link_id,transition,scheduled_ns,'
    'actual_ns,parameter_before,parameter_after,mechanism,rng_stream,status\\n',
    encoding='utf-8')
(out / 'training_source_port_allocator.csv').write_text(
    'run_id,interval_first,interval_end_exclusive,capacity,allocations,'
    'releases,reuses,active_at_stop,peak_active,pair_count,pairs_with_reuse,'
    'max_pair_allocations,max_pair_reuses,external_conflicts,exhaustions,'
    'invariant_errors,min_allocated_port,max_allocated_port,status\\n'
    f'{{run_id}},10000,49152,39152,1,0,0,1,1,1,0,1,0,0,0,0,'
    '10000,10000,PASS\\n', encoding='utf-8')
(out / 'run_lifecycle.csv').write_text(
    'run_id,event,scheduled_ns,actual_ns,finished_ranks,world_size,status\\n'
    f'{{run_id}},observation_horizon,{{finish}},{{finish}},0,16,'
    'OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE\\n', encoding='utf-8')
switch_hash = hashlib.sha256((out / 'switch_telemetry.csv').read_bytes()).hexdigest()
(out / 'semantic_validation.json').write_text(json.dumps({{
    'schema_version': 'limer.p2-run-semantics.v1',
    'run_id': run_id,
    'mechanism_id': {quoted_mechanism},
    'status': 'PASS',
    'source_artifact_sha256': switch_hash,
    'source_artifacts_sha256': {{'switch': switch_hash}},
    'checks': [{{'name': 'test_fixture_semantics', 'status': 'PASS'}}],
    'observed_effects': ['healthy_progress'],
}}) + '\\n', encoding='utf-8')
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    @classmethod
    def _write_route_template(cls) -> Path:
        manifest = json.loads(cls.min_manifest.read_text(encoding="utf-8"))
        link_map = Path(manifest["input_artifacts"]["link_map"]["path"])
        artifact = route_validator._read_bound_artifact(
            link_map, "finalizer_test_link_map", None
        )
        topology = route_validator._parse_link_map(artifact)
        topology_path = Path(manifest["input_artifacts"]["topology"]["path"])
        topology = route_validator._bind_topology_file(
            topology,
            route_validator._read_bound_artifact(
                topology_path, "finalizer_test_topology", None
            ),
        )
        rows = route_validator.reconstruct_expected_rows(topology, "template-run")
        output = cls.shared_root / "route_template.csv"
        with output.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(route_validator.ROUTE_COLUMNS)
            writer.writerows(row.identity() for row in rows)
        return output

    @staticmethod
    def _fake_runtime_qualification(
        *,
        directory: Path,
        run: dict,
        corpus: runner.ValidatedCorpus,
    ) -> dict:
        sources = runner._runtime_qualification_source_paths(directory, run)
        return {
            "schema_version": "limer.p2-workload-runtime-qualification.v1",
            "status": "PASS",
            "run_id": run["run_id"],
            "planned_run_sha256": runner.canonical_hash(run),
            "static_workload_report_sha256": runner.canonical_hash(
                corpus.manifest["workload_qualification"]["static_report"]
            ),
            "source_artifacts": {
                name: {
                    "path": source.name,
                    "sha256": runner.sha256_file(source),
                }
                for name, source in sources.items()
            },
            "checks": [{"name": "test_fixture_runtime", "status": "PASS"}],
            "errors": [],
        }

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.corpus_root = self.root / "prepared"
        self.execution = self.root / "execution"
        shutil.copytree(self.full_root, self.corpus_root)
        self.manifest = self.corpus_root / self.min_manifest.name
        self.output = self.root / "finalized"
        self.runtime_authority = FakeRuntimeAuthority()
        with (
            patch.object(
                runner,
                "_run_workload_runtime_qualification",
                side_effect=self._fake_runtime_qualification,
            ),
            patch.dict(
                os.environ,
                {"FAKE_ROUTE_TEMPLATE_PATH": str(self.route_template)},
            ),
        ):
            summary = runner.run_corpus(
                corpus_manifest=self.manifest,
                simulator_binary=self.binary,
                out_root=self.execution,
                requested_run_ids=[str(self.min_run["run_id"])],
                wall_timeout_s=5,
                _runtime_authority=self.runtime_authority,
            )
        if summary["counts"]["executed"] != 1:
            raise AssertionError(f"minimal runner fixture did not execute: {summary}")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _finalize(self) -> dict:
        with patch.object(finalizer, "EXPECTED_RUN_COUNT", 1), patch.object(
            runner,
            "_run_workload_runtime_qualification",
            side_effect=self._fake_runtime_qualification,
        ):
            return dict(
                finalizer.finalize_corpus(
                    corpus_manifest=self.manifest,
                    execution_root=self.execution,
                    simulator_binary=self.binary,
                    output_dir=self.output,
                    _runtime_authority=self.runtime_authority,
                )
            )

    def _rewrite_run_manifest(self, transform) -> dict:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        path = run_dir / "run_manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        transform(manifest)
        path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (run_dir / "run_manifest.sha256").write_text(
            f"{runner.sha256_file(path)}  run_manifest.json\n",
            encoding="ascii",
        )
        return manifest

    def _make_two_run_corpus(self) -> tuple[Path, list[dict]]:
        manifest = json.loads((self.corpus_root / "corpus_manifest.json").read_text())
        runs = [
            copy.deepcopy(run)
            for run in manifest["runs"]
            if run["run_role"] == "healthy"
        ][:2]
        self.assertEqual(len(runs), 2)
        manifest["runs"] = runs
        manifest["schedule_set_sha256"] = runner.canonical_hash(
            [runner.schedule_identity_tuple(run) for run in runs]
        )
        split_source = json.loads(
            (self.corpus_root / manifest["split_manifest"]["path"]).read_text()
        )
        run_ids = {run["run_id"] for run in runs}
        split_source["entries"] = [
            entry for entry in split_source["entries"] if entry["run_id"] in run_ids
        ]
        identity = {
            "identity_schema": manifest["identity_schema"],
            "contract_sha256": manifest["input_artifacts"]["contract"]["sha256"],
            "link_map_sha256": manifest["input_artifacts"]["link_map"]["sha256"],
            "topology_sha256": manifest["input_artifacts"]["topology"]["sha256"],
            "workload_sha256": manifest["input_artifacts"]["workload"]["sha256"],
            "simulator_config_sha256": manifest["input_artifacts"]["simulator_config"][
                "sha256"
            ],
            "seed": manifest["generation_seed"],
            "holdouts": manifest["split_manifest"]["paired_holdout_gpu_ids"],
            "runs": [runner.run_identity_entry(run) for run in runs],
        }
        manifest["corpus_id"] = "p2-" + runner.canonical_hash(identity)[:24]
        split_source["corpus_id"] = manifest["corpus_id"]
        split_path = self.corpus_root / "split_manifest.two-runs.json"
        split_path.write_text(
            json.dumps(split_source, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest["split_manifest"].update(
            {
                "path": split_path.relative_to(self.corpus_root).as_posix(),
                "sha256": runner.sha256_file(split_path),
            }
        )
        qualification = manifest["workload_qualification"]
        qualification["planned_minimum_virtual_finish_ns"] = min(
            run["virtual_finish_ns"] for run in runs
        )
        qualification["planned_maximum_virtual_finish_ns"] = max(
            run["virtual_finish_ns"] for run in runs
        )
        qualification["recovery_evaluation_run_count"] = sum(
            run.get("recovery_evaluation") is True for run in runs
        )
        path = self.corpus_root / "corpus_manifest.two-runs.json"
        path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path, runs

    def test_current_generated_corpus_reports_9_declared_blockers(self) -> None:
        output = self.root / "full-readiness"
        empty_execution = self.root / "empty-execution"
        empty_execution.mkdir()
        audit = finalizer.finalize_corpus(
            corpus_manifest=self.full_manifest,
            execution_root=empty_execution,
            simulator_binary=self.binary,
            output_dir=output,
            _runtime_authority=self.runtime_authority,
        )
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["planned"], 462)
        self.assertEqual(audit["counts"]["declared_blocked"], 9)
        self.assertEqual(audit["counts"]["missing_run"], 462)
        self.assertEqual(
            set(path.name for path in output.iterdir()), {"readiness_audit.json"}
        )
        persisted = json.loads((output / "readiness_audit.json").read_text())
        self.assertEqual(
            persisted["source_corpus"]["sha256"],
            (runner.sha256_file(self.full_manifest)),
        )
        self.assertEqual(
            persisted["run_records_sha256"],
            runner.canonical_hash(persisted["runs"]),
        )
        self.assertEqual(
            persisted["readiness_binding_sha256"],
            runner.canonical_hash(persisted["readiness_binding"]),
        )

    def test_finalizer_independently_rebuilds_a1_static_inputs_and_metadata(
        self,
    ) -> None:
        corpus = runner.validate_corpus(self.full_manifest, self.binary)
        planned = copy.deepcopy(next(
            run for run in self.full_corpus["runs"]
            if run.get("scenario") == "allreduce_burst"
        ))
        report, errors = finalizer._collective_override_static_report(
            planned, corpus
        )
        self.assertEqual(errors, [])
        self.assertEqual(report["role_counts"]["EVENT_BURST"], 8)

        planned["effective_workload_sha256"] = "f" * 64
        _, errors = finalizer._collective_override_static_report(planned, corpus)
        self.assertTrue(
            any("effective workload identity" in error for error in errors)
        )

        planned = copy.deepcopy(next(
            run for run in self.full_corpus["runs"]
            if run.get("scenario") == "allreduce_burst"
        ))
        planned["schedule"]["collective_workload_override"][
            "static_validation"
        ]["forged_pass"] = True
        _, errors = finalizer._collective_override_static_report(planned, corpus)
        self.assertTrue(
            any("independent baseline" in error for error in errors)
        )

    def test_missing_run_writes_only_blocked_readiness(self) -> None:
        shutil.rmtree(self.execution)
        self.execution.mkdir()
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["missing_run"], 1)
        self.assertEqual(audit["counts"]["verified"], 0)
        self.assertFalse((self.output / "corpus_manifest.json").exists())
        self.assertFalse((self.output / "split_manifest.json").exists())
        self.assertTrue((self.output / "readiness_audit.json").is_file())

    def test_production_gate_never_promotes_a_one_run_fixture(self) -> None:
        with patch.object(
            runner,
            "_run_workload_runtime_qualification",
            side_effect=self._fake_runtime_qualification,
        ):
            audit = finalizer.finalize_corpus(
                corpus_manifest=self.manifest,
                execution_root=self.execution,
                simulator_binary=self.binary,
                output_dir=self.output,
                _runtime_authority=self.runtime_authority,
            )
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["verified"], 1)
        self.assertEqual(
            audit["global_blockers"],
            [
                {
                    "reason_code": "UNEXPECTED_RUN_COUNT",
                    "expected": 462,
                    "observed": 1,
                }
            ],
        )
        self.assertEqual(
            set(path.name for path in self.output.iterdir()),
            {"readiness_audit.json"},
        )

    def test_tampered_sealed_artifact_cannot_finalize(self) -> None:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        with (run_dir / "switch_telemetry.csv").open("a", encoding="utf-8") as stream:
            stream.write("tampered\n")
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["invalid_run"], 1)
        self.assertIn("INVALID_RUN_EVIDENCE", audit["runs"][0]["reason_codes"])
        self.assertEqual(
            set(path.name for path in self.output.iterdir()), {"readiness_audit.json"}
        )

    def test_legacy_run_without_runtime_closure_fails_closed(self) -> None:
        self._rewrite_run_manifest(lambda manifest: manifest.pop("runtime_closure"))
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["runtime_invalid"], 1)
        self.assertIn(
            "MISSING_RUNTIME_CLOSURE_BINDING",
            audit["runs"][0]["reason_codes"],
        )
        self.assertIsNone(audit["simulator_runtime_closure"])

    def test_run_closure_must_match_current_canonical_simulator(self) -> None:
        def replace_identity(manifest: dict) -> None:
            manifest["runtime_closure"]["identity_sha256"] = "f" * 64

        self._rewrite_run_manifest(replace_identity)
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn(
            "RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR",
            audit["runs"][0]["reason_codes"],
        )
        self.assertEqual(
            audit["runtime_closure_audit"]["failure_reason_code_counts"],
            {"RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR": 1},
        )

    def test_shared_runtime_bundle_seal_tamper_fails_closed(self) -> None:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        manifest = json.loads(
            (run_dir / "run_manifest.json").read_text(encoding="utf-8")
        )
        bundle = self.execution / manifest["runtime_closure"]["bundle_path"]
        (bundle / "runtime_bundle_manifest.sha256").write_text(
            f"{'0' * 64}  runtime_bundle_manifest.json\n",
            encoding="ascii",
        )
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn("INVALID_RUNTIME_BUNDLE", audit["runs"][0]["reason_codes"])
        self.assertEqual(audit["counts"]["verified"], 0)

    def test_forged_route_validation_pass_is_recomputed_and_rejected(self) -> None:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        report_path = run_dir / "ecmp_route_candidate_validation.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["forged_pass_marker"] = True
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        def bind_forged_report(manifest: dict) -> None:
            new_hash = runner.sha256_file(report_path)
            manifest["ecmp_route_candidate_validation_sha256"] = new_hash
            for entry in manifest["artifacts"]:
                if entry["path"] == report_path.name:
                    entry["sha256"] = new_hash
                    entry["size_bytes"] = report_path.stat().st_size
                    break
            else:  # pragma: no cover - fixture construction invariant
                self.fail("route validation is absent from sealed inventory")
            manifest["artifact_set_sha256"] = runner.canonical_hash(
                manifest["artifacts"]
            )

        self._rewrite_run_manifest(bind_forged_report)
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn("INVALID_RUN_EVIDENCE", audit["runs"][0]["reason_codes"])
        self.assertTrue(
            any(
                "independent raw-evidence recomputation" in reason
                for reason in audit["runs"][0]["reasons"]
            )
        )

    def test_forged_allocator_validation_is_recomputed_and_rejected(self) -> None:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        report_path = run_dir / "training_source_port_allocator_validation.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["metrics"]["allocations"] = 2
        material = dict(report)
        material.pop("report_sha256")
        report["report_sha256"] = runner.canonical_hash(material)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        def bind_forged_report(manifest: dict) -> None:
            new_hash = runner.sha256_file(report_path)
            manifest[
                "training_source_port_allocator_validation_sha256"
            ] = new_hash
            manifest["training_source_port_allocator_report_sha256"] = report[
                "report_sha256"
            ]
            for entry in manifest["artifacts"]:
                if entry["path"] == report_path.name:
                    entry["sha256"] = new_hash
                    entry["size_bytes"] = report_path.stat().st_size
                    break
            else:  # pragma: no cover - fixture construction invariant
                self.fail("allocator validation is absent from sealed inventory")
            manifest["artifact_set_sha256"] = runner.canonical_hash(
                manifest["artifacts"]
            )

        self._rewrite_run_manifest(bind_forged_report)
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn("INVALID_RUN_EVIDENCE", audit["runs"][0]["reason_codes"])
        self.assertTrue(
            any(
                "independent raw-evidence recomputation" in reason
                for reason in audit["runs"][0]["reasons"]
            )
        )

    def test_production_cli_has_no_non_elf_runtime_bypass(self) -> None:
        audit = finalizer.finalize_corpus(
            corpus_manifest=self.manifest,
            execution_root=self.execution,
            simulator_binary=self.binary,
            output_dir=self.output,
        )
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn(
            "CURRENT_SIMULATOR_RUNTIME_INVALID",
            {item["reason_code"] for item in audit["global_blockers"]},
        )
        parsed = finalizer.parse_args(
            [
                "--corpus-manifest",
                str(self.manifest),
                "--execution-root",
                str(self.execution),
                "--simulator-binary",
                str(self.binary),
                "--output-dir",
                str(self.root / "unused"),
            ]
        )
        self.assertFalse(hasattr(parsed, "runtime_authority"))

    def test_minimal_fixture_publishes_hash_bound_executed_manifests(self) -> None:
        with (
            patch.object(
                finalizer.runner, "validate_corpus", wraps=runner.validate_corpus
            ) as validate_corpus,
            patch.object(
                finalizer.runner, "validate_final_run", wraps=runner.validate_final_run
            ) as validate_run,
            patch.object(
                runner,
                "_run_workload_runtime_qualification",
                side_effect=self._fake_runtime_qualification,
            ),
            patch.object(finalizer, "EXPECTED_RUN_COUNT", 1),
        ):
            audit = dict(
                finalizer.finalize_corpus(
                    corpus_manifest=self.manifest,
                    execution_root=self.execution,
                    simulator_binary=self.binary,
                    output_dir=self.output,
                    _runtime_authority=self.runtime_authority,
                )
            )
        validate_corpus.assert_called_once()
        validate_run.assert_called_once()
        self.assertEqual(audit["status"], "READY")
        self.assertEqual(audit["counts"]["verified"], 1)
        self.assertEqual(
            set(path.name for path in self.output.iterdir()),
            {"corpus_manifest.json", "split_manifest.json", "readiness_audit.json"},
        )
        executed = json.loads((self.output / "corpus_manifest.json").read_text())
        source = json.loads(self.manifest.read_text())
        self.assertEqual(executed["status"], "EXECUTED")
        self.assertEqual(executed["identity_schema"], source["identity_schema"])
        self.assertEqual(executed["corpus_id"], source["corpus_id"])
        self.assertEqual(executed["schedule_set_sha256"], source["schedule_set_sha256"])
        runner._validate_corpus_identity(executed)
        self.assertEqual(
            runner.run_identity_entry(executed["runs"][0]),
            runner.run_identity_entry(source["runs"][0]),
        )
        self.assertEqual(executed["workload_qualification"]["status"], "PASS")
        self.assertEqual(executed["source_corpus"]["path"], str(self.manifest))
        self.assertEqual(
            executed["source_corpus"]["sha256"], runner.sha256_file(self.manifest)
        )
        run = executed["runs"][0]
        sealed_manifest = json.loads(
            (
                self.execution / "runs" / self.min_run["run_id"] / "run_manifest.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            executed["simulator_runtime_closure"],
            sealed_manifest["runtime_closure"],
        )
        self.assertEqual(run["runtime_closure"], sealed_manifest["runtime_closure"])
        self.assertEqual(
            audit["simulator_runtime_closure"],
            sealed_manifest["runtime_closure"],
        )
        self.assertEqual(
            audit["runtime_closure_audit"]["identity_sha256"],
            sealed_manifest["runtime_closure"]["identity_sha256"],
        )
        self.assertEqual(
            audit["runtime_closure_audit"]["authority_record_sha256"],
            runner.canonical_hash(sealed_manifest["runtime_closure"]),
        )
        self.assertEqual(
            audit["runtime_closure_audit"]["verified_run_reference_count"], 1
        )
        self.assertEqual(audit["runtime_closure_audit"]["shared_bundle_count"], 1)
        self.assertTrue(
            audit["runtime_closure_audit"]["shared_bundle_outside_runs"]
        )
        self.assertEqual(run["mechanism"]["implementation_status"], "VERIFIED")
        self.assertEqual(
            run["mechanism"]["semantic_validation"],
            run["artifacts"]["semantic_validation"],
        )
        required = set(finalizer.REQUIRED_SEALED_ARTIFACTS) | {
            "run_manifest",
            "run_manifest_seal",
            "runtime_config",
        }
        self.assertTrue(required.issubset(run["artifacts"]))
        sealed_root = (self.execution / "runs" / self.min_run["run_id"]).resolve()
        for ref in run["artifacts"].values():
            path = Path(ref["path"])
            path.relative_to(sealed_root)
            self.assertEqual(ref["sha256"], runner.sha256_file(path))
        with self.assertRaises(ValueError):
            Path(audit["runtime_closure_audit"]["bundle_path"]).relative_to(
                sealed_root
            )
        split = json.loads((self.output / "split_manifest.json").read_text())
        self.assertEqual(split["status"], "EXECUTED")
        self.assertEqual(split["corpus_id"], executed["corpus_id"])
        self.assertEqual(
            executed["split_manifest"]["sha256"],
            runner.sha256_file(self.output / "split_manifest.json"),
        )
        self.assertEqual(
            audit["publication_binding_sha256"],
            runner.canonical_hash(
                {
                    "readiness_binding_sha256": audit["readiness_binding_sha256"],
                    "outputs": audit["outputs"],
                    "finalized_run_set_sha256": audit["finalized_run_set_sha256"],
                }
            ),
        )

    def test_finalizer_recomputes_generic_healthy_stability_pass(self) -> None:
        run_dir = self.execution / "runs" / self.min_run["run_id"]
        manifest_path = run_dir / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        planned = copy.deepcopy(self.min_run)
        report_path = run_dir / runner.SIMULATOR_STABILITY_FILE
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(planned["run_role"], "healthy")
        self.assertTrue(planned["simulator_stability"]["gate_required"])
        self.assertEqual(manifest["simulator_stability_status"], "PASS")
        artifact_paths = {
            **finalizer.REQUIRED_SEALED_ARTIFACTS,
            **finalizer.OPTIONAL_SEALED_ARTIFACTS,
            "run_manifest": "run_manifest.json",
        }
        artifacts = {
            name: finalizer._file_ref(run_dir / relative)
            for name, relative in artifact_paths.items()
            if (run_dir / relative).is_file()
        }
        self.assertEqual(
            finalizer._simulator_stability_errors(
                planned, run_dir, manifest, artifacts
            ),
            [],
        )
        executed = finalizer._executed_run(
            planned,
            manifest,
            artifacts,
            {"planned_run_sha256": runner.canonical_hash(planned)},
        )
        self.assertEqual(executed["simulator_stability"]["status"], "PASS")
        self.assertEqual(
            executed["simulator_stability"]["evidence_basis_sha256"],
            report["evidence_basis_sha256"],
        )

        report["evidence_basis"]["exit_code"] = 9
        report["evidence_basis_sha256"] = runner.canonical_hash(
            report["evidence_basis"]
        )
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest["simulator_stability_sha256"] = runner.sha256_file(report_path)
        artifacts["simulator_stability"] = finalizer._file_ref(report_path)
        errors = finalizer._simulator_stability_errors(
            planned, run_dir, manifest, artifacts
        )
        self.assertTrue(
            any("independent raw-evidence recomputation" in error for error in errors)
        )

    def test_leaf_simulator_symlink_resolves_to_real_executable(self) -> None:
        simulator_link = self.root / "simulator-link"
        simulator_link.symlink_to(self.binary)
        with patch.object(finalizer, "EXPECTED_RUN_COUNT", 1), patch.object(
            runner,
            "_run_workload_runtime_qualification",
            side_effect=self._fake_runtime_qualification,
        ):
            audit = finalizer.finalize_corpus(
                corpus_manifest=self.manifest,
                execution_root=self.execution,
                simulator_binary=simulator_link,
                output_dir=self.output,
                _runtime_authority=self.runtime_authority,
            )
        self.assertEqual(audit["status"], "READY")
        self.assertEqual(audit["simulator"]["path"], str(self.binary.resolve()))
        executed = json.loads((self.output / "corpus_manifest.json").read_text())
        self.assertEqual(executed["simulator"]["path"], str(self.binary.resolve()))
        self.assertEqual(executed["simulator_worker_threads"], 1)

    def test_mixed_simulator_worker_threads_fail_closed(self) -> None:
        manifest, runs = self._make_two_run_corpus()
        shutil.rmtree(self.execution)
        with (
            patch.object(
                runner,
                "_run_workload_runtime_qualification",
                side_effect=self._fake_runtime_qualification,
            ),
            patch.dict(
                os.environ,
                {"FAKE_ROUTE_TEMPLATE_PATH": str(self.route_template)},
            ),
        ):
            for worker_threads, run in enumerate(runs, start=1):
                summary = runner.run_corpus(
                    corpus_manifest=manifest,
                    simulator_binary=self.binary,
                    out_root=self.execution,
                    requested_run_ids=[run["run_id"]],
                    wall_timeout_s=5,
                    simulator_worker_threads=worker_threads,
                    _runtime_authority=self.runtime_authority,
                )
                self.assertEqual(summary["counts"]["executed"], 1)
        with patch.object(finalizer, "EXPECTED_RUN_COUNT", 2), patch.object(
            runner,
            "_run_workload_runtime_qualification",
            side_effect=self._fake_runtime_qualification,
        ):
            audit = finalizer.finalize_corpus(
                corpus_manifest=manifest,
                execution_root=self.execution,
                simulator_binary=self.binary,
                output_dir=self.output,
                _runtime_authority=self.runtime_authority,
            )
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertEqual(audit["counts"]["verified"], 2)
        self.assertEqual(
            audit["simulator_worker_threads"],
            {"values": [1, 2], "counts": {"1": 1, "2": 1}},
        )
        self.assertIn(
            {
                "reason_code": "MIXED_SIMULATOR_WORKER_THREADS",
                "counts": {"1": 1, "2": 1},
            },
            audit["global_blockers"],
        )
        self.assertEqual(
            {path.name for path in self.output.iterdir()}, {"readiness_audit.json"}
        )

    def test_second_invocation_cannot_overwrite_completed_output(self) -> None:
        self._finalize()
        before = {path.name: runner.sha256_file(path) for path in self.output.iterdir()}
        with patch.object(finalizer, "EXPECTED_RUN_COUNT", 1):
            with self.assertRaisesRegex(
                finalizer.FinalizationError, "non-empty output"
            ):
                finalizer.finalize_corpus(
                    corpus_manifest=self.manifest,
                    execution_root=self.execution,
                    simulator_binary=self.binary,
                    output_dir=self.output,
                    _runtime_authority=self.runtime_authority,
                )
        after = {path.name: runner.sha256_file(path) for path in self.output.iterdir()}
        self.assertEqual(after, before)

    def test_symlinked_run_directory_is_not_followed(self) -> None:
        original = self.execution / "runs" / self.min_run["run_id"]
        outside = self.root / "outside-sealed-run"
        original.rename(outside)
        original.symlink_to(outside, target_is_directory=True)
        audit = self._finalize()
        self.assertEqual(audit["status"], "BLOCKED")
        self.assertIn("SYMLINK_RUN_DIRECTORY", audit["runs"][0]["reason_codes"])
        self.assertFalse((self.output / "corpus_manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
