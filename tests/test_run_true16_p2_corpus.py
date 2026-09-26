#!/usr/bin/env python3
"""Safety and resume tests for the true-16 P2 corpus runner."""

from __future__ import annotations

import csv
import json
import os
import shutil
import signal
import sys
import tempfile
import _thread
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


LIMER = Path(__file__).resolve().parents[1]
TRUE16_ROOT = LIMER / "results" / "true16_hard_fault_e2e"
sys.path.insert(0, str(LIMER / "tools"))

from run_true16_p2_corpus import (  # noqa: E402
    CORPUS_SCHEMA,
    RUN_MANIFEST_SCHEMA,
    RunnerError,
    SimulatorRuntimeBinding,
    _artifact_entries,
    _validate_link_map_against_topology,
    _validate_topology,
    canonical_hash,
    run_identity_entry,
    run_corpus,
    schedule_identity_tuple,
    select_single_rail_sport,
    sha256_file,
)
import generate_true16_traffic_workload as traffic_workload  # noqa: E402
import generate_true16_p2_corpus as corpus_generator  # noqa: E402
import run_true16_p2_corpus as runner_module  # noqa: E402
import validate_ecmp_route_candidates as route_validator  # noqa: E402


class FakeRuntimeAuthority:
    """Explicit non-ELF seam used only by these runner integration tests."""

    @staticmethod
    def _validate(binding: SimulatorRuntimeBinding) -> None:
        expected = binding.record["bundle_executable_sha256"]
        if not binding.executable.is_file() or sha256_file(binding.executable) != expected:
            raise RunnerError("fake sealed runtime executable changed")
        if (
            not binding.manifest_path.is_file()
            or sha256_file(binding.manifest_path) != binding.manifest_sha256
        ):
            raise RunnerError("fake sealed runtime manifest changed")

    def prepare(
        self, execution_root: Path, source_binary: Path
    ) -> SimulatorRuntimeBinding:
        execution_root = execution_root.resolve()
        source_sha = sha256_file(source_binary)
        identity = canonical_hash(
            {
                "test_seam": "explicit-non-elf-runtime-authority",
                "source_sha256": source_sha,
            }
        )
        bundle_root = execution_root / "runtime-bundles" / identity
        executable = bundle_root / "bin" / "simulator_binary"
        library_path = bundle_root / "lib"
        manifest_path = bundle_root / "runtime_bundle_manifest.json"
        seal_path = bundle_root / "runtime_bundle_manifest.sha256"
        if not bundle_root.exists():
            executable.parent.mkdir(parents=True)
            library_path.mkdir()
            shutil.copyfile(source_binary, executable)
            executable.chmod(0o555)
            manifest = {
                "schema_version": "limer.simulator-runtime-bundle.v1",
                "status": "SEALED",
                "identity_sha256": identity,
                "executable": {
                    "source_path": str(source_binary.resolve()),
                    "execution_path": "bin/simulator_binary",
                    "sha256": source_sha,
                    "size_bytes": executable.stat().st_size,
                },
                "project_dependencies": [{"soname": "libns3-test.so"}],
                "system_dependencies": [],
                "loader_contract": {
                    "clear_inherited_prefix": "LD_",
                    "LD_LIBRARY_PATH": "lib",
                    "LD_PRELOAD": None,
                },
                "artifact_set_sha256": canonical_hash(
                    [{"path": "bin/simulator_binary", "sha256": source_sha}]
                ),
                "test_seam": True,
            }
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            seal_path.write_text(
                f"{sha256_file(manifest_path)}  runtime_bundle_manifest.json\n",
                encoding="ascii",
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_sha = sha256_file(manifest_path)
        record = {
            "schema_version": "limer.simulator-runtime-bundle.v1",
            "identity_sha256": identity,
            "execution_root": str(execution_root),
            "bundle_path": bundle_root.relative_to(execution_root).as_posix(),
            "bundle_manifest_path": manifest_path.relative_to(
                execution_root
            ).as_posix(),
            "bundle_manifest_sha256": manifest_sha,
            "bundle_manifest_seal_path": seal_path.relative_to(
                execution_root
            ).as_posix(),
            "bundle_artifact_set_sha256": manifest["artifact_set_sha256"],
            "bundle_executable_path": executable.relative_to(
                execution_root
            ).as_posix(),
            "bundle_executable_sha256": source_sha,
            "bundle_executable_size_bytes": executable.stat().st_size,
            "source_executable_path": str(source_binary.resolve()),
            "project_dependency_count": 1,
            "system_dependency_count": 0,
            "loader_isolation": manifest["loader_contract"],
            "test_seam": "explicit-non-elf-runtime-authority",
        }
        loader = {
            "status": "PASS",
            "runtime_bundle_identity_sha256": identity,
            "project_dependency_count": 1,
            "project_dependencies": [{
                "soname": "libns3-test.so",
                "path": str(library_path / "libns3-test.so"),
                "sha256": "0" * 64,
                "size_bytes": 0,
            }],
            "system_dependency_count": 0,
            "system_dependencies": [],
            "phase": "explicit_test_seam",
            "test_seam": True,
        }
        binding = SimulatorRuntimeBinding(
            execution_root=execution_root,
            bundle_root=bundle_root,
            executable=executable,
            identity_sha256=identity,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha,
            record=record,
            loader_preflight=loader,
            bundle=None,
            source_closure=None,
        )
        self._validate(binding)
        return binding

    def verify_before_run(self, binding: SimulatorRuntimeBinding) -> dict:
        self._validate(binding)
        source = Path(binding.record["source_executable_path"])
        if sha256_file(source) != binding.record["bundle_executable_sha256"]:
            raise RunnerError("fake source executable changed before run")
        return {
            "status": "PASS",
            "phase": "before_run",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
            "test_seam": True,
        }

    def execution_environment(
        self,
        binding: SimulatorRuntimeBinding,
        base_environment: dict,
        overrides: dict,
    ) -> tuple[dict, dict]:
        self._validate(binding)
        environment = dict(base_environment)
        cleared = sorted(name for name in environment if name.startswith("LD_"))
        for name in cleared:
            environment.pop(name, None)
        environment.update(overrides)
        library_path = str(binding.bundle_root / "lib")
        environment["LD_LIBRARY_PATH"] = library_path
        return environment, {
            "runtime_bundle_identity_sha256": binding.identity_sha256,
            "runtime_bundle_path": str(binding.bundle_root),
            "cleared_inherited_loader_variables": cleared,
            "LD_LIBRARY_PATH": library_path,
            "LD_PRELOAD": None,
            "test_seam": True,
        }

    def verify_process(
        self, binding: SimulatorRuntimeBinding, pid: int
    ) -> dict:
        self._validate(binding)
        return {
            "status": "PASS",
            "pid": pid,
            "runtime_bundle_identity_sha256": binding.identity_sha256,
            "project_dependency_count": 1,
            "project_dependencies": [{"soname": "libns3-test.so"}],
            "system_dependency_count": 0,
            "system_dependencies": [],
            "test_seam": "fast-process-maps-bypass",
        }

    def verify_after_run(self, binding: SimulatorRuntimeBinding) -> dict:
        self._validate(binding)
        source = Path(binding.record["source_executable_path"])
        if sha256_file(source) != binding.record["bundle_executable_sha256"]:
            raise RunnerError("fake source executable changed after run")
        return {
            "status": "PASS",
            "phase": "after_run",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
            "test_seam": True,
        }

    def verify_for_reuse(self, binding: SimulatorRuntimeBinding) -> dict:
        # Reuse intentionally checks only the sealed runtime, never its source.
        self._validate(binding)
        return {
            "status": "PASS",
            "phase": "reuse",
            "bundle_integrity_status": "PASS",
            "source_closure_reopened": False,
            "test_seam": True,
        }


class P2CorpusRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.corpus_root = self.root / "corpus"
        self.corpus_root.mkdir()
        self.out_root = self.root / "out"
        self.counter = self.root / "invocations.txt"
        self.binary = self._write_fake_simulator()
        self.runtime_authority = FakeRuntimeAuthority()
        self.manifest_path = self._write_corpus()
        self.route_template = self._write_route_template()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write(path: Path, text: str) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return sha256_file(path)

    def _write_fake_simulator(self) -> Path:
        path = self.root / "fake_simulator.py"
        path.write_text(
            """#!/usr/bin/env python3
import csv
import json
import os
import signal
import sys
import time
from pathlib import Path

out = Path(os.environ['LIMER_TELEMETRY_DIR'])
counter = Path(os.environ['FAKE_COUNTER_PATH'])
previous = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(previous + 1))
mode = os.environ.get('FAKE_SIMULATOR_MODE', 'complete')
if mode == 'timeout':
    time.sleep(30)
if mode == 'sigkill':
    os.kill(os.getpid(), signal.SIGKILL)
if mode == 'failure':
    raise SystemExit(7)
run_id = os.environ['LIMER_RUN_ID']
finish = int(os.environ['LIMER_OBSERVATION_STOP_NS'])
link_source = next((out / 'inputs').glob('link_map*'))
(out / 'link_map.csv').write_bytes(link_source.read_bytes())
if mode != 'no_route':
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
rows = []
if os.environ.get('FAKE_WORKLOAD_COMPLETE', '1') == '1':
    rows.append([run_id, 'finish_barrier', '', finish - 10, 16, 16,
                 'WORKLOAD_COMPLETE'])
if mode != 'censored':
    completed = bool(rows)
    rows.append([run_id, 'observation_horizon', finish, finish,
                 16 if completed else 0, 16,
                 ('OBSERVATION_WINDOW_COMPLETE_AFTER_WORKLOAD' if completed
                  else 'OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE')])
if mode != 'no_lifecycle':
    with (out / 'run_lifecycle.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['run_id', 'event', 'scheduled_ns', 'actual_ns',
                         'finished_ranks', 'world_size', 'status'])
        writer.writerows(rows)
target_queue = 0 if mode == 'paired_pressure' else 4096
paired_queue = 4096 if mode == 'paired_pressure' else 2048
(out / 'switch_telemetry.csv').write_text(
    'run_id,timestamp_ns,switch_id,port_id,link_id,direction,queue_bytes,'
    'max_queue_bytes,observed_throughput_bps,link_state,tx_bytes\\n'
    f'{run_id},0,20,1,L0-20,tx,0,0,0,up,0\\n'
    f'{run_id},0,24,1,L0-24,tx,0,0,0,up,0\\n'
    f'{run_id},50,20,1,L0-20,tx,2048,{target_queue},100000000000,up,10000\\n'
    f'{run_id},50,24,1,L0-24,tx,1024,{paired_queue},80000000000,up,8000\\n')
for filename in ('nic_telemetry.csv', 'collective_transaction.csv',
                 'collective_telemetry.csv'):
    (out / filename).write_text('mocked by runner integration test\\n')
(out / 'fault_application_telemetry.csv').write_text(
    'run_id,fault_id,fault_type,target_link_id,transition,scheduled_ns,'
    'actual_ns,parameter_before,parameter_after,mechanism,rng_stream,status\\n')
if mode != 'no_allocator':
    (out / 'training_source_port_allocator.csv').write_text(
        'run_id,interval_first,interval_end_exclusive,capacity,allocations,'
        'releases,reuses,active_at_stop,peak_active,pair_count,pairs_with_reuse,'
        'max_pair_allocations,max_pair_reuses,external_conflicts,exhaustions,'
        'invariant_errors,min_allocated_port,max_allocated_port,status\\n'
        f'{run_id},10000,49152,39152,1,0,0,1,1,1,0,1,0,0,0,0,'
        '10000,10000,PASS\\n')
background_schedule = os.environ.get('LIMER_BACKGROUND_FLOW_SCHEDULE')
if background_schedule:
    with Path(background_schedule).open(newline='') as stream:
        background = list(csv.DictReader(stream))
    columns = [
        'run_id', 'event_id', 'flow_id', 'scenario', 'event',
        'scheduled_start_ns', 'actual_ns', 'first_tx_ns', 'first_ack_ns',
        'src_rank', 'dst_rank', 'bytes', 'pg', 'sport', 'dport', 'status',
    ]
    with (out / 'background_flow_application.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for flow in background:
            common = {key: flow[key] for key in (
                'event_id', 'flow_id', 'scenario', 'scheduled_start_ns',
                'src_rank', 'dst_rank', 'bytes', 'pg', 'sport', 'dport')}
            writer.writerow({'run_id': run_id, **common, 'event': 'SCHEDULED',
                             'status': 'INSTALLED'})
            start = int(flow['scheduled_start_ns'])
            writer.writerow({'run_id': run_id, **common, 'event': 'START',
                             'actual_ns': start, 'status': 'QP_CREATED'})
            if mode == 'background_censored':
                writer.writerow({'run_id': run_id, **common, 'event': 'CENSORED',
                                 'actual_ns': finish,
                                 'status': 'STARTED_NOT_ACK_COMPLETE_AT_STOP'})
            else:
                complete_ns = 201 if mode == 'deadline_late' else start + 30
                writer.writerow({'run_id': run_id, **common, 'event': 'COMPLETE',
                                 'actual_ns': complete_ns,
                                 'first_tx_ns': start + 1,
                                 'first_ack_ns': start + 20,
                                 'status': 'ACK_COMPLETE'})
    with (out / 'inputs/truth_schedule.csv').open(newline='') as stream:
        truth = next(csv.DictReader(stream))
    transport = json.loads(truth['action_parameters_json'])
    rdma_columns = [
        'run_id', 'timestamp_ns', 'node_id', 'rank_id', 'logical_qp_id',
        'transport_epoch', 'traffic_class', 'event', 'event_detail',
        'wc_status', 'src_rank', 'dst_rank', 'sport', 'primary_nic',
        'backup_nic', 'active_nic', 'backup_ready_ns', 'failover_ns',
        'backup_first_tx_ns', 'backup_first_ack_ns', 'standby_tx_bytes',
        'snd_una', 'snd_nxt', 'retry_count', 'retry_limit', 'rto_us',
    ]
    if mode == 'rdma_extra_column':
        rdma_columns.append('unexpected_column')
    primary = transport['route_candidate_order_host_ports'][transport['route_bucket']]
    if mode == 'paired_port':
        primary = 3 if primary == 2 else 2
    with (out / 'rdma_wc_telemetry.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rdma_columns)
        writer.writeheader()
        for flow in background:
            start = int(flow['scheduled_start_ns'])
            logical = (f"{flow['src_rank']}-{flow['dst_rank']}-"
                       f"{flow['sport']}-{flow['pg']}")
            common = {
                'run_id': run_id, 'node_id': flow['src_rank'],
                'rank_id': flow['src_rank'], 'logical_qp_id': logical,
                'transport_epoch': 0, 'traffic_class': 'BACKGROUND',
                'src_rank': flow['src_rank'], 'dst_rank': flow['dst_rank'],
                'sport': flow['sport'], 'primary_nic': primary,
                'backup_nic': 4294967295, 'active_nic': primary,
                'standby_tx_bytes': 0, 'retry_count': 0,
                'retry_limit': os.environ['LIMER_RDMA_RETRY_LIMIT'],
                'rto_us': os.environ['LIMER_RDMA_RTO_US'],
            }
            writer.writerow({**common, 'timestamp_ns': start,
                             'event': 'QP_CREATED', 'event_detail': primary,
                             'wc_status': 'NONE', 'snd_una': 0, 'snd_nxt': 0})
            if mode != 'background_censored':
                if mode == 'retry_then_success':
                    writer.writerow({**common, 'timestamp_ns': start + 10,
                                     'event': 'RTO_RETRY', 'event_detail': 1,
                                     'wc_status': 'NONE', 'retry_count': 1,
                                     'snd_una': 0, 'snd_nxt': flow['bytes']})
                writer.writerow({**common, 'timestamp_ns': start + 30,
                                 'event': 'WC', 'event_detail': 1,
                                 'wc_status': 'SUCCESS',
                                 'snd_una': flow['bytes'],
                                 'snd_nxt': flow['bytes']})
(out / 'fake_observed.json').write_text(json.dumps({
    'argv': sys.argv,
    'runtime_config': Path(sys.argv[sys.argv.index('-c') + 1]).read_text(),
    'telemetry_interval_us': os.environ.get('LIMER_TELEMETRY_INTERVAL_US'),
    'hard_event_detector_enable': os.environ.get('LIMER_HARD_EVENT_DETECTOR_ENABLE'),
    'recovery_action_enable': os.environ.get('LIMER_RECOVERY_ACTION_ENABLE'),
    'rdma_recovery_transport_enable': os.environ.get('LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE'),
    'as_pxn_enable': os.environ.get('AS_PXN_ENABLE'),
    'as_log_level': os.environ.get('AS_LOG_LEVEL'),
    'fault_schedule': os.environ.get('LIMER_FAULT_SCHEDULE'),
    'background_flow_schedule': background_schedule,
    'rdma_rto_us': os.environ.get('LIMER_RDMA_RTO_US'),
    'rdma_retry_limit': os.environ.get('LIMER_RDMA_RETRY_LIMIT'),
    'ld_library_path': os.environ.get('LD_LIBRARY_PATH'),
    'ld_preload': os.environ.get('LD_PRELOAD'),
}))
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def _schedule(
        self,
        run_id: str,
        *,
        implementation_status: str,
        with_injector: bool,
        safe: bool = True,
        kind: str = "fault",
        with_background: bool = False,
    ) -> dict:
        schedule_dir = self.corpus_root / "schedules"
        truth = schedule_dir / f"{run_id}.truth.csv"
        event_id = f"evt-{run_id}"
        if with_background:
            parameters = {
                "destination_rank": 0,
                "bottleneck_access_link_id": "L0-20",
                "paired_access_link_id": "L0-24",
                "data_plane": "A",
                "route_candidate_order_host_ports": [2, 3],
                "route_bucket": 0,
                "hash_algorithm": "ns3-murmur3-x86-32",
                "hash_seed_u32": 0x8BADF00D,
                "hash_tuple": "native-le-sip-dip-sport-dport",
                "hash_byte_order": "little",
                "pin_reverse_ack": True,
                "predeclared_window_policy": "scheduled_qp_launch_window",
                "realized_window_policy": "first_data_tx_to_last_ack_complete",
                "completion_deadline_ns": 200,
                "rdma_rto_us": 250000,
                "rdma_retry_limit": 0,
                "max_rto_retry_events": 0,
            }
            escaped = json.dumps(
                parameters, sort_keys=True, separators=(",", ":")
            ).replace('"', '""')
            truth_text = (
                "event_id,implementation_status,start_time_ns,end_time_ns,"
                "action_scope,action_parameters_json\n"
                f'{event_id},{implementation_status},20,21,workload,"{escaped}"\n'
            )
        else:
            truth_text = (
                "event_id,implementation_status,start_time_ns,end_time_ns,"
                "action_scope\n"
                f"{event_id},{implementation_status},10,100,\n"
            )
        truth_sha = self._write(
            truth,
            truth_text,
        )
        schedule = {
            "kind": kind,
            "path": truth.relative_to(self.corpus_root).as_posix(),
            "sha256": truth_sha,
            "event_id": event_id,
            "implementation_status": implementation_status,
            "generated_before_run": True,
            "independent_of_features": True,
        }
        if with_injector:
            injection = schedule_dir / f"{run_id}.inject.csv"
            injection_sha = self._write(
                injection,
                "fault_id,fault_type,target_link_id,start_time_ns,end_time_ns,"
                "severity,parameter_before,parameter_after,recovery_delay_ns,"
                "parent_event_id,implementation_status\n"
                f"fault-{run_id},bandwidth_degradation,L0-24,10,100,0.5,"
                f"100Gbps,50Gbps,0,{event_id},{implementation_status}\n",
            )
            schedule["simulator_injection_schedule"] = {
                "path": injection.relative_to(self.corpus_root).as_posix(),
                "sha256": injection_sha,
                "safe_to_execute": safe,
            }
        if with_background:
            background_path = schedule_dir / f"{run_id}.background.csv"
            used_sports: set[int] = set()
            rows = [
                f"{event_id},flow-{run_id}-{source:02d},incast,20,{source},0,"
                f"1048576,3,{select_single_rail_sport(src=source, dst=0, dport=20000, route_bucket=0, used=used_sports)},20000"
                for source in range(4, 16)
            ]
            background_sha = self._write(
                background_path,
                "event_id,flow_id,scenario,scheduled_start_ns,src_rank,"
                "dst_rank,bytes,pg,sport,dport\n" + "\n".join(rows) + "\n",
            )
            schedule["background_flow_schedule"] = {
                "kind": "background_rdma",
                "path": background_path.relative_to(self.corpus_root).as_posix(),
                "sha256": background_sha,
                "safe_to_execute": safe,
            }
        return schedule

    def _run(
        self,
        run_id: str,
        *,
        role: str = "fault",
        class_label: str = "GRAY_FAULT",
        mechanism_id: str = "dual_endpoint_data_rate",
        mechanism_status: str = "PLANNED",
        schedule_status: str = "EXECUTABLE_CURRENT_INJECTOR",
        with_injector: bool = True,
        with_background: bool = False,
        safe: bool = True,
        scenario: str | None = None,
        fault_family: str | None = None,
        stability_gate_required: bool = False,
    ) -> dict:
        run = {
            "run_id": run_id,
            "run_role": role,
            "class_label": class_label,
            "scenario": scenario or ("incast" if with_background else "test"),
            "partition": "train",
            "split_group_id": f"grp-{run_id}",
            "simulation_seed": 17,
            "virtual_finish_ns": 1000,
            "effective_workload_sha256": sha256_file(
                LIMER
                / "configs"
                / "microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
            ),
            "target_gpu": 0 if with_background else None,
            "target_link_id": "L0-20" if with_background else None,
            "paired_link_id": "L0-24" if with_background else None,
            "mechanism": {
                "mechanism_id": mechanism_id,
                "implementation_status": mechanism_status,
            },
            "schedule": self._schedule(
                run_id,
                implementation_status=schedule_status,
                with_injector=with_injector,
                with_background=with_background,
                safe=safe,
                kind="congestion" if class_label == "CONGESTION" else role,
            ),
        }
        if fault_family is not None:
            run["fault_family"] = fault_family
        if stability_gate_required:
            run["simulator_stability"] = {
                "gate_required": True,
                "status": "PENDING_EXECUTION",
                "reason": "must be established from completed simulator artifacts",
            }
        return run

    def _write_corpus(self) -> Path:
        inputs = self.root / "inputs"
        input_values = {
            "contract": "contract: test\n",
            "link_map": (TRUE16_ROOT / "healthy" / "link_map.csv").read_text(
                encoding="utf-8"
            ),
            "topology": (
                TRUE16_ROOT
                / "topology"
                / "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
            ).read_text(encoding="utf-8"),
            "workload": (
                LIMER / "configs" /
                "microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
            ).read_text(encoding="utf-8"),
            "simulator_config": (
                "FLOW_FILE /shared/flow.txt\n"
                "TRACE_FILE /shared/trace.txt\n"
                "TRACE_OUTPUT_FILE /shared/trace.tr\n"
                "FCT_OUTPUT_FILE /shared/fct.txt\n"
                "PFC_OUTPUT_FILE /shared/pfc.txt\n"
                "QLEN_MON_FILE /shared/qlen.txt\n"
                "BW_MON_FILE /shared/bw.txt\n"
                "RATE_MON_FILE /shared/rate.txt\n"
                "CNP_MON_FILE /shared/cnp.txt\n"
            ),
        }
        input_artifacts = {}
        for name, value in input_values.items():
            path = inputs / name
            input_artifacts[name] = {
                "path": str(path.resolve()),
                "sha256": self._write(path, value),
            }

        refs = {}
        for name in (
            "feature_schema", "split_manifest", "leakage_checks", "observability_report"
        ):
            path = self.corpus_root / f"{name}.json"
            refs[name] = {
                "path": path.relative_to(self.corpus_root).as_posix(),
                "sha256": self._write(path, json.dumps({"name": name}) + "\n"),
            }
        refs["split_manifest"]["paired_holdout_gpu_ids"] = [12, 13, 14, 15]

        runs = [
            self._run(
                "fault-ok",
                fault_family="bandwidth_degradation",
                stability_gate_required=True,
            ),
            self._run(
                "random-loss-ok",
                mechanism_id="verified_random_packet_drop",
                fault_family="random_loss",
                stability_gate_required=True,
            ),
            self._run(
                "fault-blocked",
                mechanism_id="forced_link_state_flap_proxy",
                mechanism_status="UNAVAILABLE",
                schedule_status="BLOCKED_TRUE_CARRIER_FLAP",
                safe=False,
            ),
            self._run(
                "congestion-blocked",
                role="congestion",
                class_label="CONGESTION",
                mechanism_id="scheduled_incast",
                mechanism_status="UNAVAILABLE",
                schedule_status="PENDING_CONGESTION_EXECUTOR",
                with_injector=False,
            ),
            self._run(
                "congestion-ok",
                role="congestion",
                class_label="CONGESTION",
                mechanism_id="background_rdma_incast",
                mechanism_status="PLANNED",
                schedule_status="EXECUTABLE_BACKGROUND_RDMA",
                with_injector=False,
                with_background=True,
                scenario="incast",
            ),
            self._run(
                "healthy-ok",
                role="healthy",
                class_label="HEALTHY",
                mechanism_id="no_physical_injection",
                schedule_status="EXECUTABLE_NO_INJECTION",
                with_injector=False,
                stability_gate_required=True,
            ),
        ]
        schedule_set = canonical_hash([schedule_identity_tuple(run) for run in runs])
        identity = {
            "identity_schema": "limer.p2-corpus-identity.v2",
            "contract_sha256": input_artifacts["contract"]["sha256"],
            "link_map_sha256": input_artifacts["link_map"]["sha256"],
            "topology_sha256": input_artifacts["topology"]["sha256"],
            "workload_sha256": input_artifacts["workload"]["sha256"],
            "simulator_config_sha256": input_artifacts[
                "simulator_config"
            ]["sha256"],
            "seed": 23,
            "holdouts": [12, 13, 14, 15],
            "runs": [run_identity_entry(run) for run in runs],
        }
        manifest = {
            "schema_version": CORPUS_SCHEMA,
            "status": "PREPARED",
            "identity_schema": "limer.p2-corpus-identity.v2",
            "corpus_id": "p2-" + canonical_hash(identity)[:24],
            "generation_seed": 23,
            "input_artifacts": input_artifacts,
            "topology_contract": {"gpu_count": 16},
            "schedule_set_sha256": schedule_set,
            "workload_qualification": {
                "status": "PENDING_EXECUTION",
                "static_qualification_status": "PASS",
                "static_qualification_profile": (
                    traffic_workload.HORIZON_PREFIX_PROFILE
                ),
                "static_report": traffic_workload.validate_workload(
                    Path(input_artifacts["workload"]["path"])
                ),
                "planned_maximum_virtual_finish_ns": max(
                    run["virtual_finish_ns"] for run in runs
                ),
            },
            "runs": runs,
            **refs,
        }
        manifest["workload_qualification"]["static_report_sha256"] = (
            canonical_hash(manifest["workload_qualification"]["static_report"])
        )
        path = self.corpus_root / "corpus_manifest.json"
        path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        return path

    def _write_route_template(self) -> Path:
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        link_map = Path(manifest["input_artifacts"]["link_map"]["path"])
        artifact = route_validator._read_bound_artifact(
            link_map, "runner_test_link_map", None
        )
        topology = route_validator._parse_link_map(artifact)
        topology_path = Path(manifest["input_artifacts"]["topology"]["path"])
        topology = route_validator._bind_topology_file(
            topology,
            route_validator._read_bound_artifact(
                topology_path, "runner_test_topology", None
            ),
        )
        rows = route_validator.reconstruct_expected_rows(topology, "template-run")
        output = self.root / "route_template.csv"
        with output.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(route_validator.ROUTE_COLUMNS)
            writer.writerows(row.identity() for row in rows)
        return output

    def _execute(self, run_ids, **kwargs):
        environment = {
            "FAKE_COUNTER_PATH": str(self.counter),
            "FAKE_ROUTE_TEMPLATE_PATH": str(self.route_template),
            "FAKE_SIMULATOR_MODE": kwargs.pop("mode", "complete"),
            "FAKE_WORKLOAD_COMPLETE": kwargs.pop("workload_complete", "1"),
        }
        qualification_status = kwargs.pop("runtime_qualification_status", "PASS")
        semantic_status = kwargs.pop("semantic_validation_status", "PASS")

        def mocked_qualification(*, directory, run, corpus):
            sources = runner_module._runtime_qualification_source_paths(
                directory, run
            )
            collective = run.get("schedule", {}).get(
                "collective_workload_override"
            )
            static_report = (
                collective["static_validation"]
                if isinstance(collective, dict)
                else corpus.manifest["workload_qualification"]["static_report"]
            )
            override_evidence = {}
            if isinstance(collective, dict):
                override_evidence = {
                    "collective_override_pre_event_post_runtime": {
                        "actual_application_window_authority": (
                            "role_bound_collective_transaction"
                        )
                    }
                }
            return {
                "schema_version": "limer.p2-workload-runtime-qualification.v1",
                "status": qualification_status,
                "run_id": run["run_id"],
                "qualification_profile": static_report[
                    "qualification_profile"
                ],
                "planned_run_sha256": canonical_hash(run),
                "static_workload_report_sha256": canonical_hash(static_report),
                "contract": {"mocked_small_runner_fixture": True},
                "source_artifacts": {
                    name: {"path": path.name, "sha256": sha256_file(path)}
                    for name, path in sources.items()
                },
                "checks": [{
                    "name": "mocked_runner_integration",
                    "status": qualification_status,
                }],
                "errors": ([] if qualification_status == "PASS"
                           else ["injected runtime qualification failure"]),
                "evidence": override_evidence,
            }

        def mocked_semantic(*, directory, run, injection_schedule):
            sources = {
                "fault_application": directory / "fault_application_telemetry.csv",
                "switch": directory / "switch_telemetry.csv",
                "nic": directory / "nic_telemetry.csv",
                "collective": directory / "collective_telemetry.csv",
            }
            if injection_schedule is not None:
                sources["injection_schedule"] = injection_schedule
            mechanism = run["mechanism"]["mechanism_id"]
            healthy = run["class_label"] == "HEALTHY"
            return {
                "schema_version": "limer.p2-run-semantics.v1",
                "run_id": run["run_id"],
                "mechanism_id": mechanism,
                "status": semantic_status,
                "source_artifact_sha256": sha256_file(sources["switch"]),
                "source_artifacts_sha256": {
                    name: sha256_file(path) for name, path in sources.items()
                },
                "checks": [{
                    "name": "mocked_small_runner_semantic_integration",
                    "status": semantic_status,
                }],
                "injected_physical_fault": not healthy,
                "target_link_state_during_event": "up",
                "observed_effects": (
                    [] if healthy else ["throughput_degradation"]
                ),
                "impairment": "" if healthy else "capacity",
                "packet_disposition": "not_applicable",
                "recoverable_error_proxy": False,
                "fault_family": "" if healthy else "bandwidth_degradation",
                "target_link_id": None,
                "target_link_ids": [],
                "scheduled_onset_ns": None if healthy else 10,
                "actual_apply_ns": None if healthy else 10,
                "event_end_ns": None if healthy else 100,
                "healthy_reference_used": False,
                "evidence": {"mocked_small_runner_fixture": True},
            }

        with patch.dict(os.environ, environment), patch(
            "run_true16_p2_corpus._run_workload_runtime_qualification",
            side_effect=mocked_qualification,
        ), patch(
            "run_true16_p2_corpus._run_non_background_semantic_validation",
            side_effect=mocked_semantic,
        ):
            return run_corpus(
                corpus_manifest=self.manifest_path,
                simulator_binary=self.binary,
                out_root=self.out_root,
                requested_run_ids=run_ids,
                wall_timeout_s=kwargs.pop("wall_timeout_s", 2),
                _runtime_authority=self.runtime_authority,
                **kwargs,
            )

    def test_complete_observation_does_not_claim_workload_completion(self) -> None:
        summary = self._execute(["fault-ok"], workload_complete="0")
        self.assertEqual(summary["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "fault-ok"
        manifest = json.loads((final / "run_manifest.json").read_text())
        self.assertEqual(manifest["schema_version"], RUN_MANIFEST_SCHEMA)
        self.assertEqual(manifest["execution_status"], "COMPLETE")
        self.assertEqual(
            manifest["lifecycle_status"],
            "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE",
        )
        self.assertFalse(manifest["workload_completed"])
        self.assertEqual(manifest["workload_runtime_qualification_status"], "PASS")
        self.assertEqual(manifest["semantic_validation_status"], "PASS")
        semantic_path = final / "semantic_validation.json"
        self.assertEqual(
            manifest["semantic_validation_sha256"], sha256_file(semantic_path)
        )
        self.assertEqual(json.loads(semantic_path.read_text())["status"], "PASS")
        qualification = json.loads(
            (final / "workload_runtime_qualification.json").read_text()
        )
        self.assertEqual(qualification["status"], "PASS")
        self.assertEqual(
            manifest["workload_runtime_qualification_sha256"],
            sha256_file(final / "workload_runtime_qualification.json"),
        )
        route_path = final / "ecmp_route_candidates.csv"
        route_report_path = final / "ecmp_route_candidate_validation.json"
        route_report = json.loads(route_report_path.read_text())
        self.assertEqual(manifest["ecmp_route_candidate_validation_status"], "PASS")
        self.assertEqual(
            manifest["ecmp_route_candidates_sha256"], sha256_file(route_path)
        )
        self.assertEqual(
            manifest["ecmp_route_candidate_validation_sha256"],
            sha256_file(route_report_path),
        )
        self.assertEqual(
            manifest["ecmp_route_candidate_report_sha256"],
            route_report["report_sha256"],
        )
        allocator_raw = final / "training_source_port_allocator.csv"
        allocator_report_path = (
            final / "training_source_port_allocator_validation.json"
        )
        allocator_report = json.loads(allocator_report_path.read_text())
        self.assertEqual(
            manifest["training_source_port_allocator_validation_status"],
            "PASS",
        )
        self.assertEqual(
            manifest["training_source_port_allocator_sha256"],
            sha256_file(allocator_raw),
        )
        self.assertEqual(
            manifest["training_source_port_allocator_validation_sha256"],
            sha256_file(allocator_report_path),
        )
        self.assertFalse(allocator_report["requirements"]["require_reuse"])
        self.assertEqual(allocator_report["metrics"]["reuses"], 0)
        self.assertFalse(manifest["hard_event_detector_enabled"])
        self.assertFalse(manifest["recovery_action_enabled"])
        lifecycle = next(
            item for item in manifest["artifacts"]
            if item["path"] == "run_lifecycle.csv"
        )
        self.assertEqual(lifecycle["sha256"], sha256_file(final / lifecycle["path"]))
        observed = json.loads((final / "fake_observed.json").read_text())
        self.assertEqual(observed["hard_event_detector_enable"], "0")
        self.assertEqual(observed["recovery_action_enable"], "0")
        self.assertEqual(observed["telemetry_interval_us"], "1000")
        self.assertIn("simulator_injection_schedule.csv", observed["fault_schedule"])
        self.assertEqual(observed["argv"][1:3], ["-t", "1"])
        runtime_record = summary["runtime_closure"]
        bundle_binary = self.out_root / runtime_record["bundle_executable_path"]
        self.assertEqual(observed["argv"][0], str(bundle_binary))
        self.assertEqual(manifest["simulator_worker_threads"], 1)
        self.assertEqual(manifest["simulator"]["worker_threads"], 1)
        self.assertEqual(summary["simulator_worker_threads"], 1)
        self.assertIsNone(manifest["simulator"]["execution_copy"])
        self.assertFalse((final / "inputs" / "simulator_binary").exists())
        self.assertTrue(bundle_binary.is_file())
        self.assertEqual(
            sha256_file(bundle_binary), manifest["simulator"]["sha256"]
        )
        self.assertEqual(manifest["runtime_closure"], runtime_record)
        runtime_evidence = json.loads(
            (final / "runtime_execution_evidence.json").read_text()
        )
        self.assertEqual(runtime_evidence["status"], "PASS")
        self.assertEqual(
            runtime_evidence["process_mapping_verification"]["status"],
            "PASS",
        )
        self.assertEqual(runtime_evidence["post_run_verification"]["status"], "PASS")
        self.assertNotIn("/shared/", observed["runtime_config"])
        self.assertIn("FLOW_FILE empty_flow.txt", observed["runtime_config"])
        self.assertEqual((final / "raw_simai" / "empty_flow.txt").read_text(), "0\n")

    def test_worker_threads_are_recorded_and_reuse_requires_exact_match(self) -> None:
        first = self._execute(
            ["healthy-ok"], simulator_worker_threads=2
        )
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "healthy-ok"
        manifest = json.loads((final / "run_manifest.json").read_text())
        invocation = json.loads((final / "invocation.json").read_text())
        self.assertEqual(manifest["simulator_worker_threads"], 2)
        self.assertEqual(manifest["simulator"]["worker_threads"], 2)
        self.assertEqual(manifest["simulator"]["argv"][1:3], ["-t", "2"])
        self.assertEqual(invocation["simulator_worker_threads"], 2)

        mismatched = self._execute(
            ["healthy-ok"], simulator_worker_threads=1
        )
        self.assertEqual(mismatched["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn("worker-thread count", mismatched["results"][0]["error"])

        with self.assertRaisesRegex(RunnerError, "must be in"):
            self._execute(
                ["fault-ok"], dry_run=True, simulator_worker_threads=0
            )

    def test_runtime_qualification_failure_is_preserved_and_not_published(self) -> None:
        summary = self._execute(
            ["fault-ok"], workload_complete="0",
            runtime_qualification_status="FAIL",
        )
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        pending = Path(result["path"])
        qualification = json.loads(
            (pending / "workload_runtime_qualification.json").read_text()
        )
        self.assertEqual(qualification["status"], "FAIL")
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertIn("strict workload runtime qualification failed", attempt["reason"])

    def test_missing_route_sidecar_is_preserved_and_not_published(self) -> None:
        summary = self._execute(["fault-ok"], mode="no_route")
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertIn("route candidate validation failed", result["error"])
        self.assertFalse((self.out_root / "runs/fault-ok").exists())
        pending = Path(result["path"])
        report = json.loads(
            (pending / "ecmp_route_candidate_validation.json").read_text()
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["errors"][0]["code"], "ARTIFACT_MISSING")

    def test_missing_allocator_sidecar_is_preserved_and_not_published(self) -> None:
        summary = self._execute(["fault-ok"], mode="no_allocator")
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertIn("source-port allocator validation failed", result["error"])
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        pending = Path(result["path"])
        report = json.loads(
            (
                pending / "training_source_port_allocator_validation.json"
            ).read_text()
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("allocator evidence is missing", report["errors"][0])

    def test_semantic_failure_is_preserved_and_not_published(self) -> None:
        summary = self._execute(
            ["fault-ok"], semantic_validation_status="FAIL",
        )
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        pending = Path(result["path"])
        semantic = json.loads((pending / "semantic_validation.json").read_text())
        self.assertEqual(semantic["status"], "FAIL")
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertIn("mechanism semantics failed", attempt["reason"])

    def test_timeout_is_preserved_as_pending_and_not_published(self) -> None:
        summary = self._execute(
            ["fault-ok"], mode="timeout", wall_timeout_s=0.05
        )
        result = summary["results"][0]
        self.assertEqual(result["status"], "TIMED_OUT")
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        pending = Path(result["path"])
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["status"], "TIMED_OUT")
        self.assertEqual(attempt["simulator_worker_threads"], 1)
        resources = attempt["resource_observation"]
        self.assertEqual(
            resources["schema_version"],
            "limer.process-resource-observation.v1",
        )
        self.assertGreaterEqual(resources["sample_count"], 1)
        self.assertTrue((pending / "run.log").is_file())

    def test_sigkill_with_cgroup_oom_delta_is_classified_as_oom(self) -> None:
        before = {
            "available": True,
            "path": "/sys/fs/cgroup/test",
            "events": {"oom": 2, "oom_kill": 1},
        }
        after = {
            "available": True,
            "path": "/sys/fs/cgroup/test",
            "events": {"oom": 3, "oom_kill": 2},
        }
        with patch(
            "run_true16_p2_corpus._read_cgroup_memory",
            side_effect=[before, after],
        ):
            summary = self._execute(["fault-ok"], mode="sigkill")
        result = summary["results"][0]
        self.assertEqual(result["status"], "PROCESS_FAILED")
        self.assertEqual(result["exit_code"], -signal.SIGKILL)
        attempt = json.loads(
            (Path(result["path"]) / "attempt_manifest.json").read_text()
        )
        self.assertIn("OOM kill", attempt["reason"])
        resources = attempt["resource_observation"]
        self.assertTrue(resources["oom_kill_observed_during_attempt"])
        self.assertEqual(resources["cgroup_memory_event_delta"]["oom_kill"], 1)
        self.assertEqual(resources["exit_signal"], signal.SIGKILL)

    def test_operator_interrupt_terminates_child_and_preserves_progress(self) -> None:
        timer = threading.Timer(0.1, _thread.interrupt_main)
        timer.start()
        try:
            summary = self._execute(
                ["fault-ok"], mode="timeout", wall_timeout_s=10
            )
        finally:
            timer.cancel()
            timer.join()
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTERRUPTED")
        self.assertEqual(summary["counts"]["interrupted"], 1)
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        pending = Path(result["path"])
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["status"], "INTERRUPTED")
        self.assertIsNotNone(attempt["exit_code"])
        progress = json.loads(Path(summary["progress_path"]).read_text())
        self.assertEqual(progress["status"], "INTERRUPTED")
        self.assertEqual(progress["simulator_worker_threads"], 1)
        self.assertIsNone(progress["current_run_id"])
        self.assertEqual(progress["counts"]["interrupted"], 1)

    def test_successful_execution_seals_complete_progress_record(self) -> None:
        summary = self._execute(["healthy-ok"])
        self.assertEqual(summary["results"][0]["status"], "EXECUTED")
        progress_path = Path(summary["progress_path"])
        self.assertTrue(progress_path.is_file())
        progress = json.loads(progress_path.read_text())
        self.assertEqual(progress["status"], "COMPLETE")
        self.assertIsNone(progress["current_run_id"])
        self.assertEqual(progress["selected_run_ids"], ["healthy-ok"])
        self.assertEqual(progress["results"][0]["status"], "EXECUTED")
        self.assertEqual(progress["counts"]["executed"], 1)
        self.assertEqual(progress["runtime_closure"], summary["runtime_closure"])
        self.assertEqual(
            progress["runtime_loader_preflight"]["status"], "PASS"
        )

    def test_dry_run_validates_without_sealing_non_elf_fixture(self) -> None:
        dry_root = self.root / "dry-only"
        summary = run_corpus(
            corpus_manifest=self.manifest_path,
            simulator_binary=self.binary,
            out_root=dry_root,
            requested_run_ids=["healthy-ok"],
            dry_run=True,
        )
        self.assertEqual(summary["results"][0]["status"], "DRY_RUN_EXECUTABLE")
        self.assertFalse(summary["runtime_bundle_sealed"])
        self.assertIsNone(summary["runtime_closure"])
        self.assertFalse(dry_root.exists())

    def test_production_authority_rejects_non_elf_simulator(self) -> None:
        with self.assertRaisesRegex(RunnerError, "not an ELF"):
            run_corpus(
                corpus_manifest=self.manifest_path,
                simulator_binary=self.binary,
                out_root=self.root / "production-authority",
                requested_run_ids=["healthy-ok"],
            )

    def test_two_runs_share_one_runtime_bundle_without_per_run_copy(self) -> None:
        summary = self._execute(["fault-ok", "healthy-ok"])
        self.assertEqual(
            [item["status"] for item in summary["results"]],
            ["EXECUTED", "EXECUTED"],
        )
        runtime_root = self.out_root / "runtime-bundles"
        bundles = [item for item in runtime_root.iterdir() if item.is_dir()]
        self.assertEqual(len(bundles), 1)
        identity = summary["runtime_closure"]["identity_sha256"]
        self.assertEqual(bundles[0].name, identity)
        for run_id in ("fault-ok", "healthy-ok"):
            final = self.out_root / "runs" / run_id
            self.assertFalse((final / "inputs" / "simulator_binary").exists())
            manifest = json.loads((final / "run_manifest.json").read_text())
            self.assertEqual(
                manifest["runtime_closure"]["identity_sha256"], identity
            )

    def test_live_process_mapping_failure_is_fail_closed(self) -> None:
        with patch.object(
            self.runtime_authority,
            "verify_process",
            side_effect=RunnerError("injected /proc maps failure"),
        ):
            summary = self._execute(["healthy-ok"])
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertFalse((self.out_root / "runs" / "healthy-ok").exists())
        pending = Path(result["path"])
        runtime_evidence = json.loads(
            (pending / "runtime_execution_evidence.json").read_text()
        )
        self.assertEqual(runtime_evidence["status"], "FAIL")
        self.assertEqual(
            runtime_evidence["process_mapping_verification"]["status"],
            "FAIL",
        )
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["runtime_execution_status"], "FAIL")
        self.assertEqual(
            attempt["runtime_closure"]["identity_sha256"],
            summary["runtime_closure"]["identity_sha256"],
        )

    def test_interrupt_during_process_mapping_terminates_child(self) -> None:
        with patch.object(
            self.runtime_authority,
            "verify_process",
            side_effect=KeyboardInterrupt,
        ):
            summary = self._execute(
                ["healthy-ok"], mode="timeout", wall_timeout_s=10
            )
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTERRUPTED")
        pending = Path(result["path"])
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["status"], "INTERRUPTED")
        self.assertEqual(attempt["runtime_execution_status"], "INTERRUPTED")
        self.assertEqual(
            attempt["runtime_process_mapping_verification"]["status"],
            "INTERRUPTED",
        )

    def test_zero_exit_without_lifecycle_is_not_published(self) -> None:
        summary = self._execute(["fault-ok"], mode="no_lifecycle")
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertFalse((self.out_root / "runs" / "fault-ok").exists())
        attempt = json.loads(
            (Path(result["path"]) / "attempt_manifest.json").read_text()
        )
        self.assertEqual(attempt["status"], "INTEGRITY_FAILED")

    def test_hash_tampering_fails_before_simulator_invocation(self) -> None:
        manifest = json.loads(self.manifest_path.read_text())
        cases = [
            Path(manifest["input_artifacts"]["workload"]["path"]),
            self.corpus_root / manifest["runs"][0]["schedule"]["path"],
        ]
        for index, path in enumerate(cases):
            with self.subTest(path=path):
                original = path.read_bytes()
                path.write_bytes(original + b"tamper\n")
                with self.assertRaises(RunnerError):
                    self._execute(["fault-ok"], dry_run=True)
                self.assertFalse(self.counter.exists())
                path.write_bytes(original)
                self.assertEqual(index + 1, index + 1)

    def test_a1_override_is_the_effective_argv_and_reuse_is_fail_closed(
        self,
    ) -> None:
        generated = self.root / "generated-a1-corpus"
        corpus_generator.generate_corpus(
            link_map_path=(
                TRUE16_ROOT / "healthy" / "link_map.csv"
            ),
            contract_path=LIMER / "configs" / "experiment_contract.yaml",
            topology_path=(
                TRUE16_ROOT
                / "topology"
                / "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
            ),
            workload_path=(
                LIMER
                / "configs"
                / "microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
            ),
            simulator_config_path=LIMER / "configs" / "SimAI.baseline.conf",
            out_dir=generated,
            seed=20260827,
        )
        self.corpus_root = generated
        self.manifest_path = generated / "corpus_manifest.json"
        prepared = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        run = next(
            item for item in prepared["runs"]
            if item.get("scenario") == "allreduce_burst"
        )
        run_id = str(run["run_id"])
        self.assertEqual(
            run["simulator_stability"],
            {
                "gate_required": True,
                "status": "PENDING_EXECUTION",
                "reason": (
                    "must be established from completed simulator artifacts"
                ),
            },
        )
        self.route_template = self._write_route_template()

        result = self._execute([run_id])
        self.assertEqual(result["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / run_id
        manifest = json.loads(
            (final / "run_manifest.json").read_text(encoding="utf-8")
        )
        observed = json.loads(
            (final / "fake_observed.json").read_text(encoding="utf-8")
        )
        argv = observed["argv"]
        self.assertEqual(
            argv[argv.index("-w") + 1],
            "../inputs/collective_workload_override.txt",
        )
        collective = run["schedule"]["collective_workload_override"]
        role_ref = collective["layer_role_sidecar"]
        self.assertEqual(
            manifest["effective_workload_sha256"], collective["sha256"]
        )
        self.assertEqual(
            manifest["effective_workload_binding"],
            {
                "source_workload_sha256": prepared["input_artifacts"]
                ["workload"]["sha256"],
                "effective_workload_sha256": collective["sha256"],
                "execution_copy": "inputs/collective_workload_override.txt",
                "collective_override_sha256": collective["sha256"],
                "collective_layer_role_sha256": role_ref["sha256"],
                "passed_to_simulator": True,
            },
        )
        self.assertEqual(
            sha256_file(final / "inputs/collective_layer_roles.csv"),
            role_ref["sha256"],
        )
        semantic = json.loads(
            (final / "semantic_validation.json").read_text(encoding="utf-8")
        )
        self.assertEqual(semantic["status"], "PASS")
        self.assertFalse(semantic["injected_physical_fault"])
        stability_path = final / "simulator_stability.json"
        stability_bytes = stability_path.read_bytes()
        stability = json.loads(stability_bytes)
        self.assertEqual(manifest["simulator_stability_status"], "PASS")
        self.assertEqual(
            manifest["simulator_stability_sha256"], sha256_file(stability_path)
        )
        self.assertEqual(stability["status"], "PASS")
        self.assertEqual(stability["planned_run_sha256"], canonical_hash(run))

        stability_path.unlink()
        missing_stability = self._execute([run_id])
        self.assertEqual(
            missing_stability["results"][0]["status"], "INTEGRITY_FAILED"
        )
        self.assertEqual(self.counter.read_text(), "1")
        stability_path.write_bytes(stability_bytes)

        effective_copy = final / "inputs/collective_workload_override.txt"
        effective_copy.write_bytes(effective_copy.read_bytes() + b"tamper\n")
        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["artifacts"]:
            if entry["path"] == "inputs/collective_workload_override.txt":
                entry["sha256"] = sha256_file(effective_copy)
                entry["size_bytes"] = effective_copy.stat().st_size
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )
        refused = self._execute([run_id])
        self.assertEqual(
            refused["results"][0]["status"], "INTEGRITY_FAILED"
        )
        self.assertIn("effective workload", refused["results"][0]["error"])

    def test_legacy_identity_downgrade_fails_before_invocation(self) -> None:
        manifest = json.loads(self.manifest_path.read_text())
        manifest.pop("identity_schema")
        inputs = manifest["input_artifacts"]
        legacy = {
            "contract_sha256": inputs["contract"]["sha256"],
            "topology_sha256": inputs["topology"]["sha256"],
            "workload_sha256": inputs["workload"]["sha256"],
            "seed": manifest["generation_seed"],
            "holdouts": manifest["split_manifest"]["paired_holdout_gpu_ids"],
            "runs": [run_identity_entry(run) for run in manifest["runs"]],
        }
        manifest["corpus_id"] = "p2-" + canonical_hash(legacy)[:24]
        self.manifest_path.write_text(json.dumps(manifest) + "\n")
        with self.assertRaisesRegex(RunnerError, "identity_schema"):
            self._execute(["fault-ok"], dry_run=True)
        self.assertFalse(self.counter.exists())

    def test_true16_topology_and_port_plane_binding_fail_closed(self) -> None:
        manifest = json.loads(self.manifest_path.read_text())
        topology = Path(manifest["input_artifacts"]["topology"]["path"])
        link_map = Path(manifest["input_artifacts"]["link_map"]["path"])
        result = _validate_topology(topology, 16)
        _validate_link_map_against_topology(link_map, topology, result)

        invalid_topology = self.root / "invalid_topology"
        lines = topology.read_text(encoding="utf-8").splitlines()
        fields = lines[0].split()
        fields[4] = "303"
        invalid_topology.write_text(
            " ".join(fields) + "\n" + "\n".join(lines[1:]) + "\n",
            encoding="utf-8",
        )
        with self.assertRaises(RunnerError):
            _validate_topology(invalid_topology, 16)

        invalid_map = self.root / "invalid_link_map.csv"
        invalid_map.write_text(
            link_map.read_text(encoding="utf-8").replace(
                "L0-24,0,24,HOST,SWITCH,3,1,ACCESS",
                "L0-24,0,24,HOST,SWITCH,2,1,ACCESS",
                1,
            ),
            encoding="utf-8",
        )
        with self.assertRaises(RunnerError):
            _validate_link_map_against_topology(
                invalid_map, topology, result
            )

    def test_resume_reuses_valid_evidence_and_rejects_corruption(self) -> None:
        first = self._execute(["fault-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        self.assertEqual(self.counter.read_text(), "1")
        second = self._execute(["fault-ok"])
        self.assertEqual(second["results"][0]["status"], "REUSED")
        self.assertEqual(self.counter.read_text(), "1")

        artifact = self.out_root / "runs" / "fault-ok" / "switch_telemetry.csv"
        artifact.write_text("corrupt\n", encoding="utf-8")
        third = self._execute(["fault-ok"])
        self.assertEqual(third["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertEqual(self.counter.read_text(), "1")
        self.assertEqual(artifact.read_text(), "corrupt\n")

    def test_gated_loss_publishes_recomputable_stability_pass(self) -> None:
        prepared = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        planned = next(
            run for run in prepared["runs"] if run["run_id"] == "random-loss-ok"
        )
        self.assertEqual(
            planned["simulator_stability"]["status"], "PENDING_EXECUTION"
        )
        summary = self._execute(["random-loss-ok"], workload_complete="0")
        self.assertEqual(summary["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "random-loss-ok"
        manifest = json.loads((final / "run_manifest.json").read_text())
        report_path = final / "simulator_stability.json"
        report = json.loads(report_path.read_text())
        self.assertTrue(manifest["simulator_stability_gate_required"])
        self.assertEqual(manifest["simulator_stability_status"], "PASS")
        self.assertEqual(
            manifest["simulator_stability_sha256"], sha256_file(report_path)
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["planned_run_sha256"], canonical_hash(planned))
        self.assertFalse(report["evidence_basis"]["workload_completed"])
        self.assertEqual(
            report["evidence_basis_sha256"],
            canonical_hash(report["evidence_basis"]),
        )
        self.assertEqual(
            self._execute(["random-loss-ok"])["results"][0]["status"],
            "REUSED",
        )

    def test_gated_loss_rejects_resealed_forged_stability_pass(self) -> None:
        first = self._execute(["random-loss-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "random-loss-ok"
        report_path = final / "simulator_stability.json"
        report = json.loads(report_path.read_text())
        report["evidence_basis"]["exit_code"] = 7
        report["evidence_basis_sha256"] = canonical_hash(
            report["evidence_basis"]
        )
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        report_sha = sha256_file(report_path)
        manifest["simulator_stability_sha256"] = report_sha
        for artifact in manifest["artifacts"]:
            if artifact["path"] == "simulator_stability.json":
                artifact["sha256"] = report_sha
                artifact["size_bytes"] = report_path.stat().st_size
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )
        second = self._execute(["random-loss-ok"])
        self.assertEqual(second["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn("raw-evidence recomputation", second["results"][0]["error"])
        self.assertEqual(self.counter.read_text(), "1")

    def test_legacy_gated_run_without_stability_evidence_is_not_reused(self) -> None:
        first = self._execute(["random-loss-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "random-loss-ok"
        (final / "simulator_stability.json").unlink()
        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        for field in (
            "simulator_stability_gate_required",
            "simulator_stability_status",
            "simulator_stability_sha256",
        ):
            manifest.pop(field)
        manifest["artifacts"] = _artifact_entries(final)
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )
        second = self._execute(["random-loss-ok"])
        self.assertEqual(second["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn("stability gate", second["results"][0]["error"])
        self.assertEqual(self.counter.read_text(), "1")

    def test_gated_loss_validation_failure_never_publishes_pass(self) -> None:
        summary = self._execute(
            ["random-loss-ok"], semantic_validation_status="FAIL"
        )
        self.assertEqual(summary["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertFalse(
            (self.out_root / "runs" / "random-loss-ok").exists()
        )
        attempts = list((self.out_root / ".pending").glob("random-loss-ok.*"))
        self.assertEqual(len(attempts), 1)
        self.assertFalse((attempts[0] / "simulator_stability.json").exists())

    def test_reuse_recomputes_semantics_instead_of_trusting_resealed_pass(self) -> None:
        first = self._execute(["fault-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "fault-ok"
        semantic_path = final / "semantic_validation.json"
        semantic = json.loads(semantic_path.read_text())
        semantic["evidence"]["forged_but_still_pass"] = True
        semantic_path.write_text(
            json.dumps(semantic, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        semantic_sha = sha256_file(semantic_path)
        manifest["semantic_validation_sha256"] = semantic_sha
        for artifact in manifest["artifacts"]:
            if artifact["path"] == "semantic_validation.json":
                artifact["sha256"] = semantic_sha
                artifact["size_bytes"] = semantic_path.stat().st_size
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )

        second = self._execute(["fault-ok"])
        self.assertEqual(second["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn("raw-evidence recomputation", second["results"][0]["error"])
        self.assertEqual(self.counter.read_text(), "1")

    def test_reuse_recomputes_route_report_after_forged_reseal(self) -> None:
        first = self._execute(["fault-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "fault-ok"
        report_path = final / "ecmp_route_candidate_validation.json"
        report = json.loads(report_path.read_text())
        report["evidence"]["forged_but_still_pass"] = True
        report_material = dict(report)
        report_material.pop("report_sha256")
        report["report_sha256"] = canonical_hash(report_material)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        report_sha = sha256_file(report_path)
        manifest["ecmp_route_candidate_validation_sha256"] = report_sha
        manifest["ecmp_route_candidate_report_sha256"] = report["report_sha256"]
        for artifact in manifest["artifacts"]:
            if artifact["path"] == "ecmp_route_candidate_validation.json":
                artifact["sha256"] = report_sha
                artifact["size_bytes"] = report_path.stat().st_size
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )

        second = self._execute(["fault-ok"])
        self.assertEqual(second["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn(
            "independent raw-evidence recomputation",
            second["results"][0]["error"],
        )
        self.assertEqual(self.counter.read_text(), "1")

    def test_reuse_recomputes_allocator_report_after_forged_reseal(self) -> None:
        first = self._execute(["fault-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "fault-ok"
        report_path = final / "training_source_port_allocator_validation.json"
        report = json.loads(report_path.read_text())
        report["metrics"]["allocations"] = 2
        report_material = dict(report)
        report_material.pop("report_sha256")
        report["report_sha256"] = canonical_hash(report_material)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = final / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        report_sha = sha256_file(report_path)
        manifest["training_source_port_allocator_validation_sha256"] = report_sha
        manifest["training_source_port_allocator_report_sha256"] = report[
            "report_sha256"
        ]
        for artifact in manifest["artifacts"]:
            if artifact["path"] == report_path.name:
                artifact["sha256"] = report_sha
                artifact["size_bytes"] = report_path.stat().st_size
        manifest["artifact_set_sha256"] = canonical_hash(manifest["artifacts"])
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (final / "run_manifest.sha256").write_text(
            f"{sha256_file(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )
        second = self._execute(["fault-ok"])
        self.assertEqual(second["results"][0]["status"], "INTEGRITY_FAILED")
        self.assertIn(
            "independent raw-evidence recomputation",
            second["results"][0]["error"],
        )
        self.assertEqual(self.counter.read_text(), "1")

    def test_shared_runtime_bundle_tampering_prevents_reuse(self) -> None:
        first = self._execute(["healthy-ok"])
        self.assertEqual(first["results"][0]["status"], "EXECUTED")
        archived = self.out_root / (
            first["runtime_closure"]["bundle_executable_path"]
        )
        archived.chmod(0o755)
        archived.write_bytes(archived.read_bytes() + b"tamper\n")
        with self.assertRaisesRegex(RunnerError, "sealed runtime executable"):
            self._execute(["healthy-ok"])
        self.assertEqual(self.counter.read_text(), "1")

    def test_blocked_and_congestion_runs_are_never_faked(self) -> None:
        summary = self._execute(["fault-blocked", "congestion-blocked"])
        self.assertEqual(
            [result["status"] for result in summary["results"]],
            [
                "BLOCKED_UNAVAILABLE_MECHANISM",
                "BLOCKED_MISSING_CONGESTION_EXECUTOR",
            ],
        )
        self.assertEqual(summary["counts"]["blocked"], 2)
        self.assertFalse(self.counter.exists())
        self.assertFalse((self.out_root / "runs").exists())

    def test_background_congestion_requires_ack_complete_sidecar(self) -> None:
        summary = self._execute(["congestion-ok"])
        self.assertEqual(summary["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "congestion-ok"
        manifest = json.loads((final / "run_manifest.json").read_text())
        evidence = manifest["background_flow_evidence"]
        self.assertEqual(evidence["status"], "COMPLETE")
        self.assertTrue(evidence["ack_qualified"])
        self.assertEqual(evidence["declared_flow_count"], 12)
        self.assertEqual(evidence["completed_flow_count"], 12)
        self.assertEqual(evidence["censored_flow_count"], 0)
        self.assertEqual(evidence["semantic_validation_status"], "PASS")
        semantic = json.loads((final / "semantic_validation.json").read_text())
        self.assertEqual(semantic["status"], "PASS")
        self.assertIn("queue_pressure", semantic["observed_effects"])
        self.assertIn("single_target_access_link", semantic["observed_effects"])
        self.assertIn("zero_background_rto_retries", semantic["observed_effects"])
        self.assertIn("realized_wave_profile", semantic["observed_effects"])
        self.assertEqual(semantic["evidence"]["destination_rank"], 0)
        self.assertEqual(semantic["target_link_ids"], ["L0-20"])
        self.assertEqual(
            manifest["background_transport_contract"]["rdma_rto_us"], 250000
        )
        self.assertEqual(
            manifest["background_transport_contract"]["rdma_retry_limit"], 0
        )
        self.assertEqual(
            manifest["background_flow_schedule_sha256"],
            manifest["schedule_bindings"]["background_flow"]["sha256"],
        )
        observed = json.loads((final / "fake_observed.json").read_text())
        self.assertIn(
            "background_flow_schedule.csv", observed["background_flow_schedule"]
        )
        self.assertEqual(observed["rdma_rto_us"], "250000")
        self.assertEqual(observed["rdma_retry_limit"], "0")

    def test_background_runtime_contract_violations_are_not_published(self) -> None:
        expected_failed_check = {
            "paired_port": "background_qp_single_rail_transport_config",
            "retry_then_success": "zero_background_retry_or_failover_events",
            "deadline_late": "ack_completion_before_deadline",
            "paired_pressure": "target_access_queue_pressure",
        }
        for mode, check_name in expected_failed_check.items():
            with self.subTest(mode=mode):
                summary = self._execute(["congestion-ok"], mode=mode)
                result = summary["results"][0]
                self.assertEqual(result["status"], "INTEGRITY_FAILED")
                self.assertFalse(
                    (self.out_root / "runs" / "congestion-ok").exists()
                )
                semantic = json.loads(
                    (Path(result["path"]) / "semantic_validation.json").read_text()
                )
                failed = {
                    item["name"] for item in semantic["checks"]
                    if item["status"] == "FAIL"
                }
                self.assertIn(check_name, failed)

    def test_censored_background_flow_is_not_published(self) -> None:
        summary = self._execute(["congestion-ok"], mode="background_censored")
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertFalse((self.out_root / "runs" / "congestion-ok").exists())
        pending = Path(result["path"])
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["status"], "INTEGRITY_FAILED")
        self.assertIn("SCHEDULED/START/COMPLETE", attempt["reason"])

    def test_background_rdma_schema_requires_exact_26_columns(self) -> None:
        summary = self._execute(["congestion-ok"], mode="rdma_extra_column")
        result = summary["results"][0]
        self.assertEqual(result["status"], "INTEGRITY_FAILED")
        self.assertIn("exact 26-column schema mismatch", result["error"])
        self.assertFalse((self.out_root / "runs" / "congestion-ok").exists())

    def test_ambient_simulator_controls_are_cleared(self) -> None:
        with patch.dict(
            os.environ,
            {
                "LIMER_BACKGROUND_FLOW_SCHEDULE": "/tmp/ambient-background.csv",
                "LIMER_FAULT_SCHEDULE": "/tmp/ambient-fault.csv",
                "LIMER_RDMA_RTO_US": "999999",
                "LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE": "1",
                "AS_PXN_ENABLE": "1",
                "AS_LOG_LEVEL": "0",
                "LD_LIBRARY_PATH": "/tmp/ambient-libraries",
                "LD_PRELOAD": "/tmp/ambient-preload.so",
            },
        ):
            summary = self._execute(["healthy-ok"])
        self.assertEqual(summary["results"][0]["status"], "EXECUTED")
        final = self.out_root / "runs" / "healthy-ok"
        observed = json.loads((final / "fake_observed.json").read_text())
        self.assertIsNone(observed["background_flow_schedule"])
        self.assertIsNone(observed["fault_schedule"])
        self.assertIsNone(observed["rdma_rto_us"])
        self.assertEqual(observed["rdma_recovery_transport_enable"], "0")
        self.assertEqual(observed["as_pxn_enable"], "0")
        self.assertEqual(observed["as_log_level"], "1")
        self.assertEqual(
            observed["ld_library_path"],
            str(self.out_root / "runtime-bundles" /
                summary["runtime_closure"]["identity_sha256"] / "lib"),
        )
        self.assertIsNone(observed["ld_preload"])
        invocation = json.loads((final / "invocation.json").read_text())
        self.assertIn(
            "LIMER_BACKGROUND_FLOW_SCHEDULE",
            invocation["cleared_inherited_control_variables"],
        )
        self.assertIn(
            "LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE",
            invocation["cleared_inherited_control_variables"],
        )
        self.assertIn(
            "AS_PXN_ENABLE", invocation["cleared_inherited_control_variables"]
        )
        self.assertIn(
            "AS_LOG_LEVEL", invocation["cleared_inherited_control_variables"]
        )
        self.assertIn(
            "LD_LIBRARY_PATH",
            invocation["loader_environment"][
                "cleared_inherited_loader_variables"
            ],
        )
        self.assertIn(
            "LD_PRELOAD",
            invocation["loader_environment"][
                "cleared_inherited_loader_variables"
            ],
        )

    def test_censored_lifecycle_is_preserved_pending_and_not_reusable(self) -> None:
        summary = self._execute(["healthy-ok"], mode="censored")
        result = summary["results"][0]
        self.assertEqual(result["status"], "CENSORED")
        self.assertEqual(summary["counts"]["censored"], 1)
        self.assertFalse((self.out_root / "runs" / "healthy-ok").exists())
        pending = Path(result["path"])
        attempt = json.loads((pending / "attempt_manifest.json").read_text())
        self.assertEqual(attempt["status"], "CENSORED")
        observed = json.loads(
            (pending / "fake_observed.json").read_text()
        )
        self.assertIsNone(observed["fault_schedule"])


if __name__ == "__main__":
    unittest.main()
