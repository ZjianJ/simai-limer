#!/usr/bin/env python3
"""Execute a hash-locked subset of the prepared true-16 P2 corpus.

The runner is deliberately narrower than the corpus planner and evaluator:
it never invents an injector for an unavailable mechanism, never turns a
congestion truth schedule into a fault schedule, and never replaces an
existing final evidence directory.  Each simulator attempt is assembled in a
pending directory and is published with one atomic directory rename only
after the process exits successfully and an auditable run manifest is sealed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import validate_true16_dualrail
import generate_true16_p2_corpus as corpus_generator
import generate_true16_traffic_workload as traffic_workload
import validate_p2_mechanism_run as mechanism_runtime
import validate_p2_workload_runtime as workload_runtime
import validate_ecmp_route_candidates as route_candidate_runtime
import validate_training_source_port_allocator as training_source_port_runtime
import simulator_runtime_bundle as runtime_bundle


CORPUS_SCHEMA = "limer.p2-corpus-manifest.v1"
RUN_MANIFEST_SCHEMA = "limer.p2-run-manifest.v1"
EXECUTION_SUMMARY_SCHEMA = "limer.p2-execution-summary.v1"
WORKLOAD_RUNTIME_QUALIFICATION_FILE = "workload_runtime_qualification.json"
SEMANTIC_VALIDATION_FILE = "semantic_validation.json"
RUNTIME_EXECUTION_EVIDENCE_FILE = "runtime_execution_evidence.json"
ECMP_ROUTE_CANDIDATES_FILE = "ecmp_route_candidates.csv"
ECMP_ROUTE_VALIDATION_FILE = "ecmp_route_candidate_validation.json"
SIMULATOR_STABILITY_FILE = "simulator_stability.json"
SIMULATOR_STABILITY_SCHEMA = "limer.p2-simulator-stability.v1"
TRAINING_SOURCE_PORT_RAW_FILE = training_source_port_runtime.RAW_FILENAME
TRAINING_SOURCE_PORT_VALIDATION_FILE = training_source_port_runtime.REPORT_FILENAME
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUNTIME_CONFIG_PATHS = {
    "FLOW_FILE": "empty_flow.txt",
    "TRACE_FILE": "empty_trace.txt",
    "TRACE_OUTPUT_FILE": "trace_output.tr",
    "FCT_OUTPUT_FILE": "fct_output.txt",
    "PFC_OUTPUT_FILE": "pfc_output.txt",
    "QLEN_MON_FILE": "qlen_output.txt",
    "BW_MON_FILE": "bandwidth_output.txt",
    "RATE_MON_FILE": "rate_output.txt",
    "CNP_MON_FILE": "cnp_output.txt",
}
REQUIRED_RUNTIME_CONFIG_PATHS = {
    "FLOW_FILE", "TRACE_FILE", "TRACE_OUTPUT_FILE", "FCT_OUTPUT_FILE",
    "PFC_OUTPUT_FILE",
}
BACKGROUND_FLOW_CSV_COLUMNS = (
    "event_id", "flow_id", "scenario", "scheduled_start_ns", "src_rank",
    "dst_rank", "bytes", "pg", "sport", "dport",
)
BACKGROUND_APPLICATION_CSV_COLUMNS = (
    "run_id", "event_id", "flow_id", "scenario", "event",
    "scheduled_start_ns", "actual_ns", "first_tx_ns", "first_ack_ns",
    "src_rank", "dst_rank", "bytes", "pg", "sport", "dport", "status",
)
RDMA_TELEMETRY_CSV_COLUMNS = (
    "run_id", "timestamp_ns", "node_id", "rank_id", "logical_qp_id",
    "transport_epoch", "traffic_class", "event", "event_detail",
    "wc_status", "src_rank", "dst_rank", "sport", "primary_nic",
    "backup_nic", "active_nic", "backup_ready_ns", "failover_ns",
    "backup_first_tx_ns", "backup_first_ack_ns", "standby_tx_bytes",
    "snd_una", "snd_nxt", "retry_count", "retry_limit", "rto_us",
)
EXECUTABLE_BACKGROUND_SCENARIOS = frozenset({"incast", "queue_buildup"})
MURMUR3_SEED_U32 = 0x8BADF00D
UINT32_MASK = 0xFFFFFFFF
HASH_BYTE_ORDER = "little"

SIMULATOR_STABILITY_SOURCE_FILES: Mapping[str, str] = {
    "exit_code": "exit_code.txt",
    "run_lifecycle": "run_lifecycle.csv",
    "switch_telemetry": "switch_telemetry.csv",
    "nic_telemetry": "nic_telemetry.csv",
    "collective_telemetry": "collective_telemetry.csv",
    "collective_transaction": "collective_transaction.csv",
    "fault_application_telemetry": "fault_application_telemetry.csv",
    "runtime_execution_evidence": RUNTIME_EXECUTION_EVIDENCE_FILE,
    "semantic_validation": SEMANTIC_VALIDATION_FILE,
    "workload_runtime_qualification": WORKLOAD_RUNTIME_QUALIFICATION_FILE,
    "ecmp_route_candidates": ECMP_ROUTE_CANDIDATES_FILE,
    "ecmp_route_candidate_validation": ECMP_ROUTE_VALIDATION_FILE,
    "training_source_port_allocator": TRAINING_SOURCE_PORT_RAW_FILE,
    "training_source_port_allocator_validation": (
        TRAINING_SOURCE_PORT_VALIDATION_FILE
    ),
}
SIMULATOR_STABILITY_CHECK_NAMES = (
    "planned_gate_is_pending_execution",
    "simulator_process_exited_zero",
    "observation_window_complete",
    "no_timeout_interrupt_or_oom",
    "sealed_runtime_execution_validated",
    "mechanism_semantics_and_telemetry_validated",
    "workload_runtime_validated",
    "ecmp_route_candidates_validated",
    "training_source_port_allocator_validated",
)


class RunnerError(RuntimeError):
    """An integrity or execution precondition failed."""


@dataclass(frozen=True)
class SimulatorRuntimeBinding:
    """One sealed simulator closure selected for an execution root.

    ``bundle`` and ``source_closure`` are deliberately typed as optional so
    runner tests can supply an explicit authority object for a non-ELF fixture.
    The production authority below never returns ``None`` for either field,
    and the command-line interface has no switch that enables the test seam.
    """

    execution_root: Path
    bundle_root: Path
    executable: Path
    identity_sha256: str
    manifest_path: Path
    manifest_sha256: str
    record: Mapping[str, Any]
    loader_preflight: Mapping[str, Any]
    bundle: Optional[runtime_bundle.RuntimeBundle]
    source_closure: Optional[runtime_bundle.RuntimeClosure]


def _closure_loader_evidence(
    closure: runtime_bundle.RuntimeClosure,
) -> Dict[str, Any]:
    return {
        "status": "PASS",
        "runtime_bundle_identity_sha256": closure.identity_sha256,
        "executable_path": str(closure.executable),
        "executable_sha256": closure.executable_sha256,
        "project_dependency_count": len(closure.project_dependencies),
        "project_dependencies": [
            {
                "soname": item.soname,
                "path": str(item.resolved_path),
                "sha256": item.sha256,
                "size_bytes": item.size_bytes,
            }
            for item in closure.project_dependencies
        ],
        "system_dependency_count": len(closure.system_dependencies),
        "system_dependencies": [
            {
                "soname": item.soname,
                "path": str(item.resolved_path),
                "sha256": item.sha256,
                "size_bytes": item.size_bytes,
            }
            for item in closure.system_dependencies
        ],
        "virtual_dependencies": list(closure.virtual_dependencies),
    }


def _runtime_record(
    execution_root: Path,
    bundle: runtime_bundle.RuntimeBundle,
) -> Dict[str, Any]:
    root = execution_root.resolve()
    try:
        bundle_relative = bundle.root.relative_to(root).as_posix()
        executable_relative = bundle.executable.relative_to(root).as_posix()
        manifest_relative = (
            bundle.root / runtime_bundle.MANIFEST_NAME
        ).relative_to(root).as_posix()
        seal_relative = (
            bundle.root / runtime_bundle.SEAL_NAME
        ).relative_to(root).as_posix()
    except ValueError as exc:
        raise RunnerError(
            f"sealed runtime bundle escaped execution root: {bundle.root}"
        ) from exc
    manifest_path = bundle.root / runtime_bundle.MANIFEST_NAME
    executable_ref = bundle.manifest.get("executable", {})
    projects = bundle.manifest.get("project_dependencies", [])
    systems = bundle.manifest.get("system_dependencies", [])
    return {
        "schema_version": runtime_bundle.BUNDLE_SCHEMA,
        "identity_sha256": bundle.identity_sha256,
        "execution_root": str(root),
        "bundle_path": bundle_relative,
        "bundle_manifest_path": manifest_relative,
        "bundle_manifest_sha256": sha256_file(manifest_path),
        "bundle_manifest_seal_path": seal_relative,
        "bundle_artifact_set_sha256": bundle.manifest.get(
            "artifact_set_sha256"
        ),
        "bundle_executable_path": executable_relative,
        "bundle_executable_sha256": executable_ref.get("sha256"),
        "bundle_executable_size_bytes": executable_ref.get("size_bytes"),
        "source_executable_path": executable_ref.get("source_path"),
        "project_dependency_count": len(projects),
        "system_dependency_count": len(systems),
        "loader_isolation": dict(bundle.manifest.get("loader_contract", {})),
    }


class SealedRuntimeAuthority:
    """Production authority for sealing and verifying the simulator runtime."""

    @staticmethod
    def _runner_error(action: str, error: Exception) -> RunnerError:
        return RunnerError(f"simulator runtime {action} failed: {error}")

    def prepare(
        self, execution_root: Path, source_binary: Path
    ) -> SimulatorRuntimeBinding:
        try:
            bundle = runtime_bundle.seal_runtime_bundle(
                execution_root, source_binary
            )
            source_closure = runtime_bundle.discover_runtime_closure(
                source_binary
            )
            if source_closure.identity_sha256 != bundle.identity_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "source closure changed while the runtime bundle was sealed"
                )
            runtime_bundle.verify_discovered_sources(source_closure)
            bundle = runtime_bundle.validate_runtime_bundle(
                bundle.root, expected_closure=source_closure,
                reused=bundle.reused,
            )
            loader = runtime_bundle.verify_loader_resolution(bundle)
            record = _runtime_record(execution_root, bundle)
            return SimulatorRuntimeBinding(
                execution_root=execution_root.resolve(),
                bundle_root=bundle.root,
                executable=bundle.executable,
                identity_sha256=bundle.identity_sha256,
                manifest_path=bundle.root / runtime_bundle.MANIFEST_NAME,
                manifest_sha256=record["bundle_manifest_sha256"],
                record=record,
                loader_preflight={
                    **_closure_loader_evidence(loader),
                    "checked_at": utc_now(),
                    "phase": "execution_root_prepare",
                },
                bundle=bundle,
                source_closure=source_closure,
            )
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("preparation", exc) from exc

    def load_for_reuse(
        self,
        execution_root: Path,
        recorded: Mapping[str, Any],
    ) -> SimulatorRuntimeBinding:
        root = execution_root.resolve()
        raw_bundle_path = recorded.get("bundle_path")
        if not isinstance(raw_bundle_path, str) or not raw_bundle_path:
            raise RunnerError("run manifest runtime closure lacks bundle_path")
        relative = Path(raw_bundle_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise RunnerError(
                f"run manifest has unsafe runtime bundle path: {raw_bundle_path!r}"
            )
        requested = root / relative
        try:
            requested.relative_to(root)
        except ValueError as exc:
            raise RunnerError(
                f"run manifest runtime bundle escapes execution root: {requested}"
            ) from exc
        try:
            bundle = runtime_bundle.validate_runtime_bundle(requested)
            loader = runtime_bundle.verify_loader_resolution(bundle)
            current = _runtime_record(root, bundle)
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("sealed reuse loading", exc) from exc
        if dict(recorded) != current:
            raise RunnerError(
                "run manifest runtime closure differs from the sealed bundle"
            )
        return SimulatorRuntimeBinding(
            execution_root=root,
            bundle_root=bundle.root,
            executable=bundle.executable,
            identity_sha256=bundle.identity_sha256,
            manifest_path=bundle.root / runtime_bundle.MANIFEST_NAME,
            manifest_sha256=current["bundle_manifest_sha256"],
            record=current,
            loader_preflight={
                **_closure_loader_evidence(loader),
                "checked_at": utc_now(),
                "phase": "load_for_reuse",
            },
            bundle=bundle,
            source_closure=None,
        )

    def verify_before_run(
        self, binding: SimulatorRuntimeBinding
    ) -> Mapping[str, Any]:
        if binding.bundle is None or binding.source_closure is None:
            raise RunnerError("production runtime binding is incomplete")
        try:
            runtime_bundle.verify_discovered_sources(binding.source_closure)
            validated = runtime_bundle.validate_runtime_bundle(
                binding.bundle_root,
                expected_closure=binding.source_closure,
            )
            if validated.identity_sha256 != binding.identity_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "pre-run bundle identity differs from selected closure"
                )
            if sha256_file(binding.manifest_path) != binding.manifest_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "runtime bundle manifest changed before run"
                )
            return {
                "status": "PASS",
                "phase": "before_run",
                "checked_at": utc_now(),
                "source_closure_status": "PASS",
                "bundle_integrity_status": "PASS",
                "loader_preflight_identity_sha256": binding.identity_sha256,
            }
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("pre-run verification", exc) from exc

    def execution_environment(
        self,
        binding: SimulatorRuntimeBinding,
        base_environment: Mapping[str, str],
        overrides: Mapping[str, str],
    ) -> Tuple[Dict[str, str], Mapping[str, Any]]:
        if binding.bundle is None:
            raise RunnerError("production runtime binding has no sealed bundle")
        try:
            return runtime_bundle.sealed_execution_environment(
                binding.bundle,
                base_environment=base_environment,
                overrides=overrides,
            )
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("environment construction", exc) from exc

    def verify_process(
        self, binding: SimulatorRuntimeBinding, pid: int
    ) -> Mapping[str, Any]:
        if binding.bundle is None:
            raise RunnerError("production runtime binding has no sealed bundle")
        started = time.monotonic()
        deadline = started + 2.0
        attempts = 0
        last_error: Optional[Exception] = None
        while True:
            attempts += 1
            try:
                evidence = runtime_bundle.verify_process_runtime(
                    pid, binding.bundle
                )
                return {
                    **dict(evidence),
                    "inspection_attempts": attempts,
                    "inspection_elapsed_ms": round(
                        (time.monotonic() - started) * 1000.0, 3
                    ),
                }
            except (runtime_bundle.RuntimeBundleError, OSError) as exc:
                last_error = exc
                if not Path(f"/proc/{pid}").is_dir() or time.monotonic() >= deadline:
                    break
                # Popen's exec-error pipe can close just before the dynamic
                # loader finishes mapping DT_NEEDED objects.  Retry only while
                # the exact child is live; a fast exit remains fail-closed.
                time.sleep(0.01)
        if last_error is None:  # pragma: no cover - loop always attempts once
            raise RunnerError("live process mapping verification did not run")
        raise self._runner_error(
            "live process mapping verification", last_error
        ) from last_error

    def verify_after_run(
        self, binding: SimulatorRuntimeBinding
    ) -> Mapping[str, Any]:
        if binding.bundle is None or binding.source_closure is None:
            raise RunnerError("production runtime binding is incomplete")
        try:
            runtime_bundle.verify_discovered_sources(binding.source_closure)
            runtime_bundle.validate_runtime_bundle(
                binding.bundle_root,
                expected_closure=binding.source_closure,
            )
            if sha256_file(binding.manifest_path) != binding.manifest_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "runtime bundle manifest changed after run"
                )
            return {
                "status": "PASS",
                "phase": "after_run",
                "checked_at": utc_now(),
                "source_closure_status": "PASS",
                "bundle_integrity_status": "PASS",
            }
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("post-run verification", exc) from exc

    def verify_for_reuse(
        self, binding: SimulatorRuntimeBinding
    ) -> Mapping[str, Any]:
        if binding.bundle is None:
            raise RunnerError("production runtime binding has no sealed bundle")
        try:
            # Deliberately do not touch source_closure here.  Reuse is proven
            # from the sealed content-addressed bundle and its loader graph.
            validated = runtime_bundle.validate_runtime_bundle(binding.bundle_root)
            if validated.identity_sha256 != binding.identity_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "reused bundle identity differs from selected runtime closure"
                )
            if sha256_file(binding.manifest_path) != binding.manifest_sha256:
                raise runtime_bundle.RuntimeBundleError(
                    "runtime bundle manifest changed before reuse"
                )
            return {
                "status": "PASS",
                "phase": "reuse",
                "checked_at": utc_now(),
                "bundle_integrity_status": "PASS",
                "loader_preflight_identity_sha256": binding.identity_sha256,
                "source_closure_reopened": False,
            }
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise self._runner_error("reuse verification", exc) from exc


def _rotl32(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (32 - shift))) & UINT32_MASK


def ns3_murmur3_x86_32(data: bytes, seed: int = MURMUR3_SEED_U32) -> int:
    if len(data) % 4:
        raise RunnerError("RDMA hash input must contain complete uint32 blocks")
    value = seed & UINT32_MASK
    for offset in range(0, len(data), 4):
        block = int.from_bytes(data[offset : offset + 4], "little")
        block = (block * 0xCC9E2D51) & UINT32_MASK
        block = _rotl32(block, 15)
        block = (block * 0x1B873593) & UINT32_MASK
        value ^= block
        value = _rotl32(value, 13)
        value = (value * 5 + 0xE6546B64) & UINT32_MASK
    value ^= len(data)
    value ^= value >> 16
    value = (value * 0x85EBCA6B) & UINT32_MASK
    value ^= value >> 13
    value = (value * 0xC2B2AE35) & UINT32_MASK
    value ^= value >> 16
    return value & UINT32_MASK


def rank_ipv4_u32(rank: int) -> int:
    if not 0 <= rank < 256:
        raise RunnerError(f"rank cannot be represented by P2 address rule: {rank}")
    return 0x0B000001 + (rank << 8)


def rdma_route_bucket(
    *, src: int, dst: int, sport: int, dport: int, reverse: bool = False
) -> int:
    sip, dip = rank_ipv4_u32(src), rank_ipv4_u32(dst)
    if reverse:
        sip, dip, sport, dport = dip, sip, dport, sport
    return ns3_murmur3_x86_32(
        struct.pack("<IIHH", sip, dip, sport, dport)
    ) % 2


def select_single_rail_sport(
    *, src: int, dst: int, dport: int, route_bucket: int, used: set[int]
) -> int:
    """Test/helper API matching the corpus generator's deterministic search."""
    for sport in range(49152, 65536):
        if sport in used:
            continue
        if (
            rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport
            ) == route_bucket
            and rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport, reverse=True
            ) == route_bucket
        ):
            used.add(sport)
            return sport
    raise RunnerError("reserved background sport interval cannot realize rail pin")


@dataclass(frozen=True)
class ValidatedCorpus:
    manifest_path: Path
    root: Path
    manifest: Mapping[str, Any]
    manifest_sha256: str
    simulator_binary: Path
    simulator_sha256: str
    input_paths: Mapping[str, Path]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def schedule_identity_tuple(run: Mapping[str, Any]) -> Tuple[Any, ...]:
    """Bind optional sidecars without changing tuples that lack them."""
    schedule = run["schedule"]
    material: List[Any] = [
        run["run_id"],
        schedule["sha256"],
        schedule.get("simulator_injection_schedule", {}).get("sha256"),
    ]
    background = schedule.get("background_flow_schedule")
    if isinstance(background, Mapping):
        material.append(background.get("sha256"))
    collective = schedule.get("collective_workload_override")
    if isinstance(collective, Mapping):
        material.append(collective.get("sha256"))
        material.append(
            collective.get("layer_role_sidecar", {}).get("sha256")
        )
    ecmp = schedule.get("ecmp_collision_schedule")
    if isinstance(ecmp, Mapping):
        material.append(ecmp.get("sha256"))
    return tuple(material)


def run_identity_entry(run: Mapping[str, Any]) -> Dict[str, Any]:
    schedule = run["schedule"]
    entry = {
        "run_id": run["run_id"],
        "partition": run["partition"],
        "split_group_id": run["split_group_id"],
        "schedule_sha256": schedule["sha256"],
        "simulator_injection_sha256": schedule.get(
            "simulator_injection_schedule", {}
        ).get("sha256"),
    }
    background = schedule.get("background_flow_schedule")
    if isinstance(background, Mapping):
        entry["background_flow_sha256"] = background.get("sha256")
    collective = schedule.get("collective_workload_override")
    if isinstance(collective, Mapping):
        entry["collective_workload_override_sha256"] = collective.get("sha256")
        entry["collective_layer_role_sha256"] = collective.get(
            "layer_role_sidecar", {}
        ).get("sha256")
    entry["effective_workload_sha256"] = run.get(
        "effective_workload_sha256", run.get("workload_sha256")
    )
    ecmp = schedule.get("ecmp_collision_schedule")
    if isinstance(ecmp, Mapping):
        entry["ecmp_collision_sha256"] = ecmp.get("sha256")
    return entry


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def require_file(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise RunnerError(f"{description} is missing or not a file: {resolved}")
    return resolved


def verify_expected_hash(path: Path, expected: Any, description: str) -> str:
    if not isinstance(expected, str) or not SHA256_RE.fullmatch(expected):
        raise RunnerError(f"{description} has an invalid expected sha256: {expected!r}")
    actual = sha256_file(path)
    if actual != expected:
        raise RunnerError(
            f"{description} sha256 mismatch: expected {expected}, got {actual}: {path}"
        )
    return actual


def resolve_sidecar(root: Path, raw_path: Any, description: str) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise RunnerError(f"{description} path is missing")
    relative = Path(raw_path)
    if relative.is_absolute():
        raise RunnerError(f"{description} must be relative to corpus root: {raw_path}")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RunnerError(f"{description} escapes corpus root: {raw_path}") from exc
    return require_file(resolved, description)


def _validate_topology(path: Path, expected_gpu_count: int) -> Mapping[str, Any]:
    try:
        topology = validate_true16_dualrail.read_topology(path)
        result = validate_true16_dualrail.validate(
            topology, expected_gpu_count, 4
        )
    except (OSError, ValueError) as exc:
        raise RunnerError(f"cannot parse true-16 topology: {path}: {exc}") from exc
    if result.get("status") != "PASS":
        failed = [
            item.get("name") for item in result.get("checks", [])
            if item.get("status") != "PASS"
        ]
        raise RunnerError(
            "P2 runner requires the complete true-16 dual-plane topology "
            f"contract; failed checks={failed}"
        )
    return result


def _validate_link_map_against_topology(
    path: Path, topology_path: Path, topology_result: Mapping[str, Any]
) -> None:
    """Bind host ifIndex 2/3 to isolated physical Plane A/B components."""
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {
                "link_id", "src_node", "dst_node", "src_type", "dst_type",
                "src_port", "dst_port", "link_class",
            }
            if not required.issubset(reader.fieldnames or []):
                raise RunnerError("frozen link_map schema is incomplete")
            rows = list(reader)
        topology = validate_true16_dualrail.read_topology(topology_path)
    except (OSError, csv.Error, ValueError) as exc:
        raise RunnerError(f"cannot parse frozen link_map/topology binding: {exc}") from exc
    if len(rows) != len(topology["links"]):
        raise RunnerError(
            "frozen link_map must enumerate every physical topology link: "
            f"map={len(rows)}, topology={len(topology['links'])}"
        )
    link_ids = [str(row.get("link_id", "")) for row in rows]
    if any(not value for value in link_ids) or len(link_ids) != len(set(link_ids)):
        raise RunnerError("frozen link_map has empty or duplicate link_id values")
    try:
        mapped_pairs = {
            tuple(sorted((int(row["src_node"]), int(row["dst_node"]))))
            for row in rows
        }
    except (TypeError, ValueError) as exc:
        raise RunnerError("frozen link_map has invalid endpoint node IDs") from exc
    topology_pairs = {
        tuple(sorted((int(link["src"]), int(link["dst"]))))
        for link in topology["links"]
    }
    if mapped_pairs != topology_pairs or len(mapped_pairs) != len(rows):
        raise RunnerError("frozen link_map is not a bijection over topology links")

    component_by_switch = {
        int(switch): int(plane["plane_id"])
        for plane in topology_result.get("planes", [])
        for switch in plane.get("switch_ids", [])
    }
    host_ports: Dict[int, Dict[int, int]] = {}
    access_count = 0
    for row in rows:
        if row.get("link_class") != "ACCESS":
            continue
        access_count += 1
        try:
            if row.get("src_type") == "HOST" and row.get("dst_type") == "SWITCH":
                host, port, peer = (
                    int(row["src_node"]), int(row["src_port"]), int(row["dst_node"])
                )
            elif row.get("dst_type") == "HOST" and row.get("src_type") == "SWITCH":
                host, port, peer = (
                    int(row["dst_node"]), int(row["dst_port"]), int(row["src_node"])
                )
            else:
                raise RunnerError("ACCESS link lacks one HOST/SWITCH endpoint")
        except (TypeError, ValueError) as exc:
            raise RunnerError("ACCESS link has invalid endpoint/port fields") from exc
        if host not in range(16) or port not in {2, 3}:
            raise RunnerError(
                f"ACCESS {row.get('link_id')} has unexpected host/port {host}/{port}"
            )
        expected_plane = {2: 0, 3: 1}[port]
        if component_by_switch.get(peer) != expected_plane:
            raise RunnerError(
                f"ACCESS {row.get('link_id')} maps host port {port} to "
                f"plane {component_by_switch.get(peer)}, expected {expected_plane}"
            )
        if port in host_ports.setdefault(host, {}):
            raise RunnerError(f"GPU {host} has duplicate ACCESS host port {port}")
        host_ports[host][port] = peer
    if access_count != 32 or set(host_ports) != set(range(16)) or any(
        set(ports) != {2, 3} for ports in host_ports.values()
    ):
        raise RunnerError(
            "frozen link_map must contain exactly one Plane-A/Plane-B ACCESS "
            "link on host ports 2/3 for every GPU"
        )


def _validate_corpus_identity(manifest: Mapping[str, Any]) -> None:
    runs = manifest["runs"]
    schedule_material = [schedule_identity_tuple(run) for run in runs]
    expected_schedule_set = canonical_hash(schedule_material)
    if manifest.get("schedule_set_sha256") != expected_schedule_set:
        raise RunnerError(
            "corpus schedule_set_sha256 does not bind the current run schedules"
        )

    inputs = manifest["input_artifacts"]
    split = manifest.get("split_manifest", {})
    identity_schema = manifest.get("identity_schema")
    if identity_schema != "limer.p2-corpus-identity.v2":
        raise RunnerError(
            "P2 execution requires identity_schema="
            f"'limer.p2-corpus-identity.v2', got {identity_schema!r}"
        )
    identity_material = {
        "identity_schema": identity_schema,
        "contract_sha256": inputs["contract"]["sha256"],
        "link_map_sha256": inputs["link_map"]["sha256"],
        "topology_sha256": inputs["topology"]["sha256"],
        "workload_sha256": inputs["workload"]["sha256"],
        "simulator_config_sha256": inputs["simulator_config"]["sha256"],
        "seed": manifest["generation_seed"],
        "holdouts": split.get("paired_holdout_gpu_ids"),
        "runs": [run_identity_entry(run) for run in runs],
    }
    expected_corpus_id = "p2-" + canonical_hash(identity_material)[:24]
    if manifest.get("corpus_id") != expected_corpus_id:
        raise RunnerError(
            f"corpus_id mismatch: expected {expected_corpus_id}, "
            f"got {manifest.get('corpus_id')!r}"
        )


def _validate_workload_qualification(
    manifest: Mapping[str, Any], workload_path: Path,
) -> None:
    qualification = manifest.get("workload_qualification")
    if not isinstance(qualification, Mapping):
        raise RunnerError("P2 corpus lacks workload_qualification evidence")
    try:
        report = traffic_workload.validate_workload(
            workload_path,
            profile=traffic_workload.HORIZON_PREFIX_PROFILE,
        )
    except traffic_workload.WorkloadValidationError as exc:
        raise RunnerError(
            f"P2 corpus workload is not horizon-prefix qualified: {exc}"
        ) from exc
    planned_maximum = max(
        int(run["virtual_finish_ns"]) for run in manifest["runs"]
    )
    expected = {
        "static_qualification_status": "PASS",
        "static_qualification_profile": traffic_workload.HORIZON_PREFIX_PROFILE,
        "static_report_sha256": canonical_hash(report),
        "planned_maximum_virtual_finish_ns": planned_maximum,
    }
    for field, value in expected.items():
        if qualification.get(field) != value:
            raise RunnerError(
                f"P2 workload qualification {field} mismatch: "
                f"expected {value!r}, got {qualification.get(field)!r}"
            )
    if qualification.get("static_report") != report:
        raise RunnerError("P2 inline static workload report differs from recomputation")
    covered_horizon = int(
        report["duration_estimate"]["corpus_max_virtual_finish_ns"]
    )
    if covered_horizon < planned_maximum:
        raise RunnerError(
            "P2 workload horizon does not cover every run: "
            f"workload={covered_horizon}, corpus={planned_maximum}"
        )


def _read_schedule_rows(path: Path, description: str) -> List[Dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                raise RunnerError(f"{description} has no CSV header")
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"cannot parse {description}: {path}: {exc}") from exc
    if not rows:
        raise RunnerError(f"{description} has no rows: {path}")
    return rows


def _read_exact_csv(
    path: Path, columns: Sequence[str], description: str
) -> List[Dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != tuple(columns):
                raise RunnerError(
                    f"{description} header mismatch: expected {list(columns)!r}, "
                    f"got {reader.fieldnames!r}"
                )
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"cannot parse {description}: {path}: {exc}") from exc
    if not rows:
        raise RunnerError(f"{description} has no rows: {path}")
    return rows


def _parse_uint(text: Any, field: str, description: str) -> int:
    if not isinstance(text, str) or not text or not text.isdecimal():
        raise RunnerError(f"{description} has invalid unsigned {field}: {text!r}")
    return int(text)


def _background_truth_contract(
    run: Mapping[str, Any], truth_rows: Sequence[Mapping[str, str]]
) -> Dict[str, Any]:
    run_id = str(run["run_id"])
    if len(truth_rows) != 1:
        raise RunnerError(f"{run_id}: background truth must contain exactly one row")
    raw = truth_rows[0].get("action_parameters_json")
    if not isinstance(raw, str) or not raw:
        raise RunnerError(f"{run_id}: background truth lacks action_parameters_json")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RunnerError(
            f"{run_id}: background action_parameters_json is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise RunnerError(f"{run_id}: background action parameters must be an object")
    required = {
        "destination_rank", "bottleneck_access_link_id",
        "paired_access_link_id", "data_plane",
        "route_candidate_order_host_ports", "route_bucket",
        "hash_algorithm", "hash_seed_u32", "hash_tuple", "hash_byte_order",
        "pin_reverse_ack", "predeclared_window_policy",
        "realized_window_policy", "completion_deadline_ns",
        "rdma_rto_us", "rdma_retry_limit", "max_rto_retry_events",
    }
    missing = sorted(required - set(value))
    if missing:
        raise RunnerError(f"{run_id}: background truth fields missing: {missing}")
    try:
        destination = int(value["destination_rank"])
        route_bucket = int(value["route_bucket"])
        host_ports = [int(item) for item in value["route_candidate_order_host_ports"]]
        completion_deadline = int(value["completion_deadline_ns"])
        rto_us = int(value["rdma_rto_us"])
        retry_limit = int(value["rdma_retry_limit"])
        max_retries = int(value["max_rto_retry_events"])
        truth_start = _parse_uint(
            truth_rows[0].get("start_time_ns"), "start_time_ns", run_id
        )
        truth_end = _parse_uint(
            truth_rows[0].get("end_time_ns"), "end_time_ns", run_id
        )
    except (TypeError, ValueError) as exc:
        raise RunnerError(f"{run_id}: invalid numeric background truth field") from exc
    expected = {
        "destination_rank": run.get("target_gpu"),
        "bottleneck_access_link_id": run.get("target_link_id"),
        "paired_access_link_id": run.get("paired_link_id"),
    }
    observed = {key: value.get(key) for key in expected}
    if observed != expected:
        raise RunnerError(
            f"{run_id}: background target fields disagree with manifest: "
            f"expected={expected}, observed={observed}"
        )
    if not (
        0 <= destination < 16
        and route_bucket in {0, 1}
        and host_ports == [2, 3]
        and value["data_plane"] in {"A", "B"}
        and {"A": 2, "B": 3}[value["data_plane"]]
        == host_ports[route_bucket]
        and value["hash_algorithm"] == "ns3-murmur3-x86-32"
        and int(value["hash_seed_u32"]) == MURMUR3_SEED_U32
        and value["hash_tuple"] == "native-le-sip-dip-sport-dport"
        and value["hash_byte_order"] == HASH_BYTE_ORDER
        and value["pin_reverse_ack"] is True
        and value["predeclared_window_policy"] == "scheduled_qp_launch_window"
        and value["realized_window_policy"]
        == "first_data_tx_to_last_ack_complete"
        and truth_start < truth_end <= completion_deadline
        and completion_deadline <= int(run["virtual_finish_ns"])
        and rto_us > 0
        and retry_limit == 0
        and max_retries == 0
    ):
        raise RunnerError(f"{run_id}: background rail/window/transport contract is invalid")
    if host_ports[route_bucket] not in {2, 3}:
        raise RunnerError(f"{run_id}: route bucket has no declared host port")
    return {
        **value,
        "destination_rank": destination,
        "route_bucket": route_bucket,
        "route_candidate_order_host_ports": host_ports,
        "completion_deadline_ns": completion_deadline,
        "rdma_rto_us": rto_us,
        "rdma_retry_limit": retry_limit,
        "max_rto_retry_events": max_retries,
        "truth_start_ns": truth_start,
        "truth_end_ns": truth_end,
    }


def _validate_schedule_binding(
    run: Mapping[str, Any],
    truth_path: Path,
    injection_path: Optional[Path],
    background_path: Optional[Path],
) -> None:
    """Bind executable decisions to fields inside the hashed CSV sidecars."""
    run_id = str(run["run_id"])
    schedule = run["schedule"]
    event_id = str(schedule.get("event_id", ""))
    status = str(schedule.get("implementation_status", ""))
    truth_rows = _read_schedule_rows(truth_path, f"{run_id} truth schedule")
    if any(row.get("event_id") != event_id for row in truth_rows):
        raise RunnerError(f"{run_id}: truth schedule event_id does not match manifest")
    if any(row.get("implementation_status") != status for row in truth_rows):
        raise RunnerError(
            f"{run_id}: truth schedule implementation status does not match manifest"
        )
    injector = schedule.get("simulator_injection_schedule")
    background = schedule.get("background_flow_schedule")
    if injection_path is not None and background_path is not None:
        raise RunnerError(
            f"{run_id}: physical-fault and background-flow injectors are mutually exclusive"
        )
    if injection_path is None:
        if injector is not None:
            raise RunnerError(f"{run_id}: injector path resolution is inconsistent")
    else:
        injection_rows = _read_schedule_rows(
            injection_path, f"{run_id} simulator injection schedule"
        )
        required = {
            "fault_id", "fault_type", "target_link_id", "start_time_ns", "end_time_ns",
            "parameter_before", "parameter_after", "parent_event_id",
            "implementation_status",
        }
        if not required.issubset(injection_rows[0]):
            missing = sorted(required - set(injection_rows[0]))
            raise RunnerError(f"{run_id}: injector schedule misses fields: {missing}")
        if any(row.get("parent_event_id") != event_id for row in injection_rows):
            raise RunnerError(f"{run_id}: injector segment is not bound to truth event")
        if any(row.get("implementation_status") != status for row in injection_rows):
            raise RunnerError(
                f"{run_id}: injector implementation status does not match truth schedule"
            )
        expected_safe = not status.startswith("BLOCKED")
        if injector.get("safe_to_execute") is not expected_safe:
            raise RunnerError(
                f"{run_id}: safe_to_execute contradicts the hashed injector schedule"
            )

    if background_path is None:
        if background is not None:
            raise RunnerError(f"{run_id}: background path resolution is inconsistent")
        return
    if not isinstance(background, Mapping):
        raise RunnerError(f"{run_id}: background-flow reference must be an object")
    if background.get("safe_to_execute") is not True:
        raise RunnerError(f"{run_id}: background-flow schedule is not marked safe")
    scenario = str(run.get("scenario", ""))
    if scenario not in EXECUTABLE_BACKGROUND_SCENARIOS:
        raise RunnerError(f"{run_id}: unsupported background scenario {scenario!r}")
    if status != "EXECUTABLE_BACKGROUND_RDMA":
        raise RunnerError(
            f"{run_id}: background-flow schedule requires EXECUTABLE_BACKGROUND_RDMA"
        )
    background_rows = _read_exact_csv(
        background_path, BACKGROUND_FLOW_CSV_COLUMNS,
        f"{run_id} background-flow schedule",
    )
    background_contract = _background_truth_contract(run, truth_rows)
    try:
        truth_start = min(
            _parse_uint(row.get("start_time_ns"), "start_time_ns", run_id)
            for row in truth_rows
        )
        truth_end = max(
            _parse_uint(row.get("end_time_ns"), "end_time_ns", run_id)
            for row in truth_rows
        )
    except ValueError as exc:
        raise RunnerError(f"{run_id}: truth interval is empty") from exc
    flow_ids: set[str] = set()
    qp_keys: set[Tuple[int, int, int, int]] = set()
    parsed_flows: List[Dict[str, int]] = []
    for row in background_rows:
        description = f"{run_id} background flow {row.get('flow_id')!r}"
        if row.get("event_id") != event_id or row.get("scenario") != scenario:
            raise RunnerError(f"{description} is not bound to truth event/scenario")
        flow_id = str(row.get("flow_id", ""))
        if not flow_id or flow_id in flow_ids:
            raise RunnerError(f"{run_id}: empty or duplicate background flow_id")
        flow_ids.add(flow_id)
        start = _parse_uint(row.get("scheduled_start_ns"), "scheduled_start_ns", description)
        src = _parse_uint(row.get("src_rank"), "src_rank", description)
        dst = _parse_uint(row.get("dst_rank"), "dst_rank", description)
        size = _parse_uint(row.get("bytes"), "bytes", description)
        pg = _parse_uint(row.get("pg"), "pg", description)
        sport = _parse_uint(row.get("sport"), "sport", description)
        dport = _parse_uint(row.get("dport"), "dport", description)
        if not (truth_start <= start < truth_end):
            raise RunnerError(f"{description} starts outside the truth interval")
        if src >= 16 or dst >= 16 or src == dst or src // 4 == dst // 4:
            raise RunnerError(f"{description} is not a cross-server true-16 flow")
        if size <= 0 or not (1 <= pg <= 7) or not (49152 <= sport <= 65535) or dport <= 0:
            raise RunnerError(f"{description} has invalid RDMA parameters")
        qp_key = (src, dst, sport, pg)
        if qp_key in qp_keys:
            raise RunnerError(f"{run_id}: duplicate background RDMA QP identity")
        qp_keys.add(qp_key)
        if (
            rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport
            ) != background_contract["route_bucket"]
            or rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport, reverse=True
            ) != background_contract["route_bucket"]
        ):
            raise RunnerError(
                f"{description} is not pinned to the declared data/ACK rail"
            )
        parsed_flows.append(
            {
                "start": start,
                "src": src,
                "dst": dst,
                "bytes": size,
                "pg": pg,
                "sport": sport,
                "dport": dport,
            }
        )
    if any(row.get("action_scope") != "workload" for row in truth_rows):
        raise RunnerError(
            f"{run_id}: background RDMA congestion truth must use workload scope"
        )
    destinations = {flow["dst"] for flow in parsed_flows}
    sizes = {flow["bytes"] for flow in parsed_flows}
    priorities = {flow["pg"] for flow in parsed_flows}
    dports = {flow["dport"] for flow in parsed_flows}
    if len(destinations) != 1 or len(sizes) != 1 or len(priorities) != 1 \
            or len(dports) != 1:
        raise RunnerError(
            f"{run_id}: background flows do not share one destination/class/profile"
        )
    destination = next(iter(destinations))
    if destination != background_contract["destination_rank"]:
        raise RunnerError(
            f"{run_id}: scheduled destination differs from background truth"
        )
    expected_sources = {
        rank for rank in range(16) if rank // 4 != destination // 4
    }
    starts = sorted({flow["start"] for flow in parsed_flows})
    sources_by_start = {
        start: {flow["src"] for flow in parsed_flows if flow["start"] == start}
        for start in starts
    }
    counts_by_start = {
        start: sum(flow["start"] == start for flow in parsed_flows)
        for start in starts
    }
    if scenario == "incast":
        semantic_ok = (
            len(parsed_flows) == 12
            and len(starts) == 1
            and sources_by_start[starts[0]] == expected_sources
            and counts_by_start[starts[0]] == 12
        )
    else:
        semantic_ok = (
            len(parsed_flows) == 24
            and len(starts) == 2
            and starts[1] > starts[0]
            and all(sources_by_start[start] == expected_sources for start in starts)
            and all(counts_by_start[start] == 12 for start in starts)
        )
    if not semantic_ok:
        raise RunnerError(
            f"{run_id}: schedule does not realize the declared {scenario} "
            f"fan-in profile (flows={len(parsed_flows)}, starts={starts}, "
            f"sources_by_start={sources_by_start})"
        )


def _validate_special_congestion_sidecars(
    run: Mapping[str, Any],
    root: Path,
    truth_path: Path,
    link_map_path: Path,
    contract_path: Path,
    workload_path: Path,
) -> None:
    """Reconstruct executable A1 and blocked B1 sidecars independently."""

    run_id = str(run["run_id"])
    schedule = run["schedule"]
    collective = schedule.get("collective_workload_override")
    ecmp = schedule.get("ecmp_collision_schedule")
    if collective is None and ecmp is None:
        return
    if collective is not None and ecmp is not None:
        raise RunnerError(f"{run_id}: blocked static sidecars are mutually exclusive")
    if run.get("class_label") != "CONGESTION" or run.get("run_role") != "congestion":
        raise RunnerError(f"{run_id}: static congestion sidecar is attached to non-congestion run")
    scenario = str(run.get("scenario", ""))
    truth_rows = _read_schedule_rows(truth_path, f"{run_id} truth schedule")
    try:
        onset_ns = min(int(row["start_time_ns"]) for row in truth_rows)
    except (KeyError, TypeError, ValueError) as exc:
        raise RunnerError(f"{run_id}: static sidecar truth onset is invalid") from exc

    if collective is not None:
        if not isinstance(collective, Mapping):
            raise RunnerError(f"{run_id}: collective override reference is invalid")
        if scenario not in corpus_generator.STATIC_COLLECTIVE_OVERRIDE_SCENARIOS:
            raise RunnerError(f"{run_id}: unexpected collective override scenario")
        path = resolve_sidecar(
            root, collective.get("path"), f"{run_id} collective workload override"
        )
        verify_expected_hash(
            path, collective.get("sha256"), f"{run_id} collective workload override"
        )
        role_ref = collective.get("layer_role_sidecar")
        if not isinstance(role_ref, Mapping):
            raise RunnerError(f"{run_id}: collective layer-role reference is missing")
        role_path = resolve_sidecar(
            root, role_ref.get("path"), f"{run_id} collective layer roles"
        )
        verify_expected_hash(
            role_path, role_ref.get("sha256"), f"{run_id} collective layer roles"
        )
        try:
            pairs, _ = corpus_generator.load_true16_links(
                link_map_path, corpus_generator.load_contract(contract_path)
            )
            aggregate_bandwidths = {
                sum(int(link["bandwidth_bps"]) for link in rails.values())
                for rails in pairs.values()
            }
            if len(aggregate_bandwidths) != 1:
                raise corpus_generator.CorpusError(
                    "non-uniform per-rank aggregate ACCESS bandwidth"
                )
            contract = corpus_generator.collective_override_contract(
                scenario,
                onset_ns,
                source_workload_sha256=sha256_file(workload_path),
                aggregate_access_bandwidth_bps=next(iter(aggregate_bandwidths)),
            )
            report = corpus_generator.validate_collective_override(
                path, role_path, contract, workload_path, run_id
            )
        except (OSError, KeyError, TypeError, ValueError,
                corpus_generator.CorpusError) as exc:
            raise RunnerError(
                f"{run_id}: collective workload override is invalid: {exc}"
            ) from exc
        expected = {
            "kind": "simai_collective_workload_override",
            "format": corpus_generator.COLLECTIVE_OVERRIDE_FORMAT,
            "generated_before_run": True,
            "safe_to_execute": True,
            "runtime_executor_status": "READY",
            "layer_role_sidecar": {
                "kind": "collective_layer_roles",
                "format": corpus_generator.COLLECTIVE_ROLE_FORMAT,
                "path": role_ref.get("path"),
                "sha256": role_ref.get("sha256"),
                "generated_before_run": True,
            },
            "static_validation": report,
            "static_validation_sha256": canonical_hash(report),
        }
        if any(collective.get(key) != value for key, value in expected.items()):
            raise RunnerError(f"{run_id}: collective override metadata differs")
        if schedule.get("implementation_status") != \
                corpus_generator.COLLECTIVE_OVERRIDE_STATUS:
            raise RunnerError(f"{run_id}: collective override is not executable")
        if run.get("effective_workload_sha256") != collective.get("sha256"):
            raise RunnerError(f"{run_id}: effective workload identity differs")
    else:
        if not isinstance(ecmp, Mapping) or scenario != "ecmp_or_hash_contention":
            raise RunnerError(f"{run_id}: ECMP collision reference/scenario is invalid")
        path = resolve_sidecar(
            root, ecmp.get("path"), f"{run_id} ECMP collision schedule"
        )
        verify_expected_hash(
            path, ecmp.get("sha256"), f"{run_id} ECMP collision schedule"
        )
        contract = ecmp.get("contract")
        if not isinstance(contract, Mapping):
            raise RunnerError(f"{run_id}: ECMP collision contract is missing")
        try:
            report = corpus_generator.validate_ecmp_collision_schedule(
                path, contract,
                corpus_generator.load_physical_links(link_map_path),
            )
        except (OSError, KeyError, TypeError, ValueError,
                corpus_generator.CorpusError) as exc:
            raise RunnerError(
                f"{run_id}: ECMP collision schedule is invalid: {exc}"
            ) from exc
        expected = {
            "kind": "native_ecmp_collision",
            "format": corpus_generator.ECMP_COLLISION_FORMAT,
            "generated_before_run": True,
            "safe_to_execute": False,
            "runtime_route_evidence_status": "PENDING",
            "contract_sha256": canonical_hash(contract),
        }
        if any(ecmp.get(key) != value for key, value in expected.items()):
            raise RunnerError(f"{run_id}: ECMP collision metadata differs")
        if report.get("status") != "PASS":
            raise RunnerError(f"{run_id}: ECMP collision static validation failed")
        if schedule.get("implementation_status") != \
                corpus_generator.ECMP_COLLISION_STATUS:
            raise RunnerError(f"{run_id}: ECMP collision schedule is not blocked")

    classification, _ = classify_run(run)
    expected_classification = "EXECUTABLE" if collective is not None else "BLOCKED"
    if classification != expected_classification:
        raise RunnerError(
            f"{run_id}: special sidecar classification {classification}, "
            f"expected {expected_classification}"
        )


def validate_corpus(
    corpus_manifest: Path, simulator_binary: Path
) -> ValidatedCorpus:
    if sys.byteorder != HASH_BYTE_ORDER:
        raise RunnerError(
            "P2 RDMA rail pinning mirrors a native little-endian C++ tuple; "
            f"unsupported host byte order: {sys.byteorder}"
        )
    manifest_path = require_file(corpus_manifest, "corpus manifest")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"cannot read corpus manifest: {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise RunnerError("corpus manifest must contain one JSON object")
    if manifest.get("schema_version") != CORPUS_SCHEMA:
        raise RunnerError(
            f"unsupported corpus schema: {manifest.get('schema_version')!r}"
        )
    if manifest.get("status") not in {"PREPARED", "EXECUTED"}:
        raise RunnerError(f"corpus status is not runnable: {manifest.get('status')!r}")
    runs = manifest.get("runs")
    if not isinstance(runs, list) or not runs:
        raise RunnerError("corpus manifest has no runs")
    run_ids: List[str] = []
    for run in runs:
        if not isinstance(run, dict):
            raise RunnerError("each corpus run must be an object")
        run_id = run.get("run_id")
        if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
            raise RunnerError(f"unsafe or invalid run_id: {run_id!r}")
        run_ids.append(run_id)
        for field in ("partition", "split_group_id", "simulation_seed"):
            if field not in run:
                raise RunnerError(f"{run_id}: required run field is missing: {field}")
        finish = run.get("virtual_finish_ns")
        if not isinstance(finish, int) or isinstance(finish, bool) or finish <= 0:
            raise RunnerError(f"{run_id}: virtual_finish_ns must be positive")
        schedule = run.get("schedule")
        mechanism = run.get("mechanism")
        if not isinstance(schedule, dict) or not isinstance(mechanism, dict):
            raise RunnerError(f"{run_id}: schedule/mechanism object is missing")
    if len(run_ids) != len(set(run_ids)):
        raise RunnerError("corpus run_id values are not unique")

    root = manifest_path.parent.resolve()
    input_artifacts = manifest.get("input_artifacts")
    required_inputs = ("contract", "link_map", "topology", "workload", "simulator_config")
    if not isinstance(input_artifacts, dict):
        raise RunnerError("input_artifacts object is missing")
    input_paths: Dict[str, Path] = {}
    for name in required_inputs:
        ref = input_artifacts.get(name)
        if not isinstance(ref, dict):
            raise RunnerError(f"input_artifacts.{name} is missing")
        path = require_file(Path(str(ref.get("path", ""))), f"input {name}")
        verify_expected_hash(path, ref.get("sha256"), f"input {name}")
        input_paths[name] = path
    _validate_workload_qualification(manifest, input_paths["workload"])

    for name in ("feature_schema", "split_manifest", "leakage_checks", "observability_report"):
        ref = manifest.get(name)
        if not isinstance(ref, dict):
            raise RunnerError(f"hashed corpus sidecar is missing: {name}")
        path = resolve_sidecar(root, ref.get("path"), name)
        verify_expected_hash(path, ref.get("sha256"), name)

    for run in runs:
        run_id = run["run_id"]
        schedule = run["schedule"]
        truth = resolve_sidecar(root, schedule.get("path"), f"{run_id} truth schedule")
        verify_expected_hash(truth, schedule.get("sha256"), f"{run_id} truth schedule")
        injector = schedule.get("simulator_injection_schedule")
        injection_path: Optional[Path] = None
        if injector is not None:
            if not isinstance(injector, dict):
                raise RunnerError(f"{run_id}: injector reference must be an object")
            injection_path = resolve_sidecar(
                root, injector.get("path"), f"{run_id} simulator injection schedule"
            )
            verify_expected_hash(
                injection_path,
                injector.get("sha256"),
                f"{run_id} simulator injection schedule",
            )
        background = schedule.get("background_flow_schedule")
        background_path: Optional[Path] = None
        if background is not None:
            if not isinstance(background, dict):
                raise RunnerError(
                    f"{run_id}: background-flow reference must be an object"
                )
            background_path = resolve_sidecar(
                root, background.get("path"), f"{run_id} background-flow schedule"
            )
            verify_expected_hash(
                background_path,
                background.get("sha256"),
                f"{run_id} background-flow schedule",
            )
        _validate_special_congestion_sidecars(
            run,
            root,
            truth,
            input_paths["link_map"],
            input_paths["contract"],
            input_paths["workload"],
        )
        _validate_schedule_binding(run, truth, injection_path, background_path)

    _validate_corpus_identity(manifest)
    topology_contract = manifest.get("topology_contract", {})
    if topology_contract.get("gpu_count") != 16:
        raise RunnerError("topology_contract.gpu_count must equal 16")
    topology_result = _validate_topology(input_paths["topology"], 16)
    _validate_link_map_against_topology(
        input_paths["link_map"], input_paths["topology"], topology_result
    )

    binary = require_file(simulator_binary, "simulator binary")
    if not os.access(binary, os.X_OK):
        raise RunnerError(f"simulator binary is not executable: {binary}")
    return ValidatedCorpus(
        manifest_path=manifest_path,
        root=root,
        manifest=manifest,
        manifest_sha256=sha256_file(manifest_path),
        simulator_binary=binary,
        simulator_sha256=sha256_file(binary),
        input_paths=input_paths,
    )


def select_runs(
    corpus: ValidatedCorpus,
    requested_run_ids: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> List[Mapping[str, Any]]:
    runs = list(corpus.manifest["runs"])
    by_id = {run["run_id"]: run for run in runs}
    if requested_run_ids:
        if len(requested_run_ids) != len(set(requested_run_ids)):
            raise RunnerError("--run-id contains duplicates")
        unknown = [run_id for run_id in requested_run_ids if run_id not in by_id]
        if unknown:
            raise RunnerError(f"unknown run_id value(s): {unknown}")
        runs = [by_id[run_id] for run_id in requested_run_ids]
    if limit is not None:
        if limit <= 0:
            raise RunnerError("--limit must be positive")
        runs = runs[:limit]
    return runs


def classify_run(run: Mapping[str, Any]) -> Tuple[str, str]:
    """Return (EXECUTABLE|BLOCKED, precise reason/status)."""
    mechanism = run["mechanism"]
    mechanism_status = str(mechanism.get("implementation_status", ""))
    schedule = run["schedule"]
    schedule_status = str(schedule.get("implementation_status", ""))
    if run.get("class_label") == "CONGESTION" or run.get("run_role") == "congestion":
        background = schedule.get("background_flow_schedule")
        background_executable = (
            run.get("class_label") == "CONGESTION"
            and run.get("run_role") == "congestion"
            and run.get("scenario") in EXECUTABLE_BACKGROUND_SCENARIOS
            and mechanism_status == "PLANNED"
            and schedule_status == "EXECUTABLE_BACKGROUND_RDMA"
            and isinstance(background, Mapping)
            and background.get("safe_to_execute") is True
            and schedule.get("simulator_injection_schedule") is None
        )
        if background_executable:
            return "EXECUTABLE", "EXECUTABLE_BACKGROUND_RDMA"
        collective = schedule.get("collective_workload_override")
        collective_executable = (
            run.get("class_label") == "CONGESTION"
            and run.get("run_role") == "congestion"
            and run.get("scenario")
            in corpus_generator.EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS
            and mechanism_status == "PLANNED"
            and schedule_status == corpus_generator.COLLECTIVE_OVERRIDE_STATUS
            and isinstance(collective, Mapping)
            and collective.get("safe_to_execute") is True
            and collective.get("runtime_executor_status") == "READY"
            and isinstance(collective.get("layer_role_sidecar"), Mapping)
            and schedule.get("simulator_injection_schedule") is None
            and background is None
            and run.get("effective_workload_sha256") == collective.get("sha256")
        )
        if collective_executable:
            return "EXECUTABLE", "EXECUTABLE_COLLECTIVE_WORKLOAD_OVERRIDE"
        return "BLOCKED", "BLOCKED_MISSING_CONGESTION_EXECUTOR"
    if mechanism_status == "UNAVAILABLE":
        return "BLOCKED", "BLOCKED_UNAVAILABLE_MECHANISM"
    if schedule_status.startswith("BLOCKED"):
        return "BLOCKED", "BLOCKED_UNSUPPORTED_SCHEDULE"

    injector = schedule.get("simulator_injection_schedule")
    if injector is None:
        healthy_control = (
            run.get("class_label") == "HEALTHY"
            and run.get("run_role") == "healthy"
            and mechanism.get("mechanism_id") == "no_physical_injection"
            and schedule_status == "EXECUTABLE_NO_INJECTION"
        )
        if healthy_control:
            return "EXECUTABLE", "EXECUTABLE_HEALTHY_CONTROL"
        return "BLOCKED", "BLOCKED_MISSING_SIMULATOR_INJECTION_SCHEDULE"
    if not isinstance(injector, dict):
        return "BLOCKED", "BLOCKED_INVALID_SIMULATOR_INJECTION_SCHEDULE"
    if injector.get("safe_to_execute") is not True:
        return "BLOCKED", "BLOCKED_UNSAFE_SIMULATOR_INJECTION"
    if mechanism_status in {"", "PENDING", "BLOCKED"}:
        return "BLOCKED", "BLOCKED_MECHANISM_NOT_AVAILABLE"
    return "EXECUTABLE", "EXECUTABLE_SAFE_INJECTION"


def _copy_verified(source: Path, destination: Path, expected_sha256: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    verify_expected_hash(destination, expected_sha256, f"copied input {destination.name}")


def materialize_runtime_config(
    frozen_config: Path, pending: Path
) -> Tuple[Path, Dict[str, Any]]:
    """Derive an isolated config without mutating the hash-locked input.

    The repository baseline config contains absolute paths into a shared
    results directory.  Reusing it verbatim makes concurrent corpus runs
    overwrite each other's FLOW/FCT/PFC files.  Keep the exact frozen copy as
    provenance, but rewrite every recognized path into this attempt's
    ``raw_simai`` directory and bind the derived bytes in the invocation.
    """

    raw_dir = (pending / "raw_simai").resolve()
    replacements = {
        key: raw_dir / filename for key, filename in RUNTIME_CONFIG_PATHS.items()
    }
    seen: Dict[str, int] = {key: 0 for key in replacements}
    output_lines: List[str] = []
    for line_number, line in enumerate(
            frozen_config.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            output_lines.append(line)
            continue
        key = stripped.split(None, 1)[0]
        if key not in replacements:
            output_lines.append(line)
            continue
        seen[key] += 1
        if seen[key] != 1:
            raise RunnerError(
                f"simulator config contains duplicate path key {key} "
                f"at line {line_number}"
            )
        # The simulator runs with cwd=<attempt>/raw_simai.  Relative names
        # remain valid after the pending directory is atomically renamed.
        output_lines.append(f"{key} {replacements[key].name}")
    missing = sorted(key for key in REQUIRED_RUNTIME_CONFIG_PATHS if not seen[key])
    if missing:
        raise RunnerError(f"simulator config misses required path keys: {missing}")

    # SetupNetwork unconditionally reads one count from both inputs.
    replacements["FLOW_FILE"].write_text("0\n", encoding="ascii")
    replacements["TRACE_FILE"].write_text("0\n", encoding="ascii")
    runtime = pending / "inputs" / "simulator_config.runtime.conf"
    runtime.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    return runtime, {
        "path": runtime.relative_to(pending).as_posix(),
        "sha256": sha256_file(runtime),
        "derived_from_sha256": sha256_file(frozen_config),
        "isolated_paths": {
            key: path.relative_to(pending.resolve()).as_posix()
            for key, path in sorted(replacements.items())
            if seen[key]
        },
    }


def _artifact_entries(directory: Path) -> List[Dict[str, Any]]:
    excluded = {"run_manifest.json", "run_manifest.sha256"}
    entries: List[Dict[str, Any]] = []
    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(directory).as_posix()
        if relative in excluded:
            continue
        entries.append(
            {"path": relative, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
        )
    return entries


def read_lifecycle(path: Path, run_id: str, finish_ns: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "path": "run_lifecycle.csv",
        "parse_status": "MISSING",
        "workload_status": "WORKLOAD_STATUS_UNKNOWN",
        "workload_complete_ns": None,
        "observation_status": "OBSERVATION_WINDOW_CENSORED",
        "observation_scheduled_ns": finish_ns,
        "observation_actual_ns": None,
        "finished_ranks": None,
        "world_size": 16,
    }
    if not path.is_file():
        return result
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {
                "run_id", "event", "scheduled_ns", "actual_ns",
                "finished_ranks", "world_size", "status",
            }
            if not required.issubset(reader.fieldnames or []):
                result["parse_status"] = "INVALID_SCHEMA"
                return result
            rows = list(reader)
    except (OSError, csv.Error):
        result["parse_status"] = "INVALID_CSV"
        return result
    if any(row.get("run_id") != run_id for row in rows):
        result["parse_status"] = "RUN_ID_MISMATCH"
        return result
    result["parse_status"] = "PARSED"
    workload = [row for row in rows if row.get("status") == "WORKLOAD_COMPLETE"]
    if workload:
        result["workload_status"] = "WORKLOAD_COMPLETE"
        try:
            result["workload_complete_ns"] = min(int(row["actual_ns"]) for row in workload)
        except (TypeError, ValueError):
            result["parse_status"] = "INVALID_NUMERIC_VALUE"
    else:
        result["workload_status"] = "WORKLOAD_INCOMPLETE"

    horizon_rows = [
        row for row in rows
        if row.get("event") == "observation_horizon"
        and str(row.get("status", "")).startswith("OBSERVATION_WINDOW_COMPLETE")
    ]
    valid_horizons: List[Tuple[int, int, int, int, str]] = []
    for row in horizon_rows:
        try:
            scheduled = int(row["scheduled_ns"])
            actual = int(row["actual_ns"])
            finished = int(row["finished_ranks"])
            world = int(row["world_size"])
        except (TypeError, ValueError):
            result["parse_status"] = "INVALID_NUMERIC_VALUE"
            continue
        if scheduled == finish_ns and actual >= finish_ns and world == 16:
            valid_horizons.append((scheduled, actual, finished, world, row["status"]))
    if valid_horizons:
        scheduled, actual, finished, world, raw_status = valid_horizons[-1]
        result.update(
            {
                "observation_status": "OBSERVATION_WINDOW_COMPLETE",
                "observation_detail": raw_status,
                "observation_scheduled_ns": scheduled,
                "observation_actual_ns": actual,
                "finished_ranks": finished,
                "world_size": world,
            }
        )
    return result


def _stability_gate_required(run: Mapping[str, Any]) -> bool:
    """Return the hash-locked gate declaration and reject unsafe lifecycle states."""

    raw = run.get("simulator_stability")
    if not isinstance(raw, Mapping):
        if run.get("fault_family") == "random_loss":
            raise RunnerError(
                f"{run.get('run_id')}: random_loss lacks simulator stability plan"
            )
        return False
    gate_required = raw.get("gate_required") is True
    if run.get("fault_family") == "random_loss" and not gate_required:
        raise RunnerError(
            f"{run.get('run_id')}: random_loss must declare its stability gate"
        )
    if gate_required and raw.get("status") != "PENDING_EXECUTION":
        raise RunnerError(
            f"{run.get('run_id')}: executable stability gate must remain "
            "PENDING_EXECUTION until sealed simulator evidence is published"
        )
    return gate_required


def _load_passing_stability_source(path: Path, description: str) -> Mapping[str, Any]:
    require_file(path, description)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"invalid {description} JSON: {exc}") from exc
    if not isinstance(value, Mapping) or value.get("status") != "PASS":
        raise RunnerError(f"{description} does not contain a PASS object")
    return value


def _simulator_stability_report(
    directory: Path,
    run: Mapping[str, Any],
    *,
    simulator_worker_threads: Any,
    runtime_closure_identity_sha256: Any,
    exit_code: Any,
    process_status: Any,
    timed_out: Any,
    interrupted: Any,
    resource_observation: Any,
    execution_status: Any,
    lifecycle: Any,
    workload_completed: Any,
    runtime_execution_status: Any,
    semantic_validation_status: Any,
    workload_runtime_qualification_status: Any,
    ecmp_route_candidate_validation_status: Any,
    training_source_port_allocator_validation_status: Any,
) -> Dict[str, Any]:
    """Recompute a stability PASS solely from sealed process and run evidence."""

    if not _stability_gate_required(run):
        raise RunnerError(
            f"{run.get('run_id')}: simulator stability report is not required"
        )
    run_id = str(run.get("run_id", ""))
    if isinstance(simulator_worker_threads, bool) or not isinstance(
        simulator_worker_threads, int
    ) or simulator_worker_threads <= 0:
        raise RunnerError(f"{run_id}: invalid stability worker-thread evidence")
    if not isinstance(runtime_closure_identity_sha256, str) or not SHA256_RE.fullmatch(
        runtime_closure_identity_sha256
    ):
        raise RunnerError(f"{run_id}: invalid stability runtime closure identity")
    if (directory / "exit_code.txt").read_text(encoding="ascii").strip() != "0":
        raise RunnerError(f"{run_id}: stability evidence exit_code.txt is not zero")
    if exit_code != 0 or process_status != "EXITED_ZERO":
        raise RunnerError(f"{run_id}: simulator process did not exit cleanly")
    if timed_out is not False or interrupted is not False:
        raise RunnerError(f"{run_id}: timeout or interruption cannot pass stability")
    if (
        not isinstance(resource_observation, Mapping)
        or resource_observation.get("oom_kill_observed_during_attempt") is not False
    ):
        raise RunnerError(f"{run_id}: OOM-free process evidence is absent")
    if execution_status != "COMPLETE":
        raise RunnerError(f"{run_id}: incomplete execution cannot pass stability")
    recomputed_lifecycle = read_lifecycle(
        directory / "run_lifecycle.csv", run_id, int(run["virtual_finish_ns"])
    )
    if (
        not isinstance(lifecycle, Mapping)
        or dict(lifecycle) != recomputed_lifecycle
        or recomputed_lifecycle.get("parse_status") != "PARSED"
        or recomputed_lifecycle.get("observation_status")
        != "OBSERVATION_WINDOW_COMPLETE"
    ):
        raise RunnerError(f"{run_id}: observation lifecycle is not independently complete")
    if workload_completed is not (
        recomputed_lifecycle.get("workload_status") == "WORKLOAD_COMPLETE"
    ):
        raise RunnerError(f"{run_id}: workload completion contradicts lifecycle evidence")
    statuses = {
        "runtime_execution": runtime_execution_status,
        "semantic_validation": semantic_validation_status,
        "workload_runtime_qualification": workload_runtime_qualification_status,
        "ecmp_route_candidate_validation": ecmp_route_candidate_validation_status,
        "training_source_port_allocator_validation": (
            training_source_port_allocator_validation_status
        ),
    }
    failed_statuses = sorted(name for name, status in statuses.items() if status != "PASS")
    if failed_statuses:
        raise RunnerError(
            f"{run_id}: non-PASS stability prerequisites={failed_statuses}"
        )
    for filename, description in (
        (RUNTIME_EXECUTION_EVIDENCE_FILE, "runtime execution evidence"),
        (SEMANTIC_VALIDATION_FILE, "semantic validation"),
        (WORKLOAD_RUNTIME_QUALIFICATION_FILE, "workload runtime qualification"),
        (ECMP_ROUTE_VALIDATION_FILE, "ECMP route candidate validation"),
    ):
        _load_passing_stability_source(directory / filename, description)
    source_hashes = {
        name: sha256_file(require_file(directory / relative, name))
        for name, relative in sorted(SIMULATOR_STABILITY_SOURCE_FILES.items())
    }
    planned_stability = run["simulator_stability"]
    basis = {
        "planned_stability_status": planned_stability["status"],
        "exit_code": exit_code,
        "process_status": process_status,
        "timed_out": timed_out,
        "interrupted": interrupted,
        "oom_kill_observed_during_attempt": False,
        "execution_status": execution_status,
        "observation_status": recomputed_lifecycle["observation_status"],
        "workload_completed": workload_completed,
        "runtime_execution_status": runtime_execution_status,
        "semantic_validation_status": semantic_validation_status,
        "workload_runtime_qualification_status": (
            workload_runtime_qualification_status
        ),
        "ecmp_route_candidate_validation_status": (
            ecmp_route_candidate_validation_status
        ),
        "training_source_port_allocator_validation_status": (
            training_source_port_allocator_validation_status
        ),
        "runtime_closure_identity_sha256": runtime_closure_identity_sha256,
        "simulator_worker_threads": simulator_worker_threads,
        "source_artifacts_sha256": source_hashes,
    }
    return {
        "schema_version": SIMULATOR_STABILITY_SCHEMA,
        "status": "PASS",
        "run_id": run_id,
        "fault_family": run.get("fault_family"),
        "gate_required": True,
        "planned_run_sha256": canonical_hash(run),
        "virtual_finish_ns": run.get("virtual_finish_ns"),
        "evidence_basis": basis,
        "evidence_basis_sha256": canonical_hash(basis),
        "checks": [
            {"name": name, "status": "PASS"}
            for name in SIMULATOR_STABILITY_CHECK_NAMES
        ],
        "errors": [],
    }


def validate_background_application(
    path: Path,
    schedule_path: Path,
    run_id: str,
    finish_ns: int,
) -> Dict[str, Any]:
    """Require an ACK-qualified, uncensored lifecycle for every declared flow."""
    declared_rows = _read_exact_csv(
        schedule_path, BACKGROUND_FLOW_CSV_COLUMNS,
        f"{run_id} copied background-flow schedule",
    )
    observed_rows = _read_exact_csv(
        path, BACKGROUND_APPLICATION_CSV_COLUMNS,
        f"{run_id} background-flow application evidence",
    )
    declared = {row["flow_id"]: row for row in declared_rows}
    if len(declared) != len(declared_rows):
        raise RunnerError(f"{run_id}: copied background schedule has duplicate flow_id")
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in observed_rows:
        if row.get("run_id") != run_id:
            raise RunnerError(f"{run_id}: background evidence has a foreign run_id")
        flow_id = str(row.get("flow_id", ""))
        if flow_id not in declared:
            raise RunnerError(
                f"{run_id}: background evidence contains undeclared flow {flow_id!r}"
            )
        grouped.setdefault(flow_id, []).append(row)
    if set(grouped) != set(declared):
        missing = sorted(set(declared) - set(grouped))
        raise RunnerError(f"{run_id}: missing background evidence for flows {missing}")

    completions: List[int] = []
    first_txs: List[int] = []
    first_acks: List[int] = []
    lifecycles: List[Dict[str, int]] = []
    metadata = (
        "event_id", "flow_id", "scenario", "scheduled_start_ns", "src_rank",
        "dst_rank", "bytes", "pg", "sport", "dport",
    )
    for flow_id, rows in grouped.items():
        expected = declared[flow_id]
        if len(rows) != 3 or [row.get("event") for row in rows].count("SCHEDULED") != 1 \
                or [row.get("event") for row in rows].count("START") != 1 \
                or [row.get("event") for row in rows].count("COMPLETE") != 1:
            events = [row.get("event") for row in rows]
            raise RunnerError(
                f"{run_id}: flow {flow_id} is not exactly "
                f"SCHEDULED/START/COMPLETE: {events}"
            )
        if any(
            row.get(field) != expected.get(field)
            for row in rows
            for field in metadata
        ):
            raise RunnerError(
                f"{run_id}: flow {flow_id} evidence metadata differs from schedule"
            )
        by_event = {str(row["event"]): row for row in rows}
        scheduled = by_event["SCHEDULED"]
        started = by_event["START"]
        complete = by_event["COMPLETE"]
        if (
            scheduled.get("status") != "INSTALLED"
            or scheduled.get("actual_ns")
            or scheduled.get("first_tx_ns")
            or scheduled.get("first_ack_ns")
            or started.get("status") != "QP_CREATED"
            or started.get("first_tx_ns")
            or started.get("first_ack_ns")
            or complete.get("status") != "ACK_COMPLETE"
        ):
            raise RunnerError(f"{run_id}: flow {flow_id} has invalid lifecycle fields")
        scheduled_ns = _parse_uint(
            expected["scheduled_start_ns"], "scheduled_start_ns", flow_id
        )
        start_ns = _parse_uint(started.get("actual_ns"), "START actual_ns", flow_id)
        complete_ns = _parse_uint(
            complete.get("actual_ns"), "COMPLETE actual_ns", flow_id
        )
        first_tx_ns = _parse_uint(
            complete.get("first_tx_ns"), "first_tx_ns", flow_id
        )
        first_ack_ns = _parse_uint(
            complete.get("first_ack_ns"), "first_ack_ns", flow_id
        )
        if not (
            scheduled_ns <= start_ns <= first_tx_ns <= first_ack_ns
            <= complete_ns <= finish_ns
        ):
            raise RunnerError(
                f"{run_id}: flow {flow_id} has non-causal or out-of-window timestamps"
            )
        completions.append(complete_ns)
        first_txs.append(first_tx_ns)
        first_acks.append(first_ack_ns)
        lifecycles.append(
            {
                "scheduled_start_ns": scheduled_ns,
                "first_tx_ns": first_tx_ns,
                "first_ack_ns": first_ack_ns,
                "complete_ns": complete_ns,
            }
        )
    waves = [
        {
            "scheduled_start_ns": scheduled_ns,
            "flow_count": sum(
                item["scheduled_start_ns"] == scheduled_ns
                for item in lifecycles
            ),
            "earliest_first_tx_ns": min(
                item["first_tx_ns"] for item in lifecycles
                if item["scheduled_start_ns"] == scheduled_ns
            ),
            "latest_complete_ns": max(
                item["complete_ns"] for item in lifecycles
                if item["scheduled_start_ns"] == scheduled_ns
            ),
        }
        for scheduled_ns in sorted(
            {item["scheduled_start_ns"] for item in lifecycles}
        )
    ]
    return {
        "status": "COMPLETE",
        "path": "background_flow_application.csv",
        "declared_flow_count": len(declared),
        "completed_flow_count": len(completions),
        "earliest_first_tx_ns": min(first_txs),
        "earliest_first_ack_ns": min(first_acks),
        "latest_complete_ns": max(completions),
        "realized_window_start_ns": min(first_txs),
        "realized_window_end_ns": max(completions),
        "waves": waves,
        "ack_qualified": True,
        "censored_flow_count": 0,
    }


def validate_background_congestion_signal(
    *,
    run: Mapping[str, Any],
    application_evidence: Mapping[str, Any],
    truth_path: Path,
    schedule_path: Path,
    switch_path: Path,
    runtime_link_map_path: Path,
    frozen_link_map_path: Path,
    rdma_path: Path,
    fault_application_path: Path,
) -> Dict[str, Any]:
    """Prove a hash-pinned fan-in reached its one declared ACCESS port."""
    run_id = str(run["run_id"])
    mechanism_id = str(run["mechanism"]["mechanism_id"])
    scenario = str(run["scenario"])
    checks: List[Dict[str, Any]] = []

    for artifact_path, description in (
        (truth_path, "truth schedule"),
        (schedule_path, "background-flow schedule"),
        (switch_path, "switch telemetry"),
        (runtime_link_map_path, "runtime link map"),
        (frozen_link_map_path, "frozen link map"),
        (rdma_path, "RDMA telemetry"),
        (fault_application_path, "fault application telemetry"),
    ):
        require_file(artifact_path, f"{run_id} {description}")

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append(
            {"name": name, "status": "PASS" if passed else "FAIL", "detail": detail}
        )

    declared = _read_exact_csv(
        schedule_path, BACKGROUND_FLOW_CSV_COLUMNS,
        f"{run_id} copied background-flow schedule",
    )
    truth_rows = _read_schedule_rows(truth_path, f"{run_id} truth schedule")
    contract = _background_truth_contract(run, truth_rows)
    destinations = {int(row["dst_rank"]) for row in declared}
    destination = next(iter(destinations)) if len(destinations) == 1 else -1
    check(
        "declared_common_destination",
        destination >= 0,
        {"destinations": sorted(destinations), "flow_count": len(declared)},
    )

    scheduled_starts = sorted({int(row["scheduled_start_ns"]) for row in declared})
    launch_window_exact = (
        bool(scheduled_starts)
        and scheduled_starts[0] == contract["truth_start_ns"]
        and scheduled_starts[-1] + 1 == contract["truth_end_ns"]
    )
    check(
        "predeclared_launch_window_exact",
        launch_window_exact,
        {
            "scheduled_starts_ns": scheduled_starts,
            "truth_start_ns": contract["truth_start_ns"],
            "truth_end_ns": contract["truth_end_ns"],
        },
    )
    deadline_met = (
        int(application_evidence["latest_complete_ns"])
        <= contract["completion_deadline_ns"]
    )
    check(
        "ack_completion_before_deadline",
        deadline_met,
        {
            "latest_complete_ns": application_evidence["latest_complete_ns"],
            "completion_deadline_ns": contract["completion_deadline_ns"],
        },
    )
    waves = list(application_evidence.get("waves", []))
    wave_profile_ok = len(waves) == (1 if scenario == "incast" else 2)
    if scenario == "queue_buildup" and wave_profile_ok:
        wave_profile_ok = (
            int(waves[1]["earliest_first_tx_ns"])
            <= int(waves[0]["latest_complete_ns"])
        )
    check(
        "realized_wave_profile",
        wave_profile_ok,
        {"scenario": scenario, "waves": waves},
    )

    runtime_link_hash = sha256_file(runtime_link_map_path)
    frozen_link_hash = sha256_file(frozen_link_map_path)
    check(
        "runtime_link_map_matches_frozen_input",
        runtime_link_hash == frozen_link_hash,
        {"runtime_sha256": runtime_link_hash, "frozen_sha256": frozen_link_hash},
    )

    try:
        with runtime_link_map_path.open(encoding="utf-8", newline="") as stream:
            link_reader = csv.DictReader(stream)
            required = {
                "link_id", "src_node", "dst_node", "src_type", "dst_type",
                "src_port", "dst_port", "link_class",
            }
            if not required.issubset(link_reader.fieldnames or []):
                raise RunnerError("link_map lacks physical endpoint columns")
            link_rows = list(link_reader)
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"{run_id}: cannot parse link_map: {exc}") from exc
    endpoints: Dict[str, Tuple[int, int]] = {}
    host_ports: Dict[str, int] = {}
    for row in link_rows:
        if row.get("link_class") != "ACCESS":
            continue
        if row.get("src_type") == "HOST" and int(row["src_node"]) == destination:
            endpoints[row["link_id"]] = (int(row["dst_node"]), int(row["dst_port"]))
            host_ports[row["link_id"]] = int(row["src_port"])
        elif row.get("dst_type") == "HOST" and int(row["dst_node"]) == destination:
            endpoints[row["link_id"]] = (int(row["src_node"]), int(row["src_port"]))
            host_ports[row["link_id"]] = int(row["dst_port"])
    target_link_id = str(contract["bottleneck_access_link_id"])
    paired_link_id = str(contract["paired_access_link_id"])
    expected_host_port = contract["route_candidate_order_host_ports"][
        contract["route_bucket"]
    ]
    check(
        "dual_access_pair_and_target_resolved",
        len(endpoints) == 2
        and set(endpoints) == {target_link_id, paired_link_id}
        and host_ports.get(target_link_id) == expected_host_port,
        {
            "destination_rank": destination,
            "switch_endpoints": endpoints,
            "host_ports": host_ports,
            "target_link_id": target_link_id,
            "expected_target_host_port": expected_host_port,
        },
    )

    try:
        with switch_path.open(encoding="utf-8", newline="") as stream:
            switch_reader = csv.DictReader(stream)
            required_switch = {
                "run_id", "timestamp_ns", "switch_id", "port_id", "link_id",
                "direction", "queue_bytes", "max_queue_bytes",
                "observed_throughput_bps", "link_state",
            }
            if not required_switch.issubset(switch_reader.fieldnames or []):
                raise RunnerError(
                    "switch_telemetry lacks queue/throughput/link-state columns"
                )
            switch_rows = list(switch_reader)
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"{run_id}: cannot parse switch telemetry: {exc}") from exc
    if any(row.get("run_id") != run_id for row in switch_rows):
        raise RunnerError(f"{run_id}: switch telemetry contains foreign run IDs")
    target_rows = [
        row for row in switch_rows
        if row.get("direction", "").lower() == "tx"
        and row.get("link_id") == target_link_id
        and (
            int(row["switch_id"]), int(row["port_id"])
        ) == endpoints.get(target_link_id)
    ]
    first_tx = int(application_evidence["earliest_first_tx_ns"])
    latest_complete = int(application_evidence["latest_complete_ns"])
    before = [row for row in target_rows if int(row["timestamp_ns"]) < first_tx]
    during = [
        row for row in target_rows
        if first_tx <= int(row["timestamp_ns"]) <= latest_complete + 1_000_000
    ]
    baseline_queue = sorted(
        int(row["max_queue_bytes"] or row["queue_bytes"] or 0) for row in before
    )
    during_queue = [
        int(row["max_queue_bytes"] or row["queue_bytes"] or 0) for row in during
    ]
    baseline_median = (
        baseline_queue[len(baseline_queue) // 2] if baseline_queue else None
    )
    event_peak = max(during_queue) if during_queue else None
    queue_pressure = (
        event_peak is not None and event_peak > 0
        and baseline_median is not None and event_peak > baseline_median
    )
    throughput_values = [
        int(row["observed_throughput_bps"] or 0) for row in during
    ]
    throughput_active = bool(throughput_values) and max(throughput_values) > 0
    carrier_up = bool(during) and all(
        row.get("link_state", "").lower() == "up" for row in during
    )
    check(
        "target_access_queue_pressure",
        queue_pressure,
        {
            "before_samples": len(before),
            "event_samples": len(during),
            "baseline_median_max_queue_bytes": baseline_median,
            "event_peak_max_queue_bytes": event_peak,
        },
    )
    check(
        "target_access_throughput_active",
        throughput_active,
        {"maximum_observed_throughput_bps": max(throughput_values, default=0)},
    )
    check(
        "target_access_carrier_up",
        carrier_up,
        {"states": sorted({row.get("link_state", "") for row in during})},
    )

    try:
        with rdma_path.open(encoding="utf-8", newline="") as stream:
            rdma_reader = csv.DictReader(stream)
            if tuple(rdma_reader.fieldnames or ()) != RDMA_TELEMETRY_CSV_COLUMNS:
                raise RunnerError(
                    "rdma_wc_telemetry exact 26-column schema mismatch"
                )
            rdma_rows = list(rdma_reader)
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"{run_id}: cannot parse RDMA telemetry: {exc}") from exc
    if any(row.get("run_id") != run_id for row in rdma_rows):
        raise RunnerError(f"{run_id}: RDMA telemetry contains foreign run IDs")
    background_rdma = [
        row for row in rdma_rows if row.get("traffic_class") == "BACKGROUND"
    ]
    expected_qp_rows = {
        f"{int(row['src_rank'])}-{int(row['dst_rank'])}-"
        f"{int(row['sport'])}-{int(row['pg'])}": row
        for row in declared
    }
    expected_qps = set(expected_qp_rows)
    observed_qps = {
        str(row.get("logical_qp_id", "")) for row in background_rdma
    }
    check(
        "background_qp_identity_bijection",
        observed_qps == expected_qps,
        {"expected": sorted(expected_qps), "observed": sorted(observed_qps)},
    )
    qp_lifecycle_ok = observed_qps == expected_qps
    configuration_ok = observed_qps == expected_qps
    forbidden_events: List[Dict[str, str]] = []
    for logical_id in expected_qps:
        rows = [
            row for row in background_rdma
            if row.get("logical_qp_id") == logical_id
        ]
        created = [row for row in rows if row.get("event") == "QP_CREATED"]
        success = [
            row for row in rows
            if row.get("event") == "WC" and row.get("wc_status") == "SUCCESS"
        ]
        bad = [
            row for row in rows
            if row.get("event") in {
                "RTO_RETRY", "BACKUP_ACTIVATE", "QP_PREESTABLISHED",
                "FAILOVER_UNAVAILABLE",
            }
            or (row.get("event") == "WC" and row.get("wc_status") != "SUCCESS")
        ]
        forbidden_events.extend(bad)
        qp_lifecycle_ok = qp_lifecycle_ok and len(created) == 1 and len(success) == 1
        declared_qp = expected_qp_rows[logical_id]
        if len(created) == 1:
            try:
                configuration_ok = configuration_ok and (
                    int(created[0]["primary_nic"]) == expected_host_port
                    and int(created[0]["active_nic"]) == expected_host_port
                    and all(
                        int(row["primary_nic"]) == expected_host_port
                        and int(row["active_nic"]) == expected_host_port
                        and int(row["retry_count"]) == 0
                        and int(row["retry_limit"])
                        == contract["rdma_retry_limit"]
                        and int(row["rto_us"]) == contract["rdma_rto_us"]
                        and int(row["src_rank"]) == int(declared_qp["src_rank"])
                        and int(row["dst_rank"]) == int(declared_qp["dst_rank"])
                        and int(row["sport"]) == int(declared_qp["sport"])
                        for row in rows
                    )
                )
            except (TypeError, ValueError):
                configuration_ok = False
        else:
            configuration_ok = False
    no_retry_or_failover = not forbidden_events
    check(
        "background_qp_ack_success_lifecycle",
        qp_lifecycle_ok,
        {"expected_qp_count": len(expected_qps)},
    )
    check(
        "background_qp_single_rail_transport_config",
        configuration_ok,
        {
            "route_bucket": contract["route_bucket"],
            "expected_primary_nic": expected_host_port,
            "rto_us": contract["rdma_rto_us"],
            "retry_limit": contract["rdma_retry_limit"],
        },
    )
    check(
        "zero_background_retry_or_failover_events",
        no_retry_or_failover,
        {"forbidden_event_count": len(forbidden_events)},
    )

    try:
        with fault_application_path.open(encoding="utf-8", newline="") as stream:
            fault_rows = list(csv.DictReader(stream))
    except (OSError, csv.Error) as exc:
        raise RunnerError(
            f"{run_id}: cannot parse fault application telemetry: {exc}"
        ) from exc
    no_fault = not fault_rows
    check("no_physical_fault_application", no_fault, {"row_count": len(fault_rows)})
    check(
        "all_background_qps_ack_complete",
        application_evidence.get("status") == "COMPLETE"
        and application_evidence.get("ack_qualified") is True
        and application_evidence.get("completed_flow_count")
        == application_evidence.get("declared_flow_count"),
        dict(application_evidence),
    )
    status = "PASS" if all(item["status"] == "PASS" for item in checks) else "FAIL"
    source_hashes = {
        "switch": sha256_file(switch_path),
        "background_application": sha256_file(
            switch_path.parent / "background_flow_application.csv"
        ),
        "background_schedule": sha256_file(schedule_path),
        "truth_schedule": sha256_file(truth_path),
        "link_map": runtime_link_hash,
        "runtime_link_map": runtime_link_hash,
        "frozen_link_map": frozen_link_hash,
        "rdma_wc": sha256_file(rdma_path),
        "fault_application": sha256_file(fault_application_path),
    }
    return {
        "schema_version": "limer.p2-run-semantics.v1",
        "run_id": run_id,
        "mechanism_id": mechanism_id,
        "status": status,
        "source_artifact_sha256": source_hashes["switch"],
        "source_artifacts_sha256": source_hashes,
        "checks": checks,
        "injected_physical_fault": False,
        "target_link_state_during_event": "up" if carrier_up else "unproven",
        "observed_effects": [
            name for name, observed in (
                ("background_rdma_ack_complete", True),
                ("queue_pressure", queue_pressure),
                ("throughput_activity", throughput_active),
                ("common_destination_fan_in", destination >= 0),
                ("single_target_access_link", configuration_ok),
                ("zero_background_rto_retries", no_retry_or_failover),
                ("realized_wave_profile", wave_profile_ok),
            ) if observed
        ],
        "scenario": scenario,
        "fault_family": "",
        "target_link_ids": [target_link_id],
        "scheduled_onset_ns": min(int(row["scheduled_start_ns"]) for row in declared),
        "actual_apply_ns": first_tx,
        "event_end_ns": latest_complete,
        "packet_disposition": "not_applicable",
        "recoverable_error_proxy": False,
        "evidence": {
            "destination_rank": destination,
            "switch_endpoints": endpoints,
            "target_link_id": target_link_id,
            "paired_link_id": paired_link_id,
            "target_host_port": host_ports.get(target_link_id),
            "route_bucket": contract["route_bucket"],
            "transport_contract": {
                "rto_us": contract["rdma_rto_us"],
                "retry_limit": contract["rdma_retry_limit"],
                "max_rto_retry_events": contract["max_rto_retry_events"],
            },
            "background_application": dict(application_evidence),
            "baseline_median_max_queue_bytes": baseline_median,
            "event_peak_max_queue_bytes": event_peak,
        },
    }


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, ensure_ascii=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _write_manifest_seal(directory: Path, manifest: Mapping[str, Any]) -> None:
    manifest_path = directory / "run_manifest.json"
    _write_json_atomic(manifest_path, manifest)
    digest = sha256_file(manifest_path)
    seal = directory / "run_manifest.sha256"
    seal.write_text(f"{digest}  run_manifest.json\n", encoding="ascii")


def _runtime_qualification_source_paths(
    directory: Path, run: Mapping[str, Any]
) -> Dict[str, Path]:
    """Resolve the fixed source set hashed by the runtime qualification."""

    inputs = directory / "inputs"
    collective = run.get("schedule", {}).get("collective_workload_override")
    if isinstance(collective, Mapping):
        workload = inputs / "collective_workload_override.txt"
        roles: Optional[Path] = inputs / "collective_layer_roles.csv"
    else:
        workloads = sorted(inputs.glob("workload*"))
        if len(workloads) != 1 or not workloads[0].is_file():
            raise RunnerError(
                f"runtime qualification requires one frozen workload copy: {workloads}"
            )
        workload = workloads[0]
        roles = None
    paths: Dict[str, Path] = {
        "workload": workload,
        "link_map": directory / "link_map.csv",
        "switch_telemetry": directory / "switch_telemetry.csv",
        "nic_telemetry": directory / "nic_telemetry.csv",
        "collective_transaction": directory / "collective_transaction.csv",
        "collective_telemetry": directory / "collective_telemetry.csv",
        "run_lifecycle": directory / "run_lifecycle.csv",
    }
    if roles is not None:
        paths["collective_layer_roles"] = roles
    return paths


def _run_workload_runtime_qualification(
    *, directory: Path, run: Mapping[str, Any], corpus: ValidatedCorpus,
) -> Dict[str, Any]:
    qualification = corpus.manifest["workload_qualification"]
    collective = run.get("schedule", {}).get("collective_workload_override")
    report = (
        collective["static_validation"]
        if isinstance(collective, Mapping)
        else qualification["static_report"]
    )
    sources = _runtime_qualification_source_paths(directory, run)
    verify_expected_hash(
        sources["link_map"],
        corpus.manifest["input_artifacts"]["link_map"]["sha256"],
        "runtime link_map against frozen corpus input",
    )
    return workload_runtime.validate_runtime_qualification(
        run=run,
        workload_report=report,
        paths=workload_runtime.RuntimePaths(**sources),
        causal_warmup_ns=int(
            qualification.get(
                "causal_warmup_required_ns",
                workload_runtime.DEFAULT_CAUSAL_WARMUP_NS,
            )
        ),
    )


def _route_candidate_validation(
    *,
    directory: Path,
    run_id: str,
    frozen_topology_path: Path,
    frozen_link_map_path: Path,
    expected_link_map_sha256: str,
    expected_topology_sha256: str,
) -> Dict[str, Any]:
    """Rebuild and validate the route install set from immutable raw inputs."""

    return route_candidate_runtime.validate_route_candidate_evidence(
        route_candidates_path=directory / ECMP_ROUTE_CANDIDATES_FILE,
        topology_path=frozen_topology_path,
        frozen_link_map_path=frozen_link_map_path,
        runtime_link_map_path=directory / "link_map.csv",
        expected_run_id=run_id,
        expected_frozen_link_map_sha256=expected_link_map_sha256,
        expected_runtime_link_map_sha256=expected_link_map_sha256,
        expected_topology_sha256=expected_topology_sha256,
    )


def _route_failure_detail(report: Mapping[str, Any]) -> str:
    errors = report.get("errors", [])
    if not isinstance(errors, list):
        return "malformed route-candidate validation errors"
    details = [
        str(item.get("detail", item)) if isinstance(item, Mapping) else str(item)
        for item in errors[:8]
    ]
    return "; ".join(details) or "route-candidate validation did not PASS"


def _training_source_port_validation(
    *, directory: Path, run_id: str,
) -> Dict[str, Any]:
    """Validate the producer sidecar without requiring Q4-style wrap/reuse."""

    return training_source_port_runtime.validate_allocator_evidence(
        directory / TRAINING_SOURCE_PORT_RAW_FILE,
        expected_run_id=run_id,
        require_reuse=False,
    )


def _training_source_port_failure_report(
    *, directory: Path, run_id: str, error: Exception,
) -> Dict[str, Any]:
    raw_path = directory / TRAINING_SOURCE_PORT_RAW_FILE
    artifact: Dict[str, Any] = {
        "path": TRAINING_SOURCE_PORT_RAW_FILE,
        "sha256": None,
        "size_bytes": None,
        "row_count": None,
    }
    if raw_path.is_file() and not raw_path.is_symlink():
        artifact.update(
            {
                "sha256": sha256_file(raw_path),
                "size_bytes": raw_path.stat().st_size,
            }
        )
    report: Dict[str, Any] = {
        "schema_version": training_source_port_runtime.SCHEMA_VERSION,
        "status": "FAIL",
        "run_id": run_id,
        "profile": "GENERAL",
        "requirements": {
            "interval_first": training_source_port_runtime.TRAINING_FIRST,
            "interval_end_exclusive": (
                training_source_port_runtime.BACKGROUND_FIRST
            ),
            "capacity": training_source_port_runtime.CAPACITY,
            "require_reuse": False,
            "maximum_error_count": 0,
        },
        "metrics": {},
        "producer_status": None,
        "artifact": artifact,
        "errors": [str(error)],
    }
    report["report_sha256"] = training_source_port_runtime.canonical_hash(report)
    return report


def _training_source_port_failure_detail(report: Mapping[str, Any]) -> str:
    errors = report.get("errors", [])
    if not isinstance(errors, list):
        return "malformed training source-port allocator errors"
    return "; ".join(str(item) for item in errors[:8]) or (
        "training source-port allocator validation did not PASS"
    )


def _validate_training_source_port_artifact(
    directory: Path,
    manifest: Mapping[str, Any],
    run: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Replay the raw allocator validator and reject a resealed PASS report."""

    raw_path = require_file(
        directory / TRAINING_SOURCE_PORT_RAW_FILE,
        "training source-port allocator evidence",
    )
    report_path = require_file(
        directory / TRAINING_SOURCE_PORT_VALIDATION_FILE,
        "training source-port allocator validation",
    )
    verify_expected_hash(
        raw_path,
        manifest.get("training_source_port_allocator_sha256"),
        "training source-port allocator evidence",
    )
    verify_expected_hash(
        report_path,
        manifest.get("training_source_port_allocator_validation_sha256"),
        "training source-port allocator validation",
    )
    if manifest.get("training_source_port_allocator_validation_status") != "PASS":
        raise RunnerError(
            "run manifest does not record training source-port allocator PASS"
        )
    try:
        recorded = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(
            f"invalid training source-port allocator validation: {exc}"
        ) from exc
    if not isinstance(recorded, Mapping):
        raise RunnerError(
            "training source-port allocator validation must contain one object"
        )
    if recorded.get("report_sha256") != manifest.get(
        "training_source_port_allocator_report_sha256"
    ):
        raise RunnerError(
            "training source-port allocator report identity differs from manifest"
        )
    try:
        recomputed = _training_source_port_validation(
            directory=directory, run_id=str(run["run_id"])
        )
    except training_source_port_runtime.AllocatorEvidenceError as exc:
        raise RunnerError(
            f"training source-port raw-evidence recomputation failed: {exc}"
        ) from exc
    if recomputed.get("status") != "PASS":
        raise RunnerError(
            "training source-port raw-evidence recomputation failed: "
            + _training_source_port_failure_detail(recomputed)
        )
    if dict(recorded) != recomputed:
        raise RunnerError(
            "training source-port allocator validation differs from independent "
            "raw-evidence recomputation"
        )
    artifact = recorded.get("artifact")
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("path") != TRAINING_SOURCE_PORT_RAW_FILE
        or artifact.get("sha256")
        != manifest.get("training_source_port_allocator_sha256")
        or artifact.get("size_bytes") != raw_path.stat().st_size
        or artifact.get("row_count") != 1
    ):
        raise RunnerError(
            "training source-port validation lacks the manifest-bound raw artifact"
        )
    return recorded


def _validate_route_candidate_artifact(
    directory: Path,
    manifest: Mapping[str, Any],
    corpus: ValidatedCorpus,
    run: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Recompute route evidence on reuse; never trust a resealed PASS JSON."""

    raw_path = directory / ECMP_ROUTE_CANDIDATES_FILE
    report_path = directory / ECMP_ROUTE_VALIDATION_FILE
    require_file(raw_path, "ECMP route candidate sidecar")
    require_file(report_path, "ECMP route candidate validation")
    verify_expected_hash(
        raw_path,
        manifest.get("ecmp_route_candidates_sha256"),
        "ECMP route candidate sidecar",
    )
    verify_expected_hash(
        report_path,
        manifest.get("ecmp_route_candidate_validation_sha256"),
        "ECMP route candidate validation",
    )
    if manifest.get("ecmp_route_candidate_validation_status") != "PASS":
        raise RunnerError("run manifest does not record ECMP route validation PASS")
    try:
        recorded = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"invalid ECMP route candidate validation: {exc}") from exc
    if not isinstance(recorded, Mapping):
        raise RunnerError("ECMP route candidate validation must contain one object")
    report_identity = recorded.get("report_sha256")
    if (
        not isinstance(report_identity, str)
        or report_identity != manifest.get("ecmp_route_candidate_report_sha256")
    ):
        raise RunnerError("ECMP route candidate report identity differs from manifest")
    input_bindings = manifest.get("input_bindings")
    link_binding = (
        input_bindings.get("link_map")
        if isinstance(input_bindings, Mapping) else None
    )
    raw_frozen_path = (
        link_binding.get("execution_copy")
        if isinstance(link_binding, Mapping) else None
    )
    if not isinstance(raw_frozen_path, str) or not raw_frozen_path:
        raise RunnerError("run manifest lacks frozen link_map execution binding")
    frozen_relative = Path(raw_frozen_path)
    if frozen_relative.is_absolute() or ".." in frozen_relative.parts:
        raise RunnerError("run manifest has unsafe frozen link_map binding")
    frozen_path = directory / frozen_relative
    topology_binding = (
        input_bindings.get("topology")
        if isinstance(input_bindings, Mapping) else None
    )
    raw_topology_path = (
        topology_binding.get("execution_copy")
        if isinstance(topology_binding, Mapping) else None
    )
    if not isinstance(raw_topology_path, str) or not raw_topology_path:
        raise RunnerError("run manifest lacks frozen topology execution binding")
    topology_relative = Path(raw_topology_path)
    if topology_relative.is_absolute() or ".." in topology_relative.parts:
        raise RunnerError("run manifest has unsafe frozen topology binding")
    frozen_topology_path = directory / topology_relative
    expected_link_hash = corpus.manifest["input_artifacts"]["link_map"]["sha256"]
    expected_topology_hash = corpus.manifest["input_artifacts"]["topology"]["sha256"]
    recomputed = _route_candidate_validation(
        directory=directory,
        run_id=str(run["run_id"]),
        frozen_topology_path=frozen_topology_path,
        frozen_link_map_path=frozen_path,
        expected_link_map_sha256=expected_link_hash,
        expected_topology_sha256=expected_topology_hash,
    )
    if recomputed.get("status") != "PASS":
        raise RunnerError(
            "ECMP route raw-evidence recomputation failed: "
            + _route_failure_detail(recomputed)
        )
    if dict(recorded) != recomputed:
        raise RunnerError(
            "ECMP route validation differs from independent raw-evidence recomputation"
        )
    recorded_artifacts = recorded.get("artifacts")
    raw_reference = (
        recorded_artifacts.get("route_candidates", {})
        if isinstance(recorded_artifacts, Mapping) else {}
    )
    if (
        not isinstance(raw_reference, Mapping)
        or raw_reference.get("sha256")
        != manifest.get("ecmp_route_candidates_sha256")
    ):
        raise RunnerError("ECMP route validation lacks the manifest-bound raw hash")
    return recorded


def _planned_semantic_targets(run: Mapping[str, Any]) -> List[str]:
    targets: set[str] = set()
    primary = run.get("target_link_id")
    if primary not in (None, ""):
        targets.add(str(primary))
    declared = run.get("targets", [])
    if declared is None:
        declared = []
    if not isinstance(declared, list):
        raise RunnerError("planned run targets must be a list")
    for index, item in enumerate(declared):
        if not isinstance(item, Mapping):
            raise RunnerError(f"planned run target {index} is not an object")
        target = item.get("target_link_id")
        if target in (None, ""):
            raise RunnerError(f"planned run target {index} lacks target_link_id")
        targets.add(str(target))
    return sorted(targets)


def _run_non_background_semantic_validation(
    *, directory: Path, run: Mapping[str, Any],
    injection_schedule: Optional[Path],
) -> Dict[str, Any]:
    """Recompute mechanism semantics exclusively from archived raw evidence."""

    class_label = str(run.get("class_label", "")).upper()
    if class_label == "CONGESTION":
        raise RunnerError("background congestion uses its dedicated validator")
    mechanism = run.get("mechanism")
    if not isinstance(mechanism, Mapping):
        raise RunnerError("planned run lacks mechanism contract")
    mechanism_id = str(mechanism.get("mechanism_id", ""))
    if not mechanism_id:
        raise RunnerError("planned run lacks mechanism_id")
    targets = _planned_semantic_targets(run)
    healthy = class_label == "HEALTHY"
    if healthy:
        if injection_schedule is not None or targets:
            raise RunnerError(
                "healthy semantic validation forbids injector and fault targets"
            )
        onset_ns = 0
    else:
        if injection_schedule is None:
            raise RunnerError("fault semantic validation requires frozen injector")
        if not targets:
            raise RunnerError("fault semantic validation requires planned target links")
        onset = run.get("fault_scheduled_onset_ns")
        if isinstance(onset, bool) or not isinstance(onset, int) or onset < 0:
            raise RunnerError(
                "fault semantic validation requires nonnegative planned onset"
            )
        onset_ns = onset
    return mechanism_runtime.validate_run(
        run_dir=directory,
        fault_family="" if healthy else str(run.get("fault_family") or ""),
        mechanism_id=mechanism_id,
        target_link=targets[0] if targets else None,
        target_links=targets,
        scheduled_onset_ns=onset_ns,
        injection_schedule=injection_schedule,
        require_injection_schedule=not healthy,
    )


def _run_collective_override_semantic_validation(
    *, directory: Path, run: Mapping[str, Any]
) -> Dict[str, Any]:
    """Bind congestion semantics to predeclared roles and validated runtime."""

    run_id = str(run["run_id"])
    schedule = run.get("schedule", {})
    collective = schedule.get("collective_workload_override") \
        if isinstance(schedule, Mapping) else None
    if not isinstance(collective, Mapping):
        raise RunnerError(f"{run_id}: collective semantic validator lacks override")
    qualification_path = require_file(
        directory / WORKLOAD_RUNTIME_QUALIFICATION_FILE,
        "collective workload runtime qualification",
    )
    try:
        qualification = json.loads(qualification_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"{run_id}: invalid runtime qualification JSON") from exc
    profile = collective.get("static_validation", {}).get("qualification_profile")
    runtime_timeline = qualification.get("evidence", {}).get(
        "collective_override_pre_event_post_runtime"
    ) if isinstance(qualification, Mapping) else None
    fault_path = require_file(
        directory / "fault_application_telemetry.csv",
        "fault application telemetry",
    )
    try:
        with fault_path.open(encoding="utf-8", newline="") as stream:
            fault_rows = list(csv.DictReader(stream))
    except (OSError, csv.Error) as exc:
        raise RunnerError(f"{run_id}: invalid fault application telemetry") from exc
    checks = [
        {
            "name": "predeclared_collective_profile_is_label_authority",
            "status": "PASS" if (
                schedule.get("independent_of_features") is True
                and schedule.get("generated_before_run") is True
                and profile in {
                    workload_runtime.BURST_PROFILE,
                    workload_runtime.HIGH_UTIL_PROFILE,
                }
            ) else "FAIL",
        },
        {
            "name": "role_bound_application_lifecycle_validated",
            "status": "PASS" if (
                qualification.get("status") == "PASS"
                and qualification.get("qualification_profile") == profile
                and isinstance(runtime_timeline, Mapping)
                and runtime_timeline.get("actual_application_window_authority")
                == "role_bound_collective_transaction"
            ) else "FAIL",
        },
        {
            "name": "no_physical_fault_application",
            "status": "PASS" if not fault_rows else "FAIL",
        },
    ]
    sources = _runtime_qualification_source_paths(directory, run)
    source_hashes = {
        name: sha256_file(path) for name, path in sorted(sources.items())
    }
    source_hashes.update({
        "fault_application": sha256_file(fault_path),
        "workload_runtime_qualification": sha256_file(qualification_path),
    })
    return {
        "schema_version": mechanism_runtime.SCHEMA_VERSION,
        "run_id": run_id,
        "mechanism_id": run["mechanism"]["mechanism_id"],
        "status": (
            "PASS" if all(item["status"] == "PASS" for item in checks) else "FAIL"
        ),
        "source_artifact_sha256": sha256_file(
            directory / "switch_telemetry.csv"
        ),
        "source_artifacts_sha256": source_hashes,
        "checks": checks,
        "injected_physical_fault": False,
        "target_link_state_during_event": "up",
        "observed_effects": ["role_bound_collective_profile"],
        "scenario": run.get("scenario"),
        "evidence": {
            "scenario": run.get("scenario"),
            "qualification_profile": profile,
            "scheduled_application_onset_ns": run.get(
                "fault_scheduled_onset_ns"
            ),
            "application_lifecycle": runtime_timeline,
            "feature_derived_label": False,
        },
    }


def _semantic_setup_failure(
    *, run: Mapping[str, Any], error: Exception,
) -> Dict[str, Any]:
    mechanism = run.get("mechanism", {})
    mechanism_id = (
        str(mechanism.get("mechanism_id", ""))
        if isinstance(mechanism, Mapping) else ""
    )
    return {
        "schema_version": mechanism_runtime.SCHEMA_VERSION,
        "run_id": str(run.get("run_id", "UNKNOWN")),
        "mechanism_id": mechanism_id,
        "status": "FAIL",
        "source_artifact_sha256": "",
        "source_artifacts_sha256": {},
        "checks": [{
            "name": "semantic_validation_setup",
            "status": "FAIL",
            "detail": str(error),
        }],
        "evidence": {"error": str(error)},
    }


def _semantic_report_contract_errors(
    *, report: Mapping[str, Any], run: Mapping[str, Any], switch_path: Path,
) -> List[str]:
    mechanism = run.get("mechanism", {})
    expected = {
        "schema_version": mechanism_runtime.SCHEMA_VERSION,
        "status": "PASS",
        "run_id": run.get("run_id"),
        "mechanism_id": (
            mechanism.get("mechanism_id")
            if isinstance(mechanism, Mapping) else None
        ),
        "source_artifact_sha256": (
            sha256_file(switch_path) if switch_path.is_file() else None
        ),
    }
    errors = [
        f"{field}: expected={value!r}, observed={report.get(field)!r}"
        for field, value in expected.items() if report.get(field) != value
    ]
    checks = report.get("checks")
    if (
        not isinstance(checks, list) or not checks
        or any(
            not isinstance(item, Mapping) or item.get("status") != "PASS"
            for item in checks
        )
    ):
        errors.append("semantic checks are absent or not all PASS")
    return errors


def _validate_runtime_qualification_artifact(
    directory: Path, manifest: Mapping[str, Any], corpus: ValidatedCorpus,
    run: Mapping[str, Any],
) -> Mapping[str, Any]:
    path = require_file(
        directory / WORKLOAD_RUNTIME_QUALIFICATION_FILE,
        "workload runtime qualification",
    )
    expected_hash = manifest.get("workload_runtime_qualification_sha256")
    verify_expected_hash(path, expected_hash, "workload runtime qualification")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"invalid workload runtime qualification: {exc}") from exc
    collective = run.get("schedule", {}).get("collective_workload_override")
    static_report = (
        collective.get("static_validation")
        if isinstance(collective, Mapping)
        else corpus.manifest["workload_qualification"]["static_report"]
    )
    expected = {
        "schema_version": workload_runtime.SCHEMA_VERSION,
        "status": "PASS",
        "run_id": run["run_id"],
        "planned_run_sha256": canonical_hash(run),
        "static_workload_report_sha256": canonical_hash(
            static_report
        ),
    }
    for field, value in expected.items():
        if report.get(field) != value:
            raise RunnerError(
                f"workload runtime qualification {field} mismatch: "
                f"expected {value!r}, got {report.get(field)!r}"
            )
    recorded_sources = report.get("source_artifacts")
    if not isinstance(recorded_sources, Mapping):
        raise RunnerError("workload runtime qualification lacks source hash bindings")
    source_paths = _runtime_qualification_source_paths(directory, run)
    verify_expected_hash(
        source_paths["link_map"],
        corpus.manifest["input_artifacts"]["link_map"]["sha256"],
        "qualified runtime link_map against frozen corpus input",
    )
    if set(recorded_sources) != set(source_paths):
        raise RunnerError("workload runtime qualification source set mismatch")
    for name, source_path in source_paths.items():
        reference = recorded_sources.get(name)
        if not isinstance(reference, Mapping):
            raise RunnerError(f"runtime qualification source {name} is invalid")
        if reference.get("path") != source_path.name:
            raise RunnerError(f"runtime qualification source {name} path mismatch")
        verify_expected_hash(
            source_path, reference.get("sha256"),
            f"runtime qualification source {name}",
        )
    recomputed = _run_workload_runtime_qualification(
        directory=directory, run=run, corpus=corpus
    )
    if recomputed.get("status") != "PASS":
        raise RunnerError(
            "independent workload runtime recomputation failed: "
            f"{recomputed.get('errors', [])[:6]}"
        )
    if dict(report) != dict(recomputed):
        raise RunnerError(
            "stored workload runtime qualification differs from recomputation"
        )
    if manifest.get("workload_runtime_qualification_status") != "PASS":
        raise RunnerError("run manifest does not record runtime qualification PASS")
    return report


def _validate_semantic_artifact(
    directory: Path,
    manifest: Mapping[str, Any],
    run: Mapping[str, Any],
) -> Mapping[str, Any]:
    path = require_file(
        directory / SEMANTIC_VALIDATION_FILE, "semantic validation"
    )
    verify_expected_hash(
        path, manifest.get("semantic_validation_sha256"),
        "semantic validation",
    )
    if manifest.get("semantic_validation_status") != "PASS":
        raise RunnerError("run manifest does not record semantic validation PASS")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"invalid semantic validation: {exc}") from exc
    if not isinstance(report, Mapping):
        raise RunnerError("semantic validation must be a JSON object")
    contract_errors = _semantic_report_contract_errors(
        report=report, run=run,
        switch_path=directory / "switch_telemetry.csv",
    )
    if contract_errors:
        raise RunnerError(
            "semantic validation contract failed: " + "; ".join(contract_errors)
        )

    schedule = run["schedule"]
    background = schedule.get("background_flow_schedule")
    if isinstance(background, Mapping):
        evidence = manifest.get("background_flow_evidence")
        if (
            not isinstance(evidence, Mapping)
            or evidence.get("semantic_validation_status") != "PASS"
            or evidence.get("semantic_validation_sha256")
            != manifest.get("semantic_validation_sha256")
        ):
            raise RunnerError(
                "background evidence does not bind the semantic validation"
            )
        return report

    collective = schedule.get("collective_workload_override")
    if isinstance(collective, Mapping):
        recomputed = _run_collective_override_semantic_validation(
            directory=directory, run=run
        )
        if recomputed.get("status") != "PASS":
            raise RunnerError("collective override semantic recomputation failed")
        if dict(report) != dict(recomputed):
            raise RunnerError(
                "stored collective semantics differ from raw recomputation"
            )
        return report

    injector = schedule.get("simulator_injection_schedule")
    injection_path: Optional[Path] = None
    bindings = manifest.get("schedule_bindings")
    if not isinstance(bindings, Mapping):
        raise RunnerError("run manifest lacks schedule bindings")
    injection_binding = bindings.get("simulator_injection")
    if isinstance(injector, Mapping):
        if not isinstance(injection_binding, Mapping):
            raise RunnerError("fault run lacks archived injector binding")
        value = injection_binding.get("execution_copy")
        if (
            not isinstance(value, str) or Path(value).is_absolute()
            or ".." in Path(value).parts
        ):
            raise RunnerError("fault run injector binding is unsafe")
        injection_path = require_file(
            directory / value, "archived simulator injection schedule"
        )
        if injection_binding.get("sha256") != injector.get("sha256"):
            raise RunnerError("archived injector binding differs from planned run")
        verify_expected_hash(
            injection_path, injector.get("sha256"),
            "archived simulator injection schedule",
        )
    elif injection_binding is not None:
        raise RunnerError("no-injection run unexpectedly binds an injector")

    try:
        recomputed = _run_non_background_semantic_validation(
            directory=directory,
            run=run,
            injection_schedule=injection_path,
        )
    except (RunnerError, KeyError, TypeError, ValueError, OSError) as exc:
        raise RunnerError(
            f"semantic validation cannot be recomputed: {exc}"
        ) from exc
    if recomputed.get("status") != "PASS":
        failed = [
            str(item.get("name")) for item in recomputed.get("checks", [])
            if isinstance(item, Mapping) and item.get("status") != "PASS"
        ]
        raise RunnerError(
            f"raw semantic recomputation failed checks={failed}"
        )
    if report != recomputed:
        raise RunnerError(
            "stored semantic validation differs from raw-evidence recomputation"
        )
    return report


def _acquire_publish_lock(path: Path, run_id: str) -> int:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError as exc:
        raise RunnerError(
            f"{run_id}: concurrent or stale publish lock exists: {path}"
        ) from exc
    os.write(
        descriptor,
        f"pid={os.getpid()} run_id={run_id} acquired_at={utc_now()}\n".encode("ascii"),
    )
    os.fsync(descriptor)
    return descriptor


def _validate_runtime_execution_artifact(
    directory: Path,
    manifest: Mapping[str, Any],
    binding: SimulatorRuntimeBinding,
) -> None:
    if manifest.get("runtime_execution_status") != "PASS":
        raise RunnerError("run manifest does not record runtime execution PASS")
    recorded_runtime = manifest.get("runtime_closure")
    if recorded_runtime != binding.record:
        raise RunnerError(
            "existing final evidence used a different simulator runtime closure"
        )
    path = require_file(
        directory / RUNTIME_EXECUTION_EVIDENCE_FILE,
        "runtime execution evidence",
    )
    verify_expected_hash(
        path,
        manifest.get("runtime_execution_evidence_sha256"),
        "runtime execution evidence",
    )
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RunnerError("invalid runtime execution evidence JSON") from exc
    if not isinstance(evidence, Mapping):
        raise RunnerError("runtime execution evidence must contain one object")
    if evidence.get("schema_version") != "limer.runtime-execution-evidence.v1":
        raise RunnerError("runtime execution evidence schema is invalid")
    if evidence.get("status") != "PASS":
        raise RunnerError("runtime execution evidence is not PASS")
    if evidence.get("runtime_closure") != binding.record:
        raise RunnerError("runtime execution evidence changed runtime closure")
    loader_preflight = evidence.get("loader_preflight")
    loader_project_count = (
        loader_preflight.get("project_dependency_count")
        if isinstance(loader_preflight, Mapping) else None
    )
    if (
        not isinstance(loader_preflight, Mapping)
        or loader_preflight.get("status") != "PASS"
        or loader_preflight.get("runtime_bundle_identity_sha256")
        != binding.identity_sha256
        or isinstance(loader_project_count, bool)
        or not isinstance(loader_project_count, int)
        or loader_project_count != binding.record["project_dependency_count"]
        or loader_project_count <= 0
    ):
        raise RunnerError("runtime loader preflight evidence is invalid")
    pre_run = evidence.get("pre_run_verification")
    process_mapping = evidence.get("process_mapping_verification")
    post_run = evidence.get("post_run_verification")
    loader_environment = evidence.get("loader_environment")
    process_project_count = (
        process_mapping.get("project_dependency_count")
        if isinstance(process_mapping, Mapping) else None
    )
    process_system_count = (
        process_mapping.get("system_dependency_count")
        if isinstance(process_mapping, Mapping) else None
    )
    if not isinstance(pre_run, Mapping) or pre_run.get("status") != "PASS":
        raise RunnerError("runtime pre-run verification is missing or failed")
    if (
        not isinstance(process_mapping, Mapping)
        or process_mapping.get("status") != "PASS"
        or process_mapping.get("runtime_bundle_identity_sha256")
        != binding.identity_sha256
        or isinstance(process_project_count, bool)
        or not isinstance(process_project_count, int)
        or process_project_count != binding.record["project_dependency_count"]
        or process_project_count <= 0
        or isinstance(process_system_count, bool)
        or not isinstance(process_system_count, int)
        or process_system_count != binding.record["system_dependency_count"]
    ):
        raise RunnerError("live process runtime mapping evidence is invalid")
    if not isinstance(post_run, Mapping) or post_run.get("status") != "PASS":
        raise RunnerError("runtime post-run verification is missing or failed")
    if (
        not isinstance(loader_environment, Mapping)
        or loader_environment.get("runtime_bundle_identity_sha256")
        != binding.identity_sha256
        or loader_environment.get("LD_PRELOAD") is not None
        or loader_environment.get("LD_LIBRARY_PATH")
        != str(binding.bundle_root / runtime_bundle.LIB_DIRECTORY_RELATIVE)
    ):
        raise RunnerError("sealed loader environment evidence is invalid")
    direct_bindings = {
        "runtime_loader_preflight": loader_preflight,
        "runtime_pre_run_verification": pre_run,
        "runtime_process_mapping_verification": process_mapping,
        "runtime_post_run_verification": post_run,
    }
    for field, expected in direct_bindings.items():
        if manifest.get(field) != expected:
            raise RunnerError(
                f"run manifest {field} differs from runtime evidence artifact"
            )


def _validate_simulator_stability_artifact(
    directory: Path,
    manifest: Mapping[str, Any],
    run: Mapping[str, Any],
) -> None:
    """Replay the sealed stability result instead of trusting its PASS label."""

    gate_required = _stability_gate_required(run)
    path = directory / SIMULATOR_STABILITY_FILE
    if manifest.get("simulator_stability_gate_required") is not gate_required:
        raise RunnerError("run manifest changed the planned simulator stability gate")
    if not gate_required:
        if (
            manifest.get("simulator_stability_status") != "NOT_REQUIRED"
            or manifest.get("simulator_stability_sha256") is not None
            or path.exists()
        ):
            raise RunnerError(
                "non-gated run has inconsistent simulator stability evidence"
            )
        return
    if manifest.get("simulator_stability_status") != "PASS":
        raise RunnerError("gated run manifest does not record simulator stability PASS")
    require_file(path, "simulator stability evidence")
    verify_expected_hash(
        path,
        manifest.get("simulator_stability_sha256"),
        "simulator stability evidence",
    )
    try:
        recorded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError("invalid simulator stability evidence JSON") from exc
    if not isinstance(recorded, Mapping):
        raise RunnerError("simulator stability evidence must contain one object")
    execution = manifest.get("execution")
    lifecycle = manifest.get("lifecycle")
    closure = manifest.get("runtime_closure")
    if not isinstance(execution, Mapping) or not isinstance(closure, Mapping):
        raise RunnerError("simulator stability source bindings are missing")
    expected = _simulator_stability_report(
        directory,
        run,
        simulator_worker_threads=manifest.get("simulator_worker_threads"),
        runtime_closure_identity_sha256=closure.get("identity_sha256"),
        exit_code=manifest.get("exit_code"),
        process_status=execution.get("process_status"),
        timed_out=execution.get("timed_out"),
        interrupted=execution.get("interrupted"),
        resource_observation=execution.get("resource_observation"),
        execution_status=manifest.get("execution_status"),
        lifecycle=lifecycle,
        workload_completed=manifest.get("workload_completed"),
        runtime_execution_status=manifest.get("runtime_execution_status"),
        semantic_validation_status=manifest.get("semantic_validation_status"),
        workload_runtime_qualification_status=manifest.get(
            "workload_runtime_qualification_status"
        ),
        ecmp_route_candidate_validation_status=manifest.get(
            "ecmp_route_candidate_validation_status"
        ),
        training_source_port_allocator_validation_status=manifest.get(
            "training_source_port_allocator_validation_status"
        ),
    )
    if dict(recorded) != expected:
        raise RunnerError(
            "stored simulator stability differs from raw-evidence recomputation"
        )


def validate_final_run(
    final_dir: Path,
    corpus: ValidatedCorpus,
    run: Mapping[str, Any],
    simulator_worker_threads: Optional[int] = None,
    *,
    runtime_binding: Optional[SimulatorRuntimeBinding] = None,
    runtime_authority: Optional[Any] = None,
) -> Mapping[str, Any]:
    if not final_dir.is_dir():
        raise RunnerError(f"final run path is not a directory: {final_dir}")
    manifest_path = final_dir / "run_manifest.json"
    seal_path = final_dir / "run_manifest.sha256"
    require_file(manifest_path, "run manifest")
    require_file(seal_path, "run manifest seal")
    seal_fields = seal_path.read_text(encoding="ascii").strip().split()
    if len(seal_fields) != 2 or seal_fields[1] != "run_manifest.json":
        raise RunnerError(f"invalid run manifest seal: {seal_path}")
    verify_expected_hash(manifest_path, seal_fields[0], "run manifest seal")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RunnerError(f"invalid run manifest JSON: {manifest_path}") from exc
    expected_fields = {
        "schema_version": RUN_MANIFEST_SCHEMA,
        "status": "EXECUTED",
        "run_id": run["run_id"],
        "corpus_id": corpus.manifest["corpus_id"],
        "corpus_manifest_sha256": corpus.manifest_sha256,
        "planned_run_sha256": canonical_hash(run),
    }
    for field, expected in expected_fields.items():
        if manifest.get(field) != expected:
            raise RunnerError(
                f"existing final evidence has mismatched {field}: "
                f"expected {expected!r}, got {manifest.get(field)!r}"
            )
    authority = runtime_authority or SealedRuntimeAuthority()
    recorded_runtime = manifest.get("runtime_closure")
    if not isinstance(recorded_runtime, Mapping):
        raise RunnerError("existing final evidence lacks a runtime closure binding")
    if runtime_binding is None:
        runtime_binding = authority.load_for_reuse(
            final_dir.parent.parent, recorded_runtime
        )
    if runtime_binding.record != recorded_runtime:
        raise RunnerError(
            "existing final evidence runtime closure differs from selected closure"
        )
    reuse_verification = authority.verify_for_reuse(runtime_binding)
    if reuse_verification.get("status") != "PASS":
        raise RunnerError("sealed runtime reuse verification did not pass")
    simulator = manifest.get("simulator", {})
    if simulator.get("sha256") != corpus.simulator_sha256:
        raise RunnerError("existing final evidence used a different simulator binary")
    if simulator.get("runtime_closure_identity_sha256") != (
        runtime_binding.identity_sha256
    ):
        raise RunnerError("simulator record changed runtime closure identity")
    if simulator.get("runtime_bundle_manifest_sha256") != (
        runtime_binding.manifest_sha256
    ):
        raise RunnerError("simulator record changed runtime bundle manifest")
    if simulator.get("path") != runtime_binding.record["bundle_executable_path"]:
        raise RunnerError("simulator record lacks its sealed bundle executable path")
    if simulator.get("execution_copy") is not None:
        raise RunnerError("simulator executable must not be copied into each run")
    if sha256_file(runtime_binding.executable) != corpus.simulator_sha256:
        raise RunnerError("sealed simulator executable differs from corpus launcher")
    if not os.access(runtime_binding.executable, os.X_OK):
        raise RunnerError("sealed simulator executable is not executable")
    recorded_workers = manifest.get("simulator_worker_threads")
    if (isinstance(recorded_workers, bool)
            or not isinstance(recorded_workers, int)
            or recorded_workers <= 0):
        raise RunnerError(
            "existing final evidence lacks a positive simulator worker-thread count"
        )
    if (simulator_worker_threads is not None
            and recorded_workers != simulator_worker_threads):
        raise RunnerError(
            "existing final evidence used a different simulator worker-thread "
            f"count: expected {simulator_worker_threads}, got {recorded_workers}"
        )
    if simulator.get("worker_threads") != recorded_workers:
        raise RunnerError(
            "existing final evidence has inconsistent simulator worker-thread fields"
        )
    simulator_argv = simulator.get("argv")
    if not isinstance(simulator_argv, list):
        raise RunnerError("existing final evidence lacks simulator argv")
    if not simulator_argv or simulator_argv[0] != str(runtime_binding.executable):
        raise RunnerError(
            "existing final evidence did not execute the sealed bundle binary"
        )
    try:
        thread_flag = simulator_argv.index("-t")
        argv_workers = int(simulator_argv[thread_flag + 1])
    except (ValueError, IndexError, TypeError):
        raise RunnerError("existing final evidence has invalid simulator -t argv")
    if argv_workers != recorded_workers:
        raise RunnerError(
            "existing final evidence simulator argv contradicts worker-thread count"
        )
    schedule = run["schedule"]
    injector = schedule.get("simulator_injection_schedule")
    expected_injection_sha = (
        injector.get("sha256") if isinstance(injector, Mapping) else None
    )
    background = schedule.get("background_flow_schedule")
    expected_background_sha = (
        background.get("sha256") if isinstance(background, Mapping) else None
    )
    collective = schedule.get("collective_workload_override")
    expected_collective_sha = (
        collective.get("sha256") if isinstance(collective, Mapping) else None
    )
    expected_role_sha = (
        collective.get("layer_role_sidecar", {}).get("sha256")
        if isinstance(collective, Mapping) else None
    )
    if manifest.get("schedule_sha256") != schedule.get("sha256"):
        raise RunnerError("existing final evidence used a different truth schedule")
    if manifest.get("injection_schedule_sha256") != expected_injection_sha:
        raise RunnerError("existing final evidence used a different injection schedule")
    if manifest.get("background_flow_schedule_sha256") != expected_background_sha:
        raise RunnerError("existing final evidence used a different background schedule")
    if manifest.get("collective_workload_override_sha256") != expected_collective_sha:
        raise RunnerError("existing final evidence used a different collective override")
    if manifest.get("collective_layer_role_sha256") != expected_role_sha:
        raise RunnerError("existing final evidence used different collective roles")
    expected_effective_sha = run.get("effective_workload_sha256")
    if manifest.get("effective_workload_sha256") != expected_effective_sha:
        raise RunnerError("existing final evidence changed effective workload identity")
    effective = manifest.get("effective_workload_binding")
    inputs = manifest.get("input_bindings")
    if not isinstance(effective, Mapping) or not isinstance(inputs, Mapping):
        raise RunnerError("existing final evidence lacks effective workload binding")
    common_binding = inputs.get("workload")
    if not isinstance(common_binding, Mapping):
        raise RunnerError("existing final evidence lacks common workload binding")
    expected_execution_copy = (
        "inputs/collective_workload_override.txt"
        if expected_collective_sha is not None
        else common_binding.get("execution_copy")
    )
    expected_effective_binding = {
        "source_workload_sha256": corpus.manifest["input_artifacts"]["workload"][
            "sha256"
        ],
        "effective_workload_sha256": expected_effective_sha,
        "execution_copy": expected_execution_copy,
        "collective_override_sha256": expected_collective_sha,
        "collective_layer_role_sha256": expected_role_sha,
        "passed_to_simulator": True,
    }
    if dict(effective) != expected_effective_binding:
        raise RunnerError("effective workload binding differs from planned identity")
    if not isinstance(expected_execution_copy, str):
        raise RunnerError("effective workload execution copy is missing")
    effective_copy = final_dir / expected_execution_copy
    verify_expected_hash(
        effective_copy, expected_effective_sha, "effective workload execution copy"
    )
    schedule_bindings = manifest.get("schedule_bindings")
    if not isinstance(schedule_bindings, Mapping):
        raise RunnerError("existing final evidence lacks schedule bindings")
    if expected_collective_sha is not None:
        override_binding = schedule_bindings.get("collective_workload_override")
        role_binding = schedule_bindings.get("collective_layer_roles")
        if (
            not isinstance(override_binding, Mapping)
            or override_binding.get("execution_copy") != expected_execution_copy
            or override_binding.get("sha256") != expected_collective_sha
            or override_binding.get("passed_to_simulator") is not True
            or not isinstance(role_binding, Mapping)
            or role_binding.get("execution_copy") != "inputs/collective_layer_roles.csv"
            or role_binding.get("sha256") != expected_role_sha
            or role_binding.get("passed_to_simulator") is not False
        ):
            raise RunnerError("collective override execution bindings are invalid")
        verify_expected_hash(
            final_dir / "inputs/collective_layer_roles.csv",
            expected_role_sha,
            "collective layer-role execution copy",
        )
    elif any(
        name in schedule_bindings
        for name in ("collective_workload_override", "collective_layer_roles")
    ):
        raise RunnerError("common-workload run unexpectedly binds collective override")
    try:
        workload_flag = simulator_argv.index("-w")
        argv_workload = simulator_argv[workload_flag + 1]
    except (ValueError, IndexError):
        raise RunnerError("existing final evidence has invalid simulator -w argv")
    if argv_workload != str(Path("..") / expected_execution_copy):
        raise RunnerError("simulator argv did not use the effective workload copy")
    invocation_path = require_file(final_dir / "invocation.json", "invocation")
    try:
        invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError("invalid invocation JSON") from exc
    if (
        not isinstance(invocation, Mapping)
        or invocation.get("argv") != simulator_argv
        or invocation.get("effective_workload_binding") != effective
        or invocation.get("schedule_bindings") != schedule_bindings
    ):
        raise RunnerError("invocation does not bind effective workload/argv")
    if manifest.get("exit_code") != 0 or simulator.get("exit_code") != 0:
        raise RunnerError("existing final evidence did not exit successfully")
    lifecycle = manifest.get("lifecycle", {})
    observation_complete = (
        lifecycle.get("observation_status") == "OBSERVATION_WINDOW_COMPLETE"
    )
    if not observation_complete or manifest.get("execution_status") != "COMPLETE":
        raise RunnerError(
            "final run evidence did not complete the observation horizon"
        )
    workload_completed = lifecycle.get("workload_status") == "WORKLOAD_COMPLETE"
    if manifest.get("workload_completed") is not workload_completed:
        raise RunnerError("run manifest workload_completed contradicts lifecycle evidence")
    if manifest.get("hard_event_detector_enabled") is not False:
        raise RunnerError("P2 run evidence did not disable the hard-event detector")
    if manifest.get("recovery_action_enabled") is not False:
        raise RunnerError("P2 run evidence did not disable recovery actions")
    if manifest.get("rdma_recovery_transport_enabled") is not False:
        raise RunnerError("P2 run evidence enabled RDMA recovery transport")
    background_evidence = manifest.get("background_flow_evidence")
    if expected_background_sha is not None:
        truth_path = resolve_sidecar(
            corpus.root, schedule.get("path"), f"{run['run_id']} truth schedule"
        )
        expected_transport = _background_truth_contract(
            run, _read_schedule_rows(truth_path, f"{run['run_id']} truth schedule")
        )
        if not isinstance(background_evidence, Mapping) or (
            background_evidence.get("status") != "COMPLETE"
            or background_evidence.get("ack_qualified") is not True
            or background_evidence.get("censored_flow_count") != 0
        ):
            raise RunnerError(
                "existing congestion evidence lacks complete ACK-qualified background flows"
            )
        if manifest.get("background_transport_contract") != expected_transport:
            raise RunnerError("existing congestion evidence changed transport contract")
    elif background_evidence is not None:
        raise RunnerError("non-congestion run unexpectedly records background evidence")
    recorded = manifest.get("artifacts")
    if not isinstance(recorded, list):
        raise RunnerError("existing run manifest has no artifact hash inventory")
    lifecycle_artifacts = [
        entry for entry in recorded if entry.get("path") == "run_lifecycle.csv"
    ]
    if len(lifecycle_artifacts) != 1:
        raise RunnerError("run manifest must hash exactly one run_lifecycle.csv")
    current = _artifact_entries(final_dir)
    if recorded != current or manifest.get("artifact_set_sha256") != canonical_hash(current):
        raise RunnerError(
            f"existing final evidence failed artifact integrity validation: {final_dir}"
        )
    _validate_runtime_execution_artifact(
        final_dir, manifest, runtime_binding
    )
    _validate_route_candidate_artifact(final_dir, manifest, corpus, run)
    _validate_training_source_port_artifact(final_dir, manifest, run)
    _validate_runtime_qualification_artifact(final_dir, manifest, corpus, run)
    _validate_semantic_artifact(final_dir, manifest, run)
    _validate_simulator_stability_artifact(final_dir, manifest, run)
    return manifest


def _terminate_process(process: subprocess.Popen[Any]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _read_process_memory_kib(pid: int) -> Dict[str, int]:
    """Read the child's current/peak memory without changing its execution."""
    fields = {
        "VmRSS": "rss_kib",
        "VmHWM": "high_water_rss_kib",
        "VmSize": "virtual_size_kib",
        "VmPeak": "peak_virtual_size_kib",
    }
    observed: Dict[str, int] = {}
    try:
        lines = Path(f"/proc/{pid}/status").read_text(
            encoding="ascii", errors="replace"
        ).splitlines()
    except OSError:
        return observed
    for line in lines:
        name, separator, raw = line.partition(":")
        if not separator or name not in fields:
            continue
        token = raw.strip().split(maxsplit=1)[0]
        try:
            observed[fields[name]] = int(token)
        except ValueError:
            continue
    return observed


def _memory_cgroup_directory() -> Optional[Path]:
    """Find the nearest cgroup-v2 ancestor exposing memory accounting."""
    try:
        membership = Path("/proc/self/cgroup").read_text(
            encoding="ascii", errors="replace"
        )
    except OSError:
        return None
    relative: Optional[str] = None
    for line in membership.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        hierarchy, controllers, path = parts
        if hierarchy == "0" and controllers == "":
            relative = path
            break
    if relative is None:
        return None
    root = Path("/sys/fs/cgroup")
    candidate = root / relative.lstrip("/")
    while True:
        if (candidate / "memory.events").is_file():
            return candidate
        if candidate == root or root not in candidate.parents:
            return None
        candidate = candidate.parent


def _read_cgroup_memory() -> Dict[str, Any]:
    directory = _memory_cgroup_directory()
    if directory is None:
        return {"available": False}

    def integer_file(name: str) -> Optional[int | str]:
        try:
            value = (directory / name).read_text(encoding="ascii").strip()
        except OSError:
            return None
        if value == "max":
            return value
        try:
            return int(value)
        except ValueError:
            return None

    events: Dict[str, int] = {}
    try:
        for line in (directory / "memory.events").read_text(
            encoding="ascii"
        ).splitlines():
            name, value = line.split(maxsplit=1)
            events[name] = int(value)
    except (OSError, ValueError):
        events = {}
    return {
        "available": True,
        "path": str(directory),
        "current_bytes": integer_file("memory.current"),
        "peak_bytes": integer_file("memory.peak"),
        "limit_bytes": integer_file("memory.max"),
        "events": events,
    }


def _wait_with_resource_monitoring(
    process: subprocess.Popen[Any], wall_timeout_s: float,
) -> Tuple[Optional[int], bool, bool, Dict[str, Any]]:
    """Wait for one simulator while retaining timeout, interrupt, and OOM facts."""
    started = time.monotonic()
    timed_out = False
    interrupted = False
    samples = 0
    peak_rss_kib = 0
    peak_virtual_size_kib = 0
    cgroup_before = _read_cgroup_memory()
    try:
        while True:
            memory = _read_process_memory_kib(process.pid)
            if memory:
                samples += 1
                peak_rss_kib = max(
                    peak_rss_kib,
                    memory.get("rss_kib", 0),
                    memory.get("high_water_rss_kib", 0),
                )
                peak_virtual_size_kib = max(
                    peak_virtual_size_kib,
                    memory.get("virtual_size_kib", 0),
                    memory.get("peak_virtual_size_kib", 0),
                )
            exit_code = process.poll()
            if exit_code is not None:
                break
            remaining = wall_timeout_s - (time.monotonic() - started)
            if remaining <= 0:
                timed_out = True
                _terminate_process(process)
                exit_code = process.returncode
                break
            time.sleep(min(0.1, remaining))
    except KeyboardInterrupt:
        interrupted = True
        _terminate_process(process)
        exit_code = process.returncode

    cgroup_after = _read_cgroup_memory()
    before_events = cgroup_before.get("events", {})
    after_events = cgroup_after.get("events", {})
    event_delta = {
        name: int(after_events.get(name, 0)) - int(before_events.get(name, 0))
        for name in sorted(set(before_events) | set(after_events))
    }
    oom_kill_delta = event_delta.get("oom_kill", 0)
    observation = {
        "schema_version": "limer.process-resource-observation.v1",
        "sampling_interval_ms": 100,
        "sample_count": samples,
        "observed_peak_rss_kib": peak_rss_kib or None,
        "observed_peak_virtual_size_kib": peak_virtual_size_kib or None,
        "cgroup_memory_before": cgroup_before,
        "cgroup_memory_after": cgroup_after,
        "cgroup_memory_event_delta": event_delta,
        "oom_kill_observed_during_attempt": oom_kill_delta > 0,
        "exit_signal": -exit_code if exit_code is not None and exit_code < 0 else None,
    }
    return exit_code, timed_out, interrupted, observation


def _preserve_attempt_manifest(
    pending: Path,
    *,
    status: str,
    run: Mapping[str, Any],
    corpus: ValidatedCorpus,
    start_wall: str,
    end_wall: str,
    elapsed_s: float,
    exit_code: Optional[int],
    reason: str,
    simulator_worker_threads: int,
    resource_observation: Optional[Mapping[str, Any]] = None,
) -> None:
    runtime_evidence_path = pending / RUNTIME_EXECUTION_EVIDENCE_FILE
    runtime_evidence: Optional[Mapping[str, Any]] = None
    runtime_evidence_sha256: Optional[str] = None
    if runtime_evidence_path.is_file():
        try:
            loaded = json.loads(runtime_evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RunnerError(
                f"cannot preserve invalid runtime execution evidence: {exc}"
            ) from exc
        if not isinstance(loaded, Mapping):
            raise RunnerError("runtime execution evidence must contain one object")
        runtime_evidence = loaded
        runtime_evidence_sha256 = sha256_file(runtime_evidence_path)
    attempt = {
        "schema_version": "limer.p2-attempt-manifest.v1",
        "status": status,
        "reason": reason,
        "run_id": run["run_id"],
        "corpus_id": corpus.manifest["corpus_id"],
        "corpus_manifest_sha256": corpus.manifest_sha256,
        "planned_run_sha256": canonical_hash(run),
        "simulator_sha256": corpus.simulator_sha256,
        "simulator_worker_threads": simulator_worker_threads,
        "start_wall_time": start_wall,
        "end_wall_time": end_wall,
        "elapsed_wall_seconds": elapsed_s,
        "exit_code": exit_code,
        "resource_observation": resource_observation,
        "runtime_closure": (
            runtime_evidence.get("runtime_closure")
            if runtime_evidence is not None else None
        ),
        "runtime_execution_status": (
            runtime_evidence.get("status")
            if runtime_evidence is not None else None
        ),
        "runtime_loader_preflight": (
            runtime_evidence.get("loader_preflight")
            if runtime_evidence is not None else None
        ),
        "runtime_pre_run_verification": (
            runtime_evidence.get("pre_run_verification")
            if runtime_evidence is not None else None
        ),
        "runtime_process_mapping_verification": (
            runtime_evidence.get("process_mapping_verification")
            if runtime_evidence is not None else None
        ),
        "runtime_post_run_verification": (
            runtime_evidence.get("post_run_verification")
            if runtime_evidence is not None else None
        ),
        "runtime_execution_evidence_sha256": runtime_evidence_sha256,
        "artifacts": _artifact_entries(pending),
    }
    _write_json_atomic(pending / "attempt_manifest.json", attempt)


def execute_one(
    corpus: ValidatedCorpus,
    run: Mapping[str, Any],
    out_root: Path,
    wall_timeout_s: float,
    simulator_worker_threads: int,
    runtime_binding: SimulatorRuntimeBinding,
    runtime_authority: Any,
) -> Dict[str, Any]:
    run_id = run["run_id"]
    stability_gate_required = _stability_gate_required(run)
    runs_root = out_root / "runs"
    pending_root = out_root / ".pending"
    final_dir = runs_root / run_id
    if final_dir.exists():
        manifest = validate_final_run(
            final_dir,
            corpus,
            run,
            simulator_worker_threads,
            runtime_binding=runtime_binding,
            runtime_authority=runtime_authority,
        )
        return {
            "run_id": run_id,
            "status": "REUSED",
            "evidence_state": manifest["evidence_state"],
            "path": str(final_dir),
        }

    runs_root.mkdir(parents=True, exist_ok=True)
    pending_root.mkdir(parents=True, exist_ok=True)
    pending = pending_root / f"{run_id}.pending.{os.getpid()}.{uuid.uuid4().hex}"
    pending.mkdir(mode=0o755)
    (pending / "raw_simai").mkdir()
    (pending / "astra_log").mkdir()
    inputs_dir = pending / "inputs"
    inputs_dir.mkdir()

    input_bindings: Dict[str, Dict[str, Any]] = {}
    for name, source in corpus.input_paths.items():
        expected = corpus.manifest["input_artifacts"][name]["sha256"]
        suffix = source.suffix
        destination = inputs_dir / f"{name}{suffix}"
        _copy_verified(source, destination, expected)
        input_bindings[name] = {
            "source_path": str(source),
            "execution_copy": destination.relative_to(pending).as_posix(),
            "sha256": expected,
        }
    corpus_copy = inputs_dir / "corpus_manifest.json"
    _copy_verified(corpus.manifest_path, corpus_copy, corpus.manifest_sha256)

    schedule = run["schedule"]
    truth_source = resolve_sidecar(
        corpus.root, schedule["path"], f"{run_id} truth schedule"
    )
    truth_copy = inputs_dir / "truth_schedule.csv"
    _copy_verified(truth_source, truth_copy, schedule["sha256"])
    schedule_bindings: Dict[str, Any] = {
        "truth": {
            "source_path": str(truth_source),
            "execution_copy": truth_copy.relative_to(pending).as_posix(),
            "sha256": schedule["sha256"],
            "passed_to_simulator": False,
        }
    }
    injector = schedule.get("simulator_injection_schedule")
    injection_copy: Optional[Path] = None
    if injector is not None:
        injection_source = resolve_sidecar(
            corpus.root,
            injector["path"],
            f"{run_id} simulator injection schedule",
        )
        injection_copy = inputs_dir / "simulator_injection_schedule.csv"
        _copy_verified(injection_source, injection_copy, injector["sha256"])
        schedule_bindings["simulator_injection"] = {
            "source_path": str(injection_source),
            "execution_copy": injection_copy.relative_to(pending).as_posix(),
            "sha256": injector["sha256"],
            "safe_to_execute": True,
            "passed_to_simulator": True,
        }
    background = schedule.get("background_flow_schedule")
    background_copy: Optional[Path] = None
    background_transport_contract: Optional[Dict[str, Any]] = None
    if background is not None:
        if not isinstance(background, Mapping):
            raise RunnerError(f"{run_id}: invalid background-flow schedule reference")
        background_source = resolve_sidecar(
            corpus.root,
            background["path"],
            f"{run_id} background-flow schedule",
        )
        background_copy = inputs_dir / "background_flow_schedule.csv"
        _copy_verified(background_source, background_copy, background["sha256"])
        schedule_bindings["background_flow"] = {
            "source_path": str(background_source),
            "execution_copy": background_copy.relative_to(pending).as_posix(),
            "sha256": background["sha256"],
            "safe_to_execute": True,
            "passed_to_simulator": True,
        }
        background_transport_contract = _background_truth_contract(
            run, _read_schedule_rows(truth_copy, f"{run_id} copied truth schedule")
        )

    collective = schedule.get("collective_workload_override")
    collective_copy: Optional[Path] = None
    collective_role_copy: Optional[Path] = None
    if collective is not None:
        if not isinstance(collective, Mapping):
            raise RunnerError(f"{run_id}: invalid collective override reference")
        role_ref = collective.get("layer_role_sidecar")
        if not isinstance(role_ref, Mapping):
            raise RunnerError(f"{run_id}: collective override lacks role reference")
        collective_source = resolve_sidecar(
            corpus.root,
            collective["path"],
            f"{run_id} collective workload override",
        )
        role_source = resolve_sidecar(
            corpus.root,
            role_ref["path"],
            f"{run_id} collective layer roles",
        )
        collective_copy = inputs_dir / "collective_workload_override.txt"
        collective_role_copy = inputs_dir / "collective_layer_roles.csv"
        _copy_verified(collective_source, collective_copy, collective["sha256"])
        _copy_verified(role_source, collective_role_copy, role_ref["sha256"])
        schedule_bindings["collective_workload_override"] = {
            "source_path": str(collective_source),
            "execution_copy": collective_copy.relative_to(pending).as_posix(),
            "sha256": collective["sha256"],
            "safe_to_execute": True,
            "passed_to_simulator": True,
        }
        schedule_bindings["collective_layer_roles"] = {
            "source_path": str(role_source),
            "execution_copy": collective_role_copy.relative_to(pending).as_posix(),
            "sha256": role_ref["sha256"],
            "safe_to_execute": True,
            "passed_to_simulator": False,
        }

    topology_copy = pending / input_bindings["topology"]["execution_copy"]
    common_workload_copy = pending / input_bindings["workload"]["execution_copy"]
    workload_copy = collective_copy or common_workload_copy
    effective_workload_binding = {
        "source_workload_sha256": corpus.manifest["input_artifacts"]["workload"][
            "sha256"
        ],
        "effective_workload_sha256": sha256_file(workload_copy),
        "execution_copy": workload_copy.relative_to(pending).as_posix(),
        "collective_override_sha256": (
            collective.get("sha256") if isinstance(collective, Mapping) else None
        ),
        "collective_layer_role_sha256": (
            collective.get("layer_role_sidecar", {}).get("sha256")
            if isinstance(collective, Mapping) else None
        ),
        "passed_to_simulator": True,
    }
    if effective_workload_binding["effective_workload_sha256"] != run.get(
        "effective_workload_sha256"
    ):
        raise RunnerError(f"{run_id}: effective workload copy differs from run identity")
    frozen_config_copy = (
        pending / input_bindings["simulator_config"]["execution_copy"]
    )
    config_copy, runtime_config_binding = materialize_runtime_config(
        frozen_config_copy, pending
    )
    command = [
        str(runtime_binding.executable),
        "-t", str(simulator_worker_threads),
        "-w", str(Path("..") / workload_copy.relative_to(pending)),
        "-n", str(Path("..") / topology_copy.relative_to(pending)),
        "-c", str(Path("..") / config_copy.relative_to(pending)),
    ]
    environment_overrides = {
        "ASTRA_SIM_LOG_DIR": str(pending / "astra_log"),
        "LIMER_TELEMETRY_ENABLE": "1",
        "LIMER_TELEMETRY_INTERVAL_US": "1000",
        "LIMER_TELEMETRY_DIR": str(pending),
        "LIMER_RUN_ID": run_id,
        "LIMER_OBSERVATION_STOP_NS": str(run["virtual_finish_ns"]),
        "LIMER_HARD_EVENT_DETECTOR_ENABLE": "0",
        "LIMER_RECOVERY_ACTION_ENABLE": "0",
        "LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE": "0",
        "NS_GLOBAL_VALUE": f"RngSeed=1;RngRun={int(run['simulation_seed'])}",
        "AS_SEND_LAT": "3",
        "AS_NVLS_ENABLE": "1",
        "AS_PXN_ENABLE": "0",
        "AS_LOG_LEVEL": "1",
    }
    if injection_copy is not None:
        environment_overrides["LIMER_FAULT_SCHEDULE"] = str(injection_copy)
    if background_copy is not None:
        environment_overrides["LIMER_BACKGROUND_FLOW_SCHEDULE"] = str(
            background_copy
        )
        if background_transport_contract is None:
            raise RunnerError(f"{run_id}: background transport contract is missing")
        environment_overrides["LIMER_RDMA_RTO_US"] = str(
            background_transport_contract["rdma_rto_us"]
        )
        environment_overrides["LIMER_RDMA_RETRY_LIMIT"] = str(
            background_transport_contract["rdma_retry_limit"]
        )
    base_environment = os.environ.copy()
    # Simulator control variables are evidence inputs, not ambient process
    # configuration.  Remove inherited values before applying the complete,
    # recorded override set so healthy/fault runs cannot be silently polluted.
    inherited_control_names = {
        name for name in base_environment
        if name.startswith("LIMER_") or name in {
            "NS_GLOBAL_VALUE", "AS_SEND_LAT", "AS_NVLS_ENABLE",
            "AS_PXN_ENABLE", "AS_LOG_LEVEL",
        }
    }
    for name in inherited_control_names:
        base_environment.pop(name, None)

    runtime_evidence: Dict[str, Any] = {
        "schema_version": "limer.runtime-execution-evidence.v1",
        "status": "PENDING",
        "run_id": run_id,
        "runtime_closure": dict(runtime_binding.record),
        "loader_preflight": dict(runtime_binding.loader_preflight),
        "pre_run_verification": None,
        "loader_environment": None,
        "process_mapping_verification": None,
        "post_run_verification": None,
        "errors": [],
    }
    try:
        pre_run_verification = runtime_authority.verify_before_run(
            runtime_binding
        )
        if pre_run_verification.get("status") != "PASS":
            raise RunnerError("runtime authority returned non-PASS pre-run evidence")
        environment, loader_environment = runtime_authority.execution_environment(
            runtime_binding,
            base_environment,
            environment_overrides,
        )
        runtime_evidence["pre_run_verification"] = dict(pre_run_verification)
        runtime_evidence["loader_environment"] = dict(loader_environment)
    except RunnerError as exc:
        runtime_evidence["status"] = "FAIL"
        runtime_evidence["errors"] = [str(exc)]
        _write_json_atomic(
            pending / RUNTIME_EXECUTION_EVIDENCE_FILE, runtime_evidence
        )
        now = utc_now()
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=now,
            end_wall=now,
            elapsed_s=0.0,
            exit_code=None,
            reason=f"sealed runtime setup failed before launch: {exc}",
            simulator_worker_threads=simulator_worker_threads,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": str(exc),
        }
    _write_json_atomic(
        pending / RUNTIME_EXECUTION_EVIDENCE_FILE, runtime_evidence
    )
    invocation = {
        "schema_version": "limer.p2-invocation.v1",
        "run_id": run_id,
        "argv": command,
        "cwd": str(pending / "raw_simai"),
        "environment_overrides": environment_overrides,
        "cleared_inherited_control_variables": sorted(inherited_control_names),
        "runtime_closure": dict(runtime_binding.record),
        "loader_preflight": dict(runtime_binding.loader_preflight),
        "runtime_pre_run_verification": dict(pre_run_verification),
        "loader_environment": dict(loader_environment),
        "wall_timeout_seconds": wall_timeout_s,
        "simulator_worker_threads": simulator_worker_threads,
        "input_bindings": input_bindings,
        "effective_workload_binding": effective_workload_binding,
        "runtime_config_binding": runtime_config_binding,
        "schedule_bindings": schedule_bindings,
    }
    _write_json_atomic(pending / "invocation.json", invocation)

    start_wall = utc_now()
    started = time.monotonic()
    process_mapping_error: Optional[str] = None
    runtime_check_interrupted = False
    with (pending / "run.log").open("wb") as log:
        process = subprocess.Popen(
            command,
            cwd=pending / "raw_simai",
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process_mapping = runtime_authority.verify_process(
                runtime_binding, process.pid
            )
            if process_mapping.get("status") != "PASS":
                raise RunnerError(
                    "runtime authority returned non-PASS live mapping evidence"
                )
            runtime_evidence["process_mapping_verification"] = dict(
                process_mapping
            )
        except RunnerError as exc:
            process_mapping_error = str(exc)
            runtime_evidence["process_mapping_verification"] = {
                "status": "FAIL",
                "pid": process.pid,
                "runtime_bundle_identity_sha256": (
                    runtime_binding.identity_sha256
                ),
                "error": process_mapping_error,
            }
            _terminate_process(process)
        except KeyboardInterrupt:
            runtime_check_interrupted = True
            runtime_evidence["process_mapping_verification"] = {
                "status": "INTERRUPTED",
                "pid": process.pid,
                "runtime_bundle_identity_sha256": (
                    runtime_binding.identity_sha256
                ),
                "error": "operator interrupt during live process mapping check",
            }
            _terminate_process(process)
        exit_code, timed_out, interrupted, resource_observation = (
            _wait_with_resource_monitoring(process, wall_timeout_s)
        )
        interrupted = interrupted or runtime_check_interrupted
    elapsed = time.monotonic() - started
    end_wall = utc_now()
    (pending / "exit_code.txt").write_text(
        "" if exit_code is None else f"{exit_code}\n", encoding="ascii"
    )

    post_run_error: Optional[str] = None
    try:
        post_run_verification = runtime_authority.verify_after_run(
            runtime_binding
        )
        if post_run_verification.get("status") != "PASS":
            raise RunnerError("runtime authority returned non-PASS post-run evidence")
        runtime_evidence["post_run_verification"] = dict(post_run_verification)
    except RunnerError as exc:
        post_run_error = str(exc)
        runtime_evidence["post_run_verification"] = {
            "status": "FAIL",
            "runtime_bundle_identity_sha256": runtime_binding.identity_sha256,
            "error": post_run_error,
        }
    except KeyboardInterrupt:
        interrupted = True
        runtime_evidence["post_run_verification"] = {
            "status": "INTERRUPTED",
            "runtime_bundle_identity_sha256": runtime_binding.identity_sha256,
            "error": "operator interrupt during post-run runtime verification",
        }
    runtime_errors = [
        item for item in (process_mapping_error, post_run_error) if item is not None
    ]
    runtime_evidence["errors"] = runtime_errors
    runtime_evidence["status"] = (
        "INTERRUPTED" if interrupted
        else "FAIL" if runtime_errors
        else "PASS"
    )
    _write_json_atomic(
        pending / RUNTIME_EXECUTION_EVIDENCE_FILE, runtime_evidence
    )

    if runtime_errors and not interrupted:
        reason = "sealed simulator runtime verification failed: " + "; ".join(
            runtime_errors
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "exit_code": exit_code,
            "error": reason,
        }

    if interrupted:
        _preserve_attempt_manifest(
            pending,
            status="INTERRUPTED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason="runner received an operator interrupt and terminated the simulator process group",
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTERRUPTED",
            "path": str(pending),
            "exit_code": exit_code,
        }
    if timed_out:
        _preserve_attempt_manifest(
            pending,
            status="TIMED_OUT",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=f"wall timeout after {wall_timeout_s} seconds",
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "TIMED_OUT",
            "path": str(pending),
            "exit_code": exit_code,
        }
    if exit_code != 0:
        _preserve_attempt_manifest(
            pending,
            status="PROCESS_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=(
                "simulator was killed while the enclosing cgroup recorded an "
                "OOM kill"
                if resource_observation.get(
                    "oom_kill_observed_during_attempt"
                ) and exit_code == -signal.SIGKILL
                else "simulator exited non-zero"
            ),
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "PROCESS_FAILED",
            "path": str(pending),
            "exit_code": exit_code,
        }
    if resource_observation.get("oom_kill_observed_during_attempt") is not False:
        reason = (
            "successful simulator exit cannot be accepted because the enclosing "
            "cgroup recorded an OOM kill during the attempt"
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "exit_code": exit_code,
            "error": reason,
        }

    # The simulator used the copied inputs, but publication is refused if the
    # original corpus or executable changed while the process was running.
    if sha256_file(corpus.manifest_path) != corpus.manifest_sha256:
        raise RunnerError(f"{run_id}: corpus manifest changed during execution")
    if sha256_file(corpus.simulator_binary) != corpus.simulator_sha256:
        raise RunnerError(f"{run_id}: simulator binary changed during execution")

    lifecycle = read_lifecycle(
        pending / "run_lifecycle.csv", run_id, int(run["virtual_finish_ns"])
    )
    if lifecycle["parse_status"] != "PARSED":
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=(
                "successful simulator process did not produce a valid "
                f"run_lifecycle.csv: {lifecycle['parse_status']}"
            ),
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": "missing or invalid run_lifecycle.csv",
        }
    if lifecycle["observation_status"] != "OBSERVATION_WINDOW_COMPLETE":
        _preserve_attempt_manifest(
            pending,
            status="CENSORED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=(
                "simulator exited zero before the predeclared observation "
                "horizon; evidence remains pending and may not be reused as "
                "a completed P2 run"
            ),
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "CENSORED",
            "path": str(pending),
            "lifecycle": lifecycle,
        }
    try:
        allocator_validation = _training_source_port_validation(
            directory=pending, run_id=run_id
        )
    except training_source_port_runtime.AllocatorEvidenceError as exc:
        allocator_validation = _training_source_port_failure_report(
            directory=pending, run_id=run_id, error=exc
        )
    _write_json_atomic(
        pending / TRAINING_SOURCE_PORT_VALIDATION_FILE,
        allocator_validation,
    )
    if allocator_validation.get("status") != "PASS":
        reason = (
            "strict training source-port allocator validation failed: "
            + _training_source_port_failure_detail(allocator_validation)
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": reason,
        }
    frozen_link_map_copy = pending / input_bindings["link_map"]["execution_copy"]
    route_validation = _route_candidate_validation(
        directory=pending,
        run_id=run_id,
        frozen_topology_path=(
            pending / input_bindings["topology"]["execution_copy"]
        ),
        frozen_link_map_path=frozen_link_map_copy,
        expected_link_map_sha256=corpus.manifest["input_artifacts"]["link_map"][
            "sha256"
        ],
        expected_topology_sha256=corpus.manifest["input_artifacts"]["topology"][
            "sha256"
        ],
    )
    _write_json_atomic(
        pending / ECMP_ROUTE_VALIDATION_FILE,
        route_validation,
    )
    if route_validation.get("status") != "PASS":
        reason = (
            "strict ECMP route candidate validation failed: "
            + _route_failure_detail(route_validation)
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": reason,
        }
    runtime_qualification: Dict[str, Any]
    try:
        runtime_qualification = _run_workload_runtime_qualification(
            directory=pending, run=run, corpus=corpus,
        )
    except (RunnerError, KeyError, TypeError, ValueError) as exc:
        runtime_qualification = {
            "schema_version": workload_runtime.SCHEMA_VERSION,
            "status": "FAIL",
            "run_id": run_id,
            "planned_run_sha256": canonical_hash(run),
            "static_workload_report_sha256": canonical_hash(
                collective.get("static_validation", {})
                if isinstance(collective, Mapping)
                else corpus.manifest.get("workload_qualification", {}).get(
                    "static_report", {}
                )
            ),
            "source_artifacts": {},
            "checks": [{
                "name": "runtime_qualification_setup",
                "status": "FAIL",
                "error": str(exc),
            }],
            "errors": [str(exc)],
        }
    _write_json_atomic(
        pending / WORKLOAD_RUNTIME_QUALIFICATION_FILE,
        runtime_qualification,
    )
    if runtime_qualification.get("status") != "PASS":
        errors = runtime_qualification.get("errors", [])
        reason = (
            "strict workload runtime qualification failed: "
            + "; ".join(str(item) for item in errors[:8])
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": reason,
        }
    background_evidence: Optional[Dict[str, Any]] = None
    semantic_report: Optional[Dict[str, Any]] = None
    if background_copy is not None:
        try:
            background_evidence = validate_background_application(
                pending / "background_flow_application.csv",
                background_copy,
                run_id,
                int(run["virtual_finish_ns"]),
            )
            background_semantic = validate_background_congestion_signal(
                run=run,
                application_evidence=background_evidence,
                truth_path=truth_copy,
                schedule_path=background_copy,
                switch_path=pending / "switch_telemetry.csv",
                runtime_link_map_path=pending / "link_map.csv",
                frozen_link_map_path=(
                    pending / input_bindings["link_map"]["execution_copy"]
                ),
                rdma_path=pending / "rdma_wc_telemetry.csv",
                fault_application_path=pending / "fault_application_telemetry.csv",
            )
            _write_json_atomic(
                pending / SEMANTIC_VALIDATION_FILE, background_semantic
            )
            if background_semantic["status"] != "PASS":
                failed_checks = [
                    item["name"] for item in background_semantic["checks"]
                    if item["status"] != "PASS"
                ]
                raise RunnerError(
                    f"background congestion semantics failed checks={failed_checks}"
                )
            background_evidence = {
                **background_evidence,
                "semantic_validation_status": "PASS",
                "semantic_validation_sha256": sha256_file(
                    pending / SEMANTIC_VALIDATION_FILE
                ),
            }
            semantic_report = background_semantic
        except RunnerError as exc:
            _preserve_attempt_manifest(
                pending,
                status="INTEGRITY_FAILED",
                run=run,
                corpus=corpus,
                start_wall=start_wall,
                end_wall=end_wall,
                elapsed_s=elapsed,
                exit_code=exit_code,
                reason=f"background-flow evidence failed validation: {exc}",
                simulator_worker_threads=simulator_worker_threads,
                resource_observation=resource_observation,
            )
            return {
                "run_id": run_id,
                "status": "INTEGRITY_FAILED",
                "path": str(pending),
                "error": str(exc),
            }
    else:
        try:
            if collective_copy is not None:
                semantic_report = _run_collective_override_semantic_validation(
                    directory=pending, run=run
                )
            else:
                semantic_report = _run_non_background_semantic_validation(
                    directory=pending,
                    run=run,
                    injection_schedule=injection_copy,
                )
        except (RunnerError, KeyError, TypeError, ValueError, OSError) as exc:
            semantic_report = _semantic_setup_failure(run=run, error=exc)
        _write_json_atomic(
            pending / SEMANTIC_VALIDATION_FILE, semantic_report
        )
        if semantic_report.get("status") != "PASS":
            failed_checks = [
                str(item.get("name"))
                for item in semantic_report.get("checks", [])
                if isinstance(item, Mapping) and item.get("status") != "PASS"
            ]
            reason = (
                "strict non-background mechanism semantics failed: "
                f"checks={failed_checks}"
            )
            _preserve_attempt_manifest(
                pending,
                status="INTEGRITY_FAILED",
                run=run,
                corpus=corpus,
                start_wall=start_wall,
                end_wall=end_wall,
                elapsed_s=elapsed,
                exit_code=exit_code,
                reason=reason,
                simulator_worker_threads=simulator_worker_threads,
                resource_observation=resource_observation,
            )
            return {
                "run_id": run_id,
                "status": "INTEGRITY_FAILED",
                "path": str(pending),
                "error": reason,
            }
    if semantic_report is None:
        raise RunnerError(f"{run_id}: semantic validation was not produced")
    semantic_contract_errors = _semantic_report_contract_errors(
        report=semantic_report,
        run=run,
        switch_path=pending / "switch_telemetry.csv",
    )
    if semantic_contract_errors:
        reason = (
            "semantic validation contract failed before publication: "
            + "; ".join(semantic_contract_errors)
        )
        _preserve_attempt_manifest(
            pending,
            status="INTEGRITY_FAILED",
            run=run,
            corpus=corpus,
            start_wall=start_wall,
            end_wall=end_wall,
            elapsed_s=elapsed,
            exit_code=exit_code,
            reason=reason,
            simulator_worker_threads=simulator_worker_threads,
            resource_observation=resource_observation,
        )
        return {
            "run_id": run_id,
            "status": "INTEGRITY_FAILED",
            "path": str(pending),
            "error": reason,
        }
    evidence_state = (
        "OBSERVATION_WINDOW_COMPLETE"
        if lifecycle["observation_status"] == "OBSERVATION_WINDOW_COMPLETE"
        else "OBSERVATION_WINDOW_CENSORED"
    )
    execution_status = (
        "COMPLETE" if evidence_state == "OBSERVATION_WINDOW_COMPLETE" else "CENSORED"
    )
    workload_completed = lifecycle["workload_status"] == "WORKLOAD_COMPLETE"
    lifecycle_status = lifecycle.get(
        "observation_detail", lifecycle["observation_status"]
    )
    stability_report: Optional[Dict[str, Any]] = None
    if stability_gate_required:
        stability_report = _simulator_stability_report(
            pending,
            run,
            simulator_worker_threads=simulator_worker_threads,
            runtime_closure_identity_sha256=runtime_binding.identity_sha256,
            exit_code=exit_code,
            process_status="EXITED_ZERO",
            timed_out=False,
            interrupted=False,
            resource_observation=resource_observation,
            execution_status=execution_status,
            lifecycle=lifecycle,
            workload_completed=workload_completed,
            runtime_execution_status="PASS",
            semantic_validation_status="PASS",
            workload_runtime_qualification_status="PASS",
            ecmp_route_candidate_validation_status="PASS",
            training_source_port_allocator_validation_status="PASS",
        )
        _write_json_atomic(pending / SIMULATOR_STABILITY_FILE, stability_report)
    artifacts = _artifact_entries(pending)
    run_manifest = {
        "schema_version": RUN_MANIFEST_SCHEMA,
        "status": "EXECUTED",
        # COMPLETE means that the requested observation horizon was reached.
        # It deliberately says nothing about training or collective success.
        "execution_status": execution_status,
        "evidence_state": evidence_state,
        "lifecycle_status": lifecycle_status,
        "workload_completed": workload_completed,
        "run_id": run_id,
        "corpus_id": corpus.manifest["corpus_id"],
        "corpus_manifest_path": str(corpus.manifest_path),
        "corpus_manifest_sha256": corpus.manifest_sha256,
        "schedule_sha256": schedule["sha256"],
        "injection_schedule_sha256": (
            injector.get("sha256") if isinstance(injector, Mapping) else None
        ),
        "background_flow_schedule_sha256": (
            background.get("sha256") if isinstance(background, Mapping) else None
        ),
        "collective_workload_override_sha256": (
            collective.get("sha256") if isinstance(collective, Mapping) else None
        ),
        "collective_layer_role_sha256": (
            collective.get("layer_role_sidecar", {}).get("sha256")
            if isinstance(collective, Mapping) else None
        ),
        "effective_workload_sha256": run.get("effective_workload_sha256"),
        "planned_run_sha256": canonical_hash(run),
        "partition": run["partition"],
        "class_label": run.get("class_label"),
        "simulation_seed": run["simulation_seed"],
        "virtual_finish_ns": run["virtual_finish_ns"],
        "hard_event_detector_enabled": False,
        "recovery_action_enabled": False,
        "rdma_recovery_transport_enabled": False,
        "background_transport_contract": background_transport_contract,
        "telemetry_interval_us": 1000,
        "world_size": 16,
        "simulator_worker_threads": simulator_worker_threads,
        "exit_code": exit_code,
        "input_bindings": input_bindings,
        "effective_workload_binding": effective_workload_binding,
        "runtime_config_binding": runtime_config_binding,
        "schedule_bindings": schedule_bindings,
        "runtime_closure": dict(runtime_binding.record),
        "runtime_execution_status": "PASS",
        "runtime_loader_preflight": dict(runtime_binding.loader_preflight),
        "runtime_pre_run_verification": runtime_evidence[
            "pre_run_verification"
        ],
        "runtime_process_mapping_verification": runtime_evidence[
            "process_mapping_verification"
        ],
        "runtime_post_run_verification": runtime_evidence[
            "post_run_verification"
        ],
        "runtime_execution_evidence_sha256": sha256_file(
            pending / RUNTIME_EXECUTION_EVIDENCE_FILE
        ),
        "simulator_stability_gate_required": stability_gate_required,
        "simulator_stability_status": (
            "PASS" if stability_gate_required else "NOT_REQUIRED"
        ),
        "simulator_stability_sha256": (
            sha256_file(pending / SIMULATOR_STABILITY_FILE)
            if stability_report is not None else None
        ),
        "simulator": {
            "path": runtime_binding.record["bundle_executable_path"],
            "source_path": str(corpus.simulator_binary),
            "execution_copy": None,
            "sha256": corpus.simulator_sha256,
            "runtime_closure_identity_sha256": (
                runtime_binding.identity_sha256
            ),
            "runtime_bundle_manifest_sha256": (
                runtime_binding.manifest_sha256
            ),
            "argv": command,
            "exit_code": exit_code,
            "wall_timeout_seconds": wall_timeout_s,
            "worker_threads": simulator_worker_threads,
        },
        "execution": {
            "start_wall_time": start_wall,
            "end_wall_time": end_wall,
            "elapsed_wall_seconds": elapsed,
            "process_status": "EXITED_ZERO",
            "timed_out": False,
            "interrupted": False,
            "resource_observation": resource_observation,
        },
        "lifecycle": lifecycle,
        "background_flow_evidence": background_evidence,
        "semantic_validation_status": "PASS",
        "semantic_validation_sha256": sha256_file(
            pending / SEMANTIC_VALIDATION_FILE
        ),
        "ecmp_route_candidate_validation_status": "PASS",
        "ecmp_route_candidates_sha256": sha256_file(
            pending / ECMP_ROUTE_CANDIDATES_FILE
        ),
        "ecmp_route_candidate_validation_sha256": sha256_file(
            pending / ECMP_ROUTE_VALIDATION_FILE
        ),
        "ecmp_route_candidate_report_sha256": route_validation[
            "report_sha256"
        ],
        "training_source_port_allocator_validation_status": "PASS",
        "training_source_port_allocator_sha256": sha256_file(
            pending / TRAINING_SOURCE_PORT_RAW_FILE
        ),
        "training_source_port_allocator_validation_sha256": sha256_file(
            pending / TRAINING_SOURCE_PORT_VALIDATION_FILE
        ),
        "training_source_port_allocator_report_sha256": allocator_validation[
            "report_sha256"
        ],
        "workload_runtime_qualification_status": "PASS",
        "workload_runtime_qualification_sha256": sha256_file(
            pending / WORKLOAD_RUNTIME_QUALIFICATION_FILE
        ),
        "artifacts": artifacts,
        "artifact_set_sha256": canonical_hash(artifacts),
        "publish_protocol": "pending_directory_then_atomic_rename",
    }
    _write_manifest_seal(pending, run_manifest)
    # Recompute every sealed validator while the attempt is still unpublished.
    # A failure here leaves only diagnostic pending evidence and can never make
    # a COMPLETE/PASS directory visible under runs/.
    validate_final_run(
        pending,
        corpus,
        run,
        simulator_worker_threads,
        runtime_binding=runtime_binding,
        runtime_authority=runtime_authority,
    )
    publish_lock = runs_root / f".{run_id}.publish.lock"
    lock_descriptor = _acquire_publish_lock(publish_lock, run_id)
    try:
        if final_dir.exists():
            raise RunnerError(
                f"{run_id}: final evidence appeared concurrently; "
                f"pending attempt preserved: {pending}"
            )
        pending.rename(final_dir)
    finally:
        os.close(lock_descriptor)
        publish_lock.unlink(missing_ok=True)
    validate_final_run(
        final_dir,
        corpus,
        run,
        simulator_worker_threads,
        runtime_binding=runtime_binding,
        runtime_authority=runtime_authority,
    )
    return {
        "run_id": run_id,
        "status": "EXECUTED",
        "evidence_state": evidence_state,
        "workload_status": lifecycle["workload_status"],
        "path": str(final_dir),
    }


def _summary_counts(results: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts = {
        "selected": 0,
        "executable": 0,
        "executed": 0,
        "reused": 0,
        "blocked": 0,
        "censored": 0,
        "timed_out": 0,
        "interrupted": 0,
        "failed": 0,
    }
    for result in results:
        counts["selected"] += 1
        status = str(result["status"])
        if status in {"EXECUTED", "REUSED", "DRY_RUN_EXECUTABLE"}:
            counts["executable"] += 1
        if status == "EXECUTED":
            counts["executed"] += 1
        elif status == "REUSED":
            counts["reused"] += 1
        elif status.startswith("BLOCKED_"):
            counts["blocked"] += 1
        elif status == "CENSORED":
            counts["censored"] += 1
        elif status == "TIMED_OUT":
            counts["timed_out"] += 1
        elif status == "INTERRUPTED":
            counts["interrupted"] += 1
        elif status in {"PROCESS_FAILED", "INTEGRITY_FAILED"}:
            counts["failed"] += 1
    return counts


def _attach_runtime_result_evidence(result: Dict[str, Any]) -> None:
    raw_path = result.get("path")
    if not isinstance(raw_path, str):
        return
    evidence_path = Path(raw_path) / RUNTIME_EXECUTION_EVIDENCE_FILE
    if not evidence_path.is_file():
        if result.get("status") in {"EXECUTED", "REUSED"}:
            raise RunnerError(
                "completed result lacks runtime execution evidence: "
                f"{evidence_path}"
            )
        return
    try:
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(
            f"cannot summarize runtime execution evidence: {evidence_path}: {exc}"
        ) from exc
    if not isinstance(evidence, Mapping):
        raise RunnerError("runtime execution evidence summary source is not an object")
    loader = evidence.get("loader_preflight")
    process_mapping = evidence.get("process_mapping_verification")
    post_run = evidence.get("post_run_verification")
    result["runtime_execution_evidence"] = {
        "path": str(evidence_path),
        "sha256": sha256_file(evidence_path),
        "status": evidence.get("status"),
        "loader_preflight_status": (
            loader.get("status") if isinstance(loader, Mapping) else None
        ),
        "process_mapping_status": (
            process_mapping.get("status")
            if isinstance(process_mapping, Mapping) else None
        ),
        "process_project_dependency_count": (
            process_mapping.get("project_dependency_count")
            if isinstance(process_mapping, Mapping) else None
        ),
        "process_system_dependency_count": (
            process_mapping.get("system_dependency_count")
            if isinstance(process_mapping, Mapping) else None
        ),
        "post_run_status": (
            post_run.get("status") if isinstance(post_run, Mapping) else None
        ),
    }


def run_corpus(
    *,
    corpus_manifest: Path,
    simulator_binary: Path,
    out_root: Path,
    requested_run_ids: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    wall_timeout_s: float = 900.0,
    simulator_worker_threads: int = 1,
    dry_run: bool = False,
    _runtime_authority: Optional[Any] = None,
) -> Dict[str, Any]:
    if wall_timeout_s <= 0:
        raise RunnerError("--wall-timeout-s must be positive")
    if (isinstance(simulator_worker_threads, bool)
            or not isinstance(simulator_worker_threads, int)
            or not 1 <= simulator_worker_threads <= 256):
        raise RunnerError("--simulator-worker-threads must be in [1, 256]")
    corpus = validate_corpus(corpus_manifest, simulator_binary)
    selected = select_runs(corpus, requested_run_ids, limit)
    runtime_authority = _runtime_authority or SealedRuntimeAuthority()
    runtime_binding: Optional[SimulatorRuntimeBinding] = None
    has_executable_run = any(
        classify_run(run)[0] == "EXECUTABLE" for run in selected
    )
    if not dry_run and has_executable_run:
        runtime_binding = runtime_authority.prepare(
            out_root.resolve(), corpus.simulator_binary
        )
        if (
            not isinstance(runtime_binding.identity_sha256, str)
            or not SHA256_RE.fullmatch(runtime_binding.identity_sha256)
        ):
            raise RunnerError("runtime authority returned an invalid closure identity")
    results: List[Dict[str, Any]] = []
    started_at = utc_now()
    progress_path: Optional[Path] = None
    progress_id = f"{os.getpid()}.{uuid.uuid4().hex}"

    def write_progress(status: str, current_run_id: Optional[str]) -> None:
        nonlocal progress_path
        if dry_run:
            return
        progress_dir = out_root.resolve() / "execution_progress"
        progress_path = progress_dir / f"{progress_id}.json"
        _write_json_atomic(
            progress_path,
            {
                "schema_version": "limer.p2-execution-progress.v1",
                "status": status,
                "started_at": started_at,
                "updated_at": utc_now(),
                "corpus_id": corpus.manifest["corpus_id"],
                "corpus_manifest_sha256": corpus.manifest_sha256,
                "simulator_sha256": corpus.simulator_sha256,
                "runtime_closure": (
                    dict(runtime_binding.record)
                    if runtime_binding is not None else None
                ),
                "runtime_loader_preflight": (
                    dict(runtime_binding.loader_preflight)
                    if runtime_binding is not None else None
                ),
                "simulator_worker_threads": simulator_worker_threads,
                "selected_run_ids": [run["run_id"] for run in selected],
                "current_run_id": current_run_id,
                "results": results,
                "counts": _summary_counts(results),
            },
        )

    write_progress("RUNNING", None)
    for run in selected:
        write_progress("RUNNING", str(run["run_id"]))
        disposition, reason = classify_run(run)
        if disposition == "BLOCKED":
            results.append(
                {
                    "run_id": run["run_id"],
                    "status": reason,
                    "executed": False,
                    "mechanism_id": run["mechanism"].get("mechanism_id"),
                }
            )
            write_progress("RUNNING", None)
            continue
        if dry_run:
            results.append(
                {
                    "run_id": run["run_id"],
                    "status": "DRY_RUN_EXECUTABLE",
                    "execution_mode": reason,
                    "executed": False,
                }
            )
            continue
        try:
            if runtime_binding is None:
                raise RunnerError(
                    "executable run selected without a sealed runtime bundle"
                )
            result = execute_one(
                corpus,
                run,
                out_root.resolve(),
                wall_timeout_s,
                simulator_worker_threads,
                runtime_binding,
                runtime_authority,
            )
            _attach_runtime_result_evidence(result)
            result.setdefault(
                "runtime_closure_identity_sha256",
                runtime_binding.identity_sha256,
            )
            results.append(result)
            write_progress(
                "INTERRUPTED" if result["status"] == "INTERRUPTED" else "RUNNING",
                None,
            )
            if result["status"] == "INTERRUPTED":
                break
        except RunnerError as exc:
            failure = {
                "run_id": run["run_id"],
                "status": "INTEGRITY_FAILED",
                "error": str(exc),
            }
            if runtime_binding is not None:
                failure["runtime_closure_identity_sha256"] = (
                    runtime_binding.identity_sha256
                )
            results.append(failure)
            write_progress("FAILED", None)
            break
    summary = {
        "schema_version": EXECUTION_SUMMARY_SCHEMA,
        "created_at": utc_now(),
        "started_at": started_at,
        "dry_run": dry_run,
        "corpus_id": corpus.manifest["corpus_id"],
        "corpus_manifest": str(corpus.manifest_path),
        "corpus_manifest_sha256": corpus.manifest_sha256,
        "simulator_binary": str(corpus.simulator_binary),
        "simulator_sha256": corpus.simulator_sha256,
        "runtime_bundle_sealed": runtime_binding is not None,
        "runtime_closure": (
            dict(runtime_binding.record) if runtime_binding is not None else None
        ),
        "runtime_loader_preflight": (
            dict(runtime_binding.loader_preflight)
            if runtime_binding is not None else None
        ),
        "wall_timeout_seconds": wall_timeout_s,
        "simulator_worker_threads": simulator_worker_threads,
        "results": results,
        "counts": _summary_counts(results),
    }
    if not dry_run:
        summaries = out_root.resolve() / "execution_summaries"
        name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        path = summaries / f"{name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.json"
        _write_json_atomic(path, summary)
        summary["summary_path"] = str(path)
        summary["progress_path"] = str(progress_path) if progress_path else None
        final_progress_status = (
            "INTERRUPTED" if summary["counts"]["interrupted"]
            else "FAILED" if (
                summary["counts"]["failed"]
                or summary["counts"]["timed_out"]
                or summary["counts"]["censored"]
                or summary["counts"]["blocked"]
            )
            else "COMPLETE"
        )
        write_progress(final_progress_status, None)
    return summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-manifest", required=True, type=Path)
    parser.add_argument("--simulator-binary", required=True, type=Path)
    parser.add_argument("--out-root", required=True, type=Path)
    parser.add_argument(
        "--run-id", action="append", default=[],
        help="exact run_id to select; repeat for multiple runs",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--wall-timeout-s", type=float, default=900.0)
    parser.add_argument(
        "--simulator-worker-threads",
        type=int,
        default=1,
        help=(
            "ns-3 MTP worker threads (not GPU count); true-16 GPU identity is "
            "validated independently from the frozen topology/workload"
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_corpus(
            corpus_manifest=args.corpus_manifest,
            simulator_binary=args.simulator_binary,
            out_root=args.out_root,
            requested_run_ids=args.run_id,
            limit=args.limit,
            wall_timeout_s=args.wall_timeout_s,
            simulator_worker_threads=args.simulator_worker_threads,
            dry_run=args.dry_run,
        )
    except RunnerError as exc:
        print(f"P2 corpus runner error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))
    counts = summary["counts"]
    if counts["interrupted"]:
        return 130
    return 1 if (
        counts["failed"] or counts["timed_out"] or counts["censored"]
        or counts["blocked"]
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
