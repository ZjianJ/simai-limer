#!/usr/bin/env python3
"""Validate the P2 fault/congestion corpus and materialize its stage gate.

The evaluator has two intentionally different outcomes:

* ``PREPARED`` accepts a complete, hash-locked dry-run plan, but never unlocks
  P3.
* ``PASS`` requires real run artifacts, independently replayed simulator
  stability evidence, exact run-level splits, independent schedule truth,
  and zero feature leakage.

No simulation is launched by this tool.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import struct
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

import pandas as pd

import generate_true16_p2_corpus as corpus_generator
import simulator_runtime_bundle as runtime_bundle
import validate_ecmp_route_candidates as route_candidate_runtime
import validate_p2_workload_runtime as workload_runtime
import validate_training_source_port_allocator as training_source_port_runtime


PASS = "PASS"
FAIL = "FAIL"
PENDING = "PENDING"
PREPARED = "PREPARED"
EXECUTED = "EXECUTED"
SCHEMA_VERSION = "limer.stage-gate.p2.v1"
CONTRACT_ID = "limer-true16-dual-plane-v1"
CORPUS_IDENTITY_SCHEMA = "limer.p2-corpus-identity.v2"
RUN_MANIFEST_SCHEMA = "limer.p2-run-manifest.v1"
SIMULATOR_STABILITY_SCHEMA = "limer.p2-simulator-stability.v1"
REQUIRED_INPUT_ARTIFACTS = (
    "contract", "link_map", "topology", "workload", "simulator_config",
)
PARTITIONS = {
    "train", "validation", "seen_link_test", "unseen_link_test", "ood_stress",
}
REQUIRED_CLASSES = {"HEALTHY", "CONGESTION", "GRAY_FAULT"}
REQUIRED_GRAY_FAMILIES = {
    "bandwidth_degradation", "random_loss", "burst_loss",
    "service_degradation", "intermittent_service",
}
REQUIRED_CONGESTION_SCENARIOS = {
    "high_utilization", "allreduce_burst", "incast",
    "ecmp_or_hash_contention", "queue_buildup", "ecn", "pfc",
}
REQUIRED_BANDWIDTH_FRACTIONS = {0.8, 0.5, 0.2}
REQUIRED_RAMP_DURATIONS_NS = {10_000_000, 50_000_000, 100_000_000}
REQUIRED_RANDOM_LOSS = {0.0001, 0.001, 0.005, 0.01, 0.05}
REQUIRED_BURST_SHAPES = {"periodic", "random"}
REQUIRED_SERVICE_EFFECTS = {
    "queue_growth", "latency_growth", "throughput_degradation",
}
REQUIRED_INTERMITTENT_IMPAIRMENTS = {"capacity", "loss", "service"}
CAUSAL_WARMUP_NS = 100_000_000
RECOVERY_OBSERVATION_NS = 1_000_000_000
REQUIRED_COMPLETE_ARTIFACTS = {
    "run_manifest", "link_map", "switch_telemetry", "nic_telemetry",
    "collective_telemetry", "collective_transaction", "run_lifecycle",
    "semantic_validation", "fault_application_telemetry",
    "workload_runtime_qualification", "runtime_execution_evidence",
    "ecmp_route_candidates", "ecmp_route_candidate_validation",
    "training_source_port_allocator",
    "training_source_port_allocator_validation",
}
SIMULATOR_STABILITY_SOURCE_ARTIFACTS = {
    "exit_code": "exit_code",
    "run_lifecycle": "run_lifecycle",
    "switch_telemetry": "switch_telemetry",
    "nic_telemetry": "nic_telemetry",
    "collective_telemetry": "collective_telemetry",
    "collective_transaction": "collective_transaction",
    "fault_application_telemetry": "fault_application_telemetry",
    "runtime_execution_evidence": "runtime_execution_evidence",
    "semantic_validation": "semantic_validation",
    "workload_runtime_qualification": "workload_runtime_qualification",
    "ecmp_route_candidates": "ecmp_route_candidates",
    "ecmp_route_candidate_validation": "ecmp_route_candidate_validation",
    "training_source_port_allocator": "training_source_port_allocator",
    "training_source_port_allocator_validation": (
        "training_source_port_allocator_validation"
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
BACKGROUND_FLOW_COLUMNS = (
    "event_id", "flow_id", "scenario", "scheduled_start_ns", "src_rank",
    "dst_rank", "bytes", "pg", "sport", "dport",
)
BACKGROUND_APPLICATION_REQUIRED = {
    "run_id", "event_id", "flow_id", "scenario", "event",
    "scheduled_start_ns", "actual_ns", "first_tx_ns", "first_ack_ns",
    "src_rank", "dst_rank", "bytes", "pg", "sport", "dport", "status",
}
EXECUTABLE_BACKGROUND_SCENARIOS = {"incast", "queue_buildup"}
MURMUR3_SEED_U32 = 0x8BADF00D
UINT32_MASK = 0xFFFFFFFF
BACKGROUND_TRUTH_REQUIRED = {
    "destination_rank", "bottleneck_access_link_id",
    "paired_access_link_id", "data_plane",
    "route_candidate_order_host_ports", "route_bucket",
    "hash_algorithm", "hash_seed_u32", "hash_tuple", "hash_byte_order",
    "pin_reverse_ack", "predeclared_window_policy",
    "realized_window_policy", "completion_deadline_ns",
    "rdma_rto_us", "rdma_retry_limit", "max_rto_retry_events",
}
BACKGROUND_SEMANTIC_EFFECTS = {
    "background_rdma_ack_complete", "queue_pressure",
    "throughput_activity", "common_destination_fan_in",
    "single_target_access_link", "zero_background_rto_retries",
    "realized_wave_profile",
}
BACKGROUND_RDMA_COLUMNS = (
    "run_id", "timestamp_ns", "node_id", "rank_id", "logical_qp_id",
    "transport_epoch", "traffic_class", "event", "event_detail",
    "wc_status", "src_rank", "dst_rank", "sport", "primary_nic",
    "backup_nic", "active_nic", "backup_ready_ns", "failover_ns",
    "backup_first_tx_ns", "backup_first_ack_ns", "standby_tx_bytes",
    "snd_una", "snd_nxt", "retry_count", "retry_limit", "rto_us",
)
BACKGROUND_RDMA_REQUIRED = frozenset(BACKGROUND_RDMA_COLUMNS)
ALLOWED_CONGESTION_TRUTH = {
    "predeclared_workload_schedule", "predeclared_network_schedule",
}
FORBIDDEN_FEATURE_EXACT = {
    "configured_bandwidth_bps", "fault_type", "fault_start_time",
    "fault_start_time_ns", "fault_end_time", "fault_end_time_ns",
    "injected_loss_rate", "injected_bandwidth", "injected_bandwidth_bps",
    "fault_target_port", "fault_target_link", "target_link_id", "target_gpu",
    "target_gpu_id", "fault_active", "severity", "recovery_delay_ns", "label",
    "class_label", "observability_weight", "future_collective_completion",
    "final_run_duration", "final_run_duration_ns", "future_samples", "run_role",
    "split", "partition", "parameter_before", "parameter_after", "schedule_id",
    # The current simulator computes raw utilization with the mutable injected
    # data rate as denominator.  That exposes the configured impairment.  Only
    # nominal_utilization (or an explicitly proven immutable recomputation) is
    # admissible.
    "utilization",
}
FORBIDDEN_FEATURE_FRAGMENTS = {
    "fault_", "_fault", "label", "severity", "configured_", "injected_",
    "future_", "_future", "schedule_", "_schedule", "parameter_before",
    "parameter_after", "recovery_delay", "observability_weight",
}


def _add(
    checks: List[Dict[str, str]], name: str, status: str, detail: str,
) -> None:
    if status not in {PASS, FAIL, PENDING}:
        raise ValueError(f"invalid check status {status}")
    checks.append({"check": name, "status": status, "detail": detail})


def _load_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} root must be a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _schedule_identity_tuple(run: Mapping[str, Any]) -> Tuple[Any, ...]:
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


def _run_identity_entry(run: Mapping[str, Any]) -> Dict[str, Any]:
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


def _corpus_identity_evidence(
    corpus: Mapping[str, Any], *, require_v2: bool,
) -> Tuple[Dict[str, Any], List[str]]:
    """Recompute the generator's v2 schedule set and corpus identity."""

    errors: List[str] = []
    identity_schema = corpus.get("identity_schema")
    if require_v2 and identity_schema != CORPUS_IDENTITY_SCHEMA:
        errors.append(
            "executed corpus must use identity_schema="
            f"{CORPUS_IDENTITY_SCHEMA}, got={identity_schema!r}"
        )
    elif identity_schema not in {None, CORPUS_IDENTITY_SCHEMA}:
        errors.append(f"unsupported corpus identity_schema={identity_schema!r}")

    runs = corpus.get("runs", [])
    inputs = corpus.get("input_artifacts", {})
    split_ref = corpus.get("split_manifest", {})
    if not isinstance(runs, list) or not all(
            isinstance(run, Mapping) for run in runs):
        errors.append("corpus identity requires an object-valued runs list")
        runs = []
    if not isinstance(inputs, Mapping):
        errors.append("corpus identity requires input_artifacts")
        inputs = {}
    if not isinstance(split_ref, Mapping):
        errors.append("corpus identity requires split_manifest metadata")
        split_ref = {}

    input_hashes: Dict[str, str] = {}
    for name in REQUIRED_INPUT_ARTIFACTS:
        ref = inputs.get(name, {}) if isinstance(inputs, Mapping) else {}
        value = ref.get("sha256") if isinstance(ref, Mapping) else None
        if not _valid_sha256(value):
            errors.append(f"input_artifacts.{name}.sha256 is invalid")
        else:
            input_hashes[name] = str(value)

    expected_schedule_set: str | None = None
    expected_corpus_id: str | None = None
    try:
        schedule_material = [_schedule_identity_tuple(run) for run in runs]
        expected_schedule_set = _canonical_hash(schedule_material)
        if corpus.get("schedule_set_sha256") != expected_schedule_set:
            errors.append(
                "schedule_set_sha256 does not bind current run schedules: "
                f"expected={expected_schedule_set}, "
                f"observed={corpus.get('schedule_set_sha256')!r}"
            )
        if identity_schema == CORPUS_IDENTITY_SCHEMA and not (
                set(REQUIRED_INPUT_ARTIFACTS) - set(input_hashes)):
            identity_material = {
                "identity_schema": CORPUS_IDENTITY_SCHEMA,
                "contract_sha256": input_hashes["contract"],
                "link_map_sha256": input_hashes["link_map"],
                "topology_sha256": input_hashes["topology"],
                "workload_sha256": input_hashes["workload"],
                "simulator_config_sha256": input_hashes["simulator_config"],
                "seed": corpus["generation_seed"],
                "holdouts": split_ref.get("paired_holdout_gpu_ids"),
                "runs": [_run_identity_entry(run) for run in runs],
            }
            expected_corpus_id = "p2-" + _canonical_hash(identity_material)[:24]
            if corpus.get("corpus_id") != expected_corpus_id:
                errors.append(
                    "corpus_id does not match v2 identity: "
                    f"expected={expected_corpus_id}, "
                    f"observed={corpus.get('corpus_id')!r}"
                )
    except (KeyError, TypeError, ValueError) as error:
        errors.append(f"corpus identity material is invalid: {error}")

    return {
        "identity_schema": identity_schema,
        "expected_corpus_id": expected_corpus_id,
        "observed_corpus_id": corpus.get("corpus_id"),
        "expected_schedule_set_sha256": expected_schedule_set,
        "observed_schedule_set_sha256": corpus.get("schedule_set_sha256"),
        "input_sha256": input_hashes,
    }, errors


def _artifact(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _resolve(path_value: Any, base_dir: Path) -> Path:
    path = Path(str(path_value))
    return path if path.is_absolute() else base_dir / path


def _ref_path(ref: Mapping[str, Any], base_dir: Path) -> Path:
    return _resolve(ref.get("path", ""), base_dir)


def verify_ref(
    ref: Any, base_dir: Path, *, require_file: bool = True,
) -> Tuple[bool, Dict[str, Any]]:
    if not isinstance(ref, Mapping):
        return False, {"reason": "reference is not an object"}
    path = _ref_path(ref, base_dir)
    expected = ref.get("sha256")
    exists = path.is_file()
    actual = _sha256(path) if exists else None
    size = path.stat().st_size if exists else None
    size_ok = ref.get("size_bytes") in (None, size)
    digest_ok = isinstance(expected, str) and len(expected) == 64 and expected == actual
    ok = (exists and digest_ok and size_ok) if require_file else (
        (not exists and expected in (None, "")) or (exists and digest_ok and size_ok)
    )
    return ok, {
        "path": str(path.resolve()),
        "exists": exists,
        "expected_sha256": expected,
        "actual_sha256": actual,
        "size_bytes": size,
        "size_matches": size_ok,
    }


def _normal_class(value: Any) -> str:
    aliases = {
        "healthy": "HEALTHY",
        "congestion": "CONGESTION",
        "gray": "GRAY_FAULT",
        "gray_fault": "GRAY_FAULT",
        "hard": "HARD_FAULT",
        "hard_fault": "HARD_FAULT",
    }
    text = str(value or "").strip()
    return aliases.get(text.lower(), text.upper())


def _normal_artifact_key(key: str) -> str:
    aliases = {
        "run_manifest.json": "run_manifest",
        "link_map.csv": "link_map",
        "switch_telemetry.csv": "switch_telemetry",
        "nic_telemetry.csv": "nic_telemetry",
        "collective_telemetry.csv": "collective_telemetry",
        "collective_transaction.csv": "collective_transaction",
        "run_lifecycle.csv": "run_lifecycle",
        "workload_runtime_qualification.json": "workload_runtime_qualification",
        "runtime_execution_evidence.json": "runtime_execution_evidence",
        "ecmp_route_candidates.csv": "ecmp_route_candidates",
        "ecmp_route_candidate_validation.json": (
            "ecmp_route_candidate_validation"
        ),
        "fault_application_telemetry.csv": "fault_application_telemetry",
        "background_flow_application.csv": "background_flow_application",
        "run.log": "run_log",
        "exit_code.txt": "exit_code",
    }
    return aliases.get(key, key.removesuffix(".csv").removesuffix(".json"))


def _artifact_map(run: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    raw = run.get("artifacts", {})
    if not isinstance(raw, Mapping):
        return {}
    artifacts = {
        _normal_artifact_key(str(key)): value
        for key, value in raw.items() if isinstance(value, Mapping)
    }
    mechanism = run.get("mechanism", {})
    if ("semantic_validation" not in artifacts
            and isinstance(mechanism, Mapping)
            and isinstance(mechanism.get("semantic_validation"), Mapping)):
        artifacts["semantic_validation"] = mechanism["semantic_validation"]
    return artifacts


def _artifact_inventory(directory: Path) -> List[Dict[str, Any]]:
    """Mirror the runner's canonical inventory, excluding its manifest/seal."""

    excluded = {"run_manifest.json", "run_manifest.sha256"}
    entries: List[Dict[str, Any]] = []
    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(directory).as_posix()
        if relative in excluded:
            continue
        entries.append({
            "path": relative,
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        })
    return entries


def _relative_to_run(path: Path, run_dir: Path) -> str | None:
    try:
        return path.resolve().relative_to(run_dir.resolve()).as_posix()
    except ValueError:
        return None


RUNTIME_CLOSURE_RECORD_FIELDS = {
    "schema_version", "identity_sha256", "execution_root", "bundle_path",
    "bundle_manifest_path", "bundle_manifest_sha256",
    "bundle_manifest_seal_path", "bundle_artifact_set_sha256",
    "bundle_executable_path", "bundle_executable_sha256",
    "bundle_executable_size_bytes", "source_executable_path",
    "project_dependency_count", "system_dependency_count",
    "loader_isolation",
}


def _safe_runtime_relative(value: Any, description: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"runtime closure {description} is missing")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(
            f"runtime closure {description} is unsafe: {value!r}"
        )
    return relative


def _canonical_runtime_record(
    execution_root: Path, bundle: runtime_bundle.RuntimeBundle,
) -> Dict[str, Any]:
    """Reconstruct the runner's authority record from sealed bundle bytes."""

    bundle_relative = bundle.root.relative_to(execution_root).as_posix()
    executable_relative = bundle.executable.relative_to(execution_root).as_posix()
    manifest_path = bundle.root / runtime_bundle.MANIFEST_NAME
    manifest_relative = manifest_path.relative_to(execution_root).as_posix()
    seal_relative = (
        bundle.root / runtime_bundle.SEAL_NAME
    ).relative_to(execution_root).as_posix()
    executable = bundle.manifest.get("executable", {})
    projects = bundle.manifest.get("project_dependencies", [])
    systems = bundle.manifest.get("system_dependencies", [])
    return {
        "schema_version": runtime_bundle.BUNDLE_SCHEMA,
        "identity_sha256": bundle.identity_sha256,
        "execution_root": str(execution_root),
        "bundle_path": bundle_relative,
        "bundle_manifest_path": manifest_relative,
        "bundle_manifest_sha256": _sha256(manifest_path),
        "bundle_manifest_seal_path": seal_relative,
        "bundle_artifact_set_sha256": bundle.manifest.get(
            "artifact_set_sha256"
        ),
        "bundle_executable_path": executable_relative,
        "bundle_executable_sha256": executable.get("sha256")
        if isinstance(executable, Mapping) else None,
        "bundle_executable_size_bytes": executable.get("size_bytes")
        if isinstance(executable, Mapping) else None,
        "source_executable_path": executable.get("source_path")
        if isinstance(executable, Mapping) else None,
        "project_dependency_count": len(projects)
        if isinstance(projects, list) else -1,
        "system_dependency_count": len(systems)
        if isinstance(systems, list) else -1,
        "loader_isolation": dict(bundle.manifest.get("loader_contract", {}))
        if isinstance(bundle.manifest.get("loader_contract", {}), Mapping)
        else None,
    }


def _sealed_runtime_closure_evidence(
    record: Any,
    run_dir: Path,
    cache: Dict[str, Tuple[runtime_bundle.RuntimeBundle | None,
                           Mapping[str, Any], List[str]]],
) -> Tuple[runtime_bundle.RuntimeBundle | None, Dict[str, Any], List[str]]:
    """Independently validate one run's shared dynamic-loader closure.

    The expensive bundle hashing and ``ldd`` replay are cached by canonical
    bundle directory.  Every run record is still compared byte-for-byte with
    the independently reconstructed authority record.
    """

    errors: List[str] = []
    if not isinstance(record, Mapping):
        return None, {}, [
            "run manifest lacks a sealed simulator runtime closure binding"
        ]
    if set(record) != RUNTIME_CLOSURE_RECORD_FIELDS:
        errors.append(
            "runtime closure authority record has the wrong key set: "
            f"missing={sorted(RUNTIME_CLOSURE_RECORD_FIELDS - set(record))}, "
            f"extra={sorted(set(record) - RUNTIME_CLOSURE_RECORD_FIELDS)}"
        )
    identity = record.get("identity_sha256")
    if not _valid_sha256(identity):
        errors.append("runtime closure identity_sha256 is invalid")
    expected_loader_contract = {
        "clear_inherited_prefix": "LD_",
        "LD_LIBRARY_PATH": runtime_bundle.LIB_DIRECTORY_RELATIVE,
        "LD_PRELOAD": None,
    }
    if record.get("loader_isolation") != expected_loader_contract:
        errors.append("runtime closure loader isolation contract is invalid")
    raw_execution_root = record.get("execution_root")
    execution_root: Path | None = None
    if not isinstance(raw_execution_root, str) or not raw_execution_root:
        errors.append("runtime closure execution_root is missing")
    else:
        requested_root = Path(raw_execution_root)
        if not requested_root.is_absolute():
            errors.append("runtime closure execution_root is not absolute")
        try:
            execution_root = requested_root.resolve(strict=True)
            if str(execution_root) != raw_execution_root:
                errors.append("runtime closure execution_root is not canonical")
            run_dir.resolve(strict=True).relative_to(execution_root)
        except (OSError, ValueError) as error:
            errors.append(
                "sealed run is not contained by runtime closure execution_root: "
                f"{error}"
            )
            execution_root = None

    bundle_root: Path | None = None
    if execution_root is not None and _valid_sha256(identity):
        try:
            bundle_relative = _safe_runtime_relative(
                record.get("bundle_path"), "bundle_path"
            )
            expected_relative = Path(runtime_bundle.BUNDLE_ROOT_NAME) / str(identity)
            if bundle_relative != expected_relative:
                errors.append(
                    "runtime closure bundle_path is not content-addressed by identity"
                )
            bundle_root = execution_root / bundle_relative
            expected_paths = {
                "bundle_manifest_path": (
                    bundle_relative / runtime_bundle.MANIFEST_NAME
                ),
                "bundle_manifest_seal_path": (
                    bundle_relative / runtime_bundle.SEAL_NAME
                ),
                "bundle_executable_path": (
                    bundle_relative / runtime_bundle.EXECUTABLE_RELATIVE
                ),
            }
            for field, expected in expected_paths.items():
                if _safe_runtime_relative(record.get(field), field) != expected:
                    errors.append(f"runtime closure {field} is not canonical")
        except ValueError as error:
            errors.append(str(error))

    bundle: runtime_bundle.RuntimeBundle | None = None
    canonical: Mapping[str, Any] = {}
    loader_evidence: Mapping[str, Any] = {}
    if bundle_root is not None:
        cache_key = str(bundle_root)
        cached = cache.get(cache_key)
        if cached is None:
            cache_errors: List[str] = []
            try:
                validated = runtime_bundle.validate_runtime_bundle(bundle_root)
                loader = runtime_bundle.verify_loader_resolution(validated)
                canonical_value = _canonical_runtime_record(
                    execution_root, validated
                )
                loader_value = {
                    "status": PASS,
                    "runtime_bundle_identity_sha256": loader.identity_sha256,
                    "project_dependency_count": len(
                        loader.project_dependencies
                    ),
                    "system_dependency_count": len(loader.system_dependencies),
                }
                cached = (validated, {
                    "authority_record": canonical_value,
                    "loader": loader_value,
                }, cache_errors)
            except (OSError, ValueError, runtime_bundle.RuntimeBundleError) as error:
                cache_errors.append(
                    f"independent sealed runtime bundle validation failed: {error}"
                )
                cached = (None, {}, cache_errors)
            cache[cache_key] = cached
        bundle, cached_evidence, cache_errors = cached
        errors.extend(cache_errors)
        if isinstance(cached_evidence, Mapping):
            candidate = cached_evidence.get("authority_record", {})
            canonical = candidate if isinstance(candidate, Mapping) else {}
            candidate = cached_evidence.get("loader", {})
            loader_evidence = candidate if isinstance(candidate, Mapping) else {}
    if canonical and dict(record) != dict(canonical):
        errors.append(
            "runtime closure authority record differs from independently "
            "reconstructed sealed bundle record"
        )
    if bundle is not None:
        if bundle.identity_sha256 != identity:
            errors.append("runtime closure identity differs from sealed bundle")
        if bundle.manifest.get("loader_contract") != expected_loader_contract:
            errors.append("sealed runtime bundle loader contract is invalid")
        if (
            bundle.manifest.get("publish_protocol")
            != "pending_directory_then_atomic_rename"
        ):
            errors.append("sealed runtime bundle publish protocol is invalid")
        if bundle.root.is_relative_to(run_dir.resolve()):
            errors.append("shared runtime bundle was copied inside a sealed run")

    return bundle, {
        "status": PASS if not errors else FAIL,
        "identity_sha256": identity,
        "authority_record_sha256": _canonical_hash(record),
        "bundle_path": str(bundle_root) if bundle_root is not None else None,
        "bundle_manifest_sha256": (
            canonical.get("bundle_manifest_sha256") if canonical else None
        ),
        "bundle_artifact_set_sha256": (
            canonical.get("bundle_artifact_set_sha256") if canonical else None
        ),
        "project_dependency_count": (
            canonical.get("project_dependency_count") if canonical else None
        ),
        "system_dependency_count": (
            canonical.get("system_dependency_count") if canonical else None
        ),
        "loader": dict(loader_evidence),
        "errors": errors,
    }, errors


def _runtime_execution_evidence(
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path,
    run_dir: Path,
    bundle: runtime_bundle.RuntimeBundle | None,
    closure_record: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Verify the sealed per-process loader evidence against bundle bytes."""

    errors: List[str] = []
    ref = artifacts.get("runtime_execution_evidence", {})
    evidence_path = _ref_path(ref, base_dir)
    if evidence_path != run_dir / "runtime_execution_evidence.json":
        errors.append("runtime execution evidence path is not canonical")
    try:
        evidence = _load_json(evidence_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {}, [f"runtime execution evidence unreadable: {error}"]
    evidence_sha = _sha256(evidence_path)
    identity = closure_record.get("identity_sha256")
    if manifest.get("runtime_execution_status") != PASS:
        errors.append("run manifest runtime_execution_status is not PASS")
    if manifest.get("runtime_execution_evidence_sha256") != evidence_sha:
        errors.append("run manifest runtime execution evidence hash is invalid")
    if evidence.get("schema_version") != "limer.runtime-execution-evidence.v1":
        errors.append("runtime execution evidence schema is invalid")
    if evidence.get("status") != PASS or evidence.get("errors") != []:
        errors.append("runtime execution evidence is not an error-free PASS")
    if evidence.get("run_id") != manifest.get("run_id"):
        errors.append("runtime execution evidence run_id differs from manifest")
    if evidence.get("runtime_closure") != closure_record:
        errors.append("runtime execution evidence changed the runtime closure")

    loader = evidence.get("loader_preflight")
    pre_run = evidence.get("pre_run_verification")
    loader_environment = evidence.get("loader_environment")
    process = evidence.get("process_mapping_verification")
    post_run = evidence.get("post_run_verification")
    if not isinstance(loader, Mapping):
        errors.append("runtime loader preflight evidence is missing")
        loader = {}
    if (
        loader.get("status") != PASS
        or loader.get("runtime_bundle_identity_sha256") != identity
        or loader.get("project_dependency_count")
        != closure_record.get("project_dependency_count")
        or loader.get("system_dependency_count")
        != closure_record.get("system_dependency_count")
    ):
        errors.append("runtime loader preflight evidence is invalid")
    if (
        not isinstance(pre_run, Mapping)
        or pre_run.get("status") != PASS
        or pre_run.get("phase") not in {"before_run", "reuse"}
        or pre_run.get("bundle_integrity_status") != PASS
        or pre_run.get("loader_preflight_identity_sha256") != identity
    ):
        errors.append("runtime pre-run verification evidence is invalid")
    if (
        not isinstance(post_run, Mapping)
        or post_run.get("status") != PASS
        or post_run.get("phase") != "after_run"
        or post_run.get("bundle_integrity_status") != PASS
    ):
        errors.append("runtime post-run verification evidence is invalid")

    expected_library_path = None
    expected_executable = None
    expected_projects: List[Dict[str, Any]] = []
    expected_systems: List[Dict[str, Any]] = []
    if bundle is not None:
        expected_library_path = str(bundle.lib_directory)
        expected_executable = str(bundle.executable)
        expected_projects = sorted(
            [
                {
                    "soname": item.get("soname"),
                    "path": str(bundle.root / str(item.get("execution_path"))),
                    "sha256": item.get("sha256"),
                    "size_bytes": item.get("size_bytes"),
                }
                for item in bundle.manifest.get("project_dependencies", [])
                if isinstance(item, Mapping)
            ],
            key=lambda item: str(item["soname"]),
        )
        expected_systems = sorted(
            [
                {
                    "soname": item.get("soname"),
                    "path": item.get("resolved_path"),
                    "sha256": item.get("sha256"),
                    "size_bytes": item.get("size_bytes"),
                }
                for item in bundle.manifest.get("system_dependencies", [])
                if isinstance(item, Mapping)
            ],
            key=lambda item: str(item["soname"]),
        )
    loader_projects = loader.get("project_dependencies", [])
    loader_systems = loader.get("system_dependencies", [])
    loader_projects_valid = isinstance(loader_projects, list) and all(
        isinstance(item, Mapping) for item in loader_projects
    )
    loader_systems_valid = isinstance(loader_systems, list) and all(
        isinstance(item, Mapping) for item in loader_systems
    )
    expected_virtual = (
        list(bundle.manifest.get("virtual_dependencies", []))
        if bundle is not None else None
    )
    if (
        bundle is None
        or loader.get("executable_path") != expected_executable
        or loader.get("executable_sha256")
        != closure_record.get("bundle_executable_sha256")
        or loader.get("virtual_dependencies") != expected_virtual
        or not loader_projects_valid
        or not loader_systems_valid
        or (
            sorted(loader_projects, key=lambda item: str(item.get("soname")))
            != expected_projects
            if loader_projects_valid else True
        )
        or (
            sorted(loader_systems, key=lambda item: str(item.get("soname")))
            != expected_systems
            if loader_systems_valid else True
        )
    ):
        errors.append("runtime loader dependency closure evidence is invalid")
    cleared_loader_variables = (
        loader_environment.get("cleared_inherited_loader_variables", [])
        if isinstance(loader_environment, Mapping) else []
    )
    cleared_loader_variables_valid = (
        isinstance(cleared_loader_variables, list)
        and all(
            isinstance(item, str) and item.startswith("LD_")
            for item in cleared_loader_variables
        )
        and cleared_loader_variables == sorted(set(cleared_loader_variables))
    )
    if (
        not isinstance(loader_environment, Mapping)
        or loader_environment.get("runtime_bundle_identity_sha256") != identity
        or loader_environment.get("LD_PRELOAD") is not None
        or loader_environment.get("LD_LIBRARY_PATH") != expected_library_path
        or not cleared_loader_variables_valid
    ):
        errors.append("sealed loader environment evidence is invalid")
    if not isinstance(process, Mapping):
        errors.append("live process runtime mapping evidence is missing")
        process = {}
    observed_projects = process.get("project_dependencies", [])
    observed_systems = process.get("system_dependencies", [])
    observed_projects_valid = isinstance(observed_projects, list) and all(
        isinstance(item, Mapping) for item in observed_projects
    )
    observed_systems_valid = isinstance(observed_systems, list) and all(
        isinstance(item, Mapping) for item in observed_systems
    )
    if (
        process.get("status") != PASS
        or isinstance(process.get("pid"), bool)
        or not isinstance(process.get("pid"), int)
        or process.get("pid", 0) <= 0
        or process.get("runtime_bundle_identity_sha256") != identity
        or process.get("executable") != expected_executable
        or process.get("project_dependency_count") != len(expected_projects)
        or process.get("system_dependency_count") != len(expected_systems)
        or not observed_projects_valid
        or not observed_systems_valid
        or (
            sorted(observed_projects, key=lambda item: str(item.get("soname")))
            != expected_projects
            if observed_projects_valid else True
        )
        or (
            sorted(observed_systems, key=lambda item: str(item.get("soname")))
            != expected_systems
            if observed_systems_valid else True
        )
    ):
        errors.append("live process runtime mapping evidence is invalid")

    direct = {
        "runtime_loader_preflight": loader,
        "runtime_pre_run_verification": pre_run,
        "runtime_process_mapping_verification": process,
        "runtime_post_run_verification": post_run,
    }
    for field, expected in direct.items():
        if manifest.get(field) != expected:
            errors.append(
                f"run manifest {field} differs from runtime execution evidence"
            )
    return {
        "status": PASS if not errors else FAIL,
        "sha256": evidence_sha,
        "runtime_bundle_identity_sha256": identity,
        "loader_preflight_status": loader.get("status"),
        "pre_run_status": pre_run.get("status")
        if isinstance(pre_run, Mapping) else None,
        "process_mapping_status": process.get("status"),
        "post_run_status": post_run.get("status")
        if isinstance(post_run, Mapping) else None,
        "project_dependency_count": process.get("project_dependency_count"),
        "system_dependency_count": process.get("system_dependency_count"),
        "errors": errors,
    }, errors


def _ecmp_route_candidate_evidence(
    run: Mapping[str, Any],
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path,
    run_dir: Path,
    corpus: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Replay route-install validation from the raw sidecar and link maps."""

    errors: List[str] = []
    raw_path = _ref_path(artifacts.get("ecmp_route_candidates", {}), base_dir)
    report_path = _ref_path(
        artifacts.get("ecmp_route_candidate_validation", {}), base_dir
    )
    if raw_path != run_dir / "ecmp_route_candidates.csv":
        errors.append("ECMP route candidate sidecar path is not canonical")
    if report_path != run_dir / "ecmp_route_candidate_validation.json":
        errors.append("ECMP route validation report path is not canonical")
    raw_sha = _sha256(raw_path) if raw_path.is_file() else None
    report_sha = _sha256(report_path) if report_path.is_file() else None
    if manifest.get("ecmp_route_candidate_validation_status") != PASS:
        errors.append("run manifest ECMP route validation status is not PASS")
    if manifest.get("ecmp_route_candidates_sha256") != raw_sha:
        errors.append("run manifest ECMP route candidate raw hash is invalid")
    if manifest.get("ecmp_route_candidate_validation_sha256") != report_sha:
        errors.append("run manifest ECMP route validation artifact hash is invalid")
    try:
        recorded = _load_json(report_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {}, [f"ECMP route candidate validation unreadable: {error}"]
    if recorded.get("report_sha256") != manifest.get(
        "ecmp_route_candidate_report_sha256"
    ):
        errors.append("ECMP route report identity differs from run manifest")

    bindings = manifest.get("input_bindings", {})
    link_binding = bindings.get("link_map", {}) \
        if isinstance(bindings, Mapping) else {}
    execution_copy = link_binding.get("execution_copy") \
        if isinstance(link_binding, Mapping) else None
    frozen_path: Path | None = None
    if not isinstance(execution_copy, str) or not execution_copy:
        errors.append("run manifest lacks frozen link_map execution binding")
    else:
        relative = Path(execution_copy)
        if relative.is_absolute() or ".." in relative.parts:
            errors.append("run manifest frozen link_map execution binding is unsafe")
        else:
            frozen_path = run_dir / relative
    topology_binding = bindings.get("topology", {}) \
        if isinstance(bindings, Mapping) else {}
    topology_execution_copy = topology_binding.get("execution_copy") \
        if isinstance(topology_binding, Mapping) else None
    frozen_topology_path: Path | None = None
    if (
        not isinstance(topology_execution_copy, str)
        or not topology_execution_copy
    ):
        errors.append("run manifest lacks frozen topology execution binding")
    else:
        relative = Path(topology_execution_copy)
        if relative.is_absolute() or ".." in relative.parts:
            errors.append("run manifest frozen topology execution binding is unsafe")
        else:
            frozen_topology_path = run_dir / relative
    runtime_link_map = _ref_path(artifacts.get("link_map", {}), base_dir)
    input_artifacts = corpus.get("input_artifacts", {})
    link_ref = input_artifacts.get("link_map", {}) \
        if isinstance(input_artifacts, Mapping) else {}
    expected_link_sha = link_ref.get("sha256") \
        if isinstance(link_ref, Mapping) else None
    topology_ref = input_artifacts.get("topology", {}) \
        if isinstance(input_artifacts, Mapping) else {}
    expected_topology_sha = topology_ref.get("sha256") \
        if isinstance(topology_ref, Mapping) else None
    recomputed: Mapping[str, Any] = {}
    if (
        frozen_path is not None
        and frozen_topology_path is not None
        and _valid_sha256(expected_link_sha)
        and _valid_sha256(expected_topology_sha)
    ):
        recomputed = route_candidate_runtime.validate_route_candidate_evidence(
            route_candidates_path=raw_path,
            topology_path=frozen_topology_path,
            frozen_link_map_path=frozen_path,
            runtime_link_map_path=runtime_link_map,
            expected_run_id=str(run.get("run_id", "")),
            expected_topology_sha256=str(expected_topology_sha),
            expected_frozen_link_map_sha256=str(expected_link_sha),
            expected_runtime_link_map_sha256=str(expected_link_sha),
        )
        if recomputed.get("status") != PASS:
            details = [
                str(item.get("detail", item))
                if isinstance(item, Mapping) else str(item)
                for item in recomputed.get("errors", [])[:8]
            ] if isinstance(recomputed.get("errors", []), list) else []
            errors.append(
                "independent ECMP route candidate validation failed: "
                + ("; ".join(details) or "unknown validation error")
            )
        elif dict(recorded) != dict(recomputed):
            errors.append(
                "sealed ECMP route validation differs from independent "
                "raw-evidence recomputation"
            )
    else:
        errors.append(
            "frozen topology/link_map hash/path is unavailable for ECMP replay"
        )
    return {
        "status": PASS if not errors else FAIL,
        "raw_sha256": raw_sha,
        "validation_sha256": report_sha,
        "report_sha256": recorded.get("report_sha256"),
        "independent_status": recomputed.get("status"),
        "installed_route_row_count": recomputed.get("evidence", {}).get(
            "installed_route_row_count"
        ) if isinstance(recomputed.get("evidence", {}), Mapping) else None,
        "mapped_candidate_count": recomputed.get("evidence", {}).get(
            "mapped_candidate_count"
        ) if isinstance(recomputed.get("evidence", {}), Mapping) else None,
        "errors": errors,
    }, errors


def _training_source_port_allocator_evidence(
    run: Mapping[str, Any],
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path,
    run_dir: Path,
) -> Tuple[Dict[str, Any], List[str]]:
    """Replay the strict GENERAL profile from the raw allocator sidecar."""

    errors: List[str] = []
    raw_path = _ref_path(
        artifacts.get("training_source_port_allocator", {}), base_dir
    )
    report_path = _ref_path(
        artifacts.get("training_source_port_allocator_validation", {}), base_dir
    )
    if raw_path != run_dir / training_source_port_runtime.RAW_FILENAME:
        errors.append("training source-port raw artifact path is not canonical")
    if report_path != run_dir / training_source_port_runtime.REPORT_FILENAME:
        errors.append("training source-port validation path is not canonical")
    raw_sha = _sha256(raw_path) if raw_path.is_file() else None
    report_sha = _sha256(report_path) if report_path.is_file() else None
    if manifest.get("training_source_port_allocator_validation_status") != PASS:
        errors.append("run manifest training source-port status is not PASS")
    if manifest.get("training_source_port_allocator_sha256") != raw_sha:
        errors.append("run manifest training source-port raw hash is invalid")
    if (
        manifest.get("training_source_port_allocator_validation_sha256")
        != report_sha
    ):
        errors.append("run manifest training source-port report hash is invalid")
    try:
        recorded = _load_json(report_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {}, [f"training source-port validation unreadable: {error}"]
    try:
        recomputed = training_source_port_runtime.validate_allocator_evidence(
            raw_path,
            expected_run_id=str(run.get("run_id", "")),
            require_reuse=False,
        )
    except training_source_port_runtime.AllocatorEvidenceError as error:
        return {}, errors + [
            f"training source-port raw-evidence recomputation failed: {error}"
        ]
    if recomputed.get("status") != PASS:
        errors.append(
            "training source-port raw-evidence recomputation failed: "
            + "; ".join(str(item) for item in recomputed.get("errors", [])[:8])
        )
    if dict(recorded) != recomputed:
        errors.append(
            "training source-port allocator validation differs from independent "
            "raw-evidence recomputation"
        )
    if manifest.get(
        "training_source_port_allocator_report_sha256"
    ) != recomputed.get("report_sha256"):
        errors.append("run manifest training source-port report identity is invalid")
    artifact = recomputed.get("artifact")
    requirements = recomputed.get("requirements")
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("path") != training_source_port_runtime.RAW_FILENAME
        or artifact.get("sha256") != raw_sha
        or artifact.get("size_bytes")
        != (raw_path.stat().st_size if raw_path.is_file() else None)
        or artifact.get("row_count") != 1
        or not isinstance(requirements, Mapping)
        or requirements.get("require_reuse") is not False
        or recomputed.get("profile") != "GENERAL"
    ):
        errors.append("training source-port GENERAL profile binding is invalid")
    metrics = recomputed.get("metrics", {})
    return {
        "status": PASS if not errors else FAIL,
        "raw_sha256": raw_sha,
        "validation_sha256": report_sha,
        "report_sha256": recomputed.get("report_sha256"),
        "profile": recomputed.get("profile"),
        "require_reuse": (
            requirements.get("require_reuse")
            if isinstance(requirements, Mapping) else None
        ),
        "reuses": metrics.get("reuses") if isinstance(metrics, Mapping) else None,
        "external_conflicts": (
            metrics.get("external_conflicts")
            if isinstance(metrics, Mapping) else None
        ),
        "exhaustions": (
            metrics.get("exhaustions") if isinstance(metrics, Mapping) else None
        ),
        "invariant_errors": (
            metrics.get("invariant_errors")
            if isinstance(metrics, Mapping) else None
        ),
        "errors": errors,
    }, errors


def _complete_run_manifest_evidence(
    run: Mapping[str, Any], artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path, corpus: Mapping[str, Any],
    runtime_cache: Dict[
        str,
        Tuple[runtime_bundle.RuntimeBundle | None, Mapping[str, Any], List[str]],
    ],
    *,
    allow_legacy_test_binding: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """Independently verify the runner's sealed publication contract."""

    errors: List[str] = []
    run_id = str(run.get("run_id", ""))
    manifest_path = _ref_path(artifacts.get("run_manifest", {}), base_dir)
    run_dir = manifest_path.parent.resolve()
    try:
        manifest = _load_json(manifest_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {}, [f"run manifest unreadable: {error}"]

    manifest_sha256 = _sha256(manifest_path)
    seal_path = run_dir / "run_manifest.sha256"
    try:
        seal_fields = seal_path.read_text(encoding="ascii").strip().split()
    except (OSError, UnicodeError) as error:
        seal_fields = []
        errors.append(f"run manifest seal unreadable: {error}")
    if (len(seal_fields) != 2 or seal_fields[1] != "run_manifest.json"
            or seal_fields[0] != manifest_sha256):
        errors.append("run_manifest.sha256 does not seal run_manifest.json")

    expected_scalars = {
        "schema_version": RUN_MANIFEST_SCHEMA,
        "status": EXECUTED,
        "execution_status": "COMPLETE",
        "evidence_state": "OBSERVATION_WINDOW_COMPLETE",
        "run_id": run_id,
        "corpus_id": corpus.get("corpus_id"),
        "partition": run.get("partition"),
        "class_label": run.get("class_label"),
        "simulation_seed": run.get("simulation_seed"),
        "virtual_finish_ns": run.get("virtual_finish_ns"),
        "world_size": 16,
        "exit_code": 0,
        "publish_protocol": "pending_directory_then_atomic_rename",
    }
    for field, expected in expected_scalars.items():
        if manifest.get(field) != expected:
            errors.append(
                f"run manifest {field}={manifest.get(field)!r}, expected={expected!r}"
            )
    for field in (
        "hard_event_detector_enabled", "recovery_action_enabled",
        "rdma_recovery_transport_enabled",
    ):
        if manifest.get(field) is not False:
            errors.append(f"run manifest {field} must be false")
    execution = manifest.get("execution", {})
    if (not isinstance(execution, Mapping)
            or execution.get("process_status") != "EXITED_ZERO"):
        errors.append("run manifest execution.process_status is not EXITED_ZERO")

    closure_record = manifest.get("runtime_closure")
    bundle, closure_evidence, closure_errors = _sealed_runtime_closure_evidence(
        closure_record, run_dir, runtime_cache
    )
    errors.extend(closure_errors)
    if not isinstance(closure_record, Mapping):
        closure_record = {}

    simulator = manifest.get("simulator", {})
    if not isinstance(simulator, Mapping):
        errors.append("run manifest simulator binding is missing")
        simulator = {}
    simulator_sha = simulator.get("sha256")
    simulator_path_value = simulator.get("path")
    if not _valid_sha256(simulator_sha):
        errors.append("run manifest simulator.sha256 is invalid")
    if simulator.get("exit_code") != 0:
        errors.append("run manifest simulator.exit_code is not zero")
    if bundle is None:
        errors.append("run manifest simulator cannot be bound without a sealed bundle")
        simulator_path: Path | None = None
    else:
        simulator_path = bundle.executable
        if simulator_path_value != closure_record.get("bundle_executable_path"):
            errors.append("run manifest simulator.path differs from runtime closure")
        if simulator.get("execution_copy") is not None:
            errors.append("runtime-bundled simulator must not have a per-run copy")
        if simulator_sha != _sha256(simulator_path):
            errors.append("run manifest simulator hash differs from sealed executable")
        if (
            simulator.get("runtime_closure_identity_sha256")
            != closure_record.get("identity_sha256")
        ):
            errors.append("simulator record changed runtime closure identity")
        if (
            simulator.get("runtime_bundle_manifest_sha256")
            != closure_record.get("bundle_manifest_sha256")
        ):
            errors.append("simulator record changed runtime bundle manifest hash")
    worker_threads = manifest.get("simulator_worker_threads")
    if (
        isinstance(worker_threads, bool)
        or not isinstance(worker_threads, int)
        or worker_threads <= 0
    ):
        errors.append("run manifest simulator_worker_threads must be positive")
    if simulator.get("worker_threads") != worker_threads:
        errors.append(
            "run manifest simulator.worker_threads does not match "
            "simulator_worker_threads"
        )
    argv = simulator.get("argv", [])
    argv_workers: int | None = None
    if isinstance(argv, list) and all(isinstance(value, str) for value in argv):
        positions = [index for index, value in enumerate(argv) if value == "-t"]
        if len(positions) == 1 and positions[0] + 1 < len(argv):
            raw_workers = argv[positions[0] + 1]
            if raw_workers.isdigit():
                argv_workers = int(raw_workers)
    if argv_workers != worker_threads:
        errors.append(
            "run manifest simulator argv -t does not match "
            "simulator_worker_threads"
        )
    if (
        not isinstance(argv, list)
        or not argv
        or simulator_path is None
        or argv[0] != str(simulator_path)
    ):
        errors.append("run manifest did not execute the sealed runtime executable")

    recorded = manifest.get("artifacts")
    valid_recorded = isinstance(recorded, list) and all(
        isinstance(entry, Mapping)
        and set(entry) == {"path", "size_bytes", "sha256"}
        and isinstance(entry.get("path"), str)
        and bool(entry.get("path"))
        and not Path(str(entry.get("path"))).is_absolute()
        and ".." not in Path(str(entry.get("path"))).parts
        and isinstance(entry.get("size_bytes"), int)
        and not isinstance(entry.get("size_bytes"), bool)
        and entry.get("size_bytes", -1) >= 0
        and _valid_sha256(entry.get("sha256"))
        for entry in (recorded if isinstance(recorded, list) else [])
    )
    actual_inventory = _artifact_inventory(run_dir)
    if not valid_recorded:
        errors.append("run manifest artifact inventory schema is invalid")
        recorded_entries: List[Mapping[str, Any]] = []
    else:
        recorded_entries = list(recorded)
        paths = [str(entry["path"]) for entry in recorded_entries]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            errors.append("run manifest artifact inventory is not unique/sorted")
        if recorded_entries != actual_inventory:
            errors.append("run manifest artifact inventory differs from run directory")
        if manifest.get("artifact_set_sha256") != _canonical_hash(recorded_entries):
            errors.append("run manifest artifact_set_sha256 is invalid")
    inventory_by_path = {
        str(entry["path"]): entry for entry in recorded_entries
        if isinstance(entry, Mapping) and isinstance(entry.get("path"), str)
    }
    sealed_artifact_names = set(REQUIRED_COMPLETE_ARTIFACTS) - {"run_manifest"}
    stability_for_artifacts = run.get("simulator_stability", {})
    if (
        isinstance(stability_for_artifacts, Mapping)
        and stability_for_artifacts.get("gate_required") is True
    ):
        sealed_artifact_names.update({"simulator_stability", "exit_code"})
    schedule_for_artifacts = run.get("schedule", {})
    if (isinstance(schedule_for_artifacts, Mapping)
            and isinstance(
                schedule_for_artifacts.get("background_flow_schedule"), Mapping
            )):
        sealed_artifact_names.update({
            "background_flow_application", "rdma_wc_telemetry",
        })
    for name in sorted(sealed_artifact_names & set(artifacts)):
        ref = artifacts[name]
        path = _ref_path(ref, base_dir)
        relative = _relative_to_run(path, run_dir)
        if relative is None:
            errors.append(f"artifact {name} is outside the sealed run directory")
            continue
        entry = inventory_by_path.get(relative)
        if entry is None:
            errors.append(f"artifact {name} is absent from runner inventory")
        elif (entry.get("sha256") != ref.get("sha256")
              or entry.get("size_bytes") != path.stat().st_size):
            errors.append(f"artifact {name} differs from runner inventory")

    runtime_execution: Dict[str, Any] = {}
    if "runtime_execution_evidence" in artifacts:
        runtime_execution, runtime_execution_errors = (
            _runtime_execution_evidence(
                manifest,
                artifacts,
                base_dir,
                run_dir,
                bundle,
                closure_record,
            )
        )
        errors.extend(runtime_execution_errors)
    route_candidate_evidence: Dict[str, Any] = {}
    if (
        "ecmp_route_candidates" in artifacts
        and "ecmp_route_candidate_validation" in artifacts
    ):
        route_candidate_evidence, route_candidate_errors = (
            _ecmp_route_candidate_evidence(
                run, manifest, artifacts, base_dir, run_dir, corpus
            )
        )
        errors.extend(route_candidate_errors)
    allocator_evidence: Dict[str, Any] = {}
    if (
        "training_source_port_allocator" in artifacts
        and "training_source_port_allocator_validation" in artifacts
    ):
        allocator_evidence, allocator_errors = (
            _training_source_port_allocator_evidence(
                run, manifest, artifacts, base_dir, run_dir
            )
        )
        errors.extend(allocator_errors)

    source_copy = run_dir / "inputs" / "corpus_manifest.json"
    source_corpus: Mapping[str, Any] = {}
    if not source_copy.is_file():
        errors.append("runner inventory lacks inputs/corpus_manifest.json")
    else:
        source_hash = _sha256(source_copy)
        if inventory_by_path.get("inputs/corpus_manifest.json", {}).get(
                "sha256") != source_hash:
            errors.append("source corpus copy is absent from runner inventory")
        if manifest.get("corpus_manifest_sha256") != source_hash:
            errors.append("run manifest corpus hash differs from copied source corpus")
        try:
            source_corpus = _load_json(source_copy)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"copied source corpus is unreadable: {error}")
            source_corpus = {}
    if source_corpus:
        if (source_corpus.get("schema_version")
                != "limer.p2-corpus-manifest.v1"):
            errors.append("copied source corpus schema_version is invalid")
        if source_corpus.get("status") not in {PREPARED, EXECUTED}:
            errors.append("copied source corpus status is not runnable")
        source_identity, source_identity_errors = _corpus_identity_evidence(
            source_corpus, require_v2=True,
        )
        errors.extend(
            f"copied source corpus: {error}" for error in source_identity_errors
        )
        if source_identity.get("observed_corpus_id") != corpus.get("corpus_id"):
            errors.append("copied source corpus_id differs from executed corpus")
        source_runs = [
            candidate for candidate in source_corpus.get("runs", [])
            if isinstance(candidate, Mapping) and candidate.get("run_id") == run_id
        ]
        if len(source_runs) != 1:
            errors.append("copied source corpus lacks exactly one planned run")
        else:
            source_run = source_runs[0]
            if manifest.get("planned_run_sha256") != _canonical_hash(source_run):
                errors.append("run manifest planned_run_sha256 is invalid")
            planned_stability = source_run.get("simulator_stability", {})
            source_gate_required = (
                isinstance(planned_stability, Mapping)
                and planned_stability.get("gate_required") is True
            )
            executed_stability = run.get("simulator_stability", {})
            executed_gate_required = (
                isinstance(executed_stability, Mapping)
                and executed_stability.get("gate_required") is True
            )
            if (
                source_run.get("fault_family") == "random_loss"
                and not source_gate_required
            ):
                errors.append("random_loss source run lacks a stability gate")
            if source_gate_required and planned_stability.get(
                "status"
            ) != "PENDING_EXECUTION":
                errors.append("source stability state was not PENDING_EXECUTION")
            if executed_gate_required is not source_gate_required:
                errors.append("executed run changed the prepared stability gate")
            if (
                manifest.get("simulator_stability_gate_required")
                is not source_gate_required
            ):
                errors.append("run manifest changed the prepared stability gate")
            expected_stability_status = (
                PASS if source_gate_required else "NOT_REQUIRED"
            )
            if manifest.get(
                "simulator_stability_status"
            ) != expected_stability_status:
                errors.append("run manifest simulator stability status is invalid")
            stability_sha = manifest.get("simulator_stability_sha256")
            if source_gate_required:
                if not _valid_sha256(stability_sha):
                    errors.append("run manifest lacks a stability report hash")
                if (
                    not isinstance(executed_stability, Mapping)
                    or executed_stability.get("status") != PASS
                ):
                    errors.append("executed gated run does not publish stability PASS")
            elif stability_sha is not None:
                errors.append("non-gated run unexpectedly binds a stability report")

    source_inputs = source_corpus.get("input_artifacts", {}) \
        if isinstance(source_corpus, Mapping) else {}
    current_inputs = corpus.get("input_artifacts", {})
    bindings = manifest.get("input_bindings", {})
    if not isinstance(source_inputs, Mapping):
        source_inputs = {}
    if not isinstance(current_inputs, Mapping):
        current_inputs = {}
    if not isinstance(bindings, Mapping) or set(bindings) != set(
            REQUIRED_INPUT_ARTIFACTS):
        errors.append("run manifest input_bindings has the wrong key set")
        bindings = {}
    for name in REQUIRED_INPUT_ARTIFACTS:
        source_ref = source_inputs.get(name, {})
        current_ref = current_inputs.get(name, {})
        expected_sha = source_ref.get("sha256") \
            if isinstance(source_ref, Mapping) else None
        if (not _valid_sha256(expected_sha)
                or not isinstance(current_ref, Mapping)
                or current_ref.get("sha256") != expected_sha):
            errors.append(f"source/executed input hash differs for {name}")
        binding = bindings.get(name, {}) if isinstance(bindings, Mapping) else {}
        if not isinstance(binding, Mapping) or binding.get("sha256") != expected_sha:
            errors.append(f"run manifest input binding hash differs for {name}")
            continue
        execution_copy = binding.get("execution_copy")
        if not isinstance(execution_copy, str):
            errors.append(f"run manifest input binding path missing for {name}")
            continue
        entry = inventory_by_path.get(execution_copy)
        if entry is None or entry.get("sha256") != expected_sha:
            errors.append(f"run manifest input copy is unbound for {name}")

    runtime = manifest.get("runtime_config_binding", {})
    config_sha = source_inputs.get("simulator_config", {}).get("sha256") \
        if isinstance(source_inputs.get("simulator_config", {}), Mapping) else None
    if not isinstance(runtime, Mapping):
        errors.append("run manifest runtime_config_binding is missing")
    else:
        runtime_path = runtime.get("path")
        runtime_sha = runtime.get("sha256")
        if runtime.get("derived_from_sha256") != config_sha:
            errors.append("runtime config is not derived from simulator_config input")
        if (not isinstance(runtime_path, str) or not _valid_sha256(runtime_sha)
                or inventory_by_path.get(runtime_path, {}).get("sha256")
                != runtime_sha):
            errors.append("runtime config path/hash is absent from runner inventory")

    schedule = run.get("schedule", {})
    injection = schedule.get("simulator_injection_schedule", {}) \
        if isinstance(schedule, Mapping) else {}
    background = schedule.get("background_flow_schedule", {}) \
        if isinstance(schedule, Mapping) else {}
    collective = schedule.get("collective_workload_override", {}) \
        if isinstance(schedule, Mapping) else {}
    role_ref = collective.get("layer_role_sidecar", {}) \
        if isinstance(collective, Mapping) else {}
    expected_schedule_fields = {
        "schedule_sha256": schedule.get("sha256")
        if isinstance(schedule, Mapping) else None,
        "injection_schedule_sha256": injection.get("sha256")
        if isinstance(injection, Mapping) else None,
        "background_flow_schedule_sha256": background.get("sha256")
        if isinstance(background, Mapping) else None,
        "collective_workload_override_sha256": collective.get("sha256")
        if isinstance(collective, Mapping) and collective else None,
        "collective_layer_role_sha256": role_ref.get("sha256")
        if isinstance(role_ref, Mapping) and role_ref else None,
        "effective_workload_sha256": run.get("effective_workload_sha256"),
    }
    for field, expected in expected_schedule_fields.items():
        if manifest.get(field) != expected:
            errors.append(f"run manifest {field} differs from executed corpus")

    enforce_effective_binding = (
        not allow_legacy_test_binding
        or "effective_workload_binding" in manifest
        or (isinstance(collective, Mapping) and bool(collective))
    )
    schedule_bindings = manifest.get("schedule_bindings", {})
    expected_schedule_bindings: Dict[str, Tuple[str, Any, bool, bool | None]] = {
        "truth": (
            "inputs/truth_schedule.csv",
            schedule.get("sha256") if isinstance(schedule, Mapping) else None,
            False,
            None,
        ),
    }
    if isinstance(injection, Mapping) and injection:
        expected_schedule_bindings["simulator_injection"] = (
            "inputs/simulator_injection_schedule.csv",
            injection.get("sha256"),
            True,
            True,
        )
    if isinstance(background, Mapping) and background:
        expected_schedule_bindings["background_flow"] = (
            "inputs/background_flow_schedule.csv",
            background.get("sha256"),
            True,
            True,
        )
    if isinstance(collective, Mapping) and collective:
        expected_schedule_bindings["collective_workload_override"] = (
            "inputs/collective_workload_override.txt",
            collective.get("sha256"),
            True,
            True,
        )
        expected_schedule_bindings["collective_layer_roles"] = (
            "inputs/collective_layer_roles.csv",
            role_ref.get("sha256") if isinstance(role_ref, Mapping) else None,
            False,
            None,
        )
    if enforce_effective_binding and (
        not isinstance(schedule_bindings, Mapping)
        or set(schedule_bindings) != set(expected_schedule_bindings)
    ):
        errors.append("run manifest schedule_bindings has the wrong key set")
        schedule_bindings = {}
    for name, (relative, digest, passed, safe) in (
        expected_schedule_bindings.items() if enforce_effective_binding else ()
    ):
        binding = schedule_bindings.get(name, {}) \
            if isinstance(schedule_bindings, Mapping) else {}
        expected_fields = {
            "execution_copy": relative,
            "sha256": digest,
            "passed_to_simulator": passed,
        }
        if safe is not None:
            expected_fields["safe_to_execute"] = safe
        if not isinstance(binding, Mapping) or any(
            binding.get(field) != value for field, value in expected_fields.items()
        ):
            errors.append(f"run manifest schedule binding is invalid: {name}")
        entry = inventory_by_path.get(relative)
        if entry is None or entry.get("sha256") != digest:
            errors.append(f"runner inventory does not bind schedule copy: {name}")

    common_workload = bindings.get("workload", {}) \
        if isinstance(bindings, Mapping) else {}
    expected_effective_copy = (
        "inputs/collective_workload_override.txt"
        if isinstance(collective, Mapping) and collective
        else common_workload.get("execution_copy")
        if isinstance(common_workload, Mapping)
        else None
    )
    expected_effective_sha = (
        collective.get("sha256")
        if isinstance(collective, Mapping) and collective
        else source_inputs.get("workload", {}).get("sha256")
        if isinstance(source_inputs.get("workload", {}), Mapping)
        else None
    )
    expected_effective = {
        "source_workload_sha256": (
            source_inputs.get("workload", {}).get("sha256")
            if isinstance(source_inputs.get("workload", {}), Mapping)
            else None
        ),
        "effective_workload_sha256": expected_effective_sha,
        "execution_copy": expected_effective_copy,
        "collective_override_sha256": (
            collective.get("sha256")
            if isinstance(collective, Mapping) and collective else None
        ),
        "collective_layer_role_sha256": (
            role_ref.get("sha256")
            if isinstance(role_ref, Mapping) and role_ref else None
        ),
        "passed_to_simulator": True,
    }
    if enforce_effective_binding and manifest.get(
        "effective_workload_binding"
    ) != expected_effective:
        errors.append("run manifest effective workload binding is invalid")
    if enforce_effective_binding and inventory_by_path.get(
        str(expected_effective_copy), {}
    ).get(
        "sha256"
    ) != expected_effective_sha:
        errors.append("runner inventory does not bind the effective workload")
    argv_workload = None
    if isinstance(argv, list):
        try:
            argv_workload = argv[argv.index("-w") + 1]
        except (ValueError, IndexError):
            pass
    if enforce_effective_binding and argv_workload != str(
        Path("..") / str(expected_effective_copy)
    ):
        errors.append("simulator argv did not use the effective workload copy")
    try:
        if not enforce_effective_binding:
            raise FileNotFoundError
        invocation = _load_json(run_dir / "invocation.json")
        if (
            invocation.get("argv") != argv
            or invocation.get("effective_workload_binding") != expected_effective
            or invocation.get("schedule_bindings") != schedule_bindings
        ):
            errors.append("invocation does not bind effective workload/argv")
    except FileNotFoundError:
        pass
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"runner invocation unreadable: {error}")

    lifecycle = manifest.get("lifecycle", {})
    finish_ns = _int_or_none(run.get("virtual_finish_ns"))
    if (not isinstance(lifecycle, Mapping)
            or lifecycle.get("observation_status") != "OBSERVATION_WINDOW_COMPLETE"
            or _int_or_none(lifecycle.get("observation_scheduled_ns")) != finish_ns
            or _int_or_none(lifecycle.get("observation_actual_ns")) != finish_ns
            or _int_or_none(lifecycle.get("world_size")) != 16):
        errors.append("run manifest lifecycle does not bind the exact observation horizon")

    return {
        "schema_version": manifest.get("schema_version"),
        "status": manifest.get("status"),
        "execution_status": manifest.get("execution_status"),
        "manifest_sha256": manifest_sha256,
        "seal_path": str(seal_path),
        "seal_valid": not any("seal" in error for error in errors),
        "artifact_count": len(recorded_entries),
        "simulator_sha256": simulator_sha,
        "simulator_worker_threads": worker_threads,
        "simulator_argv_worker_threads": argv_workers,
        "corpus_manifest_sha256": manifest.get("corpus_manifest_sha256"),
        "planned_run_sha256": manifest.get("planned_run_sha256"),
        "runtime_closure": closure_evidence,
        "runtime_execution": runtime_execution,
        "ecmp_route_candidates": route_candidate_evidence,
        "training_source_port_allocator": allocator_evidence,
    }, errors


RUNTIME_QUALIFICATION_FIELDS = {
    "schema_version", "status", "run_id", "qualification_profile",
    "contract_source", "planned_run_sha256",
    "static_workload_report_sha256", "contract", "source_artifacts",
    "checks", "errors", "evidence",
}
RUNTIME_SOURCE_ARTIFACTS = {
    "workload", "link_map", "switch_telemetry", "nic_telemetry",
    "collective_transaction", "collective_telemetry", "run_lifecycle",
}


def _runtime_qualification_summary(report: Mapping[str, Any]) -> Dict[str, Any]:
    evidence = report.get("evidence", {})
    if not isinstance(evidence, Mapping):
        evidence = {}
    topology = evidence.get("physical_link_map_contract", {})
    transactions = evidence.get(
        "collective_transaction_state_machine_and_cadence", {}
    )
    if isinstance(transactions, Mapping):
        transactions = {
            key: value for key, value in transactions.items()
            if key != "sequence_evidence"
        }
    return {
        "status": report.get("status"),
        "contract_source": report.get("contract_source"),
        "checks": [
            {"name": item.get("name"), "status": item.get("status")}
            for item in report.get("checks", [])
            if isinstance(item, Mapping)
        ],
        "topology": topology if isinstance(topology, Mapping) else {},
        "switch_snapshots": evidence.get("exact_switch_snapshot_coverage", {}),
        "nic_snapshots": evidence.get("exact_nic_snapshot_coverage", {}),
        "transactions": transactions if isinstance(transactions, Mapping) else {},
        "access_activity": evidence.get(
            "healthy_or_pre_event_access_traffic_cadence", {}
        ),
        "lifecycle": evidence.get("exact_incomplete_horizon_lifecycle", {}),
        "collective_override_lifecycle": evidence.get(
            "collective_override_pre_event_post_runtime", {}
        ),
    }


def _workload_runtime_qualification_evidence(
    run: Mapping[str, Any], artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path, manifest: Mapping[str, Any], corpus: Mapping[str, Any],
    *, allow_test_contract: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """Verify the sealed report, then independently replay its pure validator.

    ``allow_test_contract`` is an in-process unit-test hook.  The CLI and all
    production callers leave it false, so an executed corpus cannot downgrade
    the frozen 16-rank/1-ms workload contract through self-reported JSON.
    """

    errors: List[str] = []
    run_id = str(run.get("run_id", ""))
    report_path = _ref_path(
        artifacts.get("workload_runtime_qualification", {}), base_dir
    )
    run_dir = _ref_path(artifacts.get("run_manifest", {}), base_dir).parent
    try:
        stored = _load_json(report_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {}, [f"workload runtime qualification unreadable: {error}"]
    report_sha = _sha256(report_path)
    if set(stored) != RUNTIME_QUALIFICATION_FIELDS:
        errors.append(
            "workload runtime qualification exact schema mismatch: "
            f"observed={sorted(stored)}"
        )
    expected_scalars = {
        "schema_version": workload_runtime.SCHEMA_VERSION,
        "status": PASS,
        "run_id": run_id,
    }
    for field, expected in expected_scalars.items():
        if stored.get(field) != expected:
            errors.append(
                f"workload runtime qualification {field}="
                f"{stored.get(field)!r}, expected={expected!r}"
            )
    if stored.get("errors") != []:
        errors.append("PASS workload runtime qualification contains errors")
    if manifest.get("workload_runtime_qualification_status") != PASS:
        errors.append("run manifest runtime qualification status is not PASS")
    if manifest.get("workload_runtime_qualification_sha256") != report_sha:
        errors.append("run manifest runtime qualification hash is invalid")

    source_corpus_path = run_dir / "inputs" / "corpus_manifest.json"
    source_corpus: Mapping[str, Any] = {}
    planned_run: Mapping[str, Any] = {}
    workload_report: Mapping[str, Any] = {}
    qualification: Mapping[str, Any] = {}
    try:
        source_corpus = _load_json(source_corpus_path)
        planned = [
            candidate for candidate in source_corpus.get("runs", [])
            if isinstance(candidate, Mapping)
            and str(candidate.get("run_id", "")) == run_id
        ]
        if len(planned) != 1:
            errors.append(
                "runtime qualification source corpus lacks exactly one planned run"
            )
        else:
            planned_run = planned[0]
        raw_qualification = source_corpus.get("workload_qualification", {})
        if not isinstance(raw_qualification, Mapping):
            errors.append("source corpus workload_qualification is missing")
        else:
            qualification = raw_qualification
            raw_report = qualification.get("static_report", {})
            if not isinstance(raw_report, Mapping):
                errors.append("source corpus static workload report is missing")
            else:
                workload_report = raw_report
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"runtime qualification source corpus unreadable: {error}")

    collective_static_evidence: Dict[str, Any] = {}
    collective = None
    if planned_run:
        planned_hash = _canonical_hash(planned_run)
        if stored.get("planned_run_sha256") != planned_hash:
            errors.append("runtime qualification planned run hash is invalid")
        if manifest.get("planned_run_sha256") != planned_hash:
            errors.append("run manifest and runtime qualification planned run differ")
        planned_schedule = planned_run.get("schedule", {})
        collective = planned_schedule.get("collective_workload_override") \
            if isinstance(planned_schedule, Mapping) else None
        if isinstance(collective, Mapping):
            try:
                truth_row, truth_errors = _schedule_row(
                    planned_run,
                    _ref_path(planned_schedule, source_corpus_path.parent),
                )
                errors.extend(
                    f"A1 runtime source truth: {error}" for error in truth_errors
                )
                rebuilt, collective_static_evidence, rebuild_errors = (
                    _recompute_collective_override(
                        planned_run,
                        planned_schedule,
                        source_corpus_path.parent,
                        truth_row,
                        source_corpus.get("input_artifacts", {}),
                    )
                )
                errors.extend(rebuild_errors)
                if rebuilt:
                    workload_report = rebuilt
            except (OSError, ValueError, KeyError) as error:
                errors.append(f"A1 runtime static reconstruction failed: {error}")
    expected_profile = (
        workload_report.get("qualification_profile")
        if workload_report else workload_runtime.HORIZON_PROFILE
    )
    if stored.get("qualification_profile") != expected_profile:
        errors.append(
            "workload runtime qualification profile="
            f"{stored.get('qualification_profile')!r}, expected="
            f"{expected_profile!r}"
        )
    if workload_report:
        static_hash = _canonical_hash(workload_report)
        expected_declared_static_hash = (
            collective.get("static_validation_sha256")
            if isinstance(collective, Mapping)
            else qualification.get("static_report_sha256")
        )
        if expected_declared_static_hash != static_hash:
            errors.append("source corpus static workload report hash is invalid")
        if stored.get("static_workload_report_sha256") != static_hash:
            errors.append("runtime qualification static workload report hash is invalid")

    warmup = qualification.get(
        "causal_warmup_required_ns", workload_runtime.DEFAULT_CAUSAL_WARMUP_NS
    )
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 0:
        errors.append("source corpus causal warmup is invalid")
        warmup = workload_runtime.DEFAULT_CAUSAL_WARMUP_NS

    contract_override = None
    contract_source = stored.get("contract_source")
    if contract_source == "explicit_test_override":
        if not allow_test_contract:
            errors.append(
                "explicit runtime contract is test-only and forbidden in production"
            )
        raw_contract = stored.get("contract")
        if not isinstance(raw_contract, Mapping):
            errors.append("explicit runtime test contract is missing")
        else:
            expected_fields = set(
                workload_runtime.RuntimeValidationContract.__dataclass_fields__
            )
            if set(raw_contract) != expected_fields or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in raw_contract.values()
            ):
                errors.append("explicit runtime test contract schema is invalid")
            else:
                try:
                    contract_override = workload_runtime.RuntimeValidationContract(
                        **dict(raw_contract)
                    )
                except (TypeError, ValueError) as error:
                    errors.append(f"explicit runtime test contract is invalid: {error}")
    elif contract_source == "frozen_static_workload_report":
        if workload_report:
            try:
                derived = workload_runtime._derive_contract(
                    workload_report, int(warmup)
                )
                if stored.get("contract") != workload_runtime.asdict(derived):
                    errors.append(
                        "runtime qualification contract differs from frozen report"
                    )
                if (
                    derived.world_size != 16
                    or derived.sample_interval_ns != 1_000_000
                    or derived.expected_access_links_per_rank != 2
                ):
                    errors.append(
                        "production runtime contract is not true16/1ms/dual-ACCESS"
                    )
            except (KeyError, TypeError, ValueError) as error:
                errors.append(f"frozen runtime contract is invalid: {error}")
    else:
        errors.append(f"unknown runtime contract_source={contract_source!r}")

    bindings = manifest.get("input_bindings", {})
    workload_binding = bindings.get("workload", {}) \
        if isinstance(bindings, Mapping) else {}
    effective_binding = manifest.get("effective_workload_binding", {})
    if isinstance(collective, Mapping):
        workload_copy = effective_binding.get("execution_copy") \
            if isinstance(effective_binding, Mapping) else None
    else:
        workload_copy = workload_binding.get("execution_copy") \
            if isinstance(workload_binding, Mapping) else None
    source_paths: Dict[str, Path] = {
        "workload": run_dir / str(workload_copy or ""),
        "link_map": _ref_path(artifacts.get("link_map", {}), base_dir),
        "switch_telemetry": _ref_path(
            artifacts.get("switch_telemetry", {}), base_dir
        ),
        "nic_telemetry": _ref_path(artifacts.get("nic_telemetry", {}), base_dir),
        "collective_transaction": _ref_path(
            artifacts.get("collective_transaction", {}), base_dir
        ),
        "collective_telemetry": _ref_path(
            artifacts.get("collective_telemetry", {}), base_dir
        ),
        "run_lifecycle": _ref_path(artifacts.get("run_lifecycle", {}), base_dir),
    }
    if isinstance(collective, Mapping):
        source_paths["collective_layer_roles"] = (
            run_dir / "inputs" / "collective_layer_roles.csv"
        )
        role_ref = collective.get("layer_role_sidecar", {})
        expected_effective = {
            "source_workload_sha256": source_corpus.get("input_artifacts", {})
            .get("workload", {})
            .get("sha256"),
            "effective_workload_sha256": collective.get("sha256"),
            "execution_copy": "inputs/collective_workload_override.txt",
            "collective_override_sha256": collective.get("sha256"),
            "collective_layer_role_sha256": (
                role_ref.get("sha256") if isinstance(role_ref, Mapping) else None
            ),
            "passed_to_simulator": True,
        }
        if effective_binding != expected_effective:
            errors.append("run manifest A1 effective workload binding is invalid")
        simulator = manifest.get("simulator", {})
        argv = simulator.get("argv") if isinstance(simulator, Mapping) else None
        try:
            argv_workload = argv[argv.index("-w") + 1] \
                if isinstance(argv, list) else None
        except (ValueError, IndexError):
            argv_workload = None
        if argv_workload != "../inputs/collective_workload_override.txt":
            errors.append("simulator argv did not use the A1 workload override")
        try:
            invocation = _load_json(run_dir / "invocation.json")
            if (
                invocation.get("argv") != argv
                or invocation.get("effective_workload_binding")
                != expected_effective
                or invocation.get("schedule_bindings")
                != manifest.get("schedule_bindings")
            ):
                errors.append("A1 invocation/effective workload binding is invalid")
        except (OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"A1 invocation is unreadable: {error}")
    recorded_sources = stored.get("source_artifacts", {})
    expected_runtime_sources = set(RUNTIME_SOURCE_ARTIFACTS)
    if isinstance(collective, Mapping):
        expected_runtime_sources.add("collective_layer_roles")
    if not isinstance(recorded_sources, Mapping) or set(
        recorded_sources
    ) != expected_runtime_sources:
        errors.append("runtime qualification source artifact set is invalid")
        recorded_sources = {}
    actual_sources: Dict[str, Dict[str, str]] = {}
    for name, path in source_paths.items():
        try:
            digest = _sha256(path)
        except OSError as error:
            errors.append(f"runtime source {name} unreadable: {error}")
            digest = ""
        actual_sources[name] = {"path": path.name, "sha256": digest}
        recorded = recorded_sources.get(name, {}) \
            if isinstance(recorded_sources, Mapping) else {}
        if (
            not isinstance(recorded, Mapping)
            or set(recorded) != {"path", "sha256"}
            or dict(recorded) != actual_sources[name]
        ):
            errors.append(f"runtime source {name} path/hash binding is invalid")
    if workload_report and actual_sources["workload"]["sha256"] != (
        workload_report.get("sha256")
    ):
        errors.append("runtime workload bytes differ from static workload report")

    recomputed: Mapping[str, Any] = {}
    if not errors and planned_run and workload_report:
        paths = workload_runtime.RuntimePaths(**source_paths)
        recomputed = workload_runtime.validate_runtime_qualification(
            run=planned_run,
            workload_report=workload_report,
            paths=paths,
            causal_warmup_ns=int(warmup),
            contract_override=contract_override,
        )
        if recomputed.get("status") != PASS:
            errors.append(
                "independent runtime qualification recomputation failed: "
                f"{recomputed.get('errors', [])[:6]}"
            )
        if dict(stored) != dict(recomputed):
            errors.append(
                "sealed runtime qualification differs from independent recomputation"
            )

    return {
        "artifact_sha256": report_sha,
        "schema_version": stored.get("schema_version"),
        "sealed_status": stored.get("status"),
        "planned_run_sha256": stored.get("planned_run_sha256"),
        "static_workload_report_sha256": stored.get(
            "static_workload_report_sha256"
        ),
        "source_artifacts": actual_sources,
        "collective_override_static": collective_static_evidence,
        "independent_recomputation": _runtime_qualification_summary(recomputed),
        "errors": errors,
    }, errors


def _parameter(run: Mapping[str, Any], name: str, default: Any = None) -> Any:
    if name in run:
        return run[name]
    parameters = run.get("parameters", {})
    if isinstance(parameters, Mapping) and name in parameters:
        return parameters[name]
    severity = run.get("severity", {})
    if isinstance(severity, Mapping):
        if name in severity:
            return severity[name]
        if severity.get("name") == name:
            return severity.get("value")
    return default


def _float_set(values: Iterable[Any]) -> set[float]:
    output = set()
    for value in values:
        try:
            output.add(round(float(value), 7))
        except (TypeError, ValueError):
            continue
    return output


def _contains_required_floats(observed: Iterable[Any], required: set[float]) -> bool:
    values = _float_set(observed)
    return all(any(math.isclose(value, wanted, rel_tol=0, abs_tol=1e-7)
                   for value in values) for wanted in required)


def _read_schedule(path: Path) -> List[Mapping[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    if isinstance(value, Mapping):
        events = value.get("events", value.get("schedule", [value]))
        if isinstance(events, list):
            return [row for row in events if isinstance(row, Mapping)]
    raise ValueError(f"unsupported schedule structure in {path}")


def _schedule_row(
    run: Mapping[str, Any], schedule_path: Path,
) -> Tuple[Mapping[str, Any], List[str]]:
    rows = _read_schedule(schedule_path)
    event_id = run.get("schedule", {}).get("event_id")
    if event_id is None:
        matches = rows if len(rows) == 1 else []
    else:
        direct = [
            row for row in rows if str(row.get("event_id")) == str(event_id)
        ]
        parent = [
            row for row in rows
            if str(row.get("parent_event_id", "")) == str(event_id)
        ]
        # A simple event has one direct row.  Ramp/pulse schedules have unique
        # segment event IDs and bind all rows through parent_event_id.
        matches = parent if parent else direct
    errors: List[str] = []
    if not matches:
        errors.append(
            f"event_id={event_id!r} resolved no direct/parent rows in {schedule_path}"
        )
        return {}, errors
    if event_id is None and len(matches) != 1:
        errors.append(
            f"schedule has {len(matches)} rows but manifest has no event_id"
        )
        return {}, errors

    consistency_fields = (
        "fault_family", "target_gpu", "target_link_id", "scenario",
        "severity_name", "severity_value", "severity_unit", "shape",
        "impairment", "carrier_state",
    )
    for field in consistency_fields:
        observed = {
            str(row.get(field, "")) for row in matches
            if str(row.get(field, "")).strip()
        }
        if len(observed) > 1:
            errors.append(
                f"parent event {event_id!r} has inconsistent {field}={sorted(observed)}"
            )
    starts = [_int_or_none(row.get("start_time_ns")) for row in matches]
    ends = [_int_or_none(row.get("end_time_ns")) for row in matches]
    if any(value is None for value in starts + ends):
        errors.append(f"parent event {event_id!r} has missing segment times")
    ordered = sorted(
        matches,
        key=lambda row: (
            _int_or_none(row.get("start_time_ns")) or 0,
            _int_or_none(row.get("segment_index")) or 0,
        ),
    )
    aggregate = dict(ordered[0])
    aggregate["event_id"] = event_id
    aggregate["segment_count"] = len(ordered)
    aggregate["segment_event_ids"] = [str(row.get("event_id", "")) for row in ordered]
    aggregate["segments"] = [dict(row) for row in ordered]
    if starts and all(value is not None for value in starts):
        aggregate["start_time_ns"] = min(value for value in starts if value is not None)
    if ends and all(value is not None for value in ends):
        aggregate["end_time_ns"] = max(value for value in ends if value is not None)
    return aggregate, errors


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _rotl32(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (32 - shift))) & UINT32_MASK


def ns3_murmur3_x86_32(data: bytes, seed: int = MURMUR3_SEED_U32) -> int:
    """Independently mirror ns-3's Murmur3 Hash32 for complete u32 blocks."""
    if len(data) % 4:
        raise ValueError("RDMA hash input must contain complete uint32 blocks")
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
    """Return the integer form of the true-16 address rule 11.0.<rank>.1."""
    if not 0 <= rank < 256:
        raise ValueError(f"rank cannot be represented by P2 address rule: {rank}")
    return 0x0B000001 + (rank << 8)


def rdma_route_bucket(
    *, src: int, dst: int, sport: int, dport: int, reverse: bool = False,
) -> int:
    sip, dip = rank_ipv4_u32(src), rank_ipv4_u32(dst)
    if reverse:
        sip, dip, sport, dport = dip, sip, dport, sport
    return ns3_murmur3_x86_32(
        struct.pack("<IIHH", sip, dip, sport, dport)
    ) % 2


def _background_truth_contract(
    run: Mapping[str, Any], truth_row: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Parse the hash-locked single-rail/background transport contract.

    This evaluator deliberately reimplements the contract instead of trusting
    either the corpus generator's verifier or the runner's semantic JSON.
    """
    run_id = str(run.get("run_id", ""))
    errors: List[str] = []
    segment_count = _int_or_none(truth_row.get("segment_count"))
    if segment_count is not None and segment_count != 1:
        errors.append(
            f"background truth must contain one row, observed={segment_count}"
        )
    raw = truth_row.get("action_parameters_json")
    try:
        value = json.loads(str(raw)) if raw not in (None, "") else None
    except json.JSONDecodeError as error:
        value = None
        errors.append(f"background action_parameters_json invalid: {error}")
    if not isinstance(value, Mapping):
        errors.append("background action_parameters_json must be an object")
        value = {}
    missing = sorted(BACKGROUND_TRUTH_REQUIRED - set(value))
    if missing:
        errors.append(f"background truth fields missing={missing}")

    def integer(name: str) -> int | None:
        parsed = _int_or_none(value.get(name))
        if parsed is None:
            errors.append(f"background truth {name} is not an integer")
        return parsed

    destination = integer("destination_rank")
    route_bucket = integer("route_bucket")
    hash_seed = integer("hash_seed_u32")
    completion_deadline = integer("completion_deadline_ns")
    rto_us = integer("rdma_rto_us")
    retry_limit = integer("rdma_retry_limit")
    max_retries = integer("max_rto_retry_events")
    truth_start = _int_or_none(truth_row.get("start_time_ns"))
    truth_end = _int_or_none(truth_row.get("end_time_ns"))
    virtual_finish = _int_or_none(run.get("virtual_finish_ns"))
    try:
        host_ports = [int(item) for item in value.get(
            "route_candidate_order_host_ports", [])]
    except (TypeError, ValueError):
        host_ports = []
    if host_ports != [2, 3]:
        errors.append(f"route candidate host ports must be [2, 3], got {host_ports}")

    target_link = str(value.get("bottleneck_access_link_id", ""))
    paired_link = str(value.get("paired_access_link_id", ""))
    data_plane = str(value.get("data_plane", ""))
    expected_host_port = None
    if route_bucket in {0, 1} and len(host_ports) == 2:
        expected_host_port = host_ports[route_bucket]
    if destination != _int_or_none(run.get("target_gpu")):
        errors.append(
            "background destination differs from manifest: "
            f"truth={destination}, manifest={run.get('target_gpu')}"
        )
    if target_link != str(run.get("target_link_id", "")):
        errors.append("background bottleneck link differs from manifest")
    if paired_link != str(run.get("paired_link_id", "")):
        errors.append("background paired link differs from manifest")
    if not target_link or not paired_link or target_link == paired_link:
        errors.append("background ACCESS target pair is empty or not distinct")
    if destination is None or not 0 <= destination < 16:
        errors.append(f"background destination is outside true-16: {destination}")
    if route_bucket not in {0, 1}:
        errors.append(f"background route_bucket must be 0/1: {route_bucket}")
    if data_plane not in {"A", "B"} or expected_host_port != {
            "A": 2, "B": 3}.get(data_plane):
        errors.append(
            "data plane/route bucket/host-port mapping is inconsistent: "
            f"plane={data_plane}, bucket={route_bucket}, port={expected_host_port}"
        )
    exact_fields = {
        "hash_algorithm": "ns3-murmur3-x86-32",
        "hash_tuple": "native-le-sip-dip-sport-dport",
        "hash_byte_order": "little",
        "predeclared_window_policy": "scheduled_qp_launch_window",
        "realized_window_policy": "first_data_tx_to_last_ack_complete",
    }
    for name, expected in exact_fields.items():
        if value.get(name) != expected:
            errors.append(
                f"background truth {name}={value.get(name)!r}, expected={expected!r}"
            )
    if hash_seed != MURMUR3_SEED_U32:
        errors.append(
            f"background hash seed={hash_seed}, expected={MURMUR3_SEED_U32}"
        )
    if value.get("pin_reverse_ack") is not True:
        errors.append("background truth must pin reverse ACK rail")
    if truth_start is None or truth_end is None or not truth_start < truth_end:
        errors.append(
            f"background truth interval invalid: start={truth_start}, end={truth_end}"
        )
    if (completion_deadline is None or truth_end is None
            or completion_deadline < truth_end):
        errors.append(
            "background completion deadline precedes launch interval: "
            f"deadline={completion_deadline}, end={truth_end}"
        )
    if (virtual_finish is None or completion_deadline is None
            or completion_deadline > virtual_finish):
        errors.append(
            "background completion deadline exceeds observation horizon: "
            f"deadline={completion_deadline}, finish={virtual_finish}"
        )
    if rto_us is None or rto_us <= 0:
        errors.append(f"background rdma_rto_us must be positive: {rto_us}")
    if retry_limit != 0 or max_retries != 0:
        errors.append(
            "background baseline must forbid RTO retries: "
            f"retry_limit={retry_limit}, max_events={max_retries}"
        )
    contract = dict(value)
    contract.update({
        "destination_rank": destination,
        "route_bucket": route_bucket,
        "route_candidate_order_host_ports": host_ports,
        "expected_host_port": expected_host_port,
        "hash_seed_u32": hash_seed,
        "completion_deadline_ns": completion_deadline,
        "rdma_rto_us": rto_us,
        "rdma_retry_limit": retry_limit,
        "max_rto_retry_events": max_retries,
        "truth_start_ns": truth_start,
        "truth_end_ns": truth_end,
        "virtual_finish_ns": virtual_finish,
        "run_id": run_id,
    })
    return contract, errors


def _event_times(
    run: Mapping[str, Any], schedule_row: Mapping[str, Any],
) -> Tuple[int | None, int | None]:
    start = _int_or_none(
        schedule_row.get("start_time_ns", schedule_row.get("fault_applied_ns"))
    )
    end = _int_or_none(schedule_row.get("end_time_ns"))
    if start is None:
        start = _int_or_none(run.get("fault_applied_ns", run.get("event_start_ns")))
    if end is None:
        duration = _int_or_none(run.get("duration_ns"))
        end = start + duration if start is not None and duration is not None else None
    return start, end


def load_feature_schema(
    corpus: Mapping[str, Any], base_dir: Path, executed: bool,
) -> Tuple[Mapping[str, Any], bool, Dict[str, Any]]:
    feature = corpus.get("feature_schema", {})
    if not isinstance(feature, Mapping):
        return {}, False, {"reason": "feature_schema is not an object"}
    provenance: Dict[str, Any] = {"inline": True}
    if feature.get("path"):
        ok, provenance = verify_ref(feature, base_dir)
        if not ok:
            return {}, False, provenance
        external = _load_json(_ref_path(feature, base_dir))
        merged = dict(external)
        merged.update({key: value for key, value in feature.items()
                       if key not in {"path", "sha256", "size_bytes"}})
        return merged, True, provenance
    return feature, not executed, provenance


def forbidden_feature_columns(columns: Iterable[Any]) -> List[str]:
    bad = []
    for value in columns:
        column = str(value).strip().lower()
        if (column in FORBIDDEN_FEATURE_EXACT
                or any(fragment in column for fragment in FORBIDDEN_FEATURE_FRAGMENTS)):
            bad.append(str(value))
    return sorted(set(bad))


def normalize_split_entries(split: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    raw_entries = split.get("entries", split.get("runs"))
    entries: List[Dict[str, Any]] = []
    errors: List[str] = []
    if isinstance(raw_entries, list):
        for item in raw_entries:
            if not isinstance(item, Mapping):
                errors.append("split entry is not an object")
                continue
            entries.append(dict(item))
    elif isinstance(split.get("partitions"), Mapping):
        for partition, run_ids in split["partitions"].items():
            if not isinstance(run_ids, list):
                errors.append(f"partition {partition} is not a list")
                continue
            entries.extend({"run_id": run_id, "partition": partition}
                           for run_id in run_ids)
    else:
        errors.append("split manifest has no entries/runs/partitions mapping")
    return entries, errors


def expected_plane_b_targets(link_map_path: Path) -> Tuple[Dict[int, str], List[str]]:
    frame = pd.read_csv(link_map_path)
    required = {"link_id", "src_node", "dst_node", "src_type", "dst_type",
                "src_port", "dst_port", "link_class"}
    missing = sorted(required - set(frame.columns))
    if missing:
        return {}, [f"link_map missing columns {missing}"]
    targets: Dict[int, str] = {}
    errors: List[str] = []
    for row in frame[frame["link_class"] == "ACCESS"].itertuples(index=False):
        if row.src_type == "HOST":
            gpu, port = int(row.src_node), int(row.src_port)
        elif row.dst_type == "HOST":
            gpu, port = int(row.dst_node), int(row.dst_port)
        else:
            continue
        if port != 3:
            continue
        if gpu in targets:
            errors.append(f"GPU {gpu} has duplicate Plane-B targets")
        targets[gpu] = str(row.link_id)
    if set(targets) != set(range(16)):
        errors.append(f"expected GPUs 0..15, observed={sorted(targets)}")
    if len(set(targets.values())) != 16:
        errors.append("Plane-B target link ids are not unique")
    return targets, errors


def inspect_background_schedule(
    run: Mapping[str, Any], schedule: Mapping[str, Any], base_dir: Path,
    truth_row: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    scenario = str(run.get("scenario", ""))
    supported = scenario in EXECUTABLE_BACKGROUND_SCENARIOS
    executable = (
        schedule.get("implementation_status") == "EXECUTABLE_BACKGROUND_RDMA"
    )
    ref = schedule.get("background_flow_schedule")
    errors: List[str] = []
    if not executable:
        if ref is not None:
            errors.append("non-executable congestion scenario has background executor")
        return {}, errors
    if not supported:
        errors.append("unsupported congestion scenario declares background RDMA")
    if str(truth_row.get("action_scope", "")) != "workload":
        errors.append("background RDMA truth must use workload action_scope")
    declared_status = str(truth_row.get("implementation_status", ""))
    if (declared_status and declared_status
            != str(schedule.get("implementation_status", ""))):
        errors.append("background truth implementation status differs from manifest")
    ok, evidence = verify_ref(ref, base_dir)
    if not ok:
        errors.append("background-flow schedule reference/hash invalid")
        return evidence, errors
    path = _ref_path(ref, base_dir)
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            fields = tuple(reader.fieldnames or ())
            rows = list(reader)
    except (OSError, csv.Error, UnicodeError) as error:
        errors.append(f"background-flow schedule unreadable: {error}")
        return evidence, errors
    if fields != BACKGROUND_FLOW_COLUMNS:
        errors.append(f"background-flow header invalid: {fields}")
        return evidence, errors
    contract, contract_errors = _background_truth_contract(run, truth_row)
    errors.extend(contract_errors)
    expected_count = 12 if scenario == "incast" else 24
    try:
        event_id = str(schedule.get("event_id", ""))
        destinations = {int(row["dst_rank"]) for row in rows}
        destination = next(iter(destinations)) if len(destinations) == 1 else -1
        expected_sources = {
            rank for rank in range(16)
            if destination >= 0 and rank // 4 != destination // 4
        }
        starts = sorted({int(row["scheduled_start_ns"]) for row in rows})
        sources_by_start = {
            start: {
                int(row["src_rank"]) for row in rows
                if int(row["scheduled_start_ns"]) == start
            }
            for start in starts
        }
        counts_by_start = {
            start: sum(int(row["scheduled_start_ns"]) == start for row in rows)
            for start in starts
        }
        truth_start = int(truth_row["start_time_ns"])
        truth_end = int(truth_row["end_time_ns"])
        route_bucket = int(contract["route_bucket"])
        destination_contract = int(contract["destination_rank"])
        row_contract = all(
            row["event_id"] == event_id and row["scenario"] == scenario
            and truth_start <= int(row["scheduled_start_ns"]) < truth_end
            and int(row["src_rank"]) // 4 != int(row["dst_rank"]) // 4
            and int(row["bytes"]) > 0 and 1 <= int(row["pg"]) <= 7
            and 49152 <= int(row["sport"]) <= 65535
            and int(row["dport"]) > 0
            for row in rows
        )
        launch_window_exact = (
            bool(starts) and starts[0] == truth_start
            and starts[-1] + 1 == truth_end
        )
        hash_pin_valid = all(
            rdma_route_bucket(
                src=int(row["src_rank"]), dst=int(row["dst_rank"]),
                sport=int(row["sport"]), dport=int(row["dport"]),
            ) == route_bucket
            and rdma_route_bucket(
                src=int(row["src_rank"]), dst=int(row["dst_rank"]),
                sport=int(row["sport"]), dport=int(row["dport"]),
                reverse=True,
            ) == route_bucket
            for row in rows
        )
        unique_flows = len({row["flow_id"] for row in rows}) == len(rows)
        unique_qps = len({
            (row["src_rank"], row["dst_rank"], row["sport"], row["pg"])
            for row in rows
        }) == len(rows)
        common_profile = (
            len({row["bytes"] for row in rows}) == 1
            and len({row["pg"] for row in rows}) == 1
            and len({row["dport"] for row in rows}) == 1
        )
        if scenario == "incast":
            profile = (
                len(starts) == 1
                and sources_by_start.get(starts[0], set()) == expected_sources
                and counts_by_start.get(starts[0]) == 12
            )
        else:
            profile = (
                len(starts) == 2 and starts[1] > starts[0]
                and all(
                    sources_by_start.get(start, set()) == expected_sources
                    and counts_by_start.get(start) == 12 for start in starts
                )
            )
        valid = (
            len(rows) == expected_count and row_contract and unique_flows
            and unique_qps and common_profile and profile
            and not contract_errors and destination == destination_contract
            and launch_window_exact and hash_pin_valid
        )
    except (KeyError, TypeError, ValueError):
        valid = False
        destination = -1
        starts = []
        launch_window_exact = False
        hash_pin_valid = False
    if not valid:
        errors.append(
            f"background {scenario} profile invalid: rows={len(rows)}, starts={starts}"
        )
    evidence.update(
        {
            "scenario": scenario,
            "row_count": len(rows),
            "destination_rank": destination,
            "scheduled_start_times_ns": starts,
            "launch_window_exact": launch_window_exact,
            "forward_and_reverse_hash_pin_valid": hash_pin_valid,
            "contract": contract,
            "profile_valid": valid,
        }
    )
    return evidence, errors


def _recompute_collective_override(
    run: Mapping[str, Any],
    schedule: Mapping[str, Any],
    base_dir: Path,
    truth_row: Mapping[str, Any],
    input_artifacts: Mapping[str, Any],
) -> Tuple[Mapping[str, Any], Dict[str, Any], List[str]]:
    """Rebuild one executable A1 workload/role pair from frozen inputs."""

    scenario = str(run.get("scenario", ""))
    collective = schedule.get("collective_workload_override")
    errors: List[str] = []
    evidence: Dict[str, Any] = {"scenario": scenario}
    if scenario not in corpus_generator.EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS:
        if collective is not None:
            errors.append("non-A1 congestion run declares a collective override")
        return {}, evidence, errors
    declares_executable = (
        schedule.get("implementation_status")
        == corpus_generator.COLLECTIVE_OVERRIDE_STATUS
    )
    if not declares_executable and collective is None:
        evidence["status"] = "LEGACY_PENDING_EXECUTOR"
        return {}, evidence, errors
    if not isinstance(collective, Mapping):
        return {}, evidence, ["A1 congestion run lacks collective override"]
    role_ref = collective.get("layer_role_sidecar")
    if not isinstance(role_ref, Mapping):
        return {}, evidence, ["A1 collective override lacks layer-role sidecar"]
    try:
        workload_ref = input_artifacts["workload"]
        link_map_ref = input_artifacts["link_map"]
        contract_ref = input_artifacts["contract"]
        if not all(
            isinstance(ref, Mapping)
            for ref in (workload_ref, link_map_ref, contract_ref)
        ):
            raise ValueError("A1 frozen input references are invalid")
        workload_path = _ref_path(workload_ref, base_dir).resolve()
        link_map_path = _ref_path(link_map_ref, base_dir).resolve()
        contract_path = _ref_path(contract_ref, base_dir).resolve()
        override_path = _ref_path(collective, base_dir).resolve()
        role_path = _ref_path(role_ref, base_dir).resolve()
        onset_ns = int(truth_row["start_time_ns"])
        pairs, _ = corpus_generator.load_true16_links(
            link_map_path, corpus_generator.load_contract(contract_path)
        )
        aggregate_bandwidths = {
            sum(int(link["bandwidth_bps"]) for link in rails.values())
            for rails in pairs.values()
        }
        if len(aggregate_bandwidths) != 1:
            raise ValueError("A1 ACCESS bandwidth is not uniform")
        contract = corpus_generator.collective_override_contract(
            scenario,
            onset_ns,
            source_workload_sha256=str(workload_ref.get("sha256", "")),
            aggregate_access_bandwidth_bps=next(iter(aggregate_bandwidths)),
        )
        recomputed = corpus_generator.validate_collective_override(
            override_path,
            role_path,
            contract,
            workload_path,
            str(run.get("run_id", "")),
        )
        expected_reference = {
            "kind": "simai_collective_workload_override",
            "format": corpus_generator.COLLECTIVE_OVERRIDE_FORMAT,
            "path": collective.get("path"),
            "sha256": recomputed["sha256"],
            "generated_before_run": True,
            "safe_to_execute": True,
            "runtime_executor_status": "READY",
            "layer_role_sidecar": {
                "kind": "collective_layer_roles",
                "format": corpus_generator.COLLECTIVE_ROLE_FORMAT,
                "path": role_ref.get("path"),
                "sha256": recomputed["role_sha256"],
                "generated_before_run": True,
            },
            "static_validation": recomputed,
            "static_validation_sha256": _canonical_hash(recomputed),
        }
        if dict(collective) != expected_reference:
            errors.append(
                "A1 collective reference differs from independent reconstruction"
            )
        if schedule.get(
            "implementation_status"
        ) != corpus_generator.COLLECTIVE_OVERRIDE_STATUS:
            errors.append("A1 collective schedule is not executable")
        if run.get("effective_workload_sha256") != recomputed["sha256"]:
            errors.append("A1 run identity does not bind the effective workload")
        evidence.update({
            "workload": {
                "path": str(override_path),
                "sha256": recomputed["sha256"],
            },
            "layer_roles": {
                "path": str(role_path),
                "sha256": recomputed["role_sha256"],
            },
            "qualification_profile": recomputed["qualification_profile"],
            "role_counts": recomputed["role_counts"],
            "static_validation_sha256": _canonical_hash(recomputed),
        })
        return recomputed, evidence, errors
    except (
        OSError,
        csv.Error,
        KeyError,
        TypeError,
        ValueError,
        corpus_generator.CorpusError,
    ) as error:
        errors.append(f"A1 collective override cannot be reconstructed: {error}")
        return {}, evidence, errors


def inspect_schedules(
    runs: Sequence[Mapping[str, Any]],
    base_dir: Path,
    input_artifacts: Mapping[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    evidence: List[Dict[str, Any]] = []
    global_errors: List[str] = []
    for run in runs:
        run_id = str(run.get("run_id", ""))
        class_label = _normal_class(run.get("class_label"))
        schedule = run.get("schedule", {})
        errors: List[str] = []
        if not isinstance(schedule, Mapping):
            schedule = {}
            errors.append("schedule is not an object")
        ok, ref_evidence = verify_ref(schedule, base_dir)
        row: Mapping[str, Any] = {}
        injection_evidence: Dict[str, Any] = {}
        background_evidence: Dict[str, Any] = {}
        collective_evidence: Dict[str, Any] = {}
        if not ok:
            errors.append("schedule reference/hash invalid")
        else:
            try:
                row, row_errors = _schedule_row(run, _ref_path(schedule, base_dir))
                errors.extend(row_errors)
            except (OSError, ValueError, json.JSONDecodeError) as error:
                errors.append(str(error))
        kind = str(schedule.get("kind", "")).lower()
        if kind.endswith("_schedule"):
            kind = kind.removesuffix("_schedule")
        truth = str(schedule.get("truth_source", ""))
        independent = schedule.get("independent_of_features") is True
        generated_before = schedule.get("generated_before_run") is True
        if class_label == "GRAY_FAULT" or class_label == "HARD_FAULT":
            if kind != "fault" or truth != "predeclared_fault_schedule":
                errors.append(f"fault truth contract invalid: kind={kind}, source={truth}")
            if str(row.get("fault_family", run.get("fault_family", ""))) != str(
                    run.get("fault_family", "")):
                errors.append("schedule fault_family differs from manifest")
            manifest_targets = _target_links(run)
            truth_targets = set()
            truth_target = str(row.get("target_link_id", "") or "").strip()
            if truth_target:
                truth_targets.add(truth_target)
            targets_json = row.get("targets_json")
            if targets_json:
                try:
                    decoded_targets = json.loads(str(targets_json))
                    if isinstance(decoded_targets, list):
                        truth_targets.update(
                            str(item.get("target_link_id", item.get("link_id", ""))).strip()
                            for item in decoded_targets if isinstance(item, Mapping)
                        )
                        truth_targets.discard("")
                except json.JSONDecodeError:
                    errors.append("fault truth targets_json is not valid JSON")
            if manifest_targets != truth_targets:
                errors.append(
                    "schedule targets differ from manifest: "
                    f"truth={sorted(truth_targets)}, manifest={sorted(manifest_targets)}"
                )
            injector = schedule.get("simulator_injection_schedule")
            if injector is not None:
                injection_ok, injection_evidence = verify_ref(injector, base_dir)
                if not injection_ok:
                    errors.append("simulator injection schedule reference/hash invalid")
                else:
                    try:
                        injection_rows = _read_schedule(_ref_path(injector, base_dir))
                        parent = str(schedule.get("event_id", ""))
                        bad_parent = [
                            str(item.get("event_id", "")) for item in injection_rows
                            if str(item.get("parent_event_id", "")) != parent
                        ]
                        injection_targets = {
                            str(item.get("target_link_id", "")).strip()
                            for item in injection_rows
                            if str(item.get("target_link_id", "")).strip()
                        }
                        if not injection_rows or bad_parent:
                            errors.append(
                                f"injector segments do not bind parent={parent}: {bad_parent[:8]}"
                            )
                        if injection_targets != manifest_targets:
                            errors.append(
                                "injector targets differ from manifest: "
                                f"injector={sorted(injection_targets)}, "
                                f"manifest={sorted(manifest_targets)}"
                            )
                        injection_evidence["segment_count"] = len(injection_rows)
                    except (OSError, ValueError, json.JSONDecodeError) as error:
                        errors.append(f"simulator injection schedule unreadable: {error}")
        elif class_label == "CONGESTION":
            if kind != "congestion" or truth not in ALLOWED_CONGESTION_TRUTH:
                errors.append(f"congestion truth contract invalid: kind={kind}, source={truth}")
            if any(str(row.get(field, "")).strip() for field in (
                    "fault_family", "fault_type", "target_link_id", "fault_active")):
                errors.append("congestion schedule contains physical-fault fields")
            if str(row.get("scenario", "")) != str(run.get("scenario", "")):
                errors.append("congestion scenario differs from manifest")
            background_evidence, background_errors = inspect_background_schedule(
                run, schedule, base_dir, row
            )
            errors.extend(background_errors)
            _, collective_evidence, collective_errors = (
                _recompute_collective_override(
                    run,
                    schedule,
                    base_dir,
                    row,
                    input_artifacts or {},
                )
            )
            errors.extend(collective_errors)
        elif class_label == "HEALTHY":
            if kind != "healthy" or truth != "predeclared_no_fault":
                errors.append(f"healthy truth contract invalid: kind={kind}, source={truth}")
        else:
            errors.append(f"unknown class_label={class_label}")
        if not independent or not generated_before:
            errors.append(
                f"schedule provenance invalid: independent={independent}, "
                f"generated_before_run={generated_before}"
            )
        start, end = _event_times(run, row)
        evidence.append({
            "run_id": run_id,
            "class_label": class_label,
            "kind": kind,
            "truth_source": truth,
            "schedule": ref_evidence,
            "simulator_injection_schedule": injection_evidence,
            "background_flow_schedule": background_evidence,
            "collective_workload_override": collective_evidence,
            "event_id": schedule.get("event_id"),
            "event_start_ns": start,
            "event_end_ns": end,
            "schedule_row": dict(row),
            "status": PASS if not errors else FAIL,
            "errors": errors,
        })
        global_errors.extend(f"{run_id}: {error}" for error in errors)
    return evidence, global_errors


def inspect_mechanism_contracts(
    runs: Sequence[Mapping[str, Any]], mode: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Check planned mechanism provenance without treating plans as evidence."""
    evidence: List[Dict[str, Any]] = []
    errors: List[str] = []
    allowed_planned = {"PLANNED", "READY", "UNAVAILABLE", "VERIFIED"}
    for run in runs:
        run_id = str(run.get("run_id", ""))
        mechanism = run.get("mechanism", {})
        run_errors: List[str] = []
        if not isinstance(mechanism, Mapping):
            mechanism = {}
            run_errors.append("mechanism is not an object")
        mechanism_id = str(mechanism.get("mechanism_id", "")).strip()
        implementation_status = str(
            mechanism.get("implementation_status", "")
        ).upper()
        if not mechanism_id or mechanism_id == "unassigned":
            run_errors.append("mechanism_id is missing/unassigned")
        if mode == PREPARED:
            if implementation_status not in allowed_planned:
                run_errors.append(
                    f"invalid prepared implementation_status={implementation_status}"
                )
        elif str(run.get("execution_status", "")).upper() == "COMPLETE":
            if implementation_status != "VERIFIED":
                run_errors.append(
                    "COMPLETE run requires mechanism implementation_status=VERIFIED"
                )
            if "proxy" in mechanism_id.lower():
                run_errors.append(
                    f"proxy mechanism {mechanism_id!r} cannot satisfy executed semantics"
                )
        evidence.append({
            "run_id": run_id,
            "mechanism_id": mechanism_id,
            "implementation_status": implementation_status,
            "status": PASS if not run_errors else FAIL,
            "errors": run_errors,
        })
        errors.extend(f"{run_id}: {error}" for error in run_errors)
    return evidence, errors


def _target_links(run: Mapping[str, Any]) -> set[str]:
    links = set()
    direct = str(run.get("target_link_id", "") or "").strip()
    if direct:
        links.add(direct)
    targets = run.get("targets", [])
    if isinstance(targets, list):
        for target in targets:
            if isinstance(target, Mapping):
                link = str(target.get("target_link_id", target.get("link_id", ""))).strip()
                if link:
                    links.add(link)
    return links


def _switch_telemetry_evidence(
    run: Mapping[str, Any], path: Path,
    schedule: Mapping[str, Any],
    *, runtime_cadence_qualified: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """Recompute causal warmup and event visibility from the hashed raw trace."""
    run_id = str(run.get("run_id", ""))
    errors: List[str] = []
    try:
        frame = pd.read_csv(path)
    except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        return {}, [f"switch_telemetry unreadable: {error}"]
    required = {"run_id", "timestamp_ns", "link_id", "direction", "tx_bytes"}
    missing = sorted(required - set(frame.columns))
    if frame.empty or missing:
        return {}, [f"switch_telemetry empty or missing columns={missing}"]
    observed_run_ids = set(frame["run_id"].astype(str).unique())
    if observed_run_ids != {run_id}:
        errors.append(f"switch telemetry run_ids={sorted(observed_run_ids)}")
    frame = frame.copy()
    frame["timestamp_ns"] = pd.to_numeric(frame["timestamp_ns"], errors="coerce")
    if frame["timestamp_ns"].isna().any():
        errors.append("switch telemetry has non-numeric timestamp_ns")
        frame = frame.dropna(subset=["timestamp_ns"])
    frame["timestamp_ns"] = frame["timestamp_ns"].astype("int64")
    tx = frame[frame["direction"].astype(str).str.lower() == "tx"].copy()
    targets = _target_links(run)
    if targets:
        target_tx = tx[tx["link_id"].astype(str).isin(targets)].copy()
        if target_tx.empty:
            errors.append(f"no TX telemetry for target links={sorted(targets)}")
    else:
        target_tx = tx
    start = _int_or_none(schedule.get("event_start_ns"))
    if _normal_class(run.get("class_label")) == "HEALTHY":
        warmup = target_tx
    elif start is None:
        warmup = target_tx.iloc[0:0]
        errors.append("event run lacks resolved event_start_ns")
    else:
        warmup = target_tx[target_tx["timestamp_ns"] < start]
    timestamps = sorted(int(value) for value in warmup["timestamp_ns"].unique())
    span_ns = timestamps[-1] - timestamps[0] if len(timestamps) >= 2 else 0
    tx_values = pd.to_numeric(warmup["tx_bytes"], errors="coerce")
    if tx_values.isna().any():
        errors.append("warmup tx_bytes contains non-numeric values")
    warmup = warmup.assign(_tx_bytes=tx_values.fillna(0))
    totals = warmup.groupby("timestamp_ns")["_tx_bytes"].sum().sort_index()
    traffic_growth = bool(
        len(totals) >= 2
        and float(totals.iloc[-1]) > float(totals.iloc[0])
        and (totals.diff().fillna(0) > 0).any()
    )
    warmup_ok = (
        traffic_growth
        and (
            runtime_cadence_qualified
            or (len(timestamps) >= 101 and span_ns >= CAUSAL_WARMUP_NS)
        )
    )
    if not warmup_ok:
        errors.append(
            "traffic-bearing causal warmup invalid: "
            f"snapshots={len(timestamps)}, span_ns={span_ns}, "
            f"tx_counter_growth={traffic_growth}"
        )

    end = _int_or_none(schedule.get("event_end_ns"))
    event = target_tx.iloc[0:0]
    if start is not None:
        event = target_tx[target_tx["timestamp_ns"] >= start]
        if end is not None:
            event = event[event["timestamp_ns"] <= end]
    effect_candidates: List[Tuple[int, str]] = []
    for column in ("dropped_packets", "link_errors", "flap_count", "ecn_marks", "pfc_events"):
        if column not in target_tx.columns or warmup.empty or event.empty:
            continue
        before = pd.to_numeric(warmup[column], errors="coerce").dropna()
        after = pd.to_numeric(event[column], errors="coerce").dropna()
        if not before.empty and not after.empty and after.max() > before.max():
            changed = event[pd.to_numeric(event[column], errors="coerce") > before.max()]
            effect_candidates.append((int(changed["timestamp_ns"].min()), column))
    if "link_state" in event.columns:
        changed = event[event["link_state"].astype(str).str.lower() != "up"]
        if not changed.empty:
            effect_candidates.append((int(changed["timestamp_ns"].min()), "link_state"))
    if "queue_bytes" in target_tx.columns and not warmup.empty and not event.empty:
        before = pd.to_numeric(warmup["queue_bytes"], errors="coerce").dropna()
        after = pd.to_numeric(event["queue_bytes"], errors="coerce").dropna()
        if not before.empty and not after.empty and after.max() > before.max():
            changed = event[pd.to_numeric(event["queue_bytes"], errors="coerce") > before.max()]
            effect_candidates.append((int(changed["timestamp_ns"].min()), "queue_bytes"))
    first_effect = min((item[0] for item in effect_candidates), default=None)
    return {
        "snapshot_count_before_event": len(timestamps),
        "warmup_span_ns": span_ns,
        "tx_counter_growth": traffic_growth,
        "traffic_bearing_warmup": warmup_ok,
        "target_links": sorted(targets),
        "observable": bool(effect_candidates),
        "first_observable_effect_ns": first_effect,
        "observed_signal_changes": sorted({item[1] for item in effect_candidates}),
    }, errors


def _lifecycle_evidence(
    run: Mapping[str, Any], path: Path,
) -> Tuple[Dict[str, Any], List[str]]:
    """Validate that a run reached its predeclared observation horizon.

    P2 is a detection-only stage.  A hard failure may intentionally leave the
    AllReduce incomplete, so simulator exit code zero alone is insufficient
    and collective completion is not required.  The lifecycle sidecar must
    instead prove that the complete, immutable observation interval ran.
    """

    errors: List[str] = []
    try:
        frame = pd.read_csv(path)
    except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        return {}, [f"run_lifecycle unreadable: {error}"]
    required = {
        "run_id", "event", "scheduled_ns", "actual_ns", "finished_ranks",
        "world_size", "status",
    }
    missing = sorted(required - set(frame.columns))
    if frame.empty or missing:
        return {}, [f"run_lifecycle empty or missing columns={missing}"]
    run_id = str(run.get("run_id", ""))
    observed_ids = set(frame["run_id"].astype(str).unique())
    if observed_ids != {run_id}:
        errors.append(f"run lifecycle run_ids={sorted(observed_ids)}")
    terminal = frame[
        (frame["event"].astype(str) == "observation_horizon")
        & frame["status"].astype(str).isin({
            "OBSERVATION_WINDOW_COMPLETE_AFTER_WORKLOAD",
            "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE",
        })
    ].copy()
    if terminal.empty:
        errors.append(
            "run lifecycle has no valid observation_horizon completion event"
        )
        return {"row_count": len(frame)}, errors
    if len(terminal) != 1:
        errors.append(
            f"run lifecycle has {len(terminal)} observation_horizon completion rows"
        )
    terminal["actual_ns"] = pd.to_numeric(
        terminal["actual_ns"], errors="coerce"
    )
    terminal["scheduled_ns"] = pd.to_numeric(
        terminal["scheduled_ns"], errors="coerce"
    )
    terminal["world_size"] = pd.to_numeric(
        terminal["world_size"], errors="coerce"
    )
    if terminal[["scheduled_ns", "actual_ns", "world_size"]].isna().any().any():
        errors.append("run lifecycle observation timing/world_size is non-numeric")
        return {"row_count": len(frame)}, errors
    row = terminal.iloc[0]
    scheduled_ns = int(row["scheduled_ns"])
    actual_ns = int(row["actual_ns"])
    world_size = int(row["world_size"])
    planned_finish_ns = _int_or_none(run.get("virtual_finish_ns"))
    if planned_finish_ns is None:
        errors.append("run lacks virtual_finish_ns")
    elif scheduled_ns != planned_finish_ns or actual_ns != planned_finish_ns:
        errors.append(
            "observation_horizon does not exactly match virtual_finish_ns: "
            f"scheduled_ns={scheduled_ns}, actual_ns={actual_ns}, "
            f"planned_finish_ns={planned_finish_ns}"
        )
    if world_size != 16:
        errors.append(f"run lifecycle world_size={world_size}, expected=16")
    return {
        "row_count": len(frame),
        "terminal_event": str(row["event"]),
        "terminal_status": str(row["status"]),
        "scheduled_ns": scheduled_ns,
        "actual_ns": actual_ns,
        "planned_finish_ns": planned_finish_ns,
        "finished_ranks": _int_or_none(row["finished_ranks"]),
        "world_size": world_size,
        "observation_horizon_complete": not errors,
        "workload_completed": bool(
            (frame["status"].astype(str) == "WORKLOAD_COMPLETE").any()
        ),
    }, errors


def _background_application_evidence(
    run: Mapping[str, Any], application_path: Path, schedule_path: Path,
    contract: Mapping[str, Any] | None = None,
) -> Tuple[Dict[str, Any], List[str]]:
    errors: List[str] = []
    try:
        with schedule_path.open(newline="", encoding="utf-8") as source:
            declared_reader = csv.DictReader(source)
            declared_rows = list(declared_reader)
        with application_path.open(newline="", encoding="utf-8") as source:
            observed_reader = csv.DictReader(source)
            observed_rows = list(observed_reader)
    except (OSError, csv.Error, UnicodeError) as error:
        return {}, [f"background application/schedule unreadable: {error}"]
    if tuple(declared_reader.fieldnames or ()) != BACKGROUND_FLOW_COLUMNS:
        errors.append("background schedule header changed after schedule inspection")
    if not BACKGROUND_APPLICATION_REQUIRED.issubset(observed_reader.fieldnames or []):
        errors.append("background application lifecycle columns incomplete")
    declared = {row.get("flow_id", ""): row for row in declared_rows}
    if "" in declared or len(declared) != len(declared_rows):
        errors.append("background schedule flow IDs are empty/duplicate")
    grouped: Dict[str, List[Mapping[str, str]]] = defaultdict(list)
    run_id = str(run.get("run_id", ""))
    for row in observed_rows:
        if row.get("run_id") != run_id:
            errors.append("background application contains foreign run_id")
        grouped[str(row.get("flow_id", ""))].append(row)
    if set(grouped) != set(declared):
        errors.append(
            "background application flow set differs from schedule: "
            f"declared={len(declared)}, observed={len(grouped)}"
        )
    completed = 0
    latest_complete: int | None = None
    first_txs: List[int] = []
    lifecycles: List[Dict[str, int]] = []
    metadata = (
        "event_id", "flow_id", "scenario", "scheduled_start_ns", "src_rank",
        "dst_rank", "bytes", "pg", "sport", "dport",
    )
    for flow_id, expected in declared.items():
        rows = grouped.get(flow_id, [])
        events = [str(row.get("event", "")) for row in rows]
        if len(rows) != 3 or sorted(events) != ["COMPLETE", "SCHEDULED", "START"]:
            errors.append(f"{flow_id}: lifecycle events={events}")
            continue
        if any(row.get(key) != expected.get(key) for row in rows for key in metadata):
            errors.append(f"{flow_id}: lifecycle metadata differs from schedule")
            continue
        by_event = {str(row["event"]): row for row in rows}
        try:
            scheduled = int(expected["scheduled_start_ns"])
            started = int(by_event["START"]["actual_ns"])
            first_tx = int(by_event["COMPLETE"]["first_tx_ns"])
            first_ack = int(by_event["COMPLETE"]["first_ack_ns"])
            complete = int(by_event["COMPLETE"]["actual_ns"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"{flow_id}: lifecycle timestamps invalid")
            continue
        causal = scheduled <= started <= first_tx <= first_ack <= complete
        statuses = (
            by_event["SCHEDULED"].get("status") == "INSTALLED"
            and by_event["START"].get("status") == "QP_CREATED"
            and by_event["COMPLETE"].get("status") == "ACK_COMPLETE"
        )
        if not causal or not statuses:
            errors.append(f"{flow_id}: lifecycle causality/status invalid")
            continue
        completed += 1
        latest_complete = max(latest_complete or complete, complete)
        first_txs.append(first_tx)
        lifecycles.append({
            "scheduled_start_ns": scheduled,
            "first_tx_ns": first_tx,
            "complete_ns": complete,
        })
    waves = [
        {
            "scheduled_start_ns": scheduled,
            "flow_count": sum(
                item["scheduled_start_ns"] == scheduled for item in lifecycles
            ),
            "earliest_first_tx_ns": min(
                item["first_tx_ns"] for item in lifecycles
                if item["scheduled_start_ns"] == scheduled
            ),
            "latest_complete_ns": max(
                item["complete_ns"] for item in lifecycles
                if item["scheduled_start_ns"] == scheduled
            ),
        }
        for scheduled in sorted({
            item["scheduled_start_ns"] for item in lifecycles
        })
    ]
    scenario = str(run.get("scenario", ""))
    expected_wave_count = 1 if scenario == "incast" else 2
    wave_profile_valid = len(waves) == expected_wave_count
    if scenario == "queue_buildup" and wave_profile_valid:
        wave_profile_valid = (
            waves[1]["earliest_first_tx_ns"] <= waves[0]["latest_complete_ns"]
        )
    if lifecycles and not wave_profile_valid:
        errors.append(
            f"background realized {scenario} wave profile invalid: {waves}"
        )
    deadline = _int_or_none(
        contract.get("completion_deadline_ns") if isinstance(contract, Mapping)
        else None
    )
    deadline_met = (
        latest_complete is not None and deadline is not None
        and latest_complete <= deadline
    )
    if isinstance(contract, Mapping) and not deadline_met:
        errors.append(
            "background ACK completion misses predeclared deadline: "
            f"latest={latest_complete}, deadline={deadline}"
        )
    return {
        "declared_flow_count": len(declared),
        "observed_flow_count": len(grouped),
        "ack_completed_flow_count": completed,
        "latest_complete_ns": latest_complete,
        "realized_window_start_ns": min(first_txs) if first_txs else None,
        "realized_window_end_ns": latest_complete,
        "realized_window_policy": "first_data_tx_to_last_ack_complete",
        "waves": waves,
        "realized_wave_profile_valid": wave_profile_valid,
        "completion_deadline_ns": deadline,
        "completion_deadline_met": deadline_met if contract is not None else None,
        "all_ack_complete": not errors and completed == len(declared),
    }, errors


def _background_link_map_evidence(
    run: Mapping[str, Any], runtime_path: Path, frozen_path: Path,
    contract: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Resolve the declared destination's exact dual-ACCESS pair."""
    errors: List[str] = []
    runtime_sha = _sha256(runtime_path)
    frozen_sha = _sha256(frozen_path)
    if runtime_sha != frozen_sha:
        errors.append(
            "runtime link_map differs from frozen corpus input: "
            f"runtime={runtime_sha}, frozen={frozen_sha}"
        )
    required = {
        "link_id", "src_node", "dst_node", "src_type", "dst_type",
        "src_port", "dst_port", "link_class",
    }

    def access_rows(path: Path) -> Dict[str, Dict[str, int]]:
        try:
            with path.open(newline="", encoding="utf-8") as source:
                reader = csv.DictReader(source)
                missing = sorted(required - set(reader.fieldnames or []))
                if missing:
                    errors.append(f"{path.name} link_map missing columns={missing}")
                    return {}
                rows = list(reader)
        except (OSError, csv.Error, UnicodeError) as error:
            errors.append(f"{path.name} link_map unreadable: {error}")
            return {}
        destination = _int_or_none(contract.get("destination_rank"))
        found: Dict[str, Dict[str, int]] = {}
        try:
            for row in rows:
                if row.get("link_class") != "ACCESS":
                    continue
                if (row.get("src_type") == "HOST"
                        and int(row["src_node"]) == destination):
                    found[str(row["link_id"])] = {
                        "host_port": int(row["src_port"]),
                        "switch_id": int(row["dst_node"]),
                        "switch_port": int(row["dst_port"]),
                    }
                elif (row.get("dst_type") == "HOST"
                      and int(row["dst_node"]) == destination):
                    found[str(row["link_id"])] = {
                        "host_port": int(row["dst_port"]),
                        "switch_id": int(row["src_node"]),
                        "switch_port": int(row["src_port"]),
                    }
        except (KeyError, TypeError, ValueError) as error:
            errors.append(f"{path.name} ACCESS endpoint is invalid: {error}")
        return found

    runtime_access = access_rows(runtime_path)
    frozen_access = access_rows(frozen_path)
    target = str(contract.get("bottleneck_access_link_id", ""))
    paired = str(contract.get("paired_access_link_id", ""))
    expected_port = _int_or_none(contract.get("expected_host_port"))
    expected_paired_port = 5 - expected_port if expected_port in {2, 3} else None
    pair_valid = (
        runtime_access == frozen_access
        and set(runtime_access) == {target, paired}
        and runtime_access.get(target, {}).get("host_port") == expected_port
        and runtime_access.get(paired, {}).get("host_port")
        == expected_paired_port
    )
    if not pair_valid:
        errors.append(
            "declared single-rail target does not resolve to the frozen dual-ACCESS "
            f"pair: target={target}, paired={paired}, expected_port={expected_port}, "
            f"runtime={runtime_access}, frozen={frozen_access}"
        )
    return {
        "runtime_sha256": runtime_sha,
        "frozen_sha256": frozen_sha,
        "hashes_match": runtime_sha == frozen_sha,
        "destination_rank": contract.get("destination_rank"),
        "target_link_id": target,
        "paired_link_id": paired,
        "expected_target_host_port": expected_port,
        "runtime_access_links": runtime_access,
        "frozen_access_links": frozen_access,
        "single_rail_target_resolved": pair_valid,
    }, errors


def _background_switch_evidence(
    run: Mapping[str, Any], switch_path: Path,
    application: Mapping[str, Any], link_map: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Recompute the queue/throughput effects over the realized window."""
    errors: List[str] = []
    required = {
        "run_id", "timestamp_ns", "switch_id", "port_id", "link_id",
        "direction", "queue_bytes", "max_queue_bytes",
        "observed_throughput_bps", "link_state",
    }
    try:
        with switch_path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            missing = sorted(required - set(reader.fieldnames or []))
            rows = list(reader)
    except (OSError, csv.Error, UnicodeError) as error:
        return {}, [f"switch_telemetry unreadable: {error}"]
    if not rows or missing:
        errors.append(f"switch_telemetry empty or missing columns={missing}")
    run_id = str(run.get("run_id", ""))
    if any(str(row.get("run_id", "")) != run_id for row in rows):
        errors.append("switch_telemetry contains foreign run_id")
    target = str(link_map.get("target_link_id", ""))
    runtime_access = link_map.get("runtime_access_links", {})
    endpoint = runtime_access.get(target, {}) \
        if isinstance(runtime_access, Mapping) else {}
    try:
        target_rows = [
            row for row in rows
            if str(row.get("direction", "")).lower() == "tx"
            and str(row.get("link_id", "")) == target
            and int(row["switch_id"]) == int(endpoint["switch_id"])
            and int(row["port_id"]) == int(endpoint["switch_port"])
        ]
        first_tx = int(application["realized_window_start_ns"])
        last_complete = int(application["realized_window_end_ns"])
        before = [row for row in target_rows if int(row["timestamp_ns"]) < first_tx]
        during = [
            row for row in target_rows
            if first_tx <= int(row["timestamp_ns"]) <= last_complete + 1_000_000
        ]
        before_queue = sorted(
            int(row.get("max_queue_bytes") or row.get("queue_bytes") or 0)
            for row in before
        )
        during_queue = [
            int(row.get("max_queue_bytes") or row.get("queue_bytes") or 0)
            for row in during
        ]
        baseline_median = (
            before_queue[len(before_queue) // 2] if before_queue else None
        )
        event_peak = max(during_queue) if during_queue else None
        throughput_peak = max(
            (int(row.get("observed_throughput_bps") or 0) for row in during),
            default=0,
        )
        queue_pressure = (
            baseline_median is not None and event_peak is not None
            and event_peak > 0 and event_peak > baseline_median
        )
        throughput_activity = throughput_peak > 0
        carrier_up = bool(during) and all(
            str(row.get("link_state", "")).lower() == "up" for row in during
        )
    except (KeyError, TypeError, ValueError) as error:
        errors.append(f"background switch telemetry numeric/endpoint error: {error}")
        before = []
        during = []
        baseline_median = event_peak = None
        throughput_peak = 0
        queue_pressure = throughput_activity = carrier_up = False
    if not queue_pressure:
        errors.append(
            "declared target ACCESS link lacks realized-window queue pressure"
        )
    if not throughput_activity:
        errors.append(
            "declared target ACCESS link lacks realized-window throughput activity"
        )
    if not carrier_up:
        errors.append(
            "declared congestion target carrier is not proven up over realized window"
        )
    return {
        "target_link_id": target,
        "target_switch_endpoint": dict(endpoint),
        "realized_window_start_ns": application.get("realized_window_start_ns"),
        "realized_window_end_ns": application.get("realized_window_end_ns"),
        "before_sample_count": len(before),
        "event_sample_count": len(during),
        "baseline_median_max_queue_bytes": baseline_median,
        "event_peak_max_queue_bytes": event_peak,
        "maximum_observed_throughput_bps": throughput_peak,
        "queue_pressure": queue_pressure,
        "throughput_activity": throughput_activity,
        "carrier_up": carrier_up,
    }, errors


def _background_rdma_evidence(
    run: Mapping[str, Any], rdma_path: Path, schedule_path: Path,
    contract: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Recompute the background QP bijection/configuration from raw WC rows."""
    errors: List[str] = []
    try:
        with schedule_path.open(newline="", encoding="utf-8") as source:
            schedule_reader = csv.DictReader(source)
            declared = list(schedule_reader)
        with rdma_path.open(newline="", encoding="utf-8") as source:
            rdma_reader = csv.DictReader(source)
            rdma_fields = tuple(rdma_reader.fieldnames or ())
            rows = list(rdma_reader)
    except (OSError, csv.Error, UnicodeError) as error:
        return {}, [f"background RDMA/schedule unreadable: {error}"]
    if rdma_fields != BACKGROUND_RDMA_COLUMNS:
        errors.append(
            "rdma_wc_telemetry schema must exactly match the 26-column runner "
            f"contract: observed={list(rdma_fields)}"
        )
    if not rows:
        errors.append("rdma_wc_telemetry is empty")
    run_id = str(run.get("run_id", ""))
    if any(str(row.get("run_id", "")) != run_id for row in rows):
        errors.append("rdma_wc_telemetry contains foreign run_id")
    background = [
        row for row in rows if row.get("traffic_class") == "BACKGROUND"
    ]
    expected: Dict[str, Dict[str, str]] = {}
    try:
        for flow in declared:
            logical_id = (
                f"{int(flow['src_rank'])}-{int(flow['dst_rank'])}-"
                f"{int(flow['sport'])}-{int(flow['pg'])}"
            )
            expected[logical_id] = flow
    except (KeyError, TypeError, ValueError) as error:
        errors.append(f"background schedule QP identity invalid: {error}")
    grouped: Dict[str, List[Mapping[str, str]]] = defaultdict(list)
    for row in background:
        grouped[str(row.get("logical_qp_id", ""))].append(row)
    bijection = bool(expected) and set(grouped) == set(expected)
    if not bijection:
        errors.append(
            "background RDMA QP set differs from schedule: "
            f"expected={sorted(expected)}, observed={sorted(grouped)}"
        )
    expected_port = _int_or_none(contract.get("expected_host_port"))
    expected_rto = _int_or_none(contract.get("rdma_rto_us"))
    expected_retry_limit = _int_or_none(contract.get("rdma_retry_limit"))
    lifecycle_ok = bijection
    configuration_ok = bijection
    zero_retries = bijection
    forbidden_events: List[str] = []
    for logical_id, flow in expected.items():
        qp_rows = grouped.get(logical_id, [])
        created = [row for row in qp_rows if row.get("event") == "QP_CREATED"]
        successes = [
            row for row in qp_rows
            if row.get("event") == "WC" and row.get("wc_status") == "SUCCESS"
        ]
        unexpected = [
            row for row in qp_rows
            if row.get("event") not in {"QP_CREATED", "WC"}
            or (row.get("event") == "WC" and row.get("wc_status") != "SUCCESS")
        ]
        forbidden_events.extend(
            f"{logical_id}:{row.get('event')}:{row.get('wc_status')}"
            for row in unexpected
        )
        if len(qp_rows) != 2 or len(created) != 1 or len(successes) != 1:
            lifecycle_ok = False
        for row in qp_rows:
            try:
                metadata_ok = (
                    int(row["src_rank"]) == int(flow["src_rank"])
                    and int(row["dst_rank"]) == int(flow["dst_rank"])
                    and int(row["sport"]) == int(flow["sport"])
                )
                row_config_ok = (
                    int(row["primary_nic"]) == expected_port
                    and int(row["active_nic"]) == expected_port
                    and int(row["retry_limit"]) == expected_retry_limit
                    and int(row["rto_us"]) == expected_rto
                )
                row_zero_retry = int(row["retry_count"]) == 0
            except (KeyError, TypeError, ValueError):
                metadata_ok = row_config_ok = row_zero_retry = False
            configuration_ok = configuration_ok and metadata_ok and row_config_ok
            zero_retries = zero_retries and row_zero_retry
    if not lifecycle_ok:
        errors.append("background QPs lack exactly one QP_CREATED and one SUCCESS WC")
    if not configuration_ok:
        errors.append(
            "background QP rail/RTO/retry configuration differs from truth: "
            f"port={expected_port}, rto_us={expected_rto}, "
            f"retry_limit={expected_retry_limit}"
        )
    if forbidden_events or not zero_retries:
        errors.append(
            "background QP evidence contains retry/failover/error activity: "
            f"events={forbidden_events[:8]}"
        )
    return {
        "expected_qp_count": len(expected),
        "observed_qp_count": len(grouped),
        "qp_identity_bijection": bijection,
        "ack_success_lifecycle": lifecycle_ok,
        "single_rail_transport_configuration": configuration_ok,
        "expected_primary_nic": expected_port,
        "rdma_rto_us": expected_rto,
        "rdma_retry_limit": expected_retry_limit,
        "zero_retry_or_failover_events": not forbidden_events and zero_retries,
        "forbidden_events": forbidden_events,
    }, errors


def _collective_semantic_recomputation_errors(
    run: Mapping[str, Any],
    semantic: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path,
    runtime_evidence: Mapping[str, Any],
) -> List[str]:
    """Recreate the A1 semantic report from independently replayed runtime data."""

    schedule = run.get("schedule", {})
    collective = schedule.get("collective_workload_override") \
        if isinstance(schedule, Mapping) else None
    if not isinstance(collective, Mapping):
        return []
    errors: List[str] = []
    run_dir = _ref_path(artifacts.get("run_manifest", {}), base_dir).parent
    independent = runtime_evidence.get("independent_recomputation", {})
    timeline = independent.get("collective_override_lifecycle") \
        if isinstance(independent, Mapping) else None
    profile = collective.get("static_validation", {}).get(
        "qualification_profile"
    )
    fault_path = _ref_path(
        artifacts.get("fault_application_telemetry", {}), base_dir
    )
    try:
        with fault_path.open(encoding="utf-8", newline="") as stream:
            fault_rows = list(csv.DictReader(stream))
        checks = [
            {
                "name": "predeclared_collective_profile_is_label_authority",
                "status": PASS if (
                    schedule.get("independent_of_features") is True
                    and schedule.get("generated_before_run") is True
                    and profile in {
                        workload_runtime.BURST_PROFILE,
                        workload_runtime.HIGH_UTIL_PROFILE,
                    }
                ) else FAIL,
            },
            {
                "name": "role_bound_application_lifecycle_validated",
                "status": PASS if (
                    independent.get("status") == PASS
                    and isinstance(timeline, Mapping)
                    and timeline.get("actual_application_window_authority")
                    == "role_bound_collective_transaction"
                ) else FAIL,
            },
            {
                "name": "no_physical_fault_application",
                "status": PASS if not fault_rows else FAIL,
            },
        ]
        source_paths = {
            "workload": run_dir / "inputs/collective_workload_override.txt",
            "collective_layer_roles": run_dir / "inputs/collective_layer_roles.csv",
            "link_map": _ref_path(artifacts.get("link_map", {}), base_dir),
            "switch_telemetry": _ref_path(
                artifacts.get("switch_telemetry", {}), base_dir
            ),
            "nic_telemetry": _ref_path(
                artifacts.get("nic_telemetry", {}), base_dir
            ),
            "collective_transaction": _ref_path(
                artifacts.get("collective_transaction", {}), base_dir
            ),
            "collective_telemetry": _ref_path(
                artifacts.get("collective_telemetry", {}), base_dir
            ),
            "run_lifecycle": _ref_path(
                artifacts.get("run_lifecycle", {}), base_dir
            ),
        }
        source_hashes = {
            name: _sha256(path) for name, path in sorted(source_paths.items())
        }
        source_hashes.update({
            "fault_application": _sha256(fault_path),
            "workload_runtime_qualification": _sha256(
                _ref_path(
                    artifacts.get("workload_runtime_qualification", {}), base_dir
                )
            ),
        })
        expected = {
            "schema_version": "limer.p2-run-semantics.v1",
            "run_id": str(run.get("run_id", "")),
            "mechanism_id": run.get("mechanism", {}).get("mechanism_id"),
            "status": (
                PASS if all(item["status"] == PASS for item in checks) else FAIL
            ),
            "source_artifact_sha256": _sha256(
                source_paths["switch_telemetry"]
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
                "application_lifecycle": timeline,
                "feature_derived_label": False,
            },
        }
        if expected["status"] != PASS:
            errors.append("independent A1 semantic reconstruction did not PASS")
        if dict(semantic) != expected:
            errors.append(
                "A1 semantic report differs from independent raw-evidence "
                "recomputation"
            )
    except (OSError, csv.Error, KeyError, TypeError, ValueError) as error:
        errors.append(f"A1 semantic evidence cannot be recomputed: {error}")
    return errors


def _semantic_validation_errors(
    run: Mapping[str, Any], semantic: Mapping[str, Any],
    switch_sha256: str,
    background_application_sha256: str | None = None,
    background_schedule_sha256: str | None = None,
    background_source_sha256: Mapping[str, str] | None = None,
    background_contract: Mapping[str, Any] | None = None,
    background_application: Mapping[str, Any] | None = None,
) -> List[str]:
    errors: List[str] = []
    run_id = str(run.get("run_id", ""))
    mechanism = run.get("mechanism", {})
    mechanism_id = str(mechanism.get("mechanism_id", "")) \
        if isinstance(mechanism, Mapping) else ""
    if semantic.get("schema_version") != "limer.p2-run-semantics.v1":
        errors.append("semantic_validation schema_version invalid")
    if semantic.get("run_id") != run_id:
        errors.append("semantic_validation run_id differs")
    if semantic.get("mechanism_id") != mechanism_id:
        errors.append("semantic_validation mechanism_id differs")
    if semantic.get("status") != PASS:
        errors.append(f"semantic_validation status={semantic.get('status')}")
    if semantic.get("source_artifact_sha256") != switch_sha256:
        errors.append("semantic_validation does not bind hashed switch telemetry")
    semantic_checks = semantic.get("checks", [])
    if (not isinstance(semantic_checks, list) or not semantic_checks
            or any(not isinstance(item, Mapping) or item.get("status") != PASS
                   for item in semantic_checks)):
        errors.append("semantic_validation checks must be nonempty and all PASS")

    class_label = _normal_class(run.get("class_label"))
    family = str(run.get("fault_family", ""))
    impairment = str(_parameter(run, "impairment", semantic.get("impairment", "")))
    if class_label == "GRAY_FAULT" and str(
            semantic.get("target_link_state_during_event", "")).lower() != "up":
        errors.append("gray fault semantic evidence must keep carrier up")
    if class_label in {"HEALTHY", "CONGESTION"} and \
            semantic.get("injected_physical_fault") is not False:
        errors.append("negative run semantic evidence must state no physical fault")
    if class_label == "CONGESTION" and background_schedule_sha256 is not None:
        sources = semantic.get("source_artifacts_sha256", {})
        if not isinstance(sources, Mapping):
            errors.append("background semantic evidence lacks source hash map")
            sources = {}
        if sources.get("background_application") != background_application_sha256:
            errors.append(
                "background semantic evidence does not bind application lifecycle"
            )
        if sources.get("background_schedule") != background_schedule_sha256:
            errors.append("background semantic evidence does not bind schedule")
        expected_sources = dict(background_source_sha256 or {})
        for name, expected_sha256 in expected_sources.items():
            if sources.get(name) != expected_sha256:
                errors.append(
                    "background semantic evidence source hash differs: "
                    f"{name}={sources.get(name)!r}, expected={expected_sha256!r}"
                )
        if semantic.get("scenario") != run.get("scenario"):
            errors.append("background semantic scenario differs from run")
        observed = {
            str(value) for value in semantic.get("observed_effects", [])
        }
        if not BACKGROUND_SEMANTIC_EFFECTS <= observed:
            errors.append(
                "background semantic evidence lacks effects="
                f"{sorted(BACKGROUND_SEMANTIC_EFFECTS - observed)}"
            )
        if isinstance(background_contract, Mapping):
            target = str(background_contract.get("bottleneck_access_link_id", ""))
            if semantic.get("target_link_ids") != [target]:
                errors.append(
                    "background semantic target_link_ids differs from single target"
                )
            semantic_evidence = semantic.get("evidence", {})
            if not isinstance(semantic_evidence, Mapping):
                errors.append("background semantic evidence object is missing")
                semantic_evidence = {}
            transport = semantic_evidence.get("transport_contract", {})
            expected_transport = {
                "rto_us": background_contract.get("rdma_rto_us"),
                "retry_limit": background_contract.get("rdma_retry_limit"),
                "max_rto_retry_events": background_contract.get(
                    "max_rto_retry_events"),
            }
            if transport != expected_transport:
                errors.append(
                    "background semantic transport contract differs from truth"
                )
            if semantic_evidence.get("route_bucket") != background_contract.get(
                    "route_bucket"):
                errors.append("background semantic route_bucket differs from truth")
            if semantic_evidence.get("target_host_port") != background_contract.get(
                    "expected_host_port"):
                errors.append("background semantic target host port differs from truth")
        if isinstance(background_application, Mapping):
            if semantic.get("actual_apply_ns") != background_application.get(
                    "realized_window_start_ns"):
                errors.append(
                    "background semantic actual_apply_ns differs from first data TX"
                )
            if semantic.get("event_end_ns") != background_application.get(
                    "realized_window_end_ns"):
                errors.append(
                    "background semantic event_end_ns differs from last ACK completion"
                )
    loss_semantics = family in {"random_loss", "burst_loss"} or (
        family == "intermittent_service" and impairment == "loss"
    )
    if loss_semantics:
        if str(semantic.get("packet_disposition", "")).lower() != "dropped":
            errors.append("loss semantic evidence must prove packet disposition=dropped")
        if semantic.get("recoverable_error_proxy") is not False:
            errors.append("recoverable-error proxy cannot satisfy true-loss semantics")
    observed_effects = set(str(value) for value in semantic.get("observed_effects", []))
    if family == "bandwidth_degradation" and "throughput_degradation" not in observed_effects:
        errors.append("bandwidth semantic evidence lacks throughput_degradation")
    if family == "service_degradation" and not (
            observed_effects & REQUIRED_SERVICE_EFFECTS):
        errors.append("service semantic evidence lacks required service effect")
    return errors


def _stability_lifecycle_facts(
    path: Path, run_id: str, virtual_finish_ns: int,
) -> Tuple[bool, bool, List[str]]:
    errors: List[str] = []
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {
                "run_id", "event", "scheduled_ns", "actual_ns",
                "finished_ranks", "world_size", "status",
            }
            if not required.issubset(reader.fieldnames or []):
                return False, False, ["stability lifecycle schema is invalid"]
            rows = list(reader)
    except (OSError, csv.Error) as error:
        return False, False, [f"stability lifecycle unreadable: {error}"]
    if any(row.get("run_id") != run_id for row in rows):
        errors.append("stability lifecycle contains a foreign run_id")
    observation_complete = False
    for row in rows:
        if row.get("event") != "observation_horizon" or not str(
            row.get("status", "")
        ).startswith("OBSERVATION_WINDOW_COMPLETE"):
            continue
        try:
            observation_complete = (
                int(row["scheduled_ns"]) == virtual_finish_ns
                and int(row["actual_ns"]) >= virtual_finish_ns
                and int(row["world_size"]) == 16
            )
        except (TypeError, ValueError):
            errors.append("stability lifecycle has invalid numeric fields")
        if observation_complete:
            break
    if not observation_complete:
        errors.append("stability lifecycle did not complete the observation horizon")
    workload_completed = any(
        row.get("status") == "WORKLOAD_COMPLETE" for row in rows
    )
    return observation_complete, workload_completed, errors


def _simulator_stability_evidence(
    run: Mapping[str, Any],
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    base_dir: Path,
) -> Tuple[Dict[str, Any], List[str]]:
    """Rebuild the gated PASS report without trusting corpus self-reporting."""

    errors: List[str] = []
    run_id = str(run.get("run_id", ""))
    manifest_path = _ref_path(artifacts.get("run_manifest", {}), base_dir)
    run_dir = manifest_path.parent.resolve()
    source_run: Mapping[str, Any] = {}
    try:
        source_corpus = _load_json(run_dir / "inputs" / "corpus_manifest.json")
        matches = [
            item for item in source_corpus.get("runs", [])
            if isinstance(item, Mapping) and item.get("run_id") == run_id
        ]
        if len(matches) == 1:
            source_run = matches[0]
        else:
            errors.append("stability source corpus lacks exactly one planned run")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"stability source corpus unreadable: {error}")
    planned_stability = source_run.get("simulator_stability", {}) \
        if isinstance(source_run, Mapping) else {}
    gate_required = (
        isinstance(planned_stability, Mapping)
        and planned_stability.get("gate_required") is True
    )
    if run.get("fault_family") == "random_loss" and not gate_required:
        errors.append("random_loss source run lacks a required stability gate")
    if not gate_required:
        return {"status": "NOT_REQUIRED", "errors": errors}, errors
    if planned_stability.get("status") != "PENDING_EXECUTION":
        errors.append("planned stability state was not PENDING_EXECUTION")
    summary = run.get("simulator_stability")
    if (
        not isinstance(summary, Mapping)
        or summary.get("gate_required") is not True
        or summary.get("status") != PASS
    ):
        errors.append("executed corpus does not publish a derived stability PASS")
        summary = {}
    report_ref = artifacts.get("simulator_stability")
    if not isinstance(report_ref, Mapping) or "exit_code" not in artifacts:
        errors.append("gated run lacks stability report or exit-code artifact")
        return {"status": FAIL, "errors": errors}, errors

    source_hashes: Dict[str, str] = {}
    for source_name, artifact_name in SIMULATOR_STABILITY_SOURCE_ARTIFACTS.items():
        ref = artifacts.get(artifact_name)
        if not isinstance(ref, Mapping):
            errors.append(f"stability source artifact is absent: {artifact_name}")
            continue
        path = _ref_path(ref, base_dir)
        try:
            actual = _sha256(path)
        except OSError as error:
            errors.append(f"stability source artifact unreadable: {error}")
            continue
        if actual != ref.get("sha256"):
            errors.append(f"stability source artifact hash differs: {artifact_name}")
        source_hashes[source_name] = actual
    exit_path = _ref_path(artifacts["exit_code"], base_dir)
    try:
        exit_text = exit_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as error:
        errors.append(f"stability exit-code artifact unreadable: {error}")
        exit_text = ""
    if exit_text != "0" or manifest.get("exit_code") != 0:
        errors.append("stability process exit evidence is not zero")

    execution = manifest.get("execution")
    if not isinstance(execution, Mapping):
        execution = {}
        errors.append("stability execution evidence is missing")
    resource = execution.get("resource_observation")
    if (
        execution.get("process_status") != "EXITED_ZERO"
        or execution.get("timed_out") is not False
        or execution.get("interrupted") is not False
        or not isinstance(resource, Mapping)
        or resource.get("oom_kill_observed_during_attempt") is not False
    ):
        errors.append("stability process/timeout/interrupt/OOM evidence is invalid")

    lifecycle_path = _ref_path(artifacts["run_lifecycle"], base_dir)
    _, workload_completed, lifecycle_errors = _stability_lifecycle_facts(
        lifecycle_path, run_id, int(source_run.get("virtual_finish_ns", -1))
    )
    errors.extend(lifecycle_errors)
    lifecycle = manifest.get("lifecycle")
    if (
        manifest.get("execution_status") != "COMPLETE"
        or not isinstance(lifecycle, Mapping)
        or lifecycle.get("observation_status") != "OBSERVATION_WINDOW_COMPLETE"
        or manifest.get("workload_completed") is not workload_completed
    ):
        errors.append("stability lifecycle differs from sealed raw evidence")

    status_fields = {
        "runtime_execution_evidence": "runtime_execution_status",
        "semantic_validation": "semantic_validation_status",
        "workload_runtime_qualification": (
            "workload_runtime_qualification_status"
        ),
        "ecmp_route_candidate_validation": (
            "ecmp_route_candidate_validation_status"
        ),
        "training_source_port_allocator_validation": (
            "training_source_port_allocator_validation_status"
        ),
    }
    for artifact_name, manifest_field in status_fields.items():
        try:
            value = _load_json(_ref_path(artifacts[artifact_name], base_dir))
        except (KeyError, OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"stability prerequisite unreadable: {error}")
            value = {}
        if manifest.get(manifest_field) != PASS or value.get("status") != PASS:
            errors.append(f"stability prerequisite is not PASS: {artifact_name}")

    closure = manifest.get("runtime_closure")
    identity = closure.get("identity_sha256") if isinstance(closure, Mapping) else None
    workers = manifest.get("simulator_worker_threads")
    if not _valid_sha256(identity):
        errors.append("stability runtime closure identity is invalid")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        errors.append("stability worker-thread evidence is invalid")
    basis = {
        "planned_stability_status": "PENDING_EXECUTION",
        "exit_code": manifest.get("exit_code"),
        "process_status": execution.get("process_status"),
        "timed_out": execution.get("timed_out"),
        "interrupted": execution.get("interrupted"),
        "oom_kill_observed_during_attempt": (
            resource.get("oom_kill_observed_during_attempt")
            if isinstance(resource, Mapping) else None
        ),
        "execution_status": manifest.get("execution_status"),
        "observation_status": (
            lifecycle.get("observation_status")
            if isinstance(lifecycle, Mapping) else None
        ),
        "workload_completed": workload_completed,
        "runtime_execution_status": manifest.get("runtime_execution_status"),
        "semantic_validation_status": manifest.get("semantic_validation_status"),
        "workload_runtime_qualification_status": manifest.get(
            "workload_runtime_qualification_status"
        ),
        "ecmp_route_candidate_validation_status": manifest.get(
            "ecmp_route_candidate_validation_status"
        ),
        "training_source_port_allocator_validation_status": manifest.get(
            "training_source_port_allocator_validation_status"
        ),
        "runtime_closure_identity_sha256": identity,
        "simulator_worker_threads": workers,
        "source_artifacts_sha256": source_hashes,
    }
    planned_sha = _canonical_hash(source_run) if source_run else None
    expected = {
        "schema_version": SIMULATOR_STABILITY_SCHEMA,
        "status": PASS,
        "run_id": run_id,
        "fault_family": source_run.get("fault_family"),
        "gate_required": True,
        "planned_run_sha256": planned_sha,
        "virtual_finish_ns": source_run.get("virtual_finish_ns"),
        "evidence_basis": basis,
        "evidence_basis_sha256": _canonical_hash(basis),
        "checks": [
            {"name": name, "status": PASS}
            for name in SIMULATOR_STABILITY_CHECK_NAMES
        ],
        "errors": [],
    }
    try:
        recorded = _load_json(_ref_path(report_ref, base_dir))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"simulator stability report unreadable: {error}")
        recorded = {}
    if recorded != expected:
        errors.append(
            "simulator stability report differs from independent raw recomputation"
        )
    report_sha = _sha256(_ref_path(report_ref, base_dir))
    if (
        manifest.get("planned_run_sha256") != planned_sha
        or manifest.get("simulator_stability_gate_required") is not True
        or manifest.get("simulator_stability_status") != PASS
        or manifest.get("simulator_stability_sha256") != report_sha
        or report_ref.get("sha256") != report_sha
        or summary.get("evidence") != report_ref
        or summary.get("evidence_basis_sha256") != _canonical_hash(basis)
    ):
        errors.append("stability summary/manifest does not bind recomputed evidence")
    return {
        "status": PASS if not errors else FAIL,
        "gate_required": True,
        "planned_run_sha256": planned_sha,
        "report_sha256": report_sha,
        "evidence_basis_sha256": _canonical_hash(basis),
        "source_artifacts_sha256": source_hashes,
        "errors": errors,
    }, errors


def inspect_run_artifacts(
    runs: Sequence[Mapping[str, Any]],
    base_dir: Path,
    executed: bool,
    schedule_evidence: Sequence[Mapping[str, Any]],
    corpus: Mapping[str, Any],
    expected_link_map_sha256: str | None = None,
    frozen_link_map_path: Path | None = None,
    allow_runtime_test_contract: bool = False,
) -> Tuple[List[Dict[str, Any]], List[str], int]:
    evidence: List[Dict[str, Any]] = []
    errors: List[str] = []
    completed_count = 0
    runtime_cache: Dict[
        str,
        Tuple[runtime_bundle.RuntimeBundle | None, Mapping[str, Any], List[str]],
    ] = {}
    schedule_by_run = {
        str(item.get("run_id")): item for item in schedule_evidence
    }
    for run in runs:
        run_id = str(run.get("run_id", ""))
        execution_status = str(run.get("execution_status", PREPARED)).upper()
        artifacts = _artifact_map(run)
        run_errors: List[str] = []
        refs: Dict[str, Any] = {}
        telemetry_evidence: Dict[str, Any] = {}
        lifecycle_evidence: Dict[str, Any] = {}
        run_manifest_evidence: Dict[str, Any] = {}
        runtime_qualification_evidence: Dict[str, Any] = {}
        semantic_evidence: Dict[str, Any] = {}
        simulator_stability_evidence: Dict[str, Any] = {}
        background_application_evidence: Dict[str, Any] = {}
        background_link_map_evidence: Dict[str, Any] = {}
        background_switch_evidence: Dict[str, Any] = {}
        background_rdma_evidence: Dict[str, Any] = {}
        if not executed:
            evidence.append({
                "run_id": run_id,
                "execution_status": execution_status,
                "status": PENDING,
                "artifacts": refs,
                "telemetry_observability": telemetry_evidence,
                "lifecycle": lifecycle_evidence,
                "runner_manifest": run_manifest_evidence,
                "workload_runtime_qualification": runtime_qualification_evidence,
                "semantic_validation": semantic_evidence,
                "simulator_stability": simulator_stability_evidence,
                "background_application": background_application_evidence,
                "background_link_map": background_link_map_evidence,
                "background_switch": background_switch_evidence,
                "background_rdma": background_rdma_evidence,
                "errors": [],
            })
            continue

        stability = run.get("simulator_stability", {})
        gate_required = (
            isinstance(stability, Mapping)
            and stability.get("gate_required") is True
        )
        if run.get("fault_family") == "random_loss" and not gate_required:
            run_errors.append("random_loss lacks a derived required stability gate")
        blocked_allowed = False
        required = set(
            REQUIRED_COMPLETE_ARTIFACTS
            if execution_status == "COMPLETE" else set()
        )
        if execution_status == "COMPLETE" and gate_required:
            required.update({"simulator_stability", "exit_code"})
        background_ref = run.get("schedule", {}).get("background_flow_schedule")
        background_contract: Mapping[str, Any] = {}
        schedule_item = schedule_by_run.get(run_id, {})
        if isinstance(background_ref, Mapping):
            profile = schedule_item.get("background_flow_schedule", {}) \
                if isinstance(schedule_item, Mapping) else {}
            candidate = profile.get("contract", {}) \
                if isinstance(profile, Mapping) else {}
            if isinstance(candidate, Mapping):
                background_contract = candidate
        if execution_status == "COMPLETE" and isinstance(background_ref, Mapping):
            required.update({"background_flow_application", "rdma_wc_telemetry"})
            if not background_contract:
                run_errors.append("validated background truth contract is unavailable")
        if not required:
            run_errors.append(
                f"execution_status={execution_status} is not COMPLETE; "
                "audited BLOCKED publication is not implemented"
            )
        missing = sorted(required - set(artifacts))
        if missing:
            run_errors.append(f"missing artifacts={missing}")
        for name in sorted(required & set(artifacts)):
            ok, ref_evidence = verify_ref(artifacts[name], base_dir)
            refs[name] = ref_evidence
            if not ok:
                run_errors.append(f"artifact {name} hash/path invalid")
        if execution_status == "COMPLETE" and not run_errors:
            manifest_path = _ref_path(artifacts["run_manifest"], base_dir)
            try:
                run_manifest = _load_json(manifest_path)
                run_manifest_evidence, runner_manifest_errors = (
                    _complete_run_manifest_evidence(
                        run,
                        artifacts,
                        base_dir,
                        corpus,
                        runtime_cache,
                        allow_legacy_test_binding=allow_runtime_test_contract,
                    )
                )
                run_errors.extend(runner_manifest_errors)
                if isinstance(background_ref, Mapping):
                    recorded_transport = run_manifest.get(
                        "background_transport_contract", {})
                    transport_fields = (
                        "route_bucket", "rdma_rto_us", "rdma_retry_limit",
                        "max_rto_retry_events", "completion_deadline_ns",
                        "truth_start_ns", "truth_end_ns",
                    )
                    if (not isinstance(recorded_transport, Mapping)
                            or any(recorded_transport.get(field)
                                   != background_contract.get(field)
                                   for field in transport_fields)):
                        run_errors.append(
                            "run manifest background transport contract differs from truth"
                        )
            except (OSError, ValueError, json.JSONDecodeError) as error:
                run_errors.append(str(error))
        if execution_status == "COMPLETE" and not allow_runtime_test_contract:
            qualification_ref = artifacts.get("workload_runtime_qualification")
            if isinstance(qualification_ref, Mapping):
                try:
                    stored_qualification = _load_json(
                        _ref_path(qualification_ref, base_dir)
                    )
                    if (
                        stored_qualification.get("contract_source")
                        == "explicit_test_override"
                    ):
                        run_errors.insert(
                            0,
                            "explicit runtime contract is test-only and forbidden "
                            "in production",
                        )
                except (OSError, ValueError, json.JSONDecodeError):
                    # Reference verification and the independent qualification
                    # reader below own malformed/unreadable-report diagnostics.
                    pass
        if execution_status == "COMPLETE" and not run_errors:
            runtime_qualification_evidence, runtime_errors = (
                _workload_runtime_qualification_evidence(
                    run, artifacts, base_dir, run_manifest, corpus,
                    allow_test_contract=allow_runtime_test_contract,
                )
            )
            run_errors.extend(runtime_errors)
        if execution_status == "COMPLETE" and not run_errors:
            lifecycle_evidence, lifecycle_errors = _lifecycle_evidence(
                run, _ref_path(artifacts["run_lifecycle"], base_dir)
            )
            run_errors.extend(lifecycle_errors)
        if execution_status == "COMPLETE" and not run_errors:
            if (expected_link_map_sha256 is not None
                    and artifacts["link_map"].get("sha256") != expected_link_map_sha256):
                run_errors.append("run link_map hash differs from frozen corpus topology")
            csv_contracts = {
                "switch_telemetry": {
                    "run_id", "timestamp_ns", "link_id", "direction", "tx_bytes",
                },
                "nic_telemetry": {"run_id", "timestamp_ns", "link_id", "node_id", "nic_id"},
                "collective_telemetry": {
                    "run_id", "rank_id", "world_size", "finish_time_ns", "status",
                },
            }
            if isinstance(background_ref, Mapping):
                csv_contracts["background_flow_application"] = (
                    BACKGROUND_APPLICATION_REQUIRED
                )
                csv_contracts["rdma_wc_telemetry"] = BACKGROUND_RDMA_REQUIRED
            for name, required_columns in csv_contracts.items():
                path = _ref_path(artifacts[name], base_dir)
                try:
                    sample = pd.read_csv(path, nrows=1)
                except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
                    run_errors.append(f"{name} unreadable: {error}")
                    continue
                missing_columns = sorted(required_columns - set(sample.columns))
                if sample.empty or missing_columns:
                    run_errors.append(
                        f"{name} empty or missing columns={missing_columns}"
                    )
                elif str(sample.iloc[0]["run_id"]) != run_id:
                    run_errors.append(
                        f"{name} first run_id={sample.iloc[0]['run_id']} expected={run_id}"
                    )
        if (
            execution_status == "COMPLETE"
            and not run_errors
            and isinstance(background_ref, Mapping)
        ):
            background_application_evidence, background_errors = (
                _background_application_evidence(
                    run,
                    _ref_path(artifacts["background_flow_application"], base_dir),
                    _ref_path(background_ref, base_dir),
                    background_contract,
                )
            )
            run_errors.extend(background_errors)
        if (
            execution_status == "COMPLETE"
            and not run_errors
            and isinstance(background_ref, Mapping)
        ):
            if frozen_link_map_path is None:
                run_errors.append("frozen corpus link_map path is unavailable")
            else:
                background_link_map_evidence, link_errors = (
                    _background_link_map_evidence(
                        run,
                        _ref_path(artifacts["link_map"], base_dir),
                        frozen_link_map_path,
                        background_contract,
                    )
                )
                run_errors.extend(link_errors)
        if (
            execution_status == "COMPLETE"
            and not run_errors
            and isinstance(background_ref, Mapping)
        ):
            background_switch_evidence, switch_errors = (
                _background_switch_evidence(
                    run,
                    _ref_path(artifacts["switch_telemetry"], base_dir),
                    background_application_evidence,
                    background_link_map_evidence,
                )
            )
            run_errors.extend(switch_errors)
        if (
            execution_status == "COMPLETE"
            and not run_errors
            and isinstance(background_ref, Mapping)
        ):
            background_rdma_evidence, rdma_errors = _background_rdma_evidence(
                run,
                _ref_path(artifacts["rdma_wc_telemetry"], base_dir),
                _ref_path(background_ref, base_dir),
                background_contract,
            )
            run_errors.extend(rdma_errors)
        if execution_status == "COMPLETE" and not run_errors:
            telemetry_evidence, telemetry_errors = _switch_telemetry_evidence(
                run,
                _ref_path(artifacts["switch_telemetry"], base_dir),
                schedule_by_run.get(run_id, {}),
                runtime_cadence_qualified=(
                    runtime_qualification_evidence.get(
                        "independent_recomputation", {}
                    ).get("status") == PASS
                ),
            )
            run_errors.extend(telemetry_errors)
        if execution_status == "COMPLETE" and not run_errors:
            try:
                semantic = _load_json(
                    _ref_path(artifacts["semantic_validation"], base_dir))
                semantic_errors = _semantic_validation_errors(
                    run,
                    semantic,
                    str(artifacts["switch_telemetry"].get("sha256", "")),
                    (
                        str(artifacts["background_flow_application"].get("sha256", ""))
                        if isinstance(background_ref, Mapping) else None
                    ),
                    (
                        str(background_ref.get("sha256", ""))
                        if isinstance(background_ref, Mapping) else None
                    ),
                    (
                        {
                            "switch": str(
                                artifacts["switch_telemetry"].get("sha256", "")
                            ),
                            "background_application": str(
                                artifacts["background_flow_application"].get(
                                    "sha256", "")
                            ),
                            "background_schedule": str(
                                background_ref.get("sha256", "")
                            ),
                            "truth_schedule": str(
                                run.get("schedule", {}).get("sha256", "")
                            ),
                            "link_map": str(
                                artifacts["link_map"].get("sha256", "")
                            ),
                            "runtime_link_map": str(
                                artifacts["link_map"].get("sha256", "")
                            ),
                            "frozen_link_map": str(
                                expected_link_map_sha256 or ""
                            ),
                            "rdma_wc": str(
                                artifacts["rdma_wc_telemetry"].get("sha256", "")
                            ),
                            "fault_application": str(
                                artifacts["fault_application_telemetry"].get(
                                    "sha256", ""
                                )
                            ),
                        }
                        if isinstance(background_ref, Mapping) else None
                    ),
                    background_contract if isinstance(background_ref, Mapping) else None,
                    (
                        background_application_evidence
                        if isinstance(background_ref, Mapping) else None
                    ),
                )
                if isinstance(
                    run.get("schedule", {}).get("collective_workload_override"),
                    Mapping,
                ):
                    semantic_errors.extend(
                        _collective_semantic_recomputation_errors(
                            run,
                            semantic,
                            artifacts,
                            base_dir,
                            runtime_qualification_evidence,
                        )
                    )
                semantic_evidence = {
                    "schema_version": semantic.get("schema_version"),
                    "status": semantic.get("status"),
                    "mechanism_id": semantic.get("mechanism_id"),
                    "source_artifact_sha256": semantic.get("source_artifact_sha256"),
                    "source_artifacts_sha256": semantic.get(
                        "source_artifacts_sha256", {}),
                    "observed_effects": semantic.get("observed_effects", []),
                    "errors": semantic_errors,
                }
                run_errors.extend(semantic_errors)
            except (OSError, ValueError, json.JSONDecodeError) as error:
                run_errors.append(f"semantic_validation unreadable: {error}")
        if execution_status == "COMPLETE" and not run_errors and gate_required:
            simulator_stability_evidence, stability_errors = (
                _simulator_stability_evidence(
                    run, run_manifest, artifacts, base_dir
                )
            )
            run_errors.extend(stability_errors)
        if execution_status == "COMPLETE" and not run_errors:
            completed_count += 1
        evidence.append({
            "run_id": run_id,
            "execution_status": execution_status,
            "status": PASS if not run_errors else FAIL,
            "blocked_allowed": blocked_allowed,
            "artifacts": refs,
            "telemetry_observability": telemetry_evidence,
            "lifecycle": lifecycle_evidence,
            "runner_manifest": run_manifest_evidence,
            "workload_runtime_qualification": runtime_qualification_evidence,
            "semantic_validation": semantic_evidence,
            "simulator_stability": simulator_stability_evidence,
            "background_application": background_application_evidence,
            "background_link_map": background_link_map_evidence,
            "background_switch": background_switch_evidence,
            "background_rdma": background_rdma_evidence,
            "errors": run_errors,
        })
        errors.extend(f"{run_id}: {error}" for error in run_errors)
    return evidence, errors, completed_count


def _corpus_runtime_closure_evidence(
    corpus: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    artifact_evidence: Sequence[Mapping[str, Any]],
    base_dir: Path,
    *,
    executed: bool,
) -> Tuple[Dict[str, Any], List[str]]:
    """Require one independently valid closure authority for every P2 run."""

    if not executed:
        return {
            "status": PENDING,
            "identity_sha256": None,
            "authority_record_sha256": None,
            "required_run_reference_count": len(runs),
            "verified_run_reference_count": 0,
            "observed_identity_counts": {},
            "observed_authority_record_counts": {},
            "errors": [],
        }, []

    errors: List[str] = []
    top_record = corpus.get("simulator_runtime_closure")
    if not isinstance(top_record, Mapping):
        errors.append(
            "EXECUTED corpus lacks top-level simulator_runtime_closure authority"
        )
        top_record = {}
    top_hash = _canonical_hash(top_record) if top_record else None
    top_identity = top_record.get("identity_sha256") if top_record else None
    if top_record and set(top_record) != RUNTIME_CLOSURE_RECORD_FIELDS:
        errors.append(
            "corpus simulator_runtime_closure has the wrong key set: "
            f"missing={sorted(RUNTIME_CLOSURE_RECORD_FIELDS - set(top_record))}, "
            f"extra={sorted(set(top_record) - RUNTIME_CLOSURE_RECORD_FIELDS)}"
        )

    identity_counts: Counter[str] = Counter()
    authority_counts: Counter[str] = Counter()
    verified = 0
    by_run = {
        str(item.get("run_id")): item for item in artifact_evidence
        if isinstance(item, Mapping)
    }
    for run in runs:
        run_id = str(run.get("run_id", ""))
        artifact = by_run.get(run_id, {})
        runner_manifest = artifact.get("runner_manifest", {}) \
            if isinstance(artifact, Mapping) else {}
        closure = runner_manifest.get("runtime_closure", {}) \
            if isinstance(runner_manifest, Mapping) else {}
        if not isinstance(closure, Mapping) or closure.get("status") != PASS:
            errors.append(
                f"{run_id}: no independently verified sealed runtime closure"
            )
            continue
        identity = closure.get("identity_sha256")
        authority_hash = closure.get("authority_record_sha256")
        if isinstance(identity, str):
            identity_counts[identity] += 1
        if isinstance(authority_hash, str):
            authority_counts[authority_hash] += 1
        if identity != top_identity or authority_hash != top_hash:
            errors.append(
                f"{run_id}: runtime closure differs from corpus authority"
            )
            continue
        verified += 1

    final_validation: Dict[str, Any] = {}
    representative_run_dir: Path | None = None
    for run in runs:
        artifacts = _artifact_map(run)
        ref = artifacts.get("run_manifest")
        if isinstance(ref, Mapping):
            try:
                representative_run_dir = _ref_path(ref, base_dir).parent.resolve(
                    strict=True
                )
                break
            except OSError:
                continue
    if top_record and representative_run_dir is not None:
        _, final_validation, final_errors = _sealed_runtime_closure_evidence(
            top_record, representative_run_dir, {}
        )
        errors.extend(
            f"corpus runtime authority: {error}" for error in final_errors
        )
    elif top_record:
        errors.append(
            "corpus runtime authority cannot be revalidated without a sealed run"
        )

    if len(identity_counts) != 1:
        errors.append(
            "executed runs do not share exactly one runtime closure identity"
        )
    if len(authority_counts) != 1:
        errors.append(
            "executed runs do not share exactly one runtime authority record"
        )
    if verified != len(runs):
        errors.append(
            "not every executed corpus run binds the shared runtime closure: "
            f"verified={verified}, required={len(runs)}"
        )

    return {
        "status": PASS if not errors else FAIL,
        "identity_sha256": top_identity,
        "authority_record_sha256": top_hash,
        "required_run_reference_count": len(runs),
        "verified_run_reference_count": verified,
        "observed_identity_counts": dict(sorted(identity_counts.items())),
        "observed_authority_record_counts": dict(
            sorted(authority_counts.items())
        ),
        "independent_bundle_revalidation": final_validation,
        "errors": errors,
    }, errors


def coverage_checks(
    runs: Sequence[Mapping[str, Any]],
    schedule_evidence: Sequence[Mapping[str, Any]],
    expected_targets: Mapping[int, str],
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    checks: List[Dict[str, str]] = []
    schedule_by_run = {str(item.get("run_id")): item for item in schedule_evidence}

    def truth_value(run: Mapping[str, Any], name: str, default: Any = None) -> Any:
        value = _parameter(run, name, None)
        if value not in (None, "", []):
            return value
        row = schedule_by_run.get(str(run.get("run_id")), {}).get(
            "schedule_row", {})
        if isinstance(row, Mapping):
            value = row.get(name)
            if value not in (None, ""):
                return value
        return default

    classes = Counter(_normal_class(run.get("class_label")) for run in runs)
    _add(
        checks, "required_top_level_classes",
        PASS if REQUIRED_CLASSES <= set(classes) else FAIL,
        f"required={sorted(REQUIRED_CLASSES)}, observed={dict(classes)}",
    )
    # OOD stress runs intentionally include multi-target and non-ACCESS cases;
    # they are evaluated by split/manifest integrity but must not distort the
    # canonical single Plane-B ACCESS coverage requirement.
    gray = [
        run for run in runs
        if _normal_class(run.get("class_label")) == "GRAY_FAULT"
        and str(run.get("partition", "")) != "ood_stress"
    ]
    families = Counter(str(run.get("fault_family", "")) for run in gray)
    _add(
        checks, "required_gray_families",
        PASS if REQUIRED_GRAY_FAMILIES <= set(families) else FAIL,
        f"required={sorted(REQUIRED_GRAY_FAMILIES)}, observed={dict(families)}",
    )
    congestion = [
        run for run in runs if _normal_class(run.get("class_label")) == "CONGESTION"
    ]
    scenarios = Counter(str(run.get("scenario", "")) for run in congestion)
    _add(
        checks, "required_congestion_scenarios",
        PASS if REQUIRED_CONGESTION_SCENARIOS <= set(scenarios) else FAIL,
        f"required={sorted(REQUIRED_CONGESTION_SCENARIOS)}, observed={dict(scenarios)}",
    )

    bandwidth = [run for run in gray if run.get("fault_family") == "bandwidth_degradation"]
    fractions = [truth_value(run, "remaining_nominal_capacity_fraction",
                             truth_value(run, "severity_value"))
                 for run in bandwidth]
    shapes = {str(truth_value(run, "shape", "")) for run in bandwidth}
    ramp_durations = {
        int(value) for value in (
            truth_value(run, "ramp_duration_ns") for run in bandwidth
            if str(truth_value(run, "shape", "")) == "ramp"
        ) if value not in (None, "")
    }
    random_loss = [run for run in gray if run.get("fault_family") == "random_loss"]
    probabilities = [truth_value(run, "packet_error_probability",
                                 truth_value(run, "severity_value"))
                     for run in random_loss]
    burst = [run for run in gray if run.get("fault_family") == "burst_loss"]
    burst_shapes = {str(truth_value(run, "shape", "")) for run in burst}
    service = [run for run in gray if run.get("fault_family") == "service_degradation"]
    service_effects = set()
    for run in service:
        effects = truth_value(run, "effects", [])
        if isinstance(effects, str):
            try:
                decoded = json.loads(effects)
                effects = decoded if isinstance(decoded, list) else [effects]
            except json.JSONDecodeError:
                effects = [item for item in effects.split(";") if item]
        service_effects.update(effects if isinstance(effects, list) else [str(effects)])
    intermittent = [run for run in gray if run.get("fault_family") == "intermittent_service"]
    impairments = {str(truth_value(run, "impairment", "")) for run in intermittent}
    carrier_states = {str(truth_value(run, "carrier_state", "")) for run in intermittent}
    severity_ok = (
        _contains_required_floats(fractions, REQUIRED_BANDWIDTH_FRACTIONS)
        and {"step", "ramp"} <= shapes
        and REQUIRED_RAMP_DURATIONS_NS <= ramp_durations
        and _contains_required_floats(probabilities, REQUIRED_RANDOM_LOSS)
        and REQUIRED_BURST_SHAPES <= burst_shapes
        and REQUIRED_SERVICE_EFFECTS <= service_effects
        and REQUIRED_INTERMITTENT_IMPAIRMENTS <= impairments
        and carrier_states == {"up"}
    )
    severity_detail = {
        "bandwidth_fractions": sorted(_float_set(fractions)),
        "bandwidth_shapes": sorted(shapes),
        "ramp_durations_ns": sorted(ramp_durations),
        "random_loss_probabilities": sorted(_float_set(probabilities)),
        "burst_shapes": sorted(burst_shapes),
        "service_effects": sorted(service_effects),
        "intermittent_impairments": sorted(impairments),
        "intermittent_carrier_states": sorted(carrier_states),
    }
    _add(
        checks, "required_severity_and_shape_coverage",
        PASS if severity_ok else FAIL, str(severity_detail),
    )

    target_errors = []
    observed_targets = set()
    for run in gray:
        gpu = _int_or_none(run.get("target_gpu", run.get("target_gpu_id")))
        link = str(run.get("target_link_id", ""))
        if gpu not in expected_targets or expected_targets.get(gpu) != link:
            target_errors.append(
                f"{run.get('run_id')}: GPU/link {gpu}/{link} not Plane-B target"
            )
        else:
            observed_targets.add(link)
    _add(
        checks, "all_16_plane_b_targets_covered",
        PASS if not target_errors and observed_targets == set(expected_targets.values()) else FAIL,
        f"observed={sorted(observed_targets)}, errors={target_errors[:8]}",
    )

    timing_errors = []
    for run in runs:
        run_id = str(run.get("run_id", ""))
        class_label = _normal_class(run.get("class_label"))
        start = schedule_by_run.get(run_id, {}).get("event_start_ns")
        end = schedule_by_run.get(run_id, {}).get("event_end_ns")
        virtual_start = _int_or_none(run.get("virtual_start_ns"))
        virtual_finish = _int_or_none(run.get("virtual_finish_ns"))
        if None in (virtual_start, virtual_finish) or virtual_finish <= virtual_start:
            timing_errors.append(f"{run_id}: invalid virtual interval")
            continue
        if class_label == "HEALTHY":
            if virtual_finish - virtual_start < CAUSAL_WARMUP_NS:
                timing_errors.append(f"{run_id}: healthy span <100ms")
        else:
            if start is None or start - virtual_start < CAUSAL_WARMUP_NS:
                timing_errors.append(f"{run_id}: causal warmup <100ms")
            if end is None or start is None or end <= start:
                timing_errors.append(f"{run_id}: event duration is not positive")
        if run.get("recovery_evaluation") is True:
            reference = end if end and end > 0 else start
            if reference is None or virtual_finish - reference < RECOVERY_OBSERVATION_NS:
                timing_errors.append(f"{run_id}: recovery observation <1s")
    _add(
        checks, "duration_warmup_and_recovery_observation",
        PASS if not timing_errors else FAIL,
        f"errors={timing_errors[:12]}, total={len(timing_errors)}",
    )
    return checks, {
        "class_counts": dict(classes),
        "gray_family_counts": dict(families),
        "congestion_scenario_counts": dict(scenarios),
        "severity_coverage": severity_detail,
        "plane_b_target_links": sorted(observed_targets),
        "timing_error_count": len(timing_errors),
    }


def split_checks(
    runs: Sequence[Mapping[str, Any]], split: Mapping[str, Any],
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    checks: List[Dict[str, str]] = []
    entries, parse_errors = normalize_split_entries(split)
    run_ids = [str(run.get("run_id", "")) for run in runs]
    entry_ids = [str(item.get("run_id", "")) for item in entries]
    counts = Counter(entry_ids)
    partitions = {str(item.get("partition", "")) for item in entries}
    exact = (
        not parse_errors and set(entry_ids) == set(run_ids)
        and len(entry_ids) == len(run_ids)
        and all(count == 1 for count in counts.values())
        and partitions == PARTITIONS
    )
    _add(
        checks, "complete_atomic_run_level_split",
        PASS if exact else FAIL,
        f"corpus_runs={len(run_ids)}, split_entries={len(entry_ids)}, "
        f"partitions={sorted(partitions)}, duplicates={sorted(k for k,v in counts.items() if v != 1)}, "
        f"errors={parse_errors}",
    )
    partition_by_run = {
        str(item.get("run_id")): str(item.get("partition")) for item in entries
    }
    runs_by_id = {str(run.get("run_id")): run for run in runs}
    group_partitions: Dict[str, set[str]] = defaultdict(set)
    missing_groups = []
    for run_id, run in runs_by_id.items():
        group = str(run.get("split_group_id", ""))
        if not group:
            missing_groups.append(run_id)
        else:
            group_partitions[group].add(partition_by_run.get(run_id, ""))
    leaking_groups = {
        group: sorted(values) for group, values in group_partitions.items()
        if len(values) != 1
    }
    _add(
        checks, "split_group_isolation",
        PASS if not missing_groups and not leaking_groups else FAIL,
        f"missing_group_ids={missing_groups[:8]}, leaking_groups={dict(list(leaking_groups.items())[:8])}",
    )
    holdout = {
        int(value) for value in split.get("paired_holdout_gpu_ids", [])
        if _int_or_none(value) is not None
    }
    fault_runs = [
        run for run in runs
        if _normal_class(run.get("class_label")) in {"GRAY_FAULT", "HARD_FAULT"}
    ]
    seen_leaks = []
    unseen_gpus = set()
    invalid_unseen = []
    for run in fault_runs:
        run_id = str(run.get("run_id", ""))
        gpu = _int_or_none(run.get("target_gpu", run.get("target_gpu_id")))
        partition = partition_by_run.get(run_id)
        if partition in {"train", "validation", "seen_link_test"} and gpu in holdout:
            seen_leaks.append(run_id)
        if partition == "unseen_link_test":
            if gpu not in holdout:
                invalid_unseen.append(run_id)
            elif gpu is not None:
                unseen_gpus.add(gpu)
    paired_ok = (
        len(holdout) >= 4 and not seen_leaks and not invalid_unseen
        and holdout <= unseen_gpus
    )
    _add(
        checks, "paired_gpu_unseen_link_holdout",
        PASS if paired_ok else FAIL,
        f"holdout={sorted(holdout)}, unseen_covered={sorted(unseen_gpus)}, "
        f"seen_leaks={seen_leaks[:8]}, invalid_unseen={invalid_unseen[:8]}",
    )
    provenance_ok = (
        split.get("atomic_unit") == "complete_simulation_run"
        and split.get("generated_before_training") is True
        and split.get("immutable_after_training") is True
    )
    _add(
        checks, "split_provenance_frozen_before_training",
        PASS if provenance_ok else FAIL,
        f"atomic_unit={split.get('atomic_unit')}, "
        f"generated_before_training={split.get('generated_before_training')}, "
        f"immutable_after_training={split.get('immutable_after_training')}",
    )
    return checks, {
        "entry_count": len(entries),
        "partition_counts": dict(Counter(partition_by_run.values())),
        "paired_holdout_gpu_ids": sorted(holdout),
        "split_group_count": len(group_partitions),
        "leaking_group_count": len(leaking_groups),
    }


def build_observability_report(
    runs: Sequence[Mapping[str, Any]],
    artifact_evidence: Sequence[Mapping[str, Any]],
    mode: str,
) -> Dict[str, Any]:
    artifact_by_run = {item["run_id"]: item for item in artifact_evidence}
    event_runs = [
        run for run in runs
        if _normal_class(run.get("class_label")) in {"GRAY_FAULT", "HARD_FAULT"}
    ]
    scheduled = len(event_runs)
    actual_observability: Dict[str, Any] = {}
    for run in event_runs:
        run_id = str(run.get("run_id"))
        artifact = artifact_by_run.get(run_id, {})
        telemetry = artifact.get("telemetry_observability", {})
        actual_observability[run_id] = (
            telemetry.get("observable") if isinstance(telemetry, Mapping) else None
        )
    observable = sum(value is True for value in actual_observability.values())
    unobservable = sum(value is False for value in actual_observability.values())
    unknown = scheduled - observable - unobservable
    by_family: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"scheduled": 0, "observable": 0, "unobservable": 0,
                 "unknown": 0, "complete": 0, "blocked": 0}
    )
    rows = []
    for run in event_runs:
        run_id = str(run.get("run_id"))
        family = str(run.get("fault_family"))
        observed = actual_observability.get(run_id)
        artifact = artifact_by_run.get(run_id, {})
        telemetry = artifact.get("telemetry_observability", {})
        entry = by_family[family]
        entry["scheduled"] += 1
        if observed is True:
            entry["observable"] += 1
        elif observed is False:
            entry["unobservable"] += 1
        else:
            entry["unknown"] += 1
        if artifact.get("execution_status") == "COMPLETE":
            entry["complete"] += 1
        elif artifact.get("execution_status") == "BLOCKED":
            entry["blocked"] += 1
        rows.append({
            "run_id": run_id,
            "fault_family": family,
            "scheduled": True,
            "observable": observed,
            "first_observable_effect_ns": (
                telemetry.get("first_observable_effect_ns")
                if isinstance(telemetry, Mapping) else None
            ),
            "observed_signal_changes": (
                telemetry.get("observed_signal_changes", [])
                if isinstance(telemetry, Mapping) else []
            ),
            "execution_status": artifact.get("execution_status"),
            "artifact_status": artifact.get("status"),
        })
    errors = []
    if mode == EXECUTED:
        for run in event_runs:
            artifact = artifact_by_run.get(str(run.get("run_id")), {})
            if artifact.get("execution_status") == "COMPLETE":
                telemetry = artifact.get("telemetry_observability", {})
                observed = telemetry.get("observable") \
                    if isinstance(telemetry, Mapping) else None
                if observed not in {True, False}:
                    errors.append(
                        f"{run.get('run_id')}: telemetry-derived observable is not boolean"
                    )
                if (observed is True and _int_or_none(
                        telemetry.get("first_observable_effect_ns")) is None):
                    errors.append(
                        f"{run.get('run_id')}: telemetry-derived observable lacks first effect time"
                    )
    status = (
        FAIL if errors else PREPARED if mode == PREPARED else PASS
    )
    return {
        "schema_version": "limer.p2-observability-report.v1",
        "status": status,
        "mode": mode,
        "scheduled_event_count": scheduled,
        "observable_event_count": observable,
        "unobservable_event_count": unobservable,
        "unknown_observability_count": unknown,
        "by_fault_family": dict(sorted(by_family.items())),
        "runs": rows,
        "errors": errors,
    }


def build_leakage_report(
    corpus: Mapping[str, Any], split: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]], schedule_evidence: Sequence[Mapping[str, Any]],
    split_result: Tuple[List[Dict[str, str]], Dict[str, Any]],
    feature_schema: Mapping[str, Any], feature_ref_ok: bool,
    feature_provenance: Mapping[str, Any], split_hash_ok: bool,
) -> Dict[str, Any]:
    checks: List[Dict[str, str]] = []
    model_columns = feature_schema.get(
        "model_feature_columns", feature_schema.get("features", []))
    identifiers = set(feature_schema.get("identifier_columns", []))
    labels = set(feature_schema.get("label_columns", []))
    if not isinstance(model_columns, list):
        model_columns = []
    forbidden = forbidden_feature_columns(model_columns)
    raw_utilization_provenance = {}
    if "utilization" in {str(value).strip().lower() for value in model_columns}:
        column_provenance = feature_schema.get("column_provenance", {})
        if isinstance(column_provenance, Mapping):
            candidate = column_provenance.get("utilization", {})
            if isinstance(candidate, Mapping):
                raw_utilization_provenance = dict(candidate)
        safe_recomputation = (
            raw_utilization_provenance.get("recomputed") is True
            and raw_utilization_provenance.get("denominator")
            == "nominal_bandwidth_bps_from_immutable_link_map"
            and raw_utilization_provenance.get("uses_configured_bandwidth_bps") is False
        )
        if safe_recomputation:
            forbidden = [value for value in forbidden
                         if str(value).strip().lower() != "utilization"]
    overlap = sorted(set(model_columns) & (identifiers | labels))
    _add(
        checks, "feature_schema_hash_or_prepared_inline_contract",
        PASS if feature_ref_ok else FAIL,
        f"provenance={dict(feature_provenance)}",
    )
    _add(
        checks, "model_features_exclude_label_fault_and_config_fields",
        PASS if model_columns and not forbidden and not overlap else FAIL,
        f"feature_count={len(model_columns)}, forbidden={forbidden}, "
        f"identifier_or_label_overlap={overlap}",
    )
    schedule_failures = [
        item["run_id"] for item in schedule_evidence if item["status"] != PASS
    ]
    _add(
        checks, "schedule_truth_independent_of_features",
        PASS if not schedule_failures else FAIL,
        f"failed_runs={schedule_failures[:12]}, total={len(schedule_failures)}",
    )
    for item in split_result[0]:
        checks.append(dict(item))
    _add(
        checks, "split_manifest_binds_exact_corpus_hash",
        PASS if split_hash_ok else FAIL,
        f"declared={split.get('corpus_manifest_sha256')}",
    )
    failed = sum(item["status"] == FAIL for item in checks)
    return {
        "schema_version": "limer.p2-leakage-checks.v1",
        "status": PASS if failed == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failed,
            "pending": sum(item["status"] == PENDING for item in checks),
        },
        "model_feature_columns": model_columns,
        "forbidden_feature_columns": forbidden,
        "raw_utilization_provenance": raw_utilization_provenance,
        "split": split_result[1],
        "checks": checks,
    }


def _summarize(checks: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    return {
        "pass": sum(item.get("status") == PASS for item in checks),
        "fail": sum(item.get("status") == FAIL for item in checks),
        "pending": sum(item.get("status") == PENDING for item in checks),
    }


def evaluate(
    p1_gate_path: Path,
    corpus_path: Path,
    split_path: Path,
    *,
    allow_runtime_test_contract: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    p1_gate = _load_json(p1_gate_path)
    corpus = _load_json(corpus_path)
    split = _load_json(split_path)
    base_dir = corpus_path.parent
    mode = str(corpus.get("status", "")).upper()
    if mode not in {PREPARED, EXECUTED}:
        mode = "INVALID"
    executed = mode == EXECUTED
    checks: List[Dict[str, str]] = []
    _add(
        checks, "p1_prerequisite_passed",
        PASS if (
            p1_gate.get("stage") == "P1" and p1_gate.get("status") == PASS
            and p1_gate.get("next_stage") == "P2"
            and int(p1_gate.get("summary", {}).get("fail", -1)) == 0
            and int(p1_gate.get("summary", {}).get("skip", -1)) == 0
        ) else FAIL,
        f"stage={p1_gate.get('stage')}, status={p1_gate.get('status')}, "
        f"next={p1_gate.get('next_stage')}, summary={p1_gate.get('summary')}",
    )
    _add(
        checks, "corpus_and_split_schema",
        PASS if (
            corpus.get("schema_version") == "limer.p2-corpus-manifest.v1"
            and split.get("schema_version") == "limer.p2-split-manifest.v1"
            and corpus.get("contract_id") == CONTRACT_ID
            and split.get("contract_id") == CONTRACT_ID
            and mode in {PREPARED, EXECUTED}
            and str(split.get("status", "")).upper() in {"", mode}
        ) else FAIL,
        f"corpus_schema={corpus.get('schema_version')}, "
        f"split_schema={split.get('schema_version')}, mode={mode}, "
        f"split_status={split.get('status')}",
    )
    runs_raw = corpus.get("runs", [])
    runs = [run for run in runs_raw if isinstance(run, Mapping)]
    run_ids = [str(run.get("run_id", "")) for run in runs]
    duplicate_ids = sorted(
        run_id for run_id, count in Counter(run_ids).items() if not run_id or count != 1
    )
    _add(
        checks, "nonempty_unique_run_inventory",
        PASS if runs and len(runs) == len(runs_raw) and not duplicate_ids else FAIL,
        f"runs={len(runs)}, raw_entries={len(runs_raw) if isinstance(runs_raw, list) else 'invalid'}, "
        f"duplicate_or_empty={duplicate_ids}",
    )

    identity_evidence, identity_errors = _corpus_identity_evidence(
        corpus, require_v2=executed,
    )
    input_ref_evidence: Dict[str, Any] = {}
    input_artifacts = corpus.get("input_artifacts", {})
    if not isinstance(input_artifacts, Mapping):
        input_artifacts = {}
    for name in REQUIRED_INPUT_ARTIFACTS:
        ok, ref_evidence = verify_ref(input_artifacts.get(name), base_dir)
        input_ref_evidence[name] = ref_evidence
        if not ok:
            identity_errors.append(f"input_artifacts.{name} reference/hash invalid")
    split_ref = corpus.get("split_manifest", {})
    embedded_holdouts = split_ref.get("paired_holdout_gpu_ids") \
        if isinstance(split_ref, Mapping) else None
    if embedded_holdouts != split.get("paired_holdout_gpu_ids"):
        identity_errors.append(
            "identity holdouts differ from the evaluated split manifest"
        )
    _add(
        checks, "v2_corpus_identity_and_input_hashes",
        PASS if not identity_errors else FAIL,
        f"identity={identity_evidence}, inputs={input_ref_evidence}, "
        f"errors={identity_errors[:12]}",
    )

    topology_ref = corpus.get("topology", {}).get("link_map", {})
    if not isinstance(topology_ref, Mapping) or not topology_ref.get("path"):
        topology_ref = corpus.get("input_artifacts", {}).get("link_map", {})
    topology_identity_sha = input_artifacts.get("link_map", {}).get("sha256") \
        if isinstance(input_artifacts.get("link_map", {}), Mapping) else None
    _add(
        checks, "topology_link_map_matches_v2_identity",
        PASS if topology_ref.get("sha256") == topology_identity_sha else FAIL,
        f"topology_sha256={topology_ref.get('sha256')}, "
        f"identity_sha256={topology_identity_sha}",
    )
    topology_ref_ok, topology_evidence = verify_ref(topology_ref, base_dir)
    targets: Dict[int, str] = {}
    target_errors = []
    if topology_ref_ok:
        targets, target_errors = expected_plane_b_targets(
            _ref_path(topology_ref, base_dir))
    _add(
        checks, "immutable_true16_plane_b_inventory",
        PASS if topology_ref_ok and not target_errors else FAIL,
        f"reference={topology_evidence}, errors={target_errors}",
    )

    schedule_base_dir = base_dir
    source_corpus_ref = corpus.get("source_corpus", {})
    if executed and isinstance(source_corpus_ref, Mapping):
        source_path = source_corpus_ref.get("path")
        if isinstance(source_path, str) and source_path:
            schedule_base_dir = _resolve(source_path, base_dir).parent
    schedule_evidence, schedule_errors = inspect_schedules(
        runs, schedule_base_dir, input_artifacts
    )
    _add(
        checks, "every_run_schedule_hash_and_truth_valid",
        PASS if not schedule_errors and len(schedule_evidence) == len(runs) else FAIL,
        f"failed={len(schedule_errors)}, examples={schedule_errors[:12]}",
    )
    coverage, coverage_summary = coverage_checks(runs, schedule_evidence, targets)
    checks.extend(coverage)

    split_result = split_checks(runs, split)
    corpus_hash = _sha256(corpus_path)
    declared_corpus_hash = split.get("corpus_manifest_sha256")
    split_ref = corpus.get("split_manifest", {})
    split_ref_ok = False
    if isinstance(split_ref, Mapping) and split_ref.get("path"):
        split_ref_ok, _ = verify_ref(split_ref, base_dir)
        split_ref_ok = (
            split_ref_ok
            and _ref_path(split_ref, base_dir).resolve() == split_path.resolve()
        )
    split_hash_ok = (
        declared_corpus_hash == corpus_hash
        if declared_corpus_hash is not None
        else split_ref_ok and bool(corpus.get("corpus_id"))
        and split.get("corpus_id") == corpus.get("corpus_id")
    )
    feature_schema, feature_ref_ok, feature_provenance = load_feature_schema(
        corpus, base_dir, executed)
    leakage = build_leakage_report(
        corpus, split, runs, schedule_evidence, split_result,
        feature_schema, feature_ref_ok, feature_provenance, split_hash_ok,
    )
    _add(
        checks, "leakage_and_split_checks_pass",
        PASS if leakage["status"] == PASS else FAIL,
        f"status={leakage['status']}, summary={leakage['summary']}",
    )

    mechanism_evidence, mechanism_errors = inspect_mechanism_contracts(runs, mode)
    _add(
        checks, "mechanism_identity_and_execution_semantics",
        PASS if not mechanism_errors else FAIL,
        f"failed={len(mechanism_errors)}, examples={mechanism_errors[:12]}",
    )
    artifact_evidence, artifact_errors, complete_count = inspect_run_artifacts(
        runs,
        base_dir,
        executed,
        schedule_evidence,
        corpus,
        expected_link_map_sha256=topology_evidence.get("actual_sha256"),
        frozen_link_map_path=(
            _ref_path(topology_ref, base_dir) if topology_ref_ok else None
        ),
        allow_runtime_test_contract=allow_runtime_test_contract,
    )
    runtime_closure_evidence, runtime_closure_errors = (
        _corpus_runtime_closure_evidence(
            corpus, runs, artifact_evidence, base_dir, executed=executed
        )
    )
    _add(
        checks,
        "shared_sealed_simulator_runtime_closure",
        (
            PENDING
            if mode == PREPARED
            else PASS if not runtime_closure_errors else FAIL
        ),
        "identity="
        f"{runtime_closure_evidence.get('identity_sha256')}, "
        "verified_runs="
        f"{runtime_closure_evidence.get('verified_run_reference_count')}/"
        f"{runtime_closure_evidence.get('required_run_reference_count')}, "
        f"errors={runtime_closure_errors[:12]}",
    )
    if mode == PREPARED:
        _add(
            checks, "actual_run_artifacts_complete_and_hashed", PENDING,
            f"dry-run plan has {len(runs)} planned runs; execution artifacts required for PASS",
        )
    else:
        _add(
            checks, "actual_run_artifacts_complete_and_hashed",
            PASS if not artifact_errors and complete_count > 0 else FAIL,
            f"complete_runs={complete_count}, errors={artifact_errors[:12]}, "
            f"total_errors={len(artifact_errors)}",
        )

    stability_errors = []
    stability_pending = []
    artifact_by_run = {
        str(item.get("run_id")): item for item in artifact_evidence
    }
    for run in runs:
        stability = run.get("simulator_stability", {})
        gate_required = (
            isinstance(stability, Mapping)
            and stability.get("gate_required") is True
        )
        if run.get("fault_family") == "random_loss" and not gate_required:
            stability_errors.append(
                f"{run.get('run_id')}: random_loss gate_required is not true"
            )
            continue
        if not gate_required:
            continue
        status = stability.get("status") if isinstance(stability, Mapping) else None
        if mode == PREPARED and status in {
                None, "PLANNED", "PENDING_EXECUTION", "BLOCKED_UNSUPPORTED"}:
            stability_pending.append(str(run.get("run_id")))
        elif mode == EXECUTED and status == PASS:
            artifact = artifact_by_run.get(str(run.get("run_id")), {})
            stability_evidence = artifact.get("simulator_stability", {}) \
                if isinstance(artifact, Mapping) else {}
            if (
                not isinstance(stability_evidence, Mapping)
                or stability_evidence.get("status") != PASS
            ):
                stability_errors.append(
                    f"{run.get('run_id')}: PASS lacks independently "
                    "recomputed stability evidence"
                )
        else:
            stability_errors.append(
                f"{run.get('run_id')}: stability status={status}, "
                f"reason={stability.get('reason') if isinstance(stability, Mapping) else None}"
            )
    stability_status = (
        FAIL if stability_errors else PENDING if stability_pending else PASS
    )
    _add(
        checks, "all_declared_simulator_stability_gates_accounted",
        stability_status,
        f"pending={stability_pending}, errors={stability_errors}",
    )
    observability = build_observability_report(runs, artifact_evidence, mode)
    observability_ok = (
        observability["status"] == PASS if executed
        else observability["status"] == PREPARED
    )
    _add(
        checks, "scheduled_and_observable_events_accounted",
        PASS if observability_ok else FAIL,
        f"status={observability['status']}, scheduled={observability['scheduled_event_count']}, "
        f"observable={observability['observable_event_count']}, "
        f"unknown={observability['unknown_observability_count']}",
    )
    summary = _summarize(checks)
    if summary["fail"]:
        status = FAIL
    elif mode == PREPARED:
        status = PREPARED
    elif mode == EXECUTED and summary["pending"] == 0:
        status = PASS
    else:
        status = FAIL
    gate = {
        "schema_version": SCHEMA_VERSION,
        "stage": "P2",
        "contract_id": CONTRACT_ID,
        "mode": mode,
        "status": status,
        "summary": summary,
        "next_stage": "P3" if status == PASS else None,
        "inputs": {
            "stage_gate_p1.json": _artifact(p1_gate_path),
            "corpus_manifest.json": _artifact(corpus_path),
            "split_manifest.json": _artifact(split_path),
        },
        "coverage": coverage_summary,
        "artifact_verification": artifact_evidence,
        "simulator_runtime_closure_verification": runtime_closure_evidence,
        "mechanism_verification": mechanism_evidence,
        "schedule_verification": schedule_evidence,
        "claim_boundary": (
            "PREPARED validates only the immutable corpus plan. PASS additionally "
            "validates actual SimAI artifacts and unlocks P3; neither status is "
            "evidence about a real switch, NIC, RDMA, or NCCL deployment."
        ),
        "checks": checks,
    }
    return gate, observability, leakage


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_markdown(gate: Mapping[str, Any], path: Path) -> None:
    lines = [
        "# LIMER P2 stage gate", "",
        f"**{gate['status']}: {gate['summary']['pass']} passed, "
        f"{gate['summary']['fail']} failed, "
        f"{gate['summary']['pending']} pending.**", "",
        f"Next stage: **{gate.get('next_stage') or 'BLOCKED'}**", "",
        "| Check | Status | Detail |", "|---|---|---|",
    ]
    for item in gate.get("checks", []):
        detail = str(item.get("detail", "")).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {item.get('check')} | {item.get('status')} | {detail} |")
    lines.extend(["", "## Claim boundary", "", str(gate.get("claim_boundary", ""))])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def materialize(
    p1_gate_path: Path,
    corpus_path: Path,
    split_path: Path,
    out_dir: Path,
    *,
    allow_runtime_test_contract: bool = False,
) -> Dict[str, Any]:
    gate, observability, leakage = evaluate(
        p1_gate_path, corpus_path, split_path,
        allow_runtime_test_contract=allow_runtime_test_contract,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    observability_path = out_dir / "observability_report.json"
    leakage_path = out_dir / "leakage_checks.json"
    _write_json(observability, observability_path)
    _write_json(leakage, leakage_path)
    gate["artifacts"] = {
        "corpus_manifest.json": _artifact(corpus_path),
        "split_manifest.json": _artifact(split_path),
        "observability_report.json": _artifact(observability_path),
        "leakage_checks.json": _artifact(leakage_path),
    }
    _write_json(gate, out_dir / "stage_gate_p2.json")
    _write_markdown(gate, out_dir / "stage_gate_p2.md")
    return gate


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--p1-gate", required=True, type=Path)
    parser.add_argument("--corpus-manifest", required=True, type=Path)
    parser.add_argument("--split-manifest", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        gate = materialize(
            args.p1_gate, args.corpus_manifest, args.split_manifest, args.out_dir)
    except (OSError, ValueError, KeyError, json.JSONDecodeError, pd.errors.ParserError) as error:
        gate = {
            "schema_version": SCHEMA_VERSION,
            "stage": "P2",
            "status": FAIL,
            "summary": {"pass": 0, "fail": 1, "pending": 0},
            "next_stage": None,
            "error": str(error),
            "checks": [],
            "claim_boundary": "P2 evaluation failed before evidence could be validated.",
        }
        args.out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(gate, args.out_dir / "stage_gate_p2.json")
        _write_markdown(gate, args.out_dir / "stage_gate_p2.md")
    print(
        f"{gate['status']}: {gate['summary']['pass']} passed, "
        f"{gate['summary']['fail']} failed, "
        f"{gate['summary']['pending']} pending"
    )
    print(f"Wrote P2 reports under {args.out_dir}")
    return 0 if gate["status"] in {PASS, PREPARED} else 1


if __name__ == "__main__":
    raise SystemExit(main())
