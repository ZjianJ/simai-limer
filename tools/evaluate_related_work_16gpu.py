#!/usr/bin/env python3
"""Evaluate eight related systems on one explicitly-audited 16-GPU ledger.

This is a mechanism-level benchmark, not a claim that eight authors' original
artifacts were ported into SimAI.  The output keeps measured trace replay,
paper-calibrated timing, analytical projection, and executable protocol models
in separate columns and applies the project SLOs without treating N/A as PASS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from related_work_common import (
    BLOCKED_PRECONDITION,
    FAIL,
    NOT_APPLICABLE,
    PASS,
    UNVERIFIED,
    aggregate_detection,
    build_detection_timeline,
    collective_safety_cases,
    optcc_theorem13_normalized_ratio,
    optional_int,
)


RELATED_WORK = [
    "FANcY", "Trumpet", "NetBouncer", "MP-RDMA",
    "Flor", "SHIFT", "OptCC", "ReCoVer",
]


def load_json(path: Path) -> Dict[str, Any]:
    with path.open() as stream:
        return json.load(stream)


def write_json(path: Path, value: Any) -> None:
    with path.open("w") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write("\n")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_evidence(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "exists": path.is_file(),
        "size_bytes": path.stat().st_size if path.is_file() else None,
        "sha256": sha256_file(path) if path.is_file() else None,
    }


def id_set_digest(values: List[str]) -> str:
    payload = json.dumps(sorted(set(values)), separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def build_monitoring_validation_audit(
    path: Optional[Path],
    run_dir: Optional[Path],
    contract: Dict[str, Any],
) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {"provided": False, "validated": False}
    raw = load_json(path)
    indexed = {row["check"]: row for row in raw.get("checks", [])}
    required = [
        "physical_link_coverage", "endpoint_sample_coverage",
        "queue_peak_consistent", "live_rate_and_state_fields",
        "monitoring_off_result_unchanged",
    ]
    coverage_detail = indexed.get("physical_link_coverage", {}).get("detail")
    coverage_match = re.search(r"observed\s+(\d+)/(\d+)\s+physical links", coverage_detail or "")
    expected_physical = int(contract["expected_physical_link_count"])
    exact_coverage = bool(
        coverage_match
        and int(coverage_match.group(1)) == expected_physical
        and int(coverage_match.group(2)) == expected_physical)
    raw_checks: Dict[str, Any] = {"provided": False, "validated": False}
    if run_dir is not None:
        raw_paths = {
            "link_map": run_dir / "link_map.csv",
            "switch": run_dir / "switch_telemetry.csv",
            "nic": run_dir / "nic_telemetry.csv",
        }
        if all(item.is_file() for item in raw_paths.values()):
            link_map = pd.read_csv(raw_paths["link_map"])
            switch = pd.read_csv(raw_paths["switch"])
            nic = pd.read_csv(raw_paths["nic"])
            physical_links = set(link_map["link_id"].astype(str))
            switch_links = set(switch["link_id"].astype(str))
            timestamps = sorted(int(value) for value in switch["timestamp_ns"].unique())
            tx_counts = switch[switch["direction"] == "tx"].groupby(
                "timestamp_ns").size()
            rx_counts = switch[switch["direction"] == "rx"].groupby(
                "timestamp_ns").size()
            nic_counts = nic.groupby("timestamp_ns").size()
            per_snapshot_links = switch.groupby("timestamp_ns")["link_id"].nunique()
            switch_duplicates = int(switch.duplicated([
                "timestamp_ns", "switch_id", "port_id", "direction", "link_id"]).sum())
            nic_duplicates = int(nic.duplicated([
                "timestamp_ns", "node_id", "nic_id", "link_id"]).sum())
            tx_by_link = switch[switch["direction"] == "tx"].groupby(
                "link_id")["tx_bytes"].max()
            raw_validated = bool(
                len(link_map) == expected_physical
                and link_map["link_id"].nunique() == expected_physical
                and physical_links == switch_links
                and len(timestamps) > 1
                and set(np.diff(timestamps)) == {
                    int(contract["telemetry_interval_ns"])}
                and set(int(value) for value in tx_counts) == {
                    int(contract["expected_fabric_tx_endpoints_per_snapshot"])}
                and set(int(value) for value in rx_counts) == {
                    int(contract["expected_fabric_rx_endpoints_per_snapshot"])}
                and set(int(value) for value in nic_counts) == {
                    int(contract["expected_host_endpoints_per_snapshot"])}
                and set(int(value) for value in per_snapshot_links) == {
                    expected_physical}
                and switch_duplicates == 0 and nic_duplicates == 0
                and set(tx_by_link.index.astype(str)) == physical_links
                and bool((tx_by_link > 0).all()))
            raw_checks = {
                "provided": True,
                "validated": raw_validated,
                "run_dir": str(run_dir.resolve()),
                "files": {name: file_evidence(item)
                          for name, item in raw_paths.items()},
                "physical_links": len(physical_links),
                "snapshot_count": len(timestamps),
                "snapshot_intervals_ns": sorted(
                    int(value) for value in set(np.diff(timestamps))),
                "fabric_tx_rows_per_snapshot": sorted(
                    int(value) for value in set(tx_counts)),
                "fabric_rx_rows_per_snapshot": sorted(
                    int(value) for value in set(rx_counts)),
                "host_rows_per_snapshot": sorted(
                    int(value) for value in set(nic_counts)),
                "physical_links_per_snapshot": sorted(
                    int(value) for value in set(per_snapshot_links)),
                "switch_duplicate_endpoint_rows": switch_duplicates,
                "nic_duplicate_endpoint_rows": nic_duplicates,
                "all_physical_links_have_nonzero_tx": bool((tx_by_link > 0).all()),
            }
    return {
        "provided": True,
        "file": file_evidence(path),
        "validated": bool(
            raw.get("summary", {}).get("fail") == 0
            and exact_coverage
            and raw_checks["validated"]
            and all(indexed.get(name, {}).get("status") == PASS
                    for name in required)),
        "required_checks": {name: indexed.get(name) for name in required},
        "raw_run_checks": raw_checks,
        "physical_link_coverage_detail": coverage_detail,
        "physical_link_count": int(coverage_match.group(2)) if coverage_match else None,
        "evidence_scope": "existing_2x8_healthy_run",
        "true16_fault_run_coverage_verified": False,
        "sampling_semantics": (
            "complete discrete snapshots at the configured interval; not a "
            "continuous-time record of every instant"),
    }


def parse_workload_header(path: Path) -> Dict[str, Any]:
    text = path.read_text(errors="replace").splitlines()[0]

    def value(name: str) -> Optional[int]:
        match = re.search(rf"(?:^|\s){re.escape(name)}:\s*(\d+)(?:\s|$)", text)
        return int(match.group(1)) if match else None

    return {
        "path": str(path.resolve()),
        "header": text,
        "model_parallel_npu_group": value("model_parallel_NPU_group"),
        "declared_all_gpus": value("all_gpus"),
        "expert_parallel_group": value("ep"),
        "pipeline_parallel_group": value("pp"),
    }


def parse_smoke_log(
    path: Optional[Path],
    smoke_inputs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {
            "provided": False,
            "path": str(path.resolve()) if path is not None else None,
            "true_16rank_ring_observed": False,
            "simulation_completed": False,
        }
    text = path.read_text(errors="replace")
    ring16 = bool(re.search(r"dimension: local total nodes in ring: 16(?:\s|$)", text))
    model16 = "model_parallel_NPU_group is 16" in text
    layer_finished = "fwd pass comm collective for layer: layer_00 is finished" in text
    pass_finished = bool(re.search(r"pass:\s*0\s+finished at time:\s*\d+", text))
    crashed = ("double free or corruption" in text or "Aborted" in text
               or "segmentation fault" in text.lower())
    completed = ("all passes finished at time:" in text
                 and "Percentage of finished streams: 100" in text
                 and not crashed)
    finish_match = re.search(r"all passes finished at time:\s*(\d+)", text)
    result = {
        "provided": True,
        "path": str(path.resolve()),
        "model_parallel_16_observed": model16,
        "ring_size_16_observed": ring16,
        "true_16rank_ring_observed": bool(model16 and ring16),
        "first_collective_finished_in_log": layer_finished,
        "pass_finished_in_log": pass_finished,
        "process_crash_observed": crashed,
        "simulation_completed": bool(completed),
        "completion_time_ns": int(finish_match.group(1)) if finish_match else None,
    }
    run_dir = path.parent
    provenance_path = run_dir / "input.sha256"
    provenance_entries: Dict[str, str] = {}
    if provenance_path.is_file():
        for line in provenance_path.read_text(errors="replace").splitlines():
            fields = line.split(maxsplit=1)
            if len(fields) == 2:
                recorded_path = Path(fields[1].lstrip(" *")).resolve()
                provenance_entries[str(recorded_path)] = fields[0]
    expected_inputs = [item.resolve() for item in (smoke_inputs or [])]
    provenance_matches = bool(expected_inputs) and all(
        item.is_file()
        and provenance_entries.get(str(item)) == sha256_file(item)
        for item in expected_inputs)
    result.update({
        "input_provenance_path": str(provenance_path.resolve()),
        "input_provenance_present": provenance_path.is_file(),
        "provenance_matches_current_inputs": provenance_matches,
    })
    collective_path = run_dir / "collective_telemetry.csv"
    switch_path = run_dir / "switch_telemetry.csv"
    nic_path = run_dir / "nic_telemetry.csv"
    if collective_path.is_file():
        collective = pd.read_csv(collective_path)
        ranks = sorted(int(value) for value in collective["rank_id"].dropna().unique())
        result.update({
            "flow_completion_rows": len(collective),
            "flow_rank_ids": ranks,
            "all_16_ranks_have_completed_flows": ranks == list(range(16)),
            "all_flow_rows_status_ok": bool(len(collective)
                                            and (collective["status"] == "ok").all()),
            "telemetry_world_sizes": sorted(
                int(value) for value in collective["world_size"].dropna().unique()),
            "flow_completion_max_ns": (
                int(collective["finish_time_ns"].max()) if len(collective) else None),
            "collective_telemetry_semantics": "flow_sender_completion",
            "collective_commit_observed": False,
        })
    result["periodic_switch_rows"] = (
        len(pd.read_csv(switch_path)) if switch_path.is_file() else None)
    result["periodic_nic_rows"] = (
        len(pd.read_csv(nic_path)) if nic_path.is_file() else None)
    result["periodic_rows_note"] = (
        "The 151 us smoke finishes before the 1 ms periodic sampler; empty "
        "switch/NIC snapshots are expected and are not a coverage validation."
        if result.get("completion_time_ns", 10**18) < 1_000_000 else None)
    result["validated_healthy_smoke"] = bool(
        completed and provenance_matches and model16 and ring16
        and layer_finished and pass_finished
        and result.get("all_16_ranks_have_completed_flows", False)
        and result.get("all_flow_rows_status_ok", False)
        and result.get("telemetry_world_sizes") == [16])
    return result


def build_provenance_audit(
    events: pd.DataFrame,
    feature_samples: pd.DataFrame,
    manifest_path: Path,
    input_paths: List[Path],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    manifest = load_json(manifest_path)
    manifest_runs = {str(row["run_id"]): row for row in manifest.get("runs", [])}
    selected_ids = [str(value) for value in events["run_id"]]
    missing = sorted(set(selected_ids) - set(manifest_runs))
    selected_manifest = [manifest_runs[run_id] for run_id in selected_ids
                         if run_id in manifest_runs]
    locked_test_split = bool(
        len(selected_manifest) == len(selected_ids)
        and all(row.get("split") == "test" for row in selected_manifest))

    schedule_matches = not missing
    mismatches: List[str] = []
    for _, event in events.iterrows():
        source = manifest_runs.get(str(event["run_id"]))
        if source is None:
            continue
        exact_fields = ("target_link_id", "fault_start_ns", "fault_end_ns")
        if any(str(source.get(field)) != str(event[field]) for field in exact_fields):
            schedule_matches = False
            mismatches.append(str(event["run_id"]))
        if not np.isclose(float(source.get("severity", 0.0)), float(event["severity"])):
            schedule_matches = False
            mismatches.append(str(event["run_id"]))

    selected_samples = feature_samples[
        feature_samples["run_id"].astype(str).isin(selected_ids)]
    sample_runs = set(selected_samples["run_id"].astype(str))
    interval_values: set[int] = set()
    for _, group in selected_samples.groupby("run_id"):
        timestamps = np.sort(group["timestamp_ns"].unique())
        interval_values.update(int(value) for value in np.diff(timestamps))
    expected_interval = int(config["platform_contract"]["telemetry_interval_ns"])
    manifest_links = [str(value) for value in manifest.get("access_links", [])]
    manifest_root = manifest_path.parent
    sidecar_total = 0
    sidecar_failures: List[str] = []
    for row in manifest.get("runs", []):
        for path_key, hash_key in [
            ("workload_path", "workload_sha256"),
            ("fault_events_path", "fault_events_sha256"),
        ]:
            sidecar_total += 1
            sidecar = manifest_root / str(row[path_key])
            if (not sidecar.is_file()
                    or sha256_file(sidecar) != str(row[hash_key])):
                sidecar_failures.append(f"{row['run_id']}:{path_key}")

    manifest_split = {str(row["run_id"]): str(row["split"])
                      for row in manifest.get("runs", [])}
    dataset_split = {
        str(run_id): str(group["split"].iloc[0])
        for run_id, group in feature_samples.groupby("run_id")}
    dataset_split_match = (
        set(dataset_split) == set(manifest_split)
        and all(dataset_split[run_id] == split
                for run_id, split in manifest_split.items()))
    duplicate_keys = int(feature_samples.duplicated(
        ["run_id", "timestamp_ns", "link_id", "switch_id"]).sum())
    snapshot_link_counts = feature_samples.groupby(
        ["run_id", "timestamp_ns"])["link_id"].nunique()
    expected_link_count = int(config["platform_contract"]["expected_access_link_count"])
    incomplete_snapshots = int((snapshot_link_counts != expected_link_count).sum())
    observed_links = sorted(feature_samples["link_id"].astype(str).unique())
    first_timestamps = feature_samples.groupby("run_id")["timestamp_ns"].min()
    healthy_test_ids = sorted(
        str(row["run_id"]) for row in manifest.get("runs", [])
        if row.get("split") == "test" and row.get("scenario") == "HEALTHY")
    healthy_samples = feature_samples[
        feature_samples["run_id"].astype(str).isin(healthy_test_ids)]
    healthy_exposure_ns = int(healthy_samples.groupby("run_id")["timestamp_ns"].max().sum())
    test_ids = [str(row["run_id"]) for row in manifest.get("runs", [])
                if row.get("split") == "test"]
    return {
        "schema_version": 1,
        "manifest": file_evidence(manifest_path),
        "inputs": [file_evidence(path) for path in input_paths],
        "selected_event_count": len(events),
        "manifest_run_count": len(manifest_runs),
        "manifest_split_counts": {
            str(key): int(value) for key, value in
            pd.Series(list(manifest_split.values())).value_counts().items()},
        "selected_run_ids_unique": len(set(selected_ids)) == len(selected_ids),
        "all_selected_runs_in_manifest": not missing,
        "missing_manifest_runs": missing,
        "all_selected_runs_locked_test_split": locked_test_split,
        "event_schedule_matches_manifest": schedule_matches,
        "schedule_mismatch_runs": sorted(set(mismatches)),
        "all_selected_runs_in_feature_dataset": set(selected_ids) <= sample_runs,
        "feature_dataset_selected_run_count": len(sample_runs),
        "feature_dataset_run_set_and_split_match_manifest": dataset_split_match,
        "feature_dataset_duplicate_key_count": duplicate_keys,
        "feature_dataset_incomplete_snapshot_count": incomplete_snapshots,
        "feature_dataset_access_set_matches_manifest": observed_links == sorted(manifest_links),
        "feature_dataset_first_timestamp_ns": sorted(
            int(value) for value in first_timestamps.unique()),
        "manifest_access_links": manifest_links,
        "manifest_access_link_count": len(manifest_links),
        "observed_timestamp_intervals_ns": sorted(interval_values),
        "telemetry_interval_matches_contract": interval_values == {expected_interval},
        "manifest_sidecar_count": sidecar_total,
        "manifest_sidecar_hash_failures": sidecar_failures,
        "all_manifest_sidecar_hashes_match": not sidecar_failures,
        "test_run_count": len(test_ids),
        "test_run_ids_sha256": id_set_digest(test_ids),
        "evaluated_event_ids_sha256": id_set_digest(
            [str(value) for value in events["fault_id"]]),
        "healthy_test_run_count": len(healthy_test_ids),
        "healthy_test_run_ids_sha256": id_set_digest(healthy_test_ids),
        "healthy_test_exposure_ns": healthy_exposure_ns,
    }


def build_platform_audit(
    capability: Dict[str, Any],
    config: Dict[str, Any],
    source_workload: Path,
    corrected_workload: Path,
    smoke_log: Optional[Path],
    provenance: Dict[str, Any],
    smoke_inputs: Optional[List[Path]] = None,
    monitoring_validation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    expected = int(config["platform_contract"]["world_size"])
    source = parse_workload_header(source_workload)
    corrected = parse_workload_header(corrected_workload)
    source_tp = source["model_parallel_npu_group"]
    source_groups = (expected // source_tp
                     if source_tp and expected % source_tp == 0 else None)
    topology = capability["topology"]
    smoke = parse_smoke_log(smoke_log, smoke_inputs)
    contract = config["platform_contract"]
    host_count_ok = int(topology.get("host_count", -1)) == int(contract["expected_host_count"])
    access_count_ok = int(topology.get("access_link_count", -1)) == int(
        contract["expected_access_link_count"])
    locked_split_ok = (not bool(contract.get("requires_locked_test_split", False))
                       or provenance["all_selected_runs_locked_test_split"])
    interval_ok = provenance["telemetry_interval_matches_contract"]
    hard_semantics = capability.get("hard_failure_semantics", "UNSPECIFIED")
    hard_disconnect = bool(
        capability.get("permanent_hard_failure_benchmark", False)
        and hard_semantics == "physical_permanent_link_down")
    online_alarm = bool(capability.get("online_alarm_execution", False))
    delivery_timestamp = bool(capability.get("controller_delivery_timestamp", False))
    real_rto = bool(capability.get("configurable_real_rdma_rto", False))
    backup_qp = bool(capability.get("preestablished_backup_qp", False))
    runtime_reroute = bool(capability.get("runtime_reroute", False))
    collective_guard = bool(capability.get("collective_abort_commit_redo", False))
    numerical = bool(capability.get("tensor_numerical_correctness", False))
    surviving_port = topology.get("min_direct_access_degree", 0) >= 2
    platform_contract_checks = {
        "fault_trace_is_single_16rank_collective": bool(
            source["model_parallel_npu_group"] == expected
            and source["declared_all_gpus"] == expected),
        "corrected_world_size_workload_declared": bool(
            corrected["model_parallel_npu_group"] == expected
            and corrected["declared_all_gpus"] == expected),
        "host_count": host_count_ok,
        "access_link_count": access_count_ok,
        "locked_test_split": locked_split_ok,
        "manifest_schedule_match": provenance["event_schedule_matches_manifest"],
        "feature_dataset_run_coverage": provenance["all_selected_runs_in_feature_dataset"],
        "dataset_manifest_split_match": provenance[
            "feature_dataset_run_set_and_split_match_manifest"],
        "dataset_snapshot_access_coverage": (
            provenance["feature_dataset_incomplete_snapshot_count"] == 0
            and provenance["feature_dataset_access_set_matches_manifest"]),
        "dataset_unique_keys": provenance["feature_dataset_duplicate_key_count"] == 0,
        "manifest_sidecar_hashes": provenance["all_manifest_sidecar_hashes_match"],
        "telemetry_interval": interval_ok,
    }
    ledger_contract_fields = [field for field in platform_contract_checks
                              if field != "fault_trace_is_single_16rank_collective"]
    strict_ready = bool(
        all(platform_contract_checks.values())
        and smoke.get("validated_healthy_smoke", False)
        and surviving_port and online_alarm and delivery_timestamp
        and hard_disconnect and real_rto and backup_qp and runtime_reroute
        and collective_guard and numerical)
    audit = {
        "schema_version": 1,
        "expected_world_size": expected,
        "trace_source_workload": source,
        "trace_source_is_single_16rank_collective": bool(
            source["model_parallel_npu_group"] == expected
            and source["declared_all_gpus"] == expected),
        "trace_source_communicator_count": source_groups,
        "trace_source_interpretation": (
            "single 16-rank collective" if source_groups == 1
            else f"{source_groups} collectives of {source_tp} ranks each"
        ),
        "corrected_workload": corrected,
        "corrected_workload_declares_single_16rank_collective": bool(
            corrected["model_parallel_npu_group"] == expected
            and corrected["declared_all_gpus"] == expected),
        "corrected_smoke": smoke,
        "provenance": provenance,
        "platform_contract_checks": platform_contract_checks,
        "platform_contract_validated": all(platform_contract_checks.values()),
        "ledger_input_contract_validated": all(
            platform_contract_checks[field] for field in ledger_contract_fields),
        "monitoring_validation": monitoring_validation or {
            "provided": False, "validated": False},
        "topology": topology,
        "single_homed_access": topology.get("min_direct_access_degree") == 1,
        "surviving_access_port_exists": topology.get("min_direct_access_degree", 0) >= 2,
        "online_alarm_bus": online_alarm,
        "controller_delivery_timestamp": delivery_timestamp,
        "real_rdma_rto_retry_wc": real_rto,
        "preestablished_backup_qp": backup_qp,
        "runtime_reroute": runtime_reroute,
        "collective_abort_commit_redo": collective_guard,
        "tensor_numerical_correctness": numerical,
        "hard_failure_is_physical_disconnect": hard_disconnect,
        "hard_failure_semantics": hard_semantics,
        "hard_failure_note": (
            "A permanent physical disconnect is capability-verified."
            if hard_disconnect else
            "The locked hard benchmark latches link-down telemetry and degrades "
            "the data rate; it is not a permanent physical disconnect."),
        "collective_telemetry_semantics": capability.get(
            "collective_telemetry_semantics", "UNSPECIFIED"),
        "collective_telemetry_is_flow_completion": (
            capability.get("collective_telemetry_semantics")
            == "flow_sender_completion"),
        "strict_end_to_end_platform_ready": strict_ready,
    }
    return audit


def build_recovery_timeline(
    events: pd.DataFrame,
    platform: Dict[str, Any],
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Audit current-platform and oracle/component-isolation recovery effects."""

    models = config["models"]
    deadline = int(config["slo"]["recovery_from_detection_strict_lt_ns"])
    rows: List[Dict[str, Any]] = []
    roles = {
        "FANcY": "in-switch detection and IP reroute case study",
        "Trumpet": "host trigger framework",
        "NetBouncer": "active-probe localization",
        "MP-RDMA": "multipath RDMA transport",
        "Flor": "reliable transport with backup QPs",
        "SHIFT": "backup-RNIC QP fallback",
        "OptCC": "post-failover collective scheduling",
        "ReCoVer": "rank-failure collective/training recovery",
    }

    for _, event in events.iterrows():
        for method in RELATED_WORK:
            if method in {"Trumpet", "NetBouncer", "OptCC"}:
                status = NOT_APPLICABLE
                reason = "the native mechanism does not perform ACCESS-port connectivity cutover"
            elif method == "FANcY":
                status = BLOCKED_PRECONDITION
                reason = "IP reroute is not a pre-established backup RDMA QP and no second ACCESS port exists"
            elif method == "MP-RDMA":
                status = BLOCKED_PRECONDITION
                reason = "all virtual paths share the failed local ACCESS port in the current topology"
            elif method in {"Flor", "SHIFT"}:
                status = BLOCKED_PRECONDITION
                reason = "current topology has one ACCESS port and no pre-established backup QP"
            else:
                status = UNVERIFIED
                reason = "current SimAI frontend has no communicator repair, bucket rewind, or progress event"
            rows.append({
                "run_id": event["run_id"],
                "fault_id": event["fault_id"],
                "fault_kind": event["fault_kind"],
                "fault_group": event["fault_group"],
                "method": method,
                "native_role": roles[method],
                "evaluation_mode": "current_platform",
                "trigger_contract": "not_executed",
                "recovery_latency_ns": None,
                "recovery_deadline_ns": deadline,
                "timing_status": status,
                "strict_platform_status": FAIL if status == BLOCKED_PRECONDITION else status,
                "backup_path_assumed": False,
                "backup_qp_assumed": False,
                "surviving_port_progress_verified": False,
                "fidelity": "platform_capability_audit",
                "reason": reason,
            })

        # Component isolation gives Flor and SHIFT an oracle error WC and the
        # preconditions their papers require.  It isolates their post-WC effect
        # but is deliberately never a platform PASS.
        projections = [
            ("Flor", int(models["flor"]["backup_qp_switch_ns"]), models["flor"]["fidelity"]),
            ("SHIFT", int(models["shift"]["fallback_mean_ns"]), models["shift"]["fidelity"]),
        ]
        for method, latency, fidelity in projections:
            rows.append({
                "run_id": event["run_id"],
                "fault_id": event["fault_id"],
                "fault_kind": event["fault_kind"],
                "fault_group": event["fault_group"],
                "method": method,
                "native_role": roles[method],
                "evaluation_mode": "component_isolation_oracle_error_wc",
                "trigger_contract": "oracle_error_wc_at_time_zero",
                "recovery_latency_ns": latency,
                "recovery_deadline_ns": deadline,
                "timing_status": PASS if latency < deadline else FAIL,
                "strict_platform_status": UNVERIFIED,
                "backup_path_assumed": True,
                "backup_qp_assumed": True,
                "surviving_port_progress_verified": False,
                "fidelity": fidelity,
                "reason": (
                    "paper-calibrated post-error-WC interval; failure-to-WC and "
                    "first useful surviving-port ACK are not simulated"),
            })

        rows.append({
            "run_id": event["run_id"],
            "fault_id": event["fault_id"],
            "fault_kind": event["fault_kind"],
            "fault_group": event["fault_group"],
            "method": "MP-RDMA",
            "native_role": roles["MP-RDMA"],
            "evaluation_mode": "component_isolation_multipath_assumed",
            "trigger_contract": "oracle_path_failure",
            "recovery_latency_ns": None,
            "recovery_deadline_ns": deadline,
            "timing_status": UNVERIFIED,
            "strict_platform_status": UNVERIFIED,
            "backup_path_assumed": True,
            "backup_qp_assumed": False,
            "surviving_port_progress_verified": False,
            "fidelity": models["mp_rdma"]["fidelity"],
            "reason": (
                "the published <1 s number is restored-path re-utilization, not "
                "failed-ACCESS-to-surviving-port recovery"),
        })
    return pd.DataFrame(rows)


def build_optcc_projection(events: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    model = config["models"]["optcc"]
    rows: List[Dict[str, Any]] = []
    selected = events[events["fault_kind"] == "gray_fail_slow"]
    for _, event in selected.iterrows():
        severity = float(event["severity"])
        link_remaining = 1.0 - severity
        gpu_count = int(model["gpu_count"])
        gpus_per_server = int(model["gpus_per_server"])
        # One degraded GPU/NIC is pooled with the other healthy GPU/NICs on
        # its server, as required by the OptCC multi-GPU/PXN abstraction.
        server_remaining = (gpus_per_server - 1.0 + link_remaining) / gpus_per_server
        server_slowdown = 1.0 / server_remaining
        theorem_raw = optcc_theorem13_normalized_ratio(
            server_slowdown, gpu_count=gpu_count,
            gpus_per_server=gpus_per_server)
        network_with_healthy_floor = max(1.0, theorem_raw)
        total_projection = (float(model["fixed_fraction"])
                            + float(model["bandwidth_fraction"])
                            * network_with_healthy_floor)
        rows.append({
            "run_id": event["run_id"],
            "fault_id": event["fault_id"],
            "target_link_id": event["target_link_id"],
            "access_link_remaining_capacity_fraction": link_remaining,
            "server_pooled_remaining_capacity_fraction": server_remaining,
            "server_slowdown_factor": server_slowdown,
            "gpu_count": gpu_count,
            "gpus_per_server": gpus_per_server,
            "server_count": gpu_count // gpus_per_server,
            "theorem13_normalized_ratio_raw": theorem_raw,
            "theorem13_network_ratio_with_healthy_floor": network_with_healthy_floor,
            "projected_total_runtime_ratio": total_projection,
            "paper_reported_runtime_ratio_low": float(model["reported_runtime_ratio_low"]),
            "paper_reported_runtime_ratio_high": float(model["reported_runtime_ratio_high"]),
            "inside_paper_evaluated_capacity_region": (
                link_remaining >= float(model["validated_min_remaining_capacity"])),
            "actual_optcc_schedule_executed": False,
            "dynamic_inflight_fault_supported": False,
            "fidelity": model["fidelity"],
        })
    return pd.DataFrame(rows)


def build_healthy_alarm_matrix(path: Path) -> pd.DataFrame:
    """Carry healthy-run evidence without attributing proxy behavior to papers."""

    source = pd.read_csv(path)
    indexed = {str(row["detector"]): row for _, row in source.iterrows()}
    rows: List[Dict[str, Any]] = []
    for method, detector, fidelity in [
        ("LIMER-switch-sparse", "switch_sparse", "offline_causal_trace_replay"),
        ("LIMER-QG-HMM", "qghmm_quantized", "offline_causal_trace_replay"),
        ("Trumpet-RDMA-predicate-proxy", "host_telemetry", "host_predicate_proxy_only"),
    ]:
        raw = indexed.get(detector)
        rows.append({
            "method": method,
            "healthy_runs": int(raw["healthy_runs"]) if raw is not None else None,
            "candidate_alarm_count": int(raw["healthy_alarm_count"]) if raw is not None else None,
            "candidate_alarms_per_healthy_run": (
                float(raw["alarms_per_healthy_run"]) if raw is not None else None),
            "paper_mechanism_false_positive_status": (
                NOT_APPLICABLE if method.startswith("LIMER-") else UNVERIFIED),
            "fidelity": fidelity,
            "reason": (
                "measured on locked healthy traces"
                if method.startswith("LIMER-") else
                "only our host predicate was replayed; Trumpet's full trigger runtime was not"),
        })
    for method in ["FANcY", "NetBouncer"]:
        rows.append({
            "method": method,
            "healthy_runs": None,
            "candidate_alarm_count": None,
            "candidate_alarms_per_healthy_run": None,
            "paper_mechanism_false_positive_status": UNVERIFIED,
            "fidelity": "not_executed",
            "reason": (
                "no FANcY session/hash-tree implementation on healthy traces"
                if method == "FANcY" else
                "no active probes or NetBouncer solver on healthy traces"),
        })
    return pd.DataFrame(rows)


def build_resource_matrix(config: Dict[str, Any]) -> pd.DataFrame:
    models = config["models"]
    return pd.DataFrame([
        {
            "method": "LIMER-switch-sparse", "state_location": "switch port",
            "reported_state": "64 B/port", "fits_tens_of_bytes_per_port": True,
            "comparison_status": "native logical budget in this repository",
        },
        {
            "method": "FANcY", "state_location": "programmable switch",
            "reported_state": "6.65% Tofino SRAM for full FANcY (8.1% with rerouting)",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "not convertible to a tens-of-bytes port budget",
        },
        {
            "method": "Trumpet", "state_location": "host software",
            "reported_state": "per-flow statistics and trigger repository",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "different resource domain",
        },
        {
            "method": "NetBouncer", "state_location": "probe hosts/controller",
            "reported_state": "path probes and centralized solver state",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "different resource domain",
        },
        {
            "method": "MP-RDMA", "state_location": "RDMA connection",
            "reported_state": f"{models['mp_rdma']['extra_connection_state_bytes']} B/connection extra",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "per-connection, not per-port",
        },
        {
            "method": "Flor", "state_location": "host/RNIC connection",
            "reported_state": "two pre-connected backup QPs per connection",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "per-connection, not per-port",
        },
        {
            "method": "SHIFT", "state_location": "host/RNIC connection",
            "reported_state": (
                f"about {models['shift']['backup_qp_bytes'] + models['shift']['backup_cq_bytes']} "
                "B per backup QP+CQ under the paper's queue sizes"),
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "per-QP host memory, not switch SRAM",
        },
        {
            "method": "OptCC", "state_location": "collective scheduler",
            "reported_state": "per-GPU schedule/dependency state",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "different resource domain",
        },
        {
            "method": "ReCoVer", "state_location": "training runtime",
            "reported_state": "bucket snapshots, world epochs, communicator state",
            "fits_tens_of_bytes_per_port": None,
            "comparison_status": "different resource domain",
        },
    ])


def _summary_lookup(summary: pd.DataFrame, method: str, scope: str) -> Optional[pd.Series]:
    rows = summary[(summary["method"] == method) & (summary["scope"] == scope)]
    return rows.iloc[0] if len(rows) else None


def build_method_verdicts(
    detection_summary: pd.DataFrame,
    recovery: pd.DataFrame,
    platform: Dict[str, Any],
    safety: pd.DataFrame,
) -> pd.DataFrame:
    detector_names = {
        "FANcY": "FANcY-dedicated-cadence",
        "Trumpet": "Trumpet-10ms-trigger",
        "NetBouncer": "NetBouncer-cadence-lower-bound",
    }
    roles = {
        "FANcY": "gray-loss detection; selective IP reroute case study",
        "Trumpet": "host event trigger framework",
        "NetBouncer": "active probing and link localization",
        "MP-RDMA": "multipath RDMA transport",
        "Flor": "loss-tolerant RDMA and backup-QP switch",
        "SHIFT": "backup-RNIC fallback after error WC",
        "OptCC": "AllReduce scheduling after failover",
        "ReCoVer": "rank-failure collective/training recovery",
    }
    collective = {
        "FANcY": UNVERIFIED, "Trumpet": UNVERIFIED, "NetBouncer": UNVERIFIED,
        "MP-RDMA": UNVERIFIED, "Flor": UNVERIFIED, "SHIFT": UNVERIFIED,
        "OptCC": UNVERIFIED, "ReCoVer": UNVERIFIED,
    }
    rows: List[Dict[str, Any]] = []
    for method in RELATED_WORK:
        detector = detector_names.get(method)
        hard = NOT_APPLICABLE
        gray = NOT_APPLICABLE
        detection_basis = "native work does not provide this detector contract"
        if detector:
            hard_row = _summary_lookup(detection_summary, detector, "HARD")
            gray_row = _summary_lookup(detection_summary, detector, "GRAY")
            if hard_row is not None:
                full_supported = int(hard_row["supported_events"]) == int(
                    hard_row["scheduled_events"])
                hard = (PASS if full_supported
                        and hard_row["scheduled_strict_platform_pass_rate"] == 1.0
                        else FAIL if not full_supported
                        or hard_row["scheduled_actionable_timing_pass_rate"] < 1.0
                        else UNVERIFIED)
            if gray_row is not None:
                full_supported = int(gray_row["supported_events"]) == int(gray_row["scheduled_events"])
                gray = (PASS if gray_row["scheduled_strict_platform_pass_rate"] == 1.0
                        and full_supported
                        else FAIL if not full_supported
                        or gray_row["scheduled_actionable_timing_pass_rate"] < 1.0
                        else UNVERIFIED)
            detection_basis = "same trace signal plus explicit paper/default timing model"

        if method in {"FANcY", "MP-RDMA", "Flor", "SHIFT"}:
            native_recovery = recovery[
                (recovery["method"] == method)
                & (recovery["evaluation_mode"] == "current_platform")]
            statuses = set(native_recovery["strict_platform_status"])
            recovery_status = (PASS if statuses == {PASS}
                               else FAIL if FAIL in statuses else UNVERIFIED)
        elif method == "ReCoVer":
            recovery_status = UNVERIFIED
        else:
            recovery_status = NOT_APPLICABLE

        collective_status = collective[method]
        component_effect = "none validated"
        if method == "Flor":
            component_effect = "60 us backup-QP switch projection after oracle error WC: timing PASS, not platform verified"
        elif method == "SHIFT":
            component_effect = "2.30 ms fallback projection after oracle error WC: timing PASS; LL128 caveat remains"
        elif method == "MP-RDMA":
            component_effect = "66 B/connection state audited; ACCESS failover timing not established"
        elif method == "OptCC":
            component_effect = "Theorem-13 p=16/g=4 projection generated after per-link to pooled-server mapping; no OptCC flow schedule executed"
        elif method == "ReCoVer":
            safe = bool((safety["correctness_status"] == PASS).all())
            component_effect = (
                "epoch abort/redo invariant model passes all cases" if safe
                else "epoch abort/redo invariant model failed")
        elif method == "FANcY":
            component_effect = "dedicated-counter cadence projected only from raw packet-count mismatch timestamps"
        elif method == "Trumpet":
            component_effect = "10 ms trigger epochs projected from actual host anomaly timestamps"
        elif method == "NetBouncer":
            component_effect = "5 minute probe plus processor cadence lower bound; no probe or solver output"

        requirements = [hard, gray, recovery_status, collective_status]
        overall = PASS if all(status == PASS for status in requirements) else FAIL
        rows.append({
            "method": method,
            "native_role": roles[method],
            "hard_detection_lt_1ms": hard,
            "gray_detection_lt_100ms": gray,
            "surviving_port_progress_lt_1s": recovery_status,
            "collective_complete_or_safe_redo": collective_status,
            "meets_all_project_requirements": overall,
            "component_effect": component_effect,
            "detection_basis": detection_basis,
            "platform_end_to_end_executed": bool(
                platform["strict_end_to_end_platform_ready"] and overall == PASS),
        })
    return pd.DataFrame(rows)


def consistency_checks(
    events: pd.DataFrame,
    detection: pd.DataFrame,
    recovery: pd.DataFrame,
    safety: pd.DataFrame,
    optcc: pd.DataFrame,
    verdicts: pd.DataFrame,
    platform: Dict[str, Any],
) -> Dict[str, Any]:
    recomputed = detection["actionable_detection_latency_ns"].notna() & (
        detection["actionable_detection_latency_ns"] < detection["detection_deadline_ns"])
    recorded = detection["actionable_timing_status"] == PASS
    model_methods = {
        "FANcY-dedicated-cadence", "Trumpet-10ms-trigger",
        "NetBouncer-cadence-lower-bound",
        "RDMA-error-reference", "NCCL-error-reference",
    }
    expected_overall = verdicts.apply(
        lambda row: PASS if all(row[field] == PASS for field in [
            "hard_detection_lt_1ms", "gray_detection_lt_100ms",
            "surviving_port_progress_lt_1s",
            "collective_complete_or_safe_redo",
        ]) else FAIL,
        axis=1,
    )
    strict_supported = detection[detection["supported"]]
    checks = {
        "eight_requested_works_present": {
            "pass": set(verdicts["method"]) == set(RELATED_WORK),
            "detail": sorted(verdicts["method"].tolist()),
        },
        "event_ledger_unique": {
            "pass": bool(len(events) and not events["fault_id"].duplicated().any()),
            "detail": f"events={len(events)}",
        },
        "strict_detection_deadline": {
            "pass": bool((recomputed == recorded).all()),
            "detail": "all timing PASS values use latency < deadline",
        },
        "strict_rates_use_scheduled_population": {
            "pass": bool(not strict_supported.empty and not (
                (strict_supported["strict_platform_status"] == PASS)
                & ~strict_supported["delivery_platform_verified"]).any()),
            "detail": "unobservable or unsupported scheduled faults cannot disappear from strict verdicts",
        },
        "paper_models_not_marked_executed": {
            "pass": not bool(detection[detection["method"].isin(model_methods)]["platform_executed"].any()),
            "detail": "calibrated/projected methods remain non-native",
        },
        "no_fabricated_alarm_delivery": {
            "pass": not bool(detection["delivery_platform_verified"].any()),
            "detail": "current platform has no AlarmBus delivery timestamp",
        },
        "recovery_current_platform_never_passes": {
            "pass": not bool((recovery[recovery["evaluation_mode"] == "current_platform"]
                              ["strict_platform_status"] == PASS).any()),
            "detail": "single-homed topology and missing backup QPs gate recovery",
        },
        "protocol_model_exhaustive_for_three_phases": {
            "pass": len(safety) == len(events) * 3,
            "detail": f"cases={len(safety)}, expected={len(events) * 3}",
        },
        "protocol_model_never_publishes_wrong_result": {
            "pass": bool((safety["correctness_status"] == PASS).all()
                         and not safety["wrong_result_published"].any()),
            "detail": "integer reference digest and exactly-once contribution checks",
        },
        "protocol_model_does_not_claim_backup_path": {
            "pass": "failed_rank_replayed_over_backup" not in safety.columns,
            "detail": "the transport-agnostic guard records contribution replay only",
        },
        "protocol_model_world_size_16": {
            "pass": set(safety["world_size"]) == {16},
            "detail": sorted(set(int(value) for value in safety["world_size"])),
        },
        "optcc_projection_is_not_execution": {
            "pass": bool(len(optcc) and not optcc["actual_optcc_schedule_executed"].any()),
            "detail": f"projections={len(optcc)}",
        },
        "optcc_per_link_capacity_is_mapped_to_server": {
            "pass": bool(len(optcc) and np.allclose(
                optcc["server_pooled_remaining_capacity_fraction"],
                (optcc["gpus_per_server"] - 1
                 + optcc["access_link_remaining_capacity_fraction"])
                / optcc["gpus_per_server"])),
            "detail": "one degraded GPU/NIC is pooled with g-1 healthy GPU/NICs",
        },
        "method_overall_verdict_is_derived": {
            "pass": bool((verdicts["meets_all_project_requirements"].reset_index(drop=True)
                          == expected_overall.reset_index(drop=True)).all()),
            "detail": "overall PASS iff all four requirement columns PASS",
        },
        "platform_limitation_is_explicit": {
            "pass": (not platform["trace_source_is_single_16rank_collective"]
                     and platform["single_homed_access"]
                     and not platform["hard_failure_is_physical_disconnect"]),
            "detail": "locked data are 2x8, single-homed, and hard-failure proxy",
        },
        "platform_contract_result_is_derived": {
            "pass": bool(
                platform["platform_contract_validated"]
                == all(platform["platform_contract_checks"].values())),
            "detail": platform["platform_contract_checks"],
        },
        "provenance_locked_to_manifest": {
            "pass": bool(
                platform["provenance"]["all_selected_runs_locked_test_split"]
                and platform["provenance"]["event_schedule_matches_manifest"]
                and platform["provenance"]["all_manifest_sidecar_hashes_match"]
                and platform["provenance"][
                    "feature_dataset_run_set_and_split_match_manifest"]),
            "detail": "fault ids/schedule/split, dataset run map, and 600 sidecar hashes checked",
        },
        "discrete_full_link_monitoring_evidence_present": {
            "pass": bool(platform["monitoring_validation"]["validated"]),
            "detail": platform["monitoring_validation"].get(
                "physical_link_coverage_detail"),
        },
    }
    return {
        "checks": checks,
        "all_integrity_checks_pass": all(item["pass"] for item in checks.values()),
    }


def fmt_ms(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value) / 1e6:.3f} ms"


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{100 * float(value):.2f}%"


def write_report(
    path: Path,
    events: pd.DataFrame,
    detection_summary: pd.DataFrame,
    recovery: pd.DataFrame,
    safety: pd.DataFrame,
    optcc: pd.DataFrame,
    verdicts: pd.DataFrame,
    resources: pd.DataFrame,
    healthy: pd.DataFrame,
    platform: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    with path.open("w") as out:
        out.write("# 八项相关工作统一 16-GPU 机制级评测（非原系统源码移植）\n\n")
        out.write("## 结论\n\n")
        passing = verdicts[verdicts["meets_all_project_requirements"] == PASS]["method"].tolist()
        if platform["strict_end_to_end_platform_ready"] and passing:
            out.write(f"**严格端到端要求已由 {', '.join(passing)} 满足。** ")
        else:
            out.write("**当前平台不能证明满足端到端要求；当前证据中没有一项工作单独覆盖四项要求。** ")
        out.write("可复现的检测信号、论文参数投影和协议不变量在结果中分开保存；")
        out.write("任何参数投影都没有被计作平台 PASS。\n\n")
        out.write("最先暴露出的平台问题不是算法精度，而是实验契约：锁定的故障数据使用")
        out.write(f" `{platform['trace_source_interpretation']}`，不是单个 16-rank AllReduce；")
        out.write("ACCESS 单归属、hard fault 不是物理断链、没有在线 AlarmBus、真实 RDMA error WC、")
        out.write("备用 QP 或 collective commit/redo。\n\n")
        smoke = platform["corrected_smoke"]
        if smoke.get("validated_healthy_smoke", False):
            out.write("尤其要注意：本次唯一真正执行且输入哈希匹配的单一 16-rank 运行是约 ")
            out.write(f"{fmt_ms(smoke.get('completion_time_ns'))} 的健康 smoke；")
        else:
            out.write("尤其要注意：本次没有接纳输入哈希匹配的单一 16-rank 运行；")
        out.write("82 个 fault/detection 事件仍来自锁定的 2×8-rank 数据。因而这里不能表述为‘八套原系统均已在")
        out.write("16-rank 故障 AllReduce 上复现’。\n\n")

        out.write("## 平台审计\n\n")
        out.write("| 项目 | 结果 |\n|---|---|\n")
        out.write(f"| 锁定轨迹 communicator | {platform['trace_source_interpretation']} |\n")
        out.write(f"| 修正 workload 声明 TP=16/DP=1 | {platform['corrected_workload_declares_single_16rank_collective']} |\n")
        out.write(f"| 修正 smoke 构造 16-rank ring | {smoke.get('true_16rank_ring_observed', False)} |\n")
        out.write(f"| TP=16 单层 smoke 进程完成 | {smoke.get('simulation_completed', False)} |\n")
        out.write(f"| smoke 输入哈希验证 | {smoke.get('provenance_matches_current_inputs', False)} |\n")
        out.write(f"| 16 ranks 均有 sender-flow 完成记录 | {smoke.get('all_16_ranks_have_completed_flows', False)} |\n")
        out.write(f"| smoke collective commit 被观察 | {smoke.get('collective_commit_observed', False)} |\n")
        out.write(f"| ACCESS surviving port | {platform['surviving_access_port_exists']} |\n")
        out.write(f"| hard fault 为真实物理断链 | {platform['hard_failure_is_physical_disconnect']} |\n")
        out.write(f"| 在线告警/送达时间 | {platform['online_alarm_bus']}/{platform['controller_delivery_timestamp']} |\n")
        out.write(f"| 真实 RTO/retry/error WC | {platform['real_rdma_rto_retry_wc']} |\n")
        out.write(f"| 预建 backup QP | {platform['preestablished_backup_qp']} |\n")
        out.write(f"| collective abort/commit/redo | {platform['collective_abort_commit_redo']} |\n")
        out.write(f"| tensor 数值正确性 | {platform['tensor_numerical_correctness']} |\n")
        monitoring = platform["monitoring_validation"]
        out.write(f"| 既有 2×8 健康长跑离散快照覆盖 | {monitoring.get('physical_link_coverage_detail', 'UNVERIFIED')} |\n")
        out.write(f"| true-16 fault-run 全链路覆盖 | {monitoring.get('true16_fault_run_coverage_verified', False)} |\n")
        out.write(f"| 监控验证产物哈希/检查有效 | {monitoring.get('validated', False)} |\n\n")
        out.write(f"平台契约：`{platform['platform_contract_checks']}`。其中 ledger 输入契约为 ")
        out.write(f"`{platform['ledger_input_contract_validated']}`，完整 16-rank fault 平台契约为 ")
        out.write(f"`{platform['platform_contract_validated']}`。\n\n")
        out.write("这里的 288/288 表示配置周期上的离散快照覆盖，不表示连续时间中每一个瞬间都有快照；")
        out.write("亚毫秒事件只能由事件锁存字段保留，且目前仍没有在线告警送达时间。\n\n")
        if smoke.get("periodic_rows_note"):
            out.write(f"> Smoke 说明：{smoke['periodic_rows_note']} 物理链路周期遥测覆盖仍引用既有长时 16-GPU 验证，")
            out.write("不能由这个短 smoke 单独证明。\n\n")

        out.write("## 同一故障账本上的检测时序\n\n")
        out.write(f"共 {len(events)} 个锁定测试故障，其中 {int(events['event_observable'].sum())} 个在当前流量中产生可观测信号。")
        out.write("`Signal` 是本地观察/模型时间，`Actionable` 是恢复消费者可用时间；当前平台没有真实后者。\n\n")
        out.write("| 方法 | 类型 | 已排程 | 支持 | 已排程 Signal pass | 已排程 Actionable pass | 严格平台 pass | 条件可观测 pass | Median actionable |\n")
        out.write("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in detection_summary.itertuples(index=False):
            out.write(
                f"| {row.method} | {row.scope} | {row.scheduled_events} | "
                f"{row.supported_events} | {fmt_pct(row.scheduled_signal_timing_pass_rate)} | "
                f"{fmt_pct(row.scheduled_actionable_timing_pass_rate)} | "
                f"{fmt_pct(row.scheduled_strict_platform_pass_rate)} | "
                f"{fmt_pct(row.observable_timing_pass_rate)} | "
                f"{fmt_ms(row.latency_median_ns)} |\n"
            )
        out.write("\n严格比例以全部已排程故障为分母；‘条件可观测’列只作诊断，不参与严格 PASS。")
        out.write("FANcY 只使用目标链路真实 `drop_error_delta`，不再借用 LIMER 的队列、吞吐或 link latch；")
        out.write("50 ms + 论文平均残差仍只是 cadence 投影。Trumpet 使用我们定义的 RDMA/端口谓词。")
        out.write("NetBouncer 没有 probe trace 或 solver，表中 5 分钟 + 37.3 s 仅为反事实最早结果下界，")
        out.write("定位保持 `UNVERIFIED`，不计作真实 alarm。\n\n")
        out.write("当前 schedule 中所有故障都在 10 s/30 s 参考 timer 到期前自动清除，")
        out.write("所以 RDMA/NCCL reference 不产生 error；若换成持续故障，这两个配置参考值仍分别是 10 s 和 30 s，")
        out.write("远超目标。\n\n")

        out.write("## 健康运行告警负担\n\n")
        out.write("| 方法 | 健康 runs | candidate alarms | alarms/run | 论文机制 FP 状态 |\n")
        out.write("|---|---:|---:|---:|---|\n")
        for row in healthy.itertuples(index=False):
            runs = "N/A" if pd.isna(row.healthy_runs) else str(int(row.healthy_runs))
            count = "N/A" if pd.isna(row.candidate_alarm_count) else str(int(row.candidate_alarm_count))
            rate = ("N/A" if pd.isna(row.candidate_alarms_per_healthy_run)
                    else f"{float(row.candidate_alarms_per_healthy_run):.3f}")
            out.write(f"| {row.method} | {runs} | {count} | {rate} | "
                      f"{row.paper_mechanism_false_positive_status} |\n")
        out.write("\n论文机制未执行时不从 LIMER/host proxy 的健康结果推断其误报率；")
        out.write("因此论文适配器的当前时延结果只代表 fault-conditioned timing，不代表完整检测准确率。\n\n")
        out.write(f"锁定的 8 个健康 run 合计只有 {platform['provenance']['healthy_test_exposure_ns'] / 1e6:.3f} ms 暴露，")
        out.write("即便出现 0 次 candidate alarm，也不能外推为生产环境零误报。\n\n")

        out.write("## 恢复组件隔离结果\n\n")
        isolated = recovery[recovery["evaluation_mode"] == "component_isolation_oracle_error_wc"]
        for method in ["Flor", "SHIFT"]:
            rows = isolated[isolated["method"] == method]
            latency = rows.iloc[0]["recovery_latency_ns"] if len(rows) else None
            out.write(f"- {method}: 从 oracle error WC 开始的论文参数投影为 {fmt_ms(latency)}，")
            out.write("低于 1 s；但 failure→WC、真实备用端口首个 ACK 和训练进展都未仿真，所以严格状态为 `UNVERIFIED`。\n")
        out.write("- MP-RDMA: 论文的 `<1 s` 指恢复后的路径重新达到充分利用，并非本地 ACCESS 断开后的切换时间；当前所有 VP 仍共享故障 ACCESS 端口。\n")
        out.write("- OptCC: 假定 failover 已完成，只优化随后的不对称带宽 AllReduce，不是恢复器。\n")
        out.write("- ReCoVer: 面向 rank/process failure；当前端口故障下 rank 仍存活，原生 shrink-membership 语义不等价。\n\n")

        out.write("## Collective 安全性模型\n\n")
        out.write(f"对 {len(events)} 个故障逐一覆盖 before/mid/after-reduce-before-commit 三个阶段，共 {len(safety)} 个 16-rank 精确整数用例。")
        out.write(f"安全 redo 模型通过 {int((safety['correctness_status'] == PASS).sum())}/{len(safety)}，")
        out.write("每个新 epoch 恰好接收 16 个不同 rank contribution，旧 epoch 从不提交，最终摘要与无故障 reference 一致。\n\n")
        out.write("这证明的是 `ReCoVer-inspired transport-epoch guard` 的最小协议不变量，")
        out.write("不是当前 SimAI data plane 或原 ReCoVer MPI/ULFM 实现。它没有恢复时间结论。\n\n")

        out.write("## OptCC 16-GPU Theorem-13 分析投影\n\n")
        if len(optcc):
            grouped = optcc.groupby("access_link_remaining_capacity_fraction").agg(
                events=("fault_id", "count"),
                server_remaining=("server_pooled_remaining_capacity_fraction", "min"),
                theorem_raw=("theorem13_normalized_ratio_raw", "min"),
                total_projection=("projected_total_runtime_ratio", "min"))
            out.write("| ACCESS 剩余 | pooled server 剩余 | 事件 | theorem raw | 14/86 total projection | schedule |\n")
            out.write("|---:|---:|---:|---:|---:|---|\n")
            for remaining, row in grouped.iterrows():
                out.write(f"| {remaining:.2f} | {row['server_remaining']:.4f} | "
                          f"{int(row['events'])} | {row['theorem_raw']:.4f} | "
                          f"{row['total_projection']:.4f} | no |\n")
        out.write("\n单条 ACCESS 链路容量先按 `(g-1+r)/g` 映射为 PXN pooled server 容量，再代入")
        out.write("论文 Theorem 13；同时单独保留 raw theorem 值和健康性能 floor。")
        out.write("这不是 OptCC 四阶段 schedule 的 SimAI 运行时间，也不外推为实测恢复性能。\n\n")

        out.write("## 八项工作的严格判定\n\n")
        out.write("| 工作 | hard <1 ms | gray <100 ms | surviving port <1 s | collective 安全 | 全部满足 |\n")
        out.write("|---|---|---|---|---|---|\n")
        for row in verdicts.itertuples(index=False):
            out.write(f"| {row.method} | {row.hard_detection_lt_1ms} | "
                      f"{row.gray_detection_lt_100ms} | {row.surviving_port_progress_lt_1s} | "
                      f"{row.collective_complete_or_safe_redo} | {row.meets_all_project_requirements} |\n")
        out.write("\n`NOT_APPLICABLE` 和 `UNVERIFIED` 均不会折算成 PASS。")
        out.write("Flor/SHIFT 的快速数字从 error WC 起算，不能用来替代 hard/gray detection SLO。\n\n")
        out.write("SHIFT 的 collective 项为 `UNVERIFIED`：论文指出 LL128 风险，但本次运行没有记录实际 NCCL protocol，")
        out.write("所以既不能无条件判 FAIL，也不能判 PASS。\n\n")

        out.write("## 资源可比性\n\n")
        out.write("| 方法 | 状态位置 | 论文/实现状态量 | 是否证明符合几十字节/端口 |\n")
        out.write("|---|---|---|---|\n")
        for row in resources.itertuples(index=False):
            fits = "UNVERIFIED" if pd.isna(row.fits_tens_of_bytes_per_port) else str(bool(row.fits_tens_of_bytes_per_port))
            out.write(f"| {row.method} | {row.state_location} | {row.reported_state} | {fits} |\n")

        out.write("\n## 要达到项目要求仍必须实现的公共层\n\n")
        out.write("1. 真正永久断开的 ACCESS hard fault，以及覆盖所有相位的 `<1 ms` 在线 fast path。\n")
        out.write("2. 双 ACCESS/Dual-ToR 或 PXN surviving-port 路径，并明确 primary/backup port mapping。\n")
        out.write("3. sender RTO、retry exhaustion、error WC 与可调 timeout。\n")
        out.write("4. 模拟器内 AlarmBus，记录 observe/emit/queue/deliver/consume。\n")
        out.write("5. 预建 backup QP、primary quiesce、backup 首个 useful ACK 和训练 progress。\n")
        out.write("6. 真实 collective attempt/epoch、tensor checksum、abort/commit/replay 和 optimizer exactly-once。\n\n")
        out.write("可能满足全部要求的是组合设计：switch hard/gray detector + AlarmBus + ")
        out.write("dual-rail Flor/SHIFT-style failover + epoch-safe redo + 可选 OptCC；")
        out.write("这将是 LIMER 的新系统，不属于任何一篇论文的单独结论。\n\n")

        out.write("## 参数来源\n\n")
        for name, url in config["sources"].items():
            out.write(f"- [{name}]({url})\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--source-workload", required=True, type=Path)
    parser.add_argument("--corrected-workload", required=True, type=Path)
    parser.add_argument("--dataset-manifest", required=True, type=Path)
    parser.add_argument("--feature-dataset", required=True, type=Path)
    parser.add_argument("--healthy-alarm-summary", required=True, type=Path)
    parser.add_argument("--monitoring-validation", type=Path)
    parser.add_argument("--monitoring-run-dir", type=Path)
    parser.add_argument("--provenance-input", action="append", type=Path, default=[])
    parser.add_argument("--corrected-smoke-log", type=Path)
    parser.add_argument("--smoke-workload", type=Path)
    parser.add_argument("--smoke-topology", type=Path)
    parser.add_argument("--smoke-sim-config", type=Path)
    parser.add_argument("--simulator-bin", type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    config = load_json(args.config)
    events = pd.read_csv(args.baseline_dir / "fault_events_evaluated.csv")
    original_detection = pd.read_csv(args.baseline_dir / "detection_event_timeline.csv")
    capability = load_json(args.baseline_dir / "capability_matrix.json")
    feature_columns = [
        "run_id", "timestamp_ns", "link_id", "switch_id", "split",
        "drop_error_delta",
    ]
    feature_samples = pd.read_csv(args.feature_dataset, usecols=feature_columns)
    provenance_inputs = [
        args.baseline_dir / "fault_events_evaluated.csv",
        args.baseline_dir / "detection_event_timeline.csv",
        args.baseline_dir / "capability_matrix.json",
        args.feature_dataset,
        args.config,
        args.source_workload,
        args.corrected_workload,
        *args.provenance_input,
    ]
    provenance = build_provenance_audit(
        events, feature_samples, args.dataset_manifest, provenance_inputs, config)
    smoke_inputs = [path for path in [
        args.smoke_workload, args.smoke_topology,
        args.smoke_sim_config, args.simulator_bin,
    ] if path is not None]
    monitoring_validation = build_monitoring_validation_audit(
        args.monitoring_validation, args.monitoring_run_dir,
        config["platform_contract"])
    platform = build_platform_audit(
        capability, config, args.source_workload, args.corrected_workload,
        args.corrected_smoke_log, provenance, smoke_inputs,
        monitoring_validation,
    )

    detection = build_detection_timeline(
        events, original_detection, config, feature_samples)
    detection_summary = aggregate_detection(detection)
    recovery = build_recovery_timeline(events, platform, config)
    safety = collective_safety_cases(events, world_size=16)
    optcc = build_optcc_projection(events, config)
    resources = build_resource_matrix(config)
    healthy = build_healthy_alarm_matrix(args.healthy_alarm_summary)
    verdicts = build_method_verdicts(
        detection_summary, recovery, platform, safety)
    checks = consistency_checks(
        events, detection, recovery, safety, optcc, verdicts, platform)
    if not checks["all_integrity_checks_pass"]:
        failed = [name for name, item in checks["checks"].items() if not item["pass"]]
        raise SystemExit(f"related-work benchmark integrity checks failed: {failed}")

    passing_methods = sorted(verdicts.loc[
        verdicts["meets_all_project_requirements"] == PASS, "method"].tolist())
    current_platform_pass = bool(
        platform["strict_end_to_end_platform_ready"] and passing_methods)
    primary_blockers = []
    if not platform["trace_source_is_single_16rank_collective"]:
        primary_blockers.append(
            "locked fault traces are two 8-rank collectives rather than one 16-rank AllReduce")
    blocker_fields = [
        ("surviving_access_port_exists", "single-homed ACCESS topology has no surviving port"),
        ("hard_failure_is_physical_disconnect", "hard fault is not a permanent physical disconnect"),
        ("online_alarm_bus", "no simulator-time online alarm delivery"),
        ("real_rdma_rto_retry_wc", "no real RDMA timeout/retry/error WC"),
        ("preestablished_backup_qp", "no pre-established backup QP"),
        ("collective_abort_commit_redo", "no platform collective commit-abort-redo semantics"),
        ("tensor_numerical_correctness", "no platform tensor numerical correctness"),
    ]
    primary_blockers.extend(message for field, message in blocker_fields
                            if not platform[field])
    conclusion = {
        "schema_version": 1,
        "benchmark_id": config["benchmark_id"],
        "event_counts": {
            "scheduled": len(events),
            "observable": int(events["event_observable"].sum()),
            "hard": int((events["fault_group"] == "HARD").sum()),
            "gray": int((events["fault_group"] == "GRAY").sum()),
        },
        "requested_works": RELATED_WORK,
        "individual_methods_meeting_all_requirements": passing_methods,
        "current_platform_meets_all_requirements": current_platform_pass,
        "strict_conclusion": PASS if current_platform_pass else FAIL,
        "primary_blockers": primary_blockers,
        "validated_component_effects": {
            "trace_based_detection_signal": True,
            "paper_parameter_timing_projections": True,
            "optcc_16gpu_theorem13_projection": True,
            "epoch_safe_redo_protocol_cases": len(safety),
            "epoch_safe_redo_all_pass": bool((safety["correctness_status"] == PASS).all()),
            "actual_surviving_port_failover": False,
            "actual_tensor_correctness": False,
        },
        "evidence_policy": (
            "only an executed and timestamped platform event may produce a strict PASS; "
            "paper projections remain UNVERIFIED"),
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    detection.to_csv(args.out_dir / "detection_timeline.csv", index=False)
    detection_summary.to_csv(args.out_dir / "detection_summary.csv", index=False)
    recovery.to_csv(args.out_dir / "recovery_component_timeline.csv", index=False)
    safety.to_csv(args.out_dir / "collective_safety_cases.csv", index=False)
    optcc.to_csv(args.out_dir / "optcc_16gpu_projection.csv", index=False)
    resources.to_csv(args.out_dir / "resource_matrix.csv", index=False)
    healthy.to_csv(args.out_dir / "healthy_alarm_comparison.csv", index=False)
    verdicts.to_csv(args.out_dir / "method_verdicts.csv", index=False)
    write_json(args.out_dir / "platform_audit.json", platform)
    write_json(args.out_dir / "benchmark_checks.json", checks)
    write_json(args.out_dir / "conclusion.json", conclusion)
    write_report(
        args.out_dir / "related_work_report.md", events, detection_summary,
        recovery, safety, optcc, verdicts, resources, healthy, platform, config,
    )
    print(json.dumps({
        "events": len(events),
        "detection_rows": len(detection),
        "recovery_rows": len(recovery),
        "collective_safety_cases": len(safety),
        "strict_conclusion": conclusion["strict_conclusion"],
        "report": str((args.out_dir / "related_work_report.md").resolve()),
    }, indent=2))


if __name__ == "__main__":
    main()
