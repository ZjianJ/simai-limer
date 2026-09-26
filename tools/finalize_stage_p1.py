#!/usr/bin/env python3
"""Materialize and evaluate the final P1 true-16 stage gate.

This is an evidence aggregator, not a simulator runner.  It independently
rechecks the long healthy run and all 16 raw hard-alarm CSVs, verifies their
hashes against the matrix report, and emits the five artifacts required by
the frozen experiment contract.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

import pandas as pd

import audit_true16_monitoring


PASS = "PASS"
FAIL = "FAIL"
HARD_DETECTION_LIMIT_NS = 1_000_000
MINIMUM_HEALTHY_SAMPLES = 101
MINIMUM_HEALTHY_SPAN_NS = 100_000_000
SAMPLE_INTERVAL_NS = 1_000_000
SCHEMA_VERSION = "limer.stage-gate.p1.v1"
TIMELINE_FIELDS = [
    "gpu", "target_link_id", "host_port", "plane_role", "fault_id",
    "fault_type", "fault_start_ns", "physical_fault_ns", "observe_ns",
    "emit_ns", "deliver_ns", "consume_ns", "actionable_latency_ns",
    "deadline_ns", "strict_under_1ms", "detector", "alarm_status",
    "raw_alarm_path", "raw_alarm_sha256", "matrix_alarm_sha256",
    "raw_alarm_hash_match", "matrix_status", "status", "errors",
]


def _add(checks: List[Dict[str, str]], name: str, ok: bool, detail: str) -> None:
    checks.append({
        "check": name,
        "status": PASS if ok else FAIL,
        "detail": detail,
    })


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


def _artifact(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def _int(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if value in (None, ""):
        raise ValueError(f"missing integer field {field}")
    return int(value)


def _all_pass_no_skip(value: Mapping[str, Any]) -> bool:
    checks = value.get("checks", [])
    return (
        value.get("status") == PASS
        and int(value.get("summary", {}).get("fail", -1)) == 0
        and int(value.get("summary", {}).get("skip", 0)) == 0
        and bool(checks)
        and all(item.get("status") == PASS for item in checks)
    )


def build_topology_validation(
    source_path: Path,
    matrix: Mapping[str, Any],
) -> Dict[str, Any]:
    source = _load_json(source_path)
    checks: List[Dict[str, str]] = []
    topology_input = matrix.get("inputs", {}).get("topology", {})
    topology_path = Path(str(topology_input.get("path", "")))
    source_checks = source.get("checks", [])
    _add(
        checks,
        "source_topology_validation_all_pass",
        source.get("status") == PASS
        and bool(source_checks)
        and all(item.get("status") == PASS for item in source_checks),
        f"status={source.get('status')}, checks={len(source_checks)}, "
        f"non_pass={sum(item.get('status') != PASS for item in source_checks)}",
    )
    actual_topology_sha = _sha256(topology_path) if topology_path.is_file() else None
    _add(
        checks,
        "validated_topology_hash_matches_matrix_input",
        topology_path.is_file()
        and source.get("topology_sha256") == topology_input.get("sha256")
        and actual_topology_sha == topology_input.get("sha256"),
        f"validation={source.get('topology_sha256')}, "
        f"matrix={topology_input.get('sha256')}, actual={actual_topology_sha}",
    )
    topology = source.get("topology", {})
    _add(
        checks,
        "exact_true16_topology_counts",
        topology.get("node_count") == 92
        and topology.get("gpu_count") == 16
        and topology.get("gpus_per_server") == 4
        and topology.get("nvswitch_count") == 4
        and topology.get("regular_switch_count") == 72
        and topology.get("physical_link_count") == 304
        and topology.get("link_class_counts")
        == {"ACCESS": 32, "INTER_SWITCH": 256, "INTRA_NODE": 16},
        f"topology={topology}",
    )
    planes = source.get("planes", [])
    _add(
        checks,
        "two_isolated_complete_planes",
        len(planes) == 2
        and all(
            plane.get("switch_count") == 36
            and plane.get("access_link_count") == 16
            and plane.get("inter_switch_link_count") == 128
            and plane.get("gpu_ids") == list(range(16))
            for plane in planes
        ),
        f"plane_summaries={[{key: plane.get(key) for key in ('plane_id', 'switch_count', 'access_link_count', 'inter_switch_link_count')} for plane in planes]}",
    )
    failed = sum(item["status"] == FAIL for item in checks)
    return {
        "schema_version": "limer.p1-topology-validation.v1",
        "status": PASS if failed == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failed,
            "skip": 0,
        },
        "source_validation": _artifact(source_path),
        "topology_input": {
            "path": str(topology_path.resolve()) if topology_path else "",
            "sha256": actual_topology_sha,
        },
        "validated_contract": topology,
        "planes": planes,
        "checks": checks,
    }


def expected_plane_b_targets(link_map_path: Path) -> Tuple[Dict[int, str], List[str]]:
    frame = pd.read_csv(link_map_path)
    targets: Dict[int, str] = {}
    errors: List[str] = []
    access = frame[frame["link_class"] == "ACCESS"]
    for row in access.itertuples(index=False):
        if row.src_type == "HOST":
            gpu, host_port = int(row.src_node), int(row.src_port)
        elif row.dst_type == "HOST":
            gpu, host_port = int(row.dst_node), int(row.dst_port)
        else:
            continue
        if host_port != 3:
            continue
        if gpu in targets:
            errors.append(f"GPU {gpu} has duplicate host-port3 targets")
        targets[gpu] = str(row.link_id)
    if set(targets) != set(range(16)):
        errors.append(
            f"GPU inventory mismatch: missing={sorted(set(range(16)) - set(targets))}, "
            f"extra={sorted(set(targets) - set(range(16)))}"
        )
    if len(set(targets.values())) != 16:
        errors.append("Plane-B link ids are not unique")
    return targets, errors


def _finish_time(run_log: Path) -> int | None:
    text = run_log.read_text(encoding="utf-8", errors="replace")
    matches = re.findall(
        r"LIMER-COMMIT: all 16 GPU ranks reached the finish barrier at t=(\d+) ns",
        text,
    )
    return int(matches[-1]) if matches else None


def build_healthy_summary(
    matrix: Mapping[str, Any],
    telemetry: Mapping[str, Any],
) -> Dict[str, Any]:
    checks: List[Dict[str, str]] = []
    parity = matrix.get("healthy_parity", {})
    on_dir = Path(str(parity.get("monitoring_on_dir", "")))
    off_dir = Path(str(parity.get("monitoring_off_dir", "")))
    run = (telemetry.get("runs") or [{}])[0].get("telemetry", {})
    sample_count = int(run.get("sample_count", 0) or 0)
    first_sample = run.get("first_sample_ns")
    last_sample = run.get("last_sample_ns")
    virtual_span = (
        int(last_sample) - int(first_sample)
        if first_sample is not None and last_sample is not None else -1
    )
    _add(
        checks,
        "long_healthy_snapshot_requirement",
        sample_count >= MINIMUM_HEALTHY_SAMPLES
        and virtual_span >= MINIMUM_HEALTHY_SPAN_NS,
        f"samples={sample_count}/{MINIMUM_HEALTHY_SAMPLES}, "
        f"span_ns={virtual_span}/{MINIMUM_HEALTHY_SPAN_NS}",
    )

    on_finish = _finish_time(on_dir / "run.log") if (on_dir / "run.log").is_file() else None
    off_finish = _finish_time(off_dir / "run.log") if (off_dir / "run.log").is_file() else None
    _add(
        checks,
        "monitoring_on_off_exact_virtual_completion_parity",
        on_finish is not None and on_finish == off_finish,
        f"monitoring_on_finish_ns={on_finish}, monitoring_off_finish_ns={off_finish}",
    )
    on_exit = (
        int((on_dir / "exit_code.txt").read_text(encoding="utf-8").strip())
        if (on_dir / "exit_code.txt").is_file() else None
    )
    off_exit = (
        int((off_dir / "exit_code.txt").read_text(encoding="utf-8").strip())
        if (off_dir / "exit_code.txt").is_file() else None
    )
    _add(
        checks,
        "healthy_processes_exit_successfully",
        on_exit == 0 and off_exit == 0,
        f"monitoring_on={on_exit}, monitoring_off={off_exit}",
    )

    collective_path = on_dir / "collective_telemetry.csv"
    collective = pd.read_csv(collective_path) if collective_path.is_file() else pd.DataFrame()
    ranks = sorted(
        int(value) for value in collective.get("rank_id", pd.Series(dtype=int)).dropna().unique()
    )
    world_sizes = sorted(
        int(value) for value in collective.get("world_size", pd.Series(dtype=int)).dropna().unique()
    )
    statuses = sorted(
        str(value) for value in collective.get("status", pd.Series(dtype=str)).dropna().unique()
    )
    _add(
        checks,
        "healthy_true16_collective_completes",
        not collective.empty
        and ranks == list(range(16))
        and world_sizes == [16]
        and statuses == ["ok"],
        f"rows={len(collective)}, ranks={ranks}, world_sizes={world_sizes}, "
        f"statuses={statuses}",
    )
    alarm_path = on_dir / "alarm_telemetry.csv"
    healthy_alarms = _read_csv(alarm_path) if alarm_path.is_file() else []
    _add(
        checks,
        "healthy_run_has_no_alarm",
        alarm_path.is_file() and not healthy_alarms,
        f"alarm_rows={len(healthy_alarms)}",
    )
    telemetry_names = (
        "switch_telemetry.csv", "nic_telemetry.csv", "collective_telemetry.csv",
        "alarm_telemetry.csv", "link_map.csv", "rdma_wc_telemetry.csv",
        "recovery_telemetry.csv",
    )
    unexpected_off = [name for name in telemetry_names if (off_dir / name).exists()]
    _add(
        checks,
        "monitoring_off_emits_no_limer_telemetry",
        not unexpected_off,
        f"unexpected={unexpected_off}",
    )
    failed = sum(item["status"] == FAIL for item in checks)
    source_paths = [
        on_dir / "run.log", on_dir / "exit_code.txt", collective_path,
        on_dir / "switch_telemetry.csv", on_dir / "nic_telemetry.csv",
        on_dir / "link_map.csv", alarm_path, off_dir / "run.log",
        off_dir / "exit_code.txt",
    ]
    return {
        "schema_version": "limer.p1-healthy-true16-summary.v1",
        "status": PASS if failed == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failed,
            "skip": 0,
        },
        "monitoring_on_dir": str(on_dir.resolve()),
        "monitoring_off_dir": str(off_dir.resolve()),
        "sample_count": sample_count,
        "first_sample_ns": first_sample,
        "last_sample_ns": last_sample,
        "virtual_span_ns": virtual_span,
        "monitoring_on_finish_ns": on_finish,
        "monitoring_off_finish_ns": off_finish,
        "exact_virtual_completion_parity": on_finish is not None and on_finish == off_finish,
        "collective_rank_ids": ranks,
        "collective_world_sizes": world_sizes,
        "collective_statuses": statuses,
        "source_artifacts": {
            path.name + (".off" if path.parent == off_dir else ".on"): _artifact(path)
            for path in source_paths if path.is_file()
        },
        "checks": checks,
    }


def build_hard_event_timeline(
    matrix_path: Path,
    matrix: Mapping[str, Any],
    expected_targets: Mapping[int, str],
) -> List[Dict[str, Any]]:
    matrix_root = matrix_path.parent
    matrix_rows = {
        int(row.get("gpu")): row for row in matrix.get("target_matrix", [])
        if row.get("gpu") is not None
    }
    output: List[Dict[str, Any]] = []
    for gpu in range(16):
        errors: List[str] = []
        expected_link = expected_targets.get(gpu)
        gpu_dir = matrix_root / f"gpu_{gpu:02d}"
        event_path = gpu_dir / "fault_events.csv"
        alarm_path = gpu_dir / "hard_disconnect" / "alarm_telemetry.csv"
        events = _read_csv(event_path) if event_path.is_file() else []
        alarms = _read_csv(alarm_path) if alarm_path.is_file() else []
        matrix_row = matrix_rows.get(gpu, {})
        matrix_hash = (
            matrix_row.get("evidence", {}).get("alarm_telemetry.csv", {}).get("sha256")
        )
        actual_hash = _sha256(alarm_path) if alarm_path.is_file() else None
        if len(events) != 1:
            errors.append(f"fault_event_count={len(events)}")
        if len(alarms) != 1:
            errors.append(f"alarm_count={len(alarms)}")
        event = events[0] if len(events) == 1 else {}
        alarm = alarms[0] if len(alarms) == 1 else {}
        try:
            fault_start = _int(event, "start_time_ns")
            physical_fault = _int(alarm, "physical_fault_ns")
            observe = _int(alarm, "observe_ns")
            emit = _int(alarm, "emit_ns")
            deliver = _int(alarm, "deliver_ns")
            consume = _int(alarm, "consume_ns")
            actionable_latency = consume - physical_fault
        except (TypeError, ValueError) as error:
            errors.append(str(error))
            fault_start = physical_fault = observe = emit = deliver = consume = None
            actionable_latency = None

        identifiers_ok = (
            event.get("target_link_id") == expected_link
            and alarm.get("link_id") == expected_link
            and alarm.get("fault_id") == event.get("fault_id")
            and event.get("fault_type") == "hard_disconnect"
            and alarm.get("fault_type") == "hard_disconnect"
        )
        if not identifiers_ok:
            errors.append("fault/alarm identifiers do not match expected target")
        timeline_ok = (
            None not in (fault_start, physical_fault, observe, emit, deliver, consume)
            and fault_start == physical_fault
            and physical_fault <= observe <= emit <= deliver <= consume
        )
        if not timeline_ok:
            errors.append("alarm timestamps are not causal or onset-aligned")
        strict = (
            actionable_latency is not None
            and 0 <= actionable_latency < HARD_DETECTION_LIMIT_NS
        )
        if not strict:
            errors.append(
                f"actionable_latency_ns={actionable_latency} is not strict < "
                f"{HARD_DETECTION_LIMIT_NS}"
            )
        hash_match = actual_hash is not None and actual_hash == matrix_hash
        if not hash_match:
            errors.append("raw alarm hash does not match matrix report")
        matrix_consistent = (
            matrix_row.get("status") == PASS
            and matrix_row.get("expected_link_id") == expected_link
            and matrix_row.get("observed_link_id") == expected_link
            and matrix_row.get("hard_detection_latency_ns") == actionable_latency
        )
        if not matrix_consistent:
            errors.append("matrix row is not consistent with independently parsed alarm")
        if alarm.get("status") != "delivered":
            errors.append(f"alarm_status={alarm.get('status')}")
        output.append({
            "gpu": gpu,
            "target_link_id": expected_link,
            "host_port": 3,
            "plane_role": "B_PRIMARY",
            "fault_id": event.get("fault_id"),
            "fault_type": event.get("fault_type"),
            "fault_start_ns": fault_start,
            "physical_fault_ns": physical_fault,
            "observe_ns": observe,
            "emit_ns": emit,
            "deliver_ns": deliver,
            "consume_ns": consume,
            "actionable_latency_ns": actionable_latency,
            "deadline_ns": HARD_DETECTION_LIMIT_NS,
            "strict_under_1ms": strict,
            "detector": alarm.get("detector"),
            "alarm_status": alarm.get("status"),
            "raw_alarm_path": str(alarm_path.resolve()),
            "raw_alarm_sha256": actual_hash,
            "matrix_alarm_sha256": matrix_hash,
            "raw_alarm_hash_match": hash_match,
            "matrix_status": matrix_row.get("status"),
            "status": PASS if not errors else FAIL,
            "errors": "; ".join(errors),
        })
    return output


def evaluate_stage_gate(
    p0_gate: Mapping[str, Any],
    matrix: Mapping[str, Any],
    topology: Mapping[str, Any],
    telemetry: Mapping[str, Any],
    healthy: Mapping[str, Any],
    timeline: Sequence[Mapping[str, Any]],
    input_artifacts: Mapping[str, Any],
    output_artifacts: Mapping[str, Any],
) -> Dict[str, Any]:
    checks: List[Dict[str, str]] = []
    _add(
        checks,
        "p0_prerequisite_passed",
        p0_gate.get("stage") == "P0"
        and p0_gate.get("status") == PASS
        and p0_gate.get("next_stage") == "P1"
        and int(p0_gate.get("summary", {}).get("fail", -1)) == 0,
        f"stage={p0_gate.get('stage')}, status={p0_gate.get('status')}, "
        f"next={p0_gate.get('next_stage')}, summary={p0_gate.get('summary')}",
    )
    matrix_checks = matrix.get("checks", [])
    coverage = matrix.get("coverage", {})
    _add(
        checks,
        "matrix_report_revalidated",
        matrix.get("schema_version") == "limer.p1-hard-fault-matrix.v1"
        and matrix.get("contract_id") == "limer-true16-dual-plane-v1"
        and matrix.get("status") == PASS
        and all(item.get("status") == PASS for item in matrix_checks)
        and coverage.get("expected_gpu_count") == 16
        and coverage.get("present_evaluation_count") == 16
        and coverage.get("passing_evaluation_count") == 16
        and coverage.get("missing_gpus") == []
        and coverage.get("failed_gpus") == [],
        f"schema={matrix.get('schema_version')}, status={matrix.get('status')}, "
        f"coverage={coverage}",
    )
    _add(
        checks,
        "frozen_p1_thresholds_match",
        matrix.get("thresholds_ns", {}).get("hard_detection_strict_lt")
        == HARD_DETECTION_LIMIT_NS
        and matrix.get("thresholds_ns", {}).get("minimum_healthy_span")
        == MINIMUM_HEALTHY_SPAN_NS
        and matrix.get("thresholds_ns", {}).get("sample_interval")
        == SAMPLE_INTERVAL_NS,
        f"matrix_thresholds={matrix.get('thresholds_ns')}",
    )
    _add(
        checks,
        "topology_invariants_all_pass",
        _all_pass_no_skip(topology),
        f"status={topology.get('status')}, summary={topology.get('summary')}",
    )
    _add(
        checks,
        "telemetry_integrity_no_fail_or_skip",
        _all_pass_no_skip(telemetry),
        f"status={telemetry.get('status')}, summary={telemetry.get('summary')}",
    )
    _add(
        checks,
        "healthy_true16_collective_completes",
        healthy.get("status") == PASS
        and all(item.get("status") == PASS for item in healthy.get("checks", [])),
        f"status={healthy.get('status')}, ranks={healthy.get('collective_rank_ids')}, "
        f"world_sizes={healthy.get('collective_world_sizes')}",
    )
    _add(
        checks,
        "monitoring_on_off_virtual_completion_parity",
        healthy.get("exact_virtual_completion_parity") is True,
        f"on={healthy.get('monitoring_on_finish_ns')}, "
        f"off={healthy.get('monitoring_off_finish_ns')}",
    )
    _add(
        checks,
        "long_healthy_monitoring_requirement",
        int(healthy.get("sample_count", 0)) >= MINIMUM_HEALTHY_SAMPLES
        and int(healthy.get("virtual_span_ns", -1)) >= MINIMUM_HEALTHY_SPAN_NS,
        f"samples={healthy.get('sample_count')}, span_ns={healthy.get('virtual_span_ns')}",
    )
    target_links = [row.get("target_link_id") for row in timeline]
    _add(
        checks,
        "exact_16_unique_plane_b_host_port3_targets",
        len(timeline) == 16
        and {row.get("gpu") for row in timeline} == set(range(16))
        and len(set(target_links)) == 16
        and all(row.get("host_port") == 3 for row in timeline)
        and all(row.get("plane_role") == "B_PRIMARY" for row in timeline),
        f"rows={len(timeline)}, unique_links={len(set(target_links))}, "
        f"links={sorted(str(link) for link in target_links)}",
    )
    _add(
        checks,
        "all_raw_alarm_hashes_match_matrix_report",
        len(timeline) == 16 and all(row.get("raw_alarm_hash_match") for row in timeline),
        f"mismatches={[row.get('gpu') for row in timeline if not row.get('raw_alarm_hash_match')]}",
    )
    latencies = [row.get("actionable_latency_ns") for row in timeline]
    _add(
        checks,
        "all_hard_actionable_alarms_strict_under_1ms",
        len(timeline) == 16
        and all(row.get("status") == PASS for row in timeline)
        and all(
            isinstance(value, int) and 0 <= value < HARD_DETECTION_LIMIT_NS
            for value in latencies
        ),
        f"latencies_ns={latencies}, deadline_ns={HARD_DETECTION_LIMIT_NS}",
    )
    required_outputs = {
        "topology_validation.json", "telemetry_validation.json",
        "healthy_true16_summary.json", "hard_event_timeline.csv",
    }
    _add(
        checks,
        "required_child_artifacts_materialized_and_hashed",
        set(output_artifacts) == required_outputs
        and all(
            len(str(value.get("sha256", ""))) == 64
            and int(value.get("size_bytes", 0)) > 0
            for value in output_artifacts.values()
        ),
        f"artifacts={sorted(output_artifacts)}",
    )
    failed = sum(item["status"] == FAIL for item in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "P1",
        "contract_id": "limer-true16-dual-plane-v1",
        "status": PASS if failed == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failed,
            "skip": 0,
        },
        "next_stage": "P2" if failed == 0 else None,
        "inputs": dict(input_artifacts),
        "artifacts": dict(output_artifacts),
        "metrics": {
            "healthy_snapshot_count": healthy.get("sample_count"),
            "healthy_virtual_span_ns": healthy.get("virtual_span_ns"),
            "monitoring_on_finish_ns": healthy.get("monitoring_on_finish_ns"),
            "monitoring_off_finish_ns": healthy.get("monitoring_off_finish_ns"),
            "hard_target_count": len(timeline),
            "maximum_actionable_alarm_latency_ns": max(latencies) if latencies else None,
        },
        "claim_boundary": (
            "PASS establishes P1 true-16 SimAI topology, coherent sampled telemetry, "
            "healthy virtual-time parity, and simulated switch-carrier actionable "
            "alarms. It is not real switch, ibverbs, RDMA NIC, or NCCL evidence."
        ),
        "checks": checks,
    }


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_timeline(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=TIMELINE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(result: Mapping[str, Any], path: Path) -> None:
    lines = [
        "# LIMER P1 stage gate",
        "",
        f"**{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed, {result['summary']['skip']} skipped.**",
        "",
        f"Next stage: **{result.get('next_stage') or 'BLOCKED'}**",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for item in result["checks"]:
        detail = str(item["detail"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {item['check']} | {item['status']} | {detail} |")
    lines.extend(["", "## Metrics", ""])
    for name, value in result["metrics"].items():
        lines.append(f"- `{name}`: {value}")
    lines.extend(["", "## Claim boundary", "", str(result["claim_boundary"])])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def finalize(
    p0_gate_path: Path,
    matrix_path: Path,
    topology_validation_path: Path,
    out_dir: Path,
) -> Dict[str, Any]:
    p0_gate = _load_json(p0_gate_path)
    matrix = _load_json(matrix_path)
    topology = build_topology_validation(topology_validation_path, matrix)
    topology_path = Path(str(matrix.get("inputs", {}).get("topology", {}).get("path", "")))
    on_dir = Path(str(matrix.get("healthy_parity", {}).get("monitoring_on_dir", "")))
    telemetry = audit_true16_monitoring.build_audit(
        topology_path, [on_dir], SAMPLE_INTERVAL_NS)
    telemetry["source_matrix_report"] = _artifact(matrix_path)
    healthy = build_healthy_summary(matrix, telemetry)
    targets, target_errors = expected_plane_b_targets(on_dir / "link_map.csv")
    timeline = build_hard_event_timeline(matrix_path, matrix, targets)
    if target_errors:
        for row in timeline:
            row["status"] = FAIL
            row["errors"] = "; ".join(
                value for value in (row.get("errors", ""), *target_errors) if value
            )

    child_paths = {
        "topology_validation.json": out_dir / "topology_validation.json",
        "telemetry_validation.json": out_dir / "telemetry_validation.json",
        "healthy_true16_summary.json": out_dir / "healthy_true16_summary.json",
        "hard_event_timeline.csv": out_dir / "hard_event_timeline.csv",
    }
    _write_json(topology, child_paths["topology_validation.json"])
    _write_json(telemetry, child_paths["telemetry_validation.json"])
    _write_json(healthy, child_paths["healthy_true16_summary.json"])
    _write_timeline(timeline, child_paths["hard_event_timeline.csv"])
    output_artifacts = {
        name: _artifact(path) for name, path in child_paths.items()
    }
    input_artifacts = {
        "stage_gate_p0.json": _artifact(p0_gate_path),
        "stage_matrix.json": _artifact(matrix_path),
        "source_topology_validation.json": _artifact(topology_validation_path),
    }
    gate = evaluate_stage_gate(
        p0_gate, matrix, topology, telemetry, healthy, timeline,
        input_artifacts, output_artifacts,
    )
    gate_json = out_dir / "stage_gate_p1.json"
    gate_md = out_dir / "stage_gate_p1.md"
    _write_json(gate, gate_json)
    _write_markdown(gate, gate_md)
    return gate


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--p0-gate", required=True, type=Path)
    parser.add_argument("--matrix-report", required=True, type=Path)
    parser.add_argument("--topology-validation", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = finalize(
            args.p0_gate,
            args.matrix_report,
            args.topology_validation,
            args.out_dir,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError, pd.errors.ParserError) as error:
        result = {
            "schema_version": SCHEMA_VERSION,
            "stage": "P1",
            "status": FAIL,
            "summary": {"pass": 0, "fail": 1, "skip": 0},
            "next_stage": None,
            "error": str(error),
            "checks": [],
        }
        args.out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(result, args.out_dir / "stage_gate_p1.json")
        _write_markdown(
            {**result, "metrics": {}, "claim_boundary": "P1 aggregation failed."},
            args.out_dir / "stage_gate_p1.md",
        )
    print(
        f"{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed, "
        f"{result['summary'].get('skip', 0)} skipped"
    )
    print(f"Wrote P1 artifacts under {args.out_dir}")
    return 0 if result["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
