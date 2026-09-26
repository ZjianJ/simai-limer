#!/usr/bin/env python3
"""Strict evidence audit for the true-16 dual-rail hard-fault experiment.

This evaluator never substitutes a configured delay, a source-code constant,
or a paper projection for runtime evidence.  A missing file, column, event, or
post-fault sample therefore produces FAIL (with a reason) instead of UNKNOWN
being silently interpreted as success.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PASS = "PASS"
FAIL = "FAIL"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--healthy-dir", required=True, type=Path)
    parser.add_argument("--fault-dir", required=True, type=Path)
    parser.add_argument("--fault-events", required=True, type=Path)
    parser.add_argument("--topology-validation", required=True, type=Path)
    parser.add_argument("--fault-selection", type=Path)
    parser.add_argument("--expected-ranks", type=int, default=16)
    parser.add_argument("--expected-links", type=int, default=304)
    parser.add_argument("--expected-access-links", type=int, default=32)
    parser.add_argument("--hard-detection-slo-ns", type=int, default=1_000_000)
    parser.add_argument("--recovery-slo-ns", type=int, default=1_000_000_000)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def evidence(path: Path) -> Dict[str, Any]:
    exists = path.is_file()
    return {
        "path": str(path.resolve()),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "sha256": sha256(path) if exists else None,
    }


def load_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str], Optional[str]]:
    if not path.is_file():
        return [], [], "missing file"
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            fields = list(reader.fieldnames or [])
            if not fields:
                return [], [], "missing CSV header"
            if len(fields) != len(set(fields)):
                return [], fields, "duplicate CSV columns"
            rows = list(reader)
    except (OSError, csv.Error, UnicodeError) as error:
        return [], [], str(error)
    return rows, fields, None


def load_json(path: Path) -> Tuple[Dict[str, Any], Optional[str]]:
    if not path.is_file():
        return {}, "missing file"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return {}, str(error)
    if not isinstance(value, dict):
        return {}, "top-level JSON value is not an object"
    return value, None


def integer(row: Dict[str, str], names: Sequence[str]) -> Optional[int]:
    for name in names:
        value = row.get(name)
        if value is None or value == "":
            continue
        try:
            return int(value)
        except ValueError:
            return None
    return None


def text_value(row: Dict[str, str], names: Sequence[str]) -> Optional[str]:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def first_existing(fields: Iterable[str], aliases: Sequence[str]) -> Optional[str]:
    available = set(fields)
    return next((name for name in aliases if name in available), None)


def is_event(row: Dict[str, str], *names: str) -> bool:
    event = (text_value(row, ("event", "event_type", "type")) or "").upper()
    normalized = re.sub(r"[^A-Z0-9]+", "_", event).strip("_")
    wanted = {re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_") for name in names}
    return normalized in wanted


class Audit:
    def __init__(self) -> None:
        self.checks: List[Dict[str, Any]] = []

    def add(self, name: str, passed: bool, detail: str, **metrics: Any) -> None:
        row: Dict[str, Any] = {
            "check": name,
            "status": PASS if passed else FAIL,
            "detail": detail,
        }
        if metrics:
            row["metrics"] = metrics
        self.checks.append(row)


def run_log_audit(path: Path, expected_ranks: int) -> Dict[str, Any]:
    if not path.is_file():
        return {"passed": False, "detail": "run.log missing"}
    raw = path.read_text(errors="replace")
    lower = raw.lower()
    fatal_markers = [
        marker for marker in (
            "segmentation fault", "double free", "aborted", "assert failed",
            "terminate called", "timeout: the monitored command dumped core",
        ) if marker in lower
    ]
    model16 = bool(re.search(
        rf"model_parallel_NPU_group is\s+{expected_ranks}(?:\D|$)", raw
    ))
    ring16 = bool(re.search(
        rf"dimension:\s*local total nodes in ring:\s*{expected_ranks}(?:\D|$)", raw
    ))
    barrier16 = bool(re.search(
        rf"LIMER-COMMIT:\s*all\s+{expected_ranks}\s+GPU ranks", raw
    ))
    completed = (
        "Percentage of finished streams: 100" in raw
        and "all passes finished at time:" in raw
    )
    exit_code_path = path.parent / "exit_code.txt"
    exit_code: Optional[int] = None
    if exit_code_path.is_file():
        try:
            exit_code = int(exit_code_path.read_text().strip())
        except ValueError:
            pass
    passed = bool(
        model16 and ring16 and barrier16 and completed and not fatal_markers
        and exit_code == 0
    )
    return {
        "passed": passed,
        "detail": (
            f"model16={model16}, ring16={ring16}, all-rank barrier={barrier16}, "
            f"completed={completed}, exit_code={exit_code}, fatal={fatal_markers}"
        ),
        "model_parallel_16": model16,
        "ring_16": ring16,
        "finish_barrier_16": barrier16,
        "completed": completed,
        "exit_code": exit_code,
        "fatal_markers": fatal_markers,
    }


def monotonic_counter(
    rows: List[Dict[str, str]], timestamp_field: str, counter_field: str
) -> Tuple[bool, List[Tuple[int, int]]]:
    points = []
    for row in rows:
        timestamp = integer(row, (timestamp_field,))
        counter = integer(row, (counter_field,))
        if timestamp is not None and counter is not None:
            points.append((timestamp, counter))
    points.sort()
    return (
        bool(points) and all(right[1] >= left[1] for left, right in zip(points, points[1:])),
        points,
    )


def endpoint_down_and_quiescent(
    rows: List[Dict[str, str]], fields: List[str], start_ns: int,
    *, node_field: str, port_field: str, node: int, port: int,
    direction: Optional[str] = None,
) -> Dict[str, Any]:
    required = {"timestamp_ns", "link_state", "tx_bytes", node_field, port_field}
    missing = sorted(required - set(fields))
    if missing:
        return {"passed": False, "detail": f"missing columns {missing}"}
    selected = []
    for row in rows:
        if integer(row, (node_field,)) != node or integer(row, (port_field,)) != port:
            continue
        if direction is not None and row.get("direction") != direction:
            continue
        timestamp = integer(row, ("timestamp_ns",))
        if timestamp is not None and timestamp >= start_ns:
            selected.append(row)
    timestamps = sorted({integer(row, ("timestamp_ns",)) for row in selected})
    timestamps = [value for value in timestamps if value is not None]
    down = bool(selected) and all(row.get("link_state", "").lower() == "down" for row in selected)
    down_timestamps = [integer(row, ("last_link_down_ns",)) for row in selected]
    exact_fault_timestamp = bool(down_timestamps) and all(
        value == start_ns for value in down_timestamps if value is not None
    ) and any(value is not None for value in down_timestamps)
    monotonic, points = monotonic_counter(selected, "timestamp_ns", "tx_bytes")
    unique_values = sorted({value for _, value in points})
    enough_samples = len(timestamps) >= 2
    quiescent = enough_samples and len(unique_values) == 1
    return {
        "passed": bool(down and exact_fault_timestamp and monotonic and quiescent),
        "detail": (
            f"post_fault_samples={len(timestamps)}, all_down={down}, "
            f"last_link_down_exact={exact_fault_timestamp}, "
            f"tx_counter_values={unique_values}"
        ),
        "post_fault_timestamps_ns": timestamps,
        "post_fault_tx_bytes": unique_values,
        "all_down": down,
        "last_link_down_exact": exact_fault_timestamp,
        "counter_monotonic": monotonic,
        "tx_quiescent": quiescent,
    }


def audit_ranks(
    rows: List[Dict[str, str]], fields: List[str], expected: int
) -> Dict[str, Any]:
    if "world_size" not in fields or "rank_id" not in fields:
        return {
            "passed": False,
            "detail": "collective_transaction.csv lacks world_size or rank_id",
        }
    world_sizes = sorted({
        value for row in rows
        if (value := integer(row, ("world_size",))) is not None
    })
    ready_ranks = sorted({
        value for row in rows
        if is_event(row, "LOCAL_READY")
        and (value := integer(row, ("rank_id",))) is not None
    })
    expected_ranks = list(range(expected))
    passed = world_sizes == [expected] and ready_ranks == expected_ranks
    return {
        "passed": passed,
        "detail": f"world_sizes={world_sizes}, LOCAL_READY ranks={ready_ranks}",
        "world_sizes": world_sizes,
        "ready_ranks": ready_ranks,
    }


def committed_transactions(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return [
        row for row in rows
        if is_event(row, "COMMIT")
        or (row.get("status", "").lower() in {"committed", "exactly_once"}
            and not text_value(row, ("event", "event_type")))
    ]


def qpid(row: Dict[str, str]) -> Optional[str]:
    return text_value(row, ("logical_qp_id", "qp_id", "qpid"))


def is_training_rdma(row: Dict[str, str]) -> bool:
    """Legacy traces have no class; new traces exclude independent load QPs."""
    value = row.get("traffic_class", "").strip().upper()
    return value in {"", "TRAINING"}


def audit_backup_qps(
    rows: List[Dict[str, str]], fields: List[str], start_ns: int,
    consume_ns: int,
) -> Dict[str, Any]:
    required = {"timestamp_ns", "logical_qp_id", "event", "primary_nic", "backup_nic"}
    missing = sorted(required - set(fields))
    if missing:
        return {"passed": False, "detail": f"missing columns {missing}"}
    rows = [row for row in rows if is_training_rdma(row)]

    ready = [row for row in rows if is_event(
        row, "BACKUP_READY", "QP_PREESTABLISHED", "BACKUP_QP_READY"
    )]
    failover = [row for row in rows if is_event(
        row, "FAILOVER", "BACKUP_ACTIVATE", "BACKUP_ACTIVE"
    )]
    ready_qps = {qpid(row) for row in ready if qpid(row)}
    failover_qps = {qpid(row) for row in failover if qpid(row)}
    relevant_ready = [row for row in ready if qpid(row) in failover_qps]
    ready_before = bool(relevant_ready) and all(
        (integer(row, ("backup_ready_ns", "timestamp_ns")) or start_ns + 1) < start_ns
        for row in relevant_ready
    )
    distinct_nics = bool(relevant_ready) and all(
        integer(row, ("primary_nic",)) is not None
        and integer(row, ("backup_nic",)) is not None
        and integer(row, ("primary_nic",)) != integer(row, ("backup_nic",))
        for row in relevant_ready
    )

    standby_field = first_existing(fields, (
        "standby_tx_bytes", "backup_tx_bytes_before_activation",
        "backup_tx_bytes",
    ))
    first_tx_field = first_existing(fields, (
        "backup_first_tx_ns", "first_backup_tx_ns", "first_tx_on_backup_ns",
    ))
    first_ack_field = first_existing(fields, (
        "backup_first_ack_ns", "first_backup_ack_ns", "first_useful_ack_ns",
    ))
    explicit_zero = False
    zero_detail = ""
    if standby_field:
        ready_values = [integer(row, (standby_field,)) for row in relevant_ready]
        explicit_zero = bool(ready_values) and all(value == 0 for value in ready_values)
        zero_detail = f"{standby_field} on ready rows={ready_values}"
    elif first_tx_field:
        first_values = [
            integer(row, (first_tx_field,)) for row in rows
            if qpid(row) in failover_qps and integer(row, (first_tx_field,)) is not None
        ]
        explicit_zero = bool(first_values) and all(value >= consume_ns for value in first_values)
        zero_detail = f"{first_tx_field}={first_values}, consume_ns={consume_ns}"
    else:
        zero_detail = "no standby byte counter or backup-first-TX timestamp column"

    first_tx_values = (
        [integer(row, (first_tx_field,)) for row in rows
         if qpid(row) in failover_qps and integer(row, (first_tx_field,)) is not None]
        if first_tx_field else []
    )
    first_ack_values = (
        [integer(row, (first_ack_field,)) for row in rows
         if qpid(row) in failover_qps and integer(row, (first_ack_field,)) is not None]
        if first_ack_field else []
    )
    activation_after_consume = bool(failover) and all(
        (integer(row, ("timestamp_ns",)) or -1) >= consume_ns for row in failover
    )
    active_subset = bool(failover_qps) and failover_qps <= ready_qps
    first_tx_proven = bool(first_tx_values) and all(value >= consume_ns for value in first_tx_values)
    first_ack_proven = bool(first_ack_values) and all(value >= consume_ns for value in first_ack_values)
    passed = all((
        ready_before, distinct_nics, explicit_zero, activation_after_consume,
        active_subset, first_tx_proven, first_ack_proven,
    ))
    return {
        "passed": passed,
        "detail": (
            f"ready_qps={sorted(ready_qps)}, failover_qps={sorted(failover_qps)}, "
            f"ready_before_fault={ready_before}, distinct_nics={distinct_nics}, "
            f"standby_zero=({zero_detail}), activation_after_consume="
            f"{activation_after_consume}, first_backup_tx={first_tx_values}, "
            f"first_useful_ack={first_ack_values}"
        ),
        "ready_qps": sorted(ready_qps),
        "failover_qps": sorted(failover_qps),
        "ready_before_fault": ready_before,
        "distinct_primary_backup_nics": distinct_nics,
        "standby_zero_before_fault": explicit_zero,
        "activation_after_alarm_consume": activation_after_consume,
        "first_backup_tx_ns": first_tx_values,
        "first_backup_ack_ns": first_ack_values,
    }


def audit_wc(rows: List[Dict[str, str]], fields: List[str], failed_qps: Sequence[str]) -> Dict[str, Any]:
    if "event" not in fields or "wc_status" not in fields or "logical_qp_id" not in fields:
        return {
            "passed": False,
            "detail": "missing event, wc_status, or logical_qp_id column",
        }
    rows = [row for row in rows if is_training_rdma(row)]
    wc_rows = [row for row in rows if is_event(row, "WC", "WC_COMPLETION")]
    successful = [row for row in wc_rows if row.get("wc_status", "").upper() in {
        "1", "SUCCESS", "IBV_WC_SUCCESS", "WC_SUCCESS",
    }]
    flushed = [row for row in wc_rows if row.get("wc_status", "").upper() in {
        "2", "WR_FLUSH_ERR", "IBV_WC_WR_FLUSH_ERR",
    }]
    fatal = [row for row in wc_rows if row.get("wc_status", "").upper() in {
        "3", "RETRY_EXC_ERR", "IBV_WC_RETRY_EXC_ERR",
    }]
    successful_qps = {qpid(row) for row in successful if qpid(row)}
    failed_set = set(failed_qps)
    affected_success = bool(failed_set) and failed_set <= successful_qps
    flushed_qps = {qpid(row) for row in flushed if qpid(row)}
    affected_flushed = bool(failed_set) and failed_set <= flushed_qps
    ordered = True
    for qp_id in failed_set:
        flush_times = [integer(row, ("timestamp_ns",)) for row in flushed if qpid(row) == qp_id]
        success_times = [integer(row, ("timestamp_ns",)) for row in successful if qpid(row) == qp_id]
        flush_times = [value for value in flush_times if value is not None]
        success_times = [value for value in success_times if value is not None]
        if not flush_times or not success_times or min(success_times) < min(flush_times):
            ordered = False
    passed = bool(wc_rows and affected_success and affected_flushed and ordered and not fatal)
    return {
        "passed": passed,
        "detail": (
            f"wc_events={len(wc_rows)}, successful_qps={sorted(successful_qps)}, "
            f"affected_qps={sorted(failed_set)}, flushed_qps={sorted(flushed_qps)}, "
            f"flush_before_success={ordered}, retry_exhausted={len(fatal)}"
        ),
        "wc_event_count": len(wc_rows),
        "successful_qps": sorted(successful_qps),
        "flushed_qps": sorted(flushed_qps),
        "fatal_wc_count": len(fatal),
    }


def write_markdown(path: Path, report: Dict[str, Any]) -> None:
    summary = report["summary"]
    lines = [
        "# True-16 dual-rail hard-fault end-to-end audit",
        "",
        f"**Overall: {report['status']}** — {summary['pass']} PASS, "
        f"{summary['fail']} FAIL.",
        "",
        "This report accepts only simulator-emitted runtime evidence. Missing "
        "files, columns, events, timestamps, and counters are failures.",
        "",
        "| Check | Status | Evidence |",
        "|---|---:|---|",
    ]
    for row in report["checks"]:
        detail = str(row["detail"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{row['check']}` | **{row['status']}** | {detail} |")
    lines.extend([
        "",
        "## SLO interpretation",
        "",
        "The hard-failure detection result uses `consume_ns - physical_fault_ns`, "
        "not the detector's internal observation time. Recovery is measured from "
        "that consumed, executable alarm. Collective safety requires one commit, "
        "16 ready ranks, a replay marker in the fault run, and the same non-empty "
        "result digest as the healthy run.",
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    audit = Audit()
    names = [
        "link_map.csv", "switch_telemetry.csv", "nic_telemetry.csv",
        "alarm_telemetry.csv", "recovery_telemetry.csv",
        "rdma_wc_telemetry.csv", "collective_transaction.csv", "run.log",
        "exit_code.txt",
    ]
    files: Dict[str, Dict[str, Dict[str, Any]]] = {"healthy": {}, "fault": {}}
    for scope, directory in (("healthy", args.healthy_dir), ("fault", args.fault_dir)):
        for name in names:
            files[scope][name] = evidence(directory / name)

    fault_required = names
    healthy_required = [
        "link_map.csv", "switch_telemetry.csv", "nic_telemetry.csv",
        "rdma_wc_telemetry.csv", "collective_transaction.csv", "run.log",
        "exit_code.txt",
    ]
    missing = [
        f"healthy/{name}" for name in healthy_required
        if not files["healthy"][name]["exists"]
    ] + [
        f"fault/{name}" for name in fault_required
        if not files["fault"][name]["exists"]
    ]
    audit.add("required_runtime_evidence", not missing,
              f"missing={missing}" if missing else "all required evidence files exist")

    csv_names = names[:7]
    loaded: Dict[str, Dict[str, Tuple[List[Dict[str, str]], List[str], Optional[str]]]] = {
        "healthy": {}, "fault": {}
    }
    parse_errors = []
    for scope, directory in (("healthy", args.healthy_dir), ("fault", args.fault_dir)):
        for name in csv_names:
            loaded[scope][name] = load_csv(directory / name)
            error = loaded[scope][name][2]
            if error and not (scope == "healthy" and name in {
                "alarm_telemetry.csv", "recovery_telemetry.csv"
            }):
                parse_errors.append(f"{scope}/{name}: {error}")
    audit.add("csv_integrity", not parse_errors,
              f"errors={parse_errors}" if parse_errors else "all required CSV files parse")

    topology, topology_error = load_json(args.topology_validation)
    topo_info = topology.get("topology", {}) if isinstance(topology, dict) else {}
    class_counts = topo_info.get("link_class_counts", {}) or {}
    topology_ok = bool(
        not topology_error and topology.get("status") == PASS
        and topo_info.get("gpu_count") == args.expected_ranks
        and topo_info.get("physical_link_count") == args.expected_links
        and class_counts.get("ACCESS") == args.expected_access_links
        and len(topology.get("planes", [])) == 2
    )
    audit.add(
        "dualrail_topology_contract", topology_ok,
        f"error={topology_error}, status={topology.get('status')}, "
        f"gpus={topo_info.get('gpu_count')}, links={topo_info.get('physical_link_count')}, "
        f"ACCESS={class_counts.get('ACCESS')}, planes={len(topology.get('planes', []))}",
    )

    fault_rows, fault_fields, fault_error = load_csv(args.fault_events)
    hard_rows = [row for row in fault_rows if row.get("fault_type") in {
        "hard_disconnect", "hard_link_failure"
    }]
    fault: Dict[str, str] = hard_rows[0] if len(hard_rows) == 1 else {}
    start_ns = integer(fault, ("start_time_ns",))
    end_ns = integer(fault, ("end_time_ns",))
    target_link = fault.get("target_link_id")
    permanent = end_ns in (0, None) or fault.get("parameter_after") in {
        "physically_disconnected", "permanent_down",
    }
    fault_contract_ok = bool(
        not fault_error and len(fault_rows) == 1 and len(hard_rows) == 1
        and start_ns is not None and start_ns > 0 and target_link and permanent
    )
    audit.add(
        "permanent_hard_fault_contract", fault_contract_ok,
        f"rows={len(fault_rows)}, hard_rows={len(hard_rows)}, target={target_link}, "
        f"start_ns={start_ns}, end_ns={end_ns}, permanent={permanent}, error={fault_error}",
    )
    start_ns = start_ns if start_ns is not None else -1

    healthy_map, healthy_map_fields, _ = loaded["healthy"]["link_map.csv"]
    fault_map, fault_map_fields, _ = loaded["fault"]["link_map.csv"]
    unique_links = {row.get("link_id") for row in fault_map if row.get("link_id")}
    access_links = {row.get("link_id") for row in fault_map
                    if row.get("link_class") == "ACCESS" and row.get("link_id")}
    maps_equal = bool(healthy_map and fault_map and healthy_map == fault_map)
    target_rows = [row for row in fault_map if row.get("link_id") == target_link]
    map_ok = bool(
        set(fault_map_fields) >= {
            "link_id", "src_node", "dst_node", "src_type", "dst_type",
            "src_port", "dst_port", "link_class",
        }
        and len(fault_map) == args.expected_links
        and len(unique_links) == args.expected_links
        and len(access_links) == args.expected_access_links
        and maps_equal and len(target_rows) == 1
        and target_rows[0].get("link_class") == "ACCESS"
    )
    audit.add(
        "runtime_physical_link_map", map_ok,
        f"fault_rows={len(fault_map)}, unique_links={len(unique_links)}, "
        f"ACCESS={len(access_links)}, healthy_equal={maps_equal}, "
        f"target_class={target_rows[0].get('link_class') if target_rows else None}",
    )

    for scope in ("healthy", "fault"):
        txn_rows, txn_fields, _ = loaded[scope]["collective_transaction.csv"]
        rank_result = audit_ranks(txn_rows, txn_fields, args.expected_ranks)
        audit.add(f"{scope}_true16_collective_participants",
                  rank_result["passed"], rank_result["detail"])
        log_result = run_log_audit(
            (args.healthy_dir if scope == "healthy" else args.fault_dir) / "run.log",
            args.expected_ranks,
        )
        audit.add(f"{scope}_all_rank_completion", log_result["passed"],
                  log_result["detail"])

    endpoint_results: Dict[str, Any] = {}
    if target_rows:
        target = target_rows[0]
        if target.get("src_type") == "HOST":
            host_node = integer(target, ("src_node",))
            host_port = integer(target, ("src_port",))
            switch_node = integer(target, ("dst_node",))
            switch_port = integer(target, ("dst_port",))
        else:
            host_node = integer(target, ("dst_node",))
            host_port = integer(target, ("dst_port",))
            switch_node = integer(target, ("src_node",))
            switch_port = integer(target, ("src_port",))
        nic_rows, nic_fields, _ = loaded["fault"]["nic_telemetry.csv"]
        switch_rows, switch_fields, _ = loaded["fault"]["switch_telemetry.csv"]
        if None not in (host_node, host_port):
            endpoint_results["host"] = endpoint_down_and_quiescent(
                nic_rows, nic_fields, start_ns,
                node_field="node_id", port_field="nic_id",
                node=int(host_node), port=int(host_port),
            )
        if None not in (switch_node, switch_port):
            endpoint_results["switch"] = endpoint_down_and_quiescent(
                switch_rows, switch_fields, start_ns,
                node_field="switch_id", port_field="port_id",
                node=int(switch_node), port=int(switch_port), direction="tx",
            )
    both_down = bool(
        endpoint_results.get("host", {}).get("passed")
        and endpoint_results.get("switch", {}).get("passed")
    )
    audit.add(
        "hard_disconnect_both_endpoints_and_dead_port_quiescence", both_down,
        json.dumps(endpoint_results, sort_keys=True),
    )

    alarm_rows, alarm_fields, _ = loaded["fault"]["alarm_telemetry.csv"]
    alarms = [row for row in alarm_rows if row.get("link_id") == target_link]
    alarm_required = {
        "physical_fault_ns", "observe_ns", "emit_ns", "deliver_ns", "consume_ns",
    }
    alarm_missing = sorted(alarm_required - set(alarm_fields))
    alarm_metrics: Dict[str, Any] = {}
    consume_ns: Optional[int] = None
    alarm_ok = False
    if len(alarms) == 1 and not alarm_missing:
        row = alarms[0]
        physical = integer(row, ("physical_fault_ns",))
        observe = integer(row, ("observe_ns",))
        emit = integer(row, ("emit_ns",))
        deliver = integer(row, ("deliver_ns",))
        consume_ns = integer(row, ("consume_ns",))
        ordered = (
            None not in (physical, observe, emit, deliver, consume_ns)
            and physical <= observe <= emit <= deliver <= consume_ns
        )
        latency = consume_ns - physical if ordered else None
        alarm_metrics = {
            "physical_fault_ns": physical,
            "observe_ns": observe,
            "emit_ns": emit,
            "deliver_ns": deliver,
            "consume_ns": consume_ns,
            "executable_alarm_latency_ns": latency,
        }
        alarm_ok = bool(
            physical == start_ns and ordered and latency is not None
            and latency < args.hard_detection_slo_ns
            and row.get("status", "").lower() in {"delivered", "consumed", "actionable"}
        )
    audit.add(
        "online_executable_alarm_under_1ms", alarm_ok,
        f"matching_alarms={len(alarms)}, missing_columns={alarm_missing}, metrics={alarm_metrics}",
    )

    recovery_rows, recovery_fields, _ = loaded["fault"]["recovery_telemetry.csv"]
    recoveries = [row for row in recovery_rows if row.get("link_id") == target_link]
    recovery_required = {
        "alarm_consume_ns", "route_rebuild_ns", "primary_quiesce_ns",
        "backup_activate_ns", "affected_qps", "status",
    }
    recovery_missing = sorted(recovery_required - set(recovery_fields))
    recovery_metrics: Dict[str, Any] = {}
    recovery_ok = False
    if len(recoveries) == 1 and consume_ns is not None and not recovery_missing:
        row = recoveries[0]
        recorded_consume = integer(row, ("alarm_consume_ns",))
        route = integer(row, ("route_rebuild_ns",))
        quiesce = integer(row, ("primary_quiesce_ns",))
        activate = integer(row, ("backup_activate_ns",))
        affected = integer(row, ("affected_qps",))
        ordered = (
            None not in (recorded_consume, route, quiesce, activate)
            and recorded_consume == consume_ns
            and recorded_consume <= route <= activate
            and recorded_consume <= quiesce <= activate
        )
        latency = activate - consume_ns if ordered else None
        recovery_metrics = {
            "alarm_consume_ns": recorded_consume,
            "route_rebuild_ns": route,
            "primary_quiesce_ns": quiesce,
            "backup_activate_ns": activate,
            "affected_qps": affected,
            "recovery_from_alarm_ns": latency,
            "status": row.get("status"),
        }
        recovery_ok = bool(
            ordered and affected is not None and affected > 0
            and latency is not None and latency < args.recovery_slo_ns
            and row.get("status") == "backup_active"
        )
    audit.add(
        "route_and_backup_activation_under_1s", recovery_ok,
        f"matching_rows={len(recoveries)}, missing_columns={recovery_missing}, "
        f"metrics={recovery_metrics}",
    )

    rdma_rows, rdma_fields, _ = loaded["fault"]["rdma_wc_telemetry.csv"]
    backup_result = audit_backup_qps(
        rdma_rows, rdma_fields, start_ns,
        consume_ns if consume_ns is not None else 2**63 - 1,
    )
    audit.add("preestablished_idle_backup_qp_and_live_failover",
              backup_result["passed"], backup_result["detail"])
    wc_result = audit_wc(rdma_rows, rdma_fields, backup_result.get("failover_qps", []))
    audit.add("rdma_work_completion_on_surviving_qp",
              wc_result["passed"], wc_result["detail"])

    healthy_tx, healthy_tx_fields, _ = loaded["healthy"]["collective_transaction.csv"]
    fault_tx, fault_tx_fields, _ = loaded["fault"]["collective_transaction.csv"]
    healthy_commits = committed_transactions(healthy_tx)
    fault_commits = committed_transactions(fault_tx)

    def commit_key(row: Dict[str, str]) -> Optional[str]:
        return text_value(row, ("collective_seq", "collective_id", "transaction_id"))

    healthy_by_key: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    fault_by_key: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in healthy_commits:
        if commit_key(row) is not None:
            healthy_by_key[str(commit_key(row))].append(row)
    for row in fault_commits:
        if commit_key(row) is not None:
            fault_by_key[str(commit_key(row))].append(row)
    one_each = bool(
        healthy_by_key and fault_by_key
        and set(healthy_by_key) == set(fault_by_key)
        and all(len(rows) == 1 for rows in healthy_by_key.values())
        and all(len(rows) == 1 for rows in fault_by_key.values())
    )
    healthy_digests = {
        key: text_value(rows[0], ("result_digest", "digest"))
        for key, rows in healthy_by_key.items()
    }
    fault_digests = {
        key: text_value(rows[0], ("result_digest", "digest"))
        for key, rows in fault_by_key.items()
    }
    digest_match = bool(
        one_each and all(healthy_digests.values())
        and healthy_digests == fault_digests
    )
    redo_keys = {
        str(commit_key(row)) for row in fault_tx
        if is_event(row, "REDO_START", "REDO", "REPLAY_START")
        and commit_key(row) is not None
    }
    redo_all = bool(fault_by_key and set(fault_by_key) <= redo_keys)
    ranks_at_commit = all(
        integer(rows[0], ("ready_ranks", "participant_count")) == args.expected_ranks
        and integer(rows[0], ("world_size",)) == args.expected_ranks
        for rows in list(healthy_by_key.values()) + list(fault_by_key.values())
    ) if one_each else False
    collective_safe = bool(one_each and digest_match and redo_all and ranks_at_commit)
    audit.add(
        "collective_redo_exactly_once_and_digest_safe", collective_safe,
        f"healthy_commits={len(healthy_commits)}, fault_commits={len(fault_commits)}, "
        f"keys={sorted(fault_by_key)}, redo_keys={sorted(redo_keys)}, "
        f"healthy_digests={healthy_digests}, fault_digests={fault_digests}, "
        f"16_ready_at_commit={ranks_at_commit}",
    )

    progress_metrics: Dict[str, int] = {}
    progress_ok = bool(fault_commits and consume_ns is not None)
    if progress_ok:
        for key, rows in fault_by_key.items():
            commit_ns = integer(rows[0], ("timestamp_ns", "commit_ns"))
            if commit_ns is None or commit_ns < consume_ns:
                progress_ok = False
                continue
            progress_metrics[key] = commit_ns - consume_ns
            if commit_ns - consume_ns >= args.recovery_slo_ns:
                progress_ok = False
    audit.add(
        "training_collective_progress_under_1s", progress_ok,
        f"commit_minus_alarm_consume_ns={progress_metrics}, "
        f"threshold_ns={args.recovery_slo_ns}",
    )

    status = PASS if all(row["status"] == PASS for row in audit.checks) else FAIL
    report: Dict[str, Any] = {
        "schema_version": "limer.true16-hard-fault-e2e.v1",
        "status": status,
        "summary": {
            "pass": sum(row["status"] == PASS for row in audit.checks),
            "fail": sum(row["status"] == FAIL for row in audit.checks),
        },
        "thresholds_ns": {
            "hard_detection": args.hard_detection_slo_ns,
            "recovery_and_progress": args.recovery_slo_ns,
        },
        "fault": {
            "fault_id": fault.get("fault_id"),
            "fault_type": fault.get("fault_type"),
            "target_link_id": target_link,
            "start_time_ns": start_ns,
            "permanent": permanent,
        },
        "checks": audit.checks,
        "files": files,
        "topology_validation": evidence(args.topology_validation),
        "fault_events": evidence(args.fault_events),
        "fault_selection": evidence(args.fault_selection) if args.fault_selection else None,
        "semantics": {
            "detection_latency": "alarm.consume_ns - physical_fault_ns",
            "recovery_latency": "recovery.backup_activate_ns - alarm.consume_ns",
            "training_progress": "collective COMMIT timestamp_ns - alarm.consume_ns",
            "missing_evidence_policy": FAIL,
        },
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(args.out_md, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if status == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
