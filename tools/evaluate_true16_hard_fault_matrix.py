#!/usr/bin/env python3
"""Aggregate the P1 hard-disconnect matrix over all active Plane-B links.

The existing ``evaluate_true16_hard_fault_e2e.py`` remains the full recovery
audit.  P1 uses a faster 1 MiB profile and validates only simulator-emitted
carrier/alarm events, so it does not incorrectly demand periodic snapshots or
full recovery evidence from a run that finishes before the first 1 ms tick.
This tool adds one independently executed permanent disconnect for every
GPU's Plane-B ACCESS link, plus a separate long monitoring-on/off healthy run.

Missing targets are failures.  A cached target is accepted only when its raw
event evidence still matches the fixed GPU-side port-3 target and the
actionable alarm is below 1 ms.  Legacy full-E2E GPU0 evidence can be supplied
explicitly for regression with ``--evaluation GPU=PATH``.  The tool can also
validate only the shared healthy pair or one GPU; the matrix-wide default
still fails until all 16 targets pass.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import audit_true16_monitoring
import evaluate_true16_hard_fault_e2e as single_evaluator


PASS = "PASS"
FAIL = "FAIL"
SCHEMA_VERSION = "limer.p1-hard-fault-matrix.v1"
SINGLE_SCHEMA_VERSION = "limer.true16-hard-fault-e2e.v1"

REQUIRED_SINGLE_CHECKS = {
    "required_runtime_evidence",
    "csv_integrity",
    "dualrail_topology_contract",
    "permanent_hard_fault_contract",
    "runtime_physical_link_map",
    "healthy_true16_collective_participants",
    "healthy_all_rank_completion",
    "fault_true16_collective_participants",
    "fault_all_rank_completion",
    "hard_disconnect_both_endpoints_and_dead_port_quiescence",
    "online_executable_alarm_under_1ms",
    "route_and_backup_activation_under_1s",
    "preestablished_idle_backup_qp_and_live_failover",
    "rdma_work_completion_on_surviving_qp",
    "collective_redo_exactly_once_and_digest_safe",
    "training_collective_progress_under_1s",
}

REQUIRED_HEALTHY_EVIDENCE = {
    "link_map.csv",
    "switch_telemetry.csv",
    "nic_telemetry.csv",
    "rdma_wc_telemetry.csv",
    "collective_transaction.csv",
    "run.log",
    "exit_code.txt",
}

REQUIRED_FAULT_EVIDENCE = {
    "link_map.csv",
    "switch_telemetry.csv",
    "nic_telemetry.csv",
    "alarm_telemetry.csv",
    "recovery_telemetry.csv",
    "rdma_wc_telemetry.csv",
    "collective_transaction.csv",
    "run.log",
    "exit_code.txt",
}

DISABLED_TELEMETRY_FILES = {
    "switch_telemetry.csv",
    "nic_telemetry.csv",
    "collective_telemetry.csv",
    "collective_transaction.csv",
    "alarm_telemetry.csv",
    "recovery_telemetry.csv",
    "rdma_wc_telemetry.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--topology", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--healthy-workload", required=True, type=Path)
    parser.add_argument("--fault-workload", required=True, type=Path)
    parser.add_argument("--sim-config", required=True, type=Path)
    parser.add_argument("--simulator-binary", required=True, type=Path)
    parser.add_argument("--healthy-long-on-dir", type=Path)
    parser.add_argument("--healthy-long-off-dir", type=Path)
    parser.add_argument("--expected-gpus", type=int, default=16)
    parser.add_argument("--primary-host-port", type=int, default=3)
    parser.add_argument("--sample-interval-ns", type=int, default=1_000_000)
    parser.add_argument("--minimum-healthy-span-ns", type=int, default=100_000_000)
    parser.add_argument("--hard-detection-slo-ns", type=int, default=1_000_000)
    parser.add_argument("--recovery-slo-ns", type=int, default=1_000_000_000)
    parser.add_argument(
        "--evaluation",
        action="append",
        default=[],
        metavar="GPU=PATH",
        help="override a conventional gpu_XX/evaluation.json path",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--healthy-only", action="store_true")
    mode.add_argument("--check-gpu", type=int)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_evidence(path: Path) -> Dict[str, Any]:
    exists = path.is_file()
    return {
        "path": str(path.resolve()),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "sha256": sha256(path) if exists else None,
    }


def load_json(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not path.is_file():
        return None, "missing file"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return None, str(error)
    if not isinstance(value, dict):
        return None, "top-level JSON value is not an object"
    return value, None


def load_csv(path: Path) -> Tuple[List[Dict[str, str]], Optional[str]]:
    if not path.is_file():
        return [], "missing file"
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                return [], "missing CSV header"
            if len(reader.fieldnames) != len(set(reader.fieldnames)):
                return [], "duplicate CSV columns"
            return list(reader), None
    except (OSError, UnicodeError, csv.Error) as error:
        return [], str(error)


def add_check(
    checks: List[Dict[str, Any]], name: str, passed: bool, detail: str
) -> None:
    checks.append({
        "check": name,
        "status": PASS if passed else FAIL,
        "detail": detail,
    })


def finish_tick(path: Path) -> Tuple[Optional[int], Optional[str]]:
    if not path.is_file():
        return None, "run.log missing"
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = [int(value) for value in re.findall(
        r"all passes finished at time:\s*([0-9]+)", text
    )]
    if not matches:
        return None, "no all-passes completion marker"
    if len(set(matches)) != 1:
        return None, f"conflicting completion ticks={matches}"
    return matches[-1], None


def basic_run_log_audit(path: Path, expected_ranks: int) -> Dict[str, Any]:
    """Audit a telemetry-disabled run without requiring LIMER commit events."""
    if not path.is_file():
        return {"passed": False, "detail": "run.log missing"}
    raw = path.read_text(encoding="utf-8", errors="replace")
    lower = raw.lower()
    fatal_markers = [
        marker for marker in (
            "segmentation fault", "double free", "aborted", "assert failed",
            "terminate called", "timeout: the monitored command dumped core",
        ) if marker in lower
    ]
    model = bool(re.search(
        rf"model_parallel_NPU_group is\s+{expected_ranks}(?:\D|$)", raw
    ))
    ring = bool(re.search(
        rf"dimension:\s*local total nodes in ring:\s*{expected_ranks}(?:\D|$)", raw
    ))
    completed = (
        "Percentage of finished streams: 100" in raw
        and "all passes finished at time:" in raw
    )
    exit_code: Optional[int] = None
    exit_path = path.parent / "exit_code.txt"
    if exit_path.is_file():
        try:
            exit_code = int(exit_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pass
    passed = bool(model and ring and completed and exit_code == 0 and not fatal_markers)
    return {
        "passed": passed,
        "detail": (
            f"model16={model}, ring16={ring}, completed={completed}, "
            f"exit_code={exit_code}, fatal={fatal_markers}"
        ),
    }


def validate_healthy_pair(
    topology: Path,
    monitoring_on: Path,
    monitoring_off: Path,
    sample_interval_ns: int,
    minimum_span_ns: int,
) -> Dict[str, Any]:
    """Validate healthy telemetry once before any cached fault result is used."""
    checks: List[Dict[str, Any]] = []
    try:
        monitoring_audit = audit_true16_monitoring.build_audit(
            str(topology), [str(monitoring_on)], sample_interval_ns
        )
        monitoring_ok = monitoring_audit.get("status") == PASS
        add_check(
            checks,
            "monitoring_on_strict_platform_audit",
            monitoring_ok,
            f"status={monitoring_audit.get('status')}, "
            f"summary={monitoring_audit.get('summary')}",
        )
    except Exception as error:  # converted to stage evidence, never a silent reuse
        monitoring_audit = {"status": FAIL, "error": str(error)}
        add_check(
            checks, "monitoring_on_strict_platform_audit", False, str(error)
        )

    run_stats = {}
    audit_runs = monitoring_audit.get("runs", [])
    if isinstance(audit_runs, list) and len(audit_runs) == 1:
        candidate = audit_runs[0]
        if isinstance(candidate, dict) and isinstance(candidate.get("telemetry"), dict):
            run_stats = candidate["telemetry"]
    sample_count = run_stats.get("sample_count")
    first_sample_ns = run_stats.get("first_sample_ns")
    last_sample_ns = run_stats.get("last_sample_ns")
    virtual_span_ns = (
        last_sample_ns - first_sample_ns
        if isinstance(first_sample_ns, int) and isinstance(last_sample_ns, int)
        else None
    )
    minimum_sample_count = minimum_span_ns // sample_interval_ns + 1
    long_enough = bool(
        isinstance(sample_count, int)
        and sample_count >= minimum_sample_count
        and virtual_span_ns is not None
        and virtual_span_ns >= minimum_span_ns
    )
    add_check(
        checks,
        "long_healthy_snapshot_span",
        long_enough,
        f"sample_count={sample_count}, minimum_sample_count={minimum_sample_count}, "
        f"first_sample_ns={first_sample_ns}, last_sample_ns={last_sample_ns}, "
        f"virtual_span_ns={virtual_span_ns}, minimum_span_ns={minimum_span_ns}",
    )

    on_log = single_evaluator.run_log_audit(
        monitoring_on / "run.log", expected_ranks=16
    )
    # LIMER_TELEMETRY_ENABLE=0 deliberately disables collective transaction
    # instrumentation and its LIMER-COMMIT marker.  The off run must still
    # prove the native 16-rank ring and complete successfully.
    off_log = basic_run_log_audit(
        monitoring_off / "run.log", expected_ranks=16
    )
    add_check(checks, "monitoring_on_all_rank_completion", on_log["passed"], on_log["detail"])
    add_check(checks, "monitoring_off_all_rank_completion", off_log["passed"], off_log["detail"])

    stale_disabled = sorted(
        name for name in DISABLED_TELEMETRY_FILES
        if (monitoring_off / name).exists()
    )
    add_check(
        checks,
        "monitoring_off_has_no_limer_telemetry",
        not stale_disabled,
        f"unexpected_files={stale_disabled}",
    )

    on_tick, on_error = finish_tick(monitoring_on / "run.log")
    off_tick, off_error = finish_tick(monitoring_off / "run.log")
    parity = bool(
        on_error is None and off_error is None
        and on_tick is not None and on_tick == off_tick
    )
    add_check(
        checks,
        "monitoring_on_off_virtual_completion_parity",
        parity,
        f"monitoring_on_tick_ns={on_tick}, monitoring_off_tick_ns={off_tick}, "
        f"on_error={on_error}, off_error={off_error}",
    )

    status = PASS if all(item["status"] == PASS for item in checks) else FAIL
    return {
        "status": status,
        "monitoring_on_dir": str(monitoring_on.resolve()),
        "monitoring_off_dir": str(monitoring_off.resolve()),
        "monitoring_on_finish_ns": on_tick,
        "monitoring_off_finish_ns": off_tick,
        "exact_virtual_completion": parity,
        "sample_count": sample_count,
        "first_sample_ns": first_sample_ns,
        "last_sample_ns": last_sample_ns,
        "virtual_span_ns": virtual_span_ns,
        "minimum_virtual_span_ns": minimum_span_ns,
        "monitoring_audit": monitoring_audit,
        "checks": checks,
    }


def expected_primary_links(
    link_map_path: Path, expected_gpus: int, primary_host_port: int
) -> Tuple[Dict[int, str], List[str]]:
    rows, error = load_csv(link_map_path)
    if error:
        return {}, [f"link_map: {error}"]
    required = {
        "link_id", "src_node", "dst_node", "src_type", "dst_type",
        "src_port", "dst_port", "link_class",
    }
    fields = set(rows[0]) if rows else set()
    if not required <= fields:
        return {}, [f"link_map missing columns={sorted(required - fields)}"]

    access_by_gpu: Dict[int, List[Tuple[int, str]]] = {
        gpu: [] for gpu in range(expected_gpus)
    }
    errors: List[str] = []
    for row in rows:
        if row.get("link_class") != "ACCESS":
            continue
        try:
            if row.get("src_type") == "HOST":
                gpu = int(row["src_node"])
                host_port = int(row["src_port"])
            elif row.get("dst_type") == "HOST":
                gpu = int(row["dst_node"])
                host_port = int(row["dst_port"])
            else:
                errors.append(f"ACCESS link without HOST endpoint: {row.get('link_id')}")
                continue
        except (KeyError, ValueError):
            errors.append(f"invalid ACCESS endpoint: {row}")
            continue
        if gpu in access_by_gpu:
            access_by_gpu[gpu].append((host_port, row["link_id"]))

    selected: Dict[int, str] = {}
    for gpu in range(expected_gpus):
        attachments = access_by_gpu[gpu]
        ports = {port for port, _ in attachments}
        matching = [link_id for port, link_id in attachments if port == primary_host_port]
        if len(attachments) != 2 or ports != {2, 3}:
            errors.append(
                f"GPU {gpu}: expected ACCESS host ports [2,3], observed={attachments}"
            )
        if len(matching) != 1:
            errors.append(
                f"GPU {gpu}: expected one primary host-port-{primary_host_port} "
                f"link, observed={matching}"
            )
        else:
            selected[gpu] = matching[0]
    if len(set(selected.values())) != len(selected):
        errors.append("primary target link ids are not unique")
    return selected, errors


def _verified_evidence(
    item: Any,
    digest_cache: MutableMapping[Path, str],
) -> Tuple[bool, str, Optional[Path]]:
    if not isinstance(item, dict) or not item.get("exists"):
        return False, "evidence entry absent or marked missing", None
    raw_path = item.get("path")
    expected_hash = item.get("sha256")
    if not raw_path or not expected_hash:
        return False, "evidence entry lacks path or sha256", None
    path = Path(raw_path)
    if not path.is_file():
        return False, f"evidence file missing now: {path}", path
    resolved = path.resolve()
    current_hash = digest_cache.get(resolved)
    if current_hash is None:
        current_hash = sha256(resolved)
        digest_cache[resolved] = current_hash
    if current_hash != expected_hash:
        return False, f"evidence hash changed: {path}", resolved
    return True, "hash verified", resolved


def _read_alarm_latency(path: Path, link_id: str) -> Tuple[Optional[int], str]:
    rows, error = load_csv(path)
    if error:
        return None, error
    matching = [row for row in rows if row.get("link_id") == link_id]
    if len(matching) != 1:
        return None, f"expected one target alarm, observed={len(matching)}"
    try:
        physical = int(matching[0]["physical_fault_ns"])
        consume = int(matching[0]["consume_ns"])
    except (KeyError, ValueError) as error_value:
        return None, f"invalid alarm timestamp: {error_value}"
    if consume < physical:
        return None, "alarm consume precedes physical fault"
    return consume - physical, "runtime alarm timestamps"


def _read_recovery_latency(path: Path, link_id: str) -> Tuple[Optional[int], str]:
    rows, error = load_csv(path)
    if error:
        return None, error
    matching = [row for row in rows if row.get("link_id") == link_id]
    if len(matching) != 1:
        return None, f"expected one target recovery row, observed={len(matching)}"
    try:
        consume = int(matching[0]["alarm_consume_ns"])
        activate = int(matching[0]["backup_activate_ns"])
    except (KeyError, ValueError) as error_value:
        return None, f"invalid recovery timestamp: {error_value}"
    if activate < consume:
        return None, "backup activation precedes alarm consume"
    return activate - consume, "runtime recovery timestamps"


def validate_single_evaluation(
    evaluation_path: Path,
    gpu: int,
    expected_link_id: Optional[str],
    healthy_reference_dir: Optional[Path],
    hard_detection_slo_ns: int,
    recovery_slo_ns: int,
    digest_cache: Optional[MutableMapping[Path, str]] = None,
) -> Dict[str, Any]:
    reasons: List[str] = []
    digest_cache = digest_cache if digest_cache is not None else {}
    report, error = load_json(evaluation_path)
    if error or report is None:
        return {
            "gpu": gpu,
            "expected_link_id": expected_link_id,
            "evaluation_path": str(evaluation_path.resolve()),
            "present": evaluation_path.is_file(),
            "status": FAIL,
            "reasons": [error or "invalid evaluation"],
            "hard_detection_latency_ns": None,
            "recovery_activation_latency_ns": None,
        }

    if report.get("schema_version") != SINGLE_SCHEMA_VERSION:
        reasons.append(f"unexpected schema_version={report.get('schema_version')}")
    if report.get("status") != PASS:
        reasons.append(f"per-run status={report.get('status')}")

    checks = report.get("checks")
    check_by_name = {
        item.get("check"): item
        for item in checks if isinstance(item, dict)
    } if isinstance(checks, list) else {}
    missing_checks = sorted(REQUIRED_SINGLE_CHECKS - set(check_by_name))
    failed_checks = sorted(
        name for name in REQUIRED_SINGLE_CHECKS
        if check_by_name.get(name, {}).get("status") != PASS
    )
    if missing_checks:
        reasons.append(f"missing required checks={missing_checks}")
    if failed_checks:
        reasons.append(f"failed required checks={failed_checks}")

    fault = report.get("fault") if isinstance(report.get("fault"), dict) else {}
    observed_link = fault.get("target_link_id")
    if observed_link != expected_link_id:
        reasons.append(
            f"target mismatch expected={expected_link_id}, observed={observed_link}"
        )
    if fault.get("fault_type") != "hard_disconnect" or not fault.get("permanent"):
        reasons.append(
            f"fault is not a permanent hard_disconnect: {fault}"
        )
    expected_fault_id = f"true16_gpu{gpu}_hard_disconnect"
    if fault.get("fault_id") != expected_fault_id:
        reasons.append(
            f"fault_id mismatch expected={expected_fault_id}, "
            f"observed={fault.get('fault_id')}"
        )

    thresholds = report.get("thresholds_ns") if isinstance(
        report.get("thresholds_ns"), dict
    ) else {}
    if thresholds.get("hard_detection") != hard_detection_slo_ns:
        reasons.append(
            f"hard threshold mismatch={thresholds.get('hard_detection')}"
        )
    if thresholds.get("recovery_and_progress") != recovery_slo_ns:
        reasons.append(
            f"recovery threshold mismatch={thresholds.get('recovery_and_progress')}"
        )

    files = report.get("files") if isinstance(report.get("files"), dict) else {}
    resolved_files: Dict[str, Dict[str, Path]] = {"healthy": {}, "fault": {}}
    for scope, required in (
        ("healthy", REQUIRED_HEALTHY_EVIDENCE),
        ("fault", REQUIRED_FAULT_EVIDENCE),
    ):
        scope_items = files.get(scope) if isinstance(files.get(scope), dict) else {}
        for name in sorted(required):
            ok, detail, resolved = _verified_evidence(
                scope_items.get(name), digest_cache
            )
            if not ok:
                reasons.append(f"{scope}/{name}: {detail}")
            elif resolved is not None:
                resolved_files[scope][name] = resolved

    healthy_link_map = resolved_files["healthy"].get("link_map.csv")
    expected_healthy_map = (
        (healthy_reference_dir / "link_map.csv").resolve()
        if healthy_reference_dir is not None else None
    )
    if (
        healthy_link_map is not None and expected_healthy_map is not None
        and healthy_link_map != expected_healthy_map
    ):
        reasons.append(
            f"evaluation used different healthy link_map: {healthy_link_map}"
        )

    selection_item = report.get("fault_selection")
    selection_ok, selection_detail, selection_path = _verified_evidence(
        selection_item, digest_cache
    )
    if not selection_ok:
        reasons.append(f"fault_selection: {selection_detail}")
    elif selection_path is not None:
        selection, selection_error = load_json(selection_path)
        if selection_error or selection is None:
            reasons.append(f"fault_selection parse: {selection_error}")
        else:
            if selection.get("status") != PASS:
                reasons.append(f"fault_selection status={selection.get('status')}")
            if selection.get("gpu") != gpu:
                reasons.append(f"fault_selection gpu={selection.get('gpu')}")
            if selection.get("selected_host_port") != 3:
                reasons.append(
                    f"fault_selection host_port={selection.get('selected_host_port')}"
                )
            if selection.get("selected_link_id") != expected_link_id:
                reasons.append(
                    "fault_selection selected_link_id="
                    f"{selection.get('selected_link_id')}"
                )

    fault_events_ok, fault_events_detail, _ = _verified_evidence(
        report.get("fault_events"), digest_cache
    )
    if not fault_events_ok:
        reasons.append(f"fault_events: {fault_events_detail}")

    hard_latency: Optional[int] = None
    alarm_path = resolved_files["fault"].get("alarm_telemetry.csv")
    if alarm_path is not None and expected_link_id is not None:
        hard_latency, latency_detail = _read_alarm_latency(alarm_path, expected_link_id)
        if hard_latency is None or hard_latency >= hard_detection_slo_ns:
            reasons.append(
                f"hard alarm latency={hard_latency}, detail={latency_detail}, "
                f"strict_lt={hard_detection_slo_ns}"
            )

    recovery_latency: Optional[int] = None
    recovery_path = resolved_files["fault"].get("recovery_telemetry.csv")
    if recovery_path is not None and expected_link_id is not None:
        recovery_latency, recovery_detail = _read_recovery_latency(
            recovery_path, expected_link_id
        )
        if recovery_latency is None or recovery_latency >= recovery_slo_ns:
            reasons.append(
                f"recovery activation latency={recovery_latency}, "
                f"detail={recovery_detail}, strict_lt={recovery_slo_ns}"
            )

    return {
        "gpu": gpu,
        "expected_link_id": expected_link_id,
        "observed_link_id": observed_link,
        "evaluation_path": str(evaluation_path.resolve()),
        "evaluation_sha256": sha256(evaluation_path),
        "present": True,
        "status": PASS if not reasons else FAIL,
        "reasons": reasons,
        "hard_detection_latency_ns": hard_latency,
        "recovery_activation_latency_ns": recovery_latency,
        "profile": "legacy_full_e2e",
    }


def validate_fast_target(
    gpu_dir: Path,
    gpu: int,
    expected_link_id: Optional[str],
    hard_detection_slo_ns: int,
) -> Dict[str, Any]:
    """Validate the sub-ms event path without requiring a 1 ms snapshot."""
    reasons: List[str] = []
    fault_dir = gpu_dir / "hard_disconnect"
    selection_path = gpu_dir / "fault_selection.json"
    schedule_path = gpu_dir / "fault_events.csv"
    required = {
        "fault_selection.json": selection_path,
        "fault_events.csv": schedule_path,
        "link_map.csv": fault_dir / "link_map.csv",
        "alarm_telemetry.csv": fault_dir / "alarm_telemetry.csv",
        "collective_transaction.csv": fault_dir / "collective_transaction.csv",
        "run.log": fault_dir / "run.log",
        "exit_code.txt": fault_dir / "exit_code.txt",
    }
    missing = sorted(name for name, path in required.items() if not path.is_file())
    if missing:
        return {
            "gpu": gpu,
            "expected_link_id": expected_link_id,
            "observed_link_id": None,
            "evaluation_path": str(gpu_dir.resolve()),
            "present": False,
            "status": FAIL,
            "reasons": [f"missing fast-event evidence={missing}"],
            "hard_detection_latency_ns": None,
            "recovery_activation_latency_ns": None,
            "profile": "fast_event",
        }

    selection, selection_error = load_json(selection_path)
    if selection_error or selection is None:
        reasons.append(f"fault_selection parse: {selection_error}")
    else:
        if selection.get("status") != PASS:
            reasons.append(f"fault_selection status={selection.get('status')}")
        if selection.get("gpu") != gpu:
            reasons.append(f"fault_selection gpu={selection.get('gpu')}")
        if selection.get("selected_host_port") != 3:
            reasons.append(
                f"fault_selection host_port={selection.get('selected_host_port')}"
            )
        if selection.get("selected_link_id") != expected_link_id:
            reasons.append(
                f"fault_selection link={selection.get('selected_link_id')}"
            )
        inputs = selection.get("inputs") if isinstance(selection.get("inputs"), dict) else {}
        for name in ("link_map", "healthy_nic"):
            item = inputs.get(name) if isinstance(inputs.get(name), dict) else {}
            path_value, expected_hash = item.get("path"), item.get("sha256")
            if not path_value or not expected_hash or not Path(path_value).is_file():
                reasons.append(f"fault_selection {name} provenance missing")
            elif sha256(Path(path_value)) != expected_hash:
                reasons.append(f"fault_selection {name} provenance changed")

    schedule_rows, schedule_error = load_csv(schedule_path)
    schedule = schedule_rows[0] if len(schedule_rows) == 1 else {}
    observed_link = schedule.get("target_link_id")
    try:
        start_ns = int(schedule.get("start_time_ns", ""))
        end_ns = int(schedule.get("end_time_ns", ""))
    except ValueError:
        start_ns, end_ns = -1, -1
    expected_fault_id = f"true16_gpu{gpu}_hard_disconnect"
    if (
        schedule_error or len(schedule_rows) != 1
        or schedule.get("fault_id") != expected_fault_id
        or schedule.get("fault_type") != "hard_disconnect"
        or observed_link != expected_link_id
        or start_ns <= 0
        or end_ns != 0
        or schedule.get("parameter_after") != "physically_disconnected"
    ):
        reasons.append(
            f"invalid permanent hard schedule: error={schedule_error}, rows={schedule_rows}"
        )

    map_rows, map_error = load_csv(fault_dir / "link_map.csv")
    target_map = [row for row in map_rows if row.get("link_id") == expected_link_id]
    target_port = None
    if len(target_map) == 1:
        row = target_map[0]
        try:
            if row.get("src_type") == "HOST" and int(row["src_node"]) == gpu:
                target_port = int(row["src_port"])
            elif row.get("dst_type") == "HOST" and int(row["dst_node"]) == gpu:
                target_port = int(row["dst_port"])
        except (KeyError, ValueError):
            target_port = None
    if map_error or len(target_map) != 1 or target_port != 3:
        reasons.append(
            f"runtime link map does not place target on GPU-{gpu} port 3: "
            f"error={map_error}, rows={target_map}"
        )

    log_result = single_evaluator.run_log_audit(
        fault_dir / "run.log", expected_ranks=16
    )
    if not log_result["passed"]:
        reasons.append(f"fault run completion: {log_result['detail']}")

    alarm_rows, alarm_error = load_csv(fault_dir / "alarm_telemetry.csv")
    alarms = [row for row in alarm_rows if row.get("link_id") == expected_link_id]
    hard_latency: Optional[int] = None
    if alarm_error or len(alarms) != 1 or len(alarm_rows) != 1:
        reasons.append(
            "expected exactly one alarm and it must name the target: "
            f"error={alarm_error}, target_count={len(alarms)}, "
            f"total_count={len(alarm_rows)}"
        )
    else:
        alarm = alarms[0]
        try:
            physical = int(alarm["physical_fault_ns"])
            observe = int(alarm["observe_ns"])
            emit = int(alarm["emit_ns"])
            deliver = int(alarm["deliver_ns"])
            consume = int(alarm["consume_ns"])
        except (KeyError, ValueError) as error_value:
            reasons.append(f"invalid alarm timestamps: {error_value}")
        else:
            hard_latency = consume - physical
            ordered = physical <= observe <= emit <= deliver <= consume
            if (
                physical != start_ns or not ordered
                or hard_latency < 0 or hard_latency >= hard_detection_slo_ns
                or alarm.get("fault_id") != expected_fault_id
                or alarm.get("detector") != "switch_carrier"
                or alarm.get("fault_type") != "hard_disconnect"
                or alarm.get("status", "").lower() not in {"delivered", "consumed", "actionable"}
            ):
                reasons.append(
                    f"invalid actionable hard alarm: latency={hard_latency}, "
                    f"ordered={ordered}, row={alarm}"
                )

    return {
        "gpu": gpu,
        "expected_link_id": expected_link_id,
        "observed_link_id": observed_link,
        "evaluation_path": str(gpu_dir.resolve()),
        "evidence": {name: file_evidence(path) for name, path in required.items()},
        "present": True,
        "status": PASS if not reasons else FAIL,
        "reasons": reasons,
        "hard_detection_latency_ns": hard_latency,
        "recovery_activation_latency_ns": None,
        "profile": "fast_event",
    }


def parse_evaluation_overrides(values: Iterable[str]) -> Dict[int, Path]:
    overrides: Dict[int, Path] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"invalid --evaluation {value!r}; expected GPU=PATH")
        gpu_text, path_text = value.split("=", 1)
        gpu = int(gpu_text)
        if gpu in overrides:
            raise ValueError(f"duplicate evaluation override for GPU {gpu}")
        overrides[gpu] = Path(path_text)
    return overrides


def build_report(
    *,
    matrix_root: Path,
    topology: Path,
    contract: Path,
    healthy_workload: Path,
    fault_workload: Path,
    sim_config: Path,
    simulator_binary: Path,
    healthy_long_on_dir: Path,
    healthy_long_off_dir: Path,
    expected_gpus: int,
    primary_host_port: int,
    sample_interval_ns: int,
    minimum_healthy_span_ns: int,
    hard_detection_slo_ns: int,
    recovery_slo_ns: int,
    mode: str = "full",
    check_gpu: Optional[int] = None,
    evaluation_overrides: Optional[Mapping[int, Path]] = None,
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    input_paths = {
        "topology": topology,
        "contract": contract,
        "healthy_workload": healthy_workload,
        "fault_workload": fault_workload,
        "sim_config": sim_config,
        "simulator_binary": simulator_binary,
    }
    inputs = {name: file_evidence(path) for name, path in input_paths.items()}
    missing_inputs = sorted(name for name, item in inputs.items() if not item["exists"])
    add_check(
        checks, "immutable_inputs_present", not missing_inputs,
        f"missing={missing_inputs}",
    )

    healthy = validate_healthy_pair(
        topology,
        healthy_long_on_dir,
        healthy_long_off_dir,
        sample_interval_ns,
        minimum_healthy_span_ns,
    )
    add_check(
        checks, "validated_monitoring_on_off_healthy_pair",
        healthy["status"] == PASS,
        f"status={healthy['status']}, exact_parity={healthy.get('exact_virtual_completion')}",
    )

    primary_links, inventory_errors = expected_primary_links(
        healthy_long_on_dir / "link_map.csv", expected_gpus, primary_host_port
    )
    inventory_ok = not inventory_errors and len(primary_links) == expected_gpus
    add_check(
        checks, "exact_plane_b_primary_target_inventory", inventory_ok,
        f"expected={expected_gpus}, observed={len(primary_links)}, "
        f"errors={inventory_errors}",
    )

    rows: List[Dict[str, Any]] = []
    overrides = dict(evaluation_overrides or {})
    if mode == "check_gpu":
        if check_gpu is None or check_gpu < 0 or check_gpu >= expected_gpus:
            raise ValueError(f"--check-gpu must be in [0,{expected_gpus - 1}]")
        selected_gpus: Sequence[int] = [check_gpu]
    elif mode == "healthy_only":
        selected_gpus = []
    else:
        selected_gpus = list(range(expected_gpus))

    digest_cache: Dict[Path, str] = {}
    for gpu in selected_gpus:
        gpu_dir = matrix_root / f"gpu_{gpu:02d}"
        evaluation_path = overrides.get(gpu)
        if evaluation_path is not None:
            row = validate_single_evaluation(
                evaluation_path,
                gpu,
                primary_links.get(gpu),
                None,
                hard_detection_slo_ns,
                recovery_slo_ns,
                digest_cache,
            )
        else:
            row = validate_fast_target(
                gpu_dir,
                gpu,
                primary_links.get(gpu),
                hard_detection_slo_ns,
            )
        rows.append(row)

    if mode != "healthy_only":
        expected_count = 1 if mode == "check_gpu" else expected_gpus
        present = sum(row["present"] for row in rows)
        passed = sum(row["status"] == PASS for row in rows)
        add_check(
            checks, "all_required_target_evaluations_present",
            present == expected_count,
            f"expected={expected_count}, present={present}",
        )
        add_check(
            checks, "all_required_target_evaluations_pass",
            passed == expected_count,
            f"expected={expected_count}, passed={passed}",
        )
        observed_links = [
            row.get("observed_link_id") for row in rows
            if row.get("status") == PASS and row.get("observed_link_id")
        ]
        add_check(
            checks, "passing_target_links_are_unique",
            len(observed_links) == len(set(observed_links)) == expected_count,
            f"expected={expected_count}, observed={sorted(observed_links)}",
        )

    status = PASS if all(item["status"] == PASS for item in checks) else FAIL
    missing_gpus = [row["gpu"] for row in rows if not row["present"]]
    failed_gpus = [row["gpu"] for row in rows if row["present"] and row["status"] != PASS]
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": "limer-true16-dual-plane-v1",
        "mode": mode,
        "status": status,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": sum(item["status"] == FAIL for item in checks),
        },
        "thresholds_ns": {
            "sample_interval": sample_interval_ns,
            "minimum_healthy_span": minimum_healthy_span_ns,
            "hard_detection_strict_lt": hard_detection_slo_ns,
            "recovery_strict_lt": recovery_slo_ns,
        },
        "inputs": inputs,
        "healthy_parity": healthy,
        "primary_targets": {str(gpu): link for gpu, link in sorted(primary_links.items())},
        "coverage": {
            "expected_gpu_count": expected_gpus,
            "evaluated_gpu_count": len(rows),
            "present_evaluation_count": sum(row["present"] for row in rows),
            "passing_evaluation_count": sum(row["status"] == PASS for row in rows),
            "missing_gpus": missing_gpus,
            "failed_gpus": failed_gpus,
        },
        "target_matrix": rows,
        "checks": checks,
        "claim_boundary": (
            "PASS establishes P1 simulated hard-disconnect coverage and "
            "monitoring parity only; it is not real switch/RDMA/NCCL evidence."
        ),
    }


def markdown_escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def write_markdown(path: Path, report: Dict[str, Any]) -> None:
    coverage = report["coverage"]
    lines = [
        "# P1 true-16 Plane-B hard-disconnect matrix",
        "",
        f"**Overall: {report['status']}** — mode `{report['mode']}`; "
        f"{coverage['passing_evaluation_count']}/"
        f"{coverage['expected_gpu_count']} GPU targets pass.",
        "",
        "A full-stage PASS requires one independently audited permanent hard "
        "disconnect on each GPU's host-port-3 Plane-B ACCESS link and exact "
        "healthy monitoring-on/off virtual completion parity.",
        "",
        "## Stage checks",
        "",
        "| Check | Status | Detail |",
        "|---|---:|---|",
    ]
    for item in report["checks"]:
        lines.append(
            f"| `{markdown_escape(item['check'])}` | **{item['status']}** | "
            f"{markdown_escape(item['detail'])} |"
        )
    lines.extend([
        "",
        "## Per-GPU target matrix",
        "",
        "| GPU | Expected Plane-B link | Profile | Evidence | Hard alarm | Status | Reason |",
        "|---:|---|---|---|---:|---:|---|",
    ])
    for row in report["target_matrix"]:
        reasons = "; ".join(row.get("reasons", [])) or "verified"
        lines.append(
            f"| {row['gpu']} | `{row.get('expected_link_id')}` | "
            f"`{row.get('profile')}` | "
            f"`{markdown_escape(row['evaluation_path'])}` | "
            f"{row.get('hard_detection_latency_ns')} | "
            f"**{row['status']}** | {markdown_escape(reasons)} |"
        )
    lines.extend([
        "",
        "## Evidence boundary",
        "",
        report["claim_boundary"],
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    matrix_root = args.matrix_root.resolve()
    healthy_long_on = (
        args.healthy_long_on_dir.resolve()
        if args.healthy_long_on_dir
        else matrix_root / "healthy" / "long" / "monitoring_on"
    )
    healthy_long_off = (
        args.healthy_long_off_dir.resolve()
        if args.healthy_long_off_dir
        else matrix_root / "healthy" / "long" / "monitoring_off"
    )
    try:
        overrides = parse_evaluation_overrides(args.evaluation)
        mode = (
            "healthy_only" if args.healthy_only
            else "check_gpu" if args.check_gpu is not None
            else "full"
        )
        report = build_report(
            matrix_root=matrix_root,
            topology=args.topology.resolve(),
            contract=args.contract.resolve(),
            healthy_workload=args.healthy_workload.resolve(),
            fault_workload=args.fault_workload.resolve(),
            sim_config=args.sim_config.resolve(),
            simulator_binary=args.simulator_binary.resolve(),
            healthy_long_on_dir=healthy_long_on,
            healthy_long_off_dir=healthy_long_off,
            expected_gpus=args.expected_gpus,
            primary_host_port=args.primary_host_port,
            sample_interval_ns=args.sample_interval_ns,
            minimum_healthy_span_ns=args.minimum_healthy_span_ns,
            hard_detection_slo_ns=args.hard_detection_slo_ns,
            recovery_slo_ns=args.recovery_slo_ns,
            mode=mode,
            check_gpu=args.check_gpu,
            evaluation_overrides=overrides,
        )
    except Exception as error:
        report = {
            "schema_version": SCHEMA_VERSION,
            "contract_id": "limer-true16-dual-plane-v1",
            "mode": "error",
            "status": FAIL,
            "summary": {"pass": 0, "fail": 1},
            "coverage": {
                "expected_gpu_count": args.expected_gpus,
                "evaluated_gpu_count": 0,
                "present_evaluation_count": 0,
                "passing_evaluation_count": 0,
                "missing_gpus": list(range(args.expected_gpus)),
                "failed_gpus": [],
            },
            "target_matrix": [],
            "checks": [{"check": "evaluator_exception", "status": FAIL, "detail": str(error)}],
            "claim_boundary": "No claim: matrix evaluation failed before completion.",
        }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(args.out_md, report)
    print(json.dumps({
        "status": report["status"],
        "mode": report["mode"],
        "coverage": report["coverage"],
    }, sort_keys=True))
    return 0 if report["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
