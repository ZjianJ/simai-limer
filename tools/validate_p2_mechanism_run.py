#!/usr/bin/env python3
"""Derive P2 mechanism semantics from one completed simulator run.

This validator deliberately does not read a corpus manifest.  Its claims are
derived from the simulator's fault-application sidecar and the raw switch,
NIC, and collective telemetry in ``run_dir``.  The resulting JSON implements
the ``limer.p2-run-semantics.v1`` contract consumed by
``evaluate_stage_p2.py``.

The qualified mechanisms are the no-injection healthy control, real carrier
down/up transitions, dual-endpoint data-rate changes, true packet loss, and
carrier-up service degradation.  A validation failure still produces a JSON
report, but the process exits non-zero and no failed observation is promoted
to a PASS.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import (
    Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence,
    Tuple,
)


SCHEMA_VERSION = "limer.p2-run-semantics.v1"
PASS = "PASS"
FAIL = "FAIL"
MIN_SERVICE_SAMPLES_PER_WINDOW = 2
MATERIAL_RATIO = 0.95

RAW_FILES = {
    "fault_application": "fault_application_telemetry.csv",
    "switch": "switch_telemetry.csv",
    "nic": "nic_telemetry.csv",
    "collective": "collective_telemetry.csv",
}

COMMON_REQUIRED = {
    "fault_application": {
        "run_id", "fault_id", "fault_type", "target_link_id",
        "transition", "scheduled_ns", "actual_ns", "mechanism",
        "rng_stream", "status", "parameter_before", "parameter_after",
    },
    "switch": {
        "run_id", "timestamp_ns", "switch_id", "port_id", "link_id",
        "direction", "configured_bandwidth_bps", "link_state",
        "tx_bytes", "queue_bytes", "observed_throughput_bps",
        "dropped_packets", "link_errors", "recovered_packets",
        "flap_count", "last_link_down_ns", "last_link_up_ns",
        "cumulative_link_down_ns",
    },
    "nic": {
        "run_id", "timestamp_ns", "node_id", "nic_id", "link_id",
        "configured_bandwidth_bps", "link_state", "tx_bytes",
        "queue_bytes", "effective_throughput_bps", "rx_dropped_packets",
        "link_errors", "recovered_packets",
        "flap_count", "last_link_down_ns", "last_link_up_ns",
        "cumulative_link_down_ns",
    },
    "collective": {
        "run_id", "start_time_ns", "finish_time_ns", "duration_ns",
        "status",
    },
}
INJECTION_REQUIRED = {
    "fault_id", "fault_type", "target_link_id", "start_time_ns",
    "end_time_ns", "parameter_before", "parameter_after",
}

LOSS_FAMILIES = {"random_loss", "burst_loss"}
LOSS_MECHANISM_ID = "rate_error_model_true_drop"
LOSS_SIDECAR_MECHANISM = "rate_error_model_true_drop"
SERVICE_MECHANISM_IDS = {"egress_service_fraction", "carrier_up_service_rate"}
SERVICE_SIDECAR_MECHANISM = "carrier_up_service_rate"
HEALTHY_MECHANISM_ID = "no_physical_injection"
HARD_MECHANISM_ID = "physical_link_down"
FLAP_MECHANISM_ID = "physical_carrier_flap_channel_epoch"
CAPACITY_MECHANISM_IDS = {
    "dual_endpoint_data_rate", "dual_endpoint_data_rate_pulses",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _number(value: Any) -> Optional[float]:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> Optional[int]:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _read_csv(
    path: Path, required: Iterable[str], *, allow_empty: bool = False,
) -> Tuple[List[Dict[str, str]], List[str]]:
    errors: List[str] = []
    if not path.is_file():
        return [], [f"missing file: {path}"]
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            fieldnames = list(reader.fieldnames or [])
            fields = set(fieldnames)
            if len(fields) != len(fieldnames):
                errors.append(f"{path.name} has duplicate column names")
            missing = sorted(set(required) - fields)
            if missing:
                errors.append(f"{path.name} missing columns={missing}")
            rows = []
            for line_number, row in enumerate(reader, start=2):
                if None in row or any(value is None for value in row.values()):
                    errors.append(
                        f"{path.name}:{line_number} has malformed CSV width"
                    )
                rows.append(dict(row))
    except (OSError, csv.Error, UnicodeError) as error:
        return [], [f"{path.name} unreadable: {error}"]
    if not rows and not allow_empty:
        errors.append(f"{path.name} is empty")
    return rows, errors


def _timestamps(rows: Iterable[Mapping[str, Any]]) -> List[int]:
    return sorted({value for row in rows
                   if (value := _integer(row.get("timestamp_ns"))) is not None})


def _window(
    rows: Iterable[Mapping[str, Any]], start_ns: int, end_ns: Optional[int],
) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    before: List[Mapping[str, Any]] = []
    event: List[Mapping[str, Any]] = []
    for row in rows:
        timestamp = _integer(row.get("timestamp_ns"))
        if timestamp is None:
            continue
        if timestamp < start_ns:
            before.append(row)
        elif end_ns is None or timestamp < end_ns:
            event.append(row)
    return before, event


def _values(rows: Iterable[Mapping[str, Any]], column: str) -> List[float]:
    return [value for row in rows
            if (value := _number(row.get(column))) is not None]


def _median(rows: Iterable[Mapping[str, Any]], column: str) -> Optional[float]:
    values = _values(rows, column)
    return statistics.median(values) if values else None


def _counter_delta(
    before: Iterable[Mapping[str, Any]], event: Iterable[Mapping[str, Any]], column: str,
) -> Optional[float]:
    before_rows = list(before)
    event_rows = list(event)
    before_values = _values(before_rows, column)
    event_values = _values(event_rows, column)
    if (
        not before_values or not event_values
        or len(before_values) != len(before_rows)
        or len(event_values) != len(event_rows)
    ):
        return None
    return max(event_values) - max(before_values)


def _run_ids(rows: Iterable[Mapping[str, Any]]) -> set[str]:
    return {str(row.get("run_id", "")).strip() for row in rows}


def _check(
    checks: List[Dict[str, Any]], name: str, ok: bool, detail: Any,
) -> None:
    checks.append({
        "name": name,
        "status": PASS if ok else FAIL,
        "detail": detail,
    })


def _mechanism_mode(fault_family: str, mechanism_id: str) -> Tuple[str, str, set[str]]:
    if mechanism_id == HEALTHY_MECHANISM_ID and not fault_family:
        return "healthy", "", set()
    if mechanism_id == HARD_MECHANISM_ID and fault_family == "hard_disconnect":
        return "hard_down", "physical_link_down", {
            "hard_disconnect", "hard_link_failure",
        }
    if mechanism_id == FLAP_MECHANISM_ID and fault_family == "carrier_flap":
        return "carrier_flap", "physical_carrier_down_channel_epoch", {
            "carrier_flap",
        }
    if mechanism_id in CAPACITY_MECHANISM_IDS and fault_family in {
        "bandwidth_degradation", "intermittent_service",
        "multiple_simultaneous_faults", "non_access_fault",
    }:
        return "capacity", "dual_endpoint_data_rate", {
            "bandwidth_degradation",
        }
    if (
        mechanism_id == LOSS_MECHANISM_ID
        and fault_family in LOSS_FAMILIES | {"intermittent_service"}
    ):
        return "loss", LOSS_SIDECAR_MECHANISM, set(LOSS_FAMILIES)
    if (
        mechanism_id in SERVICE_MECHANISM_IDS
        and fault_family in {"service_degradation", "intermittent_service"}
    ):
        return "service", SERVICE_SIDECAR_MECHANISM, {"service_degradation"}
    return "unsupported", "", set()


def _load_healthy_reference(
    healthy_dir: Optional[Path], target_link: str,
) -> Tuple[Dict[str, List[Dict[str, str]]], List[str], Dict[str, str]]:
    if healthy_dir is None:
        return {}, [], {}
    rows_by_source: Dict[str, List[Dict[str, str]]] = {}
    errors: List[str] = []
    hashes: Dict[str, str] = {}
    for source in ("switch", "nic", "collective"):
        path = healthy_dir / RAW_FILES[source]
        rows, source_errors = _read_csv(path, COMMON_REQUIRED[source])
        rows_by_source[source] = rows
        errors.extend(source_errors)
        if path.is_file():
            hashes[source] = _sha256(path)
    ids = {source: _run_ids(rows) for source, rows in rows_by_source.items()}
    if any(len(values) != 1 or "" in values for values in ids.values()):
        errors.append(f"healthy telemetry run-id cardinality invalid: {ids}")
    elif len({next(iter(values)) for values in ids.values()}) != 1:
        errors.append(f"healthy telemetry run IDs disagree: {ids}")
    for source in ("switch", "nic"):
        if rows_by_source.get(source) and not any(
                str(row.get("link_id")) == target_link
                for row in rows_by_source[source]):
            errors.append(f"healthy {source} telemetry lacks target {target_link}")
    return rows_by_source, errors, hashes


def _configured_bandwidth_evidence(
    switch_before: Sequence[Mapping[str, Any]],
    switch_event: Sequence[Mapping[str, Any]],
    nic_before: Sequence[Mapping[str, Any]],
    nic_event: Sequence[Mapping[str, Any]],
    healthy: Mapping[str, Sequence[Mapping[str, Any]]],
    target_link: str,
) -> Tuple[bool, Dict[str, Any]]:
    values_by_source: Dict[str, List[int]] = {}
    groups = {
        "switch_before": switch_before,
        "switch_event": switch_event,
        "nic_before": nic_before,
        "nic_event": nic_event,
    }
    if healthy:
        groups["healthy_switch"] = [
            row for row in healthy.get("switch", [])
            if str(row.get("link_id")) == target_link
        ]
        groups["healthy_nic"] = [
            row for row in healthy.get("nic", [])
            if str(row.get("link_id")) == target_link
        ]
    for name, rows in groups.items():
        values_by_source[name] = sorted({
            value for row in rows
            if (value := _integer(row.get("configured_bandwidth_bps"))) is not None
        })
    valid = all(len(values) == 1 and values[0] > 0
                for values in values_by_source.values())
    distinct = {values[0] for values in values_by_source.values() if len(values) == 1}
    valid = valid and len(distinct) == 1
    return valid, {
        "configured_bandwidth_bps_by_window": values_by_source,
        "single_immutable_nominal_bps": next(iter(distinct)) if len(distinct) == 1 else None,
    }


def _endpoint_counter_evidence(
    before: Sequence[Mapping[str, Any]],
    event: Sequence[Mapping[str, Any]],
    key_columns: Sequence[str],
    drop_column: str,
) -> Tuple[bool, List[Dict[str, Any]]]:
    before_by_key: MutableMapping[Tuple[str, ...], List[Mapping[str, Any]]] = defaultdict(list)
    event_by_key: MutableMapping[Tuple[str, ...], List[Mapping[str, Any]]] = defaultdict(list)
    for row in before:
        before_by_key[tuple(str(row.get(column, "")) for column in key_columns)].append(row)
    for row in event:
        event_by_key[tuple(str(row.get(column, "")) for column in key_columns)].append(row)
    keys = sorted(set(before_by_key) | set(event_by_key))
    evidence: List[Dict[str, Any]] = []
    for key in keys:
        error_delta = _counter_delta(
            before_by_key.get(key, []), event_by_key.get(key, []), "link_errors")
        drop_delta = _counter_delta(
            before_by_key.get(key, []), event_by_key.get(key, []), drop_column)
        evidence.append({
            "endpoint": dict(zip(key_columns, key)),
            "link_errors_delta": error_delta,
            "rx_drops_delta": drop_delta,
            "pass": error_delta is not None and error_delta > 0
                    and drop_delta is not None and drop_delta > 0,
        })
    return len(keys) == 1 and all(item["pass"] for item in evidence), evidence


def _event_tx_growth(rows: Sequence[Mapping[str, Any]]) -> bool:
    by_timestamp: MutableMapping[int, float] = defaultdict(float)
    for row in rows:
        timestamp = _integer(row.get("timestamp_ns"))
        value = _number(row.get("tx_bytes"))
        if timestamp is not None and value is not None:
            by_timestamp[timestamp] += value
    ordered = [by_timestamp[key] for key in sorted(by_timestamp)]
    return len(ordered) >= 2 and ordered[-1] > ordered[0]


def _healthy_event_rows(
    rows: Sequence[Mapping[str, Any]], start_ns: int, end_ns: Optional[int],
) -> List[Mapping[str, Any]]:
    output = []
    for row in rows:
        timestamp = _integer(row.get("timestamp_ns"))
        if timestamp is None or timestamp < start_ns:
            continue
        if end_ns is None or timestamp < end_ns:
            output.append(row)
    return output


def _collective_windows(
    rows: Sequence[Mapping[str, Any]], start_ns: int, end_ns: Optional[int],
) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    before: List[Mapping[str, Any]] = []
    event: List[Mapping[str, Any]] = []
    for row in rows:
        start = _integer(row.get("start_time_ns"))
        finish = _integer(row.get("finish_time_ns"))
        duration = _number(row.get("duration_ns"))
        if None in (start, finish) or duration is None:
            continue
        if finish <= start_ns:
            before.append(row)
        elif start >= start_ns and (end_ns is None or start < end_ns):
            event.append(row)
    return before, event


def _service_effects(
    switch_before: Sequence[Mapping[str, Any]],
    switch_event: Sequence[Mapping[str, Any]],
    nic_before: Sequence[Mapping[str, Any]],
    nic_event: Sequence[Mapping[str, Any]],
    collective: Sequence[Mapping[str, Any]],
    start_ns: int,
    end_ns: Optional[int],
    healthy: Mapping[str, Sequence[Mapping[str, Any]]],
    target_link: str,
) -> Tuple[List[str], Dict[str, Any]]:
    switch_before_tx = [row for row in switch_before
                        if str(row.get("direction", "")).lower() == "tx"]
    switch_event_tx = [row for row in switch_event
                       if str(row.get("direction", "")).lower() == "tx"]
    healthy_switch = [row for row in healthy.get("switch", [])
                      if str(row.get("link_id")) == target_link
                      and str(row.get("direction", "")).lower() == "tx"]
    healthy_nic = [row for row in healthy.get("nic", [])
                   if str(row.get("link_id")) == target_link]
    healthy_switch_event = _healthy_event_rows(
        healthy_switch, start_ns, end_ns) if healthy else []
    healthy_nic_event = _healthy_event_rows(
        healthy_nic, start_ns, end_ns) if healthy else []

    metrics: Dict[str, Any] = {}
    effects: set[str] = set()

    queue_sources = (
        ("switch", switch_before_tx, switch_event_tx, healthy_switch_event),
        ("nic", nic_before, nic_event, healthy_nic_event),
    )
    for source, before, event, reference in queue_sources:
        before_values = _values(before, "queue_bytes")
        event_values = _values(event, "queue_bytes")
        reference_values = _values(reference, "queue_bytes") if healthy else []
        enough = (
            len(_timestamps(before)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
            and len(_timestamps(event)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
            and bool(before_values) and bool(event_values)
            and (not healthy or (
                len(_timestamps(reference)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
                and bool(reference_values)
            ))
        )
        baseline = max(before_values) if before_values else None
        if healthy and reference_values:
            baseline = max(baseline if baseline is not None else 0, max(reference_values))
        observed = bool(enough and baseline is not None and max(event_values) > baseline)
        metrics[f"{source}_queue"] = {
            "sufficient_samples": enough,
            "before_timestamps": len(_timestamps(before)),
            "event_timestamps": len(_timestamps(event)),
            "healthy_event_timestamps": len(_timestamps(reference)) if healthy else None,
            "baseline_max_queue_bytes": baseline,
            "event_max_queue_bytes": max(event_values) if event_values else None,
            "effect_observed": observed,
        }
        if observed:
            effects.add("queue_growth")

    throughput_sources = (
        ("switch", switch_before_tx, switch_event_tx, healthy_switch_event,
         "observed_throughput_bps"),
        ("nic", nic_before, nic_event, healthy_nic_event,
         "effective_throughput_bps"),
    )
    for source, before, event, reference, column in throughput_sources:
        before_median = _median(before, column)
        event_median = _median(event, column)
        reference_median = _median(reference, column) if healthy else None
        enough = (
            len(_timestamps(before)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
            and len(_timestamps(event)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
            and before_median is not None and before_median > 0
            and event_median is not None
            and (not healthy or (
                len(_timestamps(reference)) >= MIN_SERVICE_SAMPLES_PER_WINDOW
                and reference_median is not None and reference_median > 0
            ))
        )
        limits = [before_median * MATERIAL_RATIO] if before_median is not None else []
        if healthy and reference_median is not None:
            limits.append(reference_median * MATERIAL_RATIO)
        observed = bool(enough and limits and event_median < min(limits))
        metrics[f"{source}_throughput"] = {
            "sufficient_samples": enough,
            "before_median_bps": before_median,
            "event_median_bps": event_median,
            "healthy_event_median_bps": reference_median,
            "material_ratio_threshold": MATERIAL_RATIO,
            "effect_observed": observed,
        }
        if observed:
            effects.add("throughput_degradation")

    coll_before, coll_event = _collective_windows(collective, start_ns, end_ns)
    healthy_coll_event: List[Mapping[str, Any]] = []
    if healthy:
        _, healthy_coll_event = _collective_windows(
            list(healthy.get("collective", [])), start_ns, end_ns)
    before_duration = _median(coll_before, "duration_ns")
    event_duration = _median(coll_event, "duration_ns")
    healthy_duration = _median(healthy_coll_event, "duration_ns") if healthy else None
    latency_enough = (
        len(coll_before) >= MIN_SERVICE_SAMPLES_PER_WINDOW
        and len(coll_event) >= MIN_SERVICE_SAMPLES_PER_WINDOW
        and before_duration is not None and event_duration is not None
        and (not healthy or (
            len(healthy_coll_event) >= MIN_SERVICE_SAMPLES_PER_WINDOW
            and healthy_duration is not None
        ))
    )
    latency_limits = [before_duration / MATERIAL_RATIO] if before_duration is not None else []
    if healthy and healthy_duration is not None:
        latency_limits.append(healthy_duration / MATERIAL_RATIO)
    latency_observed = bool(
        latency_enough and latency_limits and event_duration > max(latency_limits)
    )
    metrics["collective_latency"] = {
        "sufficient_samples": latency_enough,
        "before_count": len(coll_before),
        "event_count": len(coll_event),
        "healthy_event_count": len(healthy_coll_event) if healthy else None,
        "before_median_duration_ns": before_duration,
        "event_median_duration_ns": event_duration,
        "healthy_event_median_duration_ns": healthy_duration,
        "material_ratio_threshold": MATERIAL_RATIO,
        "effect_observed": latency_observed,
    }
    if latency_observed:
        effects.add("latency_growth")

    event_activity = (
        _event_tx_growth(switch_event_tx) or _event_tx_growth(nic_event)
        or any(value > 0 for value in _values(switch_event_tx, "queue_bytes"))
        or any(value > 0 for value in _values(nic_event, "queue_bytes"))
    )
    metrics["event_activity"] = {
        "target_traffic_or_queue_present": event_activity,
    }
    if not event_activity:
        effects.clear()
    return sorted(effects), metrics


def _parse_rate_bps(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)\s*([kKmMgGtT]?)bps", text
    )
    if not match:
        return None
    scale = {
        "": 1, "k": 1_000, "m": 1_000_000,
        "g": 1_000_000_000, "t": 1_000_000_000_000,
    }[match.group(2).lower()]
    return int(float(match.group(1)) * scale)


def _sidecar_mechanism(fault_type: str) -> Optional[str]:
    return {
        "hard_disconnect": "physical_link_down",
        "hard_link_failure": "physical_link_down",
        "carrier_flap": "physical_carrier_down_channel_epoch",
        "bandwidth_degradation": "dual_endpoint_data_rate",
        "random_loss": "rate_error_model_true_drop",
        "burst_loss": "rate_error_model_true_drop",
        "service_degradation": "carrier_up_service_rate",
    }.get(fault_type)


def _validate_schedule_sidecar(
    sidecar_rows: Sequence[Mapping[str, Any]],
    schedule_rows: Sequence[Mapping[str, Any]],
    target_links: Sequence[str],
    scheduled_onset_ns: int,
) -> Tuple[bool, Dict[str, Any], List[Dict[str, Any]]]:
    """Bind every raw apply/revert transition to the frozen injector rows."""

    errors: List[str] = []
    schedule_by_id: Dict[str, Mapping[str, Any]] = {}
    for row in schedule_rows:
        fault_id = str(row.get("fault_id", ""))
        if not fault_id or fault_id in schedule_by_id:
            errors.append(f"empty/duplicate injector fault_id={fault_id!r}")
        schedule_by_id[fault_id] = row
    scheduled_targets = {
        str(row.get("target_link_id", "")) for row in schedule_rows
    }
    if scheduled_targets != set(target_links):
        errors.append(
            f"injector targets={sorted(scheduled_targets)}, "
            f"requested={sorted(set(target_links))}"
        )
    starts = [
        value for row in schedule_rows
        if (value := _integer(row.get("start_time_ns"))) is not None
    ]
    if not starts or min(starts) < scheduled_onset_ns:
        errors.append(
            "injector schedule starts before or lacks the parent onset: "
            f"parent={scheduled_onset_ns}, starts={sorted(starts)}"
        )

    sidecar_by_id: MutableMapping[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in sidecar_rows:
        sidecar_by_id[str(row.get("fault_id", ""))].append(row)
    if set(sidecar_by_id) != set(schedule_by_id):
        errors.append(
            f"sidecar fault IDs={sorted(sidecar_by_id)}, "
            f"injector fault IDs={sorted(schedule_by_id)}"
        )

    segments: List[Dict[str, Any]] = []
    for fault_id, scheduled in schedule_by_id.items():
        fault_type = str(scheduled.get("fault_type", ""))
        target = str(scheduled.get("target_link_id", ""))
        start = _integer(scheduled.get("start_time_ns"))
        end = _integer(scheduled.get("end_time_ns"))
        before = str(scheduled.get("parameter_before", ""))
        after = str(scheduled.get("parameter_after", ""))
        expected_mechanism = _sidecar_mechanism(fault_type)
        if start is None or end is None or end < start or expected_mechanism is None:
            errors.append(f"invalid injector row for {fault_id}")
            continue
        observed = sidecar_by_id.get(fault_id, [])
        applies = [row for row in observed if row.get("transition") == "apply"]
        reverts = [row for row in observed if row.get("transition") == "revert"]
        permanent = fault_type in {"hard_disconnect", "hard_link_failure"}
        if len(applies) != 1 or len(reverts) != (0 if permanent else 1):
            errors.append(
                f"{fault_id} transition cardinality apply={len(applies)}, "
                f"revert={len(reverts)}, permanent={permanent}"
            )
            continue
        apply = applies[0]
        apply_actual = _integer(apply.get("actual_ns"))
        apply_ok = (
            str(apply.get("fault_type", "")) == fault_type
            and str(apply.get("target_link_id", "")) == target
            and _integer(apply.get("scheduled_ns")) == start
            and apply_actual is not None and apply_actual >= start
            and str(apply.get("parameter_before", "")) == before
            and str(apply.get("parameter_after", "")) == after
            and str(apply.get("mechanism", "")) == expected_mechanism
            and str(apply.get("status", "")) == "APPLIED"
        )
        if not apply_ok:
            errors.append(f"{fault_id} apply sidecar differs from injector")
            continue
        revert_actual: Optional[int] = None
        if reverts:
            revert = reverts[0]
            revert_actual = _integer(revert.get("actual_ns"))
            expected_revert_mechanism = (
                "physical_carrier_up_no_fib_or_qp_change"
                if fault_type == "carrier_flap" else "restore_baseline"
            )
            revert_ok = (
                str(revert.get("fault_type", "")) == fault_type
                and str(revert.get("target_link_id", "")) == target
                and _integer(revert.get("scheduled_ns")) == end
                and revert_actual is not None and revert_actual >= end
                and str(revert.get("parameter_before", "")) == before
                and str(revert.get("parameter_after", "")) == after
                and str(revert.get("mechanism", ""))
                == expected_revert_mechanism
                and str(revert.get("status", "")) == "REVERTED"
            )
            if not revert_ok:
                errors.append(f"{fault_id} revert sidecar differs from injector")
                continue
        if fault_type in {"random_loss", "burst_loss"} and not re.fullmatch(
            r"[0-9]+:[0-9]+", str(apply.get("rng_stream", ""))
        ):
            errors.append(f"{fault_id} lacks the two endpoint RNG streams")
            continue
        segments.append({
            "fault_id": fault_id,
            "fault_type": fault_type,
            "target_link_id": target,
            "scheduled_start_ns": start,
            "scheduled_end_ns": end,
            "actual_apply_ns": apply_actual,
            "actual_revert_ns": revert_actual,
            "parameter_before": before,
            "parameter_after": after,
            "mechanism": expected_mechanism,
        })
    return not errors and len(segments) == len(schedule_rows), {
        "injector_row_count": len(schedule_rows),
        "sidecar_row_count": len(sidecar_rows),
        "target_link_ids": sorted(set(target_links)),
        "segments": segments,
        "errors": errors,
    }, segments


def _endpoint_numeric_set(
    rows: Sequence[Mapping[str, Any]], column: str,
) -> set[int]:
    return {
        value for row in rows
        if (value := _integer(row.get(column))) is not None
    }


def _all_nonnegative_integers(
    rows: Sequence[Mapping[str, Any]], column: str,
) -> bool:
    values = [_integer(row.get(column)) for row in rows]
    return bool(values) and all(value is not None and value >= 0 for value in values)


def _all_finite_numbers(
    rows: Sequence[Mapping[str, Any]], column: str,
) -> bool:
    values = [_number(row.get(column)) for row in rows]
    return bool(values) and all(value is not None for value in values)


def _physical_endpoint_rows(
    *, target_link: str,
    switch_rows: Sequence[Mapping[str, Any]],
    nic_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Mapping[str, Any]]]:
    """Return the two device endpoints represented by raw link telemetry.

    ACCESS links have one switch TX endpoint and one NIC endpoint.  An
    inter-switch OOD link has two switch TX endpoints and no NIC endpoint.
    Grouping by the device identity instead of assuming ACCESS is what makes
    dual-endpoint rate validation faithful for both cases.
    """

    grouped: MutableMapping[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in switch_rows:
        if (
            str(row.get("link_id", "")) == target_link
            and str(row.get("direction", "")).strip().lower() == "tx"
        ):
            key = f"switch:{row.get('switch_id', '')}:{row.get('port_id', '')}"
            grouped[key].append(row)
    for row in nic_rows:
        if str(row.get("link_id", "")) == target_link:
            key = f"nic:{row.get('node_id', '')}:{row.get('nic_id', '')}"
            grouped[key].append(row)
    return dict(grouped)


def _carrier_epoch_evidence(
    *, mode: str, target_links: Sequence[str],
    switch_rows: Sequence[Mapping[str, Any]],
    nic_rows: Sequence[Mapping[str, Any]],
    segments: Sequence[Mapping[str, Any]],
) -> Tuple[bool, Dict[str, Any], List[str]]:
    errors: List[str] = []
    per_link: Dict[str, Any] = {}
    for target in target_links:
        segment = next(
            (item for item in segments if item["target_link_id"] == target), None
        )
        if segment is None:
            errors.append(f"no carrier segment for {target}")
            continue
        apply_ns = int(segment["actual_apply_ns"])
        revert_ns = segment.get("actual_revert_ns")
        sources = _physical_endpoint_rows(
            target_link=target, switch_rows=switch_rows, nic_rows=nic_rows,
        )
        if len(sources) != 2:
            errors.append(
                f"{target} physical endpoint cardinality={len(sources)}, expected=2"
            )
        source_evidence: Dict[str, Any] = {}
        for source, rows in sources.items():
            before = [row for row in rows
                      if (_integer(row.get("timestamp_ns")) or 0) < apply_ns]
            after = [row for row in rows
                     if (_integer(row.get("timestamp_ns")) or 0)
                     > (int(revert_ns) if revert_ns is not None else apply_ns)]
            before_flaps = max(_endpoint_numeric_set(before, "flap_count"), default=0)
            after_flaps = _endpoint_numeric_set(after, "flap_count")
            down_epochs = _endpoint_numeric_set(after, "last_link_down_ns")
            states = {str(row.get("link_state", "")).lower() for row in after}
            source_ok = (
                bool(before) and bool(after) and bool(after_flaps)
                and min(after_flaps) > before_flaps and apply_ns in down_epochs
                and _all_nonnegative_integers(before, "flap_count")
                and _all_nonnegative_integers(after, "flap_count")
                and _all_nonnegative_integers(after, "last_link_down_ns")
            )
            if mode == "hard_down":
                source_ok = source_ok and states == {"down"}
            else:
                up_epochs = _endpoint_numeric_set(after, "last_link_up_ns")
                cumulative = _endpoint_numeric_set(after, "cumulative_link_down_ns")
                duration = int(revert_ns) - apply_ns
                source_ok = (
                    source_ok and states == {"up"}
                    and int(revert_ns) in up_epochs
                    and bool(cumulative) and max(cumulative) >= duration
                    and _all_nonnegative_integers(after, "last_link_up_ns")
                    and _all_nonnegative_integers(
                        after, "cumulative_link_down_ns"
                    )
                )
            if not source_ok:
                errors.append(f"{target} {source} carrier epoch is unproven")
            source_evidence[source] = {
                "before_sample_count": len(before),
                "after_sample_count": len(after),
                "before_max_flap_count": before_flaps,
                "after_flap_counts": sorted(after_flaps),
                "last_link_down_ns": sorted(down_epochs),
                "link_states": sorted(states),
            }
        per_link[target] = {
            "physical_endpoint_count": len(sources),
            "endpoints": source_evidence,
        }
    return not errors and len(per_link) == len(target_links), per_link, errors


def _capacity_evidence(
    *, target_links: Sequence[str],
    switch_rows: Sequence[Mapping[str, Any]],
    nic_rows: Sequence[Mapping[str, Any]],
    segments: Sequence[Mapping[str, Any]],
) -> Tuple[bool, Dict[str, Any], List[str]]:
    errors: List[str] = []
    per_segment: List[Dict[str, Any]] = []
    throughput_ratios: List[float] = []
    first_start_by_target = {
        target: min(
            int(item["actual_apply_ns"])
            for item in segments if item["target_link_id"] == target
        )
        for target in target_links
        if any(item["target_link_id"] == target for item in segments)
    }
    for segment in segments:
        target = str(segment["target_link_id"])
        if target not in target_links:
            continue
        start = int(segment["actual_apply_ns"])
        end = segment.get("actual_revert_ns")
        before_bps = _parse_rate_bps(segment["parameter_before"])
        event_bps = _parse_rate_bps(segment["parameter_after"])
        segment_ok = (
            before_bps is not None and event_bps is not None
            and 0 < event_bps < before_bps
        )
        source_evidence: Dict[str, Any] = {}
        endpoints = _physical_endpoint_rows(
            target_link=target, switch_rows=switch_rows, nic_rows=nic_rows,
        )
        if len(endpoints) != 2:
            segment_ok = False
            errors.append(
                f"capacity segment {segment['fault_id']} has "
                f"{len(endpoints)} physical endpoints, expected 2"
            )
        for endpoint, rows in endpoints.items():
            throughput_column = (
                "observed_throughput_bps"
                if endpoint.startswith("switch:")
                else "effective_throughput_bps"
            )
            before = [row for row in rows
                      if (_integer(row.get("timestamp_ns")) or 0)
                      < first_start_by_target[target]]
            event = [row for row in rows
                     if (_integer(row.get("timestamp_ns")) or 0) > start
                     and (end is None
                          or (_integer(row.get("timestamp_ns")) or 0) < int(end))]
            before_rates = _endpoint_numeric_set(
                before, "configured_bandwidth_bps"
            )
            event_rates = _endpoint_numeric_set(
                event, "configured_bandwidth_bps"
            )
            before_throughput = _median(before, throughput_column)
            event_throughput = _median(event, throughput_column)
            rate_ok = (
                bool(before) and bool(event)
                and before_rates == {before_bps}
                and event_rates == {event_bps}
                and _all_nonnegative_integers(
                    before, "configured_bandwidth_bps"
                )
                and _all_nonnegative_integers(
                    event, "configured_bandwidth_bps"
                )
                and _all_finite_numbers(before, throughput_column)
                and _all_finite_numbers(event, throughput_column)
            )
            segment_ok = segment_ok and rate_ok
            ratio = None
            if (
                before_throughput is not None and before_throughput > 0
                and event_throughput is not None
            ):
                ratio = event_throughput / before_throughput
                throughput_ratios.append(ratio)
            source_evidence[endpoint] = {
                "before_configured_bps": sorted(before_rates),
                "event_configured_bps": sorted(event_rates),
                "expected_before_bps": before_bps,
                "expected_event_bps": event_bps,
                "before_sample_count": len(_timestamps(before)),
                "event_sample_count": len(_timestamps(event)),
                "throughput_ratio": ratio,
                "rate_transition_exact": rate_ok,
            }
        if not segment_ok:
            errors.append(
                f"capacity segment {segment['fault_id']} lacks exact dual-endpoint rate"
            )
        per_segment.append({
            "fault_id": segment["fault_id"],
            "target_link_id": target,
            "physical_endpoint_count": len(endpoints),
            "endpoints": source_evidence,
            "exact": segment_ok,
        })
    throughput_effect = bool(throughput_ratios) and min(throughput_ratios) < MATERIAL_RATIO
    if not throughput_effect:
        errors.append("no material raw throughput degradation during capacity event")
    return not errors and bool(per_segment), {
        "segments": per_segment,
        "throughput_ratios": throughput_ratios,
        "material_ratio_threshold": MATERIAL_RATIO,
        "throughput_degradation": throughput_effect,
    }, errors


def validate_run(
    run_dir: Path,
    fault_family: str,
    mechanism_id: str,
    target_link: Optional[str],
    scheduled_onset_ns: int,
    healthy_dir: Optional[Path] = None,
    *,
    target_links: Optional[Sequence[str]] = None,
    injection_schedule: Optional[Path] = None,
    require_injection_schedule: bool = False,
) -> Dict[str, Any]:
    """Validate one executed run and return evaluator-compatible semantics."""

    run_dir = Path(run_dir)
    healthy_dir = Path(healthy_dir) if healthy_dir is not None else None
    checks: List[Dict[str, Any]] = []
    rows: Dict[str, List[Dict[str, str]]] = {}
    read_errors: List[str] = []
    source_hashes: Dict[str, str] = {}
    mode, expected_sidecar_mechanism, allowed_sidecar_types = _mechanism_mode(
        fault_family, mechanism_id)
    requested_targets = sorted({
        str(value) for value in (
            list(target_links or []) + ([target_link] if target_link else [])
        ) if str(value)
    })
    for source, filename in RAW_FILES.items():
        path = run_dir / filename
        source_rows, errors = _read_csv(
            path, COMMON_REQUIRED[source],
            allow_empty=(source == "fault_application" and mode == "healthy"),
        )
        rows[source] = source_rows
        read_errors.extend(errors)
        if path.is_file():
            source_hashes[source] = _sha256(path)
    schedule_rows: List[Dict[str, str]] = []
    if injection_schedule is not None:
        schedule_rows, schedule_errors = _read_csv(
            Path(injection_schedule), INJECTION_REQUIRED
        )
        read_errors.extend(schedule_errors)
        if Path(injection_schedule).is_file():
            source_hashes["injection_schedule"] = _sha256(
                Path(injection_schedule)
            )
    elif require_injection_schedule and mode != "healthy":
        read_errors.append("completed fault run lacks frozen injection schedule")
    _check(checks, "raw_telemetry_contract", not read_errors, read_errors or {
        source: len(source_rows) for source, source_rows in rows.items()
    })

    ids = {source: _run_ids(source_rows) for source, source_rows in rows.items()}
    singleton_ids = [next(iter(values)) for values in ids.values() if len(values) == 1]
    identity_ok = (
        all(
            len(values) == 1
            for source, values in ids.items()
            if source != "fault_application" or mode != "healthy"
        )
        and (mode != "healthy" or not ids["fault_application"])
        and len(set(singleton_ids)) == 1
        and all(singleton_ids)
    )
    run_id = singleton_ids[0] if singleton_ids and len(set(singleton_ids)) == 1 else "UNKNOWN"
    _check(checks, "raw_run_identity", identity_ok, {
        key: sorted(value) for key, value in ids.items()
    })

    mechanism_ok = mode != "unsupported"
    if mode == "loss":
        mechanism_ok = mechanism_ok and mechanism_id == LOSS_MECHANISM_ID
    elif mode == "service":
        mechanism_ok = mechanism_ok and mechanism_id in SERVICE_MECHANISM_IDS
    _check(checks, "supported_mechanism_identity", mechanism_ok, {
        "fault_family": fault_family,
        "mechanism_id": mechanism_id,
        "derived_mode": mode,
        "expected_sidecar_mechanism": expected_sidecar_mechanism,
    })

    schedule_ok = False
    schedule_evidence: Dict[str, Any] = {}
    segments: List[Dict[str, Any]] = []
    if mode == "healthy":
        schedule_ok = not rows.get("fault_application") and not schedule_rows
        schedule_evidence = {
            "fault_application_row_count": len(rows.get("fault_application", [])),
            "injector_row_count": len(schedule_rows),
        }
    elif schedule_rows:
        schedule_ok, schedule_evidence, segments = _validate_schedule_sidecar(
            rows.get("fault_application", []), schedule_rows,
            requested_targets, scheduled_onset_ns,
        )
    elif not require_injection_schedule:
        schedule_evidence = {"legacy_unbound_direct_validation": True}
    _check(
        checks,
        "frozen_injector_to_application_sidecar",
        schedule_ok if (mode == "healthy" or schedule_rows
                        or require_injection_schedule) else True,
        schedule_evidence,
    )

    if mode == "healthy":
        carrier_states = {
            str(row.get("link_state", "")).strip().lower()
            for source in ("switch", "nic") for row in rows.get(source, [])
        }
        collective_ok = bool(rows.get("collective"))
        for row in rows.get("collective", []):
            start = _integer(row.get("start_time_ns"))
            finish = _integer(row.get("finish_time_ns"))
            duration = _integer(row.get("duration_ns"))
            collective_ok = bool(
                collective_ok
                and str(row.get("status", "")) == "ok"
                and start is not None and finish is not None
                and duration is not None and duration >= 0
                and finish >= start and finish - start == duration
            )
        healthy_ok = carrier_states == {"up"} and collective_ok
        _check(checks, "healthy_no_fault_raw_semantics", healthy_ok, {
            "carrier_states": sorted(carrier_states),
            "collective_row_count": len(rows.get("collective", [])),
            "collectives_all_ok_and_causal": collective_ok,
        })
        status = (
            PASS if checks and all(item["status"] == PASS for item in checks)
            else FAIL
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "mechanism_id": mechanism_id,
            "status": status,
            "source_artifact_sha256": source_hashes.get("switch", ""),
            "checks": checks,
            "injected_physical_fault": False,
            "target_link_state_during_event": "up",
            "observed_effects": [],
            "impairment": "",
            "packet_disposition": "not_applicable",
            "recoverable_error_proxy": False,
            "fault_family": "",
            "target_link_id": None,
            "target_link_ids": [],
            "scheduled_onset_ns": None,
            "actual_apply_ns": None,
            "event_end_ns": None,
            "source_artifacts_sha256": source_hashes,
            "healthy_reference_used": False,
            "evidence": schedule_evidence,
        }

    target_application = [
        row for row in rows.get("fault_application", [])
        if str(row.get("target_link_id", "")) in requested_targets
        and str(row.get("transition", "")).lower() == "apply"
        and str(row.get("status", "")) == "APPLIED"
        and str(row.get("mechanism", "")) == expected_sidecar_mechanism
        and str(row.get("fault_type", "")) in allowed_sidecar_types
    ]
    applied_times = [value for row in target_application
                     if (value := _integer(row.get("actual_ns"))) is not None]
    scheduled_times = [value for row in target_application
                       if (value := _integer(row.get("scheduled_ns"))) is not None]
    first_apply_ns = min(applied_times) if applied_times else scheduled_onset_ns
    first_scheduled_ns = min(scheduled_times) if scheduled_times else None
    sidecar_ok = schedule_ok if segments else (
        bool(target_application)
        and first_scheduled_ns is not None
        and first_scheduled_ns >= scheduled_onset_ns
        and len(applied_times) == len(target_application)
        and all(actual >= scheduled for actual, scheduled in zip(
            sorted(applied_times), sorted(scheduled_times)))
    )
    rng_ok = True
    if mode == "loss":
        rng_ok = all(re.fullmatch(r"[0-9]+:[0-9]+", str(row.get("rng_stream", "")))
                     for row in target_application)
        sidecar_ok = sidecar_ok and rng_ok
    _check(checks, "fault_application_ground_truth", sidecar_ok, {
        "matching_apply_rows": len(target_application),
        "scheduled_times_ns": sorted(scheduled_times),
        "actual_times_ns": sorted(applied_times),
        "requested_scheduled_onset_ns": scheduled_onset_ns,
        "mechanisms": sorted({str(row.get("mechanism", ""))
                              for row in target_application}),
        "rng_streams": sorted({str(row.get("rng_stream", ""))
                               for row in target_application}),
        "rng_streams_valid": rng_ok,
    })

    application_fault_ids = {str(row.get("fault_id", "")) for row in target_application}
    revert_times = [
        value for row in rows.get("fault_application", [])
        if str(row.get("fault_id", "")) in application_fault_ids
        and str(row.get("target_link_id", "")) in requested_targets
        and str(row.get("transition", "")).lower() == "revert"
        and (value := _integer(row.get("actual_ns"))) is not None
    ]
    if segments:
        applied_times = [int(item["actual_apply_ns"]) for item in segments]
        scheduled_times = [int(item["scheduled_start_ns"]) for item in segments]
        revert_times = [
            int(item["actual_revert_ns"]) for item in segments
            if item.get("actual_revert_ns") is not None
        ]
        first_apply_ns = min(applied_times)
    event_end_ns = max(revert_times) if revert_times else None

    target_switch = [row for row in rows.get("switch", [])
                     if str(row.get("link_id", "")) in requested_targets]
    target_nic = [row for row in rows.get("nic", [])
                  if str(row.get("link_id", "")) in requested_targets]
    switch_before, switch_event = _window(target_switch, first_apply_ns, event_end_ns)
    nic_before, nic_event = _window(target_nic, first_apply_ns, event_end_ns)
    minimum = MIN_SERVICE_SAMPLES_PER_WINDOW if mode == "service" else 1
    sample_counts = {
        "switch_before": len(_timestamps(switch_before)),
        "switch_event": len(_timestamps(switch_event)),
        "nic_before": len(_timestamps(nic_before)),
        "nic_event": len(_timestamps(nic_event)),
    }
    if mode == "carrier_flap":
        post_rows = [
            row for row in target_switch + target_nic
            if (_integer(row.get("timestamp_ns")) or 0)
            >= int(event_end_ns or first_apply_ns)
        ]
        samples_ok = (
            len(_timestamps(switch_before + nic_before)) >= 1
            and len(_timestamps(post_rows)) >= 1
        )
    elif mode in {"hard_down", "capacity"}:
        samples_ok = (
            len(_timestamps(switch_before + nic_before)) >= 1
            and len(_timestamps(switch_event + nic_event)) >= 1
        )
    else:
        samples_ok = all(value >= minimum for value in sample_counts.values())
    _check(checks, "target_event_window_samples", samples_ok, {
        **sample_counts,
        "minimum_distinct_timestamps_per_window": minimum,
        "actual_apply_ns": first_apply_ns if applied_times else None,
        "event_end_ns": event_end_ns,
    })

    event_states = [str(row.get("link_state", "")).strip().lower()
                    for row in switch_event + nic_event]
    carrier_up = bool(event_states) and all(state == "up" for state in event_states)
    if mode in {"loss", "service", "capacity"}:
        _check(checks, "target_carrier_remained_up", carrier_up, {
            "observed_states": sorted(set(event_states)),
            "sample_count": len(event_states),
        })

    healthy_rows, healthy_errors, healthy_hashes = _load_healthy_reference(
        healthy_dir, requested_targets[0] if requested_targets else "")
    if healthy_dir is not None:
        _check(checks, "healthy_reference_contract", not healthy_errors,
               healthy_errors or {key: len(value) for key, value in healthy_rows.items()})
        source_hashes.update({f"healthy_{key}": value
                              for key, value in healthy_hashes.items()})

    if mode in {"loss", "service"}:
        bandwidth_ok, bandwidth_evidence = _configured_bandwidth_evidence(
            switch_before, switch_event, nic_before, nic_event,
            healthy_rows if not healthy_errors else {},
            requested_targets[0] if requested_targets else "",
        )
        _check(checks, "nominal_configured_bandwidth_immutable",
               bandwidth_ok, bandwidth_evidence)

    observed_effects: List[str] = []
    packet_disposition = "not_applicable"
    recoverable_error_proxy = False
    family_evidence: Dict[str, Any] = {}
    target_state = "up" if carrier_up else "unproven"
    if mode in {"hard_down", "carrier_flap"}:
        carrier_ok, carrier_evidence, carrier_errors = _carrier_epoch_evidence(
            mode=mode,
            target_links=requested_targets,
            switch_rows=rows.get("switch", []),
            nic_rows=rows.get("nic", []),
            segments=segments,
        )
        _check(checks, "physical_carrier_epoch_at_both_endpoints", carrier_ok, {
            "per_link": carrier_evidence,
            "errors": carrier_errors,
        })
        observed_effects = (
            ["carrier_down"] if mode == "hard_down"
            else ["carrier_down", "carrier_up"]
        ) if carrier_ok else []
        target_state = "down" if mode == "hard_down" else "down_up"
        family_evidence = {
            "schedule_application": schedule_evidence,
            "carrier_epochs": carrier_evidence,
        }
    elif mode == "capacity":
        capacity_ok, capacity_evidence, capacity_errors = _capacity_evidence(
            target_links=requested_targets,
            switch_rows=rows.get("switch", []),
            nic_rows=rows.get("nic", []),
            segments=segments,
        )
        capacity_ok = capacity_ok and sidecar_ok and carrier_up
        _check(checks, "dual_endpoint_capacity_and_throughput_effect", capacity_ok, {
            "evidence": capacity_evidence,
            "errors": capacity_errors,
        })
        if capacity_evidence.get("throughput_degradation"):
            observed_effects.append("throughput_degradation")
        family_evidence = {
            "schedule_application": schedule_evidence,
            "capacity": capacity_evidence,
        }
    elif mode == "loss":
        switch_rx_before = [row for row in switch_before
                            if str(row.get("direction", "")).lower() == "rx"]
        switch_rx_event = [row for row in switch_event
                           if str(row.get("direction", "")).lower() == "rx"]
        switch_ok, switch_evidence = _endpoint_counter_evidence(
            switch_rx_before, switch_rx_event,
            ("switch_id", "port_id"), "dropped_packets")
        nic_ok, nic_evidence = _endpoint_counter_evidence(
            nic_before, nic_event, ("node_id", "nic_id"),
            "rx_dropped_packets")
        endpoints_ok = switch_ok and nic_ok
        _check(checks, "both_physical_endpoints_true_drop_counters",
               endpoints_ok, {
                   "fabric_endpoint": switch_evidence,
                   "host_endpoint": nic_evidence,
                   "required_endpoint_count": 2,
               })
        recovered_values = (
            _values(target_switch, "recovered_packets")
            + _values(target_nic, "recovered_packets")
        )
        recovered_zero = bool(recovered_values) and all(value == 0 for value in recovered_values)
        _check(checks, "no_recovered_packet_proxy", recovered_zero, {
            "numeric_sample_count": len(recovered_values),
            "maximum_recovered_packets": max(recovered_values) if recovered_values else None,
        })
        true_drop = sidecar_ok and endpoints_ok and recovered_zero
        packet_disposition = "dropped" if true_drop else "unproven"
        recoverable_error_proxy = not (sidecar_ok and recovered_zero)
        if endpoints_ok:
            observed_effects.append("packet_drop")
        family_evidence = {
            "schedule_application": schedule_evidence,
            "sidecar_true_drop": sidecar_ok,
            "both_endpoints_incremented": endpoints_ok,
            "recovered_packets_zero": recovered_zero,
        }
    elif mode == "service":
        effects, service_evidence = _service_effects(
            switch_before, switch_event, nic_before, nic_event,
            rows.get("collective", []), first_apply_ns, event_end_ns,
            healthy_rows if not healthy_errors else {},
            requested_targets[0] if requested_targets else "",
        )
        observed_effects = effects
        service_ok = sidecar_ok and bool(effects)
        _check(checks, "carrier_up_service_causal_effect", service_ok, {
            "observed_effects": effects,
            "required_any_of": [
                "queue_growth", "throughput_degradation", "latency_growth",
            ],
            "metrics": service_evidence,
        })
        family_evidence = {
            "schedule_application": schedule_evidence,
            "service": service_evidence,
        }
    else:
        _check(checks, "qualified_family_semantics", False,
               "mechanism has no faithful validator")

    status = PASS if checks and all(item["status"] == PASS for item in checks) else FAIL
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "mechanism_id": mechanism_id,
        "status": status,
        "source_artifact_sha256": source_hashes.get("switch", ""),
        "checks": checks,
        "injected_physical_fault": True,
        "target_link_state_during_event": target_state,
        "observed_effects": observed_effects,
        "impairment": mode if mode in {"loss", "service", "capacity"} else "",
        "packet_disposition": packet_disposition,
        "recoverable_error_proxy": recoverable_error_proxy,
        "fault_family": fault_family,
        "target_link_id": target_link,
        "target_link_ids": requested_targets,
        "scheduled_onset_ns": scheduled_onset_ns,
        "actual_apply_ns": first_apply_ns if applied_times else None,
        "event_end_ns": event_end_ns,
        "source_artifacts_sha256": source_hashes,
        "healthy_reference_used": healthy_dir is not None,
        "evidence": family_evidence,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--fault-family", required=True)
    parser.add_argument("--mechanism-id", required=True)
    parser.add_argument("--target-link", action="append", default=[])
    parser.add_argument("--scheduled-onset-ns", type=int, required=True)
    parser.add_argument("--healthy-dir", type=Path)
    parser.add_argument("--injection-schedule", type=Path)
    parser.add_argument(
        "--require-injection-schedule", action="store_true",
        help="fail closed unless a fault run binds a frozen injector CSV",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    report = validate_run(
        run_dir=args.run_dir,
        fault_family=args.fault_family,
        mechanism_id=args.mechanism_id,
        target_link=args.target_link[0] if args.target_link else None,
        target_links=args.target_link,
        scheduled_onset_ns=args.scheduled_onset_ns,
        healthy_dir=args.healthy_dir,
        injection_schedule=args.injection_schedule,
        require_injection_schedule=args.require_injection_schedule,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": report["status"],
        "run_id": report["run_id"],
        "output": str(args.output.resolve()),
    }, sort_keys=True))
    return 0 if report["status"] == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
