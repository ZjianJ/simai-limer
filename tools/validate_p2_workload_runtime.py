#!/usr/bin/env python3
"""Fail-closed runtime qualification for the P2 horizon workload.

The workload text is only a static plan.  This validator binds that plan and
one planned corpus run to the raw simulator artifacts which prove that the
16-rank training prefix was live, that every physical endpoint was sampled at
the declared 1 ms cadence, and that the observation stop (rather than normal
workload completion) ended the run.

``collective_transaction.csv`` is the high-level collective authority.
``collective_telemetry.csv`` contains completed sender flows only and is used
solely as lower-level causal/progress corroboration.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


SCHEMA_VERSION = "limer.p2-workload-runtime-qualification.v1"
HORIZON_PROFILE = "horizon-prefix"
BURST_PROFILE = "p2-allreduce-burst"
HIGH_UTIL_PROFILE = "p2-high-utilization"
OVERRIDE_PROFILES = frozenset({BURST_PROFILE, HIGH_UTIL_PROFILE})
PASS = "PASS"
FAIL = "FAIL"
DEFAULT_SAMPLE_INTERVAL_NS = 1_000_000
DEFAULT_MAX_SILENCE_NS = 10_000_000
DEFAULT_CAUSAL_WARMUP_NS = 100_000_000

LINK_MAP_COLUMNS = (
    "link_id", "src_node", "dst_node", "src_type", "dst_type",
    "src_port", "dst_port", "link_class", "bandwidth_bps", "delay_ns",
)
SWITCH_COLUMNS = (
    "run_id", "timestamp_ns", "switch_id", "port_id", "link_id",
    "peer_node_id", "direction", "tx_packets", "tx_bytes", "rx_packets",
    "rx_bytes", "dropped_packets", "drop_bytes", "queue_packets",
    "queue_bytes", "max_queue_packets", "max_queue_bytes", "ecn_marks",
    "pfc_events", "link_errors", "recovered_packets", "recovered_bytes",
    "configured_bandwidth_bps", "observed_throughput_bps", "utilization",
    "node_type", "link_state", "max_queue_timestamp_ns", "flap_count",
    "last_link_down_ns", "last_link_up_ns", "cumulative_link_down_ns",
)
NIC_COLUMNS = (
    "run_id", "timestamp_ns", "node_id", "rank_id", "nic_id", "link_id",
    "tx_packets", "tx_bytes", "rx_packets", "rx_bytes",
    "tx_dropped_packets", "tx_drop_bytes", "rx_dropped_packets",
    "rx_drop_bytes", "link_errors", "recovered_packets", "recovered_bytes",
    "retransmissions", "nacks", "outstanding_packets", "outstanding_bytes",
    "effective_throughput_bps", "completion_delay_ns", "rtt_proxy_ns",
    "queue_packets", "queue_bytes", "max_queue_packets", "max_queue_bytes",
    "max_queue_timestamp_ns", "configured_bandwidth_bps", "link_state",
    "utilization", "flap_count", "last_link_down_ns", "last_link_up_ns",
    "cumulative_link_down_ns",
)
COLLECTIVE_TRANSACTION_COLUMNS = (
    "run_id", "timestamp_ns", "collective_seq", "attempt", "layer_num",
    "message_size_bytes", "event", "rank_id", "world_size", "ready_ranks",
    "result_digest", "status",
)
COLLECTIVE_FLOW_COLUMNS = (
    "run_id", "collective_id", "iteration_id", "layer_id",
    "collective_type", "algorithm", "rank_id", "world_size",
    "message_size_bytes", "start_time_ns", "finish_time_ns", "duration_ns",
    "status",
)
LIFECYCLE_COLUMNS = (
    "run_id", "event", "scheduled_ns", "actual_ns", "finished_ranks",
    "world_size", "status",
)
COLLECTIVE_ROLE_COLUMNS = (
    "run_id", "scenario", "layer_num", "layer_id", "phase", "role",
    "scheduled_onset_ns", "scheduled_end_ns", "planned_issue_ns",
    "compute_ns", "collective_type", "message_size_bytes",
)

SWITCH_CUMULATIVE_COLUMNS = (
    "tx_packets", "tx_bytes", "rx_packets", "rx_bytes", "dropped_packets",
    "drop_bytes", "ecn_marks", "pfc_events", "link_errors",
    "recovered_packets", "recovered_bytes", "flap_count",
    "cumulative_link_down_ns",
)
NIC_CUMULATIVE_COLUMNS = (
    "tx_packets", "tx_bytes", "rx_packets", "rx_bytes",
    "tx_dropped_packets", "tx_drop_bytes", "rx_dropped_packets",
    "rx_drop_bytes", "link_errors", "recovered_packets", "recovered_bytes",
    "retransmissions", "nacks", "flap_count", "cumulative_link_down_ns",
)


class RuntimeQualificationError(ValueError):
    """One raw artifact violates the predeclared runtime contract."""


@dataclass(frozen=True)
class RuntimeValidationContract:
    """Explicit core contract; production callers derive it from the report.

    Tests may supply a smaller world, clock, and horizon.  The P2 runner never
    supplies an override and therefore always uses the frozen production
    workload report.
    """

    world_size: int
    sample_interval_ns: int
    max_first_full_start_ns: int
    max_inter_full_start_gap_ns: int
    max_tail_to_full_start_ns: int
    max_inflight_collectives: int
    max_traffic_silence_ns: int
    causal_warmup_ns: int
    expected_access_links_per_rank: int = 2


@dataclass(frozen=True)
class RuntimePaths:
    workload: Path
    link_map: Path
    switch_telemetry: Path
    nic_telemetry: Path
    collective_transaction: Path
    collective_telemetry: Path
    run_lifecycle: Path
    collective_layer_roles: Optional[Path] = None


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _json_ready(value: Any) -> Any:
    """Return a deterministic JSON-safe projection of internal evidence."""

    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [_json_ready(item) for item in sorted(value, key=repr)]
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _phase_report_evidence(name: str, value: Any) -> Any:
    """Keep the qualification audit useful without copying raw time series."""

    if not isinstance(value, Mapping):
        return _json_ready(value)
    if name == "physical_link_map_contract":
        return _json_ready({
            key: value[key] for key in (
                "physical_link_count", "derived_switch_rows_per_snapshot",
                "derived_host_rows_per_snapshot", "access_link_ids",
                "access_by_rank",
            )
        })
    if name == "exact_switch_snapshot_coverage":
        return {
            "snapshot_count": value["snapshot_count"],
            "rows_per_snapshot": value["rows_per_snapshot"],
            "access_tx_endpoint_count": len(
                value["access_tx_series_by_endpoint"]
            ),
        }
    if name == "exact_nic_snapshot_coverage":
        rank_links = value["access_tx_series_by_rank_and_link"]
        return {
            "snapshot_count": value["snapshot_count"],
            "rows_per_snapshot": value["rows_per_snapshot"],
            "access_rank_count": len(rank_links),
            "access_link_count": sum(len(links) for links in rank_links.values()),
        }
    return _json_ready(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _exact_reader(path: Path, columns: Sequence[str], label: str) -> csv.DictReader:
    try:
        stream = Path(path).open(encoding="utf-8", newline="")
    except OSError as exc:
        raise RuntimeQualificationError(f"{label} is unreadable: {exc}") from exc
    reader = csv.DictReader(stream)
    if tuple(reader.fieldnames or ()) != tuple(columns):
        stream.close()
        raise RuntimeQualificationError(
            f"{label} exact schema mismatch: expected={list(columns)}, "
            f"observed={reader.fieldnames}"
        )
    # csv.DictReader owns no close protocol, so attach the stream for callers.
    reader._limer_stream = stream  # type: ignore[attr-defined]
    return reader


def _close_reader(reader: csv.DictReader) -> None:
    stream = getattr(reader, "_limer_stream", None)
    if stream is not None:
        stream.close()


def _uint(raw: Any, field: str, *, allow_empty: bool = False) -> Optional[int]:
    text = "" if raw is None else str(raw)
    if allow_empty and text == "":
        return None
    if not text or not text.isdigit():
        raise RuntimeQualificationError(f"{field} must be an unsigned integer: {raw!r}")
    return int(text)


def _nonnegative_finite_decimal(raw: Any, field: str) -> float:
    """Parse one C++ ``std::to_string(double)`` telemetry value strictly."""

    text = "" if raw is None else str(raw).strip()
    if not text:
        raise RuntimeQualificationError(f"{field} must not be empty")
    try:
        value = float(text)
    except ValueError as exc:
        raise RuntimeQualificationError(
            f"{field} must be a finite non-negative decimal: {raw!r}"
        ) from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeQualificationError(
            f"{field} must be a finite non-negative decimal: {raw!r}"
        )
    return value


def _derive_contract(
    workload_report: Mapping[str, Any], causal_warmup_ns: int,
) -> RuntimeValidationContract:
    if workload_report.get("status") != PASS:
        raise RuntimeQualificationError("static workload report status is not PASS")
    profile = workload_report.get("qualification_profile")
    if profile in OVERRIDE_PROFILES:
        override = workload_report.get("contract")
        if not isinstance(override, Mapping):
            raise RuntimeQualificationError("override workload report lacks contract")
        if (
            int(override.get("world_size", 0)) != 16
            or int(override.get("source_layer_count", 0)) != 550
            or override.get("actual_application_window_authority")
            != "role_bound_collective_transaction"
            or override.get("static_clock_semantics")
            != "compute_only_not_actual_issue_time"
        ):
            raise RuntimeQualificationError("override workload contract shape is invalid")
        return RuntimeValidationContract(
            world_size=16,
            sample_interval_ns=DEFAULT_SAMPLE_INTERVAL_NS,
            max_first_full_start_ns=2_000_000,
            max_inter_full_start_gap_ns=2_000_000,
            max_tail_to_full_start_ns=int(
                override["maximum_pre_tail_to_actual_onset_ns"]
            ),
            max_inflight_collectives=1,
            max_traffic_silence_ns=DEFAULT_MAX_SILENCE_NS,
            causal_warmup_ns=int(causal_warmup_ns),
        )
    if profile != HORIZON_PROFILE:
        raise RuntimeQualificationError(
            "P2 runtime qualification requires the horizon-prefix profile"
        )
    estimate = workload_report.get("duration_estimate")
    if not isinstance(estimate, Mapping):
        raise RuntimeQualificationError("static workload report lacks duration_estimate")
    if estimate.get("may_be_used_as_p2_horizon_workload") is not True:
        raise RuntimeQualificationError("static workload is not admissible for P2")
    runtime = estimate.get("runtime_evidence_contract")
    if not isinstance(runtime, Mapping) or runtime.get("contract_id") != (
        "p2-horizon-prefix-v1"
    ):
        raise RuntimeQualificationError("unknown horizon runtime evidence contract")
    return RuntimeValidationContract(
        world_size=int(workload_report["world_size"]),
        sample_interval_ns=DEFAULT_SAMPLE_INTERVAL_NS,
        max_first_full_start_ns=int(
            runtime["first_full_collective_start_ns_at_most"]
        ),
        max_inter_full_start_gap_ns=int(
            runtime["maximum_inter_full_collective_start_gap_ns_at_most"]
        ),
        max_tail_to_full_start_ns=int(
            runtime["maximum_horizon_to_last_full_start_gap_ns_at_most"]
        ),
        max_inflight_collectives=int(
            runtime["maximum_inflight_collectives_at_horizon"]
        ),
        max_traffic_silence_ns=DEFAULT_MAX_SILENCE_NS,
        causal_warmup_ns=int(causal_warmup_ns),
    )


def _validate_context(
    run: Mapping[str, Any], workload_report: Mapping[str, Any],
    contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    run_id = str(run.get("run_id", ""))
    if not run_id:
        raise RuntimeQualificationError("planned run lacks run_id")
    start_ns = run.get("virtual_start_ns", 0)
    horizon_ns = run.get("virtual_finish_ns")
    if isinstance(start_ns, bool) or not isinstance(start_ns, int) or start_ns != 0:
        raise RuntimeQualificationError("P2 runtime run must start at virtual time zero")
    if isinstance(horizon_ns, bool) or not isinstance(horizon_ns, int) or horizon_ns <= 0:
        raise RuntimeQualificationError("planned run has invalid virtual_finish_ns")
    profile = str(workload_report.get("qualification_profile", ""))
    estimate = workload_report.get("duration_estimate", {})
    override = workload_report.get("contract", {})
    covered = int(
        override.get("runtime_horizon_ns", horizon_ns)
        if profile in OVERRIDE_PROFILES and isinstance(override, Mapping)
        else estimate.get("corpus_max_virtual_finish_ns", horizon_ns)
    )
    if horizon_ns > covered:
        raise RuntimeQualificationError(
            f"planned horizon {horizon_ns} exceeds static workload coverage {covered}"
        )
    workload_sha = str(workload_report.get("sha256", ""))
    planned_workload_sha = (
        run.get("effective_workload_sha256")
        if profile in OVERRIDE_PROFILES
        else run.get("workload_sha256")
    )
    if planned_workload_sha not in (None, workload_sha):
        raise RuntimeQualificationError("planned run workload hash differs from report")
    healthy = (
        str(run.get("class_label", "")).upper() == "HEALTHY"
        or str(run.get("run_role", "")).lower() == "healthy"
    )
    onset: Optional[int] = None
    if not healthy:
        raw_onset = run.get("fault_scheduled_onset_ns")
        if isinstance(raw_onset, bool) or not isinstance(raw_onset, int):
            raise RuntimeQualificationError("event run lacks integer planned onset")
        onset = raw_onset
        if not start_ns < onset < horizon_ns:
            raise RuntimeQualificationError("planned onset is outside the run horizon")
        if onset - start_ns < contract.causal_warmup_ns:
            raise RuntimeQualificationError(
                f"pre-event warmup is shorter than {contract.causal_warmup_ns} ns"
            )
    if contract.world_size <= 0 or contract.sample_interval_ns <= 0:
        raise RuntimeQualificationError("runtime validation contract is invalid")
    return {
        "run_id": run_id,
        "virtual_start_ns": start_ns,
        "horizon_ns": horizon_ns,
        "healthy": healthy,
        "cadence_end_ns": horizon_ns if healthy else onset,
        "onset_ns": onset,
        "qualification_profile": profile,
        "layer_count": int(
            workload_report.get(
                "validated_layer_count", workload_report.get("layer_count", 0)
            )
        ),
        "collective_bytes": (
            None
            if profile in OVERRIDE_PROFILES
            else int(workload_report["collective_bytes_per_layer"])
        ),
        "workload_sha256": workload_sha,
    }


def _read_link_map(
    path: Path, contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    reader = _exact_reader(path, LINK_MAP_COLUMNS, "link_map.csv")
    rows: List[Dict[str, Any]] = []
    try:
        for raw in reader:
            row = dict(raw)
            row["src_node"] = _uint(raw["src_node"], "link_map src_node")
            row["dst_node"] = _uint(raw["dst_node"], "link_map dst_node")
            row["src_port"] = _uint(raw["src_port"], "link_map src_port")
            row["dst_port"] = _uint(raw["dst_port"], "link_map dst_port")
            rows.append(row)
    finally:
        _close_reader(reader)
    if not rows:
        raise RuntimeQualificationError("link_map.csv has no physical links")
    link_ids = [str(row["link_id"]) for row in rows]
    if any(not value for value in link_ids) or len(link_ids) != len(set(link_ids)):
        raise RuntimeQualificationError("link_map link_id values are not unique")

    fabric_keys: set[Tuple[int, int, str, str]] = set()
    host_keys: set[Tuple[int, int, str]] = set()
    access_link_ids: set[str] = set()
    access_switch_tx_keys: set[Tuple[int, int, str, str]] = set()
    access_by_rank: Dict[int, set[str]] = {
        rank: set() for rank in range(contract.world_size)
    }
    endpoint_keys: set[Tuple[int, int]] = set()
    for row in rows:
        link_id = str(row["link_id"])
        link_class = str(row["link_class"])
        endpoints = (
            (int(row["src_node"]), int(row["src_port"]), str(row["src_type"])),
            (int(row["dst_node"]), int(row["dst_port"]), str(row["dst_type"])),
        )
        for node, port, node_type in endpoints:
            endpoint = (node, port)
            if endpoint in endpoint_keys:
                raise RuntimeQualificationError(
                    f"link_map reuses physical endpoint node={node} port={port}"
                )
            endpoint_keys.add(endpoint)
            if node_type in {"SWITCH", "NVSWITCH"}:
                for direction in ("tx", "rx"):
                    key = (node, port, link_id, direction)
                    fabric_keys.add(key)
                    if link_class == "ACCESS" and direction == "tx":
                        access_switch_tx_keys.add(key)
            elif node_type == "HOST":
                host_keys.add((node, port, link_id))
                if link_class == "ACCESS":
                    if node not in access_by_rank:
                        raise RuntimeQualificationError(
                            f"ACCESS host node {node} is outside the rank set"
                        )
                    access_by_rank[node].add(link_id)
            else:
                raise RuntimeQualificationError(f"unknown node type {node_type!r}")
        if link_class == "ACCESS":
            access_link_ids.add(link_id)
    expected_ranks = set(range(contract.world_size))
    if set(access_by_rank) != expected_ranks or any(
        len(access_by_rank[rank]) != contract.expected_access_links_per_rank
        for rank in expected_ranks
    ):
        observed = {rank: sorted(links) for rank, links in access_by_rank.items()}
        raise RuntimeQualificationError(
            "link_map does not expose the required ACCESS rails per rank: "
            f"{observed}"
        )
    return {
        "physical_link_count": len(rows),
        "fabric_keys": fabric_keys,
        "host_keys": host_keys,
        "access_link_ids": access_link_ids,
        "access_switch_tx_keys": access_switch_tx_keys,
        "access_by_rank": access_by_rank,
        "derived_switch_rows_per_snapshot": len(fabric_keys),
        "derived_host_rows_per_snapshot": len(host_keys),
    }


def _check_counter(
    state: MutableMapping[Tuple[Any, ...], int], key: Tuple[Any, ...],
    raw: Any, label: str,
) -> Optional[int]:
    value = _uint(raw, label, allow_empty=True)
    if value is None:
        return None
    previous = state.get(key)
    if previous is not None and value < previous:
        raise RuntimeQualificationError(
            f"cumulative counter rollback for {key}: {previous}->{value}"
        )
    state[key] = value
    return value


def _expected_timestamps(horizon_ns: int, interval_ns: int) -> List[int]:
    return list(range(interval_ns, horizon_ns, interval_ns))


def _read_switch_snapshots(
    path: Path, context: Mapping[str, Any], contract: RuntimeValidationContract,
    topology: Mapping[str, Any],
) -> Dict[str, Any]:
    expected_times = _expected_timestamps(
        int(context["horizon_ns"]), contract.sample_interval_ns
    )
    expected_keys = topology["fabric_keys"]
    access_keys = topology["access_switch_tx_keys"]
    reader = _exact_reader(path, SWITCH_COLUMNS, "switch_telemetry.csv")
    counters: Dict[Tuple[Any, ...], int] = {}
    series: Dict[Tuple[int, int, str, str], List[Tuple[int, int]]] = {
        key: [] for key in access_keys
    }
    throughput_series: Dict[
        Tuple[int, int, str, str], List[Tuple[int, float, int]]
    ] = {key: [] for key in access_keys}
    time_index = 0
    current_time: Optional[int] = None
    current_seen: set[Tuple[int, int, str, str]] = set()
    current_access_tx: Dict[Tuple[int, int, str, str], int] = {}
    current_access_throughput: Dict[
        Tuple[int, int, str, str], Tuple[float, int]
    ] = {}

    def finish_snapshot() -> None:
        nonlocal time_index, current_seen, current_access_tx
        nonlocal current_access_throughput
        if current_time is None:
            return
        if time_index >= len(expected_times) or current_time != expected_times[time_index]:
            expected = expected_times[time_index] if time_index < len(expected_times) else None
            raise RuntimeQualificationError(
                f"switch snapshot timestamp {current_time}, expected {expected}"
            )
        if current_seen != expected_keys:
            missing = sorted(expected_keys - current_seen)[:4]
            extra = sorted(current_seen - expected_keys)[:4]
            raise RuntimeQualificationError(
                f"switch snapshot {current_time} key mismatch: "
                f"missing={missing}, extra={extra}"
            )
        for key in access_keys:
            if key not in current_access_tx:
                raise RuntimeQualificationError(
                    f"switch snapshot {current_time} lacks ACCESS tx_bytes for {key}"
                )
            series[key].append((current_time, current_access_tx[key]))
            observed, configured = current_access_throughput[key]
            throughput_series[key].append((current_time, observed, configured))
        time_index += 1
        current_seen = set()
        current_access_tx = {}
        current_access_throughput = {}

    try:
        for row in reader:
            if row["run_id"] != context["run_id"]:
                raise RuntimeQualificationError("switch telemetry contains foreign run_id")
            timestamp = int(_uint(row["timestamp_ns"], "switch timestamp_ns"))
            if current_time is None:
                current_time = timestamp
            elif timestamp != current_time:
                if timestamp < current_time:
                    raise RuntimeQualificationError("switch timestamps are not monotonic")
                finish_snapshot()
                current_time = timestamp
            key = (
                int(_uint(row["switch_id"], "switch_id")),
                int(_uint(row["port_id"], "port_id")),
                row["link_id"], row["direction"],
            )
            if key not in expected_keys:
                raise RuntimeQualificationError(f"foreign switch endpoint key {key}")
            if key in current_seen:
                raise RuntimeQualificationError(
                    f"duplicate switch endpoint key at {timestamp}: {key}"
                )
            current_seen.add(key)
            for column in SWITCH_CUMULATIVE_COLUMNS:
                value = _check_counter(
                    counters, key + (column,), row[column],
                    f"switch {column}",
                )
                if column == "tx_bytes" and key in access_keys:
                    if value is None:
                        raise RuntimeQualificationError(
                            "ACCESS switch tx row has empty tx_bytes"
                        )
                    current_access_tx[key] = value
            if key in access_keys:
                current_access_throughput[key] = (
                    _nonnegative_finite_decimal(
                        row["observed_throughput_bps"],
                        "switch observed_throughput_bps",
                    ),
                    int(_uint(
                        row["configured_bandwidth_bps"],
                        "switch configured_bandwidth_bps",
                    )),
                )
        finish_snapshot()
    finally:
        _close_reader(reader)
    if time_index != len(expected_times):
        raise RuntimeQualificationError(
            f"switch snapshot count {time_index}, expected {len(expected_times)}"
        )
    return {
        "snapshot_count": time_index,
        "rows_per_snapshot": len(expected_keys),
        "access_tx_series_by_endpoint": series,
        "access_throughput_series_by_endpoint": throughput_series,
    }


def _read_nic_snapshots(
    path: Path, context: Mapping[str, Any], contract: RuntimeValidationContract,
    topology: Mapping[str, Any],
) -> Dict[str, Any]:
    expected_times = _expected_timestamps(
        int(context["horizon_ns"]), contract.sample_interval_ns
    )
    expected_keys = topology["host_keys"]
    access_by_rank = topology["access_by_rank"]
    reader = _exact_reader(path, NIC_COLUMNS, "nic_telemetry.csv")
    counters: Dict[Tuple[Any, ...], int] = {}
    series: Dict[int, Dict[str, List[Tuple[int, int]]]] = {
        rank: {link_id: [] for link_id in access_by_rank[rank]}
        for rank in range(contract.world_size)
    }
    time_index = 0
    current_time: Optional[int] = None
    current_seen: set[Tuple[int, int, str]] = set()
    current_access_tx: Dict[Tuple[int, str], int] = {}

    def finish_snapshot() -> None:
        nonlocal time_index, current_seen, current_access_tx
        if current_time is None:
            return
        if time_index >= len(expected_times) or current_time != expected_times[time_index]:
            expected = expected_times[time_index] if time_index < len(expected_times) else None
            raise RuntimeQualificationError(
                f"NIC snapshot timestamp {current_time}, expected {expected}"
            )
        if current_seen != expected_keys:
            missing = sorted(expected_keys - current_seen)[:4]
            extra = sorted(current_seen - expected_keys)[:4]
            raise RuntimeQualificationError(
                f"NIC snapshot {current_time} key mismatch: "
                f"missing={missing}, extra={extra}"
            )
        for rank, links in access_by_rank.items():
            for link_id in links:
                key = (rank, link_id)
                if key not in current_access_tx:
                    raise RuntimeQualificationError(
                        f"NIC snapshot {current_time} lacks ACCESS tx_bytes for {key}"
                    )
                series[rank][link_id].append(
                    (current_time, current_access_tx[key])
                )
        time_index += 1
        current_seen = set()
        current_access_tx = {}

    try:
        for row in reader:
            if row["run_id"] != context["run_id"]:
                raise RuntimeQualificationError("NIC telemetry contains foreign run_id")
            timestamp = int(_uint(row["timestamp_ns"], "NIC timestamp_ns"))
            if current_time is None:
                current_time = timestamp
            elif timestamp != current_time:
                if timestamp < current_time:
                    raise RuntimeQualificationError("NIC timestamps are not monotonic")
                finish_snapshot()
                current_time = timestamp
            node = int(_uint(row["node_id"], "NIC node_id"))
            rank = int(_uint(row["rank_id"], "NIC rank_id"))
            port = int(_uint(row["nic_id"], "NIC nic_id"))
            if rank != node or rank not in series:
                raise RuntimeQualificationError(
                    f"NIC node/rank identity is invalid: node={node}, rank={rank}"
                )
            key = (node, port, row["link_id"])
            if key not in expected_keys:
                raise RuntimeQualificationError(f"foreign NIC endpoint key {key}")
            if key in current_seen:
                raise RuntimeQualificationError(
                    f"duplicate NIC endpoint key at {timestamp}: {key}"
                )
            current_seen.add(key)
            tx_value: Optional[int] = None
            for column in NIC_CUMULATIVE_COLUMNS:
                value = _check_counter(
                    counters, key + (column,), row[column], f"NIC {column}"
                )
                if column == "tx_bytes":
                    tx_value = value
            if row["link_id"] in access_by_rank[rank]:
                if tx_value is None:
                    raise RuntimeQualificationError("ACCESS NIC row has empty tx_bytes")
                current_access_tx[(rank, row["link_id"])] = tx_value
        finish_snapshot()
    finally:
        _close_reader(reader)
    if time_index != len(expected_times):
        raise RuntimeQualificationError(
            f"NIC snapshot count {time_index}, expected {len(expected_times)}"
        )
    return {
        "snapshot_count": time_index,
        "rows_per_snapshot": len(expected_keys),
        "access_tx_series_by_rank_and_link": series,
    }


def _positive_growth_times(
    series: Iterable[Tuple[int, int]], end_ns: int,
) -> List[int]:
    previous = 0
    positive: List[int] = []
    for timestamp, value in series:
        if timestamp >= end_ns:
            break
        if value < previous:
            raise RuntimeQualificationError("aggregate ACCESS counter rolled back")
        if value > previous:
            positive.append(timestamp)
        previous = value
    return positive


def _validate_activity(
    switch_evidence: Mapping[str, Any], nic_evidence: Mapping[str, Any],
    context: Mapping[str, Any], contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    end_ns = int(context["cadence_end_ns"])
    start_ns = int(context["virtual_start_ns"])

    def evidence(series: Iterable[Tuple[int, int]], label: str) -> Dict[str, Any]:
        positive = _positive_growth_times(series, end_ns)
        if not positive:
            raise RuntimeQualificationError(f"{label} has no positive ACCESS TX growth")
        gaps = [positive[0] - start_ns]
        gaps.extend(right - left for left, right in zip(positive, positive[1:]))
        gaps.append(end_ns - positive[-1])
        maximum = max(gaps)
        if maximum > contract.max_traffic_silence_ns:
            raise RuntimeQualificationError(
                f"{label} ACCESS TX silence {maximum} ns exceeds "
                f"{contract.max_traffic_silence_ns} ns"
            )
        return {
            "positive_growth_sample_count": len(positive),
            "first_positive_growth_ns": positive[0],
            "last_positive_growth_ns": positive[-1],
            "maximum_silence_ns": maximum,
        }

    switch = {}
    for key, series in switch_evidence["access_tx_series_by_endpoint"].items():
        node, port, link_id, direction = key
        switch[f"{node}:{port}:{link_id}:{direction}"] = evidence(
            series, "switch fabric"
        )
    ranks = {}
    for rank, links in nic_evidence[
        "access_tx_series_by_rank_and_link"
    ].items():
        ranks[str(rank)] = {
            link_id: evidence(series, f"rank {rank}")
            for link_id, series in links.items()
        }
    return {
        "evaluation_end_ns": end_ns,
        "switch_access_endpoints": switch,
        "rank_access_links": ranks,
    }


def _read_override_roles(
    role_path: Path,
    workload_path: Path,
    context: MutableMapping[str, Any],
    workload_report: Mapping[str, Any],
) -> Dict[str, Any]:
    """Bind predeclared semantic roles to exact native workload rows."""

    profile = str(context.get("qualification_profile", ""))
    if profile not in OVERRIDE_PROFILES:
        raise RuntimeQualificationError("collective roles require an override profile")
    contract = workload_report.get("contract")
    if not isinstance(contract, Mapping):
        raise RuntimeQualificationError("override workload report contract is missing")
    try:
        raw_workload = workload_path.read_bytes()
        text = raw_workload.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise RuntimeQualificationError(f"override workload is unreadable: {exc}") from exc
    if not text.endswith("\n"):
        raise RuntimeQualificationError("override workload lacks final newline")
    lines = text.splitlines()
    layer_count = int(context["layer_count"])
    if len(lines) != layer_count + 2 or lines[1] != str(layer_count):
        raise RuntimeQualificationError("override workload layer count is invalid")
    workload_rows = [line.split() for line in lines[2:]]
    if any(len(row) != 12 for row in workload_rows):
        raise RuntimeQualificationError("override workload row width is invalid")

    reader = _exact_reader(
        role_path, COLLECTIVE_ROLE_COLUMNS, "collective layer role sidecar"
    )
    roles: List[Dict[str, Any]] = []
    try:
        for expected_layer, raw in enumerate(reader):
            if raw["run_id"] != context["run_id"]:
                raise RuntimeQualificationError("collective role contains foreign run_id")
            layer = int(_uint(raw["layer_num"], "role layer_num"))
            if layer != expected_layer or layer >= layer_count:
                raise RuntimeQualificationError("collective role layer_num is not contiguous")
            row = workload_rows[layer]
            compute = int(_uint(raw["compute_ns"], "role compute_ns"))
            message = int(_uint(raw["message_size_bytes"], "role message_size"))
            planned = int(_uint(raw["planned_issue_ns"], "role planned_issue_ns"))
            onset = int(_uint(raw["scheduled_onset_ns"], "role scheduled_onset_ns"))
            end = int(_uint(raw["scheduled_end_ns"], "role scheduled_end_ns"))
            if (
                raw["scenario"] != contract.get("scenario")
                or raw["layer_id"] != row[0]
                or raw["collective_type"] != "ALLREDUCE"
                or row[3] != "ALLREDUCE"
                or compute != int(row[2])
                or message != int(row[4])
                or onset != int(contract["scheduled_onset_ns"])
                or end != int(contract["scheduled_end_ns"])
            ):
                raise RuntimeQualificationError(
                    f"collective role/workload binding differs at layer {layer}"
                )
            expected_planned = compute + (
                roles[-1]["planned_issue_ns"] if roles else 0
            )
            if planned != expected_planned:
                raise RuntimeQualificationError(
                    f"collective role planned clock differs at layer {layer}"
                )
            phase = raw["phase"]
            role = raw["role"]
            allowed = {
                ("PRE", "PRE_BASELINE"),
                ("EVENT", "EVENT_BURST"),
                ("EVENT", "EVENT_BASELINE"),
                ("EVENT", "EVENT_PRESSURE"),
                ("POST", "POST_BASELINE"),
            }
            if (phase, role) not in allowed:
                raise RuntimeQualificationError(f"unknown collective role {phase}/{role}")
            roles.append({
                "layer_num": layer,
                "layer_id": row[0],
                "phase": phase,
                "role": role,
                "planned_issue_ns": planned,
                "compute_ns": compute,
                "message_size_bytes": message,
            })
    finally:
        _close_reader(reader)
    if len(roles) != layer_count:
        raise RuntimeQualificationError(
            f"collective role count {len(roles)}, expected {layer_count}"
        )
    role_counts: Dict[str, int] = {}
    for row in roles:
        role_counts[row["role"]] = role_counts.get(row["role"], 0) + 1
    expected_role_counts = workload_report.get("role_counts")
    if role_counts != expected_role_counts:
        raise RuntimeQualificationError(
            f"collective role counts differ: {role_counts} != {expected_role_counts}"
        )
    if role_counts.get("PRE_BASELINE") != int(contract["pre_layer_count"]):
        raise RuntimeQualificationError("collective PRE role count is invalid")
    if not role_counts.get("POST_BASELINE"):
        raise RuntimeQualificationError("collective override has no POST role")
    if profile == BURST_PROFILE:
        if role_counts.get("EVENT_BURST") != int(contract["burst_layer_count"]):
            raise RuntimeQualificationError("allreduce burst role count is not exact")
        if role_counts.get("EVENT_PRESSURE", 0):
            raise RuntimeQualificationError("burst profile contains pressure roles")
    else:
        if role_counts.get("EVENT_PRESSURE") != int(contract["pressure_layer_count"]):
            raise RuntimeQualificationError("high-util pressure role count is not exact")
        if role_counts.get("EVENT_BURST", 0) or role_counts.get("EVENT_BASELINE", 0):
            raise RuntimeQualificationError("high-util profile contains foreign event roles")
    context["layer_message_sizes"] = {
        row["layer_num"]: row["message_size_bytes"] for row in roles
    }
    return {
        "role_count": len(roles),
        "role_counts": role_counts,
        "scheduled_onset_ns": int(contract["scheduled_onset_ns"]),
        "scheduled_end_ns": int(contract["scheduled_end_ns"]),
        "static_clock_semantics": contract["static_clock_semantics"],
        "rows": roles,
    }


def _read_collective_transactions(
    path: Path, context: Mapping[str, Any], contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    reader = _exact_reader(
        path, COLLECTIVE_TRANSACTION_COLUMNS, "collective_transaction.csv"
    )
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    try:
        for raw in reader:
            if raw["run_id"] != context["run_id"]:
                raise RuntimeQualificationError(
                    "collective transaction contains foreign run_id"
                )
            event = raw["event"]
            if event in {"ABORT", "REDO_START"}:
                raise RuntimeQualificationError(
                    f"recovery event {event} is forbidden in P2 baseline"
                )
            if event not in {"START", "LOCAL_READY", "COMMIT"}:
                raise RuntimeQualificationError(
                    f"unknown collective transaction event {event!r}"
                )
            seq = int(_uint(raw["collective_seq"], "collective_seq"))
            row = dict(raw)
            row["timestamp_ns"] = int(_uint(raw["timestamp_ns"], "transaction timestamp"))
            row["attempt"] = int(_uint(raw["attempt"], "transaction attempt"))
            row["layer_num"] = int(_uint(raw["layer_num"], "transaction layer_num"))
            row["message_size_bytes"] = int(
                _uint(raw["message_size_bytes"], "transaction message_size_bytes")
            )
            row["world_size"] = int(_uint(raw["world_size"], "transaction world_size"))
            row["ready_ranks"] = int(_uint(raw["ready_ranks"], "transaction ready_ranks"))
            row["rank_id"] = _uint(raw["rank_id"], "transaction rank_id", allow_empty=True)
            if row["timestamp_ns"] > int(context["horizon_ns"]):
                raise RuntimeQualificationError(
                    "collective transaction occurs after the observation horizon"
                )
            if row["attempt"] != 0 or row["world_size"] != contract.world_size:
                raise RuntimeQualificationError("transaction attempt/world_size mismatch")
            if not 0 <= row["layer_num"] < int(context["layer_count"]):
                raise RuntimeQualificationError("transaction layer_num is outside workload")
            expected_sizes = context.get("layer_message_sizes")
            expected_size = (
                expected_sizes.get(row["layer_num"])
                if isinstance(expected_sizes, Mapping)
                else context["collective_bytes"]
            )
            if expected_size is None or row["message_size_bytes"] != int(expected_size):
                raise RuntimeQualificationError(
                    "transaction message size differs from workload layer"
                )
            grouped.setdefault(seq, []).append(row)
    finally:
        _close_reader(reader)
    if not grouped:
        raise RuntimeQualificationError("collective transaction is empty")
    sequences = sorted(grouped)
    if sequences != list(range(sequences[-1] + 1)):
        raise RuntimeQualificationError(
            f"collective sequence is not a contiguous zero-based prefix: {sequences}"
        )
    expected_ranks = set(range(contract.world_size))
    starts: List[int] = []
    committed: List[int] = []
    layers: set[int] = set()
    sequence_evidence: List[Dict[str, Any]] = []
    for seq in sequences:
        rows = grouped[seq]
        layer_values = {int(row["layer_num"]) for row in rows}
        message_values = {int(row["message_size_bytes"]) for row in rows}
        if len(layer_values) != 1 or len(message_values) != 1:
            raise RuntimeQualificationError(f"sequence {seq} metadata is inconsistent")
        layer = next(iter(layer_values))
        if layer in layers:
            raise RuntimeQualificationError(f"workload layer {layer} appears in two sequences")
        layers.add(layer)
        start_rows = [row for row in rows if row["event"] == "START"]
        ready_rows = [row for row in rows if row["event"] == "LOCAL_READY"]
        commit_rows = [row for row in rows if row["event"] == "COMMIT"]
        start_ranks = [row["rank_id"] for row in start_rows]
        if len(start_rows) != contract.world_size or set(start_ranks) != expected_ranks:
            raise RuntimeQualificationError(
                f"sequence {seq} does not have exactly one START for all ranks"
            )
        if any(row["status"] != "in_flight" for row in start_rows):
            raise RuntimeQualificationError(f"sequence {seq} START status is invalid")
        start_by_rank = {int(row["rank_id"]): row for row in start_rows}
        full_start = max(int(row["timestamp_ns"]) for row in start_rows)
        starts.append(full_start)
        if commit_rows:
            ready_ranks = [row["rank_id"] for row in ready_rows]
            if len(ready_rows) != contract.world_size or set(ready_ranks) != expected_ranks:
                raise RuntimeQualificationError(
                    f"sequence {seq} committed without {contract.world_size} "
                    "unique LOCAL_READY ranks"
                )
            if any(row["status"] != "ready" for row in ready_rows):
                raise RuntimeQualificationError(
                    f"sequence {seq} LOCAL_READY status is invalid"
                )
            if len(commit_rows) != 1:
                raise RuntimeQualificationError(f"sequence {seq} has multiple COMMIT rows")
            commit = commit_rows[0]
            if (
                commit["rank_id"] is not None
                or commit["ready_ranks"] != contract.world_size
                or not commit["result_digest"]
                or commit["status"] != "exactly_once"
            ):
                raise RuntimeQualificationError(f"sequence {seq} COMMIT is invalid")
            ready_by_rank = {int(row["rank_id"]): row for row in ready_rows}
            if any(
                int(ready_by_rank[rank]["timestamp_ns"])
                < int(start_by_rank[rank]["timestamp_ns"])
                for rank in expected_ranks
            ):
                raise RuntimeQualificationError(
                    f"sequence {seq} has LOCAL_READY before its rank START"
                )
            if int(commit["timestamp_ns"]) < max(
                int(row["timestamp_ns"]) for row in ready_rows
            ):
                raise RuntimeQualificationError(f"sequence {seq} COMMIT is non-causal")
            committed.append(seq)
            commit_ns: Optional[int] = int(commit["timestamp_ns"])
        else:
            if len({row["rank_id"] for row in ready_rows}) != len(ready_rows):
                raise RuntimeQualificationError(
                    f"sequence {seq} has duplicate incomplete LOCAL_READY rank"
                )
            if any(
                row["rank_id"] not in expected_ranks or row["status"] != "ready"
                for row in ready_rows
            ):
                raise RuntimeQualificationError(
                    f"sequence {seq} incomplete LOCAL_READY row is invalid"
                )
            if any(
                int(row["timestamp_ns"])
                < int(start_by_rank[int(row["rank_id"])]["timestamp_ns"])
                for row in ready_rows
            ):
                raise RuntimeQualificationError(
                    f"sequence {seq} has incomplete LOCAL_READY before START"
                )
            commit_ns = None
        sequence_evidence.append({
            "collective_seq": seq,
            "layer_num": layer,
            "full_start_ns": full_start,
            "committed": bool(commit_rows),
            "commit_ns": commit_ns,
            "ready_rank_count": len(ready_rows),
        })
    if committed != list(range(len(committed))):
        raise RuntimeQualificationError(
            f"completed collective sequences are not a contiguous prefix: {committed}"
        )
    if any(right <= left for left, right in zip(starts, starts[1:])):
        raise RuntimeQualificationError(
            "full collective START timestamps are not strictly increasing"
        )
    for seq in committed:
        if seq + 1 not in grouped:
            continue
        commit_ns = next(
            int(row["timestamp_ns"])
            for row in grouped[seq] if row["event"] == "COMMIT"
        )
        next_start_ns = min(
            int(row["timestamp_ns"])
            for row in grouped[seq + 1] if row["event"] == "START"
        )
        if commit_ns > next_start_ns:
            raise RuntimeQualificationError(
                f"sequence {seq + 1} START precedes sequence {seq} COMMIT"
            )
    incomplete = sequences[len(committed):]
    if len(incomplete) > contract.max_inflight_collectives or (
        incomplete and incomplete != [sequences[-1]]
    ):
        raise RuntimeQualificationError(
            f"too many or non-terminal in-flight collectives: {incomplete}"
        )
    if starts[0] > contract.max_first_full_start_ns:
        raise RuntimeQualificationError(
            f"first full START {starts[0]} exceeds {contract.max_first_full_start_ns}"
        )
    cadence_end = int(context["cadence_end_ns"])
    cadence_starts = [value for value in starts if value < cadence_end]
    if not cadence_starts:
        raise RuntimeQualificationError("no full-rank START before cadence boundary")
    gaps = [right - left for left, right in zip(cadence_starts, cadence_starts[1:])]
    maximum_gap = max(gaps, default=0)
    if maximum_gap > contract.max_inter_full_start_gap_ns:
        raise RuntimeQualificationError(
            f"full START cadence gap {maximum_gap} exceeds "
            f"{contract.max_inter_full_start_gap_ns}"
        )
    tail_gap = cadence_end - cadence_starts[-1]
    if tail_gap > contract.max_tail_to_full_start_ns:
        raise RuntimeQualificationError(
            f"cadence boundary to last full START gap {tail_gap} exceeds "
            f"{contract.max_tail_to_full_start_ns}"
        )
    return {
        "sequence_count": len(sequences),
        "committed_sequence_count": len(committed),
        "inflight_sequences": incomplete,
        "first_full_start_ns": starts[0],
        "last_qualified_full_start_ns": cadence_starts[-1],
        "maximum_qualified_start_gap_ns": maximum_gap,
        "qualified_tail_gap_ns": tail_gap,
        "sequence_evidence": sequence_evidence,
    }


def _validate_override_application_lifecycle(
    role_evidence: Mapping[str, Any],
    transaction_evidence: Mapping[str, Any],
    switch_evidence: Mapping[str, Any],
    context: Mapping[str, Any],
    workload_report: Mapping[str, Any],
) -> Dict[str, Any]:
    """Prove the pre/event/post lifecycle from application and raw link data.

    The high-utilization measurement window is selected solely from the first
    predeclared pressure role's full-rank START.  Throughput never moves the
    window and can only make the run fail.
    """

    contract = workload_report.get("contract")
    if not isinstance(contract, Mapping):
        raise RuntimeQualificationError("override lifecycle contract is missing")
    rows = role_evidence.get("rows")
    sequences = transaction_evidence.get("sequence_evidence")
    if not isinstance(rows, Sequence) or not isinstance(sequences, Sequence):
        raise RuntimeQualificationError("override role/transaction evidence is absent")
    by_layer = {
        int(item["layer_num"]): item
        for item in sequences
        if isinstance(item, Mapping)
    }

    def evidence_for(role: str, *, require_all: bool = False) -> List[Mapping[str, Any]]:
        role_layers = [
            int(item["layer_num"])
            for item in rows
            if isinstance(item, Mapping) and item.get("role") == role
        ]
        observed = [by_layer[layer] for layer in role_layers if layer in by_layer]
        if require_all and len(observed) != len(role_layers):
            raise RuntimeQualificationError(
                f"role {role} has {len(observed)}/{len(role_layers)} transactions"
            )
        return observed

    pre = evidence_for("PRE_BASELINE", require_all=True)
    if len(pre) < 2 or any(not item.get("committed") for item in pre):
        raise RuntimeQualificationError("PRE baseline is not fully committed")
    pre_first = int(pre[0]["full_start_ns"])
    pre_last = int(pre[-1]["full_start_ns"])
    pre_span = pre_last - pre_first
    if pre_span < int(contract["minimum_actual_pre_start_span_ns"]):
        raise RuntimeQualificationError(
            f"actual PRE full-rank START span {pre_span} is too short"
        )

    profile = str(context["qualification_profile"])
    event_role = "EVENT_BURST" if profile == BURST_PROFILE else "EVENT_PRESSURE"
    event = evidence_for(event_role, require_all=True)
    if not event or any(not item.get("committed") for item in event):
        raise RuntimeQualificationError(f"{event_role} is not fully committed")
    actual_onset = int(event[0]["full_start_ns"])
    actual_end = int(event[-1]["commit_ns"])
    scheduled_onset = int(contract["scheduled_onset_ns"])
    scheduled_end = int(contract["scheduled_end_ns"])
    onset_lag = actual_onset - scheduled_onset
    if not 0 <= onset_lag <= int(contract["maximum_pre_tail_to_actual_onset_ns"]):
        raise RuntimeQualificationError(
            f"actual application onset lag {onset_lag} is outside contract"
        )
    pre_tail_gap = actual_onset - pre_last
    if not 0 <= pre_tail_gap <= int(contract["maximum_pre_tail_to_actual_onset_ns"]):
        raise RuntimeQualificationError(
            f"PRE tail to actual application onset gap {pre_tail_gap} is invalid"
        )

    post = evidence_for("POST_BASELINE")
    if not post:
        raise RuntimeQualificationError("POST baseline has no full-rank START")
    horizon = int(context["horizon_ns"])
    post_first = int(post[0]["full_start_ns"])
    post_last = int(post[-1]["full_start_ns"])
    if post_first >= horizon:
        raise RuntimeQualificationError("POST baseline starts after the horizon")
    if horizon - post_last > int(contract["maximum_post_tail_gap_ns"]):
        raise RuntimeQualificationError(
            f"POST baseline tail gap {horizon - post_last} exceeds contract"
        )

    result: Dict[str, Any] = {
        "scheduled_application_onset_ns": scheduled_onset,
        "scheduled_application_end_ns": scheduled_end,
        "actual_application_onset_ns": actual_onset,
        "actual_application_end_ns": actual_end,
        "actual_application_window_authority": (
            "role_bound_collective_transaction"
        ),
        "pre_full_start_span_ns": pre_span,
        "pre_tail_to_actual_onset_ns": pre_tail_gap,
        "post_first_full_start_ns": post_first,
        "post_last_full_start_ns": post_last,
        "post_tail_gap_ns": horizon - post_last,
    }
    if profile == BURST_PROFILE:
        expected_count = int(contract["burst_layer_count"])
        if len(event) != expected_count:
            raise RuntimeQualificationError(
                f"allreduce burst transaction count {len(event)} != {expected_count}"
            )
        starts = [int(item["full_start_ns"]) for item in event]
        gaps = [right - left for left, right in zip(starts, starts[1:])]
        maximum_gap = max(gaps, default=0)
        span = actual_end - actual_onset
        if maximum_gap > int(contract["maximum_actual_burst_start_gap_ns"]):
            raise RuntimeQualificationError("allreduce burst START cadence is too sparse")
        if span > int(contract["maximum_actual_burst_span_ns"]):
            raise RuntimeQualificationError("allreduce burst actual span is too long")
        event_baseline = evidence_for("EVENT_BASELINE")
        if not event_baseline:
            raise RuntimeQualificationError("burst lacks sustained EVENT baseline")
        baseline_starts = [int(item["full_start_ns"]) for item in event_baseline]
        if baseline_starts[0] - actual_end > DEFAULT_MAX_SILENCE_NS:
            raise RuntimeQualificationError("baseline does not resume after burst")
        if scheduled_end - baseline_starts[-1] > DEFAULT_MAX_SILENCE_NS:
            raise RuntimeQualificationError("EVENT baseline does not reach scheduled end")
        if post_first - scheduled_end > int(
            contract["maximum_pre_tail_to_actual_onset_ns"]
        ):
            raise RuntimeQualificationError("POST baseline begins too late after event")
        result.update({
            "validated_burst_collective_count": len(event),
            "maximum_burst_start_gap_ns": maximum_gap,
            "actual_burst_span_ns": span,
            "event_baseline_full_start_count": len(event_baseline),
        })
        return result

    expected_pressure = int(contract["pressure_layer_count"])
    if len(event) != expected_pressure:
        raise RuntimeQualificationError(
            f"pressure transaction count {len(event)} != {expected_pressure}"
        )
    window_end = actual_onset + int(contract["fixed_actual_pressure_window_ns"])
    if actual_end < window_end:
        raise RuntimeQualificationError(
            "pressure roles do not cover the fixed actual application window"
        )
    idle_gaps: List[int] = []
    for left, right in zip(event, event[1:]):
        idle_gaps.append(int(right["full_start_ns"]) - int(left["commit_ns"]))
    if idle_gaps and (
        min(idle_gaps) < 0
        or max(idle_gaps) > int(contract["maximum_pressure_idle_gap_ns"])
    ):
        raise RuntimeQualificationError("pressure application has an excessive idle gap")
    if post_first < window_end:
        raise RuntimeQualificationError("POST baseline starts inside pressure window")

    series_by_endpoint = switch_evidence.get(
        "access_throughput_series_by_endpoint"
    )
    if not isinstance(series_by_endpoint, Mapping) or len(series_by_endpoint) != int(
        contract["required_access_endpoint_count"]
    ):
        raise RuntimeQualificationError("high-util ACCESS endpoint coverage is incomplete")
    threshold = float(contract["utilization_threshold"])
    minimum_coverage = float(contract["minimum_window_coverage_ratio"])
    endpoint_metrics: Dict[str, Any] = {}
    all_samples = 0
    all_qualifying = 0
    for key, series in series_by_endpoint.items():
        selected = [
            (timestamp, observed, configured)
            for timestamp, observed, configured in series
            if actual_onset < int(timestamp) <= window_end
        ]
        if not selected:
            raise RuntimeQualificationError(f"ACCESS endpoint {key} has no window samples")
        if any(
            observed is None or configured is None or int(configured) <= 0
            for _, observed, configured in selected
        ):
            raise RuntimeQualificationError(
                f"ACCESS endpoint {key} lacks throughput/configured bandwidth"
            )
        qualifying = sum(
            float(observed) / float(configured) >= threshold
            for _, observed, configured in selected
        )
        coverage = qualifying / len(selected)
        if coverage < minimum_coverage:
            raise RuntimeQualificationError(
                f"ACCESS endpoint {key} utilization coverage {coverage:.6f} "
                f"is below {minimum_coverage:.6f}"
            )
        label = ":".join(str(value) for value in key)
        endpoint_metrics[label] = {
            "sample_count": len(selected),
            "qualifying_sample_count": qualifying,
            "coverage_ratio": coverage,
        }
        all_samples += len(selected)
        all_qualifying += qualifying
    result.update({
        "validated_pressure_collective_count": len(event),
        "fixed_throughput_window_start_ns": actual_onset,
        "fixed_throughput_window_end_ns": window_end,
        "throughput_window_source": "actual_application_onset_from_transaction_role",
        "utilization_threshold": threshold,
        "minimum_window_coverage_ratio": minimum_coverage,
        "access_endpoint_count": len(endpoint_metrics),
        "access_sample_count": all_samples,
        "access_qualifying_sample_count": all_qualifying,
        "access_coverage_ratio": all_qualifying / all_samples,
        "endpoint_metrics": endpoint_metrics,
        "maximum_pressure_idle_gap_ns": max(idle_gaps, default=0),
    })
    return result


def _read_collective_flows(
    path: Path, context: Mapping[str, Any], contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    reader = _exact_reader(path, COLLECTIVE_FLOW_COLUMNS, "collective_telemetry.csv")
    ranks: set[int] = set()
    flow_ids: set[str] = set()
    row_count = 0
    first_start: Optional[int] = None
    last_finish: Optional[int] = None
    try:
        for row in reader:
            row_count += 1
            if row["run_id"] != context["run_id"]:
                raise RuntimeQualificationError("collective flow contains foreign run_id")
            flow_id = row["collective_id"]
            if not flow_id or flow_id in flow_ids:
                raise RuntimeQualificationError("collective sender-flow id is empty/duplicate")
            flow_ids.add(flow_id)
            if row["iteration_id"] or row["layer_id"]:
                raise RuntimeQualificationError(
                    "per-flow telemetry fabricates unavailable layer/iteration attribution"
                )
            if row["collective_type"] != "ALLREDUCE" or row["status"] != "ok":
                raise RuntimeQualificationError("collective sender-flow status/type is invalid")
            rank = int(_uint(row["rank_id"], "flow rank_id"))
            world = int(_uint(row["world_size"], "flow world_size"))
            message = int(_uint(row["message_size_bytes"], "flow message_size_bytes"))
            start = int(_uint(row["start_time_ns"], "flow start_time_ns"))
            finish = int(_uint(row["finish_time_ns"], "flow finish_time_ns"))
            duration = int(_uint(row["duration_ns"], "flow duration_ns"))
            if rank not in range(contract.world_size) or world != contract.world_size:
                raise RuntimeQualificationError("collective sender-flow rank/world mismatch")
            if message <= 0 or finish < start or duration != finish - start:
                raise RuntimeQualificationError("collective sender-flow is non-causal")
            if finish > int(context["horizon_ns"]):
                raise RuntimeQualificationError("collective sender-flow finishes after horizon")
            ranks.add(rank)
            first_start = start if first_start is None else min(first_start, start)
            last_finish = finish if last_finish is None else max(last_finish, finish)
    finally:
        _close_reader(reader)
    expected = set(range(contract.world_size))
    if row_count == 0 or ranks != expected:
        raise RuntimeQualificationError(
            f"collective sender-flow rank coverage={sorted(ranks)}, expected={sorted(expected)}"
        )
    return {
        "row_count": row_count,
        "rank_ids": sorted(ranks),
        "semantics": "completed_sender_flow_not_layer_collective",
        "first_completed_flow_start_ns": first_start,
        "last_flow_finish_ns": last_finish,
    }


def _read_lifecycle(
    path: Path, context: Mapping[str, Any], contract: RuntimeValidationContract,
) -> Dict[str, Any]:
    reader = _exact_reader(path, LIFECYCLE_COLUMNS, "run_lifecycle.csv")
    rows: List[Dict[str, str]] = []
    try:
        for row in reader:
            if row["run_id"] != context["run_id"]:
                raise RuntimeQualificationError("lifecycle contains foreign run_id")
            rows.append(dict(row))
    finally:
        _close_reader(reader)
    if any(row["status"] == "WORKLOAD_COMPLETE" for row in rows):
        raise RuntimeQualificationError(
            "horizon-prefix run completed the declared workload before observation stop"
        )
    horizon_rows = [row for row in rows if row["event"] == "observation_horizon"]
    if len(horizon_rows) != 1:
        raise RuntimeQualificationError("lifecycle must contain exactly one horizon row")
    row = horizon_rows[0]
    scheduled = int(_uint(row["scheduled_ns"], "lifecycle scheduled_ns"))
    actual = int(_uint(row["actual_ns"], "lifecycle actual_ns"))
    finished = int(_uint(row["finished_ranks"], "lifecycle finished_ranks"))
    world = int(_uint(row["world_size"], "lifecycle world_size"))
    if scheduled != context["horizon_ns"] or actual != context["horizon_ns"]:
        raise RuntimeQualificationError("lifecycle horizon is not exact")
    if world != contract.world_size or not 0 <= finished < contract.world_size:
        raise RuntimeQualificationError("lifecycle rank counts contradict incomplete workload")
    if row["status"] != "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE":
        raise RuntimeQualificationError("horizon-prefix lifecycle status is invalid")
    return {
        "scheduled_ns": scheduled,
        "actual_ns": actual,
        "finished_ranks": finished,
        "world_size": world,
        "status": row["status"],
    }


def validate_runtime_qualification(
    *,
    run: Mapping[str, Any],
    workload_report: Mapping[str, Any],
    paths: RuntimePaths,
    causal_warmup_ns: int = DEFAULT_CAUSAL_WARMUP_NS,
    contract_override: Optional[RuntimeValidationContract] = None,
) -> Dict[str, Any]:
    """Validate one executed run and return a hash-bound PASS/FAIL report."""

    run_id = str(run.get("run_id", ""))
    checks: List[Dict[str, Any]] = []
    errors: List[str] = []
    evidence: Dict[str, Any] = {}
    source_artifacts: Dict[str, Dict[str, str]] = {}
    for name, path in asdict(paths).items():
        if path is None:
            continue
        resolved = Path(path)
        try:
            source_artifacts[name] = {
                "path": resolved.name,
                "sha256": sha256_file(resolved),
            }
        except OSError as exc:
            source_artifacts[name] = {"path": resolved.name, "sha256": ""}
            errors.append(f"{name} missing/unreadable: {exc}")
    expected_workload_sha = str(workload_report.get("sha256", ""))
    observed_workload_sha = source_artifacts.get("workload", {}).get("sha256")
    if observed_workload_sha and observed_workload_sha != expected_workload_sha:
        errors.append(
            "workload source hash differs from the static qualification report"
        )
        checks.append({
            "name": "workload_source_matches_static_report",
            "status": FAIL,
            "error": (
                f"expected={expected_workload_sha}, "
                f"observed={observed_workload_sha}"
            ),
        })
    elif observed_workload_sha:
        checks.append({
            "name": "workload_source_matches_static_report",
            "status": PASS,
            "evidence": {"sha256": observed_workload_sha},
        })

    def phase(name: str, operation: Any) -> Any:
        try:
            result = operation()
            report_evidence = _phase_report_evidence(name, result)
            checks.append({
                "name": name, "status": PASS, "evidence": report_evidence
            })
            evidence[name] = report_evidence
            return result
        except (RuntimeQualificationError, OSError, csv.Error, KeyError, TypeError, ValueError) as exc:
            message = f"{name}: {exc}"
            errors.append(message)
            checks.append({"name": name, "status": FAIL, "error": str(exc)})
            return None

    derived_contract: Optional[RuntimeValidationContract] = contract_override
    contract_source = (
        "explicit_test_override"
        if contract_override is not None
        else "frozen_static_workload_report"
    )
    if derived_contract is None:
        derived_contract = phase(
            "static_workload_runtime_contract",
            lambda: _derive_contract(workload_report, causal_warmup_ns),
        )
    else:
        checks.append({
            "name": "explicit_test_runtime_contract",
            "status": PASS,
            "evidence": asdict(derived_contract),
        })
    context: Optional[Dict[str, Any]] = None
    topology: Optional[Dict[str, Any]] = None
    switch: Optional[Dict[str, Any]] = None
    nic: Optional[Dict[str, Any]] = None
    roles: Optional[Dict[str, Any]] = None
    transactions: Optional[Dict[str, Any]] = None
    if derived_contract is not None:
        context = phase(
            "planned_run_profile_and_horizon",
            lambda: _validate_context(run, workload_report, derived_contract),
        )
        topology = phase(
            "physical_link_map_contract",
            lambda: _read_link_map(paths.link_map, derived_contract),
        )
        if context is not None and context.get("qualification_profile") in OVERRIDE_PROFILES:
            role_path = paths.collective_layer_roles
            if role_path is None:
                errors.append("override profile lacks collective layer role sidecar")
                checks.append({
                    "name": "collective_override_role_and_workload_binding",
                    "status": FAIL,
                    "error": "role sidecar path is missing",
                })
            else:
                expected_role_sha = workload_report.get("role_sha256")
                observed_role_sha = source_artifacts.get(
                    "collective_layer_roles", {}
                ).get("sha256")
                if observed_role_sha != expected_role_sha:
                    errors.append(
                        "collective role source hash differs from static report"
                    )
                    checks.append({
                        "name": "collective_role_source_matches_static_report",
                        "status": FAIL,
                        "error": (
                            f"expected={expected_role_sha}, "
                            f"observed={observed_role_sha}"
                        ),
                    })
                else:
                    checks.append({
                        "name": "collective_role_source_matches_static_report",
                        "status": PASS,
                        "evidence": {"sha256": observed_role_sha},
                    })
                    roles = phase(
                        "collective_override_role_and_workload_binding",
                        lambda: _read_override_roles(
                            role_path, paths.workload, context, workload_report
                        ),
                    )
    if context is not None and topology is not None and derived_contract is not None:
        switch = phase(
            "exact_switch_snapshot_coverage",
            lambda: _read_switch_snapshots(
                paths.switch_telemetry, context, derived_contract, topology
            ),
        )
        nic = phase(
            "exact_nic_snapshot_coverage",
            lambda: _read_nic_snapshots(
                paths.nic_telemetry, context, derived_contract, topology
            ),
        )
        transactions = phase(
            "collective_transaction_state_machine_and_cadence",
            lambda: _read_collective_transactions(
                paths.collective_transaction, context, derived_contract
            ),
        )
        phase(
            "completed_sender_flow_causality",
            lambda: _read_collective_flows(
                paths.collective_telemetry, context, derived_contract
            ),
        )
        phase(
            "exact_incomplete_horizon_lifecycle",
            lambda: _read_lifecycle(paths.run_lifecycle, context, derived_contract),
        )
        if (
            context.get("qualification_profile") in OVERRIDE_PROFILES
            and roles is not None
            and transactions is not None
        ):
            phase(
                "collective_override_pre_event_post_runtime",
                lambda: _validate_override_application_lifecycle(
                    roles, transactions, switch, context, workload_report
                ),
            )
    if (
        context is not None and derived_contract is not None
        and switch is not None and nic is not None
    ):
        phase(
            "healthy_or_pre_event_access_traffic_cadence",
            lambda: _validate_activity(
                switch, nic, context, derived_contract
            ),
        )
    if errors and not any(item["name"] == "source_artifact_availability" for item in checks):
        # Missing source files are reported even when later phases cannot run.
        missing = [name for name, ref in source_artifacts.items() if not ref["sha256"]]
        if missing:
            checks.insert(0, {
                "name": "source_artifact_availability",
                "status": FAIL,
                "error": f"missing={missing}",
            })
    status = PASS if not errors and all(item["status"] == PASS for item in checks) else FAIL
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "run_id": run_id,
        "qualification_profile": workload_report.get("qualification_profile"),
        "contract_source": contract_source,
        "planned_run_sha256": canonical_hash(run),
        "static_workload_report_sha256": canonical_hash(workload_report),
        "contract": asdict(derived_contract) if derived_contract is not None else None,
        "source_artifacts": source_artifacts,
        "checks": _json_ready(checks),
        "errors": errors,
        "evidence": _json_ready(evidence),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-json", required=True, type=Path)
    parser.add_argument("--workload-report-json", required=True, type=Path)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--link-map", required=True, type=Path)
    parser.add_argument("--switch-telemetry", required=True, type=Path)
    parser.add_argument("--nic-telemetry", required=True, type=Path)
    parser.add_argument("--collective-transaction", required=True, type=Path)
    parser.add_argument("--collective-telemetry", required=True, type=Path)
    parser.add_argument("--run-lifecycle", required=True, type=Path)
    parser.add_argument("--collective-layer-roles", type=Path)
    parser.add_argument("--causal-warmup-ns", type=int, default=DEFAULT_CAUSAL_WARMUP_NS)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    run = json.loads(args.run_json.read_text(encoding="utf-8"))
    workload_report = json.loads(
        args.workload_report_json.read_text(encoding="utf-8")
    )
    result = validate_runtime_qualification(
        run=run,
        workload_report=workload_report,
        paths=RuntimePaths(
            workload=args.workload,
            link_map=args.link_map,
            switch_telemetry=args.switch_telemetry,
            nic_telemetry=args.nic_telemetry,
            collective_transaction=args.collective_transaction,
            collective_telemetry=args.collective_telemetry,
            run_lifecycle=args.run_lifecycle,
            collective_layer_roles=args.collective_layer_roles,
        ),
        causal_warmup_ns=args.causal_warmup_ns,
    )
    rendered = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0 if result["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
