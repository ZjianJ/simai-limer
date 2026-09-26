#!/usr/bin/env python3
"""Pure helpers for the unified related-work benchmark.

The functions in this module intentionally separate three kinds of evidence:

* measurements/replay derived from SimAI timestamps;
* timing projections parameterized from a paper;
* executable protocol invariants that have no timing claim.

Keeping those categories separate is part of the benchmark contract.  A
paper-parameter projection must never be reported as a measured SimAI event.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd


PASS = "PASS"
FAIL = "FAIL"
UNVERIFIED = "UNVERIFIED"
UNSUPPORTED = "UNSUPPORTED"
NOT_APPLICABLE = "NOT_APPLICABLE"
BLOCKED_PRECONDITION = "BLOCKED_PRECONDITION"


def optional_int(value: Any) -> Optional[int]:
    """Return an integer for a populated scalar, otherwise ``None``."""

    if value is None or pd.isna(value):
        return None
    return int(value)


def next_epoch_strict(timestamp_ns: int, period_ns: int) -> int:
    """Return the first periodic boundary strictly after ``timestamp_ns``.

    Strictly-after avoids granting a zero-time observation when a packet event
    and a periodic counter exchange happen at the same virtual timestamp but
    their event ordering is not represented in the CSV trace.
    """

    if timestamp_ns < 0 or period_ns <= 0:
        raise ValueError("timestamps must be non-negative and period positive")
    return (timestamp_ns // period_ns + 1) * period_ns


def strict_deadline_status(latency_ns: Optional[int], deadline_ns: int) -> str:
    if latency_ns is None:
        return UNVERIFIED
    return PASS if latency_ns < deadline_ns else FAIL


def source_row(indexed: Dict[str, pd.Series], fault_id: str) -> Optional[pd.Series]:
    row = indexed.get(fault_id)
    if row is None or not bool(row.get("detected", False)):
        return None
    if optional_int(row.get("alarm_time_ns")) is None:
        return None
    return row


def index_detector_rows(detections: pd.DataFrame, detector: str) -> Dict[str, pd.Series]:
    rows = detections[detections["detector"] == detector]
    if rows["fault_id"].duplicated().any():
        duplicates = sorted(rows.loc[rows["fault_id"].duplicated(), "fault_id"].unique())
        raise ValueError(f"duplicate {detector} rows for {duplicates[:3]}")
    return {str(row["fault_id"]): row for _, row in rows.iterrows()}


def canonical_detection_row(
    event: pd.Series,
    method: str,
    layer: str,
    fidelity: str,
    supported: bool,
    signal_time_ns: Optional[int],
    delivered_time_ns: Optional[int],
    localized: Optional[bool],
    platform_executed: bool,
    delivery_platform_verified: bool,
    reason: str,
    timing_semantics: str = "event_time",
    alarm_generated: Optional[bool] = None,
) -> Dict[str, Any]:
    deadline = int(event["detection_deadline_ns"])
    fault_start = int(event["fault_start_ns"])
    signal_latency = None if signal_time_ns is None else signal_time_ns - fault_start
    delivery_latency = None if delivered_time_ns is None else delivered_time_ns - fault_start
    signal_status = (UNSUPPORTED if not supported else
                     strict_deadline_status(signal_latency, deadline))
    actionable_status = (UNSUPPORTED if not supported else
                         strict_deadline_status(delivery_latency, deadline))
    strict_platform_status = actionable_status
    if actionable_status == PASS and not delivery_platform_verified:
        strict_platform_status = UNVERIFIED
    return {
        "run_id": event["run_id"],
        "fault_id": event["fault_id"],
        "fault_kind": event["fault_kind"],
        "fault_group": event["fault_group"],
        "target_link_id": event["target_link_id"],
        "event_observable": bool(event["event_observable"]),
        "method": method,
        "layer": layer,
        "fidelity": fidelity,
        "supported": bool(supported),
        "signal_time_ns": signal_time_ns,
        "delivered_alarm_time_ns": delivered_time_ns,
        "signal_latency_ns": signal_latency,
        "actionable_detection_latency_ns": delivery_latency,
        "detection_deadline_ns": deadline,
        "signal_timing_status": signal_status,
        "actionable_timing_status": actionable_status,
        "strict_platform_status": strict_platform_status,
        "localized_target_port": localized,
        "platform_executed": bool(platform_executed),
        "delivery_platform_verified": bool(delivery_platform_verified),
        "alarm_generated": bool(
            signal_time_ns is not None or delivered_time_ns is not None
            if alarm_generated is None else alarm_generated),
        "timing_semantics": timing_semantics,
        "reason": reason,
    }


def index_fancy_counter_signals(
    events: pd.DataFrame,
    feature_samples: Optional[pd.DataFrame],
) -> Dict[str, Dict[str, Any]]:
    """Index target-link paired-counter evidence without using LIMER alarms.

    ``drop_error_delta`` is the only current trace feature that corresponds to
    FANcY's upstream/downstream packet-count mismatch.  Queue, bandwidth EWMA,
    link latches, and ground-truth severity are intentionally excluded.  A
    target is uniquely localized only when it is the sole maximum positive
    counter delta at the first target evidence timestamp.
    """

    if feature_samples is None or feature_samples.empty:
        return {}
    required = {"run_id", "timestamp_ns", "link_id", "drop_error_delta"}
    missing = required - set(feature_samples.columns)
    if missing:
        raise ValueError(f"FANcY feature samples missing columns: {sorted(missing)}")

    samples = feature_samples[list(required)].copy()
    samples["drop_error_delta"] = pd.to_numeric(
        samples["drop_error_delta"], errors="coerce").fillna(0.0)
    samples = samples[samples["drop_error_delta"] > 0]
    by_run = {str(run_id): group for run_id, group in samples.groupby("run_id")}
    indexed: Dict[str, Dict[str, Any]] = {}
    for _, event in events.iterrows():
        group = by_run.get(str(event["run_id"]))
        if group is None:
            continue
        start = int(event["fault_start_ns"])
        deadline = int(event["detection_deadline_ns"])
        window = group[(group["timestamp_ns"] >= start)
                       & (group["timestamp_ns"] < start + deadline)]
        target = window[window["link_id"] == str(event["target_link_id"])]
        if target.empty:
            continue
        first_time = int(target["timestamp_ns"].min())
        simultaneous = window[window["timestamp_ns"] == first_time]
        maximum = float(simultaneous["drop_error_delta"].max())
        winners = set(simultaneous.loc[
            simultaneous["drop_error_delta"] == maximum, "link_id"].astype(str))
        indexed[str(event["fault_id"])] = {
            "signal_time_ns": first_time,
            "localized_target_port": (
                winners == {str(event["target_link_id"])}),
            "simultaneous_positive_links": int(simultaneous["link_id"].nunique()),
        }
    return indexed


def build_detection_timeline(
    events: pd.DataFrame,
    detections: pd.DataFrame,
    config: Dict[str, Any],
    feature_samples: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build one canonical detection ledger for measured and projected methods."""

    switch = index_detector_rows(detections, "switch_sparse")
    qghmm = index_detector_rows(detections, "qghmm_quantized")
    host = index_detector_rows(detections, "host_telemetry")
    fancy_counter = index_fancy_counter_signals(events, feature_samples)
    models = config["models"]
    rows: List[Dict[str, Any]] = []

    for _, event in events.iterrows():
        fault_id = str(event["fault_id"])

        # Existing LIMER timestamps were generated by offline causal replay of
        # real SimAI telemetry.  Signal exposure is measured, delivery is not.
        for method, detector_index in [
            ("LIMER-switch-sparse", switch),
            ("LIMER-QG-HMM", qghmm),
        ]:
            raw = source_row(detector_index, fault_id)
            signal = optional_int(raw.get("alarm_time_ns")) if raw is not None else None
            localized = (bool(raw.get("top1_unique_correct"))
                         if raw is not None and not pd.isna(raw.get("top1_unique_correct"))
                         else None)
            rows.append(canonical_detection_row(
                event, method, "switch", "offline_causal_trace_replay", True,
                signal, None, localized, True, False,
                "simulated port signal replayed causally; no online AlarmBus timestamp",
            ))

        # FANcY dedicated-counter cadence projection.  Only actual packet-count
        # mismatch samples are used; no LIMER queue/rate/latch alarm is reused.
        # The residual term calibrates the paper's approximate 70 ms mean and
        # is therefore not an exact per-event implementation timestamp.
        fancy = models["fancy"]
        supported = event["fault_kind"] in fancy["supported_fault_kinds"]
        raw = fancy_counter.get(fault_id) if supported else None
        first = optional_int(raw.get("signal_time_ns")) if raw is not None else None
        delivered = None
        if first is not None:
            delivered = (next_epoch_strict(first, int(fancy["counter_exchange_ns"]))
                         + int(fancy["mean_residual_calibration_ns"]))
        localized = (bool(raw["localized_target_port"]) if raw is not None else None)
        rows.append(canonical_detection_row(
            event, "FANcY-dedicated-cadence", "switch", fancy["fidelity"], supported,
            first, delivered, localized, False, False,
            ("target-link packet-count mismatch plus 50 ms exchange cadence and "
             "paper-mean residual calibration; P4 session FSM is not executed"
             if supported else fancy["unsupported_reason"]),
            "raw_counter_signal_plus_paper_mean_projection",
        ))

        # Trumpet is a trigger framework, so the RDMA predicates used here are
        # our port/host telemetry proxy.  The timing is rounded to its trigger
        # epoch; no DPDK CPU or controller channel is claimed.
        trumpet = models["trumpet"]
        supported = event["fault_kind"] in trumpet["supported_fault_kinds"]
        raw = source_row(host, fault_id) if supported else None
        first = optional_int(raw.get("alarm_time_ns")) if raw is not None else None
        trigger_time = (next_epoch_strict(first, int(trumpet["trigger_epoch_ns"]))
                        if first is not None else None)
        delivered = (trigger_time + int(trumpet["controller_delivery_projection_ns"])
                     if trigger_time is not None else None)
        localized = (bool(raw.get("top1_unique_correct"))
                     if raw is not None and not pd.isna(raw.get("top1_unique_correct"))
                     else None)
        rows.append(canonical_detection_row(
            event, "Trumpet-10ms-trigger", "host", trumpet["fidelity"], supported,
            trigger_time, delivered, localized, False, False,
            "host telemetry predicate rounded to 10 ms plus a 0.75 ms controller projection",
            "proxy_predicate_plus_paper_timing_projection",
        ))

        # NetBouncer's production cadence is orders of magnitude longer than
        # each trace.  Giving every observable supported event a modeled alarm
        # is deliberately favorable; it still fails the target deadlines.
        netbouncer = models["netbouncer"]
        supported = event["fault_kind"] in netbouncer["supported_fault_kinds"]
        delivered = (next_epoch_strict(int(event["fault_start_ns"]),
                                       int(netbouncer["probe_epoch_ns"]))
                     + int(netbouncer["processor_mean_ns"])) if supported else None
        rows.append(canonical_detection_row(
            event, "NetBouncer-cadence-lower-bound", "active-probe",
            netbouncer["fidelity"], supported, delivered, delivered,
            None, False, False,
            ("counterfactual earliest result: next 5 minute probe epoch plus mean "
             "processing; no probes or solver ran, so this is not an alarm"
             if supported else netbouncer["unsupported_reason"]),
            "counterfactual_cadence_lower_bound",
            False,
        ))

        # Timer references expose why post-WC recovery timing must not be used
        # as failure-to-detection timing.  These are explicit configured
        # references rather than the old compressed 4 ms Python proxies.
        for method, key, layer in [
            ("RDMA-error-reference", "rdma_reference", "rdma"),
            ("NCCL-error-reference", "nccl_reference", "collective"),
        ]:
            model = models[key]
            timeout_ns = int(model["error_exposure_ns"])
            duration_ns = int(event["fault_end_ns"]) - int(event["fault_start_ns"])
            continuous = duration_ns >= timeout_ns
            delivered = (int(event["fault_start_ns"]) + timeout_ns
                         if continuous else None)
            rows.append(canonical_detection_row(
                event, method, layer, model["fidelity"], True,
                delivered, delivered, None, False, False,
                (model["source_parameter"] if continuous else
                 "the scheduled fault clears before the configured no-progress timer; no error is exposed"),
            ))

    return pd.DataFrame(rows)


def aggregate_detection(timeline: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for (method, scope), group in timeline.groupby(["method", "fault_group"], sort=True):
        supported = group[group["supported"]]
        observable = supported[supported["event_observable"]]
        scheduled_signaled = supported[supported["signal_latency_ns"].notna()]
        scheduled_timed = supported[
            supported["actionable_detection_latency_ns"].notna()]
        signaled = observable[observable["signal_latency_ns"].notna()]
        timed = observable[observable["actionable_detection_latency_ns"].notna()]
        signal_latencies = pd.to_numeric(signaled["signal_latency_ns"], errors="coerce")
        latencies = pd.to_numeric(timed["actionable_detection_latency_ns"], errors="coerce")
        rows.append({
            "method": method,
            "scope": scope,
            "scheduled_events": len(group),
            "supported_events": len(supported),
            "scheduled_signals": len(scheduled_signaled),
            "scheduled_signal_timing_passes": int(
                (supported["signal_timing_status"] == PASS).sum()),
            "scheduled_signal_timing_pass_rate": (
                float((supported["signal_timing_status"] == PASS).sum() / len(group))
                if len(group) else None),
            "scheduled_timing_evidence": len(scheduled_timed),
            "scheduled_actionable_timing_passes": int(
                (supported["actionable_timing_status"] == PASS).sum()),
            "scheduled_actionable_timing_pass_rate": (
                float((supported["actionable_timing_status"] == PASS).sum() / len(group))
                if len(group) else None),
            "scheduled_strict_platform_passes": int(
                (supported["strict_platform_status"] == PASS).sum()),
            "scheduled_strict_platform_pass_rate": (
                float((supported["strict_platform_status"] == PASS).sum() / len(group))
                if len(group) else None),
            "observable_supported_events": len(observable),
            "observable_signals": len(signaled),
            "observable_signal_timing_passes": int(
                (observable["signal_timing_status"] == PASS).sum()),
            "observable_signal_timing_pass_rate": (
                float((observable["signal_timing_status"] == PASS).mean())
                if len(observable) else None),
            "timing_evidence_rows": len(timed),
            "generated_alarm_rows": int(observable["alarm_generated"].sum()),
            "observable_timing_passes": int((observable["actionable_timing_status"] == PASS).sum()),
            "observable_timing_pass_rate": (
                float((observable["actionable_timing_status"] == PASS).mean())
                if len(observable) else None),
            "strict_platform_passes": int((observable["strict_platform_status"] == PASS).sum()),
            "strict_platform_pass_rate": (
                float((observable["strict_platform_status"] == PASS).mean())
                if len(observable) else None),
            "localization_pass_rate": (
                float(observable["localized_target_port"].dropna().astype(bool).mean())
                if observable["localized_target_port"].notna().any() else None),
            "signal_latency_median_ns": (
                float(signal_latencies.median()) if len(signal_latencies) else None),
            "signal_latency_p95_ns": (
                float(signal_latencies.quantile(.95)) if len(signal_latencies) else None),
            "signal_latency_max_ns": (
                float(signal_latencies.max()) if len(signal_latencies) else None),
            "latency_median_ns": float(latencies.median()) if len(latencies) else None,
            "latency_p95_ns": float(latencies.quantile(.95)) if len(latencies) else None,
            "latency_max_ns": float(latencies.max()) if len(latencies) else None,
            "platform_delivery_verified": bool(observable["delivery_platform_verified"].all())
                if len(observable) else False,
        })
    return pd.DataFrame(rows)


def optcc_theorem13_normalized_ratio(
    server_slowdown: float,
    gpu_count: int = 16,
    gpus_per_server: int = 4,
) -> float:
    """Evaluate OptCC Theorem 13 normalized by its healthy-ring ``T0``.

    The theorem states ``T >= n/g * max(2*l*(q-1)/(l*(q-2)+2), l)`` for
    ``q=p/g`` servers.  Dividing by ``T0=2*(p-1)*n/(g*p)`` gives this raw
    ratio.  It may be below one, so callers should report both the raw theorem
    value and a separate healthy-performance floor rather than silently calling
    a truncated approximation a measured runtime or a universal lower bound.
    """

    if server_slowdown < 1:
        raise ValueError("server_slowdown must be at least one")
    if (gpu_count <= 1 or not 1 <= gpus_per_server <= gpu_count
            or gpu_count % gpus_per_server != 0):
        raise ValueError("invalid GPU geometry")
    server_count = gpu_count / gpus_per_server
    if server_count <= 2:
        raise ValueError("Theorem 13 expression requires more than two servers")
    first = (2.0 * server_slowdown * (server_count - 1.0)
             / (server_slowdown * (server_count - 2.0) + 2.0))
    theorem_term = max(first, server_slowdown)
    return gpu_count * theorem_term / (2.0 * (gpu_count - 1.0))


def rank_from_access_link(link_id: str, world_size: int) -> int:
    match = re.match(r"^L(\d+)-", str(link_id))
    if not match:
        raise ValueError(f"cannot derive rank from link_id={link_id!r}")
    rank = int(match.group(1))
    if not 0 <= rank < world_size:
        raise ValueError(f"link {link_id!r} maps outside world size {world_size}")
    return rank


def vector_digest(vector: Iterable[int]) -> str:
    payload = json.dumps(list(vector), separators=(",", ":")).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _payloads(world_size: int, width: int) -> List[List[int]]:
    return [[(rank + 1) * 1009 + (column + 3) * 17
             for column in range(width)] for rank in range(world_size)]


def _sum_vectors(vectors: Iterable[Iterable[int]]) -> List[int]:
    vectors = [list(vector) for vector in vectors]
    if not vectors:
        return []
    return [sum(vector[column] for vector in vectors)
            for column in range(len(vectors[0]))]


class CollectiveProtocolError(RuntimeError):
    """Base class for a rejected collective state transition."""


class StaleEpochError(CollectiveProtocolError):
    pass


class DuplicateContributionError(CollectiveProtocolError):
    pass


class IncompleteCollectiveError(CollectiveProtocolError):
    pass


class ClosedAttemptError(CollectiveProtocolError):
    pass


class EpochCollectiveGuard:
    """Small executable transaction guard for one 16-rank reduction.

    It is deliberately transport-agnostic: an ACCESS failure aborts an
    attempt, a surviving path is made active by the caller, and immutable rank
    contributions are replayed into a fresh epoch.  Every state transition is
    checked, which lets tests demonstrate rejection of incomplete commits,
    late old-epoch writes, and duplicate replay contributions.
    """

    def __init__(self, world_size: int, vector_width: int):
        if world_size <= 1 or vector_width <= 0:
            raise ValueError("invalid collective dimensions")
        self.world_size = world_size
        self.vector_width = vector_width
        self.epoch = 0
        self.state = "OPEN"
        self.seen: set[int] = set()
        self.scratch = [0] * vector_width
        self.aborted_epochs: List[int] = []
        self.commit_count = 0
        self.committed_digest: Optional[str] = None

    def contribute(self, rank: int, vector: Sequence[int], epoch: int) -> None:
        if epoch != self.epoch:
            raise StaleEpochError(f"write for epoch {epoch}, active epoch is {self.epoch}")
        if self.state != "OPEN":
            raise ClosedAttemptError(f"cannot contribute while state={self.state}")
        if not 0 <= rank < self.world_size:
            raise ValueError(f"rank {rank} outside world size {self.world_size}")
        if rank in self.seen:
            raise DuplicateContributionError(f"rank {rank} already contributed")
        if len(vector) != self.vector_width:
            raise ValueError("contribution width mismatch")
        self.seen.add(rank)
        self.scratch = [left + int(right)
                        for left, right in zip(self.scratch, vector)]

    def abort(self, epoch: int) -> None:
        if epoch != self.epoch:
            raise StaleEpochError(f"abort for epoch {epoch}, active epoch is {self.epoch}")
        if self.state != "OPEN":
            raise ClosedAttemptError(f"cannot abort while state={self.state}")
        self.state = "ABORTED"
        self.aborted_epochs.append(epoch)

    def begin_redo(self) -> int:
        if self.state != "ABORTED":
            raise ClosedAttemptError("redo requires an aborted attempt")
        self.epoch += 1
        self.state = "OPEN"
        self.seen = set()
        self.scratch = [0] * self.vector_width
        return self.epoch

    def commit(self, epoch: int) -> str:
        if epoch != self.epoch:
            raise StaleEpochError(f"commit for epoch {epoch}, active epoch is {self.epoch}")
        if self.state != "OPEN":
            raise ClosedAttemptError(f"cannot commit while state={self.state}")
        if len(self.seen) != self.world_size:
            raise IncompleteCollectiveError(
                f"received {len(self.seen)}/{self.world_size} contributions")
        self.state = "COMMITTED"
        self.commit_count += 1
        self.committed_digest = vector_digest(self.scratch)
        return self.committed_digest


def collective_safety_cases(
    events: pd.DataFrame,
    world_size: int = 16,
    vector_width: int = 32,
) -> pd.DataFrame:
    """Execute an epoch-tagged abort/redo invariant model for every fault.

    The model represents the minimum semantics required by the project, not
    SimAI's current flow telemetry and not a faithful ReCoVer implementation.
    It uses exact integer reductions so a digest mismatch is unambiguous.
    """

    phases = ("before_reduce", "mid_reduce", "after_reduce_before_commit")
    payloads = _payloads(world_size, vector_width)
    reference = _sum_vectors(payloads)
    expected_digest = vector_digest(reference)
    rows: List[Dict[str, Any]] = []

    for _, event in events.iterrows():
        failed_rank = rank_from_access_link(str(event["target_link_id"]), world_size)
        for phase in phases:
            guard = EpochCollectiveGuard(world_size, vector_width)
            old_epoch = guard.epoch
            if phase == "before_reduce":
                prefix = 0
            elif phase == "mid_reduce":
                prefix = 1 + ((failed_rank + int(event["fault_start_ns"]))
                              % (world_size - 1))
            else:
                prefix = world_size
            for rank in range(prefix):
                guard.contribute(rank, payloads[rank], old_epoch)
            partial_digest = (vector_digest(guard.scratch) if prefix else None)

            # A faulted attempt must be unable to publish, whether it is
            # incomplete or has all data but has not crossed the commit point.
            faulted_epoch_commit_rejected = False
            if prefix < world_size:
                try:
                    guard.commit(old_epoch)
                except IncompleteCollectiveError:
                    faulted_epoch_commit_rejected = True
                guard.abort(old_epoch)
            else:
                guard.abort(old_epoch)
                try:
                    guard.commit(old_epoch)
                except ClosedAttemptError:
                    faulted_epoch_commit_rejected = True

            redo_epoch = guard.begin_redo()
            stale_old_epoch_write_rejected = False
            try:
                guard.contribute(0, payloads[0], old_epoch)
            except StaleEpochError:
                stale_old_epoch_write_rejected = True

            # Rotate replay order so the failed rank's contribution is carried
            # by the modeled surviving path at a non-constant point.
            replay_order = list(range(failed_rank, world_size)) + list(range(failed_rank))
            duplicate_redo_write_rejected = False
            for position, rank in enumerate(replay_order):
                guard.contribute(rank, payloads[rank], redo_epoch)
                if position == 0:
                    try:
                        guard.contribute(rank, payloads[rank], redo_epoch)
                    except DuplicateContributionError:
                        duplicate_redo_write_rejected = True
            observed_digest = guard.commit(redo_epoch)
            all_ranks_agree = observed_digest == expected_digest
            contribution_count = len(guard.seen)
            contribution_ids = len(set(guard.seen))
            exactly_once = (contribution_count == world_size == contribution_ids
                            and duplicate_redo_write_rejected)
            wrong_result_published = (guard.commit_count != 1
                                      or observed_digest != expected_digest)
            safe = (
                faulted_epoch_commit_rejected
                and stale_old_epoch_write_rejected
                and duplicate_redo_write_rejected
                and failed_rank in guard.seen
                and all_ranks_agree
                and exactly_once
                and not wrong_result_published
            )
            rows.append({
                "run_id": event["run_id"],
                "fault_id": event["fault_id"],
                "fault_kind": event["fault_kind"],
                "target_link_id": event["target_link_id"],
                "fault_start_ns": int(event["fault_start_ns"]),
                "failed_rank": failed_rank,
                "injection_phase": phase,
                "world_size": world_size,
                "old_epoch": old_epoch,
                "primary_attempt_contribution_count": prefix,
                "old_epoch_aborted": old_epoch in guard.aborted_epochs,
                "faulted_epoch_commit_rejected": faulted_epoch_commit_rejected,
                "redo_epoch": redo_epoch,
                "decision": "ABORT_REPLAY_COMPLETE_CORRECT",
                "contribution_count": contribution_count,
                "unique_contribution_count": contribution_ids,
                "partial_digest": partial_digest,
                "expected_digest": expected_digest,
                "observed_digest": observed_digest,
                "all_ranks_agree": all_ranks_agree,
                "exactly_once_contributions": exactly_once,
                "stale_old_epoch_write_rejected": stale_old_epoch_write_rejected,
                "duplicate_redo_write_rejected": duplicate_redo_write_rejected,
                "failed_rank_contribution_replayed": failed_rank in guard.seen,
                "commit_count": guard.commit_count,
                "wrong_result_published": wrong_result_published,
                "correctness_status": PASS if safe else FAIL,
                "fidelity": "executable_protocol_invariant_model",
            })
    return pd.DataFrame(rows)
