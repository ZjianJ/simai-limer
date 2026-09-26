#!/usr/bin/env python3
"""Evaluate the unmodified LIMER system against detection/recovery SLOs.

This tool is intentionally observational.  It reuses locked simulator runs and
the existing detector implementations.  It does not create a backup path,
change a timeout, reroute traffic, or add collective recovery semantics.
"""
import argparse
import json
import os
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd


HARD_DEADLINE_NS = 1_000_000
GRAY_DEADLINE_NS = 100_000_000
RECOVERY_DEADLINE_NS = 1_000_000_000
DETECTORS = [
    "qghmm_quantized", "switch_sparse", "host_telemetry",
    "rdma_timeout_proxy", "nccl_watchdog_proxy",
]
DIRECT_DETECTOR_NAMES = {
    "switch_sparse": "switch_sparse",
    "host_telemetry": "host_telemetry",
    "rdma_timeout": "rdma_timeout_proxy",
    "nccl_watchdog": "nccl_watchdog_proxy",
}
EVENT_CLASSES = {
    "hard_link_down": {
        "group": "HARD", "state": {"LINK_DOWN"},
        "deadline_ns": HARD_DEADLINE_NS,
    },
    "hard_link_flap": {
        "group": "HARD", "state": {"LINK_DOWN", "LINK_FLAP"},
        "deadline_ns": HARD_DEADLINE_NS,
    },
    "gray_fail_slow": {
        "group": "GRAY", "state": {"ACCESS_FAIL_SLOW"},
        "deadline_ns": GRAY_DEADLINE_NS,
    },
    "gray_transient_error": {
        "group": "GRAY", "state": {"TRANSIENT_LINK_ERROR_PROXY"},
        "deadline_ns": GRAY_DEADLINE_NS,
    },
}


def percentile(values, q):
    values = [float(value) for value in values if not pd.isna(value)]
    return float(np.quantile(values, q)) if values else None


def event_kind(spec):
    if spec["scenario"] == "ACCESS_FAIL_SLOW":
        return "gray_fail_slow"
    if spec["scenario"] == "TRANSIENT_LINK_ERROR_PROXY":
        return "gray_transient_error"
    if spec["scenario"] == "OOD" and spec.get("ood_kind") == "link_down":
        return "hard_link_down"
    if spec["scenario"] == "OOD" and spec.get("ood_kind") == "link_flap":
        return "hard_link_flap"
    return None


def build_events(manifest, dataset):
    rows = []
    for spec in manifest["runs"]:
        kind = event_kind(spec)
        if spec["split"] != "test" or kind is None:
            continue
        run = dataset[(dataset["run_id"] == spec["run_id"])
                      & (dataset["link_id"] == spec["target_link_id"])]
        active = run[run["fault_coverage_ratio"] > 0]
        if kind == "gray_transient_error":
            observable = bool((active["drop_error_delta"] > 0).any())
            evidence = "drop_or_link_error_delta" if observable else "no_packet_error_realized"
        elif kind == "gray_fail_slow":
            observable = bool((active["observable_score"] > 0.05).any())
            evidence = "rate_or_queue_effect" if observable else "no_rate_or_queue_effect"
        else:
            observable = bool(((run["fast_link_event"] > 0)
                               | (run["link_state_down"] > 0)).any())
            evidence = "latched_link_state_event" if observable else "no_latched_link_event"
        rows.append({
            "run_id": spec["run_id"],
            "fault_id": spec["run_id"] + "_" + kind,
            "fault_kind": kind,
            "fault_group": EVENT_CLASSES[kind]["group"],
            "target_link_id": spec["target_link_id"],
            "fault_start_ns": int(spec["fault_start_ns"]),
            "fault_end_ns": int(spec["fault_end_ns"]),
            "fault_duration_ns": int(spec["fault_end_ns"] - spec["fault_start_ns"]),
            "detection_deadline_ns": EVENT_CLASSES[kind]["deadline_ns"],
            "event_observable": observable,
            "observability_evidence": evidence,
            "test_suite": spec["test_suite"],
            "workload_profile": spec["workload_profile"],
            "severity": spec.get("severity"),
        })
    return pd.DataFrame(rows)


def top1_for_alarm(alarms, first, target, global_detector=False):
    if global_detector:
        return None
    at_time = alarms[alarms["alarm_time_ns"] == first]
    if at_time.empty:
        return False
    best = at_time[at_time["score"] == at_time["score"].max()]
    return bool(len(best) == 1 and best.iloc[0]["link_id"] == target)


def qghmm_detection(event, alarms):
    expected = EVENT_CLASSES[event.fault_kind]["state"]
    end = int(event.fault_end_ns) + 5_000_000
    candidates = alarms[
        (alarms["run_id"] == event.run_id)
        & (alarms["link_id"] == event.target_link_id)
        & (alarms["timestamp_ns"] >= event.fault_start_ns)
        & (alarms["timestamp_ns"] <= end)
        & (alarms["state"].isin(expected))
    ].sort_values("timestamp_ns")
    if candidates.empty:
        return None, None, "censored_no_matching_state"
    first = candidates.iloc[0]
    top = json.loads(first["top_k_links"])
    return int(first["timestamp_ns"]), bool(top and top[0] == event.target_link_id), str(first["state"])


def detector_rows(events, qghmm_alarms, runs_root, tool_dir):
    if tool_dir not in sys.path:
        sys.path.insert(0, tool_dir)
    from compare_detection_baselines import run_detectors

    defaults = SimpleNamespace(
        warmup_ns=3_000_000, ewma_alpha=0.125,
        queue_threshold_bytes=32_768, low_rate_ratio=0.70,
        low_rate_samples=2, rdma_timeout_ns=4_000_000,
        nccl_timeout_ns=4_000_000,
    )
    run_cache = {}
    rows = []
    for event in events.itertuples(index=False):
        if event.run_id not in run_cache:
            run_cache[event.run_id] = run_detectors(
                os.path.join(runs_root, event.run_id), defaults)[0]
        direct = run_cache[event.run_id]
        alarm_time, top1, reason = qghmm_detection(event, qghmm_alarms)
        rows.append(detection_row(event, "qghmm_quantized", alarm_time, top1,
                                  reason, "offline_causal_replay"))
        window_end = int(event.fault_end_ns) + 5_000_000
        for raw_name, detector in DIRECT_DETECTOR_NAMES.items():
            candidates = direct[
                (direct["baseline"] == raw_name)
                & (direct["alarm_time_ns"] >= event.fault_start_ns)
                & (direct["alarm_time_ns"] <= window_end)
            ]
            global_detector = raw_name == "nccl_watchdog"
            target = candidates if global_detector else candidates[
                candidates["link_id"] == event.target_link_id]
            if target.empty:
                alarm_time, top1, reason = None, None, "censored_no_alarm"
            else:
                first_row = target.sort_values("alarm_time_ns").iloc[0]
                alarm_time = int(first_row["alarm_time_ns"])
                top1 = top1_for_alarm(candidates, alarm_time,
                                      event.target_link_id, global_detector)
                reason = str(first_row["reason"])
            evidence = ("compressed_timeout_proxy" if detector.endswith("_proxy")
                        else "offline_causal_replay")
            rows.append(detection_row(event, detector, alarm_time, top1,
                                      reason, evidence))
    return pd.DataFrame(rows), run_cache


def detection_row(event, detector, alarm_time, top1, reason, evidence):
    detected = alarm_time is not None
    latency = int(alarm_time - event.fault_start_ns) if detected else None
    return {
        "run_id": event.run_id,
        "fault_id": event.fault_id,
        "fault_kind": event.fault_kind,
        "fault_group": event.fault_group,
        "target_link_id": event.target_link_id,
        "event_observable": bool(event.event_observable),
        "detector": detector,
        "evidence_class": evidence,
        "detected": detected,
        "alarm_time_ns": alarm_time,
        "detection_latency_ns": latency,
        "detection_deadline_ns": int(event.detection_deadline_ns),
        "signal_deadline_pass": bool(detected and latency < event.detection_deadline_ns),
        "top1_unique_correct": top1,
        "reason": reason,
        "alarm_executed_online": False,
        "controller_delivery_measured": False,
        "actionable_slo_verified": False,
    }


def aggregate_detection(rows):
    parts = []
    for label_column in ["fault_group", "fault_kind"]:
        for (detector, label), group in rows.groupby(["detector", label_column]):
            observable = group[group["event_observable"]]
            observed_detected = observable[observable["detected"]]
            parts.append({
                "detector": detector,
                "scope": label,
                "scheduled_events": len(group),
                "observable_events": len(observable),
                "detected_scheduled": int(group["detected"].sum()),
                "detected_observable": int(observable["detected"].sum()),
                "scheduled_recall": float(group["detected"].mean()) if len(group) else None,
                "observable_recall": (float(observable["detected"].mean())
                                      if len(observable) else None),
                "scheduled_deadline_pass_rate": float(group["signal_deadline_pass"].mean()),
                "observable_deadline_pass_rate": (
                    float(observable["signal_deadline_pass"].mean())
                    if len(observable) else None),
                "latency_median_ns": percentile(observed_detected["detection_latency_ns"], .5),
                "latency_p95_ns": percentile(observed_detected["detection_latency_ns"], .95),
                "latency_p99_ns": percentile(observed_detected["detection_latency_ns"], .99),
                "latency_max_ns": (float(observed_detected["detection_latency_ns"].max())
                                   if len(observed_detected) else None),
                "unique_top1_rate_observable": (
                    float(observable["top1_unique_correct"].dropna().mean())
                    if observable["top1_unique_correct"].notna().any() else None),
                "actionable_slo_verified": False,
            })
    return pd.DataFrame(parts)


def healthy_alarm_summary(manifest, qghmm_alarms, runs_root, run_cache, tool_dir):
    if tool_dir not in sys.path:
        sys.path.insert(0, tool_dir)
    from compare_detection_baselines import run_detectors
    defaults = SimpleNamespace(
        warmup_ns=3_000_000, ewma_alpha=0.125,
        queue_threshold_bytes=32_768, low_rate_ratio=0.70,
        low_rate_samples=2, rdma_timeout_ns=4_000_000,
        nccl_timeout_ns=4_000_000,
    )
    healthy_ids = [item["run_id"] for item in manifest["runs"]
                   if item["split"] == "test" and item["scenario"] == "HEALTHY"]
    counts = {name: 0 for name in DETECTORS}
    counts["qghmm_quantized"] = int(qghmm_alarms["run_id"].isin(healthy_ids).sum())
    for run_id in healthy_ids:
        if run_id not in run_cache:
            run_cache[run_id] = run_detectors(os.path.join(runs_root, run_id), defaults)[0]
        alarms = run_cache[run_id]
        for raw, output in DIRECT_DETECTOR_NAMES.items():
            counts[output] += int((alarms["baseline"] == raw).sum())
    return pd.DataFrame([{
        "detector": detector, "healthy_runs": len(healthy_ids),
        "healthy_alarm_count": count,
        "alarms_per_healthy_run": count / len(healthy_ids) if healthy_ids else None,
    } for detector, count in counts.items()])


def topology_audit(link_map):
    access = link_map[link_map["link_class"] == "ACCESS"]
    hosts = sorted(set(access.loc[access["src_type"] == "HOST", "src_node"])
                   | set(access.loc[access["dst_type"] == "HOST", "dst_node"]))
    degree = {}
    for host in hosts:
        degree[str(host)] = int((((access["src_type"] == "HOST")
                                  & (access["src_node"] == host))
                                 | ((access["dst_type"] == "HOST")
                                    & (access["dst_node"] == host))).sum())
    return {
        "host_count": len(hosts), "access_link_count": len(access),
        "direct_access_degree_by_host": degree,
        "min_direct_access_degree": min(degree.values()) if degree else 0,
        "max_direct_access_degree": max(degree.values()) if degree else 0,
        "explicit_backup_path_mapping": False,
        "preestablished_backup_qp": False,
    }


def first_target_tx_after(run_dir, target, time_ns):
    switch = pd.read_csv(os.path.join(run_dir, "switch_telemetry.csv"),
                         usecols=["timestamp_ns", "link_id", "direction",
                                  "observed_throughput_bps"])
    rows = switch[(switch["link_id"] == target)
                  & (switch["direction"] == "tx")
                  & (switch["timestamp_ns"] >= time_ns)]
    rows = rows[pd.to_numeric(rows["observed_throughput_bps"], errors="coerce").fillna(0) > 0]
    return int(rows["timestamp_ns"].min()) if not rows.empty else None


def recovery_observations(events, detections, runs_root):
    q_alarm = detections[detections["detector"] == "qghmm_quantized"].set_index("fault_id")
    rows = []
    for event in events.itertuples(index=False):
        run_dir = os.path.join(runs_root, event.run_id)
        collective = pd.read_csv(os.path.join(run_dir, "collective_telemetry.csv"))
        collective["start_time_ns"] = pd.to_numeric(collective["start_time_ns"], errors="coerce")
        collective["finish_time_ns"] = pd.to_numeric(collective["finish_time_ns"], errors="coerce")
        alarm_value = q_alarm.loc[event.fault_id, "alarm_time_ns"]
        alarm = None if pd.isna(alarm_value) else int(alarm_value)
        next_finish = None
        if alarm is not None:
            later = collective.loc[collective["finish_time_ns"] > alarm, "finish_time_ns"]
            next_finish = int(later.min()) if not later.empty else None
        in_flight = collective[(collective["start_time_ns"] <= event.fault_start_ns)
                               & (collective["finish_time_ns"] > event.fault_start_ns)]
        run_finish = (int(collective["finish_time_ns"].max())
                      if collective["finish_time_ns"].notna().any() else None)
        log_path = os.path.join(run_dir, "run.log")
        log = open(log_path, errors="replace").read() if os.path.isfile(log_path) else ""
        same_port_resume = first_target_tx_after(run_dir, event.target_link_id,
                                                 event.fault_end_ns)
        rows.append({
            "run_id": event.run_id, "fault_id": event.fault_id,
            "fault_kind": event.fault_kind, "target_link_id": event.target_link_id,
            "fault_start_ns": int(event.fault_start_ns),
            "fault_end_ns": int(event.fault_end_ns),
            "reference_detector": "qghmm_quantized",
            "alarm_time_ns": alarm,
            "fault_schedule_auto_clear_ns": int(event.fault_end_ns),
            "backup_path_available": False,
            "backup_qp_preestablished": False,
            "controller_cutover_executed": False,
            "failed_port_quiesced_by_controller": False,
            "surviving_port_first_traffic_ns": None,
            "same_failed_port_first_traffic_after_auto_clear_ns": same_port_resume,
            "same_port_resume_latency_after_auto_clear_ns": (
                same_port_resume - event.fault_end_ns
                if same_port_resume is not None else None),
            "next_completed_flow_after_alarm_ns": next_finish,
            "next_flow_latency_from_alarm_ns": (
                next_finish - alarm if next_finish is not None and alarm is not None else None),
            "inflight_completed_flow_rows_at_fault": len(in_flight),
            "run_completion_time_ns": run_finish,
            "run_completion_latency_from_fault_ns": (
                run_finish - event.fault_start_ns if run_finish is not None else None),
            "all_streams_completed": "Percentage of finished streams: 100" in log,
            "recovery_deadline_ns": RECOVERY_DEADLINE_NS,
            "recovery_slo_status": "FAIL_NOT_IMPLEMENTED",
            "progress_on_surviving_port_verified": False,
            "collective_result_semantics": "UNVERIFIABLE_FLOW_COMPLETION_ONLY",
            "safe_collective_redo_implemented": False,
            "interpretation": "finite fault auto-clears; any later progress is not failover",
        })
    return pd.DataFrame(rows)


def aggregate_natural_completion(recovery):
    rows = []
    for kind, group in recovery.groupby("fault_kind"):
        rows.append({
            "fault_kind": kind,
            "events": len(group),
            "runs_all_streams_completed": int(group["all_streams_completed"].sum()),
            "scheduled_auto_clear_duration_median_ns": percentile(
                group["fault_end_ns"] - group["fault_start_ns"], .5),
            "same_port_resume_after_clear_observed": int(
                group["same_failed_port_first_traffic_after_auto_clear_ns"].notna().sum()),
            "same_port_resume_after_clear_median_ns": percentile(
                group["same_port_resume_latency_after_auto_clear_ns"], .5),
            "run_completion_from_fault_median_ns": percentile(
                group["run_completion_latency_from_fault_ns"], .5),
            "run_completion_from_fault_max_ns": (
                float(group["run_completion_latency_from_fault_ns"].max())
                if group["run_completion_latency_from_fault_ns"].notna().any() else None),
            "counts_as_surviving_port_recovery": False,
        })
    return pd.DataFrame(rows)


def write_report(path, events, summary, healthy, recovery, natural, topology):
    with open(path, "w") as out:
        out.write("# Current-System Detection and Recovery SLO Baseline\n\n")
        out.write("This report measures the existing system without changing detection, "
                  "timeouts, paths, QPs, or collective behavior.\n\n")
        out.write("## Verdict\n\n")
        out.write("- Detection signal timing is measurable through offline causal replay.\n")
        out.write("- Actionable online alarm delivery is not implemented, so the strict "
                  "end-to-end detection SLO is not yet verified.\n")
        out.write("- Surviving-port recovery is `FAIL_NOT_IMPLEMENTED` for every event.\n")
        out.write("- Collective numerical correctness and safe redo are not modeled.\n\n")
        out.write("## Coverage\n\n")
        out.write(f"- Test fault events: {len(events)}\n")
        for kind, group in events.groupby("fault_kind"):
            out.write(f"- `{kind}`: {len(group)} scheduled, "
                      f"{int(group['event_observable'].sum())} observable\n")
        out.write(f"- Direct ACCESS links: {topology['access_link_count']} for "
                  f"{topology['host_count']} simulated hosts; direct degree is "
                  f"{topology['min_direct_access_degree']} per host\n\n")
        out.write("## Detection signal baseline\n\n")
        out.write("These are signal-exposure times, not controller-delivery times. "
                  "Hard uses <1 ms and gray uses <100 ms.\n\n")
        out.write("| Detector | Scope | Observable | Recall | Deadline pass | Median | P95 | Max | Top-1 |\n")
        out.write("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in summary[summary["scope"].isin(["HARD", "GRAY"])].itertuples(index=False):
            def pct(value):
                return "N/A" if pd.isna(value) else f"{100 * value:.2f}%"
            median = "N/A" if pd.isna(row.latency_median_ns) else f"{row.latency_median_ns/1e6:.3f} ms"
            p95 = "N/A" if pd.isna(row.latency_p95_ns) else f"{row.latency_p95_ns/1e6:.3f} ms"
            maximum = "N/A" if pd.isna(row.latency_max_ns) else f"{row.latency_max_ns/1e6:.3f} ms"
            out.write(f"| {row.detector} | {row.scope} | {row.observable_events} | "
                      f"{pct(row.observable_recall)} | "
                      f"{pct(row.observable_deadline_pass_rate)} | {median} | {p95} | {maximum} | "
                      f"{pct(row.unique_top1_rate_observable)} |\n")
        out.write("\n## Healthy alarm baseline\n\n")
        out.write("| Detector | Healthy runs | Alarms | Alarms/run |\n")
        out.write("|---|---:|---:|---:|\n")
        for row in healthy.itertuples(index=False):
            out.write(f"| {row.detector} | {row.healthy_runs} | "
                      f"{row.healthy_alarm_count} | {row.alarms_per_healthy_run:.3f} |\n")
        out.write("\n## Recovery and collective baseline\n\n")
        out.write(f"- Recovery rows evaluated: {len(recovery)}\n")
        out.write("- Pre-established backup QP: no\n")
        out.write("- Explicit backup path mapping: no\n")
        out.write("- Controller-triggered failed-port quiescence: no\n")
        out.write("- Traffic observed on a named surviving port: no\n")
        out.write("- Runs completing after a finite fault do so after the schedule "
                  "automatically clears or degrades/restores the same port; this is "
                  "not counted as recovery.\n")
        out.write("- The telemetry contains completed flow rows but no tensor values, "
                  "collective commit/abort protocol, or redo epoch. Numerical safety "
                  "is therefore unverified.\n\n")
        out.write("### Natural finite-fault completion (not recovery)\n\n")
        out.write("| Fault | Runs complete | Auto-clear median | Same-port resume median | "
                  "Run completion from fault median |\n")
        out.write("|---|---:|---:|---:|---:|\n")
        for row in natural.itertuples(index=False):
            resume = ("N/A" if pd.isna(row.same_port_resume_after_clear_median_ns)
                      else f"{row.same_port_resume_after_clear_median_ns/1e6:.3f} ms")
            out.write(f"| {row.fault_kind} | {row.runs_all_streams_completed}/{row.events} | "
                      f"{row.scheduled_auto_clear_duration_median_ns/1e6:.3f} ms | "
                      f"{resume} | {row.run_completion_from_fault_median_ns/1e6:.3f} ms |\n")
        out.write("\nEven where these values are below one second, they do not satisfy "
                  "the recovery SLO: the injected fault disappears on schedule and "
                  "traffic returns to the same port.\n\n")
        out.write("## Evidence boundary\n\n")
        out.write("The 4 ms RDMA and NCCL rows are compressed no-progress proxies. "
                  "They are not retry exhaustion, Work Completion errors, a real NCCL "
                  "watchdog, or tuned production timeout measurements.\n")


def consistency_checks(events, detections, recovery, topology):
    expected_pairs = len(events) * len(DETECTORS)
    recomputed_deadline = detections["detected"] & (
        detections["detection_latency_ns"] < detections["detection_deadline_ns"])
    checks = {
        "one_row_per_event_detector": {
            "pass": len(detections) == expected_pairs,
            "detail": f"rows={len(detections)}, expected={expected_pairs}",
        },
        "all_selected_events_are_test_faults": {
            "pass": len(events) == 82,
            "detail": f"locked selected event count={len(events)}",
        },
        "scheduled_and_observable_populations_retained": {
            "pass": len(events) == 82 and int(events["event_observable"].sum()) == 72,
            "detail": (f"scheduled={len(events)}, "
                       f"observable={int(events['event_observable'].sum())}"),
        },
        "strict_deadline_computation": {
            "pass": bool((recomputed_deadline == detections["signal_deadline_pass"]).all()),
            "detail": "deadline pass uses latency < deadline, never <=",
        },
        "no_online_alarm_claim": {
            "pass": not bool(detections["alarm_executed_online"].any())
                    and not bool(detections["actionable_slo_verified"].any()),
            "detail": "all detector outputs are labeled offline/proxy",
        },
        "no_recovery_action_claim": {
            "pass": (not bool(recovery["controller_cutover_executed"].any())
                     and not bool(recovery["progress_on_surviving_port_verified"].any())
                     and set(recovery["recovery_slo_status"]) == {"FAIL_NOT_IMPLEMENTED"}),
            "detail": "automatic schedule clear is never counted as recovery",
        },
        "no_backup_path_or_qp_claim": {
            "pass": (not topology["explicit_backup_path_mapping"]
                     and not topology["preestablished_backup_qp"]),
            "detail": "16 hosts have one direct ACCESS link each",
        },
        "collective_correctness_not_fabricated": {
            "pass": set(recovery["collective_result_semantics"]) == {
                "UNVERIFIABLE_FLOW_COMPLETION_ONLY"},
            "detail": "flow completion is not reported as tensor correctness",
        },
    }
    return {"checks": checks, "all_pass": all(item["pass"] for item in checks.values())}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--qghmm-alarms", required=True)
    ap.add_argument("--runs-root", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    manifest = json.load(open(args.manifest))
    dataset = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    qghmm_alarms = pd.read_csv(args.qghmm_alarms)
    events = build_events(manifest, dataset)
    tool_dir = os.path.dirname(os.path.abspath(__file__))
    detections, run_cache = detector_rows(events, qghmm_alarms,
                                           args.runs_root, tool_dir)
    summary = aggregate_detection(detections)
    healthy = healthy_alarm_summary(manifest, qghmm_alarms, args.runs_root,
                                    run_cache, tool_dir)
    sample_map = pd.read_csv(os.path.join(args.runs_root,
                                          events.iloc[0]["run_id"], "link_map.csv"))
    topology = topology_audit(sample_map)
    recovery = recovery_observations(events, detections, args.runs_root)
    natural = aggregate_natural_completion(recovery)
    capability = {
        "schema_version": 1,
        "experiment_mode": "observe_existing_behavior_without_improvements",
        "slo": {"hard_detection_lt_ns": HARD_DEADLINE_NS,
                "gray_detection_lt_ns": GRAY_DEADLINE_NS,
                "recovery_from_detection_lt_ns": RECOVERY_DEADLINE_NS},
        "topology": topology,
        "online_alarm_execution": False,
        "controller_delivery_timestamp": False,
        "failed_port_quiescence_action": False,
        "runtime_reroute": False,
        "preestablished_backup_qp": False,
        "permanent_hard_failure_benchmark": False,
        "hard_failure_semantics": "forced_link_state_latch_plus_rate_degrade_auto_clear",
        "finite_fault_schedule_auto_clear": True,
        "configurable_real_rdma_rto": False,
        "collective_abort_commit_redo": False,
        "tensor_numerical_correctness": False,
        "collective_telemetry_semantics": "flow_sender_completion",
        "overall_recovery_baseline": "FAIL_NOT_IMPLEMENTED",
    }
    checks = consistency_checks(events, detections, recovery, topology)
    if not checks["all_pass"]:
        raise SystemExit("current-system baseline consistency checks failed")
    headline = summary[(summary["detector"] == "switch_sparse")
                       & summary["scope"].isin(["HARD", "GRAY"])]
    machine_summary = {
        "schema_version": 1,
        "event_counts": {
            "scheduled": len(events),
            "observable": int(events["event_observable"].sum()),
            "hard_observable": int(((events["fault_group"] == "HARD")
                                    & events["event_observable"]).sum()),
            "gray_observable": int(((events["fault_group"] == "GRAY")
                                    & events["event_observable"]).sum()),
        },
        "current_operational_reference": "switch_sparse",
        "switch_sparse_headline": headline.to_dict("records"),
        "recovery": "FAIL_NOT_IMPLEMENTED",
        "collective_correctness": "UNVERIFIABLE_FLOW_COMPLETION_ONLY",
        "all_consistency_checks_pass": checks["all_pass"],
    }
    os.makedirs(args.out_dir, exist_ok=True)
    events.to_csv(os.path.join(args.out_dir, "fault_events_evaluated.csv"), index=False)
    detections.to_csv(os.path.join(args.out_dir, "detection_event_timeline.csv"), index=False)
    summary.to_csv(os.path.join(args.out_dir, "detection_summary.csv"), index=False)
    healthy.to_csv(os.path.join(args.out_dir, "healthy_alarm_summary.csv"), index=False)
    recovery.to_csv(os.path.join(args.out_dir, "recovery_observations.csv"), index=False)
    natural.to_csv(os.path.join(args.out_dir, "natural_completion_summary.csv"), index=False)
    with open(os.path.join(args.out_dir, "capability_matrix.json"), "w") as out:
        json.dump(capability, out, indent=2)
        out.write("\n")
    with open(os.path.join(args.out_dir, "baseline_checks.json"), "w") as out:
        json.dump(checks, out, indent=2)
        out.write("\n")
    with open(os.path.join(args.out_dir, "baseline_summary.json"), "w") as out:
        json.dump(machine_summary, out, indent=2)
        out.write("\n")
    report = os.path.join(args.out_dir, "baseline_report.md")
    write_report(report, events, summary, healthy, recovery, natural, topology)
    print(json.dumps({
        "events": len(events),
        "observable_events": int(events["event_observable"].sum()),
        "detection_rows": len(detections),
        "recovery_status": capability["overall_recovery_baseline"],
        "report": os.path.abspath(report),
    }, indent=2))


if __name__ == "__main__":
    main()
