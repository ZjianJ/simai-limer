#!/usr/bin/env python3
"""Run four causal detection baselines on one shared LIMER fault schedule.

Baselines:
  1. switch_sparse: constant-state switch-port rules (64 logical bytes/port)
  2. host_telemetry: host NIC counters/gauges
  3. rdma_timeout: no byte progress while bytes remain outstanding
  4. nccl_watchdog: no flow completion while collective flows are active

No detector reads fault labels, configured bandwidth, or parameter_after.
Ground truth is used only after alarms have been produced, for latency joins.
"""
import argparse
import json
import os
import struct

import pandas as pd


SWITCH_STATE_FORMAT = "<QQIIIIffffIHHII"
HOST_STATE_FORMAT = "<QQQQIIIIffffIHH"
RDMA_STATE_FORMAT = "<QQQII"
NCCL_STATE_FORMAT = "<QQII"
SWITCH_STATE_BYTES_PER_PORT = struct.calcsize(SWITCH_STATE_FORMAT)
HOST_STATE_BYTES_PER_PORT = struct.calcsize(HOST_STATE_FORMAT)
RDMA_STATE_BYTES_PER_PORT = struct.calcsize(RDMA_STATE_FORMAT)
NCCL_STATE_BYTES_PER_JOB = struct.calcsize(NCCL_STATE_FORMAT)
assert SWITCH_STATE_BYTES_PER_PORT == 64
assert HOST_STATE_BYTES_PER_PORT == 72
assert RDMA_STATE_BYTES_PER_PORT == 32
assert NCCL_STATE_BYTES_PER_JOB == 24

ALARM_METADATA = {
    "switch_sparse": {
        "layer": "switch_direct",
        "evidence_class": "simulated_direct",
        "alarm_contract": "causal_port_rule",
        "localization_scope": "physical_link",
        "action_scope": "reroute_or_drain_link",
    },
    "host_telemetry": {
        "layer": "nic_local",
        "evidence_class": "simulated_direct",
        "alarm_contract": "causal_nic_rule",
        "localization_scope": "host_nic_link_candidate",
        "action_scope": "isolate_nic_or_host",
    },
    "rdma_timeout": {
        "layer": "rdma_transport",
        "evidence_class": "compressed_proxy",
        "alarm_contract": "no_progress_timer_proxy",
        "localization_scope": "qp_host_nic_candidate",
        "action_scope": "qp_error_or_runtime_notification",
    },
    "nccl_watchdog": {
        "layer": "collective_runtime",
        "evidence_class": "compressed_proxy",
        "alarm_contract": "collective_no_progress_timer_proxy",
        "localization_scope": "job_global",
        "action_scope": "abort_restart_or_reconfigure_job",
    },
}


def numeric(df, columns):
    for column in columns:
        if column not in df:
            df[column] = 0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_run(run_dir):
    switch = pd.read_csv(os.path.join(run_dir, "switch_telemetry.csv"))
    nic = pd.read_csv(os.path.join(run_dir, "nic_telemetry.csv"))
    collective = pd.read_csv(os.path.join(run_dir, "collective_telemetry.csv"))
    link_map = pd.read_csv(os.path.join(run_dir, "link_map.csv"))
    access = set(link_map[link_map["link_class"] == "ACCESS"]["link_id"])
    return switch, nic, collective, link_map, access


def prepare_switch(switch, access):
    df = switch[switch["link_id"].isin(access)].copy()
    df = numeric(df, ["observed_throughput_bps", "max_queue_bytes",
                      "dropped_packets", "link_errors", "flap_count"])
    if "link_state" not in df:
        df["link_state"] = "up"
    keys = ["link_id", "timestamp_ns"]
    tx = df[df["direction"] == "tx"].groupby(keys).agg(
        throughput_bps=("observed_throughput_bps", "sum"),
        queue_peak_bytes=("max_queue_bytes", "max"),
        tx_drops=("dropped_packets", "sum"),
        flap_count=("flap_count", "max"),
        down=("link_state", lambda values: int((values == "down").any())),
    ).reset_index()
    rx = df[df["direction"] == "rx"].groupby(keys).agg(
        rx_drops=("dropped_packets", "sum"),
        link_errors=("link_errors", "sum"),
        rx_flap_count=("flap_count", "max"),
        rx_down=("link_state", lambda values: int((values == "down").any())),
    ).reset_index()
    out = tx.merge(rx, on=keys, how="outer").fillna(0)
    out["flap_count"] = out[["flap_count", "rx_flap_count"]].max(axis=1)
    out["down"] = out[["down", "rx_down"]].max(axis=1)
    return out.sort_values(["link_id", "timestamp_ns"])


def prepare_host(nic, access):
    df = nic[nic["link_id"].isin(access)].copy()
    df = numeric(df, ["effective_throughput_bps", "outstanding_bytes",
                      "max_queue_bytes", "nacks", "flap_count", "tx_bytes",
                      "rx_bytes", "tx_dropped_packets", "rx_dropped_packets",
                      "link_errors"])
    if "link_state" not in df:
        df["link_state"] = "up"
    keys = ["link_id", "timestamp_ns"]
    return df.groupby(keys).agg(
        throughput_bps=("effective_throughput_bps", "sum"),
        outstanding_bytes=("outstanding_bytes", "sum"),
        queue_peak_bytes=("max_queue_bytes", "max"),
        nacks=("nacks", "sum"),
        tx_drops=("tx_dropped_packets", "sum"),
        rx_drops=("rx_dropped_packets", "sum"),
        link_errors=("link_errors", "sum"),
        flap_count=("flap_count", "max"),
        tx_bytes=("tx_bytes", "sum"),
        rx_bytes=("rx_bytes", "sum"),
        down=("link_state", lambda values: int((values == "down").any())),
    ).reset_index().sort_values(["link_id", "timestamp_ns"])


def sparse_anomaly_alarms(series, baseline, warmup_ns, alpha, queue_threshold,
                          low_rate_ratio, low_rate_samples):
    alarms = []
    for link_id, group in series.groupby("link_id"):
        rate_ewma = 0.0
        queue_ewma = 0.0
        outstanding_ewma = 0.0
        initialized = False
        previous = {name: 0 for name in
                    ["tx_drops", "rx_drops", "link_errors", "flap_count", "nacks"]}
        low_rate_streak = 0
        alarm_active = False
        normal_streak = 0
        for row in group.sort_values("timestamp_ns").to_dict("records"):
            now = int(row["timestamp_ns"])
            rate = float(row.get("throughput_bps", 0))
            queue = float(row.get("queue_peak_bytes", 0))
            outstanding = float(row.get("outstanding_bytes", 0))
            if not initialized:
                rate_ewma, queue_ewma = rate, queue
                outstanding_ewma, initialized = outstanding, True
            deltas = {}
            for name in previous:
                value = float(row.get(name, 0))
                deltas[name] = max(0.0, value - previous[name])
                previous[name] = value

            if now <= warmup_ns:
                rate_ewma = (1 - alpha) * rate_ewma + alpha * rate
                queue_ewma = (1 - alpha) * queue_ewma + alpha * queue
                outstanding_ewma = ((1 - alpha) * outstanding_ewma
                                    + alpha * outstanding)
                continue

            reasons = []
            if row.get("down", 0) or deltas["flap_count"] > 0:
                reasons.append("link_flap")
            if deltas["link_errors"] + deltas["tx_drops"] + deltas["rx_drops"] > 0:
                reasons.append("drop_or_link_error")
            if baseline == "host_telemetry" and deltas["nacks"] > 0:
                reasons.append("rdma_nack")
            if (baseline == "host_telemetry" and
                    outstanding > max(180_000.0, 2.5 * outstanding_ewma)):
                reasons.append("outstanding_growth")

            queue_limit = max(float(queue_threshold), 3.0 * queue_ewma + 9216.0)
            if queue > queue_limit:
                reasons.append("queue_high_water")

            has_demand = (baseline == "switch_sparse" or
                          float(row.get("outstanding_bytes", 0)) > 0)
            rate_low = (has_demand and rate_ewma > 1e9 and
                        rate < low_rate_ratio * rate_ewma and
                        queue > max(18_432.0, 1.5 * queue_ewma))
            low_rate_streak = low_rate_streak + 1 if rate_low else 0
            if low_rate_streak >= low_rate_samples:
                reasons.append("throughput_drop")

            anomalous = bool(reasons)
            if anomalous and not alarm_active:
                weights = {"link_flap": 4.0, "drop_or_link_error": 3.0,
                           "rdma_nack": 2.5, "queue_high_water": 2.0,
                           "outstanding_growth": 1.5, "throughput_drop": 1.0}
                alarms.append({"baseline": baseline, "alarm_time_ns": now,
                               "link_id": link_id, "reason": "+".join(reasons),
                               "score": max(weights[reason] for reason in reasons)})
                alarm_active = True
                normal_streak = 0
            elif anomalous:
                normal_streak = 0
            else:
                normal_streak += 1
                if normal_streak >= 2:
                    alarm_active = False
                rate_ewma = (1 - alpha) * rate_ewma + alpha * rate
                queue_ewma = (1 - alpha) * queue_ewma + alpha * queue
                outstanding_ewma = ((1 - alpha) * outstanding_ewma
                                    + alpha * outstanding)
    return alarms


def rdma_timeout_alarms(host, timeout_ns):
    alarms = []
    for link_id, group in host.groupby("link_id"):
        previous_total = None
        stall_start = None
        alarm_active = False
        for row in group.sort_values("timestamp_ns").to_dict("records"):
            now = int(row["timestamp_ns"])
            total = int(row["tx_bytes"] + row["rx_bytes"])
            progress = previous_total is None or total > previous_total
            active = row["outstanding_bytes"] > 0
            previous_total = total
            if active and not progress:
                if stall_start is None:
                    stall_start = now
                if now - stall_start >= timeout_ns and not alarm_active:
                    alarms.append({"baseline": "rdma_timeout", "alarm_time_ns": now,
                                   "link_id": link_id, "reason": "no_qp_byte_progress",
                                   "score": 1.0})
                    alarm_active = True
            else:
                stall_start = None
                alarm_active = False
    return alarms


def nccl_watchdog_alarms(collective, timeline, timeout_ns):
    if collective.empty:
        return []
    starts = pd.to_numeric(collective["start_time_ns"], errors="coerce").dropna()
    finishes = pd.to_numeric(collective["finish_time_ns"], errors="coerce").dropna()
    alarms = []
    alarm_active = False
    last_completion = None
    for now in sorted(set(int(value) for value in timeline)):
        completed = finishes[finishes <= now]
        if not completed.empty:
            newest = int(completed.max())
            if last_completion is None or newest > last_completion:
                last_completion = newest
                alarm_active = False
        active = bool(((starts <= now) & (finishes > now)).any())
        if active and last_completion is not None and now - last_completion >= timeout_ns:
            if not alarm_active:
                alarms.append({"baseline": "nccl_watchdog", "alarm_time_ns": now,
                               "link_id": "GLOBAL", "reason": "no_flow_completion",
                               "score": 1.0})
                alarm_active = True
        elif not active:
            alarm_active = False
    return alarms


def run_detectors(run_dir, args):
    switch, nic, collective, _, access = load_run(run_dir)
    switch_series = prepare_switch(switch, access)
    host_series = prepare_host(nic, access)
    alarms = []
    alarms += sparse_anomaly_alarms(
        switch_series, "switch_sparse", args.warmup_ns, args.ewma_alpha,
        args.queue_threshold_bytes, args.low_rate_ratio, args.low_rate_samples)
    alarms += sparse_anomaly_alarms(
        host_series, "host_telemetry", args.warmup_ns, args.ewma_alpha,
        args.queue_threshold_bytes, args.low_rate_ratio, args.low_rate_samples)
    alarms += rdma_timeout_alarms(host_series, args.rdma_timeout_ns)
    timeline = sorted(switch_series["timestamp_ns"].unique())
    alarms += nccl_watchdog_alarms(collective, timeline, args.nccl_timeout_ns)
    return pd.DataFrame(alarms, columns=["baseline", "alarm_time_ns", "link_id",
                                         "reason", "score"]), timeline


def correlate(alarms, faults, run_end_ns, followup_ns):
    baselines = ["switch_sparse", "host_telemetry", "rdma_timeout", "nccl_watchdog"]
    rows = []
    ordered = faults.sort_values("start_time_ns").reset_index(drop=True)
    for index, fault in ordered.iterrows():
        next_start = (int(ordered.loc[index + 1, "start_time_ns"])
                      if index + 1 < len(ordered) else run_end_ns + 1)
        deadline = min(run_end_ns, int(fault["end_time_ns"]) + followup_ns,
                       next_start - 1)
        for baseline in baselines:
            window_alarms = alarms[
                (alarms["baseline"] == baseline)
                & (alarms["alarm_time_ns"] >= int(fault["start_time_ns"]))
                & (alarms["alarm_time_ns"] <= deadline)
            ]
            candidates = window_alarms
            if baseline != "nccl_watchdog":
                candidates = candidates[candidates["link_id"] == fault["target_link_id"]]
            detected = not candidates.empty
            first = candidates.sort_values("alarm_time_ns").iloc[0] if detected else None
            top_candidates = pd.DataFrame()
            if baseline != "nccl_watchdog" and not window_alarms.empty:
                first_any_time = int(window_alarms["alarm_time_ns"].min())
                first_any = window_alarms[window_alarms["alarm_time_ns"] == first_any_time]
                top_score = float(first_any["score"].max())
                top_candidates = first_any[first_any["score"] == top_score]
            target_in_top = (not top_candidates.empty and
                             fault["target_link_id"] in set(top_candidates["link_id"]))
            rows.append({
                "fault_id": fault["fault_id"],
                "fault_type": fault["fault_type"],
                "target_link_id": fault["target_link_id"],
                "fault_start_ns": int(fault["start_time_ns"]),
                "fault_end_ns": int(fault["end_time_ns"]),
                "evaluation_deadline_ns": int(deadline),
                "baseline": baseline,
                "detected": bool(detected),
                "alarm_time_ns": int(first["alarm_time_ns"]) if detected else None,
                "detection_latency_ns": (int(first["alarm_time_ns"])
                                         - int(fault["start_time_ns"])) if detected else None,
                "reason": first["reason"] if detected else "censored_no_alarm",
                "top1_candidate_count": (int(len(top_candidates))
                                         if baseline != "nccl_watchdog" else None),
                "target_in_top_candidates": (bool(target_in_top)
                                             if baseline != "nccl_watchdog" else None),
                "top1_unique_correct": (bool(target_in_top and len(top_candidates) == 1)
                                        if baseline != "nccl_watchdog" else None),
                **ALARM_METADATA[baseline],
            })
    result = pd.DataFrame(rows)
    switch_latency = (result[result["baseline"] == "switch_sparse"]
                      .set_index("fault_id")["detection_latency_ns"])
    lead_values, lead_kinds = [], []
    for _, row in result.iterrows():
        reference = switch_latency.get(row["fault_id"])
        if row["baseline"] == "switch_sparse" or pd.isna(reference):
            lead_values.append(None)
            lead_kinds.append("not_applicable")
        elif row["detected"]:
            lead_values.append(float(row["detection_latency_ns"]) - float(reference))
            lead_kinds.append("exact")
        else:
            observable = row["evaluation_deadline_ns"] - row["fault_start_ns"]
            lead_values.append(float(observable) - float(reference))
            lead_kinds.append("lower_bound_censored")
    result["switch_lead_vs_baseline_ns"] = lead_values
    result["switch_lead_kind"] = lead_kinds
    return result


def alarms_outside_fault_windows(alarms, faults, followup_ns):
    count = 0
    for _, alarm in alarms.iterrows():
        matched = False
        for _, fault in faults.iterrows():
            link_match = (alarm["baseline"] == "nccl_watchdog"
                          or alarm["link_id"] == fault["target_link_id"])
            time_match = (fault["start_time_ns"] <= alarm["alarm_time_ns"]
                          <= fault["end_time_ns"] + followup_ns)
            if link_match and time_match:
                matched = True
                break
        count += int(not matched)
    return count


def write_markdown(path, comparison, summary):
    with open(path, "w") as out:
        out.write("# Four-Baseline Detection Latency Comparison\n\n")
        out.write("All baselines consumed the same telemetry run and the same fault schedule. "
                  "CENSORED means no alarm before the evaluation deadline.\n\n")
        out.write("RDMA and NCCL rows use compressed 4 ms no-progress proxies for this "
                  "short simulation; they are not production retry/watchdog defaults.\n\n")
        out.write("| Fault | Link | Baseline | Detected | Latency | Switch lead | Localization | Reason |\n")
        out.write("|---|---|---|---:|---:|---:|---|---|\n")
        for _, row in comparison.iterrows():
            latency = (f"{int(row['detection_latency_ns']) / 1e6:.3f} ms"
                       if row["detected"] else "CENSORED")
            if row["switch_lead_kind"] == "exact":
                lead = f"{row['switch_lead_vs_baseline_ns'] / 1e6:.3f} ms"
            elif row["switch_lead_kind"] == "lower_bound_censored":
                lead = f"> {row['switch_lead_vs_baseline_ns'] / 1e6:.3f} ms"
            else:
                lead = "—"
            if row["baseline"] == "nccl_watchdog":
                localization = "global only"
            elif row["top1_unique_correct"]:
                localization = "unique Top-1"
            elif row["target_in_top_candidates"]:
                localization = f"Top-1 tie ({int(row['top1_candidate_count'])})"
            else:
                localization = "incorrect/none"
            out.write(f"| {row['fault_type']} | {row['target_link_id']} | "
                      f"{row['baseline']} | {row['detected']} | {latency} | {lead} | "
                      f"{localization} | "
                      f"{row['reason']} |\n")
        out.write("\n## Logical detector state budgets\n\n")
        for name, value in summary["logical_state_bytes"].items():
            suffix = "/job" if name == "nccl_watchdog" else "/port"
            out.write(f"- `{name}`: {value} bytes{suffix}\n")
        out.write("\n## False alarms on healthy reference\n\n")
        for name, value in summary["healthy_alarm_counts"].items():
            out.write(f"- `{name}`: {value}\n")
        out.write("\n## Fault-run collateral alarms\n\n")
        out.write(
            f"- {summary['fault_run_collateral_or_outside_alarm_count']} alarms "
            "were outside the injected target link/window; these are retained "
            "as fault-propagation signals rather than hidden as false positives.\n"
        )
        out.write("\n## RQ2 alarm contracts\n\n")
        out.write("| Baseline | Layer | Evidence | Alarm contract | Action scope |\n")
        out.write("|---|---|---|---|---|\n")
        for baseline, metadata in ALARM_METADATA.items():
            out.write(
                f"| {baseline} | {metadata['layer']} | "
                f"{metadata['evidence_class']} | {metadata['alarm_contract']} | "
                f"{metadata['action_scope']} |\n"
            )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--healthy-run-dir", required=True)
    ap.add_argument("--fault-events", required=True)
    ap.add_argument("--warmup-ns", type=int, default=3_000_000)
    ap.add_argument("--ewma-alpha", type=float, default=0.125)
    ap.add_argument("--queue-threshold-bytes", type=int, default=32_768)
    ap.add_argument("--low-rate-ratio", type=float, default=0.70)
    ap.add_argument("--low-rate-samples", type=int, default=2)
    ap.add_argument(
        "--rdma-timeout-ns", type=int, default=4_000_000,
        help="compressed no-progress proxy threshold, not a production retry timeout",
    )
    ap.add_argument(
        "--nccl-timeout-ns", type=int, default=4_000_000,
        help="compressed no-progress proxy threshold, not a production NCCL default",
    )
    ap.add_argument("--followup-ns", type=int, default=5_000_000)
    ap.add_argument("--out-alarms", required=True)
    ap.add_argument("--out-comparison", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    faults = pd.read_csv(args.fault_events)
    alarms, timeline = run_detectors(args.run_dir, args)
    healthy_alarms, _ = run_detectors(args.healthy_run_dir, args)
    run_end = int(max(timeline))
    comparison = correlate(alarms, faults, run_end, args.followup_ns)
    healthy_counts = {name: int((healthy_alarms["baseline"] == name).sum())
                      for name in ["switch_sparse", "host_telemetry",
                                   "rdma_timeout", "nccl_watchdog"]}
    summary = {
        "run_dir": args.run_dir,
        "healthy_run_dir": args.healthy_run_dir,
        "fault_events": args.fault_events,
        "logical_state_bytes": {
            "switch_sparse": SWITCH_STATE_BYTES_PER_PORT,
            "host_telemetry": HOST_STATE_BYTES_PER_PORT,
            "rdma_timeout": RDMA_STATE_BYTES_PER_PORT,
            "nccl_watchdog": NCCL_STATE_BYTES_PER_JOB,
        },
        "packed_state_formats": {
            "switch_sparse": SWITCH_STATE_FORMAT,
            "host_telemetry": HOST_STATE_FORMAT,
            "rdma_timeout": RDMA_STATE_FORMAT,
            "nccl_watchdog": NCCL_STATE_FORMAT,
        },
        "alarm_metadata": ALARM_METADATA,
        "timeout_semantics": {
            "rdma_timeout": "compressed_proxy_not_retry_exhaustion_or_wc_error",
            "nccl_watchdog": "compressed_proxy_not_production_nccl_watchdog_or_ras",
            "production_reference": (
                "NCCL_IB_TIMEOUT=20 and NCCL_IB_RETRY_CNT=7 are approximately "
                "30 seconds in current NVIDIA NCCL documentation"
            ),
        },
        "healthy_alarm_counts": healthy_counts,
        "fault_run_collateral_or_outside_alarm_count": alarms_outside_fault_windows(
            alarms, faults, args.followup_ns),
        "detected_by_baseline": {
            name: int(group["detected"].sum())
            for name, group in comparison.groupby("baseline")
        },
        "unique_top1_correct_by_baseline": {
            name: int(group["top1_unique_correct"].fillna(False).sum())
            for name, group in comparison.groupby("baseline")
        },
        "parameters": {
            "warmup_ns": args.warmup_ns,
            "queue_threshold_bytes": args.queue_threshold_bytes,
            "low_rate_ratio": args.low_rate_ratio,
            "rdma_timeout_ns": args.rdma_timeout_ns,
            "nccl_timeout_ns": args.nccl_timeout_ns,
            "followup_ns": args.followup_ns,
        },
    }
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    alarms.to_csv(args.out_alarms, index=False)
    comparison.to_csv(args.out_comparison, index=False)
    with open(args.out_json, "w") as out:
        json.dump(summary, out, indent=2)
    write_markdown(args.out_md, comparison, summary)
    print(comparison.to_string(index=False))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
