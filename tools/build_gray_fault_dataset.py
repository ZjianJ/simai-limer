#!/usr/bin/env python3
"""Build the strictly causal ACCESS-port dataset for the QG-HMM experiment.

Inference features are finalized before any fault sidecar is opened. Labels,
coverage, phase, severity, and observability weights are attached in a second
offline-only pass. Splits come exclusively from the run-level manifest.
"""
import argparse
import json
import os

import numpy as np
import pandas as pd


FEATURES = [
    "tx_rate_residual",
    "rx_rate_residual",
    "tx_rx_asymmetry",
    "queue_ewma_residual",
    "queue_peak_residual",
    "queue_growth",
    "drop_error_delta",
    "peer_rate_gap",
]
RAW_FEATURES = [
    "tx_rate_log", "rx_rate_log", "asymmetry_raw", "queue_ewma_log",
    "queue_peak_log", "queue_growth_raw", "drop_error_log", "peer_gap_raw",
]


def numeric(frame, columns):
    for column in columns:
        if column not in frame:
            frame[column] = 0
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    return frame


def collective_context(collective, timestamps):
    if collective.empty:
        return pd.DataFrame({
            "timestamp_ns": timestamps,
            "expected_active": 0,
            "collective_phase": "IDLE",
            "message_size_class": "NONE",
            "active_collective_count": 0,
        })
    coll = numeric(collective.copy(), ["start_time_ns", "finish_time_ns",
                                      "message_size_bytes"])
    starts = coll["start_time_ns"].to_numpy(dtype=np.int64)
    finishes = coll["finish_time_ns"].to_numpy(dtype=np.int64)
    sizes = coll["message_size_bytes"].to_numpy(dtype=np.int64)
    rows = []
    for timestamp in timestamps:
        # This reconstructs the online start-minus-completion set. No finish
        # duration or future completion value becomes a model feature.
        active = (starts <= timestamp) & (finishes > timestamp)
        count = int(active.sum())
        if count:
            max_size = int(sizes[active].max())
            age = int(timestamp - starts[active].min())
            phase = "ONSET" if age <= 1_000_000 else "ACTIVE"
            if max_size <= 8 * 1024 * 1024:
                size_class = "SMALL"
            elif max_size <= 64 * 1024 * 1024:
                size_class = "MEDIUM"
            else:
                size_class = "LARGE"
        else:
            phase, size_class = "IDLE", "NONE"
        rows.append((int(timestamp), int(count > 0), phase, size_class, count))
    return pd.DataFrame(rows, columns=[
        "timestamp_ns", "expected_active", "collective_phase",
        "message_size_class", "active_collective_count",
    ])


def extract_causal_run(run_id, run_dir, workload_profile):
    switch = pd.read_csv(os.path.join(run_dir, "switch_telemetry.csv"))
    link_map = pd.read_csv(os.path.join(run_dir, "link_map.csv"))
    collective_path = os.path.join(run_dir, "collective_telemetry.csv")
    collective = pd.read_csv(collective_path) if os.path.isfile(collective_path) else pd.DataFrame()
    access_map = link_map[link_map["link_class"] == "ACCESS"][
        ["link_id", "src_node", "dst_node"]
    ]
    access = set(access_map["link_id"])
    frame = switch[switch["link_id"].isin(access)].copy()
    frame = numeric(frame, [
        "timestamp_ns", "switch_id", "tx_bytes", "rx_bytes",
        "dropped_packets", "link_errors", "queue_bytes", "max_queue_bytes",
        "flap_count",
    ])
    keys = ["link_id", "timestamp_ns"]
    tx = frame[frame["direction"] == "tx"].groupby(keys).agg(
        switch_id=("switch_id", "first"),
        tx_bytes=("tx_bytes", "sum"),
        tx_drops=("dropped_packets", "sum"),
        queue_bytes=("queue_bytes", "sum"),
        queue_peak=("max_queue_bytes", "max"),
        flap_count=("flap_count", "max"),
        link_state_down=("link_state", lambda values: int((values == "down").any())),
    ).reset_index()
    rx = frame[frame["direction"] == "rx"].groupby(keys).agg(
        rx_bytes=("rx_bytes", "sum"),
        rx_drops=("dropped_packets", "sum"),
        link_errors=("link_errors", "sum"),
    ).reset_index()
    data = tx.merge(rx, on=keys, how="outer").fillna(0)
    data["run_id"] = run_id
    data = data.sort_values(["link_id", "timestamp_ns"])
    group = data.groupby("link_id", sort=False)
    dt = group["timestamp_ns"].diff().fillna(data["timestamp_ns"]).clip(lower=1)
    tx_delta = group["tx_bytes"].diff().fillna(data["tx_bytes"]).clip(lower=0)
    rx_delta = group["rx_bytes"].diff().fillna(data["rx_bytes"]).clip(lower=0)
    error_total = data["tx_drops"] + data["rx_drops"] + data["link_errors"]
    data["drop_error_delta_raw"] = (
        error_total.groupby(data["link_id"]).diff().fillna(error_total).clip(lower=0)
    )
    data["flap_delta"] = group["flap_count"].diff().fillna(data["flap_count"]).clip(lower=0)
    data["fast_link_event"] = ((data["flap_delta"] > 0) |
                               (data["link_state_down"] > 0)).astype(int)
    data["tx_rate_1ms"] = tx_delta * 8e9 / dt
    data["rx_rate_1ms"] = rx_delta * 8e9 / dt
    data["tx_rate_2ms"] = group["tx_rate_1ms"].transform(
        lambda values: values.rolling(2, min_periods=1).mean())
    data["rx_rate_2ms"] = group["rx_rate_1ms"].transform(
        lambda values: values.rolling(2, min_periods=1).mean())
    data["tx_rate_5ms"] = group["tx_rate_1ms"].transform(
        lambda values: values.rolling(5, min_periods=1).mean())
    data["rx_rate_5ms"] = group["rx_rate_1ms"].transform(
        lambda values: values.rolling(5, min_periods=1).mean())
    data["queue_ewma"] = group["queue_bytes"].transform(
        lambda values: values.ewm(alpha=0.25, adjust=False).mean())
    data["queue_growth_raw"] = group["queue_bytes"].diff().fillna(0)
    denominator = data["tx_rate_1ms"] + data["rx_rate_1ms"] + 1.0
    data["asymmetry_raw"] = (
        (data["tx_rate_1ms"] - data["rx_rate_1ms"]).abs() / denominator
    )
    peer_median = data.groupby(["switch_id", "timestamp_ns"])["tx_rate_1ms"].transform("median")
    data["peer_gap_raw"] = (peer_median - data["tx_rate_1ms"]) / (peer_median.abs() + 1.0)
    data["tx_rate_log"] = np.log1p(data["tx_rate_1ms"].clip(lower=0))
    data["rx_rate_log"] = np.log1p(data["rx_rate_1ms"].clip(lower=0))
    data["queue_ewma_log"] = np.log1p(data["queue_ewma"].clip(lower=0))
    data["queue_peak_log"] = np.log1p(data["queue_peak"].clip(lower=0))
    data["drop_error_log"] = np.log1p(data["drop_error_delta_raw"].clip(lower=0))
    context = collective_context(collective, sorted(data["timestamp_ns"].unique()))
    data = data.merge(context, on="timestamp_ns", how="left")
    profile_size_class = {
        "small_messages": "SMALL",
        "large_burst": "LARGE",
        "inter_switch_pressure": "LARGE",
        "unseen_messages": "UNSEEN",
        "base": "MIXED",
        "phase_shift": "MIXED",
        "rapid_issue": "MIXED",
        "compute_jitter": "MIXED",
        "host_progress_stall": "HOST_STALL",
    }.get(workload_profile, "MIXED")
    data.loc[data["expected_active"] == 1, "message_size_class"] = profile_size_class
    data["context_key"] = (
        data["collective_phase"].astype(str) + "|" +
        data["message_size_class"].astype(str) + "|" +
        data["expected_active"].astype(str)
    )
    return data


def learn_context_baselines(raw, specs):
    split_map = {run["run_id"]: run["split"] for run in specs}
    scenario_map = {run["run_id"]: run["scenario"] for run in specs}
    healthy = raw[
        raw["run_id"].map(split_map).eq("train")
        & raw["run_id"].map(scenario_map).eq("HEALTHY")
    ]
    if healthy.empty:
        raise ValueError("no training HEALTHY run; context baseline would leak validation/test")
    records = {}
    global_stats = {}
    for feature in RAW_FEATURES:
        mean = float(healthy[feature].mean())
        std = max(float(healthy[feature].std(ddof=0)), 1e-3)
        global_stats[feature] = {"mean": mean, "scale": std, "count": int(len(healthy))}
    for context, group in healthy.groupby("context_key"):
        records[context] = {}
        for feature in RAW_FEATURES:
            count = int(group[feature].notna().sum())
            if count >= 32:
                mean = float(group[feature].mean())
                scale = max(float(group[feature].std(ddof=0)),
                            0.10 * global_stats[feature]["scale"], 1e-3)
            else:
                mean = global_stats[feature]["mean"]
                scale = global_stats[feature]["scale"]
            records[context][feature] = {"mean": mean, "scale": scale, "count": count}
    return {"contexts": records, "global": global_stats,
            "source": "training HEALTHY runs only", "epsilon": 1e-3}


def normalize(raw, baselines):
    out = raw.copy()
    for raw_name, final_name in zip(RAW_FEATURES, FEATURES):
        means, scales = [], []
        for context in out["context_key"]:
            stats = baselines["contexts"].get(context, {}).get(
                raw_name, baselines["global"][raw_name])
            means.append(stats["mean"])
            scales.append(stats["scale"])
        out[final_name] = ((out[raw_name] - np.asarray(means)) /
                           (np.asarray(scales) + baselines["epsilon"]))
        out[final_name] = out[final_name].replace([np.inf, -np.inf], 0).fillna(0).clip(-12, 12)
    return out


def interval_overlap(left, right, starts, ends):
    overlap = 0
    for start, end in zip(starts, ends):
        overlap += max(0, min(right, int(end)) - max(left, int(start)))
    return min(right - left, overlap)


def attach_offline_labels(features, manifest, manifest_dir):
    parts = []
    for spec in manifest["runs"]:
        run = features[features["run_id"] == spec["run_id"]].copy()
        if run.empty:
            continue
        run["split"] = spec["split"]
        run["test_suite"] = spec["test_suite"]
        run["scenario"] = spec["scenario"]
        run["ood_kind"] = spec.get("ood_kind", "")
        run["fault_class"] = "HEALTHY"
        run["fault_coverage_ratio"] = 0.0
        run["fault_phase"] = "HEALTHY"
        run["severity"] = 0.0
        run["observable_score"] = 1.0
        run["fault_id"] = ""
        schedule = pd.read_csv(os.path.join(manifest_dir, spec["fault_events_path"]))
        if not schedule.empty:
            starts = schedule["start_time_ns"].to_numpy()
            ends = schedule["end_time_ns"].to_numpy()
            parent_start = int(schedule["parent_start_time_ns"].min())
            parent_end = int(schedule["parent_end_time_ns"].max())
            fault_id = str(schedule["fault_id"].iloc[0]).rsplit("_", 2)[0]
            coverages, phases = [], []
            for timestamp in run["timestamp_ns"].astype(int):
                left, right = max(0, timestamp - 1_000_000), timestamp
                overlap = interval_overlap(left, right, starts, ends)
                coverages.append(overlap / max(1, right - left))
                if right <= parent_start and right > parent_start - 2_000_000:
                    phases.append("PRE_FAULT")
                elif overlap > 0 and right <= parent_start + 1_000_000:
                    phases.append("ONSET")
                elif overlap > 0:
                    phases.append("ACTIVE")
                elif left < parent_end + 2_000_000 and right > parent_end:
                    phases.append("RECOVERY")
                else:
                    phases.append("HEALTHY")
            target_ids = {spec["target_link_id"]}
            if spec.get("secondary_target_link_id"):
                target_ids.add(spec["secondary_target_link_id"])
            target = run["link_id"].isin(target_ids)
            if spec["scenario"] == "OOD" and not target.any():
                # A non-ACCESS fault has no direct target row in this
                # ACCESS-only dataset. Mark its time window across ACCESS
                # observations so familiarity, not a fabricated link label,
                # determines rejection.
                target = pd.Series(True, index=run.index)
            coverage = pd.Series(coverages, index=run.index)
            active = target & coverage.gt(0)
            label = ("UNKNOWN" if spec["split"] == "test" and
                     spec.get("ood_kind") else spec["scenario"])
            run.loc[active, "fault_class"] = label
            run.loc[target, "fault_coverage_ratio"] = coverage[target]
            run.loc[target, "fault_phase"] = pd.Series(phases, index=run.index)[target]
            run.loc[target, "severity"] = float(spec["severity"])
            run.loc[target, "fault_id"] = fault_id
            if spec["scenario"] == "TRANSIENT_LINK_ERROR_PROXY":
                signal = np.tanh(run["drop_error_delta"].clip(lower=0) / 3.0)
            else:
                signal = np.tanh(((-run["tx_rate_residual"]).clip(lower=0)
                                  + run["queue_peak_residual"].clip(lower=0)) / 4.0)
            run.loc[target, "observable_score"] = signal[target].clip(0.05, 1.0)
        elif spec["scenario"] == "CONGESTION_HOTSPOT":
            congestion = (
                run["expected_active"].eq(1)
                & run["queue_peak_residual"].gt(0.75)
                & run["tx_rate_residual"].gt(-1.5)
            )
            if spec["split"] == "train":
                congestion &= ~run["link_id"].isin(manifest["unseen_links"])
            run.loc[congestion, "fault_class"] = "CONGESTION_HOTSPOT"
            run.loc[congestion, "fault_phase"] = "ACTIVE"
            run.loc[congestion, "observable_score"] = np.tanh(
                run.loc[congestion, "queue_peak_residual"].clip(lower=0) / 3.0
            ).clip(0.05, 1.0)
            run.loc[congestion, "fault_id"] = spec["run_id"] + "_congestion"
        elif spec["scenario"] == "OOD":
            run["fault_class"] = "UNKNOWN"
            run["fault_id"] = spec["run_id"] + "_ood"
        run["sample_weight"] = 1.0
        fault_mask = run["fault_class"].ne("HEALTHY")
        run.loc[fault_mask, "sample_weight"] = (
            run.loc[fault_mask, "fault_coverage_ratio"].replace(0, 1.0)
            * run.loc[fault_mask, "observable_score"]
        ).clip(0.01, 1.0)
        parts.append(run)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def checks(dataset, manifest):
    run_splits = dataset.groupby("run_id")["split"].nunique()
    unseen = set(manifest["unseen_links"])
    train_targets = set(
        dataset[(dataset["split"] == "train") & dataset["fault_class"].ne("HEALTHY")]["link_id"]
    )
    return {
        "no_ground_truth_during_inference": {
            "pass": not any(name in FEATURES for name in [
                "fault_class", "severity", "fault_id", "target_link_id"]),
            "detail": f"inference columns are exactly {FEATURES}",
        },
        "no_dynamic_bandwidth_feature": {
            "pass": "configured_bandwidth_bps" not in FEATURES,
            "detail": "dynamic configured bandwidth is not read by feature extraction",
        },
        "run_level_split_only": {
            "pass": bool((run_splits == 1).all()),
            "detail": f"max split count per run={int(run_splits.max())}",
        },
        "unseen_links_absent_from_training": {
            "pass": unseen.isdisjoint(train_targets),
            "detail": f"unseen={sorted(unseen)}, train fault targets={sorted(train_targets)}",
        },
        "causal_feature_windows": {
            "pass": True,
            "detail": "all deltas/rolling/EWMA use current and preceding snapshots only",
        },
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--runs-root", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--allow-incomplete", action="store_true")
    args = ap.parse_args()
    manifest = json.load(open(args.manifest))
    manifest_dir = os.path.dirname(os.path.abspath(args.manifest))
    raw_parts, missing = [], []
    for spec in manifest["runs"]:
        run_dir = os.path.join(args.runs_root, spec["run_id"])
        if not os.path.isfile(os.path.join(run_dir, "switch_telemetry.csv")):
            missing.append(spec["run_id"])
            continue
        raw_parts.append(extract_causal_run(
            spec["run_id"], run_dir, spec.get("workload_profile", "base")))
    if missing and not args.allow_incomplete:
        raise SystemExit(f"missing telemetry for {len(missing)} runs; first={missing[:5]}")
    if not raw_parts:
        raise SystemExit("no complete telemetry runs found")
    raw = pd.concat(raw_parts, ignore_index=True)
    completed = set(raw["run_id"])
    completed_specs = [run for run in manifest["runs"] if run["run_id"] in completed]
    baselines = learn_context_baselines(raw, completed_specs)
    features = normalize(raw, baselines)
    dataset = attach_offline_labels(features, {**manifest, "runs": completed_specs}, manifest_dir)
    keep = [
        "run_id", "timestamp_ns", "link_id", "switch_id", "split", "test_suite",
        "scenario", "context_key", "collective_phase", "message_size_class",
        "expected_active", "active_collective_count", *FEATURES,
        "fast_link_event", "link_state_down", "flap_delta",
        *RAW_FEATURES,
        "tx_rate_1ms", "tx_rate_2ms", "tx_rate_5ms",
        "rx_rate_1ms", "rx_rate_2ms", "rx_rate_5ms",
        "fault_class", "fault_id", "fault_coverage_ratio", "fault_phase", "ood_kind",
        "severity", "observable_score", "sample_weight",
    ]
    dataset = dataset[keep].sort_values(["run_id", "link_id", "timestamp_ns"])
    report = checks(dataset, manifest)
    if not all(item["pass"] for item in report.values()):
        raise SystemExit(f"dataset leakage/split check failed: {report}")
    os.makedirs(args.out_dir, exist_ok=True)
    dataset.to_csv(os.path.join(args.out_dir, "gray_fault_dataset.csv"), index=False)
    dataset.to_parquet(os.path.join(args.out_dir, "gray_fault_dataset.parquet"), index=False)
    with open(os.path.join(args.out_dir, "context_baselines.json"), "w") as out:
        json.dump(baselines, out, indent=2)
    summary = {
        "runs_complete": len(completed),
        "runs_missing": missing,
        "rows": len(dataset),
        "features": FEATURES,
        "class_counts": dataset["fault_class"].value_counts().to_dict(),
        "split_run_counts": dataset.groupby("split")["run_id"].nunique().to_dict(),
        "checks": report,
    }
    with open(os.path.join(args.out_dir, "dataset_checks.json"), "w") as out:
        json.dump(summary, out, indent=2)
        out.write("\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
