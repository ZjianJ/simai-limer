#!/usr/bin/env python3
"""Build a windowed monitoring dataset from LIMER telemetry CSVs.

Reads switch_telemetry.csv / nic_telemetry.csv / collective_telemetry.csv
(+ optional fault_events.csv) from one or more run directories, buckets
them into fixed-width time windows per link, computes the window features
listed in the task spec, and joins fault ground truth post-hoc by
(timestamp, link_id) only - never by writing labels into the source
telemetry files (see limer/docs/telemetry_schema.md "Label isolation").
"""
import argparse
import glob
import json
import os

import pandas as pd


def load_run(run_dir):
    switch_path = os.path.join(run_dir, "switch_telemetry.csv")
    nic_path = os.path.join(run_dir, "nic_telemetry.csv")
    coll_path = os.path.join(run_dir, "collective_telemetry.csv")
    switch_df = pd.read_csv(switch_path) if os.path.isfile(switch_path) else pd.DataFrame()
    nic_df = pd.read_csv(nic_path) if os.path.isfile(nic_path) else pd.DataFrame()
    coll_df = pd.read_csv(coll_path) if os.path.isfile(coll_path) else pd.DataFrame()
    return switch_df, nic_df, coll_df


def load_link_map(run_dir):
    for cand in [
        os.path.join(run_dir, "link_map.csv"),
        os.path.join(os.path.dirname(run_dir), "..", "baseline", "topology", "link_map.csv"),
    ]:
        if os.path.isfile(cand):
            return pd.read_csv(cand)
    return pd.DataFrame()


def window_switch(switch_df, window_ns):
    if switch_df.empty:
        return pd.DataFrame()
    # INTER_SWITCH links have two independently monitored fabric endpoints.
    # Taking first/last after grouping only by link_id mixes those cumulative
    # counters and produces invalid deltas.  Compute deltas per endpoint first,
    # then aggregate the two directions into one physical-link signal.
    tx = switch_df[switch_df["direction"] == "tx"].copy()
    endpoint = ["run_id", "switch_id", "port_id", "link_id"]
    counters = ["tx_bytes", "dropped_packets", "ecn_marks", "pfc_events"]
    numeric = counters + ["queue_bytes", "max_queue_bytes",
                          "configured_bandwidth_bps"]
    for col in numeric:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0)
    tx = tx.sort_values(endpoint + ["timestamp_ns"])
    for col in counters:
        previous = tx.groupby(endpoint, sort=False)[col].shift(fill_value=0)
        tx[f"{col}_delta"] = (tx[col] - previous).clip(lower=0)

    # A sample at exactly 5 ms closes (4 ms, 5 ms], so assign it to the
    # [0 ms, 5 ms) feature window.  This retains the first 0..1 ms delta too.
    closed_interval_ns = (tx["timestamp_ns"] - 1).clip(lower=0)
    tx["window_start_ns"] = (closed_interval_ns // window_ns) * window_ns
    tx["window_end_ns"] = tx["window_start_ns"] + window_ns

    per_tick = tx.groupby(
        ["run_id", "link_id", "window_start_ns", "window_end_ns", "timestamp_ns"]
    ).agg(
        tx_bytes_delta=("tx_bytes_delta", "sum"),
        drop_packets_delta=("dropped_packets_delta", "sum"),
        ecn_delta=("ecn_marks_delta", "sum"),
        pfc_delta=("pfc_events_delta", "sum"),
        queue_bytes_total=("queue_bytes", "sum"),
        event_max_queue_bytes=("max_queue_bytes", "max"),
        aggregate_bandwidth_bps=("configured_bandwidth_bps", "sum"),
        endpoint_samples=("switch_id", "count"),
    ).reset_index()

    grouped = per_tick.groupby(
        ["run_id", "link_id", "window_start_ns", "window_end_ns"]
    )
    agg = grouped.agg(
        switch_tx_bytes_delta=("tx_bytes_delta", "sum"),
        switch_drop_packets_delta=("drop_packets_delta", "sum"),
        switch_queue_bytes_mean=("queue_bytes_total", "mean"),
        switch_queue_bytes_max=("queue_bytes_total", "max"),
        switch_event_max_queue_bytes=("event_max_queue_bytes", "max"),
        switch_ecn_delta=("ecn_delta", "sum"),
        switch_pfc_delta=("pfc_delta", "sum"),
        configured_bandwidth_bps=("aggregate_bandwidth_bps", "mean"),
        n_samples=("timestamp_ns", "nunique"),
        endpoint_samples=("endpoint_samples", "sum"),
    ).reset_index()

    window_s = window_ns / 1e9
    agg["switch_tx_byte_rate_bps"] = agg["switch_tx_bytes_delta"] * 8 / window_s
    agg["switch_drop_rate_pkts_per_s"] = agg["switch_drop_packets_delta"] / window_s
    agg["switch_ecn_rate_per_s"] = agg["switch_ecn_delta"] / window_s
    agg["switch_pfc_rate_per_s"] = agg["switch_pfc_delta"] / window_s
    agg["switch_utilization_mean"] = (
        agg["switch_tx_byte_rate_bps"] / agg["configured_bandwidth_bps"].replace(0, pd.NA)
    )
    return agg


def window_nic(nic_df, window_ns):
    if nic_df.empty:
        return pd.DataFrame()
    df = nic_df.copy()
    closed_interval_ns = (df["timestamp_ns"] - 1).clip(lower=0)
    df["window_start_ns"] = (closed_interval_ns // window_ns) * window_ns
    df["window_end_ns"] = df["window_start_ns"] + window_ns
    for col in ["tx_bytes", "nacks", "outstanding_bytes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    endpoint = ["run_id", "node_id", "nic_id", "link_id"]
    df = df.sort_values(endpoint + ["timestamp_ns"])
    for col in ["tx_bytes", "nacks"]:
        previous = df.groupby(endpoint, sort=False)[col].shift(fill_value=0)
        df[f"{col}_delta"] = (df[col] - previous).clip(lower=0)

    grouped = df.groupby(["run_id", "link_id", "window_start_ns", "window_end_ns"])
    agg = grouped.agg(
        nic_tx_bytes_delta=("tx_bytes_delta", "sum"),
        nic_nacks_delta=("nacks_delta", "sum"),
        nic_outstanding_bytes_mean=("outstanding_bytes", "mean"),
        nic_outstanding_bytes_max=("outstanding_bytes", "max"),
    ).reset_index()

    window_s = window_ns / 1e9
    agg["nic_tx_byte_rate_bps"] = agg["nic_tx_bytes_delta"] * 8 / window_s
    agg["nic_nack_rate_per_s"] = agg["nic_nacks_delta"] / window_s
    return agg


def window_collective(coll_df, window_ns):
    if coll_df.empty:
        return pd.DataFrame()
    df = coll_df.copy()
    df["window_start_ns"] = (df["finish_time_ns"] // window_ns) * window_ns
    df["window_end_ns"] = df["window_start_ns"] + window_ns
    grouped = df.groupby(["run_id", "window_start_ns", "window_end_ns"])
    agg = grouped.agg(
        active_collective_count=("collective_id", "nunique"),
        collective_duration_mean_ns=("duration_ns", "mean"),
        collective_duration_max_ns=("duration_ns", "max"),
    ).reset_index()
    return agg


def attach_fault_labels(windows_df, fault_events_path):
    windows_df["fault_id"] = ""
    windows_df["label"] = "normal"
    windows_df["fault_overlap_fraction"] = 0.0
    if not fault_events_path or not os.path.isfile(fault_events_path):
        return windows_df
    fdf = pd.read_csv(fault_events_path)
    if fdf.empty:
        return windows_df
    for _, frow in fdf.iterrows():
        if str(frow["fault_type"]).startswith("BLOCKED"):
            continue
        overlap_ns = (
            windows_df["window_end_ns"].clip(upper=frow["end_time_ns"])
            - windows_df["window_start_ns"].clip(lower=frow["start_time_ns"])
        ).clip(lower=0)
        mask = (windows_df["link_id"] == frow["target_link_id"]) & (overlap_ns > 0)
        windows_df.loc[mask, "fault_id"] = frow["fault_id"]
        windows_df.loc[mask, "label"] = frow["fault_type"]
        windows_df.loc[mask, "fault_overlap_fraction"] = (
            overlap_ns[mask] / (windows_df.loc[mask, "window_end_ns"]
                                - windows_df.loc[mask, "window_start_ns"])
        )
    return windows_df


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dirs", nargs="+", required=True, help="one or more run output directories")
    ap.add_argument("--window-ns", type=int, default=5_000_000, help="window width in ns (default 5ms)")
    ap.add_argument("--fault-events", default=None, help="fault_events.csv (optional)")
    ap.add_argument("--out-parquet", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-summary", required=True)
    args = ap.parse_args()

    all_windows = []
    link_class_map = {}
    for run_dir in args.run_dirs:
        switch_df, nic_df, coll_df = load_run(run_dir)
        link_map = load_link_map(run_dir)
        if not link_map.empty:
            for _, r in link_map.iterrows():
                link_class_map[r["link_id"]] = r["link_class"]

        sw_win = window_switch(switch_df, args.window_ns)
        nic_win = window_nic(nic_df, args.window_ns)
        coll_win = window_collective(coll_df, args.window_ns)

        merged = sw_win
        if not nic_win.empty:
            merged = pd.merge(merged, nic_win, on=["run_id", "link_id", "window_start_ns", "window_end_ns"], how="outer")
        if not merged.empty and not coll_win.empty:
            merged = pd.merge(merged, coll_win, on=["run_id", "window_start_ns", "window_end_ns"], how="left")
        if not merged.empty:
            all_windows.append(merged)

    if not all_windows:
        print("No windows produced (no telemetry found in given run-dirs).")
        windows_df = pd.DataFrame()
    else:
        windows_df = pd.concat(all_windows, ignore_index=True)
        windows_df["link_class"] = windows_df["link_id"].map(link_class_map).fillna("UNKNOWN")
        windows_df = attach_fault_labels(windows_df, args.fault_events)

    os.makedirs(os.path.dirname(args.out_parquet) or ".", exist_ok=True)
    if not windows_df.empty:
        windows_df.to_parquet(args.out_parquet, index=False)
    windows_df.to_csv(args.out_csv, index=False)

    summary = {
        "window_ns": args.window_ns,
        "run_dirs": args.run_dirs,
        "n_windows": int(len(windows_df)),
        "n_runs": int(windows_df["run_id"].nunique()) if not windows_df.empty else 0,
        "n_links": int(windows_df["link_id"].nunique()) if not windows_df.empty else 0,
        "label_counts": windows_df["label"].value_counts().to_dict() if not windows_df.empty and "label" in windows_df else {},
        "link_class_counts": windows_df["link_class"].value_counts().to_dict() if not windows_df.empty and "link_class" in windows_df else {},
    }
    with open(args.out_summary, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"Wrote {args.out_parquet}, {args.out_csv}, {args.out_summary}")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
