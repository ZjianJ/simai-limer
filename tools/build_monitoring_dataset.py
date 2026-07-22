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
    df = switch_df.copy()
    df["window_start_ns"] = (df["timestamp_ns"] // window_ns) * window_ns
    df["window_end_ns"] = df["window_start_ns"] + window_ns

    tx = df[df["direction"] == "tx"].copy()
    tx["byte_rate_bps"] = pd.to_numeric(tx["tx_bytes"], errors="coerce")
    tx["drop_rate"] = pd.to_numeric(tx["dropped_packets"], errors="coerce")
    tx["queue_bytes"] = pd.to_numeric(tx["queue_bytes"], errors="coerce")
    tx["ecn_marks"] = pd.to_numeric(tx["ecn_marks"], errors="coerce")
    tx["pfc_events"] = pd.to_numeric(tx["pfc_events"], errors="coerce")
    tx["configured_bw"] = pd.to_numeric(tx["configured_bandwidth_bps"], errors="coerce")

    grouped = tx.groupby(["run_id", "link_id", "window_start_ns", "window_end_ns"])
    agg = grouped.agg(
        switch_tx_bytes_last=("tx_bytes", "last"),
        switch_tx_bytes_first=("tx_bytes", "first"),
        switch_drop_pkts_last=("dropped_packets", "last"),
        switch_drop_pkts_first=("dropped_packets", "first"),
        switch_queue_bytes_mean=("queue_bytes", "mean"),
        switch_queue_bytes_max=("queue_bytes", "max"),
        switch_ecn_last=("ecn_marks", "last"),
        switch_ecn_first=("ecn_marks", "first"),
        switch_pfc_last=("pfc_events", "last"),
        switch_pfc_first=("pfc_events", "first"),
        configured_bandwidth_bps=("configured_bw", "max"),
        n_samples=("timestamp_ns", "count"),
    ).reset_index()

    window_s = window_ns / 1e9
    agg["switch_tx_byte_rate_bps"] = (agg["switch_tx_bytes_last"] - agg["switch_tx_bytes_first"]).clip(lower=0) * 8 / window_s
    agg["switch_drop_rate_pkts_per_s"] = (agg["switch_drop_pkts_last"] - agg["switch_drop_pkts_first"]).clip(lower=0) / window_s
    agg["switch_ecn_rate_per_s"] = (agg["switch_ecn_last"] - agg["switch_ecn_first"]).clip(lower=0) / window_s
    agg["switch_pfc_rate_per_s"] = (agg["switch_pfc_last"] - agg["switch_pfc_first"]).clip(lower=0) / window_s
    agg["switch_utilization_mean"] = agg["switch_tx_byte_rate_bps"] / agg["configured_bandwidth_bps"]
    return agg


def window_nic(nic_df, window_ns):
    if nic_df.empty:
        return pd.DataFrame()
    df = nic_df.copy()
    df["window_start_ns"] = (df["timestamp_ns"] // window_ns) * window_ns
    df["window_end_ns"] = df["window_start_ns"] + window_ns
    for col in ["tx_bytes", "nacks", "outstanding_bytes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    grouped = df.groupby(["run_id", "link_id", "window_start_ns", "window_end_ns"])
    agg = grouped.agg(
        nic_tx_bytes_last=("tx_bytes", "last"),
        nic_tx_bytes_first=("tx_bytes", "first"),
        nic_nacks_last=("nacks", "last"),
        nic_nacks_first=("nacks", "first"),
        nic_outstanding_bytes_mean=("outstanding_bytes", "mean"),
        nic_outstanding_bytes_max=("outstanding_bytes", "max"),
    ).reset_index()

    window_s = window_ns / 1e9
    agg["nic_tx_byte_rate_bps"] = (agg["nic_tx_bytes_last"] - agg["nic_tx_bytes_first"]).clip(lower=0) * 8 / window_s
    agg["nic_nack_rate_per_s"] = (agg["nic_nacks_last"] - agg["nic_nacks_first"]).clip(lower=0) / window_s
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
    if not fault_events_path or not os.path.isfile(fault_events_path):
        return windows_df
    fdf = pd.read_csv(fault_events_path)
    if fdf.empty:
        return windows_df
    for _, frow in fdf.iterrows():
        if str(frow["fault_type"]).startswith("BLOCKED"):
            continue
        mask = (
            (windows_df["link_id"] == frow["target_link_id"])
            & (windows_df["window_start_ns"] >= frow["start_time_ns"])
            & (windows_df["window_start_ns"] < frow["end_time_ns"])
        )
        windows_df.loc[mask, "fault_id"] = frow["fault_id"]
        windows_df.loc[mask, "label"] = frow["fault_type"]
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
