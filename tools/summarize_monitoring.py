#!/usr/bin/env python3
"""Generate the 6 required LIMER M1 charts (PNG + PDF) from real telemetry.

Every chart reads directly from switch_telemetry.csv / nic_telemetry.csv /
collective_telemetry.csv / monitoring_overhead.csv under limer/results/ -
no synthetic or placeholder data.
"""
import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def savefig(fig, out_dir, name):
    fig.savefig(os.path.join(out_dir, f"{name}.png"), dpi=150, bbox_inches="tight")
    fig.savefig(os.path.join(out_dir, f"{name}.pdf"), bbox_inches="tight")
    plt.close(fig)


def chart_access_link_throughput(run_dir, link_map, out_dir):
    df = pd.read_csv(os.path.join(run_dir, "switch_telemetry.csv"))
    access_links = set(link_map[link_map["link_class"] == "ACCESS"]["link_id"])
    tx = df[(df["direction"] == "tx") & (df["link_id"].isin(access_links))].copy()
    tx["tx_bytes"] = pd.to_numeric(tx["tx_bytes"], errors="coerce")

    fig, ax = plt.subplots(figsize=(9, 5))
    for link_id, g in tx.groupby("link_id"):
        g = g.sort_values("timestamp_ns")
        t_ms = g["timestamp_ns"] / 1e6
        dt_s = g["timestamp_ns"].diff() / 1e9
        rate_bps = g["tx_bytes"].diff().clip(lower=0) * 8 / dt_s
        ax.plot(t_ms, rate_bps / 1e9, marker=".", label=link_id)
    ax.set_xlabel("Simulation time (ms)")
    ax.set_ylabel("Observed throughput (Gbps)")
    ax.set_title(f"ACCESS-link throughput over time ({os.path.basename(run_dir)})")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(alpha=0.3)
    savefig(fig, out_dir, "access_link_throughput_vs_time")


def chart_faulty_link_queue_depth(fault_run_dir, target_link_id, out_dir, label):
    df = pd.read_csv(os.path.join(fault_run_dir, "switch_telemetry.csv"))
    tx = df[(df["direction"] == "tx") & (df["link_id"] == target_link_id)].copy()
    tx = tx.sort_values("timestamp_ns")
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(tx["timestamp_ns"] / 1e6, pd.to_numeric(tx["queue_bytes"], errors="coerce"), marker=".")
    ax.set_xlabel("Simulation time (ms)")
    ax.set_ylabel("Egress queue occupancy (bytes)")
    ax.set_title(f"Faulty-link ({target_link_id}) queue depth over time - {label}")
    ax.grid(alpha=0.3)
    savefig(fig, out_dir, "faulty_link_queue_depth_vs_time")


def chart_faulty_link_drops_retrans(fault_run_dir, target_link_id, out_dir, label):
    switch_df = pd.read_csv(os.path.join(fault_run_dir, "switch_telemetry.csv"))
    nic_df = pd.read_csv(os.path.join(fault_run_dir, "nic_telemetry.csv"))
    tx = switch_df[(switch_df["direction"] == "tx") & (switch_df["link_id"] == target_link_id)].sort_values("timestamp_ns")
    nic = nic_df[nic_df["link_id"] == target_link_id].sort_values("timestamp_ns")

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(tx["timestamp_ns"] / 1e6, pd.to_numeric(tx["dropped_packets"], errors="coerce"),
              color="tab:red", marker=".", label="switch dropped_packets (cumulative)")
    ax1.set_xlabel("Simulation time (ms)")
    ax1.set_ylabel("Cumulative dropped packets", color="tab:red")
    ax2 = ax1.twinx()
    if not nic.empty:
        ax2.plot(nic["timestamp_ns"] / 1e6, pd.to_numeric(nic["nacks"], errors="coerce"),
                  color="tab:blue", marker="x", label="NIC nacks/retransmissions (cumulative)")
    ax2.set_ylabel("Cumulative NACKs / retransmissions", color="tab:blue")
    fig.suptitle(f"Faulty-link ({target_link_id}) drops/retransmissions - {label}")
    savefig(fig, out_dir, "faulty_link_drops_retransmissions_vs_time")


def chart_allreduce_duration_vs_sequence(run_dir, out_dir):
    df = pd.read_csv(os.path.join(run_dir, "collective_telemetry.csv"))
    df = df.sort_values("start_time_ns").reset_index(drop=True)
    df["flow_seq"] = range(len(df))
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(df["flow_seq"], df["duration_ns"] / 1e3, s=4, alpha=0.4)
    ax.set_xlabel("Flow completion sequence (flow-level granularity; see limer/README.md limitations)")
    ax.set_ylabel("Flow duration (us)")
    ax.set_title("AllReduce flow duration vs. completion order")
    ax.grid(alpha=0.3)
    savefig(fig, out_dir, "allreduce_duration_vs_iteration")


def chart_healthy_vs_faulty(healthy_run_dir, fault_run_dir, target_link_id, out_dir, fault_label):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, run_dir, title in [(axes[0], healthy_run_dir, "healthy"), (axes[1], fault_run_dir, fault_label)]:
        df = pd.read_csv(os.path.join(run_dir, "switch_telemetry.csv"))
        tx = df[(df["direction"] == "tx") & (df["link_id"] == target_link_id)].sort_values("timestamp_ns")
        ax.plot(tx["timestamp_ns"] / 1e6, pd.to_numeric(tx["tx_bytes"], errors="coerce"), marker=".")
        ax.set_xlabel("Simulation time (ms)")
        ax.set_ylabel("Cumulative tx_bytes")
        ax.set_title(f"{target_link_id} - {title}")
        ax.grid(alpha=0.3)
    fig.suptitle(f"Healthy vs. faulty telemetry comparison ({target_link_id})")
    savefig(fig, out_dir, "healthy_vs_faulty_comparison")


def chart_overhead(overhead_csv, out_dir):
    df = pd.read_csv(overhead_csv)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].bar(df["config"], df["wall_seconds"])
    axes[0].set_ylabel("Wall-clock runtime (s)")
    axes[0].set_title("Simulation wall-clock time by sampling config")
    axes[0].tick_params(axis="x", rotation=20)

    total_rows = df["switch_csv_rows"] + df["nic_csv_rows"] + df["collective_csv_rows"]
    axes[1].bar(df["config"], total_rows)
    axes[1].set_ylabel("Total telemetry rows written")
    axes[1].set_title("Telemetry row count by sampling config")
    axes[1].tick_params(axis="x", rotation=20)

    fig.suptitle("Monitoring instrumentation overhead (simulation-side, not real HW)")
    savefig(fig, out_dir, "monitoring_overhead_by_interval")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--healthy-run-dir", required=True)
    ap.add_argument("--fault-bw-run-dir", required=True)
    ap.add_argument("--fault-link-id", required=True)
    ap.add_argument("--overhead-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    link_map = pd.read_csv(os.path.join(args.healthy_run_dir, "link_map.csv"))

    chart_access_link_throughput(args.healthy_run_dir, link_map, args.out_dir)
    chart_faulty_link_queue_depth(args.fault_bw_run_dir, args.fault_link_id, args.out_dir, "bandwidth_degradation")
    chart_faulty_link_drops_retrans(args.fault_bw_run_dir, args.fault_link_id, args.out_dir, "bandwidth_degradation")
    chart_allreduce_duration_vs_sequence(args.healthy_run_dir, args.out_dir)
    chart_healthy_vs_faulty(args.healthy_run_dir, args.fault_bw_run_dir, args.fault_link_id, args.out_dir, "bandwidth_degradation")
    if os.path.isfile(args.overhead_csv):
        chart_overhead(args.overhead_csv, args.out_dir)

    print(f"Charts written to {args.out_dir}")


if __name__ == "__main__":
    main()
