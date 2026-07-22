#!/usr/bin/env python3
"""Generate a reproducible random fault schedule for LIMER's dynamic
fault injector (limer::FaultInjector in limer_telemetry.h).

Output is a CSV with exactly fault_events.csv's schema
(fault_id,fault_type,target_link_id,start_time_ns,end_time_ns,severity,
parameter_before,parameter_after) - this file is used BOTH as the input
schedule fed to the simulator (via LIMER_FAULT_SCHEDULE) AND as the
ground-truth label file for later detection-algorithm testing (see
tools/build_monitoring_dataset.py's label alignment, which already joins
by (timestamp, link_id) post-hoc).

All randomness lives here, not in the C++ side: this SimAI build has no
exposed RNG (see docs/monitoring_design.md), and generating the schedule
ahead of time as a plain file means the exact same schedule can be
re-fed to any future run, replayed, or inspected before ever touching
the simulator - important for reproducibly testing a detection algorithm
later.

Packet-loss faults are OFF by default: 0.5% loss was found to hang the
simulator (limer/results/fault/fault-packet_loss/, exit 124). They can be
enabled with --include-packet-loss, at a lower default rate, but this
combination has not been validated to complete - see
limer/configs/packet_loss.yaml.
"""
import argparse
import csv
import random
import sys

import pandas as pd


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--link-map", required=True, help="link_map.csv to sample ACCESS links from")
    ap.add_argument("--sim-duration-ns", type=int, required=True,
                     help="approximate total simulated duration to place faults within "
                          "(use a prior healthy run's 'all passes finished at time' tick)")
    ap.add_argument("--num-faults", type=int, default=3)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--include-packet-loss", action="store_true",
                     help="also draw packet_loss faults (UNVALIDATED - known to hang at 0.5%% loss)")
    ap.add_argument("--bw-severity-min", type=float, default=0.3, help="min fractional bandwidth reduction")
    ap.add_argument("--bw-severity-max", type=float, default=0.8, help="max fractional bandwidth reduction")
    ap.add_argument("--loss-rate-min", type=float, default=0.0005)
    ap.add_argument("--loss-rate-max", type=float, default=0.002)
    ap.add_argument("--min-duration-ns", type=int, default=2_000_000, help="min fault duration (default 2ms)")
    ap.add_argument("--max-duration-ns", type=int, default=6_000_000, help="max fault duration (default 6ms)")
    ap.add_argument("--min-gap-ns", type=int, default=1_000_000, help="min gap between faults (default 1ms)")
    ap.add_argument("--edge-margin-ns", type=int, default=1_000_000,
                     help="keep faults at least this far from t=0 and from sim end")
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    rng = random.Random(args.seed)

    link_map = pd.read_csv(args.link_map)
    access_links = link_map[link_map["link_class"] == "ACCESS"]["link_id"].tolist()
    if not access_links:
        print("ERROR: no ACCESS links found in link_map.csv", file=sys.stderr)
        sys.exit(1)

    # Sequentially place non-overlapping windows: pick a duration, then a
    # start time in the remaining free space after the previous fault's
    # end (+min gap). Rejects (halves remaining budget) rather than
    # looping forever if it doesn't fit.
    lo = args.edge_margin_ns
    hi = args.sim_duration_ns - args.edge_margin_ns
    windows = []
    cursor = lo
    for i in range(args.num_faults):
        remaining = hi - cursor
        if remaining < args.min_duration_ns + args.min_gap_ns:
            print(f"WARNING: only placed {i}/{args.num_faults} faults - ran out of room "
                  f"in [{lo},{hi}]ns. Increase --sim-duration-ns or reduce --num-faults.",
                  file=sys.stderr)
            break
        duration = rng.randint(args.min_duration_ns, min(args.max_duration_ns, remaining - args.min_gap_ns))
        max_start_slack = remaining - duration - args.min_gap_ns
        start = cursor + args.min_gap_ns + (rng.randint(0, max_start_slack) if max_start_slack > 0 else 0)
        end = start + duration
        windows.append((start, end))
        cursor = end

    fault_types = ["bandwidth_degradation"]
    if args.include_packet_loss:
        fault_types.append("packet_loss")

    rows = []
    for i, (start, end) in enumerate(windows):
        link_id = rng.choice(access_links)
        ftype = rng.choice(fault_types)
        if ftype == "bandwidth_degradation":
            base_bw_bps = int(link_map.loc[link_map["link_id"] == link_id, "bandwidth_bps"].iloc[0])
            severity = rng.uniform(args.bw_severity_min, args.bw_severity_max)
            new_bw_bps = int(base_bw_bps * (1 - severity))
            param_before = f"{base_bw_bps // 1_000_000_000}Gbps"
            param_after = f"{max(1, new_bw_bps // 1_000_000_000)}Gbps"
        else:
            severity = rng.uniform(args.loss_rate_min, args.loss_rate_max)
            param_before = "0.0"
            param_after = f"{severity:.5f}"

        rows.append({
            "fault_id": f"rand{i:03d}_{ftype}",
            "fault_type": ftype,
            "target_link_id": link_id,
            "start_time_ns": start,
            "end_time_ns": end,
            "severity": f"{severity:.4f}",
            "parameter_before": param_before,
            "parameter_after": param_after,
        })

    with open(args.out_csv, "w", newline="") as f:
        # lineterminator="\n": csv's default dialect writes "\r\n", and the
        # C++ FaultInjector's line-based CSV reader only splits on "\n",
        # which left a trailing "\r" stuck to the last column and broke
        # ns3::DataRate's string parser ("40Gbps\r" != "40Gbps"). Fixed on
        # both sides - see limer_telemetry.h's SplitCsv/Init for the C++ fix.
        writer = csv.DictWriter(f, fieldnames=[
            "fault_id", "fault_type", "target_link_id", "start_time_ns",
            "end_time_ns", "severity", "parameter_before", "parameter_after"],
            lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} fault(s) to {args.out_csv} (seed={args.seed})")
    for r in rows:
        print(f"  {r['fault_id']}: {r['target_link_id']} [{r['start_time_ns']},{r['end_time_ns']}]ns "
              f"{r['parameter_before']}->{r['parameter_after']}")


if __name__ == "__main__":
    main()
