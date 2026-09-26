#!/usr/bin/env python3
"""Generate the fixed mixed ACCESS-fault schedule used by all four baselines.

The schedule deliberately contains one bandwidth degradation, one short loss
burst, and one sub-snapshot link flap.  It is generated once and replayed by
the simulator; every detector is evaluated against this exact CSV.
"""
import argparse
import csv
import random

import pandas as pd


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--link-map", required=True)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--bandwidth-start-ns", type=int, default=4_100_000)
    ap.add_argument("--bandwidth-duration-ns", type=int, default=4_000_000)
    ap.add_argument("--bandwidth-fraction", type=float, default=0.50,
                    help="remaining bandwidth fraction (default 0.50)")
    ap.add_argument("--loss-start-ns", type=int, default=11_100_000)
    ap.add_argument("--loss-duration-ns", type=int, default=500_000)
    ap.add_argument("--loss-rate", type=float, default=0.01)
    ap.add_argument("--flap-start-ns", type=int, default=16_100_000)
    ap.add_argument("--flap-duration-ns", type=int, default=200_000)
    args = ap.parse_args()

    link_map = pd.read_csv(args.link_map)
    access = link_map[link_map["link_class"] == "ACCESS"].copy()
    if len(access) < 3:
        raise SystemExit("at least three ACCESS links are required")
    links = access["link_id"].tolist()
    random.Random(args.seed).shuffle(links)
    links = links[:3]
    bandwidth = int(access.set_index("link_id").loc[links[0], "bandwidth_bps"])
    degraded = max(1, int(bandwidth * args.bandwidth_fraction))

    rows = [
        {
            "fault_id": "bench000_bandwidth_degradation",
            "fault_type": "bandwidth_degradation",
            "target_link_id": links[0],
            "start_time_ns": args.bandwidth_start_ns,
            "end_time_ns": args.bandwidth_start_ns + args.bandwidth_duration_ns,
            "severity": f"{1.0 - args.bandwidth_fraction:.4f}",
            "parameter_before": f"{bandwidth // 1_000_000_000}Gbps",
            "parameter_after": f"{degraded // 1_000_000_000}Gbps",
        },
        {
            "fault_id": "bench001_packet_loss",
            "fault_type": "packet_loss",
            "target_link_id": links[1],
            "start_time_ns": args.loss_start_ns,
            "end_time_ns": args.loss_start_ns + args.loss_duration_ns,
            "severity": f"{args.loss_rate:.5f}",
            "parameter_before": "0.0",
            "parameter_after": f"{args.loss_rate:.5f}",
        },
        {
            "fault_id": "bench002_link_flap",
            "fault_type": "link_flap",
            "target_link_id": links[2],
            "start_time_ns": args.flap_start_ns,
            "end_time_ns": args.flap_start_ns + args.flap_duration_ns,
            "severity": "1.0000",
            "parameter_before": "up",
            "parameter_after": "down",
        },
    ]

    with open(args.out_csv, "w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} benchmark faults to {args.out_csv}")
    for row in rows:
        print(f"  {row['fault_id']}: {row['target_link_id']} "
              f"[{row['start_time_ns']},{row['end_time_ns']})")


if __name__ == "__main__":
    main()
