#!/usr/bin/env python3
"""Generate a fault-injected copy of a SimAI topology file.

Topology line format (confirmed by reading
astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h's
SetupNetwork(), not guessed): each of the `link_num` link lines is
`src dst data_rate delay error_rate`, read as
`topof >> src >> dst >> data_rate >> link_delay >> error_rate;`.
error_rate is a per-link RateErrorModel probability (ERROR_UNIT_PACKET),
applied only if > 0 - so both bandwidth and packet-loss faults can be
injected purely by editing one link's line, with no code changes.
"""
import argparse
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-topo", required=True)
    ap.add_argument("--out-topo", required=True)
    ap.add_argument("--src", type=int, required=True, help="link endpoint A (order-insensitive)")
    ap.add_argument("--dst", type=int, required=True, help="link endpoint B (order-insensitive)")
    ap.add_argument("--bandwidth", default=None, help="new data_rate, e.g. 50Gbps (unset = unchanged)")
    ap.add_argument("--error-rate", type=float, default=None, help="new per-link packet error rate, e.g. 0.005 (unset = unchanged)")
    args = ap.parse_args()

    with open(args.in_topo) as f:
        lines = f.readlines()

    header = lines[0]
    switch_ids_line = lines[1]
    link_lines = lines[2:]

    matched = 0
    out_link_lines = []
    for line in link_lines:
        stripped = line.rstrip("\n")
        if not stripped.strip():
            out_link_lines.append(line)
            continue
        parts = stripped.split()
        if len(parts) < 5:
            out_link_lines.append(line)
            continue
        src, dst, bw, delay, err = parts[0], parts[1], parts[2], parts[3], parts[4]
        if (int(src) == args.src and int(dst) == args.dst) or (int(src) == args.dst and int(dst) == args.src):
            matched += 1
            if args.bandwidth is not None:
                bw = args.bandwidth
            if args.error_rate is not None:
                err = str(args.error_rate)
            out_link_lines.append(f"{src} {dst} {bw} {delay} {err}\n")
        else:
            out_link_lines.append(line)

    if matched == 0:
        print(f"ERROR: no link ({args.src},{args.dst}) found in {args.in_topo}", file=sys.stderr)
        sys.exit(1)
    if matched > 1:
        print(f"ERROR: link ({args.src},{args.dst}) matched {matched} lines, expected exactly 1", file=sys.stderr)
        sys.exit(1)

    with open(args.out_topo, "w") as f:
        f.write(header)
        f.write(switch_ids_line)
        f.writelines(out_link_lines)

    print(f"Wrote {args.out_topo}: link ({args.src},{args.dst}) bandwidth={args.bandwidth} error_rate={args.error_rate}")


if __name__ == "__main__":
    main()
