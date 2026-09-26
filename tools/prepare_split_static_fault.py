#!/usr/bin/env python3
"""Create a constant ACCESS service impairment using an actual true-16 link map."""
import argparse
import csv
import math
from pathlib import Path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--link-map",type=Path,required=True)
    parser.add_argument("--rail",choices=("A","B"),required=True)
    parser.add_argument("--fraction",type=float,required=True)
    parser.add_argument("--end-ns",type=int,default=100000000)
    parser.add_argument("--start-ns",type=int,default=0)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    if not math.isfinite(args.fraction) or not 0<args.fraction<=1 or not 0<=args.start_ns<args.end_ns:
        parser.error("fraction must be in (0,1] and end positive")
    port=2 if args.rail=="A" else 3
    with args.link_map.open(newline="") as f:
        links=[r for r in csv.DictReader(f) if r["link_class"]=="ACCESS"
               and r["src_type"]=="HOST" and int(r["src_port"])==port]
    if len(links)!=16 or {int(r["src_node"]) for r in links}!=set(range(16)):
        parser.error("link map must contain exactly one selected ACCESS rail for each of 16 GPUs")
    args.out.parent.mkdir(parents=True,exist_ok=True)
    with args.out.open("x",newline="") as f:
        writer=csv.writer(f,lineterminator="\n")
        writer.writerow(("fault_id","fault_type","target_link_id","start_time_ns","end_time_ns",
                         "severity","parameter_before","parameter_after","recovery_delay_ns"))
        for link in links:
            writer.writerow((f"split-static-{args.rail}-{link['src_node']}","service_degradation",
                             link["link_id"],args.start_ns,args.end_ns,1-args.fraction,1,args.fraction,0))


if __name__=="__main__": main()
