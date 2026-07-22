#!/usr/bin/env python3
"""Write run_manifest.json for one LIMER experiment run.

Reads the topology file's header line directly (node_num gpu_per_server
nvswitch_num switch_num link_num gpu_type - confirmed format, see
limer/docs/code_map.md) rather than re-deriving GPU count from anywhere
else, and reads git_commit.txt/submodule_status.txt if the calling shell
script already dumped them (see run_healthy_monitoring.sh /
run_fault_monitoring.sh).
"""
import argparse
import json
import os


def read_topology_header(root_dir, topology_name):
    # Search common locations for the topology file used.
    candidates = [
        os.path.join(root_dir, "limer", "results", "baseline", "topology", topology_name),
        os.path.join(root_dir, "limer", "results", "fault", topology_name),
        topology_name,
    ]
    for c in candidates:
        if os.path.isfile(c):
            with open(c) as f:
                header = f.readline().split()
            if len(header) >= 6:
                return {
                    "node_num": int(header[0]),
                    "gpus_per_server": int(header[1]),
                    "nvswitch_num": int(header[2]),
                    "switch_num": int(header[3]),
                    "link_num": int(header[4]),
                    "gpu_type": header[5],
                }
    return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--root-dir", required=True)
    ap.add_argument("--topology-name", required=True)
    ap.add_argument("--topology-path", default=None)
    ap.add_argument("--workload-file", required=True)
    ap.add_argument("--configuration-file", required=True)
    ap.add_argument("--telemetry-interval-us", type=int, required=True)
    ap.add_argument("--fault-enabled", required=True)
    ap.add_argument("--fault-id", default=None)
    ap.add_argument("--random-seed", required=True)
    ap.add_argument("--start-wall-time", required=True)
    ap.add_argument("--end-wall-time", required=True)
    ap.add_argument("--exit-code", type=int, required=True)
    args = ap.parse_args()

    out_dir = os.path.dirname(args.out)
    git_commit = None
    gc_path = os.path.join(out_dir, "git_commit.txt")
    if os.path.isfile(gc_path):
        git_commit = open(gc_path).read().strip()

    submodule_commits = None
    sc_path = os.path.join(out_dir, "submodule_status.txt")
    if os.path.isfile(sc_path):
        submodule_commits = [l.strip() for l in open(sc_path).readlines() if l.strip()]

    topo_info = {}
    if args.topology_path and os.path.isfile(args.topology_path):
        with open(args.topology_path) as f:
            header = f.readline().split()
        if len(header) >= 6:
            topo_info = {
                "node_num": int(header[0]), "gpus_per_server": int(header[1]),
                "nvswitch_num": int(header[2]), "switch_num": int(header[3]),
                "link_num": int(header[4]), "gpu_type": header[5],
            }
    else:
        topo_info = read_topology_header(args.root_dir, args.topology_name)

    gpu_count = None
    if topo_info:
        gpu_count = topo_info["node_num"] - topo_info["switch_num"] - topo_info["nvswitch_num"]

    manifest = {
        "run_id": args.run_id,
        "git_commit": git_commit,
        "submodule_commits": submodule_commits,
        "topology_name": args.topology_name,
        "gpu_count": gpu_count,
        "gpus_per_server": topo_info.get("gpus_per_server"),
        "workload_file": args.workload_file,
        "configuration_file": args.configuration_file,
        "telemetry_interval_us": args.telemetry_interval_us,
        "fault_enabled": args.fault_enabled.lower() == "true",
        "fault_id": args.fault_id,
        "random_seed": args.random_seed,
        "start_wall_time": args.start_wall_time,
        "end_wall_time": args.end_wall_time,
        "exit_code": args.exit_code,
    }
    with open(args.out, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
