#!/usr/bin/env python3
"""Re-run the three historical B8 scenarios in a separate output directory."""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simai-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--historical-results", type=Path,
                        help="saved results root, or limer/evidence/server-20260926")
    args = parser.parse_args()
    root, out = args.simai_root.resolve(), args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    evidence = args.historical_results.resolve() if args.historical_results else root / "limer/results"
    history = evidence / "split_baselines"
    runner = root / "limer/tools/run_split_baselines.py"
    binary = root / "ns-3-alibabacloud/simulation/build/scratch/ns3.36.1-AstraSimNetwork-debug"
    topology = evidence / "true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
    report = {"scope": "B2/B6/B7/B8 on healthy, static and dynamic rail degradation", "tensor_numeric_correctness": "not simulated", "scenarios": {}}
    for scenario, capacity, fault in (
        ("healthy", "healthy_64mib_20260908/capacity.csv", None),
        ("static", "static_B_quarter_64mib_20260908/capacity.csv", "static_B_quarter_20260908.csv"),
        ("dynamic", "healthy_64mib_20260908/capacity.csv", "dynamic_B_quarter_at_1ms_20260908.csv"),
    ):
        command = [sys.executable, str(runner), "--binary", str(binary), "--topology", str(topology),
                   "--workload", str(root / "limer/configs/microAllReduce_16rank_split_64mib.txt"),
                   "--config", str(root / "limer/configs/SimAI.baseline.conf"),
                   "--policies", "B2", "B6", "B7", "B8", "--capacity", str(history / capacity),
                   "--timeout", "180", "--out", str(out / scenario)]
        if fault:
            command += ["--faults", str(history / fault)]
        print("Running", scenario, flush=True)
        with (out / (scenario + ".log")).open("w") as log:
            result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
        entry = {"exit_code": result.returncode, "policies": {}}
        summary = out / scenario / "summary.json"
        if summary.exists():
            actual = json.loads(summary.read_text())
            expected = json.loads((history / ("chunk_feedback_" + scenario + "_64mib_20260908") / "summary.json").read_text())
            for policy in ("B2", "B6", "B7", "B8"):
                current = actual.get("results", {}).get(policy, {})
                prior = expected["results"][policy]
                now, then = current.get("workload_finish_ns"), prior.get("workload_finish_ns")
                entry["policies"][policy] = {"pass": current.get("pass", False), "workload_finish_ns": now,
                    "historical_finish_ns": then, "delta_ns": now - then if now is not None else None,
                    "acked_payload_bytes": current.get("acked_payload_bytes"), "pending_chunks": current.get("pending_chunks"),
                    "chunk_feedback": current.get("chunk_feedback")}
            entry["same_payload_across_policies"] = actual.get("same_payload_across_policies", False)
        report["scenarios"][scenario] = entry
        (out / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
        print(scenario, "exit", result.returncode, flush=True)
    report["pass"] = all(s["exit_code"] == 0 and s.get("same_payload_across_policies") and len(s["policies"]) == 4 and all(p["pass"] for p in s["policies"].values()) for s in report["scenarios"].values())
    report["historical_finish_times_exact"] = report["pass"] and all(p["delta_ns"] == 0 for s in report["scenarios"].values() for p in s["policies"].values())
    (out / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print("PASS:", report["pass"], "historical times exact:", report["historical_finish_times_exact"], flush=True)
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
