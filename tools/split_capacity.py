#!/usr/bin/env python3
"""Calibrated capacity input and B5's conditional, divisible-transfer bound.

This bound assumes independent rails, freely divisible payload, no startup,
no contention with other transfers and no collective dependencies. It is NOT
a measured SimAI result or a proven lower bound using noisy capacity estimates.
"""
import argparse
import csv
import json
import math
from pathlib import Path


def read_capacity(path):
    series = {}
    with Path(path).open(newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["src", "dst", "rail", "start_ns", "capacity_bps"]:
            raise ValueError("invalid capacity CSV header")
        for row in reader:
            key = tuple(int(row[k]) for k in ("src", "dst", "rail"))
            t, rate = int(row["start_ns"]), float(row["capacity_bps"])
            if not (0 <= key[0] < 16 and 0 <= key[1] < 16 and key[2] in (0, 1)):
                raise ValueError("invalid true-16 rank/rail")
            if t < 0 or not math.isfinite(rate) or rate < 0:
                raise ValueError("invalid capacity time/rate")
            points = series.setdefault(key, [])
            if (not points and t != 0) or (points and t <= points[-1][0]):
                raise ValueError("capacity points must start at zero and increase strictly")
            points.append((t, rate))
    return series


def rate_at(points, t):
    return next(rate for start, rate in reversed(points) if start <= t)


def future_bound(series, src, dst, size_bytes, start_ns=0):
    """Exact integral solution for a single fluid transfer, both rails usable."""
    if size_bytes < 0 or start_ns < 0:
        raise ValueError("negative payload/start")
    rails = [series[src, dst, r] for r in (0, 1)]
    boundaries = sorted({t for p in rails for t, _ in p if t > start_ns})
    remaining = size_bytes * 8.0
    allocation = [0.0, 0.0]
    now = float(start_ns)
    for stop in boundaries + [math.inf]:
        rates = [rate_at(p, now) for p in rails]
        total = sum(rates)
        available = total * (stop - now) / 1e9 if total else 0
        if remaining == 0 or (total and remaining <= available):
            elapsed = remaining * 1e9 / total if remaining else 0
            for r in (0, 1):
                allocation[r] += rates[r] * elapsed / 8e9
            return {"finish_ns": now + elapsed, "duration_ns": now + elapsed - start_ns,
                    "rail_bytes": allocation, "reachable": True}
        if math.isinf(stop):
            return {"finish_ns": None, "duration_ns": None,
                    "rail_bytes": allocation, "reachable": False}
        for r in (0, 1):
            allocation[r] += rates[r] * (stop-now) / 8e9
        remaining -= available
        now = stop
    raise AssertionError("unreachable")


def calibrate_completed_flows(paths, src, dst, rail):
    """Whole ACK-completed calibration workload goodput, not mean per-QP rates."""
    rates = []
    for path in paths:
        with Path(path).open(newline="") as f:
            rows = list(csv.DictReader(f))
        selected = [r for r in rows if int(r["src"]) == src and int(r["dst"]) == dst]
        starts = [r for r in selected if r["event"] == "ASSIGN"]
        ends = [r for r in selected if r["event"] == "ACK_COMPLETE"]
        if not starts or len(starts) != len(ends):
            raise ValueError("calibration must have all assigned chunks ACK-completed")
        if any(int(r["rail"]) != rail for r in selected):
            raise ValueError("calibration run is not single rail")
        if sum(int(r["bytes"]) for r in starts) != sum(int(r["bytes"]) for r in ends):
            raise ValueError("calibration byte mismatch")
        duration = max(int(r["timestamp_ns"]) for r in ends) - min(int(r["timestamp_ns"]) for r in starts)
        if duration <= 0:
            raise ValueError("invalid calibration duration")
        rates.append(sum(int(r["bytes"]) for r in ends)*8e9/duration)
    return sum(rates)/len(rates)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capacity", type=Path, required=True)
    parser.add_argument("--src", type=int, required=True)
    parser.add_argument("--dst", type=int, required=True)
    parser.add_argument("--bytes", type=int, required=True)
    parser.add_argument("--start-ns", type=int, default=0)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = future_bound(read_capacity(args.capacity), args.src, args.dst, args.bytes, args.start_ns)
    result.update(policy="B5", fidelity="conditional_two_rail_fluid_bound",
                  full_allreduce_bound=False, measured_data_plane=False)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False)+"\n")


if __name__ == "__main__":
    main()
