#!/usr/bin/env python3
"""Generate the reproducible 16-GPU gray-fault benchmark manifest.

The default manifest contains the 300 runs required by the experiment task.
Each run receives an immutable workload and, when applicable, a sidecar fault
schedule. Ground truth remains outside raw telemetry and is never consumed by
inference. ``--pilot`` emits a small representative manifest for CI/smoke use.
"""
import argparse
import csv
import hashlib
import json
import os
import random
from collections import Counter

import pandas as pd


DEFAULT_COUNTS = {
    "HEALTHY": 40,
    "ACCESS_FAIL_SLOW": 100,
    "TRANSIENT_LINK_ERROR_PROXY": 70,
    "CONGESTION_HOTSPOT": 60,
    "OOD": 30,
}
PILOT_COUNTS = {
    "HEALTHY": 2,
    "ACCESS_FAIL_SLOW": 4,
    "TRANSIENT_LINK_ERROR_PROXY": 2,
    "CONGESTION_HOTSPOT": 3,
    "OOD": 2,
}
FAIL_FRACTIONS = [0.90, 0.75, 0.50, 0.25]
FAIL_DURATIONS_NS = [1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000]
FAIL_SHAPES = ["step", "ramp", "intermittent", "oscillation"]
PHASES_NS = [50_000, 200_000, 350_000, 500_000, 650_000, 800_000, 950_000]
ERROR_RATES = [0.0001, 0.001, 0.005, 0.01]
ERROR_DURATIONS_NS = [100_000, 200_000, 500_000, 1_000_000, 2_000_000]
RECOVERY_DELAYS_NS = [10_000, 50_000, 100_000]
WORKLOAD_PROFILES = [
    "base", "small_messages", "large_burst", "phase_shift",
    "rapid_issue", "compute_jitter", "inter_switch_pressure",
]


def workload_lines(template_path, profile, seed):
    lines = open(template_path).read().splitlines()
    header, count_line, layers = lines[0], lines[1], lines[2:]
    rng = random.Random(seed)
    mib = 1024 * 1024
    profiles = {
        "base": [16, 32, 64, 128, 16, 32, 64, 128, 16, 32],
        "small_messages": [4, 8, 16, 8, 4, 16, 8, 4, 16, 8],
        # Sustained 96/128 MiB layers trigger SimAI's pre-existing double-free
        # path even without faults. Keep difficult bursts below that boundary;
        # congestion pressure comes from cadence/repetition, not invalid runs.
        "large_burst": [64, 64, 48, 64, 64, 48, 64, 64, 48, 64],
        "phase_shift": [8, 8, 16, 32, 64, 64, 32, 16, 8, 8],
        "rapid_issue": [32, 64, 48, 64, 32, 64, 48, 32, 64, 48],
        "compute_jitter": [16, 64, 32, 48, 32, 16, 64, 48, 16, 32],
        "inter_switch_pressure": [64, 64, 48, 64, 48, 64, 64, 48, 64, 48],
        "unseen_messages": [24, 48, 24, 48, 24, 48, 24, 48, 24, 48],
        "host_progress_stall": [16, 32, 64, 32, 16, 64, 32, 16, 64, 32],
    }
    sizes = profiles[profile]
    out = [header, count_line]
    for index, line in enumerate(layers):
        fields = line.split()
        fields[4] = str(sizes[index % len(sizes)] * mib)
        if profile == "rapid_issue":
            fields[2] = "250000"
        elif profile == "compute_jitter":
            fields[2] = str(rng.choice([250000, 400000, 556000, 900000]))
        elif profile == "host_progress_stall":
            fields[2] = "5000000" if index == 4 else "556000"
        out.append(" ".join(fields))
    return "\n".join(out) + "\n"


def assign_split(rng, scenario, target, unseen_links, workload_profile, ood_kind=""):
    if scenario == "OOD":
        suite = {
            "unseen_severity": "Test-Unseen-Severity",
            "unseen_workload": "Test-Unseen-Workload",
            "unseen_link": "Test-Unseen-Link",
        }.get(ood_kind, "Test-OOD")
        return "test", suite
    if target in unseen_links:
        return "test", "Test-Unseen-Link"
    if workload_profile == "unseen_messages":
        return "test", "Test-Unseen-Workload"
    draw = rng.random()
    if draw < 0.60:
        return "train", ""
    if draw < 0.80:
        return "validation", ""
    return "test", "Test-ID"


def fault_rows(spec, bandwidth_by_link):
    scenario = spec["scenario"]
    is_ood_slow = scenario == "OOD" and spec.get("ood_kind") == "unseen_severity"
    is_ood_dual = scenario == "OOD" and spec.get("ood_kind") == "dual_link"
    is_ood_state = scenario == "OOD" and spec.get("ood_kind") in {"link_flap", "link_down"}
    is_ood_non_access = scenario == "OOD" and spec.get("ood_kind") == "non_access"
    if (scenario not in {"ACCESS_FAIL_SLOW", "TRANSIENT_LINK_ERROR_PROXY"}
            and not is_ood_slow and not is_ood_dual and not is_ood_state
            and not is_ood_non_access):
        return []
    common = {
        "fault_class": "UNKNOWN" if scenario == "OOD" else scenario,
        "target_link_id": spec["target_link_id"],
        "severity": spec["severity"],
        "parent_start_time_ns": spec["fault_start_ns"],
        "parent_end_time_ns": spec["fault_end_ns"],
        "shape": spec["shape"],
    }
    rows = []
    if is_ood_state:
        rows.append({
            "fault_id": f"{spec['run_id']}_{spec['ood_kind']}",
            "fault_type": "link_flap",
            "start_time_ns": spec["fault_start_ns"],
            "end_time_ns": spec["fault_end_ns"],
            "parameter_before": "up",
            "parameter_after": "down",
            "recovery_delay_ns": "",
            **common,
        })
        return rows
    if is_ood_dual:
        for index, target in enumerate([spec["target_link_id"], spec["secondary_target_link_id"]]):
            nominal = bandwidth_by_link[target] // 1_000_000_000
            rows.append({
                "fault_id": f"{spec['run_id']}_dual_{index}",
                "fault_type": "bandwidth_degradation",
                "target_link_id": target,
                "start_time_ns": spec["fault_start_ns"],
                "end_time_ns": spec["fault_end_ns"],
                "parameter_before": f"{nominal}Gbps",
                "parameter_after": f"{max(1, round(nominal * 0.5))}Gbps",
                "recovery_delay_ns": "",
                **{key: value for key, value in common.items() if key != "target_link_id"},
            })
        return rows
    if scenario == "TRANSIENT_LINK_ERROR_PROXY":
        rows.append({
            "fault_id": f"{spec['run_id']}_error",
            "fault_type": "packet_loss",
            "start_time_ns": spec["fault_start_ns"],
            "end_time_ns": spec["fault_end_ns"],
            "parameter_before": "0.0",
            "parameter_after": f"{spec['error_rate']:.6f}",
            "recovery_delay_ns": spec["recovery_delay_ns"],
            **common,
        })
        return rows

    nominal_gbps = bandwidth_by_link[spec["target_link_id"]] // 1_000_000_000
    fraction = spec["remaining_capacity_fraction"]
    degraded_gbps = max(1, round(nominal_gbps * fraction))
    start, end = spec["fault_start_ns"], spec["fault_end_ns"]
    shape = spec["shape"]
    segments = []
    if shape == "step" or end - start <= 1_000_000:
        segments = [(start, end, degraded_gbps)]
    elif shape == "ramp":
        ramp_ns = min(end - start, spec["ramp_duration_ns"])
        steps = max(1, min(3, ramp_ns // 500_000))
        for index in range(int(steps)):
            seg_start = start + index * ramp_ns // steps
            seg_end = start + (index + 1) * ramp_ns // steps
            level = round(nominal_gbps - (nominal_gbps - degraded_gbps) * (index + 1) / steps)
            segments.append((seg_start, seg_end, max(1, level)))
        if start + ramp_ns < end:
            segments.append((start + ramp_ns, end, degraded_gbps))
    elif shape == "intermittent":
        third = max(1, (end - start) // 3)
        segments = [(start, start + third, degraded_gbps),
                    (start + 2 * third, end, degraded_gbps)]
    else:  # oscillation
        width = max(250_000, (end - start) // 4)
        cursor, index = start, 0
        mild = max(degraded_gbps, round(nominal_gbps * min(0.95, fraction + 0.20)))
        while cursor < end:
            seg_end = min(end, cursor + width)
            segments.append((cursor, seg_end, degraded_gbps if index % 2 == 0 else mild))
            cursor, index = seg_end, index + 1
    for index, (seg_start, seg_end, level) in enumerate(segments):
        rows.append({
            "fault_id": f"{spec['run_id']}_slow_{index:02d}",
            "fault_type": "bandwidth_degradation",
            "start_time_ns": int(seg_start),
            "end_time_ns": int(seg_end),
            "parameter_before": f"{nominal_gbps}Gbps",
            "parameter_after": f"{level}Gbps",
            "recovery_delay_ns": "",
            **common,
        })
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--link-map", required=True)
    ap.add_argument("--workload-template", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--pilot", action="store_true")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    link_map = pd.read_csv(args.link_map)
    access = link_map[link_map["link_class"] == "ACCESS"].sort_values("link_id")
    if len(access) != 16:
        raise SystemExit(f"expected 16 ACCESS links, found {len(access)}")
    links = access["link_id"].tolist()
    bandwidth = {row.link_id: int(row.bandwidth_bps) for row in link_map.itertuples()}
    non_access_links = link_map[link_map["link_class"] != "ACCESS"]["link_id"].tolist()
    shuffled = links[:]
    rng.shuffle(shuffled)
    unseen_links = sorted(shuffled[:4])
    counts = PILOT_COUNTS if args.pilot else DEFAULT_COUNTS
    os.makedirs(args.out_dir, exist_ok=True)
    specs_dir = os.path.join(args.out_dir, "specs")
    os.makedirs(specs_dir, exist_ok=True)

    scenarios = [name for name, count in counts.items() for _ in range(count)]
    rng.shuffle(scenarios)
    target_cycle = links * ((sum(v for k, v in counts.items()
                                  if k in {"ACCESS_FAIL_SLOW", "TRANSIENT_LINK_ERROR_PROXY"})
                              + len(links) - 1) // len(links))
    rng.shuffle(target_cycle)
    target_index = 0
    ood_index = 0
    runs = []
    for index, scenario in enumerate(scenarios):
        run_id = f"gray_{index:04d}_{scenario.lower()}"
        run_dir = os.path.join(specs_dir, run_id)
        os.makedirs(run_dir, exist_ok=True)
        ood_kind = ""
        if scenario == "OOD":
            ood_kinds = ["unseen_severity", "unseen_workload", "dual_link",
                         "link_flap", "link_down", "non_access", "host_progress"]
            ood_kind = ood_kinds[ood_index % len(ood_kinds)]
            ood_index += 1
        target = ""
        if scenario in {"ACCESS_FAIL_SLOW", "TRANSIENT_LINK_ERROR_PROXY"}:
            target = target_cycle[target_index]
            target_index += 1
        elif scenario == "OOD" and ood_kind in {
                "unseen_severity", "dual_link", "link_flap", "link_down"}:
            target = rng.choice(links)
        elif scenario == "OOD" and ood_kind == "non_access":
            target = rng.choice(non_access_links)
        workload_profile = (
            "unseen_messages" if ood_kind == "unseen_workload"
            else "host_progress_stall" if ood_kind == "host_progress"
            else rng.choice(WORKLOAD_PROFILES)
        )
        split, test_suite = assign_split(
            rng, scenario, target, unseen_links, workload_profile, ood_kind)
        phase = rng.choice(PHASES_NS)
        start_ns = rng.choice([3_000_000, 4_000_000, 5_000_000]) + phase
        spec = {
            "run_id": run_id,
            "scenario": scenario,
            "split": split,
            "test_suite": test_suite,
            "seed": args.seed * 1000 + index,
            "target_link_id": target,
            "secondary_target_link_id": "",
            "workload_profile": workload_profile,
            "fault_start_ns": 0,
            "fault_end_ns": 0,
            "shape": "none",
            "severity": 0.0,
            "remaining_capacity_fraction": None,
            "error_rate": None,
            "recovery_delay_ns": None,
            "ramp_duration_ns": None,
            "spec_dir": os.path.relpath(run_dir, args.out_dir),
        }
        if scenario == "ACCESS_FAIL_SLOW":
            fraction = rng.choice(FAIL_FRACTIONS)
            duration = rng.choice(FAIL_DURATIONS_NS)
            spec.update({
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + duration,
                "shape": rng.choice(FAIL_SHAPES),
                "severity": 1.0 - fraction,
                "remaining_capacity_fraction": fraction,
                "ramp_duration_ns": rng.choice([1_000_000, 2_000_000, 3_000_000]),
            })
        elif scenario == "TRANSIENT_LINK_ERROR_PROXY":
            duration = rng.choice(ERROR_DURATIONS_NS)
            rate = rng.choice(ERROR_RATES)
            spec.update({
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + duration,
                "shape": "burst",
                "severity": rate,
                "error_rate": rate,
                "recovery_delay_ns": rng.choice(RECOVERY_DELAYS_NS),
            })
        elif scenario == "OOD" and ood_kind == "unseen_severity":
            spec.update({
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + rng.choice([3_000_000, 7_000_000]),
                "shape": "step",
                "severity": 0.40,
                "remaining_capacity_fraction": 0.60,
                "ramp_duration_ns": 1_000_000,
                "ood_kind": ood_kind,
            })
        elif scenario == "OOD" and ood_kind == "dual_link":
            second = rng.choice([link for link in links if link != target])
            spec.update({
                "secondary_target_link_id": second,
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + 5_000_000,
                "shape": "dual_step",
                "severity": 0.50,
                "ood_kind": ood_kind,
            })
        elif scenario == "OOD" and ood_kind in {"link_flap", "link_down"}:
            duration = 200_000 if ood_kind == "link_flap" else 100_000_000
            spec.update({
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + duration,
                "shape": ood_kind,
                "severity": 1.0,
                "ood_kind": ood_kind,
            })
        elif scenario == "OOD" and ood_kind == "non_access":
            spec.update({
                "fault_start_ns": start_ns,
                "fault_end_ns": start_ns + 5_000_000,
                "shape": "step",
                "severity": 0.50,
                "remaining_capacity_fraction": 0.50,
                "ramp_duration_ns": 1_000_000,
                "ood_kind": ood_kind,
            })
        else:
            spec["ood_kind"] = ood_kind

        workload_path = os.path.join(run_dir, "workload.txt")
        with open(workload_path, "w") as out:
            out.write(workload_lines(args.workload_template, workload_profile, spec["seed"]))
        rows = fault_rows(spec, bandwidth)
        schedule_path = os.path.join(run_dir, "fault_events.csv")
        fieldnames = [
            "fault_id", "fault_type", "target_link_id", "start_time_ns",
            "end_time_ns", "severity", "parameter_before", "parameter_after",
            "recovery_delay_ns", "fault_class", "parent_start_time_ns",
            "parent_end_time_ns", "shape",
        ]
        with open(schedule_path, "w", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        spec["workload_path"] = os.path.relpath(workload_path, args.out_dir)
        spec["fault_events_path"] = os.path.relpath(schedule_path, args.out_dir)
        spec["workload_sha256"] = hashlib.sha256(open(workload_path, "rb").read()).hexdigest()
        spec["fault_events_sha256"] = hashlib.sha256(open(schedule_path, "rb").read()).hexdigest()
        runs.append(spec)

    # Guarantee the minimum roles needed to learn healthy context and tune
    # thresholds even in the tiny pilot manifest. Never move an unseen-link
    # fault into training.
    for scenario in ["HEALTHY", "ACCESS_FAIL_SLOW", "CONGESTION_HOTSPOT"]:
        candidates = [run for run in runs if run["scenario"] == scenario
                      and run["target_link_id"] not in unseen_links]
        if candidates and not any(run["split"] == "train" for run in candidates):
            candidates[0]["split"], candidates[0]["test_suite"] = "train", ""
        if len(candidates) > 1 and not any(run["split"] == "validation" for run in candidates):
            candidates[1]["split"], candidates[1]["test_suite"] = "validation", ""

    target_counts = Counter(run["target_link_id"] for run in runs
                            if run["target_link_id"])
    manifest = {
        "schema_version": 1,
        "seed": args.seed,
        "pilot": args.pilot,
        "counts_requested": counts,
        "counts_generated": dict(Counter(run["scenario"] for run in runs)),
        "unseen_links": unseen_links,
        "access_links": links,
        "target_counts": dict(sorted(target_counts.items())),
        "label_isolation": "fault schedules are sidecars and never detector inputs",
        "runs": runs,
    }
    manifest_path = os.path.join(args.out_dir, "dataset_manifest.json")
    with open(manifest_path, "w") as out:
        json.dump(manifest, out, indent=2)
        out.write("\n")
    print(f"Wrote {len(runs)} run specifications to {manifest_path}")
    print(f"Unseen links: {unseen_links}")
    print(f"Scenarios: {manifest['counts_generated']}")


if __name__ == "__main__":
    main()
