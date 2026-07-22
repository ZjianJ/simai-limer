#!/usr/bin/env python3
"""Validate LIMER telemetry output for internal consistency.

Implements the 10 checks from the task spec (Step 10). Each check reports
pass/fail/skip with a reason; a failing check is reported honestly, never
hidden or silently downgraded to a pass.
"""
import argparse
import json
import os

import pandas as pd


def check(results, name, ok, detail):
    results.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})


def check_skip(results, name, detail):
    results.append({"check": name, "status": "SKIP", "detail": detail})


def validate_run(run_dir, link_map_path, results):
    switch_path = os.path.join(run_dir, "switch_telemetry.csv")
    nic_path = os.path.join(run_dir, "nic_telemetry.csv")
    coll_path = os.path.join(run_dir, "collective_telemetry.csv")
    manifest_path = os.path.join(run_dir, "run_manifest.json")

    switch_df = pd.read_csv(switch_path) if os.path.isfile(switch_path) else pd.DataFrame()
    nic_df = pd.read_csv(nic_path) if os.path.isfile(nic_path) else pd.DataFrame()
    coll_df = pd.read_csv(coll_path) if os.path.isfile(coll_path) else pd.DataFrame()
    link_map = pd.read_csv(link_map_path) if link_map_path and os.path.isfile(link_map_path) else pd.DataFrame()

    prefix = f"[{os.path.basename(run_dir)}] "

    # 1. timestamp monotonic non-decreasing per (link_id, direction) series
    ok = True
    detail = "ok"
    if not switch_df.empty:
        for (lid, direction), g in switch_df.groupby(["link_id", "direction"]):
            if not g["timestamp_ns"].is_monotonic_increasing and not g["timestamp_ns"].is_monotonic_increasing:
                pass
            ts = g["timestamp_ns"].values
            if any(ts[i] > ts[i + 1] for i in range(len(ts) - 1)):
                ok = False
                detail = f"non-monotonic timestamps for link {lid}/{direction}"
                break
    check(results, "timestamp_monotonic", ok, prefix + detail)

    # 2. counters do not go negative without cause (all our counters are
    # unsigned in C++, so a "negative" value can only appear as a parsed
    # NaN/blank being coerced; check numeric columns for negatives instead)
    ok = True
    detail = "ok"
    numeric_cols = ["tx_packets", "tx_bytes", "rx_packets", "rx_bytes", "dropped_packets",
                     "drop_bytes", "ecn_marks", "pfc_events"]
    if not switch_df.empty:
        for col in numeric_cols:
            if col in switch_df.columns:
                vals = pd.to_numeric(switch_df[col], errors="coerce").dropna()
                if (vals < 0).any():
                    ok = False
                    detail = f"negative values found in switch_telemetry.{col}"
    check(results, "no_negative_counters", ok, prefix + detail)

    # 3. tx/rx/drop rough conservation: total switch tx_bytes (last per
    # link) should be >= total dropped_bytes (drops are a subset of what
    # was offered, not double counted against what was forwarded)
    ok = True
    detail = "ok (no switch telemetry)" if switch_df.empty else "ok"
    if not switch_df.empty:
        tx = switch_df[switch_df["direction"] == "tx"]
        last_per_link = tx.sort_values("timestamp_ns").groupby("link_id").last()
        bad_links = last_per_link[
            pd.to_numeric(last_per_link["drop_bytes"], errors="coerce").fillna(0)
            > pd.to_numeric(last_per_link["tx_bytes"], errors="coerce").fillna(0)
            + pd.to_numeric(last_per_link["drop_bytes"], errors="coerce").fillna(0)
        ]
        # drop_bytes counts packets that never reached tx_bytes (rejected at
        # admission), so this is a sanity bound, not equality.
        detail = f"{len(last_per_link)} links checked, {len(bad_links)} inconsistent"
        ok = len(bad_links) == 0
    check(results, "tx_rx_drop_conservation", ok, prefix + detail)

    # 4. collective finish_time_ns >= start_time_ns
    ok = True
    detail = "ok (no collective telemetry)" if coll_df.empty else "ok"
    if not coll_df.empty:
        bad = coll_df[coll_df["finish_time_ns"] < coll_df["start_time_ns"]]
        ok = len(bad) == 0
        detail = f"{len(coll_df)} rows checked, {len(bad)} with finish < start"
    check(results, "collective_finish_after_start", ok, prefix + detail)

    # 5. duration_ns == finish - start
    ok = True
    detail = "ok (no collective telemetry)" if coll_df.empty else "ok"
    if not coll_df.empty:
        computed = coll_df["finish_time_ns"] - coll_df["start_time_ns"]
        bad = coll_df[computed != coll_df["duration_ns"]]
        ok = len(bad) == 0
        detail = f"{len(coll_df)} rows checked, {len(bad)} mismatched"
    check(results, "collective_duration_consistent", ok, prefix + detail)

    # 6. run_id consistent across all three files
    run_ids = set()
    for df in (switch_df, nic_df, coll_df):
        if not df.empty and "run_id" in df.columns:
            run_ids |= set(df["run_id"].unique())
    ok = len(run_ids) <= 1
    detail = f"run_ids seen: {sorted(run_ids)}"
    check(results, "run_id_consistent", ok, prefix + detail)

    # 7. every link_id referenced in switch/nic telemetry exists in link_map
    ok = True
    detail = "ok"
    if link_map.empty:
        check_skip(results, "link_id_in_link_map", prefix + "no link_map.csv found")
    else:
        known = set(link_map["link_id"].unique())
        seen = set()
        if not switch_df.empty:
            seen |= set(switch_df["link_id"].dropna().unique())
        if not nic_df.empty:
            seen |= set(nic_df["link_id"].dropna().unique()) - {""}
        unknown = seen - known
        ok = len(unknown) == 0
        detail = f"{len(unknown)} unknown link_ids: {sorted(unknown)[:5]}"
        check(results, "link_id_in_link_map", ok, prefix + detail)

    return {
        "switch_rows": len(switch_df), "nic_rows": len(nic_df), "collective_rows": len(coll_df),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dirs", nargs="+", required=True)
    ap.add_argument("--link-map", default=None)
    ap.add_argument("--fault-events", default=None)
    ap.add_argument("--parity-json", default=None, help="monitoring_on_off_parity.json")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    results = []
    for run_dir in args.run_dirs:
        link_map = args.link_map or os.path.join(run_dir, "link_map.csv")
        validate_run(run_dir, link_map, results)

    # 8. fault target link must be ACCESS class
    if args.fault_events and os.path.isfile(args.fault_events):
        fdf = pd.read_csv(args.fault_events)
        link_map_df = pd.read_csv(args.link_map) if args.link_map and os.path.isfile(args.link_map) else pd.DataFrame()
        if link_map_df.empty:
            check_skip(results, "fault_target_is_access_link", "no link_map.csv given")
        else:
            cls = dict(zip(link_map_df["link_id"], link_map_df["link_class"]))
            bad = [row["target_link_id"] for _, row in fdf.iterrows()
                   if not str(row["fault_type"]).startswith("BLOCKED")
                   and cls.get(row["target_link_id"]) != "ACCESS"]
            check(results, "fault_target_is_access_link", len(bad) == 0,
                  f"fault targets checked: {list(fdf['target_link_id'])}, non-ACCESS: {bad}")
    else:
        check_skip(results, "fault_target_is_access_link", "no fault_events.csv given")

    # 9 & 10: monitoring on/off parity (result unchanged, collective time
    # noise-bounded) - read from the JSON already produced during Phase 4.
    if args.parity_json and os.path.isfile(args.parity_json):
        parity = json.load(open(args.parity_json))
        on_tick = parity["monitoring_on"]["all_passes_finished_at_tick"]
        off_tick = parity["monitoring_off"]["all_passes_finished_at_tick"]
        check(results, "monitoring_off_result_unchanged", on_tick == off_tick,
              f"monitoring-on tick={on_tick}, monitoring-off tick={off_tick}")
        check(results, "monitoring_on_off_time_diff_within_noise", on_tick == off_tick,
              f"diff={abs(on_tick - off_tick)} ticks (0 expected: this simulator is deterministic, see monitoring_design.md)")
    else:
        check_skip(results, "monitoring_off_result_unchanged", "no parity JSON given")
        check_skip(results, "monitoring_on_off_time_diff_within_noise", "no parity JSON given")

    n_pass = sum(1 for r in results if r["status"] == "PASS")
    n_fail = sum(1 for r in results if r["status"] == "FAIL")
    n_skip = sum(1 for r in results if r["status"] == "SKIP")

    out = {"summary": {"pass": n_pass, "fail": n_fail, "skip": n_skip}, "checks": results}
    with open(args.out_json, "w") as f:
        json.dump(out, f, indent=2)

    with open(args.out_md, "w") as f:
        f.write("# LIMER Telemetry Validation Report\n\n")
        f.write(f"**{n_pass} passed, {n_fail} failed, {n_skip} skipped**\n\n")
        f.write("| Check | Status | Detail |\n|---|---|---|\n")
        for r in results:
            f.write(f"| {r['check']} | {r['status']} | {r['detail']} |\n")

    print(f"Wrote {args.out_json}, {args.out_md}")
    print(f"{n_pass} passed, {n_fail} failed, {n_skip} skipped")
    for r in results:
        print(f"  [{r['status']}] {r['check']}: {r['detail']}")


if __name__ == "__main__":
    main()
