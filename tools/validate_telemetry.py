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


DEFAULT_SAMPLE_INTERVAL_NS = 1_000_000

SWITCH_REQUIRED_COLUMNS = {
    "run_id", "timestamp_ns", "switch_id", "port_id", "link_id",
    "peer_node_id", "direction", "tx_packets", "tx_bytes", "rx_packets",
    "rx_bytes", "dropped_packets", "drop_bytes",
}
NIC_REQUIRED_COLUMNS = {
    "run_id", "timestamp_ns", "node_id", "nic_id", "link_id",
    "tx_packets", "tx_bytes", "rx_packets", "rx_bytes",
    "tx_dropped_packets", "tx_drop_bytes", "rx_dropped_packets",
    "rx_drop_bytes",
}
LINK_MAP_REQUIRED_COLUMNS = {
    "link_id", "src_node", "dst_node", "src_type", "dst_type",
    "src_port", "dst_port", "link_class", "bandwidth_bps", "delay_ns",
}
COLLECTIVE_REQUIRED_COLUMNS = {
    "run_id", "start_time_ns", "finish_time_ns", "duration_ns",
}


def check(results, name, ok, detail):
    results.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})


def check_skip(results, name, detail):
    results.append({"check": name, "status": "SKIP", "detail": detail})


def _missing_columns(frame, required):
    return sorted(required - set(frame.columns))


def _endpoint_inventory(link_map):
    """Return the exact physical endpoint inventory encoded by link_map."""
    fabric = set()
    hosts = set()
    for row in link_map.itertuples(index=False):
        for side in ("src", "dst"):
            node = int(getattr(row, f"{side}_node"))
            port = int(getattr(row, f"{side}_port"))
            link_id = str(row.link_id)
            endpoint = (node, port, link_id)
            if str(getattr(row, f"{side}_type")) == "HOST":
                hosts.add(endpoint)
            else:
                fabric.add(endpoint)
    return fabric, hosts


def _sample_inventory_diagnostics(switch_df, nic_df, link_map):
    """Compare every sample against exact endpoint sets, not just row counts.

    This intentionally catches the subtle case where a missing endpoint is
    replaced by a duplicate endpoint and the aggregate 560/48 row counts still
    look correct.
    """
    expected_fabric, expected_hosts = _endpoint_inventory(link_map)
    duplicate_examples = []
    mismatch_examples = []
    bad_duplicate_samples = set()
    bad_coverage_samples = set()

    for timestamp, sample in switch_df.groupby("timestamp_ns", sort=True):
        for direction in ("tx", "rx"):
            directional = sample[sample["direction"] == direction]
            keys = [
                (int(row.switch_id), int(row.port_id), str(row.link_id))
                for row in directional.itertuples(index=False)
            ]
            duplicates = pd.Series(keys, dtype="object").duplicated().sum()
            observed = set(keys)
            missing = expected_fabric - observed
            extra = observed - expected_fabric
            if duplicates:
                bad_duplicate_samples.add(int(timestamp))
                duplicate_examples.append(
                    {"timestamp_ns": int(timestamp), "kind": f"fabric_{direction}",
                     "duplicate_rows": int(duplicates)}
                )
            if missing or extra:
                bad_coverage_samples.add(int(timestamp))
                mismatch_examples.append(
                    {"timestamp_ns": int(timestamp), "kind": f"fabric_{direction}",
                     "missing": sorted(missing)[:3], "extra": sorted(extra)[:3]}
                )

    for timestamp, sample in nic_df.groupby("timestamp_ns", sort=True):
        keys = [
            (int(row.node_id), int(row.nic_id), str(row.link_id))
            for row in sample.itertuples(index=False)
        ]
        duplicates = pd.Series(keys, dtype="object").duplicated().sum()
        observed = set(keys)
        missing = expected_hosts - observed
        extra = observed - expected_hosts
        if duplicates:
            bad_duplicate_samples.add(int(timestamp))
            duplicate_examples.append(
                {"timestamp_ns": int(timestamp), "kind": "host_nic",
                 "duplicate_rows": int(duplicates)}
            )
        if missing or extra:
            bad_coverage_samples.add(int(timestamp))
            mismatch_examples.append(
                {"timestamp_ns": int(timestamp), "kind": "host_nic",
                 "missing": sorted(missing)[:3], "extra": sorted(extra)[:3]}
            )

    return {
        "expected_fabric_endpoints": len(expected_fabric),
        "expected_host_endpoints": len(expected_hosts),
        "duplicate_sample_count": len(bad_duplicate_samples),
        "coverage_mismatch_sample_count": len(bad_coverage_samples),
        "duplicate_examples": duplicate_examples[:5],
        "mismatch_examples": mismatch_examples[:5],
    }


def _counter_monotonicity_diagnostics(switch_df, nic_df):
    switch_counters = [
        "tx_packets", "tx_bytes", "rx_packets", "rx_bytes",
        "dropped_packets", "drop_bytes", "ecn_marks", "pfc_events",
        "link_errors", "recovered_packets", "recovered_bytes", "flap_count",
        "cumulative_link_down_ns",
    ]
    nic_counters = [
        "tx_packets", "tx_bytes", "rx_packets", "rx_bytes",
        "tx_dropped_packets", "tx_drop_bytes", "rx_dropped_packets",
        "rx_drop_bytes", "link_errors", "recovered_packets",
        "recovered_bytes", "retransmissions", "nacks", "flap_count",
        "cumulative_link_down_ns",
    ]
    violations = []

    for endpoint, group in switch_df.groupby(
            ["switch_id", "port_id", "link_id", "direction"], sort=False):
        group = group.sort_values("timestamp_ns")
        for column in switch_counters:
            if column not in group.columns:
                continue
            values = pd.to_numeric(group[column], errors="coerce").dropna()
            if (values.diff().dropna() < 0).any():
                violations.append(
                    {"source": "switch", "endpoint": list(endpoint),
                     "counter": column}
                )

    for endpoint, group in nic_df.groupby(
            ["node_id", "nic_id", "link_id"], sort=False):
        group = group.sort_values("timestamp_ns")
        for column in nic_counters:
            if column not in group.columns:
                continue
            values = pd.to_numeric(group[column], errors="coerce").dropna()
            if (values.diff().dropna() < 0).any():
                violations.append(
                    {"source": "nic", "endpoint": list(endpoint),
                     "counter": column}
                )

    return violations


def _as_nonnegative_int(value):
    if pd.isna(value):
        return 0
    return int(value)


def _link_conservation_diagnostics(switch_df, nic_df, link_map):
    """Check both directions of every physical link at every sample.

    Cumulative sender TX must equal peer RX plus peer receive drops, modulo
    packets still in flight.  The allowance is one bandwidth-delay product
    plus two maximum-sized RDMA frames, matching the simulator validation
    contract.
    """
    fabric = {}
    for row in switch_df.itertuples(index=False):
        key = (int(row.timestamp_ns), int(row.switch_id), int(row.port_id),
               str(row.link_id))
        endpoint = fabric.setdefault(
            key, {"tx": None, "rx": None, "rx_drop": 0})
        if row.direction == "tx":
            endpoint["tx"] = _as_nonnegative_int(row.tx_bytes)
        elif row.direction == "rx":
            endpoint["rx"] = _as_nonnegative_int(row.rx_bytes)
            endpoint["rx_drop"] = _as_nonnegative_int(row.drop_bytes)

    hosts = {}
    for row in nic_df.itertuples(index=False):
        hosts[(int(row.timestamp_ns), int(row.node_id), int(row.nic_id),
               str(row.link_id))] = {
                   "tx": _as_nonnegative_int(row.tx_bytes),
                   "rx": _as_nonnegative_int(row.rx_bytes),
                   "rx_drop": _as_nonnegative_int(row.rx_drop_bytes),
               }

    timestamps = sorted(set(pd.to_numeric(
        switch_df["timestamp_ns"], errors="coerce").dropna().astype(int)))
    checked_directions = 0
    violations = []

    for link in link_map.itertuples(index=False):
        link_id = str(link.link_id)
        bdp_bytes = int(int(link.bandwidth_bps) * int(link.delay_ns) / 8e9)
        inflight_bound = bdp_bytes + 2 * 9216

        def get_endpoint(timestamp, side):
            node = int(getattr(link, f"{side}_node"))
            port = int(getattr(link, f"{side}_port"))
            node_type = str(getattr(link, f"{side}_type"))
            key = (timestamp, node, port, link_id)
            return hosts.get(key) if node_type == "HOST" else fabric.get(key)

        for timestamp in timestamps:
            src = get_endpoint(timestamp, "src")
            dst = get_endpoint(timestamp, "dst")
            if src is None or dst is None:
                violations.append(
                    {"timestamp_ns": timestamp, "link_id": link_id,
                     "reason": "missing_endpoint"}
                )
                continue
            for direction, sender, receiver in (
                    ("src_to_dst", src, dst), ("dst_to_src", dst, src)):
                checked_directions += 1
                if sender["tx"] is None or receiver["rx"] is None:
                    violations.append(
                        {"timestamp_ns": timestamp, "link_id": link_id,
                         "direction": direction, "reason": "missing_direction"}
                    )
                    continue
                remainder = sender["tx"] - receiver["rx"] - receiver["rx_drop"]
                if remainder < 0 or remainder > inflight_bound:
                    violations.append(
                        {"timestamp_ns": timestamp, "link_id": link_id,
                         "direction": direction, "remainder_bytes": remainder,
                         "inflight_bound_bytes": inflight_bound}
                    )

    return checked_directions, violations


def validate_run(
        run_dir, link_map_path, results,
        expected_sample_interval_ns=DEFAULT_SAMPLE_INTERVAL_NS):
    switch_path = os.path.join(run_dir, "switch_telemetry.csv")
    nic_path = os.path.join(run_dir, "nic_telemetry.csv")
    coll_path = os.path.join(run_dir, "collective_telemetry.csv")

    prefix = f"[{os.path.basename(run_dir)}] "
    required_paths = {
        "switch_telemetry.csv": switch_path,
        "nic_telemetry.csv": nic_path,
        "collective_telemetry.csv": coll_path,
        "link_map.csv": link_map_path,
    }
    missing_files = sorted(
        name for name, path in required_paths.items()
        if not path or not os.path.isfile(path)
    )
    check(
        results,
        "required_input_files",
        not missing_files,
        prefix + f"missing={missing_files}",
    )
    if missing_files:
        return {
            "run_dir": os.path.abspath(run_dir),
            "switch_rows": 0,
            "nic_rows": 0,
            "collective_rows": 0,
        }

    switch_df = pd.read_csv(switch_path)
    nic_df = pd.read_csv(nic_path)
    coll_df = pd.read_csv(coll_path)
    link_map = pd.read_csv(link_map_path)

    missing_schema = {
        "switch": _missing_columns(switch_df, SWITCH_REQUIRED_COLUMNS),
        "nic": _missing_columns(nic_df, NIC_REQUIRED_COLUMNS),
        "collective": _missing_columns(coll_df, COLLECTIVE_REQUIRED_COLUMNS),
        "link_map": _missing_columns(link_map, LINK_MAP_REQUIRED_COLUMNS),
    }
    missing_schema = {
        name: columns for name, columns in missing_schema.items() if columns
    }
    check(
        results,
        "required_input_schema",
        not missing_schema,
        prefix + f"missing_columns={missing_schema}",
    )
    if missing_schema:
        return {
            "run_dir": os.path.abspath(run_dir),
            "switch_rows": len(switch_df),
            "nic_rows": len(nic_df),
            "collective_rows": len(coll_df),
        }

    empty_inputs = [
        name for name, frame in (
            ("switch", switch_df), ("nic", nic_df),
            ("collective", coll_df), ("link_map", link_map))
        if frame.empty
    ]
    check(
        results,
        "required_inputs_nonempty",
        not empty_inputs,
        prefix + f"empty={empty_inputs}",
    )

    # 1. timestamp monotonic non-decreasing per physical endpoint/direction.
    # A SWITCH-SWITCH link has two monitored endpoints sharing one link_id,
    # so grouping by link_id alone conflates the two directional devices.
    ok = True
    detail = "ok"
    if not switch_df.empty:
        endpoint_cols = ["switch_id", "port_id", "link_id", "direction"]
        for endpoint, g in switch_df.groupby(endpoint_cols):
            ts = g["timestamp_ns"].values
            if any(ts[i] > ts[i + 1] for i in range(len(ts) - 1)):
                ok = False
                detail = f"non-monotonic timestamps for endpoint {endpoint}"
                break
    check(results, "timestamp_monotonic", ok, prefix + detail)

    switch_timestamps = sorted(set(pd.to_numeric(
        switch_df["timestamp_ns"], errors="coerce").dropna().astype(int)))
    nic_timestamps = sorted(set(pd.to_numeric(
        nic_df["timestamp_ns"], errors="coerce").dropna().astype(int)))
    check(
        results,
        "snapshot_timestamp_alignment",
        switch_timestamps == nic_timestamps and bool(switch_timestamps),
        prefix + f"switch_samples={len(switch_timestamps)}, "
        f"nic_samples={len(nic_timestamps)}, "
        f"switch_only={sorted(set(switch_timestamps) - set(nic_timestamps))[:5]}, "
        f"nic_only={sorted(set(nic_timestamps) - set(switch_timestamps))[:5]}",
    )
    sample_deltas = [
        later - earlier
        for earlier, later in zip(switch_timestamps, switch_timestamps[1:])
    ]
    cadence_ok = (
        len(switch_timestamps) >= 2
        and all(delta == expected_sample_interval_ns for delta in sample_deltas)
    )
    check(
        results,
        "sample_interval_exact",
        cadence_ok,
        prefix + f"expected_ns={expected_sample_interval_ns}, "
        f"samples={len(switch_timestamps)}, observed_deltas={sorted(set(sample_deltas))}",
    )

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

    # 3. Per-direction physical-link conservation at every snapshot, not just
    # at the final sample.
    checked_directions, conservation_violations = (
        _link_conservation_diagnostics(switch_df, nic_df, link_map)
    )
    check(
        results,
        "tx_rx_drop_conservation_every_snapshot",
        not conservation_violations,
        prefix + f"checked_directions={checked_directions}, "
        f"violations={len(conservation_violations)}, "
        f"examples={conservation_violations[:3]}",
    )

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

    # 8. No telemetry row may be detached from a physical link.
    empty_switch = int(switch_df["link_id"].isna().sum()) if not switch_df.empty else 0
    empty_nic = int((nic_df["link_id"].fillna("") == "").sum()) if not nic_df.empty else 0
    check(results, "no_empty_link_ids", empty_switch + empty_nic == 0,
          prefix + f"empty switch rows={empty_switch}, empty NIC rows={empty_nic}")

    # 9. Every physical link must have a fabric-side endpoint in switch
    # telemetry, including NVSWITCH endpoints for INTRA_NODE links.
    if link_map.empty or switch_df.empty:
        check_skip(results, "physical_link_coverage", prefix + "missing link map or switch telemetry")
    else:
        expected_links = set(link_map["link_id"])
        observed_links = set(switch_df["link_id"].dropna())
        missing = expected_links - observed_links
        check(results, "physical_link_coverage", not missing,
              prefix + f"observed {len(observed_links)}/{len(expected_links)} physical links; "
                       f"missing={sorted(missing)[:5]}")

    # 10. Each timestamp must contain the exact endpoint inventory.  Set
    # equality and duplicate checks prevent an omitted endpoint from being
    # hidden by a repeated row with the same aggregate count.
    if link_map.empty or switch_df.empty or nic_df.empty:
        check_skip(results, "endpoint_sample_coverage", prefix + "missing telemetry inputs")
        check_skip(results, "no_duplicate_endpoint_samples", prefix + "missing telemetry inputs")
    else:
        inventory = _sample_inventory_diagnostics(switch_df, nic_df, link_map)
        check(
            results,
            "no_duplicate_endpoint_samples",
            inventory["duplicate_sample_count"] == 0,
            prefix + f"bad_samples={inventory['duplicate_sample_count']}, "
            f"examples={inventory['duplicate_examples']}",
        )
        check(
            results,
            "endpoint_sample_coverage",
            inventory["coverage_mismatch_sample_count"] == 0,
            prefix + f"expected fabric={inventory['expected_fabric_endpoints']} "
            f"TX+RX and host={inventory['expected_host_endpoints']}; "
            f"bad_samples={inventory['coverage_mismatch_sample_count']}, "
            f"examples={inventory['mismatch_examples']}",
        )

    # 11. Event-updated interval peaks must dominate the sampled current
    # queue and carry a timestamp no later than the sample itself.
    required_peak_cols = {"queue_bytes", "max_queue_bytes", "max_queue_timestamp_ns"}
    if switch_df.empty or not required_peak_cols.issubset(switch_df.columns):
        check_skip(results, "queue_peak_consistent", prefix + "event-peak columns unavailable")
    else:
        tx = switch_df[switch_df["direction"] == "tx"].copy()
        current = pd.to_numeric(tx["queue_bytes"], errors="coerce")
        peak = pd.to_numeric(tx["max_queue_bytes"], errors="coerce")
        peak_ts = pd.to_numeric(tx["max_queue_timestamp_ns"], errors="coerce")
        sample_ts = pd.to_numeric(tx["timestamp_ns"], errors="coerce")
        bad = (peak < current) | (peak_ts > sample_ts) | peak.isna() | peak_ts.isna()
        check(results, "queue_peak_consistent", not bad.any(),
              prefix + f"{int(bad.sum())}/{len(tx)} inconsistent TX endpoint samples")

    # 12. Raw rates and state are now first-class live fields, rather than
    # placeholders that only become usable after offline post-processing.
    v2_cols = {"observed_throughput_bps", "utilization", "link_state", "node_type"}
    if switch_df.empty or not v2_cols.issubset(switch_df.columns):
        check_skip(results, "live_rate_and_state_fields", prefix + "v2 live fields unavailable")
    else:
        tx = switch_df[switch_df["direction"] == "tx"]
        complete = (tx["observed_throughput_bps"].notna().all()
                    and tx["utilization"].notna().all()
                    and tx["link_state"].isin(["up", "down"]).all()
                    and tx["node_type"].isin(["SWITCH", "NVSWITCH"]).all())
        check(results, "live_rate_and_state_fields", complete,
              prefix + f"checked {len(tx)} TX endpoint samples")

    # 13. All cumulative switch and NIC counters must never decrease.  The
    # former implementation checked only switch TX/RX bytes.
    counter_violations = _counter_monotonicity_diagnostics(switch_df, nic_df)
    check(
        results,
        "cumulative_counters_monotonic",
        not counter_violations,
        prefix + f"violations={len(counter_violations)}, "
        f"examples={counter_violations[:5]}",
    )

    # 14. Latched link transitions must be monotonic and temporally valid.
    flap_cols = {"flap_count", "last_link_down_ns", "last_link_up_ns",
                 "cumulative_link_down_ns"}
    if switch_df.empty or not flap_cols.issubset(switch_df.columns):
        check_skip(results, "flap_latch_consistent", prefix + "flap latch fields unavailable")
    else:
        bad = 0
        tx = switch_df[switch_df["direction"] == "tx"].copy()
        for _, group in tx.groupby(["switch_id", "port_id"]):
            group = group.sort_values("timestamp_ns")
            if (group["flap_count"].diff().fillna(0) < 0).any():
                bad += 1
            if (group["cumulative_link_down_ns"].diff().fillna(0) < 0).any():
                bad += 1
            if (group["last_link_down_ns"] > group["timestamp_ns"]).any():
                bad += 1
            if (group["last_link_up_ns"] > group["timestamp_ns"]).any():
                bad += 1
        check(results, "flap_latch_consistent", bad == 0,
              prefix + f"{bad} endpoint-series violations")

    return {
        "run_dir": os.path.abspath(run_dir),
        "switch_rows": len(switch_df),
        "nic_rows": len(nic_df),
        "collective_rows": len(coll_df),
        "sample_count": len(switch_timestamps),
        "first_sample_ns": switch_timestamps[0] if switch_timestamps else None,
        "last_sample_ns": switch_timestamps[-1] if switch_timestamps else None,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dirs", nargs="+", required=True)
    ap.add_argument("--link-map", default=None)
    ap.add_argument("--fault-events", default=None)
    ap.add_argument("--parity-json", default=None, help="monitoring_on_off_parity.json")
    ap.add_argument(
        "--expected-sample-interval-ns",
        type=int,
        default=DEFAULT_SAMPLE_INTERVAL_NS,
        help="required gap between complete telemetry snapshots",
    )
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    results = []
    runs = []
    for run_dir in args.run_dirs:
        link_map = args.link_map or os.path.join(run_dir, "link_map.csv")
        runs.append(validate_run(
            run_dir,
            link_map,
            results,
            expected_sample_interval_ns=args.expected_sample_interval_ns,
        ))

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
        with open(args.parity_json, "r", encoding="utf-8") as source:
            parity = json.load(source)
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

    status = "PASS" if n_fail == 0 else "FAIL"
    out = {
        "schema_version": "limer.telemetry-validation.v2",
        "status": status,
        "summary": {"pass": n_pass, "fail": n_fail, "skip": n_skip},
        "runs": runs,
        "checks": results,
    }
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write("# LIMER Telemetry Validation Report\n\n")
        f.write(f"**{status}: {n_pass} passed, {n_fail} failed, "
                f"{n_skip} skipped**\n\n")
        f.write("| Check | Status | Detail |\n|---|---|---|\n")
        for r in results:
            f.write(f"| {r['check']} | {r['status']} | {r['detail']} |\n")

    print(f"Wrote {args.out_json}, {args.out_md}")
    print(f"{status}: {n_pass} passed, {n_fail} failed, {n_skip} skipped")
    for r in results:
        print(f"  [{r['status']}] {r['check']}: {r['detail']}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
