#!/usr/bin/env python3
"""Independent, fail-closed trace audit for endpoint/sparse-signal policies.

This checks observable arithmetic and causal provenance; it does not establish
physical capacity, calibrated fault probabilities, or numeric AllReduce results.
CSV floats have the simulator's default six-significant-digit precision.
"""
import bisect
import csv
import json
import math
import struct
from collections import Counter, defaultdict
from pathlib import Path


def _rows(path):
    with Path(path).open(newline="") as stream:
        return list(csv.DictReader(stream))


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _close(actual, expected, message):
    _check(math.isfinite(float(actual)) and math.isclose(
        float(actual), float(expected), rel_tol=2e-5, abs_tol=1e-8), message)


def _key(row):
    return tuple(int(row[k]) for k in ("src", "dst", "sport"))


def _f32(value):
    return struct.unpack("f", struct.pack("f", value))[0]


def _audit_wait_raw(row, values, sequences):
    """Check last feedback by causal sequence, including current ACK update."""
    rail, now = int(row["rail"]), int(row["timestamp_ns"])
    # ACK handler updates its raw estimate before logging the two wait rows,
    # then logs feedback under the same lock. TIMER sees only prior feedback.
    limit = int(row["audit_seq"])+(2-rail if row["trigger"] == "ACK" else -1)
    through = bisect.bisect_right(sequences, limit)
    expected = values[through-1][1] if through else 0.
    _check(not through or values[through-1][2] <= now,
           "wait observation uses future timestamp feedback")
    _close(row["raw_bps"], expected, "wait observation uses future/wrong-sequence raw feedback")


def _signal_audit(directory, manifest, finish):
    env = manifest.get("env", {})
    enabled = int(env.get("LIMER_SPLIT_SIGNAL_ENABLE", 1)) != 0
    actuate = int(env.get("LIMER_SPLIT_SIGNAL_ACTUATE", 1)) != 0
    period = int(env.get("LIMER_SPLIT_SIGNAL_SAMPLE_NS", 10000))
    delay = int(env.get("LIMER_SPLIT_SIGNAL_DELAY_NS", 10000))
    heartbeat = int(env.get("LIMER_SPLIT_SIGNAL_HEARTBEAT_NS", 40000))
    ttl = int(env.get("LIMER_SPLIT_SIGNAL_TTL_NS", 50000))
    _check(period > 0 and heartbeat > 0 and ttl > 0 and delay >= 0,
           "invalid signal timing configuration")
    records = _rows(directory / "switch_signals.csv")
    samples = _rows(directory / "switch_signal_samples.csv")
    _check(enabled or not (records or samples), "disabled signal has active observations")
    ports = {}
    for row in _rows(directory / "link_map.csv"):
        if row["link_class"] != "ACCESS":
            continue
        host, switch = ("src", "dst") if row["src_type"] == "HOST" else ("dst", "src")
        _check(row[host + "_type"] == "HOST" and row[switch + "_type"] == "SWITCH",
               "invalid physical ACCESS endpoint types")
        rail = int(row[host + "_port"]) - 2
        _check(rail in (0, 1), "ACCESS endpoint is not NIC 2/3")
        ports[int(row[switch + "_node"]), int(row[switch + "_port"])] = (
            int(row[host + "_node"]), rail)
    _check(len(ports) == 32, "signal audit requires all 32 physical ACCESS ports")
    sample_lookup, states = {}, {}
    changes = 0
    for row in sorted(samples, key=lambda r: (int(r["node"]), int(r["port"]), int(r["timestamp_ns"]))):
        port = int(row["node"]), int(row["port"])
        now = int(row["timestamp_ns"])
        _check(port in ports and ports[port] == (int(row["dst"]), int(row["rail"])),
               "sample destination does not match physical ACCESS port")
        _check(now > 0 and now % period == 0, "switch sample is not timer aligned")
        key = now, *port
        _check(key not in sample_lookup, "duplicate switch sample")
        sample_lookup[key] = row
        tx, queue = int(row["tx_bytes"]), int(row["queue_bytes"])
        _check(tx >= 0 and queue >= 0, "negative switch counter")
        old = states.get(port)
        wire, demand, low = 0., False, 0
        reference, factor = (old[4], old[5]) if old else (0., 1.)
        if old:
            _check(now - old[0] == period and tx >= old[1], "switch sampling gap/counter regression")
            wire = (tx - old[1]) * 8e9 / (now - old[0])
            demand = old[2] > 0 and queue > 0
            if not int(row.get("up", 1)):
                factor = _f32(.1)
            elif demand:
                reference = max(reference, wire)
                ratio = wire / reference if reference else 1.
                low = min(old[3] + 1, 65535) if ratio < .7 else 0
                factor = _f32(max(.1, min(1., ratio))) if low >= 2 else 1.
            else:
                factor = 1.
            changes += not math.isclose(factor, old[5], rel_tol=1e-7, abs_tol=1e-8)
        else:
            _check(now == period, "switch sensor missed first sample")
        if not int(row.get("up", 1)):
            factor, low = _f32(.1), 0
        _close(row["wire_bps"], wire, "wire rate does not follow counter difference")
        _check(int(row["demand"]) == demand, "switch demand does not match endpoint queues")
        _close(row["reference_bps"], reference, "switch historical reference mismatch")
        _close(row["factor"], factor, "switch two-window factor mismatch")
        last_sent = old[6] if old else 0
        last_factor = old[7] if old else 1.
        emitted = not old or abs(_f32(factor - last_factor)) >= _f32(.1) or now-last_sent >= heartbeat
        _check(int(row["emitted"]) == emitted, "switch emission threshold/heartbeat mismatch")
        states[port] = (now, tx, min(queue, 2**32-1), low, reference, factor,
                        now if emitted else last_sent, factor if emitted else last_factor)
    if enabled and finish > period:
        _check(set(states) == set(ports), "switch samples do not cover every ACCESS port")
    emits, delivered, subscribers = {}, set(), defaultdict(set)
    common = ("tx_bytes", "queue_bytes", "wire_bps", "reference_bps", "demand", "factor")
    for row in records:
        now, sampled = int(row["timestamp_ns"]), int(row["sampled_ns"])
        port = int(row["node"]), int(row["port"])
        key = sampled, *port
        _check(port in ports and ports[port] == (int(row["dst"]), int(row["rail"])),
               "signal destination does not match physical ACCESS port")
        _check(key in sample_lookup and int(sample_lookup[key]["emitted"]),
               "signal has no emitted physical sample")
        for name in common:
            _close(row[name], sample_lookup[key][name], "signal payload differs from switch sample: " + name)
        _check(int(row["payload_bytes"]) == 32, "logical signal payload must be 32 bytes")
        if row["event"] == "EMIT":
            _check(now == sampled and int(row["observer"]) == 16 and key not in emits,
                   "invalid/duplicate signal emission")
            emits[key] = row
            _check(int(sample_lookup[key]["audit_seq"]) < int(row["audit_seq"]),
                   "signal emitted before its physical sample")
        else:
            _check(row["event"] == "DELIVER", "unknown signal event")
            observer = int(row["observer"])
            _check(0 <= observer < 16 and observer // 4 != int(row["dst"]) // 4,
                   "signal delivered outside topology subscription")
            _check(now == sampled + delay, "signal violates configured delivery delay")
            identity = key, observer
            _check(identity not in delivered, "duplicate signal delivery")
            delivered.add(identity)
            subscribers[key].add(observer)
    expected_emits = {key for key, row in sample_lookup.items() if int(row["emitted"])}
    _check(set(emits) == expected_emits, "switch emitted samples and EMIT events differ")
    _check(all(key in emits for key, _ in delivered), "DELIVER without matching EMIT")
    for row in records:
        if row["event"] == "DELIVER":
            key = int(row["sampled_ns"]), int(row["node"]), int(row["port"])
            _check(int(emits[key]["audit_seq"]) < int(row["audit_seq"]), "delivery precedes emission")
    for key, row in emits.items():
        if key[0] + delay < finish:
            expected = {src for src in range(16) if src // 4 != int(row["dst"]) // 4}
            _check(subscribers[key] == expected, "missing pre-finish topology subscriber delivery")
    return records, dict(enabled=enabled, actuate=actuate, sample_ns=period,
        delay_ns=delay, ttl_ns=ttl, heartbeat_ns=heartbeat,
        sample_count=len(samples), port_emits=len(emits), deliveries=len(delivered),
        emitted_logical_payload_bytes=32*len(emits),
        delivered_logical_payload_bytes=32*len(delivered),
        subscribed_logical_payload_bytes=32*12*len(emits),
        deliveries_before_workload_finish=sum(r["event"] == "DELIVER" and
            int(r["timestamp_ns"]) < finish for r in records),
        state_factor_changes=changes, monitored_access_ports=len(states),
        switch_state_budget_bytes_per_port=48,
        control_transport="fixed-latency reliable logical messages; no data-plane contention")


def _audit_adaptive(directory, feedback, events):
    """Audit B10/B11/B12 traces and return JSON-serializable evidence metrics.

    New adaptive traces must carry a shared audit_seq, allocated under the
    runtime lock. This resolves same-nanosecond ordering across different CSVs.
    """
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    policy, env = manifest["policy"], manifest.get("env", {})
    _check(policy in ("B10", "B11", "B12"), "not an adaptive policy")
    _check(not env.get("LIMER_SPLIT_CAPACITY_CSV"), "adaptive policy supplied Oracle capacity")
    period = int(env.get("LIMER_SPLIT_WAIT_SAMPLE_NS", 10000))
    _check(period > 0, "invalid host waiting-time timer")
    decisions = _rows(directory / "adaptive_decisions.csv")
    waits = _rows(directory / "adaptive_wait.csv")
    chunks = {_key(row): row for row in feedback}
    _check(len(chunks) == len(feedback), "duplicate feedback identity")
    assignment = { _key(row): row for row in events if row["event"] == "ASSIGN" }
    decision_map = {_key(row): row for row in decisions}
    _check(len(decision_map) == len(decisions) == len(assignment) and
           set(decision_map) == set(assignment) == set(chunks),
           "adaptive decisions must cover every assigned/completed chunk exactly once")
    done = [r for r in _rows(directory / "run_lifecycle.csv") if r["status"] == "WORKLOAD_COMPLETE"]
    _check(len(done) == 1, "adaptive audit requires one workload completion")
    finish = int(done[0]["actual_ns"])
    raw, reserve, active, launched = defaultdict(float), defaultdict(int), defaultdict(int), defaultdict(int)
    loads, credits, credit_error = {}, defaultdict(lambda: [0., 0.]), defaultdict(float)
    for event in events:
        key = _key(event)
        src, dst, _ = key
        rail, size, now = int(event["rail"]), int(event["bytes"]), int(event["timestamp_ns"])
        _check(rail in (0, 1) and size > 0, "invalid adaptive assignment")
        if event["event"] == "ACK_COMPLETE":
            item = chunks[key]
            _check(int(item["rail"]) == rail and int(item["bytes"]) == size and
                int(item["completion_ns"]) == now, "completion feedback mismatch")
            elapsed = now-int(item["first_tx_ns"])
            _check(elapsed > 0 and reserve[src, rail] >= size and active[src, rail] > 0,
                   "invalid completion/reservation underflow")
            raw[src, rail] = size*8e9/elapsed
            reserve[src, rail] -= size
            active[src, rail] -= 1
            continue
        _check(event["event"] == "ASSIGN", "unknown split event")
        row = decision_map[key]
        _check(int(row["timestamp_ns"]) == now and int(row["rail"]) == rail and
               int(row["bytes"]) == size, "decision does not match ASSIGN")
        rates = []
        for r, suffix in enumerate(("A", "B")):
            _close(row["raw_"+suffix], raw[src, r], "decision used future/wrong raw feedback")
            _check(int(row["reserved_"+suffix]) == reserve[src, r], "decision reservation mismatch")
            wait, signal = float(row["wait_"+suffix]), float(row["signal_"+suffix])
            _check(.1-1e-7 <= wait <= 1 and .1-1e-7 <= signal <= 1, "invalid adaptive factor")
            expected = raw[src, r]*min(wait, signal)
            _close(row["rate_"+suffix], expected, "used rate is not raw times minimum factor")
            rates.append(expected)
        if not launched[src, 0]:
            _check(rail == 0, "first probe must use rail A")
        elif not launched[src, 1]:
            _check(rail == 1, "second probe must use rail B")
        elif policy == "B10":
            total = sum(rates)
            _check(total > 0, "no rate available after probes")
            for r in (0, 1):
                credits[src, dst][r] += size*rates[r]/total
            credit_error[src, dst] += size*4e-5
            legal = 1 if rates[0] <= 0 else 0 if rates[1] <= 0 else None
            if legal is not None:
                _check(rail == legal, "credit scheduler selected unsampled rail")
            else:
                _check(credits[src, dst][rail]+2*credit_error[src, dst] >= credits[src, dst][1-rail],
                       "B10 did not select greatest byte credit")
            credits[src, dst][rail] -= size
        else:
            cost = [(reserve[src, r]+size)*8/rates[r] if rates[r] > 0 else math.inf for r in (0, 1)]
            _check(math.isfinite(cost[rail]) and cost[rail] <= cost[1-rail]*(1+5e-5),
                   "reservation scheduler did not select minimum completion cost")
        launched[src, rail] += 1
        reserve[src, rail] += size
        active[src, rail] += 1
        loads[key] = active[src, rail]
    _check(all(value == 0 for value in reserve.values()), "unfinished adaptive reservation")
    history, conditional = defaultdict(list), defaultdict(list)
    for row in feedback:
        history[int(row["src"]), int(row["rail"])].append((int(row["audit_seq"]),
            int(row["bytes"])*8e9/(int(row["completion_ns"])-int(row["first_tx_ns"])),
            int(row["completion_ns"])))
        conditional[int(row["src"]), int(row["rail"]), (int(row["bytes"])-1).bit_length(),
            (loads[_key(row)]-1).bit_length()].append(int(row["completion_ns"]))
    for values in history.values():
        values.sort()
    history_sequences = {key: [sequence for sequence, _, _ in values] for key, values in history.items()}
    for values in conditional.values():
        values.sort()
    ack_pairs, timer_pairs = Counter(), set()
    _check(len(waits) % 2 == 0, "wait observations must contain both rails")
    for a, b in zip(waits[::2], waits[1::2]):
        _check(all(a[k] == b[k] for k in ("timestamp_ns", "trigger", "src")) and
            int(a["rail"]) == 0 and int(b["rail"]) == 1, "unpaired wait observations")
        pair = int(a["src"]), int(a["timestamp_ns"])
        if a["trigger"] == "ACK":
            ack_pairs[pair] += 1
        else:
            _check(a["trigger"] == "TIMER" and pair[1] > 0 and pair[1] % period == 0,
                   "wait TIMER is not aligned to configured cadence")
            _check(pair not in timer_pairs, "duplicate source wait TIMER")
            timer_pairs.add(pair)
    _check(ack_pairs == Counter((int(r["src"]), int(r["completion_ns"])) for r in feedback),
           "ACK waiting-time observations do not cover every completion")
    required_timers = set()
    for key, row in chunks.items():
        start = int(assignment[key]["timestamp_ns"])
        for now in range((start//period+1)*period, int(row["completion_ns"]), period):
            required_timers.add((key[0], now))
    _check(required_timers <= timer_pairs, "missing timer while source has live chunks")
    penalized = 0
    for row in waits:
        now, src, rail = int(row["timestamp_ns"]), int(row["src"]), int(row["rail"])
        values = history[src, rail]
        sequences = history_sequences.get((src, rail), [])
        _audit_wait_raw(row, values, sequences)
        factor = float(row["factor"])
        _check(.1-1e-7 <= factor <= 1, "wait factor outside [0.1,1]")
        if not int(row["witness_bytes"]):
            _close(factor, 1., "penalty without unfinished-chunk witness")
            _check(all(float(row[name]) == 0 for name in ("witness_dst", "witness_sport",
                "first_tx_ns", "launch_active", "age_ns", "threshold_ns", "samples")),
                "neutral wait row has nonempty witness evidence")
            continue
        witness_key = src, int(row["witness_dst"]), int(row["witness_sport"])
        _check(witness_key in chunks, "unknown waiting-time witness")
        item = chunks[witness_key]
        first = int(item["first_tx_ns"])
        _check(int(item["rail"]) == rail and int(item["bytes"]) == int(row["witness_bytes"]) and
            int(row["first_tx_ns"]) == first and int(assignment[witness_key]["timestamp_ns"]) <= now and
            first < now <= int(item["completion_ns"]), "waiting witness is not truly unfinished")
        _check(int(row["age_ns"]) == now-first and int(row["launch_active"]) == loads[witness_key],
               "waiting witness age/launch load mismatch")
        count, threshold = int(row["samples"]), float(row["threshold_ns"])
        _check(count >= 8 and math.isfinite(threshold) and threshold > 0, "cold/invalid waiting-time threshold")
        size_bucket = (int(item["bytes"])-1).bit_length()
        load_bucket = (loads[witness_key]-1).bit_length()
        available = bisect.bisect_right(conditional[src, rail, size_bucket, load_bucket], now)
        _check(count <= available, "wait model claims future conditional samples")
        _close(factor, max(.1, min(1., threshold/(now-first))), "wait factor does not follow threshold/age")
        _check(factor < 1, "neutral factor should not nominate a witness")
        penalized += 1
    signal_rows, signal_stats = [], None
    if policy == "B12":
        signal_rows, signal_stats = _signal_audit(directory, manifest, finish)
    combined = [(row, "WAIT") for row in waits] + [(row, "DECIDE") for row in decisions] + [
        (row, "FEEDBACK") for row in feedback] + [
        (row, row["event"]) for row in signal_rows]
    sequences = [int(row["audit_seq"]) for row, _ in combined]
    _check(all(s > 0 for s in sequences) and len(set(sequences)) == len(sequences),
           "missing/duplicate cross-trace causal audit sequence")
    wait_cache, signal_cache, raw_cache = {}, {}, defaultdict(float)
    changed_signal_decisions, changed_wait_decisions = 0, 0
    for row, kind in sorted(combined, key=lambda value: int(value[0]["audit_seq"])):
        now = int(row["timestamp_ns"])
        if kind == "WAIT":
            wait_cache[int(row["src"]), int(row["rail"])] = (now, float(row["factor"]))
        elif kind == "DELIVER" and signal_stats["actuate"]:
            key = int(row["observer"]), int(row["dst"]), int(row["rail"])
            sampled = int(row["sampled_ns"])
            if key not in signal_cache or sampled > signal_cache[key][0]:
                signal_cache[key] = sampled, now, float(row["factor"])
        elif kind == "FEEDBACK":
            src, dst, rail = int(row["src"]), int(row["dst"]), int(row["rail"])
            raw_cache[src, rail] = int(row["bytes"])*8e9/(now-int(row["first_tx_ns"]))
            observed, factor = wait_cache.get((src, rail), (0, 1.))
            _check(observed <= now, "feedback used future wait state")
            signal = 1.
            cached = signal_cache.get((src, dst, rail))
            if cached and cached[1] <= now and 0 <= now-cached[0] <= signal_stats["ttl_ns"]:
                signal = cached[2]
            _close(row["used_bps"], raw_cache[src, rail]*min(factor, signal),
                   "completion feedback used rate violates causal wait/signal fusion")
        elif kind == "DECIDE":
            src, dst = int(row["src"]), int(row["dst"])
            for rail, suffix in enumerate(("A", "B")):
                _close(row["raw_"+suffix], raw_cache[src, rail], "decision raw rate precedes completion audit event")
                observed, factor = wait_cache.get((src, rail), (0, 1.))
                _check(observed <= now, "decision used future source wait observation")
                _close(row["wait_"+suffix], factor, "decision did not use causally latest wait state")
                signal = 1.
                cached = signal_cache.get((src, dst, rail))
                if cached and cached[1] <= now and 0 <= now-cached[0] <= signal_stats["ttl_ns"]:
                    signal = cached[2]
                _close(row["signal_"+suffix], signal, "decision used wrong observer/stale/unarrived signal")
            changed_signal_decisions += min(float(row["signal_A"]), float(row["signal_B"])) < 1
            changed_wait_decisions += min(float(row["wait_A"]), float(row["wait_B"])) < 1
    return dict({"pass": True}, policy=policy, decision_count=len(decisions),
        wait_observations=len(waits), wait_penalty_rows=penalized,
        wait_timer_pairs=len(timer_pairs), wait_affected_decisions=changed_wait_decisions,
        signal_affected_decisions=changed_signal_decisions, signal=signal_stats,
        conditional_threshold_audit="sample availability, witness and penalty arithmetic; model statistics not replayed",
        source_inputs="completed/unfinished source chunks; no Oracle capacity supplied")


def audit_adaptive(directory, feedback, events):
    """Public API: malformed or inconsistent evidence raises ValueError."""
    try:
        return _audit_adaptive(directory, feedback, events)
    except (KeyError, TypeError, OverflowError, OSError, ZeroDivisionError) as error:
        raise ValueError("invalid/incomplete adaptive audit evidence: " + str(error)) from error
