#!/usr/bin/env python3
"""Independent replay of endpoint-only censored-confidence split decisions.

The audit checks data provenance and policy arithmetic, not statistical coverage
under interacting, nonstationary network traffic or tensor numeric correctness.
"""
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


def _rows(path):
    with Path(path).open(newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError("missing/duplicate CSV column names")
        result = list(reader)
        if any(None in row or any(value is None for value in row.values()) for row in result):
            raise ValueError("ragged censored trace CSV")
        return result


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _close(actual, expected, message):
    actual, expected = float(actual), float(expected)
    _check(math.isfinite(actual) and math.isfinite(expected) and
           math.isclose(actual, expected, rel_tol=2e-5, abs_tol=1e-8), message)


def _key(row):
    return tuple(int(row[name]) for name in ("src", "dst", "sport"))


def _unique(rows, label):
    result = {_key(row): row for row in rows}
    _check(len(result) == len(rows), "duplicate " + label + " identity")
    return result


def _source_time_order(combined):
    """Different logical processors may interleave nonmonotone local clocks.

    audit_seq orders the shared trace lock, not a global simulation clock.
    These policies share no observations across sources, so require monotone
    time within each source while retaining global sequence uniqueness.
    """
    previous = {}
    for row, _ in combined:
        src, now = int(row["src"]), int(row["timestamp_ns"])
        _check(src not in previous or previous[src] <= now,
               "source-local causal sequence travels backward in time")
        previous[src] = now


def _trace_inputs(directory, feedback, events):
    """Qualify identities and shared causal sequence before model replay."""
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    _check(manifest["policy"] in ("B13", "B14"), "not a censored-confidence policy")
    env = manifest.get("env", {})
    _check(not env.get("LIMER_SPLIT_CAPACITY_CSV"), "censored policy supplied Oracle capacity")
    decisions = _rows(directory / "censored_decisions.csv")
    _check(bool(decisions) and bool(feedback), "empty censored-confidence evidence")
    chunks = _unique(feedback, "feedback")
    assignments = _unique([row for row in events if row["event"] == "ASSIGN"], "assignment")
    completions = _unique([row for row in events if row["event"] == "ACK_COMPLETE"], "completion")
    decision_map = _unique(decisions, "decision")
    _check(all(row["event"] in ("ASSIGN", "ACK_COMPLETE") for row in events),
           "unknown split event")
    _check(set(chunks) == set(assignments) == set(completions) == set(decision_map),
           "decisions/assignments/completions/feedback identities differ")
    for key, row in chunks.items():
        begin, end, choice = assignments[key], completions[key], decision_map[key]
        size, rail = int(row["bytes"]), int(row["rail"])
        _check(size > 0 and rail in (0, 1), "invalid chunk size/rail")
        for other in (begin, end, choice):
            _check(int(other["bytes"]) == size and int(other["rail"]) == rail,
                   "chunk size/rail differs between traces")
        _check(int(choice["timestamp_ns"]) == int(begin["timestamp_ns"]),
               "decision time differs from assignment")
        first, done = int(row["first_tx_ns"]), int(row["completion_ns"])
        _check(int(begin["timestamp_ns"]) <= first < done and
               int(end["timestamp_ns"]) == int(row["timestamp_ns"]) == done and
               int(row["elapsed_ns"]) == done-first, "invalid true first-TX/ACK timing")
        _close(row["measured_bps"], size*8e9/(done-first), "invalid ACK completion rate")
        _check(row["policy"] == manifest["policy"], "feedback policy mismatch")
    combined = [(row, "DECIDE") for row in decisions] + [(row, "ACK") for row in feedback]
    sequences = [int(row["audit_seq"]) for row, _ in combined]
    _check(all(value > 0 for value in sequences) and len(set(sequences)) == len(sequences),
           "missing/duplicate causal audit sequence")
    combined.sort(key=lambda item: int(item[0]["audit_seq"]))
    _source_time_order(combined)
    return manifest, decisions, chunks, assignments, combined


def _interval(records, now, include_pending=True, horizon=250000):
    """Replay finite-sample identified interval; no future duration for pending."""
    completed = pending = 0
    low = high = 0.
    last_completed = 0
    for _, item in sorted(records.items()):
        first = item["first"]
        _check(0 <= first <= now and item["load"] > 0, "future/invalid model record")
        if item["done"]:
            if now-first > horizon:
                continue
            last_completed = max(last_completed, item["done"])
        elif not include_pending:
            continue
        load = 1 << (item["load"]-1).bit_length()
        scale = 100e9/load
        elapsed = item["done"]-first if item["done"] else now-first
        _check(elapsed >= 0 and (not item["done"] or elapsed > 0),
               "nonpositive model completion duration")
        upper = min(1., item["bytes"]*8e9/(elapsed*scale)) if elapsed else 1.
        high += upper
        if item["done"]:
            completed += 1
            low += upper
        else:
            pending += 1
    count = completed+pending
    radius = math.sqrt(math.log(2/.05)/(2*count)) if count else 1.
    lo, hi = (low/count, high/count) if count else (0., 1.)
    return dict(identified_lower=lo, identified_upper=hi,
                lower=max(0., lo-radius), upper=min(1., hi+radius),
                radius=radius, count=count, completed=completed, pending=pending,
                last_completed_ns=last_completed, ready=int(count >= 8))


class _Model:
    """Bounded membership replay; repeated pending observations are not samples."""
    def __init__(self, window=64):
        self.window = window
        self.records = defaultdict(dict)
        self.recent = defaultdict(set)

    def upsert(self, src, rail, identity, size, load, first, now, completed):
        key = src, rail
        records, recent = self.records[key], self.recent[key]
        _check(now >= first and (not completed or now > first), "noncausal model observation")
        existing = records.get(identity)
        if existing is not None:
            _check((existing["bytes"], existing["load"], existing["first"]) == (size, load, first),
                   "model immutable metadata changed")
            if existing["done"]:
                _check(not completed or existing["done"] == now, "model completion changed")
                return
            if completed:
                existing["done"] = now
                if identity not in recent:
                    del records[identity]
            return
        outside = len(recent) == self.window and identity < min(recent)
        if completed and outside:
            return
        _check(completed or sum(not item["done"] for item in records.values()) < 8,
               "model pending limit exceeded")
        records[identity] = dict(bytes=size, load=load, first=first, done=now if completed else 0)
        recent.add(identity)
        if len(recent) > self.window:
            oldest = min(recent)
            recent.remove(oldest)
            if records.get(oldest, {}).get("done"):
                del records[oldest]

    def evaluate(self, src, rail, now, include_pending=True):
        return _interval(self.records[src, rail], now, include_pending)


def _factor(evidence, rail, actuate):
    own, other = evidence[rail], evidence[1-rail]
    if not actuate or not own["ready"] or not other["ready"] or own["upper"] >= other["lower"]:
        return 1.
    return max(.1, own["upper"]/other["lower"])


def _qualify_fault_scope(directory, manifest):
    """Fault truth is used ONLY to bound evaluator applicability, never decisions.

    The trace does not record local carrier-up flags. Consequently its exact
    credit/probe replay is qualified only for this study's carrier-up service
    degradation, not hard disconnect, link flap, or arbitrary fault mechanisms.
    """
    schedule = manifest.get("env", {}).get("LIMER_FAULT_SCHEDULE")
    if schedule:
        path = Path(schedule)
        if not path.is_absolute():
            path = directory/path
        expected_hash = manifest.get("fault_sha256")
        _check(bool(expected_hash) and hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash,
               "censored fault schedule is not bound by manifest hash")
        for row in _rows(path):
            _check(row["fault_type"] == "service_degradation",
                   "unsupported fault type for carrier-up censored replay: " + row["fault_type"])
    else:
        _check(not manifest.get("fault_sha256"), "manifest fault hash lacks bound schedule")
    trace = directory/"fault_application_telemetry.csv"
    if trace.exists():
        for row in _rows(trace):
            _check(row["fault_type"] == "service_degradation",
                   "unsupported applied fault type for carrier-up censored replay: " + row["fault_type"])
            supported = {("apply", "carrier_up_service_rate", "APPLIED"),
                         ("revert", "restore_baseline", "REVERTED")}
            _check((row["transition"], row["mechanism"], row["status"]) in supported,
                   "unsupported applied mechanism/transition/status for carrier-up censored replay")


def _audit_censored(directory, feedback, events):
    directory = Path(directory)
    manifest, decisions, chunks, assignments, combined = _trace_inputs(directory, feedback, events)
    _qualify_fault_scope(directory, manifest)
    policy, env = manifest["policy"], manifest.get("env", {})
    include_pending = int(env.get("LIMER_SPLIT_CENSORED_INCLUDE_PENDING", 1)) != 0
    actuate = int(env.get("LIMER_SPLIT_CENSORED_ACTUATE", 1)) != 0
    period = int(env.get("LIMER_SPLIT_WAIT_SAMPLE_NS", 10000))
    max_active = int(env.get("LIMER_SPLIT_MAX_ACTIVE", 8))
    _check(period > 0 and 1 <= max_active <= 8, "invalid censored timer/active configuration")
    intervals = _rows(directory / "censored_intervals.csv")
    observations = _rows(directory / "censored_observations.csv")
    queues = _rows(directory / "censored_queue.csv")
    for filename in ("switch_signals.csv", "switch_signal_samples.csv", "adaptive_wait.csv"):
        path = directory / filename
        _check(not path.exists() or not _rows(path), "endpoint-only policy has switch/other-model observations")
    _check(len(intervals) % 2 == 0, "unpaired confidence intervals")
    pairs, timers = Counter(), set()
    for a, b in zip(intervals[::2], intervals[1::2]):
        _check(all(a[field] == b[field] for field in ("timestamp_ns", "trigger", "src")) and
               int(a["rail"]) == 0 and int(b["rail"]) == 1 and
               int(b["audit_seq"]) == int(a["audit_seq"])+1, "unpaired confidence intervals")
        trigger, src, now = a["trigger"], int(a["src"]), int(a["timestamp_ns"])
        _check(trigger in ("SELECT", "ACK", "TIMER"), "unknown interval trigger")
        pairs[trigger, src, now] += 1
        if trigger == "TIMER":
            _check(now > 0 and now % period == 0 and (src, now) not in timers,
                   "misaligned/duplicate censored TIMER")
            timers.add((src, now))
    _check(Counter({(src, now): count for (trigger, src, now), count in pairs.items() if trigger == "ACK"}) ==
           Counter((int(row["src"]), int(row["timestamp_ns"])) for row in feedback),
           "ACK interval pairs do not cover every completion")
    _check(Counter({(src, now): count for (trigger, src, now), count in pairs.items() if trigger == "SELECT"}) ==
           Counter((int(row["src"]), int(row["timestamp_ns"])) for row in decisions),
           "SELECT interval pairs do not cover every decision")
    required_timers = set()
    for key, row in chunks.items():
        start = int(assignments[key]["timestamp_ns"])
        required_timers.update((key[0], now) for now in range(
            (start//period+1)*period, int(row["completion_ns"]), period))
    _check(required_timers <= timers, "missing timer while source has assigned chunks")
    combined += [(row, "INTERVAL") for row in intervals]
    combined += [(row, "OBSERVE") for row in observations]
    combined += [(row, "QUEUE") for row in queues]
    combined.sort(key=lambda item: int(item[0]["audit_seq"]))
    sequences = [int(row["audit_seq"]) for row, _ in combined]
    _check(all(value > 0 for value in sequences) and len(set(sequences)) == len(sequences),
           "missing/duplicate cross-trace causal sequence")
    _source_time_order(combined)
    by_sequence = {int(row["audit_seq"]): (row, kind) for row, kind in combined}
    model = _Model()
    raw, reserved, rail_live = defaultdict(float), defaultdict(int), defaultdict(int)
    total, probe_bytes, probe_live, last_probe = (defaultdict(int) for _ in range(4))
    last_ack, sample_counts, launched = (defaultdict(int) for _ in range(3))
    q_pending, q_active, q_popped, q_finished = (defaultdict(int) for _ in range(4))
    sequence_ids, live, model_seen, completed_observed = defaultdict(int), {}, set(), set()
    prepared_ack, ack_finished = set(), set()
    credits = defaultdict(lambda: [0., 0.])
    cached = {}
    penalties = probes = old_pending_snapshots = containment_checks = 0
    true_records = {}
    max_pending = 0
    for row, kind in combined:
        src, now, seq = int(row["src"]), int(row["timestamp_ns"]), int(row["audit_seq"])
        _check(0 <= src < 16, "invalid source rank")
        if kind == "QUEUE":
            event = row["event"]
            if event == "ENQUEUE":
                q_pending[src] += 1
            elif event == "POP":
                expected_limit = max_active if sample_counts[src, 0]+sample_counts[src, 1] else 2
                _check(int(row["limit"]) == expected_limit and q_pending[src] > 0 and
                       q_active[src] < expected_limit, "invalid local dispatch POP/limit")
                q_pending[src] -= 1
                q_active[src] += 1
                q_popped[src] += 1
            else:
                _check(event == "COMPLETE" and q_finished[src] > 0 and q_active[src] > 0,
                       "queue completion without corresponding ACK")
                q_finished[src] -= 1
                q_active[src] -= 1
            _check(int(row["queued"]) == q_pending[src] and int(row["active"]) == q_active[src],
                   "local queue trace state mismatch")
            _check(event == "POP" or int(row["limit"]) == 0, "unexpected queue limit")
            continue
        if kind == "OBSERVE":
            key = _key(row)
            _check(key in live, "model observes unassigned/already ACKed chunk")
            choice, item = live[key], chunks[key]
            rail = int(row["rail"])
            _check(all(int(row[field]) == int(choice[target]) for field, target in (
                ("id", "model_id"), ("rail", "rail"), ("bytes", "bytes"), ("launch_active", "launch_active"))) and
                int(row["first_tx_ns"]) == int(item["first_tx_ns"]), "model observation metadata mismatch")
            done = int(row["completed"])
            _check(done in (0, 1) and int(row["first_tx_ns"]) <= now, "future/invalid model admission")
            if done:
                _check(key not in completed_observed and now == int(item["completion_ns"]),
                       "duplicate/wrong-time exact completion replacement")
                completed_observed.add(key)
                prepared_ack.add(key)
                raw[src, rail] = int(row["bytes"])*8e9/(now-int(row["first_tx_ns"]))
                last_ack[src, rail] = now
            else:
                _check(key not in model_seen and now <= int(item["completion_ns"]),
                       "duplicate pending admission or admission after ACK")
            model_seen.add(key)
            model.upsert(src, rail, int(row["id"]), int(row["bytes"]), int(row["launch_active"]),
                         int(row["first_tx_ns"]), now, bool(done))
            continue
        if kind == "INTERVAL":
            rail = int(row["rail"])
            _check(rail in (0, 1), "invalid interval rail")
            evidence = [model.evaluate(src, r, now, include_pending) for r in (0, 1)]
            expected = evidence[rail]
            eventual = []
            for identity, record in model.records[src, rail].items():
                if (record["done"] and now-record["first"] > 250000) or (not record["done"] and not include_pending):
                    continue
                actual = true_records[src, rail, identity]
                elapsed = int(actual["completion_ns"])-record["first"]
                scale = 100e9/(1 << (record["load"]-1).bit_length())
                eventual.append(min(1., record["bytes"]*8e9/(elapsed*scale)))
            if eventual:
                truth = sum(eventual)/len(eventual)
                _check(expected["identified_lower"]-1e-12 <= truth <= expected["identified_upper"]+1e-12,
                       "partial-identification interval excludes eventual selected-cohort utility")
                containment_checks += 1
            for field in ("identified_lower", "identified_upper", "lower", "upper", "radius"):
                _close(row[field], expected[field], "confidence interval replay mismatch: " + field)
            for field in ("count", "completed", "pending", "last_completed_ns", "ready"):
                _check(int(row[field]) == expected[field], "confidence membership replay mismatch: " + field)
            _close(row["raw_bps"], raw[src, rail], "interval uses noncausal raw rate")
            factor = _factor(evidence, rail, actuate)
            _close(row["factor"], factor, "confidence dominance factor mismatch")
            cached[src, rail] = dict(expected, factor=factor, time=now, seq=seq, trigger=row["trigger"])
            max_pending = max(max_pending, expected["pending"])
            old_pending_snapshots += any(identity not in model.recent[src, rail] and not rec["done"]
                                        for identity, rec in model.records[src, rail].items())
            # Strict inequality avoids inventing knowledge of a first-TX event
            # scheduled later at the same nanosecond as this snapshot.
            for key in live:
                item = chunks[key]
                if key[0] == src and int(item["first_tx_ns"]) < now < int(item["completion_ns"]):
                    _check(key in model_seen, "snapshot omitted causally available pending chunk")
            if rail == 1 and row["trigger"] != "TIMER":
                following = by_sequence.get(seq+1)
                _check(following is not None and following[1] == ("DECIDE" if row["trigger"] == "SELECT" else "ACK") and
                       int(following[0]["src"]) == src and int(following[0]["timestamp_ns"]) == now,
                       "snapshot not immediately followed by triggering decision/ACK")
            continue
        key, rail, size = _key(row), int(row["rail"]), int(row["bytes"])
        if kind == "ACK":
            _check(key in live and key in prepared_ack and key not in ack_finished,
                   "ACK lacks exact completion replacement")
            _check(all(cached[src, r]["time"] == now and cached[src, r]["trigger"] == "ACK" for r in (0, 1)),
                   "ACK lacks fresh interval evidence")
            _close(row["used_bps"], raw[src, rail]*cached[src, rail]["factor"], "ACK used rate differs from confidence correction")
            sample_counts[src, rail] += 1
            _check(int(row["rail_samples"]) == sample_counts[src, rail], "ACK sample counter mismatch")
            reserved[src, rail] -= size
            rail_live[src, rail] -= 1
            _check(reserved[src, rail] >= 0 and rail_live[src, rail] >= 0, "ACK reservation underflow")
            if int(live[key]["probe"]):
                probe_live[src] -= 1
            del live[key]
            ack_finished.add(key)
            q_finished[src] += 1
            continue
        _check(kind == "DECIDE", "unknown censored audit event")
        _check(key not in live and q_popped[src] > 0, "decision without local dispatch POP")
        q_popped[src] -= 1
        _check(all(cached[src, r]["time"] == now and cached[src, r]["trigger"] == "SELECT" for r in (0, 1)),
               "decision lacks fresh source-local intervals")
        sequence_ids[src] += 1
        _check(int(row["model_id"]) == sequence_ids[src] and
               int(row["launch_active"]) == rail_live[src, rail]+1,
               "model identity/launch-load not from assignment order")
        true_records[src, rail, sequence_ids[src]] = chunks[key]
        rates = []
        for r, suffix in enumerate(("A", "B")):
            evidence = cached[src, r]
            _close(row["raw_"+suffix], raw[src, r], "decision raw rate uses unavailable feedback")
            _close(row["factor_"+suffix], evidence["factor"], "decision confidence factor mismatch")
            _close(row["lower_"+suffix], evidence["lower"], "decision lower confidence mismatch")
            _close(row["upper_"+suffix], evidence["upper"], "decision upper confidence mismatch")
            rate = raw[src, r]*evidence["factor"]
            _close(row["rate_"+suffix], rate, "decision rate not raw times confidence factor")
            _check(int(row["reserved_"+suffix]) == reserved[src, r] and
                   int(row["last_ack_"+suffix]) == last_ack[src, r], "decision reservation/ACK history mismatch")
            rates.append(rate)
        for field, expected in (("queued", q_pending[src]), ("total_bytes", total[src]),
                ("probe_bytes", probe_bytes[src]), ("probe_inflight", probe_live[src]), ("last_probe_ns", last_probe[src])):
            _check(int(row[field]) == expected, "source-local probe state mismatch: " + field)
        probe = int(row["probe"])
        _check(probe in (0, 1), "invalid exploration marker")
        if not launched[src, 0] or not launched[src, 1]:
            expected = 0 if not launched[src, 0] else 1
            _check(rail == expected and int(row["base_rail"]) == -1 and not probe,
                   "invalid mandatory paired initial probe")
        else:
            _check(sum(rates) > 0, "no usable completion rate after initial probes")
            credit = credits[src, int(row["dst"])]
            for r in (0, 1):
                credit[r] += size*rates[r]/sum(rates)
            base = 1 if rates[0] <= 0 else 0 if rates[1] <= 0 else int(credit[1] > credit[0])
            _check(int(row["base_rail"]) == base, "base rail does not maximize byte credit")
            other = 1-base
            should_probe = bool(policy == "B14" and actuate and not probe_live[src] and
                q_pending[src] >= 8 and not reserved[src, other] and now >= last_probe[src]+50000 and
                now >= last_ack[src, other]+50000 and (probe_bytes[src]+size)*16 <= total[src]+size and
                cached[src, other]["upper"] >= cached[src, base]["lower"])
            _check(bool(probe) == should_probe and rail == (other if should_probe else base),
                   "bounded UCB exploration gate/choice mismatch")
            credit[rail] -= size
        launched[src, rail] += 1
        reserved[src, rail] += size
        rail_live[src, rail] += 1
        total[src] += size
        if probe:
            probe_bytes[src] += size
            probe_live[src] += 1
            last_probe[src] = now
            probes += 1
            _check(probe_live[src] <= 1 and probe_bytes[src]*16 <= total[src],
                   "exploration source budget/outstanding cap exceeded")
        penalties += min(float(row["factor_A"]), float(row["factor_B"])) < 1
        live[key] = row
    _check(not live and set(chunks) == completed_observed == ack_finished,
           "unfinished or missing exact completion evidence")
    _check(all(value == 0 for mapping in (reserved, rail_live, probe_live, q_pending, q_active, q_popped, q_finished)
               for value in mapping.values()), "nonempty final source/queue/probe state")
    return dict({"pass": True}, policy=policy, decisions=len(decisions),
                interval_rows=len(intervals), observation_rows=len(observations),
                confidence_penalty_decisions=penalties, exploration_decisions=probes,
                exploration_bytes=sum(probe_bytes.values()), total_bytes=sum(total.values()),
                max_snapshot_pending=max_pending, older_live_pending_snapshots=old_pending_snapshots,
                partial_identification_containment_checks=containment_checks,
                partial_identification_containment_violations=0,
                containment_scope="offline eventual utility of exactly selected cohort; not expected/future-utility confidence coverage",
                timer_pairs=len(timers), include_pending=include_pending, actuate=actuate,
                model_replay="all bounded-membership records, true-TX ages, exact replacements, normalization and interval arithmetic",
                confidence_scope="working Hoeffding-shaped bands; statistical coverage not established",
                source_inputs="own completed/unfinished chunks and local dispatch queue; no Oracle/switch observations")


def audit_censored(directory, feedback, events):
    """Malformed or inconsistent causal trace evidence raises ValueError."""
    try:
        return _audit_censored(directory, feedback, events)
    except (KeyError, TypeError, OverflowError, OSError, ZeroDivisionError) as error:
        raise ValueError("invalid/incomplete censored audit evidence: " + str(error)) from error
