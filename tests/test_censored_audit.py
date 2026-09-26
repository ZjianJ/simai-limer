import csv
import hashlib
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from audit_censored_split import _interval, _Model, _factor, _source_time_order, _qualify_fault_scope, audit_censored


def record(size=1000, load=1, first=100, done=0):
    return dict(bytes=size, load=load, first=first, done=done)


class CensoredIntervalReplayTest(unittest.TestCase):
    def test_pending_is_interval_not_zero_reward_sample(self):
        result = _interval({1: record()}, 300)
        self.assertEqual(result["identified_lower"], 0.)
        self.assertAlmostEqual(result["identified_upper"], .4)
        self.assertEqual((result["count"], result["completed"], result["pending"]), (1, 0, 1))

    def test_repeated_pending_ages_without_new_observation(self):
        records = {1: record()}
        a, b = _interval(records, 300), _interval(records, 500)
        self.assertEqual(a["count"], b["count"])
        self.assertEqual(a["radius"], b["radius"])
        self.assertLess(b["identified_upper"], a["identified_upper"])

    def test_completion_replaces_pending_instead_of_appending(self):
        records = {1: record()}
        before = _interval(records, 299)
        records[1]["done"] = 300
        after = _interval(records, 300)
        self.assertEqual(before["count"], after["count"])
        self.assertEqual((after["completed"], after["pending"]), (1, 0))
        self.assertEqual(after["identified_lower"], after["identified_upper"])
        self.assertAlmostEqual(after["identified_lower"], .4)

    def test_launch_load_is_rounded_up_power_two(self):
        a = _interval({1: record(load=3, done=500)}, 500)
        b = _interval({1: record(load=4, done=500)}, 500)
        self.assertEqual(a, b)
        self.assertAlmostEqual(a["identified_lower"], .8)

    def test_expired_completion_excluded_but_pending_retained(self):
        result = _interval({1: record(done=300), 2: record()}, 300100)
        self.assertEqual((result["count"], result["completed"], result["pending"]), (1, 0, 1))
        self.assertEqual(result["last_completed_ns"], 0)

    def test_complete_only_ablation_retains_no_pending(self):
        result = _interval({1: record(done=300), 2: record()}, 500, False)
        self.assertEqual((result["count"], result["completed"], result["pending"]), (1, 1, 0))

    def test_nominal_radius_counts_individual_chunks_once(self):
        result = _interval({key: record(done=300) for key in range(8)}, 500)
        self.assertAlmostEqual(result["radius"], math.sqrt(math.log(40)/16))
        self.assertEqual(result["ready"], 1)

    def test_future_first_tx_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "future"):
            _interval({1: record(first=101)}, 100)

    def test_empty_interval_is_uninformative(self):
        result = _interval({}, 500)
        self.assertEqual((result["lower"], result["upper"], result["count"], result["ready"]),
                         (0., 1., 0, 0))


class CensoredMembershipReplayTest(unittest.TestCase):
    def test_older_pending_survives_window_until_completed(self):
        model = _Model(window=2)
        model.upsert(0, 0, 1, 1000, 1, 100, 100, False)
        model.upsert(0, 0, 2, 1000, 1, 100, 200, True)
        model.upsert(0, 0, 3, 1000, 1, 100, 200, True)
        self.assertEqual(set(model.records[0, 0]), {1, 2, 3})
        self.assertEqual(model.evaluate(0, 0, 200)["count"], 3)
        model.upsert(0, 0, 1, 1000, 1, 100, 300, True)
        self.assertEqual(set(model.records[0, 0]), {2, 3})

    def test_expiry_does_not_reenter_evicted_older_sample(self):
        model = _Model(window=2)
        for identity in (1, 2, 3):
            model.upsert(0, 0, identity, 1000, 1, 100, 200, True)
        self.assertEqual(model.evaluate(0, 0, 300100)["count"], 0)
        model.upsert(0, 0, 1, 1000, 1, 300000, 300100, True)
        self.assertEqual(model.evaluate(0, 0, 300100)["count"], 0)

    def test_future_admission_and_mutated_metadata_rejected(self):
        model = _Model()
        with self.assertRaisesRegex(ValueError, "noncausal"):
            model.upsert(0, 0, 1, 1000, 1, 101, 100, False)
        model.upsert(0, 0, 1, 1000, 1, 100, 100, False)
        with self.assertRaisesRegex(ValueError, "metadata"):
            model.upsert(0, 0, 1, 1000, 2, 100, 200, True)

    def test_pending_budget_failclosed(self):
        model = _Model()
        for identity in range(8):
            model.upsert(0, 0, identity, 1000, 1, 100, 100, False)
        with self.assertRaisesRegex(ValueError, "pending limit"):
            model.upsert(0, 0, 8, 1000, 1, 100, 100, False)


def fixture(policy="B13", long=False, actuate=True, include_pending=True):
    """Small complete traces, including a positive bounded-probe opportunity."""
    files = {name: [] for name in ("censored_intervals.csv", "censored_decisions.csv",
                                   "censored_observations.csv", "censored_queue.csv")}
    feedback, events = [], []
    manifest = {"policy": policy, "env": {"LIMER_SPLIT_CENSORED_ACTUATE": str(int(actuate)),
        "LIMER_SPLIT_CENSORED_INCLUDE_PENDING": str(int(include_pending))}}
    model = _Model()
    raw, counts, reserved, live_count = [0., 0.], [0, 0], [0, 0], [0, 0]
    launched, last_ack, credit = [0, 0], [0, 0], [0., 0.]
    live, seen, choices = {}, set(), {}
    sequence = q_pending = q_active = next_id = total = probe_bytes = probe_live = last_probe = 0

    def stamp(row):
        nonlocal sequence
        sequence += 1
        return dict(row, audit_seq=sequence)

    def queue(now, event):
        nonlocal q_pending, q_active
        limit = 0
        if event == "ENQUEUE":
            q_pending += 1
        elif event == "POP":
            q_pending -= 1
            q_active += 1
            limit = 8 if sum(counts) else 2
        else:
            q_active -= 1
        files["censored_queue.csv"].append(stamp(dict(timestamp_ns=now, src=0, event=event,
            queued=q_pending, active=q_active, limit=limit)))

    def observe(now, identity, done):
        item = live[identity]
        model.upsert(0, item["rail"], identity, 1000, item["load"], item["first"], now, done)
        if done or identity not in seen:
            files["censored_observations.csv"].append(stamp(dict(timestamp_ns=now,
                src=0, dst=4, sport=identity+10, rail=item["rail"], id=identity, bytes=1000,
                launch_active=item["load"], first_tx_ns=item["first"], completed=int(done))))
            seen.add(identity)

    def snapshot(now, trigger, completing=None):
        for identity in live:
            if identity != completing:
                observe(now, identity, False)
        evidence = [model.evaluate(0, rail, now, include_pending) for rail in (0, 1)]
        for rail in (0, 1):
            files["censored_intervals.csv"].append(stamp(dict(timestamp_ns=now, trigger=trigger,
                src=0, rail=rail, raw_bps=raw[rail], **evidence[rail], factor=_factor(evidence, rail, actuate))))
        return evidence

    def assign(now, enqueue=True):
        nonlocal next_id, total, probe_bytes, probe_live, last_probe
        if enqueue:
            queue(now, "ENQUEUE")
        queue(now, "POP")
        evidence = snapshot(now, "SELECT")
        factors = [_factor(evidence, rail, actuate) for rail in (0, 1)]
        rates = [raw[rail]*factors[rail] for rail in (0, 1)]
        base, probe = -1, False
        if not launched[0] or not launched[1]:
            rail = 0 if not launched[0] else 1
        else:
            for rail in (0, 1):
                credit[rail] += 1000*rates[rail]/sum(rates)
            base = 1 if rates[0] <= 0 else 0 if rates[1] <= 0 else int(credit[1] > credit[0])
            other = 1-base
            probe = (policy == "B14" and actuate and not probe_live and q_pending >= 8 and
                     not reserved[other] and now >= last_probe+50000 and now >= last_ack[other]+50000 and
                     (probe_bytes+1000)*16 <= total+1000 and evidence[other]["upper"] >= evidence[base]["lower"])
            rail = other if probe else base
            credit[rail] -= 1000
        next_id += 1
        live_count[rail] += 1
        row = dict(timestamp_ns=now, src=0, dst=4, sport=next_id+10, rail=rail, bytes=1000,
            model_id=next_id, launch_active=live_count[rail], raw_A=raw[0], raw_B=raw[1],
            factor_A=factors[0], factor_B=factors[1], rate_A=rates[0], rate_B=rates[1],
            reserved_A=reserved[0], reserved_B=reserved[1], lower_A=evidence[0]["lower"],
            lower_B=evidence[1]["lower"], upper_A=evidence[0]["upper"], upper_B=evidence[1]["upper"],
            base_rail=base, probe=int(probe), queued=q_pending, total_bytes=total, probe_bytes=probe_bytes,
            probe_inflight=probe_live, last_probe_ns=last_probe, last_ack_A=last_ack[0], last_ack_B=last_ack[1])
        files["censored_decisions.csv"].append(stamp(row))
        choices[next_id] = row
        live[next_id] = dict(rail=rail, load=live_count[rail], first=now, probe=probe)
        events.append(dict(timestamp_ns=now, event="ASSIGN", src=0, dst=4, sport=next_id+10,
                           rail=rail, bytes=1000, policy=policy))
        launched[rail] += 1
        reserved[rail] += 1000
        total += 1000
        if probe:
            probe_bytes += 1000
            probe_live += 1
            last_probe = now
        return next_id

    def complete(now, identity):
        nonlocal probe_live
        item, choice = live[identity], choices[identity]
        rail, first = item["rail"], item["first"]
        raw[rail] = 1000*8e9/(now-first)
        last_ack[rail] = now
        observe(now, identity, True)
        evidence = snapshot(now, "ACK", identity)
        counts[rail] += 1
        feedback.append(stamp(dict(timestamp_ns=now, policy=policy, src=0, dst=4, sport=identity+10,
            rail=rail, bytes=1000, first_tx_ns=first, completion_ns=now, elapsed_ns=now-first,
            measured_bps=raw[rail], used_bps=raw[rail]*_factor(evidence, rail, actuate), rail_samples=counts[rail])))
        events.append(dict(timestamp_ns=now, event="ACK_COMPLETE", src=0, dst=4, sport=identity+10,
                           rail=rail, bytes=1000, policy=policy))
        reserved[rail] -= 1000
        live_count[rail] -= 1
        if item["probe"]:
            probe_live -= 1
        del live[identity]
        queue(now, "COMPLETE")

    a, b = assign(1), assign(1)
    complete(11, a)
    complete(12, b)
    if long:
        for now in range(20, 281, 20):
            complete(now+10, assign(now))
        for _ in range(20):
            queue(60001, "ENQUEUE")
        for now in range(60001, 60400, 20):
            complete(now+10, assign(now, False))
    else:
        complete(30, assign(20))
    return manifest, files, feedback, events


def check(data):
    manifest, files, feedback, events = data
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        (directory / "manifest.json").write_text(json.dumps(manifest))
        for name, records in files.items():
            with (directory / name).open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(records[0]) if records else ["event"])
                writer.writeheader()
                writer.writerows(records)
        return audit_censored(directory, feedback, events)


class CensoredFullAuditTest(unittest.TestCase):
    def test_interleaved_logical_processor_clocks_are_causal(self):
        # Actual healthy B13 shared-lock order: LP 15 time 14635, LP 8
        # time 14393. Neither source moves backwards or shares observations.
        rows = [(dict(src=15, timestamp_ns=14635), "QUEUE"),
                (dict(src=8, timestamp_ns=14393), "OBSERVE"),
                (dict(src=15, timestamp_ns=14636), "QUEUE")]
        _source_time_order(rows)

    def test_same_source_backwards_time_is_rejected(self):
        rows = [(dict(src=15, timestamp_ns=14635), "QUEUE"),
                (dict(src=8, timestamp_ns=14393), "OBSERVE"),
                (dict(src=15, timestamp_ns=14634), "QUEUE")]
        with self.assertRaisesRegex(ValueError, "source-local"):
            _source_time_order(rows)

    def test_complete_causal_traces_and_shadow(self):
        for policy in ("B13", "B14"):
            for actuate, pending in ((True, True), (False, True), (True, False)):
                with self.subTest(policy=policy, actuate=actuate, pending=pending):
                    result = check(fixture(policy, actuate=actuate, include_pending=pending))
                    self.assertTrue(result["pass"])
                    self.assertEqual(result["decisions"], 3)
                    self.assertGreater(result["partial_identification_containment_checks"], 0)
                    self.assertEqual(result["partial_identification_containment_violations"], 0)

    def test_positive_bounded_probe_and_disabled_ablation(self):
        result = check(fixture("B14", long=True))
        self.assertGreater(result["exploration_decisions"], 0)
        self.assertLessEqual(result["exploration_bytes"]*16, result["total_bytes"])
        self.assertEqual(check(fixture("B13", long=True))["exploration_decisions"], 0)
        self.assertEqual(check(fixture("B14", long=True, actuate=False))["exploration_decisions"], 0)

    def test_interval_arithmetic_and_counts_reject_mutations(self):
        for field in ("identified_lower", "identified_upper", "lower", "upper", "radius", "count", "completed", "pending"):
            data = fixture()
            data[1]["censored_intervals.csv"][-1][field] += .1 if field in ("identified_lower", "identified_upper", "lower", "upper", "radius") else 1
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_model_metadata_and_future_admission_rejected(self):
        for field in ("id", "bytes", "launch_active", "first_tx_ns"):
            data = fixture()
            data[1]["censored_observations.csv"][0][field] += 1
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_decision_state_and_fusion_reject_mutations(self):
        for field in ("raw_A", "factor_A", "rate_A", "reserved_A", "lower_A", "upper_A",
                      "queued", "total_bytes", "probe_bytes", "probe_inflight", "last_probe_ns", "last_ack_A", "model_id", "launch_active"):
            data = fixture()
            data[1]["censored_decisions.csv"][-1][field] += 1e9 if field in ("raw_A", "rate_A") else 10
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_probe_gate_and_choice_reject_mutations(self):
        data = fixture("B14", long=True)
        probe = next(row for row in data[1]["censored_decisions.csv"] if row["probe"])
        probe["probe"] = 0
        with self.assertRaisesRegex(ValueError, "exploration"):
            check(data)

    def test_duplicate_causal_sequence_rejected(self):
        data = fixture()
        data[1]["censored_decisions.csv"][0]["audit_seq"] = data[1]["censored_intervals.csv"][0]["audit_seq"]
        with self.assertRaisesRegex(ValueError, "sequence"):
            check(data)

    def test_repeated_pending_admission_is_not_a_new_sample(self):
        data = fixture()
        original = next(row for row in data[1]["censored_observations.csv"] if not row["completed"])
        insert_sequence = original["audit_seq"]+1
        duplicate = dict(original, audit_seq=insert_sequence)
        for records in list(data[1].values())+[data[2]]:
            for row in records:
                if row["audit_seq"] >= insert_sequence:
                    row["audit_seq"] += 1
        data[1]["censored_observations.csv"].append(duplicate)
        with self.assertRaisesRegex(ValueError, "duplicate pending"):
            check(data)

    def test_completion_omission_is_rejected(self):
        data = fixture()
        data[1]["censored_observations.csv"] = [row for row in data[1]["censored_observations.csv"]
            if not (row["completed"] and row["id"] == 3)]
        with self.assertRaises(ValueError):
            check(data)

    def test_wrong_local_queue_state_rejected(self):
        data = fixture()
        data[1]["censored_queue.csv"][0]["queued"] += 1
        with self.assertRaisesRegex(ValueError, "queue"):
            check(data)

    def test_oracle_capacity_and_foreign_sensor_rejected(self):
        data = fixture()
        data[0]["env"]["LIMER_SPLIT_CAPACITY_CSV"] = "secret-oracle.csv"
        with self.assertRaisesRegex(ValueError, "Oracle"):
            check(data)
        data = fixture()
        data[1]["switch_signals.csv"] = [dict(event="DELIVER")]
        with self.assertRaisesRegex(ValueError, "endpoint-only"):
            check(data)

    def test_applied_hard_disconnect_and_carrier_mutation_rejected(self):
        data = fixture()
        data[1]["fault_application_telemetry.csv"] = [dict(fault_type="hard_disconnect", mechanism="physical_link_down", transition="apply", status="APPLIED")]
        with self.assertRaisesRegex(ValueError, "unsupported applied fault type"):
            check(data)
        data[1]["fault_application_telemetry.csv"] = [dict(fault_type="service_degradation", mechanism="physical_link_down", transition="apply", status="APPLIED")]
        with self.assertRaisesRegex(ValueError, "unsupported applied mechanism"):
            check(data)
        data[1]["fault_application_telemetry.csv"] = [dict(fault_type="service_degradation", mechanism="carrier_up_service_rate", transition="apply", status="APPLIED"),
            dict(fault_type="service_degradation", mechanism="restore_baseline", transition="revert", status="REVERTED")]
        self.assertTrue(check(data)["pass"])

    def test_service_revert_requires_matching_transition_and_status(self):
        for transition, status in (("apply", "REVERTED"), ("revert", "APPLIED"), ("revert", "FAILED")):
            data = fixture()
            data[1]["fault_application_telemetry.csv"] = [dict(fault_type="service_degradation",
                mechanism="restore_baseline", transition=transition, status=status)]
            with self.subTest(transition=transition, status=status), self.assertRaisesRegex(ValueError, "mechanism/transition/status"):
                check(data)

    def test_manifest_bound_schedule_rejects_hard_disconnect_and_hash_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            path = directory/"faults.csv"
            manifest = {"env": {"LIMER_FAULT_SCHEDULE": "faults.csv"}}
            for fault_type in ("service_degradation", "hard_disconnect"):
                path.write_text("fault_type\n"+fault_type+"\n")
                manifest["fault_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                if fault_type == "service_degradation":
                    _qualify_fault_scope(directory, manifest)
                else:
                    with self.assertRaisesRegex(ValueError, "unsupported fault type"):
                        _qualify_fault_scope(directory, manifest)
            manifest["fault_sha256"] = "0"*64
            with self.assertRaisesRegex(ValueError, "manifest hash"):
                _qualify_fault_scope(directory, manifest)


if __name__ == "__main__":
    unittest.main()
