import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from audit_adaptive_split import audit_adaptive, _audit_wait_raw


def fixture(policy="B10", enabled=True, actuate=True):
    manifest = {"policy": policy, "env": {"LIMER_SPLIT_SIGNAL_SAMPLE_NS": "1000",
        "LIMER_SPLIT_SIGNAL_DELAY_NS": "1000", "LIMER_SPLIT_SIGNAL_TTL_NS": "5000",
        "LIMER_SPLIT_SIGNAL_HEARTBEAT_NS": "4000", "LIMER_SPLIT_SIGNAL_ENABLE": str(int(enabled)),
        "LIMER_SPLIT_SIGNAL_ACTUATE": str(int(actuate))}}
    files = {"adaptive_decisions.csv": [], "adaptive_wait.csv": [],
             "switch_signals.csv": [], "switch_signal_samples.csv": [], "link_map.csv": []}
    feedback, events = [], []
    live = policy == "B12" and enabled and actuate
    last_rail = 1 if live else 0
    for src in range(16):
        for rail in (0, 1):
            files["link_map.csv"].append(dict(link_class="ACCESS", src_type="HOST",
                dst_type="SWITCH", src_node=src, dst_node=20+rail, src_port=2+rail, dst_port=src+1))
    raw = [0., 0.]
    reserve = [0, 0]
    counts = [0, 0]

    def assign(time, sport, rail):
        signals = [.1 if live and time >= 2000 else 1., 1.]
        files["adaptive_decisions.csv"].append(dict(timestamp_ns=time, src=0, dst=4,
            sport=sport, rail=rail, bytes=1000, raw_A=raw[0], raw_B=raw[1], wait_A=1,
            wait_B=1, signal_A=signals[0], signal_B=signals[1], rate_A=raw[0]*signals[0],
            rate_B=raw[1], reserved_A=reserve[0], reserved_B=reserve[1]))
        reserve[rail] += 1000
        events.append(dict(timestamp_ns=time, policy=policy, event="ASSIGN", src=0,
            dst=4, sport=sport, rail=rail, bytes=1000))

    def complete(time, sport, rail, first):
        raw[rail] = 1000*8e9/(time-first)
        counts[rail] += 1
        for r in (0, 1):
            files["adaptive_wait.csv"].append(dict(timestamp_ns=time, trigger="ACK", src=0,
                rail=r, raw_bps=raw[r], factor=1, witness_dst=0, witness_sport=0,
                witness_bytes=0, first_tx_ns=0, launch_active=0, age_ns=0,
                threshold_ns=0, samples=0))
        feedback.append(dict(timestamp_ns=time, policy=policy, src=0, dst=4, sport=sport,
            rail=rail, bytes=1000, first_tx_ns=first, completion_ns=time, elapsed_ns=time-first,
            measured_bps=raw[rail], used_bps=raw[rail]*(.1 if live and rail == 0 else 1.),
            rail_samples=counts[rail]))
        reserve[rail] -= 1000
        events.append(dict(timestamp_ns=time, policy=policy, event="ACK_COMPLETE", src=0,
            dst=4, sport=sport, rail=rail, bytes=1000))

    assign(1000, 10, 0)
    assign(1000, 11, 1)
    complete(6000, 10, 0, 1000)
    complete(7000, 11, 1, 1000)
    assign(8000, 12, last_rail)
    complete(9000, 12, last_rail, 8000)
    if policy == "B12" and enabled:
        for time in range(1000, 9001, 1000):
            for src in range(16):
                for rail in (0, 1):
                    up = not (src == 4 and rail == 0)
                    sample = dict(timestamp_ns=time, node=20+rail, port=src+1, dst=src,
                        rail=rail, tx_bytes=time*10, queue_bytes=100,
                        wire_bps=0 if time == 1000 else 80e9,
                        reference_bps=80e9 if time > 1000 and up else 0,
                        demand=int(time > 1000), factor=1 if up else .1,
                        emitted=int(time in (1000, 5000, 9000)), up=int(up))
                    files["switch_signal_samples.csv"].append(sample)
                    if sample["emitted"]:
                        message = {k: sample[k] for k in ("timestamp_ns", "node", "port",
                            "dst", "rail", "tx_bytes", "queue_bytes", "wire_bps",
                            "reference_bps", "demand", "factor")}
                        message.update(event="EMIT", sampled_ns=time, observer=16, payload_bytes=32)
                        files["switch_signals.csv"].append(message)
                        if time+1000 < 9006:
                            for observer in range(16):
                                if observer//4 != src//4:
                                    files["switch_signals.csv"].append(dict(message,
                                        event="DELIVER", timestamp_ns=time+1000, observer=observer))
    ordered = []
    for filename, priority in (("switch_signal_samples.csv", 0), ("switch_signals.csv", 1),
                               ("adaptive_wait.csv", 3), ("adaptive_decisions.csv", 5)):
        ordered.extend((int(r["timestamp_ns"]), priority + (r.get("event") == "DELIVER"), r)
                       for r in files[filename])
    ordered.extend((int(r["timestamp_ns"]), 4, r) for r in feedback)
    for sequence, (_, _, row) in enumerate(sorted(ordered, key=lambda v: v[:2]), 1):
        row["audit_seq"] = sequence
    for filename in ("adaptive_wait.csv", "adaptive_decisions.csv", "switch_signals.csv",
                     "switch_signal_samples.csv"):
        files[filename].sort(key=lambda r: r["audit_seq"])
    files["run_lifecycle.csv"] = [dict(status="WORKLOAD_COMPLETE", actual_ns=9006)]
    return manifest, files, feedback, events


def check(data):
    manifest, files, feedback, events = data
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        (directory / "manifest.json").write_text(json.dumps(manifest))
        for filename, records in files.items():
            with (directory / filename).open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(records[0]) if records else ["event"])
                writer.writeheader()
                writer.writerows(records)
        return audit_adaptive(directory, feedback, events)


def waiting_fixture():
    """Eight comparable completions, then a genuinely censored slow chunk."""
    manifest, files, _, _ = fixture("B11")
    files["adaptive_wait.csv"], files["adaptive_decisions.csv"] = [], []
    files["run_lifecycle.csv"] = [dict(status="WORKLOAD_COMPLETE", actual_ns=1006)]
    feedback, events, raw, reserved, counts = [], [], [0., 0.], [0, 0], [0, 0]
    sequence = 0

    def stamp(row):
        nonlocal sequence
        sequence += 1
        row["audit_seq"] = sequence
        return row

    def assign(now, sport, rail):
        files["adaptive_decisions.csv"].append(stamp(dict(timestamp_ns=now, src=0, dst=4,
            sport=sport, rail=rail, bytes=1000, raw_A=raw[0], raw_B=raw[1], wait_A=1,
            wait_B=1, signal_A=1, signal_B=1, rate_A=raw[0], rate_B=raw[1],
            reserved_A=reserved[0], reserved_B=reserved[1])))
        reserved[rail] += 1000
        events.append(dict(timestamp_ns=now, event="ASSIGN", policy="B11", src=0, dst=4,
                           sport=sport, rail=rail, bytes=1000))

    def complete(now, sport, rail, first):
        raw[rail] = 1000*8e9/(now-first)
        counts[rail] += 1
        for r in (0, 1):
            witness = now == 600 and r == 0
            files["adaptive_wait.csv"].append(stamp(dict(timestamp_ns=now, trigger="ACK",
                src=0, rail=r, raw_bps=raw[r], factor=.1 if witness else 1,
                witness_dst=4 if witness else 0, witness_sport=30 if witness else 0,
                witness_bytes=1000 if witness else 0, first_tx_ns=200 if witness else 0,
                launch_active=1 if witness else 0, age_ns=400 if witness else 0,
                threshold_ns=19.53125 if witness else 0, samples=8 if witness else 0)))
        feedback.append(stamp(dict(timestamp_ns=now, policy="B11", src=0, dst=4, sport=sport,
            rail=rail, bytes=1000, first_tx_ns=first, completion_ns=now, elapsed_ns=now-first,
            measured_bps=raw[rail], used_bps=raw[rail], rail_samples=counts[rail])))
        reserved[rail] -= 1000
        events.append(dict(timestamp_ns=now, event="ACK_COMPLETE", policy="B11", src=0,
                           dst=4, sport=sport, rail=rail, bytes=1000))

    assign(1, 10, 0)
    assign(1, 11, 1)
    complete(11, 10, 0, 1)
    complete(12, 11, 1, 1)
    for index, start in enumerate(range(30, 151, 20)):
        assign(start, 20+index, 0)
        complete(start+10, 20+index, 0, start)
    assign(200, 30, 0)
    assign(201, 31, 1)
    complete(600, 31, 1, 201)
    complete(1000, 30, 0, 200)
    return manifest, files, feedback, events


class AdaptiveAuditTest(unittest.TestCase):
    def test_same_timestamp_acks_use_sequence_not_largest_rate(self):
        # Regression: early_42/B10 actual ACKs at 1301179 ns had rates
        # 20.6526 then 20.0800 Gb/s; sorting (time,rate) wrongly chose the first.
        values = [(10, 300., 50), (13, 200., 50)]
        row = dict(rail=0, timestamp_ns=54, audit_seq=20, trigger="TIMER", raw_bps=200)
        _audit_wait_raw(row, values, [10, 13])
        row["raw_bps"] = 300
        with self.assertRaisesRegex(ValueError, "wrong-sequence"):
            _audit_wait_raw(row, values, [10, 13])

    def test_ack_includes_own_update_but_timer_cannot_see_future_seq(self):
        values = [(10, 300., 50), (13, 200., 50)]
        row = dict(rail=0, timestamp_ns=50, audit_seq=11, trigger="ACK", raw_bps=200)
        _audit_wait_raw(row, values, [10, 13])
        row["trigger"] = "TIMER"
        with self.assertRaisesRegex(ValueError, "wrong-sequence"):
            _audit_wait_raw(row, values, [10, 13])
        row["raw_bps"] = 300
        _audit_wait_raw(row, values, [10, 13])

    def test_valid_endpoint_and_signal(self):
        for policy in ("B10", "B11", "B12"):
            with self.subTest(policy=policy):
                result = check(fixture(policy))
                self.assertTrue(result["pass"])
                self.assertEqual(result["decision_count"], 3)
                if policy == "B12":
                    self.assertEqual(result["signal_affected_decisions"], 1)
                    self.assertEqual(result["signal"]["port_emits"], 96)
                    self.assertEqual(result["signal"]["deliveries"], 768)

    def test_shadow_and_disabled_are_neutral(self):
        for enabled, actuate in ((True, False), (False, True)):
            result = check(fixture("B12", enabled, actuate))
            self.assertEqual(result["signal_affected_decisions"], 0)

    def test_wrong_raw_or_fusion_rejected(self):
        for field in ("raw_A", "rate_A", "reserved_A", "wait_A"):
            data = fixture()
            data[1]["adaptive_decisions.csv"][-1][field] += 1e6
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_wrong_rail_choice_rejected(self):
        for policy in ("B10", "B11"):
            data = fixture(policy)
            data[1]["adaptive_decisions.csv"][-1]["rail"] = 1
            data[3][-2]["rail"] = 1
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                check(data)

    def test_signal_delivery_delay_rejected(self):
        data = fixture("B12")
        row = next(r for r in data[1]["switch_signals.csv"] if r["event"] == "DELIVER")
        row["timestamp_ns"] += 1
        with self.assertRaisesRegex(ValueError, "delay"):
            check(data)

    def test_wrong_observer_or_mapping_rejected(self):
        for field, value in (("observer", 16), ("dst", 99)):
            data = fixture("B12")
            row = next(r for r in data[1]["switch_signals.csv"] if r["event"] == "DELIVER")
            row[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_wire_and_demand_are_recomputed(self):
        for field, value in (("wire_bps", 7), ("demand", 0), ("factor", .5), ("emitted", 1)):
            data = fixture("B12")
            row = next(r for r in data[1]["switch_signal_samples.csv"] if r["timestamp_ns"] == 2000)
            row[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(data)

    def test_stale_signal_rejected(self):
        data = fixture("B12")
        data[0]["env"]["LIMER_SPLIT_SIGNAL_TTL_NS"] = "2000"
        with self.assertRaisesRegex(ValueError, "stale|fusion"):
            check(data)

    def test_witness_cannot_be_invented(self):
        data = fixture()
        row = data[1]["adaptive_wait.csv"][0]
        row.update(factor=.5, witness_dst=4, witness_sport=999, witness_bytes=1000,
                   first_tx_ns=1000, launch_active=1, age_ns=5000, threshold_ns=2500, samples=8)
        with self.assertRaisesRegex(ValueError, "unknown waiting"):
            check(data)

    def test_valid_conditional_witness(self):
        self.assertEqual(check(waiting_fixture())["wait_penalty_rows"], 1)

    def test_witness_timing_size_and_history_rejected(self):
        for field, value in (("age_ns", 401), ("first_tx_ns", 201), ("witness_bytes", 999),
                             ("samples", 7), ("samples", 100), ("threshold_ns", 200),
                             ("launch_active", 2)):
            data = waiting_fixture()
            row = next(r for r in data[1]["adaptive_wait.csv"] if r["witness_bytes"])
            row[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                check(data)

    def test_timer_and_sequence_tamper_rejected(self):
        data = fixture()
        for row in data[1]["adaptive_wait.csv"][:2]:
            row["trigger"] = "TIMER"
        with self.assertRaisesRegex(ValueError, "aligned"):
            check(data)
        data = fixture()
        data[1]["adaptive_decisions.csv"][-1]["audit_seq"] = 1
        with self.assertRaisesRegex(ValueError, "sequence"):
            check(data)

    def test_feedback_used_rate_is_checked(self):
        data = fixture("B12")
        data[2][0]["used_bps"] *= 2
        with self.assertRaisesRegex(ValueError, "feedback used rate"):
            check(data)


if __name__ == "__main__":
    unittest.main()
