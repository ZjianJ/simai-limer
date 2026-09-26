from __future__ import annotations
import csv
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODEL = ROOT / "ns-3-alibabacloud/simulation/src/point-to-point/model"
TOOL = ROOT / "limer/tools/validate_congestion_signal_events.py"
SPEC = importlib.util.spec_from_file_location("signal_validator", TOOL)
assert SPEC and SPEC.loader
validator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = validator
SPEC.loader.exec_module(validator)


def write(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


class CongestionSignalTest(unittest.TestCase):
    def test_explicit_sink_registration_is_standalone(self):
        compiler = shutil.which("g++")
        if not compiler:
            self.skipTest("g++ unavailable")
        src = '#include "limer-congestion-signal.h"\n#include <cassert>\nstatic void S(const ns3::LimerCongestionSignalEvent&,void*p){++*static_cast<int*>(p);}\nint main(){int n=0,o=0;ns3::LimerCongestionSignalEvent e;assert(ns3::RegisterLimerCongestionSignalSink(&S,&n));assert(!ns3::RegisterLimerCongestionSignalSink(&S,&o));ns3::EmitLimerCongestionSignal(e);assert(n==1);assert(ns3::ClearLimerCongestionSignalSink(&S,&n));}'
        with tempfile.TemporaryDirectory() as d:
            cpp = Path(d) / "t.cc"
            exe = Path(d) / "t"
            cpp.write_text(src)
            r = subprocess.run(
                [
                    compiler,
                    "-std=c++11",
                    "-pthread",
                    "-Wall",
                    "-Werror",
                    f"-I{MODEL}",
                    str(cpp),
                    str(MODEL / "limer-congestion-signal.cc"),
                    "-o",
                    str(exe),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(subprocess.run([str(exe)]).returncode, 0)

    def test_hooks_and_transition_guards_are_real(self):
        sw = (MODEL / "switch-node.cc").read_text()
        qbb = (MODEL / "qbb-net-device.cc").read_text()
        rdma = (MODEL / "rdma-hw.cc").read_text()
        front = (
            ROOT
            / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/limer_background_flow.h"
        ).read_text()
        for token in (
            "ShouldSendCN(ifIndex, qIndex)",
            "CheckShouldPause(inDev, qIndex)",
            "LIMER_SIGNAL_ECN_MARK",
        ):
            self.assertIn(token, sw)
        self.assertIn("transitioned && HasLimerCongestionSignalSink()", qbb)
        self.assertIn("new_rate < old_rate", rdma)
        self.assertNotIn("ecn_pressure", sw + qbb + rdma)
        self.assertNotIn("pfc_pressure", sw + qbb + rdma)
        self.assertIn("OBSERVED_REAL_MECHANISM_UNBOUND", front)
        self.assertIn("FATAL_OVERFLOW", front)
        self.assertIn("congestion_signal_summary.csv", front)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        p = Path(self.tmp.name)
        self.events = p / "events.csv"
        self.links = p / "links.csv"
        self.schedule = p / "schedule.csv"
        self.summary = p / "summary.csv"
        links = []
        for a, b, ap, bp, at, bt in (
            (0, 4, 1, 1, "HOST", "SWITCH"),
            (4, 5, 2, 2, "SWITCH", "SWITCH"),
            (2, 5, 1, 1, "HOST", "SWITCH"),
        ):
            links.append(
                dict(
                    link_id=f"L{min(a, b)}-{max(a, b)}",
                    src_node=a,
                    dst_node=b,
                    src_type=at,
                    dst_type=bt,
                    src_port=ap,
                    dst_port=bp,
                    link_class="ACCESS",
                    bandwidth_bps=100,
                    delay_ns=1,
                )
            )
        write(self.links, validator.LINK_COLUMNS, links)
        self.sched = dict(
            event_id="ev",
            flow_id="flow",
            scenario="ecn_pressure",
            scheduled_start_ns=1,
            src_rank=0,
            dst_rank=2,
            bytes=1000,
            pg=3,
            sport=49152,
            dport=4791,
        )
        write(self.schedule, validator.SCHEDULE_COLUMNS, [self.sched])
        self.rows = [
            self.row(k, i)
            for i, k in enumerate(
                (
                    "ECN_MARK",
                    "CNP_ACK_EMIT",
                    "CNP_ACK_RX",
                    "QP_RATE_DECREASE",
                    "PFC_PAUSE_SEND",
                    "PFC_PAUSE_RECEIVE",
                    "PFC_RESUME_SEND",
                    "PFC_RESUME_RECEIVE",
                ),
                2,
            )
        ]
        self.persist()

    def tearDown(self):
        self.tmp.cleanup()

    def row(self, kind, t):
        r = {x: "" for x in validator.COLUMNS}
        recv = kind.endswith("RECEIVE")
        send = kind.endswith("SEND")
        pfc = kind.startswith("PFC")
        pause = "PAUSE" in kind
        node, typ, port, lid = (
            (0, "HOST", 1, "L0-4")
            if recv or kind in {"CNP_ACK_RX", "QP_RATE_DECREASE"}
            else (
                (4, "SWITCH", 1, "L0-4")
                if pfc
                else (
                    (4, "SWITCH", 2, "L4-5")
                    if kind == "ECN_MARK"
                    else (2, "HOST", 1, "L2-5")
                )
            )
        )
        r.update(
            run_id="run",
            event_id="" if recv else "ev",
            flow_id="" if recv else "flow",
            scenario="" if recv else "ecn_pressure",
            event_type=kind,
            timestamp_ns=t,
            node_id=node,
            node_type=typ,
            port_id=port,
            link_id=lid,
            pg=3,
            signal_before=1
            if kind == "CNP_ACK_EMIT"
            else 0
            if pause
            else 1
            if pfc
            else 0,
            signal_after=1
            if pause or kind.startswith("CNP")
            else 3
            if kind == "ECN_MARK"
            else 0,
            pause_time=10 if pause and pfc else 0,
            cc_mode=1,
            status=validator.UNBOUND if recv else validator.BOUND,
        )
        if not recv:
            r.update(
                flow_src_rank=0, flow_dst_rank=2, flow_sport=49152, flow_dport=4791
            )
        if kind != "QP_RATE_DECREASE" and not recv:
            r.update(packet_sip=1, packet_dip=2, packet_sport=49152, packet_dport=4791)
        if kind in {"CNP_ACK_EMIT", "CNP_ACK_RX"}:
            r.update(packet_sip=2, packet_dip=1, packet_sport=4791, packet_dport=49152)
        if kind == "ECN_MARK":
            r.update(queue_bytes=20, threshold_low_bytes=5, threshold_high_bytes=15)
        if send:
            r.update(
                trigger_out_port=2,
                trigger_link_id="L4-5",
                queue_bytes=20,
                shared_used_bytes=10,
                threshold_low_bytes=5,
                threshold_high_bytes=15,
                headroom_bytes=2,
            )
        if recv:
            r.update(queue_bytes=0)
        if kind == "QP_RATE_DECREASE":
            r.update(old_rate_bps=100, new_rate_bps=50)
        return r

    def persist(self, seen=None, filtered=0, overflow=0, status="PASS"):
        write(self.events, validator.COLUMNS, self.rows)
        recorded = len(self.rows)
        seen = recorded + filtered + overflow if seen is None else seen
        write(
            self.summary,
            validator.SUMMARY_COLUMNS,
            [
                dict(
                    run_id="run",
                    window_start_ns=1,
                    window_end_ns=100,
                    max_events=100,
                    seen=seen,
                    recorded=recorded,
                    unbound=sum(r["status"] == validator.UNBOUND for r in self.rows),
                    filtered=filtered,
                    overflow=overflow,
                    status=status,
                )
            ],
        )

    def check(self, profile="RAW"):
        return validator.validate(
            events=self.events,
            link_map=self.links,
            schedule=self.schedule,
            summary=self.summary,
            expected_run_id="run",
            profile=profile,
            gpus_per_server=2,
        )

    def test_profiles_pass(self):
        for profile in ("RAW", "ECN", "PFC"):
            with self.subTest(profile=profile):
                self.assertEqual(
                    self.check(profile)["status"], "PASS", self.check(profile)
                )

    def test_fail_closed_tampers(self):
        cases = []
        cases.append(
            (
                "single_fake",
                lambda rs: rs.__setitem__(slice(None), [rs[0]]),
                "ECN_CHAIN",
                "ECN",
            )
        )
        cases.append(
            (
                "nondigit",
                lambda rs: rs[0].__setitem__("timestamp_ns", "1x"),
                "INTEGER",
                "RAW",
            )
        )
        cases.append(
            (
                "empty_id",
                lambda rs: rs[0].__setitem__("event_id", ""),
                "IDENTIFIER",
                "RAW",
            )
        )
        cases.append(
            (
                "wrong_scenario",
                lambda rs: rs[0].__setitem__("scenario", "pfc_pressure"),
                "EVENT_BINDING",
                "RAW",
            )
        )
        cases.append(
            (
                "wrong_chain",
                lambda rs: rs[3].__setitem__("timestamp_ns", 2),
                "ECN_CHAIN",
                "ECN",
            )
        )
        cases.append(
            (
                "pfc_noop",
                lambda rs: rs[5].__setitem__("signal_before", 1),
                "PFC_PAUSE_RECEIVE_CONTRACT",
                "RAW",
            )
        )
        cases.append(
            (
                "rate_packet",
                lambda rs: rs[3].update(
                    packet_sip=1, packet_dip=2, packet_sport=1, packet_dport=2
                ),
                "QP_RATE_DECREASE_CONTRACT",
                "RAW",
            )
        )
        for name, mutate, code, profile in cases:
            with self.subTest(name=name):
                original = deepcopy(self.rows)
                mutate(self.rows)
                self.persist()
                self.assertEqual(self.check(profile)["errors"][0]["code"], code)
                self.rows = original

    def test_overflow_and_missing_binding_inputs_fail(self):
        self.persist(overflow=1, status="FATAL_OVERFLOW")
        self.assertEqual(self.check()["errors"][0]["code"], "SUMMARY")
        self.assertEqual(
            validator.validate(
                events=self.events,
                link_map=self.links,
                schedule=Path("missing"),
                summary=self.summary,
                expected_run_id="run",
                gpus_per_server=2,
            )["status"],
            "FAIL",
        )


if __name__ == "__main__":
    unittest.main()
