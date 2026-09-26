#!/usr/bin/env python3
"""Static contracts for bounded-memory true-16 simulation runtime paths."""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NS3 = ROOT / "ns-3-alibabacloud" / "simulation" / "src"
P2P = NS3 / "point-to-point" / "model"
APPS = NS3 / "applications" / "model"
TELEMETRY = (
    ROOT
    / "astra-sim-alibabacloud"
    / "astra-sim"
    / "network_frontend"
    / "ns3"
    / "limer_telemetry.h"
)
ASTRA = ROOT / "astra-sim-alibabacloud" / "astra-sim"
USAGE_TRACKER = ASTRA / "system" / "UsageTracker.cc"
USAGE_TRACKER_HEADER = ASTRA / "system" / "UsageTracker.hh"
WORKLOAD = ASTRA / "workload" / "Workload.cc"


def body(text: str, signature: str) -> str:
    start = text.index(signature)
    opening = text.index("{", start)
    depth = 0
    for offset in range(opening, len(text)):
        if text[offset] == "{":
            depth += 1
        elif text[offset] == "}":
            depth -= 1
            if depth == 0:
                return text[opening : offset + 1]
    raise AssertionError(f"unterminated function: {signature}")


class SparseSwitchAccountingTests(unittest.TestCase):
    def test_dense_per_switch_cube_is_replaced_by_checked_sparse_state(self) -> None:
        for stem in ("switch-node", "nvswitch-node"):
            header = (P2P / f"{stem}.h").read_text(encoding="utf-8")
            source = (P2P / f"{stem}.cc").read_text(encoding="utf-8")
            self.assertNotRegex(
                header, r"m_bytes\s*\[\s*pCnt\s*\]\s*\[\s*pCnt\s*\]"
            )
            self.assertRegex(
                header, r"std::map\s*<\s*uint64_t\s*,\s*uint32_t\s*>\s+m_bytes"
            )
            self.assertIn("QueueByteKey", source)
            self.assertIn("static_cast<uint64_t>(inDev)", source)
            self.assertIn("inDev < pCnt", source)
            self.assertIn("outDev < pCnt", source)
            self.assertIn("qIndex < qCnt", source)
            self.assertIn("std::numeric_limits<uint32_t>::max() - bytes", source)
            self.assertIn("it->second >= bytes", source)
            self.assertIn("if (it->second == 0)", source)
            self.assertIn("m_bytes.erase(it)", source)
            self.assertEqual(
                len(re.findall(r"\bm_bytes\s*\[", source)),
                1,
                f"{stem} must index sparse state only inside AddQueuedBytes",
            )
            owner = "NVSwitchNode" if stem.startswith("nv") else "SwitchNode"
            send = body(source, f"void {owner}::SendToDev(")
            admission = body(send, "if (qIndex != 0)")
            self.assertIn("AddQueuedBytes", admission)
            self.assertNotIn(
                "AddQueuedBytes",
                send[send.index(admission) + len(admission) :],
            )


class CompletedQpReclamationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.group_h = (P2P / "rdma-queue-pair.h").read_text(encoding="utf-8")
        cls.group_cc = (P2P / "rdma-queue-pair.cc").read_text(encoding="utf-8")
        cls.qbb_h = (P2P / "qbb-net-device.h").read_text(encoding="utf-8")
        cls.qbb_cc = (P2P / "qbb-net-device.cc").read_text(encoding="utf-8")
        cls.hw_cc = (P2P / "rdma-hw.cc").read_text(encoding="utf-8")

    def test_group_erases_the_exact_completed_pointer(self) -> None:
        self.assertIn("bool RemoveQp(Ptr<RdmaQueuePair> qp", self.group_h)
        remove = body(
            self.group_cc,
            "bool RdmaQueuePairGroup::RemoveQp(Ptr<RdmaQueuePair> qp",
        )
        self.assertIn("std::find", remove)
        self.assertIn("m_qps.erase(it)", remove)
        self.assertIn("*removedIndex", remove)

    def test_egress_removal_repairs_both_scheduler_indices(self) -> None:
        self.assertIn("bool RemoveQp(Ptr<RdmaQueuePair> qp)", self.qbb_h)
        remove = body(
            self.qbb_cc, "bool RdmaEgressQueue::RemoveQp(Ptr<RdmaQueuePair> qp)"
        )
        for contract in (
            "removedIndex < oldLast",
            "removedIndex == oldLast",
            "m_rrlast = 0",
            "m_qlast = -1",
            "oldQueue > removedIndex",
        ):
            self.assertIn(contract, remove)
        scheduler = body(self.qbb_cc, "int RdmaEgressQueue::GetNextQindex(")
        self.assertNotIn("qps.resize", scheduler)
        self.assertNotIn("min_finish_id", scheduler)

    def test_delete_searches_every_rail_and_requires_one_binding(self) -> None:
        delete = body(self.hw_cc, "void RdmaHw::DeleteQueuePair(")
        self.assertIn("for (uint32_t i = 0; i < m_nic.size(); ++i)", delete)
        self.assertIn("m_nic[i].dev->RemoveQp(qp)", delete)
        self.assertIn("removedGroups == 1", delete)
        self.assertNotIn("explicitCriticalSection", delete)
        self.assertLess(delete.index("RemoveQp(qp)"), delete.index("m_qpMap.erase"))

    def test_cursor_formula_keeps_the_old_successor_next(self) -> None:
        for old_count in range(1, 9):
            for old_last in range(old_count):
                for removed in range(old_count):
                    old_order = list(range(old_count))
                    expected = old_order[(removed + 1) % old_count]
                    remaining = [value for value in old_order if value != removed]
                    if not remaining:
                        new_last = 0
                        self.assertEqual(old_count, 1)
                        continue
                    if removed < old_last:
                        new_last = old_last - 1
                    elif removed == old_last:
                        new_last = (removed + len(remaining) - 1) % len(remaining)
                    else:
                        new_last = old_last
                    actual = remaining[(new_last + 1) % len(remaining)]
                    if removed == old_last:
                        self.assertEqual(actual, expected)


class CompletedApplicationReclamationTests(unittest.TestCase):
    def test_completed_clients_release_node_ownership_after_callback(self) -> None:
        header = (APPS / "rdma-client.h").read_text(encoding="utf-8")
        source = (APPS / "rdma-client.cc").read_text(encoding="utf-8")
        self.assertIn("bool m_cleanupScheduled", header)
        finish = body(source, "void RdmaClient::Finish()")
        self.assertIn("Simulator::ScheduleWithContext", finish)
        self.assertIn("Ptr<RdmaClient>(this)", finish)
        release = body(source, "void RdmaClient::ReleaseCompletedApplication(")
        self.assertLess(
            release.index("node->DeleteApplication(client)"),
            release.index("client->Dispose()"),
        )
        dispose = body(source, "void RdmaClient::DoDispose(")
        self.assertIn("msg_handler = nullptr", dispose)
        self.assertIn("fun_arg = nullptr", dispose)


class StreamingTelemetryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.telemetry = TELEMETRY.read_text(encoding="utf-8")

    def test_all_nine_csvs_use_bounded_streams_not_horizon_buffers(self) -> None:
        names = (
            "switch",
            "nic",
            "coll",
            "alarm",
            "recovery",
            "rdma",
            "collective_tx",
            "fault_application",
            "lifecycle",
        )
        for name in names:
            self.assertIn(f"FILE* {name}_stream_ = nullptr", self.telemetry)
            self.assertNotIn(f"{name}_buffer_", self.telemetry)
            self.assertIn(f"FlushCsv({name}_stream_", self.telemetry)
            self.assertIn(f"CloseCsv({name}_stream_", self.telemetry)

    def test_streams_are_bounded_ordered_and_fail_closed(self) -> None:
        open_csv = body(self.telemetry, "static FILE* OpenCsv(")
        self.assertIn("kCsvBufferBytes = 64 * 1024", self.telemetry)
        self.assertIn("std::setvbuf", open_csv)
        self.assertIn("std::fflush", open_csv)
        append = body(self.telemetry, "static void AppendRow(FILE* out")
        self.assertIn("std::string row", append)
        self.assertEqual(append.count("std::fwrite"), 1)
        failure = body(self.telemetry, "static void CsvFailure(")
        self.assertIn("std::exit(2)", failure)

    def test_link_map_is_also_checked_and_closed(self) -> None:
        write_map = body(self.telemetry, "void WriteLinkMapCsv()")
        self.assertIn("OpenCsv(path", write_map)
        self.assertIn("AppendRow(f", write_map)
        self.assertIn("FlushCsv(f, path)", write_map)
        self.assertIn("CloseCsv(f, path)", write_map)


class MonotonicUsageTrackerTests(unittest.TestCase):
    def test_source_clamps_one_observation_and_report_is_observational(self) -> None:
        source = USAGE_TRACKER.read_text(encoding="utf-8")
        header = USAGE_TRACKER_HEADER.read_text(encoding="utf-8")
        self.assertIn("clock_regression_count", header)
        self.assertIn("max_clock_regression", header)
        self.assertIn("kMaxToleratedClockRegressionTicks", header)
        clamp = body(source, "Tick UsageTracker::clamp_observed_tick(")
        self.assertIn("observed_tick < last_tick", clamp)
        self.assertIn("regression > kMaxToleratedClockRegressionTicks", clamp)
        self.assertIn("std::abort()", clamp)
        self.assertIn("return last_tick", clamp)
        for signature in (
            "void UsageTracker::increase_usage()",
            "void UsageTracker::decrease_usage()",
            "void UsageTracker::set_usage(int level)",
        ):
            transition = body(source, signature)
            self.assertLessEqual(transition.count("Sys::boostedTick()"), 1)
            self.assertIn("record_level_until(Sys::boostedTick())", transition)
        report = body(
            source,
            "std::list<std::pair<uint64_t, double>> "
            "UsageTracker::report_percentage_at(",
        )
        self.assertNotIn("increase_usage", report)
        self.assertNotIn("decrease_usage", report)
        self.assertIn("assert(interval.start <= interval.end)", report)
        self.assertIn("assert(previous_end <= interval.start)", report)
        workload = WORKLOAD.read_text(encoding="utf-8")
        self.assertIn('"UsageTracker clock audit: rank="', workload)
        self.assertIn("tracker.clock_regression_count", workload)
        self.assertIn("tracker.max_clock_regression", workload)

    def test_compiled_boundary_regression_and_fail_closed_cases(self) -> None:
        fixture = ROOT / "limer" / "tests" / "cpp" / "usage_tracker_regression.cc"
        usage = ASTRA / "system" / "Usage.cc"
        with tempfile.TemporaryDirectory(prefix="limer-usage-tracker-") as tmp:
            executable = Path(tmp) / "usage_tracker_regression"
            compile_result = subprocess.run(
                [
                    "g++",
                    "-std=c++17",
                    "-ffunction-sections",
                    "-fdata-sections",
                    f"-I{ROOT / 'astra-sim-alibabacloud'}",
                    str(fixture),
                    str(USAGE_TRACKER),
                    str(usage),
                    "-Wl,--gc-sections",
                    "-o",
                    str(executable),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(compile_result.returncode, 0, compile_result.stderr)
            subprocess.run([str(executable)], check=True)
            invalid = subprocess.run(
                [str(executable), "invalid-reversed-interval"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(invalid.returncode, 0)
            self.assertIn("interval.start <= interval.end", invalid.stderr)
            oversized = subprocess.run(
                [str(executable), "oversized-clock-regression"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(oversized.returncode, 0)
            self.assertIn(
                "clock regression exceeds bounded MTP tolerance",
                oversized.stderr,
            )


if __name__ == "__main__":
    unittest.main()
