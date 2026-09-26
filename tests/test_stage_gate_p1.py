#!/usr/bin/env python3
"""Regression tests for the final P1 evidence gate."""

from __future__ import annotations

import copy
import csv
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
RESULTS = LIMER / "results"
P0_GATE = RESULTS / "stage_gates" / "p0" / "stage_gate_p0.json"
MATRIX_ROOT = RESULTS / "true16_hard_fault_matrix"
MATRIX_REPORT = MATRIX_ROOT / "stage_matrix.json"
TOPOLOGY_VALIDATION = MATRIX_ROOT / "topology_validation.json"

sys.path.insert(0, str(TOOLS))

import finalize_stage_p1 as p1  # noqa: E402


class PlaneBTargetTest(unittest.TestCase):
    def test_long_healthy_link_map_has_exact_plane_b_inventory(self):
        link_map = (
            MATRIX_ROOT / "healthy" / "long" / "monitoring_on" / "link_map.csv"
        )
        targets, errors = p1.expected_plane_b_targets(link_map)
        self.assertEqual(errors, [])
        self.assertEqual(set(targets), set(range(16)))
        self.assertEqual(len(set(targets.values())), 16)
        self.assertEqual(targets[0], "L0-24")
        self.assertEqual(targets[15], "L15-27")


class RawAlarmEvidenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads(MATRIX_REPORT.read_text(encoding="utf-8"))
        cls.targets, _ = p1.expected_plane_b_targets(
            MATRIX_ROOT / "healthy" / "long" / "monitoring_on" / "link_map.csv"
        )

    def test_matrix_alarm_hash_mismatch_is_rejected(self):
        matrix = copy.deepcopy(self.matrix)
        matrix["target_matrix"][0]["evidence"]["alarm_telemetry.csv"]["sha256"] = "0" * 64
        rows = p1.build_hard_event_timeline(MATRIX_REPORT, matrix, self.targets)
        self.assertEqual(rows[0]["status"], "FAIL")
        self.assertFalse(rows[0]["raw_alarm_hash_match"])
        self.assertIn("hash", rows[0]["errors"])
        self.assertTrue(all(row["status"] == "PASS" for row in rows[1:]))

    def test_actionable_alarm_at_exact_1ms_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            gpu_dir = root / "gpu_00"
            fault_dir = gpu_dir / "hard_disconnect"
            fault_dir.mkdir(parents=True)
            shutil.copy2(MATRIX_ROOT / "gpu_00" / "fault_events.csv",
                         gpu_dir / "fault_events.csv")
            source_alarm = MATRIX_ROOT / "gpu_00" / "hard_disconnect" / "alarm_telemetry.csv"
            with source_alarm.open(newline="", encoding="utf-8") as source:
                alarm_rows = list(csv.DictReader(source))
            alarm_rows[0]["consume_ns"] = str(
                int(alarm_rows[0]["physical_fault_ns"]) + 1_000_000)
            alarm_rows[0]["end_to_end_alarm_ns"] = "1000000"
            alarm_path = fault_dir / "alarm_telemetry.csv"
            with alarm_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(
                    output, fieldnames=list(alarm_rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(alarm_rows)

            matrix = copy.deepcopy(self.matrix)
            matrix["target_matrix"] = [copy.deepcopy(matrix["target_matrix"][0])]
            row = matrix["target_matrix"][0]
            row["hard_detection_latency_ns"] = 1_000_000
            row["evidence"]["alarm_telemetry.csv"]["sha256"] = p1._sha256(alarm_path)
            timeline = p1.build_hard_event_timeline(
                root / "stage_matrix.json", matrix, {0: "L0-24"})
            self.assertEqual(timeline[0]["actionable_latency_ns"], 1_000_000)
            self.assertFalse(timeline[0]["strict_under_1ms"])
            self.assertEqual(timeline[0]["status"], "FAIL")


class StageGateSemanticsTest(unittest.TestCase):
    def test_skip_is_not_accepted_as_telemetry_pass(self):
        self.assertFalse(p1._all_pass_no_skip({
            "status": "PASS",
            "summary": {"pass": 1, "fail": 0, "skip": 1},
            "checks": [{"status": "PASS"}, {"status": "SKIP"}],
        }))

    def test_full_existing_evidence_materializes_contract_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            out_dir = Path(temporary) / "p1"
            gate = p1.finalize(
                P0_GATE, MATRIX_REPORT, TOPOLOGY_VALIDATION, out_dir)
            self.assertEqual(gate["status"], "PASS")
            self.assertEqual(gate["summary"], {"pass": 12, "fail": 0, "skip": 0})
            self.assertEqual(gate["next_stage"], "P2")
            expected = {
                "topology_validation.json", "telemetry_validation.json",
                "healthy_true16_summary.json", "hard_event_timeline.csv",
                "stage_gate_p1.json", "stage_gate_p1.md",
            }
            self.assertEqual({path.name for path in out_dir.iterdir()}, expected)

            healthy = json.loads(
                (out_dir / "healthy_true16_summary.json").read_text(encoding="utf-8"))
            telemetry = json.loads(
                (out_dir / "telemetry_validation.json").read_text(encoding="utf-8"))
            with (out_dir / "hard_event_timeline.csv").open(
                    newline="", encoding="utf-8") as source:
                timeline = list(csv.DictReader(source))
            self.assertEqual(healthy["sample_count"], 120)
            self.assertEqual(healthy["virtual_span_ns"], 119_000_000)
            self.assertEqual(healthy["monitoring_on_finish_ns"], 120_148_520)
            self.assertEqual(
                healthy["monitoring_on_finish_ns"],
                healthy["monitoring_off_finish_ns"],
            )
            self.assertEqual(telemetry["summary"], {
                "pass": 52, "fail": 0, "skip": 0,
            })
            self.assertEqual(len(timeline), 16)
            self.assertEqual({row["status"] for row in timeline}, {"PASS"})
            self.assertEqual(
                {int(row["actionable_latency_ns"]) for row in timeline}, {60_000})
            self.assertTrue(all(row["raw_alarm_hash_match"] == "True" for row in timeline))

    def test_p0_failure_blocks_p2_even_with_other_passes(self):
        with tempfile.TemporaryDirectory() as temporary:
            out_dir = Path(temporary) / "p1"
            gate = p1.finalize(
                P0_GATE, MATRIX_REPORT, TOPOLOGY_VALIDATION, out_dir)
            p0 = json.loads(P0_GATE.read_text(encoding="utf-8"))
            p0["status"] = "FAIL"
            matrix = json.loads(MATRIX_REPORT.read_text(encoding="utf-8"))
            topology = json.loads(
                (out_dir / "topology_validation.json").read_text(encoding="utf-8"))
            telemetry = json.loads(
                (out_dir / "telemetry_validation.json").read_text(encoding="utf-8"))
            healthy = json.loads(
                (out_dir / "healthy_true16_summary.json").read_text(encoding="utf-8"))
            with (out_dir / "hard_event_timeline.csv").open(
                    newline="", encoding="utf-8") as source:
                timeline = list(csv.DictReader(source))
            # Restore typed fields that CSV necessarily serializes.
            for row in timeline:
                row["gpu"] = int(row["gpu"])
                row["host_port"] = int(row["host_port"])
                row["actionable_latency_ns"] = int(row["actionable_latency_ns"])
                row["raw_alarm_hash_match"] = row["raw_alarm_hash_match"] == "True"
            blocked = p1.evaluate_stage_gate(
                p0, matrix, topology, telemetry, healthy, timeline,
                gate["inputs"], gate["artifacts"],
            )
            self.assertEqual(blocked["status"], "FAIL")
            self.assertIsNone(blocked["next_stage"])
            checks = {item["check"]: item["status"] for item in blocked["checks"]}
            self.assertEqual(checks["p0_prerequisite_passed"], "FAIL")


if __name__ == "__main__":
    unittest.main()
