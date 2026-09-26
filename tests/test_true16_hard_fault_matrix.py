#!/usr/bin/env python3
"""Regression tests for the P1 all-Plane-B hard-event matrix."""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


LIMER = Path(__file__).resolve().parents[1]
SIMAI = LIMER.parent
TOOLS = LIMER / "tools"
TRUE16 = LIMER / "results" / "true16_hard_fault_e2e"
HEALTHY = TRUE16 / "healthy"
FAULT = TRUE16 / "hard_disconnect"
TOPOLOGY = (
    TRUE16 / "topology" /
    "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
)

sys.path.insert(0, str(TOOLS))

import evaluate_true16_hard_fault_matrix as matrix  # noqa: E402


class PlaneBInventoryTest(unittest.TestCase):
    def test_true16_inventory_has_one_unique_host_port3_target_per_gpu(self):
        links, errors = matrix.expected_primary_links(
            HEALTHY / "link_map.csv", expected_gpus=16, primary_host_port=3
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(links), 16)
        self.assertEqual(len(set(links.values())), 16)
        self.assertEqual(links[0], "L0-24")
        self.assertEqual(links[15], "L15-27")

    def test_explicit_prepare_selects_requested_gpu_plane_b(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            schedule = root / "fault_events.csv"
            selection = root / "fault_selection.json"
            process = subprocess.run(
                [
                    sys.executable,
                    str(TOOLS / "prepare_true16_hard_fault.py"),
                    "--link-map", str(HEALTHY / "link_map.csv"),
                    "--healthy-nic", str(HEALTHY / "nic_telemetry.csv"),
                    "--gpu", "7",
                    "--host-port", "3",
                    "--start-ns", "10000",
                    "--out-csv", str(schedule),
                    "--out-json", str(selection),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            manifest = json.loads(selection.read_text(encoding="utf-8"))
            self.assertEqual(manifest["gpu"], 7)
            self.assertEqual(manifest["selected_host_port"], 3)
            self.assertEqual(manifest["selected_link_id"], "L7-27")
            with schedule.open(newline="", encoding="utf-8") as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(row["fault_id"], "true16_gpu7_hard_disconnect")
            self.assertEqual(row["target_link_id"], "L7-27")


class LongHealthyGateTest(unittest.TestCase):
    def _logs(self, root: Path) -> tuple[Path, Path]:
        monitoring_on = root / "on"
        monitoring_off = root / "off"
        monitoring_on.mkdir()
        monitoring_off.mkdir()
        for destination in (monitoring_on, monitoring_off):
            shutil.copy2(HEALTHY / "run.log", destination / "run.log")
            shutil.copy2(HEALTHY / "exit_code.txt", destination / "exit_code.txt")
        return monitoring_on, monitoring_off

    def test_synthetic_101_snapshot_span_and_exact_parity_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            monitoring_on, monitoring_off = self._logs(Path(temporary))
            synthetic_audit = {
                "status": "PASS",
                "summary": {"pass": 83, "fail": 0, "skip": 0},
                "runs": [{
                    "telemetry": {
                        "sample_count": 101,
                        "first_sample_ns": 1_000_000,
                        "last_sample_ns": 101_000_000,
                    }
                }],
            }
            with mock.patch.object(
                matrix.audit_true16_monitoring,
                "build_audit",
                return_value=synthetic_audit,
            ):
                result = matrix.validate_healthy_pair(
                    TOPOLOGY,
                    monitoring_on,
                    monitoring_off,
                    sample_interval_ns=1_000_000,
                    minimum_span_ns=100_000_000,
                )
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["sample_count"], 101)
            self.assertEqual(result["virtual_span_ns"], 100_000_000)

    def test_two_snapshot_existing_run_fails_long_health_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            monitoring_off = Path(temporary) / "off"
            monitoring_off.mkdir()
            shutil.copy2(HEALTHY / "run.log", monitoring_off / "run.log")
            shutil.copy2(HEALTHY / "exit_code.txt", monitoring_off / "exit_code.txt")
            result = matrix.validate_healthy_pair(
                TOPOLOGY,
                HEALTHY,
                monitoring_off,
                sample_interval_ns=1_000_000,
                minimum_span_ns=100_000_000,
            )
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(result["sample_count"], 2)
            self.assertEqual(result["virtual_span_ns"], 1_000_000)
            checks = {item["check"]: item["status"] for item in result["checks"]}
            self.assertEqual(checks["long_healthy_snapshot_span"], "FAIL")


class FastEventEvidenceTest(unittest.TestCase):
    def _copy_fast_fixture(self, gpu_dir: Path) -> Path:
        fault_dir = gpu_dir / "hard_disconnect"
        fault_dir.mkdir(parents=True)
        shutil.copy2(TRUE16 / "fault_events.csv", gpu_dir / "fault_events.csv")
        shutil.copy2(TRUE16 / "fault_selection.json", gpu_dir / "fault_selection.json")
        for name in (
            "link_map.csv", "collective_transaction.csv", "run.log", "exit_code.txt",
        ):
            (fault_dir / name).symlink_to(FAULT / name)
        return fault_dir

    def test_existing_gpu0_runtime_events_satisfy_fast_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            gpu_dir = Path(temporary) / "gpu_00"
            gpu_dir.mkdir()
            shutil.copy2(TRUE16 / "fault_events.csv", gpu_dir / "fault_events.csv")
            shutil.copy2(TRUE16 / "fault_selection.json", gpu_dir / "fault_selection.json")
            (gpu_dir / "hard_disconnect").symlink_to(FAULT, target_is_directory=True)
            row = matrix.validate_fast_target(
                gpu_dir, gpu=0, expected_link_id="L0-24",
                hard_detection_slo_ns=1_000_000,
            )
            self.assertEqual(row["status"], "PASS", row["reasons"])
            self.assertEqual(row["hard_detection_latency_ns"], 60_000)
            self.assertEqual(row["profile"], "fast_event")

    def test_synthetic_alarm_at_exact_deadline_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            gpu_dir = Path(temporary) / "gpu_00"
            fault_dir = self._copy_fast_fixture(gpu_dir)
            with (FAULT / "alarm_telemetry.csv").open(
                newline="", encoding="utf-8"
            ) as stream:
                rows = list(csv.DictReader(stream))
            rows[0]["consume_ns"] = str(int(rows[0]["physical_fault_ns"]) + 1_000_000)
            with (fault_dir / "alarm_telemetry.csv").open(
                "w", newline="", encoding="utf-8"
            ) as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
            row = matrix.validate_fast_target(
                gpu_dir, gpu=0, expected_link_id="L0-24",
                hard_detection_slo_ns=1_000_000,
            )
            self.assertEqual(row["status"], "FAIL")
            self.assertIn("latency=1000000", " ".join(row["reasons"]))

    def test_collateral_alarm_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            gpu_dir = Path(temporary) / "gpu_00"
            fault_dir = self._copy_fast_fixture(gpu_dir)
            with (FAULT / "alarm_telemetry.csv").open(
                newline="", encoding="utf-8"
            ) as stream:
                rows = list(csv.DictReader(stream))
            collateral = dict(rows[0])
            collateral["alarm_id"] = "collateral"
            collateral["link_id"] = "L1-25"
            with (fault_dir / "alarm_telemetry.csv").open(
                "w", newline="", encoding="utf-8"
            ) as stream:
                writer = csv.DictWriter(
                    stream, fieldnames=list(rows[0]), lineterminator="\n"
                )
                writer.writeheader()
                writer.writerows([rows[0], collateral])
            row = matrix.validate_fast_target(
                gpu_dir, gpu=0, expected_link_id="L0-24",
                hard_detection_slo_ns=1_000_000,
            )
            self.assertEqual(row["status"], "FAIL")
            self.assertIn("total_count=2", " ".join(row["reasons"]))


class AggregateOnlyCliTest(unittest.TestCase):
    def test_one_real_gpu_evidence_cannot_pass_16_target_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "matrix"
            topology_dir = root / "topology"
            healthy_on = root / "healthy" / "long" / "monitoring_on"
            healthy_off = root / "healthy" / "long" / "monitoring_off"
            gpu_dir = root / "gpu_00"
            topology_dir.mkdir(parents=True)
            healthy_off.mkdir(parents=True)
            gpu_dir.mkdir(parents=True)
            (topology_dir / TOPOLOGY.name).symlink_to(TOPOLOGY)
            healthy_on.symlink_to(HEALTHY, target_is_directory=True)
            shutil.copy2(HEALTHY / "run.log", healthy_off / "run.log")
            shutil.copy2(HEALTHY / "exit_code.txt", healthy_off / "exit_code.txt")
            shutil.copy2(TRUE16 / "fault_events.csv", gpu_dir / "fault_events.csv")
            shutil.copy2(TRUE16 / "fault_selection.json", gpu_dir / "fault_selection.json")
            (gpu_dir / "hard_disconnect").symlink_to(FAULT, target_is_directory=True)

            process = subprocess.run(
                [
                    "bash", str(LIMER / "scripts" / "run_true16_hard_fault_matrix.sh"),
                    "--out-root", str(root), "--aggregate-only",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(process.returncode, 1, process.stderr)
            report = json.loads((root / "stage_matrix.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["coverage"]["present_evaluation_count"], 1)
            self.assertEqual(report["coverage"]["passing_evaluation_count"], 1)
            self.assertEqual(report["coverage"]["missing_gpus"], list(range(1, 16)))
            self.assertEqual(report["target_matrix"][0]["status"], "PASS")
            self.assertEqual(report["healthy_parity"]["status"], "FAIL")
            self.assertEqual(report["healthy_parity"]["sample_count"], 2)


if __name__ == "__main__":
    unittest.main()
