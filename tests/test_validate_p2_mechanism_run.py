#!/usr/bin/env python3
"""Synthetic regression tests for per-run P2 mechanism validation."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
sys.path.insert(0, str(TOOLS))

import evaluate_stage_p2 as p2  # noqa: E402
import validate_p2_mechanism_run as validator  # noqa: E402


class MechanismFixture:
    RUN_ID = "synthetic-p2-run"
    TARGET = "L0-24"
    ONSET_NS = 100
    END_NS = 120

    def __init__(self, root: Path, mode: str):
        self.root = root
        self.mode = mode
        self.schedule = root / "simulator_injection_schedule.csv"
        self.root.mkdir(parents=True, exist_ok=True)
        self.write()

    @staticmethod
    def _write_csv(path: Path, rows):
        rows = list(rows)
        if not rows:
            raise AssertionError("fixture rows cannot be empty")
        with path.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def write(self):
        family = "random_loss" if self.mode == "loss" else "service_degradation"
        mechanism = (
            "rate_error_model_true_drop"
            if self.mode == "loss" else "carrier_up_service_rate"
        )
        rng = "123:124" if self.mode == "loss" else ""
        self._write_csv(self.root / "fault_application_telemetry.csv", [
            {
                "run_id": self.RUN_ID,
                "fault_id": "event-1",
                "fault_type": family,
                "target_link_id": self.TARGET,
                "transition": "apply",
                "scheduled_ns": self.ONSET_NS,
                "actual_ns": self.ONSET_NS,
                "parameter_before": "0",
                "parameter_after": "0.01" if self.mode == "loss" else "0.5",
                "mechanism": mechanism,
                "rng_stream": rng,
                "status": "APPLIED",
            },
            {
                "run_id": self.RUN_ID,
                "fault_id": "event-1",
                "fault_type": family,
                "target_link_id": self.TARGET,
                "transition": "revert",
                "scheduled_ns": self.END_NS,
                "actual_ns": self.END_NS,
                "parameter_before": "0",
                "parameter_after": "0.01" if self.mode == "loss" else "0.5",
                "mechanism": "restore_baseline",
                "rng_stream": "",
                "status": "REVERTED",
            },
        ])
        self._write_csv(self.schedule, [{
            "fault_id": "event-1",
            "fault_type": family,
            "target_link_id": self.TARGET,
            "start_time_ns": self.ONSET_NS,
            "end_time_ns": self.END_NS,
            "parameter_before": "0",
            "parameter_after": "0.01" if self.mode == "loss" else "0.5",
        }])

        switch_rows = []
        nic_rows = []
        for index, timestamp in enumerate((90, 95, 100, 105, 110)):
            during = timestamp >= self.ONSET_NS
            counter = max(0, index - 2) if self.mode == "loss" else 0
            queue = (index - 1) * 100 if during and self.mode == "service" else 0
            throughput = 700 if during and self.mode == "service" else 1000
            tx_bytes = (index + 1) * 1000
            common_switch = {
                "run_id": self.RUN_ID,
                "timestamp_ns": timestamp,
                "switch_id": 24,
                "port_id": 1,
                "link_id": self.TARGET,
                "configured_bandwidth_bps": 100_000_000_000,
                "link_state": "up",
                "link_errors": counter,
                "flap_count": 0,
                "last_link_down_ns": 0,
                "last_link_up_ns": 0,
                "cumulative_link_down_ns": 0,
            }
            switch_rows.append({
                **common_switch,
                "direction": "tx",
                "tx_bytes": tx_bytes,
                "queue_bytes": queue,
                "observed_throughput_bps": throughput,
                "dropped_packets": 0,
                "recovered_packets": "",
            })
            switch_rows.append({
                **common_switch,
                "direction": "rx",
                "tx_bytes": "",
                "queue_bytes": "",
                "observed_throughput_bps": "",
                "dropped_packets": counter,
                "recovered_packets": 0,
            })
            nic_rows.append({
                "run_id": self.RUN_ID,
                "timestamp_ns": timestamp,
                "node_id": 0,
                "nic_id": 3,
                "link_id": self.TARGET,
                "configured_bandwidth_bps": 100_000_000_000,
                "link_state": "up",
                "tx_bytes": tx_bytes,
                "queue_bytes": queue,
                "effective_throughput_bps": throughput,
                "rx_dropped_packets": counter,
                "link_errors": counter,
                "recovered_packets": 0,
                "flap_count": 0,
                "last_link_down_ns": 0,
                "last_link_up_ns": 0,
                "cumulative_link_down_ns": 0,
            })
        self._write_csv(self.root / "switch_telemetry.csv", switch_rows)
        self._write_csv(self.root / "nic_telemetry.csv", nic_rows)

        self._write_csv(self.root / "collective_telemetry.csv", [
            {
                "run_id": self.RUN_ID, "start_time_ns": 80,
                "finish_time_ns": 85, "duration_ns": 5, "status": "ok",
            },
            {
                "run_id": self.RUN_ID, "start_time_ns": 90,
                "finish_time_ns": 95, "duration_ns": 5, "status": "ok",
            },
            {
                "run_id": self.RUN_ID, "start_time_ns": 101,
                "finish_time_ns": 116,
                "duration_ns": 15 if self.mode == "service" else 5,
                "status": "ok",
            },
            {
                "run_id": self.RUN_ID, "start_time_ns": 106,
                "finish_time_ns": 121,
                "duration_ns": 15 if self.mode == "service" else 5,
                "status": "ok",
            },
        ])

    def validate(self):
        return validator.validate_run(
            self.root,
            "random_loss" if self.mode == "loss" else "service_degradation",
            "rate_error_model_true_drop" if self.mode == "loss"
            else "egress_service_fraction",
            self.TARGET,
            self.ONSET_NS,
            injection_schedule=self.schedule,
            require_injection_schedule=True,
        )


class PhysicalMechanismFixture:
    RUN_ID = "synthetic-physical-run"
    TARGET = "L0-24"
    ONSET_NS = 100

    def __init__(self, root: Path, mode: str, *, inter_switch: bool = False):
        self.root = root
        self.mode = mode
        self.inter_switch = inter_switch
        self.root.mkdir(parents=True, exist_ok=True)
        self.schedule = self.root / "inputs" / "simulator_injection_schedule.csv"
        self.schedule.parent.mkdir()
        self._write()

    @staticmethod
    def write_csv(path: Path, rows, fieldnames=None):
        rows = list(rows)
        names = list(fieldnames or (rows[0] if rows else []))
        with path.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.DictWriter(destination, fieldnames=names)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _application_row(
        *, fault_id, fault_type, transition, scheduled, mechanism, status,
        before, after,
    ):
        return {
            "run_id": PhysicalMechanismFixture.RUN_ID,
            "fault_id": fault_id,
            "fault_type": fault_type,
            "target_link_id": PhysicalMechanismFixture.TARGET,
            "transition": transition,
            "scheduled_ns": scheduled,
            "actual_ns": scheduled,
            "parameter_before": before,
            "parameter_after": after,
            "mechanism": mechanism,
            "rng_stream": "",
            "status": status,
        }

    @staticmethod
    def _switch_row(
        timestamp, *, switch_id=24, port_id=1, state="up", rate=100_000,
        throughput=1_000, tx_bytes=1_000, flaps=0, down=0, up=0,
        cumulative=0, link_id=TARGET,
    ):
        return {
            "run_id": PhysicalMechanismFixture.RUN_ID,
            "timestamp_ns": timestamp,
            "switch_id": switch_id,
            "port_id": port_id,
            "link_id": link_id,
            "direction": "tx",
            "configured_bandwidth_bps": rate,
            "link_state": state,
            "tx_bytes": tx_bytes,
            "queue_bytes": 0,
            "observed_throughput_bps": throughput,
            "dropped_packets": 0,
            "link_errors": 0,
            "recovered_packets": 0,
            "flap_count": flaps,
            "last_link_down_ns": down,
            "last_link_up_ns": up,
            "cumulative_link_down_ns": cumulative,
        }

    @staticmethod
    def _nic_row(
        timestamp, *, state="up", rate=100_000, throughput=1_000,
        tx_bytes=1_000, flaps=0, down=0, up=0, cumulative=0,
        link_id=TARGET,
    ):
        return {
            "run_id": PhysicalMechanismFixture.RUN_ID,
            "timestamp_ns": timestamp,
            "node_id": 0,
            "nic_id": 3,
            "link_id": link_id,
            "configured_bandwidth_bps": rate,
            "link_state": state,
            "tx_bytes": tx_bytes,
            "queue_bytes": 0,
            "effective_throughput_bps": throughput,
            "rx_dropped_packets": 0,
            "link_errors": 0,
            "recovered_packets": 0,
            "flap_count": flaps,
            "last_link_down_ns": down,
            "last_link_up_ns": up,
            "cumulative_link_down_ns": cumulative,
        }

    def _write(self):
        application = []
        schedule = []
        switch_rows = []
        nic_rows = []
        if self.mode == "healthy":
            switch_rows = [self._switch_row(0), self._switch_row(1)]
            nic_rows = [self._nic_row(0), self._nic_row(1)]
        elif self.mode in {"hard", "flap"}:
            fault_type = "hard_disconnect" if self.mode == "hard" else "carrier_flap"
            end = 120 if self.mode == "hard" else 101
            before, after = ("up", "physically_disconnected") \
                if self.mode == "hard" else ("up", "down")
            apply_mechanism = (
                "physical_link_down" if self.mode == "hard"
                else "physical_carrier_down_channel_epoch"
            )
            schedule = [{
                "fault_id": "fault-0", "fault_type": fault_type,
                "target_link_id": self.TARGET, "start_time_ns": self.ONSET_NS,
                "end_time_ns": end, "parameter_before": before,
                "parameter_after": after,
            }]
            application.append(self._application_row(
                fault_id="fault-0", fault_type=fault_type,
                transition="apply", scheduled=self.ONSET_NS,
                mechanism=apply_mechanism, status="APPLIED",
                before=before, after=after,
            ))
            switch_rows.append(self._switch_row(90))
            nic_rows.append(self._nic_row(90))
            if self.mode == "hard":
                for timestamp in (100, 110):
                    switch_rows.append(self._switch_row(
                        timestamp, state="down", throughput=0, flaps=1,
                        down=100, cumulative=timestamp - 100,
                    ))
                    nic_rows.append(self._nic_row(
                        timestamp, state="down", throughput=0, flaps=1,
                        down=100, cumulative=timestamp - 100,
                    ))
            else:
                application.append(self._application_row(
                    fault_id="fault-0", fault_type=fault_type,
                    transition="revert", scheduled=end,
                    mechanism="physical_carrier_up_no_fib_or_qp_change",
                    status="REVERTED", before=before, after=after,
                ))
                # No sample falls in [100, 101); the latched event epochs are
                # the only faithful proof of this sub-snapshot carrier flap.
                switch_rows.append(self._switch_row(
                    110, flaps=2, down=100, up=101, cumulative=1,
                ))
                nic_rows.append(self._nic_row(
                    110, flaps=2, down=100, up=101, cumulative=1,
                ))
        else:
            pulse = self.mode == "capacity_pulses"
            intervals = [(100, 110), (120, 130)] if pulse else [(100, 120)]
            for index, (start, end) in enumerate(intervals):
                fault_id = f"capacity-{index}"
                schedule.append({
                    "fault_id": fault_id,
                    "fault_type": "bandwidth_degradation",
                    "target_link_id": self.TARGET,
                    "start_time_ns": start, "end_time_ns": end,
                    "parameter_before": "100Kbps",
                    "parameter_after": "50Kbps",
                })
                application.extend([
                    self._application_row(
                        fault_id=fault_id, fault_type="bandwidth_degradation",
                        transition="apply", scheduled=start,
                        mechanism="dual_endpoint_data_rate", status="APPLIED",
                        before="100Kbps", after="50Kbps",
                    ),
                    self._application_row(
                        fault_id=fault_id, fault_type="bandwidth_degradation",
                        transition="revert", scheduled=end,
                        mechanism="restore_baseline", status="REVERTED",
                        before="100Kbps", after="50Kbps",
                    ),
                ])
            samples = [(90, 100_000, 1_000), (95, 100_000, 1_000)]
            samples.extend((timestamp, 50_000, 400) for timestamp in (100, 105))
            if pulse:
                samples.append((110, 100_000, 1_000))
                samples.extend((timestamp, 50_000, 400) for timestamp in (120, 125))
                samples.append((130, 100_000, 1_000))
            else:
                samples.append((120, 100_000, 1_000))
            for index, (timestamp, rate, throughput) in enumerate(samples):
                switch_rows.append(self._switch_row(
                    timestamp, rate=rate, throughput=throughput,
                    tx_bytes=(index + 1) * 1_000,
                ))
                if self.inter_switch:
                    switch_rows.append(self._switch_row(
                        timestamp, switch_id=25, port_id=2, rate=rate,
                        throughput=throughput, tx_bytes=(index + 1) * 1_000,
                    ))
                else:
                    nic_rows.append(self._nic_row(
                        timestamp, rate=rate, throughput=throughput,
                        tx_bytes=(index + 1) * 1_000,
                    ))
            if self.inter_switch:
                nic_rows = [self._nic_row(0, link_id="L9-99")]

        application_fields = list(validator.COMMON_REQUIRED["fault_application"])
        self.write_csv(
            self.root / "fault_application_telemetry.csv", application,
            application_fields,
        )
        if schedule:
            self.write_csv(self.schedule, schedule)
        self.write_csv(self.root / "switch_telemetry.csv", switch_rows)
        self.write_csv(self.root / "nic_telemetry.csv", nic_rows)
        self.write_csv(self.root / "collective_telemetry.csv", [{
            "run_id": self.RUN_ID, "start_time_ns": 1,
            "finish_time_ns": 2, "duration_ns": 1, "status": "ok",
        }])

    def validate(self):
        family = {
            "healthy": "",
            "hard": "hard_disconnect",
            "flap": "carrier_flap",
            "capacity": "bandwidth_degradation",
            "capacity_pulses": "intermittent_service",
        }[self.mode]
        mechanism = {
            "healthy": "no_physical_injection",
            "hard": "physical_link_down",
            "flap": "physical_carrier_flap_channel_epoch",
            "capacity": "dual_endpoint_data_rate",
            "capacity_pulses": "dual_endpoint_data_rate_pulses",
        }[self.mode]
        return validator.validate_run(
            self.root, family, mechanism,
            None if self.mode == "healthy" else self.TARGET,
            0 if self.mode == "healthy" else self.ONSET_NS,
            injection_schedule=(None if self.mode == "healthy" else self.schedule),
            require_injection_schedule=self.mode != "healthy",
        )


class ValidateP2MechanismRunTest(unittest.TestCase):
    def test_healthy_requires_empty_sidecar_and_all_carriers_up(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = PhysicalMechanismFixture(Path(temporary), "healthy")
            self.assertEqual(fixture.validate()["status"], "PASS")

            path = fixture.root / "nic_telemetry.csv"
            with path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            rows[-1]["link_state"] = "down"
            fixture.write_csv(path, rows)
            report = fixture.validate()
            self.assertEqual(report["status"], "FAIL")
            self.assertIn(
                "healthy_no_fault_raw_semantics",
                {item["name"] for item in report["checks"]
                 if item["status"] == "FAIL"},
            )

    def test_hard_disconnect_requires_both_endpoint_carrier_epochs(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = PhysicalMechanismFixture(Path(temporary), "hard")
            report = fixture.validate()
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["target_link_state_during_event"], "down")
            self.assertEqual(report["observed_effects"], ["carrier_down"])

            path = fixture.root / "nic_telemetry.csv"
            with path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            for row in rows:
                row["flap_count"] = "0"
                row["last_link_down_ns"] = "0"
            fixture.write_csv(path, rows)
            report = fixture.validate()
            self.assertEqual(report["status"], "FAIL")
            self.assertIn(
                "physical_carrier_epoch_at_both_endpoints",
                {item["name"] for item in report["checks"]
                 if item["status"] == "FAIL"},
            )

    def test_sub_snapshot_carrier_flap_uses_latched_down_up_epochs(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = PhysicalMechanismFixture(Path(temporary), "flap")
            report = fixture.validate()
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["observed_effects"], ["carrier_down", "carrier_up"])
            self.assertEqual(report["target_link_state_during_event"], "down_up")

            path = fixture.root / "switch_telemetry.csv"
            with path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            rows[-1]["last_link_up_ns"] = "0"
            fixture.write_csv(path, rows)
            self.assertEqual(fixture.validate()["status"], "FAIL")

    def test_capacity_step_requires_dual_endpoint_rate_and_throughput_effect(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = PhysicalMechanismFixture(Path(temporary), "capacity")
            report = fixture.validate()
            self.assertEqual(report["status"], "PASS")
            self.assertIn("throughput_degradation", report["observed_effects"])

            path = fixture.root / "nic_telemetry.csv"
            with path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            for row in rows:
                if 100 <= int(row["timestamp_ns"]) < 120:
                    row["configured_bandwidth_bps"] = "100000"
                    row["effective_throughput_bps"] = "1000"
            fixture.write_csv(path, rows)
            self.assertEqual(fixture.validate()["status"], "FAIL")

    def test_capacity_pulses_and_inter_switch_endpoints_are_supported(self):
        for mode, inter_switch in (
            ("capacity_pulses", False), ("capacity", True),
        ):
            with self.subTest(mode=mode, inter_switch=inter_switch), \
                    tempfile.TemporaryDirectory() as temporary:
                fixture = PhysicalMechanismFixture(
                    Path(temporary), mode, inter_switch=inter_switch,
                )
                report = fixture.validate()
                self.assertEqual(report["status"], "PASS")
                capacity = report["evidence"]["capacity"]
                self.assertTrue(all(
                    segment["physical_endpoint_count"] == 2
                    for segment in capacity["segments"]
                ))

    def test_frozen_injector_sidecar_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = PhysicalMechanismFixture(Path(temporary), "capacity")
            path = fixture.root / "fault_application_telemetry.csv"
            with path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            rows[0]["parameter_after"] = "40Kbps"
            fixture.write_csv(path, rows)
            report = fixture.validate()
            self.assertEqual(report["status"], "FAIL")
            self.assertIn(
                "frozen_injector_to_application_sidecar",
                {item["name"] for item in report["checks"]
                 if item["status"] == "FAIL"},
            )

    def test_true_loss_pass_is_raw_bound_and_evaluator_compatible(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = MechanismFixture(Path(temporary), "loss")
            report = fixture.validate()

            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["schema_version"], "limer.p2-run-semantics.v1")
            self.assertEqual(report["run_id"], fixture.RUN_ID)
            self.assertEqual(report["target_link_state_during_event"], "up")
            self.assertEqual(report["packet_disposition"], "dropped")
            self.assertFalse(report["recoverable_error_proxy"])
            self.assertEqual(report["impairment"], "loss")
            self.assertIn("packet_drop", report["observed_effects"])
            self.assertEqual(
                report["source_artifact_sha256"],
                p2._sha256(fixture.root / "switch_telemetry.csv"),
            )
            self.assertTrue(all(check["status"] == "PASS"
                                for check in report["checks"]))
            run = {
                "run_id": fixture.RUN_ID,
                "class_label": "GRAY_FAULT",
                "fault_family": "random_loss",
                "mechanism": {"mechanism_id": "rate_error_model_true_drop"},
            }
            self.assertEqual(
                p2._semantic_validation_errors(
                    run, report, report["source_artifact_sha256"]),
                [],
            )

    def test_legacy_recovery_proxy_and_recovered_packets_fail_true_loss(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = MechanismFixture(Path(temporary), "loss")
            application = fixture.root / "fault_application_telemetry.csv"
            with application.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            rows[0]["mechanism"] = "legacy_recoverable_error_proxy"
            fixture._write_csv(application, rows)
            nic = fixture.root / "nic_telemetry.csv"
            with nic.open(newline="", encoding="utf-8") as source:
                nic_rows = list(csv.DictReader(source))
            nic_rows[-1]["recovered_packets"] = "1"
            fixture._write_csv(nic, nic_rows)

            report = fixture.validate()
            self.assertEqual(report["status"], "FAIL")
            self.assertNotEqual(report["packet_disposition"], "dropped")
            self.assertTrue(report["recoverable_error_proxy"])
            failed = {item["name"] for item in report["checks"]
                      if item["status"] == "FAIL"}
            self.assertIn("fault_application_ground_truth", failed)
            self.assertIn("no_recovered_packet_proxy", failed)

    def test_service_pass_requires_observed_causal_effect(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = MechanismFixture(Path(temporary), "service")
            report = fixture.validate()

            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["impairment"], "service")
            self.assertTrue(
                {"queue_growth", "throughput_degradation", "latency_growth"}
                & set(report["observed_effects"])
            )
            run = {
                "run_id": fixture.RUN_ID,
                "class_label": "GRAY_FAULT",
                "fault_family": "service_degradation",
                "mechanism": {"mechanism_id": "egress_service_fraction"},
            }
            self.assertEqual(
                p2._semantic_validation_errors(
                    run, report, report["source_artifact_sha256"]),
                [],
            )

    def test_service_insufficient_samples_fail_instead_of_guessing(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = MechanismFixture(Path(temporary), "service")
            for name in ("switch_telemetry.csv", "nic_telemetry.csv"):
                path = fixture.root / name
                with path.open(newline="", encoding="utf-8") as source:
                    rows = list(csv.DictReader(source))
                rows = [row for row in rows
                        if int(row["timestamp_ns"]) in {95, 100}]
                fixture._write_csv(path, rows)
            collective = fixture.root / "collective_telemetry.csv"
            with collective.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            fixture._write_csv(collective, [rows[0], rows[2]])

            report = fixture.validate()
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["observed_effects"], [])
            failed = {item["name"] for item in report["checks"]
                      if item["status"] == "FAIL"}
            self.assertIn("target_event_window_samples", failed)
            self.assertIn("carrier_up_service_causal_effect", failed)

    def test_cli_writes_report_and_returns_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = MechanismFixture(root / "run", "loss")
            output = root / "semantic_validation.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = validator.main([
                    "--run-dir", str(fixture.root),
                    "--fault-family", "random_loss",
                    "--mechanism-id", "rate_error_model_true_drop",
                    "--target-link", fixture.TARGET,
                    "--scheduled-onset-ns", str(fixture.ONSET_NS),
                    "--injection-schedule", str(fixture.schedule),
                    "--require-injection-schedule",
                    "--output", str(output),
                ])
            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"],
                             "PASS")


if __name__ == "__main__":
    unittest.main()
