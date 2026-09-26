#!/usr/bin/env python3
"""Small-topology tests for strict P2 workload runtime qualification."""

from __future__ import annotations

import csv
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIMER / "tools"))

import validate_p2_workload_runtime as runtime  # noqa: E402
import generate_true16_p2_corpus as corpus_generator  # noqa: E402
import generate_true16_traffic_workload as traffic_workload  # noqa: E402


class RuntimeFixture:
    def __init__(self, root: Path, *, fault: bool = False) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_id = "small-fault" if fault else "small-healthy"
        self.workload = root / "workload.txt"
        self.workload.write_text("small immutable test workload\n", encoding="utf-8")
        workload_sha = hashlib.sha256(self.workload.read_bytes()).hexdigest()
        self.report = {
            "status": "PASS",
            "qualification_profile": "horizon-prefix",
            "may_be_used_as_p2_horizon_workload": True,
            "world_size": 2,
            "layer_count": 3,
            "collective_bytes_per_layer": 64,
            "sha256": workload_sha,
            "duration_estimate": {
                "may_be_used_as_p2_horizon_workload": True,
                "corpus_max_virtual_finish_ns": 50,
            },
        }
        self.run = {
            "run_id": self.run_id,
            "run_role": "fault" if fault else "healthy",
            "class_label": "GRAY_FAULT" if fault else "HEALTHY",
            "virtual_start_ns": 0,
            "virtual_finish_ns": 50,
            "fault_scheduled_onset_ns": 30 if fault else None,
            "workload_sha256": workload_sha,
        }
        self.contract = runtime.RuntimeValidationContract(
            world_size=2,
            sample_interval_ns=10,
            max_first_full_start_ns=10,
            max_inter_full_start_gap_ns=20,
            max_tail_to_full_start_ns=20,
            max_inflight_collectives=1,
            max_traffic_silence_ns=20,
            causal_warmup_ns=20,
            expected_access_links_per_rank=2,
        )
        self.paths = runtime.RuntimePaths(
            workload=self.workload,
            link_map=root / "link_map.csv",
            switch_telemetry=root / "switch_telemetry.csv",
            nic_telemetry=root / "nic_telemetry.csv",
            collective_transaction=root / "collective_transaction.csv",
            collective_telemetry=root / "collective_telemetry.csv",
            run_lifecycle=root / "run_lifecycle.csv",
        )
        self.link_rows = [
            self._link("L0-2", 0, 2, 1, 1),
            self._link("L0-3", 0, 3, 2, 1),
            self._link("L1-2", 1, 2, 1, 2),
            self._link("L1-3", 1, 3, 2, 2),
        ]
        self.switch_rows = self._switch_rows()
        self.nic_rows = self._nic_rows()
        self.transaction_rows = self._transaction_rows()
        self.flow_rows = self._flow_rows()
        self.lifecycle_rows = [{
            "run_id": self.run_id,
            "event": "observation_horizon",
            "scheduled_ns": 50,
            "actual_ns": 50,
            "finished_ranks": 0,
            "world_size": 2,
            "status": "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE",
        }]
        self.write_all()

    @staticmethod
    def _link(link: str, host: int, switch: int, hp: int, sp: int) -> dict:
        return {
            "link_id": link, "src_node": host, "dst_node": switch,
            "src_type": "HOST", "dst_type": "SWITCH", "src_port": hp,
            "dst_port": sp, "link_class": "ACCESS",
            "bandwidth_bps": 100_000_000_000, "delay_ns": 1000,
        }

    @staticmethod
    def _blank(columns: tuple[str, ...]) -> dict:
        return {column: "" for column in columns}

    def _switch_rows(self) -> list[dict]:
        rows: list[dict] = []
        endpoints = [(2, 1, "L0-2"), (3, 1, "L0-3"),
                     (2, 2, "L1-2"), (3, 2, "L1-3")]
        for sample, timestamp in enumerate((10, 20, 30, 40), start=1):
            for node, port, link in endpoints:
                for direction in ("tx", "rx"):
                    row = self._blank(runtime.SWITCH_COLUMNS)
                    row.update({
                        "run_id": self.run_id, "timestamp_ns": timestamp,
                        "switch_id": node, "port_id": port, "link_id": link,
                        "peer_node_id": 0 if link.startswith("L0") else 1,
                        "direction": direction, "node_type": "SWITCH",
                        "link_state": "up", "configured_bandwidth_bps": 100,
                        "observed_throughput_bps": "0.000000",
                    })
                    for column in runtime.SWITCH_CUMULATIVE_COLUMNS:
                        row[column] = 0
                    if direction == "tx":
                        row["tx_packets"] = sample
                        row["tx_bytes"] = sample * 100
                        row["queue_packets"] = 0
                        row["queue_bytes"] = 0
                        row["max_queue_packets"] = 0
                        row["max_queue_bytes"] = 0
                    else:
                        row["tx_packets"] = ""
                        row["tx_bytes"] = ""
                        row["rx_packets"] = sample
                        row["rx_bytes"] = sample * 100
                    rows.append(row)
        return rows

    def _nic_rows(self) -> list[dict]:
        rows: list[dict] = []
        endpoints = [(0, 1, "L0-2"), (0, 2, "L0-3"),
                     (1, 1, "L1-2"), (1, 2, "L1-3")]
        for sample, timestamp in enumerate((10, 20, 30, 40), start=1):
            for rank, port, link in endpoints:
                row = self._blank(runtime.NIC_COLUMNS)
                row.update({
                    "run_id": self.run_id, "timestamp_ns": timestamp,
                    "node_id": rank, "rank_id": rank, "nic_id": port,
                    "link_id": link, "configured_bandwidth_bps": 100,
                    "link_state": "up", "queue_packets": 0,
                    "queue_bytes": 0, "max_queue_packets": 0,
                    "max_queue_bytes": 0,
                })
                for column in runtime.NIC_CUMULATIVE_COLUMNS:
                    row[column] = 0
                row["tx_packets"] = sample
                row["tx_bytes"] = sample * 100
                row["rx_packets"] = sample
                row["rx_bytes"] = sample * 100
                rows.append(row)
        return rows

    def _transaction_rows(self) -> list[dict]:
        rows: list[dict] = []
        for seq, (start, committed) in enumerate(((5, True), (20, True), (35, False))):
            for rank in range(2):
                rows.append({
                    "run_id": self.run_id, "timestamp_ns": start,
                    "collective_seq": seq, "attempt": 0, "layer_num": seq,
                    "message_size_bytes": 64, "event": "START",
                    "rank_id": rank, "world_size": 2,
                    "ready_ranks": rank + 1, "result_digest": "",
                    "status": "in_flight",
                })
            if committed:
                for rank in range(2):
                    rows.append({
                        "run_id": self.run_id, "timestamp_ns": start + 1,
                        "collective_seq": seq, "attempt": 0,
                        "layer_num": seq, "message_size_bytes": 64,
                        "event": "LOCAL_READY", "rank_id": rank,
                        "world_size": 2, "ready_ranks": rank + 1,
                        "result_digest": "", "status": "ready",
                    })
                rows.append({
                    "run_id": self.run_id, "timestamp_ns": start + 2,
                    "collective_seq": seq, "attempt": 0, "layer_num": seq,
                    "message_size_bytes": 64, "event": "COMMIT",
                    "rank_id": "", "world_size": 2, "ready_ranks": 2,
                    "result_digest": f"digest-{seq}", "status": "exactly_once",
                })
        return rows

    def _flow_rows(self) -> list[dict]:
        rows = []
        for rank in range(2):
            rows.append({
                "run_id": self.run_id, "collective_id": f"flow-{rank}",
                "iteration_id": "", "layer_id": "",
                "collective_type": "ALLREDUCE", "algorithm": "NcclFlowModel",
                "rank_id": rank, "world_size": 2,
                "message_size_bytes": 32, "start_time_ns": 5,
                "finish_time_ns": 7, "duration_ns": 2, "status": "ok",
            })
        return rows

    @staticmethod
    def _write_csv(path: Path, columns: tuple[str, ...], rows: list[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def write_all(self) -> None:
        self._write_csv(self.paths.link_map, runtime.LINK_MAP_COLUMNS, self.link_rows)
        self._write_csv(
            self.paths.switch_telemetry, runtime.SWITCH_COLUMNS, self.switch_rows
        )
        self._write_csv(self.paths.nic_telemetry, runtime.NIC_COLUMNS, self.nic_rows)
        self._write_csv(
            self.paths.collective_transaction,
            runtime.COLLECTIVE_TRANSACTION_COLUMNS,
            self.transaction_rows,
        )
        self._write_csv(
            self.paths.collective_telemetry,
            runtime.COLLECTIVE_FLOW_COLUMNS,
            self.flow_rows,
        )
        self._write_csv(
            self.paths.run_lifecycle, runtime.LIFECYCLE_COLUMNS,
            self.lifecycle_rows,
        )

    def validate(self) -> dict:
        return runtime.validate_runtime_qualification(
            run=self.run,
            workload_report=self.report,
            paths=self.paths,
            causal_warmup_ns=20,
            contract_override=self.contract,
        )


class P2WorkloadRuntimeQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = RuntimeFixture(Path(self.temporary.name))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def assert_failed(self, text: str) -> None:
        result = self.fixture.validate()
        self.assertEqual(result["status"], "FAIL")
        self.assertIn(text, "\n".join(result["errors"]))

    @staticmethod
    def _override_lifecycle_fixture(
        scenario: str,
    ) -> tuple[dict, dict, dict, dict, dict]:
        contract = corpus_generator.collective_override_contract(
            scenario,
            150_000_000,
            source_workload_sha256="0" * 64,
            aggregate_access_bandwidth_bps=200_000_000_000,
        )
        report = {"contract": contract}
        context = {
            "qualification_profile": contract["qualification_profile"],
            "horizon_ns": 520_000_000,
        }
        roles: list[dict] = []
        sequences: list[dict] = []
        layer = 0

        def append(role: str, start: int, commit: int) -> None:
            nonlocal layer
            roles.append({"layer_num": layer, "role": role})
            sequences.append({
                "layer_num": layer,
                "full_start_ns": start,
                "committed": True,
                "commit_ns": commit,
            })
            layer += 1

        append("PRE_BASELINE", 40_000_000, 40_500_000)
        append("PRE_BASELINE", 145_000_000, 145_500_000)
        if scenario == "allreduce_burst":
            for index in range(8):
                start = 150_000_000 + index * 100_000
                append("EVENT_BURST", start, start + 50_000)
            for start in (152_000_000, 200_000_000, 300_000_000, 345_000_000):
                append("EVENT_BASELINE", start, start + 500_000)
            append("POST_BASELINE", 355_000_000, 355_500_000)
        else:
            for index in range(int(contract["pressure_layer_count"])):
                start = 150_000_000 + index * 1_300_000
                append("EVENT_PRESSURE", start, start + 1_200_000)
            append("POST_BASELINE", 360_000_000, 360_500_000)
        append("POST_BASELINE", 515_000_000, 515_500_000)
        switch = {"access_throughput_series_by_endpoint": {}}
        for endpoint in range(32):
            switch["access_throughput_series_by_endpoint"][(
                endpoint, 1, f"L{endpoint}", "tx"
            )] = [
                (timestamp, 90.0, 100)
                for timestamp in range(170_000_000, 350_000_001, 20_000_000)
            ]
        return (
            {"rows": roles},
            {"sequence_evidence": sequences},
            switch,
            context,
            report,
        )

    def test_production_contract_and_true16_endpoint_counts_are_derived(self) -> None:
        workload = LIMER / "configs" / (
            "microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
        )
        report = traffic_workload.validate_workload(workload)
        contract = runtime._derive_contract(report, 100_000_000)
        self.assertEqual(contract.world_size, 16)
        self.assertEqual(contract.sample_interval_ns, 1_000_000)
        self.assertEqual(contract.max_inter_full_start_gap_ns, 2_000_000)
        planned = {
            "run_id": "production-shape",
            "class_label": "GRAY_FAULT",
            "run_role": "fault",
            "virtual_start_ns": 0,
            "virtual_finish_ns": 520_000_000,
            "fault_scheduled_onset_ns": 150_000_000,
            "workload_sha256": report["sha256"],
        }
        context = runtime._validate_context(planned, report, contract)
        self.assertEqual(context["cadence_end_ns"], 150_000_000)
        topology = runtime._read_link_map(
            LIMER / "results" / "true16_hard_fault_e2e" / "healthy"
            / "link_map.csv",
            contract,
        )
        self.assertEqual(topology["derived_switch_rows_per_snapshot"], 1120)
        self.assertEqual(topology["derived_host_rows_per_snapshot"], 48)

    def test_small_topology_healthy_end_to_end_passes_and_is_hash_bound(self) -> None:
        result = self.fixture.validate()
        self.assertEqual(result["status"], "PASS", result["errors"])
        json.dumps(result, sort_keys=True)
        self.assertEqual(result["contract"]["world_size"], 2)
        topology = result["evidence"]["physical_link_map_contract"]
        self.assertEqual(topology["derived_switch_rows_per_snapshot"], 8)
        self.assertEqual(topology["derived_host_rows_per_snapshot"], 4)
        self.assertEqual(
            result["source_artifacts"]["workload"]["sha256"],
            hashlib.sha256(self.fixture.workload.read_bytes()).hexdigest(),
        )

    def test_workload_bytes_must_match_static_report_hash(self) -> None:
        self.fixture.workload.write_text("tampered workload\n", encoding="utf-8")
        self.assert_failed("workload source hash differs")

    def test_fault_stall_after_onset_is_not_an_admission_failure(self) -> None:
        root = Path(self.temporary.name) / "fault"
        root.mkdir()
        fault = RuntimeFixture(root, fault=True)
        result = fault.validate()
        self.assertEqual(result["status"], "PASS", result["errors"])
        cadence = result["evidence"][
            "collective_transaction_state_machine_and_cadence"
        ]
        self.assertEqual(cadence["last_qualified_full_start_ns"], 20)

    def test_transaction_requires_all_start_ranks(self) -> None:
        self.fixture.transaction_rows = [
            row for row in self.fixture.transaction_rows
            if not (row["collective_seq"] == 1 and row["event"] == "START"
                    and row["rank_id"] == 1)
        ]
        self.fixture.write_all()
        self.assert_failed("exactly one START for all ranks")

    def test_transaction_rejects_sequence_gap_and_recovery_event(self) -> None:
        self.fixture.transaction_rows[0]["collective_seq"] = 9
        self.fixture.write_all()
        self.assert_failed("contiguous zero-based prefix")

    def test_transaction_rejects_abort(self) -> None:
        self.fixture.transaction_rows[0]["event"] = "ABORT"
        self.fixture.write_all()
        self.assert_failed("recovery event ABORT")

    def test_transaction_rejects_two_inflight_collectives(self) -> None:
        self.fixture.transaction_rows = [
            row for row in self.fixture.transaction_rows
            if not (row["collective_seq"] == 1
                    and row["event"] in {"LOCAL_READY", "COMMIT"})
        ]
        self.fixture.write_all()
        self.assert_failed("too many or non-terminal in-flight")

    def test_transaction_rejects_bad_commit_and_gap(self) -> None:
        commit = next(
            row for row in self.fixture.transaction_rows
            if row["collective_seq"] == 1 and row["event"] == "COMMIT"
        )
        commit["result_digest"] = ""
        self.fixture.write_all()
        self.assert_failed("COMMIT is invalid")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "gap")
        for row in self.fixture.transaction_rows:
            if row["collective_seq"] == 1:
                row["timestamp_ns"] = int(row["timestamp_ns"]) + 11
        self.fixture.write_all()
        self.assert_failed("cadence gap")

    def test_transaction_accepts_commit_clamped_after_reverse_lp_times(self) -> None:
        ready = [
            row for row in self.fixture.transaction_rows
            if row["collective_seq"] == 0 and row["event"] == "LOCAL_READY"
        ]
        commit = next(
            row for row in self.fixture.transaction_rows
            if row["collective_seq"] == 0 and row["event"] == "COMMIT"
        )
        # The first callback has the later LP-local clock; the second callback
        # is what triggers COMMIT.  Clamping to max(READY) is causal.
        ready[0]["timestamp_ns"] = 7
        ready[1]["timestamp_ns"] = 6
        commit["timestamp_ns"] = 7
        self.fixture.write_all()
        corrected = self.fixture.validate()
        self.assertEqual(corrected["status"], "PASS", corrected["errors"])

        # The old producer used the last callback's clock (6) directly.
        commit["timestamp_ns"] = 6
        self.fixture.write_all()
        self.assert_failed("sequence 0 COMMIT is non-causal")

    def test_snapshot_missing_duplicate_foreign_and_rollback_fail(self) -> None:
        self.fixture.switch_rows.pop()
        self.fixture.write_all()
        self.assert_failed("key mismatch")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "duplicate")
        self.fixture.nic_rows.append(dict(self.fixture.nic_rows[-1]))
        self.fixture.write_all()
        self.assert_failed("duplicate NIC endpoint")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "foreign")
        self.fixture.switch_rows[0]["link_id"] = "foreign"
        self.fixture.write_all()
        self.assert_failed("foreign switch endpoint")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "rollback")
        rows = [row for row in self.fixture.nic_rows
                if row["node_id"] == 0 and row["nic_id"] == 1]
        rows[-1]["tx_bytes"] = 1
        self.fixture.write_all()
        self.assert_failed("counter rollback")

    def test_switch_observed_throughput_accepts_finite_nonnegative_decimal(
        self,
    ) -> None:
        self.fixture.switch_rows[0]["observed_throughput_bps"] = "12.500000"
        self.fixture.write_all()
        result = self.fixture.validate()
        self.assertEqual(result["status"], "PASS", result["errors"])
        json.dumps(result, allow_nan=False, sort_keys=True)

        for index, invalid in enumerate(("-0.000001", "NaN", "Inf", "")):
            with self.subTest(value=invalid):
                self.fixture = RuntimeFixture(
                    Path(self.temporary.name) / f"bad-throughput-{index}"
                )
                self.fixture.switch_rows[0]["observed_throughput_bps"] = invalid
                self.fixture.write_all()
                result = self.fixture.validate()
                self.assertEqual(result["status"], "FAIL")
                errors = "\n".join(result["errors"])
                self.assertIn("switch observed_throughput_bps", errors)

    def test_per_rank_and_switch_silence_fail(self) -> None:
        for row in self.fixture.nic_rows:
            if row["node_id"] == 1:
                row["tx_bytes"] = 0
        self.fixture.write_all()
        self.assert_failed("rank 1 has no positive")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "switch-silent")
        for row in self.fixture.switch_rows:
            if row["direction"] == "tx":
                row["tx_bytes"] = 0
        self.fixture.write_all()
        self.assert_failed("switch fabric has no positive")

    def test_flow_is_only_causal_completed_sender_evidence(self) -> None:
        self.fixture.flow_rows[0]["layer_id"] = 7
        self.fixture.write_all()
        self.assert_failed("fabricates unavailable layer/iteration")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "flow-rank")
        self.fixture.flow_rows = self.fixture.flow_rows[:1]
        self.fixture.write_all()
        self.assert_failed("rank coverage")

    def test_horizon_must_be_exact_and_workload_incomplete(self) -> None:
        self.fixture.lifecycle_rows[0]["actual_ns"] = 49
        self.fixture.write_all()
        self.assert_failed("horizon is not exact")

        self.fixture = RuntimeFixture(Path(self.temporary.name) / "completed")
        self.fixture.lifecycle_rows.insert(0, {
            "run_id": self.fixture.run_id, "event": "finish_barrier",
            "scheduled_ns": "", "actual_ns": 49, "finished_ranks": 2,
            "world_size": 2, "status": "WORKLOAD_COMPLETE",
        })
        self.fixture.write_all()
        self.assert_failed("completed the declared workload")

    def test_a1_burst_requires_pre_post_and_exact_eight_transactions(self) -> None:
        roles, transactions, switch, context, report = (
            self._override_lifecycle_fixture("allreduce_burst")
        )
        result = runtime._validate_override_application_lifecycle(
            roles, transactions, switch, context, report
        )
        self.assertEqual(result["validated_burst_collective_count"], 8)

        for missing_role, expected in (
            ("PRE_BASELINE", "PRE baseline"),
            ("POST_BASELINE", "POST baseline"),
        ):
            with self.subTest(missing_role=missing_role):
                changed_roles = {
                    "rows": [
                        row for row in roles["rows"]
                        if row["role"] != missing_role
                    ]
                }
                with self.assertRaisesRegex(
                    runtime.RuntimeQualificationError, expected
                ):
                    runtime._validate_override_application_lifecycle(
                        changed_roles, transactions, switch, context, report
                    )

        burst_layers = [
            row["layer_num"] for row in roles["rows"]
            if row["role"] == "EVENT_BURST"
        ]
        changed_transactions = {
            "sequence_evidence": [
                row for row in transactions["sequence_evidence"]
                if row["layer_num"] != burst_layers[-1]
            ]
        }
        with self.assertRaisesRegex(
            runtime.RuntimeQualificationError,
            "EVENT_BURST has 7/8 transactions",
        ):
            runtime._validate_override_application_lifecycle(
                roles, changed_transactions, switch, context, report
            )

    def test_a1_high_util_uses_actual_onset_fixed_window_and_fails_low_rate(
        self,
    ) -> None:
        roles, transactions, switch, context, report = (
            self._override_lifecycle_fixture("high_utilization")
        )
        result = runtime._validate_override_application_lifecycle(
            roles, transactions, switch, context, report
        )
        self.assertEqual(result["validated_pressure_collective_count"], 160)
        self.assertEqual(result["fixed_throughput_window_start_ns"], 150_000_000)
        self.assertEqual(result["fixed_throughput_window_end_ns"], 350_000_000)
        self.assertEqual(
            result["throughput_window_source"],
            "actual_application_onset_from_transaction_role",
        )

        first = next(iter(switch["access_throughput_series_by_endpoint"]))
        switch["access_throughput_series_by_endpoint"][first] = [
            (timestamp, 50.0, 100)
            for timestamp in range(170_000_000, 350_000_001, 20_000_000)
        ]
        with self.assertRaisesRegex(
            runtime.RuntimeQualificationError,
            "utilization coverage .* is below",
        ):
            runtime._validate_override_application_lifecycle(
                roles, transactions, switch, context, report
            )


if __name__ == "__main__":
    unittest.main()
