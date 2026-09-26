#!/usr/bin/env python3
"""Independent regression tests for the P1 true-16 monitoring audit."""

import copy
import sys
import unittest
from pathlib import Path

import pandas as pd


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
TRUE16 = LIMER / "results" / "true16_hard_fault_e2e"
TOPOLOGY_PATH = (
    TRUE16 / "topology" /
    "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
)
HEALTHY = TRUE16 / "healthy"

sys.path.insert(0, str(TOOLS))

import audit_true16_monitoring  # noqa: E402
import validate_telemetry  # noqa: E402
import validate_true16_dualrail  # noqa: E402


class True16TopologyContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.topology = validate_true16_dualrail.read_topology(TOPOLOGY_PATH)

    def test_existing_true16_topology_passes_exact_contract(self):
        result = validate_true16_dualrail.validate(self.topology, 16, 4)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["topology"]["node_count"], 92)
        self.assertEqual(result["topology"]["physical_link_count"], 304)
        self.assertEqual(
            result["topology"]["link_class_counts"],
            {"ACCESS": 32, "INTER_SWITCH": 256, "INTRA_NODE": 16},
        )

    def test_cross_wired_access_group_fails_rail_contract(self):
        topology = copy.deepcopy(self.topology)
        first = next(
            link for link in topology["links"]
            if {link["src"], link["dst"]} == {0, 20}
        )
        second = next(
            link for link in topology["links"]
            if {link["src"], link["dst"]} == {1, 21}
        )
        # Preserve all aggregate counts and both switch components while
        # violating the same-local-slot rail grouping.
        first["dst"], second["dst"] = second["dst"], first["dst"]
        result = validate_true16_dualrail.validate(topology, 16, 4)
        checks = {item["name"]: item["status"] for item in result["checks"]}
        self.assertEqual(checks["rail_optimized_access_groups"], "FAIL")
        self.assertEqual(result["status"], "FAIL")

    def test_link_map_encodes_backup_nic2_and_primary_nic3(self):
        link_map = pd.read_csv(HEALTHY / "link_map.csv")
        topology_result = validate_true16_dualrail.validate(self.topology, 16, 4)
        checks = []
        contract = audit_true16_monitoring.validate_link_map_contract(
            link_map, self.topology, topology_result, checks, "test")
        self.assertTrue(all(item["status"] == "PASS" for item in checks))
        self.assertEqual(contract["plane_a_backup_nic"], 2)
        self.assertEqual(contract["plane_b_primary_nic"], 3)
        self.assertNotEqual(
            contract["plane_a_component"], contract["plane_b_component"])


class SnapshotAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.link_map = pd.read_csv(HEALTHY / "link_map.csv")
        cls.switch = pd.read_csv(HEALTHY / "switch_telemetry.csv")
        cls.nic = pd.read_csv(HEALTHY / "nic_telemetry.csv")

    def test_existing_snapshots_have_exact_endpoint_inventory(self):
        result = validate_telemetry._sample_inventory_diagnostics(
            self.switch, self.nic, self.link_map)
        self.assertEqual(result["expected_fabric_endpoints"], 560)
        self.assertEqual(result["expected_host_endpoints"], 48)
        self.assertEqual(result["duplicate_sample_count"], 0)
        self.assertEqual(result["coverage_mismatch_sample_count"], 0)

    def test_equal_row_count_cannot_hide_missing_endpoint(self):
        switch = self.switch.copy()
        timestamp = switch["timestamp_ns"].min()
        rows = switch[
            (switch["timestamp_ns"] == timestamp)
            & (switch["direction"] == "tx")
        ].index[:2]
        endpoint_columns = [
            "switch_id", "port_id", "link_id", "peer_node_id", "direction",
        ]
        switch.loc[rows[1], endpoint_columns] = switch.loc[
            rows[0], endpoint_columns].values
        result = validate_telemetry._sample_inventory_diagnostics(
            switch, self.nic, self.link_map)
        self.assertEqual(len(switch), len(self.switch))
        self.assertGreater(result["duplicate_sample_count"], 0)
        self.assertGreater(result["coverage_mismatch_sample_count"], 0)

    def test_counter_decrease_is_detected_for_exact_endpoint(self):
        switch = self.switch.copy()
        timestamps = sorted(switch["timestamp_ns"].unique())
        first_row = switch[
            (switch["timestamp_ns"] == timestamps[0])
            & (switch["direction"] == "tx")
            & (switch["tx_bytes"] > 0)
        ].iloc[0]
        same_endpoint_second = (
            (switch["timestamp_ns"] == timestamps[1])
            & (switch["switch_id"] == first_row["switch_id"])
            & (switch["port_id"] == first_row["port_id"])
            & (switch["link_id"] == first_row["link_id"])
            & (switch["direction"] == "tx")
        )
        switch.loc[same_endpoint_second, "tx_bytes"] = int(first_row["tx_bytes"]) - 1
        violations = validate_telemetry._counter_monotonicity_diagnostics(
            switch, self.nic)
        self.assertTrue(any(item["counter"] == "tx_bytes" for item in violations))

    def test_every_snapshot_link_conservation_is_checked(self):
        checked, violations = validate_telemetry._link_conservation_diagnostics(
            self.switch, self.nic, self.link_map)
        self.assertEqual(checked, 2 * 304 * self.switch["timestamp_ns"].nunique())
        self.assertEqual(violations, [])

        nic = self.nic.copy()
        timestamp = nic["timestamp_ns"].max()
        nic.loc[nic["timestamp_ns"] == timestamp, "rx_bytes"] += 1_000_000_000
        _, corrupted = validate_telemetry._link_conservation_diagnostics(
            self.switch, nic, self.link_map)
        self.assertTrue(corrupted)


class EndToEndAuditTest(unittest.TestCase):
    def test_existing_healthy_and_hard_runs_pass_p1_audit(self):
        result = audit_true16_monitoring.build_audit(
            TOPOLOGY_PATH,
            [TRUE16 / "healthy", TRUE16 / "hard_disconnect"],
            expected_sample_interval_ns=1_000_000,
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["summary"]["fail"], 0)
        self.assertEqual(len(result["runs"]), 2)


if __name__ == "__main__":
    unittest.main()
