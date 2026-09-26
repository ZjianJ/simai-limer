#!/usr/bin/env python3
"""Regression tests for the P0 experiment-contract gate."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

import yaml


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
CONTRACT = LIMER / "configs" / "experiment_contract.yaml"
sys.path.insert(0, str(TOOLS))

from validate_experiment_contract import FAIL, PASS, validate_contract  # noqa: E402


class ExperimentContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))

    def test_normative_contract_passes(self) -> None:
        result = validate_contract(self.contract)
        self.assertEqual(result["status"], PASS)
        self.assertEqual(result["summary"]["fail"], 0)

    def test_topology_arithmetic_drift_fails(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["topology"]["invariants"]["physical_link_count"] = 303
        result = validate_contract(contract)
        self.assertEqual(result["status"], FAIL)
        failed = {
            item["check"]
            for item in result["checks"]
            if item["status"] == FAIL
        }
        self.assertIn("true16_topology_constants", failed)
        self.assertIn("link_class_partition", failed)

    def test_alarm_time_cannot_be_internal_score_time(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["timepoints"]["primary_alarm_time"] = "controller_decision_ns"
        result = validate_contract(contract)
        self.assertEqual(result["status"], FAIL)
        self.assertTrue(
            any(
                item["check"] == "primary_time_semantics"
                and item["status"] == FAIL
                for item in result["checks"]
            )
        )

    def test_dynamic_fault_capacity_cannot_be_a_feature(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["features"]["forbidden_fields"].remove(
            "configured_bandwidth_bps"
        )
        result = validate_contract(contract)
        self.assertEqual(result["status"], FAIL)
        self.assertTrue(
            any(
                item["check"] == "feature_leakage_boundary"
                and item["status"] == FAIL
                for item in result["checks"]
            )
        )

    def test_short_healthy_trace_cannot_satisfy_p1(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["workload"][
            "healthy_monitoring_minimum_snapshot_count_at_1ms"
        ] = 2
        result = validate_contract(contract)
        self.assertEqual(result["status"], FAIL)
        self.assertTrue(
            any(
                item["check"] == "p1_long_healthy_boundary"
                and item["status"] == FAIL
                for item in result["checks"]
            )
        )


if __name__ == "__main__":
    unittest.main()
