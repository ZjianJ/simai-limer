#!/usr/bin/env python3
"""Tests for the P0 stage-gate evaluator."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

from evaluate_p0_gate import FAIL, PASS, evaluate  # noqa: E402


def fixtures():
    digest = "a" * 64
    revision = "b" * 40
    provenance = {
        "contract_id": "limer-true16-dual-plane-v1",
        "contract_sha256": digest,
        "simai_revision": revision,
        "ns3_revision": revision,
        "dirty_worktree": True,
        "topology_path": "topology",
        "topology_sha256": digest,
        "workload_path": "workload",
        "workload_sha256": digest,
        "simulator_config_path": "config",
        "simulator_config_sha256": digest,
        "detector_id": "H0",
        "detector_artifact_sha256": None,
        "split_manifest_sha256": None,
        "schedule_sha256": digest,
        "run_id": None,
        "run_role": None,
        "virtual_start_ns": None,
        "virtual_finish_ns": None,
        "wall_start_utc": None,
        "wall_finish_utc": None,
        "exit_code": None,
        "topology_validation_sha256": digest,
        "suite_exit_code_artifacts": {
            "healthy": digest,
            "hard_disconnect": digest,
        },
    }
    provenance["not_applicable_reasons"] = {
        key: "suite-level"
        for key, value in provenance.items()
        if value is None
    }
    contract = {
        "status": PASS,
        "summary": {"pass": 21, "fail": 0},
        "contract_sha256": digest,
    }
    manifest = {
        "schema_version": "limer.baseline-freeze.v1",
        "preset": "true16-hard-fault-e2e",
        "content_fingerprint_sha256": digest,
        "git": {
            "superproject": {"working_tree_fingerprint_sha256": digest}
        },
        "artifacts": [{"path": "topology", "sha256": digest}],
        "contract_provenance": provenance,
    }
    check = {"status": PASS, "summary": {"failed": 0}}
    return contract, manifest, check


class P0GateTest(unittest.TestCase):
    def test_complete_freeze_passes(self) -> None:
        contract, manifest, check = fixtures()
        result = evaluate(contract, manifest, check)
        self.assertEqual(result["status"], PASS)
        self.assertEqual(result["next_stage"], "P1")

    def test_contract_hash_drift_fails(self) -> None:
        contract, manifest, check = fixtures()
        manifest = copy.deepcopy(manifest)
        manifest["contract_provenance"]["contract_sha256"] = "c" * 64
        result = evaluate(contract, manifest, check)
        self.assertEqual(result["status"], FAIL)
        self.assertIsNone(result["next_stage"])

    def test_null_provenance_requires_reason(self) -> None:
        contract, manifest, check = fixtures()
        manifest = copy.deepcopy(manifest)
        del manifest["contract_provenance"]["not_applicable_reasons"]["run_id"]
        result = evaluate(contract, manifest, check)
        self.assertEqual(result["status"], FAIL)

    def test_manifest_verification_failure_blocks_gate(self) -> None:
        contract, manifest, check = fixtures()
        check = {"status": FAIL, "summary": {"failed": 1}}
        result = evaluate(contract, manifest, check)
        self.assertEqual(result["status"], FAIL)


if __name__ == "__main__":
    unittest.main()
