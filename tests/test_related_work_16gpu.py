#!/usr/bin/env python3
import json
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

from related_work_common import (  # noqa: E402
    DuplicateContributionError,
    EpochCollectiveGuard,
    IncompleteCollectiveError,
    PASS,
    StaleEpochError,
    UNVERIFIED,
    aggregate_detection,
    build_detection_timeline,
    collective_safety_cases,
    next_epoch_strict,
    optcc_theorem13_normalized_ratio,
    strict_deadline_status,
)
from evaluate_related_work_16gpu import (  # noqa: E402
    build_platform_audit,
    parse_smoke_log,
)


class TimingContractTest(unittest.TestCase):
    def test_periodic_observation_is_strictly_after_event(self):
        self.assertEqual(next_epoch_strict(4_000_000, 50_000_000), 50_000_000)
        self.assertEqual(next_epoch_strict(50_000_000, 50_000_000), 100_000_000)

    def test_slo_is_strict(self):
        self.assertEqual(strict_deadline_status(999_999, 1_000_000), PASS)
        self.assertNotEqual(strict_deadline_status(1_000_000, 1_000_000), PASS)

    def test_optcc_theorem13_uses_server_count_q(self):
        ratio = optcc_theorem13_normalized_ratio(
            1.0 / .875, gpu_count=16, gpus_per_server=4)
        expected_term = 2 * (1 / .875) * 3 / ((1 / .875) * 2 + 2)
        self.assertAlmostEqual(ratio, 16 / 30 * expected_term, places=12)


class DetectionLedgerTest(unittest.TestCase):
    def setUp(self):
        config_path = Path(__file__).resolve().parents[1] / "configs" / "related_work_16gpu.json"
        self.config = json.loads(config_path.read_text())
        self.events = pd.DataFrame([{
            "run_id": "r0",
            "fault_id": "f0",
            "fault_kind": "gray_transient_error",
            "fault_group": "GRAY",
            "target_link_id": "L0-20",
            "fault_start_ns": 3_650_000,
            "fault_end_ns": 5_650_000,
            "detection_deadline_ns": 100_000_000,
            "event_observable": True,
            "severity": .005,
        }])
        base = {
            "run_id": "r0", "fault_id": "f0", "fault_kind": "gray_transient_error",
            "fault_group": "GRAY", "target_link_id": "L0-20",
            "event_observable": True, "detected": True, "alarm_time_ns": 4_000_000,
            "top1_unique_correct": True,
        }
        self.detections = pd.DataFrame([
            dict(base, detector="switch_sparse"),
            dict(base, detector="qghmm_quantized"),
            dict(base, detector="host_telemetry"),
        ])
        self.features = pd.DataFrame([
            {"run_id": "r0", "timestamp_ns": 4_000_000,
             "link_id": "L0-20", "drop_error_delta": 1.0},
            {"run_id": "r0", "timestamp_ns": 4_000_000,
             "link_id": "L1-21", "drop_error_delta": 0.0},
        ])

    def test_model_timing_is_not_platform_verified(self):
        timeline = build_detection_timeline(
            self.events, self.detections, self.config, self.features)
        fancy = timeline[
            timeline["method"] == "FANcY-dedicated-cadence"].iloc[0]
        self.assertEqual(int(fancy["delivered_alarm_time_ns"]), 70_000_000)
        self.assertEqual(fancy["actionable_timing_status"], PASS)
        self.assertEqual(fancy["strict_platform_status"], UNVERIFIED)
        self.assertFalse(bool(fancy["platform_executed"]))

    def test_limer_signal_does_not_fabricate_delivery(self):
        timeline = build_detection_timeline(self.events, self.detections, self.config)
        limer = timeline[timeline["method"] == "LIMER-switch-sparse"].iloc[0]
        self.assertEqual(limer["signal_timing_status"], PASS)
        self.assertTrue(pd.isna(limer["delivered_alarm_time_ns"]))
        self.assertEqual(limer["strict_platform_status"], UNVERIFIED)

    def test_finite_fault_does_not_fabricate_timeout_error(self):
        timeline = build_detection_timeline(self.events, self.detections, self.config)
        rdma = timeline[timeline["method"] == "RDMA-error-reference"].iloc[0]
        nccl = timeline[timeline["method"] == "NCCL-error-reference"].iloc[0]
        self.assertTrue(pd.isna(rdma["delivered_alarm_time_ns"]))
        self.assertTrue(pd.isna(nccl["delivered_alarm_time_ns"]))

    def test_trumpet_separates_trigger_and_projected_delivery(self):
        timeline = build_detection_timeline(self.events, self.detections, self.config)
        trumpet = timeline[timeline["method"] == "Trumpet-10ms-trigger"].iloc[0]
        self.assertEqual(int(trumpet["signal_time_ns"]), 10_000_000)
        self.assertEqual(int(trumpet["delivered_alarm_time_ns"]), 10_750_000)

    def test_fancy_does_not_borrow_limer_alarm_without_counter_signal(self):
        timeline = build_detection_timeline(
            self.events, self.detections, self.config,
            self.features.assign(drop_error_delta=0.0))
        fancy = timeline[
            timeline["method"] == "FANcY-dedicated-cadence"].iloc[0]
        self.assertTrue(pd.isna(fancy["signal_time_ns"]))
        self.assertFalse(bool(fancy["alarm_generated"]))

    def test_strict_summary_retains_unobservable_scheduled_fault(self):
        events = pd.concat([
            self.events,
            self.events.assign(run_id="r1", fault_id="f1", event_observable=False),
        ], ignore_index=True)
        detections = pd.concat([
            self.detections,
            self.detections.assign(run_id="r1", fault_id="f1", detected=False,
                                   alarm_time_ns=float("nan"),
                                   top1_unique_correct=float("nan")),
        ], ignore_index=True)
        timeline = build_detection_timeline(events, detections, self.config, self.features)
        summary = aggregate_detection(timeline)
        row = summary[(summary["method"] == "LIMER-switch-sparse")
                      & (summary["scope"] == "GRAY")].iloc[0]
        self.assertEqual(int(row["scheduled_events"]), 2)
        self.assertEqual(float(row["scheduled_signal_timing_pass_rate"]), .5)


class CollectiveInvariantTest(unittest.TestCase):
    def test_guard_rejects_incomplete_duplicate_and_stale_transitions(self):
        guard = EpochCollectiveGuard(world_size=2, vector_width=1)
        guard.contribute(0, [3], epoch=0)
        with self.assertRaises(DuplicateContributionError):
            guard.contribute(0, [3], epoch=0)
        with self.assertRaises(IncompleteCollectiveError):
            guard.commit(epoch=0)
        guard.abort(epoch=0)
        guard.begin_redo()
        with self.assertRaises(StaleEpochError):
            guard.contribute(1, [4], epoch=0)

    def test_all_fault_phases_abort_and_redo_exactly(self):
        events = pd.DataFrame([{
            "run_id": "r0", "fault_id": "f0", "fault_kind": "hard_link_down",
            "target_link_id": "L15-23", "fault_start_ns": 17,
        }])
        cases = collective_safety_cases(events, world_size=16, vector_width=8)
        self.assertEqual(len(cases), 3)
        self.assertEqual(set(cases["failed_rank"]), {15})
        self.assertTrue((cases["correctness_status"] == PASS).all())
        self.assertTrue(cases["exactly_once_contributions"].all())
        self.assertFalse(cases["wrong_result_published"].any())
        self.assertTrue(cases["failed_rank_contribution_replayed"].all())
        self.assertTrue((cases["expected_digest"] == cases["observed_digest"]).all())


class PlatformEvidenceTest(unittest.TestCase):
    def test_smoke_requires_hash_ring_ranks_and_ok_sender_flows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "workload.txt"
            input_path.write_text("input")
            digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
            (root / "input.sha256").write_text(
                f"{digest}  {input_path.resolve()}\n")
            (root / "run.log").write_text(
                "model_parallel_NPU_group is 16\n"
                "dimension: local total nodes in ring: 16\n"
                "fwd pass comm collective for layer: layer_00 is finished\n"
                "pass: 0 finished at time: 120\n"
                "all passes finished at time: 120\n"
                "Percentage of finished streams: 100\n")
            pd.DataFrame({
                "rank_id": list(range(16)), "world_size": [16] * 16,
                "status": ["ok"] * 16, "finish_time_ns": list(range(16)),
            }).to_csv(root / "collective_telemetry.csv", index=False)
            result = parse_smoke_log(root / "run.log", [input_path])
            self.assertTrue(result["validated_healthy_smoke"])
            self.assertFalse(result["collective_commit_observed"])
            self.assertEqual(result["collective_telemetry_semantics"],
                             "flow_sender_completion")

    def test_platform_contract_failure_is_derived_from_fault_workload(self):
        limer = Path(__file__).resolve().parents[1]
        config = json.loads((limer / "configs" / "related_work_16gpu.json").read_text())
        capability = json.loads((
            limer / "results" / "current_system_slo_baseline"
            / "capability_matrix.json").read_text())
        provenance = {
            "all_selected_runs_locked_test_split": True,
            "telemetry_interval_matches_contract": True,
            "event_schedule_matches_manifest": True,
            "all_selected_runs_in_feature_dataset": True,
            "feature_dataset_run_set_and_split_match_manifest": True,
            "feature_dataset_incomplete_snapshot_count": 0,
            "feature_dataset_access_set_matches_manifest": True,
            "feature_dataset_duplicate_key_count": 0,
            "all_manifest_sidecar_hashes_match": True,
        }
        audit = build_platform_audit(
            capability, config,
            limer / "configs" / "microAllReduce_10iter.txt",
            limer / "configs" / "microAllReduce_16rank_10iter.txt",
            None, provenance, [], {"provided": True, "validated": True})
        self.assertTrue(audit["ledger_input_contract_validated"])
        self.assertFalse(audit["platform_contract_validated"])
        self.assertFalse(audit["strict_end_to_end_platform_ready"])
        self.assertFalse(audit["hard_failure_is_physical_disconnect"])


if __name__ == "__main__":
    unittest.main()
