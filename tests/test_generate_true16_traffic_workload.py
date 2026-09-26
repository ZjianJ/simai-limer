#!/usr/bin/env python3
"""Tests for profile-separated true-16 workload qualification."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
sys.path.insert(0, str(TOOLS))

from generate_true16_traffic_workload import (  # noqa: E402
    COMPLETION_LAYER_TO_STAGE,
    COMPLETION_PROFILE,
    CORPUS_MAX_VIRTUAL_FINISH_NS,
    DEFAULT_COLLECTIVE_BYTES,
    DEFAULT_COMPUTE_NS,
    DEFAULT_LAYERS,
    HEADER,
    HORIZON_COMPUTE_GUARD_NS,
    HORIZON_PREFIX_PROFILE,
    MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS,
    REQUIRED_TRAFFIC_WINDOW_NS,
    SCHEMA_VERSION,
    WorkloadValidationError,
    build_workload_text,
    generate_workload,
    main,
    validate_workload,
)


Q_CONFIGS = {
    1: "microAllReduce_16rank_p2_q1_completion_1layer_64kib.txt",
    10: "microAllReduce_16rank_p2_q2_completion_10layers_64kib.txt",
    32: "microAllReduce_16rank_p2_q3_completion_32layers_64kib.txt",
}
HORIZON_CONFIGS = (
    "microAllReduce_16rank_p2_sparse_periodic_550ms.txt",
)


def structurally_valid_text(
    layers: int,
    compute_ns: int = DEFAULT_COMPUTE_NS,
    collective_bytes: int = DEFAULT_COLLECTIVE_BYTES,
) -> str:
    rows = [HEADER, str(layers)]
    rows.extend(
        f"p2_layer_{index:04d} -1 {compute_ns} ALLREDUCE "
        f"{collective_bytes} 1 NONE 0 1 NONE 0 1"
        for index in range(layers)
    )
    return "\n".join(rows) + "\n"


class True16TrafficWorkloadTest(unittest.TestCase):
    def test_default_is_550_layer_horizon_prefix_and_is_deterministic(self) -> None:
        self.assertEqual(SCHEMA_VERSION, "limer.true16-traffic-workload.v3")
        self.assertEqual(DEFAULT_LAYERS, 550)
        self.assertEqual(DEFAULT_COMPUTE_NS, 1_000_000)
        self.assertEqual(DEFAULT_COLLECTIVE_BYTES, 64 * 1024)
        self.assertEqual(CORPUS_MAX_VIRTUAL_FINISH_NS, 520_000_000)
        self.assertEqual(REQUIRED_TRAFFIC_WINDOW_NS, 520_000_000)
        self.assertEqual(HORIZON_COMPUTE_GUARD_NS, 30_000_000)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first.txt"
            second = root / "second.txt"
            report = generate_workload(
                output=first,
                layers=DEFAULT_LAYERS,
                compute_ns=DEFAULT_COMPUTE_NS,
                collective_bytes=DEFAULT_COLLECTIVE_BYTES,
            )
            duplicate = generate_workload(
                output=second,
                layers=DEFAULT_LAYERS,
                compute_ns=DEFAULT_COMPUTE_NS,
                collective_bytes=DEFAULT_COLLECTIVE_BYTES,
            )
            self.assertEqual(first.read_bytes(), second.read_bytes())

        self.assertEqual(report["sha256"], duplicate["sha256"])
        self.assertEqual(report["qualification_profile"], HORIZON_PREFIX_PROFILE)
        self.assertEqual(report["layer_count"], 550)
        self.assertIsNone(report["completion_stage"])
        self.assertTrue(report["may_be_used_as_p2_horizon_workload"])
        estimate = report["duration_estimate"]
        self.assertTrue(estimate["qualifies_selected_profile"])
        self.assertTrue(estimate["qualifies_static_horizon_prefix_plan"])
        self.assertFalse(
            estimate["qualifies_static_no_stop_completion_plan"]
        )
        self.assertGreaterEqual(
            estimate["planned_collective_opportunity_span_ns"],
            CORPUS_MAX_VIRTUAL_FINISH_NS,
        )
        self.assertGreaterEqual(
            estimate["compute_only_workload_finish_lower_bound_ns"],
            CORPUS_MAX_VIRTUAL_FINISH_NS + HORIZON_COMPUTE_GUARD_NS,
        )
        self.assertLessEqual(
            estimate["planned_inter_allreduce_issue_gap_ns"],
            MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS,
        )
        self.assertTrue(all(estimate["static_qualification_checks"].values()))
        self.assertFalse(estimate["runtime_evidence"])

        active = estimate["runtime_evidence_contract"]
        self.assertEqual(active["contract_id"], "p2-horizon-prefix-v1")
        self.assertTrue(active["applicable"])
        self.assertFalse(active["declared_workload_completion_expected"])
        self.assertFalse(active["all_declared_collectives_completion_required"])
        self.assertNotIn(
            "all_declared_collectives_issued_and_completed", active
        )
        self.assertEqual(active["maximum_inflight_collectives_at_horizon"], 1)
        contracts = estimate["runtime_evidence_contracts"]
        self.assertFalse(contracts["no_stop_completion"]["applicable"])
        self.assertTrue(contracts["horizon_prefix"]["applicable"])

    def test_completion_profile_allows_only_q1_q2_q3_and_never_horizon(self) -> None:
        for layers, stage in COMPLETION_LAYER_TO_STAGE.items():
            with self.subTest(stage=stage):
                with tempfile.TemporaryDirectory() as temporary:
                    output = Path(temporary) / f"{stage}.txt"
                    report = generate_workload(
                        output=output,
                        layers=layers,
                        compute_ns=DEFAULT_COMPUTE_NS,
                        collective_bytes=DEFAULT_COLLECTIVE_BYTES,
                        profile=COMPLETION_PROFILE,
                    )
                    explicit = validate_workload(
                        output, profile=COMPLETION_PROFILE
                    )
                    with self.assertRaisesRegex(
                        WorkloadValidationError,
                        "horizon_profile_has_at_least_default_layers",
                    ):
                        validate_workload(output)

                self.assertEqual(report["completion_stage"], stage)
                self.assertEqual(explicit["completion_stage"], stage)
                self.assertFalse(report["may_be_used_as_p2_horizon_workload"])
                estimate = report["duration_estimate"]
                self.assertTrue(
                    estimate["qualifies_static_no_stop_completion_plan"]
                )
                self.assertFalse(
                    estimate["qualifies_static_horizon_prefix_plan"]
                )
                active = estimate["runtime_evidence_contract"]
                self.assertEqual(active["contract_id"], "no-stop-completion-v1")
                self.assertTrue(active["observation_stop_must_be_unset"])
                self.assertTrue(
                    active["all_declared_collectives_issued_and_completed"]
                )
                self.assertFalse(active["may_qualify_p2_horizon_prefix"])
                self.assertTrue(
                    estimate["runtime_evidence_contracts"]
                    ["no_stop_completion"]["applicable"]
                )
                self.assertFalse(
                    estimate["runtime_evidence_contracts"]
                    ["horizon_prefix"]["applicable"]
                )

    def test_named_q_and_final_configs_are_exact_canonical_outputs(self) -> None:
        for layers, filename in Q_CONFIGS.items():
            with self.subTest(filename=filename):
                path = LIMER / "configs" / filename
                report = validate_workload(path, profile=COMPLETION_PROFILE)
                self.assertEqual(report["completion_stage"],
                                 COMPLETION_LAYER_TO_STAGE[layers])
                self.assertEqual(
                    path.read_text(encoding="utf-8"),
                    build_workload_text(
                        layers=layers,
                        compute_ns=DEFAULT_COMPUTE_NS,
                        collective_bytes=DEFAULT_COLLECTIVE_BYTES,
                        profile=COMPLETION_PROFILE,
                    ),
                )
                with self.assertRaises(WorkloadValidationError):
                    validate_workload(path)

        expected_horizon = build_workload_text(
            layers=DEFAULT_LAYERS,
            compute_ns=DEFAULT_COMPUTE_NS,
            collective_bytes=DEFAULT_COLLECTIVE_BYTES,
        )
        observed = []
        for filename in HORIZON_CONFIGS:
            path = LIMER / "configs" / filename
            report = validate_workload(path)
            self.assertEqual(report["layer_count"], 550)
            self.assertTrue(report["may_be_used_as_p2_horizon_workload"])
            observed.append(path.read_text(encoding="utf-8"))
        self.assertEqual(observed, [expected_horizon])

    def test_v5_canonical_input_is_preserved_but_not_reused_for_horizon(self) -> None:
        legacy = LIMER / "configs" / "microAllReduce_16rank_p2_traffic.txt"
        self.assertEqual(
            hashlib.sha256(legacy.read_bytes()).hexdigest(),
            "67a671ce34fc4bb8142f38071cbdfcb9e58fab8f36013d47297828b9ca875869",
        )
        with self.assertRaises(WorkloadValidationError):
            validate_workload(legacy)

    def test_legacy_450ms_config_is_explicitly_not_horizon_qualified(self) -> None:
        legacy = (
            LIMER / "configs" /
            "microAllReduce_16rank_p2_sparse_periodic_450ms.txt"
        )
        self.assertTrue(legacy.is_file())
        with self.assertRaisesRegex(
            WorkloadValidationError,
            "horizon_profile_has_at_least_default_layers",
        ):
            validate_workload(legacy)
        with self.assertRaisesRegex(
            WorkloadValidationError,
            "completion_layer_count_is_q1_q2_or_q3",
        ):
            validate_workload(legacy, profile=COMPLETION_PROFILE)

    def test_header_and_every_default_layer_match_simai_format(self) -> None:
        text = build_workload_text(
            layers=DEFAULT_LAYERS,
            compute_ns=DEFAULT_COMPUTE_NS,
            collective_bytes=DEFAULT_COLLECTIVE_BYTES,
        )
        lines = text.splitlines()
        self.assertEqual(lines[0], HEADER)
        self.assertEqual(int(lines[1]), DEFAULT_LAYERS)
        self.assertEqual(len(lines), DEFAULT_LAYERS + 2)
        for index, row in enumerate(lines[2:]):
            fields = row.split()
            self.assertEqual(fields, [
                f"p2_layer_{index:04d}", "-1", str(DEFAULT_COMPUTE_NS),
                "ALLREDUCE", str(DEFAULT_COLLECTIVE_BYTES), "1", "NONE",
                "0", "1", "NONE", "0", "1",
            ])

    def test_cli_defaults_to_horizon_and_completion_requires_explicit_layers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            horizon = root / "horizon.txt"
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                rc = main(["--output", str(horizon)])
            self.assertEqual(rc, 0)
            report = json.loads(captured.getvalue())
            self.assertEqual(report["qualification_profile"],
                             HORIZON_PREFIX_PROFILE)
            self.assertEqual(report["layer_count"], 550)

            missing = root / "missing.txt"
            with contextlib.redirect_stderr(io.StringIO()):
                rc = main([
                    "--output", str(missing),
                    "--profile", COMPLETION_PROFILE,
                ])
            self.assertEqual(rc, 2)
            self.assertFalse(missing.exists())

            q2 = root / "q2.txt"
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                rc = main([
                    "--output", str(q2),
                    "--profile", COMPLETION_PROFILE,
                    "--layers", "10",
                ])
            self.assertEqual(rc, 0)
            report = json.loads(captured.getvalue())
            self.assertEqual(report["completion_stage"], "Q2")
            self.assertFalse(report["may_be_used_as_p2_horizon_workload"])

    def test_cli_check_requires_the_correct_explicit_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "q1.txt"
            path.write_text(structurally_valid_text(1), encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(["--output", str(path), "--check"]), 2)
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                rc = main([
                    "--output", str(path), "--check",
                    "--profile", COMPLETION_PROFILE,
                ])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(captured.getvalue())["completion_stage"],
                             "Q1")

    def test_generation_rejects_wrong_profile_shape_before_write(self) -> None:
        cases = [
            (
                HORIZON_PREFIX_PROFILE, 549, DEFAULT_COMPUTE_NS,
                DEFAULT_COLLECTIVE_BYTES,
                "horizon_profile_has_at_least_default_layers",
            ),
            (
                COMPLETION_PROFILE, 2, DEFAULT_COMPUTE_NS,
                DEFAULT_COLLECTIVE_BYTES,
                "completion_layer_count_is_q1_q2_or_q3",
            ),
            (
                HORIZON_PREFIX_PROFILE, DEFAULT_LAYERS,
                DEFAULT_COMPUTE_NS - 1, DEFAULT_COLLECTIVE_BYTES,
                "compute_ns_matches_qualified_profile",
            ),
            (
                COMPLETION_PROFILE, 10, DEFAULT_COMPUTE_NS,
                DEFAULT_COLLECTIVE_BYTES * 2,
                "collective_bytes_matches_qualified_profile",
            ),
        ]
        for profile, layers, compute_ns, collective_bytes, failed in cases:
            with self.subTest(profile=profile, failed=failed):
                with tempfile.TemporaryDirectory() as temporary:
                    output = Path(temporary) / "invalid.txt"
                    with self.assertRaisesRegex(WorkloadValidationError, failed):
                        generate_workload(
                            output=output,
                            layers=layers,
                            compute_ns=compute_ns,
                            collective_bytes=collective_bytes,
                            profile=profile,
                        )
                    self.assertFalse(output.exists())

    def test_validator_rejects_profile_mismatch_and_structural_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "workload.txt"
            for layers, profile, failed in [
                (549, HORIZON_PREFIX_PROFILE,
                 "horizon_profile_has_at_least_default_layers"),
                (31, COMPLETION_PROFILE,
                 "completion_layer_count_is_q1_q2_or_q3"),
            ]:
                path.write_text(structurally_valid_text(layers),
                                encoding="utf-8")
                with self.assertRaisesRegex(WorkloadValidationError, failed):
                    validate_workload(path, profile=profile)

            original = structurally_valid_text(DEFAULT_LAYERS)
            cases = [
                original.replace("all_gpus: 16", "all_gpus: 8", 1),
                original.replace("\n550\n", "\n551\n", 1),
                original.replace(" ALLREDUCE ", " ALLGATHER ", 1),
                original.replace("p2_layer_0001", "duplicate", 1),
            ]
            for tampered in cases:
                path.write_text(tampered, encoding="utf-8")
                with self.assertRaises(WorkloadValidationError):
                    validate_workload(path)


if __name__ == "__main__":
    unittest.main()
