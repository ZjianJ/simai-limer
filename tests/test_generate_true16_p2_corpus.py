#!/usr/bin/env python3
"""Contract tests for the deterministic true-16 P2 corpus planner."""

from __future__ import annotations

import csv
import copy
import json
import struct
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
sys.path.insert(0, str(TOOLS))

from generate_true16_p2_corpus import (  # noqa: E402
    BACKGROUND_FLOW_CSV_COLUMNS,
    BACKGROUND_COMPLETION_DEADLINE_NS,
    BACKGROUND_RDMA_RETRY_LIMIT,
    BACKGROUND_RDMA_RTO_US,
    COLLECTIVE_ROLE_CSV_COLUMNS,
    COLLECTIVE_OVERRIDE_STATUS,
    ECMP_COLLISION_COLUMNS,
    ECMP_COLLISION_STATUS,
    CORPUS_SCHEMA,
    FAIL,
    FAULT_TRUTH_CSV_COLUMNS,
    GRAY_CATEGORIES,
    INJECTOR_CSV_COLUMNS,
    NEGATIVE_CSV_COLUMNS,
    PASS,
    PARTITIONS,
    SPLIT_SCHEMA,
    CorpusError,
    generate_corpus,
    load_physical_links,
    ns3_murmur3_x86_32,
    prepared_background_contract_issues,
    rank_ipv4_u32,
    rdma_route_bucket,
    run_identity_entry,
    schedule_identity_tuple,
    stable_ecmp_candidates,
    validate_collective_override,
    validate_ecmp_collision_schedule,
    verify_corpus,
)


class True16P2CorpusTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.output = Path(cls.temporary.name) / "corpus"
        cls.inputs = {
            "link_map_path": LIMER
            / "results/true16_hard_fault_e2e/healthy/link_map.csv",
            "contract_path": LIMER / "configs/experiment_contract.yaml",
            "topology_path": LIMER / "results/true16_hard_fault_e2e/topology/"
            "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100",
            "workload_path": LIMER / "configs/"
            "microAllReduce_16rank_p2_sparse_periodic_550ms.txt",
            "simulator_config_path": LIMER / "configs/SimAI.baseline.conf",
        }
        cls.corpus = generate_corpus(
            **cls.inputs,
            out_dir=cls.output,
            seed=20260827,
        )
        cls.runs = cls.corpus["runs"]
        cls.faults = [run for run in cls.runs if run["run_role"] == "fault"]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def test_inventory_and_taxonomy_matrix(self) -> None:
        self.assertEqual(self.corpus["schema_version"], CORPUS_SCHEMA)
        self.assertEqual(self.corpus["status"], "PREPARED")
        self.assertEqual(len(self.runs), 462)
        self.assertEqual(
            Counter(run["run_role"] for run in self.runs),
            {
                "fault": 432,
                "healthy": 6,
                "congestion": 21,
                "ood_stress": 3,
            },
        )
        self.assertEqual(
            Counter(run["fault_family"] for run in self.faults),
            {
                "hard_disconnect": 16,
                "carrier_flap": 16,
                "bandwidth_degradation": 192,
                "random_loss": 80,
                "burst_loss": 32,
                "service_degradation": 48,
                "intermittent_service": 48,
            },
        )
        self.assertEqual(
            {run["gray_category"] for run in self.faults if run["gray_category"]},
            set(GRAY_CATEGORIES),
        )
        self.assertEqual({run["target_gpu"] for run in self.faults}, set(range(16)))
        qualification = self.corpus["workload_qualification"]
        self.assertEqual(
            qualification["static_qualification_profile"], "horizon-prefix"
        )
        self.assertEqual(
            qualification["planned_maximum_virtual_finish_ns"], 520_000_000
        )
        self.assertEqual(
            qualification["static_report"]["duration_estimate"][
                "corpus_max_virtual_finish_ns"
            ],
            520_000_000,
        )

    def test_required_gray_parameters_are_covered(self) -> None:
        bandwidth = [
            run for run in self.faults if run["fault_family"] == "bandwidth_degradation"
        ]
        self.assertEqual(
            {run["severity"]["value"] for run in bandwidth}, {0.8, 0.5, 0.2}
        )
        ramp_durations = set()
        shapes = set()
        for run in bandwidth:
            with (self.output / run["schedule"]["path"]).open(
                encoding="utf-8", newline=""
            ) as stream:
                row = next(csv.DictReader(stream))
            shapes.add(row["shape"])
            if row["ramp_duration_ns"]:
                ramp_durations.add(int(row["ramp_duration_ns"]))
        self.assertEqual(shapes, {"step", "ramp"})
        self.assertEqual(ramp_durations, {10_000_000, 50_000_000, 100_000_000})
        self.assertEqual(
            {
                run["severity"]["value"]
                for run in self.faults
                if run["fault_family"] == "random_loss"
            },
            {0.0001, 0.001, 0.005, 0.01, 0.05},
        )
        self.assertEqual(
            {
                run["scenario"].split("-prob-")[0]
                for run in self.faults
                if run["fault_family"] == "burst_loss"
            },
            {"periodic", "random"},
        )
        self.assertEqual(
            {
                run["scenario"].removeprefix("intermittent-")
                for run in self.faults
                if run["fault_family"] == "intermittent_service"
            },
            {"capacity", "loss", "service"},
        )

    def test_truth_and_simulator_schedules_are_separate_and_locked(self) -> None:
        fault_runs = [
            run
            for run in self.runs
            if run["class_label"] in {"HARD_FAULT", "GRAY_FAULT"}
        ]
        self.assertEqual(len(fault_runs), 435)
        for run in fault_runs:
            schedule = run["schedule"]
            injector = schedule["simulator_injection_schedule"]
            truth_path = self.output / schedule["path"]
            injector_path = self.output / injector["path"]
            self.assertEqual(schedule["kind"], "fault")
            self.assertNotEqual(truth_path, injector_path)
            with truth_path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                rows = list(reader)
                self.assertEqual(reader.fieldnames, list(FAULT_TRUTH_CSV_COLUMNS))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_id"], schedule["event_id"])
            with injector_path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                self.assertEqual(reader.fieldnames, list(INJECTOR_CSV_COLUMNS))
                self.assertTrue(list(reader))
            blocked = str(schedule["implementation_status"]).startswith("BLOCKED")
            self.assertEqual(injector["safe_to_execute"], not blocked)

    def test_independent_negative_truth_has_no_fault_fields(self) -> None:
        congestion = [run for run in self.runs if run["run_role"] == "congestion"]
        healthy = [run for run in self.runs if run["run_role"] == "healthy"]
        self.assertEqual(
            {run["scenario"] for run in congestion},
            {
                "high_utilization",
                "allreduce_burst",
                "incast",
                "ecmp_or_hash_contention",
                "queue_buildup",
                "ecn",
                "pfc",
            },
        )
        for run in congestion + healthy:
            schedule = run["schedule"]
            path = self.output / schedule["path"]
            with path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                self.assertEqual(reader.fieldnames, list(NEGATIVE_CSV_COLUMNS))
                rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertNotIn("simulator_injection_schedule", schedule)
            self.assertTrue(schedule["independent_of_features"])
            self.assertTrue(schedule["generated_before_run"])
            self.assertFalse(
                any(
                    token in column.lower()
                    for column in reader.fieldnames or []
                    for token in ("fault", "target", "label", "severity")
                )
            )

    def test_background_rdma_and_collective_override_congestion_are_executable(
        self,
    ) -> None:
        congestion = [run for run in self.runs if run["run_role"] == "congestion"]
        executable = [
            run
            for run in congestion
            if run["schedule"]["implementation_status"] == "EXECUTABLE_BACKGROUND_RDMA"
        ]
        collective_executable = [
            run for run in congestion
            if run["schedule"]["implementation_status"]
            == COLLECTIVE_OVERRIDE_STATUS
        ]
        blocked = [
            run for run in congestion
            if run not in executable and run not in collective_executable
        ]
        self.assertEqual(len(executable), 6)
        self.assertEqual(
            Counter(run["scenario"] for run in executable),
            {"incast": 3, "queue_buildup": 3},
        )
        self.assertEqual(
            Counter(run["scenario"] for run in collective_executable),
            {"allreduce_burst": 3, "high_utilization": 3},
        )
        self.assertTrue(all(
            run["mechanism"]["implementation_status"] == "PLANNED"
            and run["schedule"]["collective_workload_override"][
                "safe_to_execute"
            ] is True
            for run in collective_executable
        ))
        self.assertTrue(
            all(
                run["mechanism"]["implementation_status"] == "PLANNED"
                and run["schedule"]["background_flow_schedule"]["safe_to_execute"]
                is True
                for run in executable
            )
        )
        for run in executable:
            with (self.output / run["schedule"]["path"]).open(
                encoding="utf-8", newline=""
            ) as stream:
                truth = next(csv.DictReader(stream))
            self.assertEqual(truth["action_scope"], "workload")
            ref = run["schedule"]["background_flow_schedule"]
            path = self.output / ref["path"]
            with path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                rows = list(reader)
            self.assertEqual(reader.fieldnames, list(BACKGROUND_FLOW_CSV_COLUMNS))
            expected = 12 if run["scenario"] == "incast" else 24
            self.assertEqual(len(rows), expected)
            self.assertEqual(
                {row["event_id"] for row in rows}, {run["schedule"]["event_id"]}
            )
            self.assertEqual({row["scenario"] for row in rows}, {run["scenario"]})
            self.assertEqual(len({row["flow_id"] for row in rows}), expected)
            self.assertEqual(
                len(
                    {
                        (row["src_rank"], row["dst_rank"], row["sport"], row["pg"])
                        for row in rows
                    }
                ),
                expected,
            )
        self.assertTrue(
            all(
                str(run["schedule"]["implementation_status"]).startswith("BLOCKED_")
                and run["mechanism"]["implementation_status"] == "UNAVAILABLE"
                and "background_flow_schedule" not in run["schedule"]
                for run in blocked
            )
        )

    def test_collective_overrides_are_full_hash_locked_and_executable(
        self,
    ) -> None:
        runs = [
            run
            for run in self.runs
            if run["scenario"] in {"allreduce_burst", "high_utilization"}
        ]
        self.assertEqual(
            Counter(run["scenario"] for run in runs),
            {"allreduce_burst": 3, "high_utilization": 3},
        )
        for run in runs:
            schedule = run["schedule"]
            self.assertEqual(
                schedule["implementation_status"], COLLECTIVE_OVERRIDE_STATUS
            )
            self.assertEqual(run["mechanism"]["implementation_status"], "PLANNED")
            ref = schedule["collective_workload_override"]
            self.assertTrue(ref["safe_to_execute"])
            self.assertEqual(ref["runtime_executor_status"], "READY")
            path = self.output / ref["path"]
            role_ref = ref["layer_role_sidecar"]
            role_path = self.output / role_ref["path"]
            contract = ref["static_validation"]["contract"]
            report = validate_collective_override(
                path,
                role_path,
                contract,
                self.inputs["workload_path"],
                run["run_id"],
            )
            self.assertEqual(ref["sha256"], report["sha256"])
            self.assertEqual(role_ref["sha256"], report["role_sha256"])
            self.assertEqual(ref["static_validation"], report)
            self.assertEqual(report["validated_layer_count"], 550)
            self.assertEqual(run["effective_workload_sha256"], ref["sha256"])
            self.assertEqual(run["virtual_finish_ns"], 520_000_000)
            self.assertEqual(
                run_identity_entry(run)["collective_workload_override_sha256"],
                ref["sha256"],
            )
            self.assertEqual(
                run_identity_entry(run)["collective_layer_role_sha256"],
                role_ref["sha256"],
            )
            with role_path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                roles = list(reader)
            self.assertEqual(reader.fieldnames, list(COLLECTIVE_ROLE_CSV_COLUMNS))
            self.assertEqual(len(roles), 550)
            self.assertEqual(
                sum(row["role"] == "PRE_BASELINE" for row in roles), 130
            )
            self.assertTrue(any(row["role"] == "POST_BASELINE" for row in roles))
            self.assertEqual(
                roles[129]["planned_issue_ns"], "130000000"
            )
            self.assertEqual(
                roles[130]["planned_issue_ns"],
                str(run["fault_scheduled_onset_ns"]),
            )
            mutated = copy.deepcopy(run)
            mutated["schedule"]["collective_workload_override"]["sha256"] = "0" * 64
            self.assertNotEqual(
                schedule_identity_tuple(mutated), schedule_identity_tuple(run)
            )
            self.assertNotEqual(run_identity_entry(mutated), run_identity_entry(run))
            if run["scenario"] == "allreduce_burst":
                self.assertEqual(report["role_counts"]["EVENT_BURST"], 8)
                self.assertEqual(report["world_size"], 16)
                self.assertTrue(report["role_counts"]["EVENT_BASELINE"] > 0)
            else:
                self.assertEqual(report["role_counts"]["EVENT_PRESSURE"], 160)
                self.assertEqual(contract["minimum_formula_pressure_layer_count"], 159)
                self.assertEqual(contract["pressure_safety_layers"], 1)
                self.assertEqual(contract["utilization_threshold"], 0.8)
                self.assertEqual(contract["minimum_window_coverage_ratio"], 0.8)
                self.assertLess(
                    contract["scheduled_onset_ns"], contract["scheduled_end_ns"]
                )

    def test_collective_override_tamper_fails_static_verification(self) -> None:
        run = next(run for run in self.runs if run["scenario"] == "allreduce_burst")
        path = self.output / run["schedule"]["collective_workload_override"]["path"]
        original = path.read_bytes()
        try:
            path.write_bytes(original.replace(b"ALLREDUCE", b"ALLTOALL", 1))
            report = verify_corpus(self.output / "corpus_manifest.json")
            check = next(
                check
                for check in report["checks"]
                if check["name"] == "collective_workload_override_binding"
            )
            self.assertEqual(check["status"], FAIL)
        finally:
            path.write_bytes(original)

    def test_collective_override_rejects_world_layer_and_count_mismatches(self) -> None:
        run = next(run for run in self.runs if run["scenario"] == "allreduce_burst")
        source = self.output / run["schedule"]["collective_workload_override"]["path"]
        ref = run["schedule"]["collective_workload_override"]
        role = self.output / ref["layer_role_sidecar"]["path"]
        contract = ref["static_validation"]["contract"]
        args = (role, contract, self.inputs["workload_path"], run["run_id"])
        with self.subTest("world"):
            invalid = dict(contract, world_size=8)
            with self.assertRaisesRegex(CorpusError, "world_size"):
                validate_collective_override(
                    source, role, invalid, self.inputs["workload_path"], run["run_id"]
                )
        original = source.read_text()
        scratch = Path(self.temporary.name) / "invalid-override.txt"
        with self.subTest("layer"):
            scratch.write_text(original.replace("p2_layer_0003", "p2_layer_9999"))
            with self.assertRaisesRegex(CorpusError, "differs from reconstruction"):
                validate_collective_override(scratch, *args)
        with self.subTest("count"):
            scratch.write_text(original.replace("\n550\n", "\n549\n", 1))
            with self.assertRaisesRegex(CorpusError, "differs from reconstruction"):
                validate_collective_override(scratch, *args)

    def test_ecmp_collision_contract_is_hash_locked_and_non_executable(self) -> None:
        runs = [
            run for run in self.runs if run["scenario"] == "ecmp_or_hash_contention"
        ]
        self.assertEqual(len(runs), 3)
        links = load_physical_links(self.inputs["link_map_path"])
        for run in runs:
            schedule = run["schedule"]
            self.assertEqual(schedule["implementation_status"], ECMP_COLLISION_STATUS)
            self.assertEqual(run["mechanism"]["implementation_status"], "UNAVAILABLE")
            ref = schedule["ecmp_collision_schedule"]
            self.assertFalse(ref["safe_to_execute"])
            self.assertEqual(ref["runtime_route_evidence_status"], "PENDING")
            path = self.output / ref["path"]
            report = validate_ecmp_collision_schedule(path, ref["contract"], links)
            self.assertGreaterEqual(report["candidate_count"], 2)
            self.assertEqual(report["collision_row_count"], 8)
            self.assertEqual(report["control_row_count"], 1)
            self.assertEqual(
                run_identity_entry(run)["ecmp_collision_sha256"], ref["sha256"]
            )
            mutated = copy.deepcopy(run)
            mutated["schedule"]["ecmp_collision_schedule"]["sha256"] = "0" * 64
            self.assertNotEqual(
                schedule_identity_tuple(mutated), schedule_identity_tuple(run)
            )
            self.assertNotEqual(run_identity_entry(mutated), run_identity_entry(run))

    def test_ecmp_cpp_tuple_fixed_vector_and_schedule_tampering_fail(self) -> None:
        packed = struct.pack("<IIHH", 0x0B000001, 0x0B000101, 49152, 21000)
        self.assertEqual(packed.hex(), "0100000b0101000b00c00852")
        self.assertEqual(ns3_murmur3_x86_32(packed, seed=20), 1100062295)

        run = next(
            run for run in self.runs if run["scenario"] == "ecmp_or_hash_contention"
        )
        ref = run["schedule"]["ecmp_collision_schedule"]
        source = self.output / ref["path"]
        with source.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        links = load_physical_links(self.inputs["link_map_path"])

        def rejected(name: str, mutate) -> None:
            changed = copy.deepcopy(rows)
            mutate(changed)
            path = Path(self.temporary.name) / f"ecmp-invalid-{name}.csv"
            with path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=ECMP_COLLISION_COLUMNS)
                writer.writeheader()
                writer.writerows(changed)
            with self.assertRaises(CorpusError):
                validate_ecmp_collision_schedule(path, ref["contract"], links)

        rejected("seed", lambda rows: rows[0].__setitem__("hash_seed_u32", "21"))
        rejected("sport", lambda rows: rows[0].__setitem__("sport", "50000"))
        rejected(
            "byte-order",
            lambda rows: rows[0].__setitem__(
                "tuple_bytes_hex", bytes.fromhex(rows[0]["tuple_bytes_hex"])[::-1].hex()
            ),
        )
        rejected(
            "candidate-order",
            lambda rows: rows[0].__setitem__(
                "expected_egress_port_id", rows[-1]["expected_egress_port_id"]
            ),
        )
        rejected("no-collision", lambda rows: rows[0].__setitem__("role", "CONTROL"))
        with self.assertRaisesRegex(CorpusError, "fewer than two"):
            stable_ecmp_candidates(
                [
                    {
                        "link_id": "single",
                        "src_node": 0,
                        "dst_node": 20,
                        "src_type": "HOST",
                        "dst_type": "SWITCH",
                        "src_port": 2,
                        "dst_port": 1,
                    }
                ],
                20,
                0,
            )

    def test_background_contract_is_single_rail_and_truthful(self) -> None:
        executable = [
            run
            for run in self.runs
            if run["schedule"]["implementation_status"] == "EXECUTABLE_BACKGROUND_RDMA"
        ]
        by_scenario = {
            scenario: sorted(
                (run for run in executable if run["scenario"] == scenario),
                key=lambda run: run["run_id"],
            )
            for scenario in ("incast", "queue_buildup")
        }
        self.assertEqual(
            [run["target_gpu"] for run in by_scenario["incast"]], [0, 4, 8]
        )
        for runs in by_scenario.values():
            for index, run in enumerate(runs):
                pair = self.corpus["topology_contract"]["paired_access_paths"][
                    str(run["target_gpu"])
                ]
                bucket = index % 2
                target_key = "plane_a_link_id" if bucket == 0 else "plane_b_link_id"
                paired_key = "plane_b_link_id" if bucket == 0 else "plane_a_link_id"
                self.assertEqual(run["target_link_id"], pair[target_key])
                self.assertEqual(run["paired_link_id"], pair[paired_key])
        for run in executable:
            with (self.output / run["schedule"]["path"]).open(
                encoding="utf-8", newline=""
            ) as stream:
                truth = next(csv.DictReader(stream))
            with (
                self.output / run["schedule"]["background_flow_schedule"]["path"]
            ).open(encoding="utf-8", newline="") as stream:
                flows = list(csv.DictReader(stream))
            parameters = json.loads(truth["action_parameters_json"])
            starts = [int(row["scheduled_start_ns"]) for row in flows]
            self.assertEqual(int(truth["start_time_ns"]), min(starts))
            self.assertEqual(int(truth["end_time_ns"]), max(starts) + 1)
            self.assertEqual(
                parameters["completion_deadline_ns"],
                min(starts) + BACKGROUND_COMPLETION_DEADLINE_NS,
            )
            self.assertEqual(parameters["rdma_rto_us"], BACKGROUND_RDMA_RTO_US)
            self.assertEqual(
                parameters["rdma_retry_limit"], BACKGROUND_RDMA_RETRY_LIMIT
            )
            self.assertEqual(parameters["max_rto_retry_events"], 0)
            self.assertEqual(parameters["hash_byte_order"], "little")
            bucket = parameters["route_bucket"]
            self.assertEqual(parameters["data_plane"], "A" if bucket == 0 else "B")
            for row in flows:
                values = {
                    "src": int(row["src_rank"]),
                    "dst": int(row["dst_rank"]),
                    "sport": int(row["sport"]),
                    "dport": int(row["dport"]),
                }
                self.assertEqual(rdma_route_bucket(**values), bucket)
                self.assertEqual(rdma_route_bucket(**values, reverse=True), bucket)
            self.assertFalse(
                prepared_background_contract_issues(
                    run, truth, flows, self.corpus["topology_contract"]
                )
            )

    def test_hash_fixed_vectors_and_background_mutations_fail_closed(self) -> None:
        import struct

        data = struct.pack("<IIHH", rank_ipv4_u32(4), rank_ipv4_u32(0), 49152, 20000)
        pinned = struct.pack("<IIHH", rank_ipv4_u32(4), rank_ipv4_u32(0), 49155, 20000)
        pinned_ack = struct.pack(
            "<IIHH", rank_ipv4_u32(0), rank_ipv4_u32(4), 20000, 49155
        )
        self.assertEqual(ns3_murmur3_x86_32(data), 0x6976262F)
        self.assertEqual(ns3_murmur3_x86_32(pinned), 0x67F8E705)
        self.assertEqual(ns3_murmur3_x86_32(pinned_ack), 0xA42B39DF)
        self.assertNotEqual(
            rdma_route_bucket(src=4, dst=0, sport=49152, dport=20000),
            rdma_route_bucket(src=4, dst=0, sport=49152, dport=20000, reverse=True),
        )

        run = next(
            run for run in self.runs if run["run_id"] == "p2-congestion-incast-00"
        )
        with (self.output / run["schedule"]["path"]).open(
            encoding="utf-8", newline=""
        ) as stream:
            truth = next(csv.DictReader(stream))
        with (self.output / run["schedule"]["background_flow_schedule"]["path"]).open(
            encoding="utf-8", newline=""
        ) as stream:
            flows = list(csv.DictReader(stream))

        tampered_flows = copy.deepcopy(flows)
        first = tampered_flows[0]
        for candidate in range(49152, 65536):
            values = {
                "src": int(first["src_rank"]),
                "dst": int(first["dst_rank"]),
                "sport": candidate,
                "dport": int(first["dport"]),
            }
            if (
                rdma_route_bucket(**values) != 0
                or rdma_route_bucket(**values, reverse=True) != 0
            ):
                first["sport"] = str(candidate)
                break
        self.assertTrue(
            prepared_background_contract_issues(
                run, truth, tampered_flows, self.corpus["topology_contract"]
            )
        )

        for field, value in (
            ("bottleneck_access_link_id", run["paired_link_id"]),
            ("rdma_rto_us", 100),
            ("hash_byte_order", "big"),
        ):
            with self.subTest(field=field):
                altered_truth = dict(truth)
                parameters = json.loads(altered_truth["action_parameters_json"])
                parameters[field] = value
                altered_truth["action_parameters_json"] = json.dumps(parameters)
                self.assertTrue(
                    prepared_background_contract_issues(
                        run,
                        altered_truth,
                        flows,
                        self.corpus["topology_contract"],
                    )
                )
        altered_truth = dict(truth)
        altered_truth["end_time_ns"] = str(int(truth["end_time_ns"]) + 1)
        self.assertTrue(
            prepared_background_contract_issues(
                run, altered_truth, flows, self.corpus["topology_contract"]
            )
        )

    def test_split_is_atomic_and_holds_out_paired_gpus(self) -> None:
        split = json.loads(
            (self.output / "split_manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(split["schema_version"], SPLIT_SCHEMA)
        self.assertEqual(set(split["partitions"]), set(PARTITIONS))
        self.assertEqual(len(split["entries"]), len(self.runs))
        self.assertEqual(
            {entry["run_id"] for entry in split["entries"]},
            {run["run_id"] for run in self.runs},
        )
        holdouts = set(split["paired_holdout_gpu_ids"])
        self.assertGreaterEqual(len(holdouts), 4)
        regular = {"train", "validation", "seen_link_test"}
        self.assertFalse(
            [
                run
                for run in self.faults
                if run["partition"] in regular and run["target_gpu"] in holdouts
            ]
        )
        unseen = [run for run in self.faults if run["partition"] == "unseen_link_test"]
        self.assertEqual({run["target_gpu"] for run in unseen}, holdouts)
        self.assertTrue(all(run["target_gpu"] in holdouts for run in unseen))

    def test_feature_and_mechanism_claim_boundaries(self) -> None:
        feature = self.corpus["feature_schema"]
        forbidden = {
            "fault",
            "severity",
            "label",
            "configured",
            "injected",
            "schedule",
            "parameter",
            "target",
            "split",
            "future",
        }
        self.assertTrue(feature["model_feature_columns"])
        self.assertIn("nominal_utilization", feature["model_feature_columns"])
        self.assertNotIn("utilization", feature["model_feature_columns"])
        self.assertFalse(
            [
                column
                for column in feature["model_feature_columns"]
                if any(token in column.lower() for token in forbidden)
            ]
        )
        self.assertTrue(
            all(
                run["mechanism"]["implementation_status"] in {"PLANNED", "UNAVAILABLE"}
                and run["mechanism"]["semantic_validation"] is None
                for run in self.runs
            )
        )
        hard = [run for run in self.faults if run["fault_family"] == "hard_disconnect"]
        self.assertTrue(
            all(
                run["mechanism"]["mechanism_id"] == "physical_link_down"
                and run["mechanism"]["implementation_status"] == "PLANNED"
                for run in hard
            )
        )
        carrier_flaps = [
            run for run in self.faults if run["fault_family"] == "carrier_flap"
        ]
        self.assertTrue(
            all(
                run["mechanism"]["mechanism_id"]
                == "physical_carrier_flap_channel_epoch"
                and run["mechanism"]["implementation_status"] == "PLANNED"
                and run["schedule"]["simulator_injection_schedule"]["safe_to_execute"]
                is True
                for run in carrier_flaps
            )
        )
        true_loss = [
            run
            for run in self.faults
            if run["fault_family"] in {"random_loss", "burst_loss"}
            or (
                run["fault_family"] == "intermittent_service"
                and run["scenario"] == "intermittent-loss"
            )
        ]
        self.assertTrue(
            all(
                run["mechanism"]["mechanism_id"] == "rate_error_model_true_drop"
                and run["mechanism"]["implementation_status"] == "PLANNED"
                for run in true_loss
            )
        )
        service = [
            run
            for run in self.faults
            if run["fault_family"] == "service_degradation"
            or (
                run["fault_family"] == "intermittent_service"
                and run["scenario"] == "intermittent-service"
            )
        ]
        self.assertTrue(
            all(
                run["mechanism"]["mechanism_id"] == "egress_service_fraction"
                and run["mechanism"]["implementation_status"] == "PLANNED"
                for run in service
            )
        )
        self.assertTrue(
            all(
                run["fault_applied_ns"] is None
                and run["first_observable_effect_ns"] is None
                and run["observable"] is None
                for run in self.runs
            )
        )

    def test_all_new_runs_require_sealed_simulator_stability(self) -> None:
        blocked = [
            run
            for run in self.runs
            if str(run["schedule"]["implementation_status"]).startswith(
                "BLOCKED"
            )
        ]
        executable = [run for run in self.runs if run not in blocked]
        self.assertEqual(len(executable), 453)
        self.assertEqual(len(blocked), 9)
        self.assertTrue(all(
            run["simulator_stability"] == {
                "gate_required": True,
                "status": "PENDING_EXECUTION",
                "reason": (
                    "must be established from completed simulator artifacts"
                ),
            }
            for run in executable
        ))
        self.assertTrue(all(
            run["simulator_stability"]["gate_required"] is True
            and run["simulator_stability"]["status"] == "BLOCKED_UNSUPPORTED"
            for run in blocked
        ))
        required_examples = [
            run
            for run in executable
            if run["run_role"] == "healthy"
            or run.get("fault_family") == "bandwidth_degradation"
            or run.get("scenario") in {"allreduce_burst", "high_utilization"}
        ]
        self.assertEqual(len(required_examples), 205)
        self.assertTrue(all(
            run["simulator_stability"]["gate_required"] is True
            for run in required_examples
        ))

        manifest = self.output / "corpus_manifest.json"
        original = manifest.read_bytes()
        try:
            value = json.loads(original)
            healthy = next(
                run for run in value["runs"] if run["run_role"] == "healthy"
            )
            healthy["simulator_stability"] = {
                "gate_required": False,
                "status": "NOT_REQUIRED",
            }
            manifest.write_text(
                json.dumps(value, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            report = verify_corpus(manifest)
            check = next(
                item
                for item in report["checks"]
                if item["name"]
                == "all_run_simulator_stability_admission_declared"
            )
            self.assertEqual(check["status"], FAIL)
            self.assertEqual(check["detail"]["mismatches"][0]["run_id"], healthy[
                "run_id"
            ])
        finally:
            manifest.write_bytes(original)

    def test_check_passes_and_detects_schedule_tampering(self) -> None:
        manifest = self.output / "corpus_manifest.json"
        report = verify_corpus(manifest)
        self.assertEqual(report["status"], PASS)
        self.assertEqual(report["summary"]["failed"], 0)

        run = self.faults[0]
        schedule_path = self.output / run["schedule"]["path"]
        original = schedule_path.read_bytes()
        try:
            schedule_path.write_bytes(original + b"\n")
            report = verify_corpus(manifest)
            self.assertEqual(report["status"], FAIL)
            self.assertTrue(
                any(
                    check["name"] == "schedule_hashes" and check["status"] == FAIL
                    for check in report["checks"]
                )
            )
        finally:
            schedule_path.write_bytes(original)

    def test_generation_is_deterministic_and_refuses_overwrite(self) -> None:
        second = Path(self.temporary.name) / "second"
        generated = generate_corpus(
            **self.inputs,
            out_dir=second,
            seed=20260827,
        )
        self.assertEqual(generated["corpus_id"], self.corpus["corpus_id"])
        self.assertEqual(
            generated["schedule_set_sha256"],
            self.corpus["schedule_set_sha256"],
        )
        self.assertEqual(
            (second / "corpus_manifest.json").read_bytes(),
            (self.output / "corpus_manifest.json").read_bytes(),
        )
        with self.assertRaises(CorpusError):
            generate_corpus(
                **self.inputs,
                out_dir=second,
                seed=20260827,
            )

    def test_completion_or_legacy_workload_cannot_enter_p2_corpus(self) -> None:
        for filename in (
            "microAllReduce_16rank_p2_q1_completion_1layer_64kib.txt",
            "microAllReduce_16rank_p2_traffic.txt",
        ):
            with self.subTest(filename=filename):
                values = dict(self.inputs)
                values["workload_path"] = LIMER / "configs" / filename
                with self.assertRaisesRegex(
                    CorpusError, "not horizon-prefix qualified"
                ):
                    generate_corpus(
                        **values,
                        out_dir=Path(self.temporary.name) / f"rejected-{filename}",
                        seed=20260827,
                    )


if __name__ == "__main__":
    unittest.main()
