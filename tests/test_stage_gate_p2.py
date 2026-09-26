#!/usr/bin/env python3
"""Regression tests for the P2 corpus evidence gate."""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
TOOLS = LIMER / "tools"
RESULTS = LIMER / "results"
P1_GATE = RESULTS / "stage_gates" / "p1" / "stage_gate_p1.json"
SOURCE_LINK_MAP = (
    RESULTS / "true16_hard_fault_matrix" / "healthy" / "long"
    / "monitoring_on" / "link_map.csv"
)

sys.path.insert(0, str(TOOLS))

import evaluate_stage_p2 as p2  # noqa: E402

PREPARED = p2.PREPARED
EXECUTED = p2.EXECUTED


class P2Fixture:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.link_map = root / "link_map.csv"
        self._write_small_true16_link_map(self.link_map)
        self.contract = root / "experiment_contract.yaml"
        self.contract.write_text("contract_id: limer-true16-dual-plane-v1\n")
        self.topology = root / "true16.topology"
        self._write_small_true16_topology(self.topology)
        self.workload = root / "workload.txt"
        self.workload.write_text("fixture true16 horizon workload\n")
        self.simulator_config = root / "simulator.conf"
        self.simulator_config.write_text("FLOW_FILE empty_flow.txt\n")
        self.simulator_binary, self.simulator_library = (
            self._compile_runtime_fixture(root / "runtime-source")
        )
        self.runtime_bundle = None
        self.runtime_closure_record = None
        self.input_paths = {
            "contract": self.contract,
            "link_map": self.link_map,
            "topology": self.topology,
            "workload": self.workload,
            "simulator_config": self.simulator_config,
        }
        self.runtime_contract = p2.workload_runtime.RuntimeValidationContract(
            world_size=16,
            sample_interval_ns=34_000_000,
            max_first_full_start_ns=2_000_000,
            max_inter_full_start_gap_ns=40_000_000,
            max_tail_to_full_start_ns=40_000_000,
            max_inflight_collectives=1,
            max_traffic_silence_ns=40_000_000,
            causal_warmup_ns=100_000_000,
            expected_access_links_per_rank=2,
        )
        self.workload_report = {
            "status": "PASS",
            "qualification_profile": "horizon-prefix",
            "world_size": 16,
            "layer_count": 7,
            "collective_bytes_per_layer": 65_536,
            "sha256": p2._sha256(self.workload),
            "duration_estimate": {
                "may_be_used_as_p2_horizon_workload": True,
                "corpus_max_virtual_finish_ns": 200_000_000,
            },
        }
        self.targets, errors = p2.expected_plane_b_targets(self.link_map)
        if errors:
            raise AssertionError(errors)
        self.runs = []
        self.fault_rows = []
        self.congestion_rows = []
        self.healthy_rows = []
        self._build_runs()
        self._write_schedules()
        self._attach_schedules()
        partition_by_run = {
            item["run_id"]: item["partition"] for item in self._partitions()
        }
        for index, run in enumerate(self.runs):
            run["partition"] = partition_by_run[run["run_id"]]
            run["simulation_seed"] = 10_000 + index
            run["topology_sha256"] = p2._sha256(self.topology)
            run["workload_sha256"] = p2._sha256(self.workload)
        self.corpus_path = root / "corpus_manifest.json"
        self.split_path = root / "split_manifest.json"
        self.write_prepared()

    @staticmethod
    def _compile_runtime_fixture(directory):
        directory.mkdir(parents=True)
        library_source = directory / "fixture.c"
        program_source = directory / "main.c"
        library = directory / "libns3-p2-evaluator-fixture.so"
        executable = directory / "SimAI_simulator"
        library_source.write_text(
            "int p2_fixture_value(void) { return 42; }\n", encoding="ascii"
        )
        program_source.write_text(
            "extern int p2_fixture_value(void);\n"
            "int main(void) { return p2_fixture_value() == 42 ? 0 : 9; }\n",
            encoding="ascii",
        )
        subprocess.run(
            [
                "gcc", "-shared", "-fPIC",
                "-Wl,-soname,libns3-p2-evaluator-fixture.so",
                str(library_source), "-o", str(library),
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "gcc", str(program_source), f"-L{directory}",
                "-lns3-p2-evaluator-fixture", f"-Wl,-rpath,{directory}",
                "-o", str(executable),
            ],
            check=True,
            capture_output=True,
        )
        return executable, library

    @staticmethod
    def _write_small_true16_link_map(path):
        rows = []
        for rank in range(16):
            for host_port, switch in ((2, 16), (3, 17)):
                rows.append({
                    "link_id": f"L{rank}-{switch}",
                    "src_node": rank,
                    "dst_node": switch,
                    "src_type": "HOST",
                    "dst_type": "SWITCH",
                    "src_port": host_port,
                    "dst_port": rank + 1,
                    "link_class": "ACCESS",
                    "bandwidth_bps": 100_000_000_000,
                    "delay_ns": 500,
                })
        P2Fixture._write_csv(path, rows)

    @staticmethod
    def _write_small_true16_topology(path):
        lines = ["18 4 0 2 32 TEST_GPU", "16 17"]
        for rank in range(16):
            lines.extend([
                f"{rank} 16 100Gbps 0.0005ms 0",
                f"{rank} 17 100Gbps 0.0005ms 0",
            ])
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _base_run(self, run_id, class_label, event_id):
        return {
            "run_id": run_id,
            "run_role": "p2_corpus",
            "class_label": class_label,
            "split_group_id": "group-" + run_id,
            "virtual_start_ns": 0,
            "virtual_finish_ns": 200_000_000,
            "event_id": event_id,
            "execution_status": "PREPARED",
            "mechanism": {
                "mechanism_id": "no_physical_injection",
                "implementation_status": "PLANNED",
                "semantic_validation": None,
            },
            "simulator_stability": {
                "gate_required": True,
                "status": "PENDING_EXECUTION",
                "reason": (
                    "must be established from completed simulator artifacts"
                ),
            },
            "artifacts": {},
        }

    def _add_gray(self, family, parameters):
        index = sum(run["class_label"] == "GRAY_FAULT" for run in self.runs)
        gpu = index % 16
        run_id = f"gray-{index:03d}-{family}"
        event_id = "event-" + run_id
        run = self._base_run(run_id, "GRAY_FAULT", event_id)
        run.update({
            "gray_category": {
                "bandwidth_degradation": "capacity",
                "random_loss": "loss",
                "burst_loss": "loss",
                "service_degradation": "service",
                "intermittent_service": "intermittent",
            }[family],
            "fault_family": family,
            "target_gpu": gpu,
            "target_link_id": self.targets[gpu],
            "parameters": parameters,
            "severity": {
                "name": next(iter(parameters)),
                "value": next(iter(parameters.values())),
                "unit": "contract",
            },
            "fault_applied_ns": 102_000_000,
            "fault_scheduled_onset_ns": 102_000_000,
            "duration_ns": 10_000_000,
            "recovery_evaluation": False,
            "observable": None,
            "first_observable_effect_ns": None,
        })
        mechanism_ids = {
            "bandwidth_degradation": "verified_capacity_shaper",
            "random_loss": "verified_random_packet_drop",
            "burst_loss": "verified_burst_packet_drop",
            "service_degradation": "verified_service_impairment",
            "intermittent_service": "verified_intermittent_impairment",
        }
        run["mechanism"] = {
            "mechanism_id": mechanism_ids[family],
            "implementation_status": "PLANNED",
            "semantic_validation": None,
        }
        self.runs.append(run)
        self.fault_rows.append({
            "event_id": event_id,
            "fault_family": family,
            "target_gpu": gpu,
            "target_link_id": self.targets[gpu],
            "start_time_ns": 102_000_000,
            "end_time_ns": 112_000_000,
            "severity_name": run["severity"]["name"],
            "severity_value": run["severity"]["value"],
            "severity_unit": "contract",
            "shape": parameters.get("shape", ""),
        })

    def _build_runs(self):
        healthy = self._base_run("healthy-000", "HEALTHY", "event-healthy-000")
        self.runs.append(healthy)
        self.healthy_rows.append({
            "event_id": healthy["event_id"], "scenario": "healthy",
            "start_time_ns": 0, "end_time_ns": 200_000_000,
            "truth_source": "predeclared_no_fault",
        })
        for index, scenario in enumerate(sorted(p2.REQUIRED_CONGESTION_SCENARIOS)):
            run_id = f"congestion-{index:02d}-{scenario}"
            event_id = "event-" + run_id
            run = self._base_run(run_id, "CONGESTION", event_id)
            run.update({
                "scenario": scenario,
                "duration_ns": 10_000_000,
                "fault_scheduled_onset_ns": 102_000_000,
            })
            run["mechanism"] = {
                "mechanism_id": f"scheduled_{scenario}",
                "implementation_status": "PLANNED",
                "semantic_validation": None,
            }
            self.runs.append(run)
            self.congestion_rows.append({
                "event_id": event_id, "scenario": scenario,
                "start_time_ns": 102_000_000, "end_time_ns": 112_000_000,
                "truth_source": "predeclared_workload_schedule",
            })

        for fraction in (0.8, 0.5, 0.2):
            self._add_gray("bandwidth_degradation", {
                "remaining_nominal_capacity_fraction": fraction, "shape": "step",
            })
        for fraction, duration in zip(
                (0.8, 0.5, 0.2), (10_000_000, 50_000_000, 100_000_000)):
            self._add_gray("bandwidth_degradation", {
                "remaining_nominal_capacity_fraction": fraction, "shape": "ramp",
                "ramp_duration_ns": duration,
            })
        for probability in (0.0001, 0.001, 0.005, 0.01, 0.05):
            self._add_gray("random_loss", {
                "packet_error_probability": probability,
            })
        for shape in ("periodic", "random"):
            self._add_gray("burst_loss", {"shape": shape})
        for effect in sorted(p2.REQUIRED_SERVICE_EFFECTS):
            self._add_gray("service_degradation", {"effects": [effect]})
        for impairment in sorted(p2.REQUIRED_INTERMITTENT_IMPAIRMENTS):
            self._add_gray("intermittent_service", {
                "impairment": impairment, "carrier_state": "up",
            })

    @staticmethod
    def _write_csv(path, rows):
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
        with path.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _blank_runtime_row(columns):
        return {column: "" for column in columns}

    def _runtime_switch_rows(self, run):
        runtime = p2.workload_runtime
        rows = []
        target = run.get("target_link_id") or "L0-17"
        event_run = run["class_label"] != "HEALTHY"
        gray = run["class_label"] == "GRAY_FAULT"
        for sample, timestamp in enumerate(
            (34_000_000, 68_000_000, 102_000_000, 136_000_000, 170_000_000),
            start=1,
        ):
            for rank in range(16):
                for host_port, switch in ((2, 16), (3, 17)):
                    link_id = f"L{rank}-{switch}"
                    for direction in ("tx", "rx"):
                        row = self._blank_runtime_row(runtime.SWITCH_COLUMNS)
                        row.update({
                            "run_id": run["run_id"],
                            "timestamp_ns": timestamp,
                            "switch_id": switch,
                            "port_id": rank + 1,
                            "link_id": link_id,
                            "peer_node_id": rank,
                            "direction": direction,
                            "queue_packets": 0,
                            "queue_bytes": 0,
                            "max_queue_packets": 0,
                            "max_queue_bytes": 0,
                            "configured_bandwidth_bps": 100_000_000_000,
                            "observed_throughput_bps": 1_000_000,
                            "utilization": 0.00001,
                            "node_type": "SWITCH",
                            "link_state": "up",
                            "max_queue_timestamp_ns": 0,
                        })
                        for column in runtime.SWITCH_CUMULATIVE_COLUMNS:
                            row[column] = 0
                        if direction == "tx":
                            row["tx_packets"] = sample
                            row["tx_bytes"] = sample * 1000 + rank
                        else:
                            row["rx_packets"] = sample
                            row["rx_bytes"] = sample * 1000 + rank
                        if event_run and link_id == target and timestamp >= 102_000_000:
                            row["max_queue_packets"] = 1
                            row["max_queue_bytes"] = 100
                            row["max_queue_timestamp_ns"] = 102_000_000
                            if direction == "tx" and timestamp == 102_000_000:
                                row["queue_packets"] = 1
                                row["queue_bytes"] = 100
                            if gray:
                                row["dropped_packets"] = 1
                                row["drop_bytes"] = 64
                                row["link_errors"] = 1
                            else:
                                row["ecn_marks"] = 1
                        rows.append(row)
        return rows

    def _runtime_nic_rows(self, run):
        runtime = p2.workload_runtime
        rows = []
        for sample, timestamp in enumerate(
            (34_000_000, 68_000_000, 102_000_000, 136_000_000, 170_000_000),
            start=1,
        ):
            for rank in range(16):
                for nic_id, switch in ((2, 16), (3, 17)):
                    row = self._blank_runtime_row(runtime.NIC_COLUMNS)
                    row.update({
                        "run_id": run["run_id"],
                        "timestamp_ns": timestamp,
                        "node_id": rank,
                        "rank_id": rank,
                        "nic_id": nic_id,
                        "link_id": f"L{rank}-{switch}",
                        "queue_packets": 0,
                        "queue_bytes": 0,
                        "max_queue_packets": 0,
                        "max_queue_bytes": 0,
                        "max_queue_timestamp_ns": 0,
                        "configured_bandwidth_bps": 100_000_000_000,
                        "link_state": "up",
                        "utilization": 0.00001,
                    })
                    for column in runtime.NIC_CUMULATIVE_COLUMNS:
                        row[column] = 0
                    row["tx_packets"] = sample
                    row["tx_bytes"] = sample * 500 + rank
                    row["rx_packets"] = sample
                    row["rx_bytes"] = sample * 500 + rank
                    rows.append(row)
        return rows

    def _runtime_transaction_rows(self, run):
        rows = []
        starts = (
            1_000_000, 35_000_000, 69_000_000, 103_000_000,
            137_000_000, 171_000_000, 199_000_000,
        )
        for sequence, start in enumerate(starts):
            for rank in range(16):
                rows.append({
                    "run_id": run["run_id"],
                    "timestamp_ns": start,
                    "collective_seq": sequence,
                    "attempt": 0,
                    "layer_num": sequence,
                    "message_size_bytes": 65_536,
                    "event": "START",
                    "rank_id": rank,
                    "world_size": 16,
                    "ready_ranks": rank + 1,
                    "result_digest": "",
                    "status": "in_flight",
                })
            if sequence == len(starts) - 1:
                continue
            for rank in range(16):
                rows.append({
                    "run_id": run["run_id"],
                    "timestamp_ns": start + 500_000,
                    "collective_seq": sequence,
                    "attempt": 0,
                    "layer_num": sequence,
                    "message_size_bytes": 65_536,
                    "event": "LOCAL_READY",
                    "rank_id": rank,
                    "world_size": 16,
                    "ready_ranks": rank + 1,
                    "result_digest": "",
                    "status": "ready",
                })
            rows.append({
                "run_id": run["run_id"],
                "timestamp_ns": start + 600_000,
                "collective_seq": sequence,
                "attempt": 0,
                "layer_num": sequence,
                "message_size_bytes": 65_536,
                "event": "COMMIT",
                "rank_id": "",
                "world_size": 16,
                "ready_ranks": 16,
                "result_digest": f"fixture-digest-{sequence}",
                "status": "exactly_once",
            })
        return rows

    def _runtime_flow_rows(self, run):
        return [
            {
                "run_id": run["run_id"],
                "collective_id": f"flow-{rank}",
                "iteration_id": "",
                "layer_id": "",
                "collective_type": "ALLREDUCE",
                "algorithm": "NcclFlowModel",
                "rank_id": rank,
                "world_size": 16,
                "message_size_bytes": 4096,
                "start_time_ns": 1_000_000,
                "finish_time_ns": 2_000_000,
                "duration_ns": 1_000_000,
                "status": "ok",
            }
            for rank in range(16)
        ]

    def _write_schedules(self):
        self.fault_schedule = self.root / "fault_schedules.csv"
        self.congestion_schedule = self.root / "congestion_schedules.csv"
        self.healthy_schedule = self.root / "healthy_schedules.csv"
        self._write_csv(self.fault_schedule, self.fault_rows)
        self._write_csv(self.congestion_schedule, self.congestion_rows)
        self._write_csv(self.healthy_schedule, self.healthy_rows)

    def _attach_schedules(self):
        refs = {
            "GRAY_FAULT": (
                self.fault_schedule, "fault", "predeclared_fault_schedule"),
            "CONGESTION": (
                self.congestion_schedule, "congestion",
                "predeclared_workload_schedule"),
            "HEALTHY": (
                self.healthy_schedule, "healthy", "predeclared_no_fault"),
        }
        for run in self.runs:
            path, kind, truth = refs[run["class_label"]]
            run["schedule"] = {
                "path": path.name,
                "sha256": p2._sha256(path),
                "event_id": run["event_id"],
                "kind": kind,
                "truth_source": truth,
                "independent_of_features": True,
                "generated_before_run": True,
            }

    def _partitions(self):
        entries = []
        ordinary_index = 0
        for run in self.runs:
            gpu = run.get("target_gpu")
            if run["class_label"] == "GRAY_FAULT" and gpu in {12, 13, 14, 15}:
                partition = "unseen_link_test"
            elif run["class_label"] == "CONGESTION" and run["scenario"] == "pfc":
                partition = "ood_stress"
            else:
                partition = ("train", "validation", "seen_link_test")[ordinary_index % 3]
                ordinary_index += 1
            entries.append({"run_id": run["run_id"], "partition": partition})
        return entries

    def _input_artifacts(self):
        return {
            name: {
                "path": path.name,
                "sha256": p2._sha256(path),
            }
            for name, path in self.input_paths.items()
        }

    def _corpus_id(self, inputs):
        material = {
            "identity_schema": p2.CORPUS_IDENTITY_SCHEMA,
            "contract_sha256": inputs["contract"]["sha256"],
            "link_map_sha256": inputs["link_map"]["sha256"],
            "topology_sha256": inputs["topology"]["sha256"],
            "workload_sha256": inputs["workload"]["sha256"],
            "simulator_config_sha256": inputs["simulator_config"]["sha256"],
            "seed": 23,
            "holdouts": [12, 13, 14, 15],
            "runs": [p2._run_identity_entry(run) for run in self.runs],
        }
        return "p2-" + p2._canonical_hash(material)[:24]

    def corpus(self, status="PREPARED"):
        inputs = self._input_artifacts()
        return {
            "schema_version": "limer.p2-corpus-manifest.v1",
            "contract_id": p2.CONTRACT_ID,
            "status": status,
            "identity_schema": p2.CORPUS_IDENTITY_SCHEMA,
            "generation_seed": 23,
            "corpus_id": self._corpus_id(inputs),
            "input_artifacts": inputs,
            "split_manifest": {
                "paired_holdout_gpu_ids": [12, 13, 14, 15],
            },
            "schedule_set_sha256": p2._canonical_hash([
                p2._schedule_identity_tuple(run) for run in self.runs
            ]),
            "topology": {"link_map": {
                "path": self.link_map.name,
                "sha256": p2._sha256(self.link_map),
            }},
            "workload_qualification": {
                "static_qualification_status": "PASS",
                "static_qualification_profile": "horizon-prefix",
                "static_report_sha256": p2._canonical_hash(
                    self.workload_report
                ),
                "static_report": self.workload_report,
                "causal_warmup_required_ns": 100_000_000,
            },
            "feature_schema": {
                "model_feature_columns": [
                    "tx_rate_bps", "queue_bytes", "drop_delta",
                    "nominal_bandwidth_bps",
                ],
                "identifier_columns": ["run_id", "timestamp_ns", "link_id"],
                "label_columns": ["class_label"],
            },
            "runs": self.runs,
        }

    def split(self, corpus_hash, status="PREPARED", corpus_id=None):
        return {
            "schema_version": "limer.p2-split-manifest.v1",
            "contract_id": p2.CONTRACT_ID,
            "status": status,
            "corpus_id": corpus_id or self.corpus(status)["corpus_id"],
            "corpus_manifest_sha256": corpus_hash,
            "atomic_unit": "complete_simulation_run",
            "generated_before_training": True,
            "immutable_after_training": True,
            "paired_holdout_gpu_ids": [12, 13, 14, 15],
            "entries": self._partitions(),
        }

    def _write_manifests(self, corpus, split_status):
        self.corpus_path.write_text(
            json.dumps(corpus, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        split = self.split(
            p2._sha256(self.corpus_path), split_status, corpus.get("corpus_id")
        )
        self.split_path.write_text(
            json.dumps(split, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def write_prepared(self):
        self._write_manifests(self.corpus(PREPARED), PREPARED)

    def rewrite_prepared(self, corpus):
        self._write_manifests(corpus, PREPARED)

    def _prepare_runtime_bundle(self):
        self.runtime_bundle = p2.runtime_bundle.seal_runtime_bundle(
            self.root, self.simulator_binary
        )
        self.runtime_closure_record = p2._canonical_runtime_record(
            self.root.resolve(), self.runtime_bundle
        )

    def _runtime_execution_value(self, run_id):
        bundle = self.runtime_bundle
        record = self.runtime_closure_record
        if bundle is None or record is None:
            raise AssertionError("runtime bundle was not prepared")
        project_dependencies = [
            {
                "soname": item["soname"],
                "path": str(bundle.root / item["execution_path"]),
                "sha256": item["sha256"],
                "size_bytes": item["size_bytes"],
            }
            for item in bundle.manifest["project_dependencies"]
        ]
        system_dependencies = [
            {
                "soname": item["soname"],
                "path": item["resolved_path"],
                "sha256": item["sha256"],
                "size_bytes": item["size_bytes"],
            }
            for item in bundle.manifest["system_dependencies"]
        ]
        loader = {
            "status": "PASS",
            "runtime_bundle_identity_sha256": bundle.identity_sha256,
            "executable_path": str(bundle.executable),
            "executable_sha256": record["bundle_executable_sha256"],
            "project_dependency_count": len(project_dependencies),
            "project_dependencies": project_dependencies,
            "system_dependency_count": len(system_dependencies),
            "system_dependencies": system_dependencies,
            "virtual_dependencies": list(
                bundle.manifest["virtual_dependencies"]
            ),
            "checked_at": "fixture",
            "phase": "execution_root_prepare",
        }
        pre_run = {
            "status": "PASS",
            "phase": "before_run",
            "checked_at": "fixture",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
            "loader_preflight_identity_sha256": bundle.identity_sha256,
        }
        process = {
            "pid": 12345,
            "executable": str(bundle.executable),
            "runtime_bundle_identity_sha256": bundle.identity_sha256,
            "project_dependency_count": len(project_dependencies),
            "project_dependencies": project_dependencies,
            "system_dependency_count": len(system_dependencies),
            "system_dependencies": system_dependencies,
            "status": "PASS",
            "inspection_attempts": 1,
            "inspection_elapsed_ms": 0.1,
        }
        post_run = {
            "status": "PASS",
            "phase": "after_run",
            "checked_at": "fixture",
            "source_closure_status": "PASS",
            "bundle_integrity_status": "PASS",
        }
        loader_environment = {
            "runtime_bundle_identity_sha256": bundle.identity_sha256,
            "runtime_bundle_path": str(bundle.root),
            "cleared_inherited_loader_variables": [],
            "LD_LIBRARY_PATH": str(bundle.lib_directory),
            "LD_PRELOAD": None,
        }
        return {
            "schema_version": "limer.runtime-execution-evidence.v1",
            "status": "PASS",
            "run_id": run_id,
            "runtime_closure": record,
            "loader_preflight": loader,
            "pre_run_verification": pre_run,
            "loader_environment": loader_environment,
            "process_mapping_verification": process,
            "post_run_verification": post_run,
            "errors": [],
        }

    def _write_route_artifacts(
        self, run, frozen_topology, frozen_link_map, runtime_link_map
    ):
        route = p2.route_candidate_runtime
        bound = route._read_bound_artifact(
            frozen_link_map,
            "fixture_frozen_link_map",
            p2._sha256(frozen_link_map),
        )
        topology = route._parse_link_map(bound)
        topology = route._bind_topology_file(
            topology,
            route._read_bound_artifact(
                frozen_topology,
                "fixture_frozen_topology",
                p2._sha256(frozen_topology),
            ),
        )
        expected = route.reconstruct_expected_rows(topology, run["run_id"])
        raw_path = runtime_link_map.parent / "ecmp_route_candidates.csv"
        self._write_csv(raw_path, [
            {
                "run_id": row.run_id,
                "node_id": row.node_id,
                "node_type": row.node_type,
                "destination_node_id": row.destination_node_id,
                "destination_ip": row.destination_ip,
                "candidate_index": row.candidate_index,
                "candidate_count": row.candidate_count,
                "egress_port_id": row.egress_port_id,
                "next_hop_node_id": row.next_hop_node_id,
                "status": row.status,
            }
            for row in expected
        ])
        report = route.validate_route_candidate_evidence(
            route_candidates_path=raw_path,
            topology_path=frozen_topology,
            frozen_link_map_path=frozen_link_map,
            runtime_link_map_path=runtime_link_map,
            expected_run_id=run["run_id"],
            expected_topology_sha256=p2._sha256(frozen_topology),
            expected_frozen_link_map_sha256=p2._sha256(frozen_link_map),
            expected_runtime_link_map_sha256=p2._sha256(runtime_link_map),
        )
        if report["status"] != "PASS":
            raise AssertionError(report["errors"])
        report_path = runtime_link_map.parent / (
            "ecmp_route_candidate_validation.json"
        )
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return raw_path, report_path, report

    def write_executed(self, include_artifacts=True):
        feature_path = self.root / "feature_schema.json"
        feature_value = self.corpus()["feature_schema"]
        feature_path.write_text(
            json.dumps(feature_value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if include_artifacts:
            self._prepare_runtime_bundle()
            prepared_bytes = self.corpus_path.read_bytes()
            prepared_corpus = json.loads(prepared_bytes)
            prepared_runs = {
                item["run_id"]: item for item in prepared_corpus["runs"]
            }
            for run in self.runs:
                run_dir = self.root / "runs" / run["run_id"]
                run_dir.mkdir(parents=True)
                inputs_dir = run_dir / "inputs"
                inputs_dir.mkdir()
                source_corpus = inputs_dir / "corpus_manifest.json"
                source_corpus.write_bytes(prepared_bytes)
                input_bindings = {}
                for name, source in self.input_paths.items():
                    copied = inputs_dir / f"{name}{source.suffix}"
                    shutil.copy2(source, copied)
                    input_bindings[name] = {
                        "source_path": str(source.resolve()),
                        "execution_copy": copied.relative_to(run_dir).as_posix(),
                        "sha256": p2._sha256(source),
                    }
                runtime_config = inputs_dir / "simulator_config.runtime.conf"
                shutil.copy2(self.simulator_config, runtime_config)
                (run_dir / "run.log").write_text("fixture runner log\n")
                (run_dir / "exit_code.txt").write_text("0\n")
                schedule_hash = run["schedule"]["sha256"]
                runtime_link_map = run_dir / "link_map.csv"
                shutil.copy2(self.link_map, runtime_link_map)
                route_raw, route_report_path, route_report = (
                    self._write_route_artifacts(
                        run,
                        inputs_dir / "topology.topology",
                        inputs_dir / "link_map.csv",
                        runtime_link_map,
                    )
                )
                switch = run_dir / "switch_telemetry.csv"
                self._write_csv(switch, self._runtime_switch_rows(run))
                nic = run_dir / "nic_telemetry.csv"
                self._write_csv(nic, self._runtime_nic_rows(run))
                transaction = run_dir / "collective_transaction.csv"
                self._write_csv(
                    transaction, self._runtime_transaction_rows(run)
                )
                collective = run_dir / "collective_telemetry.csv"
                self._write_csv(collective, self._runtime_flow_rows(run))
                lifecycle = run_dir / "run_lifecycle.csv"
                lifecycle.write_text(
                    "run_id,event,scheduled_ns,actual_ns,finished_ranks,"
                    "world_size,status\n"
                    f"{run['run_id']},observation_horizon,200000000,"
                    "200000000,0,16,"
                    "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE\n",
                    encoding="utf-8",
                )
                fault_application = run_dir / "fault_application_telemetry.csv"
                fault_application.write_text(
                    "run_id,fault_id,fault_type,target_link_id,transition,"
                    "scheduled_ns,actual_ns,parameter_before,parameter_after,"
                    "mechanism,rng_stream,status\n",
                    encoding="utf-8",
                )
                runtime_qualification = run_dir / (
                    "workload_runtime_qualification.json"
                )
                runtime_value = (
                    p2.workload_runtime.validate_runtime_qualification(
                        run=prepared_runs[run["run_id"]],
                        workload_report=self.workload_report,
                        paths=p2.workload_runtime.RuntimePaths(
                            workload=inputs_dir / "workload.txt",
                            link_map=runtime_link_map,
                            switch_telemetry=switch,
                            nic_telemetry=nic,
                            collective_transaction=transaction,
                            collective_telemetry=collective,
                            run_lifecycle=lifecycle,
                        ),
                        causal_warmup_ns=100_000_000,
                        contract_override=self.runtime_contract,
                    )
                )
                if runtime_value["status"] != "PASS":
                    raise AssertionError(runtime_value["errors"])
                runtime_qualification.write_text(
                    json.dumps(runtime_value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                run["execution_status"] = "COMPLETE"
                run["mechanism"]["implementation_status"] = "VERIFIED"
                family = run.get("fault_family")
                parameters = run.get("parameters", {})
                observed_effects = []
                if family == "bandwidth_degradation":
                    observed_effects = ["throughput_degradation"]
                elif family == "service_degradation":
                    observed_effects = list(parameters.get("effects", []))
                elif family == "intermittent_service":
                    observed_effects = [{
                        "capacity": "throughput_degradation",
                        "loss": "packet_drop",
                        "service": "queue_growth",
                    }[parameters["impairment"]]]
                semantic = run_dir / "semantic_validation.json"
                semantic_value = {
                    "schema_version": "limer.p2-run-semantics.v1",
                    "run_id": run["run_id"],
                    "mechanism_id": run["mechanism"]["mechanism_id"],
                    "status": "PASS",
                    "source_artifact_sha256": p2._sha256(switch),
                    "source_artifacts_sha256": {
                        "fault_application": p2._sha256(fault_application),
                    },
                    "checks": [{"name": "fixture_semantics", "status": "PASS"}],
                    "injected_physical_fault": False,
                    "target_link_state_during_event": "up",
                    "observed_effects": observed_effects,
                    "impairment": parameters.get("impairment", ""),
                    "packet_disposition": (
                        "dropped" if family in {"random_loss", "burst_loss"}
                        or (family == "intermittent_service"
                            and parameters.get("impairment") == "loss")
                        else "not_applicable"
                    ),
                    "recoverable_error_proxy": False,
                }
                semantic.write_text(
                    json.dumps(semantic_value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                runtime_execution = run_dir / "runtime_execution_evidence.json"
                runtime_execution_value = self._runtime_execution_value(
                    run["run_id"]
                )
                runtime_execution.write_text(
                    json.dumps(
                        runtime_execution_value, indent=2, sort_keys=True
                    ) + "\n",
                    encoding="utf-8",
                )
                allocator_raw = run_dir / "training_source_port_allocator.csv"
                allocator_raw.write_text(
                    "run_id,interval_first,interval_end_exclusive,capacity,"
                    "allocations,releases,reuses,active_at_stop,peak_active,"
                    "pair_count,pairs_with_reuse,max_pair_allocations,"
                    "max_pair_reuses,external_conflicts,exhaustions,"
                    "invariant_errors,min_allocated_port,max_allocated_port,"
                    "status\n"
                    f"{run['run_id']},10000,49152,39152,1,0,0,1,1,1,0,1,"
                    "0,0,0,0,10000,10000,PASS\n",
                    encoding="utf-8",
                )
                allocator_value = (
                    p2.training_source_port_runtime.validate_allocator_evidence(
                        allocator_raw,
                        expected_run_id=run["run_id"],
                        require_reuse=False,
                    )
                )
                allocator_report = run_dir / (
                    "training_source_port_allocator_validation.json"
                )
                allocator_report.write_text(
                    json.dumps(allocator_value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                prepared_run = prepared_runs[run["run_id"]]
                prepared_stability = prepared_run.get(
                    "simulator_stability", {}
                )
                stability_gate_required = (
                    isinstance(prepared_stability, dict)
                    and prepared_stability.get("gate_required") is True
                )
                resource_observation = {
                    "schema_version": "limer.process-resource-observation.v1",
                    "sampling_interval_ms": 100,
                    "sample_count": 1,
                    "observed_peak_rss_kib": 1,
                    "observed_peak_virtual_size_kib": 1,
                    "cgroup_memory_before": {"available": False},
                    "cgroup_memory_after": {"available": False},
                    "cgroup_memory_event_delta": {},
                    "oom_kill_observed_during_attempt": False,
                    "exit_signal": None,
                }
                stability_path = run_dir / "simulator_stability.json"
                stability_value = None
                if stability_gate_required:
                    stability_sources = {
                        "exit_code": run_dir / "exit_code.txt",
                        "run_lifecycle": lifecycle,
                        "switch_telemetry": switch,
                        "nic_telemetry": nic,
                        "collective_telemetry": collective,
                        "collective_transaction": transaction,
                        "fault_application_telemetry": fault_application,
                        "runtime_execution_evidence": runtime_execution,
                        "semantic_validation": semantic,
                        "workload_runtime_qualification": runtime_qualification,
                        "ecmp_route_candidates": route_raw,
                        "ecmp_route_candidate_validation": route_report_path,
                        "training_source_port_allocator": allocator_raw,
                        "training_source_port_allocator_validation": (
                            allocator_report
                        ),
                    }
                    stability_basis = {
                        "planned_stability_status": "PENDING_EXECUTION",
                        "exit_code": 0,
                        "process_status": "EXITED_ZERO",
                        "timed_out": False,
                        "interrupted": False,
                        "oom_kill_observed_during_attempt": False,
                        "execution_status": "COMPLETE",
                        "observation_status": "OBSERVATION_WINDOW_COMPLETE",
                        "workload_completed": False,
                        "runtime_execution_status": "PASS",
                        "semantic_validation_status": "PASS",
                        "workload_runtime_qualification_status": "PASS",
                        "ecmp_route_candidate_validation_status": "PASS",
                        "training_source_port_allocator_validation_status": (
                            "PASS"
                        ),
                        "runtime_closure_identity_sha256": (
                            self.runtime_closure_record["identity_sha256"]
                        ),
                        "simulator_worker_threads": 16,
                        "source_artifacts_sha256": {
                            name: p2._sha256(path)
                            for name, path in sorted(stability_sources.items())
                        },
                    }
                    stability_value = {
                        "schema_version": p2.SIMULATOR_STABILITY_SCHEMA,
                        "status": "PASS",
                        "run_id": run["run_id"],
                        "fault_family": prepared_run.get("fault_family"),
                        "gate_required": True,
                        "planned_run_sha256": p2._canonical_hash(prepared_run),
                        "virtual_finish_ns": prepared_run["virtual_finish_ns"],
                        "evidence_basis": stability_basis,
                        "evidence_basis_sha256": p2._canonical_hash(
                            stability_basis
                        ),
                        "checks": [
                            {"name": name, "status": "PASS"}
                            for name in p2.SIMULATOR_STABILITY_CHECK_NAMES
                        ],
                        "errors": [],
                    }
                    stability_path.write_text(
                        json.dumps(stability_value, indent=2, sort_keys=True)
                        + "\n",
                        encoding="utf-8",
                    )
                lifecycle_manifest = {
                    "path": "run_lifecycle.csv",
                    "parse_status": "PARSED",
                    "workload_status": "WORKLOAD_INCOMPLETE",
                    "workload_complete_ns": None,
                    "observation_status": "OBSERVATION_WINDOW_COMPLETE",
                    "observation_detail": (
                        "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE"
                    ),
                    "observation_scheduled_ns": run["virtual_finish_ns"],
                    "observation_actual_ns": run["virtual_finish_ns"],
                    "finished_ranks": 0,
                    "world_size": 16,
                }
                artifact_inventory = p2._artifact_inventory(run_dir)
                run_manifest = run_dir / "run_manifest.json"
                manifest_value = {
                    "schema_version": p2.RUN_MANIFEST_SCHEMA,
                    "status": "EXECUTED",
                    "execution_status": "COMPLETE",
                    "evidence_state": "OBSERVATION_WINDOW_COMPLETE",
                    "lifecycle_status": (
                        "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE"
                    ),
                    "workload_completed": False,
                    "run_id": run["run_id"],
                    "corpus_id": prepared_corpus["corpus_id"],
                    "corpus_manifest_path": str(self.corpus_path.resolve()),
                    "corpus_manifest_sha256": p2._sha256(source_corpus),
                    "schedule_sha256": schedule_hash,
                    "injection_schedule_sha256": None,
                    "background_flow_schedule_sha256": None,
                    "planned_run_sha256": p2._canonical_hash(
                        prepared_runs[run["run_id"]]
                    ),
                    "partition": run["partition"],
                    "class_label": run["class_label"],
                    "simulation_seed": run["simulation_seed"],
                    "virtual_finish_ns": run["virtual_finish_ns"],
                    "hard_event_detector_enabled": False,
                    "recovery_action_enabled": False,
                    "rdma_recovery_transport_enabled": False,
                    "workload_runtime_qualification_status": "PASS",
                    "workload_runtime_qualification_sha256": p2._sha256(
                        runtime_qualification
                    ),
                    "background_transport_contract": None,
                    "telemetry_interval_us": 1000,
                    "simulator_worker_threads": 16,
                    "world_size": 16,
                    "exit_code": 0,
                    "input_bindings": input_bindings,
                    "runtime_config_binding": {
                        "path": runtime_config.relative_to(run_dir).as_posix(),
                        "sha256": p2._sha256(runtime_config),
                        "derived_from_sha256": p2._sha256(
                            self.simulator_config
                        ),
                        "isolated_paths": {},
                    },
                    "schedule_bindings": {
                        "truth": {
                            "sha256": schedule_hash,
                            "passed_to_simulator": False,
                        },
                    },
                    "runtime_closure": self.runtime_closure_record,
                    "runtime_execution_status": "PASS",
                    "runtime_loader_preflight": runtime_execution_value[
                        "loader_preflight"
                    ],
                    "runtime_pre_run_verification": runtime_execution_value[
                        "pre_run_verification"
                    ],
                    "runtime_process_mapping_verification": (
                        runtime_execution_value[
                            "process_mapping_verification"
                        ]
                    ),
                    "runtime_post_run_verification": runtime_execution_value[
                        "post_run_verification"
                    ],
                    "runtime_execution_evidence_sha256": p2._sha256(
                        runtime_execution
                    ),
                    "simulator_stability_gate_required": (
                        stability_gate_required
                    ),
                    "simulator_stability_status": (
                        "PASS" if stability_gate_required else "NOT_REQUIRED"
                    ),
                    "simulator_stability_sha256": (
                        p2._sha256(stability_path)
                        if stability_gate_required else None
                    ),
                    "simulator": {
                        "path": self.runtime_closure_record[
                            "bundle_executable_path"
                        ],
                        "source_path": str(self.simulator_binary.resolve()),
                        "execution_copy": None,
                        "sha256": p2._sha256(self.simulator_binary),
                        "runtime_closure_identity_sha256": (
                            self.runtime_closure_record["identity_sha256"]
                        ),
                        "runtime_bundle_manifest_sha256": (
                            self.runtime_closure_record[
                                "bundle_manifest_sha256"
                            ]
                        ),
                        "argv": [
                            str(self.runtime_bundle.executable), "-t", "16"
                        ],
                        "worker_threads": 16,
                        "exit_code": 0,
                        "wall_timeout_seconds": 1,
                    },
                    "execution": {
                        "process_status": "EXITED_ZERO",
                        "timed_out": False,
                        "interrupted": False,
                        "resource_observation": resource_observation,
                    },
                    "lifecycle": lifecycle_manifest,
                    "background_flow_evidence": None,
                    "semantic_validation_status": "PASS",
                    "semantic_validation_sha256": p2._sha256(semantic),
                    "ecmp_route_candidate_validation_status": "PASS",
                    "ecmp_route_candidates_sha256": p2._sha256(route_raw),
                    "ecmp_route_candidate_validation_sha256": p2._sha256(
                        route_report_path
                    ),
                    "ecmp_route_candidate_report_sha256": route_report[
                        "report_sha256"
                    ],
                    "training_source_port_allocator_validation_status": "PASS",
                    "training_source_port_allocator_sha256": p2._sha256(
                        allocator_raw
                    ),
                    "training_source_port_allocator_validation_sha256": (
                        p2._sha256(allocator_report)
                    ),
                    "training_source_port_allocator_report_sha256": (
                        allocator_value["report_sha256"]
                    ),
                    "artifacts": artifact_inventory,
                    "artifact_set_sha256": p2._canonical_hash(
                        artifact_inventory
                    ),
                    "publish_protocol": "pending_directory_then_atomic_rename",
                }
                run_manifest.write_text(
                    json.dumps(manifest_value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                (run_dir / "run_manifest.sha256").write_text(
                    f"{p2._sha256(run_manifest)}  run_manifest.json\n",
                    encoding="ascii",
                )
                run["artifacts"] = {
                    "run_manifest": self._ref(run_manifest),
                    "link_map": self._ref(runtime_link_map),
                    "switch_telemetry": self._ref(switch),
                    "nic_telemetry": self._ref(nic),
                    "collective_telemetry": self._ref(collective),
                    "collective_transaction": self._ref(transaction),
                    "run_lifecycle": self._ref(lifecycle),
                    "workload_runtime_qualification": self._ref(
                        runtime_qualification
                    ),
                    "semantic_validation": self._ref(semantic),
                    "fault_application_telemetry": self._ref(
                        fault_application
                    ),
                    "runtime_execution_evidence": self._ref(
                        runtime_execution
                    ),
                    "ecmp_route_candidates": self._ref(route_raw),
                    "ecmp_route_candidate_validation": self._ref(
                        route_report_path
                    ),
                    "training_source_port_allocator": self._ref(allocator_raw),
                    "training_source_port_allocator_validation": self._ref(
                        allocator_report
                    ),
                    "exit_code": self._ref(run_dir / "exit_code.txt"),
                }
                if run["class_label"] == "GRAY_FAULT":
                    # Deliberately contradictory self-reported values: the
                    # evaluator must use raw telemetry instead.
                    run["observable"] = False
                    run["first_observable_effect_ns"] = None
                if run["simulator_stability"]["gate_required"] is True:
                    stability_ref = self._ref(stability_path)
                    run["artifacts"]["simulator_stability"] = stability_ref
                    run["simulator_stability"] = {
                        "gate_required": True,
                        "status": "PASS",
                        "reason": (
                            "sealed completed run passed independent simulator "
                            "stability recomputation"
                        ),
                        "evidence": stability_ref,
                        "evidence_basis_sha256": stability_value[
                            "evidence_basis_sha256"
                        ],
                    }
        corpus = self.corpus(EXECUTED)
        if include_artifacts:
            corpus["simulator_runtime_closure"] = (
                self.runtime_closure_record
            )
        corpus["feature_schema"] = {
            "path": feature_path.name,
            "sha256": p2._sha256(feature_path),
        }
        self._write_manifests(corpus, EXECUTED)

    def refresh_run_manifest(self, run):
        manifest_path = self.root / run["artifacts"]["run_manifest"]["path"]
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        runtime_ref = run["artifacts"].get("workload_runtime_qualification")
        if isinstance(runtime_ref, dict):
            runtime_path = self.root / runtime_ref["path"]
            runtime_value = json.loads(runtime_path.read_text(encoding="utf-8"))
            value["workload_runtime_qualification_status"] = (
                runtime_value.get("status")
            )
            value["workload_runtime_qualification_sha256"] = p2._sha256(
                runtime_path
            )
        inventory = p2._artifact_inventory(manifest_path.parent)
        value["artifacts"] = inventory
        value["artifact_set_sha256"] = p2._canonical_hash(inventory)
        manifest_path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path.with_name("run_manifest.sha256").write_text(
            f"{p2._sha256(manifest_path)}  run_manifest.json\n",
            encoding="ascii",
        )
        run["artifacts"]["run_manifest"] = self._ref(manifest_path)

    def forge_runtime_source_binding(self, run, source_name, source_path):
        """Keep a forged PASS report hash-current so replay must catch damage."""

        qualification_path = self.root / run["artifacts"][
            "workload_runtime_qualification"
        ]["path"]
        value = json.loads(qualification_path.read_text(encoding="utf-8"))
        value["source_artifacts"][source_name] = {
            "path": source_path.name,
            "sha256": p2._sha256(source_path),
        }
        qualification_path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        run["artifacts"]["workload_runtime_qualification"] = self._ref(
            qualification_path
        )

    def rewrite_executed(self):
        feature_path = self.root / "feature_schema.json"
        corpus = self.corpus(EXECUTED)
        if self.runtime_closure_record is not None:
            corpus["simulator_runtime_closure"] = self.runtime_closure_record
        corpus["feature_schema"] = self._ref(feature_path)
        self._write_manifests(corpus, EXECUTED)

    def _ref(self, path):
        return {
            "path": str(path.relative_to(self.root)),
            "sha256": p2._sha256(path),
            "size_bytes": path.stat().st_size,
        }


class PreparedGateTest(unittest.TestCase):
    def test_complete_dry_run_is_prepared_but_does_not_unlock_p3(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            gate, observability, leakage = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "PREPARED")
            self.assertIsNone(gate["next_stage"])
            self.assertEqual(gate["summary"]["fail"], 0)
            self.assertGreater(gate["summary"]["pending"], 0)
            self.assertEqual(observability["status"], "PREPARED")
            self.assertEqual(leakage["status"], "PASS")
            random_loss = next(
                run for run in fixture.runs
                if run.get("fault_family") == "random_loss"
            )
            self.assertEqual(
                random_loss["simulator_stability"]["status"],
                "PENDING_EXECUTION",
            )


class A1CollectiveOverrideContractTest(unittest.TestCase):
    def test_evaluator_rebuilds_six_a1_plans_and_rejects_forged_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "prepared"
            corpus = p2.corpus_generator.generate_corpus(
                link_map_path=(
                    RESULTS / "true16_hard_fault_e2e/healthy/link_map.csv"
                ),
                contract_path=LIMER / "configs/experiment_contract.yaml",
                topology_path=(
                    RESULTS
                    / "true16_hard_fault_e2e/topology"
                    / "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
                ),
                workload_path=(
                    LIMER
                    / "configs"
                    / "microAllReduce_16rank_p2_sparse_periodic_550ms.txt"
                ),
                simulator_config_path=LIMER / "configs/SimAI.baseline.conf",
                out_dir=root,
                seed=20260827,
            )
            schedules, errors = p2.inspect_schedules(
                corpus["runs"], root, corpus["input_artifacts"]
            )
            self.assertEqual(errors, [])
            a1 = [
                item for item in schedules
                if item["collective_workload_override"].get(
                    "qualification_profile"
                ) in {
                    p2.workload_runtime.BURST_PROFILE,
                    p2.workload_runtime.HIGH_UTIL_PROFILE,
                }
            ]
            self.assertEqual(len(a1), 6)

            planned = next(
                run for run in corpus["runs"]
                if run.get("scenario") == "allreduce_burst"
            )
            planned = json.loads(json.dumps(planned))
            planned["schedule"]["collective_workload_override"][
                "static_validation"
            ]["forged_pass"] = True
            truth, truth_errors = p2._schedule_row(
                planned, root / planned["schedule"]["path"]
            )
            self.assertEqual(truth_errors, [])
            _, _, errors = p2._recompute_collective_override(
                planned,
                planned["schedule"],
                root,
                truth,
                corpus["input_artifacts"],
            )
            self.assertTrue(
                any("independent reconstruction" in error for error in errors)
            )

    def test_executed_flag_without_artifacts_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=False)
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            checks = {item["check"]: item["status"] for item in gate["checks"]}
            self.assertEqual(checks["actual_run_artifacts_complete_and_hashed"], "FAIL")


class LeakageAndSplitTest(unittest.TestCase):
    def test_label_or_fault_configuration_in_model_features_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            corpus = fixture.corpus(PREPARED)
            corpus["feature_schema"]["model_feature_columns"].extend(
                ["class_label", "configured_bandwidth_bps"])
            fixture.rewrite_prepared(corpus)
            gate, _, leakage = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            self.assertEqual(
                leakage["forbidden_feature_columns"],
                ["class_label", "configured_bandwidth_bps"],
            )

    def test_paired_holdout_gpu_in_training_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            split = json.loads(fixture.split_path.read_text(encoding="utf-8"))
            holdout_run = next(
                run for run in fixture.runs
                if run.get("target_gpu") == 12)
            next(item for item in split["entries"]
                 if item["run_id"] == holdout_run["run_id"])["partition"] = "train"
            fixture.split_path.write_text(
                json.dumps(split, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            gate, _, leakage = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            checks = {item["check"]: item["status"] for item in leakage["checks"]}
            self.assertEqual(checks["paired_gpu_unseen_link_holdout"], "FAIL")

    def test_schedule_content_mutation_breaks_hash_integrity(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.congestion_schedule.write_text(
                fixture.congestion_schedule.read_text(encoding="utf-8") + "\n",
                encoding="utf-8",
            )
            gate, _, leakage = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            self.assertEqual(leakage["status"], "FAIL")

    def test_raw_mutable_denominator_utilization_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            corpus = fixture.corpus(PREPARED)
            corpus["feature_schema"]["model_feature_columns"].append("utilization")
            fixture.rewrite_prepared(corpus)
            gate, _, leakage = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            self.assertIn("utilization", leakage["forbidden_feature_columns"])


class ScheduleAggregationTest(unittest.TestCase):
    def test_parent_event_aggregates_multiple_injector_segments(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "segments.csv"
            P2Fixture._write_csv(path, [
                {"event_id": "segment-1", "parent_event_id": "event-1",
                 "fault_family": "intermittent_service", "target_gpu": 0,
                 "target_link_id": "L0-24", "start_time_ns": 10,
                 "end_time_ns": 20, "shape": "pulse", "impairment": "service"},
                {"event_id": "segment-2", "parent_event_id": "event-1",
                 "fault_family": "intermittent_service", "target_gpu": 0,
                 "target_link_id": "L0-24", "start_time_ns": 30,
                 "end_time_ns": 40, "shape": "pulse", "impairment": "service"},
            ])
            row, errors = p2._schedule_row(
                {"schedule": {"event_id": "event-1"}}, path)
            self.assertEqual(errors, [])
            self.assertEqual(row["segment_count"], 2)
            self.assertEqual(row["start_time_ns"], 10)
            self.assertEqual(row["end_time_ns"], 40)


class ExecutedGateTest(unittest.TestCase):
    def test_only_hashed_actual_run_artifacts_can_pass_and_unlock_p3(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            out_dir = fixture.root / "gate"
            gate = p2.materialize(
                P1_GATE, fixture.corpus_path, fixture.split_path, out_dir,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "PASS")
            self.assertEqual(gate["next_stage"], "P3")
            self.assertEqual(gate["summary"]["fail"], 0)
            self.assertEqual(gate["summary"]["pending"], 0)
            closure = gate["simulator_runtime_closure_verification"]
            self.assertEqual(closure["status"], "PASS")
            self.assertEqual(
                closure["identity_sha256"],
                fixture.runtime_bundle.identity_sha256,
            )
            self.assertEqual(
                closure["verified_run_reference_count"], len(fixture.runs)
            )
            self.assertEqual(
                closure["independent_bundle_revalidation"]["loader"]["status"],
                "PASS",
            )
            self.assertEqual(
                gate["artifact_verification"][0]["runner_manifest"]
                ["ecmp_route_candidates"]["independent_status"],
                "PASS",
            )
            allocator = gate["artifact_verification"][0]["runner_manifest"][
                "training_source_port_allocator"
            ]
            self.assertEqual(allocator["status"], "PASS")
            self.assertFalse(allocator["require_reuse"])
            self.assertEqual(allocator["reuses"], 0)
            self.assertEqual(allocator["external_conflicts"], 0)
            self.assertEqual(allocator["exhaustions"], 0)
            self.assertEqual(allocator["invariant_errors"], 0)
            self.assertEqual(
                len(list((fixture.root / "runtime-bundles").iterdir())), 1
            )
            self.assertFalse(any(
                (fixture.root / run["artifacts"]["run_manifest"]["path"])
                .parent.joinpath("inputs", "simulator_binary").exists()
                for run in fixture.runs
            ))
            runtime_evidence = gate["artifact_verification"][0][
                "workload_runtime_qualification"
            ]
            recomputed = runtime_evidence["independent_recomputation"]
            self.assertEqual(recomputed["status"], "PASS")
            self.assertEqual(
                recomputed["topology"]["derived_switch_rows_per_snapshot"],
                64,
            )
            self.assertEqual(
                recomputed["topology"]["derived_host_rows_per_snapshot"],
                32,
            )
            observability = json.loads(
                (out_dir / "observability_report.json").read_text(encoding="utf-8"))
            self.assertGreater(observability["observable_event_count"], 0)
            self.assertTrue(all(
                item["first_observable_effect_ns"] == 102_000_000
                for item in observability["runs"]
            ))
            self.assertEqual(
                {path.name for path in out_dir.iterdir()},
                {"observability_report.json", "leakage_checks.json",
                 "stage_gate_p2.json", "stage_gate_p2.md"},
            )

    def test_stability_self_report_without_sealed_artifact_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(
                item for item in fixture.runs
                if item.get("fault_family") == "random_loss"
            )
            run["artifacts"].pop("simulator_stability")
            run["simulator_stability"] = {
                "gate_required": True,
                "status": "PASS",
                "reason": "self-reported without evidence",
            }
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("simulator_stability", check["detail"])

    def test_generic_healthy_and_bandwidth_complete_require_stability(self):
        cases = {
            "healthy": lambda run: run.get("class_label") == "HEALTHY",
            "bandwidth": lambda run: (
                run.get("fault_family") == "bandwidth_degradation"
            ),
        }
        for case, predicate in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                fixture = P2Fixture(Path(temporary))
                fixture.write_executed(include_artifacts=True)
                run = next(item for item in fixture.runs if predicate(item))
                self.assertTrue(run["simulator_stability"]["gate_required"])
                run["artifacts"].pop("simulator_stability")
                fixture.rewrite_executed()
                gate, _, _ = p2.evaluate(
                    P1_GATE,
                    fixture.corpus_path,
                    fixture.split_path,
                    allow_runtime_test_contract=True,
                )
                self.assertEqual(gate["status"], "FAIL")
                check = next(
                    item for item in gate["checks"]
                    if item["check"]
                    == "actual_run_artifacts_complete_and_hashed"
                )
                self.assertIn("simulator_stability", check["detail"])

    def test_missing_allocator_evidence_in_executed_run_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            run["artifacts"].pop("training_source_port_allocator")
            run["artifacts"].pop(
                "training_source_port_allocator_validation"
            )
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("training_source_port_allocator", check["detail"])

    def test_resealed_allocator_report_is_independently_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            report_path = fixture.root / run["artifacts"][
                "training_source_port_allocator_validation"
            ]["path"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["metrics"]["allocations"] = 2
            material = dict(report)
            material.pop("report_sha256")
            report["report_sha256"] = p2._canonical_hash(material)
            report_path.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            run["artifacts"][
                "training_source_port_allocator_validation"
            ] = fixture._ref(report_path)
            manifest_path = fixture.root / run["artifacts"]["run_manifest"][
                "path"
            ]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest[
                "training_source_port_allocator_validation_sha256"
            ] = p2._sha256(report_path)
            manifest["training_source_port_allocator_report_sha256"] = report[
                "report_sha256"
            ]
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("independent raw-evidence recomputation", check["detail"])

    def test_resealed_forged_stability_pass_is_independently_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(
                item for item in fixture.runs
                if item.get("fault_family") == "random_loss"
            )
            report_path = fixture.root / run["artifacts"][
                "simulator_stability"
            ]["path"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["evidence_basis"]["exit_code"] = 13
            report["evidence_basis_sha256"] = p2._canonical_hash(
                report["evidence_basis"]
            )
            report_path.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            report_ref = fixture._ref(report_path)
            run["artifacts"]["simulator_stability"] = report_ref
            run["simulator_stability"]["evidence"] = report_ref
            run["simulator_stability"]["evidence_basis_sha256"] = report[
                "evidence_basis_sha256"
            ]
            manifest_path = fixture.root / run["artifacts"]["run_manifest"][
                "path"
            ]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["simulator_stability_sha256"] = p2._sha256(report_path)
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("independent raw recomputation", check["detail"])

    def test_self_reported_blocked_stability_is_not_an_executed_exemption(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(
                item for item in fixture.runs
                if item.get("fault_family") == "random_loss"
            )
            run["execution_status"] = "BLOCKED"
            run["simulator_stability"] = {
                "gate_required": True,
                "status": "BLOCKED",
                "reason": "ordinary simulator failure",
            }
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn(
                "audited BLOCKED publication is not implemented", check["detail"]
            )

    def test_legacy_gated_manifest_without_stability_binding_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(
                item for item in fixture.runs
                if item.get("fault_family") == "random_loss"
            )
            manifest_path = fixture.root / run["artifacts"]["run_manifest"][
                "path"
            ]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for field in (
                "simulator_stability_gate_required",
                "simulator_stability_status",
                "simulator_stability_sha256",
            ):
                manifest.pop(field)
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE,
                fixture.corpus_path,
                fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("stability", check["detail"])

    def test_old_launcher_only_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            manifest_path = fixture.root / run["artifacts"]["run_manifest"]["path"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest.pop("runtime_closure")
            for field in (
                "runtime_execution_status",
                "runtime_loader_preflight",
                "runtime_pre_run_verification",
                "runtime_process_mapping_verification",
                "runtime_post_run_verification",
                "runtime_execution_evidence_sha256",
            ):
                manifest.pop(field, None)
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            detail = next(
                item["detail"] for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("lacks a sealed simulator runtime closure", detail)

    def test_shared_runtime_bundle_tamper_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            library = next(fixture.runtime_bundle.lib_directory.iterdir())
            value = library.read_bytes()
            library.chmod(0o644)
            library.write_bytes(value[:-1] + bytes([value[-1] ^ 1]))
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            closure = gate["simulator_runtime_closure_verification"]
            self.assertEqual(closure["status"], "FAIL")
            self.assertTrue(any(
                "sealed runtime bundle validation failed" in error
                for error in closure["errors"]
            ))

    def test_corpus_runtime_authority_and_process_maps_fail_closed(self):
        for case in ("top_authority", "process_mapping"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                fixture = P2Fixture(Path(temporary))
                fixture.write_executed(include_artifacts=True)
                if case == "top_authority":
                    corpus = json.loads(
                        fixture.corpus_path.read_text(encoding="utf-8")
                    )
                    corpus["simulator_runtime_closure"] = dict(
                        corpus["simulator_runtime_closure"]
                    )
                    corpus["simulator_runtime_closure"][
                        "source_executable_path"
                    ] = "/forged/launcher"
                    fixture._write_manifests(corpus, EXECUTED)
                else:
                    run = fixture.runs[0]
                    evidence_path = fixture.root / run["artifacts"][
                        "runtime_execution_evidence"
                    ]["path"]
                    evidence = json.loads(
                        evidence_path.read_text(encoding="utf-8")
                    )
                    evidence["process_mapping_verification"][
                        "project_dependencies"
                    ][0]["path"] = "/tmp/ambient/libns3-forged.so"
                    evidence_path.write_text(
                        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    run["artifacts"]["runtime_execution_evidence"] = (
                        fixture._ref(evidence_path)
                    )
                    manifest_path = fixture.root / run["artifacts"][
                        "run_manifest"
                    ]["path"]
                    manifest = json.loads(
                        manifest_path.read_text(encoding="utf-8")
                    )
                    manifest["runtime_process_mapping_verification"] = (
                        evidence["process_mapping_verification"]
                    )
                    manifest["runtime_execution_evidence_sha256"] = (
                        p2._sha256(evidence_path)
                    )
                    manifest_path.write_text(
                        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    fixture.refresh_run_manifest(run)
                    fixture.rewrite_executed()
                gate, _, _ = p2.evaluate(
                    P1_GATE, fixture.corpus_path, fixture.split_path,
                    allow_runtime_test_contract=True,
                )
                self.assertEqual(gate["status"], "FAIL")
                details = " ".join(
                    str(item["detail"]) for item in gate["checks"]
                )
                if case == "top_authority":
                    self.assertIn("differs from corpus authority", details)
                else:
                    self.assertIn("live process runtime mapping", details)

    def test_ecmp_route_raw_or_report_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            report_path = fixture.root / run["artifacts"][
                "ecmp_route_candidate_validation"
            ]["path"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["evidence"]["installed_route_row_count"] += 1
            report["report_sha256"] = p2.route_candidate_runtime.canonical_hash({
                key: value for key, value in report.items()
                if key != "report_sha256"
            })
            report_path.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            run["artifacts"]["ecmp_route_candidate_validation"] = fixture._ref(
                report_path
            )
            manifest_path = fixture.root / run["artifacts"]["run_manifest"]["path"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["ecmp_route_candidate_validation_sha256"] = p2._sha256(
                report_path
            )
            manifest["ecmp_route_candidate_report_sha256"] = report[
                "report_sha256"
            ]
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            detail = next(
                item["detail"] for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("differs from independent raw-evidence", detail)

    def test_ecmp_topology_content_hash_path_and_legacy_report_fail_closed(self):
        cases = {
            "content": "topology SHA-256 differs",
            "sha": "input binding hash differs for topology",
            "path": "topology is missing",
            "legacy_report": "differs from independent raw-evidence",
        }
        for case, expected in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                fixture = P2Fixture(Path(temporary))
                fixture.write_executed(include_artifacts=True)
                run = fixture.runs[0]
                manifest_path = fixture.root / run["artifacts"][
                    "run_manifest"
                ]["path"]
                manifest = json.loads(
                    manifest_path.read_text(encoding="utf-8")
                )
                if case == "content":
                    topology_copy = manifest_path.parent / manifest[
                        "input_bindings"
                    ]["topology"]["execution_copy"]
                    topology_copy.write_text(
                        topology_copy.read_text(encoding="utf-8") + "\n",
                        encoding="utf-8",
                    )
                elif case == "sha":
                    manifest["input_bindings"]["topology"]["sha256"] = "0" * 64
                elif case == "path":
                    manifest["input_bindings"]["topology"][
                        "execution_copy"
                    ] = "inputs/missing.topology"
                else:
                    report_path = fixture.root / run["artifacts"][
                        "ecmp_route_candidate_validation"
                    ]["path"]
                    report = json.loads(
                        report_path.read_text(encoding="utf-8")
                    )
                    report["artifacts"].pop("topology")
                    report["contract"]["expected_hashes"].pop("topology")
                    report["report_sha256"] = (
                        p2.route_candidate_runtime.canonical_hash({
                            key: value for key, value in report.items()
                            if key != "report_sha256"
                        })
                    )
                    report_path.write_text(
                        json.dumps(report, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    run["artifacts"][
                        "ecmp_route_candidate_validation"
                    ] = fixture._ref(report_path)
                    manifest[
                        "ecmp_route_candidate_validation_sha256"
                    ] = p2._sha256(report_path)
                    manifest["ecmp_route_candidate_report_sha256"] = report[
                        "report_sha256"
                    ]
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                fixture.refresh_run_manifest(run)
                fixture.rewrite_executed()
                gate, _, _ = p2.evaluate(
                    P1_GATE, fixture.corpus_path, fixture.split_path,
                    allow_runtime_test_contract=True,
                )
                self.assertEqual(gate["status"], "FAIL")
                detail = next(
                    item["detail"] for item in gate["checks"]
                    if item["check"]
                    == "actual_run_artifacts_complete_and_hashed"
                )
                self.assertIn(expected, detail)

    def test_runtime_qualification_is_recomputed_and_fail_closed(self):
        cases = ("tampered_pass", "source_hash", "missing_snapshot", "missing_rank")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                fixture = P2Fixture(Path(temporary))
                fixture.write_executed(include_artifacts=True)
                run = fixture.runs[0]
                qualification_path = fixture.root / run["artifacts"][
                    "workload_runtime_qualification"
                ]["path"]
                if case == "tampered_pass":
                    value = json.loads(
                        qualification_path.read_text(encoding="utf-8")
                    )
                    value["checks"][0]["status"] = "FAIL"
                    qualification_path.write_text(
                        json.dumps(value, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    run["artifacts"]["workload_runtime_qualification"] = (
                        fixture._ref(qualification_path)
                    )
                elif case == "source_hash":
                    value = json.loads(
                        qualification_path.read_text(encoding="utf-8")
                    )
                    value["source_artifacts"]["switch_telemetry"][
                        "sha256"
                    ] = "0" * 64
                    qualification_path.write_text(
                        json.dumps(value, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    run["artifacts"]["workload_runtime_qualification"] = (
                        fixture._ref(qualification_path)
                    )
                elif case == "missing_snapshot":
                    switch_path = fixture.root / run["artifacts"][
                        "switch_telemetry"
                    ]["path"]
                    with switch_path.open(encoding="utf-8") as source:
                        rows = list(csv.DictReader(source))
                    rows = [
                        row for row in rows
                        if int(row["timestamp_ns"]) != 68_000_000
                    ]
                    fixture._write_csv(switch_path, rows)
                    run["artifacts"]["switch_telemetry"] = fixture._ref(
                        switch_path
                    )
                    fixture.forge_runtime_source_binding(
                        run, "switch_telemetry", switch_path
                    )
                else:
                    transaction_path = fixture.root / run["artifacts"][
                        "collective_transaction"
                    ]["path"]
                    with transaction_path.open(encoding="utf-8") as source:
                        rows = list(csv.DictReader(source))
                    rows = [
                        row for row in rows
                        if not (
                            row["collective_seq"] == "0"
                            and row["event"] == "START"
                            and row["rank_id"] == "15"
                        )
                    ]
                    fixture._write_csv(transaction_path, rows)
                    run["artifacts"]["collective_transaction"] = fixture._ref(
                        transaction_path
                    )
                    fixture.forge_runtime_source_binding(
                        run, "collective_transaction", transaction_path
                    )
                fixture.refresh_run_manifest(run)
                fixture.rewrite_executed()
                gate, _, _ = p2.evaluate(
                    P1_GATE, fixture.corpus_path, fixture.split_path,
                    allow_runtime_test_contract=True,
                )
                self.assertEqual(gate["status"], "FAIL")
                check = next(
                    item for item in gate["checks"]
                    if item["check"]
                    == "actual_run_artifacts_complete_and_hashed"
                )
                if case == "tampered_pass":
                    self.assertIn(
                        "differs from independent recomputation", check["detail"]
                    )
                elif case == "source_hash":
                    self.assertIn(
                        "source switch_telemetry path/hash binding is invalid",
                        check["detail"],
                    )
                else:
                    self.assertIn(
                        "independent runtime qualification recomputation failed",
                        check["detail"],
                    )

    def test_explicit_runtime_contract_is_rejected_without_test_hook(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("test-only and forbidden in production", check["detail"])

    def test_legacy_three_field_run_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            manifest_path = (
                fixture.root / run["artifacts"]["run_manifest"]["path"]
            )
            manifest_path.write_text(
                json.dumps({
                    "run_id": run["run_id"],
                    "exit_code": 0,
                    "schedule_sha256": run["schedule"]["sha256"],
                }) + "\n",
                encoding="utf-8",
            )
            manifest_path.with_name("run_manifest.sha256").write_text(
                f"{p2._sha256(manifest_path)}  run_manifest.json\n",
                encoding="ascii",
            )
            run["artifacts"]["run_manifest"] = fixture._ref(manifest_path)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("schema_version", check["detail"])

    def test_runner_seal_inventory_input_and_control_bindings_fail_closed(self):
        for case, expected_error in (
            ("seal", "does not seal"),
            ("inventory", "inventory differs"),
            ("input", "input binding hash differs"),
            ("detector", "hard_event_detector_enabled must be false"),
            ("simulator", "simulator hash differs"),
            ("simulator_external", "simulator.path differs from runtime closure"),
            ("simulator_argv_binary", "did not execute the sealed runtime"),
            ("worker_top", "simulator_worker_threads must be positive"),
            ("worker_nested", "simulator.worker_threads does not match"),
            ("worker_argv", "simulator argv -t does not match"),
        ):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                fixture = P2Fixture(Path(temporary))
                fixture.write_executed(include_artifacts=True)
                run = fixture.runs[0]
                manifest_path = (
                    fixture.root / run["artifacts"]["run_manifest"]["path"]
                )
                seal_path = manifest_path.with_name("run_manifest.sha256")
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                if case == "seal":
                    seal_path.write_text(
                        f"{'0' * 64}  run_manifest.json\n", encoding="ascii"
                    )
                else:
                    if case == "inventory":
                        manifest["artifacts"] = manifest["artifacts"][:-1]
                        manifest["artifact_set_sha256"] = p2._canonical_hash(
                            manifest["artifacts"]
                        )
                    elif case == "input":
                        manifest["input_bindings"]["workload"]["sha256"] = (
                            "0" * 64
                        )
                    elif case == "detector":
                        manifest["hard_event_detector_enabled"] = True
                    elif case == "simulator":
                        manifest["simulator"]["sha256"] = "0" * 64
                    elif case == "simulator_external":
                        external = str(fixture.simulator_binary.resolve())
                        manifest["simulator"]["path"] = external
                        manifest["simulator"]["execution_copy"] = external
                    elif case == "simulator_argv_binary":
                        manifest["simulator"]["argv"][0] = str(
                            fixture.simulator_binary.resolve()
                        )
                    elif case == "worker_top":
                        manifest["simulator_worker_threads"] = 0
                    elif case == "worker_nested":
                        manifest["simulator"]["worker_threads"] = 8
                    elif case == "worker_argv":
                        manifest["simulator"]["argv"][-1] = "8"
                    manifest_path.write_text(
                        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    seal_path.write_text(
                        f"{p2._sha256(manifest_path)}  run_manifest.json\n",
                        encoding="ascii",
                    )
                    run["artifacts"]["run_manifest"] = fixture._ref(
                        manifest_path
                    )
                    fixture.rewrite_executed()
                gate, _, _ = p2.evaluate(
                    P1_GATE, fixture.corpus_path, fixture.split_path,
                    allow_runtime_test_contract=True,
                )
                self.assertEqual(gate["status"], "FAIL")
                check = next(
                    item for item in gate["checks"]
                    if item["check"]
                    == "actual_run_artifacts_complete_and_hashed"
                )
                self.assertIn(expected_error, check["detail"])

    def test_executed_corpus_cannot_downgrade_v2_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            corpus = json.loads(fixture.corpus_path.read_text(encoding="utf-8"))
            inputs = corpus["input_artifacts"]
            legacy_material = {
                "contract_sha256": inputs["contract"]["sha256"],
                "topology_sha256": inputs["topology"]["sha256"],
                "workload_sha256": inputs["workload"]["sha256"],
                "seed": corpus["generation_seed"],
                "holdouts": corpus["split_manifest"]["paired_holdout_gpu_ids"],
                "runs": [p2._run_identity_entry(run) for run in corpus["runs"]],
            }
            corpus.pop("identity_schema")
            corpus["corpus_id"] = "p2-" + p2._canonical_hash(
                legacy_material
            )[:24]
            fixture._write_manifests(corpus, EXECUTED)
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "v2_corpus_identity_and_input_hashes"
            )
            self.assertEqual(check["status"], "FAIL")
            self.assertIn("must use identity_schema", check["detail"])

    def test_complete_run_requires_fault_application_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            fault_path = fixture.root / run["artifacts"][
                "fault_application_telemetry"
            ]["path"]
            fault_path.unlink()
            run["artifacts"].pop("fault_application_telemetry")
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn("fault_application_telemetry", check["detail"])

    def test_recoverable_error_proxy_cannot_claim_true_packet_loss(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(item for item in fixture.runs
                       if item.get("fault_family") == "random_loss")
            run["mechanism"]["mechanism_id"] = "recoverable_error_proxy"
            semantic_path = fixture.root / run["artifacts"]["semantic_validation"]["path"]
            semantic = json.loads(semantic_path.read_text(encoding="utf-8"))
            semantic["mechanism_id"] = "recoverable_error_proxy"
            semantic["packet_disposition"] = "recovered_delayed"
            semantic["recoverable_error_proxy"] = True
            semantic_path.write_text(
                json.dumps(semantic, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            run["artifacts"]["semantic_validation"] = fixture._ref(semantic_path)
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            checks = {item["check"]: item for item in gate["checks"]}
            self.assertEqual(
                checks["mechanism_identity_and_execution_semantics"]["status"],
                "FAIL",
            )

    def test_all_zero_nominal_warmup_is_not_causal_training_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = next(item for item in fixture.runs
                       if item.get("fault_family") == "bandwidth_degradation")
            switch_path = fixture.root / run["artifacts"]["switch_telemetry"]["path"]
            with switch_path.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            for row in rows:
                row["tx_bytes"] = "0"
            fixture._write_csv(switch_path, rows)
            run["artifacts"]["switch_telemetry"] = fixture._ref(switch_path)
            semantic_path = fixture.root / run["artifacts"]["semantic_validation"]["path"]
            semantic = json.loads(semantic_path.read_text(encoding="utf-8"))
            semantic["source_artifact_sha256"] = p2._sha256(switch_path)
            semantic_path.write_text(
                json.dumps(semantic, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            run["artifacts"]["semantic_validation"] = fixture._ref(semantic_path)
            fixture.forge_runtime_source_binding(
                run, "switch_telemetry", switch_path
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True)
            self.assertEqual(gate["status"], "FAIL")
            check = next(item for item in gate["checks"]
                         if item["check"] == "actual_run_artifacts_complete_and_hashed")
            self.assertIn(
                "independent runtime qualification recomputation failed",
                check["detail"],
            )
            self.assertIn("no positive ACCESS TX growth", check["detail"])

    def test_late_workload_complete_is_not_an_observation_horizon(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            lifecycle_path = (
                fixture.root / run["artifacts"]["run_lifecycle"]["path"]
            )
            lifecycle_path.write_text(
                "run_id,event,scheduled_ns,actual_ns,finished_ranks,"
                "world_size,status\n"
                f"{run['run_id']},finish_barrier,,250000000,16,16,"
                "WORKLOAD_COMPLETE\n",
                encoding="utf-8",
            )
            run["artifacts"]["run_lifecycle"] = fixture._ref(lifecycle_path)
            fixture.forge_runtime_source_binding(
                run, "run_lifecycle", lifecycle_path
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn(
                "independent runtime qualification recomputation failed",
                check["detail"],
            )
            self.assertIn("completed the declared workload", check["detail"])

    def test_observation_horizon_timestamp_must_be_exact(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = P2Fixture(Path(temporary))
            fixture.write_executed(include_artifacts=True)
            run = fixture.runs[0]
            lifecycle_path = (
                fixture.root / run["artifacts"]["run_lifecycle"]["path"]
            )
            lifecycle_path.write_text(
                "run_id,event,scheduled_ns,actual_ns,finished_ranks,"
                "world_size,status\n"
                f"{run['run_id']},observation_horizon,200000000,"
                "200000001,0,16,"
                "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE\n",
                encoding="utf-8",
            )
            run["artifacts"]["run_lifecycle"] = fixture._ref(lifecycle_path)
            fixture.forge_runtime_source_binding(
                run, "run_lifecycle", lifecycle_path
            )
            fixture.refresh_run_manifest(run)
            fixture.rewrite_executed()
            gate, _, _ = p2.evaluate(
                P1_GATE, fixture.corpus_path, fixture.split_path,
                allow_runtime_test_contract=True,
            )
            self.assertEqual(gate["status"], "FAIL")
            check = next(
                item for item in gate["checks"]
                if item["check"] == "actual_run_artifacts_complete_and_hashed"
            )
            self.assertIn(
                "independent runtime qualification recomputation failed",
                check["detail"],
            )
            self.assertIn("horizon is not exact", check["detail"])


class BackgroundCongestionEvidenceTest(unittest.TestCase):
    @staticmethod
    def _single_rail_sport(source, destination, dport, bucket, used):
        for sport in range(49152, 65536):
            if sport in used:
                continue
            if (
                p2.rdma_route_bucket(
                    src=source, dst=destination, sport=sport, dport=dport
                ) == bucket
                and p2.rdma_route_bucket(
                    src=source, dst=destination, sport=sport, dport=dport,
                    reverse=True,
                ) == bucket
            ):
                used.add(sport)
                return sport
        raise AssertionError("test could not pin a reserved source port")

    def _artifacts(self, root):
        event_id = "event-incast"
        start_ns = 102_000_000
        deadline_ns = 302_000_000
        used = set()
        declared = [
            {
                "event_id": event_id,
                "flow_id": f"flow-{source}",
                "scenario": "incast",
                "scheduled_start_ns": start_ns,
                "src_rank": source,
                "dst_rank": 0,
                "bytes": 1_048_576,
                "pg": 3,
                "sport": self._single_rail_sport(
                    source, 0, 20000, 0, used),
                "dport": 20000,
            }
            for source in range(4, 16)
        ]
        schedule_path = root / "background.csv"
        P2Fixture._write_csv(schedule_path, declared)
        run = {
            "run_id": "incast-run",
            "scenario": "incast",
            "class_label": "CONGESTION",
            "target_gpu": 0,
            "target_link_id": "L0-24",
            "paired_link_id": "L0-25",
            "virtual_finish_ns": 400_000_000,
            "mechanism": {"mechanism_id": "background_rdma_incast"},
        }
        action = {
            "destination_rank": 0,
            "bottleneck_access_link_id": "L0-24",
            "paired_access_link_id": "L0-25",
            "data_plane": "A",
            "route_candidate_order_host_ports": [2, 3],
            "route_bucket": 0,
            "hash_algorithm": "ns3-murmur3-x86-32",
            "hash_seed_u32": p2.MURMUR3_SEED_U32,
            "hash_tuple": "native-le-sip-dip-sport-dport",
            "hash_byte_order": "little",
            "pin_reverse_ack": True,
            "predeclared_window_policy": "scheduled_qp_launch_window",
            "realized_window_policy": "first_data_tx_to_last_ack_complete",
            "completion_deadline_ns": deadline_ns,
            "rdma_rto_us": 250_000,
            "rdma_retry_limit": 0,
            "max_rto_retry_events": 0,
        }
        truth = {
            "event_id": event_id,
            "start_time_ns": str(start_ns),
            "end_time_ns": str(start_ns + 1),
            "action_scope": "workload",
            "action_parameters_json": json.dumps(action, sort_keys=True),
        }
        truth_path = root / "truth.csv"
        P2Fixture._write_csv(truth_path, [truth])
        schedule = {
            "event_id": event_id,
            "implementation_status": "EXECUTABLE_BACKGROUND_RDMA",
            "background_flow_schedule": {
                "path": schedule_path.name,
                "sha256": p2._sha256(schedule_path),
            },
        }
        application = []
        for flow in declared:
            common = dict(flow)
            application.extend([
                {
                    "run_id": run["run_id"], **common,
                    "event": "SCHEDULED", "status": "INSTALLED",
                },
                {
                    "run_id": run["run_id"], **common,
                    "event": "START", "actual_ns": start_ns,
                    "status": "QP_CREATED",
                },
                {
                    "run_id": run["run_id"], **common,
                    "event": "COMPLETE", "actual_ns": 103_000_000,
                    "first_tx_ns": 102_000_001,
                    "first_ack_ns": 102_500_000,
                    "status": "ACK_COMPLETE",
                },
            ])
        application_path = root / "background_flow_application.csv"
        P2Fixture._write_csv(application_path, application)
        link_rows = [
            {
                "link_id": "L0-24", "src_node": 0, "dst_node": 24,
                "src_type": "HOST", "dst_type": "SWITCH",
                "src_port": 2, "dst_port": 1, "link_class": "ACCESS",
            },
            {
                "link_id": "L0-25", "src_node": 0, "dst_node": 25,
                "src_type": "HOST", "dst_type": "SWITCH",
                "src_port": 3, "dst_port": 1, "link_class": "ACCESS",
            },
        ]
        frozen_link_map = root / "frozen_link_map.csv"
        runtime_link_map = root / "runtime_link_map.csv"
        P2Fixture._write_csv(frozen_link_map, link_rows)
        shutil.copy2(frozen_link_map, runtime_link_map)
        rdma_rows = []
        for flow in declared:
            logical = (
                f"{flow['src_rank']}-{flow['dst_rank']}-"
                f"{flow['sport']}-{flow['pg']}"
            )
            common = {
                "run_id": run["run_id"], "node_id": flow["src_rank"],
                "rank_id": flow["src_rank"], "logical_qp_id": logical,
                "transport_epoch": 0, "traffic_class": "BACKGROUND",
                "event_detail": 2, "src_rank": flow["src_rank"],
                "dst_rank": flow["dst_rank"], "sport": flow["sport"],
                "primary_nic": 2, "backup_nic": 4_294_967_295,
                "active_nic": 2, "backup_ready_ns": "", "failover_ns": "",
                "backup_first_tx_ns": "", "backup_first_ack_ns": "",
                "standby_tx_bytes": 0, "retry_count": 0,
                "retry_limit": 0, "rto_us": 250_000,
            }
            rdma_rows.extend([
                {
                    **common, "timestamp_ns": start_ns, "event": "QP_CREATED",
                    "wc_status": "NONE", "snd_una": 0, "snd_nxt": 0,
                },
                {
                    **common, "timestamp_ns": 103_000_000, "event": "WC",
                    "wc_status": "SUCCESS", "snd_una": flow["bytes"],
                    "snd_nxt": flow["bytes"],
                },
            ])
        rdma_path = root / "rdma_wc_telemetry.csv"
        with rdma_path.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(
                output, fieldnames=p2.BACKGROUND_RDMA_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rdma_rows)
        switch_path = root / "switch_telemetry.csv"
        P2Fixture._write_csv(switch_path, [
            {
                "run_id": run["run_id"], "timestamp_ns": 102_000_000,
                "switch_id": 24, "port_id": 1, "link_id": "L0-24",
                "direction": "tx", "queue_bytes": 0,
                "max_queue_bytes": 0, "observed_throughput_bps": 1,
                "link_state": "up",
            },
            {
                "run_id": run["run_id"], "timestamp_ns": 103_000_000,
                "switch_id": 24, "port_id": 1, "link_id": "L0-24",
                "direction": "tx", "queue_bytes": 100,
                "max_queue_bytes": 100, "observed_throughput_bps": 1_000,
                "link_state": "up",
            },
        ])
        fault_application_path = root / "fault_application_telemetry.csv"
        fault_application_path.write_text(
            "run_id,fault_id,fault_type,target_link_id,transition,"
            "scheduled_ns,actual_ns,parameter_before,parameter_after,"
            "mechanism,rng_stream,status\n",
            encoding="utf-8",
        )
        return {
            "run": run, "schedule": schedule, "truth": truth,
            "truth_path": truth_path, "declared": declared,
            "schedule_path": schedule_path, "application": application,
            "application_path": application_path,
            "frozen_link_map": frozen_link_map,
            "runtime_link_map": runtime_link_map,
            "rdma_rows": rdma_rows, "rdma_path": rdma_path,
            "switch_path": switch_path,
            "fault_application_path": fault_application_path,
        }

    def test_incast_profile_and_ack_lifecycle_are_independently_rechecked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = self._artifacts(root)
            run = values["run"]
            profile, profile_errors = p2.inspect_background_schedule(
                run, values["schedule"], root, values["truth"]
            )
            self.assertEqual(profile_errors, [])
            self.assertTrue(profile["profile_valid"])
            self.assertTrue(profile["launch_window_exact"])
            self.assertTrue(profile["forward_and_reverse_hash_pin_valid"])
            self.assertEqual(profile["row_count"], 12)

            evidence, lifecycle_errors = p2._background_application_evidence(
                run, values["application_path"], values["schedule_path"],
                profile["contract"],
            )
            self.assertEqual(lifecycle_errors, [])
            self.assertTrue(evidence["all_ack_complete"])
            self.assertTrue(evidence["realized_wave_profile_valid"])
            self.assertTrue(evidence["completion_deadline_met"])
            self.assertEqual(evidence["ack_completed_flow_count"], 12)

            link_evidence, link_errors = p2._background_link_map_evidence(
                run, values["runtime_link_map"], values["frozen_link_map"],
                profile["contract"],
            )
            self.assertEqual(link_errors, [])
            self.assertTrue(link_evidence["single_rail_target_resolved"])
            switch_evidence, switch_errors = p2._background_switch_evidence(
                run, values["switch_path"], evidence, link_evidence,
            )
            self.assertEqual(switch_errors, [])
            self.assertTrue(switch_evidence["queue_pressure"])
            self.assertTrue(switch_evidence["throughput_activity"])
            rdma_evidence, rdma_errors = p2._background_rdma_evidence(
                run, values["rdma_path"], values["schedule_path"],
                profile["contract"],
            )
            self.assertEqual(rdma_errors, [])
            self.assertTrue(rdma_evidence["qp_identity_bijection"])
            self.assertTrue(rdma_evidence["ack_success_lifecycle"])
            self.assertTrue(rdma_evidence["zero_retry_or_failover_events"])

            switch_hash = p2._sha256(values["switch_path"])
            source_hashes = {
                "switch": switch_hash,
                "background_application": p2._sha256(
                    values["application_path"]),
                "background_schedule": p2._sha256(values["schedule_path"]),
                "truth_schedule": p2._sha256(values["truth_path"]),
                "link_map": p2._sha256(values["runtime_link_map"]),
                "runtime_link_map": p2._sha256(values["runtime_link_map"]),
                "frozen_link_map": p2._sha256(values["frozen_link_map"]),
                "rdma_wc": p2._sha256(values["rdma_path"]),
                "fault_application": p2._sha256(
                    values["fault_application_path"]
                ),
            }
            semantic = {
                "schema_version": "limer.p2-run-semantics.v1",
                "run_id": run["run_id"],
                "mechanism_id": "background_rdma_incast",
                "status": "PASS",
                "source_artifact_sha256": switch_hash,
                "source_artifacts_sha256": source_hashes,
                "checks": [{"name": "real_pressure", "status": "PASS"}],
                "injected_physical_fault": False,
                "scenario": "incast",
                "observed_effects": [
                    "background_rdma_ack_complete", "queue_pressure",
                    "throughput_activity", "common_destination_fan_in",
                    "single_target_access_link", "zero_background_rto_retries",
                    "realized_wave_profile",
                ],
                "target_link_ids": ["L0-24"],
                "actual_apply_ns": evidence["realized_window_start_ns"],
                "event_end_ns": evidence["realized_window_end_ns"],
                "evidence": {
                    "route_bucket": 0,
                    "target_host_port": 2,
                    "transport_contract": {
                        "rto_us": 250_000,
                        "retry_limit": 0,
                        "max_rto_retry_events": 0,
                    },
                },
            }
            self.assertEqual(
                p2._semantic_validation_errors(
                    run, semantic, switch_hash,
                    p2._sha256(values["application_path"]),
                    p2._sha256(values["schedule_path"]), source_hashes,
                    profile["contract"], evidence,
                ),
                [],
            )

            missing_fault_binding = json.loads(json.dumps(semantic))
            missing_fault_binding["source_artifacts_sha256"].pop(
                "fault_application"
            )
            missing_fault_errors = p2._semantic_validation_errors(
                run, missing_fault_binding, switch_hash,
                p2._sha256(values["application_path"]),
                p2._sha256(values["schedule_path"]), source_hashes,
                profile["contract"], evidence,
            )
            self.assertTrue(any(
                "fault_application" in error
                for error in missing_fault_errors
            ))

            P2Fixture._write_csv(values["schedule_path"], values["declared"][:1])
            values["schedule"]["background_flow_schedule"]["sha256"] = p2._sha256(
                values["schedule_path"]
            )
            _, profile_errors = p2.inspect_background_schedule(
                run, values["schedule"], root, values["truth"]
            )
            self.assertTrue(profile_errors)

    def test_mutated_hash_window_link_map_rdma_and_semantics_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = self._artifacts(root)
            run = values["run"]
            profile, errors = p2.inspect_background_schedule(
                run, values["schedule"], root, values["truth"]
            )
            self.assertEqual(errors, [])
            contract = profile["contract"]

            inflated_truth = dict(values["truth"])
            inflated_truth["end_time_ns"] = "302000000"
            _, errors = p2.inspect_background_schedule(
                run, values["schedule"], root, inflated_truth
            )
            self.assertTrue(any("profile invalid" in error for error in errors))

            wrong_endian_truth = dict(values["truth"])
            wrong_endian_action = json.loads(
                wrong_endian_truth["action_parameters_json"])
            wrong_endian_action["hash_byte_order"] = "big"
            wrong_endian_truth["action_parameters_json"] = json.dumps(
                wrong_endian_action, sort_keys=True)
            _, errors = p2.inspect_background_schedule(
                run, values["schedule"], root, wrong_endian_truth
            )
            self.assertTrue(any("hash_byte_order" in error for error in errors))

            bad_flows = [dict(row) for row in values["declared"]]
            bad = bad_flows[0]
            for sport in range(49152, 65536):
                if (p2.rdma_route_bucket(
                        src=int(bad["src_rank"]), dst=0, sport=sport,
                        dport=20000) != 0
                        or p2.rdma_route_bucket(
                            src=int(bad["src_rank"]), dst=0, sport=sport,
                            dport=20000, reverse=True) != 0):
                    bad["sport"] = sport
                    break
            P2Fixture._write_csv(values["schedule_path"], bad_flows)
            values["schedule"]["background_flow_schedule"]["sha256"] = p2._sha256(
                values["schedule_path"])
            profile, errors = p2.inspect_background_schedule(
                run, values["schedule"], root, values["truth"]
            )
            self.assertFalse(profile["forward_and_reverse_hash_pin_valid"])
            self.assertTrue(errors)

            shutil.copy2(values["frozen_link_map"], values["runtime_link_map"])
            with values["runtime_link_map"].open("a", encoding="utf-8") as output:
                output.write("\n")
            _, errors = p2._background_link_map_evidence(
                run, values["runtime_link_map"], values["frozen_link_map"],
                contract,
            )
            self.assertTrue(any("differs from frozen" in error for error in errors))

            P2Fixture._write_csv(values["schedule_path"], values["declared"])
            late_application = [dict(row) for row in values["application"]]
            for row in late_application:
                if row.get("event") == "COMPLETE":
                    row["actual_ns"] = 303_000_000
                    row["first_ack_ns"] = 302_500_000
            late_path = root / "late_application.csv"
            P2Fixture._write_csv(late_path, late_application)
            _, errors = p2._background_application_evidence(
                run, late_path, values["schedule_path"], contract,
            )
            self.assertTrue(any("misses predeclared deadline" in error
                                for error in errors))
            queue_run = {**run, "scenario": "queue_buildup"}
            _, errors = p2._background_application_evidence(
                queue_run, values["application_path"], values["schedule_path"],
                contract,
            )
            self.assertTrue(any("wave profile invalid" in error for error in errors))

            bad_rdma = [dict(row) for row in values["rdma_rows"]]
            bad_rdma[0]["retry_count"] = 1
            bad_rdma[0]["rto_us"] = 100
            P2Fixture._write_csv(values["rdma_path"], bad_rdma)
            _, errors = p2._background_rdma_evidence(
                run, values["rdma_path"], values["schedule_path"], contract,
            )
            self.assertTrue(any("configuration differs" in error for error in errors))
            self.assertTrue(any("retry/failover" in error for error in errors))

            application_evidence, errors = p2._background_application_evidence(
                run, values["application_path"], values["schedule_path"], contract,
            )
            self.assertEqual(errors, [])
            semantic = {
                "schema_version": "limer.p2-run-semantics.v1",
                "run_id": run["run_id"],
                "mechanism_id": "background_rdma_incast",
                "status": "PASS", "source_artifact_sha256": "a" * 64,
                "source_artifacts_sha256": {},
                "checks": [{"name": "self_report", "status": "PASS"}],
                "injected_physical_fault": False, "scenario": "incast",
                "observed_effects": ["queue_pressure"],
            }
            semantic_errors = p2._semantic_validation_errors(
                run, semantic, "a" * 64,
                p2._sha256(values["application_path"]),
                p2._sha256(values["schedule_path"]),
                {"rdma_wc": p2._sha256(values["rdma_path"])},
                contract, application_evidence,
            )
            self.assertTrue(any("source hash differs" in error
                                for error in semantic_errors))
            self.assertTrue(any("lacks effects" in error
                                for error in semantic_errors))

    def test_rdma_schema_rejects_even_one_extra_column(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = self._artifacts(root)
            profile, profile_errors = p2.inspect_background_schedule(
                values["run"], values["schedule"], root, values["truth"]
            )
            self.assertEqual(profile_errors, [])
            with values["rdma_path"].open(
                    "w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=(*p2.BACKGROUND_RDMA_COLUMNS, "unexpected"),
                    lineterminator="\n",
                )
                writer.writeheader()
                for row in values["rdma_rows"]:
                    writer.writerow({**row, "unexpected": "forbidden"})
            _, errors = p2._background_rdma_evidence(
                values["run"], values["rdma_path"],
                values["schedule_path"], profile["contract"],
            )
            self.assertTrue(any("exactly match the 26-column" in error
                                for error in errors))


if __name__ == "__main__":
    unittest.main()
