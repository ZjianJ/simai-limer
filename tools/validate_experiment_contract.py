#!/usr/bin/env python3
"""Validate the normative LIMER experiment contract.

This is the P0 schema/semantic gate.  It intentionally checks relationships
between fields (for example endpoint counts and strict SLO values), instead of
accepting any YAML document that happens to parse.  It never launches the
simulator and never changes the contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import yaml


SCHEMA_VERSION = "limer.contract-check.v1"
PASS = "PASS"
FAIL = "FAIL"

REQUIRED_TOP_LEVEL = {
    "schema_version",
    "contract_id",
    "status",
    "reproducibility",
    "topology",
    "telemetry",
    "workload",
    "fault_taxonomy",
    "timepoints",
    "features",
    "splits",
    "metrics",
    "slo",
    "evidence_classes",
    "claim_boundaries",
    "stage_gates",
}

REQUIRED_TIMEPOINTS = {
    "fault_applied_ns",
    "first_observable_effect_ns",
    "layer0_observe_ns",
    "layer1_suspect_ns",
    "controller_ingest_ns",
    "controller_decision_ns",
    "alarm_emit_ns",
    "alarm_deliver_ns",
    "alarm_actionable_ns",
    "quarantine_applied_ns",
    "primary_quiesced_ns",
    "route_update_complete_ns",
    "backup_activated_ns",
    "first_backup_tx_ns",
    "first_backup_ack_ns",
    "collective_abort_ns",
    "collective_redo_start_ns",
    "training_progress_resume_ns",
    "collective_commit_ns",
}

REQUIRED_MANIFEST_FIELDS = {
    "contract_id",
    "contract_sha256",
    "simai_revision",
    "ns3_revision",
    "dirty_worktree",
    "topology_path",
    "topology_sha256",
    "workload_path",
    "workload_sha256",
    "simulator_config_path",
    "simulator_config_sha256",
    "detector_id",
    "detector_artifact_sha256",
    "split_manifest_sha256",
    "schedule_sha256",
    "run_id",
    "run_role",
    "virtual_start_ns",
    "virtual_finish_ns",
    "wall_start_utc",
    "wall_finish_utc",
    "exit_code",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _at(value: Mapping[str, Any], path: Sequence[str]) -> Any:
    current: Any = value
    for component in path:
        if not isinstance(current, Mapping) or component not in current:
            return None
        current = current[component]
    return current


def _add(
    checks: List[Dict[str, Any]], name: str, condition: bool, detail: str
) -> None:
    checks.append(
        {
            "check": name,
            "status": PASS if condition else FAIL,
            "detail": detail,
        }
    )


def _exact_keys(
    checks: List[Dict[str, Any]],
    name: str,
    observed: Iterable[str],
    expected: Iterable[str],
) -> None:
    observed_set = set(observed)
    expected_set = set(expected)
    _add(
        checks,
        name,
        observed_set == expected_set,
        f"missing={sorted(expected_set - observed_set)}, "
        f"extra={sorted(observed_set - expected_set)}",
    )


def validate_contract(contract: Mapping[str, Any]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    _add(checks, "top_level_is_mapping", isinstance(contract, Mapping),
         f"type={type(contract).__name__}")
    _add(
        checks,
        "required_top_level_sections",
        REQUIRED_TOP_LEVEL <= set(contract),
        f"missing={sorted(REQUIRED_TOP_LEVEL - set(contract))}",
    )
    _add(
        checks,
        "contract_identity",
        contract.get("schema_version") == 1
        and contract.get("contract_id") == "limer-true16-dual-plane-v1"
        and contract.get("status") == "normative",
        f"schema={contract.get('schema_version')!r}, "
        f"id={contract.get('contract_id')!r}, status={contract.get('status')!r}",
    )

    manifest_fields = _at(
        contract, ("reproducibility", "required_manifest_fields")
    ) or []
    _exact_keys(
        checks,
        "required_manifest_fields_are_frozen",
        manifest_fields,
        REQUIRED_MANIFEST_FIELDS,
    )
    _add(
        checks,
        "required_manifest_fields_are_unique",
        len(manifest_fields) == len(set(manifest_fields)),
        f"count={len(manifest_fields)}, unique={len(set(manifest_fields))}",
    )

    invariants = _at(contract, ("topology", "invariants")) or {}
    class_counts = invariants.get("link_class_counts", {})
    expected_topology = {
        "gpu_count": 16,
        "server_count": 4,
        "gpus_per_server": 4,
        "nvswitch_count": 4,
        "asw_count": 8,
        "psw_count": 64,
        "regular_switch_count": 72,
        "total_node_count": 92,
        "physical_link_count": 304,
        "access_links_per_plane": 16,
        "isolated_switch_plane_count": 2,
        "switch_nvswitch_port_endpoints": 560,
        "switch_directional_rows_per_snapshot": 1120,
        "host_device_rows_per_snapshot": 48,
    }
    topology_mismatches = {
        name: {"expected": expected, "observed": invariants.get(name)}
        for name, expected in expected_topology.items()
        if invariants.get(name) != expected
    }
    _add(
        checks,
        "true16_topology_constants",
        not topology_mismatches,
        f"mismatches={topology_mismatches}",
    )
    _add(
        checks,
        "link_class_partition",
        class_counts == {"INTRA_NODE": 16, "ACCESS": 32, "INTER_SWITCH": 256}
        and sum(class_counts.values()) == invariants.get("physical_link_count"),
        f"class_counts={class_counts}, physical={invariants.get('physical_link_count')}",
    )
    fabric_endpoints = invariants.get("switch_nvswitch_port_endpoints")
    host_endpoints = invariants.get("host_device_rows_per_snapshot")
    physical_links = invariants.get("physical_link_count")
    directional_rows = invariants.get("switch_directional_rows_per_snapshot")
    _add(
        checks,
        "endpoint_arithmetic",
        isinstance(fabric_endpoints, int)
        and isinstance(host_endpoints, int)
        and isinstance(physical_links, int)
        and isinstance(directional_rows, int)
        and fabric_endpoints + host_endpoints == 2 * physical_links
        and directional_rows == 2 * fabric_endpoints,
        f"fabric={fabric_endpoints}, host={host_endpoints}, "
        f"links={physical_links}, directional_rows={directional_rows}",
    )

    plane_a = _at(contract, ("topology", "plane_a")) or {}
    plane_b = _at(contract, ("topology", "plane_b")) or {}
    _add(
        checks,
        "active_standby_plane_roles",
        plane_a.get("role") == "standby"
        and plane_b.get("role") == "active"
        and plane_a.get("host_port") == 2
        and plane_b.get("host_port") == 3
        and plane_a.get("plane_id") != plane_b.get("plane_id"),
        f"plane_a={plane_a}, plane_b={plane_b}",
    )
    scope = _at(contract, ("topology", "initial_scored_fault_scope")) or {}
    _add(
        checks,
        "initial_fault_scope",
        scope.get("link_class") == "ACCESS"
        and scope.get("plane") == "B"
        and scope.get("target_count") == 16,
        f"scope={scope}",
    )

    telemetry = contract.get("telemetry", {})
    _add(
        checks,
        "telemetry_cadence_and_rows",
        telemetry.get("coherent_snapshot_interval_ns") == 1_000_000
        and telemetry.get("expected_switch_directional_rows_per_snapshot")
        == directional_rows
        and telemetry.get("expected_host_device_rows_per_snapshot")
        == host_endpoints,
        f"interval={telemetry.get('coherent_snapshot_interval_ns')}, "
        f"switch_rows={telemetry.get('expected_switch_directional_rows_per_snapshot')}, "
        f"host_rows={telemetry.get('expected_host_device_rows_per_snapshot')}",
    )
    workload = contract.get("workload", {})
    p1_conditions = set(
        _at(contract, ("stage_gates", "P1", "pass_conditions")) or []
    )
    _add(
        checks,
        "p1_long_healthy_boundary",
        workload.get("causal_feature_warmup_ns") == 100_000_000
        and workload.get("healthy_monitoring_minimum_virtual_span_ns")
        == 100_000_000
        and workload.get("healthy_monitoring_minimum_snapshot_count_at_1ms")
        == 101
        and {
            "healthy_monitoring_virtual_span_at_least_100ms",
            "healthy_monitoring_snapshot_count_at_least_101",
        }
        <= p1_conditions,
        f"warmup={workload.get('causal_feature_warmup_ns')}, "
        f"span={workload.get('healthy_monitoring_minimum_virtual_span_ns')}, "
        "snapshots="
        f"{workload.get('healthy_monitoring_minimum_snapshot_count_at_1ms')}",
    )

    hard = _at(contract, ("fault_taxonomy", "hard", "families")) or {}
    gray = _at(contract, ("fault_taxonomy", "gray")) or {}
    _add(
        checks,
        "hard_gray_link_state_boundary",
        _at(hard, ("hard_disconnect", "link_state_during_fault")) == "down"
        and gray.get("link_state_during_scored_interval") == "up",
        "hard disconnect must be DOWN while every scored gray interval stays UP",
    )
    _add(
        checks,
        "independent_congestion_truth",
        _at(contract, ("fault_taxonomy", "negative", "congestion", "feature_derived_labels_forbidden"))
        is True
        and _at(contract, ("fault_taxonomy", "negative", "congestion", "label_source"))
        == "predeclared_workload_or_network_schedule",
        "congestion labels must come from a schedule, not telemetry features",
    )

    timepoint_fields = (_at(contract, ("timepoints", "fields")) or {}).keys()
    _add(
        checks,
        "required_online_timepoints",
        REQUIRED_TIMEPOINTS <= set(timepoint_fields),
        f"missing={sorted(REQUIRED_TIMEPOINTS - set(timepoint_fields))}",
    )
    _add(
        checks,
        "primary_time_semantics",
        _at(contract, ("timepoints", "primary_fault_time")) == "fault_applied_ns"
        and _at(contract, ("timepoints", "primary_alarm_time"))
        == "alarm_actionable_ns",
        f"fault={_at(contract, ('timepoints', 'primary_fault_time'))}, "
        f"alarm={_at(contract, ('timepoints', 'primary_alarm_time'))}",
    )

    forbidden = set(_at(contract, ("features", "forbidden_fields")) or [])
    _add(
        checks,
        "feature_leakage_boundary",
        {"configured_bandwidth_bps", "label", "future_samples"} <= forbidden
        and _at(contract, ("features", "utilization_denominator"))
        == "nominal_bandwidth_bps_from_immutable_link_map",
        f"required_forbidden_present={sorted({'configured_bandwidth_bps', 'label', 'future_samples'} & forbidden)}",
    )
    _add(
        checks,
        "paired_plane_semantics",
        _at(contract, ("features", "paired_plane_policy", "raw_active_standby_equality_assumption_forbidden"))
        is True
        and _at(contract, ("features", "topology_context", "independent_same_asw_same_rail_claim_allowed"))
        is False,
        "active/standby raw equality and collinear same-ASW/same-rail claims are forbidden",
    )

    slo = contract.get("slo", {})
    _add(
        checks,
        "strict_detection_slos",
        _at(slo, ("hard_detection", "strict_lt_ns")) == 1_000_000
        and _at(slo, ("gray_recall", "scheduled_event_min")) == 0.95
        and _at(slo, ("gray_recall", "observable_event_min")) == 0.95
        and _at(slo, ("gray_detection_latency", "strict_lt_ns"))
        == 100_000_000,
        f"hard={_at(slo, ('hard_detection', 'strict_lt_ns'))}, "
        f"gray_recall={slo.get('gray_recall')}, "
        f"gray_latency={_at(slo, ('gray_detection_latency', 'strict_lt_ns'))}",
    )
    _add(
        checks,
        "strict_recovery_and_correctness_slos",
        _at(slo, ("recovery_from_detection", "strict_lt_ns"))
        == 1_000_000_000
        and _at(slo, ("end_to_end_recovery", "strict_lt_ns"))
        == 1_000_000_000
        and _at(slo, ("affected_qp_selection", "precision_min")) == 1.0
        and _at(slo, ("affected_qp_selection", "recall_min")) == 1.0
        and _at(slo, ("affected_qp_selection", "unaffected_qps_moved_max")) == 0
        and _at(slo, ("collective_correctness", "correct_or_safely_redone_fraction_min"))
        == 1.0
        and _at(slo, ("collective_correctness", "silent_wrong_commits_max")) == 0,
        "both recovery bounds must be 1 s and QP/correctness gates must be exact",
    )

    stage_gates = contract.get("stage_gates", {})
    _exact_keys(
        checks,
        "all_stage_gates_declared",
        stage_gates,
        [f"P{number}" for number in range(10)],
    )
    _add(
        checks,
        "claim_boundary",
        _at(contract, ("claim_boundaries", "p8_claim"))
        == "targets_met_in_true16_dual_plane_simai_model"
        and _at(contract, ("claim_boundaries", "real_system_claim_requires_p9"))
        is True,
        f"boundaries={contract.get('claim_boundaries')}",
    )

    failures = sum(item["status"] == FAIL for item in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": PASS if failures == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failures,
        },
        "checks": checks,
    }


def write_report(result: Mapping[str, Any], out_json: Path, out_md: Path | None) -> None:
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if out_md is None:
        return
    out_md.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# LIMER experiment-contract validation",
        "",
        f"**{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed.**",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for item in result["checks"]:
        detail = str(item["detail"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {item['check']} | {item['status']} | {detail} |")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        raw = args.contract.read_text(encoding="utf-8")
        loaded = yaml.safe_load(raw)
        if not isinstance(loaded, Mapping):
            raise ValueError("contract YAML root must be a mapping")
        result = validate_contract(loaded)
        result["contract_path"] = str(args.contract.resolve())
        result["contract_sha256"] = sha256_file(args.contract)
    except (OSError, ValueError, yaml.YAMLError) as error:
        result = {
            "schema_version": SCHEMA_VERSION,
            "status": FAIL,
            "summary": {"pass": 0, "fail": 1},
            "error": str(error),
            "checks": [],
        }
    write_report(result, args.out_json, args.out_md)
    print(
        f"{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed"
    )
    print(f"Wrote {args.out_json}" + (f" and {args.out_md}" if args.out_md else ""))
    return 0 if result["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
