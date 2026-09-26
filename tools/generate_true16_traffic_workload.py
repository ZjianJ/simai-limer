#!/usr/bin/env python3
"""Generate and statically validate qualified sparse true-16 workloads.

The generated workload uses the SimAI transformer format already exercised by
the true-16 runners.  Every layer performs a blocking forward-pass AllReduce
over one 16-rank tensor-parallel group.  Two deliberately disjoint profiles
exist:

* ``horizon-prefix`` is the default P2 corpus workload: 550 small 64 KiB
  collectives separated by 1 ms of compute.  Its compute-only lower bound
  extends 30 ms beyond the corpus' maximum 520 ms observation horizon, so a
  corpus run is stopped by its observation event while a workload prefix is
  still active.
* ``completion`` is an explicit runtime-qualification profile.  It permits
  exactly 1, 10, or 32 layers (Q1/Q2/Q3) and must be run without an observation
  stop so every declared collective can reach the 16-rank commit barrier.
  A completion workload is never accepted as a P2 horizon workload.

The duration calculation is a planning estimate, not runtime evidence.
AllReduce is blocking, so simulator cost or transport stalls can make actual
issue gaps much larger than the optimistic static plan.  Runtime evidence must
therefore satisfy the profile-specific contract reported by this tool.  In
particular, a horizon run must not claim that all 550 declared collectives
completed: its observation event intentionally executes first.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


SCHEMA_VERSION = "limer.true16-traffic-workload.v3"
POLICY = "HYBRID_TRANSFORMER_FWD_IN_BCKWD"
WORLD_SIZE = 16
HORIZON_PREFIX_PROFILE = "horizon-prefix"
COMPLETION_PROFILE = "completion"
PROFILE_CHOICES = (HORIZON_PREFIX_PROFILE, COMPLETION_PROFILE)
COMPLETION_LAYER_TO_STAGE = {1: "Q1", 10: "Q2", 32: "Q3"}
COMPLETION_LAYER_COUNTS = frozenset(COMPLETION_LAYER_TO_STAGE)
DEFAULT_LAYERS = 550
DEFAULT_COMPUTE_NS = 1_000_000
DEFAULT_COLLECTIVE_BYTES = 64 * 1024
# This is derived from the current immutable P2 corpus plan.  The workload
# generator and its tests fail closed if a default horizon workload cannot
# cover this maximum.  A future corpus with a larger horizon must update this
# bound and regenerate/re-hash its workload before execution.
CORPUS_MAX_VIRTUAL_FINISH_NS = 520_000_000
HORIZON_COMPUTE_GUARD_NS = 30_000_000
REQUIRED_TRAFFIC_WINDOW_NS = CORPUS_MAX_VIRTUAL_FINISH_NS
PRE_FAULT_TRAFFIC_WINDOW_NS = 150_000_000
MAX_FIRST_ALLREDUCE_ISSUE_NS = 1_000_000
MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS = 2_000_000
# Prevent a compute-only timeline from qualifying with a token payload.  This
# 16 MiB declared-tensor floor is only a static anti-degeneracy guard; positive
# transmitted bytes must still be proved from a fault-free execution.
MIN_TOTAL_COLLECTIVE_BYTES = 16 * 1024 * 1024
# Duty cycle is descriptive, never a qualification check.  The default is
# intentionally far below this threshold so the workload remains executable.
LOW_COMM_DUTY_CYCLE_THRESHOLD = 0.10

# The target topology exposes two 100 Gbit/s ACCESS rails per GPU.  Using the
# sum is intentionally optimistic: if the workload is long enough at this
# fastest assumed rate, a single active rail only makes it longer.
MAX_AGGREGATE_HOST_BPS = 200_000_000_000

HEADER_FIELDS = {
    "model_parallel_NPU_group": WORLD_SIZE,
    "ep": 1,
    "pp": 1,
    "vpp": WORLD_SIZE,
    "ga": 1,
    "all_gpus": WORLD_SIZE,
    "checkpoints": 0,
    "checkpoint_initiates": 0,
}
HEADER = (
    f"{POLICY} model_parallel_NPU_group: {WORLD_SIZE} ep: 1 pp: 1 "
    f"vpp: {WORLD_SIZE} ga: 1 all_gpus: {WORLD_SIZE} checkpoints: 0 "
    "checkpoint_initiates: 0"
)
LAYER_FIELD_COUNT = 12


class WorkloadValidationError(ValueError):
    """Raised when a workload cannot satisfy the true-16 P2 contract."""


def _positive_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WorkloadValidationError(f"{name} must be a positive integer")
    return value


def _qualification_profile(profile: str) -> str:
    if profile not in PROFILE_CHOICES:
        raise WorkloadValidationError(
            f"profile must be one of {PROFILE_CHOICES}, observed {profile!r}"
        )
    return profile


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def estimate_duration(
    *,
    layers: int,
    compute_ns: int,
    collective_bytes: int,
    profile: str = HORIZON_PREFIX_PROFILE,
) -> Dict[str, Any]:
    """Return a fail-closed static plan for one qualification profile.

    ``optimistic_payload_*`` counts only one payload serialization per rank at
    the full bandwidth of both ACCESS rails.  It deliberately ignores the
    additional traffic required by a concrete AllReduce algorithm.  The ring
    estimate is reported separately as a useful, but less conservative,
    reference.  Neither estimate substitutes for an executed trace.
    """

    profile = _qualification_profile(profile)
    layers = _positive_int("layers", layers)
    compute_ns = _positive_int("compute_ns", compute_ns)
    collective_bytes = _positive_int("collective_bytes", collective_bytes)
    if collective_bytes < 4096:
        raise WorkloadValidationError(
            "collective_bytes must be at least 4096; SimAI otherwise rewrites "
            "the requested payload"
        )

    payload_wire_ns = _ceil_div(
        collective_bytes * 8 * 1_000_000_000,
        MAX_AGGREGATE_HOST_BPS,
    )
    optimistic_round_ns = compute_ns + payload_wire_ns
    first_issue_ns = compute_ns
    last_issue_ns = first_issue_ns + (layers - 1) * optimistic_round_ns
    issue_span_ns = last_issue_ns - first_issue_ns
    optimistic_finish_ns = last_issue_ns + payload_wire_ns
    traffic_window_ns = optimistic_finish_ns - first_issue_ns
    planned_comm_duty_cycle = payload_wire_ns / optimistic_round_ns
    planned_total_collective_bytes = layers * collective_bytes

    # Conventional ring traffic per rank is 2 * (N - 1) / N payloads.
    ring_bytes_per_rank = _ceil_div(
        2 * (WORLD_SIZE - 1) * collective_bytes,
        WORLD_SIZE,
    )
    ring_wire_ns = _ceil_div(
        ring_bytes_per_rank * 8 * 1_000_000_000,
        MAX_AGGREGATE_HOST_BPS,
    )
    ring_finish_ns = layers * (compute_ns + ring_wire_ns)

    if first_issue_ns <= REQUIRED_TRAFFIC_WINDOW_NS:
        horizon_rounds_started = min(
            layers,
            ((REQUIRED_TRAFFIC_WINDOW_NS - first_issue_ns)
             // optimistic_round_ns) + 1,
        )
    else:
        horizon_rounds_started = 0
    horizon_rounds_completed = min(
        layers, REQUIRED_TRAFFIC_WINDOW_NS // optimistic_round_ns
    )
    if first_issue_ns <= PRE_FAULT_TRAFFIC_WINDOW_NS:
        pre_fault_rounds_started = min(
            layers,
            ((PRE_FAULT_TRAFFIC_WINDOW_NS - first_issue_ns)
             // optimistic_round_ns) + 1,
        )
    else:
        pre_fault_rounds_started = 0
    pre_fault_rounds_completed = min(
        layers, PRE_FAULT_TRAFFIC_WINDOW_NS // optimistic_round_ns
    )

    compute_only_finish_lower_bound_ns = layers * compute_ns
    shared_profile_checks = {
        "compute_ns_matches_qualified_profile": (
            compute_ns == DEFAULT_COMPUTE_NS
        ),
        "collective_bytes_matches_qualified_profile": (
            collective_bytes == DEFAULT_COLLECTIVE_BYTES
        ),
        "first_allreduce_issue_within_limit": (
            first_issue_ns <= MAX_FIRST_ALLREDUCE_ISSUE_NS
        ),
    }
    horizon_checks = {
        **shared_profile_checks,
        "horizon_profile_has_at_least_default_layers": (
            layers >= DEFAULT_LAYERS
        ),
        "collective_issue_span_covers_corpus_max_horizon": (
            issue_span_ns >= CORPUS_MAX_VIRTUAL_FINISH_NS
        ),
        "compute_only_finish_lower_bound_covers_horizon_guard": (
            compute_only_finish_lower_bound_ns
            >= CORPUS_MAX_VIRTUAL_FINISH_NS + HORIZON_COMPUTE_GUARD_NS
        ),
        "inter_allreduce_issue_gap_within_limit": (
            optimistic_round_ns
            <= MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS
        ),
        "total_collective_payload_meets_minimum": (
            planned_total_collective_bytes >= MIN_TOTAL_COLLECTIVE_BYTES
        ),
    }
    completion_checks = {
        **shared_profile_checks,
        "completion_layer_count_is_q1_q2_or_q3": (
            layers in COMPLETION_LAYER_COUNTS
        ),
    }
    static_checks = (
        horizon_checks
        if profile == HORIZON_PREFIX_PROFILE
        else completion_checks
    )
    qualifies_selected_profile = all(static_checks.values())
    qualifies_horizon = (
        profile == HORIZON_PREFIX_PROFILE
        and all(horizon_checks.values())
    )
    qualifies_completion = (
        profile == COMPLETION_PROFILE
        and all(completion_checks.values())
    )
    completion_stage = (
        COMPLETION_LAYER_TO_STAGE.get(layers)
        if profile == COMPLETION_PROFILE
        else None
    )

    no_stop_completion_contract = {
        "contract_id": "no-stop-completion-v1",
        "applicable": profile == COMPLETION_PROFILE,
        "scope": "executed fault-free true-16 completion qualification",
        "observation_stop_must_be_unset": True,
        "all_declared_collectives_issued_and_completed": True,
        "each_collective_has_16_rank_start_and_commit": True,
        "finish_barrier_reports_workload_complete": True,
        "raw_collective_transaction_and_lifecycle_required": True,
        "may_qualify_p2_horizon_prefix": False,
    }
    horizon_prefix_contract = {
        "contract_id": "p2-horizon-prefix-v1",
        "applicable": profile == HORIZON_PREFIX_PROFILE,
        "scope": "executed fault-free true-16 maximum-horizon reference run",
        "observation_stop_ns": CORPUS_MAX_VIRTUAL_FINISH_NS,
        "expected_lifecycle_status":
            "OBSERVATION_WINDOW_COMPLETE_WORKLOAD_INCOMPLETE",
        "declared_workload_completion_expected": False,
        "all_declared_collectives_completion_required": False,
        "all_16_ranks_make_progress": True,
        "first_full_collective_start_ns_at_most":
            MAX_FIRST_ALLREDUCE_ISSUE_NS,
        "maximum_inter_full_collective_start_gap_ns_at_most":
            MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS,
        "maximum_horizon_to_last_full_start_gap_ns_at_most":
            MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS,
        "all_prior_fully_started_collectives_committed": True,
        "maximum_inflight_collectives_at_horizon": 1,
        "nic_and_switch_tx_byte_growth_strictly_positive": True,
        "raw_collective_transaction_lifecycle_and_telemetry_required": True,
    }
    active_runtime_contract = (
        horizon_prefix_contract
        if profile == HORIZON_PREFIX_PROFILE
        else no_stop_completion_contract
    )
    return {
        "qualification_profile": profile,
        "completion_stage": completion_stage,
        "assumed_max_aggregate_host_bps": MAX_AGGREGATE_HOST_BPS,
        "assumed_access_rails": 2,
        "assumed_bandwidth_per_rail_bps": 100_000_000_000,
        "planned_first_allreduce_issue_ns": first_issue_ns,
        "planned_last_allreduce_issue_ns": last_issue_ns,
        "planned_max_inter_round_compute_gap_ns": compute_ns,
        "planned_inter_allreduce_issue_gap_ns": optimistic_round_ns,
        "maximum_allowed_inter_allreduce_issue_gap_ns":
            MAX_PLANNED_INTER_ALLREDUCE_ISSUE_GAP_NS,
        "planned_collective_opportunity_count": layers,
        "planned_collective_opportunity_span_ns": issue_span_ns,
        "optimistic_payload_serialization_per_round_ns": payload_wire_ns,
        "optimistic_round_period_ns": optimistic_round_ns,
        "optimistic_payload_comm_duty_cycle": planned_comm_duty_cycle,
        "low_communication_duty_cycle": (
            planned_comm_duty_cycle < LOW_COMM_DUTY_CYCLE_THRESHOLD
        ),
        "low_communication_duty_cycle_threshold":
            LOW_COMM_DUTY_CYCLE_THRESHOLD,
        "communication_duty_cycle_is_a_qualification_requirement": False,
        "planned_total_collective_payload_bytes":
            planned_total_collective_bytes,
        "minimum_total_collective_payload_bytes":
            MIN_TOTAL_COLLECTIVE_BYTES,
        "compute_only_workload_finish_lower_bound_ns":
            compute_only_finish_lower_bound_ns,
        "corpus_max_virtual_finish_ns": CORPUS_MAX_VIRTUAL_FINISH_NS,
        "horizon_compute_guard_ns": HORIZON_COMPUTE_GUARD_NS,
        "optimistic_workload_finish_ns": optimistic_finish_ns,
        "optimistic_allreduce_window_ns": traffic_window_ns,
        "conventional_ring_bytes_per_rank_per_round": ring_bytes_per_rank,
        "conventional_ring_serialization_per_round_ns": ring_wire_ns,
        "conventional_ring_workload_finish_ns": ring_finish_ns,
        "required_detection_horizon_traffic_ns": REQUIRED_TRAFFIC_WINDOW_NS,
        "required_pre_fault_traffic_window_ns": PRE_FAULT_TRAFFIC_WINDOW_NS,
        "maximum_allowed_first_allreduce_issue_ns":
            MAX_FIRST_ALLREDUCE_ISSUE_NS,
        "estimated_rounds_started_before_150ms": pre_fault_rounds_started,
        "estimated_rounds_completed_before_150ms": pre_fault_rounds_completed,
        "estimated_rounds_started_before_detection_horizon":
            horizon_rounds_started,
        "estimated_rounds_completed_before_detection_horizon":
            horizon_rounds_completed,
        "static_qualification_checks": static_checks,
        "horizon_static_qualification_checks": horizon_checks,
        "completion_static_qualification_checks": completion_checks,
        "qualifies_selected_profile": qualifies_selected_profile,
        "qualifies_static_detection_horizon_plan": qualifies_horizon,
        "qualifies_static_horizon_prefix_plan": qualifies_horizon,
        "qualifies_static_no_stop_completion_plan": qualifies_completion,
        "may_be_used_as_p2_horizon_workload": qualifies_horizon,
        "runtime_evidence": False,
        "runtime_evidence_contract": active_runtime_contract,
        "runtime_evidence_contracts": {
            "no_stop_completion": no_stop_completion_contract,
            "horizon_prefix": horizon_prefix_contract,
        },
        "estimate_semantics": (
            "profile-specific static planning only; completion profiles prove "
            "all declared collectives only in separate no-stop runs, whereas "
            "the P2 horizon profile proves a live committed prefix through the "
            "observation stop and intentionally leaves the declared suffix "
            "incomplete; all runtime claims require executed artifacts"
        ),
    }


def build_workload_text(
    *,
    layers: int,
    compute_ns: int,
    collective_bytes: int,
    profile: str = HORIZON_PREFIX_PROFILE,
) -> str:
    """Build deterministic text, rejecting a profile-contract mismatch."""

    estimate = estimate_duration(
        layers=layers,
        compute_ns=compute_ns,
        collective_bytes=collective_bytes,
        profile=profile,
    )
    if not estimate["qualifies_selected_profile"]:
        failed = ", ".join(
            name
            for name, passed in estimate["static_qualification_checks"].items()
            if not passed
        )
        raise WorkloadValidationError(
            f"configuration does not satisfy {profile!r} profile; "
            f"failed checks: {failed}"
        )

    rows = [HEADER, str(layers)]
    rows.extend(
        f"p2_layer_{index:04d} -1 {compute_ns} ALLREDUCE "
        f"{collective_bytes} 1 NONE 0 1 NONE 0 1"
        for index in range(layers)
    )
    return "\n".join(rows) + "\n"


def _header_value(header: str, name: str) -> Optional[int]:
    match = re.search(
        rf"(?:^|\s){re.escape(name)}:\s*(\d+)(?:\s|$)", header
    )
    return int(match.group(1)) if match else None


def validate_workload(
    path: Path, *, profile: str = HORIZON_PREFIX_PROFILE
) -> Dict[str, Any]:
    """Validate every row against an explicit qualification profile.

    The default is intentionally the P2 horizon profile.  Consequently, a
    short Q1/Q2/Q3 file cannot receive a generic PASS unless its caller opts
    into ``profile=completion`` and therefore also receives an explicit
    ``may_be_used_as_p2_horizon_workload=false`` result.
    """

    profile = _qualification_profile(profile)
    path = Path(path)
    if not path.is_file():
        raise WorkloadValidationError(f"workload does not exist: {path}")
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise WorkloadValidationError("workload must be UTF-8") from error
    if not text.endswith("\n"):
        raise WorkloadValidationError("workload must end with one newline")
    lines = text.splitlines()
    if len(lines) < 3:
        raise WorkloadValidationError("workload must contain header, count, rows")

    header_tokens = lines[0].split()
    if not header_tokens or header_tokens[0] != POLICY:
        raise WorkloadValidationError(f"unexpected policy header: {lines[0]}")
    for name, expected in HEADER_FIELDS.items():
        observed = _header_value(lines[0], name)
        if observed != expected:
            raise WorkloadValidationError(
                f"header {name} must be {expected}, observed {observed}"
            )
    try:
        layer_count = int(lines[1])
    except ValueError as error:
        raise WorkloadValidationError("layer count must be an integer") from error
    if layer_count <= 0:
        raise WorkloadValidationError("layer count must be positive")
    if len(lines) != layer_count + 2:
        raise WorkloadValidationError(
            f"declared {layer_count} layers but found {len(lines) - 2} rows"
        )

    compute_ns: Optional[int] = None
    collective_bytes: Optional[int] = None
    for index, row in enumerate(lines[2:]):
        fields = row.split()
        if len(fields) != LAYER_FIELD_COUNT:
            raise WorkloadValidationError(
                f"layer {index} must contain {LAYER_FIELD_COUNT} fields, "
                f"found {len(fields)}"
            )
        expected_id = f"p2_layer_{index:04d}"
        if fields[0] != expected_id:
            raise WorkloadValidationError(
                f"layer {index} id must be {expected_id}, found {fields[0]}"
            )
        expected_fixed = {
            1: "-1",
            3: "ALLREDUCE",
            5: "1",
            6: "NONE",
            7: "0",
            8: "1",
            9: "NONE",
            10: "0",
            11: "1",
        }
        for field_index, expected in expected_fixed.items():
            if fields[field_index] != expected:
                raise WorkloadValidationError(
                    f"layer {index} field {field_index} must be {expected}, "
                    f"found {fields[field_index]}"
                )
        try:
            row_compute_ns = int(fields[2])
            row_collective_bytes = int(fields[4])
        except ValueError as error:
            raise WorkloadValidationError(
                f"layer {index} compute and payload must be integers"
            ) from error
        _positive_int(f"layer {index} compute_ns", row_compute_ns)
        _positive_int(
            f"layer {index} collective_bytes", row_collective_bytes
        )
        if compute_ns is None:
            compute_ns = row_compute_ns
            collective_bytes = row_collective_bytes
        elif (
            row_compute_ns != compute_ns
            or row_collective_bytes != collective_bytes
        ):
            raise WorkloadValidationError(
                "all layers must use identical compute and collective sizes"
            )

    assert compute_ns is not None and collective_bytes is not None
    estimate = estimate_duration(
        layers=layer_count,
        compute_ns=compute_ns,
        collective_bytes=collective_bytes,
        profile=profile,
    )
    if not estimate["qualifies_selected_profile"]:
        failed = ", ".join(
            name
            for name, passed in estimate["static_qualification_checks"].items()
            if not passed
        )
        raise WorkloadValidationError(
            "workload is structurally valid but does not satisfy "
            f"{profile!r} profile; failed checks: {failed}"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "qualification_profile": profile,
        "completion_stage": estimate["completion_stage"],
        "may_be_used_as_p2_horizon_workload":
            estimate["may_be_used_as_p2_horizon_workload"],
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "policy": POLICY,
        "world_size": WORLD_SIZE,
        "declared_all_gpus": HEADER_FIELDS["all_gpus"],
        "layer_count": layer_count,
        "validated_layer_count": layer_count,
        "fields_per_layer": LAYER_FIELD_COUNT,
        "compute_ns_per_layer": compute_ns,
        "collective": "ALLREDUCE",
        "collective_bytes_per_layer": collective_bytes,
        "all_layer_rows_validated": True,
        "duration_estimate": estimate,
    }


def generate_workload(
    *,
    output: Path,
    layers: int,
    compute_ns: int,
    collective_bytes: int,
    profile: str = HORIZON_PREFIX_PROFILE,
) -> Dict[str, Any]:
    """Write one qualified deterministic workload and validate it again."""

    text = build_workload_text(
        layers=layers,
        compute_ns=compute_ns,
        collective_bytes=collective_bytes,
        profile=profile,
    )
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")
    return validate_workload(output, profile=profile)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--profile",
        choices=PROFILE_CHOICES,
        default=HORIZON_PREFIX_PROFILE,
        help=(
            "qualification profile; horizon-prefix defaults to 550 layers, "
            "while completion requires an explicit 1/10/32 --layers"
        ),
    )
    parser.add_argument(
        "--layers",
        type=int,
        help=(
            "layer count; defaults to 550 for horizon-prefix and is required "
            "to select Q1/Q2/Q3 for completion"
        ),
    )
    parser.add_argument("--compute-ns", type=int, default=DEFAULT_COMPUTE_NS)
    parser.add_argument(
        "--collective-bytes", type=int, default=DEFAULT_COLLECTIVE_BYTES
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate an existing --output instead of generating it",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.check:
            if args.layers is not None:
                raise WorkloadValidationError(
                    "--layers is not accepted with --check; the file's "
                    "declared count is validated against --profile"
                )
            report = validate_workload(args.output, profile=args.profile)
        else:
            layers = args.layers
            if layers is None:
                if args.profile == COMPLETION_PROFILE:
                    raise WorkloadValidationError(
                        "completion profile requires explicit --layers "
                        "equal to 1, 10, or 32"
                    )
                layers = DEFAULT_LAYERS
            report = generate_workload(
                output=args.output,
                layers=layers,
                compute_ns=args.compute_ns,
                collective_bytes=args.collective_bytes,
                profile=args.profile,
            )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (OSError, WorkloadValidationError) as error:
        print(f"generate_true16_traffic_workload: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
