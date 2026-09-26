#!/usr/bin/env python3
"""Generate and verify the immutable true-16 P2 corpus plan.

This is a schedule/manifest generator, not an experiment runner.  It produces
ground-truth sidecars before simulation, assigns complete runs to locked
partitions, and records unsupported/pending execution gates honestly.  It does
not read telemetry and never derives congestion truth or labels from features.

The legacy single-plane generator is intentionally independent of this file.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import struct
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml

import generate_true16_traffic_workload as traffic_workload


CORPUS_SCHEMA = "limer.p2-corpus-manifest.v1"
SPLIT_SCHEMA = "limer.p2-split-manifest.v1"
FEATURE_SCHEMA = "limer.p2-feature-schema.v1"
CHECK_SCHEMA = "limer.p2-corpus-check.v1"
PASS = "PASS"
FAIL = "FAIL"
PENDING = "PENDING"

PARTITIONS = (
    "train",
    "validation",
    "seen_link_test",
    "unseen_link_test",
    "ood_stress",
)

GRAY_CATEGORIES = {
    "capacity": ("bandwidth_degradation",),
    "loss": ("random_loss", "burst_loss"),
    "service": ("service_degradation",),
    "intermittent": ("intermittent_service",),
}

INJECTOR_CSV_COLUMNS = (
    # The first nine fields preserve compatibility with the current C++
    # FaultInjector.  The remaining fields bind injector segments to the
    # separately hash-locked canonical truth event.
    "fault_id",
    "fault_type",
    "target_link_id",
    "start_time_ns",
    "end_time_ns",
    "severity",
    "parameter_before",
    "parameter_after",
    "recovery_delay_ns",
    "event_id",
    "parent_event_id",
    "fault_family",
    "target_gpu",
    "severity_name",
    "severity_value",
    "severity_unit",
    "shape",
    "ramp_duration_ns",
    "effects",
    "impairment",
    "carrier_state",
    "implementation_status",
    "segment_index",
    "permanent",
)

FAULT_TRUTH_CSV_COLUMNS = (
    "event_id",
    "fault_family",
    "target_gpu",
    "target_link_id",
    "start_time_ns",
    "end_time_ns",
    "severity_name",
    "severity_value",
    "severity_unit",
    "shape",
    "ramp_duration_ns",
    "effects",
    "impairment",
    "carrier_state",
    "implementation_status",
    "permanent",
    "targets_json",
)

NEGATIVE_CSV_COLUMNS = (
    "event_id",
    "scenario",
    "start_time_ns",
    "end_time_ns",
    "truth_source",
    "action_scope",
    "action_name",
    "action_parameters_json",
    "implementation_status",
)

BACKGROUND_FLOW_CSV_COLUMNS = (
    "event_id",
    "flow_id",
    "scenario",
    "scheduled_start_ns",
    "src_rank",
    "dst_rank",
    "bytes",
    "pg",
    "sport",
    "dport",
)

EXECUTABLE_BACKGROUND_SCENARIOS = frozenset({"incast", "queue_buildup"})
EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS = frozenset(
    {"allreduce_burst", "high_utilization"}
)
# Compatibility name for callers which enumerate the two A1 profiles.  The
# sidecars themselves are no longer static-only: execution is admitted only
# after the runner independently reconstructs both files.
STATIC_COLLECTIVE_OVERRIDE_SCENARIOS = EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS
COLLECTIVE_OVERRIDE_FORMAT = "simai-transformer-workload-v2"
COLLECTIVE_ROLE_FORMAT = "limer-p2-collective-layer-roles-v1"
COLLECTIVE_OVERRIDE_STATUS = "EXECUTABLE_COLLECTIVE_WORKLOAD_OVERRIDE"
COLLECTIVE_OVERRIDE_PROFILE_BURST = "p2-allreduce-burst"
COLLECTIVE_OVERRIDE_PROFILE_HIGH_UTIL = "p2-high-utilization"
COLLECTIVE_PRE_LAYER_COUNT = 130
COLLECTIVE_FULL_LAYER_COUNT = 550
COLLECTIVE_RUNTIME_HORIZON_NS = 520_000_000
COLLECTIVE_EVENT_WINDOW_NS = 200_000_000
COLLECTIVE_PRE_START_SPAN_NS = 100_000_000
COLLECTIVE_PRE_TO_EVENT_MAX_GAP_NS = 50_000_000
COLLECTIVE_POST_TAIL_MAX_GAP_NS = 10_000_000
COLLECTIVE_BURST_COUNT = 8
COLLECTIVE_BURST_GAP_NS = 1
COLLECTIVE_BURST_MAX_START_GAP_NS = 2_000_000
COLLECTIVE_BURST_MAX_SPAN_NS = 20_000_000
COLLECTIVE_PRESSURE_BYTES = 16 * 1024 * 1024
COLLECTIVE_UTILIZATION_THRESHOLD = 0.8
COLLECTIVE_UTILIZATION_COVERAGE = 0.8
COLLECTIVE_ROLE_CSV_COLUMNS = (
    "run_id",
    "scenario",
    "layer_num",
    "layer_id",
    "phase",
    "role",
    "scheduled_onset_ns",
    "scheduled_end_ns",
    "planned_issue_ns",
    "compute_ns",
    "collective_type",
    "message_size_bytes",
)
ECMP_COLLISION_STATUS = "BLOCKED_STATIC_ECMP_COLLISION_PENDING_RUNTIME_ROUTE_EVIDENCE"
ECMP_COLLISION_FORMAT = "limer-ecmp-collision-v1"
ECMP_COLLISION_COLUMNS = (
    "event_id",
    "flow_group_id",
    "flow_id",
    "scheduled_start_ns",
    "src_rank",
    "dst_rank",
    "bytes",
    "pg",
    "sport",
    "dport",
    "tuple_bytes_hex",
    "hash_seed_u32",
    "expected_hash_u32",
    "expected_bucket",
    "candidate_count",
    "asw_node_id",
    "destination_node_id",
    "expected_egress_port_id",
    "expected_next_hop_node_id",
    "expected_link_id",
    "role",
)

MODEL_FEATURE_COLUMNS = (
    "tx_rate_gbps",
    "rx_rate_gbps",
    "nominal_utilization",
    "queue_bytes",
    "queue_peak_bytes",
    "tx_rx_asymmetry",
    "drop_delta",
    "link_error_delta",
    "ecn_delta",
    "pfc_delta",
    "link_state",
    "flap_delta",
    "throughput_residual",
    "queue_slope",
    "peer_directional_rate_gap",
    "active_same_asw_median_rate",
    "cross_plane_role_normalized_residual",
)

IDENTIFIER_COLUMNS = (
    "run_id",
    "timestamp_ns",
    "link_id",
    "switch_id",
    "port_id",
    "direction",
    "gpu_id",
    "plane_id",
    "asw_id",
    "peer_link_id",
    "paired_link_id",
)

LABEL_COLUMNS = (
    "class_label",
    "fault_family",
    "event_id",
    "observable",
)

LEAKAGE_PATTERNS = (
    "configured",
    "fault",
    "severity",
    "label",
    "injected",
    "schedule",
    "parameter",
    "target",
    "split",
    "future",
)

BASE_ONSET_NS = 150_000_000
# P2 is a detection-corpus stage and recovery actions are explicitly disabled.
# Keep a causal 100 ms detection tail after an event.  The separate 1 s SLO
# applies only to runs explicitly marked recovery_evaluation=true in P6+.
DETECTION_POST_EVENT_NS = 100_000_000
RECOVERY_POST_FAULT_OBSERVATION_NS = 1_000_000_000
DEFAULT_EVENT_DURATION_NS = 200_000_000
# A finite background congestion event is truthfully defined by its
# predeclared QP-launch window.  Completion is runtime evidence, bounded by an
# independent deadline; it is never guessed as a 200 ms pressure interval.
BACKGROUND_COMPLETION_DEADLINE_NS = 200_000_000
BACKGROUND_RDMA_RTO_US = 250_000
BACKGROUND_RDMA_RETRY_LIMIT = 0
MURMUR3_SEED_U32 = 0x8BADF00D
UINT32_MASK = 0xFFFFFFFF
HASH_BYTE_ORDER = "little"


class CorpusError(RuntimeError):
    """The P2 plan violates its generation contract."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def _rotl32(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (32 - shift))) & UINT32_MASK


def ns3_murmur3_x86_32(data: bytes, seed: int = MURMUR3_SEED_U32) -> int:
    """Match ns-3 Hash32/Murmur3 for the 12-byte native little-endian tuple."""
    if len(data) % 4:
        raise CorpusError("P2 RDMA hash helper accepts complete uint32 blocks only")
    value = seed & UINT32_MASK
    for offset in range(0, len(data), 4):
        block = int.from_bytes(data[offset : offset + 4], "little")
        block = (block * 0xCC9E2D51) & UINT32_MASK
        block = _rotl32(block, 15)
        block = (block * 0x1B873593) & UINT32_MASK
        value ^= block
        value = _rotl32(value, 13)
        value = (value * 5 + 0xE6546B64) & UINT32_MASK
    value ^= len(data)
    value ^= value >> 16
    value = (value * 0x85EBCA6B) & UINT32_MASK
    value ^= value >> 13
    value = (value * 0xC2B2AE35) & UINT32_MASK
    value ^= value >> 16
    return value & UINT32_MASK


def rank_ipv4_u32(rank: int) -> int:
    """Mirror common.h's 11.0.<rank>.1 serverAddress construction."""
    if not 0 <= rank < 256:
        raise CorpusError(f"rank cannot be represented by P2 address rule: {rank}")
    return 0x0B000001 + (rank << 8)


def rdma_route_bucket(
    *, src: int, dst: int, sport: int, dport: int, reverse: bool = False
) -> int:
    sip, dip = rank_ipv4_u32(src), rank_ipv4_u32(dst)
    if reverse:
        sip, dip, sport, dport = dip, sip, dport, sport
    packed = struct.pack("<IIHH", sip, dip, sport, dport)
    return ns3_murmur3_x86_32(packed) % 2


def select_single_rail_sport(
    *, src: int, dst: int, dport: int, route_bucket: int, used: set[int]
) -> int:
    """Choose a reserved source port whose data and ACK hashes use one rail."""
    for sport in range(49152, 65536):
        if sport in used:
            continue
        if (
            rdma_route_bucket(src=src, dst=dst, sport=sport, dport=dport)
            == route_bucket
            and rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport, reverse=True
            )
            == route_bucket
        ):
            used.add(sport)
            return sport
    raise CorpusError(
        f"no reserved sport pins {src}->{dst} dport={dport} to bucket {route_bucket}"
    )


def schedule_identity_tuple(run: Mapping[str, Any]) -> Tuple[Any, ...]:
    """Bind optional execution sidecars without changing legacy v1 tuples."""
    schedule = run["schedule"]
    material: List[Any] = [
        run["run_id"],
        schedule["sha256"],
        schedule.get("simulator_injection_schedule", {}).get("sha256"),
    ]
    background = schedule.get("background_flow_schedule")
    if isinstance(background, Mapping):
        material.append(background.get("sha256"))
    collective = schedule.get("collective_workload_override")
    if isinstance(collective, Mapping):
        material.append(collective.get("sha256"))
        material.append(
            collective.get("layer_role_sidecar", {}).get("sha256")
        )
    ecmp = schedule.get("ecmp_collision_schedule")
    if isinstance(ecmp, Mapping):
        material.append(ecmp.get("sha256"))
    return tuple(material)


def run_identity_entry(run: Mapping[str, Any]) -> Dict[str, Any]:
    schedule = run["schedule"]
    entry = {
        "run_id": run["run_id"],
        "partition": run["partition"],
        "split_group_id": run["split_group_id"],
        "schedule_sha256": schedule["sha256"],
        "simulator_injection_sha256": schedule.get(
            "simulator_injection_schedule", {}
        ).get("sha256"),
    }
    background = schedule.get("background_flow_schedule")
    if isinstance(background, Mapping):
        entry["background_flow_sha256"] = background.get("sha256")
    collective = schedule.get("collective_workload_override")
    if isinstance(collective, Mapping):
        entry["collective_workload_override_sha256"] = collective.get("sha256")
        entry["collective_layer_role_sha256"] = collective.get(
            "layer_role_sidecar", {}
        ).get("sha256")
    entry["effective_workload_sha256"] = run.get(
        "effective_workload_sha256", run.get("workload_sha256")
    )
    ecmp = schedule.get("ecmp_collision_schedule")
    if isinstance(ecmp, Mapping):
        entry["ecmp_collision_sha256"] = ecmp.get("sha256")
    return entry


def stable_int(seed: int, *parts: Any, modulo: Optional[int] = None) -> int:
    material = ":".join([str(seed), *(str(part) for part in parts)])
    value = int.from_bytes(hashlib.sha256(material.encode("utf-8")).digest()[:8], "big")
    return value if modulo is None else value % modulo


def simulation_seed(seed: int, run_id: str) -> int:
    return stable_int(seed, "simulation", run_id, modulo=2_147_483_647) + 1


def write_json(path: Path, value: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return sha256_file(path)


def write_csv(
    path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]]
) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})
    return sha256_file(path)


def safe_slug(value: Any) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    if not slug:
        raise CorpusError(f"cannot construct slug from {value!r}")
    return slug


def load_contract(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise CorpusError(f"{path}: contract root must be a mapping")
    required = [
        "contract_id",
        "topology",
        "workload",
        "fault_taxonomy",
        "features",
        "splits",
        "stage_gates",
    ]
    missing = [key for key in required if key not in value]
    if missing:
        raise CorpusError(f"{path}: missing contract keys {missing}")
    if value["splits"]["partitions"] != list(PARTITIONS):
        raise CorpusError(
            f"contract partitions changed: {value['splits']['partitions']!r}"
        )
    return value


def link_host_identity(row: Mapping[str, str]) -> Optional[Tuple[int, int]]:
    if row.get("src_type") == "HOST":
        return int(row["src_node"]), int(row["src_port"])
    if row.get("dst_type") == "HOST":
        return int(row["dst_node"]), int(row["dst_port"])
    return None


def load_true16_links(
    path: Path, contract: Mapping[str, Any]
) -> Tuple[Dict[int, Dict[str, Dict[str, Any]]], List[Dict[str, Any]]]:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    required = {
        "link_id",
        "src_node",
        "dst_node",
        "src_type",
        "dst_type",
        "src_port",
        "dst_port",
        "link_class",
        "bandwidth_bps",
    }
    if not rows or not required.issubset(rows[0]):
        raise CorpusError(f"{path}: link_map schema is incomplete")

    plane_contract = {
        "A": contract["topology"]["plane_a"],
        "B": contract["topology"]["plane_b"],
    }
    plane_ports = {
        int(value["host_port"]): plane for plane, value in plane_contract.items()
    }
    route_indices = {
        plane: int(value["route_bucket"]) for plane, value in plane_contract.items()
    }
    if set(route_indices.values()) != {0, 1}:
        raise CorpusError(
            f"dual-plane route_bucket must be exactly 0/1: {route_indices}"
        )
    pairs: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    non_access: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        row["bandwidth_bps"] = int(row["bandwidth_bps"])
        if row["link_class"] != "ACCESS":
            non_access.append(row)
            continue
        host = link_host_identity(raw)
        if host is None:
            raise CorpusError(f"ACCESS link lacks HOST endpoint: {row['link_id']}")
        gpu_id, host_port = host
        plane = plane_ports.get(host_port)
        if plane is None:
            raise CorpusError(
                f"ACCESS {row['link_id']} has unexpected host port {host_port}"
            )
        if plane in pairs[gpu_id]:
            raise CorpusError(f"GPU {gpu_id} has duplicate Plane-{plane} ACCESS links")
        pairs[gpu_id][plane] = {
            "link_id": row["link_id"],
            "gpu_id": gpu_id,
            "plane": plane,
            "host_port": host_port,
            "route_bucket": route_indices[plane],
            "bandwidth_bps": row["bandwidth_bps"],
        }

    expected_gpus = int(contract["topology"]["generator"]["gpu_count"])
    expected_access = int(
        contract["topology"]["invariants"]["link_class_counts"]["ACCESS"]
    )
    if len(pairs) != expected_gpus:
        raise CorpusError(f"expected {expected_gpus} GPU pairs, found {len(pairs)}")
    if sum(len(value) for value in pairs.values()) != expected_access:
        raise CorpusError(
            f"expected {expected_access} ACCESS links, found "
            f"{sum(len(value) for value in pairs.values())}"
        )
    for gpu_id in range(expected_gpus):
        if set(pairs[gpu_id]) != {"A", "B"}:
            raise CorpusError(f"GPU {gpu_id} does not have exactly one A/B pair")
    return dict(sorted(pairs.items())), non_access


def load_physical_links(path: Path) -> List[Dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    required = {
        "link_id",
        "src_node",
        "dst_node",
        "src_type",
        "dst_type",
        "src_port",
        "dst_port",
    }
    if not rows or not required.issubset(rows[0]):
        raise CorpusError("physical link map lacks ECMP topology fields")
    return [
        {
            **row,
            "src_node": int(row["src_node"]),
            "dst_node": int(row["dst_node"]),
            "src_port": int(row["src_port"]),
            "dst_port": int(row["dst_port"]),
        }
        for row in rows
    ]


def stable_ecmp_candidates(
    links: Sequence[Mapping[str, Any]], asw_node: int, destination_node: int
) -> List[Dict[str, Any]]:
    """Rebuild shortest-path candidates and the C++ (interface,node) order."""
    adjacency: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    node_types: Dict[int, str] = {}
    for row in links:
        src, dst = int(row["src_node"]), int(row["dst_node"])
        node_types[src], node_types[dst] = str(row["src_type"]), str(row["dst_type"])
        adjacency[src].append(
            {"node": dst, "port": int(row["src_port"]), "link_id": row["link_id"]}
        )
        adjacency[dst].append(
            {"node": src, "port": int(row["dst_port"]), "link_id": row["link_id"]}
        )
    if (
        node_types.get(asw_node) != "SWITCH"
        or node_types.get(destination_node) != "HOST"
    ):
        raise CorpusError("ECMP contract requires a SWITCH source and HOST destination")
    distances = {destination_node: 0}
    queue = [destination_node]
    for node in queue:
        for edge in adjacency[node]:
            neighbor = int(edge["node"])
            if neighbor not in distances:
                distances[neighbor] = distances[node] + 1
                if node_types.get(neighbor) in {"SWITCH", "NVSWITCH"}:
                    queue.append(neighbor)
    if asw_node not in distances:
        raise CorpusError("selected ASW cannot reach ECMP destination")
    candidates = [
        dict(edge)
        for edge in adjacency[asw_node]
        if distances.get(int(edge["node"])) == distances[asw_node] - 1
    ]
    candidates.sort(key=lambda edge: (int(edge["port"]), int(edge["node"])))
    if len(candidates) < 2:
        raise CorpusError("selected ASW route has fewer than two ECMP candidates")
    return candidates


def ecmp_collision_rows(
    *,
    event_id: str,
    start_ns: int,
    replicate: int,
    links: Sequence[Mapping[str, Any]],
    pairs: Mapping[int, Mapping[str, Mapping[str, Any]]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    src_rank, dst_rank = replicate * 4, replicate * 4 + 1
    target = pairs[src_rank]["A"]
    asw_node = next(
        int(row["dst_node"] if row["src_type"] == "HOST" else row["src_node"])
        for row in links
        if row["link_id"] == target["link_id"]
    )
    candidates = stable_ecmp_candidates(links, asw_node, dst_rank)
    collision_bucket = replicate % len(candidates)
    control_bucket = (collision_bucket + 1) % len(candidates)
    rows: List[Dict[str, Any]] = []
    dport = 21000 + replicate
    used: set[int] = set()
    wanted = [collision_bucket] * 8 + [control_bucket]
    for index, bucket in enumerate(wanted):
        for sport in range(49152, 65536):
            if sport in used or rdma_route_bucket(
                src=src_rank, dst=dst_rank, sport=sport, dport=dport
            ) != int(target["route_bucket"]):
                continue
            packed = struct.pack(
                "<IIHH",
                rank_ipv4_u32(src_rank),
                rank_ipv4_u32(dst_rank),
                sport,
                dport,
            )
            hash_value = ns3_murmur3_x86_32(packed, seed=asw_node)
            if hash_value % len(candidates) == bucket:
                used.add(sport)
                candidate = candidates[bucket]
                rows.append(
                    {
                        "event_id": event_id,
                        "flow_group_id": f"ecmp-{event_id}",
                        "flow_id": f"{event_id}-flow-{index:02d}",
                        "scheduled_start_ns": start_ns,
                        "src_rank": src_rank,
                        "dst_rank": dst_rank,
                        "bytes": 8 * 1024 * 1024,
                        "pg": 3,
                        "sport": sport,
                        "dport": dport,
                        "tuple_bytes_hex": packed.hex(),
                        "hash_seed_u32": asw_node,
                        "expected_hash_u32": hash_value,
                        "expected_bucket": bucket,
                        "candidate_count": len(candidates),
                        "asw_node_id": asw_node,
                        "destination_node_id": dst_rank,
                        "expected_egress_port_id": candidate["port"],
                        "expected_next_hop_node_id": candidate["node"],
                        "expected_link_id": candidate["link_id"],
                        "role": "COLLISION" if index < 8 else "CONTROL",
                    }
                )
                break
        else:
            raise CorpusError(f"cannot find ECMP tuple for bucket {bucket}")
    return rows, {
        "asw_node_id": asw_node,
        "destination_node_id": dst_rank,
        "hash_algorithm": "ns3-switch-murmur3-x86-32",
        "hash_seed_u32": asw_node,
        "hash_byte_order": HASH_BYTE_ORDER,
        "hash_tuple": "native-le-sip-dip-sport-dport",
        "candidate_order": "local-interface-ascending,next-node-id-ascending",
        "candidate_count": len(candidates),
        "collision_bucket": collision_bucket,
        "control_bucket": control_bucket,
    }


def validate_ecmp_collision_schedule(
    path: Path, contract: Mapping[str, Any], links: Sequence[Mapping[str, Any]]
) -> Dict[str, Any]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
    if tuple(reader.fieldnames or ()) != ECMP_COLLISION_COLUMNS:
        raise CorpusError("ECMP collision schedule header is invalid")
    asw = int(contract["asw_node_id"])
    destination = int(contract["destination_node_id"])
    candidates = stable_ecmp_candidates(links, asw, destination)
    if int(contract["candidate_count"]) != len(candidates):
        raise CorpusError("ECMP collision candidate count changed")
    collision_bucket = int(contract["collision_bucket"])
    control_bucket = int(contract["control_bucket"])
    if collision_bucket == control_bucket:
        raise CorpusError("ECMP collision lacks a distinct control bucket")
    collision_rows = 0
    control_rows = 0
    tuples: set[Tuple[int, int, int, int]] = set()
    for row in rows:
        try:
            src, dst = int(row["src_rank"]), int(row["dst_rank"])
            sport, dport = int(row["sport"]), int(row["dport"])
            seed = int(row["hash_seed_u32"])
            packed = struct.pack(
                "<IIHH", rank_ipv4_u32(src), rank_ipv4_u32(dst), sport, dport
            )
            observed_hash = ns3_murmur3_x86_32(packed, seed=seed)
            bucket = observed_hash % len(candidates)
            candidate = candidates[bucket]
            valid = (
                seed == asw
                and dst == destination
                and row["tuple_bytes_hex"] == packed.hex()
                and int(row["expected_hash_u32"]) == observed_hash
                and int(row["expected_bucket"]) == bucket
                and int(row["candidate_count"]) == len(candidates)
                and int(row["asw_node_id"]) == asw
                and int(row["destination_node_id"]) == destination
                and int(row["expected_egress_port_id"]) == int(candidate["port"])
                and int(row["expected_next_hop_node_id"]) == int(candidate["node"])
                and row["expected_link_id"] == candidate["link_id"]
            )
        except (KeyError, TypeError, ValueError, struct.error):
            valid = False
        if not valid:
            raise CorpusError(f"ECMP collision row is invalid: {row.get('flow_id')}")
        identity = (src, dst, sport, dport)
        if identity in tuples:
            raise CorpusError("ECMP collision five-tuples are not unique")
        tuples.add(identity)
        if row["role"] == "COLLISION" and bucket == collision_bucket:
            collision_rows += 1
        elif row["role"] == "CONTROL" and bucket == control_bucket:
            control_rows += 1
        else:
            raise CorpusError("ECMP collision role/bucket contract is invalid")
    if collision_rows < 8 or control_rows < 1:
        raise CorpusError("ECMP schedule lacks collision/control coverage")
    return {
        "schema_version": "limer.p2-ecmp-collision-static.v1",
        "status": "PASS",
        "sha256": sha256_file(path),
        "row_count": len(rows),
        "collision_row_count": collision_rows,
        "control_row_count": control_rows,
        "candidate_count": len(candidates),
    }


def deterministic_holdouts(seed: int, gpu_ids: Iterable[int], count: int) -> List[int]:
    ordered = sorted(gpu_ids, key=lambda gpu: (stable_int(seed, "holdout", gpu), gpu))
    return sorted(ordered[:count])


def choose_partition(
    seed: int, gpu_id: int, variant_index: int, holdouts: set[int]
) -> str:
    if gpu_id in holdouts:
        return "unseen_link_test"
    cycle = ("train", "train", "train", "validation", "seen_link_test")
    offset = stable_int(seed, "split-offset", gpu_id, modulo=len(cycle))
    return cycle[(variant_index + offset) % len(cycle)]


def gray_variants(contract: Mapping[str, Any]) -> List[Dict[str, Any]]:
    gray = contract["fault_taxonomy"]["gray"]["families"]
    variants: List[Dict[str, Any]] = []
    bandwidth = gray["bandwidth_degradation"]
    for fraction in bandwidth["remaining_nominal_capacity_fractions"]:
        variants.append(
            {
                "family": "bandwidth_degradation",
                "category": "capacity",
                "variant": f"step-rem-{int(round(float(fraction) * 100)):03d}",
                "shape": "step",
                "severity": {
                    "name": "remaining_nominal_capacity_fraction",
                    "value": float(fraction),
                    "unit": "fraction",
                },
                "duration_ns": DEFAULT_EVENT_DURATION_NS,
                "effects": ["throughput_degradation"],
                "implementation": "EXECUTABLE_CURRENT_INJECTOR",
            }
        )
        for ramp_ns in bandwidth["ramp_durations_ns"]:
            variants.append(
                {
                    "family": "bandwidth_degradation",
                    "category": "capacity",
                    "variant": (
                        f"ramp-rem-{int(round(float(fraction) * 100)):03d}-"
                        f"{int(ramp_ns) // 1_000_000:03d}ms"
                    ),
                    "shape": "ramp",
                    "severity": {
                        "name": "remaining_nominal_capacity_fraction",
                        "value": float(fraction),
                        "unit": "fraction",
                    },
                    "duration_ns": int(ramp_ns) + 100_000_000,
                    "ramp_duration_ns": int(ramp_ns),
                    "effects": ["throughput_degradation"],
                    "implementation": "EXECUTABLE_CURRENT_INJECTOR",
                }
            )

    for probability in gray["random_loss"]["packet_error_probabilities"]:
        variants.append(
            {
                "family": "random_loss",
                "category": "loss",
                "variant": f"prob-{str(probability).replace('.', 'p')}",
                "shape": "continuous_random",
                "severity": {
                    "name": "packet_error_probability",
                    "value": float(probability),
                    "unit": "probability",
                },
                "duration_ns": DEFAULT_EVENT_DURATION_NS,
                "effects": ["packet_error"],
                # The P2 injector disables delayed recovery for this mode, so
                # corrupted packets are dropped by the receive path.  It is
                # executable, but remains PLANNED until a completed run proves
                # drop counters, carrier-up semantics, and stability.
                "implementation": "EXECUTABLE_TRUE_DROP_INJECTOR",
            }
        )

    for shape in gray["burst_loss"]["shapes"]:
        variants.append(
            {
                "family": "burst_loss",
                "category": "loss",
                "variant": f"{shape}-prob-0p05",
                "shape": str(shape),
                "severity": {
                    "name": "packet_error_probability",
                    "value": 0.05,
                    "unit": "probability",
                },
                "duration_ns": DEFAULT_EVENT_DURATION_NS,
                "effects": ["finite_packet_error_bursts"],
                "implementation": "EXECUTABLE_TRUE_DROP_INJECTOR",
            }
        )

    required_effects = list(gray["service_degradation"]["required_effects"])
    for fraction in (0.8, 0.5, 0.2):
        variants.append(
            {
                "family": "service_degradation",
                "category": "service",
                "variant": f"service-rem-{int(fraction * 100):03d}",
                "shape": "step",
                "severity": {
                    "name": "remaining_service_fraction",
                    "value": fraction,
                    "unit": "fraction",
                },
                "duration_ns": DEFAULT_EVENT_DURATION_NS,
                "effects": required_effects,
                "implementation": "EXECUTABLE_SERVICE_FRACTION_INJECTOR",
            }
        )

    for impairment in gray["intermittent_service"]["allowed_impairments"]:
        severity = (
            {
                "name": "packet_error_probability",
                "value": 0.01,
                "unit": "probability",
            }
            if impairment == "loss"
            else {
                "name": "remaining_service_fraction"
                if impairment == "service"
                else "remaining_nominal_capacity_fraction",
                "value": 0.5,
                "unit": "fraction",
            }
        )
        variants.append(
            {
                "family": "intermittent_service",
                "category": "intermittent",
                "variant": f"intermittent-{impairment}",
                "shape": "periodic_on_off",
                "severity": severity,
                "duration_ns": 250_000_000,
                "effects": [f"intermittent_{impairment}_impairment"],
                "impairment": str(impairment),
                "implementation": (
                    "EXECUTABLE_SERVICE_FRACTION_INJECTOR"
                    if impairment == "service"
                    else "EXECUTABLE_TRUE_DROP_INJECTOR"
                    if impairment == "loss"
                    else "EXECUTABLE_CURRENT_INJECTOR"
                ),
            }
        )
    return variants


def hard_variants(contract: Mapping[str, Any]) -> List[Dict[str, Any]]:
    families = contract["fault_taxonomy"]["hard"]["families"]
    if set(families) != {"hard_disconnect", "carrier_flap"}:
        raise CorpusError(f"unexpected hard families: {sorted(families)}")
    return [
        {
            "family": "hard_disconnect",
            "variant": "permanent",
            "shape": "permanent",
            "severity": {
                "name": "unavailable_fraction",
                "value": 1.0,
                "unit": "fraction",
            },
            "duration_ns": 0,
            "permanent": True,
            "effects": ["carrier_down"],
            "implementation": "EXECUTABLE_CURRENT_INJECTOR",
        },
        {
            "family": "carrier_flap",
            "variant": "down-up-100us",
            "shape": "down_up",
            "severity": {
                "name": "down_duration_ns",
                "value": 100_000,
                "unit": "ns",
            },
            "duration_ns": 100_000,
            "permanent": False,
            "effects": ["carrier_down", "carrier_up"],
            "implementation": "EXECUTABLE_TRUE_CARRIER_FLAP_INJECTOR",
        },
    ]


def common_fault_row(
    *,
    run_id: str,
    event_id: str,
    family: str,
    gpu_id: Optional[int],
    link_id: str,
    start_ns: int,
    end_ns: int,
    severity: Mapping[str, Any],
    shape: str,
    effects: Sequence[str],
    impairment: str,
    implementation: str,
    segment_index: int,
    fault_type: str,
    before: str,
    after: str,
    recovery_delay_ns: Any = "",
    ramp_duration_ns: Any = "",
) -> Dict[str, Any]:
    return {
        "fault_id": f"{event_id}-segment-{segment_index:02d}",
        "fault_type": fault_type,
        "target_link_id": link_id,
        "start_time_ns": int(start_ns),
        "end_time_ns": int(end_ns),
        "severity": severity["value"],
        "parameter_before": before,
        "parameter_after": after,
        "recovery_delay_ns": recovery_delay_ns,
        "event_id": f"{event_id}-segment-{segment_index:02d}",
        "parent_event_id": event_id,
        "fault_family": family,
        "target_gpu": "" if gpu_id is None else gpu_id,
        "severity_name": severity["name"],
        "severity_value": severity["value"],
        "severity_unit": severity["unit"],
        "shape": shape,
        "ramp_duration_ns": ramp_duration_ns,
        "effects": json.dumps(list(effects), separators=(",", ":")),
        "impairment": impairment,
        "carrier_state": "down"
        if family in {"hard_disconnect", "carrier_flap"}
        else "up",
        "implementation_status": implementation,
        "segment_index": segment_index,
        "permanent": family == "hard_disconnect",
    }


def fault_schedule_rows(
    *,
    seed: int,
    run_id: str,
    event_id: str,
    variant: Mapping[str, Any],
    gpu_id: Optional[int],
    link_id: str,
    nominal_bps: int,
    start_ns: int,
) -> List[Dict[str, Any]]:
    family = str(variant["family"])
    duration_ns = int(variant["duration_ns"])
    # A permanent hard fault uses the run's evaluation horizon as its truth
    # interval end.  The current injector never schedules a hard-fault revert,
    # even when end > start; ``permanent=true`` preserves that distinction.
    end_ns = (
        start_ns + DETECTION_POST_EVENT_NS
        if variant.get("permanent")
        else start_ns + duration_ns
    )
    severity = variant["severity"]
    shape = str(variant["shape"])
    effects = variant.get("effects", [])
    impairment = str(variant.get("impairment", ""))
    implementation = str(variant["implementation"])
    nominal_gbps = nominal_bps // 1_000_000_000

    def row(
        segment_index: int,
        segment_start: int,
        segment_end: int,
        fault_type: str,
        before: str,
        after: str,
        recovery_delay_ns: Any = "",
        ramp_duration_ns: Any = "",
    ) -> Dict[str, Any]:
        return common_fault_row(
            run_id=run_id,
            event_id=event_id,
            family=family,
            gpu_id=gpu_id,
            link_id=link_id,
            start_ns=segment_start,
            end_ns=segment_end,
            severity=severity,
            shape=shape,
            effects=effects,
            impairment=impairment,
            implementation=implementation,
            segment_index=segment_index,
            fault_type=fault_type,
            before=before,
            after=after,
            recovery_delay_ns=recovery_delay_ns,
            ramp_duration_ns=ramp_duration_ns,
        )

    if family == "hard_disconnect":
        return [
            row(
                0,
                start_ns,
                end_ns,
                "hard_disconnect",
                "up",
                "physically_disconnected",
            )
        ]
    if family == "carrier_flap":
        return [row(0, start_ns, end_ns, "carrier_flap", "up", "down")]
    if family == "bandwidth_degradation":
        fraction = float(severity["value"])
        degraded = max(1, round(nominal_gbps * fraction))
        if shape == "step":
            return [
                row(
                    0,
                    start_ns,
                    end_ns,
                    "bandwidth_degradation",
                    f"{nominal_gbps}Gbps",
                    f"{degraded}Gbps",
                )
            ]
        ramp_ns = int(variant["ramp_duration_ns"])
        step_count = max(1, ramp_ns // 10_000_000)
        rows = []
        for index in range(step_count):
            segment_start = start_ns + index * ramp_ns // step_count
            segment_end = start_ns + (index + 1) * ramp_ns // step_count
            level = max(
                1,
                round(
                    nominal_gbps - (nominal_gbps - degraded) * (index + 1) / step_count
                ),
            )
            rows.append(
                row(
                    index,
                    segment_start,
                    segment_end,
                    "bandwidth_degradation",
                    f"{nominal_gbps}Gbps",
                    f"{level}Gbps",
                    ramp_duration_ns=ramp_ns,
                )
            )
        rows.append(
            row(
                len(rows),
                start_ns + ramp_ns,
                end_ns,
                "bandwidth_degradation",
                f"{nominal_gbps}Gbps",
                f"{degraded}Gbps",
                ramp_duration_ns=ramp_ns,
            )
        )
        return rows
    if family == "random_loss":
        return [
            row(
                0,
                start_ns,
                end_ns,
                "random_loss",
                "0.0",
                str(severity["value"]),
            )
        ]
    if family == "burst_loss":
        rows = []
        burst_count = 8
        slot_ns = duration_ns // burst_count
        for index in range(burst_count):
            if shape == "periodic":
                offset_ns = index * slot_ns + 5_000_000
            else:
                jitter_ns = stable_int(
                    seed,
                    run_id,
                    "burst",
                    index,
                    modulo=max(1, slot_ns - 2_000_000),
                )
                offset_ns = index * slot_ns + jitter_ns
            segment_start = start_ns + offset_ns
            segment_end = min(start_ns + duration_ns, segment_start + 1_000_000)
            rows.append(
                row(
                    index,
                    segment_start,
                    segment_end,
                    "burst_loss",
                    "0.0",
                    str(severity["value"]),
                )
            )
        return rows
    if family == "service_degradation":
        return [
            row(
                0,
                start_ns,
                end_ns,
                "service_degradation",
                "1.0",
                str(severity["value"]),
            )
        ]
    if family == "intermittent_service":
        rows = []
        pulse_ns = 20_000_000
        period_ns = 50_000_000
        for index in range(5):
            segment_start = start_ns + index * period_ns
            segment_end = min(end_ns, segment_start + pulse_ns)
            if impairment == "capacity":
                fault_type = "bandwidth_degradation"
                before, after = (
                    f"{nominal_gbps}Gbps",
                    f"{max(1, nominal_gbps // 2)}Gbps",
                )
                recovery = ""
            elif impairment == "loss":
                fault_type = "burst_loss"
                before, after, recovery = "0.0", str(severity["value"]), ""
            else:
                fault_type = "service_degradation"
                before, after, recovery = "1.0", str(severity["value"]), ""
            rows.append(
                row(
                    index,
                    segment_start,
                    segment_end,
                    fault_type,
                    before,
                    after,
                    recovery_delay_ns=recovery,
                )
            )
        return rows
    raise CorpusError(f"no schedule encoder for family={family!r}")


def fault_truth_row(
    *,
    event_id: str,
    variant: Mapping[str, Any],
    gpu_id: Optional[int],
    link_id: Optional[str],
    start_ns: int,
    targets: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return one canonical event row, independent from injector segmentation."""
    duration_ns = int(variant["duration_ns"])
    permanent = bool(variant.get("permanent", False))
    end_ns = start_ns + (DETECTION_POST_EVENT_NS if permanent else duration_ns)
    family = str(variant["family"])
    severity = variant["severity"]
    if family == "hard_disconnect":
        carrier_state = "down"
    elif family == "carrier_flap":
        carrier_state = "down_up"
    else:
        # Every gray-fault truth row keeps carrier up.  A telemetry-only
        # forced-link-state proxy must never silently qualify as gray truth.
        carrier_state = "up"
    return {
        "event_id": event_id,
        "fault_family": family,
        "target_gpu": "" if gpu_id is None else gpu_id,
        "target_link_id": "" if link_id is None else link_id,
        "start_time_ns": int(start_ns),
        "end_time_ns": int(end_ns),
        "severity_name": severity["name"],
        "severity_value": severity["value"],
        "severity_unit": severity["unit"],
        "shape": variant["shape"],
        "ramp_duration_ns": variant.get("ramp_duration_ns", ""),
        "effects": json.dumps(list(variant.get("effects", [])), separators=(",", ":")),
        "impairment": variant.get("impairment", ""),
        "carrier_state": carrier_state,
        "implementation_status": variant["implementation"],
        "permanent": permanent,
        "targets_json": json.dumps(
            list(targets or []), sort_keys=True, separators=(",", ":")
        ),
    }


def feature_schema(contract: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": FEATURE_SCHEMA,
        "contract_id": contract["contract_id"],
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "identifier_columns": list(IDENTIFIER_COLUMNS),
        "label_columns": list(LABEL_COLUMNS),
        "causal_windows_ns": list(contract["features"]["causal_windows_ns"]),
        "utilization_denominator": contract["features"]["utilization_denominator"],
        "forbidden_feature_fields": list(contract["features"]["forbidden_fields"]),
        "schedule_sidecars_are_inference_inputs": False,
        "split_membership_is_model_feature": False,
        "all_windows_end_at_current_sample": True,
        "paired_plane_features_require_role_normalization": True,
    }


def base_run(
    *,
    seed: int,
    run_id: str,
    run_role: str,
    class_label: str,
    scenario: str,
    partition: str,
    schedule: Mapping[str, Any],
    topology_sha256: str,
    workload_sha256: str,
    effective_workload_sha256: Optional[str] = None,
    virtual_finish_ns: int,
    target_gpu: Optional[int] = None,
    target_link_id: Optional[str] = None,
    paired_link_id: Optional[str] = None,
    fault_family: Optional[str] = None,
    gray_category: Optional[str] = None,
    severity: Optional[Mapping[str, Any]] = None,
    duration_ns: int = 0,
    event_start_ns: Optional[int] = None,
    implementation: str = "PENDING_EXECUTION",
    mechanism_id: str = "unassigned",
    mechanism_status: Optional[str] = None,
    targets: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    sim_seed = simulation_seed(seed, run_id)
    effective_sha256 = effective_workload_sha256 or workload_sha256
    group_material = {
        "simulation_seed_or_schedule_id": sim_seed,
        "fault_instance": schedule["event_id"],
        "target_physical_link": target_link_id,
        "targets": targets or [],
        "workload": effective_sha256,
        "topology": topology_sha256,
    }
    blocked = implementation.startswith("BLOCKED")
    if mechanism_status is None:
        mechanism_status = (
            "UNAVAILABLE"
            if blocked or implementation.startswith("PENDING")
            else "PLANNED"
        )
    return {
        "run_id": run_id,
        "run_role": run_role,
        "class_label": class_label,
        "scenario": scenario,
        "fault_family": fault_family,
        "gray_category": gray_category,
        "target_gpu": target_gpu,
        "target_link_id": target_link_id,
        "paired_link_id": paired_link_id,
        "targets": targets or [],
        "severity": dict(severity or {"name": "none", "value": 0, "unit": "none"}),
        "duration_ns": int(duration_ns),
        "simulation_seed": sim_seed,
        "partition": partition,
        "split_group_id": f"grp-{canonical_hash(group_material)[:20]}",
        "schedule": dict(schedule),
        "topology_sha256": topology_sha256,
        "workload_sha256": workload_sha256,
        "effective_workload_sha256": effective_sha256,
        "virtual_start_ns": 0,
        "virtual_finish_ns": int(virtual_finish_ns),
        "fault_scheduled_onset_ns": event_start_ns,
        "recovery_evaluation": False,
        "fault_applied_ns": None,
        "first_observable_effect_ns": None,
        "observable": None,
        "simulator_stability": {
            # Every newly generated run must prove simulator stability from
            # sealed execution evidence before it can become COMPLETE.  Runs
            # whose executor is unavailable retain their existing blocked
            # lifecycle instead of manufacturing a PASS exemption.
            "gate_required": True,
            "status": "BLOCKED_UNSUPPORTED" if blocked else "PENDING_EXECUTION",
            "reason": (
                "current FaultInjector has no faithful implementation"
                if blocked
                else "must be established from completed simulator artifacts"
            ),
        },
        "mechanism": {
            "mechanism_id": mechanism_id,
            "implementation_status": mechanism_status,
            "semantic_validation": None,
            "semantic_validation_reason": (
                "PREPARED corpus has no executed per-run semantic evidence"
            ),
        },
        "artifacts": {
            "run_manifest": None,
            "raw_telemetry": None,
            "derived_features": None,
            "missing_reason": "corpus status is PREPARED; simulation has not run",
        },
    }


def relative_schedule(
    path: Path,
    out_dir: Path,
    sha256: str,
    event_id: str,
    kind: str,
    truth_source: str,
    implementation: str,
    *,
    injector_path: Optional[Path] = None,
    injector_sha256: Optional[str] = None,
    background_path: Optional[Path] = None,
    background_sha256: Optional[str] = None,
    collective_override_path: Optional[Path] = None,
    collective_override_sha256: Optional[str] = None,
    collective_role_path: Optional[Path] = None,
    collective_role_sha256: Optional[str] = None,
    collective_override_report: Optional[Mapping[str, Any]] = None,
    ecmp_collision_path: Optional[Path] = None,
    ecmp_collision_sha256: Optional[str] = None,
    ecmp_collision_contract: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    schedule = {
        "kind": kind,
        "path": path.relative_to(out_dir).as_posix(),
        "sha256": sha256,
        "event_id": event_id,
        "truth_source": truth_source,
        "independent_of_features": True,
        "generated_before_run": True,
        "implementation_status": implementation,
    }
    if injector_path is not None:
        if injector_sha256 is None:
            raise CorpusError("injector schedule is missing its sha256")
        schedule["simulator_injection_schedule"] = {
            "kind": "simulator_injection",
            "format": "limer-current-fault-injector-v1",
            "path": injector_path.relative_to(out_dir).as_posix(),
            "sha256": injector_sha256,
            "generated_before_run": True,
            "safe_to_execute": not implementation.startswith("BLOCKED"),
        }
    if background_path is not None:
        if background_sha256 is None:
            raise CorpusError("background-flow schedule is missing its sha256")
        if injector_path is not None:
            raise CorpusError(
                "fault injection and background-flow schedules are mutually exclusive"
            )
        schedule["background_flow_schedule"] = {
            "kind": "background_rdma",
            "format": "limer-background-rdma-v1",
            "path": background_path.relative_to(out_dir).as_posix(),
            "sha256": background_sha256,
            "generated_before_run": True,
            "safe_to_execute": implementation == "EXECUTABLE_BACKGROUND_RDMA",
        }
    if collective_override_path is not None:
        if (
            collective_override_sha256 is None
            or collective_role_path is None
            or collective_role_sha256 is None
            or collective_override_report is None
        ):
            raise CorpusError(
                "collective workload override lacks workload/role hash/static report"
            )
        if injector_path is not None or background_path is not None:
            raise CorpusError(
                "collective workload override is mutually exclusive with other executors"
            )
        schedule["collective_workload_override"] = {
            "kind": "simai_collective_workload_override",
            "format": COLLECTIVE_OVERRIDE_FORMAT,
            "path": collective_override_path.relative_to(out_dir).as_posix(),
            "sha256": collective_override_sha256,
            "generated_before_run": True,
            "safe_to_execute": True,
            "runtime_executor_status": "READY",
            "layer_role_sidecar": {
                "kind": "collective_layer_roles",
                "format": COLLECTIVE_ROLE_FORMAT,
                "path": collective_role_path.relative_to(out_dir).as_posix(),
                "sha256": collective_role_sha256,
                "generated_before_run": True,
            },
            "static_validation": dict(collective_override_report),
            "static_validation_sha256": canonical_hash(collective_override_report),
        }
    if ecmp_collision_path is not None:
        if ecmp_collision_sha256 is None or ecmp_collision_contract is None:
            raise CorpusError("ECMP collision schedule lacks hash/static contract")
        if (
            injector_path is not None
            or background_path is not None
            or collective_override_path is not None
        ):
            raise CorpusError(
                "ECMP collision schedule is mutually exclusive with other executors"
            )
        schedule["ecmp_collision_schedule"] = {
            "kind": "native_ecmp_collision",
            "format": ECMP_COLLISION_FORMAT,
            "path": ecmp_collision_path.relative_to(out_dir).as_posix(),
            "sha256": ecmp_collision_sha256,
            "generated_before_run": True,
            "safe_to_execute": False,
            "runtime_route_evidence_status": "PENDING",
            "contract": dict(ecmp_collision_contract),
            "contract_sha256": canonical_hash(ecmp_collision_contract),
        }
    return schedule


def make_fault_runs(
    *,
    seed: int,
    out_dir: Path,
    contract: Mapping[str, Any],
    pairs: Mapping[int, Mapping[str, Mapping[str, Any]]],
    holdouts: set[int],
    topology_sha256: str,
    workload_sha256: str,
) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    variants: List[Tuple[str, Dict[str, Any]]] = [
        ("HARD_FAULT", variant) for variant in hard_variants(contract)
    ] + [("GRAY_FAULT", variant) for variant in gray_variants(contract)]
    for gpu_id in sorted(pairs):
        active = pairs[gpu_id]["B"]
        standby = pairs[gpu_id]["A"]
        for variant_index, (class_label, variant) in enumerate(variants):
            family = str(variant["family"])
            variant_name = str(variant["variant"])
            run_id = f"p2-g{gpu_id:02d}-{safe_slug(family)}-{safe_slug(variant_name)}"
            event_id = f"evt-{run_id}"
            onset_ns = (
                BASE_ONSET_NS
                + stable_int(seed, run_id, "onset-ms", modulo=21) * 1_000_000
            )
            injector_rows = fault_schedule_rows(
                seed=seed,
                run_id=run_id,
                event_id=event_id,
                variant=variant,
                gpu_id=gpu_id,
                link_id=str(active["link_id"]),
                nominal_bps=int(active["bandwidth_bps"]),
                start_ns=onset_ns,
            )
            injector_path = out_dir / "simulator_injection_schedules" / f"{run_id}.csv"
            injector_sha = write_csv(injector_path, INJECTOR_CSV_COLUMNS, injector_rows)
            schedule_path = out_dir / "fault_schedules" / f"{run_id}.csv"
            schedule_sha = write_csv(
                schedule_path,
                FAULT_TRUTH_CSV_COLUMNS,
                [
                    fault_truth_row(
                        event_id=event_id,
                        variant=variant,
                        gpu_id=gpu_id,
                        link_id=str(active["link_id"]),
                        start_ns=onset_ns,
                    )
                ],
            )
            schedule = relative_schedule(
                schedule_path,
                out_dir,
                schedule_sha,
                event_id,
                "fault",
                "predeclared_fault_schedule",
                str(variant["implementation"]),
                injector_path=injector_path,
                injector_sha256=injector_sha,
            )
            partition = choose_partition(seed, gpu_id, variant_index, holdouts)
            event_end = onset_ns + int(variant["duration_ns"])
            finish_ns = max(onset_ns, event_end) + DETECTION_POST_EVENT_NS
            impairment = str(variant.get("impairment", ""))
            if family == "hard_disconnect":
                mechanism_id, mechanism_status = "physical_link_down", "PLANNED"
            elif family == "carrier_flap":
                mechanism_id, mechanism_status = (
                    "physical_carrier_flap_channel_epoch",
                    "PLANNED",
                )
            elif family == "bandwidth_degradation":
                mechanism_id, mechanism_status = "dual_endpoint_data_rate", "PLANNED"
            elif family in {"random_loss", "burst_loss"} or (
                family == "intermittent_service" and impairment == "loss"
            ):
                mechanism_id, mechanism_status = "rate_error_model_true_drop", "PLANNED"
            elif family == "intermittent_service" and impairment == "capacity":
                mechanism_id, mechanism_status = (
                    "dual_endpoint_data_rate_pulses",
                    "PLANNED",
                )
            elif family == "service_degradation" or (
                family == "intermittent_service" and impairment == "service"
            ):
                mechanism_id, mechanism_status = "egress_service_fraction", "PLANNED"
            else:
                mechanism_id, mechanism_status = (
                    f"{family}_unimplemented",
                    "UNAVAILABLE",
                )
            runs.append(
                base_run(
                    seed=seed,
                    run_id=run_id,
                    run_role="fault",
                    class_label=class_label,
                    scenario=variant_name,
                    partition=partition,
                    schedule=schedule,
                    topology_sha256=topology_sha256,
                    workload_sha256=workload_sha256,
                    virtual_finish_ns=finish_ns,
                    target_gpu=gpu_id,
                    target_link_id=str(active["link_id"]),
                    paired_link_id=str(standby["link_id"]),
                    fault_family=family,
                    gray_category=variant.get("category"),
                    severity=variant["severity"],
                    duration_ns=int(variant["duration_ns"]),
                    event_start_ns=onset_ns,
                    implementation=str(variant["implementation"]),
                    mechanism_id=mechanism_id,
                    mechanism_status=mechanism_status,
                )
            )
    return runs


def congestion_action(scenario: str) -> Tuple[str, str, Dict[str, Any]]:
    workload_actions = {
        "high_utilization": (
            "workload",
            "sustained_collective_load",
            {"profile": "high_utilization"},
        ),
        "allreduce_burst": (
            "workload",
            "synchronized_allreduce_burst",
            {"burst_count": 8},
        ),
        "incast": ("workload", "scheduled_incast", {"fan_in": 12}),
        "queue_buildup": (
            "workload",
            "scheduled_background_rdma_pressure",
            {"pressure_profile": "two_wave_queue", "fan_in": 12},
        ),
    }
    network_actions = {
        "ecmp_or_hash_contention": (
            "network",
            "fixed_hash_contention",
            {"flow_group_count": 8},
        ),
        "ecn": (
            "network",
            "scheduled_ecn_pressure",
            {"marking_profile": "contract_default"},
        ),
        "pfc": (
            "network",
            "scheduled_pfc_pressure",
            {"pause_profile": "contract_default"},
        ),
    }
    if scenario in workload_actions:
        return workload_actions[scenario]
    if scenario in network_actions:
        return network_actions[scenario]
    raise CorpusError(f"no independent congestion action for {scenario!r}")


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def collective_override_contract(
    scenario: str,
    onset_ns: int,
    *,
    source_workload_sha256: str = "",
    aggregate_access_bandwidth_bps: int = 200_000_000_000,
) -> Dict[str, Any]:
    """Freeze one executable A1 profile without consulting run telemetry.

    ``planned_issue_ns`` in the role file is a compute-only plan.  It is never
    treated as the application's actual issue timestamp.  Actual application
    onset/end are reconstructed later from role-bound collective transactions.
    """

    if scenario not in EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS:
        raise CorpusError(f"no collective workload override for {scenario!r}")
    if onset_ns <= COLLECTIVE_PRE_LAYER_COUNT * traffic_workload.DEFAULT_COMPUTE_NS:
        raise CorpusError("collective override onset leaves no positive boundary compute")
    if aggregate_access_bandwidth_bps <= 0:
        raise CorpusError("aggregate ACCESS bandwidth must be positive")
    common: Dict[str, Any] = {
        "scenario": scenario,
        "world_size": traffic_workload.WORLD_SIZE,
        "collective_type": "ALLREDUCE",
        "source_workload_sha256": source_workload_sha256,
        "source_layer_count": COLLECTIVE_FULL_LAYER_COUNT,
        "baseline_compute_ns": traffic_workload.DEFAULT_COMPUTE_NS,
        "baseline_collective_bytes": traffic_workload.DEFAULT_COLLECTIVE_BYTES,
        "pre_layer_count": COLLECTIVE_PRE_LAYER_COUNT,
        "scheduled_onset_ns": int(onset_ns),
        "scheduled_end_ns": int(onset_ns) + COLLECTIVE_EVENT_WINDOW_NS,
        "runtime_horizon_ns": COLLECTIVE_RUNTIME_HORIZON_NS,
        "minimum_actual_pre_start_span_ns": COLLECTIVE_PRE_START_SPAN_NS,
        "maximum_pre_tail_to_actual_onset_ns": COLLECTIVE_PRE_TO_EVENT_MAX_GAP_NS,
        "maximum_post_tail_gap_ns": COLLECTIVE_POST_TAIL_MAX_GAP_NS,
        "static_clock_semantics": "compute_only_not_actual_issue_time",
        "actual_application_window_authority": (
            "role_bound_collective_transaction"
        ),
    }
    if scenario == "allreduce_burst":
        return {
            **common,
            "qualification_profile": COLLECTIVE_OVERRIDE_PROFILE_BURST,
            "burst_layer_count": COLLECTIVE_BURST_COUNT,
            "burst_collective_bytes": traffic_workload.DEFAULT_COLLECTIVE_BYTES,
            "burst_inter_layer_compute_ns": COLLECTIVE_BURST_GAP_NS,
            "maximum_actual_burst_start_gap_ns": (
                COLLECTIVE_BURST_MAX_START_GAP_NS
            ),
            "maximum_actual_burst_span_ns": COLLECTIVE_BURST_MAX_SPAN_NS,
        }

    payload = COLLECTIVE_PRESSURE_BYTES
    ring_bytes_per_rank = _ceil_div(
        2 * (traffic_workload.WORLD_SIZE - 1) * payload,
        traffic_workload.WORLD_SIZE,
    )
    optimistic_round_wire_ns = _ceil_div(
        ring_bytes_per_rank * 8 * 1_000_000_000,
        aggregate_access_bandwidth_bps,
    )
    minimum_rounds = _ceil_div(
        COLLECTIVE_EVENT_WINDOW_NS, optimistic_round_wire_ns
    )
    # One fully specified extra round prevents integer-ceiling equality from
    # ending pressure just before the fixed 200 ms validation boundary.
    pressure_count = minimum_rounds + 1
    return {
        **common,
        "qualification_profile": COLLECTIVE_OVERRIDE_PROFILE_HIGH_UTIL,
        "pressure_collective_bytes": payload,
        "pressure_inter_layer_compute_ns": 1,
        "aggregate_access_bandwidth_bps": aggregate_access_bandwidth_bps,
        "ring_bytes_per_rank": ring_bytes_per_rank,
        "optimistic_round_wire_ns": optimistic_round_wire_ns,
        "minimum_formula_pressure_layer_count": minimum_rounds,
        "pressure_safety_layers": 1,
        "pressure_layer_count": pressure_count,
        "fixed_actual_pressure_window_ns": COLLECTIVE_EVENT_WINDOW_NS,
        "maximum_pressure_idle_gap_ns": 2_000_000,
        "utilization_metric": "access_link_observed_throughput_bps",
        "utilization_threshold": COLLECTIVE_UTILIZATION_THRESHOLD,
        "minimum_window_coverage_ratio": COLLECTIVE_UTILIZATION_COVERAGE,
        "required_access_endpoint_count": 32,
    }


def _source_workload_rows(path: Path) -> Tuple[str, List[List[str]], str]:
    try:
        report = traffic_workload.validate_workload(
            path, profile=traffic_workload.HORIZON_PREFIX_PROFILE
        )
    except traffic_workload.WorkloadValidationError as exc:
        raise CorpusError(f"collective override source workload is invalid: {exc}") from exc
    raw = path.read_bytes()
    lines = raw.decode("utf-8").splitlines()
    rows = [line.split() for line in lines[2:]]
    if (
        report.get("layer_count") != COLLECTIVE_FULL_LAYER_COUNT
        or report.get("compute_ns_per_layer") != traffic_workload.DEFAULT_COMPUTE_NS
        or report.get("collective_bytes_per_layer")
        != traffic_workload.DEFAULT_COLLECTIVE_BYTES
    ):
        raise CorpusError("collective override source is not the frozen 550 ms baseline")
    return lines[0], rows, hashlib.sha256(raw).hexdigest()


def build_collective_override(
    source_workload_path: Path,
    contract: Mapping[str, Any],
    run_id: str,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Derive a complete 550-layer workload and independent role mapping."""

    header, source_rows, source_sha = _source_workload_rows(source_workload_path)
    if int(contract.get("world_size", -1)) != traffic_workload.WORLD_SIZE:
        raise CorpusError("collective override world_size must be 16")
    if contract.get("collective_type") != "ALLREDUCE":
        raise CorpusError("collective override collective_type must be ALLREDUCE")
    if int(contract.get("source_layer_count", -1)) != COLLECTIVE_FULL_LAYER_COUNT:
        raise CorpusError("collective override source layer count must be 550")
    if contract.get("source_workload_sha256") != source_sha:
        raise CorpusError("collective override contract source workload hash differs")
    if len(source_rows) != int(contract.get("source_layer_count", -1)):
        raise CorpusError("collective override source layer count differs")
    scenario = str(contract.get("scenario", ""))
    onset = int(contract["scheduled_onset_ns"])
    end = int(contract["scheduled_end_ns"])
    pre_count = int(contract["pre_layer_count"])
    baseline_compute = int(contract["baseline_compute_ns"])
    boundary_compute = onset - pre_count * baseline_compute
    if boundary_compute <= 0:
        raise CorpusError("collective override event boundary compute is not positive")

    rows = [list(fields) for fields in source_rows]
    roles: List[Tuple[str, str]] = [("PRE", "PRE_BASELINE")] * pre_count
    if scenario == "allreduce_burst":
        burst_count = int(contract["burst_layer_count"])
        rows[pre_count][2] = str(boundary_compute)
        for index in range(pre_count, pre_count + burst_count):
            if index > pre_count:
                rows[index][2] = str(contract["burst_inter_layer_compute_ns"])
            rows[index][4] = str(contract["burst_collective_bytes"])
            roles.append(("EVENT", "EVENT_BURST"))
        restore_index = pre_count + burst_count
        consumed = (burst_count - 1) * int(contract["burst_inter_layer_compute_ns"])
        restore_compute = burst_count * baseline_compute - consumed
        rows[restore_index][2] = str(restore_compute)
        planned = sum(int(row[2]) for row in rows[:restore_index])
        for index in range(restore_index, len(rows)):
            planned += int(rows[index][2])
            roles.append(
                ("EVENT", "EVENT_BASELINE")
                if planned < end
                else ("POST", "POST_BASELINE")
            )
    elif scenario == "high_utilization":
        pressure_count = int(contract["pressure_layer_count"])
        formula_count = _ceil_div(
            int(contract["fixed_actual_pressure_window_ns"]),
            int(contract["optimistic_round_wire_ns"]),
        ) + int(contract["pressure_safety_layers"])
        if pressure_count != formula_count or pressure_count != 160:
            raise CorpusError(
                "high_utilization pressure layer count differs from frozen formula"
            )
        rows[pre_count][2] = str(boundary_compute)
        for index in range(pre_count, pre_count + pressure_count):
            if index > pre_count:
                rows[index][2] = str(contract["pressure_inter_layer_compute_ns"])
            rows[index][4] = str(contract["pressure_collective_bytes"])
            roles.append(("EVENT", "EVENT_PRESSURE"))
        roles.extend(
            [("POST", "POST_BASELINE")]
            * (len(rows) - pre_count - pressure_count)
        )
    else:
        raise CorpusError(f"unknown collective override scenario {scenario!r}")
    if len(roles) != len(rows):
        raise CorpusError("collective override role count differs from workload")

    planned_issue = 0
    role_rows: List[Dict[str, Any]] = []
    for layer_num, (fields, (phase, role)) in enumerate(zip(rows, roles)):
        planned_issue += int(fields[2])
        role_rows.append({
            "run_id": run_id,
            "scenario": scenario,
            "layer_num": layer_num,
            "layer_id": fields[0],
            "phase": phase,
            "role": role,
            "scheduled_onset_ns": onset,
            "scheduled_end_ns": end,
            "planned_issue_ns": planned_issue,
            "compute_ns": int(fields[2]),
            "collective_type": fields[3],
            "message_size_bytes": int(fields[4]),
        })
    text = "\n".join([header, str(len(rows)), *(" ".join(row) for row in rows)])
    return text + "\n", role_rows


def validate_collective_override(
    path: Path,
    role_path: Path,
    contract: Mapping[str, Any],
    source_workload_path: Path,
    run_id: str,
) -> Dict[str, Any]:
    """Reconstruct and byte-compare both A1 inputs from the frozen baseline."""

    expected_text, expected_roles = build_collective_override(
        source_workload_path, contract, run_id
    )
    raw = path.read_bytes()
    if raw != expected_text.encode("utf-8"):
        raise CorpusError("collective workload override differs from reconstruction")
    try:
        with role_path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            observed_header = tuple(reader.fieldnames or ())
            observed_roles = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise CorpusError(f"collective role sidecar is unreadable: {exc}") from exc
    expected_as_text = [
        {column: str(row[column]) for column in COLLECTIVE_ROLE_CSV_COLUMNS}
        for row in expected_roles
    ]
    if observed_header != COLLECTIVE_ROLE_CSV_COLUMNS:
        raise CorpusError("collective role sidecar header is invalid")
    if observed_roles != expected_as_text:
        raise CorpusError("collective role sidecar differs from reconstruction")
    role_raw = role_path.read_bytes()
    role_counts = dict(sorted(Counter(row["role"] for row in expected_roles).items()))
    phase_counts = dict(sorted(Counter(row["phase"] for row in expected_roles).items()))
    scenario = str(contract.get("scenario", ""))
    if scenario == "allreduce_burst" and role_counts.get("EVENT_BURST") != 8:
        raise CorpusError("allreduce_burst must designate exactly 8 burst layers")
    if scenario == "high_utilization" and role_counts.get(
        "EVENT_PRESSURE"
    ) != int(contract["pressure_layer_count"]):
        raise CorpusError("high_utilization formula must designate exactly 160 layers")
    if not role_counts.get("PRE_BASELINE") or not role_counts.get("POST_BASELINE"):
        raise CorpusError("collective override must contain PRE and POST baseline roles")
    return {
        "schema_version": "limer.p2-collective-override-static.v2",
        "status": "PASS",
        "scenario": scenario,
        "qualification_profile": contract["qualification_profile"],
        "file_name": path.name,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "role_file_name": role_path.name,
        "role_sha256": hashlib.sha256(role_raw).hexdigest(),
        "source_workload_sha256": contract["source_workload_sha256"],
        "format": COLLECTIVE_OVERRIDE_FORMAT,
        "role_format": COLLECTIVE_ROLE_FORMAT,
        "world_size": 16,
        "collective_type": "ALLREDUCE",
        "validated_layer_count": len(expected_roles),
        "role_counts": role_counts,
        "phase_counts": phase_counts,
        "layer_binding_sha256": canonical_hash(expected_roles),
        "contract": dict(contract),
    }


def congestion_implementation(scenario: str) -> Tuple[str, str, str]:
    """Return schedule status, mechanism id, and prepared mechanism status."""
    if scenario == "incast":
        return "EXECUTABLE_BACKGROUND_RDMA", "background_rdma_incast", "PLANNED"
    if scenario == "queue_buildup":
        return (
            "EXECUTABLE_BACKGROUND_RDMA",
            "background_rdma_queue_buildup",
            "PLANNED",
        )
    if scenario in EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS:
        return (
            COLLECTIVE_OVERRIDE_STATUS,
            "simai_collective_workload_override",
            "PLANNED",
        )
    blockers = {
        "ecmp_or_hash_contention": ECMP_COLLISION_STATUS,
        "ecn": "BLOCKED_MISSING_EXPLICIT_ECN_PRESSURE_EXECUTOR",
        "pfc": "BLOCKED_MISSING_EXPLICIT_PFC_PRESSURE_EXECUTOR",
    }
    try:
        status = blockers[scenario]
    except KeyError as exc:
        raise CorpusError(
            f"no congestion implementation contract for {scenario!r}"
        ) from exc
    return status, f"unavailable_{safe_slug(scenario)}", "UNAVAILABLE"


def background_flow_rows(
    *,
    event_id: str,
    scenario: str,
    start_ns: int,
    replicate: int,
    destination: int,
    route_bucket: int,
) -> List[Dict[str, Any]]:
    """Create finite real RDMA pressure hash-pinned to one physical rail."""
    if scenario not in EXECUTABLE_BACKGROUND_SCENARIOS:
        raise CorpusError(f"background RDMA does not implement {scenario!r}")
    if route_bucket not in {0, 1}:
        raise CorpusError(f"background route bucket must be 0/1: {route_bucket}")
    sources = [rank for rank in range(16) if rank // 4 != destination // 4]
    waves = 1 if scenario == "incast" else 2
    bytes_per_flow = 8 * 1024 * 1024 if scenario == "incast" else 16 * 1024 * 1024
    dport = 20000 + replicate
    used_sports: set[int] = set()
    rows: List[Dict[str, Any]] = []
    for wave in range(waves):
        wave_start = start_ns + wave * 2_000_000
        for source in sources:
            rows.append(
                {
                    "event_id": event_id,
                    "flow_id": (
                        f"bg-{safe_slug(scenario)}-r{replicate:02d}-"
                        f"w{wave:02d}-s{source:02d}-d{destination:02d}"
                    ),
                    "scenario": scenario,
                    "scheduled_start_ns": wave_start,
                    "src_rank": source,
                    "dst_rank": destination,
                    "bytes": bytes_per_flow,
                    "pg": 3,
                    "sport": select_single_rail_sport(
                        src=source,
                        dst=destination,
                        dport=dport,
                        route_bucket=route_bucket,
                        used=used_sports,
                    ),
                    "dport": dport,
                }
            )
    return rows


def make_negative_runs(
    *,
    seed: int,
    out_dir: Path,
    contract: Mapping[str, Any],
    pairs: Mapping[int, Mapping[str, Mapping[str, Any]]],
    physical_links: Sequence[Mapping[str, Any]],
    topology_sha256: str,
    workload_sha256: str,
    workload_path: Path,
) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    negative_finish_ns = (
        BASE_ONSET_NS + DEFAULT_EVENT_DURATION_NS + DETECTION_POST_EVENT_NS
    )
    healthy_qualification_finish_ns = max(
        negative_finish_ns,
        traffic_workload.CORPUS_MAX_VIRTUAL_FINISH_NS,
    )
    negative_partitions = ("train", "validation", "seen_link_test")

    for index in range(6):
        partition = negative_partitions[index % len(negative_partitions)]
        run_id = f"p2-healthy-{index:02d}"
        event_id = f"evt-{run_id}"
        path = out_dir / "negative_schedules" / f"{run_id}.csv"
        rows = [
            {
                "event_id": event_id,
                "scenario": "healthy",
                "start_time_ns": 0,
                "end_time_ns": healthy_qualification_finish_ns,
                "truth_source": "predeclared_no_fault",
                "action_scope": "none",
                "action_name": "none",
                "action_parameters_json": "{}",
                "implementation_status": "EXECUTABLE_NO_INJECTION",
            }
        ]
        digest = write_csv(path, NEGATIVE_CSV_COLUMNS, rows)
        schedule = relative_schedule(
            path,
            out_dir,
            digest,
            event_id,
            "healthy",
            "predeclared_no_fault",
            "EXECUTABLE_NO_INJECTION",
        )
        runs.append(
            base_run(
                seed=seed,
                run_id=run_id,
                run_role="healthy",
                class_label="HEALTHY",
                scenario="healthy",
                partition=partition,
                schedule=schedule,
                topology_sha256=topology_sha256,
                workload_sha256=workload_sha256,
                virtual_finish_ns=healthy_qualification_finish_ns,
                implementation="EXECUTABLE_NO_INJECTION",
                mechanism_id="no_physical_injection",
                mechanism_status="PLANNED",
            )
        )

    scenarios = list(
        contract["fault_taxonomy"]["negative"]["congestion"]["required_scenarios"]
    )
    for scenario in scenarios:
        scope, action_name, base_parameters = congestion_action(str(scenario))
        implementation, mechanism_id, mechanism_status = congestion_implementation(
            str(scenario)
        )
        truth_source = (
            "predeclared_workload_schedule"
            if scope == "workload"
            else "predeclared_network_schedule"
        )
        for replicate, partition in enumerate(negative_partitions):
            run_id = f"p2-congestion-{safe_slug(scenario)}-{replicate:02d}"
            event_id = f"evt-{run_id}"
            start_ns = BASE_ONSET_NS + replicate * 5_000_000
            parameters = dict(base_parameters)
            background_rows: Optional[List[Dict[str, Any]]] = None
            collective_override_path: Optional[Path] = None
            collective_override_sha: Optional[str] = None
            collective_role_path: Optional[Path] = None
            collective_role_sha: Optional[str] = None
            collective_override_report: Optional[Dict[str, Any]] = None
            ecmp_path: Optional[Path] = None
            ecmp_sha: Optional[str] = None
            ecmp_contract: Optional[Dict[str, Any]] = None
            target_gpu: Optional[int] = None
            target_link_id: Optional[str] = None
            paired_link_id: Optional[str] = None
            completion_deadline_ns: Optional[int] = None
            if str(scenario) in EXECUTABLE_BACKGROUND_SCENARIOS:
                target_gpu = (replicate % 3) * 4
                route_bucket = replicate % 2
                plane_by_bucket = {
                    int(pair["route_bucket"]): plane
                    for plane, pair in pairs[target_gpu].items()
                }
                if set(plane_by_bucket) != {0, 1}:
                    raise CorpusError(
                        f"GPU {target_gpu} has invalid RDMA rail mapping: "
                        f"{plane_by_bucket}"
                    )
                target_plane = plane_by_bucket[route_bucket]
                paired_plane = "B" if target_plane == "A" else "A"
                target = pairs[target_gpu][target_plane]
                paired = pairs[target_gpu][paired_plane]
                target_link_id = str(target["link_id"])
                paired_link_id = str(paired["link_id"])
                background_rows = background_flow_rows(
                    event_id=event_id,
                    scenario=str(scenario),
                    start_ns=start_ns,
                    replicate=replicate,
                    destination=target_gpu,
                    route_bucket=route_bucket,
                )
                # The truth interval describes only the independently
                # scheduled QP launches.  The realized congestion window is
                # later read from the application lifecycle, never guessed.
                end_ns = (
                    max(int(row["scheduled_start_ns"]) for row in background_rows) + 1
                )
                completion_deadline_ns = start_ns + BACKGROUND_COMPLETION_DEADLINE_NS
                route_host_ports = [
                    int(pair["host_port"])
                    for _, pair in sorted(
                        pairs[target_gpu].items(),
                        key=lambda item: int(item[1]["route_bucket"]),
                    )
                ]
                parameters.update(
                    {
                        "destination_rank": target_gpu,
                        "bottleneck_access_link_id": target_link_id,
                        "paired_access_link_id": paired_link_id,
                        "data_plane": target_plane,
                        "route_candidate_order_host_ports": route_host_ports,
                        "route_bucket": route_bucket,
                        "hash_algorithm": "ns3-murmur3-x86-32",
                        "hash_seed_u32": MURMUR3_SEED_U32,
                        "hash_tuple": "native-le-sip-dip-sport-dport",
                        "hash_byte_order": HASH_BYTE_ORDER,
                        "pin_reverse_ack": True,
                        "predeclared_window_policy": "scheduled_qp_launch_window",
                        "realized_window_policy": (
                            "first_data_tx_to_last_ack_complete"
                        ),
                        "completion_deadline_ns": completion_deadline_ns,
                        "rdma_rto_us": BACKGROUND_RDMA_RTO_US,
                        "rdma_retry_limit": BACKGROUND_RDMA_RETRY_LIMIT,
                        "max_rto_retry_events": 0,
                    }
                )
            else:
                end_ns = start_ns + DEFAULT_EVENT_DURATION_NS
                if str(scenario) in EXECUTABLE_COLLECTIVE_OVERRIDE_SCENARIOS:
                    aggregate_bandwidths = {
                        sum(int(link["bandwidth_bps"]) for link in rails.values())
                        for rails in pairs.values()
                    }
                    if len(aggregate_bandwidths) != 1:
                        raise CorpusError(
                            "collective override requires uniform per-rank dual-rail bandwidth"
                        )
                    override_contract = collective_override_contract(
                        str(scenario),
                        start_ns,
                        source_workload_sha256=workload_sha256,
                        aggregate_access_bandwidth_bps=next(
                            iter(aggregate_bandwidths)
                        ),
                    )
                    collective_override_path = (
                        out_dir / "collective_workload_overrides" / f"{run_id}.txt"
                    )
                    collective_role_path = (
                        out_dir / "collective_layer_roles" / f"{run_id}.csv"
                    )
                    collective_override_path.parent.mkdir(parents=True, exist_ok=True)
                    collective_role_path.parent.mkdir(parents=True, exist_ok=True)
                    override_text, role_rows = build_collective_override(
                        workload_path, override_contract, run_id
                    )
                    collective_override_path.write_text(
                        override_text,
                        encoding="utf-8",
                    )
                    collective_role_sha = write_csv(
                        collective_role_path,
                        COLLECTIVE_ROLE_CSV_COLUMNS,
                        role_rows,
                    )
                    collective_override_sha = sha256_file(collective_override_path)
                    collective_override_report = validate_collective_override(
                        collective_override_path,
                        collective_role_path,
                        override_contract,
                        workload_path,
                        run_id,
                    )
                    parameters.update({
                        "qualification_profile": override_contract[
                            "qualification_profile"
                        ],
                        "scheduled_application_onset_ns": start_ns,
                        "scheduled_application_end_ns": end_ns,
                        "actual_application_window_authority": (
                            "role_bound_collective_transaction"
                        ),
                    })
                elif str(scenario) == "ecmp_or_hash_contention":
                    collision_rows, ecmp_contract = ecmp_collision_rows(
                        event_id=event_id,
                        start_ns=start_ns,
                        replicate=replicate,
                        links=physical_links,
                        pairs=pairs,
                    )
                    ecmp_path = out_dir / "ecmp_collision_schedules" / f"{run_id}.csv"
                    ecmp_sha = write_csv(
                        ecmp_path, ECMP_COLLISION_COLUMNS, collision_rows
                    )
            path = out_dir / "congestion_schedules" / f"{run_id}.csv"
            rows = [
                {
                    "event_id": event_id,
                    "scenario": scenario,
                    "start_time_ns": start_ns,
                    "end_time_ns": end_ns,
                    "truth_source": truth_source,
                    "action_scope": scope,
                    "action_name": action_name,
                    "action_parameters_json": json.dumps(
                        parameters, sort_keys=True, separators=(",", ":")
                    ),
                    "implementation_status": implementation,
                }
            ]
            digest = write_csv(path, NEGATIVE_CSV_COLUMNS, rows)
            background_path: Optional[Path] = None
            background_sha: Optional[str] = None
            if background_rows is not None:
                background_path = (
                    out_dir / "background_flow_schedules" / f"{run_id}.csv"
                )
                background_sha = write_csv(
                    background_path,
                    BACKGROUND_FLOW_CSV_COLUMNS,
                    background_rows,
                )
            schedule = relative_schedule(
                path,
                out_dir,
                digest,
                event_id,
                "congestion",
                truth_source,
                implementation,
                background_path=background_path,
                background_sha256=background_sha,
                collective_override_path=collective_override_path,
                collective_override_sha256=collective_override_sha,
                collective_role_path=collective_role_path,
                collective_role_sha256=collective_role_sha,
                collective_override_report=collective_override_report,
                ecmp_collision_path=ecmp_path,
                ecmp_collision_sha256=ecmp_sha,
                ecmp_collision_contract=ecmp_contract,
            )
            runs.append(
                base_run(
                    seed=seed,
                    run_id=run_id,
                    run_role="congestion",
                    class_label="CONGESTION",
                    scenario=str(scenario),
                    partition=partition,
                    schedule=schedule,
                    topology_sha256=topology_sha256,
                    workload_sha256=workload_sha256,
                    effective_workload_sha256=(
                        collective_override_sha or workload_sha256
                    ),
                    virtual_finish_ns=(
                        completion_deadline_ns + DETECTION_POST_EVENT_NS
                        if completion_deadline_ns is not None
                        else COLLECTIVE_RUNTIME_HORIZON_NS
                        if collective_override_path is not None
                        else end_ns + DETECTION_POST_EVENT_NS
                    ),
                    target_gpu=target_gpu,
                    target_link_id=target_link_id,
                    paired_link_id=paired_link_id,
                    duration_ns=end_ns - start_ns,
                    event_start_ns=start_ns,
                    implementation=implementation,
                    mechanism_id=mechanism_id,
                    mechanism_status=mechanism_status,
                )
            )
    return runs


def make_ood_runs(
    *,
    seed: int,
    out_dir: Path,
    pairs: Mapping[int, Mapping[str, Mapping[str, Any]]],
    non_access: Sequence[Mapping[str, Any]],
    topology_sha256: str,
    workload_sha256: str,
) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    active = {gpu: pair["B"] for gpu, pair in pairs.items()}
    ood_specs = [
        {
            "run_id": "p2-ood-unseen-bandwidth-065",
            "scenario": "unseen_fault_parameter",
            "family": "bandwidth_degradation",
            "targets": [(0, active[0])],
            "fraction": 0.65,
        },
        {
            "run_id": "p2-ood-multiple-simultaneous",
            "scenario": "multiple_simultaneous_faults",
            "family": "multiple_simultaneous_faults",
            "targets": [(0, active[0]), (1, active[1])],
            "fraction": 0.5,
        },
    ]
    inter_switch = sorted(
        (row for row in non_access if row["link_class"] == "INTER_SWITCH"),
        key=lambda row: row["link_id"],
    )
    if not inter_switch:
        raise CorpusError("cannot create required non-ACCESS OOD schedule")
    ood_specs.append(
        {
            "run_id": "p2-ood-non-access",
            "scenario": "non_access_fault",
            "family": "non_access_fault",
            "targets": [(None, inter_switch[0])],
            "fraction": 0.5,
        }
    )

    for spec in ood_specs:
        run_id = str(spec["run_id"])
        event_id = f"evt-{run_id}"
        start_ns = BASE_ONSET_NS + stable_int(seed, run_id, modulo=21) * 1_000_000
        end_ns = start_ns + DEFAULT_EVENT_DURATION_NS
        rows = []
        targets = []
        for index, (gpu_id, link) in enumerate(spec["targets"]):
            nominal_gbps = int(link["bandwidth_bps"]) // 1_000_000_000
            fraction = float(spec["fraction"])
            severity = {
                "name": "remaining_nominal_capacity_fraction",
                "value": fraction,
                "unit": "fraction",
            }
            rows.append(
                common_fault_row(
                    run_id=run_id,
                    event_id=event_id,
                    family=str(spec["family"]),
                    gpu_id=gpu_id,
                    link_id=str(link["link_id"]),
                    start_ns=start_ns,
                    end_ns=end_ns,
                    severity=severity,
                    shape="step",
                    effects=["throughput_degradation"],
                    impairment="capacity",
                    implementation="EXECUTABLE_CURRENT_INJECTOR",
                    segment_index=index,
                    fault_type="bandwidth_degradation",
                    before=f"{nominal_gbps}Gbps",
                    after=f"{max(1, round(nominal_gbps * fraction))}Gbps",
                )
            )
            targets.append(
                {"target_gpu": gpu_id, "target_link_id": str(link["link_id"])}
            )
        injector_path = out_dir / "simulator_injection_schedules" / f"{run_id}.csv"
        injector_digest = write_csv(injector_path, INJECTOR_CSV_COLUMNS, rows)
        single_target = len(targets) == 1
        truth_variant = {
            "family": str(spec["family"]),
            "duration_ns": DEFAULT_EVENT_DURATION_NS,
            "severity": {
                "name": "remaining_nominal_capacity_fraction",
                "value": float(spec["fraction"]),
                "unit": "fraction",
            },
            "shape": "step",
            "effects": ["throughput_degradation"],
            "impairment": "capacity",
            "implementation": "EXECUTABLE_CURRENT_INJECTOR",
        }
        path = out_dir / "fault_schedules" / f"{run_id}.csv"
        digest = write_csv(
            path,
            FAULT_TRUTH_CSV_COLUMNS,
            [
                fault_truth_row(
                    event_id=event_id,
                    variant=truth_variant,
                    gpu_id=targets[0]["target_gpu"] if single_target else None,
                    link_id=(targets[0]["target_link_id"] if single_target else None),
                    start_ns=start_ns,
                    targets=targets,
                )
            ],
        )
        schedule = relative_schedule(
            path,
            out_dir,
            digest,
            event_id,
            "fault",
            "predeclared_fault_schedule",
            "EXECUTABLE_CURRENT_INJECTOR",
            injector_path=injector_path,
            injector_sha256=injector_digest,
        )
        runs.append(
            base_run(
                seed=seed,
                run_id=run_id,
                run_role="ood_stress",
                class_label="GRAY_FAULT",
                scenario=str(spec["scenario"]),
                partition="ood_stress",
                schedule=schedule,
                topology_sha256=topology_sha256,
                workload_sha256=workload_sha256,
                virtual_finish_ns=end_ns + DETECTION_POST_EVENT_NS,
                target_gpu=targets[0]["target_gpu"] if single_target else None,
                target_link_id=(
                    targets[0]["target_link_id"] if single_target else None
                ),
                fault_family=str(spec["family"]),
                severity={
                    "name": "remaining_nominal_capacity_fraction",
                    "value": float(spec["fraction"]),
                    "unit": "fraction",
                },
                duration_ns=DEFAULT_EVENT_DURATION_NS,
                event_start_ns=start_ns,
                implementation="EXECUTABLE_CURRENT_INJECTOR",
                mechanism_id="dual_endpoint_data_rate",
                mechanism_status="PLANNED",
                targets=targets,
            )
        )
    return runs


def coverage_summary(
    runs: Sequence[Mapping[str, Any]], contract: Mapping[str, Any]
) -> Dict[str, Any]:
    scored = [run for run in runs if run["partition"] != "ood_stress"]
    family_counts = Counter(
        run["fault_family"] for run in scored if run.get("fault_family")
    )
    category_counts = Counter(
        run["gray_category"] for run in scored if run.get("gray_category")
    )
    class_counts = Counter(run["class_label"] for run in scored)
    target_counts = Counter(
        int(run["target_gpu"])
        for run in scored
        if run.get("target_gpu") is not None and run["run_role"] == "fault"
    )
    return {
        "class_counts": dict(sorted(class_counts.items())),
        "fault_family_counts": dict(sorted(family_counts.items())),
        "gray_category_counts": dict(sorted(category_counts.items())),
        "active_target_gpu_counts": {
            str(key): value for key, value in sorted(target_counts.items())
        },
        "required_hard_families": sorted(
            contract["fault_taxonomy"]["hard"]["families"]
        ),
        "required_gray_families": sorted(
            contract["fault_taxonomy"]["gray"]["families"]
        ),
        "required_gray_categories": sorted(GRAY_CATEGORIES),
        "required_congestion_scenarios": list(
            contract["fault_taxonomy"]["negative"]["congestion"]["required_scenarios"]
        ),
    }


def leakage_checks(
    runs: Sequence[Mapping[str, Any]],
    split_manifest: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append(
            {"name": name, "status": PASS if passed else FAIL, "detail": detail}
        )

    run_ids = [str(run["run_id"]) for run in runs]
    add(
        "unique_run_ids",
        len(run_ids) == len(set(run_ids)),
        f"runs={len(run_ids)}, unique={len(set(run_ids))}",
    )
    entries = split_manifest["entries"]
    entry_ids = [str(entry["run_id"]) for entry in entries]
    add(
        "split_bijection",
        sorted(run_ids) == sorted(entry_ids),
        f"runs={len(run_ids)}, entries={len(entry_ids)}",
    )
    group_partitions: Dict[str, set[str]] = defaultdict(set)
    for entry in entries:
        group_partitions[str(entry["split_group_id"])].add(str(entry["partition"]))
    bad_groups = {
        key: sorted(value) for key, value in group_partitions.items() if len(value) != 1
    }
    add(
        "complete_run_group_atomicity",
        not bad_groups,
        f"cross_partition_groups={bad_groups}",
    )

    holdouts = set(int(value) for value in split_manifest["paired_holdout_gpu_ids"])
    invalid_non_unseen = [
        run["run_id"]
        for run in runs
        if run["partition"] in {"train", "validation", "seen_link_test"}
        and run.get("target_gpu") in holdouts
        and run["run_role"] == "fault"
    ]
    invalid_unseen = [
        run["run_id"]
        for run in runs
        if run["partition"] == "unseen_link_test"
        and run.get("target_gpu") not in holdouts
    ]
    unseen_covered = {
        int(run["target_gpu"])
        for run in runs
        if run["partition"] == "unseen_link_test" and run.get("target_gpu") is not None
    }
    add(
        "paired_holdout_isolation",
        not invalid_non_unseen and not invalid_unseen,
        f"bad_regular={invalid_non_unseen}, bad_unseen={invalid_unseen}",
    )
    add(
        "paired_holdout_coverage",
        unseen_covered == holdouts,
        f"expected={sorted(holdouts)}, observed={sorted(unseen_covered)}",
    )

    leaking_features = [
        column
        for column in schema["model_feature_columns"]
        if any(pattern in column.lower() for pattern in LEAKAGE_PATTERNS)
    ]
    add(
        "model_feature_leakage",
        not leaking_features,
        f"forbidden_model_features={leaking_features}",
    )
    add(
        "schedule_not_model_input",
        schema["schedule_sidecars_are_inference_inputs"] is False,
        "schedule_sidecars_are_inference_inputs=false",
    )
    add(
        "split_not_model_input",
        schema["split_membership_is_model_feature"] is False,
        "split_membership_is_model_feature=false",
    )

    schedule_contract_ok = all(
        run["schedule"]["independent_of_features"]
        and run["schedule"]["generated_before_run"]
        for run in runs
    )
    add(
        "schedule_truth_independence",
        schedule_contract_ok,
        "all schedule objects predeclared and independent",
    )
    return {
        "schema_version": "limer.p2-leakage-checks.v1",
        "status": PASS if all(item["status"] == PASS for item in checks) else FAIL,
        "checks": checks,
    }


def require_empty_output(out_dir: Path) -> None:
    if out_dir.exists() and any(out_dir.iterdir()):
        raise CorpusError(
            f"output directory is not empty: {out_dir}; refusing to overwrite an immutable corpus"
        )
    out_dir.mkdir(parents=True, exist_ok=True)


def generate_corpus(
    *,
    link_map_path: Path,
    contract_path: Path,
    topology_path: Path,
    workload_path: Path,
    simulator_config_path: Path,
    out_dir: Path,
    seed: int,
    holdout_gpu_ids: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    if sys.byteorder != HASH_BYTE_ORDER:
        raise CorpusError(
            "P2 RDMA rail pinning mirrors a native little-endian C++ tuple; "
            f"unsupported host byte order: {sys.byteorder}"
        )
    for path in (
        link_map_path,
        contract_path,
        topology_path,
        workload_path,
        simulator_config_path,
    ):
        if not path.is_file():
            raise CorpusError(f"required input does not exist: {path}")
    if seed < 0:
        raise CorpusError("seed must be non-negative")
    contract = load_contract(contract_path)
    try:
        workload_report = traffic_workload.validate_workload(
            workload_path,
            profile=traffic_workload.HORIZON_PREFIX_PROFILE,
        )
    except traffic_workload.WorkloadValidationError as exc:
        raise CorpusError(
            f"P2 corpus workload is not horizon-prefix qualified: {exc}"
        ) from exc
    pairs, non_access = load_true16_links(link_map_path, contract)
    physical_links = load_physical_links(link_map_path)
    minimum_holdouts = int(
        contract["splits"]["unseen_link"]["minimum_held_out_gpu_groups"]
    )
    if holdout_gpu_ids is None:
        holdouts = deterministic_holdouts(seed, pairs, minimum_holdouts)
    else:
        holdouts = sorted(set(int(value) for value in holdout_gpu_ids))
    if len(holdouts) < minimum_holdouts:
        raise CorpusError(
            f"at least {minimum_holdouts} paired GPU holdouts are required"
        )
    if not set(holdouts).issubset(pairs):
        raise CorpusError(f"holdout GPU outside topology: {holdouts}")

    require_empty_output(out_dir)
    input_artifacts = {
        "contract": {
            "path": str(contract_path.resolve()),
            "sha256": sha256_file(contract_path),
        },
        "link_map": {
            "path": str(link_map_path.resolve()),
            "sha256": sha256_file(link_map_path),
        },
        "topology": {
            "path": str(topology_path.resolve()),
            "sha256": sha256_file(topology_path),
        },
        "workload": {
            "path": str(workload_path.resolve()),
            "sha256": sha256_file(workload_path),
        },
        "simulator_config": {
            "path": str(simulator_config_path.resolve()),
            "sha256": sha256_file(simulator_config_path),
        },
    }
    topology_sha = input_artifacts["topology"]["sha256"]
    workload_sha = input_artifacts["workload"]["sha256"]

    schema_value = feature_schema(contract)
    schema_path = out_dir / "feature_schema.json"
    schema_sha = write_json(schema_path, schema_value)

    runs = make_fault_runs(
        seed=seed,
        out_dir=out_dir,
        contract=contract,
        pairs=pairs,
        holdouts=set(holdouts),
        topology_sha256=topology_sha,
        workload_sha256=workload_sha,
    )
    runs.extend(
        make_negative_runs(
            seed=seed,
            out_dir=out_dir,
            contract=contract,
            pairs=pairs,
            physical_links=physical_links,
            topology_sha256=topology_sha,
            workload_sha256=workload_sha,
            workload_path=workload_path,
        )
    )
    runs.extend(
        make_ood_runs(
            seed=seed,
            out_dir=out_dir,
            pairs=pairs,
            non_access=non_access,
            topology_sha256=topology_sha,
            workload_sha256=workload_sha,
        )
    )
    runs.sort(key=lambda run: run["run_id"])
    planned_maximum_virtual_finish_ns = max(
        int(run["virtual_finish_ns"]) for run in runs
    )
    planned_minimum_virtual_finish_ns = min(
        int(run["virtual_finish_ns"]) for run in runs
    )
    workload_horizon_ns = int(
        workload_report["duration_estimate"]["corpus_max_virtual_finish_ns"]
    )
    if workload_horizon_ns < planned_maximum_virtual_finish_ns:
        raise CorpusError(
            "qualified workload horizon is shorter than the generated corpus: "
            f"workload={workload_horizon_ns}, "
            f"corpus={planned_maximum_virtual_finish_ns}"
        )

    corpus_id = (
        "p2-"
        + canonical_hash(
            {
                "identity_schema": "limer.p2-corpus-identity.v2",
                "contract_sha256": input_artifacts["contract"]["sha256"],
                "link_map_sha256": input_artifacts["link_map"]["sha256"],
                "topology_sha256": topology_sha,
                "workload_sha256": workload_sha,
                "simulator_config_sha256": input_artifacts["simulator_config"][
                    "sha256"
                ],
                "seed": seed,
                "holdouts": holdouts,
                "runs": [run_identity_entry(run) for run in runs],
            }
        )[:24]
    )

    split_manifest = {
        "schema_version": SPLIT_SCHEMA,
        "contract_id": contract["contract_id"],
        "corpus_id": corpus_id,
        "seed": seed,
        "atomic_unit": "complete_simulation_run",
        "generated_before_training": True,
        "immutable_after_training": True,
        "partitions": list(PARTITIONS),
        "paired_holdout_gpu_ids": holdouts,
        "paired_holdout_links": {
            str(gpu): {
                "plane_a_link_id": pairs[gpu]["A"]["link_id"],
                "plane_b_link_id": pairs[gpu]["B"]["link_id"],
            }
            for gpu in holdouts
        },
        "entries": [
            {
                "run_id": run["run_id"],
                "partition": run["partition"],
                "split_group_id": run["split_group_id"],
                "target_gpu": run["target_gpu"],
                "target_link_id": run["target_link_id"],
                "test_labels_sealed_until_final_evaluator": run["partition"]
                in {"seen_link_test", "unseen_link_test", "ood_stress"},
            }
            for run in runs
        ],
    }
    split_path = out_dir / "split_manifest.json"
    split_sha = write_json(split_path, split_manifest)

    leakage = leakage_checks(runs, split_manifest, schema_value)
    leakage_path = out_dir / "leakage_checks.json"
    leakage_sha = write_json(leakage_path, leakage)
    if leakage["status"] != PASS:
        raise CorpusError("generated split or feature schema failed leakage checks")

    coverage = coverage_summary(runs, contract)
    observable_report = {
        "schema_version": "limer.p2-observability-report.v1",
        "status": "PENDING_EXECUTION",
        "corpus_id": corpus_id,
        "scheduled_run_count": len(runs),
        "scheduled_event_count": len(runs),
        "observable_event_count": None,
        "not_observable_event_count": None,
        "policy": {
            "retain_scheduled_unobservable_events": True,
            "report_scheduled_and_observable_recall": True,
            "observability_must_come_from_data_plane_evidence": True,
        },
        "reason": "no simulator traces were executed by the manifest generator",
    }
    observability_path = out_dir / "observability_report.json"
    observability_sha = write_json(observability_path, observable_report)

    feature_ref = {
        **schema_value,
        "path": schema_path.relative_to(out_dir).as_posix(),
        "sha256": schema_sha,
    }
    corpus = {
        "schema_version": CORPUS_SCHEMA,
        "status": "PREPARED",
        "contract_id": contract["contract_id"],
        "contract_sha256": input_artifacts["contract"]["sha256"],
        "corpus_id": corpus_id,
        "identity_schema": "limer.p2-corpus-identity.v2",
        "generation_seed": seed,
        "generated_before_training": True,
        "input_artifacts": input_artifacts,
        "topology_contract": {
            "gpu_count": len(pairs),
            "access_link_count": sum(len(value) for value in pairs.values()),
            "active_plane": "B",
            "standby_plane": "A",
            "active_target_link_ids": [
                pairs[gpu]["B"]["link_id"] for gpu in sorted(pairs)
            ],
            "paired_access_paths": {
                str(gpu): {
                    "plane_a_link_id": pairs[gpu]["A"]["link_id"],
                    "plane_b_link_id": pairs[gpu]["B"]["link_id"],
                }
                for gpu in sorted(pairs)
            },
        },
        "feature_schema": feature_ref,
        "split_manifest": {
            "path": split_path.relative_to(out_dir).as_posix(),
            "sha256": split_sha,
            "paired_holdout_gpu_ids": holdouts,
            "generated_before_training": True,
        },
        "leakage_checks": {
            "path": leakage_path.relative_to(out_dir).as_posix(),
            "sha256": leakage_sha,
            "status": leakage["status"],
        },
        "observability_report": {
            "path": observability_path.relative_to(out_dir).as_posix(),
            "sha256": observability_sha,
            "status": observable_report["status"],
        },
        "schedule_set_sha256": canonical_hash(
            [schedule_identity_tuple(run) for run in runs]
        ),
        "workload_qualification": {
            "status": "PENDING_EXECUTION",
            "static_qualification_status": workload_report["status"],
            "static_qualification_profile": workload_report["qualification_profile"],
            "static_report_sha256": canonical_hash(workload_report),
            "static_report": workload_report,
            "causal_warmup_required_ns": int(
                contract["workload"]["causal_feature_warmup_ns"]
            ),
            "detection_post_event_required_ns": DETECTION_POST_EVENT_NS,
            "recovery_post_fault_observation_required_ns": int(
                contract["workload"]["recovery_observation_after_fault_ns"]
            ),
            "recovery_evaluation_run_count": sum(
                run.get("recovery_evaluation") is True for run in runs
            ),
            "recovery_actions_enabled": False,
            "planned_minimum_virtual_finish_ns": planned_minimum_virtual_finish_ns,
            "planned_maximum_virtual_finish_ns": planned_maximum_virtual_finish_ns,
            "reason": (
                "P2 plans detection-only runs with recovery disabled; the 1 s "
                "tail is required only for future runs explicitly marked "
                "recovery_evaluation=true. Planned duration is not execution evidence."
            ),
        },
        "coverage": coverage,
        "runs": runs,
    }
    corpus_path = out_dir / "corpus_manifest.json"
    write_json(corpus_path, corpus)

    stage_gate = {
        "schema_version": "limer.stage-gate-p2.v1",
        "corpus_id": corpus_id,
        "status": "BLOCKED_PENDING_EXECUTION",
        "preparation_status": PASS,
        "checks": [
            {"name": "immutable_schedules_generated", "status": PASS},
            {"name": "run_level_split_and_pair_holdout", "status": leakage["status"]},
            {"name": "feature_leakage_checks", "status": leakage["status"]},
            {"name": "observability_accounting", "status": PENDING},
            {"name": "stable_long_workload", "status": PENDING},
            {"name": "simulator_stability", "status": PENDING},
            {"name": "true_packet_drop_loss_mechanism", "status": PENDING},
            {"name": "true_carrier_flap_mechanism", "status": PENDING},
            {"name": "service_fraction_injector", "status": PENDING},
            {"name": "independent_congestion_executor", "status": PENDING},
        ],
        "claim": "P2 corpus is prepared but has not passed the execution/data-quality gate",
    }
    write_json(out_dir / "stage_gate_p2.json", stage_gate)

    report = verify_corpus(corpus_path)
    if report["status"] != PASS:
        failed = [check for check in report["checks"] if check["status"] == FAIL]
        raise CorpusError(f"self-verification failed: {failed}")
    return corpus


def read_csv_header(path: Path) -> List[str]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        return next(reader, [])


def prepared_background_contract_issues(
    run: Mapping[str, Any],
    truth_row: Mapping[str, str],
    background_rows: Sequence[Mapping[str, str]],
    topology_contract: Mapping[str, Any],
) -> List[str]:
    """Independently prove the immutable single-rail background contract."""
    issues: List[str] = []
    try:
        parameters = json.loads(truth_row["action_parameters_json"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        return [f"invalid action_parameters_json: {exc}"]
    if not isinstance(parameters, dict):
        return ["action_parameters_json is not an object"]
    required = {
        "destination_rank",
        "bottleneck_access_link_id",
        "paired_access_link_id",
        "data_plane",
        "route_candidate_order_host_ports",
        "route_bucket",
        "hash_algorithm",
        "hash_seed_u32",
        "hash_tuple",
        "hash_byte_order",
        "pin_reverse_ack",
        "predeclared_window_policy",
        "realized_window_policy",
        "completion_deadline_ns",
        "rdma_rto_us",
        "rdma_retry_limit",
        "max_rto_retry_events",
    }
    missing = sorted(required - set(parameters))
    if missing:
        return [f"missing action fields: {missing}"]
    try:
        destination = int(parameters["destination_rank"])
        route_bucket = int(parameters["route_bucket"])
        host_ports = [
            int(value) for value in parameters["route_candidate_order_host_ports"]
        ]
        truth_start = int(truth_row["start_time_ns"])
        truth_end = int(truth_row["end_time_ns"])
        starts = [int(row["scheduled_start_ns"]) for row in background_rows]
        deadline = int(parameters["completion_deadline_ns"])
        rto_us = int(parameters["rdma_rto_us"])
        retry_limit = int(parameters["rdma_retry_limit"])
        max_retries = int(parameters["max_rto_retry_events"])
        hash_seed = int(parameters["hash_seed_u32"])
    except (KeyError, TypeError, ValueError) as exc:
        return [f"invalid numeric contract field: {exc}"]
    if not starts:
        return ["background schedule is empty"]

    expected_plane = {0: "A", 1: "B"}.get(route_bucket)
    paired_paths = topology_contract.get("paired_access_paths", {})
    pair = paired_paths.get(str(destination), {})
    expected_target = pair.get(
        "plane_a_link_id" if expected_plane == "A" else "plane_b_link_id"
    )
    expected_paired = pair.get(
        "plane_b_link_id" if expected_plane == "A" else "plane_a_link_id"
    )
    exact_values = {
        "manifest target_gpu": (run.get("target_gpu"), destination),
        "manifest target_link_id": (run.get("target_link_id"), expected_target),
        "manifest paired_link_id": (run.get("paired_link_id"), expected_paired),
        "truth bottleneck link": (
            parameters.get("bottleneck_access_link_id"),
            expected_target,
        ),
        "truth paired link": (parameters.get("paired_access_link_id"), expected_paired),
        "truth data plane": (parameters.get("data_plane"), expected_plane),
        "route candidate ports": (host_ports, [2, 3]),
        "hash algorithm": (parameters.get("hash_algorithm"), "ns3-murmur3-x86-32"),
        "hash seed": (hash_seed, MURMUR3_SEED_U32),
        "hash tuple": (parameters.get("hash_tuple"), "native-le-sip-dip-sport-dport"),
        "hash byte order": (parameters.get("hash_byte_order"), HASH_BYTE_ORDER),
        "pin reverse ACK": (parameters.get("pin_reverse_ack"), True),
        "predeclared window policy": (
            parameters.get("predeclared_window_policy"),
            "scheduled_qp_launch_window",
        ),
        "realized window policy": (
            parameters.get("realized_window_policy"),
            "first_data_tx_to_last_ack_complete",
        ),
        "truth start": (truth_start, min(starts)),
        "truth end": (truth_end, max(starts) + 1),
        "manifest event start": (run.get("fault_scheduled_onset_ns"), truth_start),
        "manifest duration": (run.get("duration_ns"), truth_end - truth_start),
        "completion deadline": (
            deadline,
            truth_start + BACKGROUND_COMPLETION_DEADLINE_NS,
        ),
        "virtual finish": (
            run.get("virtual_finish_ns"),
            deadline + DETECTION_POST_EVENT_NS,
        ),
        "RDMA RTO": (rto_us, BACKGROUND_RDMA_RTO_US),
        "RDMA retry limit": (retry_limit, BACKGROUND_RDMA_RETRY_LIMIT),
        "maximum retry events": (max_retries, 0),
    }
    for description, (observed, expected) in exact_values.items():
        if observed != expected:
            issues.append(
                f"{description}: expected {expected!r}, observed {observed!r}"
            )
    if expected_target is None or expected_paired is None:
        issues.append(f"destination {destination} has no frozen A/B link pair")
    if expected_target == expected_paired:
        issues.append("target and paired ACCESS links are not distinct")
    if sys.byteorder != HASH_BYTE_ORDER:
        issues.append(f"unsupported host byte order: {sys.byteorder}")

    for row in background_rows:
        try:
            src = int(row["src_rank"])
            dst = int(row["dst_rank"])
            sport = int(row["sport"])
            dport = int(row["dport"])
            data_bucket = rdma_route_bucket(src=src, dst=dst, sport=sport, dport=dport)
            ack_bucket = rdma_route_bucket(
                src=src, dst=dst, sport=sport, dport=dport, reverse=True
            )
        except (KeyError, TypeError, ValueError, CorpusError) as exc:
            issues.append(f"invalid flow hash tuple: {exc}")
            continue
        if dst != destination:
            issues.append(
                f"flow {row.get('flow_id')} destination {dst} != {destination}"
            )
        if data_bucket != route_bucket or ack_bucket != route_bucket:
            issues.append(
                f"flow {row.get('flow_id')} hashes data/ACK to "
                f"{data_bucket}/{ack_bucket}, expected {route_bucket}"
            )
    return issues


def verify_corpus(corpus_path: Path) -> Dict[str, Any]:
    corpus_path = corpus_path.resolve()
    root = corpus_path.parent
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    checks: List[Dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append(
            {
                "name": name,
                "status": PASS if passed else FAIL,
                "detail": detail,
            }
        )

    add(
        "schema",
        corpus.get("schema_version") == CORPUS_SCHEMA,
        corpus.get("schema_version"),
    )
    add("prepared_status", corpus.get("status") == "PREPARED", corpus.get("status"))
    runs = corpus.get("runs", [])
    run_ids = [run.get("run_id") for run in runs]
    add(
        "unique_run_ids",
        len(run_ids) == len(set(run_ids)),
        {"runs": len(run_ids), "unique": len(set(run_ids))},
    )

    input_mismatches = []
    for name, reference in corpus.get("input_artifacts", {}).items():
        path = Path(str(reference.get("path", ""))).resolve()
        if not path.is_file():
            input_mismatches.append({"artifact": name, "reason": "missing"})
            continue
        observed = sha256_file(path)
        if observed != reference.get("sha256"):
            input_mismatches.append(
                {
                    "artifact": name,
                    "expected": reference.get("sha256"),
                    "observed": observed,
                }
            )
    add("input_artifact_hashes", not input_mismatches, input_mismatches)

    workload_issues: List[str] = []
    qualification = corpus.get("workload_qualification", {})
    workload_ref = corpus.get("input_artifacts", {}).get("workload", {})
    try:
        workload_path = Path(str(workload_ref.get("path", ""))).resolve()
        workload_report = traffic_workload.validate_workload(
            workload_path,
            profile=traffic_workload.HORIZON_PREFIX_PROFILE,
        )
        planned_maximum = max(int(run["virtual_finish_ns"]) for run in runs)
        if qualification.get("static_qualification_status") != "PASS":
            workload_issues.append("static qualification status is not PASS")
        if qualification.get("static_qualification_profile") != (
            traffic_workload.HORIZON_PREFIX_PROFILE
        ):
            workload_issues.append("static qualification profile is not horizon-prefix")
        if qualification.get("static_report") != workload_report:
            workload_issues.append(
                "inline static workload report differs from recomputation"
            )
        if qualification.get("static_report_sha256") != canonical_hash(workload_report):
            workload_issues.append("static workload report hash differs")
        if int(qualification.get("planned_maximum_virtual_finish_ns", -1)) != (
            planned_maximum
        ):
            workload_issues.append(
                "planned maximum virtual finish differs from run inventory"
            )
        if (
            int(workload_report["duration_estimate"]["corpus_max_virtual_finish_ns"])
            < planned_maximum
        ):
            workload_issues.append("workload horizon does not cover maximum run finish")
    except (
        OSError,
        TypeError,
        ValueError,
        KeyError,
        traffic_workload.WorkloadValidationError,
    ) as exc:
        workload_issues.append(str(exc))
    add(
        "horizon_prefix_workload_static_qualification",
        not workload_issues,
        workload_issues,
    )

    identity_schema = corpus.get("identity_schema")
    identity_material = {
        "identity_schema": identity_schema,
        "contract_sha256": corpus.get("input_artifacts", {})
        .get("contract", {})
        .get("sha256"),
        "link_map_sha256": corpus.get("input_artifacts", {})
        .get("link_map", {})
        .get("sha256"),
        "topology_sha256": corpus.get("input_artifacts", {})
        .get("topology", {})
        .get("sha256"),
        "workload_sha256": corpus.get("input_artifacts", {})
        .get("workload", {})
        .get("sha256"),
        "simulator_config_sha256": corpus.get("input_artifacts", {})
        .get("simulator_config", {})
        .get("sha256"),
        "seed": corpus.get("generation_seed"),
        "holdouts": corpus.get("split_manifest", {}).get("paired_holdout_gpu_ids"),
        "runs": [run_identity_entry(run) for run in runs],
    }
    expected_corpus_id = "p2-" + canonical_hash(identity_material)[:24]
    add(
        "corpus_identity_v2_binding",
        identity_schema == "limer.p2-corpus-identity.v2"
        and corpus.get("corpus_id") == expected_corpus_id,
        {
            "identity_schema": identity_schema,
            "expected": expected_corpus_id,
            "observed": corpus.get("corpus_id"),
        },
    )

    schedule_mismatches = []
    bad_schedule_flags = []
    bad_congestion_headers = []
    background_mismatches = []
    background_contract_mismatches = []
    collective_override_mismatches = []
    effective_workload_mismatches = []
    ecmp_collision_mismatches = []
    congestion_mechanism_mismatches = []
    bad_schedule_kinds = []
    bad_fault_truth = []
    injector_mismatches = []
    bad_mechanisms = []
    has_background_contract = any(
        isinstance(run.get("schedule", {}).get("background_flow_schedule"), Mapping)
        for run in runs
    )
    for run in runs:
        schedule = run["schedule"]
        collective_ref = schedule.get("collective_workload_override")
        expected_effective = (
            collective_ref.get("sha256")
            if isinstance(collective_ref, Mapping)
            else corpus.get("input_artifacts", {}).get("workload", {}).get("sha256")
        )
        if run.get("effective_workload_sha256") != expected_effective:
            effective_workload_mismatches.append({
                "run_id": run.get("run_id"),
                "expected": expected_effective,
                "observed": run.get("effective_workload_sha256"),
            })
        path = (root / schedule["path"]).resolve()
        try:
            path.relative_to(root)
        except ValueError:
            schedule_mismatches.append(
                {"run_id": run["run_id"], "reason": "path_escape"}
            )
            continue
        if not path.is_file():
            schedule_mismatches.append({"run_id": run["run_id"], "reason": "missing"})
            continue
        observed = sha256_file(path)
        if observed != schedule["sha256"]:
            schedule_mismatches.append(
                {
                    "run_id": run["run_id"],
                    "expected": schedule["sha256"],
                    "observed": observed,
                }
            )
        if not schedule.get("independent_of_features") or not schedule.get(
            "generated_before_run"
        ):
            bad_schedule_flags.append(run["run_id"])
        expected_kind = (
            "fault"
            if run["class_label"] in {"HARD_FAULT", "GRAY_FAULT"}
            else "congestion"
            if run["class_label"] == "CONGESTION"
            else "healthy"
        )
        if schedule.get("kind") != expected_kind:
            bad_schedule_kinds.append(
                {
                    "run_id": run["run_id"],
                    "expected": expected_kind,
                    "observed": schedule.get("kind"),
                }
            )
        mechanism = run.get("mechanism", {})
        mechanism_status = mechanism.get("implementation_status")
        if (
            not mechanism.get("mechanism_id")
            or mechanism_status not in {"PLANNED", "UNAVAILABLE"}
            or mechanism.get("semantic_validation") is not None
        ):
            bad_mechanisms.append({"run_id": run["run_id"], "mechanism": mechanism})
        if expected_kind == "fault":
            with path.open(encoding="utf-8", newline="") as stream:
                truth_rows = list(csv.DictReader(stream))
            target = (
                "" if run.get("target_link_id") is None else str(run["target_link_id"])
            )
            if (
                read_csv_header(path) != list(FAULT_TRUTH_CSV_COLUMNS)
                or len(truth_rows) != 1
                or truth_rows[0].get("event_id") != schedule.get("event_id")
                or truth_rows[0].get("fault_family") != run.get("fault_family")
                or truth_rows[0].get("target_link_id", "") != target
            ):
                bad_fault_truth.append(
                    {
                        "run_id": run["run_id"],
                        "header": read_csv_header(path),
                        "row_count": len(truth_rows),
                    }
                )
            injector = schedule.get("simulator_injection_schedule")
            if not isinstance(injector, Mapping):
                injector_mismatches.append(
                    {"run_id": run["run_id"], "reason": "missing reference"}
                )
            else:
                injector_path = (root / str(injector.get("path", ""))).resolve()
                try:
                    injector_path.relative_to(root)
                except ValueError:
                    injector_mismatches.append(
                        {"run_id": run["run_id"], "reason": "path_escape"}
                    )
                    continue
                expected_safe = not str(
                    schedule.get("implementation_status", "")
                ).startswith("BLOCKED")
                if (
                    not injector_path.is_file()
                    or sha256_file(injector_path) != injector.get("sha256")
                    or read_csv_header(injector_path) != list(INJECTOR_CSV_COLUMNS)
                    or injector_path == path
                    or injector.get("safe_to_execute") is not expected_safe
                ):
                    injector_mismatches.append(
                        {
                            "run_id": run["run_id"],
                            "path": str(injector_path),
                            "safe": injector.get("safe_to_execute"),
                            "expected_safe": expected_safe,
                        }
                    )
        if run["run_role"] == "congestion":
            header = read_csv_header(path)
            with path.open(encoding="utf-8", newline="") as stream:
                negative_rows = list(csv.DictReader(stream))
            forbidden = [
                column
                for column in header
                if any(
                    token in column.lower()
                    for token in ("fault", "target", "label", "severity")
                )
            ]
            if forbidden or header != list(NEGATIVE_CSV_COLUMNS):
                bad_congestion_headers.append(
                    {"run_id": run["run_id"], "forbidden": forbidden, "header": header}
                )
            if not has_background_contract:
                continue
            scenario = str(run.get("scenario", ""))
            supported = scenario in EXECUTABLE_BACKGROUND_SCENARIOS
            collective_supported = scenario in STATIC_COLLECTIVE_OVERRIDE_SCENARIOS
            expected_impl, expected_mechanism, expected_mechanism_status = (
                congestion_implementation(scenario)
            )
            if (
                schedule.get("implementation_status") != expected_impl
                or mechanism.get("mechanism_id") != expected_mechanism
                or mechanism_status != expected_mechanism_status
                or len(negative_rows) != 1
                or negative_rows[0].get("implementation_status") != expected_impl
            ):
                congestion_mechanism_mismatches.append(
                    {
                        "run_id": run["run_id"],
                        "expected_implementation": expected_impl,
                        "schedule_implementation": schedule.get(
                            "implementation_status"
                        ),
                        "expected_mechanism": expected_mechanism,
                        "mechanism": mechanism,
                    }
                )
            background = schedule.get("background_flow_schedule")
            collective = schedule.get("collective_workload_override")
            ecmp = schedule.get("ecmp_collision_schedule")
            if collective_supported:
                if not isinstance(collective, Mapping):
                    collective_override_mismatches.append(
                        {"run_id": run["run_id"], "reason": "missing_reference"}
                    )
                else:
                    override_path = (root / str(collective.get("path", ""))).resolve()
                    try:
                        override_path.relative_to(root)
                        role_ref = collective.get("layer_role_sidecar")
                        if not isinstance(role_ref, Mapping):
                            raise CorpusError("collective role reference is missing")
                        role_path = (root / str(role_ref.get("path", ""))).resolve()
                        role_path.relative_to(root)
                        truth_start = int(negative_rows[0]["start_time_ns"])
                        source_workload = Path(str(
                            corpus["input_artifacts"]["workload"]["path"]
                        )).resolve()
                        contract_path = Path(str(
                            corpus["input_artifacts"]["contract"]["path"]
                        )).resolve()
                        link_map_path = Path(str(
                            corpus["input_artifacts"]["link_map"]["path"]
                        )).resolve()
                        verify_pairs, _ = load_true16_links(
                            link_map_path, load_contract(contract_path)
                        )
                        aggregate_bandwidths = {
                            sum(
                                int(link["bandwidth_bps"])
                                for link in rails.values()
                            )
                            for rails in verify_pairs.values()
                        }
                        if len(aggregate_bandwidths) != 1:
                            raise CorpusError("non-uniform aggregate ACCESS bandwidth")
                        contract = collective_override_contract(
                            scenario,
                            truth_start,
                            source_workload_sha256=corpus["input_artifacts"][
                                "workload"
                            ]["sha256"],
                            aggregate_access_bandwidth_bps=next(
                                iter(aggregate_bandwidths)
                            ),
                        )
                        report = validate_collective_override(
                            override_path,
                            role_path,
                            contract,
                            source_workload,
                            str(run["run_id"]),
                        )
                        valid_collective = (
                            sha256_file(override_path) == collective.get("sha256")
                            and sha256_file(role_path) == role_ref.get("sha256")
                            and collective.get("kind")
                            == "simai_collective_workload_override"
                            and collective.get("format") == COLLECTIVE_OVERRIDE_FORMAT
                            and collective.get("generated_before_run") is True
                            and collective.get("safe_to_execute") is True
                            and collective.get("runtime_executor_status") == "READY"
                            and role_ref.get("kind") == "collective_layer_roles"
                            and role_ref.get("format") == COLLECTIVE_ROLE_FORMAT
                            and role_ref.get("generated_before_run") is True
                            and collective.get("static_validation") == report
                            and collective.get("static_validation_sha256")
                            == canonical_hash(report)
                            and schedule.get("implementation_status")
                            == COLLECTIVE_OVERRIDE_STATUS
                            and run.get("effective_workload_sha256")
                            == collective.get("sha256")
                        )
                    except (OSError, KeyError, TypeError, ValueError, CorpusError):
                        valid_collective = False
                    if not valid_collective:
                        collective_override_mismatches.append(
                            {"run_id": run["run_id"], "reason": "invalid_contract"}
                        )
            elif collective is not None:
                collective_override_mismatches.append(
                    {"run_id": run["run_id"], "reason": "unexpected_reference"}
                )
            if scenario == "ecmp_or_hash_contention":
                if not isinstance(ecmp, Mapping):
                    ecmp_collision_mismatches.append(
                        {"run_id": run["run_id"], "reason": "missing_reference"}
                    )
                else:
                    ecmp_path = (root / str(ecmp.get("path", ""))).resolve()
                    try:
                        ecmp_path.relative_to(root)
                        contract = ecmp.get("contract")
                        if not isinstance(contract, Mapping):
                            raise CorpusError("ECMP collision contract is missing")
                        validate_ecmp_collision_schedule(
                            ecmp_path,
                            contract,
                            load_physical_links(
                                Path(str(corpus["input_artifacts"]["link_map"]["path"]))
                            ),
                        )
                        valid_ecmp = (
                            sha256_file(ecmp_path) == ecmp.get("sha256")
                            and ecmp.get("kind") == "native_ecmp_collision"
                            and ecmp.get("format") == ECMP_COLLISION_FORMAT
                            and ecmp.get("generated_before_run") is True
                            and ecmp.get("safe_to_execute") is False
                            and ecmp.get("runtime_route_evidence_status") == "PENDING"
                            and ecmp.get("contract_sha256") == canonical_hash(contract)
                            and schedule.get("implementation_status")
                            == ECMP_COLLISION_STATUS
                        )
                    except (OSError, KeyError, TypeError, ValueError, CorpusError):
                        valid_ecmp = False
                    if not valid_ecmp:
                        ecmp_collision_mismatches.append(
                            {"run_id": run["run_id"], "reason": "invalid_contract"}
                        )
            elif ecmp is not None:
                ecmp_collision_mismatches.append(
                    {"run_id": run["run_id"], "reason": "unexpected_reference"}
                )
            if not supported:
                if background is not None:
                    background_mismatches.append(
                        {"run_id": run["run_id"], "reason": "unexpected_reference"}
                    )
                continue
            if not isinstance(background, Mapping):
                background_mismatches.append(
                    {"run_id": run["run_id"], "reason": "missing_reference"}
                )
                continue
            background_path = (root / str(background.get("path", ""))).resolve()
            try:
                background_path.relative_to(root)
            except ValueError:
                background_mismatches.append(
                    {"run_id": run["run_id"], "reason": "path_escape"}
                )
                continue
            if (
                not background_path.is_file()
                or sha256_file(background_path) != background.get("sha256")
                or read_csv_header(background_path) != list(BACKGROUND_FLOW_CSV_COLUMNS)
                or background.get("kind") != "background_rdma"
                or background.get("format") != "limer-background-rdma-v1"
                or background.get("safe_to_execute") is not True
                or background.get("generated_before_run") is not True
            ):
                background_mismatches.append(
                    {"run_id": run["run_id"], "reason": "reference_or_hash"}
                )
                continue
            with background_path.open(encoding="utf-8", newline="") as stream:
                background_rows = list(csv.DictReader(stream))
            expected_count = 12 if scenario == "incast" else 24
            event_id = str(schedule.get("event_id", ""))
            try:
                truth_start = int(negative_rows[0]["start_time_ns"])
                truth_end = int(negative_rows[0]["end_time_ns"])
                flow_ids = {row["flow_id"] for row in background_rows}
                qp_keys = {
                    (
                        int(row["src_rank"]),
                        int(row["dst_rank"]),
                        int(row["sport"]),
                        int(row["pg"]),
                    )
                    for row in background_rows
                }
                rows_valid = all(
                    row["event_id"] == event_id
                    and row["scenario"] == scenario
                    and truth_start <= int(row["scheduled_start_ns"]) < truth_end
                    and 0 <= int(row["src_rank"]) < 16
                    and 0 <= int(row["dst_rank"]) < 16
                    and int(row["src_rank"]) // 4 != int(row["dst_rank"]) // 4
                    and int(row["bytes"]) > 0
                    and 1 <= int(row["pg"]) <= 7
                    and 49152 <= int(row["sport"]) <= 65535
                    and int(row["dport"]) > 0
                    for row in background_rows
                )
                destinations = {int(row["dst_rank"]) for row in background_rows}
                starts = sorted(
                    {int(row["scheduled_start_ns"]) for row in background_rows}
                )
                destination = next(iter(destinations)) if len(destinations) == 1 else -1
                expected_sources = {
                    rank
                    for rank in range(16)
                    if destination >= 0 and rank // 4 != destination // 4
                }
                sources_by_start = {
                    start: {
                        int(row["src_rank"])
                        for row in background_rows
                        if int(row["scheduled_start_ns"]) == start
                    }
                    for start in starts
                }
                counts_by_start = {
                    start: sum(
                        int(row["scheduled_start_ns"]) == start
                        for row in background_rows
                    )
                    for start in starts
                }
                shared_profile = (
                    len({int(row["bytes"]) for row in background_rows}) == 1
                    and len({int(row["pg"]) for row in background_rows}) == 1
                    and len({int(row["dport"]) for row in background_rows}) == 1
                )
                if scenario == "incast":
                    profile_valid = (
                        len(starts) == 1
                        and sources_by_start.get(starts[0], set()) == expected_sources
                        and counts_by_start.get(starts[0]) == 12
                    )
                else:
                    profile_valid = (
                        len(starts) == 2
                        and starts[1] > starts[0]
                        and all(
                            sources_by_start.get(start, set()) == expected_sources
                            and counts_by_start.get(start) == 12
                            for start in starts
                        )
                    )
                rows_valid = (
                    rows_valid
                    and shared_profile
                    and profile_valid
                    and negative_rows[0].get("action_scope") == "workload"
                )
            except (KeyError, TypeError, ValueError):
                rows_valid = False
                flow_ids = set()
                qp_keys = set()
            if (
                len(background_rows) != expected_count
                or len(flow_ids) != len(background_rows)
                or len(qp_keys) != len(background_rows)
                or not rows_valid
            ):
                background_mismatches.append(
                    {
                        "run_id": run["run_id"],
                        "reason": "invalid_rows",
                        "row_count": len(background_rows),
                        "expected_count": expected_count,
                    }
                )
            if len(negative_rows) == 1:
                contract_issues = prepared_background_contract_issues(
                    run,
                    negative_rows[0],
                    background_rows,
                    corpus.get("topology_contract", {}),
                )
                if contract_issues:
                    background_contract_mismatches.append(
                        {"run_id": run["run_id"], "issues": contract_issues}
                    )
    add("schedule_hashes", not schedule_mismatches, schedule_mismatches)
    add("schedule_truth_flags", not bad_schedule_flags, bad_schedule_flags)
    add("canonical_schedule_kinds", not bad_schedule_kinds, bad_schedule_kinds)
    add("canonical_fault_truth", not bad_fault_truth, bad_fault_truth)
    add(
        "separate_simulator_injection_schedules",
        not injector_mismatches,
        injector_mismatches,
    )
    add(
        "congestion_schema_independence",
        not bad_congestion_headers,
        bad_congestion_headers,
    )
    add(
        "background_flow_schedule_binding",
        not background_mismatches,
        background_mismatches,
    )
    add(
        "collective_workload_override_binding",
        not collective_override_mismatches,
        collective_override_mismatches,
    )
    add(
        "effective_workload_identity_binding",
        not effective_workload_mismatches,
        effective_workload_mismatches,
    )
    add(
        "ecmp_collision_schedule_binding",
        not ecmp_collision_mismatches,
        ecmp_collision_mismatches,
    )
    add(
        "background_single_rail_contract",
        not background_contract_mismatches,
        background_contract_mismatches,
    )
    add(
        "congestion_mechanism_contract",
        not congestion_mechanism_mismatches,
        congestion_mechanism_mismatches,
    )
    add("prepared_mechanism_contract", not bad_mechanisms, bad_mechanisms)
    observed_schedule_set = canonical_hash(
        [schedule_identity_tuple(run) for run in runs]
    )
    add(
        "schedule_set_hash",
        observed_schedule_set == corpus.get("schedule_set_sha256"),
        {
            "expected": corpus.get("schedule_set_sha256"),
            "observed": observed_schedule_set,
        },
    )

    split_ref = corpus["split_manifest"]
    split_path = root / split_ref["path"]
    add(
        "split_manifest_hash",
        split_path.is_file() and sha256_file(split_path) == split_ref["sha256"],
        split_ref["path"],
    )
    split = (
        json.loads(split_path.read_text(encoding="utf-8"))
        if split_path.is_file()
        else {}
    )
    add(
        "split_schema",
        split.get("schema_version") == SPLIT_SCHEMA,
        split.get("schema_version"),
    )
    entries = split.get("entries", [])
    add(
        "split_run_bijection",
        sorted(run_ids) == sorted(entry.get("run_id") for entry in entries),
        {"runs": len(run_ids), "entries": len(entries)},
    )
    add(
        "partition_vocabulary",
        set(entry.get("partition") for entry in entries).issubset(PARTITIONS)
        and set(PARTITIONS).issubset(entry.get("partition") for entry in entries),
        sorted(set(entry.get("partition") for entry in entries)),
    )
    grouped: Dict[str, set[str]] = defaultdict(set)
    for entry in entries:
        grouped[str(entry.get("split_group_id"))].add(str(entry.get("partition")))
    crossing = {key: sorted(value) for key, value in grouped.items() if len(value) > 1}
    add("run_group_atomicity", not crossing, crossing)

    holdouts = set(split.get("paired_holdout_gpu_ids", []))
    add("minimum_paired_holdouts", len(holdouts) >= 4, sorted(holdouts))
    regular_holdout_faults = [
        run["run_id"]
        for run in runs
        if run["run_role"] == "fault"
        and run["partition"] in {"train", "validation", "seen_link_test"}
        and run["target_gpu"] in holdouts
    ]
    unseen_wrong = [
        run["run_id"]
        for run in runs
        if run["partition"] == "unseen_link_test" and run["target_gpu"] not in holdouts
    ]
    unseen_coverage = {
        run["target_gpu"] for run in runs if run["partition"] == "unseen_link_test"
    }
    add(
        "paired_holdout_isolation",
        not regular_holdout_faults and not unseen_wrong,
        {"regular": regular_holdout_faults, "unseen": unseen_wrong},
    )
    add(
        "paired_holdout_coverage",
        unseen_coverage == holdouts,
        {"expected": sorted(holdouts), "observed": sorted(unseen_coverage)},
    )

    feature_ref = corpus["feature_schema"]
    feature_path = root / feature_ref["path"]
    add(
        "feature_schema_hash",
        feature_path.is_file() and sha256_file(feature_path) == feature_ref["sha256"],
        feature_ref["path"],
    )
    external_feature = (
        json.loads(feature_path.read_text(encoding="utf-8"))
        if feature_path.is_file()
        else {}
    )
    inline_feature = {
        key: value
        for key, value in feature_ref.items()
        if key not in {"path", "sha256"}
    }
    add(
        "feature_schema_inline_binding",
        external_feature == inline_feature,
        "inline feature schema equals hashed sidecar",
    )
    model_features = feature_ref.get("model_feature_columns", [])
    leaking = [
        column
        for column in model_features
        if any(pattern in column.lower() for pattern in LEAKAGE_PATTERNS)
    ]
    add("model_features_nonempty", bool(model_features), len(model_features))
    add("model_feature_leakage", not leaking, leaking)
    add(
        "feature_column_roles",
        bool(feature_ref.get("identifier_columns"))
        and bool(feature_ref.get("label_columns")),
        {
            "identifiers": len(feature_ref.get("identifier_columns", [])),
            "labels": len(feature_ref.get("label_columns", [])),
        },
    )

    auxiliary_mismatches = []
    for name in ("leakage_checks", "observability_report"):
        reference = corpus.get(name, {})
        path = root / str(reference.get("path", ""))
        if not path.is_file() or sha256_file(path) != reference.get("sha256"):
            auxiliary_mismatches.append(name)
    add("auxiliary_report_hashes", not auxiliary_mismatches, auxiliary_mismatches)

    topology_sha = corpus.get("input_artifacts", {}).get("topology", {}).get("sha256")
    workload_sha = corpus.get("input_artifacts", {}).get("workload", {}).get("sha256")
    bad_run_inputs = [
        run["run_id"]
        for run in runs
        if run.get("topology_sha256") != topology_sha
        or run.get("workload_sha256") != workload_sha
    ]
    add("run_input_hash_binding", not bad_run_inputs, bad_run_inputs)

    scored_faults = [run for run in runs if run["run_role"] == "fault"]
    required_hard = set(corpus["coverage"]["required_hard_families"])
    required_gray = set(corpus["coverage"]["required_gray_families"])
    observed_hard = {
        run["fault_family"]
        for run in scored_faults
        if run["class_label"] == "HARD_FAULT"
    }
    observed_gray = {
        run["fault_family"]
        for run in scored_faults
        if run["class_label"] == "GRAY_FAULT"
    }
    add(
        "hard_family_coverage",
        observed_hard == required_hard,
        {"required": sorted(required_hard), "observed": sorted(observed_hard)},
    )
    add(
        "gray_family_coverage",
        observed_gray == required_gray,
        {"required": sorted(required_gray), "observed": sorted(observed_gray)},
    )
    add(
        "gray_category_coverage",
        {run["gray_category"] for run in scored_faults if run.get("gray_category")}
        == set(GRAY_CATEGORIES),
        sorted(
            {run["gray_category"] for run in scored_faults if run.get("gray_category")}
        ),
    )
    active_targets = set(corpus["topology_contract"]["active_target_link_ids"])
    observed_targets = {run["target_link_id"] for run in scored_faults}
    add(
        "all_active_targets_covered",
        observed_targets == active_targets,
        {"expected": sorted(active_targets), "observed": sorted(observed_targets)},
    )

    warmup_ns = int(corpus["workload_qualification"]["causal_warmup_required_ns"])
    bad_warmup = [
        run["run_id"]
        for run in runs
        if run.get("fault_scheduled_onset_ns") is not None
        and int(run["fault_scheduled_onset_ns"]) < warmup_ns
    ]
    add("causal_warmup_planned", not bad_warmup, bad_warmup)
    bad_observation = []
    for run in scored_faults:
        onset = int(run["fault_scheduled_onset_ns"])
        event_end = onset + int(run["duration_ns"])
        required_tail = (
            RECOVERY_POST_FAULT_OBSERVATION_NS
            if run.get("recovery_evaluation") is True
            else DETECTION_POST_EVENT_NS
        )
        if int(run["virtual_finish_ns"]) - max(onset, event_end) < required_tail:
            bad_observation.append(run["run_id"])
    add("post_fault_observation_planned", not bad_observation, bad_observation)

    stability_mismatches = []
    blocked_stability_count = 0
    pending_stability_count = 0
    for run in runs:
        implementation = str(
            run.get("schedule", {}).get("implementation_status", "")
        )
        blocked = implementation.startswith("BLOCKED")
        expected_status = (
            "BLOCKED_UNSUPPORTED" if blocked else "PENDING_EXECUTION"
        )
        stability = run.get("simulator_stability", {})
        if blocked:
            blocked_stability_count += 1
        else:
            pending_stability_count += 1
        if (
            not isinstance(stability, Mapping)
            or stability.get("gate_required") is not True
            or stability.get("status") != expected_status
        ):
            stability_mismatches.append(
                {
                    "run_id": run.get("run_id"),
                    "implementation_status": implementation,
                    "expected_status": expected_status,
                    "observed": stability,
                }
            )
    add(
        "all_run_simulator_stability_admission_declared",
        not stability_mismatches,
        {
            "pending_execution_run_count": pending_stability_count,
            "blocked_unsupported_run_count": blocked_stability_count,
            "mismatches": stability_mismatches,
        },
    )
    carrier_flaps = [
        run for run in scored_faults if run["fault_family"] == "carrier_flap"
    ]
    add(
        "carrier_flap_is_planned_real_mechanism",
        bool(carrier_flaps)
        and all(
            run["mechanism"]["mechanism_id"] == "physical_carrier_flap_channel_epoch"
            and run["mechanism"]["implementation_status"] == "PLANNED"
            for run in carrier_flaps
        ),
        len(carrier_flaps),
    )
    hard_disconnect = [
        run for run in scored_faults if run["fault_family"] == "hard_disconnect"
    ]
    add(
        "hard_disconnect_is_planned_real_mechanism",
        bool(hard_disconnect)
        and all(
            run["mechanism"]["mechanism_id"] == "physical_link_down"
            and run["mechanism"]["implementation_status"] == "PLANNED"
            for run in hard_disconnect
        ),
        len(hard_disconnect),
    )
    true_loss = [
        run
        for run in scored_faults
        if run["fault_family"] in {"random_loss", "burst_loss"}
        or (
            run["fault_family"] == "intermittent_service"
            and run["scenario"] == "intermittent-loss"
        )
    ]
    add(
        "true_drop_is_planned_not_verified",
        bool(true_loss)
        and all(
            run["mechanism"]["mechanism_id"] == "rate_error_model_true_drop"
            and run["mechanism"]["implementation_status"] == "PLANNED"
            for run in true_loss
        ),
        len(true_loss),
    )
    service = [
        run
        for run in scored_faults
        if run["fault_family"] == "service_degradation"
        or (
            run["fault_family"] == "intermittent_service"
            and run["scenario"] == "intermittent-service"
        )
    ]
    add(
        "service_fraction_is_planned_not_verified",
        bool(service)
        and all(
            run["mechanism"]["mechanism_id"] == "egress_service_fraction"
            and run["mechanism"]["implementation_status"] == "PLANNED"
            for run in service
        ),
        len(service),
    )
    add(
        "observability_not_fabricated",
        all(
            run["observable"] is None
            and run["fault_applied_ns"] is None
            and run["first_observable_effect_ns"] is None
            for run in runs
        ),
        "all PREPARED observations are null",
    )

    failed = [check for check in checks if check["status"] == FAIL]
    return {
        "schema_version": CHECK_SCHEMA,
        "corpus_manifest": str(corpus_path),
        "status": FAIL if failed else PASS,
        "summary": {"checks": len(checks), "failed": len(failed)},
        "checks": checks,
    }


def parse_holdout_gpus(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return None
    try:
        parsed = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise CorpusError(f"invalid --paired-holdout-gpus={value!r}") from error
    if not parsed:
        raise CorpusError("--paired-holdout-gpus cannot be empty")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path, metavar="CORPUS_MANIFEST")
    parser.add_argument("--link-map", type=Path)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--topology", type=Path)
    parser.add_argument("--workload", type=Path)
    parser.add_argument("--simulator-config", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--seed", type=int, default=20260827)
    parser.add_argument(
        "--paired-holdout-gpus",
        help="comma-separated explicit GPU ids; default is deterministic from seed",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.check is not None:
            creation_values = [
                args.link_map,
                args.contract,
                args.topology,
                args.workload,
                args.simulator_config,
                args.out_dir,
                args.paired_holdout_gpus,
            ]
            if any(value is not None for value in creation_values):
                parser.error("--check cannot be combined with creation inputs")
            report = verify_corpus(args.check)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["status"] == PASS else 1

        required = {
            "--link-map": args.link_map,
            "--contract": args.contract,
            "--topology": args.topology,
            "--workload": args.workload,
            "--simulator-config": args.simulator_config,
            "--out-dir": args.out_dir,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            parser.error(f"creation requires {', '.join(missing)}")
        corpus = generate_corpus(
            link_map_path=args.link_map,
            contract_path=args.contract,
            topology_path=args.topology,
            workload_path=args.workload,
            simulator_config_path=args.simulator_config,
            out_dir=args.out_dir,
            seed=args.seed,
            holdout_gpu_ids=parse_holdout_gpus(args.paired_holdout_gpus),
        )
        print(
            json.dumps(
                {
                    "status": corpus["status"],
                    "corpus_id": corpus["corpus_id"],
                    "runs": len(corpus["runs"]),
                    "paired_holdout_gpu_ids": corpus["split_manifest"][
                        "paired_holdout_gpu_ids"
                    ],
                    "stage_gate": "BLOCKED_PENDING_EXECUTION",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        CorpusError,
        OSError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
        yaml.YAMLError,
    ) as error:
        print(f"generate_true16_p2_corpus: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
