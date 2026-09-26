#!/usr/bin/env python3
"""Validate live background-RDMA ECMP choices against installed routes.

``background_route_choices.csv`` is emitted inside SwitchNode/NVSwitchNode's
real ``GetOutDev`` path.  This validator independently recomputes the packet
hash, binds the selected bucket to the simulator's installed candidate vector,
and binds the egress to a physical link.  Scenario names never substitute for
real ECN marks or PFC frames; those mechanisms have separate telemetry.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


SCHEMA_VERSION = "limer.p2-background-route-choice-evidence.v1"
PASS = "PASS"
FAIL = "FAIL"
CHOICE_STATUS = "OBSERVED_ACTUAL_ROUTE_SELECTION"
UINT = re.compile(r"(?:0|[1-9][0-9]*)\Z")
IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
UINT16_MAX = (1 << 16) - 1
UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1
BACKGROUND_SPORT_MIN = 49152
BACKGROUND_SPORT_MAX = 65535
SCENARIOS = frozenset(
    {"incast", "queue_buildup", "ecmp_collision", "ecn_pressure", "pfc_pressure"}
)
FABRIC_TYPES = frozenset({"SWITCH", "NVSWITCH"})

SCHEDULE_COLUMNS = (
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
CHOICE_COLUMNS = (
    "run_id",
    "event_id",
    "flow_id",
    "scenario",
    "direction",
    "packet_type",
    "timestamp_ns",
    "flow_src_rank",
    "flow_dst_rank",
    "flow_sport",
    "flow_dport",
    "node_id",
    "node_type",
    "packet_sip",
    "packet_dip",
    "packet_sport",
    "packet_dport",
    "pg",
    "l3_protocol",
    "ecmp_seed",
    "ecmp_hash",
    "candidate_count",
    "bucket",
    "egress_port_id",
    "next_hop_node_id",
    "link_id",
    "status",
)
ROUTE_COLUMNS = (
    "run_id",
    "node_id",
    "node_type",
    "destination_node_id",
    "destination_ip",
    "candidate_index",
    "candidate_count",
    "egress_port_id",
    "next_hop_node_id",
    "status",
)
LINK_COLUMNS = (
    "link_id",
    "src_node",
    "dst_node",
    "src_type",
    "dst_type",
    "src_port",
    "dst_port",
    "link_class",
    "bandwidth_bps",
    "delay_ns",
)


class EvidenceError(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class Artifact:
    label: str
    path: str
    sha256: str
    size_bytes: int


CANONICAL_ARTIFACT_PATHS = {
    "background_schedule": "inputs/background_flow_schedule.csv",
    "background_route_choices": "background_route_choices.csv",
    "ecmp_route_candidates": "ecmp_route_candidates.csv",
    "link_map": "link_map.csv",
}


@dataclass(frozen=True)
class Flow:
    event_id: str
    flow_id: str
    scenario: str
    start_ns: int
    src: int
    dst: int
    size: int
    pg: int
    sport: int
    dport: int


@dataclass(frozen=True)
class Candidate:
    node: int
    node_type: str
    destination: int
    index: int
    count: int
    egress: int
    next_hop: int


@dataclass(frozen=True)
class Endpoint:
    local_type: str
    local_port: int
    peer: int
    peer_type: str
    link_id: str


@dataclass(frozen=True)
class Choice:
    flow_id: str
    scenario: str
    direction: str
    packet_type: str
    timestamp_ns: int
    node: int
    node_type: str
    packet_sip: int
    packet_dip: int
    packet_sport: int
    packet_dport: int
    pg: int
    protocol: int
    seed: int
    hash_value: int
    candidate_count: int
    bucket: int
    egress: int
    next_hop: int
    link_id: str


def _read(path: Path, label: str) -> Tuple[Artifact, bytes]:
    if path.is_symlink():
        raise EvidenceError("ARTIFACT_SYMLINK", f"{label} must not be a symlink")
    try:
        data = path.read_bytes()
    except OSError as error:
        raise EvidenceError("ARTIFACT_READ", f"cannot read {label}: {error}") from error
    if not data:
        raise EvidenceError("ARTIFACT_EMPTY", f"{label} is empty")
    return (
        Artifact(
            label,
            CANONICAL_ARTIFACT_PATHS[label],
            hashlib.sha256(data).hexdigest(),
            len(data),
        ),
        data,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()


def _finalize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    finalized = dict(report)
    finalized["report_sha256"] = canonical_hash(finalized)
    return finalized


def _rows(data: bytes, columns: Sequence[str], label: str) -> List[Dict[str, str]]:
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise EvidenceError("CSV_ENCODING", f"{label} is not UTF-8") from error
    reader = csv.reader(text.splitlines(), strict=True)
    try:
        raw = list(reader)
    except csv.Error as error:
        raise EvidenceError("CSV_PARSE", f"malformed {label}: {error}") from error
    if not raw or tuple(raw[0]) != tuple(columns):
        raise EvidenceError(
            "CSV_SCHEMA", f"{label} header must exactly equal {','.join(columns)}"
        )
    result: List[Dict[str, str]] = []
    for line, row in enumerate(raw[1:], 2):
        if len(row) != len(columns):
            raise EvidenceError(
                "CSV_FIELD_COUNT",
                f"{label} line {line} has {len(row)} fields, expected {len(columns)}",
            )
        result.append(dict(zip(columns, row)))
    if not result:
        raise EvidenceError("CSV_NO_ROWS", f"{label} has no data rows")
    return result


def _u(text: str, field: str, maximum: int = UINT64_MAX) -> int:
    if not UINT.fullmatch(text):
        raise EvidenceError("INVALID_INTEGER", f"{field} is not canonical unsigned")
    value = int(text)
    if value > maximum:
        raise EvidenceError("INTEGER_RANGE", f"{field} exceeds {maximum}")
    return value


def simai_ip(node: int) -> int:
    """Exact numeric address returned by common.h::node_id_to_ip."""

    return 0x0B000001 + (node // 256) * 0x00010000 + (node % 256) * 0x00000100


def ecmp_hash(sip: int, dip: int, sport: int, dport: int, seed: int) -> int:
    """Exact EcmpHash over the x86 little-endian union used by GetOutDev."""

    key = struct.pack("<IIHH", sip, dip, sport, dport)
    value = seed & UINT32_MAX
    offset = 0
    remaining = len(key)
    while remaining > 3:
        word = int.from_bytes(key[offset : offset + 4], "little")
        word = (word * 0xCC9E2D51) & UINT32_MAX
        word = ((word << 15) | (word >> 17)) & UINT32_MAX
        word = (word * 0x1B873593) & UINT32_MAX
        value ^= word
        value = ((value << 13) | (value >> 19)) & UINT32_MAX
        value = (value + ((value << 2) & UINT32_MAX) + 0xE6546B64) & UINT32_MAX
        offset += 4
        remaining -= 4
    if remaining:
        word = int.from_bytes(key[offset : offset + remaining], "little")
        word = (word * 0xCC9E2D51) & UINT32_MAX
        word = ((word << 15) | (word >> 17)) & UINT32_MAX
        word = (word * 0x1B873593) & UINT32_MAX
        value ^= word
    value ^= len(key)
    value ^= value >> 16
    value = (value * 0x85EBCA6B) & UINT32_MAX
    value ^= value >> 13
    value = (value * 0xC2B2AE35) & UINT32_MAX
    value ^= value >> 16
    return value & UINT32_MAX


def _parse_schedule(rows: Iterable[Mapping[str, str]], gpus_per_server: int) -> Dict[str, Flow]:
    flows: Dict[str, Flow] = {}
    qp_keys = set()
    for number, row in enumerate(rows, 2):
        scenario = row["scenario"]
        if scenario not in SCENARIOS:
            raise EvidenceError("SCENARIO", f"schedule line {number} has unsupported scenario")
        flow = Flow(
            row["event_id"],
            row["flow_id"],
            scenario,
            _u(row["scheduled_start_ns"], "scheduled_start_ns"),
            _u(row["src_rank"], "src_rank", UINT32_MAX),
            _u(row["dst_rank"], "dst_rank", UINT32_MAX),
            _u(row["bytes"], "bytes"),
            _u(row["pg"], "pg", UINT16_MAX),
            _u(row["sport"], "sport", UINT16_MAX),
            _u(row["dport"], "dport", UINT16_MAX),
        )
        if not IDENTIFIER.fullmatch(flow.event_id):
            raise EvidenceError("EVENT_ID", "schedule event_id is empty or invalid")
        if not IDENTIFIER.fullmatch(flow.flow_id) or flow.flow_id in flows:
            raise EvidenceError("FLOW_ID", "schedule flow_id is invalid or duplicated")
        if flow.src == flow.dst or flow.src // gpus_per_server == flow.dst // gpus_per_server:
            raise EvidenceError("CROSS_SERVER", f"flow {flow.flow_id} is not cross-server")
        if flow.size == 0 or flow.pg == 0 or flow.pg > 7 or flow.dport == 0:
            raise EvidenceError("FLOW_FIELDS", f"flow {flow.flow_id} has invalid RDMA fields")
        if not BACKGROUND_SPORT_MIN <= flow.sport <= BACKGROUND_SPORT_MAX:
            raise EvidenceError("SOURCE_PORT", f"flow {flow.flow_id} is outside background ports")
        qp_key = (flow.src, flow.dst, flow.sport, flow.pg)
        if qp_key in qp_keys:
            raise EvidenceError("QP_IDENTITY", "schedule has a duplicate background QP key")
        qp_keys.add(qp_key)
        flows[flow.flow_id] = flow
    return flows


def _parse_candidates(
    rows: Iterable[Mapping[str, str]], expected_run_id: str
) -> Dict[Tuple[int, int], Tuple[Candidate, ...]]:
    grouped: Dict[Tuple[int, int], List[Candidate]] = defaultdict(list)
    for number, row in enumerate(rows, 2):
        if row["run_id"] != expected_run_id or row["status"] != "INSTALLED":
            raise EvidenceError("ROUTE_BINDING", f"route line {number} has wrong run/status")
        node_type = row["node_type"]
        if node_type not in {"HOST", "SWITCH", "NVSWITCH"}:
            raise EvidenceError("NODE_TYPE", f"route line {number} has invalid node type")
        candidate = Candidate(
            _u(row["node_id"], "node_id", UINT32_MAX),
            node_type,
            _u(row["destination_node_id"], "destination_node_id", UINT32_MAX),
            _u(row["candidate_index"], "candidate_index", UINT32_MAX),
            _u(row["candidate_count"], "candidate_count", UINT32_MAX),
            _u(row["egress_port_id"], "egress_port_id", UINT32_MAX),
            _u(row["next_hop_node_id"], "next_hop_node_id", UINT32_MAX),
        )
        grouped[(candidate.node, candidate.destination)].append(candidate)
    result: Dict[Tuple[int, int], Tuple[Candidate, ...]] = {}
    for key, values in grouped.items():
        values.sort(key=lambda item: item.index)
        count = len(values)
        if count == 0 or any(item.count != count for item in values):
            raise EvidenceError("CANDIDATE_COUNT", f"route group {key} count is inconsistent")
        if [item.index for item in values] != list(range(count)):
            raise EvidenceError("CANDIDATE_ORDER", f"route group {key} indexes are not contiguous")
        if len({(item.egress, item.next_hop) for item in values}) != count:
            raise EvidenceError("CANDIDATE_DUPLICATE", f"route group {key} duplicates a candidate")
        result[key] = tuple(values)
    return result


def _parse_links(rows: Iterable[Mapping[str, str]]) -> Dict[Tuple[int, int], Endpoint]:
    endpoints: Dict[Tuple[int, int], Endpoint] = {}
    link_ids = set()
    for number, row in enumerate(rows, 2):
        link_id = row["link_id"]
        src = _u(row["src_node"], "src_node", UINT32_MAX)
        dst = _u(row["dst_node"], "dst_node", UINT32_MAX)
        src_port = _u(row["src_port"], "src_port", UINT32_MAX)
        dst_port = _u(row["dst_port"], "dst_port", UINT32_MAX)
        src_type, dst_type = row["src_type"], row["dst_type"]
        if src_type not in {"HOST", "SWITCH", "NVSWITCH"} or dst_type not in {
            "HOST",
            "SWITCH",
            "NVSWITCH",
        }:
            raise EvidenceError("NODE_TYPE", f"link line {number} has invalid endpoint type")
        canonical = f"L{min(src, dst)}-{max(src, dst)}"
        if src == dst or link_id != canonical or link_id in link_ids:
            raise EvidenceError("LINK_ID", f"link line {number} is not canonical/unique")
        link_ids.add(link_id)
        for key, endpoint in (
            ((src, src_port), Endpoint(src_type, src_port, dst, dst_type, link_id)),
            ((dst, dst_port), Endpoint(dst_type, dst_port, src, src_type, link_id)),
        ):
            if key in endpoints:
                raise EvidenceError("LINK_ENDPOINT", f"duplicate physical endpoint {key}")
            endpoints[key] = endpoint
    return endpoints


def _expected_wire(flow: Flow, packet_type: str) -> Tuple[str, int, int, int, int, int]:
    if packet_type == "DATA":
        return "FORWARD", 0x11, simai_ip(flow.src), simai_ip(flow.dst), flow.sport, flow.dport
    if packet_type == "ACK":
        return "REVERSE", 0xFC, simai_ip(flow.dst), simai_ip(flow.src), flow.dport, flow.sport
    if packet_type == "NACK":
        return "REVERSE", 0xFD, simai_ip(flow.dst), simai_ip(flow.src), flow.dport, flow.sport
    raise EvidenceError("PACKET_TYPE", f"unsupported packet_type {packet_type!r}")


def _validate_single_chain(
    key: Tuple[str, str],
    packet_choices: Sequence[Choice],
    flow: Flow,
    endpoints: Mapping[Tuple[int, int], Endpoint],
) -> None:
    """Prove that one packet direction is exactly one source-to-target chain.

    Timestamps are intentionally absent from this proof: MTP callbacks from
    different LPs need not arrive in strict hop order.  Physical adjacency and
    the actual selected egress edges are sufficient.
    """

    by_node: Dict[int, Choice] = {}
    for choice in packet_choices:
        if choice.node in by_node:
            raise EvidenceError(
                "CHAIN_BRANCH",
                f"{key} has more than one actual egress choice at node {choice.node}",
            )
        by_node[choice.node] = choice

    referenced_fabric_nodes = set()
    for choice in packet_choices:
        endpoint = endpoints[(choice.node, choice.egress)]
        if endpoint.peer_type in FABRIC_TYPES:
            referenced_fabric_nodes.add(endpoint.peer)
    roots = set(by_node) - referenced_fabric_nodes
    if len(roots) != 1:
        raise EvidenceError(
            "CHAIN_ROOT",
            f"{key} has {len(roots)} roots; exactly one is required",
        )
    root = next(iter(roots))

    source_host = flow.src if key[1] == "DATA" else flow.dst
    target_host = flow.dst if key[1] == "DATA" else flow.src
    source_neighbors = {
        endpoint.peer
        for (node, _), endpoint in endpoints.items()
        if node == source_host
        and endpoint.local_type == "HOST"
        and endpoint.peer_type in FABRIC_TYPES
    }
    if root not in source_neighbors:
        raise EvidenceError(
            "CHAIN_SOURCE_ADJACENCY",
            f"{key} root {root} is not physically adjacent to source HOST {source_host}",
        )

    visited = set()
    current = root
    terminal_count = 0
    while True:
        if current in visited:
            raise EvidenceError("CHAIN_CYCLE", f"{key} contains a forwarding cycle")
        choice = by_node.get(current)
        if choice is None:
            raise EvidenceError(
                "CHAIN_DISCONNECTED",
                f"{key} selects unrecorded fabric node {current}",
            )
        visited.add(current)
        endpoint = endpoints[(choice.node, choice.egress)]
        if endpoint.peer_type == "HOST":
            terminal_count += 1
            if endpoint.peer != target_host:
                raise EvidenceError(
                    "CHAIN_TERMINAL",
                    f"{key} terminates at HOST {endpoint.peer}, expected {target_host}",
                )
            break
        if endpoint.peer_type not in FABRIC_TYPES:
            raise EvidenceError(
                "CHAIN_TERMINAL", f"{key} reaches an unsupported node type"
            )
        current = endpoint.peer

    if terminal_count != 1:
        raise EvidenceError(
            "CHAIN_TERMINAL",
            f"{key} has {terminal_count} terminals; exactly one is required",
        )
    if visited != set(by_node):
        extras = sorted(set(by_node) - visited)
        raise EvidenceError(
            "CHAIN_DISCONNECTED",
            f"{key} has disconnected/unconsumed nodes {extras}",
        )


def _validate_choices(
    rows: Iterable[Mapping[str, str]],
    expected_run_id: str,
    flows: Mapping[str, Flow],
    routes: Mapping[Tuple[int, int], Tuple[Candidate, ...]],
    endpoints: Mapping[Tuple[int, int], Endpoint],
) -> Tuple[List[Choice], Dict[str, Any]]:
    choices: List[Choice] = []
    seen = set()
    for number, row in enumerate(rows, 2):
        if row["run_id"] != expected_run_id or row["status"] != CHOICE_STATUS:
            raise EvidenceError("CHOICE_BINDING", f"choice line {number} has wrong run/status")
        flow = flows.get(row["flow_id"])
        if flow is None:
            raise EvidenceError("UNKNOWN_FLOW", f"choice line {number} names an unknown flow")
        if row["event_id"] != flow.event_id or row["scenario"] != flow.scenario:
            raise EvidenceError("SCHEDULE_BINDING", f"choice line {number} differs from schedule labels")
        if (
            _u(row["flow_src_rank"], "flow_src_rank", UINT32_MAX) != flow.src
            or _u(row["flow_dst_rank"], "flow_dst_rank", UINT32_MAX) != flow.dst
            or _u(row["flow_sport"], "flow_sport", UINT16_MAX) != flow.sport
            or _u(row["flow_dport"], "flow_dport", UINT16_MAX) != flow.dport
        ):
            raise EvidenceError("FLOW_TUPLE", f"choice line {number} normalized tuple differs")
        expected = _expected_wire(flow, row["packet_type"])
        direction, protocol, sip, dip, sport, dport = expected
        choice = Choice(
            flow.flow_id,
            flow.scenario,
            row["direction"],
            row["packet_type"],
            _u(row["timestamp_ns"], "timestamp_ns"),
            _u(row["node_id"], "node_id", UINT32_MAX),
            row["node_type"],
            _u(row["packet_sip"], "packet_sip", UINT32_MAX),
            _u(row["packet_dip"], "packet_dip", UINT32_MAX),
            _u(row["packet_sport"], "packet_sport", UINT16_MAX),
            _u(row["packet_dport"], "packet_dport", UINT16_MAX),
            _u(row["pg"], "pg", UINT16_MAX),
            _u(row["l3_protocol"], "l3_protocol", 255),
            _u(row["ecmp_seed"], "ecmp_seed", UINT32_MAX),
            _u(row["ecmp_hash"], "ecmp_hash", UINT32_MAX),
            _u(row["candidate_count"], "candidate_count", UINT32_MAX),
            _u(row["bucket"], "bucket", UINT32_MAX),
            _u(row["egress_port_id"], "egress_port_id", UINT32_MAX),
            _u(row["next_hop_node_id"], "next_hop_node_id", UINT32_MAX),
            row["link_id"],
        )
        if choice.timestamp_ns < flow.start_ns:
            raise EvidenceError("TIMESTAMP", f"choice line {number} predates its schedule")
        if choice.node_type not in FABRIC_TYPES:
            raise EvidenceError("HOST_TRANSIT", f"choice line {number} was emitted by a HOST")
        # SwitchNode/NVSwitchNode currently seed the exact data-plane hash with
        # their node ID.  Binding this independently prevents a producer from
        # changing both the self-reported seed and hash while retaining a
        # superficially reproducible bucket.
        if choice.seed != choice.node:
            raise EvidenceError(
                "ECMP_SEED",
                f"choice line {number} seed {choice.seed} differs from node_id {choice.node}",
            )
        if (
            choice.direction != direction
            or choice.protocol != protocol
            or (choice.packet_sip, choice.packet_dip, choice.packet_sport, choice.packet_dport)
            != (sip, dip, sport, dport)
            or choice.pg != flow.pg
        ):
            raise EvidenceError("WIRE_TUPLE", f"choice line {number} has a wrong wire tuple")
        computed = ecmp_hash(sip, dip, sport, dport, choice.seed)
        if choice.hash_value != computed:
            raise EvidenceError("ECMP_HASH", f"choice line {number} hash is not reproducible")
        candidates = routes.get((choice.node, flow.dst if direction == "FORWARD" else flow.src))
        if candidates is None:
            raise EvidenceError("ROUTE_GROUP", f"choice line {number} has no installed route group")
        if choice.candidate_count != len(candidates) or choice.candidate_count == 0:
            raise EvidenceError("CANDIDATE_COUNT", f"choice line {number} candidate count differs")
        bucket = computed % choice.candidate_count
        if choice.bucket != bucket:
            raise EvidenceError("ECMP_BUCKET", f"choice line {number} bucket differs from hash")
        installed = candidates[bucket]
        if installed.node_type != choice.node_type or (
            installed.egress,
            installed.next_hop,
        ) != (choice.egress, choice.next_hop):
            raise EvidenceError("CANDIDATE_SELECTION", f"choice line {number} differs from installed order")
        endpoint = endpoints.get((choice.node, choice.egress))
        if endpoint is None or (
            endpoint.local_type,
            endpoint.peer,
            endpoint.link_id,
        ) != (choice.node_type, choice.next_hop, choice.link_id):
            raise EvidenceError("PHYSICAL_LINK", f"choice line {number} does not bind to its link")
        wire_destination = flow.dst if direction == "FORWARD" else flow.src
        if endpoint.peer_type == "HOST" and endpoint.peer != wire_destination:
            raise EvidenceError("HOST_TRANSIT", f"choice line {number} selects a transit HOST")
        identity = (
            choice.flow_id,
            choice.direction,
            choice.packet_type,
            choice.node,
            choice.packet_sip,
            choice.packet_dip,
            choice.packet_sport,
            choice.packet_dport,
            choice.seed,
            choice.hash_value,
            choice.candidate_count,
            choice.bucket,
            choice.egress,
        )
        if identity in seen:
            raise EvidenceError("DUPLICATE_CHOICE", f"choice line {number} duplicates a live decision")
        seen.add(identity)
        choices.append(choice)

    if {choice.flow_id for choice in choices} != set(flows):
        raise EvidenceError("FLOW_COVERAGE", "route sidecar does not cover exactly every scheduled flow")
    by_flow_type: Dict[Tuple[str, str], List[Choice]] = defaultdict(list)
    for choice in choices:
        by_flow_type[(choice.flow_id, choice.packet_type)].append(choice)
    for flow_id in flows:
        for packet_type in ("DATA", "ACK"):
            key = (flow_id, packet_type)
            if key not in by_flow_type:
                raise EvidenceError(
                    "DIRECTION_COVERAGE",
                    f"flow {flow_id} lacks {packet_type} route evidence",
                )
    for key, packet_choices in by_flow_type.items():
        _validate_single_chain(key, packet_choices, flows[key[0]], endpoints)

    collision_groups: Dict[Tuple[int, int, int, int, int], set[str]] = defaultdict(set)
    for choice in choices:
        if choice.scenario == "ecmp_collision" and choice.packet_type == "DATA" and choice.candidate_count > 1:
            flow = flows[choice.flow_id]
            collision_groups[
                (choice.node, flow.dst, choice.candidate_count, choice.bucket, choice.egress)
            ].add(choice.flow_id)
    collision_count = sum(1 for flow_ids in collision_groups.values() if len(flow_ids) >= 2)
    if any(flow.scenario == "ecmp_collision" for flow in flows.values()) and collision_count == 0:
        raise EvidenceError("ECMP_COLLISION_NOT_OBSERVED", "scenario has no shared live ECMP bucket")

    metrics = {
        "flow_count": len(flows),
        "choice_count": len(choices),
        "forward_choice_count": sum(item.direction == "FORWARD" for item in choices),
        "reverse_choice_count": sum(item.direction == "REVERSE" for item in choices),
        "ack_choice_count": sum(item.packet_type == "ACK" for item in choices),
        "nack_choice_count": sum(item.packet_type == "NACK" for item in choices),
        "switch_choice_count": sum(item.node_type == "SWITCH" for item in choices),
        "nvswitch_choice_count": sum(item.node_type == "NVSWITCH" for item in choices),
        "unique_link_count": len({item.link_id for item in choices}),
        "observed_collision_group_count": collision_count,
        "validated_chain_count": len(by_flow_type),
    }
    return choices, metrics


def validate(
    *,
    route_choices: Path,
    schedule: Path,
    route_candidates: Path,
    link_map: Path,
    expected_run_id: str,
    gpus_per_server: int,
) -> Dict[str, Any]:
    artifacts: List[Artifact] = []
    try:
        if not expected_run_id:
            raise EvidenceError("RUN_ID", "expected_run_id is empty")
        if gpus_per_server <= 0:
            raise EvidenceError("GPU_GEOMETRY", "gpus_per_server must be positive")
        loaded = []
        for path, label, columns in (
            (schedule, "background_schedule", SCHEDULE_COLUMNS),
            (route_choices, "background_route_choices", CHOICE_COLUMNS),
            (route_candidates, "ecmp_route_candidates", ROUTE_COLUMNS),
            (link_map, "link_map", LINK_COLUMNS),
        ):
            artifact, data = _read(path, label)
            artifacts.append(artifact)
            loaded.append(_rows(data, columns, label))
        schedule_rows, choice_rows, route_rows, link_rows = loaded
        flows = _parse_schedule(schedule_rows, gpus_per_server)
        routes = _parse_candidates(route_rows, expected_run_id)
        endpoints = _parse_links(link_rows)
        _, metrics = _validate_choices(
            choice_rows, expected_run_id, flows, routes, endpoints
        )
        return _finalize_report({
            "schema_version": SCHEMA_VERSION,
            "status": PASS,
            "expected_run_id": expected_run_id,
            "gpus_per_server": gpus_per_server,
            "artifacts": [artifact.__dict__ for artifact in artifacts],
            "metrics": metrics,
            "errors": [],
        })
    except EvidenceError as error:
        return _finalize_report({
            "schema_version": SCHEMA_VERSION,
            "status": FAIL,
            "expected_run_id": expected_run_id,
            "gpus_per_server": gpus_per_server,
            "artifacts": [artifact.__dict__ for artifact in artifacts],
            "metrics": {},
            "errors": [{"code": error.code, "detail": error.detail}],
        })


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route-choices", required=True, type=Path)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--route-candidates", required=True, type=Path)
    parser.add_argument("--link-map", required=True, type=Path)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--gpus-per-server", required=True, type=int)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = validate(
        route_choices=args.route_choices,
        schedule=args.schedule,
        route_candidates=args.route_candidates,
        link_map=args.link_map,
        expected_run_id=args.expected_run_id,
        gpus_per_server=args.gpus_per_server,
    )
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output is None:
        sys.stdout.write(rendered)
    else:
        args.output.write_text(rendered, encoding="utf-8")
    return 0 if report["status"] == PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
