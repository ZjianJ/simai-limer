#!/usr/bin/env python3
"""Fail-closed validation for SimAI's installed ECMP route evidence.

The simulator emits ``ecmp_route_candidates.csv`` from the same loop that
calls ``AddTableEntry``.  This module binds that sidecar to both the frozen and
runtime physical link maps and independently reconstructs the routing table
created by ``CalculateRoute``/``SetRoutingEntries`` in ``common.h``.

No claim about an ECMP collision workload is made here.  A PASS only means
that every installed candidate has an unambiguous physical-link identity and
that the complete routing sidecar matches the topology-derived install set.
"""

from __future__ import annotations

import argparse
import csv
import decimal
import hashlib
import io
import ipaddress
import json
import os
import re
import stat
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple


SCHEMA_VERSION = "limer.p2-ecmp-route-candidate-evidence.v2"
PASS = "PASS"
FAIL = "FAIL"
MAX_INPUT_BYTES = 512 * 1024 * 1024
UINT32_MAX = (1 << 32) - 1
UINT16_MAX = (1 << 16) - 1

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
LINK_MAP_COLUMNS = (
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
NODE_TYPES = frozenset({"HOST", "SWITCH", "NVSWITCH"})
FABRIC_TYPES = frozenset({"SWITCH", "NVSWITCH"})
LINK_CLASSES = frozenset({"ACCESS", "INTRA_NODE", "INTER_SWITCH"})
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
UINT_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)\Z")


class RouteEvidenceError(ValueError):
    """One input violates the immutable route-evidence contract."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class BoundArtifact:
    """Bytes read from one regular, non-symlink file descriptor."""

    label: str
    data: bytes
    sha256: str
    size_bytes: int
    device: int
    inode: int

    def report(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class Edge:
    neighbor: int
    local_port: int
    remote_port: int
    link_id: str


@dataclass(frozen=True)
class RouteRow:
    run_id: str
    node_id: int
    node_type: str
    destination_node_id: int
    destination_ip: str
    candidate_index: int
    candidate_count: int
    egress_port_id: int
    next_hop_node_id: int
    status: str

    def sort_key(self) -> Tuple[int, int, int, int, int]:
        return (
            self.node_id,
            self.destination_node_id,
            self.candidate_index,
            self.egress_port_id,
            self.next_hop_node_id,
        )

    def group_key(self) -> Tuple[int, int]:
        return self.node_id, self.destination_node_id

    def candidate_key(self) -> Tuple[int, int]:
        return self.egress_port_id, self.next_hop_node_id

    def identity(self) -> Tuple[Any, ...]:
        return (
            self.run_id,
            self.node_id,
            self.node_type,
            self.destination_node_id,
            self.destination_ip,
            self.candidate_index,
            self.candidate_count,
            self.egress_port_id,
            self.next_hop_node_id,
            self.status,
        )


@dataclass(frozen=True)
class Topology:
    rows: Tuple[Mapping[str, Any], ...]
    node_types: Mapping[int, str]
    adjacency: Mapping[int, Tuple[Edge, ...]]
    endpoint_links: Mapping[Tuple[int, int], Tuple[int, str]]
    link_ids: frozenset[str]
    destination_ips: Mapping[int, str]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_expected_hash(value: str | None, label: str) -> None:
    if value is not None and not SHA256_PATTERN.fullmatch(value):
        raise RouteEvidenceError(
            "INVALID_EXPECTED_HASH",
            f"{label} expected SHA-256 must be 64 lowercase hexadecimal digits",
        )


def _reject_symlink_chain(path: Path, label: str) -> Path:
    """Reject a symlink in any existing component without resolving it."""

    absolute = path.absolute()
    components = [absolute, *absolute.parents]
    for component in components:
        try:
            metadata = os.lstat(component)
        except FileNotFoundError:
            continue
        except OSError as error:
            raise RouteEvidenceError(
                "ARTIFACT_STAT_ERROR", f"{label} cannot be inspected: {error}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode):
            raise RouteEvidenceError(
                "ARTIFACT_SYMLINK", f"{label} traverses a symbolic link"
            )
    return absolute


def _read_bound_artifact(
    path: Path,
    label: str,
    expected_sha256: str | None,
    *,
    max_bytes: int = MAX_INPUT_BYTES,
) -> BoundArtifact:
    """Read one immutable snapshot through a no-follow file descriptor."""

    _validate_expected_hash(expected_sha256, label)
    absolute = _reject_symlink_chain(path, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(absolute, flags)
    except FileNotFoundError as error:
        raise RouteEvidenceError("ARTIFACT_MISSING", f"{label} is missing") from error
    except OSError as error:
        raise RouteEvidenceError(
            "ARTIFACT_OPEN_ERROR", f"{label} cannot be opened: {error}"
        ) from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RouteEvidenceError(
                "ARTIFACT_NOT_REGULAR", f"{label} is not a regular file"
            )
        if before.st_size <= 0:
            raise RouteEvidenceError("ARTIFACT_EMPTY", f"{label} is empty")
        if before.st_size > max_bytes:
            raise RouteEvidenceError(
                "ARTIFACT_TOO_LARGE",
                f"{label} exceeds the {max_bytes}-byte validation bound",
            )
        chunks: List[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or len(data) != before.st_size:
        raise RouteEvidenceError(
            "ARTIFACT_CHANGED_DURING_READ", f"{label} changed while being read"
        )
    try:
        named = os.lstat(absolute)
    except OSError as error:
        raise RouteEvidenceError(
            "ARTIFACT_CHANGED_DURING_READ", f"{label} disappeared after reading"
        ) from error
    if (
        stat.S_ISLNK(named.st_mode)
        or not stat.S_ISREG(named.st_mode)
        or named.st_dev != after.st_dev
        or named.st_ino != after.st_ino
    ):
        raise RouteEvidenceError(
            "ARTIFACT_CHANGED_DURING_READ",
            f"{label} path no longer names the validated file",
        )
    actual_sha256 = sha256_bytes(data)
    if expected_sha256 is not None and actual_sha256 != expected_sha256:
        raise RouteEvidenceError(
            "ARTIFACT_HASH_MISMATCH",
            f"{label} SHA-256 differs from its immutable binding",
        )
    return BoundArtifact(
        label, data, actual_sha256, len(data), after.st_dev, after.st_ino
    )


def _csv_records(data: bytes, columns: Sequence[str], label: str) -> List[List[str]]:
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise RouteEvidenceError(
            "CSV_ENCODING", f"{label} is not strict UTF-8"
        ) from error
    if "\x00" in text:
        raise RouteEvidenceError("CSV_ENCODING", f"{label} contains a NUL byte")
    try:
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except csv.Error as error:
        raise RouteEvidenceError("CSV_PARSE", f"{label} is malformed: {error}") from error
    if not rows:
        raise RouteEvidenceError("CSV_EMPTY", f"{label} has no header")
    if tuple(rows[0]) != tuple(columns):
        raise RouteEvidenceError(
            "CSV_SCHEMA",
            f"{label} header must exactly equal {','.join(columns)}",
        )
    records: List[List[str]] = []
    for line_number, row in enumerate(rows[1:], start=2):
        if len(row) != len(columns):
            raise RouteEvidenceError(
                "CSV_FIELD_COUNT",
                f"{label} line {line_number} has {len(row)} fields, expected {len(columns)}",
            )
        if all(value == "" for value in row):
            raise RouteEvidenceError(
                "CSV_BLANK_ROW", f"{label} line {line_number} is blank"
            )
        records.append(row)
    if not records:
        raise RouteEvidenceError("CSV_NO_ROWS", f"{label} has no data rows")
    return records


def _uint(value: str, label: str, *, positive: bool = False, maximum: int = UINT32_MAX) -> int:
    if not UINT_PATTERN.fullmatch(value):
        raise RouteEvidenceError(
            "NONCANONICAL_INTEGER", f"{label} must be canonical unsigned decimal"
        )
    parsed = int(value)
    if parsed > maximum or (positive and parsed == 0):
        qualifier = "positive " if positive else ""
        raise RouteEvidenceError(
            "INTEGER_RANGE", f"{label} is outside the {qualifier}unsigned range"
        )
    return parsed


def _parse_link_map(artifact: BoundArtifact) -> Topology:
    records = _csv_records(artifact.data, LINK_MAP_COLUMNS, artifact.label)
    rows: List[Dict[str, Any]] = []
    node_types: Dict[int, str] = {}
    adjacency: MutableMapping[int, List[Edge]] = defaultdict(list)
    endpoint_links: Dict[Tuple[int, int], Tuple[int, str]] = {}
    neighbor_pairs: set[Tuple[int, int]] = set()
    link_ids: set[str] = set()
    for offset, values in enumerate(records, start=2):
        raw = dict(zip(LINK_MAP_COLUMNS, values))
        link_id = raw["link_id"]
        if not link_id or "," in link_id or "\n" in link_id or "\r" in link_id:
            raise RouteEvidenceError(
                "LINK_ID_INVALID", f"{artifact.label} line {offset} has invalid link_id"
            )
        if link_id in link_ids:
            raise RouteEvidenceError(
                "LINK_ID_DUPLICATE", f"{artifact.label} repeats link_id {link_id!r}"
            )
        link_ids.add(link_id)
        src = _uint(raw["src_node"], f"{artifact.label} line {offset} src_node", maximum=UINT16_MAX)
        dst = _uint(raw["dst_node"], f"{artifact.label} line {offset} dst_node", maximum=UINT16_MAX)
        src_port = _uint(raw["src_port"], f"{artifact.label} line {offset} src_port", positive=True)
        dst_port = _uint(raw["dst_port"], f"{artifact.label} line {offset} dst_port", positive=True)
        bandwidth = _uint(
            raw["bandwidth_bps"],
            f"{artifact.label} line {offset} bandwidth_bps",
            positive=True,
            maximum=(1 << 64) - 1,
        )
        delay = _uint(
            raw["delay_ns"],
            f"{artifact.label} line {offset} delay_ns",
            maximum=(1 << 64) - 1,
        )
        src_type, dst_type = raw["src_type"], raw["dst_type"]
        if src_type not in NODE_TYPES or dst_type not in NODE_TYPES:
            raise RouteEvidenceError(
                "NODE_TYPE_INVALID", f"{artifact.label} line {offset} has unknown node type"
            )
        if raw["link_class"] not in LINK_CLASSES:
            raise RouteEvidenceError(
                "LINK_CLASS_INVALID", f"{artifact.label} line {offset} has unknown link class"
            )
        if src == dst:
            raise RouteEvidenceError(
                "SELF_LINK", f"{artifact.label} line {offset} is a self-link"
            )
        for node, node_type in ((src, src_type), (dst, dst_type)):
            previous = node_types.setdefault(node, node_type)
            if previous != node_type:
                raise RouteEvidenceError(
                    "NODE_TYPE_INCONSISTENT",
                    f"{artifact.label} assigns multiple types to node {node}",
                )
        pair = tuple(sorted((src, dst)))
        if pair in neighbor_pairs:
            raise RouteEvidenceError(
                "PARALLEL_NEIGHBOR_LINK",
                f"{artifact.label} has multiple links between nodes {pair[0]} and {pair[1]}",
            )
        neighbor_pairs.add(pair)
        for node, port, neighbor in (
            (src, src_port, dst),
            (dst, dst_port, src),
        ):
            endpoint = (node, port)
            if endpoint in endpoint_links:
                raise RouteEvidenceError(
                    "ENDPOINT_REUSED",
                    f"{artifact.label} reuses node {node} port {port}",
                )
            endpoint_links[endpoint] = (neighbor, link_id)
        adjacency[src].append(Edge(dst, src_port, dst_port, link_id))
        adjacency[dst].append(Edge(src, dst_port, src_port, link_id))
        rows.append({
            "link_id": link_id,
            "src_node": src,
            "dst_node": dst,
            "src_type": src_type,
            "dst_type": dst_type,
            "src_port": src_port,
            "dst_port": dst_port,
            "link_class": raw["link_class"],
            "bandwidth_bps": bandwidth,
            "delay_ns": delay,
        })
    hosts = {node for node, node_type in node_types.items() if node_type == "HOST"}
    fabrics = {node for node, node_type in node_types.items() if node_type in FABRIC_TYPES}
    if not hosts or not fabrics:
        raise RouteEvidenceError(
            "TOPOLOGY_NODE_COVERAGE", f"{artifact.label} needs HOST and fabric nodes"
        )
    start = min(node_types)
    visited = {start}
    pending = deque([start])
    while pending:
        node = pending.popleft()
        for edge in adjacency[node]:
            if edge.neighbor not in visited:
                visited.add(edge.neighbor)
                pending.append(edge.neighbor)
    if visited != set(node_types):
        missing = sorted(set(node_types) - visited)
        raise RouteEvidenceError(
            "TOPOLOGY_DISCONNECTED",
            f"{artifact.label} has disconnected nodes {missing}",
        )
    ordered_adjacency = {
        node: tuple(sorted(edges, key=lambda edge: (edge.local_port, edge.neighbor)))
        for node, edges in sorted(adjacency.items())
    }
    return Topology(
        rows=tuple(rows),
        node_types=dict(sorted(node_types.items())),
        adjacency=ordered_adjacency,
        endpoint_links=dict(sorted(endpoint_links.items())),
        link_ids=frozenset(link_ids),
        destination_ips={},
    )


def _scaled_decimal(
    value: str,
    units: Mapping[str, int],
    label: str,
) -> int:
    match = re.fullmatch(r"(0|[1-9][0-9]*)(?:\.([0-9]+))?([A-Za-z]+)", value)
    if match is None or match.group(3) not in units:
        raise RouteEvidenceError("TOPOLOGY_QUANTITY", f"{label} has invalid units")
    try:
        number = decimal.Decimal(
            match.group(1) + ("." + match.group(2) if match.group(2) else "")
        )
        scaled = number * units[match.group(3)]
    except decimal.InvalidOperation as error:
        raise RouteEvidenceError("TOPOLOGY_QUANTITY", f"{label} is invalid") from error
    if scaled != scaled.to_integral_value() or scaled < 0:
        raise RouteEvidenceError(
            "TOPOLOGY_QUANTITY", f"{label} is not an integral base-unit value"
        )
    return int(scaled)


def _bind_topology_file(link_topology: Topology, artifact: BoundArtifact) -> Topology:
    """Bind physical topology order, including IPv4AddressHelper allocation."""

    try:
        text = artifact.data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise RouteEvidenceError("TOPOLOGY_ENCODING", "topology is not UTF-8") from error
    if "\x00" in text:
        raise RouteEvidenceError("TOPOLOGY_ENCODING", "topology contains a NUL byte")
    tokens = text.split()
    if len(tokens) < 6:
        raise RouteEvidenceError("TOPOLOGY_SCHEMA", "topology header is incomplete")
    node_count = _uint(tokens[0], "topology node_count", positive=True)
    _uint(tokens[1], "topology gpus_per_server", positive=True)
    nvswitch_count = _uint(tokens[2], "topology nvswitch_count")
    switch_count = _uint(tokens[3], "topology switch_count", positive=True)
    link_count = _uint(tokens[4], "topology link_count", positive=True)
    gpu_type = tokens[5]
    if not re.fullmatch(r"[A-Za-z0-9._-]+", gpu_type):
        raise RouteEvidenceError("TOPOLOGY_GPU_TYPE", "topology GPU type is invalid")
    expected_tokens = 6 + nvswitch_count + switch_count + link_count * 5
    if len(tokens) != expected_tokens:
        raise RouteEvidenceError(
            "TOPOLOGY_SCHEMA",
            f"topology has {len(tokens)} tokens, expected exactly {expected_tokens}",
        )
    cursor = 6
    nvswitches = [
        _uint(tokens[cursor + index], "topology NVSwitch node", maximum=node_count - 1)
        for index in range(nvswitch_count)
    ]
    cursor += nvswitch_count
    switches = [
        _uint(tokens[cursor + index], "topology switch node", maximum=node_count - 1)
        for index in range(switch_count)
    ]
    cursor += switch_count
    fabric_ids = nvswitches + switches
    if len(fabric_ids) != len(set(fabric_ids)):
        raise RouteEvidenceError("TOPOLOGY_NODE_DUPLICATE", "topology repeats a fabric node")
    topology_node_types = {
        node: (
            "NVSWITCH" if node in set(nvswitches)
            else "SWITCH" if node in set(switches)
            else "HOST"
        )
        for node in range(node_count)
    }
    if topology_node_types != dict(link_topology.node_types):
        raise RouteEvidenceError(
            "TOPOLOGY_NODE_BINDING",
            "topology node inventory/types differ from frozen link_map",
        )
    link_rows = {
        (int(row["src_node"]), int(row["dst_node"])): row
        for row in link_topology.rows
    }
    observed_pairs: set[Tuple[int, int]] = set()
    destination_ips = {
        node: _node_id_ip(node)
        for node, node_type in topology_node_types.items()
        if node_type in {"HOST", "NVSWITCH"}
    }
    for link_index in range(link_count):
        src = _uint(tokens[cursor], f"topology link {link_index} src", maximum=node_count - 1)
        dst = _uint(tokens[cursor + 1], f"topology link {link_index} dst", maximum=node_count - 1)
        rate = _scaled_decimal(
            tokens[cursor + 2],
            {"bps": 1, "Kbps": 10**3, "Mbps": 10**6, "Gbps": 10**9, "Tbps": 10**12},
            f"topology link {link_index} rate",
        )
        delay = _scaled_decimal(
            tokens[cursor + 3],
            {"ns": 1, "us": 10**3, "ms": 10**6, "s": 10**9},
            f"topology link {link_index} delay",
        )
        try:
            error_rate = decimal.Decimal(tokens[cursor + 4])
        except decimal.InvalidOperation as error:
            raise RouteEvidenceError(
                "TOPOLOGY_ERROR_RATE", f"topology link {link_index} error rate is invalid"
            ) from error
        if not error_rate.is_finite() or not decimal.Decimal(0) <= error_rate <= decimal.Decimal(1):
            raise RouteEvidenceError(
                "TOPOLOGY_ERROR_RATE", f"topology link {link_index} error rate is out of range"
            )
        cursor += 5
        pair = (src, dst)
        if pair in observed_pairs:
            raise RouteEvidenceError("TOPOLOGY_LINK_DUPLICATE", f"topology repeats link {pair}")
        observed_pairs.add(pair)
        mapped = link_rows.get(pair)
        if mapped is None:
            reverse = link_rows.get((dst, src))
            detail = "orientation differs" if reverse is not None else "is absent"
            raise RouteEvidenceError(
                "TOPOLOGY_LINK_BINDING",
                f"topology link {pair} {detail} in frozen link_map",
            )
        if int(mapped["bandwidth_bps"]) != rate or int(mapped["delay_ns"]) != delay:
            raise RouteEvidenceError(
                "TOPOLOGY_LINK_PARAMETERS",
                f"topology link {pair} rate/delay differ from frozen link_map",
            )
        second_octet = link_index // 254 + 1
        third_octet = link_index % 254 + 1
        if second_octet > 255:
            raise RouteEvidenceError(
                "LINK_ADDRESS_RANGE", "topology exceeds SimAI's 10.x.y.0 allocator"
            )
        for node, host_octet in ((src, 1), (dst, 2)):
            if topology_node_types[node] == "SWITCH" and node not in destination_ips:
                destination_ips[node] = f"10.{second_octet}.{third_octet}.{host_octet}"
    if len(observed_pairs) != len(link_rows) or link_count != len(link_topology.rows):
        raise RouteEvidenceError(
            "TOPOLOGY_LINK_COVERAGE",
            "topology and frozen link_map do not contain the same physical links",
        )
    if set(destination_ips) != set(topology_node_types):
        raise RouteEvidenceError(
            "DESTINATION_IP_COVERAGE", "topology cannot derive every node destination IP"
        )
    return Topology(
        rows=link_topology.rows,
        node_types=link_topology.node_types,
        adjacency=link_topology.adjacency,
        endpoint_links=link_topology.endpoint_links,
        link_ids=link_topology.link_ids,
        destination_ips=dict(sorted(destination_ips.items())),
    )


def _node_id_ip(node_id: int) -> str:
    if not 0 <= node_id <= UINT16_MAX:
        raise RouteEvidenceError(
            "DESTINATION_NODE_RANGE", f"destination node {node_id} cannot have a unique SimAI IP"
        )
    encoded = 0x0B000001 + ((node_id // 256) * 0x00010000) + ((node_id % 256) * 0x00000100)
    return str(ipaddress.IPv4Address(encoded))


def _distances_from_host(topology: Topology, destination: int) -> Dict[int, int]:
    """Mirror CalculateRoute: expand the destination and fabric nodes only."""

    distances = {destination: 0}
    pending = deque([destination])
    while pending:
        current = pending.popleft()
        distance = distances[current]
        for edge in topology.adjacency[current]:
            neighbor = edge.neighbor
            if neighbor not in distances:
                distances[neighbor] = distance + 1
                if topology.node_types[neighbor] in FABRIC_TYPES:
                    pending.append(neighbor)
    return distances


def _preferred_predecessors(
    topology: Topology,
    source: int,
    destination: int,
    distances: Mapping[int, int],
) -> Tuple[Edge, ...]:
    source_distance = distances[source]
    candidates = tuple(
        edge
        for edge in topology.adjacency[source]
        if distances.get(edge.neighbor) == source_distance - 1
        # CalculateRoute queues only the root HOST and fabric nodes.  Another
        # HOST can receive a distance label but is never popped, so it cannot
        # become an installed predecessor for one of its neighbors.
        and (
            edge.neighbor == destination
            or topology.node_types[edge.neighbor] in FABRIC_TYPES
        )
    )
    nvswitch = tuple(
        edge
        for edge in candidates
        if topology.node_types[edge.neighbor] == "NVSWITCH"
    )
    # CalculateRoute removes non-NVSwitch candidates as soon as an equal-hop
    # NVSwitch predecessor is seen, independent of neighbor iteration order.
    selected = nvswitch or candidates
    return tuple(sorted(selected, key=lambda edge: (edge.local_port, edge.neighbor)))


def reconstruct_expected_rows(topology: Topology, run_id: str) -> Tuple[RouteRow, ...]:
    """Reconstruct the complete non-empty ``nextHop`` map installed by C++."""

    routes: Dict[Tuple[int, int], Tuple[Edge, ...]] = {}
    hosts = sorted(node for node, kind in topology.node_types.items() if kind == "HOST")
    for destination in hosts:
        distances = _distances_from_host(topology, destination)
        for source in sorted(distances):
            if source == destination:
                continue
            candidates = _preferred_predecessors(
                topology, source, destination, distances
            )
            if not candidates:
                raise RouteEvidenceError(
                    "ROUTE_RECONSTRUCTION_EMPTY",
                    f"no predecessor for node {source} toward host {destination}",
                )
            routes[(source, destination)] = candidates

        # CalculateRoute also installs direct host-to-neighbor routes whenever
        # that fabric neighbor is on a shortest path to this host destination.
        for source, source_type in sorted(topology.node_types.items()):
            if source_type != "HOST" or source == destination or source not in distances:
                continue
            for edge in topology.adjacency[source]:
                if (
                    topology.node_types[edge.neighbor] in FABRIC_TYPES
                    and distances.get(edge.neighbor) == distances[source] - 1
                ):
                    direct = Edge(
                        neighbor=edge.neighbor,
                        local_port=edge.local_port,
                        remote_port=edge.remote_port,
                        link_id=edge.link_id,
                    )
                    routes.setdefault((source, edge.neighbor), (direct,))

    expected: List[RouteRow] = []
    for (source, destination), candidates in sorted(routes.items()):
        count = len(candidates)
        for index, edge in enumerate(candidates):
            expected.append(RouteRow(
                run_id=run_id,
                node_id=source,
                node_type=topology.node_types[source],
                destination_node_id=destination,
                destination_ip=topology.destination_ips[destination],
                candidate_index=index,
                candidate_count=count,
                egress_port_id=edge.local_port,
                next_hop_node_id=edge.neighbor,
                status="INSTALLED",
            ))
    expected.sort(key=RouteRow.sort_key)
    return tuple(expected)


def _parse_route_rows(artifact: BoundArtifact, expected_run_id: str) -> Tuple[RouteRow, ...]:
    records = _csv_records(artifact.data, ROUTE_COLUMNS, artifact.label)
    parsed: List[RouteRow] = []
    for offset, values in enumerate(records, start=2):
        raw = dict(zip(ROUTE_COLUMNS, values))
        if raw["run_id"] != expected_run_id:
            raise RouteEvidenceError(
                "FOREIGN_RUN_ID",
                f"{artifact.label} line {offset} has run_id {raw['run_id']!r}",
            )
        node_type = raw["node_type"]
        if node_type not in NODE_TYPES:
            raise RouteEvidenceError(
                "ROUTE_NODE_TYPE", f"{artifact.label} line {offset} has invalid node_type"
            )
        if raw["status"] != "INSTALLED":
            raise RouteEvidenceError(
                "ROUTE_STATUS", f"{artifact.label} line {offset} is not INSTALLED"
            )
        destination_ip = raw["destination_ip"]
        try:
            parsed_ip = ipaddress.IPv4Address(destination_ip)
        except ipaddress.AddressValueError as error:
            raise RouteEvidenceError(
                "DESTINATION_IP", f"{artifact.label} line {offset} has invalid destination_ip"
            ) from error
        if str(parsed_ip) != destination_ip:
            raise RouteEvidenceError(
                "DESTINATION_IP",
                f"{artifact.label} line {offset} destination_ip is not canonical",
            )
        parsed.append(RouteRow(
            run_id=raw["run_id"],
            node_id=_uint(raw["node_id"], f"{artifact.label} line {offset} node_id", maximum=UINT16_MAX),
            node_type=node_type,
            destination_node_id=_uint(
                raw["destination_node_id"],
                f"{artifact.label} line {offset} destination_node_id",
                maximum=UINT16_MAX,
            ),
            destination_ip=destination_ip,
            candidate_index=_uint(
                raw["candidate_index"], f"{artifact.label} line {offset} candidate_index"
            ),
            candidate_count=_uint(
                raw["candidate_count"],
                f"{artifact.label} line {offset} candidate_count",
                positive=True,
            ),
            egress_port_id=_uint(
                raw["egress_port_id"],
                f"{artifact.label} line {offset} egress_port_id",
                positive=True,
            ),
            next_hop_node_id=_uint(
                raw["next_hop_node_id"],
                f"{artifact.label} line {offset} next_hop_node_id",
                maximum=UINT16_MAX,
            ),
            status=raw["status"],
        ))
    identities = [row.identity() for row in parsed]
    if len(identities) != len(set(identities)):
        raise RouteEvidenceError("ROUTE_DUPLICATE", f"{artifact.label} repeats a route row")
    keys = [row.sort_key() for row in parsed]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise RouteEvidenceError(
            "ROUTE_GLOBAL_ORDER",
            f"{artifact.label} rows are not strictly ordered by the C++ evidence key",
        )
    grouped: MutableMapping[Tuple[int, int], List[RouteRow]] = defaultdict(list)
    for row in parsed:
        grouped[row.group_key()].append(row)
    for group, rows in sorted(grouped.items()):
        counts = {row.candidate_count for row in rows}
        if counts != {len(rows)}:
            raise RouteEvidenceError(
                "CANDIDATE_COUNT",
                f"route group {group} declares candidate_count {sorted(counts)}, actual {len(rows)}",
            )
        if [row.candidate_index for row in rows] != list(range(len(rows))):
            raise RouteEvidenceError(
                "CANDIDATE_INDEX",
                f"route group {group} candidate_index is not contiguous from zero",
            )
        candidate_keys = [row.candidate_key() for row in rows]
        if candidate_keys != sorted(candidate_keys) or len(candidate_keys) != len(set(candidate_keys)):
            raise RouteEvidenceError(
                "CANDIDATE_ORDER",
                f"route group {group} violates (egress_port_id,next_hop_node_id) order",
            )
    return tuple(parsed)


def _candidate_mapping(
    rows: Iterable[RouteRow], topology: Topology
) -> Tuple[List[Dict[str, Any]], frozenset[str]]:
    mapped: List[Dict[str, Any]] = []
    used: set[str] = set()
    for row in rows:
        if row.node_id not in topology.node_types:
            raise RouteEvidenceError(
                "ROUTE_UNKNOWN_NODE", f"route source node {row.node_id} is absent from link_map"
            )
        if topology.node_types[row.node_id] != row.node_type:
            raise RouteEvidenceError(
                "ROUTE_NODE_TYPE_MISMATCH",
                f"route source node {row.node_id} type differs from link_map",
            )
        if row.destination_node_id not in topology.node_types:
            raise RouteEvidenceError(
                "ROUTE_UNKNOWN_DESTINATION",
                f"route destination node {row.destination_node_id} is absent from link_map",
            )
        expected_ip = topology.destination_ips[row.destination_node_id]
        if row.destination_ip != expected_ip:
            raise RouteEvidenceError(
                "DESTINATION_IP_MISMATCH",
                f"route destination {row.destination_node_id} IP differs from SimAI encoding",
            )
        endpoint = topology.endpoint_links.get((row.node_id, row.egress_port_id))
        if endpoint is None:
            raise RouteEvidenceError(
                "ROUTE_PORT_UNMAPPED",
                f"node {row.node_id} port {row.egress_port_id} is absent from link_map",
            )
        neighbor, link_id = endpoint
        if neighbor != row.next_hop_node_id:
            raise RouteEvidenceError(
                "ROUTE_NEXT_HOP_MISMATCH",
                f"node {row.node_id} port {row.egress_port_id} maps to node {neighbor}, not {row.next_hop_node_id}",
            )
        used.add(link_id)
        mapped.append({
            "candidate_index": row.candidate_index,
            "destination_node_id": row.destination_node_id,
            "egress_port_id": row.egress_port_id,
            "link_id": link_id,
            "next_hop_node_id": row.next_hop_node_id,
            "node_id": row.node_id,
        })
    mapped.sort(key=lambda value: (
        value["node_id"],
        value["destination_node_id"],
        value["candidate_index"],
        value["egress_port_id"],
        value["next_hop_node_id"],
    ))
    return mapped, frozenset(used)


def _compare_rows(actual: Sequence[RouteRow], expected: Sequence[RouteRow]) -> None:
    actual_identities = [row.identity() for row in actual]
    expected_identities = [row.identity() for row in expected]
    if actual_identities == expected_identities:
        return
    actual_set, expected_set = set(actual_identities), set(expected_identities)
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    detail = (
        f"installed route set differs from topology reconstruction: "
        f"expected_rows={len(expected)}, actual_rows={len(actual)}, "
        f"missing={len(missing)}, extra={len(extra)}"
    )
    if not missing and not extra:
        detail += ", row ordering differs"
    raise RouteEvidenceError("ROUTE_SET_MISMATCH", detail)


def _coverage_report(
    rows: Sequence[RouteRow],
    expected_rows: Sequence[RouteRow],
    topology: Topology,
) -> Dict[str, Any]:
    hosts = sorted(node for node, kind in topology.node_types.items() if kind == "HOST")
    switches = sorted(node for node, kind in topology.node_types.items() if kind == "SWITCH")
    nvswitches = sorted(node for node, kind in topology.node_types.items() if kind == "NVSWITCH")
    fabric = switches + nvswitches
    groups = {row.group_key() for row in rows}
    cartesian_fabric_groups = {
        (node, destination) for node in fabric for destination in hosts
    }
    # SimAI deliberately does not expand intermediate HOST nodes while
    # calculating routes.  A server-local NVSwitch therefore need not be
    # routable to hosts attached to another NVSwitch.  "Full" coverage means
    # every group reconstructable under that real forwarding rule, not an
    # impossible fabric x host Cartesian product.
    expected_fabric_groups = {
        row.group_key()
        for row in expected_rows
        if row.node_id in set(fabric) and row.destination_node_id in set(hosts)
    }
    observed_fabric_groups = {
        group for group in groups if group[0] in set(fabric) and group[1] in set(hosts)
    }
    if observed_fabric_groups != expected_fabric_groups:
        raise RouteEvidenceError(
            "FABRIC_ROUTE_COVERAGE",
            "not every reconstructable SWITCH/NVSWITCH-to-HOST route group is present",
        )
    fabric_nodes_observed = {source for source, _ in observed_fabric_groups}
    if fabric_nodes_observed != set(fabric):
        raise RouteEvidenceError(
            "FABRIC_NODE_COVERAGE",
            "one or more physical SWITCH/NVSWITCH nodes have no installed host route",
        )
    return {
        "host_count": len(hosts),
        "switch_count": len(switches),
        "nvswitch_count": len(nvswitches),
        "fabric_node_count": len(fabric),
        "route_group_count": len(groups),
        "fabric_to_host_group_count": len(observed_fabric_groups),
        "expected_fabric_to_host_group_count": len(expected_fabric_groups),
        "cartesian_fabric_to_host_group_count": len(cartesian_fabric_groups),
        "structurally_unreachable_fabric_to_host_group_count": len(
            cartesian_fabric_groups - expected_fabric_groups
        ),
        "all_physical_fabric_nodes_observed": True,
        "all_reconstructable_fabric_to_host_groups_present": True,
    }


def _finalize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    finalized = dict(report)
    finalized["report_sha256"] = canonical_hash(finalized)
    return finalized


def validate_route_candidate_evidence(
    *,
    route_candidates_path: Path,
    topology_path: Path,
    frozen_link_map_path: Path,
    runtime_link_map_path: Path,
    expected_run_id: str,
    expected_route_candidates_sha256: str | None = None,
    expected_topology_sha256: str | None = None,
    expected_frozen_link_map_sha256: str | None = None,
    expected_runtime_link_map_sha256: str | None = None,
) -> Dict[str, Any]:
    """Return a deterministic PASS/FAIL report without trusting file names."""

    contract = {
        "expected_run_id": expected_run_id,
        "route_columns": list(ROUTE_COLUMNS),
        "link_map_columns": list(LINK_MAP_COLUMNS),
        "expected_hashes": {
            "route_candidates": expected_route_candidates_sha256,
            "topology": expected_topology_sha256,
            "frozen_link_map": expected_frozen_link_map_sha256,
            "runtime_link_map": expected_runtime_link_map_sha256,
        },
        "candidate_order": ["egress_port_id", "next_hop_node_id"],
        "global_order": [
            "node_id", "destination_node_id", "candidate_index",
            "egress_port_id", "next_hop_node_id",
        ],
        "fabric_coverage": (
            "every SWITCH/NVSWITCH-to-HOST group reconstructable under "
            "SimAI's non-HOST-transit routing rule"
        ),
        "destination_ip_contract": {
            "HOST_NVSWITCH": "node_id_to_ip(11.*)",
            "SWITCH": (
                "first interface assigned in immutable topology link order; "
                "10.(i//254+1).(i%254+1).{src=1,dst=2}"
            ),
        },
    }
    artifacts: Dict[str, Any] = {}
    evidence: Dict[str, Any] = {}
    try:
        if not RUN_ID_PATTERN.fullmatch(expected_run_id):
            raise RouteEvidenceError(
                "EXPECTED_RUN_ID_INVALID", "expected_run_id violates the evidence grammar"
            )
        frozen = _read_bound_artifact(
            frozen_link_map_path,
            "frozen_link_map",
            expected_frozen_link_map_sha256,
        )
        artifacts["frozen_link_map"] = frozen.report()
        runtime = _read_bound_artifact(
            runtime_link_map_path,
            "runtime_link_map",
            expected_runtime_link_map_sha256,
        )
        artifacts["runtime_link_map"] = runtime.report()
        if (frozen.device, frozen.inode) == (runtime.device, runtime.inode):
            raise RouteEvidenceError(
                "LINK_MAP_ARTIFACT_ALIAS",
                "runtime and frozen link_map must be independent regular files",
            )
        if frozen.data != runtime.data:
            raise RouteEvidenceError(
                "LINK_MAP_BINDING_MISMATCH",
                "runtime link_map is not byte-identical to the frozen link_map",
            )
        frozen_topology = _parse_link_map(frozen)
        # Parse the execution copy independently even though its bytes match.
        runtime_topology = _parse_link_map(runtime)
        if frozen_topology != runtime_topology:
            raise RouteEvidenceError(
                "LINK_MAP_SEMANTIC_MISMATCH",
                "runtime and frozen link_map topology projections differ",
            )
        topology_artifact = _read_bound_artifact(
            topology_path,
            "topology",
            expected_topology_sha256,
        )
        artifacts["topology"] = topology_artifact.report()
        frozen_topology = _bind_topology_file(
            frozen_topology, topology_artifact
        )
        runtime_topology = _bind_topology_file(
            runtime_topology, topology_artifact
        )
        route = _read_bound_artifact(
            route_candidates_path,
            "route_candidates",
            expected_route_candidates_sha256,
        )
        artifacts["route_candidates"] = route.report()
        actual_rows = _parse_route_rows(route, expected_run_id)
        mapped, used_link_ids = _candidate_mapping(actual_rows, runtime_topology)
        expected_rows = reconstruct_expected_rows(frozen_topology, expected_run_id)
        _compare_rows(actual_rows, expected_rows)
        coverage = _coverage_report(actual_rows, expected_rows, runtime_topology)
        route_groups = len({row.group_key() for row in actual_rows})
        multi_candidate_groups = len({
            row.group_key() for row in actual_rows if row.candidate_count > 1
        })
        evidence = {
            "physical_link_count": len(runtime_topology.link_ids),
            "topology_node_count": len(runtime_topology.node_types),
            "installed_route_row_count": len(actual_rows),
            "reconstructed_route_row_count": len(expected_rows),
            "route_group_count": route_groups,
            "multi_candidate_route_group_count": multi_candidate_groups,
            "mapped_candidate_count": len(mapped),
            "mapped_candidate_projection_sha256": canonical_hash(mapped),
            "candidate_link_ids_used": sorted(used_link_ids),
            "candidate_link_id_count": len(used_link_ids),
            "coverage": coverage,
            "contracts": {
                "exact_schema": True,
                "single_run_id": True,
                "canonical_unsigned_integers": True,
                "contiguous_candidate_indices": True,
                "declared_candidate_counts_exact": True,
                "candidate_order_exact": True,
                "global_row_order_exact": True,
                "every_candidate_maps_to_one_physical_link": True,
                "route_set_matches_independent_reconstruction": True,
                "runtime_link_map_matches_frozen_bytes": True,
            },
        }
        return _finalize_report({
            "schema_version": SCHEMA_VERSION,
            "status": PASS,
            "contract": contract,
            "artifacts": artifacts,
            "evidence": evidence,
            "errors": [],
            "scope_limit": (
                "Route-install evidence only; this does not qualify or unlock "
                "an ECMP/hash-contention workload executor."
            ),
        })
    except RouteEvidenceError as error:
        return _finalize_report({
            "schema_version": SCHEMA_VERSION,
            "status": FAIL,
            "contract": contract,
            "artifacts": artifacts,
            "evidence": evidence,
            "errors": [{"code": error.code, "detail": error.detail}],
            "scope_limit": (
                "Route-install evidence only; this does not qualify or unlock "
                "an ECMP/hash-contention workload executor."
            ),
        })
    except Exception as error:  # pragma: no cover - defensive fail-closed boundary
        return _finalize_report({
            "schema_version": SCHEMA_VERSION,
            "status": FAIL,
            "contract": contract,
            "artifacts": artifacts,
            "evidence": evidence,
            "errors": [{
                "code": "VALIDATOR_INTERNAL_ERROR",
                "detail": f"{type(error).__name__}: {error}",
            }],
            "scope_limit": (
                "Route-install evidence only; this does not qualify or unlock "
                "an ECMP/hash-contention workload executor."
            ),
        })


def require_valid(report: Mapping[str, Any]) -> None:
    if report.get("status") != PASS:
        errors = report.get("errors", [])
        detail = errors[0].get("detail", "route evidence failed") if errors else "route evidence failed"
        raise RouteEvidenceError("ROUTE_EVIDENCE_FAILED", str(detail))


def write_report(path: Path, report: Mapping[str, Any]) -> None:
    """Atomically publish canonical JSON; refuse to replace a symlink."""

    path = path.absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RouteEvidenceError("REPORT_SYMLINK", "report output is a symbolic link")
    payload = json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    pending = path.with_name(path.name + ".pending")
    if pending.exists() or pending.is_symlink():
        raise RouteEvidenceError("REPORT_PENDING_EXISTS", "report pending path already exists")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(pending, flags, 0o600)
    try:
        encoded = payload.encode("utf-8")
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(pending, path)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route-candidates", required=True, type=Path)
    parser.add_argument("--topology", required=True, type=Path)
    parser.add_argument("--frozen-link-map", required=True, type=Path)
    parser.add_argument("--runtime-link-map", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--route-candidates-sha256")
    parser.add_argument("--topology-sha256")
    parser.add_argument("--frozen-link-map-sha256")
    parser.add_argument("--runtime-link-map-sha256")
    parser.add_argument("--report", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = validate_route_candidate_evidence(
        route_candidates_path=args.route_candidates,
        topology_path=args.topology,
        frozen_link_map_path=args.frozen_link_map,
        runtime_link_map_path=args.runtime_link_map,
        expected_run_id=args.run_id,
        expected_route_candidates_sha256=args.route_candidates_sha256,
        expected_topology_sha256=args.topology_sha256,
        expected_frozen_link_map_sha256=args.frozen_link_map_sha256,
        expected_runtime_link_map_sha256=args.runtime_link_map_sha256,
    )
    if args.report is not None:
        try:
            write_report(args.report, report)
        except RouteEvidenceError as error:
            print(f"ERROR [{error.code}]: {error.detail}", file=sys.stderr)
            return 2
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0 if report["status"] == PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
