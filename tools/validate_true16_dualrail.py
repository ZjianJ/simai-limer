#!/usr/bin/env python3
"""Strictly validate the physical contract of a true-16 dual-rail topology.

The two fabric planes are defined structurally, not by switch-id ranges: after
HOST and NVSWITCH nodes are removed, regular SWITCH nodes must form exactly two
connected components.  Every GPU must have exactly one ACCESS link into each
component.  This proves that the two ACCESS/ToR paths do not share a regular
switch or an inter-switch physical link.
"""

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict, deque


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", required=True, help="SimAI topology file")
    parser.add_argument("--expected-gpus", type=int, default=16)
    parser.add_argument("--expected-gpus-per-server", type=int, default=4)
    parser.add_argument("--expected-nodes", type=int, default=92)
    parser.add_argument("--expected-nvswitches", type=int, default=4)
    parser.add_argument("--expected-regular-switches", type=int, default=72)
    parser.add_argument("--expected-links", type=int, default=304)
    parser.add_argument("--expected-inter-switch-links", type=int, default=256)
    parser.add_argument("--out-json", help="write the complete result to this JSON file")
    return parser.parse_args()


def read_topology(path):
    with open(path, "r", encoding="utf-8") as source:
        lines = [line.strip() for line in source if line.strip()]
    if len(lines) < 2:
        raise ValueError("topology must contain a header and a switch-id line")

    header = lines[0].split()
    if len(header) != 6:
        raise ValueError(
            "topology header must be: node_num gpus_per_server "
            "nvswitch_num switch_num link_num gpu_type"
        )
    node_num, gpus_per_server, nvswitch_num, switch_num, link_num = map(
        int, header[:5]
    )
    switch_ids = [int(value) for value in lines[1].split()]

    links = []
    for line_number, line in enumerate(lines[2:], start=3):
        fields = line.split()
        if len(fields) != 5:
            raise ValueError(
                f"line {line_number}: expected 5 link fields, found {len(fields)}"
            )
        src, dst = map(int, fields[:2])
        links.append(
            {
                "src": src,
                "dst": dst,
                "bandwidth": fields[2],
                "delay": fields[3],
                "error_rate": fields[4],
            }
        )

    return {
        "node_num": node_num,
        "gpus_per_server": gpus_per_server,
        "nvswitch_num": nvswitch_num,
        "switch_num": switch_num,
        "link_num": link_num,
        "gpu_type": header[5],
        "switch_ids": switch_ids,
        "links": links,
    }


def connected_components(nodes, adjacency):
    remaining = set(nodes)
    components = []
    while remaining:
        start = min(remaining)
        queue = deque([start])
        component = set()
        remaining.remove(start)
        while queue:
            node = queue.popleft()
            component.add(node)
            for peer in adjacency[node]:
                if peer in remaining:
                    remaining.remove(peer)
                    queue.append(peer)
        components.append(component)
    return sorted(components, key=lambda component: min(component))


def validate(
    topology,
    expected_gpus,
    expected_gpus_per_server,
    expected_nodes=92,
    expected_nvswitches=4,
    expected_regular_switches=72,
    expected_links=304,
    expected_inter_switch_links=256,
):
    checks = []

    def check(name, condition, detail):
        checks.append(
            {"name": name, "status": "PASS" if condition else "FAIL", "detail": detail}
        )

    node_num = topology["node_num"]
    nvswitch_num = topology["nvswitch_num"]
    switch_num = topology["switch_num"]
    switch_ids = topology["switch_ids"]
    links = topology["links"]

    check(
        "expected_node_count",
        node_num == expected_nodes,
        f"expected={expected_nodes}, parsed={node_num}",
    )
    check(
        "expected_nvswitch_count",
        nvswitch_num == expected_nvswitches,
        f"expected={expected_nvswitches}, parsed={nvswitch_num}",
    )
    check(
        "expected_regular_switch_count",
        switch_num == expected_regular_switches,
        f"expected={expected_regular_switches}, parsed={switch_num}",
    )
    check(
        "expected_physical_link_count",
        topology["link_num"] == expected_links,
        f"expected={expected_links}, header={topology['link_num']}",
    )

    check(
        "switch_id_count_matches_header",
        len(switch_ids) == nvswitch_num + switch_num,
        f"header={nvswitch_num + switch_num}, parsed={len(switch_ids)}",
    )
    check(
        "switch_ids_are_unique",
        len(switch_ids) == len(set(switch_ids)),
        f"unique={len(set(switch_ids))}, parsed={len(switch_ids)}",
    )

    nvswitch_ids = set(switch_ids[:nvswitch_num])
    regular_switch_ids = set(switch_ids[nvswitch_num:])
    all_node_ids = set(range(node_num))
    host_ids = all_node_ids - nvswitch_ids - regular_switch_ids

    check(
        "true_16_gpu_nodes",
        len(host_ids) == expected_gpus and host_ids == set(range(expected_gpus)),
        f"expected ids=0..{expected_gpus - 1}, parsed={sorted(host_ids)}",
    )
    check(
        "four_gpus_per_server",
        topology["gpus_per_server"] == expected_gpus_per_server,
        f"expected={expected_gpus_per_server}, parsed={topology['gpus_per_server']}",
    )
    check(
        "link_count_matches_header",
        len(links) == topology["link_num"],
        f"header={topology['link_num']}, parsed={len(links)}",
    )

    endpoints_valid = all(
        link["src"] in all_node_ids
        and link["dst"] in all_node_ids
        and link["src"] != link["dst"]
        for link in links
    )
    check("valid_link_endpoints", endpoints_valid, f"node range=0..{node_num - 1}")

    physical_pairs = [tuple(sorted((link["src"], link["dst"]))) for link in links]
    check(
        "unique_physical_links",
        len(physical_pairs) == len(set(physical_pairs)),
        f"unique={len(set(physical_pairs))}, parsed={len(physical_pairs)}",
    )

    access_peers = defaultdict(list)
    fabric_adjacency = defaultdict(set)
    class_counts = defaultdict(int)
    unexpected = []
    for link in links:
        src, dst = link["src"], link["dst"]
        if ((src in host_ids and dst in regular_switch_ids)
                or (dst in host_ids and src in regular_switch_ids)):
            host = src if src in host_ids else dst
            peer = dst if src in host_ids else src
            access_peers[host].append(peer)
            class_counts["ACCESS"] += 1
        elif ((src in host_ids and dst in nvswitch_ids)
              or (dst in host_ids and src in nvswitch_ids)):
            class_counts["INTRA_NODE"] += 1
        elif src in regular_switch_ids and dst in regular_switch_ids:
            fabric_adjacency[src].add(dst)
            fabric_adjacency[dst].add(src)
            class_counts["INTER_SWITCH"] += 1
        else:
            unexpected.append([src, dst])
            class_counts["OTHER"] += 1

    check(
        "only_expected_link_classes",
        not unexpected,
        f"unexpected_links={unexpected[:8]}",
    )
    check(
        "exactly_32_access_links",
        class_counts["ACCESS"] == expected_gpus * 2,
        f"expected={expected_gpus * 2}, parsed={class_counts['ACCESS']}",
    )
    check(
        "expected_intra_node_link_count",
        class_counts["INTRA_NODE"] == expected_gpus,
        f"expected={expected_gpus}, parsed={class_counts['INTRA_NODE']}",
    )
    check(
        "expected_inter_switch_link_count",
        class_counts["INTER_SWITCH"] == expected_inter_switch_links,
        f"expected={expected_inter_switch_links}, "
        f"parsed={class_counts['INTER_SWITCH']}",
    )

    bad_degree = {
        str(host): peers
        for host, peers in sorted(access_peers.items())
        if len(peers) != 2 or len(set(peers)) != 2
    }
    missing_hosts = sorted(host_ids - set(access_peers))
    check(
        "two_distinct_access_peers_per_gpu",
        not bad_degree and not missing_hosts,
        f"bad_degree={bad_degree}, missing_hosts={missing_hosts}",
    )

    components = connected_components(regular_switch_ids, fabric_adjacency)
    component_by_switch = {
        switch: plane for plane, component in enumerate(components)
        for switch in component
    }
    check(
        "exactly_two_isolated_switch_planes",
        len(components) == 2,
        "component_sizes=" + str([len(component) for component in components]),
    )

    host_plane_peers = {}
    per_host_plane_ok = True
    for host in sorted(host_ids):
        plane_peers = defaultdict(list)
        for peer in access_peers.get(host, []):
            if peer in component_by_switch:
                plane_peers[component_by_switch[peer]].append(peer)
        host_plane_peers[str(host)] = {
            str(plane): peers for plane, peers in sorted(plane_peers.items())
        }
        if len(components) != 2 or set(plane_peers) != {0, 1}:
            per_host_plane_ok = False
        elif any(len(peers) != 1 for peers in plane_peers.values()):
            per_host_plane_ok = False
    check(
        "one_access_peer_per_gpu_per_plane",
        per_host_plane_ok,
        "each GPU must attach once to plane 0 and once to plane 1",
    )

    # In the rail-optimized contract, each plane has one ACCESS switch for
    # each local GPU slot.  That switch attaches the same slot from every
    # server (for example GPUs 0, 4, 8, and 12).  Checking only degree=2 at
    # the hosts is insufficient: a topology with arbitrary cross-wiring can
    # otherwise pass the dual-plane checks while violating the experiment's
    # rail grouping.
    access_switch_hosts = defaultdict(set)
    for host, peers in access_peers.items():
        for peer in peers:
            access_switch_hosts[peer].add(host)

    expected_server_count = expected_gpus // expected_gpus_per_server
    rail_group_violations = []
    for plane, component in enumerate(components):
        plane_access_switches = sorted(component & set(access_switch_hosts))
        if len(plane_access_switches) != expected_gpus_per_server:
            rail_group_violations.append(
                {
                    "plane": plane,
                    "reason": "access_switch_count",
                    "expected": expected_gpus_per_server,
                    "observed": len(plane_access_switches),
                }
            )
        observed_slots = []
        for switch in plane_access_switches:
            hosts = sorted(access_switch_hosts[switch])
            slots = {host % expected_gpus_per_server for host in hosts}
            servers = {host // expected_gpus_per_server for host in hosts}
            if len(slots) == 1:
                observed_slots.extend(slots)
            if (len(hosts) != expected_server_count or len(slots) != 1
                    or servers != set(range(expected_server_count))):
                rail_group_violations.append(
                    {
                        "plane": plane,
                        "switch": switch,
                        "hosts": hosts,
                        "slots": sorted(slots),
                        "servers": sorted(servers),
                    }
                )
        if sorted(observed_slots) != list(range(expected_gpus_per_server)):
            rail_group_violations.append(
                {
                    "plane": plane,
                    "reason": "local_gpu_slots",
                    "observed": sorted(observed_slots),
                }
            )
    check(
        "rail_optimized_access_groups",
        not rail_group_violations,
        f"violations={rail_group_violations[:8]}",
    )

    # Each server must also have exactly one local NVSwitch joined to all and
    # only its four GPUs.  This closes another loophole where aggregate link
    # counts are right but the server membership is wrong.
    nvswitch_hosts = defaultdict(set)
    for link in links:
        src, dst = link["src"], link["dst"]
        if src in host_ids and dst in nvswitch_ids:
            nvswitch_hosts[dst].add(src)
        elif dst in host_ids and src in nvswitch_ids:
            nvswitch_hosts[src].add(dst)
    expected_server_host_sets = {
        frozenset(range(server * expected_gpus_per_server,
                        (server + 1) * expected_gpus_per_server))
        for server in range(expected_server_count)
    }
    observed_nvswitch_host_sets = {
        frozenset(hosts) for hosts in nvswitch_hosts.values()
    }
    check(
        "one_nvswitch_per_server_group",
        (len(nvswitch_hosts) == expected_server_count
         and observed_nvswitch_host_sets == expected_server_host_sets),
        "groups=" + str(sorted(sorted(group) for group in observed_nvswitch_host_sets)),
    )

    planes = []
    for plane, component in enumerate(components):
        plane_hosts = sorted(
            host for host in host_ids
            if any(peer in component for peer in access_peers.get(host, []))
        )
        inter_links = sum(
            1 for link in links
            if link["src"] in component and link["dst"] in component
        )
        planes.append(
            {
                "plane_id": plane,
                "switch_ids": sorted(component),
                "switch_count": len(component),
                "gpu_ids": plane_hosts,
                "access_link_count": sum(
                    1 for host in host_ids
                    for peer in access_peers.get(host, []) if peer in component
                ),
                "inter_switch_link_count": inter_links,
            }
        )

    all_passed = all(item["status"] == "PASS" for item in checks)
    return {
        "status": "PASS" if all_passed else "FAIL",
        "topology": {
            "node_count": node_num,
            "gpu_count": len(host_ids),
            "gpus_per_server": topology["gpus_per_server"],
            "nvswitch_count": nvswitch_num,
            "regular_switch_count": switch_num,
            "physical_link_count": len(links),
            "link_class_counts": dict(sorted(class_counts.items())),
        },
        "planes": planes,
        "host_access_peers_by_plane": host_plane_peers,
        "checks": checks,
    }


def main():
    args = parse_args()
    result = None
    try:
        topology = read_topology(args.topology)
        result = validate(
            topology,
            args.expected_gpus,
            args.expected_gpus_per_server,
            args.expected_nodes,
            args.expected_nvswitches,
            args.expected_regular_switches,
            args.expected_links,
            args.expected_inter_switch_links,
        )
        with open(args.topology, "rb") as source:
            result["topology_sha256"] = hashlib.sha256(source.read()).hexdigest()
        result["topology_path"] = os.path.abspath(args.topology)
    except (OSError, ValueError) as error:
        result = {
            "status": "FAIL",
            "topology_path": os.path.abspath(args.topology),
            "error": str(error),
            "checks": [],
        }

    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.out_json:
        parent = os.path.dirname(os.path.abspath(args.out_json))
        os.makedirs(parent, exist_ok=True)
        with open(args.out_json, "w", encoding="utf-8") as output:
            output.write(rendered)
    sys.stdout.write(rendered)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
