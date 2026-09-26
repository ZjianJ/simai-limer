#!/usr/bin/env python3
"""Run the P1 true-16 topology and telemetry acceptance audit.

The audit is deliberately strict and machine-readable.  It verifies the
physical dual-plane contract, the link-map/NIC-plane mapping, and every
configured telemetry snapshot.  A failed check produces both a top-level
``"status": "FAIL"`` and a non-zero process exit status.

This tool consumes existing artifacts only; it never launches SimAI.
"""

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict

import pandas as pd

import validate_telemetry
import validate_true16_dualrail


EXPECTED_LINK_CLASSES = {
    "ACCESS": 32,
    "INTER_SWITCH": 256,
    "INTRA_NODE": 16,
}
EXPECTED_LINK_PROPERTIES = {
    "ACCESS": {"bandwidth_bps": 100_000_000_000, "delay_ns": 500},
    "INTER_SWITCH": {"bandwidth_bps": 400_000_000_000, "delay_ns": 500},
    "INTRA_NODE": {"bandwidth_bps": 2_400_000_000_000, "delay_ns": 25},
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", required=True)
    parser.add_argument("--run-dirs", nargs="+", required=True)
    parser.add_argument(
        "--expected-sample-interval-ns",
        type=int,
        default=validate_telemetry.DEFAULT_SAMPLE_INTERVAL_NS,
    )
    parser.add_argument("--out-json", required=True)
    parser.add_argument("--out-md", required=True)
    return parser.parse_args()


def _check(results, name, ok, detail, scope="platform"):
    results.append({
        "scope": scope,
        "check": name,
        "status": "PASS" if ok else "FAIL",
        "detail": detail,
    })


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _physical_pairs_from_topology(topology):
    return {
        tuple(sorted((int(link["src"]), int(link["dst"]))))
        for link in topology["links"]
    }


def validate_link_map_contract(link_map, topology, topology_result, results, scope):
    """Validate exact counts, endpoint uniqueness, and A/B NIC semantics."""
    missing = sorted(
        validate_telemetry.LINK_MAP_REQUIRED_COLUMNS - set(link_map.columns)
    )
    _check(results, "link_map_schema", not missing,
           f"missing_columns={missing}", scope)
    if missing:
        return {}

    class_counts = {
        str(key): int(value)
        for key, value in link_map["link_class"].value_counts().items()
    }
    _check(
        results,
        "link_map_exact_class_counts",
        class_counts == EXPECTED_LINK_CLASSES,
        f"expected={EXPECTED_LINK_CLASSES}, observed={class_counts}",
        scope,
    )
    _check(
        results,
        "link_map_unique_link_ids",
        len(link_map) == link_map["link_id"].nunique() == 304,
        f"rows={len(link_map)}, unique={link_map['link_id'].nunique()}",
        scope,
    )

    map_pairs = {
        tuple(sorted((int(row.src_node), int(row.dst_node))))
        for row in link_map.itertuples(index=False)
    }
    topology_pairs = _physical_pairs_from_topology(topology)
    _check(
        results,
        "link_map_matches_topology_edges",
        map_pairs == topology_pairs and len(map_pairs) == len(link_map),
        f"map_only={sorted(map_pairs - topology_pairs)[:5]}, "
        f"topology_only={sorted(topology_pairs - map_pairs)[:5]}",
        scope,
    )

    endpoints = []
    fabric_count = 0
    host_count = 0
    type_violations = []
    property_violations = []
    for row in link_map.itertuples(index=False):
        endpoint_types = {str(row.src_type), str(row.dst_type)}
        expected_types = {
            "ACCESS": {"HOST", "SWITCH"},
            "INTER_SWITCH": {"SWITCH"},
            "INTRA_NODE": {"HOST", "NVSWITCH"},
        }.get(str(row.link_class))
        if endpoint_types != expected_types:
            type_violations.append(
                {"link_id": str(row.link_id), "types": sorted(endpoint_types)}
            )
        expected_properties = EXPECTED_LINK_PROPERTIES.get(str(row.link_class), {})
        if (int(row.bandwidth_bps) != expected_properties.get("bandwidth_bps")
                or int(row.delay_ns) != expected_properties.get("delay_ns")):
            property_violations.append({
                "link_id": str(row.link_id),
                "bandwidth_bps": int(row.bandwidth_bps),
                "delay_ns": int(row.delay_ns),
            })
        for side in ("src", "dst"):
            node = int(getattr(row, f"{side}_node"))
            port = int(getattr(row, f"{side}_port"))
            node_type = str(getattr(row, f"{side}_type"))
            endpoints.append((node, port))
            if node_type == "HOST":
                host_count += 1
            else:
                fabric_count += 1

    endpoint_counts = Counter(endpoints)
    duplicate_endpoints = sorted(
        endpoint for endpoint, count in endpoint_counts.items() if count != 1
    )
    _check(
        results,
        "link_map_unique_physical_ports",
        not duplicate_endpoints and len(endpoint_counts) == 608,
        f"expected=608, observed={len(endpoint_counts)}, "
        f"duplicates={duplicate_endpoints[:5]}",
        scope,
    )
    _check(
        results,
        "link_map_endpoint_counts",
        fabric_count == 560 and host_count == 48,
        f"expected fabric=560 host=48, observed fabric={fabric_count} "
        f"host={host_count}",
        scope,
    )
    _check(results, "link_class_endpoint_types", not type_violations,
           f"violations={type_violations[:5]}", scope)
    _check(results, "nominal_link_properties", not property_violations,
           f"violations={property_violations[:5]}", scope)

    component_by_switch = {}
    for plane in topology_result.get("planes", []):
        for switch in plane["switch_ids"]:
            component_by_switch[int(switch)] = int(plane["plane_id"])

    host_access = defaultdict(list)
    port_planes = defaultdict(set)
    access_switch_hosts = defaultdict(set)
    malformed_access = []
    access = link_map[link_map["link_class"] == "ACCESS"]
    for row in access.itertuples(index=False):
        if row.src_type == "HOST" and row.dst_type == "SWITCH":
            host, host_port, switch = int(row.src_node), int(row.src_port), int(row.dst_node)
        elif row.dst_type == "HOST" and row.src_type == "SWITCH":
            host, host_port, switch = int(row.dst_node), int(row.dst_port), int(row.src_node)
        else:
            malformed_access.append(str(row.link_id))
            continue
        plane = component_by_switch.get(switch)
        host_access[host].append({
            "link_id": str(row.link_id),
            "host_port": host_port,
            "switch_id": switch,
            "plane_id": plane,
        })
        port_planes[host_port].add(plane)
        access_switch_hosts[(plane, switch)].add(host)

    bad_hosts = {}
    for host in range(16):
        attachments = host_access.get(host, [])
        ports = {item["host_port"] for item in attachments}
        planes = {item["plane_id"] for item in attachments}
        if len(attachments) != 2 or ports != {2, 3} or len(planes) != 2:
            bad_hosts[str(host)] = attachments
    plane_a = next(iter(port_planes[2])) if len(port_planes[2]) == 1 else None
    plane_b = next(iter(port_planes[3])) if len(port_planes[3]) == 1 else None
    _check(
        results,
        "one_plane_a_and_b_access_per_gpu",
        not malformed_access and not bad_hosts and plane_a != plane_b,
        f"nic2_plane={plane_a}, nic3_plane={plane_b}, "
        f"malformed={malformed_access[:5]}, bad_hosts={bad_hosts}",
        scope,
    )

    rail_violations = []
    for (plane, switch), hosts in sorted(access_switch_hosts.items()):
        ordered_hosts = sorted(hosts)
        slots = {host % 4 for host in hosts}
        servers = {host // 4 for host in hosts}
        if len(hosts) != 4 or len(slots) != 1 or servers != {0, 1, 2, 3}:
            rail_violations.append({
                "plane": plane, "switch": switch, "hosts": ordered_hosts,
            })
    _check(
        results,
        "dual_plane_rail_optimized_access_mapping",
        len(access_switch_hosts) == 8 and not rail_violations,
        f"access_switches={len(access_switch_hosts)}, "
        f"violations={rail_violations[:5]}",
        scope,
    )

    return {
        "row_count": len(link_map),
        "class_counts": class_counts,
        "fabric_endpoint_count": fabric_count,
        "host_endpoint_count": host_count,
        "plane_a_backup_nic": 2,
        "plane_a_component": plane_a,
        "plane_b_primary_nic": 3,
        "plane_b_component": plane_b,
        "host_access": {str(host): host_access[host] for host in sorted(host_access)},
    }


def build_audit(topology_path, run_dirs, expected_sample_interval_ns):
    checks = []
    runs = []
    topology = validate_true16_dualrail.read_topology(topology_path)
    topology_result = validate_true16_dualrail.validate(topology, 16, 4)
    for item in topology_result["checks"]:
        checks.append({
            "scope": "topology",
            "check": item["name"],
            "status": item["status"],
            "detail": item["detail"],
        })

    link_map_hashes = set()
    for run_dir in run_dirs:
        scope = f"run:{os.path.basename(os.path.abspath(run_dir))}"
        link_map_path = os.path.join(run_dir, "link_map.csv")
        if not os.path.isfile(link_map_path):
            _check(checks, "link_map_present", False,
                   f"missing={os.path.abspath(link_map_path)}", scope)
            link_map_contract = {}
            link_map_sha256 = None
        else:
            _check(checks, "link_map_present", True,
                   os.path.abspath(link_map_path), scope)
            link_map = pd.read_csv(link_map_path)
            link_map_contract = validate_link_map_contract(
                link_map, topology, topology_result, checks, scope)
            link_map_sha256 = _sha256(link_map_path)
            link_map_hashes.add(link_map_sha256)

        telemetry_checks = []
        telemetry_stats = validate_telemetry.validate_run(
            run_dir,
            link_map_path,
            telemetry_checks,
            expected_sample_interval_ns=expected_sample_interval_ns,
        )
        for item in telemetry_checks:
            # The generic telemetry validator can legitimately SKIP optional
            # v2 fields for legacy traces.  P1 is a strict stage gate, so a
            # missing queue/rate/link-state signal is an unmet requirement,
            # not a successful audit with a footnote.
            item_status = "FAIL" if item["status"] == "SKIP" else item["status"]
            checks.append({
                "scope": scope,
                "check": item["check"],
                "status": item_status,
                "detail": (
                    "P1 required check was skipped: " + item["detail"]
                    if item["status"] == "SKIP" else item["detail"]
                ),
            })
        runs.append({
            "run_dir": os.path.abspath(run_dir),
            "link_map_sha256": link_map_sha256,
            "link_map_contract": link_map_contract,
            "telemetry": telemetry_stats,
        })

    _check(
        checks,
        "identical_link_map_across_runs",
        len(link_map_hashes) == 1 and len(runs) >= 1,
        f"unique_hashes={sorted(link_map_hashes)}",
    )

    passed = sum(item["status"] == "PASS" for item in checks)
    failed = sum(item["status"] == "FAIL" for item in checks)
    skipped = sum(item["status"] == "SKIP" for item in checks)
    return {
        "schema_version": "limer.p1-platform-audit.v1",
        "status": "PASS" if failed == 0 else "FAIL",
        "summary": {"pass": passed, "fail": failed, "skip": skipped},
        "contract": {
            "gpu_count": 16,
            "server_count": 4,
            "gpus_per_server": 4,
            "physical_link_count": 304,
            "fabric_endpoint_count": 560,
            "host_endpoint_count": 48,
            "sample_interval_ns": expected_sample_interval_ns,
            "sampling_semantics": "complete state at every configured sample; not continuous event logging",
        },
        "evidence_limitations": [
            "This audit checks only timestamps present in the supplied artifacts.",
            "It does not establish long-duration telemetry stability unless long runs are supplied.",
            "It does not establish per-link hard-fault coverage unless all 16 active ACCESS targets are supplied.",
            "Sub-millisecond queue peaks are event-latched summaries, not complete per-event traces.",
        ],
        "topology_path": os.path.abspath(topology_path),
        "topology_sha256": _sha256(topology_path),
        "topology": topology_result,
        "runs": runs,
        "checks": checks,
    }


def _markdown_escape(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def write_outputs(result, out_json, out_md):
    for path in (out_json, out_md):
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    with open(out_md, "w", encoding="utf-8") as output:
        summary = result["summary"]
        output.write("# LIMER P1 true-16 platform audit\n\n")
        output.write(
            f"**{result['status']}: {summary['pass']} passed, "
            f"{summary['fail']} failed, {summary['skip']} skipped.**\n\n"
        )
        output.write(
            "Coverage means one complete state record for every physical "
            "endpoint at every configured 1 ms sample. It does not mean "
            "continuous per-packet or per-event logging.\n\n"
        )
        output.write("| Scope | Check | Status | Detail |\n")
        output.write("|---|---|---|---|\n")
        for item in result["checks"]:
            output.write(
                f"| {_markdown_escape(item['scope'])} "
                f"| {_markdown_escape(item['check'])} "
                f"| {item['status']} "
                f"| {_markdown_escape(item['detail'])} |\n"
            )


def main():
    args = parse_args()
    try:
        result = build_audit(
            args.topology,
            args.run_dirs,
            args.expected_sample_interval_ns,
        )
    except (OSError, ValueError, KeyError, pd.errors.ParserError) as error:
        result = {
            "schema_version": "limer.p1-platform-audit.v1",
            "status": "FAIL",
            "summary": {"pass": 0, "fail": 1, "skip": 0},
            "error": str(error),
            "checks": [],
        }
    write_outputs(result, args.out_json, args.out_md)
    print(
        f"{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed, "
        f"{result['summary']['skip']} skipped"
    )
    print(f"Wrote {args.out_json} and {args.out_md}")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
