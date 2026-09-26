#!/usr/bin/env python3
"""Regression tests for fail-closed ECMP route-install evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIMER / "tools"))

import validate_ecmp_route_candidates as validator  # noqa: E402


class RouteEvidenceFixture:
    RUN_ID = "p2-test-ecmp-route"

    def __init__(self, root: Path):
        self.root = root
        self.frozen = root / "frozen_link_map.csv"
        self.runtime = root / "runtime_link_map.csv"
        self.topology = root / "topology.txt"
        self.routes = root / "ecmp_route_candidates.csv"
        self.write_csv(self.frozen, validator.LINK_MAP_COLUMNS, self.link_rows())
        shutil.copyfile(self.frozen, self.runtime)
        self.topology.write_text(
            "6 2 1 3 8 TEST_GPU\n"
            "5 2 3 4\n"
            "0 5 100Gbps 0.0005ms 0\n"
            "0 3 100Gbps 0.0005ms 0\n"
            "0 2 100Gbps 0.0005ms 0\n"
            "1 2 100Gbps 0.0005ms 0\n"
            "1 3 100Gbps 0.0005ms 0\n"
            "2 4 100Gbps 0.0005ms 0\n"
            "3 4 100Gbps 0.0005ms 0\n"
            "2 5 100Gbps 0.0005ms 0\n",
            encoding="utf-8",
        )
        self.write_csv(self.routes, validator.ROUTE_COLUMNS, self.route_rows())

    @staticmethod
    def write_csv(path: Path, columns, rows) -> None:
        with path.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=list(columns))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def link_rows():
        def link(link_id, src, dst, src_type, dst_type, src_port, dst_port, kind):
            return {
                "link_id": link_id,
                "src_node": src,
                "dst_node": dst,
                "src_type": src_type,
                "dst_type": dst_type,
                "src_port": src_port,
                "dst_port": dst_port,
                "link_class": kind,
                "bandwidth_bps": 100_000_000_000,
                "delay_ns": 500,
            }

        # Two dual-attached hosts, a two-way ECMP diamond, and one NVSwitch.
        # The hand-written route rows below are the expected C++ nextHop map.
        return [
            link("L0-2", 0, 2, "HOST", "SWITCH", 2, 1, "ACCESS"),
            link("L0-3", 0, 3, "HOST", "SWITCH", 3, 1, "ACCESS"),
            link("L0-5", 0, 5, "HOST", "NVSWITCH", 1, 1, "INTRA_NODE"),
            link("L1-2", 1, 2, "HOST", "SWITCH", 2, 2, "ACCESS"),
            link("L1-3", 1, 3, "HOST", "SWITCH", 3, 2, "ACCESS"),
            link("L2-4", 2, 4, "SWITCH", "SWITCH", 5, 8, "INTER_SWITCH"),
            link("L3-4", 3, 4, "SWITCH", "SWITCH", 7, 4, "INTER_SWITCH"),
            link("L2-5", 2, 5, "SWITCH", "NVSWITCH", 6, 4, "INTER_SWITCH"),
        ]

    @classmethod
    def route_rows(cls):
        def row(source, source_type, destination, index, count, port, next_hop):
            destination_ip = {
                2: "10.1.3.2",
                3: "10.1.2.2",
                4: "10.1.6.2",
            }.get(destination, f"11.0.{destination}.1")
            return {
                "run_id": cls.RUN_ID,
                "node_id": source,
                "node_type": source_type,
                "destination_node_id": destination,
                "destination_ip": destination_ip,
                "candidate_index": index,
                "candidate_count": count,
                "egress_port_id": port,
                "next_hop_node_id": next_hop,
                "status": "INSTALLED",
            }

        rows = [
            row(0, "HOST", 1, 0, 2, 2, 2),
            row(0, "HOST", 1, 1, 2, 3, 3),
            row(0, "HOST", 2, 0, 1, 2, 2),
            row(0, "HOST", 3, 0, 1, 3, 3),
            row(1, "HOST", 0, 0, 2, 2, 2),
            row(1, "HOST", 0, 1, 2, 3, 3),
            row(1, "HOST", 2, 0, 1, 2, 2),
            row(1, "HOST", 3, 0, 1, 3, 3),
            row(2, "SWITCH", 0, 0, 1, 1, 0),
            row(2, "SWITCH", 1, 0, 1, 2, 1),
            row(3, "SWITCH", 0, 0, 1, 1, 0),
            row(3, "SWITCH", 1, 0, 1, 2, 1),
            row(4, "SWITCH", 0, 0, 2, 4, 3),
            row(4, "SWITCH", 0, 1, 2, 8, 2),
            row(4, "SWITCH", 1, 0, 2, 4, 3),
            row(4, "SWITCH", 1, 1, 2, 8, 2),
            row(5, "NVSWITCH", 0, 0, 1, 1, 0),
            row(5, "NVSWITCH", 1, 0, 1, 4, 2),
        ]
        return rows

    def validate(self, **overrides):
        arguments = {
            "route_candidates_path": self.routes,
            "topology_path": self.topology,
            "frozen_link_map_path": self.frozen,
            "runtime_link_map_path": self.runtime,
            "expected_run_id": self.RUN_ID,
        }
        arguments.update(overrides)
        return validator.validate_route_candidate_evidence(**arguments)

    def read_routes(self):
        with self.routes.open(encoding="utf-8", newline="") as source:
            return list(csv.DictReader(source))

    def write_routes(self, rows) -> None:
        self.write_csv(self.routes, validator.ROUTE_COLUMNS, rows)


class ValidateEcmpRouteCandidatesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fixture = RouteEvidenceFixture(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def error_code(report) -> str:
        return report["errors"][0]["code"]

    def test_valid_complete_route_set_is_deterministic_and_hash_bound(self) -> None:
        route_sha = hashlib.sha256(self.fixture.routes.read_bytes()).hexdigest()
        link_sha = hashlib.sha256(self.fixture.frozen.read_bytes()).hexdigest()
        topology_sha = hashlib.sha256(self.fixture.topology.read_bytes()).hexdigest()
        first = self.fixture.validate(
            expected_route_candidates_sha256=route_sha,
            expected_topology_sha256=topology_sha,
            expected_frozen_link_map_sha256=link_sha,
            expected_runtime_link_map_sha256=link_sha,
        )
        second = self.fixture.validate(
            expected_route_candidates_sha256=route_sha,
            expected_topology_sha256=topology_sha,
            expected_frozen_link_map_sha256=link_sha,
            expected_runtime_link_map_sha256=link_sha,
        )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], validator.PASS)
        self.assertEqual(first["evidence"]["installed_route_row_count"], 18)
        self.assertEqual(first["evidence"]["physical_link_count"], 8)
        self.assertEqual(first["artifacts"]["topology"]["sha256"], topology_sha)
        coverage = first["evidence"]["coverage"]
        self.assertEqual(coverage["host_count"], 2)
        self.assertEqual(coverage["switch_count"], 3)
        self.assertEqual(coverage["nvswitch_count"], 1)
        self.assertEqual(coverage["fabric_to_host_group_count"], 8)
        self.assertTrue(coverage["all_physical_fabric_nodes_observed"])
        self.assertTrue(coverage["all_reconstructable_fabric_to_host_groups_present"])
        digest = first["report_sha256"]
        material = dict(first)
        del material["report_sha256"]
        self.assertEqual(digest, validator.canonical_hash(material))
        self.assertIn("does not qualify or unlock", first["scope_limit"])

    def test_report_writer_round_trips_canonical_report(self) -> None:
        report = self.fixture.validate()
        output = self.root / "report.json"
        validator.write_report(output, report)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), report)

    def test_foreign_run_id_fails_closed(self) -> None:
        rows = self.fixture.read_routes()
        rows[0]["run_id"] = "foreign-run"
        self.fixture.write_routes(rows)
        report = self.fixture.validate()
        self.assertEqual(report["status"], validator.FAIL)
        self.assertEqual(self.error_code(report), "FOREIGN_RUN_ID")

    def test_duplicate_and_missing_rows_fail_closed(self) -> None:
        rows = self.fixture.read_routes()
        self.fixture.write_routes([*rows, rows[-1]])
        duplicate = self.fixture.validate()
        self.assertEqual(self.error_code(duplicate), "ROUTE_DUPLICATE")

        # Remove a whole single-candidate fabric route group: local group
        # syntax remains valid, but topology reconstruction detects the hole.
        self.fixture.write_routes(rows[:-1])
        missing = self.fixture.validate()
        self.assertEqual(self.error_code(missing), "ROUTE_SET_MISMATCH")

    def test_candidate_count_index_and_order_are_exact(self) -> None:
        original = self.fixture.read_routes()

        rows = [dict(row) for row in original]
        rows[13]["candidate_count"] = "3"
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "CANDIDATE_COUNT")

        rows = [dict(row) for row in original]
        rows[13]["candidate_index"] = "2"
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "CANDIDATE_INDEX")

        rows = [dict(row) for row in original]
        # Keep candidate indices/global row order but reverse their physical
        # (local port,next hop) meanings.
        for column in ("egress_port_id", "next_hop_node_id"):
            rows[12][column], rows[13][column] = rows[13][column], rows[12][column]
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "CANDIDATE_ORDER")

    def test_every_candidate_must_map_to_one_exact_physical_tuple(self) -> None:
        rows = self.fixture.read_routes()
        rows[8]["next_hop_node_id"] = "1"
        self.fixture.write_routes(rows)
        report = self.fixture.validate()
        self.assertEqual(self.error_code(report), "ROUTE_NEXT_HOP_MISMATCH")

    def test_route_schema_and_canonical_integers_are_exact(self) -> None:
        rows = self.fixture.read_routes()
        with self.fixture.routes.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(
                destination, fieldnames=[*validator.ROUTE_COLUMNS, "unexpected"]
            )
            writer.writeheader()
            writer.writerows({**row, "unexpected": ""} for row in rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "CSV_SCHEMA")

        self.fixture.write_routes(rows)
        rows = self.fixture.read_routes()
        rows[0]["candidate_index"] = "00"
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "NONCANONICAL_INTEGER")

    def test_destination_ip_and_node_type_are_bound_to_topology(self) -> None:
        topology = validator._parse_link_map(
            validator._read_bound_artifact(
                self.fixture.frozen, "fixed_address_fixture", None
            )
        )
        topology = validator._bind_topology_file(
            topology,
            validator._read_bound_artifact(
                self.fixture.topology, "fixed_topology_fixture", None
            ),
        )
        self.assertEqual(topology.destination_ips[0], "11.0.0.1")
        self.assertEqual(topology.destination_ips[5], "11.0.5.1")
        # Link-map row order would assign node 2 10.1.1.2.  The real topology
        # creates its first interface at link index 2, hence 10.1.3.2.
        self.assertEqual(topology.destination_ips[2], "10.1.3.2")
        self.assertEqual(topology.destination_ips[3], "10.1.2.2")
        # Node 4 first occurs as the dst endpoint of zero-based link row 5.
        self.assertEqual(topology.destination_ips[4], "10.1.6.2")
        rows = self.fixture.read_routes()
        rows[0]["destination_ip"] = "11.0.9.1"
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "DESTINATION_IP_MISMATCH")

        self.fixture.write_routes(RouteEvidenceFixture.route_rows())
        rows = self.fixture.read_routes()
        rows[8]["node_type"] = "NVSWITCH"
        self.fixture.write_routes(rows)
        self.assertEqual(self.error_code(self.fixture.validate()), "ROUTE_NODE_TYPE_MISMATCH")

    def test_frozen_runtime_binding_and_immutable_hashes_reject_tamper(self) -> None:
        original_route_sha = hashlib.sha256(self.fixture.routes.read_bytes()).hexdigest()
        rows = self.fixture.read_routes()
        rows[0]["status"] = "BROKEN"
        self.fixture.write_routes(rows)
        route_report = self.fixture.validate(
            expected_route_candidates_sha256=original_route_sha
        )
        self.assertEqual(self.error_code(route_report), "ARTIFACT_HASH_MISMATCH")

        self.fixture.write_routes(RouteEvidenceFixture.route_rows())
        with self.fixture.runtime.open("a", encoding="utf-8") as destination:
            destination.write("\n")
        link_report = self.fixture.validate()
        self.assertEqual(self.error_code(link_report), "LINK_MAP_BINDING_MISMATCH")

    def test_frozen_and_runtime_maps_cannot_alias_the_same_inode(self) -> None:
        self.fixture.runtime.unlink()
        os.link(self.fixture.frozen, self.fixture.runtime)
        report = self.fixture.validate()
        self.assertEqual(self.error_code(report), "LINK_MAP_ARTIFACT_ALIAS")

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_symlink_inputs_are_rejected_including_parent_components(self) -> None:
        real_routes = self.root / "real-routes.csv"
        shutil.copyfile(self.fixture.routes, real_routes)
        self.fixture.routes.unlink()
        self.fixture.routes.symlink_to(real_routes)
        route_report = self.fixture.validate()
        self.assertEqual(self.error_code(route_report), "ARTIFACT_SYMLINK")

        self.fixture.routes.unlink()
        shutil.copyfile(real_routes, self.fixture.routes)
        real_directory = self.root / "real-dir"
        real_directory.mkdir()
        nested = real_directory / "runtime.csv"
        shutil.copyfile(self.fixture.runtime, nested)
        linked_directory = self.root / "linked-dir"
        linked_directory.symlink_to(real_directory, target_is_directory=True)
        parent_report = self.fixture.validate(
            runtime_link_map_path=linked_directory / "runtime.csv"
        )
        self.assertEqual(self.error_code(parent_report), "ARTIFACT_SYMLINK")

    def test_link_map_rejects_ambiguous_physical_endpoint_mapping(self) -> None:
        rows = RouteEvidenceFixture.link_rows()
        rows[-1] = {**rows[-1], "src_port": rows[5]["src_port"]}
        RouteEvidenceFixture.write_csv(
            self.fixture.runtime, validator.LINK_MAP_COLUMNS, rows
        )
        # Make both copies identical so semantic link validation—not only the
        # frozen/runtime byte lock—must reject endpoint reuse.
        shutil.copyfile(self.fixture.runtime, self.fixture.frozen)
        report = self.fixture.validate()
        self.assertEqual(self.error_code(report), "ENDPOINT_REUSED")

    def test_cli_returns_nonzero_and_publishes_fail_report(self) -> None:
        rows = self.fixture.read_routes()
        rows.pop()
        self.fixture.write_routes(rows)
        report_path = self.root / "cli-report.json"
        with open(os.devnull, "w", encoding="utf-8") as sink:
            old_stdout = sys.stdout
            try:
                sys.stdout = sink
                status = validator.main([
                    "--route-candidates", str(self.fixture.routes),
                    "--topology", str(self.fixture.topology),
                    "--frozen-link-map", str(self.fixture.frozen),
                    "--runtime-link-map", str(self.fixture.runtime),
                    "--run-id", self.fixture.RUN_ID,
                    "--report", str(report_path),
                ])
            finally:
                sys.stdout = old_stdout
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(report_path.read_text())["status"], validator.FAIL)


if __name__ == "__main__":
    unittest.main()
