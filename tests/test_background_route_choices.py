#!/usr/bin/env python3
"""Narrow tests for live background-RDMA route-choice evidence."""

from __future__ import annotations

import csv
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODEL = ROOT / "ns-3-alibabacloud/simulation/src/point-to-point/model"
FRONTEND = ROOT / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3"
VALIDATOR_PATH = ROOT / "limer/tools/validate_background_route_choices.py"
SPEC = importlib.util.spec_from_file_location("route_validator", VALIDATOR_PATH)
assert SPEC and SPEC.loader
validator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = validator
SPEC.loader.exec_module(validator)


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class RouteChoiceCppTest(unittest.TestCase):
    def test_standalone_selection_classification_dedup_and_registration(self) -> None:
        compiler = shutil.which("g++")
        if compiler is None:
            self.skipTest("g++ unavailable")
        source = r'''
#include "limer-route-choice.h"
#include <cassert>
#include <set>
#include <vector>

static void Count(const ns3::LimerRouteChoiceEvent&, void* raw) {
  ++*static_cast<int*>(raw);
}
static void Other(const ns3::LimerRouteChoiceEvent&, void*) {}

int main() {
  std::vector<int> candidates{4, 7, 9};
  ns3::LimerEcmpSelection selected = ns3::LimerSelectEcmp(4, candidates);
  assert(selected.ok && selected.candidate_count == 3);
  assert(selected.bucket == 1 && selected.egress_port == 7);
  assert(!ns3::LimerSelectEcmp(4, std::vector<int>()).ok);

  ns3::LimerRouteChoiceEvent data;
  assert(ns3::LimerClassifyBackgroundRoute(
      0x11, 0x0b000101, 0x0b000201, 49152, 4791, 3, &data));
  assert(data.direction == ns3::LIMER_ROUTE_DATA);
  assert(data.flow_src == 1 && data.flow_dst == 2);
  assert(data.flow_sport == 49152 && data.flow_dport == 4791);

  ns3::LimerRouteChoiceEvent ack;
  assert(ns3::LimerClassifyBackgroundRoute(
      0xFC, data.packet_dip, data.packet_sip, data.packet_dport,
      data.packet_sport, data.pg, &ack));
  assert(ack.direction == ns3::LIMER_ROUTE_ACK);
  assert(ack.flow_src == data.flow_src && ack.flow_dst == data.flow_dst);
  assert(ack.flow_sport == data.flow_sport && ack.flow_dport == data.flow_dport);
  assert(!ns3::LimerClassifyBackgroundRoute(
      0x11, data.packet_sip, data.packet_dip, 49151, 4791, 3, &ack));

  int first = 0;
  int second = 0;
  assert(ns3::RegisterLimerRouteChoiceSink(&Count, &first));
  assert(!ns3::RegisterLimerRouteChoiceSink(&Other, &second));
  ns3::EmitLimerRouteChoice(data);
  assert(first == 1 && second == 0);
  assert(!ns3::ClearLimerRouteChoiceSink(&Count, &second));
  ns3::EmitLimerRouteChoice(data);
  assert(first == 2);
  assert(ns3::ClearLimerRouteChoiceSink(&Count, &first));
  assert(!ns3::HasLimerRouteChoiceSink());

  assert(ns3::RegisterLimerRouteChoiceSink(&Count, &first));
  std::set<ns3::LimerRouteChoiceKey> observed;
  ns3::EmitLimerRouteChoiceOnce(&observed, data);
  ns3::EmitLimerRouteChoiceOnce(&observed, data);
  assert(first == 3 && observed.size() == 1);
  assert(ns3::ClearLimerRouteChoiceSink(&Count, &first));
  return 0;
}
'''
        with tempfile.TemporaryDirectory() as raw:
            temp = Path(raw)
            cpp = temp / "route_choice_test.cc"
            binary = temp / "route_choice_test"
            cpp.write_text(source, encoding="utf-8")
            built = subprocess.run(
                [
                    compiler,
                    "-std=c++11",
                    "-pthread",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    f"-I{MODEL}",
                    str(cpp),
                    str(MODEL / "limer-route-choice.cc"),
                    "-o",
                    str(binary),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(built.returncode, 0, built.stderr)
            ran = subprocess.run(
                [str(binary)], text=True, capture_output=True, check=False
            )
            self.assertEqual(ran.returncode, 0, ran.stderr)

    def test_instrumentation_is_in_both_actual_forwarding_paths(self) -> None:
        for name in ("switch-node.cc", "nvswitch-node.cc"):
            source = (MODEL / name).read_text(encoding="utf-8")
            get_out = source[source.index("::GetOutDev") : source.index("::SendToDev")]
            self.assertIn("EcmpHash(buf.u8, 12, m_ecmpSeed)", get_out)
            self.assertIn("LimerSelectEcmp(hash, nexthops)", get_out)
            self.assertIn("LimerClassifyBackgroundRoute", get_out)
            self.assertIn("EmitLimerRouteChoice(event)", get_out)
            self.assertLess(get_out.index("LimerSelectEcmp"), get_out.index("EmitLimerRouteChoice"))
        cmake = (ROOT / "ns-3-alibabacloud/simulation/src/point-to-point/CMakeLists.txt").read_text(
            encoding="utf-8"
        )
        self.assertIn("model/limer-route-choice.cc", cmake)
        self.assertIn("model/limer-route-choice.h", cmake)

    def test_scenarios_are_labels_not_synthetic_marks_or_pause(self) -> None:
        source = (FRONTEND / "limer_background_flow.h").read_text(encoding="utf-8")
        valid = source[source.index("static bool ValidScenario") : source.index("static std::vector", source.index("static bool ValidScenario"))]
        for scenario in ("ecmp_collision", "ecn_pressure", "pfc_pressure"):
            self.assertIn(scenario, valid)
        for forbidden in ("SetEcn", "SendPfc", "SetPause", "m_limerEcnMarks", "m_limerPfcEvents"):
            self.assertNotIn(forbidden, valid)
        self.assertIn("background_route_choices.csv", source)
        self.assertIn("HOST transit is forbidden", source)


class RouteChoiceValidatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.run_id = "route-test"
        self.schedule = self.root / "schedule.csv"
        self.choices = self.root / "choices.csv"
        self.routes = self.root / "routes.csv"
        self.links = self.root / "links.csv"
        self.flow = {
            "event_id": "pressure-1",
            "flow_id": "flow-1",
            "scenario": "ecn_pressure",
            "scheduled_start_ns": 100,
            "src_rank": 0,
            "dst_rank": 2,
            "bytes": 4096,
            "pg": 3,
            "sport": 49152,
            "dport": 4791,
        }
        self._write_valid()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _candidate_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []

        def add(node: int, node_type: str, destination: int, candidates: list[tuple[int, int]]) -> None:
            for index, (egress, next_hop) in enumerate(candidates):
                rows.append(
                    {
                        "run_id": self.run_id,
                        "node_id": node,
                        "node_type": node_type,
                        "destination_node_id": destination,
                        "destination_ip": str(destination),
                        "candidate_index": index,
                        "candidate_count": len(candidates),
                        "egress_port_id": egress,
                        "next_hop_node_id": next_hop,
                        "status": "INSTALLED",
                    }
                )

        # The actual vector order is significant.  The selected switch-4
        # candidate is placed at the packet hash's real bucket.
        data_hash = validator.ecmp_hash(
            validator.simai_ip(0), validator.simai_ip(2), 49152, 4791, 4
        )
        data_vector = [(2, 5), (3, 6)]
        if data_hash % 2 == 1:
            data_vector.reverse()
        add(4, "SWITCH", 2, data_vector)
        add(5, "SWITCH", 2, [(1, 2)])
        add(5, "SWITCH", 0, [(2, 4)])
        add(4, "SWITCH", 0, [(1, 0)])
        return rows

    def _link_rows(self) -> list[dict[str, object]]:
        result = []
        for src, dst, src_type, dst_type, src_port, dst_port in (
            (0, 4, "HOST", "SWITCH", 1, 1),
            (4, 5, "SWITCH", "SWITCH", 2, 2),
            (2, 5, "HOST", "SWITCH", 1, 1),
            (4, 6, "SWITCH", "SWITCH", 3, 1),
        ):
            result.append(
                {
                    "link_id": f"L{min(src, dst)}-{max(src, dst)}",
                    "src_node": src,
                    "dst_node": dst,
                    "src_type": src_type,
                    "dst_type": dst_type,
                    "src_port": src_port,
                    "dst_port": dst_port,
                    "link_class": "ACCESS",
                    "bandwidth_bps": 100000000000,
                    "delay_ns": 1000,
                }
            )
        return result

    def _choice_row(
        self,
        *,
        packet_type: str,
        node: int,
        node_type: str,
        seed: int,
        candidates: list[tuple[int, int]],
        timestamp: int,
        flow: dict[str, object] | None = None,
    ) -> dict[str, object]:
        flow = self.flow if flow is None else flow
        direction, protocol, sip, dip, sport, dport = validator._expected_wire(
            validator.Flow(
                str(flow["event_id"]),
                str(flow["flow_id"]),
                str(flow["scenario"]),
                int(flow["scheduled_start_ns"]),
                int(flow["src_rank"]),
                int(flow["dst_rank"]),
                int(flow["bytes"]),
                int(flow["pg"]),
                int(flow["sport"]),
                int(flow["dport"]),
            ),
            packet_type,
        )
        hash_value = validator.ecmp_hash(sip, dip, sport, dport, seed)
        bucket = hash_value % len(candidates)
        egress, next_hop = candidates[bucket]
        return {
            "run_id": self.run_id,
            "event_id": flow["event_id"],
            "flow_id": flow["flow_id"],
            "scenario": flow["scenario"],
            "direction": direction,
            "packet_type": packet_type,
            "timestamp_ns": timestamp,
            "flow_src_rank": flow["src_rank"],
            "flow_dst_rank": flow["dst_rank"],
            "flow_sport": flow["sport"],
            "flow_dport": flow["dport"],
            "node_id": node,
            "node_type": node_type,
            "packet_sip": sip,
            "packet_dip": dip,
            "packet_sport": sport,
            "packet_dport": dport,
            "pg": flow["pg"],
            "l3_protocol": protocol,
            "ecmp_seed": seed,
            "ecmp_hash": hash_value,
            "candidate_count": len(candidates),
            "bucket": bucket,
            "egress_port_id": egress,
            "next_hop_node_id": next_hop,
            "link_id": f"L{min(node, next_hop)}-{max(node, next_hop)}",
            "status": "OBSERVED_ACTUAL_ROUTE_SELECTION",
        }

    def _valid_choice_rows(
        self, flow: dict[str, object] | None = None
    ) -> list[dict[str, object]]:
        flow = self.flow if flow is None else flow
        routes = self._candidate_rows()
        vector = [
            (int(row["egress_port_id"]), int(row["next_hop_node_id"]))
            for row in routes
            if row["node_id"] == 4 and row["destination_node_id"] == 2
        ]
        return [
            self._choice_row(packet_type="DATA", node=4, node_type="SWITCH", seed=4, candidates=vector, timestamp=110, flow=flow),
            self._choice_row(packet_type="DATA", node=5, node_type="SWITCH", seed=5, candidates=[(1, 2)], timestamp=120, flow=flow),
            self._choice_row(packet_type="ACK", node=5, node_type="SWITCH", seed=5, candidates=[(2, 4)], timestamp=130, flow=flow),
            self._choice_row(packet_type="ACK", node=4, node_type="SWITCH", seed=4, candidates=[(1, 0)], timestamp=140, flow=flow),
        ]

    def _write_valid(self) -> None:
        write_csv(self.schedule, validator.SCHEDULE_COLUMNS, [self.flow])
        write_csv(self.routes, validator.ROUTE_COLUMNS, self._candidate_rows())
        write_csv(self.links, validator.LINK_COLUMNS, self._link_rows())
        write_csv(self.choices, validator.CHOICE_COLUMNS, self._valid_choice_rows())

    def _validate(self) -> dict[str, object]:
        return validator.validate(
            route_choices=self.choices,
            schedule=self.schedule,
            route_candidates=self.routes,
            link_map=self.links,
            expected_run_id=self.run_id,
            gpus_per_server=2,
        )

    def test_forward_ack_hash_candidate_and_link_bindings_pass(self) -> None:
        self.assertEqual(
            validator.ecmp_hash(0x0B000001, 0x0B000101, 49152, 21000, 20),
            1100062295,
        )
        report = self._validate()
        self.assertEqual(report["status"], "PASS", report)
        self.assertEqual(report["metrics"]["flow_count"], 1)
        self.assertEqual(report["metrics"]["forward_choice_count"], 2)
        self.assertEqual(report["metrics"]["ack_choice_count"], 2)
        self.assertEqual(
            report["report_sha256"],
            validator.canonical_hash(
                {key: value for key, value in report.items() if key != "report_sha256"}
            ),
        )
        self.assertEqual(
            [item["path"] for item in report["artifacts"]],
            [
                "inputs/background_flow_schedule.csv",
                "background_route_choices.csv",
                "ecmp_route_candidates.csv",
                "link_map.csv",
            ],
        )

    def test_hash_tamper_fails(self) -> None:
        rows = self._valid_choice_rows()
        rows[0]["ecmp_hash"] = (int(rows[0]["ecmp_hash"]) + 1) & validator.UINT32_MAX
        write_csv(self.choices, validator.CHOICE_COLUMNS, rows)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "ECMP_HASH")

    def test_seed_and_legacy_forwarding_status_fail_closed(self) -> None:
        rows = self._valid_choice_rows()
        rows[0]["ecmp_seed"] = 99
        sip = int(rows[0]["packet_sip"])
        dip = int(rows[0]["packet_dip"])
        sport = int(rows[0]["packet_sport"])
        dport = int(rows[0]["packet_dport"])
        rows[0]["ecmp_hash"] = validator.ecmp_hash(sip, dip, sport, dport, 99)
        rows[0]["bucket"] = int(rows[0]["ecmp_hash"]) % int(
            rows[0]["candidate_count"]
        )
        write_csv(self.choices, validator.CHOICE_COLUMNS, rows)
        self.assertEqual(self._validate()["errors"][0]["code"], "ECMP_SEED")

        rows = self._valid_choice_rows()
        rows[0]["status"] = "OBSERVED_ACTUAL_FORWARDING"
        write_csv(self.choices, validator.CHOICE_COLUMNS, rows)
        self.assertEqual(self._validate()["errors"][0]["code"], "CHOICE_BINDING")

    def test_installed_candidate_order_tamper_fails(self) -> None:
        rows = self._candidate_rows()
        selected = [row for row in rows if row["node_id"] == 4 and row["destination_node_id"] == 2]
        selected[0]["egress_port_id"], selected[1]["egress_port_id"] = (
            selected[1]["egress_port_id"],
            selected[0]["egress_port_id"],
        )
        selected[0]["next_hop_node_id"], selected[1]["next_hop_node_id"] = (
            selected[1]["next_hop_node_id"],
            selected[0]["next_hop_node_id"],
        )
        write_csv(self.routes, validator.ROUTE_COLUMNS, rows)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "CANDIDATE_SELECTION")

    def test_missing_ack_and_same_server_fail_closed(self) -> None:
        write_csv(
            self.choices,
            validator.CHOICE_COLUMNS,
            [row for row in self._valid_choice_rows() if row["packet_type"] == "DATA"],
        )
        self.assertEqual(self._validate()["errors"][0]["code"], "DIRECTION_COVERAGE")
        self._write_valid()
        same_server = dict(self.flow, dst_rank=1)
        write_csv(self.schedule, validator.SCHEDULE_COLUMNS, [same_server])
        self.assertEqual(self._validate()["errors"][0]["code"], "CROSS_SERVER")

    def test_ecmp_label_requires_a_live_shared_bucket(self) -> None:
        collision_flow = dict(self.flow, scenario="ecmp_collision")
        choices = self._valid_choice_rows()
        for row in choices:
            row["scenario"] = "ecmp_collision"
        write_csv(self.schedule, validator.SCHEDULE_COLUMNS, [collision_flow])
        write_csv(self.choices, validator.CHOICE_COLUMNS, choices)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "ECMP_COLLISION_NOT_OBSERVED")

    def test_two_flows_in_a_live_shared_bucket_pass_collision_semantics(self) -> None:
        first = dict(self.flow, scenario="ecmp_collision")
        first_bucket = validator.ecmp_hash(
            validator.simai_ip(0), validator.simai_ip(2), 49152, 4791, 4
        ) % 2
        second_sport = next(
            sport
            for sport in range(49153, 65536)
            if validator.ecmp_hash(
                validator.simai_ip(0), validator.simai_ip(2), sport, 4791, 4
            )
            % 2
            == first_bucket
        )
        second = dict(
            first,
            event_id="collision-2",
            flow_id="flow-2",
            sport=second_sport,
            scheduled_start_ns=101,
        )
        write_csv(self.schedule, validator.SCHEDULE_COLUMNS, [first, second])
        write_csv(
            self.choices,
            validator.CHOICE_COLUMNS,
            self._valid_choice_rows(first) + self._valid_choice_rows(second),
        )
        report = self._validate()
        self.assertEqual(report["status"], "PASS", report)
        self.assertGreater(report["metrics"]["observed_collision_group_count"], 0)

    def test_selected_transit_host_fails_after_route_and_link_binding(self) -> None:
        choices = self._valid_choice_rows()
        selected = choices[0]
        selected.update(
            {
                "egress_port_id": 1,
                "next_hop_node_id": 0,
                "link_id": "L0-4",
            }
        )
        routes = self._candidate_rows()
        installed = next(
            row
            for row in routes
            if row["node_id"] == 4
            and row["destination_node_id"] == 2
            and row["candidate_index"] == selected["bucket"]
        )
        installed["egress_port_id"] = 1
        installed["next_hop_node_id"] = 0
        write_csv(self.routes, validator.ROUTE_COLUMNS, routes)
        write_csv(self.choices, validator.CHOICE_COLUMNS, choices)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "HOST_TRANSIT")

    def test_disconnected_fabric_cycle_is_not_hidden_by_set_closure(self) -> None:
        routes = self._candidate_rows()
        for node, egress, next_hop in ((6, 2, 7), (7, 1, 6)):
            routes.append(
                {
                    "run_id": self.run_id,
                    "node_id": node,
                    "node_type": "SWITCH",
                    "destination_node_id": 2,
                    "destination_ip": "2",
                    "candidate_index": 0,
                    "candidate_count": 1,
                    "egress_port_id": egress,
                    "next_hop_node_id": next_hop,
                    "status": "INSTALLED",
                }
            )
        links = self._link_rows()
        links.append(
            {
                "link_id": "L6-7",
                "src_node": 6,
                "dst_node": 7,
                "src_type": "SWITCH",
                "dst_type": "SWITCH",
                "src_port": 2,
                "dst_port": 1,
                "link_class": "INTER_SWITCH",
                "bandwidth_bps": 100000000000,
                "delay_ns": 1000,
            }
        )
        choices = self._valid_choice_rows()
        choices.extend(
            [
                self._choice_row(
                    packet_type="DATA",
                    node=6,
                    node_type="SWITCH",
                    seed=6,
                    candidates=[(2, 7)],
                    timestamp=150,
                ),
                self._choice_row(
                    packet_type="DATA",
                    node=7,
                    node_type="SWITCH",
                    seed=7,
                    candidates=[(1, 6)],
                    timestamp=151,
                ),
            ]
        )
        write_csv(self.routes, validator.ROUTE_COLUMNS, routes)
        write_csv(self.links, validator.LINK_COLUMNS, links)
        write_csv(self.choices, validator.CHOICE_COLUMNS, choices)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "CHAIN_DISCONNECTED")

    def test_multiple_roots_and_terminals_fail_closed(self) -> None:
        routes = self._candidate_rows()
        routes.append(
            {
                "run_id": self.run_id,
                "node_id": 6,
                "node_type": "SWITCH",
                "destination_node_id": 2,
                "destination_ip": "2",
                "candidate_index": 0,
                "candidate_count": 1,
                "egress_port_id": 2,
                "next_hop_node_id": 2,
                "status": "INSTALLED",
            }
        )
        links = self._link_rows()
        links.append(
            {
                "link_id": "L2-6",
                "src_node": 2,
                "dst_node": 6,
                "src_type": "HOST",
                "dst_type": "SWITCH",
                "src_port": 2,
                "dst_port": 2,
                "link_class": "ACCESS",
                "bandwidth_bps": 100000000000,
                "delay_ns": 1000,
            }
        )
        choices = self._valid_choice_rows()
        choices.append(
            self._choice_row(
                packet_type="DATA",
                node=6,
                node_type="SWITCH",
                seed=6,
                candidates=[(2, 2)],
                timestamp=150,
            )
        )
        write_csv(self.routes, validator.ROUTE_COLUMNS, routes)
        write_csv(self.links, validator.LINK_COLUMNS, links)
        write_csv(self.choices, validator.CHOICE_COLUMNS, choices)
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "CHAIN_ROOT")

    def test_root_must_be_physically_adjacent_to_direction_source(self) -> None:
        routes = self._candidate_rows()
        routes.append(
            {
                "run_id": self.run_id,
                "node_id": 6,
                "node_type": "SWITCH",
                "destination_node_id": 2,
                "destination_ip": "2",
                "candidate_index": 0,
                "candidate_count": 1,
                "egress_port_id": 2,
                "next_hop_node_id": 5,
                "status": "INSTALLED",
            }
        )
        links = self._link_rows()
        links.append(
            {
                "link_id": "L5-6",
                "src_node": 5,
                "dst_node": 6,
                "src_type": "SWITCH",
                "dst_type": "SWITCH",
                "src_port": 3,
                "dst_port": 2,
                "link_class": "INTER_SWITCH",
                "bandwidth_bps": 100000000000,
                "delay_ns": 1000,
            }
        )
        choices = [
            row
            for row in self._valid_choice_rows()
            if not (row["packet_type"] == "DATA" and row["node_id"] == 4)
        ]
        choices.append(
            self._choice_row(
                packet_type="DATA",
                node=6,
                node_type="SWITCH",
                seed=6,
                candidates=[(2, 5)],
                timestamp=109,
            )
        )
        write_csv(self.routes, validator.ROUTE_COLUMNS, routes)
        write_csv(self.links, validator.LINK_COLUMNS, links)
        write_csv(self.choices, validator.CHOICE_COLUMNS, choices)
        report = self._validate()
        self.assertEqual(
            report["errors"][0]["code"], "CHAIN_SOURCE_ADJACENCY"
        )

    def test_empty_schedule_event_id_fails_closed(self) -> None:
        write_csv(
            self.schedule,
            validator.SCHEDULE_COLUMNS,
            [dict(self.flow, event_id="")],
        )
        report = self._validate()
        self.assertEqual(report["errors"][0]["code"], "EVENT_ID")


if __name__ == "__main__":
    unittest.main()
