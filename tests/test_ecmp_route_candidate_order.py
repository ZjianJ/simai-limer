#!/usr/bin/env python3
"""Static contract tests for deterministic ECMP route candidate ordering."""

from __future__ import annotations

import itertools
import re
import tempfile
import unittest
from pathlib import Path


SIMAI = Path(__file__).resolve().parents[2]
COMMON = SIMAI / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h"


def stable_candidates(candidates: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Model the C++ key: (local interface index, next-hop node id)."""
    return sorted(candidates, key=lambda candidate: (candidate[0], candidate[1]))


def model_candidate_sidecar(
    output: Path, enabled: str | None, records: list[tuple[int, int, int, int, int]]
) -> None:
    """Small model of the C++ enable gate and final physical-key sort."""
    if enabled != "1":
        return
    ordered = sorted(records)
    output.write_text(
        "node_id,destination_node_id,candidate_index,egress_port_id,"
        "next_hop_node_id\n"
        + "".join(",".join(map(str, record)) + "\n" for record in ordered),
        encoding="utf-8",
    )


class EcmpRouteCandidateOrderTest(unittest.TestCase):
    def test_set_routing_entries_sorts_all_node_types(self) -> None:
        source = COMMON.read_text(encoding="utf-8")
        start = source.index("void SetRoutingEntries()")
        end = source.index("void printRoutingEntries()", start)
        body = source[start:end]
        self.assertNotIn("Switch forwarding is\n      // left untouched", body)
        self.assertNotRegex(
            body,
            r"if\s*\(node->GetNodeType\(\)\s*==\s*0\)\s*\{\s*std::sort",
        )
        self.assertEqual(body.count("std::sort(nexts.begin(), nexts.end()"), 1)
        self.assertRegex(
            body,
            re.compile(
                r"leftIf\s*=\s*nbr2if\[node\]\[left\]\.idx;.*"
                r"rightIf\s*=\s*nbr2if\[node\]\[right\]\.idx;.*"
                r"if\s*\(leftIf\s*!=\s*rightIf\)\s*return leftIf < rightIf;.*"
                r"return left->GetId\(\) < right->GetId\(\);",
                re.DOTALL,
            ),
        )
        sort_position = body.index("std::sort(nexts.begin(), nexts.end()")
        dispatch_position = body.index("if (node->GetNodeType() == 1)")
        self.assertLess(sort_position, dispatch_position)
        # Switch and NVSwitch are explicit branches; hosts consume the final
        # else branch.  The unconditional sort precedes the complete dispatch.
        self.assertIn("GetNodeType() == 1", body)
        self.assertIn("GetNodeType() == 2", body)
        self.assertRegex(body, r"else\s*\{\s*bool is_nvswitch")

    def test_actual_install_path_emits_guarded_byte_stable_evidence(self) -> None:
        source = COMMON.read_text(encoding="utf-8")
        start = source.index("void SetRoutingEntries()")
        end = source.index("void printRoutingEntries()", start)
        body = source[start:end]
        self.assertIn(
            'telemetry_enable != nullptr && std::string(telemetry_enable) == "1"',
            body,
        )
        self.assertIn('"/ecmp_route_candidates.csv"', body)
        self.assertIn(
            "run_id,node_id,node_type,destination_node_id,destination_ip,",
            body,
        )
        install = body.index("if (node->GetNodeType() == 1)")
        collect = body.index("ecmp_candidate_records.push_back")
        self.assertLess(install, collect)
        self.assertIn("output.flush()", body)
        self.assertIn("output.close()", body)
        self.assertGreaterEqual(body.count("std::exit(2)"), 4)

        records = [(20, 9, 1, 12, 70), (20, 9, 0, 11, 69), (3, 8, 0, 2, 20)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            disabled = root / "disabled.csv"
            model_candidate_sidecar(disabled, None, list(reversed(records)))
            self.assertFalse(disabled.exists())
            model_candidate_sidecar(disabled, "0", records)
            self.assertFalse(disabled.exists())
            first = root / "first.csv"
            second = root / "second.csv"
            model_candidate_sidecar(first, "1", records)
            model_candidate_sidecar(second, "1", list(reversed(records)))
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_ptr_iteration_permutation_cannot_change_bucket_mapping(self) -> None:
        candidates = [(7, 42), (3, 91), (3, 28), (11, 17)]
        expected = [(3, 28), (3, 91), (7, 42), (11, 17)]
        observed = {
            tuple(stable_candidates(list(permutation)))
            for permutation in itertools.permutations(candidates)
        }
        self.assertEqual(observed, {tuple(expected)})
        for bucket, candidate in enumerate(expected):
            self.assertEqual(next(iter(observed))[bucket], candidate)


if __name__ == "__main__":
    unittest.main()
