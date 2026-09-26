#!/usr/bin/env python3
"""Select an exercised GPU ACCESS rail and write one permanent hard fault.

The candidate links come only from the run's physical ``link_map.csv``.  The
healthy NIC counters are used solely to avoid choosing an idle rail.  A zero
``end_time_ns`` is the on-disk representation of a permanent disconnect; the
simulator deliberately does not schedule a revert for ``hard_disconnect``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--link-map", required=True, type=Path)
    parser.add_argument("--healthy-nic", required=True, type=Path)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument(
        "--host-port",
        type=int,
        help=(
            "require this GPU-side ACCESS port instead of selecting the rail "
            "with the largest healthy TX count (the dual-plane matrix uses "
            "host port 3, Plane B)"
        ),
    )
    parser.add_argument("--start-ns", type=int, default=50_000)
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    return parser.parse_args()


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError(f"{path}: missing CSV header")
        return list(reader)


def as_int(value: str, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid {field}={value!r}") from error


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    args = parse_args()
    if args.start_ns <= 0:
        raise SystemExit("--start-ns must be positive")

    link_rows = read_csv(args.link_map)
    candidates = []
    for row in link_rows:
        if row.get("link_class") != "ACCESS":
            continue
        src = as_int(row.get("src_node", ""), "src_node")
        dst = as_int(row.get("dst_node", ""), "dst_node")
        if args.gpu not in (src, dst):
            continue
        host_port = (
            as_int(row.get("src_port", ""), "src_port")
            if src == args.gpu
            else as_int(row.get("dst_port", ""), "dst_port")
        )
        candidates.append({"link_id": row["link_id"], "host_port": host_port})
    if len(candidates) != 2:
        raise SystemExit(
            f"expected exactly two GPU-{args.gpu} ACCESS links, found {candidates}"
        )

    nic_rows = read_csv(args.healthy_nic)
    final_tx = {item["link_id"]: 0 for item in candidates}
    sample_count = {item["link_id"]: 0 for item in candidates}
    for row in nic_rows:
        try:
            node_id = int(row.get("node_id", ""))
        except ValueError:
            continue
        link_id = row.get("link_id", "")
        if node_id != args.gpu or link_id not in final_tx:
            continue
        try:
            tx_bytes = int(row.get("tx_bytes", ""))
        except ValueError:
            continue
        final_tx[link_id] = max(final_tx[link_id], tx_bytes)
        sample_count[link_id] += 1

    if args.host_port is None:
        selected = sorted(
            candidates,
            key=lambda item: (-final_tx[item["link_id"]], item["link_id"]),
        )[0]
        selection_policy = (
            "highest final healthy tx_bytes among the exactly two physical "
            f"GPU-{args.gpu} ACCESS links; lexical link_id tie-break"
        )
    else:
        matching = [
            item for item in candidates if item["host_port"] == args.host_port
        ]
        if len(matching) != 1:
            raise SystemExit(
                f"expected exactly one GPU-{args.gpu} ACCESS link on host port "
                f"{args.host_port}, found {matching}"
            )
        selected = matching[0]
        selection_policy = (
            f"explicit GPU-{args.gpu} host port {args.host_port}; topology role "
            "is authoritative and healthy TX is recorded but not used for selection"
        )
    if args.host_port is None and final_tx[selected["link_id"]] <= 0:
        raise SystemExit(
            f"healthy run contains no transmitted bytes on selected GPU-{args.gpu} "
            f"ACCESS link {selected['link_id']}"
        )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.out_csv.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "fault_id",
                "fault_type",
                "target_link_id",
                "start_time_ns",
                "end_time_ns",
                "severity",
                "parameter_before",
                "parameter_after",
            ]
        )
        writer.writerow(
            [
                f"true16_gpu{args.gpu}_hard_disconnect",
                "hard_disconnect",
                selected["link_id"],
                args.start_ns,
                0,
                "1.0",
                "up",
                "physically_disconnected",
            ]
        )

    manifest = {
        "status": "PASS",
        "selection_policy": selection_policy,
        "gpu": args.gpu,
        "requested_host_port": args.host_port,
        "candidates": [
            {
                **item,
                "healthy_final_tx_bytes": final_tx[item["link_id"]],
                "healthy_sample_count": sample_count[item["link_id"]],
            }
            for item in sorted(candidates, key=lambda item: item["link_id"])
        ],
        "selected_link_id": selected["link_id"],
        "selected_host_port": selected["host_port"],
        "fault_start_ns": args.start_ns,
        "permanent": True,
        "inputs": {
            "link_map": {
                "path": str(args.link_map.resolve()),
                "sha256": sha256(args.link_map),
            },
            "healthy_nic": {
                "path": str(args.healthy_nic.resolve()),
                "sha256": sha256(args.healthy_nic),
            },
        },
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(selected["link_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
