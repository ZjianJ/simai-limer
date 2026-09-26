#!/usr/bin/env python3
"""Validate compact training-QP source-port allocator runtime evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence


SCHEMA_VERSION = "limer.training-source-port-validation.v1"
RAW_FILENAME = "training_source_port_allocator.csv"
REPORT_FILENAME = "training_source_port_allocator_validation.json"
TRAINING_FIRST = 10_000
BACKGROUND_FIRST = 49_152
CAPACITY = BACKGROUND_FIRST - TRAINING_FIRST
RAW_COLUMNS = [
    "run_id",
    "interval_first",
    "interval_end_exclusive",
    "capacity",
    "allocations",
    "releases",
    "reuses",
    "active_at_stop",
    "peak_active",
    "pair_count",
    "pairs_with_reuse",
    "max_pair_allocations",
    "max_pair_reuses",
    "external_conflicts",
    "exhaustions",
    "invariant_errors",
    "min_allocated_port",
    "max_allocated_port",
    "status",
]
_UINT_RE = re.compile(r"0|[1-9][0-9]*")


class AllocatorEvidenceError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _uint(row: Mapping[str, str], field: str, errors: list[str]) -> int:
    raw = row.get(field, "")
    if not _UINT_RE.fullmatch(raw):
        errors.append(f"{field} is not a canonical non-negative integer")
        return 0
    return int(raw)


def validate_allocator_evidence(
    path: Path,
    *,
    expected_run_id: str,
    require_reuse: bool,
) -> Dict[str, Any]:
    errors: list[str] = []
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AllocatorEvidenceError(f"allocator evidence is missing: {path}") from exc
    if path.is_symlink() or not resolved.is_file():
        raise AllocatorEvidenceError("allocator evidence must be a plain file")
    try:
        with resolved.open("r", newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != RAW_COLUMNS:
                raise AllocatorEvidenceError(
                    f"allocator evidence header mismatch: {reader.fieldnames!r}"
                )
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise AllocatorEvidenceError(f"cannot read allocator evidence: {exc}") from exc
    if len(rows) != 1:
        raise AllocatorEvidenceError(
            f"allocator evidence must contain exactly one row, found {len(rows)}"
        )
    row = rows[0]
    if None in row or any(value is None for value in row.values()):
        raise AllocatorEvidenceError("allocator evidence row has malformed fields")
    if row["run_id"] != expected_run_id:
        errors.append("run_id differs from the sealed stage")

    numbers = {
        field: _uint(row, field, errors)
        for field in RAW_COLUMNS
        if field not in {"run_id", "status"}
    }
    if numbers["interval_first"] != TRAINING_FIRST:
        errors.append(f"interval_first must equal {TRAINING_FIRST}")
    if numbers["interval_end_exclusive"] != BACKGROUND_FIRST:
        errors.append(f"interval_end_exclusive must equal {BACKGROUND_FIRST}")
    if numbers["capacity"] != CAPACITY:
        errors.append(f"capacity must equal {CAPACITY}")
    if numbers["allocations"] == 0:
        errors.append("allocator observed no training QP allocations")
    if numbers["pair_count"] == 0:
        errors.append("allocator observed no (src,dst) pairs")
    if numbers["reuses"] > numbers["allocations"]:
        errors.append("reuses exceeds allocations")
    if numbers["allocations"] != (
        numbers["releases"] + numbers["active_at_stop"]
    ):
        errors.append("allocations must equal releases plus active_at_stop")
    if numbers["active_at_stop"] > numbers["peak_active"]:
        errors.append("active_at_stop exceeds peak_active")
    if numbers["peak_active"] > numbers["allocations"]:
        errors.append("peak_active exceeds allocations")
    if numbers["pairs_with_reuse"] > numbers["pair_count"]:
        errors.append("pairs_with_reuse exceeds pair_count")
    if numbers["max_pair_allocations"] > numbers["allocations"]:
        errors.append("max_pair_allocations exceeds total allocations")
    if numbers["max_pair_reuses"] > numbers["reuses"]:
        errors.append("max_pair_reuses exceeds total reuses")
    if numbers["external_conflicts"] != 0:
        errors.append("external source-port conflicts were observed")
    if numbers["exhaustions"] != 0:
        errors.append("training source-port exhaustion was observed")
    if numbers["invariant_errors"] != 0:
        errors.append("training source-port lifecycle invariant errors were observed")
    if not (
        TRAINING_FIRST <= numbers["min_allocated_port"]
        <= numbers["max_allocated_port"] < BACKGROUND_FIRST
    ):
        errors.append("allocated source-port bounds escaped the training interval")
    if numbers["reuses"] == 0:
        if numbers["pairs_with_reuse"] != 0 or numbers["max_pair_reuses"] != 0:
            errors.append("zero total reuses disagrees with per-pair reuse metrics")
    else:
        if numbers["pairs_with_reuse"] == 0:
            errors.append("positive reuses requires pairs_with_reuse > 0")
        if numbers["max_pair_reuses"] == 0:
            errors.append("positive reuses requires max_pair_reuses > 0")
        if numbers["max_pair_allocations"] <= CAPACITY:
            errors.append(
                "reuse claim lacks a pair that crossed the full port interval"
            )
    if require_reuse and (
        numbers["reuses"] == 0
        or numbers["pairs_with_reuse"] == 0
        or numbers["max_pair_allocations"] <= CAPACITY
    ):
        errors.append("Q4 requires runtime-proven wrap and reuse on at least one pair")
    if row["status"] != "PASS":
        errors.append("runtime allocator producer did not report PASS")

    raw_artifact = {
        "path": RAW_FILENAME,
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
        "row_count": 1,
    }
    report: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not errors else "FAIL",
        "run_id": expected_run_id,
        "profile": "Q4_REUSE_REQUIRED" if require_reuse else "GENERAL",
        "requirements": {
            "interval_first": TRAINING_FIRST,
            "interval_end_exclusive": BACKGROUND_FIRST,
            "capacity": CAPACITY,
            "require_reuse": require_reuse,
            "maximum_error_count": 0,
        },
        "metrics": numbers,
        "producer_status": row["status"],
        "artifact": raw_artifact,
        "errors": errors,
    }
    report["report_sha256"] = canonical_hash(report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--require-reuse", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = validate_allocator_evidence(
            args.input,
            expected_run_id=args.run_id,
            require_reuse=args.require_reuse,
        )
    except AllocatorEvidenceError as exc:
        print(f"allocator evidence refused: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.write_text(rendered, encoding="utf-8")
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
