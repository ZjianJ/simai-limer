#!/usr/bin/env python3
"""Evaluate the P0 contract-and-reproducibility stage gate."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


PASS = "PASS"
FAIL = "FAIL"
SCHEMA_VERSION = "limer.stage-gate.p0.v1"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
GIT_REVISION = re.compile(r"^[0-9a-f]{40}$")


def _add(checks: List[Dict[str, str]], name: str, ok: bool, detail: str) -> None:
    checks.append(
        {"check": name, "status": PASS if ok else FAIL, "detail": detail}
    )


def _load(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} root must be a JSON object")
    return value


def evaluate(
    contract_check: Mapping[str, Any],
    baseline_manifest: Mapping[str, Any],
    manifest_check: Mapping[str, Any],
) -> Dict[str, Any]:
    checks: List[Dict[str, str]] = []
    _add(
        checks,
        "contract_schema_and_semantics",
        contract_check.get("status") == PASS
        and int(contract_check.get("summary", {}).get("fail", -1)) == 0,
        f"status={contract_check.get('status')}, "
        f"summary={contract_check.get('summary')}",
    )
    _add(
        checks,
        "baseline_manifest_integrity",
        manifest_check.get("status") == PASS
        and int(manifest_check.get("summary", {}).get("failed", -1)) == 0,
        f"status={manifest_check.get('status')}, "
        f"summary={manifest_check.get('summary')}",
    )
    _add(
        checks,
        "baseline_manifest_schema",
        baseline_manifest.get("schema_version") == "limer.baseline-freeze.v1"
        and baseline_manifest.get("preset") == "true16-hard-fault-e2e",
        f"schema={baseline_manifest.get('schema_version')}, "
        f"preset={baseline_manifest.get('preset')}",
    )

    provenance = baseline_manifest.get("contract_provenance", {})
    contract_hash = contract_check.get("contract_sha256")
    _add(
        checks,
        "contract_identity_and_hash_frozen",
        provenance.get("contract_id") == "limer-true16-dual-plane-v1"
        and isinstance(contract_hash, str)
        and SHA256.fullmatch(contract_hash) is not None
        and provenance.get("contract_sha256") == contract_hash,
        f"id={provenance.get('contract_id')}, "
        f"parse_hash={contract_hash}, manifest_hash={provenance.get('contract_sha256')}",
    )

    revision_fields = {
        "simai_revision": provenance.get("simai_revision"),
        "ns3_revision": provenance.get("ns3_revision"),
    }
    _add(
        checks,
        "source_revisions_frozen",
        all(
            isinstance(value, str) and GIT_REVISION.fullmatch(value) is not None
            for value in revision_fields.values()
        ),
        f"revisions={revision_fields}",
    )
    _add(
        checks,
        "dirty_state_content_addressed",
        isinstance(provenance.get("dirty_worktree"), bool)
        and isinstance(
            baseline_manifest.get("git", {})
            .get("superproject", {})
            .get("working_tree_fingerprint_sha256"),
            str,
        )
        and SHA256.fullmatch(
            baseline_manifest["git"]["superproject"][
                "working_tree_fingerprint_sha256"
            ]
        )
        is not None,
        f"dirty={provenance.get('dirty_worktree')}, "
        "fingerprint="
        f"{baseline_manifest.get('git', {}).get('superproject', {}).get('working_tree_fingerprint_sha256')}",
    )

    required_digests = {
        name: provenance.get(name)
        for name in (
            "topology_sha256",
            "workload_sha256",
            "simulator_config_sha256",
            "schedule_sha256",
            "topology_validation_sha256",
        )
    }
    _add(
        checks,
        "core_artifact_hashes_frozen",
        all(
            isinstance(value, str) and SHA256.fullmatch(value) is not None
            for value in required_digests.values()
        ),
        f"digests={required_digests}",
    )

    exit_hashes = provenance.get("suite_exit_code_artifacts", {})
    _add(
        checks,
        "suite_exit_evidence_frozen",
        set(exit_hashes) == {"healthy", "hard_disconnect"}
        and all(
            isinstance(value, str) and SHA256.fullmatch(value) is not None
            for value in exit_hashes.values()
        ),
        f"exit_hashes={exit_hashes}",
    )

    required_fields = {
        "contract_id",
        "contract_sha256",
        "simai_revision",
        "ns3_revision",
        "dirty_worktree",
        "topology_path",
        "topology_sha256",
        "workload_path",
        "workload_sha256",
        "simulator_config_path",
        "simulator_config_sha256",
        "detector_id",
        "detector_artifact_sha256",
        "split_manifest_sha256",
        "schedule_sha256",
        "run_id",
        "run_role",
        "virtual_start_ns",
        "virtual_finish_ns",
        "wall_start_utc",
        "wall_finish_utc",
        "exit_code",
    }
    missing_fields = sorted(required_fields - set(provenance))
    null_without_reason = sorted(
        field
        for field in required_fields
        if provenance.get(field) is None
        and field not in provenance.get("not_applicable_reasons", {})
    )
    _add(
        checks,
        "provenance_fields_complete_or_explained",
        not missing_fields and not null_without_reason,
        f"missing={missing_fields}, null_without_reason={null_without_reason}",
    )

    artifact_count = len(baseline_manifest.get("artifacts", []))
    fingerprint = baseline_manifest.get("content_fingerprint_sha256")
    _add(
        checks,
        "content_addressed_artifact_set",
        artifact_count > 0
        and isinstance(fingerprint, str)
        and SHA256.fullmatch(fingerprint) is not None,
        f"artifacts={artifact_count}, fingerprint={fingerprint}",
    )

    failed = sum(item["status"] == FAIL for item in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "P0",
        "status": PASS if failed == 0 else FAIL,
        "summary": {
            "pass": sum(item["status"] == PASS for item in checks),
            "fail": failed,
        },
        "next_stage": "P1" if failed == 0 else None,
        "checks": checks,
    }


def _write_markdown(result: Mapping[str, Any], path: Path) -> None:
    lines = [
        "# LIMER P0 stage gate",
        "",
        f"**{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed.**",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for item in result["checks"]:
        detail = str(item["detail"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {item['check']} | {item['status']} | {detail} |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-check", required=True, type=Path)
    parser.add_argument("--baseline-manifest", required=True, type=Path)
    parser.add_argument("--manifest-check", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = evaluate(
            _load(args.contract_check),
            _load(args.baseline_manifest),
            _load(args.manifest_check),
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        result = {
            "schema_version": SCHEMA_VERSION,
            "stage": "P0",
            "status": FAIL,
            "summary": {"pass": 0, "fail": 1},
            "next_stage": None,
            "error": str(error),
            "checks": [],
        }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(
        json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_markdown(result, args.out_md)
    print(
        f"{result['status']}: {result['summary']['pass']} passed, "
        f"{result['summary']['fail']} failed"
    )
    print(f"Wrote {args.out_json} and {args.out_md}")
    return 0 if result["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
