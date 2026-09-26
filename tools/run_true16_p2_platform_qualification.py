#!/usr/bin/env python3
"""Fail-stop, four-level qualification for the true-16 SimAI platform."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

import generate_true16_traffic_workload as workload
import run_true16_p2_corpus as runner
import simulator_runtime_bundle as runtime_bundle
import validate_p2_workload_runtime as runtime
import validate_training_source_port_allocator as sport_validator


SCHEMA = "limer.p2-platform-qualification.v3"
STAGE_SCHEMA = "limer.p2-platform-stage.v3"
ALLOCATOR_STAGE_SCHEMA = "limer.p2-platform-stage.v2"
LEGACY_STAGE_SCHEMA = "limer.p2-platform-stage.v1"
HARNESS_SOURCE_SCHEMA = "limer.python-harness-source-binding.v1"
HORIZON_NS = 520_000_000
LIMER = Path(__file__).resolve().parents[1]
TOOLS_ROOT = Path(__file__).resolve().parent
DEFAULT_WORKLOADS = (
    LIMER / "configs/microAllReduce_16rank_p2_q1_completion_1layer_64kib.txt",
    LIMER / "configs/microAllReduce_16rank_p2_q2_completion_10layers_64kib.txt",
    LIMER / "configs/microAllReduce_16rank_p2_q3_completion_32layers_64kib.txt",
    LIMER / "configs/microAllReduce_16rank_p2_sparse_periodic_550ms.txt",
)


class QualificationError(RuntimeError):
    pass


def _source_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _harness_binding_identity(value: Mapping[str, Any]) -> str:
    material = {
        "schema_version": value.get("schema_version"),
        "source_root": value.get("source_root"),
        "sources": value.get("sources"),
    }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _discover_harness_source_paths() -> tuple[Path, ...]:
    """Return every already-loaded Python source module under limer/tools."""

    discovered: set[Path] = set()
    for module in tuple(sys.modules.values()):
        raw = getattr(module, "__file__", None)
        if not isinstance(raw, str) or not raw.endswith(".py"):
            continue
        try:
            source = Path(raw).resolve(strict=True)
            source.relative_to(TOOLS_ROOT)
        except (OSError, ValueError):
            continue
        if source.is_file():
            discovered.add(source)
    required = {
        Path(__file__).resolve(strict=True),
        *(Path(module.__file__).resolve(strict=True) for module in (
            workload, runner, runtime_bundle, runtime, sport_validator
        )),
    }
    if not required <= discovered:
        missing = sorted(str(path) for path in required - discovered)
        raise QualificationError(
            f"harness source discovery omitted required modules: {missing}"
        )
    if not discovered:
        raise QualificationError("harness source discovery returned no modules")
    return tuple(sorted(discovered, key=lambda path: path.as_posix()))


def _build_harness_source_binding(
    source_root: Path, source_paths: Sequence[Path]
) -> Dict[str, Any]:
    try:
        root = source_root.resolve(strict=True)
    except OSError as exc:
        raise QualificationError(
            f"harness source root is missing: {source_root}"
        ) from exc
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in source_paths:
        if raw.is_symlink():
            raise QualificationError(f"harness source must not be a symlink: {raw}")
        try:
            path = raw.resolve(strict=True)
            relative = path.relative_to(root).as_posix()
        except (OSError, ValueError) as exc:
            raise QualificationError(
                f"harness source escaped its root: {raw}"
            ) from exc
        if path.suffix != ".py" or not path.is_file() or relative in seen:
            raise QualificationError(f"invalid or duplicate harness source: {raw}")
        seen.add(relative)
        rows.append({
            "path": relative,
            "sha256": _source_sha256(path),
            "size_bytes": path.stat().st_size,
        })
    rows.sort(key=lambda item: item["path"])
    binding: Dict[str, Any] = {
        "schema_version": HARNESS_SOURCE_SCHEMA,
        "source_root": str(root),
        "source_count": len(rows),
        "sources": rows,
    }
    binding["identity_sha256"] = _harness_binding_identity(binding)
    return binding


def _validate_harness_source_binding(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise QualificationError("harness source binding is missing")
    sources = value.get("sources")
    if (
        value.get("schema_version") != HARNESS_SOURCE_SCHEMA
        or not isinstance(value.get("source_root"), str)
        or not Path(value["source_root"]).is_absolute()
        or not isinstance(sources, list)
        or not sources
        or value.get("source_count") != len(sources)
    ):
        raise QualificationError("harness source binding shape is invalid")
    paths: List[str] = []
    for row in sources:
        if not isinstance(row, Mapping):
            raise QualificationError("harness source binding row is invalid")
        path = row.get("path")
        digest = row.get("sha256")
        size = row.get("size_bytes")
        if (
            not isinstance(path, str)
            or not path
            or Path(path).is_absolute()
            or ".." in Path(path).parts
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            raise QualificationError("harness source binding row is malformed")
        paths.append(path)
    if paths != sorted(set(paths)):
        raise QualificationError("harness source binding paths are not canonical")
    if value.get("identity_sha256") != _harness_binding_identity(value):
        raise QualificationError("harness source binding identity is invalid")
    return value


def _capture_harness_source_binding() -> Mapping[str, Any]:
    binding = _build_harness_source_binding(
        TOOLS_ROOT, _discover_harness_source_paths()
    )
    return _validate_harness_source_binding(binding)


def _verify_harness_source_binding(
    expected: Mapping[str, Any], *, phase: str,
    _source_root: Path | None = None,
    _source_paths: Sequence[Path] | None = None,
) -> None:
    _validate_harness_source_binding(expected)
    source_root = _source_root or TOOLS_ROOT
    source_paths = (
        tuple(_source_paths)
        if _source_paths is not None
        else _discover_harness_source_paths()
    )
    observed = _build_harness_source_binding(
        source_root, source_paths
    )
    if observed != expected:
        expected_rows = {
            row["path"]: (row["sha256"], row["size_bytes"])
            for row in expected["sources"]
        }
        observed_rows = {
            row["path"]: (row["sha256"], row["size_bytes"])
            for row in observed["sources"]
        }
        changed = sorted(
            path for path in expected_rows.keys() & observed_rows.keys()
            if expected_rows[path] != observed_rows[path]
        )
        added = sorted(observed_rows.keys() - expected_rows.keys())
        removed = sorted(expected_rows.keys() - observed_rows.keys())
        raise QualificationError(
            f"Python harness source drift at {phase}: "
            f"changed={changed}, added={added}, removed={removed}"
        )


def _as_qualification_error(action: str, error: Exception) -> QualificationError:
    return QualificationError(f"sealed simulator runtime {action} failed: {error}")


def _loader_evidence(
    binding: runner.SimulatorRuntimeBinding,
    *,
    phase: str,
) -> Dict[str, Any]:
    """Independently re-resolve the loader graph from the sealed bundle."""

    if binding.bundle is None:
        raise QualificationError(
            "platform qualification requires a production sealed runtime bundle"
        )
    try:
        observed = runtime_bundle.verify_loader_resolution(binding.bundle)
    except runtime_bundle.RuntimeBundleError as exc:
        raise _as_qualification_error(f"{phase} loader verification", exc) from exc
    return {
        **runner._closure_loader_evidence(observed),
        "phase": phase,
        "checked_at": runner.utc_now(),
    }


def _runtime_bundle_references(
    execution_root: Path,
    binding: runner.SimulatorRuntimeBinding,
) -> Dict[str, Any]:
    """Return immutable, execution-root-relative references to the shared bundle."""

    root = execution_root.resolve()
    try:
        bundle_path = binding.bundle_root.resolve(strict=True).relative_to(root)
        manifest_path = binding.manifest_path.resolve(strict=True).relative_to(root)
        seal_absolute = binding.bundle_root / runtime_bundle.SEAL_NAME
        seal_path = seal_absolute.resolve(strict=True).relative_to(root)
        executable_path = binding.executable.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise QualificationError(
            f"sealed runtime bundle escaped qualification root: {binding.bundle_root}"
        ) from exc
    return {
        "identity_sha256": binding.identity_sha256,
        "bundle_path": bundle_path.as_posix(),
        "manifest_path": manifest_path.as_posix(),
        "manifest_sha256": runner.sha256_file(binding.manifest_path),
        "seal_path": seal_path.as_posix(),
        "seal_sha256": runner.sha256_file(seal_absolute),
        "executable_path": executable_path.as_posix(),
        "executable_sha256": runner.sha256_file(binding.executable),
    }


def _safe_execution_root_reference(
    root: Path, raw: Any, description: str
) -> Path:
    if not isinstance(raw, str) or not raw:
        raise QualificationError(f"{description} is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise QualificationError(f"{description} is unsafe: {raw!r}")
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise QualificationError(f"{description} escaped the output root") from exc
    return resolved


def _load_stage_manifest(directory: Path) -> Mapping[str, Any]:
    try:
        manifest_path = directory / "run_manifest.json"
        seal_path = directory / "run_manifest.sha256"
        if manifest_path.is_symlink() or seal_path.is_symlink():
            raise QualificationError("stage manifest/seal must not be symlinks")
        fields = seal_path.read_text(encoding="ascii").split()
        if (
            len(fields) != 2
            or fields[1] != "run_manifest.json"
            or fields[0] != runner.sha256_file(manifest_path)
        ):
            raise QualificationError("stage manifest seal is invalid")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"cannot load sealed stage {directory}: {exc}") from exc
    if not isinstance(manifest, Mapping):
        raise QualificationError("stage manifest must contain one object")
    if manifest.get("schema_version") not in {
        LEGACY_STAGE_SCHEMA,
        ALLOCATOR_STAGE_SCHEMA,
        STAGE_SCHEMA,
    }:
        raise QualificationError("stage manifest schema is invalid")
    return manifest


def _validate_loader_report(
    report: Any,
    bundle: runtime_bundle.RuntimeBundle,
    description: str,
) -> Mapping[str, Any]:
    if not isinstance(report, Mapping) or report.get("status") != "PASS":
        raise QualificationError(f"{description} is missing or failed")
    projects = bundle.manifest["project_dependencies"]
    systems = bundle.manifest["system_dependencies"]
    try:
        metadata_matches = (
            report.get("runtime_bundle_identity_sha256")
            == bundle.identity_sha256
            and report.get("executable_sha256")
            == bundle.manifest["executable"]["sha256"]
            and int(report.get("project_dependency_count", -1)) == len(projects)
            and int(report.get("system_dependency_count", -1)) == len(systems)
        )
    except (TypeError, ValueError):
        metadata_matches = False
    if not metadata_matches:
        raise QualificationError(f"{description} changed runtime closure metadata")
    expected_projects = sorted(
        (
            str(item["soname"]),
            str(bundle.root / str(item["execution_path"])),
            str(item["sha256"]),
            int(item["size_bytes"]),
        )
        for item in projects
    )
    expected_systems = sorted(
        (
            str(item["soname"]),
            str(item["resolved_path"]),
            str(item["sha256"]),
            int(item["size_bytes"]),
        )
        for item in systems
    )

    def observed(raw: Any, field: str) -> List[tuple[str, str, str, int]]:
        if not isinstance(raw, list) or any(
            not isinstance(item, Mapping) for item in raw
        ):
            raise QualificationError(f"{description} {field} is malformed")
        try:
            return sorted(
                (
                    str(item["soname"]),
                    str(item["path"]),
                    str(item["sha256"]),
                    int(item["size_bytes"]),
                )
                for item in raw
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise QualificationError(
                f"{description} {field} is malformed"
            ) from exc

    if observed(report.get("project_dependencies"), "project dependencies") != (
        expected_projects
    ):
        raise QualificationError(f"{description} project closure differs")
    if observed(report.get("system_dependencies"), "system dependencies") != (
        expected_systems
    ):
        raise QualificationError(f"{description} system closure differs")
    return report


def _validate_process_mapping_report(
    report: Any,
    bundle: runtime_bundle.RuntimeBundle,
) -> Mapping[str, Any]:
    if not isinstance(report, Mapping) or report.get("status") != "PASS":
        raise QualificationError("live process mapping evidence is missing or failed")
    projects = bundle.manifest["project_dependencies"]
    systems = bundle.manifest["system_dependencies"]
    try:
        metadata_matches = (
            report.get("runtime_bundle_identity_sha256")
            == bundle.identity_sha256
            and report.get("executable") == str(bundle.executable)
            and int(report.get("project_dependency_count", -1)) == len(projects)
            and int(report.get("system_dependency_count", -1)) == len(systems)
        )
    except (TypeError, ValueError):
        metadata_matches = False
    if not metadata_matches:
        raise QualificationError("live process closure metadata is invalid")
    if not all(
        isinstance(report.get(field), list)
        and all(isinstance(item, Mapping) for item in report[field])
        for field in ("project_dependencies", "system_dependencies")
    ):
        raise QualificationError("live process closure evidence is malformed")

    expected_projects = sorted(
        (
            str(item["soname"]),
            str(bundle.root / str(item["execution_path"])),
            str(item["sha256"]),
            int(item["size_bytes"]),
        )
        for item in projects
    )
    expected_systems = sorted(
        (
            str(item["soname"]),
            str(item["resolved_path"]),
            str(item["sha256"]),
            int(item["size_bytes"]),
        )
        for item in systems
    )
    try:
        observed_projects = sorted(
            (
                str(item["soname"]),
                str(item["path"]),
                str(item["sha256"]),
                int(item["size_bytes"]),
            )
            for item in report.get("project_dependencies", [])
        )
        observed_systems = sorted(
            (
                str(item["soname"]),
                str(item["path"]),
                str(item["sha256"]),
                int(item["size_bytes"]),
            )
            for item in report.get("system_dependencies", [])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise QualificationError("live process closure evidence is malformed") from exc
    if observed_projects != expected_projects or observed_systems != expected_systems:
        raise QualificationError("live process did not prove the complete loader closure")
    return report


def _exact_csv(path: Path, columns: Sequence[str]) -> List[Dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != tuple(columns):
                raise QualificationError(
                    f"{path.name} exact schema mismatch: {reader.fieldnames}"
                )
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        raise QualificationError(f"cannot read {path}: {exc}") from exc
    if not rows:
        raise QualificationError(f"{path.name} is empty")
    return rows


def _validate_completion(
    directory: Path, report: Mapping[str, Any], run_id: str
) -> Dict[str, Any]:
    layers = int(report["layer_count"])
    message = int(report["collective_bytes_per_layer"])
    rows = _exact_csv(
        directory / "collective_transaction.csv", runtime.COLLECTIVE_TRANSACTION_COLUMNS
    )
    if any(row["run_id"] != run_id for row in rows):
        raise QualificationError("collective transaction contains foreign run_id")
    by_seq: Dict[int, List[Dict[str, str]]] = {}
    try:
        for row in rows:
            by_seq.setdefault(int(row["collective_seq"]), []).append(row)
    except ValueError as exc:
        raise QualificationError("collective_seq is not an integer") from exc
    if set(by_seq) != set(range(layers)):
        raise QualificationError("collective sequence set is not exactly 0..N-1")
    observed_layers: set[int] = set()
    for seq, group in by_seq.items():
        starts = [row for row in group if row["event"] == "START"]
        ready = [row for row in group if row["event"] in {"READY", "LOCAL_READY"}]
        commits = [row for row in group if row["event"] == "COMMIT"]
        if (
            len(starts) != 16
            or len(ready) != 16
            or len(commits) != 1
            or len(group) != 33
        ):
            raise QualificationError(
                f"collective {seq} lacks 16 START/16 READY/1 COMMIT"
            )
        if len({row["event"] for row in ready}) != 1:
            raise QualificationError(f"collective {seq} mixes READY event encodings")
        for event_rows in (starts, ready):
            if {int(row["rank_id"]) for row in event_rows} != set(range(16)):
                raise QualificationError(f"collective {seq} rank set is not true-16")
        layer_values = {int(row["layer_num"]) for row in group}
        if len(layer_values) != 1 or next(iter(layer_values)) in observed_layers:
            raise QualificationError(f"collective {seq} layer identity is inconsistent")
        observed_layers.update(layer_values)
        if any(
            int(row["attempt"]) != 0
            or int(row["message_size_bytes"]) != message
            or int(row["world_size"]) != 16
            for row in group
        ):
            raise QualificationError(f"collective {seq} metadata mismatch")
        if commits[0]["status"] != "exactly_once":
            raise QualificationError(f"collective {seq} is not exactly_once")
        if (
            int(commits[0]["ready_ranks"]) != 16
            or commits[0]["rank_id"] != ""
            or not commits[0]["result_digest"]
            or any(row["status"] != "in_flight" for row in starts)
            or any(row["status"] != "ready" for row in ready)
        ):
            raise QualificationError(f"collective {seq} commit is incomplete")
    if observed_layers != set(range(layers)):
        raise QualificationError("completion stages do not cover every workload layer")

    lifecycle = _exact_csv(directory / "run_lifecycle.csv", runtime.LIFECYCLE_COLUMNS)
    terminal = [
        row
        for row in lifecycle
        if row["event"] == "finish_barrier" and row["status"] == "WORKLOAD_COMPLETE"
    ]
    if len(lifecycle) != 1 or len(terminal) != 1:
        raise QualificationError(
            "completion stage lacks unique WORKLOAD_COMPLETE barrier"
        )
    if any(row["event"] == "observation_horizon" for row in lifecycle):
        raise QualificationError("completion stage unexpectedly contains a horizon")
    if int(terminal[0]["finished_ranks"]) != 16 or int(terminal[0]["world_size"]) != 16:
        raise QualificationError("completion barrier is not true-16")
    for filename, columns in (
        ("switch_telemetry.csv", runtime.SWITCH_COLUMNS),
        ("nic_telemetry.csv", runtime.NIC_COLUMNS),
        ("collective_telemetry.csv", runtime.COLLECTIVE_FLOW_COLUMNS),
    ):
        _exact_csv(directory / filename, columns)
    return {
        "layer_count": layers,
        "message_size_bytes": message,
        "transaction_row_count": len(rows),
        "finish_time_ns": int(terminal[0]["actual_ns"]),
    }


def _runtime_report(
    directory: Path, static: Mapping[str, Any], run_id: str
) -> Mapping[str, Any]:
    planned = {
        "run_id": run_id,
        "run_role": "healthy",
        "class_label": "HEALTHY",
        "virtual_start_ns": 0,
        "virtual_finish_ns": HORIZON_NS,
        "workload_sha256": static["sha256"],
    }
    paths = runtime.RuntimePaths(
        workload=directory / "inputs/workload.txt",
        link_map=directory / "link_map.csv",
        switch_telemetry=directory / "switch_telemetry.csv",
        nic_telemetry=directory / "nic_telemetry.csv",
        collective_transaction=directory / "collective_transaction.csv",
        collective_telemetry=directory / "collective_telemetry.csv",
        run_lifecycle=directory / "run_lifecycle.csv",
    )
    result = runtime.validate_runtime_qualification(
        run=planned,
        workload_report=static,
        paths=paths,
        causal_warmup_ns=runtime.DEFAULT_CAUSAL_WARMUP_NS,
    )
    if result.get("status") != "PASS":
        raise QualificationError(
            "Q4 runtime qualification failed: "
            + "; ".join(str(item) for item in result.get("errors", [])[:8])
        )
    return result


def _write_seal(directory: Path, manifest: Mapping[str, Any]) -> None:
    runner._write_manifest_seal(directory, manifest)


def _stage_route_evidence_record(
    directory: Path, report: Mapping[str, Any]
) -> Dict[str, Any]:
    artifacts = report.get("artifacts")
    raw = (
        artifacts.get("route_candidates")
        if isinstance(artifacts, Mapping) else None
    )
    return {
        "status": report.get("status"),
        "raw_path": runner.ECMP_ROUTE_CANDIDATES_FILE,
        "raw_sha256": raw.get("sha256") if isinstance(raw, Mapping) else None,
        "validation_path": runner.ECMP_ROUTE_VALIDATION_FILE,
        "validation_sha256": runner.sha256_file(
            directory / runner.ECMP_ROUTE_VALIDATION_FILE
        ),
        "report_sha256": report.get("report_sha256"),
    }


def _stage_allocator_evidence_record(
    directory: Path, report: Mapping[str, Any]
) -> Dict[str, Any]:
    artifact = report.get("artifact")
    return {
        "status": report.get("status"),
        "profile": report.get("profile"),
        "raw_path": sport_validator.RAW_FILENAME,
        "raw_sha256": (
            artifact.get("sha256") if isinstance(artifact, Mapping) else None
        ),
        "validation_path": sport_validator.REPORT_FILENAME,
        "validation_sha256": runner.sha256_file(
            directory / sport_validator.REPORT_FILENAME
        ),
        "report_sha256": report.get("report_sha256"),
    }


def _validate_stage_allocator_evidence(
    directory: Path, manifest: Mapping[str, Any]
) -> Mapping[str, Any]:
    record = manifest.get("training_source_port_allocator_evidence")
    if not isinstance(record, Mapping):
        if manifest.get("status") == "PASS":
            raise QualificationError(
                "PASS v2/v3 stage lacks training source-port allocator evidence"
            )
        return {}
    if (
        record.get("raw_path") != sport_validator.RAW_FILENAME
        or record.get("validation_path") != sport_validator.REPORT_FILENAME
    ):
        raise QualificationError("stage allocator evidence paths are invalid")
    report_path = directory / sport_validator.REPORT_FILENAME
    try:
        if runner.sha256_file(report_path) != record.get("validation_sha256"):
            raise QualificationError("stage allocator validation hash changed")
        recorded = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"cannot read stage allocator validation: {exc}") from exc
    if not isinstance(recorded, Mapping):
        raise QualificationError("stage allocator validation is not an object")
    if recorded.get("report_sha256") != record.get("report_sha256"):
        raise QualificationError("stage allocator report identity changed")
    try:
        recomputed = sport_validator.validate_allocator_evidence(
            directory / sport_validator.RAW_FILENAME,
            expected_run_id=str(manifest.get("run_id", "")),
            require_reuse=manifest.get("stage") == "Q4",
        )
    except sport_validator.AllocatorEvidenceError as exc:
        raise QualificationError(f"allocator evidence validation failed: {exc}") from exc
    if dict(recorded) != recomputed:
        raise QualificationError(
            "stage allocator report differs from independent raw-evidence recomputation"
        )
    artifact = recomputed.get("artifact")
    if (
        artifact.get("sha256") if isinstance(artifact, Mapping) else None
    ) != record.get("raw_sha256"):
        raise QualificationError("stage allocator raw hash binding changed")
    expected_profile = "Q4_REUSE_REQUIRED" if manifest.get("stage") == "Q4" else "GENERAL"
    if record.get("profile") != expected_profile:
        raise QualificationError("stage allocator validation profile changed")
    if manifest.get("status") == "PASS" and (
        recomputed.get("status") != "PASS" or record.get("status") != "PASS"
    ):
        raise QualificationError("PASS stage has non-PASS allocator evidence")
    return recomputed


def _validate_stage_route_evidence(
    directory: Path, manifest: Mapping[str, Any]
) -> Mapping[str, Any]:
    """Recompute a sealed stage's route report from raw candidate rows."""

    record = manifest.get("ecmp_route_candidate_evidence")
    if not isinstance(record, Mapping):
        if manifest.get("status") == "PASS":
            raise QualificationError("PASS stage lacks ECMP route evidence")
        return {}
    if (
        record.get("raw_path") != runner.ECMP_ROUTE_CANDIDATES_FILE
        or record.get("validation_path") != runner.ECMP_ROUTE_VALIDATION_FILE
    ):
        raise QualificationError("stage ECMP route evidence paths are invalid")
    report_path = directory / runner.ECMP_ROUTE_VALIDATION_FILE
    try:
        if runner.sha256_file(report_path) != record.get("validation_sha256"):
            raise QualificationError("stage ECMP route validation hash changed")
        recorded = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"cannot read stage ECMP route validation: {exc}") from exc
    if not isinstance(recorded, Mapping):
        raise QualificationError("stage ECMP route validation is not an object")
    if recorded.get("report_sha256") != record.get("report_sha256"):
        raise QualificationError("stage ECMP route report identity changed")
    inputs = manifest.get("inputs")
    link_input = inputs.get("link_map") if isinstance(inputs, Mapping) else None
    topology_input = inputs.get("topology") if isinstance(inputs, Mapping) else None
    expected_link_hash = (
        link_input.get("sha256") if isinstance(link_input, Mapping) else None
    )
    if not isinstance(expected_link_hash, str):
        raise QualificationError("stage lacks frozen link_map hash binding")
    expected_topology_hash = (
        topology_input.get("sha256")
        if isinstance(topology_input, Mapping) else None
    )
    if not isinstance(expected_topology_hash, str):
        raise QualificationError("stage lacks frozen topology hash binding")
    recomputed = runner._route_candidate_validation(
        directory=directory,
        run_id=str(manifest.get("run_id", "")),
        frozen_topology_path=directory / "inputs/topology.txt",
        frozen_link_map_path=directory / "inputs/link_map.csv",
        expected_link_map_sha256=expected_link_hash,
        expected_topology_sha256=expected_topology_hash,
    )
    if dict(recorded) != recomputed:
        raise QualificationError(
            "stage ECMP route report differs from independent raw-evidence recomputation"
        )
    raw_artifacts = recorded.get("artifacts")
    raw = (
        raw_artifacts.get("route_candidates")
        if isinstance(raw_artifacts, Mapping) else None
    )
    if (
        (raw.get("sha256") if isinstance(raw, Mapping) else None)
        != record.get("raw_sha256")
    ):
        raise QualificationError("stage ECMP route raw hash binding changed")
    if manifest.get("status") == "PASS" and (
        recomputed.get("status") != "PASS" or record.get("status") != "PASS"
    ):
        raise QualificationError("PASS stage has non-PASS ECMP route evidence")
    return recomputed


def validate_sealed_stage(directory: Path) -> Mapping[str, Any]:
    """Revalidate a stage without trusting its recorded runtime assertions."""

    try:
        resolved_directory = directory.resolve(strict=True)
    except OSError as exc:
        raise QualificationError(f"stage directory is missing: {directory}") from exc
    if not resolved_directory.is_dir() or directory.is_symlink():
        raise QualificationError(f"stage is not a plain directory: {directory}")
    if resolved_directory.parent.name != "stages":
        raise QualificationError("stage is not under the canonical stages directory")
    execution_root = resolved_directory.parent.parent.resolve(strict=True)
    manifest = _load_stage_manifest(resolved_directory)
    if manifest.get("status") not in {"PASS", "FAIL"}:
        raise QualificationError("stage manifest status is invalid")

    for path in resolved_directory.rglob("*"):
        if path.is_symlink():
            raise QualificationError(f"sealed stage contains a symlink: {path}")
    if (resolved_directory / "inputs/simulator_binary").exists():
        raise QualificationError("stage contains a forbidden simulator copy")
    current = runner._artifact_entries(resolved_directory)
    if any(
        Path(str(item.get("path", ""))).name == "simulator_binary"
        or runtime_bundle.PROJECT_SONAME_RE.fullmatch(
            Path(str(item.get("path", ""))).name
        )
        for item in current
    ):
        raise QualificationError("stage copied simulator runtime artifacts")
    if manifest.get("artifacts") != current or manifest.get(
        "artifact_set_sha256"
    ) != runner.canonical_hash(current):
        raise QualificationError("sealed stage artifact inventory is invalid")
    _validate_stage_route_evidence(resolved_directory, manifest)
    if manifest.get("schema_version") in {
        ALLOCATOR_STAGE_SCHEMA,
        STAGE_SCHEMA,
    }:
        _validate_stage_allocator_evidence(resolved_directory, manifest)
    if manifest.get("schema_version") == STAGE_SCHEMA:
        _validate_harness_source_binding(manifest.get("harness_source_binding"))

    recorded_closure = manifest.get("runtime_closure")
    references = manifest.get("shared_runtime_bundle")
    if not isinstance(recorded_closure, Mapping) or not isinstance(
        references, Mapping
    ):
        raise QualificationError("stage lacks sealed runtime closure references")
    bundle_root = _safe_execution_root_reference(
        execution_root, references.get("bundle_path"), "runtime bundle path"
    )
    expected_identity = references.get("identity_sha256")
    try:
        bundle = runtime_bundle.validate_runtime_bundle(bundle_root)
        loader = runtime_bundle.verify_loader_resolution(bundle)
    except runtime_bundle.RuntimeBundleError as exc:
        raise _as_qualification_error("independent bundle validation", exc) from exc
    if expected_identity != bundle.identity_sha256:
        raise QualificationError("stage runtime identity differs from sealed bundle")
    current_record = runner._runtime_record(execution_root, bundle)
    if dict(recorded_closure) != current_record:
        raise QualificationError("stage runtime closure record is not reproducible")
    expected_references = _runtime_bundle_references(
        execution_root,
        runner.SimulatorRuntimeBinding(
            execution_root=execution_root,
            bundle_root=bundle.root,
            executable=bundle.executable,
            identity_sha256=bundle.identity_sha256,
            manifest_path=bundle.root / runtime_bundle.MANIFEST_NAME,
            manifest_sha256=runner.sha256_file(
                bundle.root / runtime_bundle.MANIFEST_NAME
            ),
            record=current_record,
            loader_preflight={},
            bundle=bundle,
            source_closure=None,
        ),
    )
    if dict(references) != expected_references:
        raise QualificationError("shared runtime bundle references changed")

    bundle_parent = execution_root / runtime_bundle.BUNDLE_ROOT_NAME
    try:
        bundle_children = list(bundle_parent.iterdir())
    except OSError as exc:
        raise QualificationError(f"cannot inspect shared bundle root: {exc}") from exc
    if (
        len(bundle_children) != 1
        or bundle_children[0].is_symlink()
        or bundle_children[0].resolve() != bundle.root
    ):
        raise QualificationError(
            "qualification output must contain exactly one shared runtime bundle"
        )

    command = manifest.get("command", [])
    if (
        not isinstance(command, list)
        or not command
        or command[0] != str(bundle.executable)
    ):
        raise QualificationError("stage did not execute the sealed bundle executable")
    simulator = manifest.get("simulator")
    if (
        not isinstance(simulator, Mapping)
        or simulator.get("execution_copy") is not None
        or simulator.get("runtime_closure_identity_sha256")
        != bundle.identity_sha256
        or simulator.get("path") != current_record["bundle_executable_path"]
        or simulator.get("sha256")
        != bundle.manifest["executable"]["sha256"]
    ):
        raise QualificationError("stage simulator binding is invalid")

    runtime_evidence = manifest.get("runtime_execution")
    if not isinstance(runtime_evidence, Mapping):
        raise QualificationError("stage runtime execution evidence is missing")
    if runtime_evidence.get("runtime_closure_identity_sha256") != bundle.identity_sha256:
        raise QualificationError("runtime execution evidence changed closure identity")
    recorded_loader_preflight = runtime_evidence.get("loader_preflight")
    _validate_loader_report(
        recorded_loader_preflight, bundle, "stage loader preflight evidence"
    )
    pre_run = runtime_evidence.get("pre_run_verification")
    pre_loader = runtime_evidence.get("pre_run_loader_resolution")
    setup_complete = (
        isinstance(pre_run, Mapping)
        and pre_run.get("status") == "PASS"
        and isinstance(pre_loader, Mapping)
        and pre_loader.get("status") == "PASS"
    )
    if setup_complete:
        if (
            pre_run.get("source_closure_status") != "PASS"
            or pre_run.get("bundle_integrity_status") != "PASS"
            or pre_run.get("loader_preflight_identity_sha256")
            != bundle.identity_sha256
        ):
            raise QualificationError("stage pre-run source/bundle proof is invalid")
        _validate_loader_report(
            pre_loader, bundle, "stage explicit pre-run loader evidence"
        )
    elif manifest.get("status") == "PASS":
        raise QualificationError("PASS stage lacks sealed pre-run verification")
    loader_environment = runtime_evidence.get("loader_environment")
    if setup_complete:
        if (
            not isinstance(loader_environment, Mapping)
            or loader_environment.get("runtime_bundle_identity_sha256")
            != bundle.identity_sha256
            or loader_environment.get("LD_LIBRARY_PATH")
            != str(bundle.lib_directory)
            or loader_environment.get("LD_PRELOAD") is not None
        ):
            raise QualificationError("stage loader environment evidence is invalid")

    if manifest.get("status") == "PASS":
        if runtime_evidence.get("status") != "PASS" or not runtime_evidence.get(
            "process_started"
        ):
            raise QualificationError("PASS stage lacks complete runtime execution proof")
        _validate_process_mapping_report(
            runtime_evidence.get("process_mapping_verification"), bundle
        )
        post_run = runtime_evidence.get("post_run_verification")
        if (
            not isinstance(post_run, Mapping)
            or post_run.get("status") != "PASS"
            or post_run.get("source_closure_status") != "PASS"
            or post_run.get("bundle_integrity_status") != "PASS"
        ):
            raise QualificationError("PASS stage lacks post-run source/bundle proof")
        _validate_loader_report(
            runtime_evidence.get("post_run_loader_resolution"),
            bundle,
            "stage post-run loader evidence",
        )
    elif not manifest.get("errors"):
        raise QualificationError("failed stage has no recorded failure reason")

    # Every published sibling stage must bind the exact same closure and bundle.
    for sibling in sorted(resolved_directory.parent.iterdir()):
        if sibling == resolved_directory:
            continue
        if sibling.is_symlink() or not sibling.is_dir():
            raise QualificationError(f"unexpected entry under stages: {sibling}")
        sibling_manifest = _load_stage_manifest(sibling)
        sibling_closure = sibling_manifest.get("runtime_closure")
        sibling_refs = sibling_manifest.get("shared_runtime_bundle")
        if sibling_closure != current_record or sibling_refs != expected_references:
            raise QualificationError(
                "qualification stages do not share one runtime closure identity"
            )
        if manifest.get("schema_version") == STAGE_SCHEMA and (
            sibling_manifest.get("schema_version") != STAGE_SCHEMA
            or sibling_manifest.get("harness_source_binding")
            != manifest.get("harness_source_binding")
        ):
            raise QualificationError(
                "qualification stages do not share one Python harness binding"
            )

    # The fresh loader proof must agree with the content identity as well.
    if loader.identity_sha256 != bundle.identity_sha256:
        raise QualificationError("independent loader proof changed closure identity")
    return manifest


def _run_stage(
    *,
    name: str,
    workload_path: Path,
    profile: str,
    runtime_binding: runner.SimulatorRuntimeBinding,
    runtime_authority: Any,
    topology: Path,
    link_map: Path,
    config: Path,
    out_root: Path,
    worker_threads: int,
    wall_timeout_s: float,
    harness_source_binding: Mapping[str, Any],
) -> Mapping[str, Any]:
    _verify_harness_source_binding(
        harness_source_binding, phase=f"{name}_before_stage"
    )
    final = out_root / "stages" / name
    if final.exists() or final.is_symlink():
        raise QualificationError(f"refusing to overwrite stage: {final}")
    pending = out_root / f".{name}.pending.{os.getpid()}.{uuid.uuid4().hex}"
    pending.mkdir(parents=True)
    (pending / "inputs").mkdir()
    (pending / "raw_simai").mkdir()
    run_id = f"platform-{name.lower()}"
    static = workload.validate_workload(workload_path, profile=profile)
    if static.get("status") != "PASS" or int(static.get("world_size", 0)) != 16:
        raise QualificationError(f"{name} workload is not statically true-16 qualified")
    expected_layers = {"Q1": 1, "Q2": 10, "Q3": 32}
    if name in expected_layers and int(static["layer_count"]) != expected_layers[name]:
        raise QualificationError(
            f"{name} workload layer_count={static['layer_count']}, "
            f"expected={expected_layers[name]}"
        )
    copies = {
        "workload": (workload_path, pending / "inputs/workload.txt"),
        "topology": (topology, pending / "inputs/topology.txt"),
        "link_map": (link_map, pending / "inputs/link_map.csv"),
        "config": (config, pending / "inputs/simulator_config.conf"),
    }
    for source, destination in copies.values():
        shutil.copyfile(source, destination)
    runtime_config, runtime_config_binding = runner.materialize_runtime_config(
        pending / "inputs/simulator_config.conf", pending
    )
    command = [
        str(runtime_binding.executable),
        "-t",
        str(worker_threads),
        "-w",
        str(pending / "inputs/workload.txt"),
        "-n",
        str(pending / "inputs/topology.txt"),
        "-c",
        str(runtime_config),
    ]
    inherited_controls = sorted(
        key
        for key in os.environ
        if key.startswith("LIMER_") or key.startswith("AS_") or key.startswith("NS_")
    )
    base_environment = {
        key: value for key, value in os.environ.items() if key not in inherited_controls
    }
    overrides = {
        "LIMER_TELEMETRY_ENABLE": "1",
        "LIMER_TELEMETRY_INTERVAL_US": "1000",
        "LIMER_TELEMETRY_DIR": str(pending),
        "LIMER_RUN_ID": run_id,
        "LIMER_HARD_EVENT_DETECTOR_ENABLE": "0",
        "LIMER_RECOVERY_ACTION_ENABLE": "0",
        "LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE": "0",
        "LIMER_QUALIFICATION_STAGE": name,
        "LIMER_EXPECTED_LAYERS": str(static["layer_count"]),
        "LIMER_EXPECTED_MESSAGE_BYTES": str(static["collective_bytes_per_layer"]),
        "AS_SEND_LAT": "3",
        "AS_NVLS_ENABLE": "1",
        "AS_PXN_ENABLE": "0",
        "AS_LOG_LEVEL": "1",
        "NS_GLOBAL_VALUE": f"RngSeed=1;RngRun={100 + int(name[1:])}",
    }
    if name == "Q4":
        overrides["LIMER_OBSERVATION_STOP_NS"] = str(HORIZON_NS)

    runtime_execution: Dict[str, Any] = {
        "status": "PENDING",
        "runtime_closure_identity_sha256": runtime_binding.identity_sha256,
        "loader_preflight": dict(runtime_binding.loader_preflight),
        "pre_run_verification": None,
        "pre_run_loader_resolution": None,
        "loader_environment": None,
        "process_mapping_verification": None,
        "post_run_verification": None,
        "post_run_loader_resolution": None,
        "errors": [],
    }
    errors: List[str] = []
    environment: Dict[str, str] = {}
    started = time.monotonic()
    try:
        pre_run = runtime_authority.verify_before_run(runtime_binding)
        if pre_run.get("status") != "PASS":
            raise QualificationError("runtime authority returned non-PASS pre-run proof")
        pre_loader = _loader_evidence(runtime_binding, phase=f"{name}_before_run")
        environment, loader_environment = runtime_authority.execution_environment(
            runtime_binding, base_environment, overrides
        )
        runtime_execution["pre_run_verification"] = dict(pre_run)
        runtime_execution["pre_run_loader_resolution"] = pre_loader
        runtime_execution["loader_environment"] = dict(loader_environment)
    except (QualificationError, runner.RunnerError) as exc:
        errors.append(str(exc))

    exit_code = None
    timed_out = False
    interrupted = False
    resources: Mapping[str, Any] = {}
    process_started = False
    if not errors:
        with (pending / "run.log").open("wb") as log:
            try:
                process = subprocess.Popen(
                    command,
                    cwd=pending / "raw_simai",
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                process_started = True
            except OSError as exc:
                errors.append(f"cannot launch sealed simulator: {exc}")
            if process_started:
                try:
                    # This is intentionally the first operation after Popen.
                    # If a process is too short-lived to expose /proc/<pid>/maps,
                    # qualification fails closed instead of inferring the loader.
                    process_mapping = runtime_authority.verify_process(
                        runtime_binding, process.pid
                    )
                    if process_mapping.get("status") != "PASS":
                        raise QualificationError(
                            "runtime authority returned non-PASS process maps proof"
                        )
                    runtime_execution["process_mapping_verification"] = dict(
                        process_mapping
                    )
                except (QualificationError, runner.RunnerError) as exc:
                    mapping_error = str(exc)
                    errors.append(mapping_error)
                    runtime_execution["process_mapping_verification"] = {
                        "status": "FAIL",
                        "pid": process.pid,
                        "runtime_bundle_identity_sha256": (
                            runtime_binding.identity_sha256
                        ),
                        "error": mapping_error,
                    }
                    runner._terminate_process(process)
                exit_code, timed_out, interrupted, resources = (
                    runner._wait_with_resource_monitoring(process, wall_timeout_s)
                )

    if process_started:
        try:
            post_run = runtime_authority.verify_after_run(runtime_binding)
            if post_run.get("status") != "PASS":
                raise QualificationError(
                    "runtime authority returned non-PASS post-run proof"
                )
            post_loader = _loader_evidence(runtime_binding, phase=f"{name}_after_run")
            runtime_execution["post_run_verification"] = dict(post_run)
            runtime_execution["post_run_loader_resolution"] = post_loader
        except (QualificationError, runner.RunnerError) as exc:
            post_error = str(exc)
            errors.append(post_error)
            runtime_execution["post_run_verification"] = {
                "status": "FAIL",
                "runtime_bundle_identity_sha256": runtime_binding.identity_sha256,
                "error": post_error,
            }

    runtime_errors = [
        str(item)
        for item in errors
        if "runtime" in str(item).lower()
        or "loader" in str(item).lower()
        or "simulator" in str(item).lower()
    ]
    runtime_execution["errors"] = runtime_errors
    runtime_execution["process_started"] = process_started
    runtime_execution["status"] = (
        "PASS"
        if process_started
        and isinstance(runtime_execution.get("process_mapping_verification"), Mapping)
        and runtime_execution["process_mapping_verification"].get("status") == "PASS"
        and isinstance(runtime_execution.get("post_run_verification"), Mapping)
        and runtime_execution["post_run_verification"].get("status") == "PASS"
        and isinstance(runtime_execution.get("post_run_loader_resolution"), Mapping)
        and runtime_execution["post_run_loader_resolution"].get("status") == "PASS"
        else "FAIL"
    )
    _verify_harness_source_binding(
        harness_source_binding, phase=f"{name}_after_simulator"
    )

    if process_started and (timed_out or interrupted or exit_code != 0):
        errors.append(
            f"process exit={exit_code}, timed_out={timed_out}, interrupted={interrupted}"
        )
    evidence: Mapping[str, Any] = {}
    route_evidence_record: Mapping[str, Any] | None = None
    allocator_evidence_record: Mapping[str, Any] | None = None
    if resources.get("oom_kill_observed_during_attempt"):
        errors.append("cgroup reported an OOM kill during the stage")
    if not errors:
        try:
            runtime_link = pending / "link_map.csv"
            if runner.sha256_file(runtime_link) != runner.sha256_file(link_map):
                raise QualificationError("runtime link_map differs from frozen input")
            _exact_csv(runtime_link, runtime.LINK_MAP_COLUMNS)
            route_report = runner._route_candidate_validation(
                directory=pending,
                run_id=run_id,
                frozen_topology_path=pending / "inputs/topology.txt",
                frozen_link_map_path=pending / "inputs/link_map.csv",
                expected_link_map_sha256=runner.sha256_file(link_map),
                expected_topology_sha256=runner.sha256_file(topology),
            )
            runner._write_json_atomic(
                pending / runner.ECMP_ROUTE_VALIDATION_FILE,
                route_report,
            )
            route_evidence_record = _stage_route_evidence_record(
                pending, route_report
            )
            if route_report.get("status") != "PASS":
                raise QualificationError(
                    "ECMP route candidate validation failed: "
                    + runner._route_failure_detail(route_report)
                )
            allocator_report = sport_validator.validate_allocator_evidence(
                pending / sport_validator.RAW_FILENAME,
                expected_run_id=run_id,
                require_reuse=name == "Q4",
            )
            runner._write_json_atomic(
                pending / sport_validator.REPORT_FILENAME,
                allocator_report,
            )
            allocator_evidence_record = _stage_allocator_evidence_record(
                pending, allocator_report
            )
            if allocator_report.get("status") != "PASS":
                raise QualificationError(
                    "training source-port allocator validation failed: "
                    + "; ".join(
                        str(item)
                        for item in allocator_report.get("errors", [])[:8]
                    )
                )
            if name == "Q4":
                evidence = _runtime_report(pending, static, run_id)
                runner._write_json_atomic(
                    pending / "workload_runtime_qualification.json", evidence
                )
            else:
                evidence = _validate_completion(pending, static, run_id)
        except (
            QualificationError,
            sport_validator.AllocatorEvidenceError,
            OSError,
            ValueError,
            KeyError,
        ) as exc:
            errors.append(str(exc))
    artifacts = runner._artifact_entries(pending)
    manifest = {
        "schema_version": STAGE_SCHEMA,
        "status": "PASS" if not errors else "FAIL",
        "stage": name,
        "run_id": run_id,
        "worker_threads": worker_threads,
        "elapsed_wall_seconds": time.monotonic() - started,
        "exit_code": exit_code,
        "workload_profile": profile,
        "workload_report": static,
        "runtime_closure": dict(runtime_binding.record),
        "harness_source_binding": dict(harness_source_binding),
        "shared_runtime_bundle": _runtime_bundle_references(
            out_root, runtime_binding
        ),
        "simulator": {
            "source_path": runtime_binding.record.get("source_executable_path"),
            "path": runtime_binding.record["bundle_executable_path"],
            "execution_copy": None,
            "sha256": runtime_binding.record["bundle_executable_sha256"],
            "runtime_closure_identity_sha256": runtime_binding.identity_sha256,
        },
        "inputs": {
            key: {"path": str(source), "sha256": runner.sha256_file(source)}
            for key, (source, _) in copies.items()
        },
        "command": command,
        "environment_overrides": overrides,
        "cleared_inherited_control_variables": inherited_controls,
        "runtime_config_binding": runtime_config_binding,
        "runtime_execution": runtime_execution,
        "ecmp_route_candidate_evidence": route_evidence_record,
        "training_source_port_allocator_evidence": allocator_evidence_record,
        "resource_observation": resources,
        "evidence": evidence,
        "errors": errors,
        "artifacts": artifacts,
        "artifact_set_sha256": runner.canonical_hash(artifacts),
        "publish_protocol": "pending_directory_then_atomic_rename",
    }
    _verify_harness_source_binding(
        harness_source_binding, phase=f"{name}_before_publish"
    )
    _write_seal(pending, manifest)
    final.parent.mkdir(parents=True, exist_ok=True)
    pending.rename(final)
    return validate_sealed_stage(final)


def qualify_platform(
    *,
    binary: Path,
    topology: Path,
    link_map: Path,
    config: Path,
    out_root: Path,
    workloads: Sequence[Path] = DEFAULT_WORKLOADS,
    worker_threads: int = 1,
    wall_timeout_s: float = 900.0,
    _runtime_authority: Any = None,
) -> Mapping[str, Any]:
    harness_source_binding = _capture_harness_source_binding()
    _verify_harness_source_binding(
        harness_source_binding, phase="qualification_start"
    )
    if len(workloads) != 4:
        raise QualificationError("exactly four workloads are required")
    if isinstance(worker_threads, bool) or not 1 <= worker_threads <= 256:
        raise QualificationError("worker_threads must be in [1, 256]")
    try:
        binary = binary.resolve(strict=True)
    except OSError as exc:
        raise QualificationError(f"simulator binary is missing: {binary}") from exc
    paths = [binary, topology, link_map, config, *workloads]
    for path in paths:
        if path != binary and path.is_symlink():
            raise QualificationError(f"input missing or symlinked: {path}")
        if not path.is_file():
            raise QualificationError(f"input missing or symlinked: {path}")
    if not os.access(binary, os.X_OK):
        raise QualificationError(f"simulator binary is not executable: {binary}")
    try:
        topology_result = runner._validate_topology(topology, 16)
        runner._validate_link_map_against_topology(link_map, topology, topology_result)
    except runner.RunnerError as exc:
        raise QualificationError(str(exc)) from exc
    if out_root.is_symlink() or (out_root.exists() and not out_root.is_dir()):
        raise QualificationError(f"output root is not a plain directory: {out_root}")
    if out_root.exists() and any(out_root.iterdir()):
        raise QualificationError(f"refusing non-empty output root: {out_root}")
    out_root.mkdir(parents=True, exist_ok=True)
    execution_root = out_root.resolve()
    runtime_authority = _runtime_authority or runner.SealedRuntimeAuthority()
    try:
        runtime_binding = runtime_authority.prepare(execution_root, binary)
    except (runner.RunnerError, runtime_bundle.RuntimeBundleError) as exc:
        raise _as_qualification_error("preparation", exc) from exc
    if (
        runtime_binding.bundle is None
        or runtime_binding.source_closure is None
        or runtime_binding.identity_sha256
        != runtime_binding.source_closure.identity_sha256
    ):
        raise QualificationError(
            "platform qualification requires a production ELF runtime closure"
        )
    shared_bundle = _runtime_bundle_references(execution_root, runtime_binding)
    bundle_children = list(
        (execution_root / runtime_bundle.BUNDLE_ROOT_NAME).iterdir()
    )
    if (
        len(bundle_children) != 1
        or bundle_children[0].is_symlink()
        or bundle_children[0].resolve() != runtime_binding.bundle_root
    ):
        raise QualificationError(
            "qualification must use exactly one content-addressed runtime bundle"
        )
    stages: List[Mapping[str, Any]] = []
    profiles = ("completion", "completion", "completion", "horizon-prefix")
    for index, (workload_path, profile) in enumerate(zip(workloads, profiles), 1):
        result = _run_stage(
            name=f"Q{index}",
            workload_path=workload_path,
            profile=profile,
            runtime_binding=runtime_binding,
            runtime_authority=runtime_authority,
            topology=topology.resolve(),
            link_map=link_map.resolve(),
            config=config.resolve(),
            out_root=out_root.resolve(),
            worker_threads=worker_threads,
            wall_timeout_s=wall_timeout_s,
            harness_source_binding=harness_source_binding,
        )
        _verify_harness_source_binding(
            harness_source_binding, phase=f"Q{index}_after_stage"
        )
        stages.append(result)
        if result["status"] != "PASS":
            break
    # Re-open every published stage after the sequence so a later stage cannot
    # silently diverge from an earlier runtime identity.
    stages = [
        validate_sealed_stage(execution_root / "stages" / stage["stage"])
        for stage in stages
    ]
    if {
        stage["runtime_closure"]["identity_sha256"] for stage in stages
    } != {runtime_binding.identity_sha256}:
        raise QualificationError("published stages used mixed runtime identities")
    if {
        stage.get("harness_source_binding", {}).get("identity_sha256")
        for stage in stages
    } != {harness_source_binding["identity_sha256"]}:
        raise QualificationError("published stages used mixed Python harness sources")
    _verify_harness_source_binding(
        harness_source_binding, phase="qualification_summary"
    )
    summary = {
        "schema_version": SCHEMA,
        "status": "PASS"
        if len(stages) == 4 and all(stage["status"] == "PASS" for stage in stages)
        else "FAIL",
        "worker_threads": worker_threads,
        "binary_sha256": runner.sha256_file(binary),
        "runtime_closure_identity_sha256": runtime_binding.identity_sha256,
        "harness_source_binding": dict(harness_source_binding),
        "shared_runtime_bundle": shared_bundle,
        "completed_stage_count": len(stages),
        "stages": [
            {
                "stage": stage["stage"],
                "status": stage["status"],
                "manifest": str(
                    out_root / "stages" / stage["stage"] / "run_manifest.json"
                ),
                "manifest_sha256": runner.sha256_file(
                    out_root / "stages" / stage["stage"] / "run_manifest.json"
                ),
            }
            for stage in stages
        ],
        "fail_stop": True,
    }
    runner._write_json_atomic(out_root / "qualification.json", summary)
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--topology", required=True, type=Path)
    parser.add_argument("--link-map", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--out-root", required=True, type=Path)
    parser.add_argument(
        "--simulator-worker-threads",
        "--worker-threads",
        dest="simulator_worker_threads",
        type=int,
        default=1,
    )
    parser.add_argument("--wall-timeout-s", type=float, default=900.0)
    args = parser.parse_args(argv)
    try:
        result = qualify_platform(
            binary=args.binary,
            topology=args.topology,
            link_map=args.link_map,
            config=args.config,
            out_root=args.out_root,
            worker_threads=args.simulator_worker_threads,
            wall_timeout_s=args.wall_timeout_s,
        )
    except (QualificationError, runner.RunnerError, OSError) as exc:
        print(f"platform qualification refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
