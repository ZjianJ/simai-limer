#!/usr/bin/env python3
"""Publish a P2 EXECUTED corpus only from a complete sealed run set.

The runner deliberately publishes one run at a time.  This finalizer is the
only bridge from that per-run evidence to an ``EXECUTED`` corpus manifest.  It
is intentionally fail closed: an incomplete or invalid run set produces only
``readiness_audit.json`` and never a partial executed corpus or split.
"""

from __future__ import annotations

import argparse
import csv
import copy
import hashlib
import json
import os
import shutil
import sys
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import run_true16_p2_corpus as runner
import generate_true16_p2_corpus as corpus_generator
import simulator_runtime_bundle as runtime_bundle
import validate_p2_workload_runtime as workload_runtime
import validate_training_source_port_allocator as training_source_port_runtime


EXPECTED_RUN_COUNT = 462
READINESS_SCHEMA = "limer.p2-readiness-audit.v1"
CORPUS_SCHEMA = "limer.p2-corpus-manifest.v1"
SPLIT_SCHEMA = "limer.p2-split-manifest.v1"
IDENTITY_SCHEMA = "limer.p2-corpus-identity.v2"
SEMANTIC_SCHEMA = "limer.p2-run-semantics.v1"
RUNTIME_QUALIFICATION_SCHEMA = "limer.p2-workload-runtime-qualification.v1"
SIMULATOR_STABILITY_SCHEMA = "limer.p2-simulator-stability.v1"

REQUIRED_SEALED_ARTIFACTS: Mapping[str, str] = {
    "link_map": "link_map.csv",
    "switch_telemetry": "switch_telemetry.csv",
    "nic_telemetry": "nic_telemetry.csv",
    "collective_telemetry": "collective_telemetry.csv",
    "collective_transaction": "collective_transaction.csv",
    "fault_application_telemetry": "fault_application_telemetry.csv",
    "run_lifecycle": "run_lifecycle.csv",
    "semantic_validation": "semantic_validation.json",
    "workload_runtime_qualification": "workload_runtime_qualification.json",
    "runtime_execution_evidence": "runtime_execution_evidence.json",
    "ecmp_route_candidates": "ecmp_route_candidates.csv",
    "ecmp_route_candidate_validation": "ecmp_route_candidate_validation.json",
    "training_source_port_allocator": (
        training_source_port_runtime.RAW_FILENAME
    ),
    "training_source_port_allocator_validation": (
        training_source_port_runtime.REPORT_FILENAME
    ),
}

OPTIONAL_SEALED_ARTIFACTS: Mapping[str, str] = {
    "background_flow_application": "background_flow_application.csv",
    "rdma_wc_telemetry": "rdma_wc_telemetry.csv",
    "invocation": "invocation.json",
    "run_log": "run.log",
    "exit_code": "exit_code.txt",
    "simulator_stability": "simulator_stability.json",
}

SIMULATOR_STABILITY_SOURCE_ARTIFACTS: Mapping[str, str] = {
    "exit_code": "exit_code",
    "run_lifecycle": "run_lifecycle",
    "switch_telemetry": "switch_telemetry",
    "nic_telemetry": "nic_telemetry",
    "collective_telemetry": "collective_telemetry",
    "collective_transaction": "collective_transaction",
    "fault_application_telemetry": "fault_application_telemetry",
    "runtime_execution_evidence": "runtime_execution_evidence",
    "semantic_validation": "semantic_validation",
    "workload_runtime_qualification": "workload_runtime_qualification",
    "ecmp_route_candidates": "ecmp_route_candidates",
    "ecmp_route_candidate_validation": "ecmp_route_candidate_validation",
    "training_source_port_allocator": "training_source_port_allocator",
    "training_source_port_allocator_validation": (
        "training_source_port_allocator_validation"
    ),
}
SIMULATOR_STABILITY_CHECK_NAMES = (
    "planned_gate_is_pending_execution",
    "simulator_process_exited_zero",
    "observation_window_complete",
    "no_timeout_interrupt_or_oom",
    "sealed_runtime_execution_validated",
    "mechanism_semantics_and_telemetry_validated",
    "workload_runtime_validated",
    "ecmp_route_candidates_validated",
    "training_source_port_allocator_validated",
)


class FinalizationError(RuntimeError):
    """The requested finalization cannot be performed safely."""


class RuntimeAuditError(FinalizationError):
    """A sealed simulator runtime failed one specific finalization gate."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class CurrentSimulatorRuntime:
    """The closure independently rediscovered from the canonical launcher."""

    identity_sha256: str
    source_evidence: Mapping[str, Any]
    closure: Optional[runtime_bundle.RuntimeClosure]


@dataclass(frozen=True)
class AuditedRuntimeBinding:
    """A run's exact shared-bundle authority and finalizer audit evidence."""

    binding: runner.SimulatorRuntimeBinding
    evidence: Mapping[str, Any]


def _closure_evidence(
    closure: runtime_bundle.RuntimeClosure,
) -> Dict[str, Any]:
    """Record the path-and-byte authority used in closure comparison."""

    return {
        "identity_sha256": closure.identity_sha256,
        "executable": {
            "path": str(closure.executable),
            "sha256": closure.executable_sha256,
            "size_bytes": closure.executable_size_bytes,
        },
        "project_dependencies": [
            {
                "soname": dependency.soname,
                "path": str(dependency.resolved_path),
                "sha256": dependency.sha256,
                "size_bytes": dependency.size_bytes,
            }
            for dependency in closure.project_dependencies
        ],
        "system_dependencies": [
            {
                "soname": dependency.soname,
                "path": str(dependency.resolved_path),
                "sha256": dependency.sha256,
                "size_bytes": dependency.size_bytes,
            }
            for dependency in closure.system_dependencies
        ],
        "virtual_dependencies": list(closure.virtual_dependencies),
        "identity_material_sha256": runtime_bundle.canonical_hash(
            closure.identity_material()
        ),
    }


class FinalizerRuntimeAuthority(runner.SealedRuntimeAuthority):
    """Production-only authority for an independent finalizer closure audit.

    Unlike the runner, this class never creates a bundle.  It rediscovers the
    current source closure, opens only the bundle named by each sealed run,
    checks that bundle against the current closure (including system-library
    canonical paths and hashes), and proves loader resolution again.
    """

    def discover_current(self, source_binary: Path) -> CurrentSimulatorRuntime:
        try:
            closure = runtime_bundle.discover_runtime_closure(source_binary)
            runtime_bundle.verify_discovered_sources(closure)
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_INVALID",
                f"cannot discover current canonical simulator closure: {exc}",
            ) from exc
        return CurrentSimulatorRuntime(
            identity_sha256=closure.identity_sha256,
            source_evidence=_closure_evidence(closure),
            closure=closure,
        )

    @staticmethod
    def _relative_runtime_path(
        execution_root: Path,
        raw_path: Any,
        expected: Path,
        description: str,
    ) -> Path:
        if not isinstance(raw_path, str) or not raw_path:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                f"runtime closure lacks {description}",
            )
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                f"runtime closure has unsafe {description}: {raw_path!r}",
            )
        actual = execution_root / relative
        if actual != expected:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                f"runtime closure {description} is non-canonical: "
                f"expected={expected}, observed={actual}",
            )
        return actual

    def validate_record(
        self,
        execution_root: Path,
        recorded: Mapping[str, Any],
        current: CurrentSimulatorRuntime,
    ) -> AuditedRuntimeBinding:
        if current.closure is None:
            raise RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_INVALID",
                "production runtime audit lacks a discovered ELF closure",
            )
        root = execution_root.resolve(strict=True)
        identity = recorded.get("identity_sha256")
        if not isinstance(identity, str) or not runner.SHA256_RE.fullmatch(identity):
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "run manifest runtime closure identity is invalid",
            )
        if identity != current.identity_sha256:
            raise RuntimeAuditError(
                "RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR",
                "run runtime closure differs from the currently discovered "
                f"canonical simulator closure: run={identity}, "
                f"current={current.identity_sha256}",
            )
        if recorded.get("execution_root") != str(root):
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "run runtime authority names a different execution root",
            )

        bundle_root = root / runtime_bundle.BUNDLE_ROOT_NAME / identity
        self._relative_runtime_path(
            root, recorded.get("bundle_path"), bundle_root, "bundle_path"
        )
        manifest_path = bundle_root / runtime_bundle.MANIFEST_NAME
        seal_path = bundle_root / runtime_bundle.SEAL_NAME
        executable_path = bundle_root / runtime_bundle.EXECUTABLE_RELATIVE
        self._relative_runtime_path(
            root,
            recorded.get("bundle_manifest_path"),
            manifest_path,
            "bundle_manifest_path",
        )
        self._relative_runtime_path(
            root,
            recorded.get("bundle_manifest_seal_path"),
            seal_path,
            "bundle_manifest_seal_path",
        )
        self._relative_runtime_path(
            root,
            recorded.get("bundle_executable_path"),
            executable_path,
            "bundle_executable_path",
        )
        try:
            runtime_bundle.verify_discovered_sources(current.closure)
            bundle = runtime_bundle.validate_runtime_bundle(
                bundle_root,
                expected_closure=current.closure,
            )
            loader = runtime_bundle.verify_loader_resolution(bundle)
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_BUNDLE",
                f"shared simulator runtime bundle validation failed: {exc}",
            ) from exc
        if loader.identity_sha256 != current.identity_sha256:
            raise RuntimeAuditError(
                "RUNTIME_LOADER_RESOLUTION_MISMATCH",
                "sealed loader preflight resolved a different runtime closure",
            )

        expected_record = runner._runtime_record(root, bundle)
        if dict(recorded) != expected_record:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "run manifest runtime authority differs from the independently "
                "validated shared bundle",
            )
        manifest_ref = _file_ref(manifest_path)
        seal_ref = _file_ref(seal_path)
        if recorded.get("bundle_manifest_sha256") != manifest_ref["sha256"]:
            raise RuntimeAuditError(
                "INVALID_RUNTIME_CLOSURE_BINDING",
                "run runtime authority does not bind the current bundle manifest",
            )
        binding = runner.SimulatorRuntimeBinding(
            execution_root=root,
            bundle_root=bundle.root,
            executable=bundle.executable,
            identity_sha256=bundle.identity_sha256,
            manifest_path=manifest_path,
            manifest_sha256=manifest_ref["sha256"],
            record=expected_record,
            loader_preflight={
                **runner._closure_loader_evidence(loader),
                "phase": "finalizer_independent_loader_preflight",
            },
            bundle=bundle,
            source_closure=current.closure,
        )
        return AuditedRuntimeBinding(
            binding=binding,
            evidence={
                "status": "PASS",
                "identity_sha256": identity,
                "authority_record_sha256": runner.canonical_hash(expected_record),
                "bundle_path": str(bundle.root),
                "bundle_manifest": manifest_ref,
                "bundle_manifest_seal": seal_ref,
                "bundle_artifact_set_sha256": bundle.manifest.get(
                    "artifact_set_sha256"
                ),
                "loader_resolution_identity_sha256": loader.identity_sha256,
                "project_dependency_count": len(loader.project_dependencies),
                "system_dependency_count": len(loader.system_dependencies),
                "shared_bundle_outside_runs": (
                    bundle.root.parent == root / runtime_bundle.BUNDLE_ROOT_NAME
                    and bundle.root.parent != root / "runs"
                ),
            },
        )

    def verify_current(self, current: CurrentSimulatorRuntime) -> None:
        if current.closure is None:
            raise RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_INVALID",
                "production runtime audit lacks a discovered ELF closure",
            )
        try:
            runtime_bundle.verify_discovered_sources(current.closure)
        except (runtime_bundle.RuntimeBundleError, OSError) as exc:
            raise RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_CHANGED",
                f"current simulator closure changed during finalization: {exc}",
            ) from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_ref(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": runner.sha256_file(path),
    }


def _load_json(path: Path, description: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FinalizationError(f"cannot read {description}: {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise FinalizationError(f"{description} must contain one JSON object: {path}")
    return value


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _reject_existing_symlink_components(path: Path, description: str) -> None:
    """Reject a caller-supplied path that is redirected by an existing link."""

    absolute = _lexical_absolute(path)
    components = [absolute, *absolute.parents]
    for component in reversed(components):
        if component.exists() or component.is_symlink():
            if component.is_symlink():
                raise FinalizationError(
                    f"{description} traverses symbolic link: {component}"
                )


def _require_plain_file(path: Path, description: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise FinalizationError(f"{description} is missing or is a symlink: {path}")
    return path.resolve(strict=True)


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        return False
    return True


def _tree_symlinks(root: Path) -> List[str]:
    if root.is_symlink():
        return [str(root)]
    try:
        return [str(path) for path in root.rglob("*") if path.is_symlink()]
    except OSError as exc:
        raise FinalizationError(
            f"cannot inspect sealed run tree {root}: {exc}"
        ) from exc


def _safe_output_path(
    output_dir: Path, source_root: Path, execution_root: Path
) -> Path:
    output = _lexical_absolute(output_dir)
    _reject_existing_symlink_components(output, "output directory")
    if output.exists():
        if not output.is_dir():
            raise FinalizationError(f"output path is not a directory: {output}")
        if any(output.iterdir()):
            raise FinalizationError(f"refusing to overwrite non-empty output: {output}")

    source = source_root.resolve(strict=True)
    execution = (
        execution_root.resolve(strict=True)
        if execution_root.exists()
        else (_lexical_absolute(execution_root))
    )
    if output in {source, execution}:
        raise FinalizationError("output directory cannot replace an input root")
    runs_root = execution / "runs"
    try:
        output.relative_to(runs_root)
    except ValueError:
        pass
    else:
        raise FinalizationError("output directory cannot be inside sealed runs/")
    try:
        source.relative_to(output)
    except ValueError:
        pass
    else:
        raise FinalizationError("output directory cannot contain the source corpus")
    try:
        execution.relative_to(output)
    except ValueError:
        pass
    else:
        raise FinalizationError("output directory cannot contain the execution root")
    return output


def _write_new(path: Path, payload: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _publish_bundle(output_dir: Path, payloads: Mapping[str, bytes]) -> None:
    """Publish a complete directory bundle with one directory rename."""

    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    _reject_existing_symlink_components(parent, "output parent")
    lock = parent / f".{output_dir.name}.finalize.lock"
    try:
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError as exc:
        raise FinalizationError(f"concurrent finalization lock exists: {lock}") from exc
    stage = parent / f".{output_dir.name}.pending.{os.getpid()}.{uuid.uuid4().hex}"
    try:
        os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
        os.fsync(descriptor)
        if output_dir.exists():
            if output_dir.is_symlink() or not output_dir.is_dir():
                raise FinalizationError(f"unsafe output path: {output_dir}")
            if any(output_dir.iterdir()):
                raise FinalizationError(
                    f"refusing to overwrite non-empty output: {output_dir}"
                )
        stage.mkdir(mode=0o755)
        for name, payload in sorted(payloads.items()):
            if Path(name).name != name:
                raise FinalizationError(f"unsafe output filename: {name!r}")
            _write_new(stage / name, payload)
        directory_fd = os.open(stage, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if output_dir.exists():
            output_dir.rmdir()
        os.rename(stage, output_dir)
        parent_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        os.close(descriptor)
        lock.unlink(missing_ok=True)
        if stage.exists():
            shutil.rmtree(stage)


def _resolve_source_ref(
    ref: Any,
    corpus: runner.ValidatedCorpus,
    description: str,
) -> Dict[str, Any]:
    if not isinstance(ref, Mapping):
        raise FinalizationError(f"{description} reference is not an object")
    raw_path = ref.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise FinalizationError(f"{description} reference lacks path")
    candidate = Path(raw_path)
    if candidate.is_absolute():
        _reject_existing_symlink_components(candidate, description)
        path = _require_plain_file(candidate, description)
    else:
        lexical = corpus.root / candidate
        _reject_existing_symlink_components(lexical, description)
        try:
            path = runner.resolve_sidecar(corpus.root, raw_path, description)
        except runner.RunnerError as exc:
            raise FinalizationError(str(exc)) from exc
    expected = ref.get("sha256")
    actual = runner.sha256_file(path)
    if expected != actual:
        raise FinalizationError(
            f"{description} hash mismatch: expected={expected!r}, actual={actual}"
        )
    result = copy.deepcopy(dict(ref))
    result.update(
        {"path": str(path), "sha256": actual, "size_bytes": path.stat().st_size}
    )
    return result


def _absolutize_prepared_refs(
    manifest: MutableMapping[str, Any],
    corpus: runner.ValidatedCorpus,
) -> None:
    inputs = manifest.get("input_artifacts")
    if not isinstance(inputs, MutableMapping):
        raise FinalizationError("prepared corpus input_artifacts is invalid")
    for name in ("contract", "link_map", "topology", "workload", "simulator_config"):
        inputs[name] = _resolve_source_ref(inputs.get(name), corpus, f"input {name}")

    for name in ("feature_schema", "leakage_checks", "observability_report"):
        manifest[name] = _resolve_source_ref(
            manifest.get(name), corpus, f"corpus {name}"
        )
    topology = manifest.get("topology")
    if isinstance(topology, MutableMapping) and isinstance(
        topology.get("link_map"), Mapping
    ):
        topology["link_map"] = _resolve_source_ref(
            topology["link_map"], corpus, "topology link_map"
        )

    runs = manifest.get("runs", [])
    for run in runs:
        if not isinstance(run, MutableMapping):
            raise FinalizationError("prepared run is not an object")
        schedule = run.get("schedule")
        if not isinstance(schedule, MutableMapping):
            raise FinalizationError(f"{run.get('run_id')}: schedule is invalid")
        resolved = _resolve_source_ref(
            schedule, corpus, f"{run.get('run_id')} truth schedule"
        )
        schedule.update(resolved)
        for key, label in (
            ("simulator_injection_schedule", "simulator injection schedule"),
            ("background_flow_schedule", "background flow schedule"),
        ):
            if isinstance(schedule.get(key), Mapping):
                schedule[key] = _resolve_source_ref(
                    schedule[key], corpus, f"{run.get('run_id')} {label}"
                )


def _source_split(
    corpus: runner.ValidatedCorpus,
) -> Tuple[Mapping[str, Any], Dict[str, Any]]:
    ref = corpus.manifest.get("split_manifest")
    normalized = _resolve_source_ref(ref, corpus, "split manifest")
    path = Path(normalized["path"])
    split = _load_json(path, "split manifest")
    if split.get("schema_version") != SPLIT_SCHEMA:
        raise FinalizationError(
            f"unsupported split schema: {split.get('schema_version')!r}"
        )
    if split.get("corpus_id") != corpus.manifest.get("corpus_id"):
        raise FinalizationError("split manifest corpus_id differs from prepared corpus")
    entries = split.get("entries")
    if not isinstance(entries, list) or not all(
        isinstance(item, Mapping) for item in entries
    ):
        raise FinalizationError("split manifest entries are invalid")
    expected = {
        str(run["run_id"]): str(run["partition"]) for run in corpus.manifest["runs"]
    }
    observed: Dict[str, str] = {}
    duplicates: List[str] = []
    for item in entries:
        run_id = str(item.get("run_id", ""))
        if run_id in observed:
            duplicates.append(run_id)
        observed[run_id] = str(item.get("partition", ""))
    if duplicates or observed != expected:
        raise FinalizationError(
            "split manifest does not bind the exact prepared run inventory"
        )
    return split, normalized


def _declared_block_reasons(run: Mapping[str, Any]) -> List[str]:
    reasons: List[str] = []
    schedule = run.get("schedule", {})
    mechanism = run.get("mechanism", {})
    schedule_status = str(
        schedule.get("implementation_status", "")
        if isinstance(schedule, Mapping)
        else ""
    ).upper()
    mechanism_status = str(
        mechanism.get("implementation_status", "")
        if isinstance(mechanism, Mapping)
        else ""
    ).upper()
    if schedule_status.startswith("BLOCKED"):
        reasons.append(f"DECLARED_SCHEDULE_{schedule_status}")
    if mechanism_status in {"BLOCKED", "UNAVAILABLE"}:
        reasons.append(f"DECLARED_MECHANISM_{mechanism_status}")
    return reasons


def _inventory_index(
    manifest: Mapping[str, Any],
    run_dir: Path,
) -> Tuple[Dict[str, Mapping[str, Any]], List[str]]:
    """Index inventory already byte-verified by ``validate_final_run``.

    The caller invokes the runner validator immediately before this helper;
    rehashing every large telemetry file here would double finalization I/O.
    This pass adds the path/symlink/schema checks absent from the runner.
    """

    errors: List[str] = []
    raw = manifest.get("artifacts")
    if not isinstance(raw, list):
        return {}, ["runner manifest artifact inventory is missing"]
    index: Dict[str, Mapping[str, Any]] = {}
    for entry in raw:
        if not isinstance(entry, Mapping):
            errors.append("runner artifact inventory contains a non-object entry")
            continue
        relative = entry.get("path")
        if not isinstance(relative, str) or not relative:
            errors.append("runner artifact inventory contains an empty path")
            continue
        parsed = Path(relative)
        if parsed.is_absolute() or ".." in parsed.parts or relative in index:
            errors.append(f"unsafe or duplicate runner artifact path: {relative!r}")
            continue
        path = run_dir / parsed
        if path.is_symlink() or not path.is_file() or not _path_within(path, run_dir):
            errors.append(f"runner artifact is missing, linked, or escaped: {relative}")
            continue
        actual_size = path.stat().st_size
        sha = entry.get("sha256")
        if (
            not isinstance(sha, str)
            or not runner.SHA256_RE.fullmatch(sha)
            or entry.get("size_bytes") != actual_size
        ):
            errors.append(f"runner artifact digest/size is invalid: {relative}")
            continue
        index[relative] = entry
    return index, errors


def _sealed_ref(
    run_dir: Path,
    relative: str,
    inventory: Mapping[str, Mapping[str, Any]],
) -> Tuple[Dict[str, Any] | None, str | None]:
    entry = inventory.get(relative)
    if entry is None:
        return None, f"required sealed artifact is absent: {relative}"
    path = run_dir / relative
    if path.is_symlink() or not path.is_file() or not _path_within(path, run_dir):
        return None, f"required sealed artifact is unsafe: {relative}"
    return {
        "path": str(path.resolve(strict=True)),
        "size_bytes": int(entry["size_bytes"]),
        "sha256": str(entry["sha256"]),
    }, None


def _passing_report(
    path: Path,
    *,
    schema: str,
    run_id: str,
    description: str,
    mechanism_id: str | None = None,
) -> Tuple[Mapping[str, Any] | None, List[str]]:
    errors: List[str] = []
    try:
        report = _load_json(path, description)
    except FinalizationError as exc:
        return None, [str(exc)]
    if report.get("schema_version") != schema:
        errors.append(
            f"{description} schema={report.get('schema_version')!r}, expected={schema}"
        )
    if report.get("status") != "PASS":
        errors.append(f"{description} status is not PASS")
    if report.get("run_id") != run_id:
        errors.append(f"{description} run_id differs from planned run")
    if mechanism_id is not None and report.get("mechanism_id") != mechanism_id:
        errors.append(f"{description} mechanism_id differs from planned mechanism")
    checks = report.get("checks")
    if not isinstance(checks, list) or not checks:
        errors.append(f"{description} has no nonempty checks list")
    elif any(
        not isinstance(check, Mapping) or check.get("status") != "PASS"
        for check in checks
    ):
        errors.append(f"{description} contains a non-PASS check")
    return report, errors


def _training_source_port_errors(
    run: Mapping[str, Any],
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    """Independently replay the strict GENERAL allocator evidence profile."""

    errors: List[str] = []
    raw_ref = artifacts.get("training_source_port_allocator")
    report_ref = artifacts.get("training_source_port_allocator_validation")
    if not isinstance(raw_ref, Mapping) or not isinstance(report_ref, Mapping):
        return ["sealed training source-port allocator artifacts are missing"]
    raw_path = Path(str(raw_ref.get("path", "")))
    report_path = Path(str(report_ref.get("path", "")))
    try:
        raw_sha = runner.sha256_file(raw_path)
        report_sha = runner.sha256_file(report_path)
    except OSError as exc:
        return [f"training source-port allocator artifact is unreadable: {exc}"]
    if raw_ref.get("sha256") != raw_sha:
        errors.append("training source-port raw hash differs from sealed artifact")
    if report_ref.get("sha256") != report_sha:
        errors.append("training source-port report hash differs from sealed artifact")
    if (
        manifest.get("training_source_port_allocator_validation_status") != "PASS"
        or manifest.get("training_source_port_allocator_sha256") != raw_sha
        or manifest.get("training_source_port_allocator_validation_sha256")
        != report_sha
    ):
        errors.append("run manifest allocator status/hash binding is invalid")
    try:
        recorded = _load_json(
            report_path, "training source-port allocator validation"
        )
    except FinalizationError as exc:
        return errors + [str(exc)]
    try:
        recomputed = training_source_port_runtime.validate_allocator_evidence(
            raw_path,
            expected_run_id=str(run.get("run_id", "")),
            require_reuse=False,
        )
    except training_source_port_runtime.AllocatorEvidenceError as exc:
        return errors + [
            f"training source-port raw-evidence recomputation failed: {exc}"
        ]
    if recomputed.get("status") != "PASS":
        errors.append(
            "training source-port raw-evidence recomputation failed: "
            + "; ".join(str(item) for item in recomputed.get("errors", [])[:8])
        )
    if dict(recorded) != recomputed:
        errors.append(
            "training source-port allocator report differs from independent "
            "raw-evidence recomputation"
        )
    if manifest.get(
        "training_source_port_allocator_report_sha256"
    ) != recomputed.get("report_sha256"):
        errors.append("run manifest allocator report identity is invalid")
    artifact = recomputed.get("artifact")
    requirements = recomputed.get("requirements")
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("path") != training_source_port_runtime.RAW_FILENAME
        or artifact.get("sha256") != raw_sha
        or artifact.get("size_bytes") != raw_path.stat().st_size
        or artifact.get("row_count") != 1
        or not isinstance(requirements, Mapping)
        or requirements.get("require_reuse") is not False
        or recomputed.get("profile") != "GENERAL"
    ):
        errors.append("training source-port GENERAL profile binding is invalid")
    return errors


def _planned_stability_gate(
    run: Mapping[str, Any],
) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    raw = run.get("simulator_stability")
    if not isinstance(raw, Mapping):
        if run.get("fault_family") == "random_loss":
            errors.append("random_loss lacks its prepared simulator stability gate")
        return False, errors
    gate_required = raw.get("gate_required") is True
    if run.get("fault_family") == "random_loss" and not gate_required:
        errors.append("random_loss prepared run does not require simulator stability")
    if gate_required and raw.get("status") != "PENDING_EXECUTION":
        errors.append(
            "prepared simulator stability state is not PENDING_EXECUTION"
        )
    return gate_required, errors


def _stability_lifecycle_facts(
    path: Path,
    run_id: str,
    virtual_finish_ns: int,
) -> Tuple[bool, bool, List[str]]:
    errors: List[str] = []
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {
                "run_id", "event", "scheduled_ns", "actual_ns",
                "finished_ranks", "world_size", "status",
            }
            if not required.issubset(reader.fieldnames or []):
                return False, False, ["stability lifecycle schema is invalid"]
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        return False, False, [f"stability lifecycle is unreadable: {exc}"]
    if any(row.get("run_id") != run_id for row in rows):
        errors.append("stability lifecycle contains a foreign run_id")
    observation_complete = False
    for row in rows:
        if row.get("event") != "observation_horizon" or not str(
            row.get("status", "")
        ).startswith("OBSERVATION_WINDOW_COMPLETE"):
            continue
        try:
            observation_complete = (
                int(row["scheduled_ns"]) == virtual_finish_ns
                and int(row["actual_ns"]) >= virtual_finish_ns
                and int(row["world_size"]) == 16
            )
        except (TypeError, ValueError):
            errors.append("stability lifecycle has invalid numeric fields")
        if observation_complete:
            break
    if not observation_complete:
        errors.append("stability lifecycle did not complete the observation horizon")
    workload_completed = any(
        row.get("status") == "WORKLOAD_COMPLETE" for row in rows
    )
    return observation_complete, workload_completed, errors


def _simulator_stability_errors(
    run: Mapping[str, Any],
    run_dir: Path,
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    """Independently reconstruct the stability report from sealed raw files."""

    errors: List[str] = []
    gate_required, gate_errors = _planned_stability_gate(run)
    errors.extend(gate_errors)
    report_ref = artifacts.get("simulator_stability")
    if manifest.get("simulator_stability_gate_required") is not gate_required:
        errors.append("run manifest changed the planned simulator stability gate")
    if not gate_required:
        if (
            manifest.get("simulator_stability_status") != "NOT_REQUIRED"
            or manifest.get("simulator_stability_sha256") is not None
            or report_ref is not None
        ):
            errors.append("non-gated run has simulator stability PASS evidence")
        return errors
    if report_ref is None or artifacts.get("exit_code") is None:
        errors.append("gated run lacks sealed stability or exit-code evidence")
        return errors

    source_hashes: Dict[str, str] = {}
    for source_name, artifact_name in SIMULATOR_STABILITY_SOURCE_ARTIFACTS.items():
        ref = artifacts.get(artifact_name)
        if not isinstance(ref, Mapping):
            errors.append(f"stability source artifact is absent: {artifact_name}")
            continue
        path = Path(str(ref.get("path", "")))
        try:
            actual = runner.sha256_file(path)
        except OSError as exc:
            errors.append(f"stability source artifact is unreadable: {exc}")
            continue
        if actual != ref.get("sha256"):
            errors.append(f"stability source artifact hash differs: {artifact_name}")
        source_hashes[source_name] = actual

    exit_path = Path(str(artifacts["exit_code"]["path"]))
    try:
        exit_text = exit_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        errors.append(f"stability exit-code evidence is unreadable: {exc}")
        exit_text = ""
    if exit_text != "0" or manifest.get("exit_code") != 0:
        errors.append("stability process exit evidence is not zero")

    execution = manifest.get("execution")
    if not isinstance(execution, Mapping):
        errors.append("stability execution evidence is missing")
        execution = {}
    resource = execution.get("resource_observation")
    if (
        execution.get("process_status") != "EXITED_ZERO"
        or execution.get("timed_out") is not False
        or execution.get("interrupted") is not False
        or not isinstance(resource, Mapping)
        or resource.get("oom_kill_observed_during_attempt") is not False
    ):
        errors.append("stability process/timeout/interrupt/OOM evidence is invalid")

    run_id = str(run.get("run_id", ""))
    _, workload_completed, lifecycle_errors = _stability_lifecycle_facts(
        Path(str(artifacts["run_lifecycle"]["path"])),
        run_id,
        int(run["virtual_finish_ns"]),
    )
    errors.extend(lifecycle_errors)
    lifecycle = manifest.get("lifecycle")
    if (
        manifest.get("execution_status") != "COMPLETE"
        or not isinstance(lifecycle, Mapping)
        or lifecycle.get("observation_status") != "OBSERVATION_WINDOW_COMPLETE"
        or manifest.get("workload_completed") is not workload_completed
    ):
        errors.append("stability lifecycle differs from sealed raw evidence")

    status_fields = {
        "runtime_execution_evidence": "runtime_execution_status",
        "semantic_validation": "semantic_validation_status",
        "workload_runtime_qualification": (
            "workload_runtime_qualification_status"
        ),
        "ecmp_route_candidate_validation": (
            "ecmp_route_candidate_validation_status"
        ),
        "training_source_port_allocator_validation": (
            "training_source_port_allocator_validation_status"
        ),
    }
    for source_name, manifest_field in status_fields.items():
        ref = artifacts.get(source_name)
        try:
            report = _load_json(Path(str(ref["path"])), source_name) \
                if isinstance(ref, Mapping) else {}
        except FinalizationError as exc:
            errors.append(str(exc))
            report = {}
        if manifest.get(manifest_field) != "PASS" or report.get("status") != "PASS":
            errors.append(f"stability prerequisite is not PASS: {source_name}")

    closure = manifest.get("runtime_closure")
    identity = closure.get("identity_sha256") if isinstance(closure, Mapping) else None
    workers = manifest.get("simulator_worker_threads")
    if not isinstance(identity, str) or not runner.SHA256_RE.fullmatch(identity):
        errors.append("stability runtime closure identity is invalid")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        errors.append("stability worker-thread evidence is invalid")

    try:
        recorded = _load_json(
            Path(str(report_ref["path"])), "simulator stability evidence"
        )
    except FinalizationError as exc:
        errors.append(str(exc))
        return errors
    basis = {
        "planned_stability_status": "PENDING_EXECUTION",
        "exit_code": manifest.get("exit_code"),
        "process_status": execution.get("process_status"),
        "timed_out": execution.get("timed_out"),
        "interrupted": execution.get("interrupted"),
        "oom_kill_observed_during_attempt": (
            resource.get("oom_kill_observed_during_attempt")
            if isinstance(resource, Mapping) else None
        ),
        "execution_status": manifest.get("execution_status"),
        "observation_status": (
            lifecycle.get("observation_status")
            if isinstance(lifecycle, Mapping) else None
        ),
        "workload_completed": workload_completed,
        "runtime_execution_status": manifest.get("runtime_execution_status"),
        "semantic_validation_status": manifest.get("semantic_validation_status"),
        "workload_runtime_qualification_status": manifest.get(
            "workload_runtime_qualification_status"
        ),
        "ecmp_route_candidate_validation_status": manifest.get(
            "ecmp_route_candidate_validation_status"
        ),
        "training_source_port_allocator_validation_status": manifest.get(
            "training_source_port_allocator_validation_status"
        ),
        "runtime_closure_identity_sha256": identity,
        "simulator_worker_threads": workers,
        "source_artifacts_sha256": source_hashes,
    }
    expected = {
        "schema_version": SIMULATOR_STABILITY_SCHEMA,
        "status": "PASS",
        "run_id": run_id,
        "fault_family": run.get("fault_family"),
        "gate_required": True,
        "planned_run_sha256": runner.canonical_hash(run),
        "virtual_finish_ns": run.get("virtual_finish_ns"),
        "evidence_basis": basis,
        "evidence_basis_sha256": runner.canonical_hash(basis),
        "checks": [
            {"name": name, "status": "PASS"}
            for name in SIMULATOR_STABILITY_CHECK_NAMES
        ],
        "errors": [],
    }
    if dict(recorded) != expected:
        errors.append(
            "sealed simulator stability report differs from independent "
            "raw-evidence recomputation"
        )
    report_sha = runner.sha256_file(Path(str(report_ref["path"])))
    if (
        manifest.get("simulator_stability_status") != "PASS"
        or manifest.get("simulator_stability_sha256") != report_sha
        or report_ref.get("sha256") != report_sha
    ):
        errors.append("run manifest does not bind the recomputed stability report")
    return errors


def _collective_override_static_report(
    run: Mapping[str, Any],
    corpus: runner.ValidatedCorpus,
) -> Tuple[Optional[Mapping[str, Any]], List[str]]:
    """Rebuild an A1 override and its roles from immutable corpus inputs."""

    errors: List[str] = []
    schedule = run.get("schedule")
    collective = (
        schedule.get("collective_workload_override")
        if isinstance(schedule, Mapping)
        else None
    )
    if collective is None:
        return None, errors
    if not isinstance(collective, Mapping):
        return None, ["collective workload override reference is invalid"]
    role_ref = collective.get("layer_role_sidecar")
    if not isinstance(role_ref, Mapping):
        return None, ["collective workload override lacks its role sidecar"]
    try:
        truth_path = runner.resolve_sidecar(
            corpus.root,
            schedule.get("path"),
            f"{run.get('run_id')} congestion truth schedule",
        )
        with truth_path.open(encoding="utf-8", newline="") as stream:
            truth_rows = list(csv.DictReader(stream))
        if len(truth_rows) != 1:
            raise FinalizationError(
                "collective congestion truth schedule must contain one row"
            )
        onset_ns = int(truth_rows[0]["start_time_ns"])
        scenario = str(run.get("scenario", ""))
        contract = corpus_generator.load_contract(corpus.input_paths["contract"])
        pairs, _ = corpus_generator.load_true16_links(
            corpus.input_paths["link_map"], contract
        )
        aggregate_bandwidths = {
            sum(int(link["bandwidth_bps"]) for link in rails.values())
            for rails in pairs.values()
        }
        if len(aggregate_bandwidths) != 1:
            raise FinalizationError(
                "collective override has non-uniform dual-rail ACCESS bandwidth"
            )
        expected_contract = corpus_generator.collective_override_contract(
            scenario,
            onset_ns,
            source_workload_sha256=corpus.manifest["input_artifacts"][
                "workload"
            ]["sha256"],
            aggregate_access_bandwidth_bps=next(iter(aggregate_bandwidths)),
        )
        override_path = runner.resolve_sidecar(
            corpus.root,
            collective.get("path"),
            f"{run.get('run_id')} collective workload override",
        )
        role_path = runner.resolve_sidecar(
            corpus.root,
            role_ref.get("path"),
            f"{run.get('run_id')} collective layer roles",
        )
        recomputed = corpus_generator.validate_collective_override(
            override_path,
            role_path,
            expected_contract,
            corpus.input_paths["workload"],
            str(run.get("run_id", "")),
        )
        expected_reference = {
            "kind": "simai_collective_workload_override",
            "format": corpus_generator.COLLECTIVE_OVERRIDE_FORMAT,
            "path": collective.get("path"),
            "sha256": recomputed["sha256"],
            "generated_before_run": True,
            "safe_to_execute": True,
            "runtime_executor_status": "READY",
            "layer_role_sidecar": {
                "kind": "collective_layer_roles",
                "format": corpus_generator.COLLECTIVE_ROLE_FORMAT,
                "path": role_ref.get("path"),
                "sha256": recomputed["role_sha256"],
                "generated_before_run": True,
            },
            "static_validation": recomputed,
            "static_validation_sha256": runner.canonical_hash(recomputed),
        }
        if dict(collective) != expected_reference:
            errors.append(
                "planned collective override differs from independent baseline "
                "and role reconstruction"
            )
        if run.get("effective_workload_sha256") != recomputed["sha256"]:
            errors.append(
                "planned effective workload identity differs from reconstructed "
                "collective override"
            )
        return recomputed, errors
    except (
        OSError,
        csv.Error,
        KeyError,
        TypeError,
        ValueError,
        runner.RunnerError,
        corpus_generator.CorpusError,
        FinalizationError,
    ) as exc:
        return None, [f"collective override cannot be reconstructed: {exc}"]


def _independent_collective_semantic_report(
    run: Mapping[str, Any],
    run_dir: Path,
    qualification: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Derive A1 semantic evidence without calling the runner validator."""

    schedule = run["schedule"]
    collective = schedule["collective_workload_override"]
    profile = collective["static_validation"]["qualification_profile"]
    timeline = qualification.get("evidence", {}).get(
        "collective_override_pre_event_post_runtime"
    )
    fault_path = run_dir / "fault_application_telemetry.csv"
    with fault_path.open(encoding="utf-8", newline="") as stream:
        fault_rows = list(csv.DictReader(stream))
    checks = [
        {
            "name": "predeclared_collective_profile_is_label_authority",
            "status": "PASS" if (
                schedule.get("independent_of_features") is True
                and schedule.get("generated_before_run") is True
                and profile in {
                    workload_runtime.BURST_PROFILE,
                    workload_runtime.HIGH_UTIL_PROFILE,
                }
            ) else "FAIL",
        },
        {
            "name": "role_bound_application_lifecycle_validated",
            "status": "PASS" if (
                qualification.get("status") == "PASS"
                and qualification.get("qualification_profile") == profile
                and isinstance(timeline, Mapping)
                and timeline.get("actual_application_window_authority")
                == "role_bound_collective_transaction"
            ) else "FAIL",
        },
        {
            "name": "no_physical_fault_application",
            "status": "PASS" if not fault_rows else "FAIL",
        },
    ]
    source_paths: Dict[str, Path] = {
        "workload": run_dir / "inputs/collective_workload_override.txt",
        "collective_layer_roles": run_dir / "inputs/collective_layer_roles.csv",
        "link_map": run_dir / "link_map.csv",
        "switch_telemetry": run_dir / "switch_telemetry.csv",
        "nic_telemetry": run_dir / "nic_telemetry.csv",
        "collective_transaction": run_dir / "collective_transaction.csv",
        "collective_telemetry": run_dir / "collective_telemetry.csv",
        "run_lifecycle": run_dir / "run_lifecycle.csv",
    }
    source_hashes = {
        name: runner.sha256_file(path)
        for name, path in sorted(source_paths.items())
    }
    source_hashes.update({
        "fault_application": runner.sha256_file(fault_path),
        "workload_runtime_qualification": runner.sha256_file(
            run_dir / "workload_runtime_qualification.json"
        ),
    })
    return {
        "schema_version": SEMANTIC_SCHEMA,
        "run_id": str(run["run_id"]),
        "mechanism_id": run["mechanism"]["mechanism_id"],
        "status": (
            "PASS" if all(item["status"] == "PASS" for item in checks)
            else "FAIL"
        ),
        "source_artifact_sha256": runner.sha256_file(
            run_dir / "switch_telemetry.csv"
        ),
        "source_artifacts_sha256": source_hashes,
        "checks": checks,
        "injected_physical_fault": False,
        "target_link_state_during_event": "up",
        "observed_effects": ["role_bound_collective_profile"],
        "scenario": run.get("scenario"),
        "evidence": {
            "scenario": run.get("scenario"),
            "qualification_profile": profile,
            "scheduled_application_onset_ns": run.get(
                "fault_scheduled_onset_ns"
            ),
            "application_lifecycle": timeline,
            "feature_derived_label": False,
        },
    }


def _independent_collective_runtime_errors(
    run: Mapping[str, Any],
    run_dir: Path,
    manifest: Mapping[str, Any],
    corpus: runner.ValidatedCorpus,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    """Recompute every A1 workload/role/runtime/semantic binding from raw bytes."""

    schedule = run.get("schedule")
    collective = (
        schedule.get("collective_workload_override")
        if isinstance(schedule, Mapping)
        else None
    )
    if collective is None:
        return []
    static_report, errors = _collective_override_static_report(run, corpus)
    if static_report is None:
        return errors or ["collective override static reconstruction is absent"]
    try:
        paths = workload_runtime.RuntimePaths(
            workload=run_dir / "inputs/collective_workload_override.txt",
            collective_layer_roles=run_dir / "inputs/collective_layer_roles.csv",
            link_map=run_dir / "link_map.csv",
            switch_telemetry=run_dir / "switch_telemetry.csv",
            nic_telemetry=run_dir / "nic_telemetry.csv",
            collective_transaction=run_dir / "collective_transaction.csv",
            collective_telemetry=run_dir / "collective_telemetry.csv",
            run_lifecycle=run_dir / "run_lifecycle.csv",
        )
        recomputed = workload_runtime.validate_runtime_qualification(
            run=run,
            workload_report=static_report,
            paths=paths,
            causal_warmup_ns=int(
                corpus.manifest["workload_qualification"].get(
                    "causal_warmup_required_ns",
                    workload_runtime.DEFAULT_CAUSAL_WARMUP_NS,
                )
            ),
        )
        recorded = _load_json(
            run_dir / "workload_runtime_qualification.json",
            "collective workload runtime qualification",
        )
        if recomputed.get("status") != "PASS":
            errors.append(
                "collective workload runtime recomputation failed: "
                + "; ".join(str(item) for item in recomputed.get("errors", [])[:6])
            )
        if dict(recorded) != dict(recomputed):
            errors.append(
                "stored collective workload runtime report differs from "
                "independent raw-evidence recomputation"
            )

        expected_effective = {
            "source_workload_sha256": corpus.manifest["input_artifacts"]
            ["workload"]["sha256"],
            "effective_workload_sha256": static_report["sha256"],
            "execution_copy": "inputs/collective_workload_override.txt",
            "collective_override_sha256": static_report["sha256"],
            "collective_layer_role_sha256": static_report["role_sha256"],
            "passed_to_simulator": True,
        }
        effective = manifest.get("effective_workload_binding")
        if effective != expected_effective:
            errors.append("runner effective workload binding is invalid")
        simulator = manifest.get("simulator")
        argv = simulator.get("argv") if isinstance(simulator, Mapping) else None
        if not isinstance(argv, list):
            errors.append("runner simulator argv is absent for collective override")
        else:
            try:
                workload_arg = argv[argv.index("-w") + 1]
            except (ValueError, IndexError):
                workload_arg = None
            if workload_arg != "../inputs/collective_workload_override.txt":
                errors.append("simulator argv ignored the effective workload override")
        invocation = _load_json(run_dir / "invocation.json", "runner invocation")
        if (
            invocation.get("argv") != argv
            or invocation.get("effective_workload_binding") != expected_effective
            or invocation.get("schedule_bindings")
            != manifest.get("schedule_bindings")
        ):
            errors.append("invocation does not bind effective workload and argv")

        semantic = _load_json(
            run_dir / "semantic_validation.json",
            "collective semantic validation",
        )
        expected_semantic = _independent_collective_semantic_report(
            run, run_dir, recomputed
        )
        if expected_semantic.get("status") != "PASS":
            errors.append("independently recomputed collective semantics did not PASS")
        if dict(semantic) != dict(expected_semantic):
            errors.append(
                "stored collective semantic report differs from independent "
                "raw-evidence recomputation"
            )
        qualification_ref = artifacts.get("workload_runtime_qualification", {})
        semantic_ref = artifacts.get("semantic_validation", {})
        if (
            manifest.get("workload_runtime_qualification_sha256")
            != qualification_ref.get("sha256")
            or manifest.get("semantic_validation_sha256")
            != semantic_ref.get("sha256")
        ):
            errors.append("run manifest does not bind A1 validation reports")
    except (
        OSError,
        csv.Error,
        KeyError,
        TypeError,
        ValueError,
        FinalizationError,
    ) as exc:
        errors.append(f"collective runtime evidence cannot be recomputed: {exc}")
    return errors


def _verified_artifacts(
    run: Mapping[str, Any],
    run_dir: Path,
    manifest: Mapping[str, Any],
    corpus: runner.ValidatedCorpus,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    errors: List[str] = []
    inventory, inventory_errors = _inventory_index(manifest, run_dir)
    errors.extend(inventory_errors)
    artifacts: Dict[str, Dict[str, Any]] = {}
    for key, relative in REQUIRED_SEALED_ARTIFACTS.items():
        ref, error = _sealed_ref(run_dir, relative, inventory)
        if error:
            errors.append(error)
        elif ref is not None:
            artifacts[key] = ref
    for key, relative in OPTIONAL_SEALED_ARTIFACTS.items():
        if relative in inventory:
            ref, error = _sealed_ref(run_dir, relative, inventory)
            if error:
                errors.append(error)
            elif ref is not None:
                artifacts[key] = ref

    source_corpus_ref, source_corpus_error = _sealed_ref(
        run_dir, "inputs/corpus_manifest.json", inventory
    )
    if source_corpus_error:
        errors.append(source_corpus_error)
    elif source_corpus_ref is not None:
        artifacts["source_corpus_copy"] = source_corpus_ref
        if source_corpus_ref["sha256"] != corpus.manifest_sha256:
            errors.append("sealed source corpus copy differs from prepared corpus")

    input_bindings = manifest.get("input_bindings")
    expected_inputs = corpus.manifest.get("input_artifacts", {})
    if not isinstance(input_bindings, Mapping) or set(input_bindings) != set(
        corpus.input_paths
    ):
        errors.append("runner manifest input_bindings has the wrong key set")
    else:
        for name in sorted(corpus.input_paths):
            binding = input_bindings.get(name)
            expected = (
                expected_inputs.get(name, {})
                if isinstance(expected_inputs, Mapping)
                else {}
            )
            expected_sha = (
                expected.get("sha256") if isinstance(expected, Mapping) else None
            )
            if not isinstance(binding, Mapping):
                errors.append(f"runner input binding is invalid: {name}")
                continue
            relative = binding.get("execution_copy")
            if not isinstance(relative, str):
                errors.append(f"runner input binding path is missing: {name}")
                continue
            ref, error = _sealed_ref(run_dir, relative, inventory)
            if error:
                errors.append(error)
            elif ref is not None:
                artifacts[f"input_{name}"] = ref
                if (
                    binding.get("sha256") != expected_sha
                    or ref["sha256"] != expected_sha
                ):
                    errors.append(f"runner input copy hash differs for {name}")

    schedule_bindings = manifest.get("schedule_bindings")
    expected_schedule_bindings: Dict[str, Tuple[Mapping[str, Any], str, bool]] = {
        "truth": (run.get("schedule", {}), "inputs/truth_schedule.csv", False),
    }
    schedule = run.get("schedule", {})
    if isinstance(schedule, Mapping):
        if isinstance(schedule.get("simulator_injection_schedule"), Mapping):
            expected_schedule_bindings["simulator_injection"] = (
                schedule["simulator_injection_schedule"],
                "inputs/simulator_injection_schedule.csv",
                True,
            )
        if isinstance(schedule.get("background_flow_schedule"), Mapping):
            expected_schedule_bindings["background_flow"] = (
                schedule["background_flow_schedule"],
                "inputs/background_flow_schedule.csv",
                True,
            )
        collective = schedule.get("collective_workload_override")
        if isinstance(collective, Mapping):
            role_ref = collective.get("layer_role_sidecar")
            if not isinstance(role_ref, Mapping):
                errors.append("planned collective override lacks role sidecar")
            else:
                expected_schedule_bindings["collective_workload_override"] = (
                    collective,
                    "inputs/collective_workload_override.txt",
                    True,
                )
                expected_schedule_bindings["collective_layer_roles"] = (
                    role_ref,
                    "inputs/collective_layer_roles.csv",
                    False,
                )
    if not isinstance(schedule_bindings, Mapping) or set(schedule_bindings) != set(
        expected_schedule_bindings
    ):
        errors.append("runner manifest schedule_bindings has the wrong key set")
    else:
        for name, expected_tuple in expected_schedule_bindings.items():
            expected, expected_copy, passed_to_simulator = expected_tuple
            binding = schedule_bindings.get(name)
            if not isinstance(binding, Mapping):
                errors.append(f"runner schedule binding is invalid: {name}")
                continue
            relative = binding.get("execution_copy")
            expected_sha = (
                expected.get("sha256") if isinstance(expected, Mapping) else None
            )
            if not isinstance(relative, str):
                errors.append(f"runner schedule binding path is missing: {name}")
                continue
            expected_binding: Dict[str, Any] = {
                "source_path": str(
                    runner.resolve_sidecar(
                        corpus.root,
                        expected.get("path"),
                        f"{run.get('run_id')} {name} source",
                    )
                ),
                "execution_copy": expected_copy,
                "sha256": expected_sha,
                "passed_to_simulator": passed_to_simulator,
            }
            if name in {
                "simulator_injection",
                "background_flow",
                "collective_workload_override",
            }:
                expected_binding["safe_to_execute"] = True
            if dict(binding) != expected_binding:
                errors.append(f"runner schedule binding contract differs for {name}")
            ref, error = _sealed_ref(run_dir, relative, inventory)
            if error:
                errors.append(error)
            elif ref is not None:
                artifacts[f"{name}_schedule_copy"] = ref
                if (
                    binding.get("sha256") != expected_sha
                    or ref["sha256"] != expected_sha
                ):
                    errors.append(f"runner schedule copy hash differs for {name}")

    runtime = manifest.get("runtime_config_binding")
    if not isinstance(runtime, Mapping):
        errors.append("runner manifest runtime_config_binding is missing")
    else:
        relative = runtime.get("path")
        if not isinstance(relative, str):
            errors.append("runtime_config_binding.path is missing")
        else:
            ref, error = _sealed_ref(run_dir, relative, inventory)
            if error:
                errors.append(error)
            elif ref is not None:
                if runtime.get("sha256") != ref["sha256"]:
                    errors.append("runtime_config_binding hash differs from inventory")
                artifacts["runtime_config"] = ref

    manifest_path = run_dir / "run_manifest.json"
    seal_path = run_dir / "run_manifest.sha256"
    try:
        artifacts["run_manifest"] = _file_ref(
            _require_plain_file(manifest_path, "run manifest")
        )
        artifacts["run_manifest_seal"] = _file_ref(
            _require_plain_file(seal_path, "run manifest seal")
        )
    except FinalizationError as exc:
        errors.append(str(exc))

    run_id = str(run.get("run_id", ""))
    mechanism = run.get("mechanism", {})
    mechanism_id = str(
        mechanism.get("mechanism_id", "") if isinstance(mechanism, Mapping) else ""
    )
    semantic_ref = artifacts.get("semantic_validation")
    if semantic_ref is not None:
        semantic_report, report_errors = _passing_report(
            Path(semantic_ref["path"]),
            schema=SEMANTIC_SCHEMA,
            run_id=run_id,
            description="semantic validation",
            mechanism_id=mechanism_id,
        )
        errors.extend(report_errors)
        if semantic_report is not None:
            switch_sha = artifacts.get("switch_telemetry", {}).get("sha256")
            if semantic_report.get("source_artifact_sha256") != switch_sha:
                errors.append("semantic validation does not bind switch telemetry")
            source_hashes = semantic_report.get("source_artifacts_sha256")
            if not isinstance(source_hashes, Mapping) or not source_hashes:
                errors.append("semantic validation lacks source artifact hashes")
            else:
                semantic_bindings = {
                    "switch": artifacts.get("switch_telemetry", {}).get("sha256"),
                    "nic": artifacts.get("nic_telemetry", {}).get("sha256"),
                    "collective": artifacts.get("collective_telemetry", {}).get(
                        "sha256"
                    ),
                    "fault_application": artifacts.get(
                        "fault_application_telemetry", {}
                    ).get("sha256"),
                    "link_map": artifacts.get("link_map", {}).get("sha256"),
                    "runtime_link_map": artifacts.get("link_map", {}).get("sha256"),
                    "frozen_link_map": corpus.manifest.get("input_artifacts", {})
                    .get("link_map", {})
                    .get("sha256"),
                    "rdma_wc": artifacts.get("rdma_wc_telemetry", {}).get("sha256"),
                    "background_application": artifacts.get(
                        "background_flow_application", {}
                    ).get("sha256"),
                    "truth_schedule": schedule.get("sha256")
                    if isinstance(schedule, Mapping)
                    else None,
                    "background_schedule": schedule.get(
                        "background_flow_schedule", {}
                    ).get("sha256")
                    if isinstance(schedule, Mapping)
                    else None,
                }
                for name, observed in source_hashes.items():
                    expected = semantic_bindings.get(str(name))
                    if expected is not None and observed != expected:
                        errors.append(
                            f"semantic validation source hash differs for {name}"
                        )
    qualification_ref = artifacts.get("workload_runtime_qualification")
    if qualification_ref is not None:
        _, report_errors = _passing_report(
            Path(qualification_ref["path"]),
            schema=RUNTIME_QUALIFICATION_SCHEMA,
            run_id=run_id,
            description="workload runtime qualification",
        )
        errors.extend(report_errors)
        if (
            manifest.get("workload_runtime_qualification_sha256")
            != (qualification_ref["sha256"])
        ):
            errors.append(
                "runner manifest does not bind workload runtime qualification hash"
            )
        if manifest.get("workload_runtime_qualification_status") != "PASS":
            errors.append(
                "runner manifest workload_runtime_qualification_status is not PASS"
            )
    errors.extend(
        _independent_collective_runtime_errors(
            run, run_dir, manifest, corpus, artifacts
        )
    )
    errors.extend(_training_source_port_errors(run, manifest, artifacts))
    errors.extend(_simulator_stability_errors(run, run_dir, manifest, artifacts))
    return artifacts, errors


def _audit_record(
    run: Mapping[str, Any],
    execution_root: Path,
    corpus: runner.ValidatedCorpus,
    current_runtime: Optional[CurrentSimulatorRuntime],
    runtime_authority: Any,
) -> Tuple[Dict[str, Any], Mapping[str, Any] | None, Dict[str, Dict[str, Any]]]:
    run_id = str(run["run_id"])
    planned_hash = runner.canonical_hash(run)
    declared = _declared_block_reasons(run)
    record: Dict[str, Any] = {
        "run_id": run_id,
        "partition": run.get("partition"),
        "planned_run_sha256": planned_hash,
        "declarations": {
            "schedule_implementation_status": str(
                run.get("schedule", {}).get("implementation_status", "")
                if isinstance(run.get("schedule"), Mapping)
                else ""
            ),
            "mechanism_implementation_status": str(
                run.get("mechanism", {}).get("implementation_status", "")
                if isinstance(run.get("mechanism"), Mapping)
                else ""
            ),
        },
        "status": "PENDING",
        "reason_codes": list(declared),
        "reasons": [
            f"prepared corpus declares blocker {reason}" for reason in declared
        ],
    }
    final_dir = execution_root / "runs" / run_id
    manifest: Mapping[str, Any] | None = None
    artifacts: Dict[str, Dict[str, Any]] = {}
    if not final_dir.exists() and not final_dir.is_symlink():
        record["reason_codes"].append("MISSING_RUN_EVIDENCE")
        record["reasons"].append(f"sealed run directory is missing: {final_dir}")
    elif final_dir.is_symlink():
        record["reason_codes"].append("SYMLINK_RUN_DIRECTORY")
        record["reasons"].append(f"run directory is a symbolic link: {final_dir}")
    elif not final_dir.is_dir():
        record["reason_codes"].append("INVALID_RUN_PATH")
        record["reasons"].append(f"run path is not a directory: {final_dir}")
    elif not _path_within(final_dir, execution_root):
        record["reason_codes"].append("RUN_PATH_ESCAPE")
        record["reasons"].append(f"run directory escapes execution root: {final_dir}")
    else:
        symlinks = _tree_symlinks(final_dir)
        if symlinks:
            record["reason_codes"].append("SYMLINK_IN_SEALED_RUN")
            record["reasons"].append(
                f"sealed run tree contains symbolic links: {symlinks[:8]}"
            )
        else:
            try:
                untrusted_manifest = _load_json(
                    final_dir / "run_manifest.json", "run manifest"
                )
                recorded_runtime = untrusted_manifest.get("runtime_closure")
                if not isinstance(recorded_runtime, Mapping):
                    raise RuntimeAuditError(
                        "MISSING_RUNTIME_CLOSURE_BINDING",
                        "run manifest does not bind a sealed simulator runtime closure",
                    )
                raw_identity = recorded_runtime.get("identity_sha256")
                if isinstance(raw_identity, str):
                    record["runtime_closure_identity_sha256"] = raw_identity
                record["runtime_authority_record_sha256"] = runner.canonical_hash(
                    recorded_runtime
                )
                if current_runtime is None:
                    raise RuntimeAuditError(
                        "CURRENT_SIMULATOR_RUNTIME_UNAVAILABLE",
                        "the current canonical simulator closure could not be audited",
                    )
                audited_runtime = runtime_authority.validate_record(
                    execution_root, recorded_runtime, current_runtime
                )
                record["runtime_bundle"] = copy.deepcopy(
                    dict(audited_runtime.evidence)
                )
                manifest = runner.validate_final_run(
                    final_dir,
                    corpus,
                    run,
                    runtime_binding=audited_runtime.binding,
                    runtime_authority=runtime_authority,
                )
                artifacts, artifact_errors = _verified_artifacts(
                    run, final_dir.resolve(strict=True), manifest, corpus
                )
                if artifact_errors:
                    record["reason_codes"].append("INVALID_REQUIRED_ARTIFACTS")
                    record["reasons"].extend(artifact_errors)
            except RuntimeAuditError as exc:
                record["reason_codes"].append(exc.reason_code)
                record["reasons"].append(str(exc))
            except (runner.RunnerError, FinalizationError, OSError, ValueError) as exc:
                record["reason_codes"].append("INVALID_RUN_EVIDENCE")
                record["reasons"].append(str(exc))

    # A prepared declaration of an unavailable mechanism remains a blocker
    # even if an out-of-band directory happens to look like a completed run.
    codes = list(dict.fromkeys(record["reason_codes"]))
    record["reason_codes"] = codes
    if manifest is not None:
        manifest_path = final_dir / "run_manifest.json"
        record["run_manifest"] = _file_ref(manifest_path)
        record["runner_artifact_set_sha256"] = manifest.get("artifact_set_sha256")
        record["simulator_worker_threads"] = manifest.get("simulator_worker_threads")
    if codes:
        record["status"] = "BLOCKED"
    else:
        record["status"] = "VERIFIED"
    record["record_sha256"] = runner.canonical_hash(
        {key: value for key, value in record.items() if key != "record_sha256"}
    )
    return record, manifest, artifacts


def _counts(records: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    return {
        "planned": len(records),
        "verified": sum(record.get("status") == "VERIFIED" for record in records),
        "blocked_total": sum(record.get("status") != "VERIFIED" for record in records),
        "declared_blocked": sum(
            any(
                str(code).startswith("DECLARED_")
                for code in record.get("reason_codes", [])
            )
            for record in records
        ),
        "missing_run": sum(
            "MISSING_RUN_EVIDENCE" in record.get("reason_codes", [])
            for record in records
        ),
        "invalid_run": sum(
            any(
                code
                in {
                    "INVALID_RUN_PATH",
                    "RUN_PATH_ESCAPE",
                    "SYMLINK_RUN_DIRECTORY",
                    "SYMLINK_IN_SEALED_RUN",
                    "INVALID_RUN_EVIDENCE",
                    "INVALID_REQUIRED_ARTIFACTS",
                    "MISSING_RUNTIME_CLOSURE_BINDING",
                    "CURRENT_SIMULATOR_RUNTIME_UNAVAILABLE",
                    "INVALID_RUNTIME_CLOSURE_BINDING",
                    "RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR",
                    "INVALID_RUNTIME_BUNDLE",
                    "RUNTIME_LOADER_RESOLUTION_MISMATCH",
                }
                for code in record.get("reason_codes", [])
            )
            for record in records
        ),
        "runtime_invalid": sum(
            any(
                code
                in {
                    "MISSING_RUNTIME_CLOSURE_BINDING",
                    "CURRENT_SIMULATOR_RUNTIME_UNAVAILABLE",
                    "INVALID_RUNTIME_CLOSURE_BINDING",
                    "RUNTIME_CLOSURE_DIFFERS_FROM_CURRENT_SIMULATOR",
                    "INVALID_RUNTIME_BUNDLE",
                    "RUNTIME_LOADER_RESOLUTION_MISMATCH",
                }
                for code in record.get("reason_codes", [])
            )
            for record in records
        ),
    }


def _runtime_summary(
    current_runtime: Optional[CurrentSimulatorRuntime],
    records: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    verified = [record for record in records if record.get("status") == "VERIFIED"]
    observed_identity_counts = Counter(
        str(record["runtime_closure_identity_sha256"])
        for record in records
        if isinstance(record.get("runtime_closure_identity_sha256"), str)
    )
    verified_identity_counts = Counter(
        str(record["runtime_closure_identity_sha256"])
        for record in verified
        if isinstance(record.get("runtime_closure_identity_sha256"), str)
    )
    authority_counts = Counter(
        str(record["runtime_authority_record_sha256"])
        for record in verified
        if isinstance(record.get("runtime_authority_record_sha256"), str)
    )
    bundle_paths = sorted(
        {
            str(record["runtime_bundle"]["bundle_path"])
            for record in verified
            if isinstance(record.get("runtime_bundle"), Mapping)
            and isinstance(record["runtime_bundle"].get("bundle_path"), str)
        }
    )
    runtime_failures = Counter(
        str(code)
        for record in records
        for code in record.get("reason_codes", [])
        if "RUNTIME" in str(code) or "CLOSURE" in str(code)
    )
    representative = next(
        (
            record["runtime_bundle"]
            for record in verified
            if isinstance(record.get("runtime_bundle"), Mapping)
        ),
        None,
    )
    summary: Dict[str, Any] = {
        "schema_version": runtime_bundle.BUNDLE_SCHEMA,
        "status": (
            "PASS"
            if current_runtime is not None
            and bool(verified)
            and len(verified_identity_counts) <= 1
            and len(authority_counts) <= 1
            and not runtime_failures
            else "BLOCKED"
        ),
        "identity_sha256": (
            current_runtime.identity_sha256 if current_runtime is not None else None
        ),
        "current_source_closure": (
            copy.deepcopy(dict(current_runtime.source_evidence))
            if current_runtime is not None
            else None
        ),
        "observed_run_identity_counts": dict(sorted(observed_identity_counts.items())),
        "verified_run_identity_counts": dict(sorted(verified_identity_counts.items())),
        "verified_authority_record_counts": dict(sorted(authority_counts.items())),
        "verified_run_reference_count": len(verified),
        "shared_bundle_count": len(bundle_paths),
        "shared_bundle_paths": bundle_paths,
        "failure_reason_code_counts": dict(sorted(runtime_failures.items())),
    }
    if representative is not None:
        summary.update(
            {
                "authority_record_sha256": representative.get(
                    "authority_record_sha256"
                ),
                "bundle_path": representative.get("bundle_path"),
                "bundle_manifest": copy.deepcopy(
                    representative.get("bundle_manifest")
                ),
                "bundle_manifest_seal": copy.deepcopy(
                    representative.get("bundle_manifest_seal")
                ),
                "bundle_artifact_set_sha256": representative.get(
                    "bundle_artifact_set_sha256"
                ),
                "project_dependency_count": representative.get(
                    "project_dependency_count"
                ),
                "system_dependency_count": representative.get(
                    "system_dependency_count"
                ),
                "loader_resolution_identity_sha256": representative.get(
                    "loader_resolution_identity_sha256"
                ),
                "shared_bundle_outside_runs": representative.get(
                    "shared_bundle_outside_runs"
                ),
            }
        )
    return summary


def _base_audit(
    corpus: runner.ValidatedCorpus,
    execution_root: Path,
    records: Sequence[Mapping[str, Any]],
    global_blockers: Sequence[Mapping[str, Any]],
    runtime_closure_audit: Mapping[str, Any],
    simulator_runtime_closure: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    counts = _counts(records)
    reason_code_counts = dict(
        sorted(
            Counter(
                str(code)
                for record in records
                for code in record.get("reason_codes", [])
            ).items()
        )
    )
    binding = {
        "corpus_id": corpus.manifest.get("corpus_id"),
        "source_corpus_sha256": corpus.manifest_sha256,
        "schedule_set_sha256": corpus.manifest.get("schedule_set_sha256"),
        "simulator_sha256": corpus.simulator_sha256,
        "runtime_closure_identity_sha256": runtime_closure_audit.get(
            "identity_sha256"
        ),
        "runtime_authority_record_sha256": runtime_closure_audit.get(
            "authority_record_sha256"
        ),
        "runtime_closure_distinct_identity_count": len(
            runtime_closure_audit.get("verified_run_identity_counts", {})
        ),
        "runtime_closure_reference_count": runtime_closure_audit.get(
            "verified_run_reference_count"
        ),
        "runtime_closure_audit_sha256": runner.canonical_hash(
            runtime_closure_audit
        ),
        "execution_root": str(execution_root),
        "run_records_sha256": runner.canonical_hash(records),
        "counts": counts,
        "reason_code_counts": reason_code_counts,
        "global_blockers": list(global_blockers),
        "expected_run_count": EXPECTED_RUN_COUNT,
    }
    return {
        "schema_version": READINESS_SCHEMA,
        "status": "BLOCKED",
        "created_at": _utc_now(),
        "source_corpus": {
            "path": str(corpus.manifest_path),
            "sha256": corpus.manifest_sha256,
            "corpus_id": corpus.manifest.get("corpus_id"),
            "identity_schema": corpus.manifest.get("identity_schema"),
            "schedule_set_sha256": corpus.manifest.get("schedule_set_sha256"),
            "run_count": len(corpus.manifest["runs"]),
        },
        "execution_root": str(execution_root),
        "simulator": _file_ref(corpus.simulator_binary),
        "simulator_runtime_closure": (
            copy.deepcopy(dict(simulator_runtime_closure))
            if simulator_runtime_closure is not None
            else None
        ),
        "runtime_closure_audit": copy.deepcopy(dict(runtime_closure_audit)),
        "required_run_count": EXPECTED_RUN_COUNT,
        "counts": counts,
        "reason_code_counts": reason_code_counts,
        "global_blockers": list(global_blockers),
        "runs": list(records),
        "run_records_sha256": binding["run_records_sha256"],
        "readiness_binding": binding,
        "readiness_binding_sha256": runner.canonical_hash(binding),
    }


def _assert_primary_inputs_unchanged(
    corpus: runner.ValidatedCorpus,
    current_runtime: Optional[CurrentSimulatorRuntime],
    runtime_authority: Any,
) -> None:
    checks = (
        (corpus.manifest_path, corpus.manifest_sha256, "prepared corpus"),
        (corpus.simulator_binary, corpus.simulator_sha256, "simulator binary"),
    )
    for path, expected, description in checks:
        if path.is_symlink() or not path.is_file():
            raise FinalizationError(f"{description} disappeared or became a symlink")
        actual = runner.sha256_file(path)
        if actual != expected:
            raise FinalizationError(
                f"{description} changed during finalization: "
                f"expected={expected}, actual={actual}"
            )
    if current_runtime is not None:
        runtime_authority.verify_current(current_runtime)


def _executed_run(
    planned: Mapping[str, Any],
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
    record: Mapping[str, Any],
) -> Dict[str, Any]:
    result = copy.deepcopy(dict(planned))
    mechanism = result.get("mechanism")
    if not isinstance(mechanism, MutableMapping):
        raise FinalizationError(f"{planned.get('run_id')}: mechanism is invalid")
    semantic = copy.deepcopy(dict(artifacts["semantic_validation"]))
    mechanism.update(
        {
            "implementation_status": "VERIFIED",
            "semantic_validation": semantic,
            "verification": {
                "status": "PASS",
                "run_manifest_sha256": artifacts["run_manifest"]["sha256"],
                "semantic_validation_sha256": semantic["sha256"],
                "workload_runtime_qualification_sha256": artifacts[
                    "workload_runtime_qualification"
                ]["sha256"],
                "training_source_port_allocator_sha256": artifacts[
                    "training_source_port_allocator"
                ]["sha256"],
                "training_source_port_allocator_validation_sha256": artifacts[
                    "training_source_port_allocator_validation"
                ]["sha256"],
            },
        }
    )
    gate_required, gate_errors = _planned_stability_gate(planned)
    if gate_errors:
        raise FinalizationError(
            f"{planned.get('run_id')}: invalid prepared stability lifecycle: "
            + "; ".join(gate_errors)
        )
    if gate_required:
        stability_ref = artifacts.get("simulator_stability")
        if not isinstance(stability_ref, Mapping):
            raise FinalizationError(
                f"{planned.get('run_id')}: stability evidence disappeared"
            )
        stability_report = _load_json(
            Path(str(stability_ref["path"])), "simulator stability evidence"
        )
        simulator_stability = {
            "gate_required": True,
            "status": "PASS",
            "reason": (
                "sealed completed run passed independent simulator stability "
                "recomputation"
            ),
            "evidence": copy.deepcopy(dict(stability_ref)),
            "evidence_basis_sha256": stability_report.get(
                "evidence_basis_sha256"
            ),
        }
    else:
        simulator_stability = {
            "gate_required": False,
            "status": "NOT_REQUIRED",
            "reason": "prepared run does not require the simulator stability gate",
        }
    result.update(
        {
            "execution_status": "COMPLETE",
            "evidence_state": manifest.get("evidence_state"),
            "workload_completed": manifest.get("workload_completed"),
            "source_planned_run_sha256": record["planned_run_sha256"],
            "runner_artifact_set_sha256": manifest.get("artifact_set_sha256"),
            "simulator_worker_threads": manifest.get("simulator_worker_threads"),
            "runtime_closure": copy.deepcopy(dict(manifest["runtime_closure"])),
            "artifacts": copy.deepcopy(dict(artifacts)),
            "workload_runtime_qualification": copy.deepcopy(
                dict(artifacts["workload_runtime_qualification"])
            ),
            "simulator_stability": simulator_stability,
        }
    )
    return result


def _executed_split(
    source: Mapping[str, Any],
    source_ref: Mapping[str, Any],
    corpus_id: str,
) -> Dict[str, Any]:
    split = copy.deepcopy(dict(source))
    split.pop("corpus_manifest_sha256", None)
    split.update(
        {
            "schema_version": SPLIT_SCHEMA,
            "status": "EXECUTED",
            "corpus_id": corpus_id,
            "source_split_manifest": copy.deepcopy(dict(source_ref)),
            "immutable_after_finalization": True,
        }
    )
    return split


def finalize_corpus(
    *,
    corpus_manifest: Path,
    execution_root: Path,
    simulator_binary: Path,
    output_dir: Path,
    _runtime_authority: Optional[Any] = None,
) -> Mapping[str, Any]:
    """Audit all planned runs and atomically publish the permitted result."""

    corpus_path = _lexical_absolute(corpus_manifest)
    binary_lexical = _lexical_absolute(simulator_binary)
    try:
        # The repository's supported entry point is bin/SimAI_simulator, a
        # symlink to the build artifact.  Resolve that leaf once, then bind
        # and record the canonical executable; run/output trees remain
        # strictly symlink-free.
        binary_path = binary_lexical.resolve(strict=True)
    except OSError as exc:
        raise FinalizationError(
            f"simulator binary is missing: {binary_lexical}"
        ) from exc
    execution = _lexical_absolute(execution_root)
    _reject_existing_symlink_components(corpus_path, "corpus manifest")
    _reject_existing_symlink_components(binary_path, "simulator binary")
    if execution.exists() or execution.is_symlink():
        _reject_existing_symlink_components(execution, "execution root")
        if execution.is_symlink() or not execution.is_dir():
            raise FinalizationError(
                f"execution root is not a plain directory: {execution}"
            )
    try:
        corpus = runner.validate_corpus(corpus_path, binary_path)
    except runner.RunnerError as exc:
        raise FinalizationError(str(exc)) from exc
    if corpus.manifest.get("status") != "PREPARED":
        raise FinalizationError("finalizer input must be a PREPARED corpus")
    if corpus.manifest.get("identity_schema") != IDENTITY_SCHEMA:
        raise FinalizationError("finalizer requires v2 corpus identity")
    output = _safe_output_path(output_dir, corpus.root, execution)

    global_blockers: List[Dict[str, Any]] = []
    runtime_authority = _runtime_authority or FinalizerRuntimeAuthority()
    current_runtime: Optional[CurrentSimulatorRuntime]
    try:
        current_runtime = runtime_authority.discover_current(binary_path)
        if (
            not isinstance(current_runtime, CurrentSimulatorRuntime)
            or not runner.SHA256_RE.fullmatch(current_runtime.identity_sha256)
        ):
            raise RuntimeAuditError(
                "CURRENT_SIMULATOR_RUNTIME_INVALID",
                "runtime authority returned an invalid current closure",
            )
    except (RuntimeAuditError, runner.RunnerError, OSError, ValueError) as exc:
        current_runtime = None
        global_blockers.append(
            {
                "reason_code": getattr(
                    exc, "reason_code", "CURRENT_SIMULATOR_RUNTIME_INVALID"
                ),
                "reason": str(exc),
            }
        )
    if len(corpus.manifest["runs"]) != EXPECTED_RUN_COUNT:
        global_blockers.append(
            {
                "reason_code": "UNEXPECTED_RUN_COUNT",
                "expected": EXPECTED_RUN_COUNT,
                "observed": len(corpus.manifest["runs"]),
            }
        )
    try:
        source_split, source_split_ref = _source_split(corpus)
    except FinalizationError as exc:
        source_split = {}
        source_split_ref = {}
        global_blockers.append(
            {
                "reason_code": "INVALID_SOURCE_SPLIT",
                "reason": str(exc),
            }
        )

    records: List[Dict[str, Any]] = []
    validated: Dict[str, Tuple[Mapping[str, Any], Dict[str, Dict[str, Any]]]] = {}
    for run in corpus.manifest["runs"]:
        record, run_manifest, artifacts = _audit_record(
            run,
            execution,
            corpus,
            current_runtime,
            runtime_authority,
        )
        records.append(record)
        if record["status"] == "VERIFIED" and run_manifest is not None:
            validated[str(run["run_id"])] = (run_manifest, artifacts)

    worker_thread_counts = Counter(
        int(manifest["simulator_worker_threads"])
        for manifest, _artifacts in validated.values()
    )
    if len(worker_thread_counts) > 1:
        global_blockers.append(
            {
                "reason_code": "MIXED_SIMULATOR_WORKER_THREADS",
                "counts": {
                    str(worker_threads): count
                    for worker_threads, count in sorted(worker_thread_counts.items())
                },
            }
        )

    observed_runtime_identity_counts = Counter(
        str(record["runtime_closure_identity_sha256"])
        for record in records
        if isinstance(record.get("runtime_closure_identity_sha256"), str)
    )
    if len(observed_runtime_identity_counts) > 1:
        global_blockers.append(
            {
                "reason_code": "MIXED_RUNTIME_CLOSURE_IDENTITIES",
                "counts": dict(sorted(observed_runtime_identity_counts.items())),
            }
        )
    verified_runtime_records = [
        manifest["runtime_closure"]
        for manifest, _artifacts in validated.values()
        if isinstance(manifest.get("runtime_closure"), Mapping)
    ]
    verified_runtime_authority_counts = Counter(
        runner.canonical_hash(record) for record in verified_runtime_records
    )
    if len(verified_runtime_authority_counts) > 1:
        global_blockers.append(
            {
                "reason_code": "MIXED_RUNTIME_CLOSURE_AUTHORITIES",
                "counts": dict(sorted(verified_runtime_authority_counts.items())),
            }
        )
    simulator_runtime_closure: Optional[Mapping[str, Any]] = (
        copy.deepcopy(dict(verified_runtime_records[0]))
        if len(verified_runtime_authority_counts) == 1
        else None
    )
    runtime_closure_audit = _runtime_summary(current_runtime, records)

    runs_root = execution / "runs"
    expected_ids = {str(run["run_id"]) for run in corpus.manifest["runs"]}
    unexpected: List[str] = []
    if runs_root.exists():
        if runs_root.is_symlink() or not runs_root.is_dir():
            global_blockers.append(
                {
                    "reason_code": "INVALID_RUNS_ROOT",
                    "reason": f"runs root is not a plain directory: {runs_root}",
                }
            )
        else:
            unexpected = sorted(
                entry.name
                for entry in runs_root.iterdir()
                if entry.name not in expected_ids
            )
            if unexpected:
                global_blockers.append(
                    {
                        "reason_code": "UNEXPECTED_RUN_ENTRIES",
                        "count": len(unexpected),
                        "entries": unexpected[:64],
                    }
                )

    audit = _base_audit(
        corpus,
        execution,
        records,
        global_blockers,
        runtime_closure_audit,
        simulator_runtime_closure,
    )
    audit["simulator_worker_threads"] = {
        "values": sorted(worker_thread_counts),
        "counts": {
            str(worker_threads): count
            for worker_threads, count in sorted(worker_thread_counts.items())
        },
    }
    audit["readiness_binding"]["simulator_worker_threads"] = copy.deepcopy(
        audit["simulator_worker_threads"]
    )
    audit["readiness_binding"]["simulator_runtime_closure"] = (
        copy.deepcopy(dict(simulator_runtime_closure))
        if simulator_runtime_closure is not None
        else None
    )
    audit["readiness_binding_sha256"] = runner.canonical_hash(
        audit["readiness_binding"]
    )
    if global_blockers or audit["counts"]["blocked_total"]:
        _assert_primary_inputs_unchanged(
            corpus, current_runtime, runtime_authority
        )
        _publish_bundle(
            output,
            {"readiness_audit.json": _json_bytes(audit)},
        )
        return audit

    # No executed identity is materialized until every run has passed both the
    # runner validator and the finalizer's stronger required-artifact checks.
    executed = copy.deepcopy(dict(corpus.manifest))
    _absolutize_prepared_refs(executed, corpus)
    executed_runs: List[Dict[str, Any]] = []
    for run in corpus.manifest["runs"]:
        run_id = str(run["run_id"])
        manifest, artifacts = validated[run_id]
        executed_runs.append(
            _executed_run(run, manifest, artifacts, records[len(executed_runs)])
        )
    executed["runs"] = executed_runs
    qualification = executed.get("workload_qualification")
    if not isinstance(qualification, MutableMapping):
        raise FinalizationError("prepared corpus workload_qualification is invalid")
    qualification.update(
        {
            "status": "PASS",
            "runtime_qualified_run_count": len(executed_runs),
            "runtime_qualification_set_sha256": runner.canonical_hash(
                [
                    {
                        "run_id": run["run_id"],
                        "sha256": run["artifacts"]["workload_runtime_qualification"][
                            "sha256"
                        ],
                    }
                    for run in executed_runs
                ]
            ),
        }
    )
    executed.update(
        {
            "schema_version": CORPUS_SCHEMA,
            "status": "EXECUTED",
            "identity_schema": IDENTITY_SCHEMA,
            "source_corpus": {
                "path": str(corpus.manifest_path),
                "size_bytes": corpus.manifest_path.stat().st_size,
                "sha256": corpus.manifest_sha256,
                "status": "PREPARED",
            },
            "execution_root": str(execution),
            "simulator": _file_ref(corpus.simulator_binary),
            "simulator_runtime_closure": copy.deepcopy(
                dict(simulator_runtime_closure)
            ),
            "simulator_worker_threads": next(iter(worker_thread_counts)),
            "immutable_after_finalization": True,
        }
    )

    split = _executed_split(
        source_split, source_split_ref, str(corpus.manifest["corpus_id"])
    )
    split_payload = _json_bytes(split)
    executed["split_manifest"] = {
        "path": "split_manifest.json",
        "sha256": _sha256_bytes(split_payload),
        "size_bytes": len(split_payload),
        "paired_holdout_gpu_ids": split.get("paired_holdout_gpu_ids"),
        "generated_before_training": split.get("generated_before_training"),
    }
    corpus_payload = _json_bytes(executed)
    audit["status"] = "READY"
    audit["outputs"] = {
        "corpus_manifest": {
            "path": str(output / "corpus_manifest.json"),
            "sha256": _sha256_bytes(corpus_payload),
            "size_bytes": len(corpus_payload),
        },
        "split_manifest": {
            "path": str(output / "split_manifest.json"),
            "sha256": _sha256_bytes(split_payload),
            "size_bytes": len(split_payload),
        },
    }
    audit["finalized_run_set_sha256"] = runner.canonical_hash(executed_runs)
    audit["publication_binding_sha256"] = runner.canonical_hash(
        {
            "readiness_binding_sha256": audit["readiness_binding_sha256"],
            "outputs": audit["outputs"],
            "finalized_run_set_sha256": audit["finalized_run_set_sha256"],
        }
    )
    _assert_primary_inputs_unchanged(corpus, current_runtime, runtime_authority)
    _publish_bundle(
        output,
        {
            "corpus_manifest.json": corpus_payload,
            "split_manifest.json": split_payload,
            "readiness_audit.json": _json_bytes(audit),
        },
    )
    return audit


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-manifest", required=True, type=Path)
    parser.add_argument("--execution-root", required=True, type=Path)
    parser.add_argument("--simulator-binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        audit = finalize_corpus(
            corpus_manifest=args.corpus_manifest,
            execution_root=args.execution_root,
            simulator_binary=args.simulator_binary,
            output_dir=args.output_dir,
        )
    except FinalizationError as exc:
        print(f"P2 finalization refused: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": audit["status"],
                "counts": audit["counts"],
                "output_dir": str(_lexical_absolute(args.output_dir)),
            },
            sort_keys=True,
        )
    )
    return 0 if audit["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
