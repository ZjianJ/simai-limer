#!/usr/bin/env python3
"""Create or verify a content-addressed LIMER baseline manifest.

The tool is deliberately read-only with respect to the repository and experiment
artifacts.  Creating a manifest writes only ``--output``; checking one writes
nothing.  A freeze records both Git revisions *and* dirty working-tree content,
because a commit ID alone is not sufficient for the current LIMER experiments.

Examples::

    python3 limer/tools/freeze_baseline.py \
      --repo-root . --preset true16-hard-fault-e2e \
      --output /tmp/true16-baseline.json

    python3 limer/tools/freeze_baseline.py \
      --repo-root . --check /tmp/true16-baseline.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


SCHEMA_VERSION = "limer.baseline-freeze.v1"
PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


TRUE16_PRESET = {
    "label": "true16-hard-fault-e2e",
    "command": "bash limer/scripts/run_true16_hard_fault_e2e.sh",
    "environment": [
        "LIMER_TRUE16_OUT_ROOT",
        "SIMAI_TRUE16_TIMEOUT_S",
        "LIMER_TRUE16_FAULT_START_NS",
    ],
    "paths": [
        "bin/SimAI_simulator",
        "limer/configs/experiment_contract.yaml",
        "limer/configs/SimAI.baseline.conf",
        "limer/configs/microAllReduce_16rank_hardfault.txt",
        "limer/docs/experiment_contract.md",
        "limer/scripts/run_true16_hard_fault_e2e.sh",
        "limer/tools/prepare_true16_hard_fault.py",
        "limer/tools/validate_true16_dualrail.py",
        "limer/tools/evaluate_true16_hard_fault_e2e.py",
        "astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py",
        "limer/results/true16_hard_fault_e2e",
    ],
}

CONTRACT_REQUIRED_MANIFEST_FIELDS = (
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
)

VOLATILE_UNTRACKED_POLICY = (
    "Python bytecode under __pycache__ or with .pyc/.pyo suffix is excluded; "
    "source files remain content-addressed"
)


class FreezeError(RuntimeError):
    """A manifest cannot be created or verified safely."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return sha256_bytes(encoded)


def run_git(repo: Path, *arguments: str, allow_failure: bool = False) -> bytes:
    process = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if process.returncode and not allow_failure:
        message = process.stderr.decode("utf-8", errors="replace").strip()
        raise FreezeError(
            f"git -C {repo} {' '.join(arguments)} failed: {message}"
        )
    return process.stdout


def repository_relative(path: Path, repo_root: Path) -> Optional[str]:
    # ``resolve`` would erase the identity of an artifact symlink (notably
    # bin/SimAI_simulator).  Normalize ``..`` lexically while preserving the
    # final path component as a symlink.
    absolute = Path(os.path.abspath(path))
    try:
        relative = absolute.relative_to(repo_root.resolve())
    except ValueError:
        return None
    return relative.as_posix() or "."


def describe_path(path: Path, repo_root: Path) -> str:
    relative = repository_relative(path, repo_root)
    return relative if relative is not None else str(path.resolve(strict=False))


def untracked_files(
    repo: Path, excluded: Set[str]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    raw = run_git(repo, "ls-files", "--others", "--exclude-standard", "-z")
    entries: List[Dict[str, Any]] = []
    volatile_paths: List[str] = []
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        relative = encoded.decode("utf-8", errors="surrogateescape")
        if relative in excluded:
            continue
        relative_path = Path(relative)
        if (
            "__pycache__" in relative_path.parts
            or relative_path.suffix in {".pyc", ".pyo"}
        ):
            volatile_paths.append(relative)
            continue
        path = repo / relative
        if path.is_symlink():
            target = os.readlink(path)
            entries.append(
                {
                    "path": relative,
                    "kind": "symlink",
                    "target": target,
                    "sha256": sha256_bytes(target.encode("utf-8")),
                }
            )
        elif path.is_file():
            entries.append(
                {
                    "path": relative,
                    "kind": "file",
                    "size_bytes": path.stat().st_size,
                    "mode": stat.S_IMODE(path.stat().st_mode),
                    "sha256": sha256_file(path),
                }
            )
        else:
            # Git may report a path that changed concurrently.  Preserve that
            # fact so a later check cannot silently treat the snapshot as clean.
            entries.append({"path": relative, "kind": "missing"})
    return (
        sorted(entries, key=lambda item: item["path"]),
        sorted(volatile_paths),
    )


def snapshot_git_repository(repo: Path, excluded: Iterable[str] = ()) -> Dict[str, Any]:
    repo = repo.resolve()
    inside = run_git(repo, "rev-parse", "--is-inside-work-tree").strip()
    if inside != b"true":
        raise FreezeError(f"not a Git working tree: {repo}")

    head = run_git(repo, "rev-parse", "HEAD").decode().strip()
    branch_raw = run_git(
        repo, "symbolic-ref", "--quiet", "--short", "HEAD", allow_failure=True
    )
    branch = branch_raw.decode("utf-8", errors="replace").strip() or None
    staged = run_git(repo, "diff", "--binary", "--no-ext-diff", "--cached", "--")
    worktree = run_git(repo, "diff", "--binary", "--no-ext-diff", "--")
    untracked, volatile_untracked = untracked_files(repo, set(excluded))
    fingerprint_material = {
        "staged_diff_sha256": sha256_bytes(staged),
        "staged_diff_size_bytes": len(staged),
        "worktree_diff_sha256": sha256_bytes(worktree),
        "worktree_diff_size_bytes": len(worktree),
        "untracked_files": untracked,
    }
    return {
        "head": head,
        "branch": branch,
        "dirty": bool(staged or worktree or untracked or volatile_untracked),
        "reproducible_dirty": bool(staged or worktree or untracked),
        "volatile_untracked_policy": VOLATILE_UNTRACKED_POLICY,
        "volatile_untracked_paths_at_freeze": volatile_untracked,
        **fingerprint_material,
        "working_tree_fingerprint_sha256": canonical_json_hash(fingerprint_material),
    }


SUBMODULE_LINE = re.compile(
    r"^(?P<state>[ +\-U])(?P<commit>[0-9a-fA-F]+) "
    r"(?P<path>.*?)(?: \((?P<description>.*)\))?$"
)


def discover_submodules(repo_root: Path) -> List[Dict[str, Any]]:
    output = run_git(repo_root, "submodule", "status", "--recursive", allow_failure=True)
    submodules: List[Dict[str, Any]] = []
    for raw_line in output.decode("utf-8", errors="replace").splitlines():
        match = SUBMODULE_LINE.match(raw_line)
        if not match:
            raise FreezeError(f"cannot parse git submodule status line: {raw_line!r}")
        relative = match.group("path")
        path = repo_root / relative
        initialized = False
        if path.exists() and match.group("state") != "-":
            initialized = (
                run_git(
                    path,
                    "rev-parse",
                    "--is-inside-work-tree",
                    allow_failure=True,
                ).strip()
                == b"true"
            )
        entry: Dict[str, Any] = {
            "path": relative,
            "gitlink_state": match.group("state"),
            "observed_commit": match.group("commit"),
            "description": match.group("description"),
            "initialized": initialized,
        }
        if initialized:
            entry["repository"] = snapshot_git_repository(path)
        submodules.append(entry)
    return sorted(submodules, key=lambda item: item["path"])


def artifact_entry(path: Path, repo_root: Path) -> Dict[str, Any]:
    display_path = describe_path(path, repo_root)
    if path.is_symlink():
        link_target = os.readlink(path)
        entry: Dict[str, Any] = {
            "path": display_path,
            "kind": "symlink",
            "link_target": link_target,
            "link_target_sha256": sha256_bytes(link_target.encode("utf-8")),
        }
        resolved = path.resolve(strict=False)
        if resolved.is_file():
            entry["resolved_target"] = describe_path(resolved, repo_root)
            entry["resolved_size_bytes"] = resolved.stat().st_size
            entry["resolved_sha256"] = sha256_file(resolved)
        else:
            entry["resolved_target_missing"] = True
        return entry
    if not path.is_file():
        raise FreezeError(f"artifact is not a regular file or symlink: {path}")
    file_stat = path.stat()
    return {
        "path": display_path,
        "kind": "file",
        "size_bytes": file_stat.st_size,
        "mode": stat.S_IMODE(file_stat.st_mode),
        "sha256": sha256_file(path),
    }


def expand_artifacts(
    include_paths: Iterable[Path], repo_root: Path, output_path: Path
) -> List[Dict[str, Any]]:
    files: Dict[str, Path] = {}
    output_resolved = output_path.resolve(strict=False)
    for supplied in include_paths:
        path = supplied if supplied.is_absolute() else repo_root / supplied
        if not path.exists() and not path.is_symlink():
            raise FreezeError(f"included artifact does not exist: {path}")
        candidates = [path]
        if path.is_dir() and not path.is_symlink():
            candidates = sorted(
                (item for item in path.rglob("*") if item.is_file() or item.is_symlink()),
                key=lambda item: item.as_posix(),
            )
        for candidate in candidates:
            if candidate.resolve(strict=False) == output_resolved:
                continue
            key = describe_path(candidate, repo_root)
            files[key] = candidate
    return [artifact_entry(files[key], repo_root) for key in sorted(files)]


def runtime_environment(selected_variables: Iterable[str], repo_root: Path) -> Dict[str, Any]:
    variables = {name: os.environ.get(name) for name in sorted(set(selected_variables))}
    cwd = Path.cwd()
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "cwd": describe_path(cwd, repo_root),
        "selected_variables": variables,
    }


def parse_parameters(values: Iterable[str]) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise FreezeError(f"parameter must use NAME=VALUE: {value!r}")
        name, parameter_value = value.split("=", 1)
        if not name:
            raise FreezeError(f"parameter name is empty: {value!r}")
        if name in parsed:
            raise FreezeError(f"duplicate parameter: {name}")
        parsed[name] = parameter_value
    return dict(sorted(parsed.items()))


def find_artifact(
    artifacts: Iterable[Mapping[str, Any]], path_value: str
) -> Optional[Mapping[str, Any]]:
    return next((item for item in artifacts if item.get("path") == path_value), None)


def artifact_digest(
    artifacts: Iterable[Mapping[str, Any]], path_value: str
) -> Optional[str]:
    artifact = find_artifact(artifacts, path_value)
    if artifact is None:
        return None
    return artifact.get("sha256") or artifact.get("resolved_sha256")


def read_contract_id(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^contract_id:\s*([^#\s]+)\s*(?:#.*)?$", line)
        if match:
            return match.group(1)
    return None


def contract_provenance(
    *,
    repo_root: Path,
    preset: Optional[str],
    artifacts: List[Dict[str, Any]],
    git_snapshot: Mapping[str, Any],
    parameters: Mapping[str, str],
) -> Dict[str, Any]:
    """Map a suite freeze onto the normative per-run provenance vocabulary.

    A baseline freeze can represent more than one run (the true-16 preset has
    healthy and hard-disconnect roles), so run-only fields are explicit nulls
    with reasons rather than being silently omitted or guessed from mtimes.
    """

    contract_path = "limer/configs/experiment_contract.yaml"
    topology_path = (
        "limer/results/true16_hard_fault_e2e/topology/"
        "Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
    )
    workload_path = "limer/configs/microAllReduce_16rank_hardfault.txt"
    config_path = "limer/configs/SimAI.baseline.conf"
    schedule_path = "limer/results/true16_hard_fault_e2e/fault_events.csv"
    topology_validation_path = (
        "limer/results/true16_hard_fault_e2e/topology_validation.json"
    )

    submodules = {
        item.get("path"): item for item in git_snapshot.get("submodules", [])
    }
    ns3 = submodules.get("ns-3-alibabacloud", {})
    superproject = git_snapshot["superproject"]
    any_dirty = bool(superproject.get("dirty")) or any(
        bool(item.get("repository", {}).get("dirty"))
        for item in git_snapshot.get("submodules", [])
    )

    values: Dict[str, Any] = {
        "contract_id": read_contract_id(repo_root / contract_path),
        "contract_sha256": artifact_digest(artifacts, contract_path),
        "simai_revision": superproject.get("head"),
        "ns3_revision": ns3.get("repository", {}).get("head"),
        "dirty_worktree": any_dirty,
        "topology_path": topology_path if find_artifact(artifacts, topology_path) else None,
        "topology_sha256": artifact_digest(artifacts, topology_path),
        "workload_path": workload_path if find_artifact(artifacts, workload_path) else None,
        "workload_sha256": artifact_digest(artifacts, workload_path),
        "simulator_config_path": config_path if find_artifact(artifacts, config_path) else None,
        "simulator_config_sha256": artifact_digest(artifacts, config_path),
        "detector_id": parameters.get("detector_id") or (
            "H0-switch-carrier-event"
            if preset == "true16-hard-fault-e2e"
            else None
        ),
        "detector_artifact_sha256": parameters.get("detector_artifact_sha256"),
        "split_manifest_sha256": parameters.get("split_manifest_sha256"),
        "schedule_sha256": artifact_digest(artifacts, schedule_path),
        "run_id": parameters.get("run_id"),
        "run_role": parameters.get("run_role"),
        "virtual_start_ns": parameters.get("virtual_start_ns"),
        "virtual_finish_ns": parameters.get("virtual_finish_ns"),
        "wall_start_utc": parameters.get("wall_start_utc"),
        "wall_finish_utc": parameters.get("wall_finish_utc"),
        "exit_code": parameters.get("exit_code"),
    }
    reasons = {
        "detector_artifact_sha256": (
            "H0 is embedded in the frozen SimAI/ns-3 dirty source snapshots; "
            "there is no standalone model artifact"
        ),
        "split_manifest_sha256": "the H0 healthy/fault suite has no ML data split",
        "run_id": "suite-level freeze contains healthy and hard_disconnect runs",
        "run_role": "suite-level freeze contains more than one run role",
        "virtual_start_ns": "historical suite has no single aggregate virtual start",
        "virtual_finish_ns": "historical suite has no single aggregate virtual finish",
        "wall_start_utc": "historical runner did not persist a wall start timestamp",
        "wall_finish_utc": "historical runner did not persist a wall finish timestamp",
        "exit_code": (
            "suite-level freeze hashes each role's exit_code.txt instead of "
            "inventing one aggregate exit code"
        ),
    }
    for field in CONTRACT_REQUIRED_MANIFEST_FIELDS:
        if field not in values:
            values[field] = None
        if values[field] is None and field not in reasons:
            reasons[field] = "not supplied or not applicable to this baseline freeze"
    values["not_applicable_reasons"] = {
        field: reasons[field]
        for field in CONTRACT_REQUIRED_MANIFEST_FIELDS
        if values[field] is None
    }
    values["topology_validation_sha256"] = artifact_digest(
        artifacts, topology_validation_path
    )
    values["suite_exit_code_artifacts"] = {
        role: artifact_digest(
            artifacts,
            f"limer/results/true16_hard_fault_e2e/{role}/exit_code.txt",
        )
        for role in ("healthy", "hard_disconnect")
    }
    return values


def create_manifest(
    *,
    repo_root: Path,
    output_path: Path,
    include_paths: Iterable[Path],
    label: str,
    command: Optional[str],
    parameters: Mapping[str, str],
    environment_variables: Iterable[str],
    preset: Optional[str] = None,
) -> Dict[str, Any]:
    repo_root = repo_root.resolve()
    output_path = output_path.resolve(strict=False)
    output_relative = repository_relative(output_path, repo_root)
    exclusions = {output_relative} if output_relative is not None else set()

    artifacts = expand_artifacts(include_paths, repo_root, output_path)
    git_snapshot = {
        "superproject": snapshot_git_repository(repo_root, exclusions),
        "submodules": discover_submodules(repo_root),
    }
    experiment = {
        "command": command,
        "command_argv": shlex.split(command) if command else None,
        "parameters": dict(sorted(parameters.items())),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "label": label,
        "preset": preset,
        "repository_root_hint": str(repo_root),
        "git_snapshot_exclusions": sorted(exclusions),
        "git": git_snapshot,
        "artifacts": artifacts,
        "environment": runtime_environment(environment_variables, repo_root),
        "experiment": experiment,
        "contract_provenance": contract_provenance(
            repo_root=repo_root,
            preset=preset,
            artifacts=artifacts,
            git_snapshot=git_snapshot,
            parameters=parameters,
        ),
    }
    manifest["content_fingerprint_sha256"] = canonical_json_hash(
        {
            "git": manifest["git"],
            "artifacts": artifacts,
            "experiment": experiment,
            "contract_provenance": manifest["contract_provenance"],
        }
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def check_item(
    checks: List[Dict[str, str]], name: str, expected: Any, observed: Any
) -> None:
    status_value = PASS if expected == observed else FAIL

    def compact(value: Any) -> str:
        rendered = repr(value)
        if len(rendered) <= 500:
            return rendered
        return (
            f"<{type(value).__name__} repr_bytes={len(rendered.encode('utf-8'))} "
            f"sha256={sha256_bytes(rendered.encode('utf-8'))}>"
        )

    checks.append(
        {
            "name": name,
            "status": status_value,
            "detail": f"expected={compact(expected)}, observed={compact(observed)}",
        }
    )


def resolve_manifest_artifact(path_value: str, repo_root: Path) -> Path:
    candidate = Path(path_value)
    return candidate if candidate.is_absolute() else repo_root / candidate


def verify_artifact(expected: Mapping[str, Any], repo_root: Path) -> List[Dict[str, str]]:
    checks: List[Dict[str, str]] = []
    path = resolve_manifest_artifact(str(expected["path"]), repo_root)
    kind = expected.get("kind")
    exists = path.exists() or path.is_symlink()
    check_item(checks, f"artifact:{expected['path']}:exists", True, exists)
    if not exists:
        return checks
    if kind == "file":
        check_item(checks, f"artifact:{expected['path']}:kind", True, path.is_file() and not path.is_symlink())
        if path.is_file() and not path.is_symlink():
            check_item(checks, f"artifact:{expected['path']}:size", expected.get("size_bytes"), path.stat().st_size)
            check_item(checks, f"artifact:{expected['path']}:mode", expected.get("mode"), stat.S_IMODE(path.stat().st_mode))
            check_item(checks, f"artifact:{expected['path']}:sha256", expected.get("sha256"), sha256_file(path))
    elif kind == "symlink":
        check_item(checks, f"artifact:{expected['path']}:kind", True, path.is_symlink())
        if path.is_symlink():
            target = os.readlink(path)
            check_item(checks, f"artifact:{expected['path']}:link_target", expected.get("link_target"), target)
            resolved = path.resolve(strict=False)
            target_exists = resolved.is_file()
            expected_missing = bool(expected.get("resolved_target_missing", False))
            check_item(checks, f"artifact:{expected['path']}:resolved_exists", not expected_missing, target_exists)
            if target_exists and "resolved_sha256" in expected:
                check_item(checks, f"artifact:{expected['path']}:resolved_size", expected.get("resolved_size_bytes"), resolved.stat().st_size)
                check_item(checks, f"artifact:{expected['path']}:resolved_sha256", expected.get("resolved_sha256"), sha256_file(resolved))
    else:
        checks.append({"name": f"artifact:{expected['path']}:kind", "status": FAIL, "detail": f"unknown manifest kind={kind!r}"})
    return checks


def compare_git_snapshot(
    checks: List[Dict[str, str]], prefix: str, expected: Mapping[str, Any], observed: Mapping[str, Any]
) -> None:
    for field in (
        "head",
        "branch",
        "dirty",
        "reproducible_dirty",
        "volatile_untracked_policy",
        "staged_diff_sha256",
        "staged_diff_size_bytes",
        "worktree_diff_sha256",
        "worktree_diff_size_bytes",
        "untracked_files",
        "working_tree_fingerprint_sha256",
    ):
        check_item(checks, f"{prefix}:{field}", expected.get(field), observed.get(field))


def verify_manifest(
    manifest_path: Path,
    repo_root_override: Optional[Path] = None,
    strict_environment: bool = False,
) -> Dict[str, Any]:
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise FreezeError(
            f"unsupported schema_version={manifest.get('schema_version')!r}"
        )
    repo_root = (
        repo_root_override.resolve()
        if repo_root_override is not None
        else Path(manifest["repository_root_hint"]).resolve()
    )
    checks: List[Dict[str, str]] = []

    fingerprint_material = {
        "git": manifest.get("git"),
        "artifacts": manifest.get("artifacts"),
        "experiment": manifest.get("experiment"),
        "contract_provenance": manifest.get("contract_provenance"),
    }
    check_item(
        checks,
        "manifest:content_fingerprint_sha256",
        manifest.get("content_fingerprint_sha256"),
        canonical_json_hash(fingerprint_material),
    )

    expected_git = manifest["git"]
    exclusions = set(manifest.get("git_snapshot_exclusions", []))
    observed_superproject = snapshot_git_repository(repo_root, exclusions)
    compare_git_snapshot(
        checks,
        "git:superproject",
        expected_git["superproject"],
        observed_superproject,
    )

    observed_submodule_list = discover_submodules(repo_root)
    observed_submodules = {item["path"]: item for item in observed_submodule_list}
    expected_submodules = {item["path"]: item for item in expected_git["submodules"]}
    check_item(
        checks,
        "git:submodule_paths",
        sorted(expected_submodules),
        sorted(observed_submodules),
    )
    for path_value in sorted(set(expected_submodules) & set(observed_submodules)):
        expected = expected_submodules[path_value]
        observed = observed_submodules[path_value]
        for field in ("gitlink_state", "observed_commit", "initialized"):
            check_item(
                checks,
                f"git:submodule:{path_value}:{field}",
                expected.get(field),
                observed.get(field),
            )
        if "repository" in expected and "repository" in observed:
            compare_git_snapshot(
                checks,
                f"git:submodule:{path_value}:repository",
                expected["repository"],
                observed["repository"],
            )
        elif "repository" in expected or "repository" in observed:
            check_item(
                checks,
                f"git:submodule:{path_value}:repository_available",
                "repository" in expected,
                "repository" in observed,
            )

    for artifact in manifest["artifacts"]:
        checks.extend(verify_artifact(artifact, repo_root))

    observed_contract = contract_provenance(
        repo_root=repo_root,
        preset=manifest.get("preset"),
        artifacts=manifest["artifacts"],
        git_snapshot={
            "superproject": observed_superproject,
            "submodules": observed_submodule_list,
        },
        parameters=manifest.get("experiment", {}).get("parameters", {}),
    )
    check_item(
        checks,
        "manifest:contract_provenance",
        manifest.get("contract_provenance"),
        observed_contract,
    )

    selected_names = manifest.get("environment", {}).get("selected_variables", {}).keys()
    observed_environment = runtime_environment(selected_names, repo_root)
    expected_environment = manifest.get("environment", {})
    for field in (
        "system",
        "release",
        "machine",
        "python_implementation",
        "python_version",
        "python_executable",
        "selected_variables",
    ):
        expected_value = expected_environment.get(field)
        observed_value = observed_environment.get(field)
        if expected_value == observed_value:
            status_value = PASS
        else:
            status_value = FAIL if strict_environment else WARN
        checks.append(
            {
                "name": f"environment:{field}",
                "status": status_value,
                "detail": f"expected={expected_value!r}, observed={observed_value!r}",
            }
        )

    failed = [item for item in checks if item["status"] == FAIL]
    warnings = [item for item in checks if item["status"] == WARN]
    return {
        "schema_version": "limer.baseline-freeze-check.v1",
        "manifest": str(manifest_path),
        "repository_root": str(repo_root),
        "status": FAIL if failed else PASS,
        "summary": {
            "checks": len(checks),
            "failed": len(failed),
            "warnings": len(warnings),
        },
        "checks": checks,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, help="Git superproject root")
    parser.add_argument("--output", type=Path, help="new manifest path")
    parser.add_argument("--check", type=Path, metavar="MANIFEST", help="verify an existing manifest")
    parser.add_argument("--strict-environment", action="store_true", help="treat environment drift as a verification failure")
    parser.add_argument("--preset", choices=["true16-hard-fault-e2e"])
    parser.add_argument("--include", action="append", default=[], type=Path, help="file or directory to hash; repeatable")
    parser.add_argument("--label")
    parser.add_argument("--command", help="experiment command represented by the freeze")
    parser.add_argument("--parameter", action="append", default=[], metavar="NAME=VALUE", help="experiment parameter; repeatable")
    parser.add_argument("--env", action="append", default=[], metavar="NAME", help="selected environment variable to record; repeatable")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.check:
            if args.output or args.include or args.preset or args.label or args.command or args.parameter or args.env:
                parser.error("--check cannot be combined with manifest creation options")
            report = verify_manifest(
                args.check,
                repo_root_override=args.repo_root,
                strict_environment=args.strict_environment,
            )
            print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
            return 0 if report["status"] == PASS else 1

        if args.strict_environment:
            parser.error("--strict-environment is valid only with --check")
        if args.output is None:
            parser.error("--output is required when creating a manifest")
        if args.repo_root is None:
            parser.error("--repo-root is required when creating a manifest")

        include_paths = list(args.include)
        environment_variables = list(args.env)
        label = args.label
        command = args.command
        if args.preset == "true16-hard-fault-e2e":
            include_paths = [Path(item) for item in TRUE16_PRESET["paths"]] + include_paths
            environment_variables = list(TRUE16_PRESET["environment"]) + environment_variables
            label = label or str(TRUE16_PRESET["label"])
            command = command or str(TRUE16_PRESET["command"])
        if not include_paths:
            parser.error("provide at least one --include or select --preset")
        label = label or "limer-baseline"

        manifest = create_manifest(
            repo_root=args.repo_root,
            output_path=args.output,
            include_paths=include_paths,
            label=label,
            command=command,
            parameters=parse_parameters(args.parameter),
            environment_variables=environment_variables,
            preset=args.preset,
        )
        print(
            json.dumps(
                {
                    "status": PASS,
                    "output": str(args.output.resolve()),
                    "artifacts": len(manifest["artifacts"]),
                    "content_fingerprint_sha256": manifest["content_fingerprint_sha256"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (FreezeError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"freeze_baseline: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
