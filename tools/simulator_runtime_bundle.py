#!/usr/bin/env python3
"""Seal the dynamic runtime used by the SimAI ns-3 executable.

Hashing the small scratch executable is insufficient: the simulator logic is
loaded from ``libns3*.so`` files through a mutable build-tree RUNPATH.  This
module discovers that closure with a clean loader environment, copies the
project libraries once into a content-addressed execution-root bundle, and
binds the non-copied system libraries by path and digest.

The module is intentionally independent of the corpus runner.  Callers can
create one bundle per execution root, derive a closed loader environment,
verify the process mappings, and revalidate the bundle before and after a
run.  No caller should fall back to hashing only the executable.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple


BUNDLE_SCHEMA = "limer.simulator-runtime-bundle.v1"
BUNDLE_ROOT_NAME = "runtime-bundles"
MANIFEST_NAME = "runtime_bundle_manifest.json"
SEAL_NAME = "runtime_bundle_manifest.sha256"
EXECUTABLE_RELATIVE = "bin/simulator_binary"
LIB_DIRECTORY_RELATIVE = "lib"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PROJECT_SONAME_RE = re.compile(
    r"^libns3[A-Za-z0-9_.+\-]*\.so(?:\.[0-9]+)*$"
)
LDD_ARROW_RE = re.compile(
    r"^(?P<soname>\S+)\s+=>\s+(?P<target>\S+)\s+\(0x[0-9a-fA-F]+\)$"
)
LDD_DIRECT_RE = re.compile(r"^(?P<target>/\S+)\s+\(0x[0-9a-fA-F]+\)$")
LDD_VIRTUAL_RE = re.compile(
    r"^(?P<soname>linux-vdso\.so(?:\.\d+)?)\s+\(0x[0-9a-fA-F]+\)$"
)
DEFAULT_LDD = Path("/usr/bin/ldd")
DEFAULT_LDD_TIMEOUT_SECONDS = 30.0
DEFAULT_LOCK_TIMEOUT_SECONDS = 120.0


class RuntimeBundleError(RuntimeError):
    """The executable closure or a sealed runtime bundle is invalid."""


@dataclass(frozen=True)
class RuntimeDependency:
    soname: str
    loader_path: Path
    resolved_path: Path
    sha256: str
    size_bytes: int
    category: str

    def identity_entry(self) -> Dict[str, Any]:
        entry = {
            "soname": self.soname,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }
        # Project objects are copied, so their source path is provenance only.
        # System objects remain external runtime inputs and must be bound to
        # both their canonical path and bytes.
        if self.category == "system":
            entry["resolved_path"] = str(self.resolved_path)
        return entry

    def manifest_entry(self) -> Dict[str, Any]:
        return {
            **self.identity_entry(),
            "loader_path": str(self.loader_path),
            "resolved_path": str(self.resolved_path),
        }


@dataclass(frozen=True)
class RuntimeClosure:
    executable: Path
    executable_sha256: str
    executable_size_bytes: int
    project_dependencies: Tuple[RuntimeDependency, ...]
    system_dependencies: Tuple[RuntimeDependency, ...]
    virtual_dependencies: Tuple[str, ...]
    identity_sha256: str

    def identity_material(self) -> Dict[str, Any]:
        return {
            "schema_version": BUNDLE_SCHEMA,
            "executable": {
                "sha256": self.executable_sha256,
                "size_bytes": self.executable_size_bytes,
            },
            "project_dependencies": [
                item.identity_entry() for item in self.project_dependencies
            ],
            "system_dependencies": [
                item.identity_entry() for item in self.system_dependencies
            ],
            "virtual_dependencies": list(self.virtual_dependencies),
        }


@dataclass(frozen=True)
class RuntimeBundle:
    root: Path
    executable: Path
    lib_directory: Path
    identity_sha256: str
    manifest: Mapping[str, Any]
    reused: bool


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _reject_symlink_components(path: Path, description: str) -> None:
    expanded = path.expanduser()
    if ".." in expanded.parts:
        raise RuntimeBundleError(
            f"{description} escapes through a '..' path component: {path}"
        )
    absolute = expanded.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if not current.exists() and not current.is_symlink():
            break
        if current.is_symlink():
            raise RuntimeBundleError(
                f"{description} contains a symlinked path component: {current}"
            )


def _require_plain_file(
    path: Path, description: str, *, allow_symlink: bool = False
) -> Path:
    expanded = path.expanduser()
    if not allow_symlink:
        _reject_symlink_components(expanded, description)
    try:
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise RuntimeBundleError(f"{description} is missing: {path}") from exc
    if not resolved.is_file():
        raise RuntimeBundleError(f"{description} is not a plain file: {resolved}")
    return resolved


def _require_elf(path: Path, description: str) -> None:
    try:
        with path.open("rb") as stream:
            magic = stream.read(4)
    except OSError as exc:
        raise RuntimeBundleError(f"cannot read {description}: {path}: {exc}") from exc
    if magic != b"\x7fELF":
        raise RuntimeBundleError(f"{description} is not an ELF file: {path}")


def _validate_soname(soname: str) -> None:
    if not soname or Path(soname).name != soname or "/" in soname or "\\" in soname:
        raise RuntimeBundleError(f"unsafe dynamic-library soname: {soname!r}")


def _parse_ldd_output(output: str) -> Tuple[Tuple[str, Path], Tuple[str, ...]]:
    """Parse one ``LC_ALL=C ldd`` result without accepting unknown lines."""

    resolved: list[Tuple[str, Path]] = []
    virtual: list[str] = []
    by_soname: Dict[str, Path] = {}
    for line_number, raw in enumerate(output.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if "=> not found" in line:
            soname = line.split("=>", 1)[0].strip()
            raise RuntimeBundleError(f"unresolved dynamic dependency: {soname}")
        if line in {"statically linked", "not a dynamic executable"}:
            raise RuntimeBundleError(f"ldd cannot resolve simulator closure: {line}")
        virtual_match = LDD_VIRTUAL_RE.fullmatch(line)
        if virtual_match:
            soname = virtual_match.group("soname")
            _validate_soname(soname)
            if soname in virtual:
                raise RuntimeBundleError(f"duplicate virtual dependency: {soname}")
            virtual.append(soname)
            continue
        arrow_match = LDD_ARROW_RE.fullmatch(line)
        if arrow_match:
            soname = arrow_match.group("soname")
            target = Path(arrow_match.group("target"))
        else:
            direct_match = LDD_DIRECT_RE.fullmatch(line)
            if not direct_match:
                raise RuntimeBundleError(
                    f"unrecognized ldd output at line {line_number}: {line!r}"
                )
            target = Path(direct_match.group("target"))
            soname = target.name
        _validate_soname(soname)
        if not target.is_absolute():
            raise RuntimeBundleError(
                f"ldd returned a non-absolute path for {soname}: {target}"
            )
        prior = by_soname.get(soname)
        if prior is not None:
            raise RuntimeBundleError(
                f"duplicate dynamic dependency soname {soname}: {prior}, {target}"
            )
        by_soname[soname] = target
        resolved.append((soname, target))
    if not resolved:
        raise RuntimeBundleError("ldd returned no file-backed dependencies")
    return tuple(resolved), tuple(sorted(virtual))


def _clean_loader_environment(
    library_path: Optional[Path] = None,
) -> Dict[str, str]:
    environment = {
        "PATH": "/usr/bin:/bin",
        "LANG": "C",
        "LC_ALL": "C",
    }
    if library_path is not None:
        environment["LD_LIBRARY_PATH"] = str(library_path)
    return environment


def _run_ldd(
    executable: Path,
    *,
    ldd_path: Path = DEFAULT_LDD,
    library_path: Optional[Path] = None,
    timeout_seconds: float = DEFAULT_LDD_TIMEOUT_SECONDS,
) -> Tuple[Tuple[str, Path], Tuple[str, ...]]:
    ldd = _require_plain_file(ldd_path, "ldd executable")
    if not os.access(ldd, os.X_OK):
        raise RuntimeBundleError(f"ldd is not executable: {ldd}")
    try:
        completed = subprocess.run(
            [str(ldd), str(executable)],
            check=False,
            capture_output=True,
            text=True,
            env=_clean_loader_environment(library_path),
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeBundleError(f"ldd failed for {executable}: {exc}") from exc
    combined = "\n".join(
        part for part in (completed.stdout, completed.stderr) if part
    )
    if completed.returncode != 0:
        raise RuntimeBundleError(
            f"ldd exited {completed.returncode} for {executable}: {combined.strip()}"
        )
    return _parse_ldd_output(combined)


def discover_runtime_closure(
    executable: Path,
    *,
    ldd_path: Path = DEFAULT_LDD,
    library_path: Optional[Path] = None,
) -> RuntimeClosure:
    """Discover and hash the executable's complete file-backed loader graph."""

    binary = _require_plain_file(executable, "simulator executable")
    _require_elf(binary, "simulator executable")
    if not os.access(binary, os.X_OK):
        raise RuntimeBundleError(f"simulator executable is not executable: {binary}")
    resolved, virtual = _run_ldd(
        binary, ldd_path=ldd_path, library_path=library_path
    )
    project: list[RuntimeDependency] = []
    system: list[RuntimeDependency] = []
    for soname, loader_path in resolved:
        category = "project" if PROJECT_SONAME_RE.fullmatch(soname) else "system"
        resolved_path = _require_plain_file(
            loader_path,
            f"resolved dependency {soname}",
            allow_symlink=category == "system",
        )
        dependency = RuntimeDependency(
            soname=soname,
            loader_path=loader_path,
            resolved_path=resolved_path,
            sha256=sha256_file(resolved_path),
            size_bytes=resolved_path.stat().st_size,
            category=category,
        )
        (project if dependency.category == "project" else system).append(dependency)
    if not project:
        raise RuntimeBundleError(
            "simulator closure contains no libns3 project dependencies"
        )
    project.sort(key=lambda item: item.soname)
    system.sort(key=lambda item: item.soname)
    closure_without_identity = RuntimeClosure(
        executable=binary,
        executable_sha256=sha256_file(binary),
        executable_size_bytes=binary.stat().st_size,
        project_dependencies=tuple(project),
        system_dependencies=tuple(system),
        virtual_dependencies=virtual,
        identity_sha256="",
    )
    identity = canonical_hash(closure_without_identity.identity_material())
    return RuntimeClosure(
        executable=closure_without_identity.executable,
        executable_sha256=closure_without_identity.executable_sha256,
        executable_size_bytes=closure_without_identity.executable_size_bytes,
        project_dependencies=closure_without_identity.project_dependencies,
        system_dependencies=closure_without_identity.system_dependencies,
        virtual_dependencies=closure_without_identity.virtual_dependencies,
        identity_sha256=identity,
    )


def verify_discovered_sources(closure: RuntimeClosure) -> None:
    """Fail if any source file used to construct ``closure`` has changed."""

    _verify_file_identity(
        closure.executable,
        closure.executable_sha256,
        closure.executable_size_bytes,
        "source executable",
    )
    for dependency in (*closure.project_dependencies, *closure.system_dependencies):
        _verify_file_identity(
            dependency.resolved_path,
            dependency.sha256,
            dependency.size_bytes,
            f"source dependency {dependency.soname}",
        )


def _verify_file_identity(
    path: Path, expected_sha256: str, expected_size: int, description: str
) -> None:
    candidate = _require_plain_file(path, description)
    actual_size = candidate.stat().st_size
    if actual_size != expected_size:
        raise RuntimeBundleError(
            f"{description} size changed: expected {expected_size}, got {actual_size}: "
            f"{candidate}"
        )
    actual_sha256 = sha256_file(candidate)
    if actual_sha256 != expected_sha256:
        raise RuntimeBundleError(
            f"{description} sha256 changed: expected {expected_sha256}, "
            f"got {actual_sha256}: {candidate}"
        )


def _copy_verified(
    source: Path, destination: Path, expected_sha256: str, expected_size: int
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    _verify_file_identity(
        destination,
        expected_sha256,
        expected_size,
        f"copied runtime artifact {destination.name}",
    )


def _artifact_inventory(directory: Path) -> list[Dict[str, Any]]:
    entries: list[Dict[str, Any]] = []
    allowed_directories = {"bin", LIB_DIRECTORY_RELATIVE}
    for root, directory_names, file_names in os.walk(directory, followlinks=False):
        root_path = Path(root)
        for name in directory_names:
            candidate = root_path / name
            if candidate.is_symlink():
                raise RuntimeBundleError(
                    f"runtime bundle contains a symlinked directory: {candidate}"
                )
            relative_directory = candidate.relative_to(directory).as_posix()
            if relative_directory not in allowed_directories:
                raise RuntimeBundleError(
                    "runtime bundle contains an unexpected directory: "
                    f"{relative_directory}"
                )
        for name in file_names:
            path = root_path / name
            relative = path.relative_to(directory).as_posix()
            if relative in {MANIFEST_NAME, SEAL_NAME}:
                continue
            if path.is_symlink() or not path.is_file():
                raise RuntimeBundleError(
                    f"runtime bundle contains a non-plain artifact: {path}"
                )
            entries.append(
                {
                    "path": relative,
                    "sha256": sha256_file(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    return sorted(entries, key=lambda item: item["path"])


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, ensure_ascii=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _write_seal(directory: Path, manifest: Mapping[str, Any]) -> None:
    manifest_path = directory / MANIFEST_NAME
    _write_json(manifest_path, manifest)
    digest = sha256_file(manifest_path)
    seal_path = directory / SEAL_NAME
    with seal_path.open("x", encoding="ascii") as stream:
        stream.write(f"{digest}  {MANIFEST_NAME}\n")
        stream.flush()
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_relative(value: Any, description: str) -> Path:
    if not isinstance(value, str) or not value:
        raise RuntimeBundleError(f"{description} path is missing")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeBundleError(f"unsafe {description} path: {value!r}")
    return relative


def _load_manifest(bundle_root: Path) -> Mapping[str, Any]:
    manifest_path = bundle_root / MANIFEST_NAME
    seal_path = bundle_root / SEAL_NAME
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeBundleError(f"runtime bundle manifest is missing: {manifest_path}")
    if not seal_path.is_file() or seal_path.is_symlink():
        raise RuntimeBundleError(f"runtime bundle seal is missing: {seal_path}")
    fields = seal_path.read_text(encoding="ascii").strip().split()
    if (
        len(fields) != 2
        or fields[1] != MANIFEST_NAME
        or not SHA256_RE.fullmatch(fields[0])
        or sha256_file(manifest_path) != fields[0]
    ):
        raise RuntimeBundleError("runtime bundle manifest seal is invalid")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeBundleError(f"runtime bundle manifest is invalid: {exc}") from exc
    if not isinstance(manifest, Mapping):
        raise RuntimeBundleError("runtime bundle manifest must be one object")
    return manifest


def validate_runtime_bundle(
    bundle_root: Path,
    *,
    expected_closure: Optional[RuntimeClosure] = None,
    verify_system_dependencies: bool = True,
    reused: bool = True,
) -> RuntimeBundle:
    """Validate the seal, inventory, identity, and current system libraries."""

    requested_root = bundle_root.expanduser()
    _reject_symlink_components(requested_root, "runtime bundle path")
    try:
        root = requested_root.resolve(strict=True)
    except OSError as exc:
        raise RuntimeBundleError(f"runtime bundle is missing: {bundle_root}") from exc
    if not root.is_dir() or root.is_symlink():
        raise RuntimeBundleError(f"runtime bundle is not a plain directory: {root}")
    manifest = _load_manifest(root)
    if manifest.get("schema_version") != BUNDLE_SCHEMA or manifest.get("status") != "SEALED":
        raise RuntimeBundleError("runtime bundle schema/status is invalid")
    identity = manifest.get("identity_sha256")
    if not isinstance(identity, str) or not SHA256_RE.fullmatch(identity):
        raise RuntimeBundleError("runtime bundle identity is invalid")
    if root.name != identity:
        raise RuntimeBundleError(
            f"runtime bundle directory does not match identity: {root.name}, {identity}"
        )
    if expected_closure is not None and identity != expected_closure.identity_sha256:
        raise RuntimeBundleError(
            "runtime bundle identity differs from the discovered source closure"
        )

    binary_ref = manifest.get("executable")
    project_refs = manifest.get("project_dependencies")
    system_refs = manifest.get("system_dependencies")
    virtual_refs = manifest.get("virtual_dependencies")
    if (
        not isinstance(binary_ref, Mapping)
        or not isinstance(project_refs, list)
        or not isinstance(system_refs, list)
        or not isinstance(virtual_refs, list)
    ):
        raise RuntimeBundleError("runtime bundle dependency inventory is malformed")
    binary_relative = _safe_relative(binary_ref.get("execution_path"), "executable")
    if binary_relative.as_posix() != EXECUTABLE_RELATIVE:
        raise RuntimeBundleError("runtime bundle executable path is not canonical")
    binary = root / binary_relative
    _verify_file_identity(
        binary,
        str(binary_ref.get("sha256")),
        int(binary_ref.get("size_bytes", -1)),
        "sealed simulator executable",
    )
    _require_elf(binary, "sealed simulator executable")
    if not os.access(binary, os.X_OK):
        raise RuntimeBundleError(f"sealed simulator is not executable: {binary}")

    project_identity: list[Dict[str, Any]] = []
    seen_sonames: set[str] = set()
    for raw in project_refs:
        if not isinstance(raw, Mapping):
            raise RuntimeBundleError("project dependency entry is not an object")
        soname = str(raw.get("soname", ""))
        if not PROJECT_SONAME_RE.fullmatch(soname) or soname in seen_sonames:
            raise RuntimeBundleError(f"invalid/duplicate project dependency: {soname!r}")
        seen_sonames.add(soname)
        relative = _safe_relative(raw.get("execution_path"), f"project {soname}")
        if relative.as_posix() != f"{LIB_DIRECTORY_RELATIVE}/{soname}":
            raise RuntimeBundleError(
                f"project dependency path is not canonical: {relative}"
            )
        _verify_file_identity(
            root / relative,
            str(raw.get("sha256")),
            int(raw.get("size_bytes", -1)),
            f"sealed project dependency {soname}",
        )
        project_identity.append(
            {
                "soname": soname,
                "sha256": raw.get("sha256"),
                "size_bytes": raw.get("size_bytes"),
            }
        )
    if not project_identity:
        raise RuntimeBundleError("runtime bundle has no project dependencies")

    system_identity: list[Dict[str, Any]] = []
    for raw in system_refs:
        if not isinstance(raw, Mapping):
            raise RuntimeBundleError("system dependency entry is not an object")
        soname = str(raw.get("soname", ""))
        _validate_soname(soname)
        if soname in seen_sonames:
            raise RuntimeBundleError(f"duplicate dependency soname: {soname}")
        seen_sonames.add(soname)
        raw_resolved_path = raw.get("resolved_path")
        if not isinstance(raw_resolved_path, str) or not raw_resolved_path:
            raise RuntimeBundleError(
                f"system dependency {soname} canonical path is missing"
            )
        requested_system_path = Path(raw_resolved_path)
        resolved_path = _require_plain_file(
            requested_system_path,
            f"system dependency {soname}",
        )
        if (
            not requested_system_path.is_absolute()
            or str(requested_system_path) != str(resolved_path)
        ):
            raise RuntimeBundleError(
                f"system dependency {soname} path is not canonical: "
                f"{requested_system_path}"
            )
        if verify_system_dependencies:
            _verify_file_identity(
                resolved_path,
                str(raw.get("sha256")),
                int(raw.get("size_bytes", -1)),
                f"system dependency {soname}",
            )
        system_identity.append(
            {
                "soname": soname,
                "sha256": raw.get("sha256"),
                "size_bytes": raw.get("size_bytes"),
                "resolved_path": str(resolved_path),
            }
        )

    identity_material = {
        "schema_version": BUNDLE_SCHEMA,
        "executable": {
            "sha256": binary_ref.get("sha256"),
            "size_bytes": binary_ref.get("size_bytes"),
        },
        "project_dependencies": sorted(project_identity, key=lambda item: item["soname"]),
        "system_dependencies": sorted(system_identity, key=lambda item: item["soname"]),
        "virtual_dependencies": sorted(str(item) for item in virtual_refs),
    }
    if canonical_hash(identity_material) != identity:
        raise RuntimeBundleError("runtime bundle content identity is invalid")
    if expected_closure is not None and identity_material != expected_closure.identity_material():
        raise RuntimeBundleError("runtime bundle manifest differs from source closure")

    recorded_artifacts = manifest.get("artifacts")
    current_artifacts = _artifact_inventory(root)
    if (
        recorded_artifacts != current_artifacts
        or manifest.get("artifact_set_sha256") != canonical_hash(current_artifacts)
    ):
        raise RuntimeBundleError("runtime bundle artifact inventory is invalid")
    return RuntimeBundle(
        root=root,
        executable=binary,
        lib_directory=root / LIB_DIRECTORY_RELATIVE,
        identity_sha256=identity,
        manifest=manifest,
        reused=reused,
    )


def _acquire_bundle_lock(
    bundle_parent: Path,
    timeout_seconds: float,
) -> int:
    """Lock the bundle directory; the kernel releases this lock on a crash."""

    descriptor = os.open(
        bundle_parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    )
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(descriptor)
                raise RuntimeBundleError(
                    f"timed out waiting for runtime bundle lock: {bundle_parent}"
                )
            time.sleep(0.05)


def _clean_orphan_pending(bundle_parent: Path, identity: str) -> None:
    pattern = re.compile(
        rf"^\.{re.escape(identity)}\.pending\.[0-9]+\.[0-9a-f]{{32}}$"
    )
    for candidate in bundle_parent.glob(f".{identity}.pending.*"):
        if not pattern.fullmatch(candidate.name):
            raise RuntimeBundleError(
                f"malformed orphan runtime-bundle path: {candidate}"
            )
        if candidate.is_symlink() or not candidate.is_dir():
            raise RuntimeBundleError(
                f"unsafe orphan runtime-bundle path: {candidate}"
            )
        shutil.rmtree(candidate)


def seal_runtime_bundle(
    execution_root: Path,
    executable: Path,
    *,
    ldd_path: Path = DEFAULT_LDD,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> RuntimeBundle:
    """Create or validate one content-addressed bundle under an execution root."""

    if lock_timeout_seconds <= 0:
        raise RuntimeBundleError("bundle lock timeout must be positive")
    closure = discover_runtime_closure(executable, ldd_path=ldd_path)
    requested_root = execution_root.expanduser()
    _reject_symlink_components(requested_root, "execution root")
    root = requested_root.resolve()
    if root.exists() and not root.is_dir():
        raise RuntimeBundleError(f"execution root is not a plain directory: {root}")
    root.mkdir(parents=True, exist_ok=True)
    bundle_parent = root / BUNDLE_ROOT_NAME
    if bundle_parent.exists() and (
        not bundle_parent.is_dir() or bundle_parent.is_symlink()
    ):
        raise RuntimeBundleError(
            f"runtime bundle root is not a plain directory: {bundle_parent}"
        )
    bundle_parent.mkdir(mode=0o755, exist_ok=True)
    final_path = bundle_parent / closure.identity_sha256
    if final_path.exists():
        return validate_runtime_bundle(
            final_path, expected_closure=closure, reused=True
        )
    lock_descriptor = _acquire_bundle_lock(
        bundle_parent, lock_timeout_seconds
    )
    pending = bundle_parent / (
        f".{closure.identity_sha256}.pending.{os.getpid()}.{uuid.uuid4().hex}"
    )
    try:
        _clean_orphan_pending(bundle_parent, closure.identity_sha256)
        if final_path.exists():
            return validate_runtime_bundle(
                final_path, expected_closure=closure, reused=True
            )
        pending.mkdir(mode=0o755)
        (pending / "bin").mkdir()
        (pending / LIB_DIRECTORY_RELATIVE).mkdir()
        binary_copy = pending / EXECUTABLE_RELATIVE
        _copy_verified(
            closure.executable,
            binary_copy,
            closure.executable_sha256,
            closure.executable_size_bytes,
        )
        binary_copy.chmod(0o555)
        project_manifest: list[Dict[str, Any]] = []
        for dependency in closure.project_dependencies:
            destination = pending / LIB_DIRECTORY_RELATIVE / dependency.soname
            _copy_verified(
                dependency.resolved_path,
                destination,
                dependency.sha256,
                dependency.size_bytes,
            )
            destination.chmod(0o444)
            project_manifest.append(
                {
                    **dependency.manifest_entry(),
                    "execution_path": destination.relative_to(pending).as_posix(),
                }
            )
        verify_discovered_sources(closure)
        artifacts = _artifact_inventory(pending)
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "status": "SEALED",
            "identity_sha256": closure.identity_sha256,
            "created_at": _utc_now(),
            "executable": {
                "source_path": str(closure.executable),
                "execution_path": EXECUTABLE_RELATIVE,
                "sha256": closure.executable_sha256,
                "size_bytes": closure.executable_size_bytes,
            },
            "project_dependencies": project_manifest,
            "system_dependencies": [
                dependency.manifest_entry()
                for dependency in closure.system_dependencies
            ],
            "virtual_dependencies": list(closure.virtual_dependencies),
            "loader_contract": {
                "clear_inherited_prefix": "LD_",
                "LD_LIBRARY_PATH": LIB_DIRECTORY_RELATIVE,
                "LD_PRELOAD": None,
            },
            "artifacts": artifacts,
            "artifact_set_sha256": canonical_hash(artifacts),
            "publish_protocol": "pending_directory_then_atomic_rename",
        }
        _write_seal(pending, manifest)
        _fsync_directory(pending / "bin")
        _fsync_directory(pending / LIB_DIRECTORY_RELATIVE)
        _fsync_directory(pending)
        try:
            pending.rename(final_path)
        except OSError as exc:
            if final_path.exists():
                return validate_runtime_bundle(
                    final_path, expected_closure=closure, reused=True
                )
            raise RuntimeBundleError(
                f"cannot atomically publish runtime bundle: {exc}"
            ) from exc
        _fsync_directory(bundle_parent)
        return validate_runtime_bundle(
            final_path, expected_closure=closure, reused=False
        )
    finally:
        try:
            if pending.exists():
                shutil.rmtree(pending)
        finally:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            os.close(lock_descriptor)


def sealed_execution_environment(
    bundle: RuntimeBundle,
    base_environment: Optional[Mapping[str, str]] = None,
    overrides: Optional[Mapping[str, str]] = None,
) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """Return an environment that cannot inherit an alternate loader graph."""

    validated = validate_runtime_bundle(bundle.root)
    environment = dict(os.environ if base_environment is None else base_environment)
    cleared = sorted(name for name in environment if name.startswith("LD_"))
    for name in cleared:
        environment.pop(name, None)
    if overrides:
        forbidden = sorted(name for name in overrides if name.startswith("LD_"))
        if forbidden:
            raise RuntimeBundleError(
                f"runtime overrides cannot set loader controls: {forbidden}"
            )
        environment.update({str(key): str(value) for key, value in overrides.items()})
    environment["LD_LIBRARY_PATH"] = str(validated.lib_directory)
    audit = {
        "runtime_bundle_identity_sha256": validated.identity_sha256,
        "runtime_bundle_path": str(validated.root),
        "cleared_inherited_loader_variables": cleared,
        "LD_LIBRARY_PATH": str(validated.lib_directory),
        "LD_PRELOAD": None,
    }
    return environment, audit


def verify_loader_resolution(
    bundle: RuntimeBundle,
    *,
    ldd_path: Path = DEFAULT_LDD,
) -> RuntimeClosure:
    """Prove clean loader resolution selects every archived libns3 object."""

    validated = validate_runtime_bundle(bundle.root)
    observed = discover_runtime_closure(
        validated.executable,
        ldd_path=ldd_path,
        library_path=validated.lib_directory,
    )
    if observed.identity_sha256 != validated.identity_sha256:
        raise RuntimeBundleError(
            "sealed loader resolution differs from runtime bundle identity"
        )
    expected_names = {
        str(item["soname"])
        for item in validated.manifest["project_dependencies"]
    }
    observed_names = {item.soname for item in observed.project_dependencies}
    if observed_names != expected_names:
        raise RuntimeBundleError(
            f"sealed loader project set differs: expected={sorted(expected_names)}, "
            f"observed={sorted(observed_names)}"
        )
    lib_root = validated.lib_directory.resolve(strict=True)
    escaped = [
        item.soname
        for item in observed.project_dependencies
        if item.resolved_path.parent != lib_root
    ]
    if escaped:
        raise RuntimeBundleError(
            f"project dependencies escaped the sealed lib directory: {escaped}"
        )
    return observed


def _decode_proc_maps_path(value: str) -> str:
    return (
        value.replace(r"\040", " ")
        .replace(r"\011", "\t")
        .replace(r"\012", "\n")
        .replace(r"\134", "\\")
    )


def verify_process_runtime(pid: int, bundle: RuntimeBundle) -> Mapping[str, Any]:
    """Verify a live process mapped every project and system dependency."""

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise RuntimeBundleError(f"invalid process id: {pid!r}")
    validated = validate_runtime_bundle(bundle.root)
    maps_path = Path(f"/proc/{pid}/maps")
    try:
        lines = maps_path.read_text(encoding="utf-8", errors="strict").splitlines()
    except OSError as exc:
        raise RuntimeBundleError(
            f"cannot inspect process mappings for pid {pid}: {exc}"
        ) from exc
    try:
        process_executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
    except OSError as exc:
        raise RuntimeBundleError(
            f"cannot resolve process executable for pid {pid}: {exc}"
        ) from exc
    if process_executable != validated.executable.resolve(strict=True):
        raise RuntimeBundleError(
            f"process executable escaped the sealed bundle: {process_executable}"
        )
    observed_paths: Dict[str, Path] = {}
    all_mapped_files: set[Path] = set()
    for line in lines:
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not fields[5].startswith("/"):
            continue
        raw_path = _decode_proc_maps_path(fields[5])
        if raw_path.endswith(" (deleted)"):
            raw_path = raw_path[: -len(" (deleted)")]
            deleted = True
        else:
            deleted = False
        candidate = Path(raw_path)
        if deleted and (".so" in candidate.name or candidate.name.startswith("ld-")):
            raise RuntimeBundleError(
                f"process mapped a deleted runtime dependency: {candidate}"
            )
        try:
            resolved_candidate = candidate.resolve(strict=True)
        except OSError:
            continue
        all_mapped_files.add(resolved_candidate)
        soname = candidate.name
        if not PROJECT_SONAME_RE.fullmatch(soname):
            continue
        prior = observed_paths.get(soname)
        if prior is not None and prior != resolved_candidate:
            raise RuntimeBundleError(
                f"process mapped project soname from multiple paths: {soname}"
            )
        observed_paths[soname] = resolved_candidate
    expected = {
        str(item["soname"]): validated.root / str(item["execution_path"])
        for item in validated.manifest["project_dependencies"]
    }
    if set(observed_paths) != set(expected):
        raise RuntimeBundleError(
            f"process project dependency set differs: expected={sorted(expected)}, "
            f"observed={sorted(observed_paths)}"
        )
    project_evidence: list[Dict[str, Any]] = []
    for soname in sorted(expected):
        observed = observed_paths[soname].resolve(strict=True)
        required = expected[soname].resolve(strict=True)
        if observed != required:
            raise RuntimeBundleError(
                f"process loaded {soname} outside sealed bundle: {observed}"
            )
        reference = next(
            item
            for item in validated.manifest["project_dependencies"]
            if item["soname"] == soname
        )
        _verify_file_identity(
            observed,
            str(reference["sha256"]),
            int(reference["size_bytes"]),
            f"mapped project dependency {soname}",
        )
        project_evidence.append(
            {
                "soname": soname,
                "path": str(observed),
                "sha256": reference["sha256"],
                "size_bytes": reference["size_bytes"],
            }
        )
    system_evidence: list[Dict[str, Any]] = []
    for reference in validated.manifest["system_dependencies"]:
        soname = str(reference["soname"])
        required = Path(str(reference["resolved_path"])).resolve(strict=True)
        if required not in all_mapped_files:
            raise RuntimeBundleError(
                f"process did not map expected system dependency {soname}: {required}"
            )
        _verify_file_identity(
            required,
            str(reference["sha256"]),
            int(reference["size_bytes"]),
            f"mapped system dependency {soname}",
        )
        system_evidence.append(
            {
                "soname": soname,
                "path": str(required),
                "sha256": reference["sha256"],
                "size_bytes": reference["size_bytes"],
            }
        )
    return {
        "pid": pid,
        "executable": str(process_executable),
        "runtime_bundle_identity_sha256": validated.identity_sha256,
        "project_dependency_count": len(project_evidence),
        "project_dependencies": project_evidence,
        "system_dependency_count": len(system_evidence),
        "system_dependencies": system_evidence,
        "status": "PASS",
    }


def runtime_bundle_total_bytes(bundle: RuntimeBundle) -> int:
    validated = validate_runtime_bundle(bundle.root)
    return sum(int(item["size_bytes"]) for item in validated.manifest["artifacts"])


__all__ = [
    "BUNDLE_SCHEMA",
    "RuntimeBundle",
    "RuntimeBundleError",
    "RuntimeClosure",
    "RuntimeDependency",
    "discover_runtime_closure",
    "runtime_bundle_total_bytes",
    "seal_runtime_bundle",
    "sealed_execution_environment",
    "validate_runtime_bundle",
    "verify_discovered_sources",
    "verify_loader_resolution",
    "verify_process_runtime",
]
