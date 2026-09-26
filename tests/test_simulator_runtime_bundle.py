#!/usr/bin/env python3
"""Runtime-closure tests using a real ELF and a tiny libns3 fixture."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from copy import deepcopy
from pathlib import Path


LIMER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIMER / "tools"))

import simulator_runtime_bundle as runtime  # noqa: E402


class SimulatorRuntimeBundleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="limer-runtime-bundle-test-"
        )
        self.root = Path(self.temporary.name)
        self.build = self.root / "build"
        self.build.mkdir()
        self.executable, self.library = self._compile_fixture(self.build, 42)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _compile_fixture(
        self, directory: Path, return_value: int
    ) -> tuple[Path, Path]:
        directory.mkdir(parents=True, exist_ok=True)
        library_source = directory / "fixture.c"
        program_source = directory / "main.c"
        library = directory / "libns3-fixture.so"
        executable = directory / "fixture-simulator"
        library_source.write_text(
            f"int fixture_value(void) {{ return {return_value}; }}\n",
            encoding="ascii",
        )
        program_source.write_text(
            """
#include <stdlib.h>
#include <unistd.h>
extern int fixture_value(void);
int main(void) {
  if (fixture_value() != 42) return 7;
  const char* delay = getenv("BUNDLE_FIXTURE_SLEEP_SECONDS");
  if (delay != NULL) sleep((unsigned int)atoi(delay));
  return 0;
}
""".strip()
            + "\n",
            encoding="ascii",
        )
        subprocess.run(
            [
                "gcc",
                "-shared",
                "-fPIC",
                "-Wl,-soname,libns3-fixture.so",
                str(library_source),
                "-o",
                str(library),
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "gcc",
                str(program_source),
                f"-L{directory}",
                "-lns3-fixture",
                f"-Wl,-rpath,{directory}",
                "-o",
                str(executable),
            ],
            check=True,
            capture_output=True,
        )
        return executable, library

    def test_real_elf_discovery_copy_reuse_and_loader_binding(self) -> None:
        closure = runtime.discover_runtime_closure(self.executable)
        self.assertEqual(
            [item.soname for item in closure.project_dependencies],
            ["libns3-fixture.so"],
        )
        self.assertGreaterEqual(len(closure.system_dependencies), 2)
        for dependency in closure.system_dependencies:
            self.assertEqual(
                dependency.identity_entry()["resolved_path"],
                str(dependency.resolved_path),
            )
            self.assertEqual(
                dependency.resolved_path,
                dependency.resolved_path.resolve(strict=True),
            )
        changed_system_path = deepcopy(closure.identity_material())
        changed_system_path["system_dependencies"][0]["resolved_path"] += ".moved"
        self.assertNotEqual(
            runtime.canonical_hash(changed_system_path), closure.identity_sha256
        )
        execution_root = self.root / "execution"
        first = runtime.seal_runtime_bundle(execution_root, self.executable)
        self.assertFalse(first.reused)
        self.assertEqual(first.root.name, closure.identity_sha256)
        self.assertEqual(
            sorted(path.name for path in first.lib_directory.iterdir()),
            ["libns3-fixture.so"],
        )
        loader_closure = runtime.verify_loader_resolution(first)
        self.assertEqual(
            [
                (item.soname, str(item.resolved_path))
                for item in loader_closure.system_dependencies
            ],
            [
                (item["soname"], item["resolved_path"])
                for item in first.manifest["system_dependencies"]
            ],
        )
        second = runtime.seal_runtime_bundle(execution_root, self.executable)
        self.assertTrue(second.reused)
        self.assertEqual(first.root, second.root)
        self.assertEqual(
            len(list((execution_root / runtime.BUNDLE_ROOT_NAME).glob("[0-9a-f]*"))),
            1,
        )

    def test_closed_environment_ignores_ambient_loader_injection(self) -> None:
        alternate = self.root / "ambient"
        _, alternate_library = self._compile_fixture(alternate, 13)
        self.assertTrue(alternate_library.is_file())
        old_library_path = os.environ.get("LD_LIBRARY_PATH")
        old_preload = os.environ.get("LD_PRELOAD")
        os.environ["LD_LIBRARY_PATH"] = str(alternate)
        os.environ["LD_PRELOAD"] = str(alternate_library)
        try:
            closure = runtime.discover_runtime_closure(self.executable)
        finally:
            if old_library_path is None:
                os.environ.pop("LD_LIBRARY_PATH", None)
            else:
                os.environ["LD_LIBRARY_PATH"] = old_library_path
            if old_preload is None:
                os.environ.pop("LD_PRELOAD", None)
            else:
                os.environ["LD_PRELOAD"] = old_preload
        self.assertEqual(
            closure.project_dependencies[0].resolved_path, self.library.resolve()
        )
        bundle = runtime.seal_runtime_bundle(
            self.root / "execution", self.executable
        )
        environment, audit = runtime.sealed_execution_environment(
            bundle,
            {
                "PATH": "/usr/bin:/bin",
                "LD_LIBRARY_PATH": str(alternate),
                "LD_PRELOAD": str(alternate_library),
                "LD_AUDIT": "/does/not/exist",
            },
        )
        self.assertNotIn("LD_PRELOAD", environment)
        self.assertNotIn("LD_AUDIT", environment)
        self.assertEqual(environment["LD_LIBRARY_PATH"], str(bundle.lib_directory))
        self.assertEqual(
            audit["cleared_inherited_loader_variables"],
            ["LD_AUDIT", "LD_LIBRARY_PATH", "LD_PRELOAD"],
        )
        completed = subprocess.run(
            [str(bundle.executable)], env=environment, check=False
        )
        self.assertEqual(completed.returncode, 0)

    def test_live_mapping_and_post_start_tamper_are_detected(self) -> None:
        bundle = runtime.seal_runtime_bundle(
            self.root / "execution", self.executable
        )
        environment, _ = runtime.sealed_execution_environment(
            bundle,
            {"PATH": "/usr/bin:/bin", "BUNDLE_FIXTURE_SLEEP_SECONDS": "2"},
        )
        process = subprocess.Popen([str(bundle.executable)], env=environment)
        try:
            evidence = runtime.verify_process_runtime(process.pid, bundle)
            self.assertEqual(evidence["status"], "PASS")
            self.assertEqual(evidence["project_dependency_count"], 1)
            self.assertEqual(
                evidence["system_dependency_count"],
                len(bundle.manifest["system_dependencies"]),
            )
            self.assertEqual(
                {
                    (item["soname"], item["path"])
                    for item in evidence["system_dependencies"]
                },
                {
                    (item["soname"], item["resolved_path"])
                    for item in bundle.manifest["system_dependencies"]
                },
            )
            library = bundle.lib_directory / "libns3-fixture.so"
            replacement = bundle.lib_directory / ".tampered.so"
            data = library.read_bytes()
            replacement.write_bytes(data[:-1] + bytes([data[-1] ^ 0x01]))
            os.replace(replacement, library)
            with self.assertRaisesRegex(
                runtime.RuntimeBundleError, "sha256 changed"
            ):
                runtime.validate_runtime_bundle(bundle.root)
        finally:
            process.wait(timeout=5)

    def test_source_change_after_discovery_is_detected(self) -> None:
        closure = runtime.discover_runtime_closure(self.executable)
        replacement = self.build / ".replacement.so"
        data = self.library.read_bytes()
        replacement.write_bytes(data[:-1] + bytes([data[-1] ^ 0x01]))
        os.replace(replacement, self.library)
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "sha256 changed"):
            runtime.verify_discovered_sources(closure)

    def test_unresolved_dependency_and_fake_script_fail_closed(self) -> None:
        script = self.root / "fake-simulator"
        script.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
        script.chmod(0o755)
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "not an ELF"):
            runtime.discover_runtime_closure(script)
        self.library.rename(self.library.with_suffix(".missing"))
        with self.assertRaisesRegex(
            runtime.RuntimeBundleError, "unresolved dynamic dependency"
        ):
            runtime.discover_runtime_closure(self.executable)

    def test_duplicate_soname_and_unknown_ldd_lines_fail_closed(self) -> None:
        first = "/tmp/a/libns3-duplicate.so"
        second = "/tmp/b/libns3-duplicate.so"
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "duplicate"):
            runtime._parse_ldd_output(  # pylint: disable=protected-access
                f"libns3-duplicate.so => {first} (0x1)\n"
                f"libns3-duplicate.so => {second} (0x2)\n"
            )
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "unrecognized"):
            runtime._parse_ldd_output(  # pylint: disable=protected-access
                "this output is not a loader binding\n"
            )

    def test_project_artifact_tamper_refuses_reuse(self) -> None:
        execution_root = self.root / "execution"
        bundle = runtime.seal_runtime_bundle(execution_root, self.executable)
        library = bundle.lib_directory / "libns3-fixture.so"
        library.chmod(0o644)
        library.write_bytes(library.read_bytes() + b"tamper")
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "size changed"):
            runtime.seal_runtime_bundle(execution_root, self.executable)

    def test_symlink_and_uninventoried_directory_are_rejected(self) -> None:
        bundle = runtime.seal_runtime_bundle(
            self.root / "execution", self.executable
        )
        alias = self.root / "bundle-alias"
        alias.symlink_to(bundle.root, target_is_directory=True)
        with self.assertRaisesRegex(
            runtime.RuntimeBundleError, "symlinked path component"
        ):
            runtime.validate_runtime_bundle(alias)
        (bundle.root / "unrecorded-empty-directory").mkdir()
        with self.assertRaisesRegex(runtime.RuntimeBundleError, "unexpected directory"):
            runtime.validate_runtime_bundle(bundle.root)

    def test_original_symlink_and_parent_escape_paths_fail_closed(self) -> None:
        actual_execution = self.root / "actual-execution"
        actual_execution.mkdir()
        execution_alias = self.root / "execution-alias"
        execution_alias.symlink_to(actual_execution, target_is_directory=True)
        with self.assertRaisesRegex(
            runtime.RuntimeBundleError, "symlinked path component"
        ):
            runtime.seal_runtime_bundle(
                execution_alias / "nested", self.executable
            )

        executable_alias = self.root / "simulator-alias"
        executable_alias.symlink_to(self.executable)
        with self.assertRaisesRegex(
            runtime.RuntimeBundleError, "symlinked path component"
        ):
            runtime.discover_runtime_closure(executable_alias)

        with self.assertRaisesRegex(runtime.RuntimeBundleError, "escapes through"):
            runtime.seal_runtime_bundle(
                self.root / "uncreated" / ".." / "escaped", self.executable
            )

    def test_creator_crash_releases_flock_and_orphan_is_recovered(self) -> None:
        closure = runtime.discover_runtime_closure(self.executable)
        execution_root = self.root / "execution"
        bundle_parent = execution_root / runtime.BUNDLE_ROOT_NAME
        bundle_parent.mkdir(parents=True)
        token = uuid.uuid4().hex
        script = "\n".join(
            [
                "import fcntl, os, pathlib, sys, time",
                "parent = pathlib.Path(sys.argv[1])",
                "identity = sys.argv[2]",
                "token = sys.argv[3]",
                "fd = os.open(parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))",
                "fcntl.flock(fd, fcntl.LOCK_EX)",
                "pending = parent / f'.{identity}.pending.{os.getpid()}.{token}'",
                "pending.mkdir()",
                "(pending / 'partial-copy').write_bytes(b'partial')",
                "print(pending, flush=True)",
                "time.sleep(60)",
            ]
        )
        creator = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                str(bundle_parent),
                closure.identity_sha256,
                token,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert creator.stdout is not None
            orphan = Path(creator.stdout.readline().strip())
            self.assertTrue(orphan.is_dir())
            creator.kill()
            creator.wait(timeout=5)
            bundle = runtime.seal_runtime_bundle(
                execution_root,
                self.executable,
                lock_timeout_seconds=5,
            )
            self.assertTrue(bundle.root.is_dir())
            self.assertFalse(orphan.exists())
            runtime.validate_runtime_bundle(bundle.root)
        finally:
            if creator.poll() is None:
                creator.kill()
                creator.wait(timeout=5)
            if creator.stdout is not None:
                creator.stdout.close()
            if creator.stderr is not None:
                creator.stderr.close()

    def test_concurrent_publish_produces_one_valid_bundle(self) -> None:
        execution_root = self.root / "execution"
        barrier = threading.Barrier(2)
        results: list[runtime.RuntimeBundle] = []
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                barrier.wait(timeout=5)
                results.append(
                    runtime.seal_runtime_bundle(
                        execution_root, self.executable, lock_timeout_seconds=10
                    )
                )
            except BaseException as exc:  # preserve the thread failure for assertion
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
        self.assertFalse(errors, errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].root, results[1].root)
        self.assertEqual(sorted(item.reused for item in results), [False, True])
        bundle_parent = execution_root / runtime.BUNDLE_ROOT_NAME
        self.assertEqual(
            [path.name for path in bundle_parent.iterdir() if path.is_dir()],
            [results[0].identity_sha256],
        )
        self.assertFalse(list(bundle_parent.glob(".*.pending.*")))
        self.assertFalse(list(bundle_parent.glob(".*.lock")))
        runtime.validate_runtime_bundle(results[0].root)


if __name__ == "__main__":
    unittest.main()
