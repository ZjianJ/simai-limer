#!/usr/bin/env python3
"""Tests for the non-destructive P0 baseline freeze contract."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

from freeze_baseline import PASS, create_manifest, verify_manifest  # noqa: E402


def git(repo: Path, *arguments: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if process.returncode:
        raise AssertionError(process.stderr)
    return process.stdout.strip()


def initialize_repository(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    git(path, "init", "--quiet")
    git(path, "config", "user.email", "limer-test@example.invalid")
    git(path, "config", "user.name", "LIMER Test")


class BaselineFreezeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "repo"
        initialize_repository(self.root)
        (self.root / "tracked.conf").write_text("mode=healthy\n", encoding="utf-8")
        git(self.root, "add", "tracked.conf")
        git(self.root, "commit", "--quiet", "-m", "initial")
        (self.root / "tracked.conf").write_text("mode=true16\n", encoding="utf-8")
        (self.root / "untracked-tool.py").write_text("VALUE = 16\n", encoding="utf-8")
        self.results = self.root / "results"
        self.results.mkdir()
        (self.results / "evaluation.json").write_text(
            '{"status":"PASS"}\n', encoding="utf-8"
        )
        self.manifest_path = self.root / "baseline.freeze.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def create(self) -> dict:
        return create_manifest(
            repo_root=self.root,
            output_path=self.manifest_path,
            include_paths=[Path("tracked.conf"), Path("results")],
            label="test-baseline",
            command="simulator --ranks 16",
            parameters={"ranks": "16", "telemetry_us": "1000"},
            environment_variables=["LIMER_TEST_ENV"],
        )

    def test_dirty_repository_and_artifacts_round_trip(self) -> None:
        old_value = os.environ.get("LIMER_TEST_ENV")
        os.environ["LIMER_TEST_ENV"] = "online"
        try:
            manifest = self.create()
            self.assertTrue(manifest["git"]["superproject"]["dirty"])
            self.assertEqual(
                manifest["environment"]["selected_variables"]["LIMER_TEST_ENV"],
                "online",
            )
            self.assertEqual(manifest["experiment"]["parameters"]["ranks"], "16")
            self.assertEqual(
                manifest["experiment"]["command_argv"],
                ["simulator", "--ranks", "16"],
            )
            required = {
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
            self.assertTrue(required.issubset(manifest["contract_provenance"]))
            for field in required:
                if manifest["contract_provenance"][field] is None:
                    self.assertIn(
                        field,
                        manifest["contract_provenance"]["not_applicable_reasons"],
                    )
            paths = {item["path"] for item in manifest["artifacts"]}
            self.assertEqual(paths, {"tracked.conf", "results/evaluation.json"})
            self.assertNotIn(
                "baseline.freeze.json",
                {item["path"] for item in manifest["git"]["superproject"]["untracked_files"]},
            )
            report = verify_manifest(self.manifest_path, self.root)
            self.assertEqual(report["status"], PASS)
            self.assertEqual(report["summary"]["failed"], 0)
        finally:
            if old_value is None:
                os.environ.pop("LIMER_TEST_ENV", None)
            else:
                os.environ["LIMER_TEST_ENV"] = old_value

    def test_artifact_mutation_fails_check(self) -> None:
        self.create()
        (self.results / "evaluation.json").write_text(
            '{"status":"CHANGED"}\n', encoding="utf-8"
        )
        report = verify_manifest(self.manifest_path, self.root)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(
            any(
                item["status"] == "FAIL" and item["name"].endswith(":sha256")
                for item in report["checks"]
            )
        )

    def test_dirty_source_mutation_fails_git_fingerprint(self) -> None:
        self.create()
        (self.root / "untracked-tool.py").write_text("VALUE = 32\n", encoding="utf-8")
        report = verify_manifest(self.manifest_path, self.root)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(
            any(
                item["status"] == "FAIL" and "untracked_files" in item["name"]
                for item in report["checks"]
            )
        )

    def test_symlink_identity_and_resolved_content_are_both_checked(self) -> None:
        binary = self.root / "simulator.bin"
        binary.write_bytes(b"binary-v1")
        link = self.root / "simulator"
        link.symlink_to("simulator.bin")
        manifest = create_manifest(
            repo_root=self.root,
            output_path=self.manifest_path,
            include_paths=[Path("simulator")],
            label="symlink-baseline",
            command=None,
            parameters={},
            environment_variables=[],
        )
        artifact = manifest["artifacts"][0]
        self.assertEqual(artifact["path"], "simulator")
        self.assertEqual(artifact["kind"], "symlink")
        self.assertEqual(artifact["link_target"], "simulator.bin")
        self.assertEqual(verify_manifest(self.manifest_path, self.root)["status"], PASS)

        binary.write_bytes(b"binary-v2")
        report = verify_manifest(self.manifest_path, self.root)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(
            any(
                item["status"] == "FAIL"
                and item["name"].endswith(":resolved_sha256")
                for item in report["checks"]
            )
        )

    def test_check_cli_exit_code(self) -> None:
        self.create()
        tool = TOOLS / "freeze_baseline.py"
        passed = subprocess.run(
            [
                sys.executable,
                str(tool),
                "--repo-root",
                str(self.root),
                "--check",
                str(self.manifest_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(passed.returncode, 0, passed.stderr)
        self.assertEqual(json.loads(passed.stdout)["status"], PASS)
        (self.root / "tracked.conf").write_text("mode=broken\n", encoding="utf-8")
        failed = subprocess.run(
            [
                sys.executable,
                str(tool),
                "--repo-root",
                str(self.root),
                "--check",
                str(self.manifest_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(failed.returncode, 1, failed.stderr)
        self.assertEqual(json.loads(failed.stdout)["status"], "FAIL")


class SubmoduleFreezeTest(unittest.TestCase):
    def test_submodule_dirty_state_is_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            child_source = temporary_path / "child-source"
            initialize_repository(child_source)
            (child_source / "child.txt").write_text("v1\n", encoding="utf-8")
            git(child_source, "add", "child.txt")
            git(child_source, "commit", "--quiet", "-m", "child")

            root = temporary_path / "root"
            initialize_repository(root)
            (root / "artifact.txt").write_text("baseline\n", encoding="utf-8")
            git(root, "add", "artifact.txt")
            git(root, "commit", "--quiet", "-m", "root")
            process = subprocess.run(
                [
                    "git",
                    "-c",
                    "protocol.file.allow=always",
                    "-C",
                    str(root),
                    "submodule",
                    "add",
                    "--quiet",
                    str(child_source),
                    "deps/child",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            git(root, "commit", "--quiet", "-am", "add child")

            manifest_path = root / "baseline.json"
            manifest = create_manifest(
                repo_root=root,
                output_path=manifest_path,
                include_paths=[Path("artifact.txt")],
                label="with-submodule",
                command=None,
                parameters={},
                environment_variables=[],
            )
            self.assertEqual(len(manifest["git"]["submodules"]), 1)
            self.assertEqual(verify_manifest(manifest_path, root)["status"], PASS)

            (root / "deps/child/child.txt").write_text("dirty\n", encoding="utf-8")
            report = verify_manifest(manifest_path, root)
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(
                any(
                    item["status"] == "FAIL" and "submodule" in item["name"]
                    for item in report["checks"]
                )
            )


if __name__ == "__main__":
    unittest.main()
