#!/usr/bin/env python3
"""Export tracked and untracked research source without changing Git's index."""
import argparse
import json
import subprocess
from pathlib import Path


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def export(root, base, scopes, destination):
    patch = git(root, "diff", "--binary", base, "--", *scopes)
    names = git(root, "ls-files", "--others", "--exclude-standard", "-z", "--", *scopes)
    added = []
    for raw in names.split(b"\0"):
        if not raw:
            continue
        name = raw.decode()
        path = root / name
        if path.is_symlink() or not path.is_file():
            continue
        if any(part in {"__pycache__", "cmake-cache", "results"} for part in path.parts):
            continue
        if name.startswith(("simulation/build/", "simulation/scratch/", "simulation/src/applications/astra-sim/", "simulation/.lock")):
            continue
        if path.suffix in {".pyc", ".o"}:
            continue
        diff = subprocess.run(["git", "diff", "--no-index", "--binary", "--", "/dev/null", name], cwd=root, stdout=subprocess.PIPE, check=False)
        if diff.returncode not in (0, 1):
            raise RuntimeError(f"Cannot export {name}")
        patch += diff.stdout
        added.append(name)
    destination.write_bytes(patch)
    return {"base": git(root, "rev-parse", base).decode().strip(), "untracked_sources": added, "bytes": len(patch)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simai-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    report = {}
    for name, root, base, scopes in (
        ("astra-sim-alibabacloud", args.simai_root, "f5efb5a", ["astra-sim-alibabacloud", "scripts/build.sh"]),
        ("ns-3-alibabacloud", args.simai_root / "ns-3-alibabacloud", "7e3cb5b", ["."]),
    ):
        report[name] = export(root, base, scopes, args.output / (name + ".patch"))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
