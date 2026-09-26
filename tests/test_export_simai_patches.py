import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("exporter", Path(__file__).resolve().parents[1] / "tools/export_simai_patches.py")
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)


class ExportTests(unittest.TestCase):
    def test_dirty_and_untracked_sources_without_index_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "repo"
            root.mkdir()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / "tracked.txt").write_text("before\n")
            subprocess.run(["git", "-C", str(root), "add", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"], check=True)
            index_before = (root / ".git/index").read_bytes()
            (root / "tracked.txt").write_text("after\n")
            (root / "new.h").write_text("// preserved research source\n")
            generated = root / "simulation/build/include"
            generated.mkdir(parents=True)
            (generated / "generated.h").write_text("// not canonical\n")
            patch = Path(folder) / "rescue.patch"
            report = exporter.export(root, "HEAD", ["."], patch)
            self.assertEqual(report["untracked_sources"], ["new.h"])
            self.assertEqual(index_before, (root / ".git/index").read_bytes())
            self.assertIn(b"+after", patch.read_bytes())
            self.assertIn(b"+// preserved research source", patch.read_bytes())
            clean = Path(folder) / "clean"
            subprocess.run(["git", "clone", "-q", str(root), str(clean)], check=True)
            subprocess.run(["git", "-C", str(clean), "apply", str(patch)], check=True)
            self.assertEqual((clean / "new.h").read_text(), (root / "new.h").read_text())
            self.assertEqual((clean / "tracked.txt").read_text(), "after\n")


if __name__ == "__main__":
    unittest.main()
