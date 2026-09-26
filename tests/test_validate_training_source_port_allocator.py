import csv
import tempfile
import unittest
from pathlib import Path

from limer.tools import validate_training_source_port_allocator as validator


class AllocatorEvidenceValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write(self, **updates: object) -> Path:
        row: dict[str, object] = {
            "run_id": "platform-q1",
            "interval_first": 10000,
            "interval_end_exclusive": 49152,
            "capacity": 39152,
            "allocations": 32,
            "releases": 31,
            "reuses": 0,
            "active_at_stop": 1,
            "peak_active": 8,
            "pair_count": 16,
            "pairs_with_reuse": 0,
            "max_pair_allocations": 2,
            "max_pair_reuses": 0,
            "external_conflicts": 0,
            "exhaustions": 0,
            "invariant_errors": 0,
            "min_allocated_port": 10000,
            "max_allocated_port": 10003,
            "status": "PASS",
        }
        row.update(updates)
        path = self.root / validator.RAW_FILENAME
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=validator.RAW_COLUMNS)
            writer.writeheader()
            writer.writerow(row)
        return path

    def test_general_and_q4_profiles(self) -> None:
        general = validator.validate_allocator_evidence(
            self._write(), expected_run_id="platform-q1", require_reuse=False
        )
        self.assertEqual(general["status"], "PASS")
        self.assertEqual(general["metrics"]["active_at_stop"], 1)

        q4 = validator.validate_allocator_evidence(
            self._write(
                run_id="platform-q4",
                allocations=39153,
                releases=39152,
                reuses=1,
                pairs_with_reuse=1,
                max_pair_allocations=39153,
                max_pair_reuses=1,
                max_allocated_port=49151,
            ),
            expected_run_id="platform-q4",
            require_reuse=True,
        )
        self.assertEqual(q4["status"], "PASS")
        self.assertTrue(q4["requirements"]["require_reuse"])

    def test_q4_without_reuse_and_any_runtime_error_fail(self) -> None:
        no_reuse = validator.validate_allocator_evidence(
            self._write(run_id="platform-q4"),
            expected_run_id="platform-q4",
            require_reuse=True,
        )
        self.assertEqual(no_reuse["status"], "FAIL")
        self.assertTrue(any("requires" in error for error in no_reuse["errors"]))

        forged_multi_pair = validator.validate_allocator_evidence(
            self._write(
                run_id="platform-q4",
                allocations=40000,
                releases=39999,
                reuses=1,
                pair_count=100,
                pairs_with_reuse=1,
                max_pair_allocations=400,
                max_pair_reuses=1,
            ),
            expected_run_id="platform-q4",
            require_reuse=True,
        )
        self.assertEqual(forged_multi_pair["status"], "FAIL")
        self.assertTrue(
            any("full port interval" in error for error in forged_multi_pair["errors"])
        )

        conflict = validator.validate_allocator_evidence(
            self._write(external_conflicts=1),
            expected_run_id="platform-q1",
            require_reuse=False,
        )
        self.assertEqual(conflict["status"], "FAIL")
        self.assertTrue(any("conflicts" in error for error in conflict["errors"]))

    def test_reserved_interval_escape_and_symlink_are_rejected(self) -> None:
        escaped = validator.validate_allocator_evidence(
            self._write(max_allocated_port=49152),
            expected_run_id="platform-q1",
            require_reuse=False,
        )
        self.assertEqual(escaped["status"], "FAIL")
        target = self._write()
        link = self.root / "allocator-link.csv"
        link.symlink_to(target.name)
        with self.assertRaises(validator.AllocatorEvidenceError):
            validator.validate_allocator_evidence(
                link, expected_run_id="platform-q1", require_reuse=False
            )


if __name__ == "__main__":
    unittest.main()
