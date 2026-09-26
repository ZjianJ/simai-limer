import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from summarize_adaptive_comparison import POLICIES, render, summarize


class AdaptiveSummaryTest(unittest.TestCase):
    def fixture(self):
        scenarios = {}
        for family in ('healthy', 'standard', 'early', 'recovery'):
            for seed, duration in (('42', 1000000), ('43', 2000000), ('44', 4000000)):
                if family == 'healthy' and seed != '42':
                    continue
                name = 'healthy' if family == 'healthy' else family + '_' + seed
                scenarios[name] = {
                    policy: {'pass': True, 'workload_finish_ns': duration}
                    for policy in POLICIES
                }
        for seed, duration in (('42', 1000000), ('43', 1000000), ('44', 2000000)):
            scenarios['standard_' + seed]['B11']['workload_finish_ns'] = duration
        scenarios['healthy']['B12_signal_neutral'] = {
            'pass': True, 'workload_finish_ns': scenarios['healthy']['B11']['workload_finish_ns']}
        scenarios['healthy']['B12_failed_ablation'] = {
            'pass': False, 'workload_finish_ns': 1}
        return {'pass': True, 'scenarios': scenarios}

    def evaluate(self, report):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'summary.json'
            source.write_text(json.dumps(report))
            result = summarize(root)
            self.assertEqual(result['source_summary_sha256'],
                             hashlib.sha256(source.read_bytes()).hexdigest())
            return result

    def test_family_means_and_paired_percentages_are_distinct(self):
        result = self.evaluate(self.fixture())
        family = result['families']['standard']
        self.assertEqual(family['B11']['count'], 3)
        self.assertAlmostEqual(family['B8']['mean_finish_ms'], 7 / 3)
        self.assertAlmostEqual(family['B11']['mean_finish_ms'], 4 / 3)
        self.assertAlmostEqual(family['B11']['gain_from_mean_vs_B8_pct'], 300 / 7)
        self.assertAlmostEqual(family['B11']['mean_paired_gain_vs_B8_pct'], 100 / 3)
        self.assertEqual(family['B11']['min_paired_gain_vs_B8_pct'], 0)
        self.assertEqual(family['B11']['max_paired_gain_vs_B8_pct'], 50)
        self.assertEqual(result['families']['healthy']['B8']['count'], 1)
        self.assertEqual(family['B8']['gain_from_mean_vs_B8_pct'], 0)
        self.assertEqual(result['individual']['standard_43']['B11'], 1000000)

    def test_render_has_units_and_gain_direction(self):
        markdown = render(self.evaluate(self.fixture()))
        self.assertIn('Positive gain means faster', markdown)
        self.assertIn('simulation zero, in ms', markdown)
        self.assertIn('| standard | 2.333333 | 2.333333 | 2.333333 | 1.333333 | 2.333333 |', markdown)
        self.assertIn('+42.857', markdown)

    def test_only_passing_ablations_reported(self):
        result = self.evaluate(self.fixture())
        ablations = result['ablations']['healthy']
        self.assertTrue(ablations['B12_signal_neutral']['exact_B11_parity'])
        self.assertEqual(ablations['B12_signal_neutral']['gain_vs_B11_pct'], 0)
        self.assertNotIn('B12_failed_ablation', ablations)

    def test_failed_or_missing_top_level_pass_rejected(self):
        for value in (False, None, 'false', 1):
            with self.subTest(value=value):
                report = self.fixture()
                report['pass'] = value
                with self.assertRaises(ValueError):
                    self.evaluate(report)
        report = self.fixture()
        del report['pass']
        with self.assertRaises(ValueError):
            self.evaluate(report)

    def test_incomplete_matrix_rejected(self):
        for missing in ('scenario', 'policy'):
            with self.subTest(missing=missing):
                report = self.fixture()
                if missing == 'scenario':
                    del report['scenarios']['recovery_44']
                else:
                    del report['scenarios']['early_43']['B10']
                with self.assertRaises((ValueError, KeyError)):
                    self.evaluate(report)

    def test_stale_top_level_pass_cannot_hide_failed_cell(self):
        for value in (False, None, 'true', 1):
            with self.subTest(value=value):
                report = self.fixture()
                report['scenarios']['early_43']['B10']['pass'] = value
                with self.assertRaises(ValueError):
                    self.evaluate(report)
        report = self.fixture()
        del report['scenarios']['early_43']['B10']['pass']
        with self.assertRaises(ValueError):
            self.evaluate(report)

    def test_invalid_main_durations_rejected(self):
        for value in (0, -1, float('nan'), float('inf'), None, '1000', True):
            with self.subTest(value=value):
                report = self.fixture()
                report['scenarios']['standard_42']['B11']['workload_finish_ns'] = value
                with self.assertRaises(ValueError):
                    self.evaluate(report)

    def test_bad_baseline_rejected_before_division(self):
        report = self.fixture()
        report['scenarios']['standard_42']['B8']['workload_finish_ns'] = 0
        with self.assertRaises(ValueError):
            self.evaluate(report)

    def test_no_input_mutation(self):
        report = self.fixture()
        original = copy.deepcopy(report)
        self.evaluate(report)
        self.assertEqual(report, original)


if __name__ == '__main__':
    unittest.main()
