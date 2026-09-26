import csv
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from run_adaptive_comparison import (SCENARIOS, artifact_hashes, audit_faults, canonical_hash,
    collect_result, comparison_rows, ensure_fault_file, frozen_fault_bytes, policy_spec,
    reaudit_existing, resume_result, scenario_names)


def write_csv(path, records):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(records)


class AdaptiveComparisonTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def faults(self):
        directory = self.root / 'seed_42'
        directory.mkdir()
        data = [dict(fault_id=f'f{i}', fault_type='service_degradation', target_link_id=f'L{i}-20',
                     start_time_ns=1000000, end_time_ns=100000000, severity=0.5,
                     parameter_before=1, parameter_after=0.5, recovery_delay_ns=0) for i in range(8)]
        write_csv(directory / 'faults.csv', data)
        return data

    def observed_faults(self, data, recovery=True):
        records = []
        for row in data:
            for transition, field, status in (('apply', 'start_time_ns', 'APPLIED'),
                                              ('revert', 'end_time_ns', 'REVERTED')):
                if transition == 'revert' and not recovery:
                    continue
                records.append(dict(fault_id=row['fault_id'], fault_type=row['fault_type'],
                    target_link_id=row['target_link_id'], transition=transition,
                    scheduled_ns=row[field], actual_ns=row[field], status=status,
                    parameter_before=row['parameter_before'], parameter_after=row['parameter_after']))
        return records

    def test_scenario_expansion_and_selection(self):
        self.assertEqual(scenario_names(['all']), list(SCENARIOS))
        self.assertEqual(scenario_names(['healthy', 'standard_42']), ['healthy', 'standard_42'])
        self.assertEqual(scenario_names(['early', 'early_42']), ['early_42', 'early_43', 'early_44'])
        with self.assertRaises(ValueError):
            scenario_names(['invalid'])

    def test_nested_policy_ablation_contract(self):
        self.assertEqual(policy_spec('B8'), ('B8', {}))
        self.assertEqual(policy_spec('B10')[1], {'LIMER_SPLIT_WAIT_SAMPLE_NS': '10000'})
        self.assertEqual(policy_spec('B12_no_signal')[0], 'B12')
        self.assertEqual(policy_spec('B12_no_signal')[1]['LIMER_SPLIT_SIGNAL_ENABLE'], '0')
        self.assertEqual(policy_spec('B12_ideal')[1]['LIMER_SPLIT_SIGNAL_SAMPLE_NS'], '10000')
        self.assertEqual(policy_spec('B12_ideal')[1]['LIMER_SPLIT_SIGNAL_DELAY_NS'], '0')
        self.assertEqual(policy_spec('B12_shadow')[1]['LIMER_SPLIT_SIGNAL_ACTUATE'], '0')
        self.assertEqual(policy_spec('B12')[1]['LIMER_SPLIT_SIGNAL_DELAY_NS'], '10000')

    def test_standard_schedule_byte_identical(self):
        self.faults()
        self.assertEqual(frozen_fault_bytes(self.root, 'standard_42'),
                         (self.root / 'seed_42/faults.csv').read_bytes())

    def test_recovery_changes_only_times(self):
        data = self.faults()
        target = self.root / 'recovery'
        target.mkdir()
        path = ensure_fault_file(target, frozen_fault_bytes(self.root, 'recovery_42'))
        with path.open() as stream:
            changed = list(csv.DictReader(stream))
        for original, row in zip(data, changed):
            self.assertEqual(row['start_time_ns'], '250000')
            self.assertEqual(row['end_time_ns'], '750000')
            for key in set(original) - {'start_time_ns', 'end_time_ns'}:
                self.assertEqual(str(original[key]), row[key])
        ensure_fault_file(target, path.read_bytes())
        with self.assertRaises(ValueError):
            ensure_fault_file(target, b'changed')

    def test_fault_contract_rejects_more_than_eighty_percent(self):
        data = self.faults()
        data[0]['parameter_after'] = 0.1
        write_csv(self.root / 'seed_42/faults.csv', data)
        with self.assertRaises(ValueError):
            frozen_fault_bytes(self.root, 'standard_42')

    def test_exact_apply_and_revert_audit(self):
        data = self.faults()
        for row in data:
            row['start_time_ns'], row['end_time_ns'] = 250000, 750000
        faults = self.root / 'faults.csv'
        write_csv(faults, data)
        write_csv(self.root / 'link_map.csv', [dict(link_id=r['target_link_id'], link_class='ACCESS') for r in data])
        observed = self.observed_faults(data)
        write_csv(self.root / 'fault_application_telemetry.csv', observed)
        result = audit_faults(self.root, faults, 2000000)
        self.assertEqual(result['apply_count'], 8)
        self.assertEqual(result['revert_count'], 8)
        observed[-1]['actual_ns'] += 1
        write_csv(self.root / 'fault_application_telemetry.csv', observed)
        with self.assertRaises(ValueError):
            audit_faults(self.root, faults, 2000000)

    def test_missing_revert_rejected_and_future_revert_not_required(self):
        data = self.faults()
        faults = self.root / 'seed_42/faults.csv'
        write_csv(self.root / 'link_map.csv', [dict(link_id=r['target_link_id'], link_class='ACCESS') for r in data])
        write_csv(self.root / 'fault_application_telemetry.csv', self.observed_faults(data, recovery=False))
        self.assertTrue(audit_faults(self.root, faults, 2000000)['pass'])
        with self.assertRaises(ValueError):
            audit_faults(self.root, faults, 100000001)

    def test_resume_hash_mutation_rejected(self):
        prior = {'protocol_sha256': 'protocol', 'artifacts_sha256': {'events.csv': 'old'}}
        (self.root / 'result.json').write_text(json.dumps(prior))
        with patch('run_adaptive_comparison.artifact_hashes', return_value={'events.csv': 'new'}):
            with self.assertRaisesRegex(ValueError, 'evidence changed'):
                resume_result(self.root, SimpleNamespace(), 'healthy', 'B8', {}, 'protocol')

    def test_interrupted_run_not_overwritten(self):
        with self.assertRaisesRegex(ValueError, 'incomplete run preserved'):
            resume_result(self.root, SimpleNamespace(), 'healthy', 'B8', {}, 'protocol')

    def test_audit_failure_is_retained_as_failed_result(self):
        (self.root / 'exit_code.txt').write_text('0\n')
        (self.root / 'manifest.json').write_text('{}\n')
        with patch('run_adaptive_comparison.verify_manifest'), \
                patch('run_adaptive_comparison.audit', side_effect=ValueError('invalid ACK evidence')):
            result = collect_result(self.root, SimpleNamespace(), 'healthy', 'B10', {}, 'protocol')
        self.assertFalse(result['pass'])
        self.assertEqual(result['reason'], 'invalid ACK evidence')
        self.assertEqual(result['exit_code'], 0)
        self.assertIn('manifest.json', result['artifacts_sha256'])
        self.assertEqual(result['protocol_sha256'], 'protocol')

    def test_failed_result_resume_reaudits_without_rerun(self):
        prior = {'pass': False, 'protocol_sha256': 'protocol',
                 'artifacts_sha256': {'exit_code.txt': 'fixed'}, 'reason': 'failure retained'}
        (self.root / 'result.json').write_text(json.dumps(prior))
        with patch('run_adaptive_comparison.artifact_hashes', return_value=prior['artifacts_sha256']), \
                patch('run_adaptive_comparison.collect_result', return_value=prior) as collect:
            result = resume_result(self.root, SimpleNamespace(), 'healthy', 'B10', {}, 'protocol')
        self.assertEqual(result, prior)
        self.assertEqual(collect.call_count, 1)

    def recorded_failure(self):
        directory = self.root / 'healthy' / 'B10'
        directory.mkdir(parents=True)
        (directory / 'exit_code.txt').write_text('0\n')
        (directory / 'manifest.json').write_text('{}\n')
        prior = {'pass': False, 'protocol_sha256': 'protocol', 'reason': 'old evaluator failure',
                 'artifacts_sha256': artifact_hashes(directory)}
        old_bytes = json.dumps(prior, separators=(',', ':')).encode() + b'\n'
        (directory / 'result.json').write_bytes(old_bytes)
        (self.root / 'summary.json').write_text('{"pass": false}\n')
        return directory, prior, old_bytes

    def test_explicit_reaudit_retains_old_failure_and_never_runs_simulator(self):
        directory, prior, old_bytes = self.recorded_failure()
        current = {**prior, 'pass': True}
        current.pop('reason')
        with patch('run_adaptive_comparison.collect_result', return_value=current), \
                patch('run_adaptive_comparison.run_one', side_effect=AssertionError('must not launch')):
            result = reaudit_existing(directory, SimpleNamespace(), 'healthy', 'B10', {},
                                      'protocol', 'fix same-timestamp evaluator ordering', self.root)
        self.assertTrue(result['pass'])
        revision = directory / 'audit_revisions/0001'
        self.assertEqual((revision / 'result.before.json').read_bytes(), old_bytes)
        metadata = json.loads((revision / 'revision.json').read_text())
        self.assertEqual(metadata['prior_result_sha256'], hashlib.sha256(old_bytes).hexdigest())
        self.assertEqual(metadata['revised_result_sha256'],
                         hashlib.sha256((directory / 'result.json').read_bytes()).hexdigest())
        self.assertFalse(metadata['simulator_reexecuted'])
        self.assertFalse(metadata['protocol_changed'])
        self.assertEqual(artifact_hashes(directory), prior['artifacts_sha256'])
        self.assertEqual((revision / 'summary.before.json').read_bytes(), (self.root / 'summary.json').read_bytes())

    def test_explicit_reaudit_rejects_raw_mutation(self):
        directory, prior, old_bytes = self.recorded_failure()
        (directory / 'manifest.json').write_text('{"modified":true}\n')
        with self.assertRaisesRegex(ValueError, 'unchanged raw evidence'):
            reaudit_existing(directory, SimpleNamespace(), 'healthy', 'B10', {},
                             'protocol', 'audit fix', self.root)
        self.assertEqual((directory / 'result.json').read_bytes(), old_bytes)
        self.assertFalse((directory / 'audit_revisions').exists())

    def test_explicit_reaudit_requires_existing_result(self):
        with self.assertRaisesRegex(ValueError, 'existing result.json'):
            reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B10', {},
                             'protocol', 'audit fix', self.root)

    def test_explicit_reaudit_rejects_protocol_change_and_nonzero_exit(self):
        directory, prior, old_bytes = self.recorded_failure()
        with self.assertRaisesRegex(ValueError, 'protocol identity mismatch'):
            reaudit_existing(directory, SimpleNamespace(), 'healthy', 'B10', {},
                             'another-protocol', 'audit fix', self.root)
        (directory / 'exit_code.txt').write_text('124\n')
        prior['artifacts_sha256'] = artifact_hashes(directory)
        (directory / 'result.json').write_text(json.dumps(prior))
        with self.assertRaisesRegex(ValueError, 'exit code zero'):
            reaudit_existing(directory, SimpleNamespace(), 'healthy', 'B10', {},
                             'protocol', 'audit fix', self.root)

    def test_explicit_reaudit_unchanged_does_not_create_revision(self):
        directory, prior, old_bytes = self.recorded_failure()
        with patch('run_adaptive_comparison.collect_result', return_value=prior):
            result = reaudit_existing(directory, SimpleNamespace(), 'healthy', 'B10', {},
                                      'protocol', 'audit fix', self.root)
        self.assertEqual(result, prior)
        self.assertFalse((directory / 'audit_revisions').exists())

    def test_paired_changes_preserve_negative_results(self):
        results = {'healthy': {'B8': {'pass': True, 'workload_finish_ns': 100},
                              'B9': {'pass': True, 'workload_finish_ns': 110},
                              'B10': {'pass': True, 'workload_finish_ns': 90},
                              'B11': {'pass': False}}}
        values = {row['policy']: row for row in comparison_rows(results)}
        self.assertAlmostEqual(values['B9']['time_change_vs_B8_pct'], 10)
        self.assertAlmostEqual(values['B10']['time_change_vs_B8_pct'], -10)
        self.assertIsNone(values['B11']['time_change_vs_B8_pct'])
        self.assertEqual(canonical_hash({'a': 1, 'b': 2}), canonical_hash({'b': 2, 'a': 1}))


if __name__ == '__main__':
    unittest.main()
