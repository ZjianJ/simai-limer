import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
import run_censored_comparison as runner
import summarize_censored_comparison as summary


def write_csv(path, data):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(data[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(data)


class CensoredComparisonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.topology = {'host_access_peers_by_plane': {
            str(host): {'0': [20+host % 4], '1': [24+host % 4]} for host in range(16)}}

    def test_main_and_ablation_matrix_is_prespecified_90_runs(self):
        matrix = runner.planned_matrix()
        self.assertEqual(len(matrix), 19)
        self.assertEqual(sum(map(len, matrix.values())), 90)
        self.assertEqual(sum('B14_shadow' in ps for ps in matrix.values()), 4)
        self.assertEqual(sum('B14_completed_only' in ps for ps in matrix.values()), 10)
        self.assertTrue(all(set(runner.MAIN_POLICIES) <= set(ps) for ps in matrix.values()))

    def test_scenario_expansion_is_stable_and_deduplicated(self):
        self.assertEqual(runner.scenario_names(['all']), list(runner.SCENARIOS))
        self.assertEqual(runner.scenario_names(['temporary', 'temporary_42']),
                         ['temporary_42', 'temporary_43', 'temporary_44'])
        with self.assertRaises(ValueError):
            runner.scenario_names(['future_bad'])

    def test_seed_hosts_one_per_server_and_reproducible(self):
        expected = {'42': (0, 4, 10, 13), '43': (0, 6, 9, 15), '44': (3, 4, 9, 15)}
        for seed, hosts in expected.items():
            self.assertEqual(runner.targeted_hosts(seed), hosts)
            self.assertEqual([host//4 for host in hosts], list(range(4)))

    def test_ablation_changes_only_one_environment_key(self):
        policy, base = runner.policy_spec('B14')
        self.assertEqual(policy, 'B14')
        for label, key in (('B14_completed_only', 'LIMER_SPLIT_CENSORED_INCLUDE_PENDING'),
                           ('B14_shadow', 'LIMER_SPLIT_CENSORED_ACTUATE')):
            actual_policy, env = runner.policy_spec(label)
            self.assertEqual(actual_policy, policy)
            self.assertEqual([k for k in env if env[k] != base[k]], [key])
        self.assertEqual(runner.policy_spec('B13')[1], base)
        with self.assertRaises(ValueError):
            runner.policy_spec('B15')

    def schedule(self, scenario):
        data = runner.scenario_fault_bytes(self.root, self.topology, scenario)
        return list(csv.DictReader(io.StringIO(data.decode())))

    def test_temporary_is_severe_single_rail_and_finite(self):
        data = self.schedule('temporary_42')
        self.assertEqual(len(data), 4)
        self.assertEqual({r['target_link_id'] for r in data}, {'L0-20', 'L4-20', 'L10-22', 'L13-21'})
        for row in data:
            self.assertEqual(row['fault_type'], 'service_degradation')
            self.assertEqual(row['start_time_ns'], '200000')
            self.assertEqual(row['end_time_ns'], '650000')
            self.assertEqual(float(row['parameter_after']), .1)
            self.assertEqual(row['recovery_delay_ns'], '0')

    def test_alternating_switches_rail_without_overlap(self):
        data = self.schedule('alternating_43')
        self.assertEqual(len(data), 8)
        for a, b in zip(data[:4], data[4:]):
            ahost, aswitch = map(int, a['target_link_id'][1:].split('-'))
            bhost, bswitch = map(int, b['target_link_id'][1:].split('-'))
            self.assertEqual(ahost, bhost)
            self.assertEqual(bswitch-aswitch, 4)
            self.assertLess(int(a['end_time_ns']), int(b['start_time_ns']))

    def test_mild_is_fifteen_percent_slowdown(self):
        for row in self.schedule('mild_44'):
            self.assertAlmostEqual(float(row['severity']), .15)
            self.assertEqual(float(row['parameter_after']), .85)
            self.assertEqual(row['end_time_ns'], '100000000')

    def test_faults_are_identical_every_policy(self):
        data = runner.scenario_fault_bytes(self.root, self.topology, 'temporary_42')
        self.assertEqual(data, runner.scenario_fault_bytes(self.root, self.topology, 'temporary_42'))
        self.assertNotEqual(data, runner.scenario_fault_bytes(self.root, self.topology, 'temporary_43'))

    def test_healthy_contains_no_fault_schedule(self):
        self.assertIsNone(runner.scenario_fault_bytes(self.root, self.topology, 'healthy'))

    def test_raw_mutation_is_fail_closed(self):
        (self.root/'exit_code.txt').write_text('0\n')
        (self.root/'result.json').write_text(json.dumps({'protocol_sha256': 'x', 'artifacts_sha256': {}}))
        with patch.object(runner, 'artifact_hashes', return_value={'x': 'changed'}):
            with self.assertRaisesRegex(ValueError, 'evidence changed'):
                runner.resume_result(self.root, SimpleNamespace(), 'healthy', 'B8', 'x', {})

    def test_incomplete_run_is_not_reexecuted(self):
        with self.assertRaisesRegex(ValueError, 'incomplete run preserved'):
            runner.resume_result(self.root, SimpleNamespace(), 'healthy', 'B8', 'x', {})

    def test_environment_contamination_rejected(self):
        (self.root/'manifest.json').write_text(json.dumps({'env': {'LIMER_SPLIT_CENSORED_ACTUATE': '0'}}))
        with patch.object(runner, 'verify_manifest'):
            with self.assertRaisesRegex(ValueError, 'unexpected adaptive-policy'):
                runner.collect_result(self.root, SimpleNamespace(extra_env={}), 'healthy', 'B8', 'x', {})

    def test_reaudit_preserves_old_failed_result_and_never_runs_simulator(self):
        before = {'pass': False, 'protocol_sha256': 'x', 'artifacts_sha256': {'csv': 'fixed'}, 'reason': 'old evaluator'}
        after = {'pass': True, 'protocol_sha256': 'x', 'artifacts_sha256': {'csv': 'fixed'}}
        (self.root/'exit_code.txt').write_text('0\n')
        (self.root/'result.json').write_text(json.dumps(before))
        (self.root/'summary.json').write_text('{"pass": false}\n')
        before_bytes = (self.root/'result.json').read_bytes()
        with patch.object(runner, 'artifact_hashes', return_value=before['artifacts_sha256']), \
                patch.object(runner, 'collect_result', return_value=after), \
                patch.object(runner, 'run_one') as simulator:
            got = runner.reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B13', 'x', {}, 'ordering fix', self.root)
        self.assertEqual(got, after)
        simulator.assert_not_called()
        revision = self.root/'audit_revisions/0001'
        self.assertEqual((revision/'result.before.json').read_bytes(), before_bytes)
        self.assertEqual(json.loads((revision/'result.after.json').read_text()), after)
        metadata = json.loads((revision/'revision.json').read_text())
        self.assertFalse(metadata['simulator_reexecuted'])
        self.assertFalse(metadata['protocol_changed'])
        self.assertEqual(metadata['reason'], 'ordering fix')

    def test_reaudit_rejects_missing_result_nonzero_exit_and_mutated_evidence(self):
        with self.assertRaisesRegex(ValueError, 'existing result'):
            runner.reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B13', 'x', {}, 'fix', self.root)
        before = {'pass': False, 'protocol_sha256': 'x', 'artifacts_sha256': {'csv': 'fixed'}}
        (self.root/'result.json').write_text(json.dumps(before))
        (self.root/'exit_code.txt').write_text('1\n')
        with patch.object(runner, 'artifact_hashes', return_value=before['artifacts_sha256']):
            with self.assertRaisesRegex(ValueError, 'exit zero'):
                runner.reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B13', 'x', {}, 'fix', self.root)
        with patch.object(runner, 'artifact_hashes', return_value={'csv': 'mutated'}):
            with self.assertRaisesRegex(ValueError, 'unchanged raw'):
                runner.reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B13', 'x', {}, 'fix', self.root)

    def test_reaudit_nochange_does_not_create_revision(self):
        value = {'pass': True, 'protocol_sha256': 'x', 'artifacts_sha256': {'csv': 'fixed'}}
        (self.root/'result.json').write_text(json.dumps(value))
        (self.root/'exit_code.txt').write_text('0\n')
        with patch.object(runner, 'artifact_hashes', return_value=value['artifacts_sha256']), \
                patch.object(runner, 'collect_result', return_value=value):
            self.assertEqual(runner.reaudit_existing(self.root, SimpleNamespace(), 'healthy', 'B13', 'x', {}, 'fix', self.root), value)
        self.assertFalse((self.root/'audit_revisions').exists())

    def test_fault_diagnostics_use_actual_time_not_ever_faulted(self):
        links = [dict(link_id=f'L{host}-{20+host % 4+rail*4}', src_node=host,
                      src_port=rail+2, link_class='ACCESS', src_type='HOST')
                 for host in range(16) for rail in (0, 1)]
        write_csv(self.root/'link_map.csv', links)
        data = [dict(event='ASSIGN', timestamp_ns=t, src=0, dst=4, rail=rail, bytes=10)
                for t, rail in ((100000, 0), (300000, 0), (400000, 1), (700000, 0))]
        data += [dict(event='ACK_COMPLETE', timestamp_ns=t+100, src=0, dst=4, rail=rail, bytes=10)
                 for t, rail in ((100000, 0), (300000, 0), (400000, 1), (700000, 0))]
        write_csv(self.root/'split_events.csv', data)
        fault = self.schedule('temporary_42')[0]
        write_csv(self.root/'faults.csv', [fault])
        result = runner.fault_diagnostics(self.root, self.root/'faults.csv')
        self.assertEqual(result['assigned_on_currently_impaired_path_bytes'], 10)
        self.assertEqual(result['assigned_on_impaired_path_with_clean_alternate_bytes'], 10)
        self.assertEqual(result['fault_episodes'][0]['first_post_revert_assignment_lag_ns'], 50000)
        self.assertEqual(result['fault_episodes'][0]['assigned_fraction_before_start'], .25)


class CensoredSummaryTest(unittest.TestCase):
    def test_assignment_trace_ignores_only_policy_label(self):
        a = [dict(event='ASSIGN', policy='B8', timestamp_ns='10', rail='0', bytes='65536', src='0', dst='4', sport='10000'),
             dict(event='ACK_COMPLETE', policy='B8', timestamp_ns='20', rail='0', bytes='65536', src='0', dst='4', sport='10000')]
        b = [{**row, 'policy': 'B14'} for row in a]
        with patch.object(summary, 'rows', return_value=a):
            first = summary.assignment_trace(Path('/unneeded'))
        with patch.object(summary, 'rows', return_value=b):
            second = summary.assignment_trace(Path('/unneeded'))
        self.assertEqual(first, second)
        b[0]['timestamp_ns'] = '11'
        with patch.object(summary, 'rows', return_value=b):
            self.assertNotEqual(first, summary.assignment_trace(Path('/unneeded')))

    def test_cross_source_reordering_preserves_keyed_behavior_but_retime_or_rail_change_does_not(self):
        a = [dict(event='ASSIGN', policy='B8', timestamp_ns='10', rail='0', bytes='65536', src='0', dst='4', sport='10000'),
             dict(event='ASSIGN', policy='B8', timestamp_ns='9', rail='1', bytes='65536', src='1', dst='5', sport='10000')]
        with patch.object(summary, 'rows', return_value=a):
            original = summary.assignment_trace(Path('/unneeded'))
        with patch.object(summary, 'rows', return_value=list(reversed(a))):
            reordered = summary.assignment_trace(Path('/unneeded'))
        self.assertNotEqual(original['sha256'], reordered['sha256'])
        self.assertEqual(original['keyed_sha256'], reordered['keyed_sha256'])
        for field, changed in (('timestamp_ns', '11'), ('rail', '1'), ('bytes', '32768')):
            b = [dict(row) for row in a]
            b[0][field] = changed
            with patch.object(summary, 'rows', return_value=b):
                self.assertNotEqual(original['keyed_sha256'], summary.assignment_trace(Path('/unneeded'))['keyed_sha256'])

    def test_duplicate_chunk_key_is_not_silently_collapsed_by_canonical_comparison(self):
        row = dict(event='ASSIGN', policy='B8', timestamp_ns='10', rail='0', bytes='65536', src='0', dst='4', sport='10000')
        with patch.object(summary, 'rows', return_value=[row, row]):
            with self.assertRaisesRegex(ValueError, 'unique chunk keys'):
                summary.assignment_trace(Path('/unneeded'))

    def test_summary_requires_complete_matrix_and_unchanged_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            protocol = {'planned_matrix': runner.planned_matrix()}
            identity = runner.canonical_hash(protocol)
            cells = {s: {p: {'pass': True, 'workload_finish_ns': 100, 'protocol_sha256': identity,
                             'artifacts_sha256': {'x': 'fixed'}} for p in ps}
                     for s, ps in runner.planned_matrix().items()}
            report = {'pass': True, 'planned_matrix_complete': True,
                      'protocol_sha256': identity, 'scenarios': cells}
            (root/'protocol.json').write_text(json.dumps(protocol))
            (root/'summary.json').write_text(json.dumps(report))
            with patch.object(summary, 'artifact_hashes', return_value={'x': 'fixed'}):
                self.assertEqual(summary.validate_report(root)[0], cells)
            with patch.object(summary, 'artifact_hashes', return_value={'x': 'changed'}):
                with self.assertRaisesRegex(ValueError, 'artifact changed'):
                    summary.validate_report(root)
            report['planned_matrix_complete'] = False
            (root/'summary.json').write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, '90 prespecified'):
                summary.validate_report(root)

    def test_gain_is_ratio_of_means_distinct_from_mean_of_ratios(self):
        value = summary.compare([5, 9], [10, 10])
        self.assertEqual(value['faster_count'], 2)
        self.assertAlmostEqual(value['gain_from_mean_pct'], 30)
        value = summary.compare([5, 9], [10, 100])
        self.assertNotEqual(value['gain_from_mean_pct'], value['mean_paired_gain_pct'])

    def test_empty_or_mismatched_pairs_rejected(self):
        for values, refs in (([], []), ([1], [1, 2])):
            with self.assertRaises(ValueError):
                summary.compare(values, refs)

    def test_generated_summary_retains_negative_results_and_shadow_warning(self):
        cells = {}
        for scenario, policies in runner.planned_matrix().items():
            cells[scenario] = {policy: {'workload_finish_ns': 100 if policy != 'B14' else 110,
                'diagnostics': {'assigned_on_currently_impaired_path_bytes': 1,
                                'assigned_on_impaired_path_with_clean_alternate_bytes': 1}}
                               for policy in policies}
        cells['temporary_42']['B14_shadow']['workload_finish_ns'] = 101
        with patch.object(summary, 'validate_report', return_value=(cells, 'sha')), \
                patch.object(summary, 'assignment_trace', return_value={'count': 1, 'sha256': 'same', 'keyed_sha256': 'same'}), \
                patch.object(summary, 'policy_mechanisms', return_value={'probe_chunks': 0, 'assignments_reading_nonunit_risk_factor': 1}):
            result = summary.summarize(Path('/unneeded'))
        self.assertEqual(result['run_count'], 90)
        self.assertFalse(result['all_shadow_controls_exact_B8_parity'])
        self.assertTrue(result['all_shadow_controls_exact_B8_assignment_trace_parity'])
        self.assertTrue(result['all_shadow_controls_exact_B8_keyed_assignment_parity'])
        self.assertAlmostEqual(result['families']['mild']['B14']['vs']['B8']['gain_from_mean_pct'], -10)
        rendered = summary.render(result)
        self.assertIn('-10.000', rendered)
        self.assertIn('No GP', rendered)


if __name__ == '__main__':
    unittest.main()
