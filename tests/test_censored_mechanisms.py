import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from analyze_censored_mechanisms import (access_links, analyze_root, assignment_signature,
    canonical_hash, comparison, confidence_metrics, digest, distribution, fault_index, path_label, run_metrics)


def links(hosts=(0, 4)):
    return [dict(link_id=f'L{host}-{20 + rail * 4}', src_node=host,
                 dst_node=20 + rail * 4, src_type='HOST', dst_type='SWITCH',
                 src_port=rail + 2, dst_port=host + 1, link_class='ACCESS')
            for host in hosts for rail in (0, 1)]


def fault(start=20, end=50):
    return dict(fault_id='f', target_link_id='L0-20', start_time_ns=start,
                end_time_ns=end, parameter_after=.1, recovery_delay_ns=0,
                fault_type='service_degradation')


def samples(probe=False):
    events, feedback, decisions = [], [], []
    for i, (when, rail) in enumerate(((10, 0), (20, 0), (50, 0), (70, 1))):
        row = dict(timestamp_ns=when, src=0, dst=4, sport=10000 + i, rail=rail,
                   bytes=100, logical_flow_id=1, chunk_offset=i * 100, flow_bytes=400)
        events.extend([dict(row, event='ASSIGN'), dict(row, event='ACK_COMPLETE', timestamp_ns=when + 10)])
        feedback.append(dict(row, timestamp_ns=when + 10, elapsed_ns=5, audit_seq=104 + 4 * i))
        decisions.append(dict(row, base_rail=(0 if probe and i == 3 else rail),
                              probe=int(probe and i == 3), factor_A=(.5 if i == 3 else 1), factor_B=1,
                              probe_inflight=0, queued=10, reserved_A=0, reserved_B=0,
                              last_probe_ns=0, last_ack_A=0, last_ack_B=0,
                              probe_bytes=0, total_bytes=1500,
                              lower_A=0, lower_B=0, upper_A=1, upper_B=1,
                              raw_A=100, raw_B=100, audit_seq=102 + 4 * i))
    return events, feedback, decisions


def write_rows(path, values):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(values[0]))
        writer.writeheader()
        writer.writerows(values)


def fixture(root):
    protocol = {'planned_matrix': {'healthy': ['B13', 'B14']}, 'fault_sha256': {'healthy': None}}
    (root / 'protocol.json').write_text(json.dumps(protocol))
    result = {'protocol_sha256': canonical_hash(protocol), 'scenarios': {'healthy': {}}}
    for policy in ('B13', 'B14'):
        directory = root / 'healthy' / policy
        directory.mkdir(parents=True)
        events, feedback, decisions = samples()
        for name, values in [('split_events.csv', events), ('chunk_feedback.csv', feedback),
                             ('censored_decisions.csv', decisions), ('link_map.csv', links())]:
            write_rows(directory / name, values)
        intervals = [dict(timestamp_ns=row['timestamp_ns'], src=0, rail=r,
                          trigger='SELECT', lower=0, upper=1, identified_lower=0,
                          identified_upper=1, radius=1, count=0, pending=0, completed=0,
                          ready=0, audit_seq=row['audit_seq'] - 2 + r)
                     for row in decisions for r in (0, 1)]
        write_rows(directory / 'censored_intervals.csv', intervals)
        (directory / 'censored_observations.csv').write_text(
            'timestamp_ns,src,dst,sport,rail,id,bytes,launch_active,first_tx_ns,completed,audit_seq\n')
        audit = {'pass': True, 'workload_finish_ns': 80,
                 'artifacts_sha256': {p.name: digest(p) for p in directory.glob('*.csv')}}
        (directory / 'result.json').write_text(json.dumps(audit))
        result['scenarios']['healthy'][policy] = audit
    (root / 'summary.json').write_text(json.dumps(result))
    return result


class CensoredMechanismTests(unittest.TestCase):
    def test_offline_half_open_fault_labels_and_recovery(self):
        mapping, schedule = access_links(links()), fault_index([fault()])
        expected = {10: 'clean_access_before_fault', 20: 'scheduled_impaired_access',
                    49: 'scheduled_impaired_access', 50: 'clean_access_after_reversion'}
        for when, phase in expected.items():
            label = path_label(dict(src=0, dst=4, rail=0, timestamp_ns=when), mapping, schedule)
            self.assertEqual(label['phase'], phase)
            self.assertEqual(label['avoidable_impaired_assignment'], when in (20, 49))
        label = path_label(dict(src=0, dst=4, rail=0, timestamp_ns=51), mapping, schedule)
        self.assertEqual(label['lag_since_latest_selected_access_reversion_ns'], 1)
        self.assertEqual(label['selected_min_scheduled_fraction'], 1)
        self.assertEqual(path_label(dict(src=0, dst=4, rail=1, timestamp_ns=20),
                                   mapping, schedule)['phase'], 'untargeted_clean_access')

    def test_source_and_destination_access_both_count(self):
        destination = dict(fault(), target_link_id='L4-20')
        alternate = dict(fault(), target_link_id='L4-24')
        label = path_label(dict(src=0, dst=4, rail=0, timestamp_ns=20),
                           access_links(links()), fault_index([destination, alternate]))
        self.assertEqual(label['phase'], 'scheduled_impaired_access')
        self.assertTrue(label['alternate_scheduled_impaired'])
        self.assertFalse(label['avoidable_impaired_assignment'])

    def test_corrections_probes_and_observed_durations(self):
        events, feedback, decisions = samples(probe=True)
        result = run_metrics(events, feedback, decisions, links(), [fault()])
        self.assertEqual(result['rail_assigned_bytes'], {'0': 300, '1': 100})
        self.assertEqual(result['avoidable_scheduled_impaired_assignment_bytes'], 100)
        self.assertEqual(result['confidence']['any_rail_corrected_decisions'], 1)
        self.assertEqual(result['confidence']['any_rail_corrected_fraction'], .25)
        self.assertEqual(result['confidence']['chosen_rail_corrected_decisions'], 0)
        exploration = result['exploration']
        self.assertEqual(exploration['probe_chunks'], 1)
        self.assertEqual(exploration['probe_bytes'], 100)
        self.assertEqual(exploration['probe_first_tx_ack']['median_ns'], 5)
        self.assertEqual(exploration['probes'][0]['assign_ack_ns'], 10)
        self.assertEqual(exploration['probe_by_path_phase'], {'untargeted_clean_access': 1})

    def test_zero_probes_are_explicit_and_not_nan(self):
        events, feedback, decisions = samples()
        result = run_metrics(events, feedback, decisions, links(), [])
        self.assertEqual(result['exploration']['probe_chunks'], 0)
        self.assertIsNone(result['exploration']['probe_first_tx_ack']['median_ns'])
        self.assertEqual(result['exploration']['probe_assigned_byte_fraction'], 0)
        self.assertEqual(result['exploration']['probe_by_path_phase'], {})
        json.dumps(result, allow_nan=False)
        self.assertEqual(distribution([])['sum_ns'], 0)

    def test_guard_funnel_distinguishes_inactive_exploration(self):
        events, feedback, decisions = samples()
        result = run_metrics(events, feedback, decisions, links(), [])['exploration']
        self.assertEqual(result['guard_pass_counts']['target_idle'], 4)
        self.assertEqual(result['guard_fail_counts']['target_last_ack_stale_50us'], 4)
        self.assertEqual(result['first_failed_guard_counts']['source_probe_cooldown_50us'], 4)
        self.assertEqual(result['all_logged_guard_conditions_met'], 0)

    def test_confidence_overlap_and_actual_destination_pooling(self):
        observations = []
        for rail in (0, 1):
            for i in range(8):
                observations.append(dict(timestamp_ns=200, src=0, dst=4 if rail or i < 2 else 8,
                    sport=10000 + rail * 8 + i, rail=rail, id=1 + rail * 8 + i,
                    bytes=100, launch_active=4, first_tx_ns=100, completed=1,
                    audit_seq=1 + rail * 8 + i))
        intervals = [dict(timestamp_ns=300, trigger='SELECT', src=0, rail=r,
                          count=8, pending=0, completed=8, ready=1, radius=.2,
                          lower=lo, upper=hi, identified_lower=.7, identified_upper=.7,
                          audit_seq=17 + r)
                     for r, lo, hi in ((0, .6, .9), (1, .5, .8))]
        decision = dict(timestamp_ns=300, src=0, dst=4, sport=10050, rail=1,
                        base_rail=1, probe=0, lower_A=.6, upper_A=.9,
                        lower_B=.5, upper_B=.8, audit_seq=19)
        result = confidence_metrics([decision], intervals, observations, links((0, 4, 8)),
                                    [dict(fault(end=500), target_link_id='L4-20')])
        group = result['groups']['only_A_scheduled_impaired']
        self.assertEqual(group['counts']['both_ready'], 1)
        self.assertEqual(group['counts']['ready_but_overlapping'], 1)
        self.assertEqual(group['counts']['ready_separation_against_impaired_rail'], 0)
        self.assertAlmostEqual(group['numeric']['separation_gap']['mean'], -.2)
        self.assertAlmostEqual(group['numeric']['impaired_upper_minus_clean_lower']['mean'], .4)
        self.assertEqual(group['numeric']['same_destination_history_fraction_A']['mean'], .25)
        self.assertEqual(group['numeric']['scheduled_impaired_history_fraction_A']['mean'], .25)
        self.assertEqual(group['numeric']['same_destination_history_fraction_B']['mean'], 1)

    def test_confidence_snapshot_and_membership_corruption_rejected(self):
        decision = dict(timestamp_ns=10, src=0, dst=4, sport=1, rail=0, base_rail=0,
                        probe=0, lower_A=0, lower_B=0, upper_A=1, upper_B=1, audit_seq=3)
        intervals = [dict(timestamp_ns=10, trigger='SELECT', src=0, rail=r,
                          count=0, pending=0, completed=0, ready=0, radius=1,
                          lower=0, upper=1, identified_lower=0, identified_upper=1,
                          audit_seq=1 + r) for r in (0, 1)]
        result = confidence_metrics([decision], intervals, [], links(), [])
        self.assertEqual(result['groups']['noninitial_decisions']['counts']['cold_or_insufficient'], 1)
        intervals[0]['count'] = 1
        with self.assertRaisesRegex(ValueError, 'membership'):
            confidence_metrics([decision], intervals, [], links(), [])
        intervals[0]['count'] = 0
        intervals[1]['timestamp_ns'] = 9
        with self.assertRaisesRegex(ValueError, 'current source-local'):
            confidence_metrics([decision], intervals, [], links(), [])

    def test_assignment_parity_checks_timing_and_order_separately(self):
        events, _, _ = samples()
        normal = assignment_signature(events)
        reversed_signature = assignment_signature(list(reversed(events)))
        self.assertEqual(normal['content_sha256'], reversed_signature['content_sha256'])
        self.assertNotEqual(normal['ordered_sha256'], reversed_signature['ordered_sha256'])
        changed = [dict(r, timestamp_ns=int(r['timestamp_ns']) + 1) for r in events]
        self.assertNotEqual(normal['content_sha256'], assignment_signature(changed)['content_sha256'])

    def test_negative_results_and_ratio_of_means_remain_visible(self):
        result = comparison({'s1': 100, 's2': 200, 's3': 300},
                            {'s1': 110, 's2': 210, 's3': 290})
        self.assertEqual(result['n'], 3)
        self.assertEqual(result['candidate_wins'], 1)
        self.assertEqual(result['candidate_losses'], 2)
        self.assertLess(result['gain_percent_ratio_of_means'], 0)
        self.assertNotEqual(result['gain_percent_mean_of_pairs'], result['gain_percent_ratio_of_means'])
        for bad in (0, float('nan'), True):
            with self.assertRaises(ValueError):
                comparison({'s': 1}, {'s': bad})
        with self.assertRaises(ValueError):
            comparison({'a': 1}, {'b': 1})

    def test_incomplete_and_duplicate_raw_events_rejected(self):
        events, feedback, decisions = samples()
        with self.assertRaises(ValueError):
            run_metrics(events[:-1], feedback, decisions, links(), [])
        with self.assertRaises(ValueError):
            run_metrics(events + [events[0]], feedback, decisions, links(), [])
        with self.assertRaises(ValueError):
            run_metrics(events, feedback, decisions[:-1], links(), [])
        with self.assertRaises(ValueError):
            fault_index([dict(fault(), recovery_delay_ns=1)])

    def test_completed_root_parity_and_healthy_cost(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root)
            result = analyze_root(root)
            self.assertTrue(result['planned_matrix_complete'])
            self.assertEqual(result['completed_runs'], 2)
            self.assertTrue(result['B13_B14_assignment_parity']['healthy']['no_exploration_assignment_parity'])
            self.assertEqual(result['healthy_cost']['B14_minus_B13_finish_ns'], 0)
            self.assertEqual(result['families']['healthy']['comparisons']['B14_vs_B13']['n'], 1)

    def test_partial_matrix_requires_opt_in(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = fixture(root)
            del report['scenarios']['healthy']['B14']
            (root / 'summary.json').write_text(json.dumps(report))
            with self.assertRaises(ValueError):
                analyze_root(root)
            result = analyze_root(root, True)
            self.assertEqual(result['missing_runs'], ['healthy/B14'])
            self.assertIsNone(result['healthy_cost']['B14_probes'])

    def test_changed_raw_evidence_or_failed_audit_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = fixture(root)
            file = root / 'healthy/B14/censored_decisions.csv'
            file.write_text(file.read_text() + '\n')
            with self.assertRaisesRegex(ValueError, 'raw evidence changed'):
                analyze_root(root)
            report['scenarios']['healthy']['B13']['pass'] = False
            (root / 'summary.json').write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, 'failed run'):
                analyze_root(root)

    def test_cli_exclusive_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root)
            out = root / 'derived.json'
            script = Path(__file__).resolve().parents[1] / 'tools/analyze_censored_mechanisms.py'
            command = [sys.executable, str(script), '--root', str(root), '--out', str(out)]
            first = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            data = out.read_bytes()
            second = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual(out.read_bytes(), data)


if __name__ == '__main__':
    unittest.main()
