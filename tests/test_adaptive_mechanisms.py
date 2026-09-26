import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from analyze_adaptive_mechanisms import analyze_root, analyze_run, decision_metrics, rows, strict_choice, switch_metrics


class MechanismTest(unittest.TestCase):
    def test_strict_choice_uses_relative_tie_margin(self):
        self.assertEqual(strict_choice([1e-5, 2e-5]), 0)
        self.assertEqual(strict_choice([2e-5, 1e-5]), 1)
        self.assertIsNone(strict_choice([1e-5, 1.00001e-5]))
        with self.assertRaises(ValueError):
            strict_choice([0, 1])

    def test_probes_cost_ratio_and_same_state_flips(self):
        configs = [
            # rail, raw_A, raw_B, signal_A, signal_B, backlog_A, backlog_B
            (0, 0, 0, 1, 1, 0, 0),
            (1, 0, 0, 1, 1, 0, 0),
            (1, 16e9, 16e9, .25, 1, 0, 10),  # Actual B, no-signal A.
            (0, 16e9, 8e9, 1, .5, 0, 0),    # A remains best.
            (1, 16e9, 16e9, .5, 1, 0, 0),   # No-signal tie.
            (0, 16e9, 8e9, .5, 1, 0, 0),    # Actual tie.
            (0, 16e9, 0, .5, 1, 0, 0),      # B unmeasured; exclude.
        ]
        decisions, events, feedback = [], [], []
        for i, (rail, raw_a, raw_b, sig_a, sig_b, back_a, back_b) in enumerate(configs):
            row = dict(timestamp_ns=100 * i, audit_seq=i, src=0, dst=4,
                       sport=i, rail=rail, bytes=10, raw_A=raw_a, raw_B=raw_b,
                       wait_A=1, wait_B=1, signal_A=sig_a, signal_B=sig_b,
                       rate_A=raw_a * sig_a, rate_B=raw_b * sig_b,
                       reserved_A=back_a, reserved_B=back_b)
            decisions.append(row)
            events.extend([dict(row, event='ASSIGN'),
                           dict(row, event='ACK_COMPLETE', timestamp_ns=100 * i + 10)])
            feedback.append(dict(row, elapsed_ns=10))
        result = decision_metrics(decisions, events, feedback, 'B12')
        self.assertEqual(result['chunk_count'], 7)
        self.assertEqual(result['chunk_elapsed_median_us'], .01)
        self.assertEqual(result['predicted_cost_ratio_count'], 5)
        self.assertEqual(result['predicted_cost_over_assign_ack_median'], 1)
        counterfactual = result['same_state_no_signal_counterfactual']
        self.assertEqual(counterfactual['excluded_initial_probes'], 2)
        self.assertEqual(counterfactual['signal_limited_decisions'], 5)
        self.assertEqual(counterfactual['strict_comparable_decisions'], 2)
        self.assertEqual(counterfactual['strict_rail_flips'], 1)
        self.assertEqual(counterfactual['strict_unchanged_decisions'], 1)
        self.assertEqual(counterfactual['ambiguous_near_tie'], 2)
        self.assertEqual(counterfactual['excluded_nonpositive_or_nonfinite_rates'], 1)
        self.assertEqual(result['nonprobe_rail_choices'], {'0': 3, '1': 2})

    def test_fault_phase_boundaries_and_exit_demand(self):
        link_map = [dict(link_id='L0-20', src_node=0, dst_node=20,
                         src_type='HOST', dst_type='SWITCH', src_port=2,
                         dst_port=1, link_class='ACCESS')]
        faults = [dict(target_link_id='L0-20', start_time_ns=20, end_time_ns=50,
                       recovery_delay_ns=0)]
        samples = [dict(timestamp_ns=t, node=20, port=1, factor=f, demand=d)
                   for t, f, d in [(10, .5, 1), (20, .5, 1), (30, .5, 1),
                                    (40, 1, 0), (50, .5, 1), (60, 1, 1)]]
        result = switch_metrics(samples, link_map, faults)
        self.assertEqual(result['limited_samples'], 4)
        self.assertEqual(result['limited_entries'], 2)
        self.assertEqual(result['limited_exits'], 2)
        self.assertEqual(result['limited_exits_without_demand'], 1)
        self.assertEqual(result['limited_samples_during_target_fault'], 2)
        self.assertEqual(result['limited_samples_outside_target_fault'], 2)
        self.assertEqual(result['limited_samples_by_phase'], {
            'before_target_fault': 1, 'active_target_fault': 2, 'after_target_fault': 1})
        self.assertEqual(result['remaining_limited_ports'], 0)
        healthy = switch_metrics(samples, link_map, [])
        self.assertEqual(healthy['limited_samples_during_target_fault'], 0)
        self.assertEqual(healthy['limited_samples_outside_target_fault'], 4)

    def test_frozen_real_runs_when_available(self):
        root = Path(__file__).resolve().parents[1] / 'results/split_baselines/adaptive_three_20260916'
        if not (root / 'standard_42/B12/switch_signal_samples.csv').exists():
            self.skipTest('optional frozen real-run fixture is not present')
        healthy = analyze_run(root / 'healthy/B11', [], 'B11')
        self.assertEqual(healthy['chunk_count'], 7680)
        self.assertEqual(healthy['predicted_cost_ratio_count'], 7648)
        self.assertAlmostEqual(healthy['predicted_cost_over_assign_ack_median'],
                               2.7634620008794464, places=10)
        fault = analyze_run(root / 'standard_42/B12', rows(root / 'standard_42/faults.csv'), 'B12')
        signal = fault['same_state_no_signal_counterfactual']
        self.assertEqual(signal['signal_limited_decisions'], 357)
        self.assertEqual(signal['strict_rail_flips'], 117)
        self.assertEqual(signal['strict_unchanged_decisions'], 240)
        self.assertEqual(signal['ambiguous_near_tie'], 0)
        self.assertEqual(fault['nonprobe_rail_choices'], {'0': 3800, '1': 3848})
        self.assertEqual(fault['switch_samples']['limited_samples_during_target_fault'], 167)
        self.assertEqual(fault['switch_samples']['limited_samples_outside_target_fault'], 1)
        self.assertEqual(fault['switch_samples']['limited_exits_without_demand'], 28)

    def test_fault_schedule_missing_is_not_labeled_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'summary.json').write_text(json.dumps({'scenarios': {'standard_42': {}}}))
            with self.assertRaises(ValueError):
                analyze_root(root)

    def test_cli_output_is_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'summary.json').write_text(json.dumps({'scenarios': {}}))
            target = root / 'out.json'
            script = Path(__file__).resolve().parents[1] / 'tools/analyze_adaptive_mechanisms.py'
            command = [sys.executable, str(script), '--root', str(root), '--out', str(target)]
            first = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            saved = target.read_bytes()
            second = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual(target.read_bytes(), saved)


if __name__ == '__main__':
    unittest.main()
