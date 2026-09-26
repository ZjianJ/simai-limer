import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from run_zero_delay_shadow import audit_shadow, command_argument, paired_change, shadow_environment


class ZeroDelayShadowTest(unittest.TestCase):
    def environment(self):
        return {'LIMER_SPLIT_WAIT_SAMPLE_NS': '10000', 'LIMER_SPLIT_SIGNAL_SAMPLE_NS': '10000',
                'LIMER_SPLIT_SIGNAL_DELAY_NS': '0', 'LIMER_SPLIT_SIGNAL_TTL_NS': '50000',
                'LIMER_SPLIT_SIGNAL_HEARTBEAT_NS': '40000', 'LIMER_SPLIT_SIGNAL_ENABLE': '1',
                'LIMER_SPLIT_SIGNAL_ACTUATE': '1'}

    def test_only_actuation_changes(self):
        original = self.environment()
        protocol = {'policy_specs': {'B12_ideal': {'extra_env': original.copy()}}}
        shadow = shadow_environment({**original, 'UNRELATED': 'ignored'}, protocol)
        self.assertEqual(shadow, {**original, 'LIMER_SPLIT_SIGNAL_ACTUATE': '0'})
        self.assertEqual(original['LIMER_SPLIT_SIGNAL_ACTUATE'], '1')
        self.assertEqual(shadow['LIMER_SPLIT_SIGNAL_DELAY_NS'], '0')

    def test_nonmatching_reference_rejected(self):
        original = self.environment()
        protocol = {'policy_specs': {'B12_ideal': {'extra_env': original}}}
        with self.assertRaises(ValueError):
            shadow_environment({**original, 'LIMER_SPLIT_SIGNAL_DELAY_NS': '10000'}, protocol)

    def test_command_argument_and_paired_sign(self):
        self.assertEqual(command_argument({'command': ['sim', '-n', 'topology']}, '-n'), 'topology')
        with self.assertRaises(ValueError):
            command_argument({'command': ['sim', '-n']}, '-n')
        self.assertEqual(paired_change({'pass': True, 'workload_finish_ns': 110},
                                      {'workload_finish_ns': 100})['delta_ns'], 10)
        self.assertIsNone(paired_change({'pass': False}, {'workload_finish_ns': 100}))

    def test_disabled_signal_and_zero_delivery_audit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {**self.environment(), 'LIMER_SPLIT_SIGNAL_ACTUATE': '0'}
            (root / 'manifest.json').write_text(json.dumps({'policy': 'B12', 'env': env}))
            (root / 'adaptive_decisions.csv').write_text('signal_A,signal_B\n1,1\n')
            with (root / 'switch_signals.csv').open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['event', 'timestamp_ns', 'sampled_ns', 'factor'])
                writer.writerow(['EMIT', 10000, 10000, .5])
                writer.writerow(['DELIVER', 10000, 10000, .5])
            result = audit_shadow(root)
            self.assertTrue(result['pass'])
            self.assertEqual(result['limited_emitted_reports'], 1)
            (root / 'adaptive_decisions.csv').write_text('signal_A,signal_B\n1,.5\n')
            with self.assertRaisesRegex(ValueError, 'scheduling factor'):
                audit_shadow(root)


if __name__ == '__main__':
    unittest.main()
