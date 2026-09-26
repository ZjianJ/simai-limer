#!/usr/bin/env python3
"""Post-hoc zero-delivery-delay shadow control; separate from the main matrix.

The 0 ns B12_ideal callbacks may change same-time event ordering even when all
signal factors equal one. This control preserves those callbacks but disables
signal actuation. It never alters the main protocol, results, or policy list.
"""
import argparse
import json
import math
import resource
import sys
from pathlib import Path
from types import SimpleNamespace

from run_adaptive_comparison import (EXPECTED_PAYLOAD, artifact_hashes, audit_faults,
    canonical_hash, endpoint_diagnostics, write_json)
from run_split_baselines import ROOT, digest, rows, run_one
from simulator_runtime_bundle import discover_runtime_closure, seal_runtime_bundle, validate_runtime_bundle


SCENARIOS = ('healthy', 'standard_42', 'standard_43', 'standard_44')
RUN_LABEL = 'B12_zero_delay_shadow'


def shadow_environment(manifest_env, protocol):
    expected = protocol['policy_specs']['B12_ideal']['extra_env']
    actual = {k: v for k, v in manifest_env.items()
              if k.startswith('LIMER_SPLIT_SIGNAL_') or k == 'LIMER_SPLIT_WAIT_SAMPLE_NS'}
    if actual != expected:
        raise ValueError('reference ideal manifest environment differs from its fixed protocol')
    if (actual.get('LIMER_SPLIT_SIGNAL_DELAY_NS') != '0'
            or actual.get('LIMER_SPLIT_SIGNAL_ENABLE') != '1'
            or actual.get('LIMER_SPLIT_SIGNAL_ACTUATE') != '1'):
        raise ValueError('reference must be the enabled, actuated, zero-delay ideal ablation')
    shadow = dict(actual)
    shadow['LIMER_SPLIT_SIGNAL_ACTUATE'] = '0'
    return shadow


def reference_result(directory, protocol_hash):
    result = json.loads((directory / 'result.json').read_text())
    if (result.get('protocol_sha256') != protocol_hash or not result.get('pass')
            or sum(result.get('acked_payload_bytes', [])) != EXPECTED_PAYLOAD
            or int((directory / 'exit_code.txt').read_text().strip()) != 0):
        raise ValueError(f'reference lacks a passing completed run: {directory}')
    if result.get('artifacts_sha256') != artifact_hashes(directory):
        raise ValueError(f'reference raw evidence has changed: {directory}')
    return result


def command_argument(manifest, option):
    command = manifest['command']
    if command.count(option) != 1:
        raise ValueError(f'reference command must contain exactly one {option}')
    index = command.index(option)
    if index + 1 >= len(command):
        raise ValueError(f'missing reference command value for {option}')
    return command[index + 1]


def audit_shadow(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    env = manifest['env']
    if (manifest['policy'] != 'B12' or env.get('LIMER_SPLIT_SIGNAL_ACTUATE') != '0'
            or env.get('LIMER_SPLIT_SIGNAL_ENABLE') != '1'
            or env.get('LIMER_SPLIT_SIGNAL_DELAY_NS') != '0'):
        raise ValueError('shadow control has the wrong actuation or delivery settings')
    decisions = rows(directory / 'adaptive_decisions.csv')
    if not decisions:
        raise ValueError('missing shadow decisions')
    if any(not math.isclose(float(e['signal_' + rail]), 1, rel_tol=0, abs_tol=1e-12)
           for e in decisions for rail in ('A', 'B')):
        raise ValueError('disabled shadow signal changed a scheduling factor')
    signals = rows(directory / 'switch_signals.csv')
    emitted = [e for e in signals if e['event'] == 'EMIT']
    delivered = [e for e in signals if e['event'] == 'DELIVER']
    if not emitted or not delivered:
        raise ValueError('zero-delay shadow did not preserve signal callbacks')
    if any(int(e['timestamp_ns']) != int(e['sampled_ns']) for e in delivered):
        raise ValueError('shadow signal delivery was not zero delay')
    return {'pass': True, 'decision_count': len(decisions), 'signal_affected_decisions': 0,
            'emitted_reports': len(emitted), 'delivered_messages': len(delivered),
            'limited_emitted_reports': sum(float(e['factor']) < 1 for e in emitted)}


def paired_change(result, baseline):
    if not result.get('pass'):
        return None
    finish = result['workload_finish_ns']
    reference = baseline['workload_finish_ns']
    return {'delta_ns': finish - reference, 'time_change_pct': 100 * (finish / reference - 1),
            'same_completion_ns': finish == reference}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary', type=Path,
                        help='default: original executable source path recorded in the reference runtime bundle')
    parser.add_argument('--topology', type=Path, help='default: reference healthy/B12_ideal command -n')
    parser.add_argument('--workload', type=Path, help='default: reference healthy/B12_ideal command -w')
    parser.add_argument('--config', type=Path, default=ROOT / 'limer/configs/SimAI.baseline.conf',
                        help='original base config, verified against the reference protocol hash')
    opt = parser.parse_args()
    opt.reference, opt.out = opt.reference.resolve(), opt.out.resolve()
    try:
        if opt.out.exists():
            raise ValueError('shadow output root must be new; existing runs are never overwritten')
        protocol_path = opt.reference / 'protocol.json'
        protocol = json.loads(protocol_path.read_text())
        protocol_hash = canonical_hash(protocol)
        reference_bundle = validate_runtime_bundle(
            opt.reference / 'runtime-bundles' / protocol['runtime_identity_sha256'])
        reference = {}
        for scenario in SCENARIOS:
            reference[scenario] = {label: reference_result(opt.reference / scenario / label, protocol_hash)
                                   for label in ('B11', 'B12_ideal')}
        manifest = json.loads((opt.reference / 'healthy/B12_ideal/manifest.json').read_text())
        env = shadow_environment(manifest['env'], protocol)
        if command_argument(manifest, '-t') != '1':
            raise ValueError('reference must use one simulator worker')
        binary = (opt.binary or Path(reference_bundle.manifest['executable']['source_path'])).resolve()
        topology = (opt.topology or Path(command_argument(manifest, '-n'))).resolve()
        workload = (opt.workload or Path(command_argument(manifest, '-w'))).resolve()
        config = opt.config.resolve()
        for key, path in (('topology', topology), ('workload', workload), ('config', config)):
            if digest(path) != protocol['input_sha256'][key]:
                raise ValueError(f'{key} differs from the reference protocol')
        if discover_runtime_closure(binary).identity_sha256 != reference_bundle.identity_sha256:
            raise ValueError('current executable/runtime closure differs from the reference sealed runtime')
        fault_sources = {}
        for scenario in SCENARIOS:
            path = opt.reference / scenario / 'faults.csv'
            if scenario == 'healthy':
                if path.exists():
                    raise ValueError('healthy reference unexpectedly contains a fault schedule')
                fault_sources[scenario] = None
            else:
                fault_sources[scenario] = path
            for label in ('B11', 'B12_ideal'):
                record = json.loads((opt.reference / scenario / label / 'manifest.json').read_text())
                if (record.get('topology_sha256') != digest(topology)
                        or record.get('workload_sha256') != digest(workload)
                        or record.get('binary_sha256') != digest(reference_bundle.executable)
                        or record.get('fault_sha256') != (digest(path) if scenario != 'healthy' else None)):
                    raise ValueError('reference policy inputs do not match the common comparison')
                if label == 'B12_ideal' and shadow_environment(record['env'], protocol) != env:
                    raise ValueError('reference ideal settings differ across scenarios')
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        opt.out.mkdir(parents=True, exist_ok=False)
        bundle = seal_runtime_bundle(opt.out, binary)
        if bundle.identity_sha256 != reference_bundle.identity_sha256:
            raise ValueError('new runtime seal does not match the reference')
        control_protocol = {
            'schema': 'limer.zero-delay-shadow-control.v1',
            'classification': 'post-hoc evaluator/event-scheduling sensitivity control',
            'not_in_main_50_or_original_12_ablations': True,
            'reference_protocol_sha256': protocol_hash,
            'reference_protocol_file_sha256': digest(protocol_path),
            'reference_root': str(opt.reference), 'runtime_identity_sha256': bundle.identity_sha256,
            'input_sha256': protocol['input_sha256'], 'policy': 'B12', 'run_label': RUN_LABEL,
            'extra_env': env, 'scenarios': list(SCENARIOS),
            'threads': 1, 'timeout_seconds': 180, 'chunk_bytes': 65536, 'max_active': 8,
            'telemetry_interval_us': 1000, 'horizon_ns': 100000000,
            'reference_result_sha256': {
                scenario: {label: digest(opt.reference / scenario / label / 'result.json')
                           for label in ('B11', 'B12_ideal')} for scenario in SCENARIOS},
            'scope': 'callbacks sampled every 10us, delivered at 0ns, actuation disabled; no GP or transport recovery'}
        write_json(opt.out / 'protocol.json', control_protocol)
        report = {'schema': control_protocol['schema'], 'protocol_sha256': canonical_hash(control_protocol),
                  'classification': control_protocol['classification'], 'scenarios': {}}
        args = SimpleNamespace(binary=bundle.executable, bundle=bundle, topology=topology, workload=workload,
            config=config, threads=1, timeout=180, chunk_bytes=65536, max_active=8,
            sample_us=1000, horizon_ns=100000000, extra_env=env)
        for scenario in SCENARIOS:
            args.out = opt.out / scenario
            args.out.mkdir()
            source_faults = fault_sources[scenario]
            args.faults = args.out / 'faults.csv' if source_faults else None
            if source_faults:
                args.faults.write_bytes(source_faults.read_bytes())
                if digest(args.faults) != digest(source_faults):
                    raise ValueError('fault schedule copy hash mismatch')
            result = run_one(args, RUN_LABEL, 'B12')
            directory = args.out / RUN_LABEL
            if result.get('pass'):
                try:
                    result['payload_exact'] = sum(result['acked_payload_bytes']) == EXPECTED_PAYLOAD
                    result['fault_audit'] = audit_faults(directory, args.faults, result['workload_finish_ns'])
                    result['shadow_audit'] = audit_shadow(directory)
                    result['diagnostics'] = endpoint_diagnostics(directory, args.faults)
                    result['pass'] = result['payload_exact']
                except (ValueError, KeyError, OSError) as exc:
                    result.update({'pass': False, 'reason': str(exc)})
            result.update(protocol_sha256=report['protocol_sha256'], artifacts_sha256=artifact_hashes(directory))
            write_json(directory / 'result.json', result)
            report['scenarios'][scenario] = {
                'result': result,
                'reference_finish_ns': {label: reference[scenario][label]['workload_finish_ns']
                                        for label in ('B11', 'B12_ideal')},
                'comparison': {label: paired_change(result, reference[scenario][label])
                               for label in ('B11', 'B12_ideal')}}
            report['complete'] = len(report['scenarios']) == len(SCENARIOS)
            report['pass'] = report['complete'] and all(v['result']['pass'] for v in report['scenarios'].values())
            write_json(opt.out / 'summary.json', report)
            print(scenario, RUN_LABEL, 'pass=' + str(result.get('pass')),
                  'finish_ns=' + str(result.get('workload_finish_ns')), flush=True)
            validate_runtime_bundle(bundle.root)
        return 0 if report['pass'] else 1
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(2, f'zero-delay shadow stopped without overwriting reference data: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())
