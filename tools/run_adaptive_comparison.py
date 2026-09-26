#!/usr/bin/env python3
"""Paired, resumable true-16 comparison of B8/B9 and nested B10/B11/B12.

This runner does not tune policies or use fault truth as policy input. Fault
truth is supplied only to the injector and to post-run audits. Existing runs
are never overwritten, and resume verifies the protocol and evidence hashes.
"""
import argparse
import csv
import hashlib
import io
import json
import math
import resource
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from run_split_baselines import ROOT, audit, digest, rows, run_one
from simulator_runtime_bundle import discover_runtime_closure, seal_runtime_bundle, validate_runtime_bundle


SCHEMA = 'limer.adaptive-comparison.v1'
SEEDS = ('42', '43', '44')
MAIN_POLICIES = ('B8', 'B9', 'B10', 'B11', 'B12')
POLICIES = MAIN_POLICIES + ('B12_no_signal', 'B12_ideal', 'B12_shadow')
FAMILIES = ('standard', 'early', 'recovery')
SCENARIOS = ('healthy',) + tuple(f'{family}_{seed}' for family in FAMILIES for seed in SEEDS)
TIMES = {'standard': (1000000, 100000000), 'early': (250000, 100000000),
         'recovery': (250000, 750000)}
EXPECTED_PAYLOAD = 503316480
SIGNAL_ENV = {'LIMER_SPLIT_SIGNAL_SAMPLE_NS': '10000',
              'LIMER_SPLIT_SIGNAL_DELAY_NS': '10000',
              'LIMER_SPLIT_SIGNAL_TTL_NS': '50000',
              'LIMER_SPLIT_SIGNAL_HEARTBEAT_NS': '40000'}


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def write_json(path, value):
    temporary = path.with_name(path.name + '.pending')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def scenario_names(tokens):
    values = []
    for token in tokens:
        expanded = SCENARIOS if token == 'all' else (
            tuple(f'{token}_{seed}' for seed in SEEDS) if token in FAMILIES else (token,))
        for value in expanded:
            if value not in SCENARIOS:
                raise ValueError(f'unknown scenario: {value}')
            if value not in values:
                values.append(value)
    return values


def policy_spec(label):
    if label not in POLICIES:
        raise ValueError(f'unknown policy: {label}')
    env = {}
    policy = 'B12' if label.startswith('B12') else label
    if policy in ('B10', 'B11', 'B12'):
        env['LIMER_SPLIT_WAIT_SAMPLE_NS'] = '10000'
    if policy == 'B12':
        env.update(SIGNAL_ENV)
        env['LIMER_SPLIT_SIGNAL_ENABLE'] = '0' if label == 'B12_no_signal' else '1'
        env['LIMER_SPLIT_SIGNAL_ACTUATE'] = '0' if label == 'B12_shadow' else '1'
        if label == 'B12_ideal':
            env['LIMER_SPLIT_SIGNAL_DELAY_NS'] = '0'
    return policy, env


def frozen_fault_bytes(frozen, scenario):
    if scenario == 'healthy':
        return None
    family, seed = scenario.split('_')
    original = frozen / f'seed_{seed}' / 'faults.csv'
    data = rows(original)
    if len(data) != 8 or len({r['target_link_id'] for r in data}) != 8:
        raise ValueError('frozen schedule must contain eight distinct ACCESS links')
    for row in data:
        if (row['fault_type'] != 'service_degradation'
                or int(row['start_time_ns']) != 1000000
                or int(row['end_time_ns']) != 100000000
                or int(row['recovery_delay_ns']) != 0
                or float(row['parameter_before']) != 1
                or not 0.2 <= float(row['parameter_after']) <= 1):
            raise ValueError('frozen fault violates the original a=8, b<=80%, t=1ms contract')
    if family == 'standard':
        return original.read_bytes()
    for row in data:
        row['start_time_ns'], row['end_time_ns'] = map(str, TIMES[family])
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=list(data[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(data)
    return stream.getvalue().encode()


def ensure_fault_file(directory, content):
    if content is None:
        if (directory / 'faults.csv').exists():
            raise ValueError('healthy scenario unexpectedly contains a fault schedule')
        return None
    path = directory / 'faults.csv'
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(f'resume fault schedule mismatch: {path}')
    else:
        path.write_bytes(content)
    return path


def audit_faults(directory, faults, finish_ns):
    observed = rows(directory / 'fault_application_telemetry.csv')
    schedule = rows(faults) if faults else []
    access = {r['link_id'] for r in rows(directory / 'link_map.csv') if r['link_class'] == 'ACCESS'}
    expected = {}
    for fault in schedule:
        if fault['target_link_id'] not in access:
            raise ValueError('fault target is not a physical ACCESS link')
        for transition, field, status in (('apply', 'start_time_ns', 'APPLIED'),
                                          ('revert', 'end_time_ns', 'REVERTED')):
            when = int(fault[field])
            if when <= finish_ns:
                expected[fault['fault_id'], transition] = (fault, when, status)
    if len(observed) != len(expected):
        raise ValueError('missing/extra fault application or recovery transition')
    for record in observed:
        key = record['fault_id'], record['transition']
        if key not in expected:
            raise ValueError('unknown or duplicate fault transition')
        fault, when, status = expected.pop(key)
        if (int(record['scheduled_ns']) != when or int(record['actual_ns']) != when
                or record['status'] != status
                or record['target_link_id'] != fault['target_link_id']
                or record['fault_type'] != fault['fault_type']
                or any(not math.isclose(float(record[field]), float(fault[field]),
                                        rel_tol=1e-12, abs_tol=1e-12)
                       for field in ('parameter_before', 'parameter_after'))):
            raise ValueError('fault timeline/target/parameter mismatch')
    return {'pass': True, 'expected_transitions': len(observed),
            'apply_count': sum(r['transition'] == 'apply' for r in observed),
            'revert_count': sum(r['transition'] == 'revert' for r in observed)}


def endpoint_diagnostics(directory, faults):
    events = rows(directory / 'split_events.csv')
    assignments = [r for r in events if r['event'] == 'ASSIGN']
    total = sum(int(r['bytes']) for r in assignments)
    last = max((r for r in events if r['event'] == 'ACK_COMPLETE'),
               key=lambda r: int(r['timestamp_ns']))
    result = {'last_ack': {k: int(last[k]) for k in ('timestamp_ns', 'src', 'dst', 'rail', 'bytes')}}
    signal_path = directory / 'switch_signals.csv'
    if signal_path.exists():
        signals = rows(signal_path)
        emits = [r for r in signals if r['event'] == 'EMIT']
        delivered = [r for r in signals if r['event'] == 'DELIVER']
        result['switch_signal_overhead'] = {
            'emitted_reports': len(emits), 'delivered_subscriber_messages': len(delivered),
            'emitted_payload_bytes_before_fanout': sum(int(r['payload_bytes']) for r in emits),
            'scheduled_subscriber_payload_bytes': 12 * sum(int(r['payload_bytes']) for r in emits),
            'delivered_subscriber_payload_bytes': sum(int(r['payload_bytes']) for r in delivered),
            'switch_state_bytes_per_monitored_port': 48,
            'packet_headers_and_data_plane_contention': 'not simulated; logical payload accounting only'}
    if faults:
        schedule = rows(faults)
        start = min(int(r['start_time_ns']) for r in schedule)
        affected = {r['target_link_id'] for r in schedule}
        links = {(int(r['src_node']), int(r['src_port']) - 2): r['link_id']
                 for r in rows(directory / 'link_map.csv')
                 if r['link_class'] == 'ACCESS' and r['src_type'] == 'HOST'}
        before = sum(int(r['bytes']) for r in assignments if int(r['timestamp_ns']) < start)
        after_affected = sum(int(r['bytes']) for r in assignments
                             if int(r['timestamp_ns']) >= start and (
                                 links[int(r['src']), int(r['rail'])] in affected or
                                 links[int(r['dst']), int(r['rail'])] in affected))
        result.update(assigned_before_fault_bytes=before,
                      assigned_before_fault_fraction=before / total,
                      post_fault_assigned_bytes_on_affected_access_paths=after_affected)
    return result


def artifact_hashes(directory):
    paths = sorted(directory.glob('*.csv')) + [directory / 'manifest.json', directory / 'exit_code.txt']
    return {p.name: digest(p) for p in paths}


def verify_manifest(directory, args, policy, label):
    manifest = json.loads((directory / 'manifest.json').read_text())
    hashes = {'binary_sha256': digest(args.binary), 'topology_sha256': digest(args.topology),
              'workload_sha256': digest(args.workload),
              'fault_sha256': digest(args.faults) if args.faults else None,
              'capacity_sha256': None}
    if manifest.get('policy') != policy or any(manifest.get(k) != v for k, v in hashes.items()):
        raise ValueError(f'run manifest inputs do not match protocol: {directory}')
    env = manifest['env']
    expected = {'LIMER_SPLIT_POLICY': policy, 'LIMER_RUN_ID': label,
                'LIMER_SPLIT_CHUNK_BYTES': str(args.chunk_bytes),
                'LIMER_SPLIT_MAX_ACTIVE': str(args.max_active),
                'LIMER_TELEMETRY_INTERVAL_US': str(args.sample_us),
                'LIMER_OBSERVATION_STOP_NS': str(args.horizon_ns),
                'LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE': '0', **args.extra_env}
    if any(env.get(k) != v for k, v in expected.items()):
        raise ValueError(f'run manifest environment does not match protocol: {directory}')
    actual_signal = {k: v for k, v in env.items() if k.startswith('LIMER_SPLIT_SIGNAL_')}
    expected_signal = {k: v for k, v in args.extra_env.items() if k.startswith('LIMER_SPLIT_SIGNAL_')}
    if actual_signal != expected_signal:
        raise ValueError('unexpected signal-policy environment on resume')


def collect_result(directory, args, scenario, label, original, protocol_hash):
    policy, _ = policy_spec(label)
    verify_manifest(directory, args, policy, label)
    code = int((directory / 'exit_code.txt').read_text().strip())
    result = {'pass': False, 'exit_code': code}
    if code == 0:
        try:
            result.update(audit(directory))
            if result['pass']:
                result['payload_exact'] = sum(result['acked_payload_bytes']) == EXPECTED_PAYLOAD
                result['fault_audit'] = audit_faults(directory, args.faults, result['workload_finish_ns'])
                result['diagnostics'] = endpoint_diagnostics(directory, args.faults)
                if scenario.startswith('standard_') and label == 'B8':
                    seed = scenario.split('_')[1]
                    reference = original['seeds'][seed]['results']['B8']['workload_finish_ns']
                    result['matches_previous_B8'] = result['workload_finish_ns'] == reference
                    result['previous_B8_finish_ns'] = reference
                result['pass'] = result['payload_exact'] and result.get('matches_previous_B8', True)
        except (ValueError, KeyError, OSError) as exc:
            result['reason'] = str(exc)
            result['pass'] = False
    else:
        result['reason'] = 'simulator failed or timed out; see run.log'
    result.update(protocol_sha256=protocol_hash, artifacts_sha256=artifact_hashes(directory))
    return result


def resume_result(directory, args, scenario, label, original, protocol_hash):
    result_path = directory / 'result.json'
    if result_path.exists():
        prior = json.loads(result_path.read_text())
        if prior.get('protocol_sha256') != protocol_hash:
            raise ValueError('existing result protocol identity mismatch')
        if prior.get('artifacts_sha256') != artifact_hashes(directory):
            raise ValueError('existing run evidence changed; refusing resume')
        current = collect_result(directory, args, scenario, label, original, protocol_hash)
        if current != prior:
            raise ValueError('existing result differs from raw-evidence re-audit')
        return current
    if not (directory / 'exit_code.txt').exists():
        raise ValueError(f'incomplete run preserved at {directory}; use a new output root')
    result = collect_result(directory, args, scenario, label, original, protocol_hash)
    write_json(result_path, result)
    return result


def reaudit_existing(directory, args, scenario, label, original, protocol_hash, reason, report_root):
    """Revise derived audit results, retaining old bytes; never launch a run.

    This branch intentionally does not reuse resume_result's ability to create
    a result for a previously interrupted audit. A recorded old result is
    mandatory so the old failure and its exact evidence remain attributable.
    """
    if not reason.strip():
        raise ValueError('re-audit requires an explicit reason')
    result_path = directory / 'result.json'
    if not result_path.is_file():
        raise ValueError('re-audit requires an existing result.json; no simulation will be started')
    previous_bytes = result_path.read_bytes()
    previous = json.loads(previous_bytes)
    if previous.get('protocol_sha256') != protocol_hash:
        raise ValueError('re-audit protocol identity mismatch')
    expected_artifacts = previous.get('artifacts_sha256')
    if not expected_artifacts or expected_artifacts != artifact_hashes(directory):
        raise ValueError('re-audit requires unchanged raw evidence hashes')
    if int((directory / 'exit_code.txt').read_text().strip()) != 0:
        raise ValueError('re-audit only accepts simulator exit code zero; failed simulation is preserved')
    current = collect_result(directory, args, scenario, label, original, protocol_hash)
    if artifact_hashes(directory) != expected_artifacts:
        raise ValueError('raw evidence changed during re-audit; no derived result was replaced')
    if current == previous:
        return current
    # The prior result itself must not have been edited concurrently either.
    if result_path.read_bytes() != previous_bytes:
        raise ValueError('result changed concurrently during re-audit')
    next_bytes = (json.dumps(current, indent=2, allow_nan=False) + '\n').encode()
    revisions = directory / 'audit_revisions'
    revisions.mkdir(exist_ok=True)
    index = 1
    while (revisions / f'{index:04d}').exists():
        index += 1
    revision = revisions / f'{index:04d}'
    revision.mkdir(exist_ok=False)
    (revision / 'result.before.json').write_bytes(previous_bytes)
    (revision / 'result.after.json').write_bytes(next_bytes)
    summary = report_root / 'summary.json'
    summary_hash = None
    if summary.exists():
        (revision / 'summary.before.json').write_bytes(summary.read_bytes())
        summary_hash = digest(revision / 'summary.before.json')
    auditor_files = ('run_adaptive_comparison.py', 'run_split_baselines.py', 'audit_adaptive_split.py')
    write_json(revision / 'revision.json', {
        'schema': 'limer.audit-revision.v1', 'reason': reason,
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'scenario': scenario, 'policy': label, 'protocol_sha256': protocol_hash,
        'prior_result_sha256': hashlib.sha256(previous_bytes).hexdigest(),
        'revised_result_sha256': hashlib.sha256(next_bytes).hexdigest(),
        'prior_summary_sha256': summary_hash,
        'prior_auditor_sha256': 'not recorded in original result',
        'revised_auditor_sha256': {name: digest(Path(__file__).parent / name) for name in auditor_files},
        'unchanged_artifacts_sha256': expected_artifacts,
        'simulator_reexecuted': False, 'protocol_changed': False})
    write_json(result_path, current)
    print('AUDIT_REVISION', revision, 'old_pass=' + str(previous.get('pass')),
          'new_pass=' + str(current.get('pass')), flush=True)
    return current


def comparison_rows(scenarios):
    output = []
    for scenario in SCENARIOS:
        results = scenarios.get(scenario, {})
        for policy in POLICIES:
            if policy not in results:
                continue
            result = results[policy]
            row = {'scenario': scenario, 'policy': policy, 'pass': result['pass'],
                   'finish_ns': result.get('workload_finish_ns')}
            for reference in ('B8', 'B9', 'B10', 'B11'):
                baseline = results.get(reference, {})
                value = None
                if result['pass'] and baseline.get('pass'):
                    value = 100 * (result['workload_finish_ns'] / baseline['workload_finish_ns'] - 1)
                row[f'time_change_vs_{reference}_pct'] = value
            output.append(row)
    return output


def save_report(root, report):
    values = [r for scenario in report['scenarios'].values() for r in scenario.values()]
    report['pass_completed_runs'] = bool(values) and all(r['pass'] for r in values)
    report['same_payload'] = bool(values) and all(
        r.get('pass') and sum(r['acked_payload_bytes']) == EXPECTED_PAYLOAD for r in values)
    report['main_matrix_complete'] = all(p in report['scenarios'].get(s, {})
                                         for s in SCENARIOS for p in MAIN_POLICIES)
    report['paired'] = comparison_rows(report['scenarios'])
    report['pass'] = report['main_matrix_complete'] and report['pass_completed_runs'] and report['same_payload']
    write_json(root / 'summary.json', report)
    if report['paired']:
        with (root / 'paired.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(report['paired'][0]), lineterminator='\n')
            writer.writeheader()
            writer.writerows(report['paired'])


def make_protocol(opt, bundle):
    source_faults = {}
    original = json.loads((opt.frozen / 'summary.json').read_text())
    for seed in SEEDS:
        value = digest(opt.frozen / f'seed_{seed}' / 'faults.csv')
        if value != original['seeds'][seed]['fault_sha256']:
            raise ValueError('frozen fault hash differs from original experiment manifest')
        source_faults[seed] = value
    return {'schema': SCHEMA, 'runtime_identity_sha256': bundle.identity_sha256,
            'input_sha256': {key: digest(getattr(opt, key)) for key in ('topology', 'workload', 'config')},
            'frozen_summary_sha256': digest(opt.frozen / 'summary.json'),
            'frozen_fault_sha256': source_faults,
            'chunk_bytes': 65536, 'max_active_per_source': 8, 'telemetry_interval_us': 1000,
            'horizon_ns': 100000000, 'timeout_seconds': 180, 'simulator_threads': 1,
            'expected_payload_bytes': EXPECTED_PAYLOAD, 'fault_times_ns': TIMES,
            'policy_specs': {label: {'policy': policy_spec(label)[0], 'extra_env': policy_spec(label)[1]}
                             for label in POLICIES},
            'main_scenarios': SCENARIOS, 'main_policies': MAIN_POLICIES,
            'design': 'nested incremental exploratory paired simulation; no GP; no transport recovery',
            'primary_outcome': 'all 16 ranks WORKLOAD_COMPLETE timestamp_ns from simulation zero',
            'numerical_tensor_correctness': 'not simulated',
            'ideal_ablation': 'same 10us samples, zero delivery delay; not a complete-information oracle',
            'shadow_ablation': 'same sensor reports/delivery events as B12, no signal actuation',
            'signal_budget': {'state_bytes_per_switch_port': 48, 'payload_bytes_per_report': 32,
                              'subscriber_fanout': 12, 'data_plane_control_contention_modeled': False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('binary', 'topology', 'frozen', 'out'):
        parser.add_argument('--' + flag, type=Path, required=True)
    parser.add_argument('--workload', type=Path, default=ROOT / 'limer/configs/microAllReduce_16rank_split_64mib.txt')
    parser.add_argument('--config', type=Path, default=ROOT / 'limer/configs/SimAI.baseline.conf')
    parser.add_argument('--scenarios', nargs='+', default=['all'])
    parser.add_argument('--policies', nargs='+', choices=POLICIES, default=list(MAIN_POLICIES))
    parser.add_argument('--reaudit-existing', action='store_true',
                        help='only revise derived audits of recorded exit-zero runs; never execute the simulator')
    parser.add_argument('--reaudit-reason', help='required explanation for an explicit evaluator-only revision')
    opt = parser.parse_args()
    for field in ('binary', 'topology', 'frozen', 'out', 'workload', 'config'):
        setattr(opt, field, getattr(opt, field).resolve())
    try:
        selected = scenario_names(opt.scenarios)
        if len(set(opt.policies)) != len(opt.policies):
            raise ValueError('duplicate policy')
        if opt.reaudit_existing and not (opt.reaudit_reason or '').strip():
            raise ValueError('--reaudit-existing requires --reaudit-reason')
        if opt.reaudit_reason and not opt.reaudit_existing:
            raise ValueError('--reaudit-reason requires --reaudit-existing')
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        if opt.reaudit_existing and not (opt.out / 'protocol.json').is_file():
            raise ValueError('re-audit requires an existing experiment with protocol.json')
        if opt.out.exists() and not (opt.out / 'protocol.json').exists():
            raise ValueError('existing output root lacks protocol.json; refusing to mix experiments')
        opt.out.mkdir(parents=True, exist_ok=True)
        if opt.reaudit_existing:
            prior_protocol = json.loads((opt.out / 'protocol.json').read_text())
            bundle = validate_runtime_bundle(opt.out / 'runtime-bundles' / prior_protocol['runtime_identity_sha256'])
            if discover_runtime_closure(opt.binary).identity_sha256 != bundle.identity_sha256:
                raise ValueError('re-audit binary/runtime closure differs from the fixed protocol')
        else:
            bundle = seal_runtime_bundle(opt.out, opt.binary)
        protocol = make_protocol(opt, bundle)
        # JSON-normalize tuples before identity comparison with a resumed file.
        protocol = json.loads(json.dumps(protocol))
        protocol_hash = canonical_hash(protocol)
        protocol_path = opt.out / 'protocol.json'
        if protocol_path.exists():
            if json.loads(protocol_path.read_text()) != protocol:
                raise ValueError('protocol identity changed; use a new output root')
            topology_audit = json.loads((opt.out / 'topology_validation.json').read_text())
            if (topology_audit.get('status') != 'PASS'
                    or topology_audit.get('topology_sha256') != digest(opt.topology)):
                raise ValueError('missing or mismatched topology validation on resume')
        else:
            subprocess.run([sys.executable, str(ROOT / 'limer/tools/validate_true16_dualrail.py'),
                            '--topology', str(opt.topology), '--out-json', str(opt.out / 'topology_validation.json')],
                           check=True, stdout=subprocess.DEVNULL)
            write_json(protocol_path, protocol)
        original = json.loads((opt.frozen / 'summary.json').read_text())
        report = {'schema': SCHEMA, 'protocol_sha256': protocol_hash, 'scenarios': {}}
        old_summary = opt.out / 'summary.json'
        if old_summary.exists():
            report = json.loads(old_summary.read_text())
            if report.get('protocol_sha256') != protocol_hash:
                raise ValueError('summary protocol identity mismatch')
        args = SimpleNamespace(binary=bundle.executable, bundle=bundle, topology=opt.topology,
            workload=opt.workload, config=opt.config, threads=1, chunk_bytes=65536,
            max_active=8, sample_us=1000, horizon_ns=100000000, timeout=180)
        selected_pass = True
        for scenario in selected:
            args.out = opt.out / scenario
            content = frozen_fault_bytes(opt.frozen, scenario)
            if opt.reaudit_existing:
                if not args.out.is_dir():
                    raise ValueError('re-audit scenario does not exist; no simulation will be started')
                args.faults = args.out / 'faults.csv' if content is not None else None
                if ((args.faults and (not args.faults.is_file() or args.faults.read_bytes() != content))
                        or (not args.faults and (args.out / 'faults.csv').exists())):
                    raise ValueError('re-audit scenario fault schedule differs from fixed protocol')
            else:
                args.out.mkdir(exist_ok=True)
                args.faults = ensure_fault_file(args.out, content)
            results = report['scenarios'].setdefault(scenario, {})
            for label in opt.policies:
                policy, args.extra_env = policy_spec(label)
                directory = args.out / label
                if opt.reaudit_existing:
                    result = reaudit_existing(directory, args, scenario, label, original,
                                              protocol_hash, opt.reaudit_reason, opt.out)
                    reused = True
                elif directory.exists():
                    result = resume_result(directory, args, scenario, label, original, protocol_hash)
                    reused = True
                else:
                    run_one(args, label, policy)
                    result = collect_result(directory, args, scenario, label, original, protocol_hash)
                    write_json(directory / 'result.json', result)
                    reused = False
                results[label] = result
                selected_pass = selected_pass and result['pass']
                save_report(opt.out, report)
                print(scenario, label, 'resume=' + str(reused), 'pass=' + str(result['pass']),
                      'finish_ns=' + str(result.get('workload_finish_ns')), flush=True)
                validate_runtime_bundle(bundle.root)
        return 0 if selected_pass else 1
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(2, f'comparison stopped without overwriting existing runs: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())
