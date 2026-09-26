#!/usr/bin/env python3
"""Frozen, paired true-16 censored-feedback and bounded-exploration experiment.

All schedules and the complete intended matrix are sealed before simulation.
Existing runs are hash checked and re-audited, never overwritten or reexecuted.
Fault truth is used only by the injector and post-run diagnostics.
"""
import argparse
import csv
import hashlib
import io
import json
import random
import resource
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from run_adaptive_comparison import (EXPECTED_PAYLOAD, artifact_hashes, audit_faults,
    canonical_hash, ensure_fault_file, frozen_fault_bytes, verify_manifest, write_json)
from run_split_baselines import ROOT, audit, digest, rows, run_one
from simulator_runtime_bundle import discover_runtime_closure, seal_runtime_bundle, validate_runtime_bundle
from validate_true16_dualrail import read_topology, validate


SCHEMA = 'limer.censored-comparison.v1'
SEEDS = ('42', '43', '44')
FAMILIES = ('standard', 'early', 'recovery', 'temporary', 'alternating', 'mild')
TARGETED = ('temporary', 'alternating', 'mild')
SCENARIOS = ('healthy',) + tuple(f'{family}_{seed}' for family in FAMILIES for seed in SEEDS)
MAIN_POLICIES = ('B8', 'B10', 'B13', 'B14')
POLICIES = MAIN_POLICIES + ('B14_completed_only', 'B14_shadow')
TARGET_TIMES = {
    'temporary': ((0, 200000, 650000, 0.1),),
    'alternating': ((0, 200000, 550000, 0.1), (1, 650000, 1000000, 0.1)),
    'mild': ((0, 200000, 100000000, 0.85),),
}
FAULT_FIELDS = ('fault_id', 'fault_type', 'target_link_id', 'start_time_ns', 'end_time_ns',
                'severity', 'parameter_before', 'parameter_after', 'recovery_delay_ns')


def scenario_names(tokens):
    selected = []
    for token in tokens:
        values = SCENARIOS if token == 'all' else (
            tuple(f'{token}_{seed}' for seed in SEEDS) if token in FAMILIES else (token,))
        for value in values:
            if value not in SCENARIOS:
                raise ValueError('unknown scenario: ' + value)
            if value not in selected:
                selected.append(value)
    return selected


def policy_spec(label):
    if label not in POLICIES:
        raise ValueError('unknown policy: ' + label)
    if label == 'B8':
        return label, {}
    if label == 'B10':
        return label, {'LIMER_SPLIT_WAIT_SAMPLE_NS': '10000'}
    policy = 'B14' if label.startswith('B14') else label
    env = {'LIMER_SPLIT_WAIT_SAMPLE_NS': '10000',
           'LIMER_SPLIT_CENSORED_INCLUDE_PENDING': '0' if label == 'B14_completed_only' else '1',
           'LIMER_SPLIT_CENSORED_ACTUATE': '0' if label == 'B14_shadow' else '1'}
    return policy, env


def planned_matrix():
    result = {scenario: list(MAIN_POLICIES) for scenario in SCENARIOS}
    for scenario in ('healthy',) + tuple(f'{family}_{seed}' for family in TARGETED for seed in SEEDS):
        result[scenario].append('B14_completed_only')
    for scenario in ('healthy',) + tuple(f'temporary_{seed}' for seed in SEEDS):
        result[scenario].append('B14_shadow')
    return result


def checked_topology(path):
    result = validate(read_topology(path), 16, 4)
    if result['status'] != 'PASS':
        raise ValueError('topology does not satisfy the true-16 dual-rail physical contract')
    result['topology_sha256'] = digest(path)
    return result


def targeted_hosts(seed):
    rng = random.Random(int(seed))
    return tuple(server * 4 + rng.randrange(4) for server in range(4))


def scenario_fault_bytes(frozen, topology_audit, scenario):
    if scenario == 'healthy' or scenario.split('_')[0] not in TARGETED:
        return frozen_fault_bytes(frozen, scenario)
    family, seed = scenario.split('_')
    data = []
    peers = topology_audit['host_access_peers_by_plane']
    for rail, start, end, fraction in TARGET_TIMES[family]:
        for host in targeted_hosts(seed):
            peer = peers[str(host)][str(rail)]
            if len(peer) != 1:
                raise ValueError('target host needs exactly one ACCESS link per rail')
            link = f'L{min(host, peer[0])}-{max(host, peer[0])}'
            data.append(dict(fault_id=f'{family}-{seed}-{link}', fault_type='service_degradation',
                target_link_id=link, start_time_ns=start, end_time_ns=end,
                severity=1-fraction, parameter_before=1, parameter_after=fraction,
                recovery_delay_ns=0))
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=FAULT_FIELDS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(data)
    return stream.getvalue().encode()


def fault_diagnostics(directory, faults):
    """Descriptive actual-state labels, never used by any online policy."""
    events = rows(directory / 'split_events.csv')
    assigned = [r for r in events if r['event'] == 'ASSIGN']
    complete = [r for r in events if r['event'] == 'ACK_COMPLETE']
    total = sum(int(r['bytes']) for r in assigned)
    schedule = rows(faults) if faults else []
    links = {(int(r['src_node']), int(r['src_port'])-2): r['link_id']
             for r in rows(directory / 'link_map.csv')
             if r['link_class'] == 'ACCESS' and r['src_type'] == 'HOST'}
    if len(links) != 32 or any((host, rail) not in links for host in range(16) for rail in (0, 1)):
        raise ValueError('diagnostics require all 32 physical ACCESS host-port mappings')

    def path_links(row):
        rail = int(row['rail'])
        return {links[int(row['src']), rail], links[int(row['dst']), rail]}

    def active_links(now):
        return {r['target_link_id'] for r in schedule
                if int(r['start_time_ns']) <= now < int(r['end_time_ns'])}

    impaired = sum(int(r['bytes']) for r in assigned
                   if path_links(r) & active_links(int(r['timestamp_ns'])))
    wrong_rail = sum(int(r['bytes']) for r in assigned
                    if path_links(r) & active_links(int(r['timestamp_ns']))
                    and not {links[int(r['src']), 1-int(r['rail'])],
                             links[int(r['dst']), 1-int(r['rail'])]} & active_links(int(r['timestamp_ns'])))
    finish = max(int(r['timestamp_ns']) for r in complete)
    episodes = []
    for fault in schedule:
        start, end = (int(fault[k]) for k in ('start_time_ns', 'end_time_ns'))
        before = sum(int(r['bytes']) for r in assigned if int(r['timestamp_ns']) < start)
        acked = sum(int(r['bytes']) for r in complete if int(r['timestamp_ns']) < start)
        after = [int(r['timestamp_ns']) for r in assigned
                 if int(r['timestamp_ns']) >= end and fault['target_link_id'] in path_links(r)]
        eligible = [int(r['timestamp_ns']) for r in assigned if int(r['timestamp_ns']) >= end
                    and any(fault['target_link_id'] == links[int(r[node]), rail]
                            for node in ('src', 'dst') for rail in (0, 1))]
        episodes.append({'fault_id': fault['fault_id'], 'target_link_id': fault['target_link_id'],
            'start_ns': start, 'end_ns': end, 'revert_executed_before_last_ack': end <= finish,
            'assigned_fraction_before_start': before/total, 'acked_fraction_before_start': acked/total,
            'first_post_revert_assignment_lag_ns': min(after)-end if after else None,
            'first_post_revert_eligible_assignment_lag_ns': min(eligible)-end if eligible else None})
    return {'assigned_on_currently_impaired_path_bytes': impaired,
            'assigned_on_impaired_path_with_clean_alternate_bytes': wrong_rail,
            'total_assigned_bytes': total, 'fault_episodes': episodes,
            'post_revert_lag_scope': 'first new chunk involving the reverted ACCESS link; not connectivity or training recovery time',
            'allocation_scope': 'whole chunk assigned while ACCESS fault active; includes only endpoint ACCESS truth, not queue/throughput optimality'}


def make_protocol(opt, bundle, topology_audit):
    frozen = json.loads((opt.frozen / 'summary.json').read_text())
    source_hashes = {seed: digest(opt.frozen / f'seed_{seed}' / 'faults.csv') for seed in SEEDS}
    if any(source_hashes[s] != frozen['seeds'][s]['fault_sha256'] for s in SEEDS):
        raise ValueError('source frozen fault evidence changed')
    schedules = {s: scenario_fault_bytes(opt.frozen, topology_audit, s) for s in SCENARIOS}
    return {'schema': SCHEMA, 'runtime_identity_sha256': bundle.identity_sha256,
        'input_sha256': {key: digest(getattr(opt, key)) for key in ('topology', 'workload', 'config')},
        'frozen_source_fault_sha256': source_hashes,
        'frozen_source_summary_sha256': digest(opt.frozen / 'summary.json'),
        'fault_sha256': {s: hashlib.sha256(b).hexdigest() if b is not None else None for s, b in schedules.items()},
        'targeted_hosts': {seed: targeted_hosts(seed) for seed in SEEDS},
        'target_times_rail_start_end_fraction': TARGET_TIMES,
        'algorithm_source_sha256': {name: digest(ROOT/'astra-sim-alibabacloud/astra-sim/network_frontend/ns3'/name)
            for name in ('limer_censored_model.h', 'limer_split_policy.h', 'limer_split_runtime.h')},
        'algorithm_parameters': {
            'utility_normalization_bps': 100000000000,
            'utility_normalization_load': 'ceil-power-of-two launch rail active count',
            'utility_scope': 'bounded normalized chunk completion-speed proxy, not physical capacity',
            'history_membership': 'highest 64 admitted true-first-TX assignment IDs plus any older live pending records',
            'history_recent_assignment_ids': 64, 'include_all_current_pending': True,
            'completed_history_max_age_ns': 250000,
            'completed_history_age_origin': 'actual first TX, not completion time', 'minimum_records': 8,
            'nominal_hoeffding_delta': 0.05,
            'interval_scope': 'working confidence intervals; coverage is not guaranteed under correlated adaptive nonstationary chunks',
            'confidence_correction': 'only when impaired rail upper < alternate rail lower',
            'exploration_assigned_byte_budget_fraction': 0.0625,
            'exploration_cooldown_ns': 50000, 'exploration_max_outstanding_per_source': 1,
            'exploration_requires_target_rail_no_reservation': True,
            'exploration_min_local_pending_chunks': 8,
            'exploration_min_last_ack_age_ns': 50000},
        'policy_specs': {p: {'policy': policy_spec(p)[0], 'extra_env': policy_spec(p)[1]} for p in POLICIES},
        'planned_matrix': planned_matrix(), 'main_scenarios': SCENARIOS, 'main_policies': MAIN_POLICIES,
        'chunk_bytes': 65536, 'max_active_per_source': 8, 'telemetry_interval_us': 1000,
        'horizon_ns': 100000000, 'timeout_seconds': 180, 'simulator_threads': 1,
        'expected_payload_bytes': EXPECTED_PAYLOAD,
        'primary_outcome': 'all 16 ranks WORKLOAD_COMPLETE timestamp_ns from simulation zero',
        'secondary_outcomes': ['paired workload duration change', 'allocation under active endpoint ACCESS impairment',
                               'post-revert path reuse opportunity lag', 'confidence/exploration audit counts'],
        'design': 'exploratory paired deterministic simulation; no parameter tuning after results',
        'scope': 'carrier-up service degradation; automatic physical service reversion, adaptive placement of new chunks; no GP or transport recovery',
        'tensor_numeric_correctness': 'not simulated', 'human_scientific_review': 'pending'}


def collect_result(directory, args, scenario, label, protocol_hash, original):
    policy, _ = policy_spec(label)
    verify_manifest(directory, args, policy, label)
    manifest = json.loads((directory / 'manifest.json').read_text())
    prefixes = ('LIMER_SPLIT_WAIT_', 'LIMER_SPLIT_SIGNAL_', 'LIMER_SPLIT_CENSORED_')
    actual = {k: v for k, v in manifest['env'].items() if k.startswith(prefixes)}
    if actual != args.extra_env:
        raise ValueError('unexpected adaptive-policy environment; refusing mixed parameters')
    code = int((directory / 'exit_code.txt').read_text().strip())
    result = {'pass': False, 'exit_code': code}
    if code == 0:
        try:
            result.update(audit(directory))
            if result['pass']:
                result['payload_exact'] = sum(result['acked_payload_bytes']) == EXPECTED_PAYLOAD
                result['fault_audit'] = audit_faults(directory, args.faults, result['workload_finish_ns'])
                result['diagnostics'] = fault_diagnostics(directory, args.faults)
                if scenario.startswith('standard_') and label == 'B8':
                    reference = original['seeds'][scenario.split('_')[1]]['results']['B8']['workload_finish_ns']
                    result['matches_previous_B8'] = result['workload_finish_ns'] == reference
                    result['previous_B8_finish_ns'] = reference
                result['pass'] = result['payload_exact'] and result.get('matches_previous_B8', True)
        except (ValueError, KeyError, OSError) as exc:
            result.update({'pass': False, 'reason': str(exc)})
    else:
        result['reason'] = 'simulator failed or timed out; raw run retained'
    result.update(protocol_sha256=protocol_hash, artifacts_sha256=artifact_hashes(directory))
    return result


def resume_result(directory, args, scenario, label, protocol_hash, original):
    if not (directory / 'exit_code.txt').is_file():
        raise ValueError('incomplete run preserved; use a new root: ' + str(directory))
    path = directory / 'result.json'
    if path.exists():
        prior = json.loads(path.read_text())
        if prior.get('protocol_sha256') != protocol_hash:
            raise ValueError('result protocol identity mismatch')
        if prior.get('artifacts_sha256') != artifact_hashes(directory):
            raise ValueError('raw evidence changed; refusing resume')
        current = collect_result(directory, args, scenario, label, protocol_hash, original)
        if current != prior:
            raise ValueError('derived audit changed; existing result preserved')
        return current
    result = collect_result(directory, args, scenario, label, protocol_hash, original)
    write_json(path, result)
    return result


def reaudit_existing(directory, args, scenario, label, protocol_hash, original, reason, report_root):
    """Revise derived evaluation only, keeping every previous result and raw byte."""
    if not reason.strip():
        raise ValueError('re-audit requires an explicit reason')
    result_path = directory/'result.json'
    if not result_path.is_file():
        raise ValueError('re-audit requires existing result.json; never create a missing simulation')
    before_bytes = result_path.read_bytes()
    before = json.loads(before_bytes)
    if before.get('protocol_sha256') != protocol_hash:
        raise ValueError('re-audit protocol identity mismatch')
    expected = before.get('artifacts_sha256')
    if not expected or expected != artifact_hashes(directory):
        raise ValueError('re-audit requires unchanged raw evidence hashes')
    if int((directory/'exit_code.txt').read_text().strip()) != 0:
        raise ValueError('re-audit requires simulator exit zero; failed simulation preserved')
    current = collect_result(directory, args, scenario, label, protocol_hash, original)
    if artifact_hashes(directory) != expected:
        raise ValueError('raw evidence changed during re-audit; no result replaced')
    if current == before:
        return current
    if result_path.read_bytes() != before_bytes:
        raise ValueError('result changed concurrently during re-audit')
    revisions = directory/'audit_revisions'
    revisions.mkdir(exist_ok=True)
    index = 1
    while (revisions/f'{index:04d}').exists():
        index += 1
    revision = revisions/f'{index:04d}'
    revision.mkdir(exist_ok=False)
    (revision/'result.before.json').write_bytes(before_bytes)
    write_json(revision/'result.after.json', current)
    prior_summary = report_root/'summary.json'
    summary_hash = None
    if prior_summary.exists():
        (revision/'summary.before.json').write_bytes(prior_summary.read_bytes())
        summary_hash = digest(revision/'summary.before.json')
    evaluator_names = ('run_censored_comparison.py', 'run_split_baselines.py', 'audit_censored_split.py')
    write_json(revision/'revision.json', {
        'schema': 'limer.audit-revision.v1', 'reason': reason,
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'scenario': scenario, 'policy': label, 'protocol_sha256': protocol_hash,
        'prior_result_sha256': hashlib.sha256(before_bytes).hexdigest(),
        'revised_result_sha256': digest(revision/'result.after.json'),
        'prior_summary_sha256': summary_hash,
        'prior_evaluator_sha256': 'not recorded in original result',
        'revised_evaluator_sha256': {name: digest(Path(__file__).parent/name) for name in evaluator_names},
        'unchanged_artifacts_sha256': expected,
        'simulator_reexecuted': False, 'protocol_changed': False})
    write_json(result_path, current)
    print('AUDIT_REVISION', revision, 'old_pass='+str(before.get('pass')),
          'new_pass='+str(current.get('pass')), flush=True)
    return current


def save_report(root, report):
    actual = report['scenarios']
    values = [r for scenario in actual.values() for r in scenario.values()]
    report['pass_completed_runs'] = bool(values) and all(r.get('pass') is True for r in values)
    report['main_matrix_complete'] = all(p in actual.get(s, {}) for s in SCENARIOS for p in MAIN_POLICIES)
    report['planned_matrix_complete'] = all(p in actual.get(s, {}) for s, ps in planned_matrix().items() for p in ps)
    report['same_payload'] = bool(values) and all(r.get('pass') is True and
        sum(r['acked_payload_bytes']) == EXPECTED_PAYLOAD for r in values)
    report['pass'] = report['main_matrix_complete'] and report['pass_completed_runs'] and report['same_payload']
    write_json(root / 'summary.json', report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('binary', 'topology', 'frozen', 'out'):
        parser.add_argument('--'+flag, type=Path, required=True)
    parser.add_argument('--workload', type=Path, default=ROOT/'limer/configs/microAllReduce_16rank_split_64mib.txt')
    parser.add_argument('--config', type=Path, default=ROOT/'limer/configs/SimAI.baseline.conf')
    parser.add_argument('--scenarios', nargs='+', default=['all'])
    parser.add_argument('--policies', nargs='+', choices=POLICIES, default=list(MAIN_POLICIES))
    parser.add_argument('--freeze-only', action='store_true')
    parser.add_argument('--reaudit-existing', action='store_true',
                        help='revise derived audits of existing exit-zero runs; never launch simulator')
    parser.add_argument('--reaudit-reason', help='required explicit evaluator-only revision explanation')
    opt = parser.parse_args()
    for field in ('binary', 'topology', 'frozen', 'out', 'workload', 'config'):
        setattr(opt, field, getattr(opt, field).resolve())
    try:
        selected = scenario_names(opt.scenarios)
        if len(set(opt.policies)) != len(opt.policies):
            raise ValueError('duplicate policy')
        if opt.reaudit_existing and (not (opt.reaudit_reason or '').strip() or opt.freeze_only):
            raise ValueError('re-audit requires --reaudit-reason and excludes --freeze-only')
        if opt.reaudit_reason and not opt.reaudit_existing:
            raise ValueError('--reaudit-reason requires --reaudit-existing')
        if any(p not in planned_matrix()[s] for s in selected for p in opt.policies):
            raise ValueError('requested combination is outside the predeclared matrix')
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        if opt.reaudit_existing and (not (opt.out/'protocol.json').is_file() or not (opt.out/'summary.json').is_file()):
            raise ValueError('re-audit requires existing protocol and summary; no simulations will be created')
        if opt.out.exists() and not (opt.out/'protocol.json').exists():
            raise ValueError('existing root lacks protocol; refusing to mix experiments')
        topology_audit = checked_topology(opt.topology)
        opt.out.mkdir(parents=True, exist_ok=True)
        if opt.reaudit_existing:
            prior = json.loads((opt.out/'protocol.json').read_text())
            bundle = validate_runtime_bundle(opt.out/'runtime-bundles'/prior['runtime_identity_sha256'])
            if discover_runtime_closure(opt.binary).identity_sha256 != bundle.identity_sha256:
                raise ValueError('re-audit runtime closure changed from protocol')
        else:
            bundle = seal_runtime_bundle(opt.out, opt.binary)
        protocol = json.loads(json.dumps(make_protocol(opt, bundle, topology_audit)))
        protocol_hash = canonical_hash(protocol)
        path = opt.out/'protocol.json'
        if path.exists():
            if json.loads(path.read_text()) != protocol:
                raise ValueError('protocol identity changed; use a new output root')
            if json.loads((opt.out/'topology_validation.json').read_text()) != topology_audit:
                raise ValueError('topology validation changed on resume')
        else:
            write_json(opt.out/'topology_validation.json', topology_audit)
            write_json(path, protocol)
        # Materialize every schedule before permitting the first simulation.
        for scenario in SCENARIOS:
            directory = opt.out/scenario
            content = scenario_fault_bytes(opt.frozen, topology_audit, scenario)
            if opt.reaudit_existing:
                fault_path = directory/'faults.csv'
                if (not directory.is_dir() or (content is None and fault_path.exists()) or
                        (content is not None and (not fault_path.is_file() or fault_path.read_bytes() != content))):
                    raise ValueError('re-audit scenario schedule missing or changed; no simulation will be created')
            else:
                directory.mkdir(exist_ok=True)
                ensure_fault_file(directory, content)
        if opt.freeze_only:
            print('FROZEN', protocol_hash, 'planned_runs='+str(sum(map(len, planned_matrix().values()))), flush=True)
            return 0
        report = {'schema': SCHEMA, 'protocol_sha256': protocol_hash, 'scenarios': {}}
        if (opt.out/'summary.json').exists():
            report = json.loads((opt.out/'summary.json').read_text())
            if report.get('protocol_sha256') != protocol_hash:
                raise ValueError('summary protocol identity mismatch')
        args = SimpleNamespace(binary=bundle.executable, bundle=bundle, topology=opt.topology,
            workload=opt.workload, config=opt.config, threads=1, chunk_bytes=65536,
            max_active=8, sample_us=1000, horizon_ns=100000000, timeout=180)
        original = json.loads((opt.frozen/'summary.json').read_text())
        passed = True
        for scenario in selected:
            args.out = opt.out/scenario
            args.faults = args.out/'faults.csv' if scenario != 'healthy' else None
            cells = report['scenarios'].setdefault(scenario, {})
            for label in opt.policies:
                policy, args.extra_env = policy_spec(label)
                directory = args.out/label
                reused = directory.exists()
                if opt.reaudit_existing:
                    result = reaudit_existing(directory, args, scenario, label, protocol_hash, original,
                                              opt.reaudit_reason, opt.out)
                elif reused:
                    result = resume_result(directory, args, scenario, label, protocol_hash, original)
                else:
                    run_one(args, label, policy)
                    result = collect_result(directory, args, scenario, label, protocol_hash, original)
                    write_json(directory/'result.json', result)
                cells[label] = result
                passed = passed and result['pass']
                save_report(opt.out, report)
                print(scenario, label, 'resume='+str(reused), 'pass='+str(result['pass']),
                      'finish_ns='+str(result.get('workload_finish_ns')), flush=True)
                validate_runtime_bundle(bundle.root)
        return 0 if passed else 1
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(2, 'comparison stopped with evidence preserved: '+str(exc)+'\n')


if __name__ == '__main__':
    raise SystemExit(main())
