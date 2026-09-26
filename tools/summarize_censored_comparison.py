#!/usr/bin/env python3
"""Descriptive paired results for the prespecified 90-run censored experiment."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path

from run_adaptive_comparison import artifact_hashes, canonical_hash
from run_censored_comparison import FAMILIES, MAIN_POLICIES, SCENARIOS, SEEDS, planned_matrix
from run_split_baselines import rows
from summarize_adaptive_comparison import validate_duration


def compare(values, references):
    if len(values) != len(references) or not values:
        raise ValueError('paired comparison requires equal nonempty groups')
    gains = [100*(1-v/b) for v, b in zip(values, references)]
    return {'count': len(values), 'mean_finish_ms': statistics.mean(values)/1e6,
            'gain_from_mean_pct': 100*(1-statistics.mean(values)/statistics.mean(references)),
            'mean_paired_gain_pct': statistics.mean(gains),
            'min_paired_gain_pct': min(gains), 'max_paired_gain_pct': max(gains),
            'faster_count': sum(v < b for v, b in zip(values, references)),
            'equal_count': sum(v == b for v, b in zip(values, references))}


def validate_report(root):
    protocol = json.loads((root/'protocol.json').read_text())
    source = (root/'summary.json').read_bytes()
    report = json.loads(source)
    if report.get('pass') is not True or report.get('planned_matrix_complete') is not True:
        raise ValueError('all 90 prespecified runs must finish and pass before final summary')
    if report.get('protocol_sha256') != canonical_hash(protocol):
        raise ValueError('summary/protocol identity mismatch')
    if protocol.get('planned_matrix') != planned_matrix():
        raise ValueError('protocol matrix differs from prespecified experiment')
    cells = report['scenarios']
    if set(cells) != set(SCENARIOS):
        raise ValueError('unexpected or missing scenario')
    for scenario, policies in planned_matrix().items():
        if set(cells[scenario]) != set(policies):
            raise ValueError('unexpected or missing policy in '+scenario)
        for policy in policies:
            cell = cells[scenario][policy]
            validate_duration(cell, f'{scenario}/{policy}')
            if cell.get('protocol_sha256') != report['protocol_sha256']:
                raise ValueError('run protocol identity mismatch')
            if cell.get('artifacts_sha256') != artifact_hashes(root/scenario/policy):
                raise ValueError('raw artifact changed after audit: '+scenario+'/'+policy)
    return cells, hashlib.sha256(source).hexdigest()


def assignment_trace(directory):
    """Global log order and canonical chunk-keyed assignment field identities.

    Independent source LP callbacks can interleave differently in a global log
    without changing any chunk's time, selected rail, bytes or other fields.
    Retain both metrics; only the global hash must depend on interleaving.
    """
    data = [{key: value for key, value in row.items() if key != 'policy'}
            for row in rows(directory/'split_events.csv') if row['event'] == 'ASSIGN']
    keyed = {}
    for row in data:
        key = tuple(int(row[field]) for field in ('src', 'dst', 'sport'))
        if key in keyed:
            raise ValueError('canonical assignment comparison requires unique chunk keys')
        keyed[key] = row
    return {'count': len(data), 'sha256': canonical_hash(data),
            'keyed_sha256': canonical_hash([keyed[key] for key in sorted(keyed)])}


def policy_mechanisms(directory):
    decisions = rows(directory/'censored_decisions.csv')
    intervals = rows(directory/'censored_intervals.csv')
    total = sum(int(d['bytes']) for d in decisions)
    probes = [d for d in decisions if int(d['probe'])]
    return {'decisions': len(decisions), 'probe_chunks': len(probes),
            'probe_bytes': sum(int(d['bytes']) for d in probes),
            'probe_fraction_total_bytes': sum(int(d['bytes']) for d in probes)/total,
            'probe_changed_base_rail_count': sum(d['rail'] != d['base_rail'] for d in probes),
            'assignments_reading_nonunit_risk_factor': sum(float(d['factor_A']) < 1 or float(d['factor_B']) < 1 for d in decisions),
            'interval_snapshots': len(intervals),
            'interval_snapshots_with_pending': sum(int(d['pending']) > 0 for d in intervals),
            'ready_interval_snapshots': sum(int(d['ready']) != 0 for d in intervals)}


def summarize(root):
    cells, source_hash = validate_report(root)
    families = ('healthy',) + FAMILIES
    result = {'source_summary_sha256': source_hash, 'run_count': sum(map(len, cells.values())),
        'analysis': 'exploratory descriptive paired simulation; three frozen host selections/fault seeds, one healthy case; no significance or general coverage claims',
        'families': {}, 'individual': {}, 'ablations': {}, 'ablation_families': {},
        'diagnostics': {}, 'mechanisms': {}, 'assignment_traces': {},
        'scope': 'reallocation of new chunks during carrier-up degradation/reversion; no failed in-flight chunk migration, hard-link recovery or tensor correctness validation'}
    for family in families:
        names = ('healthy',) if family == 'healthy' else tuple(f'{family}_{seed}' for seed in SEEDS)
        group = {}
        for policy in MAIN_POLICIES:
            values = [cells[s][policy]['workload_finish_ns'] for s in names]
            group[policy] = {'mean_finish_ms': statistics.mean(values)/1e6,
                'vs': {ref: compare(values, [cells[s][ref]['workload_finish_ns'] for s in names])
                       for ref in MAIN_POLICIES if ref != policy}}
        result['families'][family] = group
        diagnostic = {}
        for policy in MAIN_POLICIES:
            data = [cells[s][policy]['diagnostics'] for s in names]
            diagnostic[policy] = {
                'mean_assigned_on_impaired_path_bytes': statistics.mean(d['assigned_on_currently_impaired_path_bytes'] for d in data),
                'mean_impaired_assignment_with_clean_alternate_bytes': statistics.mean(d['assigned_on_impaired_path_with_clean_alternate_bytes'] for d in data)}
        result['diagnostics'][family] = diagnostic
        if all('B14_completed_only' in cells[s] for s in names):
            result['ablation_families'][family] = {'include_pending': compare(
                [cells[s]['B14']['workload_finish_ns'] for s in names],
                [cells[s]['B14_completed_only']['workload_finish_ns'] for s in names])}
    for name, policies in cells.items():
        result['individual'][name] = {p: v['workload_finish_ns'] for p, v in policies.items()}
        result['assignment_traces'][name] = {p: assignment_trace(root/name/p) for p in policies}
        result['mechanisms'][name] = {p: policy_mechanisms(root/name/p) for p in policies if p.startswith(('B13', 'B14'))}
        ablation = {}
        if 'B14_completed_only' in policies:
            baseline = policies['B14_completed_only']['workload_finish_ns']
            ablation['include_pending'] = compare([policies['B14']['workload_finish_ns']], [baseline])
        if 'B14_shadow' in policies:
            shadow = policies['B14_shadow']['workload_finish_ns']
            baseline = policies['B8']['workload_finish_ns']
            shadow_trace = result['assignment_traces'][name]['B14_shadow']
            baseline_trace = result['assignment_traces'][name]['B8']
            ablation['shadow'] = {'finish_ns': shadow, 'B8_finish_ns': baseline,
                                  'exact_B8_parity': shadow == baseline,
                                  'exact_B8_assignment_trace_parity': shadow_trace['count'] == baseline_trace['count'] and shadow_trace['sha256'] == baseline_trace['sha256'],
                                  'exact_B8_keyed_assignment_parity': shadow_trace['count'] == baseline_trace['count'] and shadow_trace['keyed_sha256'] == baseline_trace['keyed_sha256'],
                                  'time_change_vs_B8_pct': 100*(shadow/baseline-1)}
            ablation['shadow']['exact_B8_behavioral_parity'] = (
                ablation['shadow']['exact_B8_parity'] and
                ablation['shadow']['exact_B8_keyed_assignment_parity'])
        if ablation:
            result['ablations'][name] = ablation
    shadows = [d['shadow'] for d in result['ablations'].values() if 'shadow' in d]
    result['all_shadow_controls_exact_B8_parity'] = len(shadows) == 4 and all(d['exact_B8_parity'] for d in shadows)
    result['all_shadow_controls_exact_B8_assignment_trace_parity'] = len(shadows) == 4 and all(d['exact_B8_assignment_trace_parity'] for d in shadows)
    result['all_shadow_controls_exact_B8_keyed_assignment_parity'] = len(shadows) == 4 and all(d['exact_B8_keyed_assignment_parity'] for d in shadows)
    result['all_shadow_controls_exact_B8_behavioral_parity'] = len(shadows) == 4 and all(d['exact_B8_behavioral_parity'] for d in shadows)
    result['shadow_parity_scope'] = 'exact_B8_parity means workload finish time; keyed_assignment_parity means every canonical chunk-keyed assignment field; behavioral_parity requires both. Global-order assignment parity is a separate LP-interleaving diagnostic.'
    result['assignment_trace_scope'] = 'global-order and canonical (src,dst,sport)-keyed ASSIGN rows, every split_events.csv field except policy label; includes timestamp, selected rail, bytes, logical intervals, rate and reservation. Independent source-LP interleaving alone changes global order, not keyed behavior.'
    return result


def render(result):
    lines = ['# Censored feedback and bounded exploration: descriptive results', '',
        'DRAFT: automated evidence checks complete; accountable human scientific review pending.', '',
        'All durations are 16-rank workload completion from simulation zero (ms). Positive gain means faster.',
        'Fault-family means pair the same three seeded schedules. Healthy is one deterministic case.',
        'Working confidence intervals do not have demonstrated coverage in this adaptive nonstationary simulator.',
        'No GP, hard-link/QP failover, in-flight migration, numerical tensor correctness, or <1s restoration claim.', '',
        '| Scenario family | B8 | B10 | B13: censored CI | B14: CI + bounded UCB |',
        '|---|---:|---:|---:|---:|']
    for family, policies in result['families'].items():
        lines.append('| '+family+' | '+' | '.join(f"{policies[p]['mean_finish_ms']:.6f}" for p in MAIN_POLICIES)+' |')
    lines += ['', '## B14 paired gains (%)', '',
              '| Family | vs B8 (ratio of means) | vs B10 | vs B13 | B14 wins vs B13 | vs B13 min–max paired gain |',
              '|---|---:|---:|---:|---:|---:|']
    for family, policies in result['families'].items():
        comparisons = policies['B14']['vs']
        ci = comparisons['B13']
        lines.append(f"| {family} | {comparisons['B8']['gain_from_mean_pct']:+.3f} | {comparisons['B10']['gain_from_mean_pct']:+.3f} | {ci['gain_from_mean_pct']:+.3f} | {ci['faster_count']}/{ci['count']} | {ci['min_paired_gain_pct']:+.3f} to {ci['max_paired_gain_pct']:+.3f} |")
    lines += ['', '## Individual durations (ms)', '', '| Scenario | B8 | B10 | B13 | B14 |', '|---|---:|---:|---:|---:|']
    for scenario, data in result['individual'].items():
        lines.append('| '+scenario+' | '+' | '.join(f'{data[p]/1e6:.6f}' for p in MAIN_POLICIES)+' |')
    lines += ['', '## Pending-information ablation', '',
              'Positive gain: B14 with unfinished intervals is faster than the same exploration policy using completed observations only.', '',
              '| Scenario | B14 gain vs completed-only (%) |', '|---|---:|']
    for scenario, data in result['ablations'].items():
        if 'include_pending' in data:
            lines.append(f"| {scenario} | {data['include_pending']['gain_from_mean_pct']:+.3f} |")
    lines += ['', 'Family-paired pending-information gains:', '', '| Family | B14 gain vs completed-only (%) | Wins |', '|---|---:|---:|']
    for family, data in result['ablation_families'].items():
        d = data['include_pending']
        lines.append(f"| {family} | {d['gain_from_mean_pct']:+.3f} | {d['faster_count']}/{d['count']} |")
    lines += ['', '## No-actuation controls', '', '| Scenario | B14 shadow (ms) | B8 (ms) | Exact time parity | Global-order assignment parity | Canonical chunk-keyed assignment parity |', '|---|---:|---:|---|---|---|']
    for scenario, data in result['ablations'].items():
        if 'shadow' in data:
            d = data['shadow']
            lines.append(f"| {scenario} | {d['finish_ns']/1e6:.6f} | {d['B8_finish_ns']/1e6:.6f} | {d['exact_B8_parity']} | {d['exact_B8_assignment_trace_parity']} | {d['exact_B8_keyed_assignment_parity']} |")
    lines += ['', '## Policy activity', '', '| Scenario | B13 corrected assignments | B14 corrected assignments | B14 exploration chunks |', '|---|---:|---:|---:|']
    for scenario, policies in result['mechanisms'].items():
        lines.append(f"| {scenario} | {policies['B13']['assignments_reading_nonunit_risk_factor']} | {policies['B14']['assignments_reading_nonunit_risk_factor']} | {policies['B14']['probe_chunks']} |")
    lines += ['', '## Diagnostic limitations', '',
        'Impaired-path assignment bytes are whole chunks assigned while an endpoint ACCESS fault is active. They do not measure lost bytes or prove an optimal allocation.',
        'Post-revert reuse lag is the first new assignment involving the recovered link, not recovery completion. Fault reversion is performed by the injector, not the algorithm.',
        'A negative result, null exploration increment, or shadow mismatch in completion time or canonical chunk-keyed fields must be retained and interpreted. A global-order difference alone is not behavioral event sensitivity when independently interleaved source LPs preserve every keyed chunk field.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root)
    outputs = {'analysis.json': json.dumps(result, indent=2, allow_nan=False)+'\n', 'comparison.md': render(result)}
    if any((args.root/name).exists() for name in outputs):
        raise ValueError('existing analysis is preserved; use a new analysis path')
    for name, text in outputs.items():
        (args.root/name).write_text(text)
    print(render(result))


if __name__ == '__main__':
    main()
