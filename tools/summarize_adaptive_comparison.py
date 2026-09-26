#!/usr/bin/env python3
"""Descriptive paired summaries; no confidence or significance claims."""
import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path

POLICIES = ('B8', 'B9', 'B10', 'B11', 'B12')
FAMILY_SCENARIOS = {
    family: ('healthy',) if family == 'healthy' else
    tuple(f'{family}_{seed}' for seed in ('42', '43', '44'))
    for family in ('healthy', 'standard', 'early', 'recovery')
}


def validate_duration(cell, label):
    if not isinstance(cell, dict) or cell.get('pass') is not True:
        raise ValueError(f'run must explicitly pass before inclusion: {label}')
    value = cell.get('workload_finish_ns')
    # bool is an int subclass, and NaN/Infinity may be accepted by the JSON
    # parser. These are not valid timing observations; reject before arithmetic.
    valid = type(value) in (int, float)
    try:
        valid = valid and math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f'run needs finite positive completion time: {label}')


def validate_matrix(report):
    if not isinstance(report, dict) or report.get('pass') is not True:
        raise ValueError('full main matrix must pass before final summary')
    scenarios = report.get('scenarios')
    expected = {name for names in FAMILY_SCENARIOS.values() for name in names}
    if not isinstance(scenarios, dict) or set(scenarios) != expected:
        raise ValueError('summary requires the exact ten-scenario main matrix')
    for name in expected:
        cells = scenarios[name]
        if not isinstance(cells, dict) or any(p not in cells for p in POLICIES):
            raise ValueError(f'missing main policy result: {name}')
        for policy in POLICIES:
            validate_duration(cells[policy], f'{name}/{policy}')
        # Optional ablations are not part of the main matrix. Failed ablations
        # remain visible in individual raw results but are not scored. Passing
        # ablations must satisfy the same finite-positive timing contract.
        for policy, cell in cells.items():
            if not isinstance(cell, dict):
                raise ValueError(f'invalid run result: {name}/{policy}')
            if policy.startswith('B12') and policy not in POLICIES:
                if cell.get('pass') is True:
                    validate_duration(cell, f'{name}/{policy}')
                elif cell.get('pass') is not False:
                    raise ValueError(f'ablation needs explicit pass status: {name}/{policy}')
    return scenarios


def summarize(root):
    source = root / 'summary.json'
    source_bytes = source.read_bytes()
    report = json.loads(source_bytes)
    scenarios = validate_matrix(report)
    result = {'source_summary_sha256': hashlib.sha256(source_bytes).hexdigest(),
              'analysis': 'descriptive; three frozen seeds per fault family; no statistical significance test',
              'families': {}, 'ablations': {}, 'individual': {}}
    for family, names in FAMILY_SCENARIOS.items():
        family_result = {}
        for policy in POLICIES:
            values = [scenarios[s][policy]['workload_finish_ns'] for s in names]
            baseline = [scenarios[s]['B8']['workload_finish_ns'] for s in names]
            gains = [100 * (1 - v / b) for v, b in zip(values, baseline)]
            family_result[policy] = {'count': len(values), 'mean_finish_ms': statistics.mean(values) / 1e6,
                'gain_from_mean_vs_B8_pct': 100 * (1 - statistics.mean(values) / statistics.mean(baseline)),
                'mean_paired_gain_vs_B8_pct': statistics.mean(gains),
                'min_paired_gain_vs_B8_pct': min(gains), 'max_paired_gain_vs_B8_pct': max(gains)}
        result['families'][family] = family_result
    for name, values in scenarios.items():
        result['individual'][name] = {p: v.get('workload_finish_ns') for p, v in values.items()}
        if 'B11' in values:
            ref = values['B11']['workload_finish_ns']
            result['ablations'][name] = {p: {'finish_ns': v['workload_finish_ns'],
                'gain_vs_B11_pct': 100 * (1 - v['workload_finish_ns'] / ref),
                'exact_B11_parity': v['workload_finish_ns'] == ref}
                for p, v in values.items() if p.startswith('B12') and v.get('pass') is True}
    return result


def render(result):
    lines = ['# B10–B12 descriptive comparison', '',
        'All times are full 16-rank workload completion from simulation zero, in ms.',
        'Positive gain means faster. Fault-family entries average the same three frozen seeds.',
        'These are exploratory descriptive results, not statistical significance evidence.', '',
        'Caution: B12_ideal is only a zero-delivery-delay diagnostic, not an upper bound. Zero-delay callbacks changed completion time even in a no-actuation control; its gain versus B11 is not isolated signal benefit. See the separate zero-delay-shadow results.', '',
        '| Family | B8 | B9 | B10 | B11 | B12 |',
        '|---|---:|---:|---:|---:|---:|']
    for family, policies in result['families'].items():
        lines.append('| ' + family + ' | ' + ' | '.join(f"{policies[p]['mean_finish_ms']:.6f}" for p in POLICIES) + ' |')
    lines += ['', '## Gain from family mean relative to B8 (%)', '',
              '| Family | B9 | B10 | B11 | B12 |', '|---|---:|---:|---:|---:|']
    for family, policies in result['families'].items():
        lines.append('| ' + family + ' | ' + ' | '.join(f"{policies[p]['gain_from_mean_vs_B8_pct']:+.3f}" for p in POLICIES[1:]) + ' |')
    lines += ['', '## Individual runs (ms)', '', '| Scenario | B8 | B9 | B10 | B11 | B12 |', '|---|---:|---:|---:|---:|---:|']
    for scenario, values in result['individual'].items():
        lines.append('| ' + scenario + ' | ' + ' | '.join(f'{values[p]/1e6:.6f}' for p in POLICIES) + ' |')
    lines += ['', '## Signal ablations', '', '| Scenario | Policy | ms | Gain vs B11 (%) | Exact B11 parity |',
              '|---|---|---:|---:|---|']
    for scenario, values in result['ablations'].items():
        for policy, v in values.items():
            lines.append(f"| {scenario} | {policy} | {v['finish_ns']/1e6:.6f} | {v['gain_vs_B11_pct']:+.3f} | {v['exact_B11_parity']} |")
    return '\n'.join(lines) + '\n'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root)
    for name, data in (('analysis.json', json.dumps(result, indent=2, allow_nan=False) + '\n'),
                       ('comparison.md', render(result))):
        path = args.root / name
        if path.exists():
            raise ValueError('refusing to overwrite existing analysis: ' + str(path))
        path.write_text(data)
    print(render(result))
