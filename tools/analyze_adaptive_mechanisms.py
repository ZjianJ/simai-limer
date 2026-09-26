#!/usr/bin/env python3
"""Read-only descriptive mechanism diagnostics from audited adaptive-run CSVs."""
import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


def rows(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def identity(row):
    return tuple(int(row[k]) for k in ('src', 'dst', 'sport'))


def median(values):
    return statistics.median(values) if values else None


def strict_choice(costs, relative_tolerance=1e-4):
    """Return None for near ties; cost units cancel in the relative margin."""
    if not all(math.isfinite(x) and x > 0 for x in costs):
        raise ValueError('strict choice needs two finite positive costs')
    if abs(costs[0] - costs[1]) / max(costs) <= relative_tolerance:
        return None
    return 0 if costs[0] < costs[1] else 1


def decision_metrics(decisions, events, feedback, label):
    assigned, completed = {}, {}
    for row in events:
        target = assigned if row['event'] == 'ASSIGN' else (
            completed if row['event'] == 'ACK_COMPLETE' else None)
        if target is None:
            continue
        key = identity(row)
        if key in target:
            raise ValueError('duplicate chunk event identity')
        target[key] = int(row['timestamp_ns'])
    elapsed = [int(row['elapsed_ns']) / 1000 for row in feedback]
    result = {'chunk_count': len(elapsed), 'chunk_elapsed_median_us': median(elapsed)}
    if label not in ('B11', 'B12') and not label.startswith('B12_'):
        return result
    source_count = Counter()
    ratios = []
    signal = Counter()
    choices = Counter()
    for row in sorted(decisions, key=lambda r: (int(r['timestamp_ns']), int(r.get('audit_seq', 0)))):
        src = int(row['src'])
        source_count[src] += 1
        if source_count[src] <= 2:
            signal['excluded_initial_probes'] += 1
            continue
        key = identity(row)
        if key not in assigned or key not in completed:
            raise ValueError('decision lacks complete chunk event pair')
        if int(row['timestamp_ns']) != assigned[key]:
            raise ValueError('decision/assignment timestamp mismatch')
        actual_ns = completed[key] - assigned[key]
        if actual_ns <= 0:
            raise ValueError('nonpositive ASSIGN-to-ACK duration')
        rail = int(row['rail'])
        if rail not in (0, 1):
            raise ValueError('invalid decision rail')
        rates = [float(row['rate_' + r]) for r in ('A', 'B')]
        amounts = [int(row['reserved_' + r]) + int(row['bytes']) for r in ('A', 'B')]
        if rates[rail] <= 0 or not math.isfinite(rates[rail]):
            raise ValueError('chosen non-probe rail has no finite positive rate')
        ratios.append(amounts[rail] * 8e9 / rates[rail] / actual_ns)
        choices[str(rail)] += 1
        if not label.startswith('B12'):
            continue
        if not any(float(row['signal_' + r]) < 1 for r in ('A', 'B')):
            continue
        signal['signal_limited_decisions'] += 1
        no_signal_rates = [float(row['raw_' + r]) * float(row['wait_' + r]) for r in ('A', 'B')]
        if not all(math.isfinite(v) and v > 0 for v in rates + no_signal_rates):
            signal['excluded_nonpositive_or_nonfinite_rates'] += 1
            continue
        actual_choice = strict_choice([amounts[r] / rates[r] for r in (0, 1)])
        alternative = strict_choice([amounts[r] / no_signal_rates[r] for r in (0, 1)])
        if actual_choice is None or alternative is None:
            signal['ambiguous_near_tie'] += 1
            continue
        signal['strict_comparable_decisions'] += 1
        if actual_choice != rail:
            raise ValueError('logged rail disagrees with strict positive-rate cost choice')
        if alternative != rail:
            signal['strict_rail_flips'] += 1
        else:
            signal['strict_unchanged_decisions'] += 1
    result.update(predicted_cost_over_assign_ack_median=median(ratios),
                  predicted_cost_ratio_count=len(ratios),
                  nonprobe_rail_choices={r: choices[r] for r in ('0', '1')})
    if label.startswith('B12'):
        fields = ('excluded_initial_probes', 'signal_limited_decisions',
                  'excluded_nonpositive_or_nonfinite_rates', 'ambiguous_near_tie',
                  'strict_comparable_decisions', 'strict_rail_flips', 'strict_unchanged_decisions')
        result['same_state_no_signal_counterfactual'] = {k: signal[k] for k in fields}
    return result


def fault_intervals(link_map, faults):
    ports = {}
    for link in link_map:
        if link['link_class'] != 'ACCESS':
            continue
        if link['src_type'] == 'SWITCH' and link['dst_type'] == 'HOST':
            key = int(link['src_node']), int(link['src_port'])
        elif link['dst_type'] == 'SWITCH' and link['src_type'] == 'HOST':
            key = int(link['dst_node']), int(link['dst_port'])
        else:
            raise ValueError('ACCESS link lacks a switch-host endpoint pair')
        ports[link['link_id']] = key
    intervals = defaultdict(list)
    for fault in faults:
        if fault['target_link_id'] not in ports:
            raise ValueError('fault lacks monitored ACCESS port mapping')
        if int(fault.get('recovery_delay_ns', 0)):
            raise ValueError('nonzero recovery delay needs actual-transition labeling')
        start, end = int(fault['start_time_ns']), int(fault['end_time_ns'])
        if end <= start:
            raise ValueError('invalid fault interval')
        intervals[ports[fault['target_link_id']]].append((start, end))
    return intervals


def fault_phase(intervals, when):
    if not intervals:
        return 'untargeted_port'
    if any(start <= when < end for start, end in intervals):
        return 'active_target_fault'
    if when < min(start for start, _ in intervals):
        return 'before_target_fault'
    if when >= max(end for _, end in intervals):
        return 'after_target_fault'
    return 'between_target_faults'


def switch_metrics(samples, link_map, faults):
    intervals = fault_intervals(link_map, faults)
    previous = {}
    last_time = {}
    counts = Counter()
    limited_phases = Counter()
    all_phases = Counter()
    for row in sorted(samples, key=lambda r: (int(r['timestamp_ns']), int(r.get('audit_seq', 0)))):
        port = int(row['node']), int(row['port'])
        when = int(row['timestamp_ns'])
        if port in last_time and when <= last_time[port]:
            raise ValueError('duplicate/noncausal port sample')
        last_time[port] = when
        phase = fault_phase(intervals.get(port, ()), when)
        limited = float(row['factor']) < 1
        all_phases[phase] += 1
        counts['samples'] += 1
        if limited:
            counts['limited_samples'] += 1
            limited_phases[phase] += 1
        if limited and not previous.get(port, False):
            counts['limited_entries'] += 1
        if not limited and previous.get(port, False):
            counts['limited_exits'] += 1
            if not int(row['demand']):
                counts['limited_exits_without_demand'] += 1
        previous[port] = limited
    fields = ('samples', 'limited_samples', 'limited_entries', 'limited_exits', 'limited_exits_without_demand')
    result = {k: counts[k] for k in fields}
    result.update(limited_samples_during_target_fault=limited_phases['active_target_fault'],
                  limited_samples_outside_target_fault=counts['limited_samples'] - limited_phases['active_target_fault'],
                  remaining_limited_ports=sum(previous.values()),
                  limited_samples_by_phase=dict(limited_phases), samples_by_phase=dict(all_phases))
    return result


def analyze_run(directory, faults, label):
    result = decision_metrics(rows(directory / 'adaptive_decisions.csv')
                              if (directory / 'adaptive_decisions.csv').exists() else [],
                              rows(directory / 'split_events.csv'), rows(directory / 'chunk_feedback.csv'), label)
    if (directory / 'switch_signal_samples.csv').exists():
        result['switch_samples'] = switch_metrics(rows(directory / 'switch_signal_samples.csv'),
                                                  rows(directory / 'link_map.csv'), faults)
    return result


def analyze_root(root):
    source_bytes = (root / 'summary.json').read_bytes()
    report = json.loads(source_bytes)
    result = {
        'source_summary_sha256': hashlib.sha256(source_bytes).hexdigest(),
        'definitions': {
            'cost_ratio': '(selected rail reserved bytes + new bytes) * 8e9 / used_bps / (ACK_ns - ASSIGN_ns); exclude first two decisions per source',
            'counterfactual': 'same-state counterfactual using raw_bps * wait_factor; not a full alternate run or causal completion-time benefit',
            'near_tie': 'either choice has abs(cost_A-cost_B)/max(cost_A,cost_B) <= 1e-4',
            'fault_labels': 'offline only: switch destination ACCESS port and start <= sample < end; other faults/path congestion may affect outside-target samples',
            'transitions': 'per-port limited factor < 1 entries/exits; initial state treated as unlimited',
        },
        'runs': {},
    }
    for scenario, runs in report['scenarios'].items():
        fault_path = root / scenario / 'faults.csv'
        if scenario != 'healthy' and not fault_path.exists():
            raise ValueError('fault scenario lacks offline schedule: ' + scenario)
        faults = rows(fault_path) if fault_path.exists() else []
        result['runs'][scenario] = {}
        for label, audit in runs.items():
            result['runs'][scenario][label] = (
                analyze_run(root / scenario / label, faults, label)
                if audit.get('pass') is True else {'skipped': 'run did not pass its audit'})
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    output = json.dumps(analyze_root(args.root), indent=2, allow_nan=False) + '\n'
    if args.out:
        with args.out.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')
