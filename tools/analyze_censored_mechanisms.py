#!/usr/bin/env python3
"""Offline, read-only mechanism attribution for audited censored/UCB experiments.

No scheduler reads the fault labels produced here. Partial matrices require an
explicit --allow-incomplete flag. Output files use exclusive creation.
"""
import argparse
import bisect
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


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True,
                                    separators=(',', ':')).encode()).hexdigest()


def identity(row):
    return tuple(int(row[k]) for k in ('src', 'dst', 'sport'))


def distribution(values):
    return {'count': len(values), 'mean_ns': statistics.mean(values) if values else None,
            'median_ns': statistics.median(values) if values else None,
            'min_ns': min(values) if values else None,
            'max_ns': max(values) if values else None,
            'sum_ns': sum(values)}


def access_links(link_map):
    result = {}
    for row in link_map:
        if row['link_class'] != 'ACCESS':
            continue
        if row['src_type'] == 'HOST' and row['dst_type'] == 'SWITCH':
            host, port = int(row['src_node']), int(row['src_port'])
        elif row['dst_type'] == 'HOST' and row['src_type'] == 'SWITCH':
            host, port = int(row['dst_node']), int(row['dst_port'])
        else:
            raise ValueError('ACCESS link must connect host and switch')
        key = host, port - 2
        if key[1] not in (0, 1) or key in result:
            raise ValueError('invalid/duplicate ACCESS host rail')
        result[key] = row['link_id']
    return result


def fault_index(faults):
    result = defaultdict(list)
    for row in faults:
        start, end = int(row['start_time_ns']), int(row['end_time_ns'])
        fraction = float(row['parameter_after'])
        if row['fault_type'] != 'service_degradation' or int(row['recovery_delay_ns']):
            raise ValueError('mechanism labels require zero-delay service degradation')
        if end <= start or not math.isfinite(fraction) or not 0 < fraction < 1:
            raise ValueError('invalid degradation interval/fraction')
        result[row['target_link_id']].append((start, end, fraction))
    return result


def path_label(row, links, faults):
    """Half-open scheduled-fault labels, not an oracle available online."""
    src, dst, rail, now = (int(row[k]) for k in ('src', 'dst', 'rail', 'timestamp_ns'))
    selected = {links[src, rail], links[dst, rail]}
    alternate = {links[src, 1 - rail], links[dst, 1 - rail]}
    intervals = [interval for link in selected for interval in faults.get(link, ())]
    active = [item for item in intervals if item[0] <= now < item[1]]
    active_other = [item for link in alternate for item in faults.get(link, ())
                    if item[0] <= now < item[1]]
    reverted = [end for _, end, _ in intervals if end <= now]
    if active:
        phase = 'scheduled_impaired_access'
    elif reverted:
        phase = 'clean_access_after_reversion'
    elif intervals:
        phase = 'clean_access_before_fault'
    else:
        phase = 'untargeted_clean_access'
    return {'phase': phase, 'selected_access_links': sorted(selected),
            'selected_min_scheduled_fraction': min((v for _, _, v in active), default=1.0),
            'alternate_scheduled_impaired': bool(active_other),
            'avoidable_impaired_assignment': bool(active) and not active_other,
            'lag_since_latest_selected_access_reversion_ns': now - max(reverted)
                if reverted and not active else None}


def assignment_signature(events):
    fields = ('timestamp_ns', 'src', 'dst', 'sport', 'rail', 'bytes',
              'logical_flow_id', 'chunk_offset', 'flow_bytes')
    records = [tuple(int(row[k]) for k in fields)
               for row in events if row['event'] == 'ASSIGN']
    return {'count': len(records), 'ordered_sha256': canonical_hash(records),
            'content_sha256': canonical_hash(sorted(records))}


def run_metrics(events, feedback, decisions, link_map, faults):
    assigned, completed, measured = {}, {}, {}
    for row in events:
        target = assigned if row['event'] == 'ASSIGN' else (
            completed if row['event'] == 'ACK_COMPLETE' else None)
        if target is not None:
            key = identity(row)
            if key in target:
                raise ValueError('duplicate split event identity')
            target[key] = row
    for row in feedback:
        key = identity(row)
        if key in measured:
            raise ValueError('duplicate measured chunk identity')
        measured[key] = row
    if set(assigned) != set(completed) or set(assigned) != set(measured):
        raise ValueError('incomplete assignment/ACK/feedback identity coverage')
    if not assigned:
        raise ValueError('empty completed run')
    for key, row in assigned.items():
        ack, measurement = completed[key], measured[key]
        if int(ack['timestamp_ns']) <= int(row['timestamp_ns']):
            raise ValueError('nonpositive assignment-to-ACK interval')
        if int(measurement['elapsed_ns']) <= 0:
            raise ValueError('nonpositive measured interval')
        for field in ('bytes', 'rail'):
            if int(row[field]) != int(ack[field]) or int(row[field]) != int(measurement[field]):
                raise ValueError('assignment/ACK/feedback payload or rail differs')
    links = access_links(link_map)
    indexed_faults = fault_index(faults)
    if set(indexed_faults) - set(links.values()):
        raise ValueError('fault lacks ACCESS link mapping')
    phases, phase_bytes, rail_bytes = Counter(), Counter(), Counter()
    avoidable_bytes = 0
    for row in assigned.values():
        label = path_label(row, links, indexed_faults)
        phases[label['phase']] += 1
        phase_bytes[label['phase']] += int(row['bytes'])
        rail_bytes[str(row['rail'])] += int(row['bytes'])
        if label['avoidable_impaired_assignment']:
            avoidable_bytes += int(row['bytes'])
    total_bytes = sum(rail_bytes.values())
    result = {'chunks': len(assigned), 'assigned_bytes': total_bytes,
              'rail_assigned_bytes': {str(r): rail_bytes[str(r)] for r in (0, 1)},
              'assignment_signature': assignment_signature(events),
              'assignments_by_path_phase': dict(phases),
              'assigned_bytes_by_path_phase': dict(phase_bytes),
              'avoidable_scheduled_impaired_assignment_bytes': avoidable_bytes,
              'all_chunk_first_tx_ack': distribution([int(r['elapsed_ns']) for r in feedback]),
              'censored_decisions_present': decisions is not None}
    if decisions is None:
        return result
    if len(decisions) != len(assigned) or {identity(r) for r in decisions} != set(assigned):
        raise ValueError('censored decisions do not exactly cover assignments')
    corrected, chosen_corrected, corrected_bytes = 0, 0, 0
    noninitial, probes = 0, []
    probe_phases, probe_phase_bytes = Counter(), Counter()
    ordinary_durations = []
    source_probes, source_bytes = Counter(), Counter()
    guard_pass, guard_fail, first_failure = Counter(), Counter(), Counter()
    eligible_guard_rows = 0
    prior_feedback = defaultdict(list)
    for row in feedback:
        prior_feedback[int(row['src']), int(row['rail'])].append((int(row['audit_seq']), row))
    feedback_sequences = {}
    for key, values in prior_feedback.items():
        values.sort(key=lambda item: item[0])
        feedback_sequences[key] = [item[0] for item in values]
    for row in decisions:
        key = identity(row)
        assignment = assigned[key]
        for field in ('timestamp_ns', 'bytes', 'rail'):
            if int(row[field]) != int(assignment[field]):
                raise ValueError('censored decision differs from assignment')
        src, rail, amount = int(row['src']), int(row['rail']), int(row['bytes'])
        source_bytes[src] += amount
        if int(row['base_rail']) >= 0:
            noninitial += 1
            factors = [float(row['factor_' + r]) for r in ('A', 'B')]
            if any(not math.isfinite(v) or not .1 <= v <= 1 for v in factors):
                raise ValueError('invalid confidence correction factor')
            if min(factors) < 1:
                corrected += 1
                corrected_bytes += amount
            if factors[rail] < 1:
                chosen_corrected += 1
            base = int(row['base_rail'])
            other_name, base_name = ('A', 'B')[1 - base], ('A', 'B')[base]
            now = int(row['timestamp_ns'])
            guards = {
                'no_source_probe_outstanding': int(row['probe_inflight']) == 0,
                'queued_at_least_8': int(row['queued']) >= 8,
                'target_idle': int(row['reserved_' + other_name]) == 0,
                'source_probe_cooldown_50us': now >= int(row['last_probe_ns']) + 50000,
                'target_last_ack_stale_50us': now >= int(row['last_ack_' + other_name]) + 50000,
                'cumulative_one_sixteenth_byte_budget':
                    (int(row['probe_bytes']) + amount) * 16 <= int(row['total_bytes']) + amount,
                'target_ucb_at_least_incumbent_lcb':
                    float(row['upper_' + other_name]) >= float(row['lower_' + base_name]),
            }
            failed = []
            for name, passed in guards.items():
                (guard_pass if passed else guard_fail)[name] += 1
                if not passed:
                    failed.append(name)
            if failed:
                first_failure[failed[0]] += 1
            else:
                eligible_guard_rows += 1
        elapsed = int(measured[key]['elapsed_ns'])
        if int(row['probe']) not in (0, 1):
            raise ValueError('invalid exploration marker')
        if not int(row['probe']):
            ordinary_durations.append(elapsed)
            continue
        if int(row['base_rail']) < 0 or rail == int(row['base_rail']):
            raise ValueError('exploration must override a normal credit decision')
        label = path_label(row, links, indexed_faults)
        probe_phases[label['phase']] += 1
        probe_phase_bytes[label['phase']] += amount
        source_probes[src] += amount
        previous_index = bisect.bisect_left(feedback_sequences.get((src, rail), []), int(row['audit_seq'])) - 1
        previous = prior_feedback[src, rail][previous_index][1] if previous_index >= 0 else None
        raw_rates = [float(row['raw_' + r]) for r in ('A', 'B')]
        probes.append({'src': src, 'dst': int(row['dst']), 'sport': int(row['sport']),
                       'rail': rail, 'bytes': amount,
                       'assigned_ns': int(row['timestamp_ns']),
                       'first_tx_ack_ns': elapsed,
                       'assign_ack_ns': int(completed[key]['timestamp_ns']) - int(row['timestamp_ns']),
                       'both_rails_idle_at_assignment': not int(row['reserved_A']) and not int(row['reserved_B']),
                       'local_queued_chunks': int(row['queued']),
                       'raw_A_bps': raw_rates[0], 'raw_B_bps': raw_rates[1],
                       'selected_raw_rate_at_least_alternate': raw_rates[rail] >= raw_rates[1 - rail],
                       'previous_same_rail_feedback_dst': int(previous['dst']) if previous else None,
                       'previous_same_rail_feedback_same_dst': int(previous['dst']) == int(row['dst']) if previous else None,
                       'previous_same_rail_ack_age_ns': int(row['timestamp_ns']) - int(previous['timestamp_ns']) if previous else None,
                       **label})
    probe_bytes = sum(p['bytes'] for p in probes)
    result['confidence'] = {
        'noninitial_decisions': noninitial,
        'any_rail_corrected_decisions': corrected,
        'any_rail_corrected_fraction': corrected / noninitial if noninitial else 0.0,
        'chosen_rail_corrected_decisions': chosen_corrected,
        'bytes_assigned_at_any_rail_corrected_decision': corrected_bytes}
    result['exploration'] = {
        'probe_chunks': len(probes), 'probe_bytes': probe_bytes,
        'probe_assigned_byte_fraction': probe_bytes / total_bytes,
        'probe_by_path_phase': dict(probe_phases),
        'probe_bytes_by_path_phase': dict(probe_phase_bytes),
        'probe_first_tx_ack': distribution([p['first_tx_ack_ns'] for p in probes]),
        'ordinary_first_tx_ack': distribution(ordinary_durations),
        'per_source_probe_byte_fraction': {str(src): source_probes[src] / amount
                                          for src, amount in sorted(source_bytes.items())},
        'guard_pass_counts': dict(guard_pass), 'guard_fail_counts': dict(guard_fail),
        'first_failed_guard_counts': dict(first_failure),
        'all_logged_guard_conditions_met': eligible_guard_rows,
        'both_rails_idle_probe_chunks': sum(p['both_rails_idle_at_assignment'] for p in probes),
        'selected_raw_not_lower_probe_chunks': sum(p['selected_raw_rate_at_least_alternate'] for p in probes),
        'previous_same_destination_probe_chunks': sum(p['previous_same_rail_feedback_same_dst'] is True for p in probes),
        'probes': probes}
    return result


def comparison(reference, candidate):
    if not reference or set(reference) != set(candidate):
        raise ValueError('paired comparison needs the same nonempty scenarios')
    for value in list(reference.values()) + list(candidate.values()):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError('comparison time must be finite and positive')
    pairs = {s: (reference[s] - candidate[s]) / reference[s] * 100 for s in sorted(reference)}
    ref_mean = statistics.mean(reference.values())
    test_mean = statistics.mean(candidate.values())
    return {'n': len(pairs), 'reference_mean_ns': ref_mean, 'candidate_mean_ns': test_mean,
            'gain_percent_ratio_of_means': (ref_mean - test_mean) / ref_mean * 100,
            'gain_percent_mean_of_pairs': statistics.mean(pairs.values()),
            'paired_gain_percent': pairs,
            'candidate_wins': sum(candidate[s] < reference[s] for s in pairs),
            'candidate_losses': sum(candidate[s] > reference[s] for s in pairs),
            'ties': sum(candidate[s] == reference[s] for s in pairs)}


def numeric_summary(values):
    return {'n': len(values), 'mean': statistics.mean(values) if values else None,
            'median': statistics.median(values) if values else None,
            'min': min(values) if values else None, 'max': max(values) if values else None}


class _Membership:
    """Diagnostic history-membership replay only; CI arithmetic is not replayed."""
    def __init__(self):
        self.records = defaultdict(dict)
        self.recent = defaultdict(set)

    def observe(self, row):
        key = int(row['src']), int(row['rail'])
        identity = int(row['id'])
        recent, records = self.recent[key], self.records[key]
        completed = int(row['completed']) != 0
        if identity in records:
            previous = records[identity]
            if any(int(previous[k]) != int(row[k]) for k in ('dst', 'bytes', 'launch_active', 'first_tx_ns')):
                raise ValueError('diagnostic history immutable metadata differs')
            if completed:
                records[identity] = row
                if identity not in recent:
                    del records[identity]
            return
        if completed and len(recent) == 64 and identity < min(recent):
            return
        records[identity] = row
        recent.add(identity)
        if len(recent) > 64:
            oldest = min(recent)
            recent.remove(oldest)
            if int(records[oldest]['completed']):
                del records[oldest]

    def participating(self, src, rail, now, include_pending):
        return [row for row in self.records[src, rail].values()
                if ((int(row['completed']) and now - int(row['first_tx_ns']) <= 250000)
                    or (not int(row['completed']) and include_pending))]


def confidence_metrics(decisions, intervals, observations, link_map, faults,
                       include_pending=True):
    """Explain interval inaction and destination pooling at the actual SELECT."""
    links, schedule = access_links(link_map), fault_index(faults)
    model, latest = _Membership(), {}
    boundaries = sorted({time for values in schedule.values() for start, end, _ in values for time in (start, end)})
    exposure_cache = {}
    buckets = defaultdict(lambda: {'counts': Counter(), 'values': defaultdict(list)})
    probe_contexts = []
    combined = ([(row, 'observation') for row in observations]
                + [(row, 'interval') for row in intervals if row['trigger'] == 'SELECT']
                + [(row, 'decision') for row in decisions])
    combined.sort(key=lambda item: int(item[0]['audit_seq']))
    sequences = [int(row['audit_seq']) for row, _ in combined]
    if len(sequences) != len(set(sequences)):
        raise ValueError('diagnostic trace has duplicate audit sequence')
    for row, kind in combined:
        src, now = int(row['src']), int(row['timestamp_ns'])
        if kind == 'observation':
            model.observe(row)
            continue
        if kind == 'interval':
            latest[src, int(row['rail'])] = row
            continue
        evidence = [latest.get((src, r)) for r in (0, 1)]
        if any(e is None or int(e['timestamp_ns']) != now for e in evidence):
            raise ValueError('decision lacks current source-local SELECT intervals')
        if int(evidence[1]['audit_seq']) != int(evidence[0]['audit_seq']) + 1:
            raise ValueError('decision SELECT interval pair differs')
        for rail, suffix in enumerate(('A', 'B')):
            for field in ('lower', 'upper'):
                if float(evidence[rail][field]) != float(row[field + '_' + suffix]):
                    raise ValueError('SELECT interval differs from decision confidence')
        ready = all(int(e['ready']) for e in evidence)
        # Positive gap means strictly separated; zero means touching intervals.
        gap = max(float(evidence[0]['lower']) - float(evidence[1]['upper']),
                  float(evidence[1]['lower']) - float(evidence[0]['upper']))
        state = 'cold_or_insufficient' if not ready else (
            'ready_and_separated' if gap > 0 else 'ready_but_overlapping')
        labels = [path_label(dict(row, rail=r), links, schedule) for r in (0, 1)]
        impaired = [label['phase'] == 'scheduled_impaired_access' for label in labels]
        phase = ('both_rails_scheduled_impaired' if all(impaired) else
                 'only_A_scheduled_impaired' if impaired[0] else
                 'only_B_scheduled_impaired' if impaired[1] else
                 'neither_rail_scheduled_impaired')
        values = {'separation_gap': gap}
        histories = []
        for rail, suffix in enumerate(('A', 'B')):
            ev = evidence[rail]
            for field in ('count', 'completed', 'pending', 'radius'):
                values[field + '_' + suffix] = float(ev[field])
            values['working_width_' + suffix] = float(ev['upper']) - float(ev['lower'])
            values['identified_width_' + suffix] = float(ev['identified_upper']) - float(ev['identified_lower'])
            history = model.participating(src, rail, now, include_pending)
            if len(history) != int(ev['count']):
                raise ValueError('diagnostic membership differs from logged interval count')
            destinations = Counter(int(record['dst']) for record in history)
            same_dst = destinations[int(row['dst'])]
            active_paths = 0
            epoch = bisect.bisect_right(boundaries, now)
            for dst, count in destinations.items():
                cache_key = src, dst, rail, epoch
                if cache_key not in exposure_cache:
                    exposure_cache[cache_key] = any(
                        start <= now < end for link in {links[src, rail], links[dst, rail]}
                        for start, end, _ in schedule.get(link, ()))
                active_paths += count * exposure_cache[cache_key]
            context = {'count': len(history), 'same_current_destination_records': same_dst,
                       'currently_scheduled_impaired_history_paths': active_paths,
                       'distinct_history_destinations': len(destinations)}
            histories.append(context)
            if history:
                values['same_destination_history_fraction_' + suffix] = same_dst / len(history)
                values['scheduled_impaired_history_fraction_' + suffix] = active_paths / len(history)
                values['distinct_history_destinations_' + suffix] = context['distinct_history_destinations']
        opportunity = impaired[0] != impaired[1]
        correct_separation = False
        opposite_separation = False
        if opportunity:
            bad = int(impaired[1])
            risk_margin = float(evidence[bad]['upper']) - float(evidence[1 - bad]['lower'])
            values['impaired_upper_minus_clean_lower'] = risk_margin
            if histories[bad]['count']:
                values['impaired_path_history_exposed_fraction'] = (
                    histories[bad]['currently_scheduled_impaired_history_paths'] / histories[bad]['count'])
            correct_separation = ready and risk_margin < 0
            opposite_separation = ready and float(evidence[1 - bad]['upper']) < float(evidence[bad]['lower'])
        names = ['all_decisions']
        names += ['initial_mandatory_probes'] if int(row['base_rail']) < 0 else ['noninitial_decisions', phase]
        for name in names:
            bucket = buckets[name]
            bucket['counts']['decisions'] += 1
            bucket['counts'][state] += 1
            bucket['counts']['both_ready'] += int(ready)
            bucket['counts']['one_impaired_one_clean_opportunities'] += int(opportunity)
            bucket['counts']['ready_separation_against_impaired_rail'] += int(correct_separation)
            bucket['counts']['ready_separation_against_clean_rail'] += int(opposite_separation)
            for field, value in values.items():
                bucket['values'][field].append(value)
        if int(row['probe']):
            rail = int(row['rail'])
            probe_contexts.append({'src': src, 'dst': int(row['dst']), 'sport': int(row['sport']),
                'assigned_ns': now, 'rail': rail, 'both_ready': ready,
                'confidence_state': state, 'selected_path_phase': labels[rail]['phase'],
                'selected_rail_history': histories[rail],
                'alternate_rail_history': histories[1 - rail]})
    return {'scope': 'SELECT snapshots; membership replay only; confidence arithmetic independently audited elsewhere',
            'history_scope': 'per-source/rail pooled across destinations, individually load-normalized; exposure labels at current decision time, not observation time',
            'groups': {name: {'counts': dict(bucket['counts']),
                              'numeric': {field: numeric_summary(samples)
                                          for field, samples in bucket['values'].items()}}
                       for name, bucket in buckets.items()},
            'probe_contexts': probe_contexts}


def analyze_root(root, allow_incomplete=False):
    summary_bytes = (root / 'summary.json').read_bytes()
    summary = json.loads(summary_bytes)
    protocol = json.loads((root / 'protocol.json').read_text())
    if summary['protocol_sha256'] != canonical_hash(protocol):
        raise ValueError('summary protocol identity mismatch')
    plan, actual = protocol['planned_matrix'], summary['scenarios']
    missing = [f'{s}/{p}' for s, labels in plan.items() for p in labels if p not in actual.get(s, {})]
    if missing and not allow_incomplete:
        raise ValueError('planned experiment matrix is incomplete')
    if set(actual) - set(plan) or any(set(labels) - set(plan[s]) for s, labels in actual.items()):
        raise ValueError('unplanned result cannot be silently pooled')
    report = {'source_summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
              'protocol_sha256': canonical_hash(protocol),
              'planned_runs': sum(map(len, plan.values())),
              'completed_runs': sum(map(len, actual.values())),
              'planned_matrix_complete': not missing, 'missing_runs': missing,
              'definitions': {
                  'confidence_correction': 'any factor<1 among noninitial decisions; this need not change the selected rail',
                  'probe': 'useful-data B14 rail override, not extra payload or initial mandatory probes',
                  'probe_timing': 'measured chunk first-TX-to-ACK duration; not isolated overhead or recovery time',
                  'offline_fault_label': 'selected source+destination ACCESS only; scheduled start<=ASSIGN<end; no inference about queues or other links',
                  'clean_access_after_reversion': 'selected ACCESS has an ended target fault and none currently scheduled active; not proof of empty queues',
                  'comparison_sign': 'positive gain means candidate faster; ratio of mean durations, with paired gains also shown',
                  'attribution': 'B14 vs B13 isolates enabling exploration as a policy change, not a randomized probe cost; zero exploration plus assignment parity cannot establish a UCB benefit',
                  'healthy_cost': 'B14 minus B13 overall completion time; probe-vs-ordinary chunk durations are descriptive and selection-confounded',
                  'exploration_guard_counts': 'noninitial rows; fixed frozen 50us cooldown/ACK age, 8 queued chunks, 1/16 bytes. Counts ignore policy enable and physical-up guard (not logged; study injects carrier-up service degradation)',
                  'scope': 'exploratory three reused seeds per fault family; not statistical significance or calibrated CI coverage'},
              'runs': {}, 'families': {}, 'B13_B14_assignment_parity': {}}
    for scenario, run_audits in actual.items():
        fault_path = root / scenario / 'faults.csv'
        expected_fault = protocol['fault_sha256'][scenario]
        if expected_fault is None:
            if fault_path.exists():
                raise ValueError('healthy schedule unexpectedly exists')
            faults = []
        else:
            if not fault_path.is_file() or digest(fault_path) != expected_fault:
                raise ValueError('offline fault schedule changed')
            faults = rows(fault_path)
        report['runs'][scenario] = {}
        for policy, audit in run_audits.items():
            if audit.get('pass') is not True:
                raise ValueError('analysis refuses failed run: ' + scenario + '/' + policy)
            directory = root / scenario / policy
            if json.loads((directory / 'result.json').read_text()) != audit:
                raise ValueError('run result and summary differ')
            consumed = ('split_events.csv', 'chunk_feedback.csv', 'link_map.csv')
            if policy.startswith(('B13', 'B14')):
                consumed += ('censored_decisions.csv', 'censored_intervals.csv', 'censored_observations.csv')
            for name in consumed:
                if audit['artifacts_sha256'].get(name) != digest(directory / name):
                    raise ValueError('consumed raw evidence changed: ' + name)
            metrics = run_metrics(rows(directory / 'split_events.csv'),
                                  rows(directory / 'chunk_feedback.csv'),
                                  rows(directory / 'censored_decisions.csv')
                                  if 'censored_decisions.csv' in consumed else None,
                                  rows(directory / 'link_map.csv'), faults)
            metrics['workload_finish_ns'] = audit['workload_finish_ns']
            if 'censored_decisions.csv' in consumed:
                include_pending = policy != 'B14_completed_only'
                metrics['confidence_diagnostics'] = confidence_metrics(
                    rows(directory / 'censored_decisions.csv'), rows(directory / 'censored_intervals.csv'),
                    rows(directory / 'censored_observations.csv'), rows(directory / 'link_map.csv'),
                    faults, include_pending)
            comparison({'run': metrics['workload_finish_ns']}, {'run': metrics['workload_finish_ns']})
            report['runs'][scenario][policy] = metrics
        values = report['runs'][scenario]
        if 'B13' in values and 'B14' in values:
            left, right = values['B13'], values['B14']
            a, b = left['assignment_signature'], right['assignment_signature']
            probes = right['exploration']['probe_chunks']
            report['B13_B14_assignment_parity'][scenario] = {
                'content_equal': a['content_sha256'] == b['content_sha256'],
                'order_equal': a['ordered_sha256'] == b['ordered_sha256'],
                'finish_equal': left['workload_finish_ns'] == right['workload_finish_ns'],
                'B14_probes': probes,
                'no_exploration_assignment_parity': not probes and a['content_sha256'] == b['content_sha256']}
    families = sorted({s.split('_')[0] for s in report['runs']})
    for family in families:
        scenarios = [s for s in report['runs'] if s.split('_')[0] == family]
        family_result = {'scenarios': scenarios, 'expected_scenarios':
                         [s for s in plan if s.split('_')[0] == family], 'policies': {}, 'comparisons': {}}
        labels = sorted({p for s in scenarios for p in report['runs'][s]})
        for policy in labels:
            samples = [report['runs'][s][policy] for s in scenarios if policy in report['runs'][s]]
            family_result['policies'][policy] = {
                'n': len(samples), 'mean_finish_ns': statistics.mean(r['workload_finish_ns'] for r in samples),
                'total_confidence_corrected_decisions': sum(r.get('confidence', {}).get('any_rail_corrected_decisions', 0) for r in samples),
                'total_probe_chunks': sum(r.get('exploration', {}).get('probe_chunks', 0) for r in samples),
                'total_avoidable_impaired_assigned_bytes': sum(r['avoidable_scheduled_impaired_assignment_bytes'] for r in samples)}
        for ref in ('B8', 'B10', 'B13', 'B14_completed_only'):
            paired = [s for s in scenarios if ref in report['runs'][s] and 'B14' in report['runs'][s]]
            if paired:
                family_result['comparisons']['B14_vs_' + ref] = comparison(
                    {s: report['runs'][s][ref]['workload_finish_ns'] for s in paired},
                    {s: report['runs'][s]['B14']['workload_finish_ns'] for s in paired})
        report['families'][family] = family_result
    healthy = report['runs'].get('healthy', {})
    report['healthy_cost'] = {
        'B14_minus_B13_finish_ns': healthy['B14']['workload_finish_ns'] - healthy['B13']['workload_finish_ns']
        if 'B14' in healthy and 'B13' in healthy else None,
        'B14_minus_B8_finish_ns': healthy['B14']['workload_finish_ns'] - healthy['B8']['workload_finish_ns']
        if 'B14' in healthy and 'B8' in healthy else None,
        'B14_probes': healthy.get('B14', {}).get('exploration', {}).get('probe_chunks')}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--allow-incomplete', action='store_true')
    args = parser.parse_args()
    output = json.dumps(analyze_root(args.root, args.allow_incomplete), indent=2, allow_nan=False) + '\n'
    if args.out:
        with args.out.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')


if __name__ == '__main__':
    main()
