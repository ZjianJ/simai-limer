#!/usr/bin/env python3
"""Frozen random ACCESS impairments, independent calibration, all split baselines."""
import argparse
import copy
import csv
import json
import math
import random
import resource
import statistics
import subprocess
import sys
from pathlib import Path

from run_split_baselines import ROOT, POLICIES, rows, digest, run_one, calibration
from simulator_runtime_bundle import seal_runtime_bundle
from split_capacity import read_capacity, future_bound

FAULT_FIELDS = ('fault_id', 'fault_type', 'target_link_id', 'start_time_ns',
                'end_time_ns', 'severity', 'parameter_before', 'parameter_after', 'recovery_delay_ns')


def schedule(link_rows, count, maximum, start, end, seed):
    links = sorted((r for r in link_rows if r['link_class'] == 'ACCESS'
                    and r['src_type'] == 'HOST' and int(r['src_port']) in (2, 3)),
                   key=lambda r: r['link_id'])
    if len(links) != 32 or len({r['link_id'] for r in links}) != 32 or {
            (int(r['src_node']), int(r['src_port'])) for r in links} != {
            (gpu, port) for gpu in range(16) for port in (2, 3)}:
        raise ValueError('expected two ACCESS links per GPU in a true-16 map')
    if not 0 <= count <= 32 or not math.isfinite(maximum) or not 0 <= maximum < 100:
        raise ValueError('a must be 0..32; b-max-percent must be finite in [0,100)')
    if not 0 < start < end:
        raise ValueError('require 0 < start < end')
    rng = random.Random(seed)
    selected = rng.sample(links, count)
    result = []
    for link in selected:
        slowdown = rng.uniform(0, maximum / 100)
        result.append(dict(zip(FAULT_FIELDS, (f'random-{seed}-{link["link_id"]}',
            'service_degradation', link['link_id'], start, end, slowdown, 1, 1-slowdown, 0))))
    return sorted(result, key=lambda r: r['target_link_id'])


def write_csv(path, fields, records):
    with path.open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(records)


def oracle(healthy, impaired, start, end, output):
    h, d = read_capacity(healthy), read_capacity(impaired)
    if h.keys() != d.keys():
        raise ValueError('calibration pair sets differ')
    records = []
    for (src, dst, rail), values in sorted(h.items()):
        for time, rate in ((0, values[0][1]), (start, d[src, dst, rail][0][1]), (end, values[0][1])):
            records.append(dict(src=src, dst=dst, rail=rail, start_ns=time, capacity_bps=rate))
    write_csv(output, ('src', 'dst', 'rail', 'start_ns', 'capacity_bps'), records)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--topology', type=Path, required=True)
    p.add_argument('--link-map', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--a', type=int, default=8)
    p.add_argument('--b-max-percent', type=float, default=80)
    p.add_argument('--seeds', nargs='+', type=int, default=[42, 43, 44])
    p.add_argument('--start-ns', type=int, default=1000000)
    p.add_argument('--horizon-ns', type=int, default=100000000)
    p.add_argument('--workload', type=Path, default=ROOT/'limer/configs/microAllReduce_16rank_split_64mib.txt')
    p.add_argument('--config', type=Path, default=ROOT/'limer/configs/SimAI.baseline.conf')
    p.add_argument('--chunk-bytes', type=int, default=65536)
    p.add_argument('--max-active', type=int, default=8)
    p.add_argument('--sample-us', type=int, default=1000)
    p.add_argument('--timeout', type=int, default=180)
    args = p.parse_args()
    if len(set(args.seeds)) != len(args.seeds):
        p.error('duplicate seeds')
    if min(args.chunk_bytes, args.max_active, args.sample_us, args.timeout) <= 0:
        p.error('runtime sizes and periods must be positive')
    for k, v in vars(args).items():
        if isinstance(v, Path):
            setattr(args, k, v.resolve())
    plans = {seed: schedule(rows(args.link_map), args.a, args.b_max_percent,
                           args.start_ns, args.horizon_ns, seed) for seed in args.seeds}
    args.out.mkdir(parents=True, exist_ok=False)
    root = args.out
    report = {'parameters': {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              'scope': 'true16_chunk_qp_split', 'tensor_numeric_correctness': 'not simulated',
              'oracle_scope': 'piecewise independent single-rail workload-conditioned goodput; not exact instantaneous capacity',
              'seeds': {}, 'calibration': {}}
    def save():
        (root/'summary.json').write_text(json.dumps(report, indent=2, allow_nan=False)+'\n')
    for seed, records in plans.items():
        directory = root/f'seed_{seed}'
        directory.mkdir()
        write_csv(directory/'faults.csv', FAULT_FIELDS, records)
        write_csv(directory/'calibration_faults.csv', FAULT_FIELDS,
                  [dict(r, start_time_ns=0) for r in records])
        report['seeds'][str(seed)] = {'schedule': records, 'fault_sha256': digest(directory/'faults.csv'), 'results': {}}
    save()
    subprocess.run([sys.executable, str(ROOT/'limer/tools/validate_true16_dualrail.py'),
                    '--topology', str(args.topology), '--out-json', str(root/'topology_validation.json')], check=True)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    args.bundle = seal_runtime_bundle(root, args.binary)
    args.binary = args.bundle.executable
    args.threads = 1
    args.faults = None
    def measured(directory, faults, target):
        local = copy.copy(args)
        local.out, local.faults = directory, faults
        for rail in (0, 1):
            name = f'calibration_{rail}'
            result = run_one(local, name, 'B0', rail=rail)
            target[name] = result
            save()
            print(directory.name, name, result.get('workload_finish_ns'), result['pass'], flush=True)
            if not result['pass']:
                raise RuntimeError(f'calibration failed: {directory/name}')
        capacity = directory/'capacity.csv'
        calibration([directory/'calibration_0', directory/'calibration_1'], capacity)
        return capacity
    healthy = root/'healthy'
    healthy.mkdir()
    healthy_capacity = measured(healthy, None, report['calibration'])
    for seed in args.seeds:
        directory = root/f'seed_{seed}'
        sr = report['seeds'][str(seed)]
        sr['calibration'] = {}
        degraded = measured(directory, directory/'calibration_faults.csv', sr['calibration'])
        capacity = directory/'oracle.csv'
        oracle(healthy_capacity, degraded, args.start_ns, args.horizon_ns, capacity)
        local = copy.copy(args)
        local.out, local.faults = directory, directory/'faults.csv'
        for policy in POLICIES:
            supplied = healthy_capacity if policy == 'B2' else capacity if policy in ('B3', 'B4') else None
            result = run_one(local, policy, policy, supplied)
            result['same_fault_schedule'] = (
                json.loads((directory/policy/'manifest.json').read_text())['fault_sha256'] == sr['fault_sha256'])
            applied = [r for r in rows(directory/policy/'fault_application_telemetry.csv')
                       if r['transition'] == 'apply']
            expected = {r['fault_id']: r for r in plans[seed]}
            result['fault_application_exact'] = len(applied) == len(expected) and {
                r['fault_id'] for r in applied} == set(expected) and all(
                r['status'] == 'APPLIED' and int(r['actual_ns']) == args.start_ns
                and int(r['scheduled_ns']) == args.start_ns
                and r['target_link_id'] == expected[r['fault_id']]['target_link_id']
                and math.isclose(float(r['parameter_after']), expected[r['fault_id']]['parameter_after'], rel_tol=1e-5)
                for r in applied)
            sr['results'][policy] = result
            save()
            print(seed, policy, result.get('workload_finish_ns'), result['pass'], flush=True)
        payload = {}
        for event in rows(directory/'B0/split_events.csv'):
            if event['event'] == 'ASSIGN':
                key = int(event['src']), int(event['dst'])
                payload[key] = payload.get(key, 0) + int(event['bytes'])
        series = read_capacity(capacity)
        sr['B5'] = {'full_allreduce_bound': False, 'scope': 'conditional independent-pair fluid relaxation',
                    'pairs': [dict(src=s, dst=d, **future_bound(series, s, d, n)) for (s,d),n in sorted(payload.items())]}
        save()
    results = [r for sr in report['seeds'].values() for r in sr['results'].values()]
    report['pass'] = all(r['pass'] and r['same_fault_schedule'] and r['fault_application_exact'] for r in results)
    report['same_payload'] = len({sum(r['acked_payload_bytes']) for r in results if r['pass']}) == 1
    report['aggregate_ms'] = {policy: {'mean': statistics.mean(values), 'min': min(values), 'max': max(values)}
        for policy in POLICIES if len(values := [sr['results'][policy]['workload_finish_ns']/1e6
            for sr in report['seeds'].values() if sr['results'][policy]['pass']]) == len(args.seeds)}
    save()
    return 0 if report['pass'] and report['same_payload'] else 1


if __name__ == '__main__':
    sys.exit(main())
