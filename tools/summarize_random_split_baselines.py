#!/usr/bin/env python3
"""Reaudit completed random comparisons and print a concise Markdown report."""
import argparse
import json
from pathlib import Path
from run_split_baselines import POLICIES, audit, digest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('root', type=Path)
    args = p.parse_args()
    root = args.root.resolve()
    summary = json.loads((root/'summary.json').read_text())
    if not summary.get('pass') or not summary.get('same_payload'):
        raise ValueError('experiment incomplete or failed')
    seeds = summary['seeds']
    print('# 随机 ACCESS 减速实验结果\n')
    params = summary['parameters']
    print(f"参数：a={params['a']}、减速上限{params['b_max_percent']}%、"
          f"{params['start_ns']/1e6:g} ms开始；16 GPU，chunk={params['chunk_bytes']}字节。\n")
    print('| 策略 | '+' | '.join(f'种子{s} / ms' for s in seeds)+' | 平均 / ms |')
    print('|---|'+'---:|'*(len(seeds)+1))
    for policy in POLICIES:
        values = []
        for seed, sr in seeds.items():
            directory = root/f'seed_{seed}'/policy
            verified = audit(directory)
            if not verified['pass'] or verified['workload_finish_ns'] != sr['results'][policy]['workload_finish_ns']:
                raise ValueError(f'audit mismatch {directory}')
            manifest = json.loads((directory/'manifest.json').read_text())
            if manifest['fault_sha256'] != digest(directory.parent/'faults.csv'):
                raise ValueError('fault schedule changed')
            values.append(verified['workload_finish_ns']/1e6)
        print(f'| {policy} | '+' | '.join(f'{v:.6f}' for v in values)+f' | {sum(values)/len(values):.6f} |')
    print('\n时间为仿真零点到全部16 rank完成屏障，不是恢复延迟。\n')
    for seed, sr in seeds.items():
        b5 = max(pair['finish_ns'] for pair in sr['B5']['pairs'])/1e6
        b6 = sr['results']['B6']['informative_samples']
        print(f'- 种子{seed}：B5独立通信对条件流体时间最大值 {b5:.6f} ms；B6有效窗口记录 {b6} 条。')
    print('\nB5不是实际AllReduce结果，也不是经过证明的完整系统下界。B3/B4容量是独立单rail校准近似。')
    print(f'{len(seeds)*len(POLICIES)}次正式比较与{2+2*len(seeds)}次校准；不模拟tensor数值正确性，不证明检测/恢复SLO。')
    print(f'\n原始证据：`{root}/summary.json`。')


if __name__ == '__main__':
    main()
