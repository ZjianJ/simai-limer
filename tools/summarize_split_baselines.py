#!/usr/bin/env python3
"""Render measured split artifacts without turning conditional B5 into ns-3 evidence."""
import argparse
import json
from pathlib import Path
from run_split_baselines import rows, audit, POLICIES


def summarize(runs):
    text = ["# 双 rail 分流实现验证", "",
            "下表来自真实 ns-3 QP 和 16-rank 完成屏障。时间为仿真时间；每场景使用相同工作负载、chunk、并发额度和故障时间表。", "",
            "当前资格验证固定单个仿真工作线程，GPU 数为 16。B5 单独保存为条件流体下界，不列作实际 AllReduce 完成时间。", ""]
    for directory in runs:
        summary = json.loads((directory/"summary.json").read_text())
        measured = summary["results"]
        baseline = measured.get("B0", {}).get("workload_finish_ns")
        text += [f"## {directory.name}", "",
                 "| 组别 | 审计 | 完成时间 ms | 相对 B0 加速 | A/B 有效完成字节 | 有效观测窗口数 |",
                 "|---|---|---:|---:|---|---:|"]
        for policy in POLICIES:
            if policy not in measured:
                continue
            result = measured[policy]
            if result.get("pass"):
                verified = audit(directory/policy)
                if not verified["pass"]:
                    raise ValueError(f"published pass no longer passes audit: {directory/policy}")
                ns = verified["workload_finish_ns"]
                speedup = f"{baseline/ns:.3f}x" if baseline else "—"
                text.append(f"| {policy} | PASS | {ns/1e6:.6f} | {speedup} | "
                            f"{verified['acked_payload_bytes'][0]} / {verified['acked_payload_bytes'][1]} | {verified['informative_samples']} |")
            else:
                text.append(f"| {policy} | FAIL / 未完成 | — | — | — | — |")
        text += ["", f"原始结果：`{directory / 'summary.json'}`。", ""]
    text += ["## 解释边界", "",
             "- 未完成、超时或运行崩溃保留为失败，不能只筛选成功组。",
             "- 校准容量是单 rail 测得的工作负载条件 goodput；不是注入的物理速率，也不是多流竞争下不变的精确容量。",
             "- B4 是容量加端点队列的启发式；它不保证比 B2/B3 更快。",
             "- B6 有效观测窗口为零时，结果只验证先验/队列分配，不能证明 EWMA 学习了容量。",
             "- B5 放松独立 rail、可任意分片、协议开销、共享瓶颈和 collective 依赖；当前只计算每个通信对的条件下界，不声称得到完整 AllReduce 的绝对最优解。",
             "- chunk 区间守恒、ACK 完成和 rank 屏障通过，不等价于真实 tensor 数值正确性验证。", ""]
    return "\n".join(text)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs",type=Path,nargs="+",required=True)
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(summarize([r.resolve() for r in args.runs]))


if __name__=="__main__": main()
