# 八项相关工作统一 16-GPU 机制级评测说明

## 实验边界

本实验接入 FANcY、Trumpet、NetBouncer、MP-RDMA、Flor、SHIFT、OptCC 和 ReCoVer 的机制级适配器。它不是八套作者原始系统的源码移植。统一结果中的证据分为四类：

1. `offline_causal_trace_replay`：使用实际 SimAI 虚拟时间戳做因果回放；
2. `calibrated_*` / `paper_parameter_projection`：使用论文给出的周期或阶段耗时做投影；
3. `theorem13_analytical_projection_not_schedule_execution`：只执行 OptCC 论文公式，不执行其 flow schedule；
4. `executable_protocol_invariant_model`：执行协议正确性不变量，但不产生数据面时延结论。

只有模拟器内实际执行、具有送达时间戳且满足严格小于号的事件，才允许成为平台 `PASS`。参数投影即使数值低于门限，平台状态也保持 `UNVERIFIED`。

## 16-GPU 定义修正

旧的 `microAllReduce_10iter.txt` 虽然以 `-t 16` 启动 16 个活跃 rank，但 workload 声明 `model_parallel_NPU_group: 8` 和 `all_gpus: 8`，实际形成两个 8-rank communicator。

新增两个 workload：

- `microAllReduce_16rank_10iter.txt`：后续正式实验使用的 TP=16、DP=1 workload；
- `microAllReduce_16rank_smoke.txt`：快速检查 communicator 构造与健康态完成。

带输入哈希的单层 smoke 已观察到 16-rank ring、16 个 rank 的 sender-flow 完成记录和进程正常退出。它耗时约 0.151 ms，短于 1 ms 采样周期，因此 switch/NIC 周期表为空。该 smoke 不证明 10-layer workload、fault run、collective commit 或 tensor 正确性。

旧的 82-event 数据仍可做受限的检测对比，因为 16 个 rank 和 16 条 ACCESS 都有流量，但报告必须标明它是两个 8-rank communicator，不能用于证明单一 16-rank fault AllReduce 或 collective 正确性。

既有健康长跑的原始遥测已经重新核对：25 个 1 ms 离散快照，每个快照有 288/288 物理链路、544 个 fabric TX endpoint、544 个 fabric RX endpoint 和 32 个 host endpoint，且无重复键、所有物理链路均有非零 TX。这个证据的范围仍然只是健康 2×8 工作负载；“离散快照无缺口”不等价于“连续时间每个瞬间都记录”。

## 统一接口

检测输出：

```text
fault_id, method, layer, fidelity,
signal_time_ns, delivered_alarm_time_ns,
signal_latency_ns, actionable_detection_latency_ns,
signal_timing_status, actionable_timing_status,
strict_platform_status, localized_target_port,
alarm_generated, timing_semantics
```

恢复输出：

```text
fault_id, method, evaluation_mode, trigger_contract,
recovery_latency_ns, timing_status, strict_platform_status,
backup_path_assumed, backup_qp_assumed,
surviving_port_progress_verified
```

`current_platform` 与 `component_isolation_oracle_error_wc` 必须分开。后者只回答“error WC 已经出现且备用资源已经存在之后，论文机制的阶段耗时是多少”，不能回答 failure-to-WC 或端到端恢复时间。

## 当前实现的八个适配器

| 工作 | 实现内容 | 不能声称的内容 |
|---|---|---|
| FANcY | 仅从原始 `drop_error_delta` 计数不一致起算，再做 50 ms cadence + 论文均值残差投影 | P4/session/hash-tree 原实现、无丢包带宽下降检测、精确逐事件 70 ms、RDMA QP 恢复 |
| Trumpet | actual host anomaly + 10 ms trigger epoch + 0.75 ms controller 投影 | DPDK/CPU 实测、原论文固定故障分类器 |
| NetBouncer | 5 分钟 probe epoch + 37.3 s processor 的反事实最早结果下界 | 主动 probe/solver、真实 alarm、端口定位、带宽下降检测 |
| MP-RDMA | 路径前置条件、66 B/connection 和论文 claim 审计 | 本地 ACCESS 端口失效后的备用 rail 切换 |
| Flor | oracle error WC 后 60 us backup-QP switch 投影 | failure-to-WC、真实备用端口 ACK、AllReduce exactly-once |
| SHIFT | oracle error WC 后 2.30 ms fallback 投影 | failure-to-WC；本次没有协议选择证据，因此 LL128 正确性风险只能判 `UNVERIFIED` |
| OptCC | 将单链路剩余容量按 `(g-1+r)/g` 映射到 pooled server，再计算 p=16、g=4 的 Theorem 13 | 四阶段 schedule 的 SimAI 执行、动态 in-flight fault、实测 runtime |
| ReCoVer | 16-rank epoch abort/rewind/redo 整数摘要不变量 | MPI/ULFM 原实现、恢复时延、tensor-level SimAI 验证 |

## Collective 安全模型

每个故障覆盖三个注入阶段：

- reduce 前；
- reduce 中；
- reduce 完成但尚未 commit。

旧 epoch 的 scratch 状态不可发布；新 epoch 从 16 个 immutable rank contribution 重新执行；输出检查：

- contribution 数量为 16；
- rank ID 唯一且每个一次；
- 所有 rank 结果一致；
- SHA-256 摘要等于无故障精确整数 reference；
- aborted epoch 没有提交错误结果。

状态机还主动注入并验证三个拒绝路径：不完整 collective 不可 commit、abort 后旧 epoch 写入被拒绝、redo 中重复 rank contribution 被拒绝。模型不跟踪物理路径，因此只记录 `failed_rank_contribution_replayed`，不声称经 backup port 执行。

这只是将 ReCoVer 的 epoch/rewind 思路适配到“端口切换但 rank 不丢失”的最小保护协议。实际系统仍需 tensor bucket、communicator attempt 和 optimizer exactly-once。

## 运行

复用锁定故障轨迹并重新生成全部结果：

```bash
bash limer/scripts/run_related_work_16gpu.sh
```

同时重跑轻量的真实 16-rank 健康 smoke：

```bash
LIMER_RELATED_WORK_RUN_TRUE16_SMOKE=1 \
  bash limer/scripts/run_related_work_16gpu.sh
```

只有显式设置下面变量且输入哈希仍匹配时，才复用已有 smoke；默认不会静默消费旧日志：

```bash
LIMER_RELATED_WORK_REUSE_TRUE16_SMOKE=1 \
  bash limer/scripts/run_related_work_16gpu.sh
```

运行单元测试：

```bash
limer/.venv/bin/python -m unittest discover \
  -s limer/tests -p 'test_related_work_16gpu.py' -v
```

主要产物位于 `limer/results/related_work_16gpu/`：

- `related_work_report.md`：面向研究问题的结论；
- `platform_audit.json`：实验契约和缺失能力；
- `detection_timeline.csv` / `detection_summary.csv`：同一故障时间表下的检测比较；
- `recovery_component_timeline.csv`：当前平台与 oracle 组件隔离结果；
- `collective_safety_cases.csv`：可执行的安全重做不变量；
- `optcc_16gpu_projection.csv`：明确标记为未执行 schedule 的分析投影；
- `healthy_alarm_comparison.csv`：LIMER/host predicate 的健康 candidate alarm，以及论文机制误报率的证据缺口；
- `method_verdicts.csv` / `conclusion.json`：严格四项 SLO 判定；
- `benchmark_checks.json`：防止证据等级混淆的完整性检查。

## 当前严格结论

八项工作没有任何一项单独同时提供：hard `<1 ms`、gray `<100 ms`、surviving port `<1 s` 和 in-flight AllReduce 安全语义。严格检测按全部 82 个已排程故障统计，而非只保留 72 个可观测事件。当前结果包括：

- FANcY dedicated cadence：只在 31 个丢包型 gray fault 中受支持，21 个出现计数不一致；其投影时延中位数约 65.65 ms，但 hard 和无丢包降速不满足；
- Trumpet predicate proxy：gray 已排程通过率 75.68%，hard 因 10 ms epoch 不满足；
- NetBouncer paper cadence：约 337.3 s，远超两个检测门限，且未执行 probe/solver；
- Flor/SHIFT：只在 oracle error WC 后分别投影 0.060/2.300 ms，未验证 failure-to-WC 或 surviving-port progress；
- ReCoVer-inspired guard：246/246 协议不变量用例通过，但不是平台恢复或 tensor 实测。

当前平台也不具备端到端验证条件。健康负载只有 8 个 run、合计 177 ms 暴露，不能从零或少量 candidate alarm 外推生产误报率。

下一阶段必须先实现公共层：永久 ACCESS down、双 rail、在线 AlarmBus、真实 RTO/retry/error WC、预建 backup QP、surviving-port useful ACK/训练进展，以及 collective attempt/commit/abort/replay。之后才能把 Flor/SHIFT/OptCC/ReCoVer 从参数或协议模型升级为 SimAI 数据面证据。
