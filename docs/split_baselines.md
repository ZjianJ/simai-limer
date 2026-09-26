# B0–B6 双 rail 容量分流基线

新增的 B7 首对 chunk 固定比例、B8 持续完成反馈实验见
[chunk_feedback_experiment.md](chunk_feedback_experiment.md)。

本实验不训练 GP。B0/B1/B2/B3/B4/B6 接入真实 ns-3 RDMA QP；B5
是单独计算的、带明确假设的未来容量积分下界。不要将 B5 称为已经复现的
完整 16-GPU AllReduce 最优调度器。

## 运行时策略

| 组别 | 分配规则 | 信息来源 |
|---|---|---|
| B0 | 所有跨服务器 chunk 走 NIC 3 / rail B | 固定主 rail |
| B1 | 每个有向通信对按累计 payload 字节近似 50:50 | 已分配字节 |
| B2 | 按 t=0 校准容量加权分配 | 独立单 rail 校准 |
| B3 | 按当前时间的 Oracle 容量加权分配，容量变化时重置比例计数 | 校准容量时间表的当前值 |
| B4 | 选择 `(backlog + chunk_bytes) * 8 / capacity_bps` 最小的 rail | 当前 Oracle 容量、端点队列、未完成额度 |
| B5 | 求两条 rail 未来容量积分足以承载全部 payload 的最早时间 | 完整未来容量时间表 |
| B6 | 与 B4 共用调度规则，容量替换为 ACK goodput 的 EWMA | 过去 ACK、因果遥测、已分配额度 |

B4 是本次实现的队列感知启发式，不是最优性证明。B3 也不保证总比 B6 快；
完美容量信息并不能保证加权启发式具有最优调度结果。

启用 `LIMER_SPLIT_POLICY` 才改变数据面。原生未启用路径保留原来的 QP 数和
ECMP 选择。实验 B0 与其它运行时组使用相同 chunk 大小、并发额度和启动延迟，
因此它是控制调度开销后的单 rail 对照；它不能替代未改动原生配置的历史基线。

## 数据面与完成条件

固定 true-16、每服务器 4 GPU、NIC 2=A / NIC 3=B；运行器先执行现有物理
双平面拓扑验证器。服务器内通信保留原来的 NVSwitch 路径。

跨服务器逻辑 flow 被拆成固定大小的 chunk，尾块保留精确剩余字节数。每个
chunk 使用独立有限 QP，创建时验证指定 NIC 是目的节点的合法路由候选。每个
发送端最多同时启动 `LIMER_SPLIT_MAX_ACTIVE` 个 chunk，ACK 完成后再启动
下一个，因此动态策略能对仍未分配的数据响应容量变化。

每个 chunk 记录逻辑 flow ID、offset、原 flow 大小、rail、源端口及分配和
ACK 完成时间。原有 flow 的发送/接收回调仍需等待所有子 QP 完成。审计额外
要求所有区间恰好覆盖原 flow、没有重叠、ACK 字节/rail 与分配一致、16 rank
全部完成。仿真不计算 tensor 数值，这些检查不构成真实 NCCL 数值正确性证明。

本轮禁用 recovery transport，避免未声明的自动换 rail 污染分流对照。
已经在故障 rail 上的 QP 继续遵守原 RDMA 重试行为；改变分配仅作用于新 chunk。
因此单 rail 永久故障可能得到未完成/超时结果，这是基线结果，不应丢弃。
两条 rail 同时不可用时当前运行时拒绝继续派发并终止实验，运行器标记失败；
尚未实现“全部暂停、等待恢复”的控制器。

## 队列和 B6 的观测边界

队列使用现有一致性快照中的源 NIC 队列、目的 ACCESS 对应 ToR 出口队列。
未完成额度同时按源/rail、目的/rail 记账。调度器取这几项的最大值而不是相加，
防止同一批数据在 QP、NIC 和交换机重复计数。它是端点积压代理，尚未对所有
中间交换机共享链路建立全局最优排队模型。

新增 ACK hook 对 `snd_una` 的正增量按 payload 大小截断，只统计绑定到分流器
的训练 QP；不计 background、重复 ACK、重传线速字节。`split_samples.csv`
给出累计有效 ACK 字节、采样 goodput、EWMA、队列、outstanding 和需求判定。

B6 默认 alpha=0.2；初始先验为 100 Gbps，符合本轮 100-Gbps ACCESS 拓扑。
连续两个采样点满足 `queue>0 或 outstanding>=64 KiB` 才更新 EWMA。这是明确
记录的需求启发式，不声称能证明整个间隔持续饱和。空闲窗口不更新成零；
每 5 ms 允许给无未完成额度的可用 rail 一个探索 chunk。没有新 chunk 时不会
生成额外训练数据或探测流量。硬 down 对新派发读取本地实时 link state；
它不迁移已经在飞的 chunk，也不代表完整端到端告警送达时间。

B6 的策略代码不打开容量 CSV，不读取注入参数或未来轨迹。初始速率和观测
不足的问题应在后续长流量实验中单独报告，不能用短于采样周期的 smoke run
宣称 EWMA 自适应有效。

## 容量文件与 B5

CSV 格式严格为：

```text
src,dst,rail,start_ns,capacity_bps
0,4,0,0,30000000000
0,4,1,0,100000000000
0,4,0,10000000,0
0,4,0,20000000,30000000000
```

每个实际使用的有向通信对必须有两条 rail、t=0 初值及严格递增的变化点。
最后一个值持续到实验结束。以上数字只演示格式，不能当作测量证据。

健康运行默认先执行两次单 rail 校准。容量采用该通信对 ACK 完成 payload
除以活动 chunk 时间区间的并集长度；并发 QP 的重叠时间不重复计入。它是
本工作负载条件下的实测速率，不保证饱和，也不保证双 rail 同时使用时仍可
线性相加。故障实验必须提供独立校准的时间表；仿真配置带宽不能冒充丢包、
重试条件下的有效容量。

B5 解 `integral(C_A(t)+C_B(t), start, finish) >= 8*bytes`。
假设数据可任意细分、两条 rail 独立、没有启动/ACK 开销、其它通信竞争和
collective 依赖。运行器对 B0 实际派发的每个通信对汇总 payload 后计算独立
流体下界，输出 `full_allreduce_bound=false`，不把这些时间相加或取最大后
伪装为完整 AllReduce 最优时间。若容量输入只是实测均值，该下界也仅在输入
容量模型成立的条件下有效。

## 使用

构建（默认两个编译进程）：

```bash
bash SimAI/limer/scripts/build_split_baselines.sh
```

健康比较，输出目录必须不存在：

```bash
python3 SimAI/limer/tools/run_split_baselines.py \
  --binary SimAI/ns-3-alibabacloud/simulation/build/scratch/ns3.36.1-AstraSimNetwork-debug \
  --topology SimAI/limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100 \
  --out SimAI/limer/results/split_baselines/healthy_run
```

二进制实际名称以 CMake 构建输出为准。当前运行器固定 `--threads 1`：两线程
初次冒烟曾在 B2 完成附近触发内存释放错误，多线程结果尚未通过资格验证。
CPU 仿真线程数不等于 GPU 数；GPU 数始终由 true-16 拓扑和工作负载决定。
默认 64-KiB chunk、每发送端
8 个活动 chunk、1-ms 快照。消息大小、chunk 和并发度应随后分别扫描。

故障比较额外传 `--faults fault_events.csv --capacity calibrated_capacity.csv`。
对于整个观察期内不变、t=0 起生效的 service_degradation，可用
`--faults static_fault.csv --calibrate-static-faults` 独立测量 A/B 后再比较。
运行器拒绝用这个捷径校准中途变化的故障。`prepare_split_static_fault.py`
可根据真实 link_map 生成指定 rail 的 16 条 ACCESS 固定退化事件，例如
`--rail B --fraction 0.25`。只复查部分运行时组可指定 `--policies B6`；此时
没有 B0 payload 或容量表，就不生成 B5 数值。

运行器给各组相同故障文件、工作负载、配置和采样周期；封存二进制及 ns-3
动态库，防止重编译改变实验中途加载的程序。每组独立输出配置和日志，不会
覆盖历史 baseline 的输出路径。

交付文件：每组 `split_events.csv`、`split_samples.csv`、原始物理遥测、
`run_lifecycle.csv`、运行 manifest；总结果 `summary.json`，包含运行审计和
单独标注的 B5 条件下界。失败、未完成、超时不会被记录成成功恢复。

测试：

```bash
python3 -m unittest discover -s SimAI/limer/tests -p test_split_capacity.py -v
g++ -std=c++17 -Wall -Wextra -Werror \
  -I SimAI/astra-sim-alibabacloud/astra-sim/network_frontend/ns3 \
  SimAI/limer/tests/split_policy_test.cc -o /tmp/limer-split-policy-test
/tmp/limer-split-policy-test
```
