# 随机 ACCESS 减速分流对照实验

入口：`limer/tools/run_random_split_baselines.py`。不训练 GP，不新增恢复机制。
首轮实测见 [随机减速资格验证](random_split_qualification.md)。

## 冻结的实验条件

- 16 GPU、每 GPU 两条 ACCESS rail；从真实 link_map 的32条链路无放回抽取 a 条。
- 各链路独立均匀采样减速比例 b_i∈[0,b_max]，剩余服务比例为 1-b_i。
- 同一固定仿真时刻开始，持续到观察窗口结束；两个方向都受影响，carrier 保持 UP。
- 参数默认 a=8、b_max=80%、开始1 ms、结束100 ms、种子42/43/44。
- 在任何模拟启动前生成所有 faults.csv；同种子各策略共用该文件并检查 SHA256。
- 64 MiB AllReduce 输入，64 KiB chunk，最多8个活动 chunk，1 ms遥测周期。
- 单CPU模拟线程，但仍然是16个独立GPU节点的通信，不是单GPU代理。

## 对照组和信息边界

| 组别 | 输入/分配 |
|---|---|
| B0 | 主 rail B 单路 |
| B1 | 固定50:50 |
| B2 | 独立健康单 rail 校准后固定比例；不读取未来故障 |
| B3 | 健康/退化独立单 rail 校准容量，在故障开始时切换容量表 |
| B4 | B3容量表加队列/已分配额度 |
| B5 | 单独计算已知未来容量表下独立流体传输的条件下界 |
| B6 | ACK吞吐窗口 EWMA + 队列/额度 |
| B7 | 每GPU首对同时发送chunk的完成速率，后续固定比例 |
| B8 | 持续使用最新chunk完成速率更新比例 |

每种子另做两次退化校准：相同链路及减速比例从 t=0 生效，分别只使用 A、B。
据此构造 B3/B4 的分段容量表：0时刻健康，1 ms时刻退化，100 ms恢复。
校准测量是工作负载条件下的 ACK 有效吞吐，不保证饱和，也没有精确描述动态
过渡及双rail竞争。故 B3/B4 仍是校准Oracle近似，不保证最快。
B5更忽略共享竞争、collective依赖、chunk离散性和协议开销；不作为真实
AllReduce运行时间，也不是基于这些估计就能证明的系统理论天花板。

## 复现

```bash
python3 limer/tools/run_random_split_baselines.py \
  --binary ns-3-alibabacloud/simulation/build/scratch/ns3.36.1-AstraSimNetwork-debug \
  --topology limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100 \
  --link-map limer/results/split_baselines/healthy_smoke_single_worker_20260908/B0/link_map.csv \
  --a 8 --b-max-percent 80 --seeds 42 43 44 --start-ns 1000000 \
  --out limer/results/split_baselines/NEW_RANDOM_RUN
```

输出目录必须尚不存在。运行时封存二进制及项目动态库。summary.json保存参数、
随机时间表、各组审计、完成时间、B5逐对结果及三种子均值/范围。
逐组检查故障 APPLIED 的链路、时刻、比例，并检查chunk无重复/缺失、发送与ACK
字节相同、全部16 rank完成。总完成时间从仿真零点计，不是故障后的恢复延迟。
不模拟tensor数值正确性，不证明硬故障检测或1秒恢复指标。
三个种子仅是小规模配对对照，不支持p99或统计显著性结论。
