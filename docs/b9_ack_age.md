# B9：ACK 触发的在途 chunk 等待时间修正

在 B8 的最新完成速率及字节额度规则上，只增加一个端侧估计修正。
每收到源GPU的一个chunk完成ACK，先更新该rail最新完成速率，再检查同源GPU
两条rail的其他活动chunk。仅考虑已经发生真实first-TX、尚未完成的chunk。

对rail r，本次ACK触发时：

```
cap_r = min(size_i * 8e9 / (now_ns - first_tx_i_ns))
        over started, incomplete chunks i on rail r
effective_r = min(latest_completed_rate_r, cap_r)
```

没有有效在途样本时cap为无穷，不降低历史速率。尚无首个完成样本的rail保持
原B8的零估计与探测逻辑。多个并发chunk取最小上界，这是刻意明确的保守启发式，
不等于实际rail可用带宽。尾块按实际大小计算；起点不是ASSIGN或入队时间。

本次版本只在完成ACK触发时重新计算两条rail，Select不继续随时间衰减，无定时器。
恢复/空闲时会在下次同源完成ACK清除失效的旧上界。完全没有完成ACK则不会更新。
保留字节额度余额、64KiB chunk、每源最多8块、初始两rail各一探测块等原设置。
不会迁移已发送块，没有GP、交换机容量估计或故障时间表输入调度器。

记录 `chunk_age_feedback.csv`：每个完成ACK严格对应两个rail快照，包括触发
chunk、最新原始速率、有效速率，以及产生最小上界的在途chunk真实first-TX、
大小、等待时间。没有见证样本时age/cap字段为0（表示无约束，不表示零带宽）。
审计将见证与该chunk最终完成记录核对，并核对两条rail原始速率与最新完成样本一致。

## 复测方法

```bash
bash limer/scripts/build_split_baselines.sh
python3 limer/tools/run_b9_comparison.py \
  --binary ns-3-alibabacloud/simulation/build/scratch/ns3.36.1-AstraSimNetwork-debug \
  --topology limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100 \
  --frozen limer/results/split_baselines/random_a8_b80_t1ms_20260908 \
  --out limer/results/split_baselines/NEW_B9_COMPARISON
```

两算法共用新的封存二进制/动态库。健康对照加种子42/43/44，共8次正式运行；
每种子故障CSV直接复制旧实验，校验SHA256。B8必须复现旧故障实验完成时间。
无重新抽样、无重新校准。输入64MiB/每GPU，16 GPU，1ms开始减速8条ACCESS，
减速上限80%，100ms观察窗口，1ms遥测，单仿真CPU线程。

额外统计1ms后分配到“源或目的ACCESS受故障影响的rail路径”的payload，不能将
这些字节全部称为误分配：慢路径仍可能有正贡献。跨rail限速记录数也不是故障检测
次数；健康排队抖动同样可能触发。最终完成时间不能解释成故障恢复延迟。
