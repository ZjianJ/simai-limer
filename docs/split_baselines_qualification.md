# 分流基线实现资格验证：2026-09-08

实现入口与假设见 [split_baselines.md](split_baselines.md)。本次未训练 GP，
未实现新的恢复机制。B0/B1/B2/B3/B4/B6 的 chunk 分配驱动实际 ns-3 QP，
B5 是单独计算的条件流体下界。

## 真实数据面结果

true-16、4 GPU/server、双平面、100-Gbps 额定 ACCESS、64-KiB chunk、
每发送端最多 8 个活动 chunk、1-ms 遥测、单个仿真工作线程。固定退化场景
将 rail B 的 16 条 ACCESS 从 t=0 起设为 0.25 service fraction，整个观察期
保持不变。两次独立单 rail 校准先测 ACK goodput，再运行各组。

| 策略 | 健康 64-MiB 工作负载完成时间 ms | B rail 固定退化完成时间 ms |
|---|---:|---:|
| B0 | 2.779128 | 10.485179 |
| B1 | 1.452228 | 5.110284 |
| B2 | 1.435602 | 2.173988 |
| B3 | 1.435602 | 2.173988 |
| B4 | 1.479303 | 2.344153 |
| B6 | 1.484791 | 2.350511 |

上述 12 次运行均通过：相同跨服务器有效 payload 总量 503316480 字节、
所有 chunk 区间恰好覆盖逻辑 flow、全部 ACK 完成、零未完成 chunk、16 个
GPU rank 全部到达完成屏障。该 payload 总量包含集合通信算法实际产生的
跨服务器通信，不能直接与输入消息的 64 MiB 混为一谈。

B2/B3 在固定容量场景相同是预期现象，不能据此验证动态 Oracle 的收益。
B4 本次不优于 B2/B3，因此只能称为队列感知启发式，不能称为已证明的最优
调度器。B6 健康 64-MiB 运行短于两个采样周期，尚未发生有效 EWMA 更新；
固定退化运行产生 19 个有效源/rail 观测，单独的健康 256-MiB B6 运行产生
99 个有效观测并于 5.745362 ms 完成，验证了实际 ACK 驱动的 EWMA 更新。

原始证据（均在 `limer/results/split_baselines/`）：

- `healthy_smoke_single_worker_20260908/summary.json`：六组小消息冒烟通过。
- `healthy_64mib_20260908/summary.json`：健康容量聚合比较。
- `static_B_quarter_64mib_20260908/summary.json`：同一固定故障时间表比较。
- `B6_256mib_20260908/summary.json`：B6 长消息验证。
- `validation_20260908.md`：从原始事件重新审计生成的完整表。

## 失败记录和限制

初次双线程冒烟 `healthy_smoke_20260908/B2/run.log` 在完成附近出现
`double free or corruption (!prev)`，进程退出 -6。该失败记录保留；根因
尚未定位，不能声称多线程稳定性已验证。当前比较运行器明确拒绝多个工作
线程，所有正式结果重跑于单线程；物理拓扑和 16-rank 工作负载不变。

B5 忽略协议开销、共享瓶颈及 collective 依赖，仅计算有向通信对的条件
流体下界，不是完整 AllReduce 绝对最优结果。数据面仿真不计算 tensor 数值，
所以通过字节/ACK/完成屏障检查不能代替真实 NCCL 数值正确性实验。动态
故障、丢包、flap、消息/chunk/并发度扫描尚未做完整性能矩阵。

## 检查

目标二进制构建通过。37 项 Python 测试（容量/审计 7、background contract
17、内存界限 11、source-port allocator 2）及独立 C++ 分配策略行为测试通过。
内存界限测试原正则误把 `hdrm_bytes[` 当作 `m_bytes[`，本次增加单词边界，
未修改交换机内存计数实现。两个仓库的 `git diff --check` 通过。
