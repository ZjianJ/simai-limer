# 首对 chunk 与持续完成反馈分流实验

新增 B7、B8；原 B0–B6 策略保留。GPU 数仍为16，每服务器4 GPU，每GPU
两条独立 rail；单个仿真工作线程。没有训练 GP，没有增加故障恢复协议。

## 两个新对照组

- B7：每个发送 GPU 在本次运行开始时，向同一目的GPU同时发送两块等大的、
  内容不重复的训练数据，A/B 各一块。每条 rail 的第一次完整 ACK 到达时，
  计算 `bytes * 8 / (ACK_complete - first_TX)`；该 rail 之后保持此估计。
  两条 rail 都获得结果后，按估计速率比例分配后续固定大小 chunk。
- B8：相同的首对测量；之后每个 chunk 完整 ACK 到达时，用本块实际耗时
  得到的最新速率更新该 rail。第一版使用最新样本，不额外平滑，以隔离
  “逐 chunk 完成反馈”的效果。它不等于 B6 的1-ms窗口 ACK-goodput EWMA。

初始仅放行两块。快路径先完成即可继续承载后续数据，不等待慢路径。
慢路径尚未获得首个有效样本时，不给它额外数据；原探测 chunk 仍由 RDMA
跟踪，未完成不能当作成功。两条都未完成时不会新增流量，实验受统一
observation horizon / wall timeout 限制。未实现自动 redo 或坏路径重连。

稳态同样使用每发送端最多8个活动 chunk。B7/B8 采用加权字节额度，根据
当时两条速率的比例选择下一块，不把单块大小按比例改变。更新速率时不
清零累计调度信用，避免每次反馈都重新偏向较快 rail。除物理 link-down
门控外，不使用交换机队列评分、周期性吞吐、校准容量表或注入时间表。

首对测量按发送GPU保存、每运行一次；本实验每个发送GPU只使用一个跨服务器
目的端。多目的端需改为每有向通信对估计，不能直接沿用本实验的解释。
首个逻辑 flow 必须至少有两块可立即派发的数据；不足时严格审计拒绝将其
计为同时探测实验。原运行器的默认策略仍是原六组，新组通过 `--policies`
显式选择。

## 可核验的测量

`chunk_feedback.csv` 记录每个完成样本的 src/dst/sport/rail、payload 字节、
真实 first-TX 和 ACK-complete 时间、耗时、测得速率、策略实际使用的速率、
累计样本数。B7 的 used_bps 保持该 rail 第一条样本；B8 等于当前样本。

运行器逐源检查：首两次分配时间相等、不同 rail、相同目的端、相同字节数；
两条 rail 的真实 first-TX 时间也必须相等。所有16个发送GPU都必须通过；
所有 chunk 的反馈与 ACK 完成数量相等。同时执行原区间守恒、ACK 和16-rank
屏障审计。首对数据属于训练 payload，不是复制同一chunk，也不增加总字节。

## 对比条件

输入消息64 MiB、chunk64 KiB、max-active8、遥测1ms。比较 B2/B6/B7/B8：

1. 健康：两条 rail 都保持原服务速率。
2. 固定退化：rail B 的16条 ACCESS 在t=0起保持25%服务速率；B2 使用独立
   单 rail 校准的退化容量。
3. 运行中退化：B 在t=1ms起降到25%，此前健康；B2 使用故障前健康校准，
   B7 首次测量也发生在健康期。B6/B8 只能通过实际反馈作出响应。

这三个场景都是整条 B rail 的对称服务退化，不代表单端口丢包/flap实验。
每场景所有策略使用同一故障文件、相同payload与固定协议参数。二进制和
动态库使用已有 runtime bundle 封存。

## 复现入口

```bash
bash SimAI/limer/scripts/build_split_baselines.sh
python3 SimAI/limer/tools/run_split_baselines.py \
  --binary SimAI/ns-3-alibabacloud/simulation/build/scratch/ns3.36.1-AstraSimNetwork-debug \
  --topology SimAI/limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100 \
  --workload SimAI/limer/configs/microAllReduce_16rank_split_64mib.txt \
  --policies B2 B6 B7 B8 \
  --capacity SimAI/limer/results/split_baselines/healthy_64mib_20260908/capacity.csv \
  --out SimAI/limer/results/split_baselines/chunk_feedback_new_run
```

故障场景额外传 `--faults`；固定退化需换成对应校准容量，动态退化仍用健康
初始容量。`prepare_split_static_fault.py --start-ns 1000000` 可生成1ms后
开始的固定服务退化区间；这个非零起点不允许使用静态校准捷径。

解释时必须区分：单块完成速率、长流饱和服务率和整个AllReduce完成时间。
首块耗时包含传播、协议和初始传输状态影响，不能视为无偏的长期容量测量。
B2 是容量比例启发式而非最优调度证明；其它组比它快也不违反容量约束。
