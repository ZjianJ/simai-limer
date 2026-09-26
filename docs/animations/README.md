# B8 持续 chunk 反馈动画

新增 [完整拓扑交互动画](topology_explorer.html)：展开全部 16 GPU、4 NVSwitch、
8 ASW、64 PSW 和 304 条物理链路；可点击设备看直接邻居，演示本地、同 ASW、
跨核心与双 rail 数据路径。支持中英文切换、暂停、章节跳转、路径自选与缩放。
连接匹配真实拓扑，光点和时间仅为示意；[使用与证据边界](topology_explorer.md)。
此新增页面尚未提交或发布 GitHub，不能假定已有线上地址。
可下载 [离线演示包](limer_topology_demo.zip)，解压后打开 `topology_explorer.html`；
包内包含 JavaScript 依赖和原有算法/仿真流程页面，离线页面间链接可用。

另有 [仿真实验流程动画](simulation_workflow.html)：展示16 GPU拓扑、固定配置、
三种子故障时间表、独立对照运行、完成时间和审计；支持中英文切换。
页面：https://zjianj.github.io/simai-limer/animations/simulation_workflow.html 。
说明及数据来源见 [simulation_workflow.md](simulation_workflow.md)。

打开 `chunk_feedback.html`，点击播放即可；不需要服务器、安装依赖或网络连接。
英文版：`chunk_feedback_en.html`。英文 GitHub Pages 展示入口：
该文件现支持右上角“中文 / English”即时切换，保留进度、播放状态和速度；
GitHub 更新提交为 `bd398b8`。中英文渲染及切换状态保持已通过脚本检查。
https://zjianj.github.io/simai-limer/ 。展示文件发布提交：`8d3defb`；
该提交仅包含动画和展示页，不包含最新 B8 仿真器源代码及依赖。
支持暂停、拖动进度、五个阶段跳转、0.5–2倍速、全屏；空格播放/暂停，方向键跳转。
一轮约52秒（1倍速）。组会建议在“B线路减速”和“反馈后重分配”两阶段暂停讲解。

## 展示边界与来源（A01，2026-09-16）

本文件是算法原理动画，不是此前三种子实验的录像、逐包日志回放或性能预测。
演示一个发送/接收对，96个64 KiB chunk；最多8块在途。96及动画时间参数是讲解
场景设定，不是实验测量。每块运动使用简化独立进度，不包含共享服务竞争、
交换机队列、包级协议、完整collective依赖或tensor计算。

- E01：`astra-sim-alibabacloud/astra-sim/network_frontend/ns3/limer_split_policy.h`
  的 `ChunkComplete`、`Select`。支持“最新完成速率”“每次累加字节额度”“取最大额度，
  平局选A，扣除整块字节”“保留负额度”的算法描述。
- E02：同目录 `limer_split_runtime.h` 的 `Pump`。支持“初始两块，任一完成反馈后
  扩展活动额度”的描述。不得解读为必须等待两条rail的探测都完成。
- E03：动画脚本的 `arrival`、`build`。人为设定第9演示秒B变慢；正常数据行程2秒、
  慢速6秒、ACK行程0.65秒。显示速率为KiB/演示秒，不是Gb/s；不使用实验毫秒。
- 解读：只有完成样本变慢后才降低B权重；已分配chunk不迁移。快路径更多承接后续
  数据是一种效率改善机制，不等于硬断链恢复、SLO达标或数值正确性证明。

制作：AI辅助编写原创HTML/CSS/SVG/JavaScript，无外部图像资产、字体或CDN。
人工展示批准状态：待用户审阅；代码事实已由助手读取源文件核对，不能冒称人工核验。
内容边界检查使用本地 `scientific-writing` 技能；它使本图显式区分原理示意与实测证据。
技能提供的参考条目（未作外部书目核验，正式引用前请核验）：Kassis et al. (2026),
*Scientific Agent Skills: A Library of Procedural Knowledge for Research Agents*,
arXiv:2609.00065。

## 已执行检查

在V8 JavaScript环境中执行HTML的实际脚本，使用最小DOM替身：

- 所有96块恰好分配并收到完成反馈；最大在途数8。
- 首对探测同一时刻、不同rail；慢速反馈在减速时刻之后才到达。
- 稳定慢速样本后A权重约71.5%，来自上述示意耗时，非预设75%。
- 从0到结束每0.1秒调用绘制函数，未出现运行错误。

当前执行环境没有浏览器，尚未进行实际浏览器截图或人工视觉验收。
请在组会使用的浏览器打开后检查字体、全屏与投影可读性。
