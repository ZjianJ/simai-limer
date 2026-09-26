# LIMER 完整拓扑交互动画 / Full topology explorer

展示 ID：T01，版本 1，2026-09-17。用途：组会中的物理拓扑与可行数据路径讲解。
不是新的仿真结果、逐包回放或论文性能图。人工展示批准：待用户审阅。

## 打开与操作

直接用浏览器打开 [topology_explorer.html](topology_explorer.html)。必须把
`topology_model.js`、`topology_explorer.js` 与 HTML 保留在同一目录；不需要安装依赖、
启动服务器或访问网络。默认中文，可即时切换 English，保留当前进度、选择与播放状态。
若复制到组会电脑，可下载 [离线演示包](limer_topology_demo.zip)，完整解压后再打开 HTML。

- 点击“完整导览”：按八个章节播放约 72 演示秒；这些秒数不是仿真时延。
- 点击章节：全景 → GPU 双接入 → 单个核心 → 单个 ASW → 服务器内 → 同 ASW → 跨核心 → 双 rail。
- 点击任意 GPU、NVSwitch、ASW 或 PSW：仅高亮其**直接物理邻居**；右侧邻居按钮可继续探索。
- 自选源/目标 GPU、A/B/双平面与核心索引，然后点击“演示所选路径”。
- 核心索引 0–31 分别映射为 A 的节点 28–59、B 的节点 60–91。
- 支持暂停、拖动进度、0.5/1/1.5/2 倍速、全屏、平面筛选、连线显隐和 100–200% 缩放。
- 小屏横向滚动拓扑；所有节点始终存在，不把核心折叠为一个云。
- 空格播放/暂停，左右方向键调整两演示秒；设备可用 Tab 与 Enter/空格选择。

完整图默认画出 304 条物理边。焦点模式会淡化无关边，但不改变图本身。
平面筛选隐藏某条演示路径时，路径说明同步提示已隐藏。

## 连接事实与来源

本页只使用本地项目文件，不上传源数据或部署网站。

| 证据 ID | 来源与位置 | 支持的内容 |
|---|---|---|
| E01 | `limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100`，首行和全部 304 条边 | 节点、物理连接、标称速率、传播时延 |
| E02 | `astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py`，`Rail_Opti_DualToR_DualPlane` | GPU 按槽位接入、两平面 ASW–PSW 分别全连接 |
| E03 | `ns-3-alibabacloud/simulation/src/point-to-point/model/switch-node.cc`，`SwitchNode::GetOutDev` | 平面内部候选路由使用流头信息进行哈希选择 |
| E04 | 本目录 `topology_model.js`、`topology_explorer.js` | 图的确定性展开、可行路径与人为动画编排 |

E01 SHA256：
`c0b59d59b9f02a468601b8d20e186936262323318b39d8216d0a31841ba52fac`。
代码与文件核对由 AI 助手及自动测试完成，不冒称已由人工科学审核。

### 数值核对记录

| ID | 拓扑事实 | 来源 |
|---|---|---|
| N01 | 16 GPU，4 台逻辑服务器，每服务器 4 GPU；服务器不是新增图节点 | E01、E02 |
| N02 | 4 NVSwitch + 8 ASW + 64 PSW + 16 GPU = 92 节点 | E01 |
| N03 | 16 GPU–NVSwitch + 32 ACCESS + 256 ASW–PSW = 304 物理链路 | E01 |
| N04 | 每平面 4 ASW × 32 PSW = 128 条骨干链路 | E01、E02 |
| N05 | 每 GPU 的 NVSwitch 链路 2400 Gbps / 25 ns；A/B ACCESS 各 100 Gbps / 500 ns | E01 |
| N06 | ASW–PSW 每条 400 Gbps / 500 ns；速率非实测 goodput | E01 |
| N07 | 每个 PSW 有 4 ASW 邻居；每个 ASW 有 32 PSW + 4 GPU 邻居 | E01 |

所有链路双向。A、B 的交换机子图不相连；无 ASW–ASW 或 PSW–PSW 边。
例如 GPU0/4/8/12 共同连接 ASW20 和 ASW24；不是一台服务器独占一对 ASW。

## 演示边界

- 当前图的 32 个核心是**每个平面同一层的并行候选**，不是串联路径，也不是每个 ASW 专属 8 台。
- 自选路径保证由真实图中的边组成，但不重建某次 QP 的实际 ECMP 哈希。
  下拉框不是调度器真正使用的核心选择接口。
- 同服务器默认演示本地 NVSwitch 路径；跨服务器同槽位可经同一 ASW；不同槽位经一台核心。
- 双平面演示仅表示同一端点对存在两条可并行使用的数据路径。光点数量不代表实际 chunk 比例、QP 数量或负载。
- 不展示 ACK 回程；实际 ACK 路由不应无依据地画成原数据路径逆序。
- 不运行 AllReduce、不表达它的全部逻辑环、chunk 依赖或真实张量运算。
- 不模拟序列化耗时、共享队列、PFC、ECN、拥塞、丢包、故障、重传或恢复。
- 布局不表示地理位置；颜色、光点速度和 72 秒章节长度都是教学设计参数。
- 不提供性能增益、检测时延、恢复 SLO 或数值正确性的证据。

## 检查与复现

从 `SimAI` 目录运行：

```bash
python3 -m unittest discover -s limer/tests -p 'test_topology_animation_*.py' -v
```

`test_topology_animation_model.py` 会校验冻结拓扑的 SHA256、全部 304 条边的
端点/速率/时延、节点度数与路径规则。Node 若不可用，Node 执行用例明确跳过，
不能把 Python 参考实现的成功当作真实 JavaScript 执行成功。

`test_topology_animation_ui.py` 检查 HTML 控件、ID、离线资源与脚本引用。
`topology_animation_ui_harness.js` 提供最小 DOM 替身，可在 V8 环境执行实际页面
JavaScript；它不渲染像素，也不代替实际浏览器的字体、布局、键盘可访问性及全屏检查。
本轮执行结果记录在交付说明中。当前环境没有浏览器；**浏览器视觉验收尚未完成**。

2026-09-17 检查记录：[topology_explorer_qa.json](topology_explorer_qa.json)。
Python 共 12 项，10 通过、2 项因缺少 Node 明确跳过；另外在 V8 中执行实际模型与
页面脚本，53 项检查全部通过，包括全部 92 个设备的邻居、16,384 个候选路径组合、
中英文状态保持及完整 72 秒导览。全屏仅检查替身接口和失败提示，未验证浏览器全屏。

没有运行新仿真、改变任何算法/拓扑输入、修改故障时间表或提交 GitHub。

## 制作、素材与审核

AI 辅助编写原创 HTML/CSS/SVG/JavaScript，无外部图片、字体、CDN 或第三方绘图资产。
原始拓扑未被改动；变换仅为展开节点/边、屏幕布局、高亮及示意光点。
颜色之外还提供设备编号、节点类型、路径文本、B 平面虚线高亮与不同光点形状。

`scientific-writing` 技能用于来源记录、数值一致性与“拓扑事实/示意动画”的边界核对；
不把技能或自动检查作为人工批准。制作方法参考：Timothy Kassis, Vinayak Agarwal,
Yuhuan He, Darshil Patel, and Aubrey M. Brueckner (2026),
*Scientific Agent Skills: A Library of Procedural Knowledge for Research Agents*,
[arXiv:2609.00065](https://arxiv.org/abs/2609.00065)。2026-09-17 核对公开记录，当前为 v2；
这是一条制作工具参考，不是本项目性能论据。
