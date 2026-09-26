/* LIMER topology presentation. Motion is illustrative, never simulator time. */
(function () {
  'use strict';
  const graph = globalThis.LimerTopology;
  const $ = id => document.getElementById(id);
  if (!graph) {
    $('captionTitle').textContent = '缺少 topology_model.js / Missing topology_model.js';
    $('captionText').textContent = '请保留三个动画文件在同一目录。Keep the HTML and both JavaScript files in one directory.';
    return;
  }
  const NS = 'http://www.w3.org/2000/svg';
  const COLORS = { A: '#53e1c2', B: '#96b5ff', local: '#ffd18b' };
  const CUES = [0, 8, 16, 24, 32, 42, 52, 62];
  const DURATION = 72;
  const state = { lang: 'zh', time: 0, playing: false, tour: true, chapter: 0,
    scene: 'overview', selected: null, src: 0, dst: 5, rail: 'A', spine: 0,
    showLinks: true, filter: 'all', speed: 1, zoom: 1 };
  const chapterNames = [
    ['全景', 'Overview'], ['GPU 双接入', 'GPU attachments'], ['核心的邻居', 'One spine'],
    ['ASW 的邻居', 'One ASW'], ['服务器内', 'Same server'], ['同一 ASW', 'Same ASW'],
    ['跨核心', 'Across a spine'], ['双 rail 并行', 'Two rails']
  ];
  const chapterText = [
    ['这是同一批 16 个 GPU 的两套网络。上方为 A，下方为 B；每平面 32 台核心同层并列。服务器方框只是分组，不是额外节点。',
      'These are two fabrics for the same 16 GPUs: A above, B below. Each plane has 32 parallel spines in one tier. Server boxes are groups, not additional nodes.'],
    ['GPU0 有三个邻居：本地 NVSwitch16、平面 A 的 ASW20、平面 B 的 ASW24。每个 GPU 都接入两个平面，不是把 16 个 GPU 分成两组。',
      'GPU0 has three neighbors: local NVSwitch16, ASW20 in A, and ASW24 in B. Every GPU attaches to both fabrics; the GPUs are not split between them.'],
    ['看 PSW28：它只连接 A 平面的 ASW20、21、22、23。其他 31 台核心也分别连接这四台 ASW。核心之间没有直连，也不是 32 台串联。',
      'PSW28 connects only to ASW20, 21, 22 and 23 in plane A. Each of the other 31 spines independently connects to the same four ASWs. Spines have no direct links to each other.'],
    ['看 ASW20：下接 GPU0、4、8、12，上接全部 32 台 A 平面核心。所以它有 36 个物理邻居；不是独占其中 8 台核心。',
      'ASW20 connects to GPU0, 4, 8 and 12 below and all 32 A-plane spines above: 36 physical neighbors. No subset of eight spines is dedicated to this ASW.'],
    ['GPU0 → GPU1：两者同属服务器 S0，经 NVSwitch16 通信；无需经过 ASW 或任何网络核心。金色光点仅示意传输方向。',
      'GPU0 → GPU1 stays inside server S0 via NVSwitch16. No ASW or fabric spine is traversed. Gold pulses illustrate direction only.'],
    ['GPU0 → GPU4：它们在不同服务器，但都接入 ASW20；路径是 0 → 20 → 4。跨服务器通信不一定需要经过核心。',
      'GPU0 → GPU4 crosses servers but both GPUs attach to ASW20: 0 → 20 → 4. Cross-server traffic does not always need a spine.'],
    ['GPU0 → GPU5：从 ASW20 经一台核心到 ASW21，再到目标 GPU。这里示例选择 PSW28；其余 31 台也是等价候选，不会被依次经过。',
      'GPU0 → GPU5 goes from ASW20 through one spine to ASW21. PSW28 is the illustrated choice; the other 31 are alternatives, not extra serial hops.'],
    ['同一个 GPU 对可以同时在 A、B 分配不同的 chunk。分流策略选平面，平面内部选核心。本图展示两条可用数据路径，不演示 ACK、故障恢复或实际分流比例。',
      'The same GPU pair can send different chunks over A and B concurrently. The splitter chooses a plane; routing within that plane chooses a spine. No ACKs, recovery or measured split ratio are replayed.']
  ];
  const nodeMap = new Map(graph.nodes.map(node => [node.id, node]));
  const positions = new Map();
  const nodeEls = new Map();
  const edgeEls = new Map();
  let activeRoutes = [];
  let particleEls = [];
  let lastFrame = null;
  const tr = (zh, en) => state.lang === 'zh' ? zh : en;
  const color = plane => COLORS[plane || 'local'];
  const edgeId = (a, b) => `L${Math.min(a, b)}-${Math.max(a, b)}`;
  const label = id => {
    const node = nodeMap.get(id);
    return `${{ gpu: 'GPU', nv: 'NVSwitch', asw: 'ASW', psw: 'PSW' }[node.kind]}${id}`;
  };
  function svg(tag, attrs, parent, content) {
    const element = document.createElementNS(NS, tag);
    for (const [key, value] of Object.entries(attrs || {})) element.setAttribute(key, String(value));
    if (content !== undefined) element.textContent = content;
    if (parent) parent.appendChild(element);
    return element;
  }
  function wipe(element) { while (element.firstChild) element.removeChild(element.firstChild); }
  function position(node) {
    if (node.kind === 'gpu') return { x: Math.floor(node.id / 4) * 360 + 60 + (node.id % 4) * 80, y: 402, w: 62, h: 34 };
    if (node.kind === 'nv') return { x: (node.id - 16) * 360 + 180, y: 488, w: 130, h: 34 };
    if (node.kind === 'asw') return { x: ((node.id - 20) % 4) * 360 + 180, y: node.plane === 'A' ? 240 : 610, w: 112, h: 38 };
    return { x: 40 + (node.id - (node.plane === 'A' ? 28 : 60)) * (1360 / 31), y: node.plane === 'A' ? 102 : 748, w: 35, h: 30 };
  }
  // Each physical edge is drawn independently: no bundled edge implies a bus.
  function ends(a, b) {
    const p = positions.get(a), q = positions.get(b);
    function border(from, to) {
      const dx = to.x - from.x, dy = to.y - from.y;
      const amount = Math.min(dx === 0 ? Infinity : from.w / 2 / Math.abs(dx), dy === 0 ? Infinity : from.h / 2 / Math.abs(dy));
      return { x: from.x + dx * amount, y: from.y + dy * amount };
    }
    return [border(p, q), border(q, p)];
  }
  function edgePath(a, b) { const [p, q] = ends(a, b); return `M${p.x},${p.y} L${q.x},${q.y}`; }
  function buildGraph() {
    for (const node of graph.nodes) positions.set(node.id, position(node));
    const background = $('graphBackground');
    svg('rect', { x: 8, y: 8, width: 1424, height: 305, rx: 16, fill: '#0c282b', stroke: '#1e4b49' }, background);
    svg('rect', { x: 8, y: 542, width: 1424, height: 300, rx: 16, fill: '#12223d', stroke: '#2e4068' }, background);
    for (let server = 0; server < 4; server++) {
      svg('rect', { x: 8 + server * 360, y: 340, width: 344, height: 182, rx: 14, fill: '#132236', stroke: '#425a75' }, background);
      svg('text', { x: 26 + server * 360, y: 365, 'font-size': 13, fill: '#d9e9f7', id: `serverLabel${server}` }, background);
    }
    for (const [id, x, y, fill, className] of [
      ['planeALabel', 26, 35, COLORS.A, 'layer-label'], ['planeANote', 26, 58, null, 'layer-note'],
      ['planeBLabel', 26, 807, COLORS.B, 'layer-label'], ['planeBNote', 26, 829, null, 'layer-note'],
      ['aswALabel', 26, 295, COLORS.A, 'layer-note'], ['aswBLabel', 26, 567, COLORS.B, 'layer-note']
    ]) svg('text', { id, x, y, class: className, ...(fill ? { fill } : {}) }, background);
    for (const link of graph.links) {
      const path = svg('path', { id: `edge-${link.id}`, 'data-link': link.id, 'data-source': link.source, 'data-target': link.target,
        d: edgePath(link.source, link.target), class: 'physical-link', stroke: color(link.plane), 'stroke-width': 1,
        ...(link.kind === 'intra' ? { 'stroke-dasharray': '5 4' } : {}) }, $('graphLinks'));
      svg('title', {}, path, `${label(link.source)} ↔ ${label(link.target)} · ${link.gbps} Gbps · ${link.delayNs} ns`);
      edgeEls.set(link.id, path);
    }
    for (const node of graph.nodes) {
      const p = positions.get(node.id);
      const group = svg('g', { id: `node-${node.id}`, 'data-node': node.id, class: `node ${node.kind}`, tabindex: 0, role: 'button', 'aria-pressed': 'false' }, $('graphNodes'));
      svg('title', {}, group, label(node.id));
      svg('rect', { x: p.x - p.w / 2, y: p.y - p.h / 2, width: p.w, height: p.h, rx: node.kind === 'gpu' ? 8 : 4,
        class: 'node-box', fill: node.kind === 'gpu' ? '#263d56' : node.kind === 'nv' ? '#3d3424' : node.plane === 'A' ? '#163f3c' : '#25385a',
        stroke: node.kind === 'gpu' ? '#b5c7da' : color(node.plane), 'stroke-width': 1.3 }, group);
      svg('text', { x: p.x, y: p.y + 4, 'text-anchor': 'middle', fill: '#f3f9ff', 'font-size': node.kind === 'psw' ? 12 : 11.5 }, group,
        node.kind === 'psw' ? String(node.id) : label(node.id));
      group.addEventListener('click', () => selectNode(node.id));
      group.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); selectNode(node.id); } });
      nodeEls.set(node.id, group);
    }
  }
  function visible(node) { return !node.plane || state.filter === 'all' || node.plane === state.filter; }
  function routePaths() {
    if (!['local', 'same', 'cross', 'dual', 'custom'].includes(state.scene)) return [];
    const first = graph.route(state.src, state.dst, state.rail === 'B' ? 'B' : 'A', state.spine);
    const local = first.length <= 1 || first.some(id => nodeMap.get(id).kind === 'nv');
    if (local) return [{ plane: null, nodes: first }];
    if (state.rail === 'both') return ['A', 'B'].map(plane => ({ plane, nodes: graph.route(state.src, state.dst, plane, state.spine) }));
    return [{ plane: state.rail, nodes: first }];
  }
  function drawFocus() {
    const selected = state.selected;
    const neighborSet = new Set(selected === null ? [] : [selected, ...graph.neighbors(selected)]);
    const routeNodes = new Set(activeRoutes.flatMap(route => route.nodes));
    const routeEdges = new Set();
    for (const route of activeRoutes) for (let i = 1; i < route.nodes.length; i++) routeEdges.add(edgeId(route.nodes[i - 1], route.nodes[i]));
    const focused = selected !== null || activeRoutes.length > 0;
    for (const link of graph.links) {
      const selectedEdge = selected !== null && (link.source === selected || link.target === selected);
      const onRoute = routeEdges.has(link.id);
      const isVisible = visible(nodeMap.get(link.source)) && visible(nodeMap.get(link.target));
      const opacity = !isVisible ? 0 : selectedEdge || onRoute ? .95 : !state.showLinks ? 0 : focused ? .035 : link.kind === 'fabric' ? .16 : .32;
      const element = edgeEls.get(link.id);
      element.setAttribute('opacity', opacity);
      element.setAttribute('stroke-width', selectedEdge || onRoute ? 2.1 : link.kind === 'fabric' ? .8 : 1.2);
      element.setAttribute('data-highlighted', selectedEdge || onRoute ? 'true' : 'false');
    }
    for (const node of graph.nodes) {
      const element = nodeEls.get(node.id);
      const isVisible = visible(node);
      element.setAttribute('opacity', !isVisible ? .07 : focused && !neighborSet.has(node.id) && !routeNodes.has(node.id) ? .24 : 1);
      element.setAttribute('aria-pressed', node.id === selected ? 'true' : 'false');
      element.setAttribute('aria-label', `${label(node.id)} · ${graph.neighbors(node.id).length} ${tr('个物理邻居；按回车查看', 'physical neighbors; press Enter to inspect')}`);
      element.setAttribute('tabindex', isVisible ? 0 : -1);
      element.style.pointerEvents = isVisible ? '' : 'none';
      const box = element.querySelector('rect');
      box.setAttribute('stroke-width', node.id === selected ? 3 : 1.3);
      box.setAttribute('stroke', node.id === selected ? '#fff' : node.kind === 'gpu' ? '#b5c7da' : color(node.plane));
    }
  }
  function drawRoutes() {
    wipe($('graphRoutes')); wipe($('graphPackets')); particleEls = [];
    for (const route of activeRoutes) {
      if (route.plane && state.filter !== 'all' && route.plane !== state.filter) continue;
      for (let i = 1; i < route.nodes.length; i++) svg('path', { d: edgePath(route.nodes[i - 1], route.nodes[i]), class: 'route-overlay',
        stroke: color(route.plane), 'stroke-width': 3.5, opacity: .9, ...(route.plane === 'B' || !route.plane ? { 'stroke-dasharray': '7 4' } : {}) }, $('graphRoutes'));
      if (route.nodes.length > 1) for (let i = 0; i < 3; i++) {
        const group = svg('g', {}, $('graphPackets'));
        svg('circle', { r: 9, fill: color(route.plane), opacity: .17 }, group);
        if (route.plane === 'B') svg('rect', { x: -3.5, y: -3.5, width: 7, height: 7, rx: 1, fill: '#fff', stroke: color(route.plane), 'stroke-width': 1.5 }, group);
        else svg('circle', { r: 3.8, fill: '#fff', stroke: color(route.plane), 'stroke-width': 1.5 }, group);
        particleEls.push({ element: group, route, offset: i / 3 });
      }
    }
  }
  function particles() {
    for (const particle of particleEls) {
      const nodes = particle.route.nodes;
      const phase = (state.time / 4.2 + particle.offset) % 1;
      const edgePosition = phase * (nodes.length - 1);
      const index = Math.min(nodes.length - 2, Math.floor(edgePosition));
      const fraction = edgePosition - index;
      const [a, b] = ends(nodes[index], nodes[index + 1]);
      particle.element.setAttribute('transform', `translate(${a.x + (b.x - a.x) * fraction},${a.y + (b.y - a.y) * fraction})`);
    }
  }
  function selectionInfo() {
    $('selectionHeading').textContent = state.selected === null ? tr('点击设备，查看邻居', 'Select a device') : label(state.selected);
    const container = $('selectionBody'); wipe(container);
    if (state.selected === null) {
      const p = document.createElement('p');
      p.textContent = tr('试着点 PSW28：它有 4 个 ASW 邻居。再点 ASW20：它有 32 个核心和 4 个 GPU 邻居。点中的链路会高亮，其余淡化。',
        'Try PSW28: four ASW neighbors. Then ASW20: 32 spines and four GPUs. Selected connections brighten while unrelated links fade.');
      container.appendChild(p); return;
    }
    const node = nodeMap.get(state.selected), neighbors = graph.neighbors(node.id);
    const counts = {}; for (const id of neighbors) { const kind = nodeMap.get(id).kind; counts[kind] = (counts[kind] || 0) + 1; }
    const descriptions = {
      gpu: tr('一条本地 NVSwitch 链路 + A/B 各一条 ACCESS 链路。', 'One local NVSwitch link plus one ACCESS link to each plane.'),
      nv: tr('只连接本服务器的四个 GPU，不连接其他服务器或核心。', 'Connects only the four GPUs in its own server; no fabric-spine links.'),
      asw: tr('4 个 GPU 分别来自四台服务器；32 个核心都属于同一平面。', 'The four GPUs come from four servers; all 32 spines belong to the same plane.'),
      psw: tr('这四台 ASW 共享本核心；其他核心也各自连接相同的四台 ASW。无核心间直连。', 'This spine is shared by four ASWs. Every other spine separately connects to those same ASWs. No spine-to-spine links.')
    };
    const info = document.createElement('p');
    info.textContent = `${neighbors.length} ${tr('个物理邻居', 'physical neighbors')} · ${Object.entries(counts).map(([kind, count]) => `${count} ${{ gpu: 'GPU', nv: 'NVSwitch', asw: 'ASW', psw: 'PSW' }[kind]}`).join(' + ')}`;
    container.appendChild(info);
    const grid = document.createElement('div'); grid.className = 'neighbor-grid';
    for (const id of neighbors) { const button = document.createElement('button'); button.type = 'button'; button.textContent = label(id); button.addEventListener('click', () => selectNode(id)); grid.appendChild(button); }
    container.appendChild(grid);
    const note = document.createElement('p'); note.textContent = descriptions[node.kind]; container.appendChild(note);
  }
  function routeInfo() {
    const container = $('routeReadout'); wipe(container);
    if (!activeRoutes.length) {
      const p = document.createElement('div'); p.className = 'route-caption';
      p.textContent = tr('选择源、目标和平面，然后点击“演示所选路径”。也可用上方章节逐步讲解。', 'Choose endpoints and a plane, then trace the path. The chapter buttons provide a guided explanation.'); container.appendChild(p); return;
    }
    for (const route of activeRoutes) {
      const row = document.createElement('div'); row.className = `route-row ${route.plane === 'B' ? 'b' : route.plane ? '' : 'local'}`;
      const filtered = route.plane && state.filter !== 'all' && route.plane !== state.filter;
      row.textContent = `${route.plane ? `${tr('平面', 'Plane')} ${route.plane}` : tr('本地', 'Local')}: ${route.nodes.map(label).join(' → ')}${filtered ? tr('（已被视图筛选隐藏）', ' (hidden by view filter)') : ''}`;
      container.appendChild(row);
      const hasCore = route.nodes.some(id => nodeMap.get(id).kind === 'psw');
      const note = document.createElement('div'); note.className = 'route-caption';
      note.textContent = route.nodes.length === 1 ? tr('源和目标相同，无网络传输。', 'Identical endpoints: no network transfer.') : hasCore
        ? tr('经过 1 台核心、4 条物理链路；本平面有 32 个等价核心候选。', 'One spine, four physical links; 32 equivalent spine candidates in this plane.')
        : route.plane ? tr('跨服务器，但共享 ASW：2 条物理链路，不经过核心。', 'Cross-server, shared ASW: two physical links, no spine.')
        : tr('同服务器：经本地 NVSwitch 的 2 条链路，A/B 和核心选项不影响此路径。', 'Same server: two local NVSwitch links; plane and spine choices do not affect this path.');
      container.appendChild(note);
    }
  }
  function timeline() {
    const duration = state.tour ? DURATION : 10;
    $('seek').value = Math.round(state.time / duration * 1000);
    $('progressText').textContent = `${state.time.toFixed(1)} / ${duration} ${tr('演示秒', 'demo s')}`;
    $('play').textContent = state.playing ? tr('Ⅱ 暂停', 'Ⅱ Pause') : tr('▶ 播放', '▶ Play');
    $('play').setAttribute('aria-pressed', state.playing ? 'true' : 'false');
  }
  function render() {
    activeRoutes = routePaths();
    drawFocus(); drawRoutes(); particles(); selectionInfo(); routeInfo(); timeline();
    const buttons = $('chapters').querySelectorAll('button');
    buttons.forEach((button, index) => { button.classList.toggle('active', state.tour && index === state.chapter); button.setAttribute('aria-current', state.tour && index === state.chapter ? 'step' : 'false'); });
    if (state.tour) {
      $('captionTitle').textContent = `${String(state.chapter + 1).padStart(2, '0')} / ${tr(...chapterNames[state.chapter])}`;
      $('captionText').textContent = tr(...chapterText[state.chapter]);
    } else {
      $('captionTitle').textContent = state.scene === 'inspect' ? tr('自由探索 · 物理邻接关系', 'Explore · Physical neighbors') : state.scene === 'custom' ? tr('自选路径 · 正向数据示意', 'Custom path · Forward data illustration') : tr('全景 · 所有设备展开', 'Overview · Every device expanded');
      $('captionText').textContent = state.scene === 'inspect' ? tr('高亮的是所选设备直接相连的物理链路，不是它能间接到达的全部设备。点击邻居可以继续探索。', 'Highlighted links connect directly to the selected device, not to every indirectly reachable device. Select a neighbor to keep exploring.')
        : tr('A、B 平面之间没有交换机直连。自选核心仅用于展示可行路径；实际仿真使用流/QP 的 ECMP 哈希，不是这里的下拉框。', 'There are no switch links between A and B. The spine selector illustrates a valid path; actual simulation routing uses flow/QP ECMP hashing.');
    }
  }
  function syncSelectors() { $('src').value = String(state.src); $('dst').value = String(state.dst); $('rail').value = state.rail; $('spine').value = String(state.spine); $('planeFilter').value = state.filter; }
  function applyChapter(index) {
    state.chapter = index; state.tour = true; state.selected = [null, 0, 28, 20, null, null, null, null][index];
    state.scene = ['overview', 'inspect', 'inspect', 'inspect', 'local', 'same', 'cross', 'dual'][index];
    state.src = 0; state.dst = index === 4 ? 1 : index === 5 ? 4 : 5; state.rail = index === 7 ? 'both' : 'A'; state.spine = 0;
    state.filter = 'all'; syncSelectors();
  }
  function setChapter(index) {
    if (!Number.isInteger(index) || index < 0 || index >= CUES.length) throw new RangeError('Invalid chapter');
    state.time = CUES[index]; applyChapter(index); render();
  }
  function selectNode(id) {
    if (!nodeMap.has(id)) throw new RangeError('Unknown node');
    state.selected = id; state.tour = false; state.scene = 'inspect'; state.playing = false; state.time = 0;
    if (!visible(nodeMap.get(id))) { state.filter = 'all'; syncSelectors(); }
    render();
  }
  function showRoute(src = Number($('src').value), dst = Number($('dst').value), rail = $('rail').value, spine = Number($('spine').value)) {
    if (!['A', 'B', 'both'].includes(rail)) throw new RangeError('Invalid plane');
    graph.route(src, dst, rail === 'B' ? 'B' : 'A', spine); // Validate before changing state.
    Object.assign(state, { src, dst, rail, spine, selected: null, time: 0, tour: false, scene: 'custom', playing: true, filter: 'all' });
    syncSelectors(); render();
  }
  function setLanguage(language) {
    state.lang = language === 'en' ? 'en' : 'zh';
    document.documentElement.lang = state.lang === 'zh' ? 'zh-CN' : 'en';
    document.title = tr('LIMER · 完整拓扑动画', 'LIMER · Full Topology Explorer');
    const labels = {
      title: ['完整拓扑，一条路径看清楚', 'The whole fabric. One path at a time.'],
      subtitle: ['16 个 GPU · 双平面 · 所有核心展开，不再折叠。', '16 GPUs · Two planes · Every spine visible. Nothing collapsed.'],
      language: ['English', '中文'], fullscreen: ['全屏', 'Fullscreen'], networkTitle: ['真实连接 · 92 节点 / 304 链路', 'Physical graph · 92 nodes / 304 links'],
      showLinksLabel: ['显示全部连线', 'All links'], planeFilterLabel: ['视图', 'View'], zoomLabel: ['缩放', 'Zoom'],
      legendA: ['A 平面 · 实线高亮', 'Plane A · Solid highlight'], legendB: ['B 平面 · 虚线高亮', 'Plane B · Dashed highlight'],
      legendLocal: ['NVSwitch · 2400 Gbps', 'NVSwitch · 2400 Gbps'], legendUnits: ['ACCESS 100 Gbps · ASW–PSW 400 Gbps', 'ACCESS 100 Gbps · ASW–PSW 400 Gbps'],
      routeTitle: ['自己选一条通信路径', 'Trace your own path'], srcLabel: ['源 GPU', 'Source GPU'], dstLabel: ['目标 GPU', 'Destination GPU'],
      railLabel: ['数据平面', 'Data plane'], spineLabel: ['核心候选编号', 'Spine candidate index'], trace: ['▶ 演示所选路径', '▶ Trace this path'],
      ecmpNote: ['索引 0～31 对应 A 的 PSW28～59 / B 的 PSW60～91。下拉框仅选择示意路径，不模拟真实 ECMP 哈希。', 'Index 0–31 maps to PSW28–59 in A / PSW60–91 in B. This illustrates a candidate, not an actual ECMP hash result.'],
      clearSelection: ['清除', 'Clear'], restart: ['↺ 完整导览', '↺ Full tour'],
      scopeNote: ['连接关系来自实际仿真拓扑；移动光点只是正向数据传输示意，不是逐包日志、ACK 回放或性能测量。动画秒数不代表仿真时延。', 'Connections match the simulator topology. Moving pulses illustrate forward data, not packet logs, ACK replay or measured performance. Animation seconds are not simulation latency.'],
      provenanceTitle: ['拓扑来源与展示边界', 'Topology source & scope'], keyboardNote: ['空格：播放/暂停 · ← →：进度 ±2 秒 · Tab / 回车：选择设备 · 小屏可横向滚动拓扑', 'Space: play/pause · ← →: seek ±2 s · Tab / Enter: inspect devices · Scroll the graph horizontally on small screens'],
      workflowLink: ['仿真实验流程', 'Simulation workflow'], algorithmLink: ['分流算法动画', 'Splitting algorithm'], scopeLink: ['使用与核对说明', 'Guide & provenance'],
      svgTitle: ['LIMER 完整双平面拓扑', 'LIMER full dual-plane topology'],
      svgDesc: ['四台服务器内共16个GPU和4台NVSwitch。每个GPU接入A、B各一次；每平面4台ASW分别连接全部32台同层并列核心。选择设备查看邻居，选择路径观看示意动画。', 'Four servers contain 16 GPUs and four NVSwitches. Each GPU attaches once to each plane. In each plane every one of four ASWs connects to all 32 parallel spines. Select devices to inspect neighbors or trace illustrative data paths.'],
      planeALabel: ['平面 A  ·  PSW28–59  ·  32 台核心同层并列，彼此不直连', 'PLANE A  ·  PSW28–59  ·  32 parallel spines; no spine-to-spine links'],
      planeANote: ['每台核心连接下面全部 4 台 ASW；每条链路 400 Gbps。核心方框内显示节点 ID。', 'Each spine connects to all four ASWs below at 400 Gbps per link. Boxes show node IDs.'],
      planeBLabel: ['平面 B  ·  PSW60–91  ·  同样的结构，独立的交换机与链路', 'PLANE B  ·  PSW60–91  ·  Same structure, separate switches and links'],
      planeBNote: ['每台核心连接上面全部 4 台 ASW；上下布局仅为展示，A 与 B 之间没有交换机直连。', 'Each spine connects to all four ASWs above. Vertical placement is illustrative; there are no A–B switch links.'],
      aswALabel: ['ASW20–23  ·  每台下接 4 个跨服务器 GPU，上接全部 32 台核心', 'ASW20–23  ·  Each connects to four GPUs across servers and all 32 spines'],
      aswBLabel: ['ASW24–27  ·  接入同一批 16 个 GPU，而不是另外 16 个', 'ASW24–27  ·  Attach to the same 16 GPUs, not a second set']
    };
    for (const [id, pair] of Object.entries(labels)) $(id).textContent = tr(...pair);
    for (let server = 0; server < 4; server++) $(`serverLabel${server}`).textContent = `${tr('服务器', 'SERVER')} S${server}  ·  GPU${server * 4}–${server * 4 + 3}`;
    $('seek').setAttribute('aria-label', tr('演示进度', 'Presentation progress'));
    $('speed').setAttribute('aria-label', tr('播放速度', 'Playback speed'));
    $('language').setAttribute('aria-label', tr('Switch to English', '切换到中文'));
    $('chapters').setAttribute('aria-label', tr('分步导览章节', 'Guided tour chapters'));
    $('facts').setAttribute('aria-label', tr('拓扑数量', 'Topology counts'));
    wipe($('facts'));
    for (const [number, zh, en] of [[16, 'GPU / 4 台服务器', 'GPUs / 4 servers'], [4, '本地 NVSwitch', 'local NVSwitches'], [8, 'ASW · 每平面 4 台', 'ASWs · 4 per plane'], [64, '核心 · 每平面 32 台', 'spines · 32 per plane'], [304, '双向物理链路', 'bidirectional physical links']]) {
      const fact = document.createElement('span'); fact.className = 'fact'; const strong = document.createElement('strong'); strong.textContent = String(number); fact.appendChild(strong); fact.appendChild(document.createTextNode(tr(zh, en))); $('facts').appendChild(fact);
    }
    wipe($('chapters'));
    chapterNames.forEach((pair, index) => { const button = document.createElement('button'); button.type = 'button'; button.textContent = `${String(index + 1).padStart(2, '0')} ${tr(...pair)}`; button.addEventListener('click', () => setChapter(index)); $('chapters').appendChild(button); });
    wipe($('provenanceBody'));
    for (const text of [
      tr('来源：', 'Source: ') + graph.source.path,
      `SHA256: ${graph.source.sha256}`,
      tr('链路核对：16 条 GPU–NVSwitch + 32 条 ACCESS + 256 条 ASW–PSW = 304。每条物理链路双向；不是 608 条独立物理链路。', 'Link audit: 16 GPU–NVSwitch + 32 ACCESS + 256 ASW–PSW = 304. Each link is bidirectional, not two separate physical links.'),
      tr('位置、颜色、光点数量及运动速度均为讲解编排。这里不运行 AllReduce、不模拟队列与拥塞，也不证明恢复时延或数值正确性。AllReduce 的逻辑环不是本图的物理布线。', 'Layout, color, pulse count and motion speed are presentation choices. No AllReduce, queueing or congestion is simulated here; this is not recovery-latency or numerical-correctness evidence. An AllReduce logical ring is not the physical wiring.'),
      tr('制作：AI 辅助编写的本地 HTML/CSS/SVG/JavaScript；不使用外部素材、字体或 CDN。源文件连接与代码已做自动核对；人工展示批准待用户审阅。', 'Created with AI-assisted local HTML/CSS/SVG/JavaScript; no external artwork, fonts or CDN. Automated source and code checks do not replace human review; presentation approval is pending.')
    ]) { const p = document.createElement('p'); p.textContent = text; $('provenanceBody').appendChild(p); }
    render();
  }
  function toggle() {
    if (state.tour && state.time >= DURATION) setChapter(0);
    state.playing = !state.playing; lastFrame = null; timeline();
  }
  function seek(time) {
    state.time = Math.max(0, Math.min(state.tour ? DURATION : 10, time));
    if (state.tour) {
      let index = 0; for (let i = 0; i < CUES.length; i++) if (state.time >= CUES[i]) index = i;
      if (index !== state.chapter) applyChapter(index);
      if (state.time >= DURATION) state.playing = false;
    }
    render();
  }
  function frame(now) {
    if (lastFrame !== null && state.playing) {
      // Ignore long background-tab gaps; display time is not a measured clock.
      const delta = Math.min(.15, Math.max(0, (now - lastFrame) / 1000)) * state.speed;
      state.time += delta;
      if (state.tour) {
        state.time = Math.min(DURATION, state.time);
        let index = 0; for (let i = 0; i < CUES.length; i++) if (state.time >= CUES[i]) index = i;
        if (index !== state.chapter) { applyChapter(index); render(); }
        if (state.time >= DURATION) state.playing = false;
      } else state.time %= 10;
      particles(); timeline();
    }
    lastFrame = now; requestAnimationFrame(frame);
  }
  for (const id of ['src', 'dst']) for (let gpu = 0; gpu < 16; gpu++) { const option = document.createElement('option'); option.value = String(gpu); option.textContent = `GPU${gpu}`; $(id).appendChild(option); }
  for (let index = 0; index < 32; index++) { const option = document.createElement('option'); option.value = String(index); option.textContent = `${index} · A:${28 + index} / B:${60 + index}`; $('spine').appendChild(option); }
  buildGraph(); syncSelectors(); setLanguage('zh');
  $('language').addEventListener('click', () => setLanguage(state.lang === 'zh' ? 'en' : 'zh'));
  $('play').addEventListener('click', toggle);
  $('restart').addEventListener('click', () => { setChapter(0); state.playing = true; lastFrame = null; timeline(); });
  $('trace').addEventListener('click', () => showRoute());
  $('clearSelection').addEventListener('click', () => { state.selected = null; if (state.scene === 'inspect') { state.tour = false; state.scene = 'overview'; state.time = 0; state.playing = false; } render(); });
  $('seek').addEventListener('input', event => seek(Number(event.target.value) / 1000 * (state.tour ? DURATION : 10)));
  $('speed').addEventListener('change', event => { state.speed = Number(event.target.value); });
  $('showLinks').addEventListener('change', event => { state.showLinks = event.target.checked; render(); });
  $('planeFilter').addEventListener('change', event => { state.filter = event.target.value; render(); });
  $('zoom').addEventListener('change', event => { state.zoom = Number(event.target.value); $('network').style.width = `${state.zoom * 100}%`; });
  $('fullscreen').addEventListener('click', async () => {
    try {
      if (document.fullscreenElement) await document.exitFullscreen();
      else if (document.documentElement.requestFullscreen) await document.documentElement.requestFullscreen();
      else throw new Error('Fullscreen unavailable');
    } catch (_) { $('fullscreen').textContent = tr('请用浏览器全屏', 'Use browser fullscreen'); }
  });
  document.addEventListener('keydown', event => {
    if (event.altKey || event.ctrlKey || event.metaKey || ['INPUT', 'SELECT', 'BUTTON', 'TEXTAREA'].includes((event.target.tagName || '').toUpperCase()) || event.target.isContentEditable || event.target.getAttribute('role') === 'button') return;
    if (event.code === 'Space') { event.preventDefault(); toggle(); }
    if (event.code === 'ArrowLeft' || event.code === 'ArrowRight') { event.preventDefault(); seek(state.time + (event.code === 'ArrowRight' ? 2 : -2)); }
  });
  globalThis.topologyExplorer = { state, graph, render, setLanguage, selectNode, showRoute, setChapter, frame, routePaths, seek, positions };
  requestAnimationFrame(frame);
})();
