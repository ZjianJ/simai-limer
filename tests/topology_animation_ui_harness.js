/* Minimal DOM substitute for executing the real topology presentation scripts.
 * This checks program behavior; it does NOT render pixels, measure text, prove
 * browser accessibility, or test a real browser's fullscreen implementation.
 * No npm, browser, DOM library, network access, or simulation is required.
 */
(function (root) {
  'use strict';

  function makeDom(html) {
    const registered = new Map();
    const frameCallbacks = new Map();
    let nextFrame = 1;
    let document;

    function matches(element, selector) {
      if (selector.includes(',')) {
        return selector.split(',').some(item => matches(element, item.trim()));
      }
      const attribute = selector.match(/\[([^=\]]+)(?:=["']?([^\]"']+)["']?)?\]/);
      if (attribute && (!element.hasAttribute(attribute[1]) ||
          (attribute[2] !== undefined && element.getAttribute(attribute[1]) !== attribute[2]))) return false;
      const simple = selector.replace(/\[[^\]]+\]/g, '');
      const id = simple.match(/#([\w-]+)/);
      if (id && element.id !== id[1]) return false;
      const classes = [...simple.matchAll(/\.([\w-]+)/g)].map(match => match[1]);
      if (classes.some(name => !element.classList.contains(name))) return false;
      const tag = simple.match(/^[\w-]+/);
      return !tag || element.tagName === tag[0].toUpperCase();
    }

    class Element {
      constructor(tag) {
        this.tagName = tag.toUpperCase();
        this.localName = tag;
        this.children = [];
        this.parentNode = null;
        this.attributes = {};
        this.style = {setProperty(name, value) { this[name] = String(value); }};
        this.dataset = {};
        this.listeners = {};
        this._text = '';
        this._html = '';
        this._value = '';
        this.checked = false;
        this.disabled = false;
        this.hidden = false;
        this.clientWidth = 1440;
        this.clientHeight = 850;
        this.classList = {
          add: (...names) => this.updateClasses(names, []),
          remove: (...names) => this.updateClasses([], names),
          contains: name => (this.attributes.class || '').split(/\s+/).includes(name),
          toggle: (name, force) => {
            const enabled = force === undefined ? !this.classList.contains(name) : !!force;
            if (enabled) this.classList.add(name); else this.classList.remove(name);
            return enabled;
          },
        };
      }
      updateClasses(add, remove) {
        const names = new Set((this.attributes.class || '').split(/\s+/).filter(Boolean));
        for (const name of add) names.add(name);
        for (const name of remove) names.delete(name);
        this.attributes.class = [...names].join(' ');
      }
      get id() { return this.attributes.id || ''; }
      set id(value) { this.setAttribute('id', value); }
      get className() { return this.attributes.class || ''; }
      set className(value) { this.setAttribute('class', value); }
      get ownerDocument() { return document; }
      get firstChild() { return this.children[0] || null; }
      get lastChild() { return this.children[this.children.length - 1] || null; }
      get childNodes() { return this.children; }
      get options() { return this.tagName === 'SELECT' ? this.children : undefined; }
      get value() { return this._value; }
      set value(value) { this._value = String(value); }
      get textContent() { return this._text + this.children.map(node => node.textContent).join(''); }
      set textContent(value) { this.replaceChildren(); this._text = String(value); }
      get innerHTML() { return this._html; }
      set innerHTML(value) {
        this.replaceChildren();
        this._html = String(value);
        parseMarkup(this._html, this);
      }
      setAttribute(name, value) {
        const text = String(value);
        this.attributes[name] = text;
        if (name === 'id') registered.set(text, this);
        if (name === 'value') this.value = text;
        if (name === 'checked') this.checked = true;
        if (name === 'disabled') this.disabled = true;
        if (name.startsWith('data-')) {
          const key = name.slice(5).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
          this.dataset[key] = text;
        }
      }
      getAttribute(name) { return Object.hasOwn(this.attributes, name) ? this.attributes[name] : null; }
      hasAttribute(name) { return Object.hasOwn(this.attributes, name); }
      removeAttribute(name) { delete this.attributes[name]; }
      appendChild(child) {
        if (child.parentNode) child.parentNode.removeChild(child);
        child.parentNode = this;
        this.children.push(child);
        if (this.tagName === 'SELECT' && child.tagName === 'OPTION' &&
            (this.children.length === 1 || child.hasAttribute('selected'))) this.value = child.value;
        return child;
      }
      append(...items) {
        for (const item of items) this.appendChild(typeof item === 'string' ? document.createTextNode(item) : item);
      }
      removeChild(child) {
        const index = this.children.indexOf(child);
        if (index >= 0) this.children.splice(index, 1);
        child.parentNode = null;
        return child;
      }
      replaceChildren(...items) {
        for (const child of this.children) child.parentNode = null;
        this.children = [];
        this._text = '';
        this._html = '';
        this.append(...items);
      }
      remove() { if (this.parentNode) this.parentNode.removeChild(this); }
      querySelectorAll(selector) {
        const found = [];
        const visit = element => {
          for (const child of element.children) {
            if (matches(child, selector)) found.push(child);
            visit(child);
          }
        };
        visit(this);
        return found;
      }
      querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
      closest(selector) {
        for (let current = this; current; current = current.parentNode) {
          if (matches(current, selector)) return current;
        }
        return null;
      }
      addEventListener(type, listener) { (this.listeners[type] ||= []).push(listener); }
      removeEventListener(type, listener) {
        this.listeners[type] = (this.listeners[type] || []).filter(item => item !== listener);
      }
      dispatchEvent(event) {
        event.target ||= this;
        event.currentTarget = this;
        event.preventDefault ||= () => { event.defaultPrevented = true; };
        event.stopPropagation ||= () => {};
        const property = this['on' + event.type];
        if (typeof property === 'function') property.call(this, event);
        for (const listener of this.listeners[event.type] || []) listener.call(this, event);
        return !event.defaultPrevented;
      }
      click() {
        if (this.tagName === 'INPUT' && this.getAttribute('type') === 'checkbox') this.checked = !this.checked;
        this.dispatchEvent({type: 'click'});
      }
      focus() { document.activeElement = this; }
      scrollIntoView() {}
      getBoundingClientRect() { return {x: 0, y: 0, left: 0, top: 0, width: this.clientWidth, height: this.clientHeight}; }
      requestFullscreen() { document.fullscreenElement = this; return Promise.resolve(); }
    }

    const voidTags = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr']);
    function parseMarkup(markup, parent) {
      const stack = [parent];
      const tokens = markup.match(/<!--[\s\S]*?-->|<![^>]*>|<[^>]+>|[^<]+/g) || [];
      for (const token of tokens) {
        if (token.startsWith('<!')) continue;
        if (token.startsWith('</')) {
          if (stack.length > 1) stack.pop();
        } else if (token.startsWith('<')) {
          const start = token.match(/^<([\w:-]+)/);
          if (!start) continue;
          const element = new Element(start[1]);
          const rest = token.slice(start[0].length, token.length - 1).replace(/\/$/, '');
          const expression = /([^\s=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"']+)))?/g;
          for (const attribute of rest.matchAll(expression)) {
            element.setAttribute(attribute[1], attribute[2] ?? attribute[3] ?? attribute[4] ?? '');
          }
          stack[stack.length - 1].appendChild(element);
          if (!token.endsWith('/>') && !voidTags.has(start[1].toLowerCase())) stack.push(element);
        } else {
          stack[stack.length - 1]._text += token;
        }
      }
    }

    const container = new Element('document');
    document = {
      body: container,
      documentElement: container,
      activeElement: null,
      fullscreenElement: null,
      createElement: tag => new Element(tag),
      createElementNS: (_namespace, tag) => new Element(tag),
      createTextNode: value => { const node = new Element('#text'); node.textContent = value; return node; },
      getElementById: id => registered.get(id) || null,
      querySelector: selector => container.querySelector(selector),
      querySelectorAll: selector => container.querySelectorAll(selector),
      addEventListener: (...args) => container.addEventListener(...args),
      removeEventListener: (...args) => container.removeEventListener(...args),
      dispatchEvent: (...args) => container.dispatchEvent(...args),
      exitFullscreen: () => { document.fullscreenElement = null; return Promise.resolve(); },
    };
    parseMarkup(html, container);
    document.documentElement = container.querySelector('html') || container;
    document.body = container.querySelector('body') || container;
    return {
      document,
      Element,
      frameCallbacks,
      requestAnimationFrame: callback => { const id = nextFrame++; frameCallbacks.set(id, callback); return id; },
      cancelAnimationFrame: id => frameCallbacks.delete(id),
    };
  }

  root.createTopologyAnimationTestDom = makeDom;

  root.mountTopologyAnimationForTest = function (html, modelSource, uiSource) {
    const dom = makeDom(html);
    const replacements = {
      document: dom.document,
      window: root,
      HTMLElement: dom.Element,
      SVGElement: dom.Element,
      requestAnimationFrame: dom.requestAnimationFrame,
      cancelAnimationFrame: dom.cancelAnimationFrame,
      matchMedia: () => ({matches: false, addEventListener() {}, removeEventListener() {}}),
      addEventListener: (...args) => dom.document.addEventListener(...args),
      removeEventListener: (...args) => dom.document.removeEventListener(...args),
      performance: {now: () => 0},
    };
    const names = [...Object.keys(replacements), 'LimerTopology', 'topologyExplorer'];
    const previous = new Map(names.map(name => [name, Object.getOwnPropertyDescriptor(root, name)]));
    function restore() {
      for (const [name, descriptor] of previous) {
        if (descriptor) Object.defineProperty(root, name, descriptor);
        else delete root[name];
      }
    }
    try {
      for (const [name, value] of Object.entries(replacements)) {
        Object.defineProperty(root, name, {value, configurable: true, writable: true});
      }
      new Function(modelSource)();
      new Function(uiSource)();
      return {dom, model: root.LimerTopology, explorer: root.topologyExplorer, restore};
    } catch (error) {
      restore();
      throw error;
    }
  };

  root.runTopologyAnimationUiChecks = function (html, modelSource, uiSource) {
    const mounted = root.mountTopologyAnimationForTest(html, modelSource, uiSource);
    const {model, explorer: ui} = mounted;
    const document = mounted.dom.document;
    const get = id => document.getElementById(id);
    const checks = [];
    function check(condition, description) {
      if (!condition) throw new Error(description);
      checks.push(description);
    }
    function change(id, value) {
      get(id).value = value;
      get(id).dispatchEvent({type: 'change'});
    }
    function snapshot() { return JSON.stringify(ui.state); }
    try {
      check(get('graphNodes').children.length === 92 && get('graphLinks').children.length === 304,
        'Full physical graph rendered as 92 device groups and 304 link elements');
      check(ui.state.lang === 'zh' && !ui.state.playing,
        'Initial view is Chinese and paused');
      check(get('src').children.length === 16 && get('dst').children.length === 16 && get('spine').children.length === 32,
        'Endpoint and spine selectors expose every physical candidate');

      let allNeighborsCorrect = true;
      for (const node of model.nodes) {
        ui.selectNode(node.id);
        const expected = model.neighbors(node.id).length;
        const actual = get('graphLinks').children.filter(element => element.getAttribute('data-highlighted') === 'true').length;
        allNeighborsCorrect &&= actual === expected && get('selectionBody').querySelectorAll('button').length === expected;
      }
      check(allNeighborsCorrect, 'All 92 device selections highlight exactly their direct physical neighbors');
      for (const [node, degree] of [[0, 3], [16, 4], [20, 36], [28, 4], [24, 36], [91, 4]]) {
        ui.selectNode(node);
        check(get('graphLinks').children.filter(element => element.getAttribute('data-highlighted') === 'true').length === degree,
          `Device ${node} exposes ${degree} actual physical links`);
      }
      get('node-28').dispatchEvent({type: 'keydown', key: 'Enter'});
      check(ui.state.selected === 28, 'Keyboard Enter selects a graph device');
      get('selectionBody').querySelector('button').click();
      check(ui.state.selected === 20, 'Neighbor buttons navigate to the connected device');
      get('clearSelection').click();
      check(ui.state.selected === null && ui.state.scene === 'overview', 'Clear selection returns to the overview');
      ui.setChapter(2); ui.state.playing = true; get('clearSelection').click();
      check(!ui.state.tour && !ui.state.playing && ui.state.time === 0,
        'Clearing guided selection resets the shorter free-exploration timeline');

      for (const language of ['en', 'zh']) {
        ui.setLanguage(language);
        for (let index = 0; index < 8; index++) {
          ui.setChapter(index);
          for (const id of ['captionTitle', 'captionText', 'selectionBody', 'routeReadout', 'scopeNote', 'progressText']) {
            if (!get(id).textContent.trim()) throw new Error(`Empty ${language} chapter ${index} field ${id}`);
          }
        }
      }
      check(true, 'All eight guided chapters have nonempty Chinese and English explanations');

      const physicalEdges = new Set(model.links.map(link => link.id));
      let validRoutes = 0;
      for (let src = 0; src < 16; src++) for (let dst = 0; dst < 16; dst++) {
        for (const plane of ['A', 'B']) for (let spine = 0; spine < 32; spine++) {
          const path = model.route(src, dst, plane, spine);
          if (path[0] !== src || path[path.length - 1] !== dst) throw new Error('Invalid route endpoint');
          for (let index = 1; index < path.length; index++) {
            const a = path[index - 1], b = path[index];
            if (!physicalEdges.has(`L${Math.min(a, b)}-${Math.max(a, b)}`)) throw new Error('Route uses a nonexistent edge');
          }
          validRoutes++;
        }
      }
      check(validRoutes === 16384, 'All 16,384 model routes use existing physical graph edges');
      const byId = new Map(model.nodes.map(node => [node.id, node]));
      check(model.links.every(link => {
        const a = byId.get(link.source), b = byId.get(link.target);
        return !(a.kind === 'psw' && b.kind === 'psw') && !(a.kind === 'asw' && b.kind === 'asw') &&
          !(a.plane && b.plane && a.plane !== b.plane);
      }), 'No core-core, ASW-ASW or cross-plane switch links exist');

      for (const [src, dst, rail, spine] of [[0, 1, 'A', 0], [0, 4, 'A', 0], [0, 4, 'both', 0],
        [0, 5, 'A', 31], [0, 5, 'B', 31], [15, 0, 'both', 17], [0, 0, 'both', 31]]) {
        ui.showRoute(src, dst, rail, spine);
        const routes = ui.routePaths();
        check(get('graphRoutes').children.length === routes.reduce((sum, route) => sum + route.nodes.length - 1, 0),
          `Route ${src}→${dst} ${rail}/${spine}: overlays follow exact physical hop count`);
        check(get('graphPackets').children.length === routes.filter(route => route.nodes.length > 1).length * 3,
          `Route ${src}→${dst} ${rail}/${spine}: pulses appear only on nonempty data paths`);
      }
      const original = snapshot();
      for (const args of [[-1, 5, 'A', 0], [0, 16, 'A', 0], [0, 5, 'C', 0], [0, 5, 'A', 32]]) {
        let rejected = false;
        try { ui.showRoute(...args); } catch (error) { rejected = error instanceof RangeError; }
        if (!rejected || snapshot() !== original) throw new Error('Invalid route changed presentation state');
      }
      check(true, 'Invalid route arguments are rejected before changing UI state');

      ui.showRoute(0, 5, 'both', 8); ui.seek(3.7);
      const previous = {...ui.state};
      get('language').click();
      check(Object.entries(previous).filter(([key]) => key !== 'lang').every(([key, value]) => ui.state[key] === value),
        'Language switching preserves every non-language state field');
      check(document.documentElement.lang === 'en' && get('title').textContent.includes('whole fabric'),
        'English text and document language are updated');
      check(get('scopeNote').textContent.includes('not packet logs') && get('scopeNote').textContent.includes('ACK replay'),
        'English scope notice distinguishes illustration from measured packet/ACK replay');
      get('language').click();
      check(document.documentElement.lang === 'zh-CN' && get('scopeNote').textContent.includes('不是逐包日志'),
        'Chinese text and scope notice are restored');

      get('play').click(); ui.frame(0); ui.frame(100);
      check(!ui.state.playing && ui.state.time === 3.7, 'Paused frames do not advance presentation time');
      get('seek').value = '450'; get('seek').dispatchEvent({type: 'input'});
      check(ui.state.time === 4.5 && !ui.state.playing, 'Seeking preserves the paused state');
      change('speed', '2'); get('play').click(); ui.frame(200); ui.frame(300);
      check(Math.abs(ui.state.time - 4.7) < 1e-9, '2× speed advances 0.2 demo seconds per 100 ms');
      change('planeFilter', 'B');
      check(get('graphPackets').children.length === 3 && get('edge-L20-36').getAttribute('opacity') === '0',
        'Plane filter hides the opposite plane links and animated route');
      check(get('routeReadout').textContent.includes('已被视图筛选隐藏'),
        'Route readout explicitly labels a path hidden by the plane filter');
      change('zoom', '1.5');
      check(get('network').style.width === '150%', 'Zoom updates the SVG presentation width');
      get('showLinks').checked = false; get('showLinks').dispatchEvent({type: 'change'});
      check(!ui.state.showLinks && get('edge-L1-25').getAttribute('opacity') === '0',
        'Hide-all-links control removes unrelated background links');
      get('src').value = '3'; get('dst').value = '8'; get('rail').value = 'B'; get('spine').value = '31';
      get('trace').click();
      check(JSON.stringify(ui.routePaths()[0].nodes) === JSON.stringify([3, 27, 91, 24, 8]),
        'Trace button reads all endpoint/rail/spine selectors');

      document.dispatchEvent({type: 'keydown', target: document.body, code: 'Space'});
      check(!ui.state.playing, 'Space shortcut pauses the animation');
      document.dispatchEvent({type: 'keydown', target: document.body, code: 'ArrowRight'});
      check(ui.state.time === 2, 'Right-arrow shortcut seeks forward');
      const beforeInputKey = snapshot();
      document.dispatchEvent({type: 'keydown', target: get('seek'), code: 'Space'});
      check(snapshot() === beforeInputKey, 'Global shortcuts do not interfere with form controls');
      get('fullscreen').click();
      check(document.fullscreenElement === document.documentElement, 'Fullscreen handler calls the simulated document API');
      get('fullscreen').click();
      check(document.fullscreenElement === null, 'Fullscreen handler calls the simulated exit API');
      document.documentElement.requestFullscreen = null;
      get('fullscreen').click();
      check(get('fullscreen').textContent.includes('浏览器全屏'), 'Unavailable fullscreen gets a visible fallback message');

      change('speed', '1'); get('restart').click();
      check(ui.state.tour && ui.state.playing && ui.state.time === 0 && ui.state.chapter === 0,
        'Restart returns to chapter zero and plays the complete tour');
      const chapters = new Set(); ui.frame(1000);
      for (let now = 1050; now <= 74000; now += 50) {
        ui.frame(now); chapters.add(ui.state.chapter);
        for (const element of get('graphPackets').children) {
          const transform = element.getAttribute('transform');
          if (!transform || /NaN|Infinity/.test(transform)) throw new Error('Nonfinite animated pulse position');
        }
      }
      check(chapters.size === 8 && ui.state.time === 72 && !ui.state.playing,
        'Complete 72-second tour visits all eight chapters and stops with finite pulse positions');
      check(get('graphNodes').children.length === 92 && get('graphLinks').children.length === 304,
        'Full playback retains all 92 device nodes and 304 physical links');
      return {pass: true, checks, checkedModelRoutes: validRoutes,
        execution: 'Actual JavaScript executed with a minimal DOM substitute',
        realBrowserRenderingChecked: false, simulatedNetworkRun: false};
    } finally {
      mounted.restore();
    }
  };
})(globalThis);
