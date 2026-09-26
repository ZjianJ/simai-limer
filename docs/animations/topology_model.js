/* Physical topology for the offline LIMER topology explainer.
 * The compact specification is checked against the original simulator input by
 * tests/test_topology_animation_model.py. Paths below are valid physical paths,
 * not a reconstruction of a particular QP's ECMP hash or ACK return path.
 */
(function (root) {
  "use strict";

  const spec = /* TOPOLOGY_SPEC_START */
  {
    "source": {
      "path": "limer/results/true16_hard_fault_e2e/topology/Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100",
      "sha256": "c0b59d59b9f02a468601b8d20e186936262323318b39d8216d0a31841ba52fac"
    },
    "servers": 4,
    "gpusPerServer": 4,
    "gpuStart": 0,
    "nvStart": 16,
    "planes": [
      { "name": "A", "aswStart": 20, "pswStart": 28 },
      { "name": "B", "aswStart": 24, "pswStart": 60 }
    ],
    "aswPerPlane": 4,
    "pswPerPlane": 32,
    "intra": { "gbps": 2400, "delayNs": 25 },
    "access": { "gbps": 100, "delayNs": 500 },
    "fabric": { "gbps": 400, "delayNs": 500 }
  }
  /* TOPOLOGY_SPEC_END */;

  function freezeTree(value) {
    if (value !== null && typeof value === "object") {
      Object.values(value).forEach(freezeTree);
      Object.freeze(value);
    }
    return value;
  }

  const nodes = [];
  const links = [];
  const gpuCount = spec.servers * spec.gpusPerServer;
  const planes = new Map(spec.planes.map((plane) => [plane.name, plane]));

  function addNode(id, kind, plane = null, server = null, slot = null) {
    nodes.push({ id, kind, plane, server, slot });
  }

  function addLink(a, b, kind, plane = null) {
    const source = Math.min(a, b);
    const target = Math.max(a, b);
    links.push({
      id: `L${source}-${target}`,
      source,
      target,
      kind,
      plane,
      gbps: spec[kind].gbps,
      delayNs: spec[kind].delayNs
    });
  }

  for (let rank = 0; rank < gpuCount; rank += 1) {
    const id = spec.gpuStart + rank;
    const server = Math.floor(rank / spec.gpusPerServer);
    const slot = rank % spec.gpusPerServer;
    addNode(id, "gpu", null, server, slot);
    addLink(id, spec.nvStart + server, "intra");
    spec.planes.forEach((plane) => {
      addLink(id, plane.aswStart + slot, "access", plane.name);
    });
  }
  for (let server = 0; server < spec.servers; server += 1) {
    addNode(spec.nvStart + server, "nv", null, server);
  }
  spec.planes.forEach((plane) => {
    for (let slot = 0; slot < spec.aswPerPlane; slot += 1) {
      addNode(plane.aswStart + slot, "asw", plane.name, null, slot);
      for (let spine = 0; spine < spec.pswPerPlane; spine += 1) {
        addLink(plane.aswStart + slot, plane.pswStart + spine,
          "fabric", plane.name);
      }
    }
    for (let spine = 0; spine < spec.pswPerPlane; spine += 1) {
      addNode(plane.pswStart + spine, "psw", plane.name);
    }
  });
  nodes.sort((a, b) => a.id - b.id);

  const adjacency = new Map(nodes.map((node) => [node.id, []]));
  links.forEach((link) => {
    adjacency.get(link.source).push(link.target);
    adjacency.get(link.target).push(link.source);
  });
  adjacency.forEach((list) => Object.freeze(list.sort((a, b) => a - b)));

  function requireGpu(id) {
    if (!Number.isInteger(id) || id < spec.gpuStart ||
        id >= spec.gpuStart + gpuCount) {
      throw new RangeError("GPU ID must be an integer from 0 through 15.");
    }
  }

  function neighbors(id) {
    if (!Number.isInteger(id) || !adjacency.has(id)) {
      throw new RangeError("Node ID must be an integer from 0 through 91.");
    }
    return adjacency.get(id);
  }

  function route(src, dst, plane = "A", spineIndex = 0) {
    requireGpu(src);
    requireGpu(dst);
    if (!planes.has(plane)) {
      throw new RangeError("Plane must be A or B.");
    }
    if (!Number.isInteger(spineIndex) || spineIndex < 0 ||
        spineIndex >= spec.pswPerPlane) {
      throw new RangeError("Spine index must be an integer from 0 through 31.");
    }
    if (src === dst) return Object.freeze([src]);
    const srcRank = src - spec.gpuStart;
    const dstRank = dst - spec.gpuStart;
    const srcServer = Math.floor(srcRank / spec.gpusPerServer);
    const dstServer = Math.floor(dstRank / spec.gpusPerServer);
    if (srcServer === dstServer) {
      return Object.freeze([src, spec.nvStart + srcServer, dst]);
    }
    const srcSlot = srcRank % spec.gpusPerServer;
    const dstSlot = dstRank % spec.gpusPerServer;
    const selected = planes.get(plane);
    const srcAsw = selected.aswStart + srcSlot;
    if (srcSlot === dstSlot) return Object.freeze([src, srcAsw, dst]);
    return Object.freeze([
      src, srcAsw, selected.pswStart + spineIndex,
      selected.aswStart + dstSlot, dst
    ]);
  }

  function countKind(items, kind) {
    return items.filter((item) => item.kind === kind).length;
  }

  const summary = {
    nodes: nodes.length,
    links: links.length,
    gpus: countKind(nodes, "gpu"),
    servers: spec.servers,
    nvSwitches: countKind(nodes, "nv"),
    accessSwitches: countKind(nodes, "asw"),
    coreSwitches: countKind(nodes, "psw"),
    intraLinks: countKind(links, "intra"),
    accessLinks: countKind(links, "access"),
    fabricLinks: countKind(links, "fabric"),
    perPlane: {
      accessSwitches: spec.aswPerPlane,
      coreSwitches: spec.pswPerPlane,
      accessLinks: gpuCount,
      fabricLinks: spec.aswPerPlane * spec.pswPerPlane
    }
  };

  const api = freezeTree({ spec, source: spec.source, nodes, links, summary,
    neighbors, route });
  root.LimerTopology = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : window);
