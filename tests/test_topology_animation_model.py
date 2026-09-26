"""Check the animation's physical graph against its simulator input.

The source/spec tests do not execute JavaScript. The optional Node test executes
the real module when a Node runtime is installed and is explicitly skipped
otherwise; it must not be mistaken for the independent specification checks.
"""

from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest


SIMAI = Path(__file__).resolve().parents[2]
MODEL = SIMAI / 'limer/docs/animations/topology_model.js'
NODE = shutil.which('node') or shutil.which('nodejs')


def read_spec():
    match = re.search(
        r'/\* TOPOLOGY_SPEC_START \*/\s*(\{.*?\})\s*'
        r'/\* TOPOLOGY_SPEC_END \*/', MODEL.read_text(), re.DOTALL)
    if not match:
        raise ValueError('Missing strict-JSON topology specification')
    return json.loads(match.group(1))


def expand_spec(spec):
    """Independent Python graph expansion, not JavaScript execution."""
    nodes = {}
    edges = {}

    def node(node_id, kind, plane=None, server=None, slot=None):
        if node_id in nodes:
            raise ValueError('Duplicate node')
        nodes[node_id] = (kind, plane, server, slot)

    def edge(a, b, kind, plane=None):
        key = tuple(sorted((a, b)))
        if key in edges:
            raise ValueError('Duplicate edge')
        edges[key] = (spec[kind]['gbps'], spec[kind]['delayNs'], kind, plane)

    for server in range(spec['servers']):
        node(spec['nvStart'] + server, 'nv', server=server)
        for slot in range(spec['gpusPerServer']):
            gpu = spec['gpuStart'] + server * spec['gpusPerServer'] + slot
            node(gpu, 'gpu', server=server, slot=slot)
            edge(gpu, spec['nvStart'] + server, 'intra')
            for plane in spec['planes']:
                edge(gpu, plane['aswStart'] + slot, 'access', plane['name'])
    for plane in spec['planes']:
        for slot in range(spec['aswPerPlane']):
            node(plane['aswStart'] + slot, 'asw', plane['name'], slot=slot)
        for spine in range(spec['pswPerPlane']):
            node(plane['pswStart'] + spine, 'psw', plane['name'])
            for slot in range(spec['aswPerPlane']):
                edge(plane['aswStart'] + slot, plane['pswStart'] + spine,
                     'fabric', plane['name'])
    return nodes, edges


def expected_route(spec, src, dst, plane, spine):
    """Reference routes used for exhaustive physical-edge checks."""
    src_rank, dst_rank = src - spec['gpuStart'], dst - spec['gpuStart']
    width = spec['gpusPerServer']
    if src == dst:
        return [src]
    if src_rank // width == dst_rank // width:
        return [src, spec['nvStart'] + src_rank // width, dst]
    p = next(p for p in spec['planes'] if p['name'] == plane)
    src_asw = p['aswStart'] + src_rank % width
    if src_rank % width == dst_rank % width:
        return [src, src_asw, dst]
    return [src, src_asw, p['pswStart'] + spine,
            p['aswStart'] + dst_rank % width, dst]


class TopologyAnimationModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = read_spec()
        cls.nodes, cls.edges = expand_spec(cls.spec)
        cls.topology = SIMAI / cls.spec['source']['path']

    def test_source_sha256_is_exact(self):
        self.assertEqual(hashlib.sha256(self.topology.read_bytes()).hexdigest(),
                         self.spec['source']['sha256'])

    def test_every_edge_rate_and_delay_matches_source(self):
        lines = self.topology.read_text().splitlines()
        self.assertEqual(lines[0].split(), ['92', '4', '4', '72', '304', 'A100'])
        self.assertEqual(set(map(int, lines[1].split())), set(range(16, 92)))
        original = {}
        for row in lines[2:]:
            source, target, rate, delay, error = row.split()
            key = tuple(sorted((int(source), int(target))))
            self.assertNotIn(key, original)
            self.assertEqual(error, '0')
            self.assertTrue(rate.endswith('Gbps'))
            self.assertTrue(delay.endswith('ms'))
            original[key] = (int(rate[:-4]),
                             int(Decimal(delay[:-2]) * 1_000_000))
        actual = {key: value[:2] for key, value in self.edges.items()}
        self.assertEqual(actual, original)

    def test_counts_and_degrees(self):
        self.assertEqual(set(self.nodes), set(range(92)))
        self.assertEqual(Counter(n[0] for n in self.nodes.values()),
                         {'gpu': 16, 'nv': 4, 'asw': 8, 'psw': 64})
        self.assertEqual(Counter(e[2] for e in self.edges.values()),
                         {'intra': 16, 'access': 32, 'fabric': 256})
        degree = Counter(node for pair in self.edges for node in pair)
        for node_id, (kind, _plane, _server, _slot) in self.nodes.items():
            self.assertEqual(degree[node_id],
                             {'gpu': 3, 'nv': 4, 'asw': 36, 'psw': 4}[kind])

    def test_full_bipartite_fabric_and_plane_isolation(self):
        fabric_neighbors = defaultdict(set)
        for (a, b), (_rate, _delay, kind, plane) in self.edges.items():
            if kind == 'fabric':
                self.assertEqual(self.nodes[a][0], 'asw')
                self.assertEqual(self.nodes[b][0], 'psw')
                self.assertEqual(self.nodes[a][1], plane)
                self.assertEqual(self.nodes[b][1], plane)
                fabric_neighbors[b].add(a)
        for plane in self.spec['planes']:
            expected = set(range(plane['aswStart'], plane['aswStart'] + 4))
            for spine in range(plane['pswStart'], plane['pswStart'] + 32):
                self.assertEqual(fabric_neighbors[spine], expected)

    def test_all_reference_routes_use_existing_edges(self):
        checked = 0
        for src in range(16):
            for dst in range(16):
                for plane in ('A', 'B'):
                    routes = []
                    for spine in range(32):
                        path = expected_route(self.spec, src, dst, plane, spine)
                        self.assertEqual((path[0], path[-1]), (src, dst))
                        self.assertEqual(len(path), len(set(path)))
                        for a, b in zip(path, path[1:]):
                            edge = self.edges[tuple(sorted((a, b)))]
                            if edge[2] != 'intra':
                                self.assertEqual(edge[3], plane)
                        routes.append(tuple(path))
                        checked += 1
                    different_asw = src // 4 != dst // 4 and src % 4 != dst % 4
                    self.assertEqual(len(set(routes)), 32 if different_asw else 1)
        self.assertEqual(checked, 16384)

    @unittest.skipUnless(NODE, 'No Node runtime: JS execution is not checked here')
    def test_real_javascript_module(self):
        script = r'''
const t = require(process.argv[1]);
const rows = [];
for (let src = 0; src < 16; src++) for (let dst = 0; dst < 16; dst++)
  for (const p of ['A', 'B']) for (let s = 0; s < 32; s++)
    rows.push([src, dst, p, s, t.route(src, dst, p, s)]);
const invalid = [() => t.route(-1, 2), () => t.route(0, 16),
  () => t.route('0', 2), () => t.route(0, 2, 'C'),
  () => t.route(0, 2, 'A', 32), () => t.route(0, 2, 'A', 0.5),
  () => t.neighbors(-1), () => t.neighbors(92), () => t.neighbors('0')];
const rejected = invalid.map(f => { try { f(); return false; }
  catch (e) { return e instanceof RangeError; } });
console.log(JSON.stringify({nodes: t.nodes, links: t.links, summary: t.summary,
  rows, rejected, neighbors: t.nodes.map(n => [n.id, t.neighbors(n.id)]),
  frozen: [t, t.spec, t.nodes, t.links, t.nodes[0], t.links[0], t.summary,
    t.neighbors(0), t.route(0, 5)].every(Object.isFrozen)}));
'''
        result = subprocess.run([NODE, '-e', script, str(MODEL)], check=True,
                                capture_output=True, text=True)
        actual = json.loads(result.stdout)
        self.assertTrue(all(actual['rejected']))
        self.assertTrue(actual['frozen'])
        self.assertEqual(len(actual['nodes']), 92)
        self.assertEqual(len(actual['links']), 304)
        for node in actual['nodes']:
            self.assertEqual((node['kind'], node['plane'], node['server'], node['slot']),
                             self.nodes[node['id']])
        for link in actual['links']:
            pair = (link['source'], link['target'])
            self.assertEqual(link['id'], f'L{pair[0]}-{pair[1]}')
            self.assertEqual((link['gbps'], link['delayNs'], link['kind'], link['plane']),
                             self.edges[pair])
        for src, dst, plane, spine, path in actual['rows']:
            self.assertEqual(path, expected_route(self.spec, src, dst, plane, spine))
        for node_id, neighbors in actual['neighbors']:
            expected = sorted(b if a == node_id else a
                              for a, b in self.edges if node_id in (a, b))
            self.assertEqual(neighbors, expected)
        self.assertEqual(actual['summary']['nodes'], 92)
        self.assertEqual(actual['summary']['links'], 304)


if __name__ == '__main__':
    unittest.main()
