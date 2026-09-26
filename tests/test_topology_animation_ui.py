"""Offline markup checks, not a browser rendering or simulator validation.

The companion topology_animation_ui_harness.js executes the actual scripts with
a deliberately small DOM substitute when a JavaScript runtime is available.
"""

from html.parser import HTMLParser
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest


ANIMATIONS = Path(__file__).resolve().parents[1] / "docs" / "animations"
NODE = shutil.which("node") or shutil.which("nodejs")


class PageParser(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.elements = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)


class TopologyAnimationUiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ANIMATIONS / "topology_explorer.html").read_text()
        cls.ui = (ANIMATIONS / "topology_explorer.js").read_text()
        cls.page = PageParser(cls.html)
        cls.by_id = {
            attrs["id"]: (tag, attrs)
            for tag, attrs in cls.page.elements
            if "id" in attrs
        }

    def test_ids_are_unique_and_controls_exist(self):
        ids = [attrs["id"] for _, attrs in self.page.elements if "id" in attrs]
        self.assertEqual(len(ids), len(set(ids)))
        expected = {
            "network", "graphLinks", "graphNodes", "graphPackets", "language",
            "chapters", "play", "restart", "seek", "speed", "fullscreen",
            "src", "dst", "rail", "spine", "trace", "showLinks",
            "planeFilter", "zoom", "clearSelection", "selectionTitle",
            "selectionBody", "routeReadout", "captionTitle", "captionText",
            "progressText",
        }
        self.assertFalse(expected - self.by_id.keys())

    def test_model_loads_before_ui_and_offline_assets_exist(self):
        scripts = [attrs["src"] for tag, attrs in self.page.elements
                   if tag == "script" and "src" in attrs]
        self.assertIn("topology_model.js", scripts)
        self.assertIn("topology_explorer.js", scripts)
        self.assertLess(scripts.index("topology_model.js"),
                        scripts.index("topology_explorer.js"))
        for tag, attrs in self.page.elements:
            resource = attrs.get("src")
            if tag == "link" and attrs.get("rel") == "stylesheet":
                resource = attrs.get("href")
            if resource:
                self.assertNotRegex(resource, r"^(?:https?:)?//")
                self.assertTrue((ANIMATIONS / resource).is_file(), resource)
        self.assertNotRegex(self.ui, r"\bfetch\s*\(|\bXMLHttpRequest\b")

    def test_svg_and_form_controls_have_semantic_types(self):
        tag, attrs = self.by_id["network"]
        self.assertEqual(tag, "svg")
        self.assertEqual(attrs.get("viewbox"), "0 0 1440 850")
        self.assertEqual(self.by_id["seek"][1].get("type"), "range")
        self.assertEqual(self.by_id["showLinks"][1].get("type"), "checkbox")
        for name in ("src", "dst", "rail", "spine", "speed", "planeFilter"):
            self.assertEqual(self.by_id[name][0], "select", name)
        for name in ("play", "restart", "language", "fullscreen", "trace"):
            self.assertEqual(self.by_id[name][0], "button", name)

    def test_no_broken_literal_id_references(self):
        # Dynamic graph-node ids are not matched; these are fixed UI references.
        ids = re.findall(r"\$\(['\"]([^'\"]+)['\"]\)", self.ui)
        self.assertTrue(ids)
        self.assertFalse(set(ids) - self.by_id.keys(), set(ids) - self.by_id.keys())

    def test_local_navigation_links_exist(self):
        for tag, attrs in self.page.elements:
            if tag != "a" or "href" not in attrs:
                continue
            target = attrs["href"].split("#", 1)[0].split("?", 1)[0]
            if target and not re.match(r"^[a-z]+:|^//", target):
                self.assertTrue((ANIMATIONS / target).is_file(), target)

    @unittest.skipUnless(NODE, "No Node runtime: use the V8 DOM harness separately")
    def test_actual_scripts_with_dom_substitute(self):
        harness = Path(__file__).with_name("topology_animation_ui_harness.js")
        script = r"""
const fs = require('fs');
const path = require('path');
require(process.argv[1]);
const source = name => fs.readFileSync(path.join(process.argv[2], name), 'utf8');
const result = globalThis.runTopologyAnimationUiChecks(
  source('topology_explorer.html'), source('topology_model.js'),
  source('topology_explorer.js'));
process.stdout.write(JSON.stringify(result));
"""
        process = subprocess.run([NODE, "-e", script, str(harness), str(ANIMATIONS)],
                                 capture_output=True, text=True, check=True)
        result = json.loads(process.stdout)
        self.assertTrue(result["pass"])
        self.assertGreaterEqual(len(result["checks"]), 50)
        self.assertEqual(result["checkedModelRoutes"], 16384)
        self.assertFalse(result["realBrowserRenderingChecked"])


if __name__ == "__main__":
    unittest.main()
