# Simulation workflow animation — A02

Open `simulation_workflow.html` locally or from GitHub Pages. The self-contained
HTML contains its evidence extract, so it works without a server or network.
Use 中文 / English, seed selection, Play/Pause, Restart, timeline, stage buttons,
playback speed and fullscreen. One presentation takes 61 seconds at 1x.
Changing language or seed preserves presentation progress.

## Illustration versus measurement

- Illustrated: topology layout, stages, link emphasis and synchronized display
  of independent runs. No packet trace or byte-progress reconstruction is shown.
- Recorded: 32 ACCESS identities; three frozen eight-link schedules; fractions,
  start/end times; configuration; exact finish timestamps and audit summary fields.
- Derived: milliseconds = nanoseconds / 1e6; slowdown percent = severity * 100;
  remaining percent = parameter_after * 100; arithmetic means over seeds 42–44.
  Tables round times to three decimals.
- Running bars show elapsed simulation time until recorded completion, not bytes
  transferred. The 0–9 ms zoom is inside a 100 ms observation window. Policies
  actually run separately, not together on one live fabric.

## Evidence provenance

Source: `random_a8_b80_t1ms_20260908`, local SimAI `limer/results/split_baselines/`.

- E01: `summary.json`, parameters, schedules/results, pass/same_payload.
- E02: `seed_42/B8/link_map.csv`, GPU nodes, ports, ToRs, bandwidths and delays.
- E03: `seed_42/B8/manifest.json`, single-worker execution, telemetry period,
  disabled recovery transport and topology digest.
- E04: `microAllReduce_16rank_split_64mib.txt`, workload and input message size.
- E05: `random_split_qualification.md`, experiment audits and scope.

`simulation_evidence.json` preserves relevant values plus SHA256 of E01 and the
topology. Machine-specific paths and loader details are omitted. The identical
extract is embedded in the HTML. This is not the complete raw-log archive or a
release of the latest simulator source.

Four servers contain contiguous groups of four GPUs with local NVSwitches.
A ToRs 20–23 and B ToRs 24–27 each serve the same GPU slot across servers, not
one dedicated server. Only the 32 ACCESS edges are drawn; fabric interiors and
NVSwitch edges are visually omitted.

Eight policies × three seeds = 24 formal runs. Two healthy rail calibrations
plus two impaired rail calibrations per seed = eight calibrations. B5 is a
separate offline conditional model. The three-policy highlight is a subset.

## Scope and review

This is a frozen historical experiment, not an editable what-if simulator.
Seed selection loads existing records, not new runs. No claims of tensor
arithmetic validation, real NCCL behavior, hard-disconnect recovery or recovery
SLO compliance. Three seeds do not establish statistical or tail guarantees.

Original HTML/CSS/SVG/JavaScript, AI-assisted, without external visual assets.
Scientific-writing checks distinguish observed evidence from illustration;
methodological attribution is in the companion animation README. Human review
before formal presentation remains recommended, not implied as completed.

Validation covers source/extract equality; eight unique valid faults per seed;
finish times and equal payloads; rendering of every stage/seed/language; and
playback/language state preservation. Browser visual acceptance is outstanding
because this environment has no browser.
