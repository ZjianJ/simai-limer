# Continuous chunk-feedback animation (English)

Open `chunk_feedback.html` in a browser; no dependencies or network are required.
The GitHub Pages landing page embeds the same self-contained animation.
Controls: Play/Pause, Restart, timeline, five chapter buttons, speed, fullscreen,
Space and arrow keys. Runtime: approximately 52 demo seconds at 1x.
Use the 中文 / English button at the top right to switch the entire animation,
including captions, rate/credit readouts and accessibility labels. Switching
preserves progress, playback state, speed and the underlying model.

## Scope and provenance

Display A01-EN, translated from the local Chinese A01 teaching animation on
2026-09-16. Original HTML/CSS/SVG/JavaScript; no third-party visual assets or CDN.
AI-assisted implementation; human presentation review remains recommended.

Algorithm basis inspected locally:

- `limer_split_policy.h`: `ChunkComplete`, `Select`; latest completion-rate
  estimates, proportional byte-credit accrual, maximum-credit selection,
  A-first ties, and whole-chunk debit with retained credit balances.
  SHA256: `f705654e293dd12635edbd887a33aab24b4ffb6fe44fbcad623b473040977c04`.
- `limer_split_runtime.h`: `Pump`; two initial chunks, expanded concurrency
  after the first completion on either rail.
  SHA256: `e4771dfa849232fce5dd4e48c8b9b2fac9d95608c469cdac8775d89eab0ad904`.

Those local source files and the complete runtime dependencies are not included
in this animation-only publication. The demonstration is not a source release
of the latest simulator algorithm.

The animation uses 96 chunks of 64 KiB and at most eight active chunks for one
sender/receiver pair. The count of 96 and the timing constants are pedagogical,
not measurements. B slows at demo second 9; normal/slower data travel times are
2/6 demo seconds, followed by a 0.65-second ACK journey. Each chunk has independent
illustrative progress; no shared serialization or queueing model is implied.
Displayed rates are KiB/demo second, NOT measured experiment throughput.

The scheduler learns of the slowdown only through completed chunks. It does not
read the fault schedule. In-flight chunks do not move between rails. This does
not establish hard-failure recovery, recovery SLOs, or tensor correctness.

Validation: execute the actual embedded JavaScript in V8 with a minimal DOM stub;
check all 96 completions, at most 8 in-flight chunks, simultaneous initial probes,
delayed slow feedback, byte-credit choices, and rendering calls across the full
timeline. Browser visual inspection remains outstanding in the authoring environment.

Scientific scope review followed the local scientific-writing skill, retaining
the distinction between illustration and measured evidence. Its suggested
methodological reference is Kassis et al., *Scientific Agent Skills: A Library
of Procedural Knowledge for Research Agents* (2026), arXiv:2609.00065;
bibliographic metadata has not been independently verified for this demo.
