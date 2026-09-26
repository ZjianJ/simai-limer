# Server worktree rescue: 2026-09-26

## Scope

The source was exported from the Isambard `limer-monitoring` worktree, including
modified tracked files and untracked research source. The server index and source
files were not changed. A separate Docker checkout at `/opt/limer-b8` is used for
validation; the previous M1 deployment remains at `/opt/limer-build/SimAI`.

The import includes B8 and later B9-B14 policies, transport/platform extensions,
configs, experiment runners, documentation and tests. Canonical simulator source
is distributed through `patches/`, as in the original repository. Generated
scratch copies, build headers, binaries, caches and crash dumps are not in Git.

## Patch bases

| Repository | Base |
|---|---|
| SimAI | `f5efb5a93ea9be7db25a8843f9f7ff54044f6062` |
| ns-3-alibabacloud | `7e3cb5b88c99abcb582c5abc3919484a4805111b` |

Both patches were applied successfully to fresh checkouts at these bases.
The ns-3 patch deliberately excludes staged copies under `scratch/` and
`src/applications/astra-sim/`; the build script regenerates these from canonical
source. Do not combine these patches with the historical M1 patches.

## Backups and status

The local `.deploy/b8-rescue-20260926/` directory stores the source archive,
per-file hash manifest, raw export patches and downloaded experiment evidence.
It is excluded from Git. All three archive SHA256 values match the server:

| Archive | Bytes | SHA256 |
|---|---:|---|
| source.tar.gz | 71655480 | `f8c887de9837cb9cc897a06dac699128e074d076ab448aae1a5a6978086bfb0f` |
| evidence.tar.gz | 240065214 | `e26a9dd6d53a50fd5f77c2eda495ec9b659877305b12d9e36877ef4e3b48ad30` |
| all-results.tar.gz | 2165207708 | `1a645e1c3e33fc4f1b89388330e8971ef5853eaecfb374234c8a7cf9d858b470` |

The full results archive preserves 21,769 entries and 21,327,120,229 logical
bytes. Identical files are represented as tar hardlinks. It includes the entire
server `limer/results` directory, not just B8. This is not a backup of the whole
server account or its unrelated projects.

The [GitHub rescue release](https://github.com/ZjianJ/simai-limer/releases/tag/server-rescue-20260926)
stores the archives outside Git history, with SHA256 manifests. The full results
archive is split into two numbered parts; concatenate them in order and verify
the complete archive hash before extracting into a new directory.

## New local validation

The independent x86-64/GCC 9.4 build completed. All 314 checked canonical source,
point-to-point model, tool and test files match their server SHA256 values.
The standalone C++ split-policy test and 22 existing Python tests passed
(capacity/feedback: 8; random schedules: 3; runtime memory bounds: 11). The new
patch-export regression test also passed and verifies untracked-file capture,
unchanged source index and successful patch application.

All 12 B2/B6/B7/B8 runs passed payload and completion audits; all finish times
match the historical aarch64 results exactly, without tolerance adjustments:

| Policy | Healthy ns | Static B=25% ns | B=25% after 1 ms ns |
|---|---:|---:|---:|
| B2 | 1435602 | 2173988 | 2527485 |
| B6 | 1484791 | 2350511 | 1822208 |
| B7 | 1445572 | 2909666 | 2617752 |
| B8 | 1380376 | 2372824 | 1749208 |

Each run completed 503,316,480 inter-server payload bytes with zero pending
chunks. B7/B8 each produced 7,680 completion feedback samples and passed the
16-source equal-size simultaneous-first-probe audit. These were fresh simulator
runs, not a replay of stored timestamps. See the [machine-readable report](../evidence/server-20260926/b8_verification.json)
and [reproduction instructions](../evidence/server-20260926/README.md).

Only this three-scenario matrix was rerun; the historical randomized matrix and
B9-B14 results were preserved, not newly validated. The historical capacity
tables were reused as fixed inputs to the reproduction.

The snapshot does not imply that every historical policy is beneficial or that
simulated payload accounting verifies numerical reduction of real tensors.

## Future exports

`tools/export_simai_patches.py` includes untracked source without modifying the
server Git index. It excludes generated ns-3 copies. The previous HEAD-only
export could silently omit uncommitted research and must not be used for rescue.
