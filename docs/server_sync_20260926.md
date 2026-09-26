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
It is excluded from Git. Historical experiment reports are not newly measured
results. Compilation and local B8 reproduction are in progress; this document
will be updated with actual verification outcomes.

The snapshot does not imply that every historical policy is beneficial or that
simulated payload accounting verifies numerical reduction of real tensors.

## Future exports

`tools/export_simai_patches.py` includes untracked source without modifying the
server Git index. It excludes generated ns-3 copies. The previous HEAD-only
export could silently omit uncommitted research and must not be used for rescue.
