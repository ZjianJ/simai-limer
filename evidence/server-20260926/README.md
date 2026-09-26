# Preserved server evidence

These inputs and summaries are copied from the server's historical runs, without
editing their measured values. `provenance.json` contains original Git heads,
dirty-worktree status, file hashes and backup archive hashes. The full raw data
are stored separately from Git. Historical absolute paths in manifests do not
refer to the current machine.

`source_verification.json` checks the independently reconstructed source against
314 original server file hashes. `b8_verification.json` records the new local
x86-64/GCC 9.4 reproduction, not the original aarch64 run. All 12 runs passed
payload, completion and applicable feedback audits, with exactly matching finish
times. This is not validation of real tensor arithmetic or all B9-B14 policies.

From a fresh, patched SimAI checkout with this repository at `limer/`:

```bash
bash limer/scripts/build_split_baselines.sh
python3 limer/tools/verify_b8_snapshot.py \
  --simai-root . \
  --historical-results limer/evidence/server-20260926 \
  --out limer/results/b8_verification_new_run
```

The output directory must not already exist. The archived capacity tables are
the historical calibration inputs, retained to reproduce the original matrix;
they are not a new calibration on different hardware or workloads.
