# simai-limer

## Interactive English demonstration

[Open the B8 chunk-feedback animation](https://ZjianJ.github.io/simai-limer/)
to see probing, feedback delay, rate updates, and weighted byte-credit splitting.
The page is a conceptual teaching aid, not an experiment replay or a release of
the latest simulator implementation. [Scope and validation](docs/animations/README.md).

**LIMER Phase M1: Lightweight In-Network Monitoring for SimAI**, plus a
dynamic randomized fault-injection extension for testing future detection
algorithms.

This repo implements **only the monitoring layer (M1)** of the LIMER
proposal on top of [Alibaba's SimAI simulator](https://github.com/aliyun/SimAI):
switch/NIC/collective telemetry extraction from an ns-3-based RDMA/collective
simulation, plus a `FaultInjector` that can apply/revert bandwidth-degradation
faults on random links at random times during a run. It does **not**
implement fault detection, weak supervision, uncertainty quantification, or
topology recovery — see "Next steps" below.

This is a standalone extraction: the actual patches were developed as commits
on the `limer-monitoring` branch of a SimAI checkout. Since SimAI's build
lives across the superproject and its `ns-3-alibabacloud` submodule, this
repo ships as **this directory's original content (configs/scripts/tools/docs)
plus two small unified diffs** (`patches/`) that apply cleanly to upstream
SimAI. Nothing here is fabricated or hand-edited after the fact — every
number in this README comes from an actual run, reproducible with the
commands below.

## Repository layout

```
configs/    # topology/fault-injection configs and workload files used by every experiment
docs/       # code_map.md, telemetry_schema.md, monitoring_design.md
patches/    # unified diffs against upstream SimAI (see "Setup" below)
scripts/    # build + experiment driver scripts (build_simai.sh, run_*_monitoring.sh, run_smoke_test.sh)
tools/      # Python: dataset builder, telemetry validator, chart generator, fault schedule/topology generators
```

## Setup

The build and experiment scripts assume they live at `<SimAI-checkout>/limer/`
(they resolve paths via `$SCRIPT_DIR/../..`), so after applying the patches,
symlink or copy this repo in as `limer/` inside a SimAI checkout:

```bash
git clone --recurse-submodules https://github.com/aliyun/SimAI.git
cd SimAI

# apply the two patches that carry the actual instrumentation code
git apply /path/to/simai-limer/patches/astra-sim-alibabacloud.patch
(cd ns-3-alibabacloud && git apply /path/to/simai-limer/patches/ns-3-alibabacloud.patch)

# drop this repo in as limer/ (symlink keeps it a separate git repo you can pull/push independently)
ln -s /path/to/simai-limer limer
```

The patches were generated from, and verified to apply cleanly against,
SimAI commit `f5efb5a` (`astra-sim-alibabacloud.patch`) and
`ns-3-alibabacloud` commit `1484b1a` (`ns-3-alibabacloud.patch`) — the exact
pre-LIMER state of each repo. If upstream has since diverged at those paths,
`git apply --check` will tell you before anything is touched.

## One-command reproduction (after Setup above)

```bash
# 1. Build (native aarch64, ~5-10 min; see "Environment" for why gcc-native/12.3)
bash limer/scripts/build_simai.sh

# 2. Generate the 8-GPU topology used by every experiment below
mkdir -p limer/results/baseline/topology && cd limer/results/baseline/topology
python3 ../../../../astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py \
  -topo Spectrum-X -g 8 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps
cd -

# 3. Smoke test (stock 2-collective microAllReduce.txt, instrumentation on)
bash limer/scripts/run_smoke_test.sh

# 4. Full experiments
bash limer/scripts/run_healthy_monitoring.sh   # 3 seeds
bash limer/scripts/run_fault_monitoring.sh     # bandwidth + packet-loss faults
bash limer/scripts/run_overhead_test.sh        # 0/1/5/10ms sampling sweep
bash limer/scripts/run_random_fault_monitoring.sh <seed> <num_faults>  # dynamic randomized faults

# 5. Analysis (project-local venv, not the base conda env)
python3 -m venv limer/.venv && source limer/.venv/bin/activate
pip install pandas numpy matplotlib pyarrow pyyaml

python3 limer/tools/build_monitoring_dataset.py \
  --run-dirs limer/results/healthy/healthy-seed42 limer/results/healthy/healthy-seed123 \
             limer/results/healthy/healthy-seed2026 limer/results/fault/fault-bandwidth_degradation \
  --window-ns 5000000 --fault-events limer/results/fault/fault_events.csv \
  --out-parquet limer/results/monitoring_windows.parquet \
  --out-csv limer/results/monitoring_windows.csv \
  --out-summary limer/results/dataset_summary.json

python3 limer/tools/validate_telemetry.py \
  --run-dirs limer/results/healthy/healthy-seed42 limer/results/healthy/healthy-seed123 \
             limer/results/healthy/healthy-seed2026 limer/results/fault/fault-bandwidth_degradation \
  --link-map limer/results/healthy/healthy-seed42/link_map.csv \
  --fault-events limer/results/fault/fault_events.csv \
  --parity-json limer/results/baseline/monitoring_on_off_parity.json \
  --out-json limer/results/validation_report.json --out-md limer/results/validation_report.md

python3 limer/tools/summarize_monitoring.py \
  --healthy-run-dir limer/results/healthy/healthy-seed42 \
  --fault-bw-run-dir limer/results/fault/fault-bandwidth_degradation \
  --fault-link-id L3-13 --overhead-csv limer/results/monitoring_overhead.csv \
  --out-dir limer/results/charts
```

`results/` is gitignored, same convention as upstream SimAI's `results/`/
`bin/`/`*.log` rules — every number quoted below is regenerable with the
commands above.

## Environment

Isambard-AI Grace Hopper aarch64 node, SUSE-based Cray Shasta OS (not the
`Ubuntu 20.04 + GCC 9.4.0` upstream tests on). Native build succeeds with
`module load gcc-native/12.3` after two small, documented
environment-compatibility patches — see `docs/code_map.md`. No container
fallback was needed.

## Documentation index

- `docs/code_map.md` — exact files/functions read from the real source for
  the switch/NIC/collective instrumentation points.
- `docs/telemetry_schema.md` — every CSV/JSON field, with source and
  live/proxy/unavailable status (nothing is fabricated where a real signal
  doesn't exist).
- `docs/monitoring_design.md` — counter model, sampling loop, and every
  design pivot forced by something discovered empirically (topology
  choice, workload length, determinism, a pre-existing crash).

## Results summary (from actual runs)

- **Baseline**: official unmodified `microAllReduce.txt` example runs to
  completion (exit 0) after 3 environment-compat patches (none change
  simulation semantics).
- **Healthy**: 3 seeded runs, 8 GPUs across 2 servers, 10-layer AllReduce
  workload. All three are bit-identical once the `run_id` column is
  stripped — this SimAI build has no exposed RNG source, confirmed in
  `docs/monitoring_design.md`.
- **Fault A — bandwidth degradation**: halving one GPU-ToR ACCESS link
  (rank 3, `L3-13`) from 100→50Gbps nearly doubles AllReduce completion time
  (25.25ms → 49.93ms) — a real, clearly observable effect in both the
  completion tick and the telemetry.
- **Fault B — packet loss**: **BLOCKED**. 0.5% per-packet loss on the same
  link causes the simulation to stall (280s wall-clock timeout, exit 124)
  partway through layer 1 of 10. Marked `BLOCKED-packet_loss` in
  `fault_events.csv`, not worked around or faked. See
  `configs/packet_loss.yaml` for the best available diagnosis.
- **Overhead**: disabled/1ms/5ms/10ms sampling all produce the identical
  simulated completion tick; wall-clock difference between them is within
  noise relative to the simulator's own ~19s baseline runtime for this
  workload. This is *simulation instrumentation* overhead, not real
  switch/NIC hardware telemetry overhead.
- **Validation**: 31/31 checks pass.
- **Dataset**: 7616 5ms windows across 4 runs, labels attached post-hoc by
  (timestamp, link_id) only — never written into the source telemetry CSVs.
- **Randomized dynamic fault injection** (added for testing a future
  detection algorithm): unlike Fault A/B above (static, present from t=0),
  `scripts/run_random_fault_monitoring.sh <seed> <num_faults>` generates a
  reproducible random schedule of non-overlapping bandwidth-degradation
  faults on random ACCESS links at random times mid-run, applies/reverts
  them live via `limer::FaultInjector`, and writes the same `fault_events.csv`
  schema as ground truth. Verified end-to-end (seed 7, 3 faults):
  `configured_bandwidth_bps` switches exactly within the scheduled window,
  `queue_bytes` jumps from tens of bytes to ~450KB during the fault, and
  reverts cleanly after — see `docs/monitoring_design.md` "Dynamic,
  randomized fault injection" for the 3 real bugs found and fixed while
  building this (CRLF parsing, stale bandwidth reporting, one-sided device
  fault application).

## Known limitations

- **NIC packet counts are host-level, not per-NIC.** `tx_packets`/
  `rx_packets` in `nic_telemetry.csv` come from a counter on the `RdmaHw`
  object (one per host), so they repeat identically across every `nic_id`
  row for a host in a given sample; `tx_bytes`/`rx_bytes` are correctly
  per-NIC. See `docs/telemetry_schema.md`.
- **`rtt_proxy_ns` is a static routing-table value** (`pairRtt[src][dst]`,
  used for BDP/window sizing), not a measured per-packet RTT — this
  simulator doesn't compute one.
- **`retransmissions` and `nacks` are the same underlying event** (this
  RDMA model's go-back-N NACK path), reported in both columns for schema
  compatibility, not two independent measurements.
- **Static faults (Fault A/B) are whole-run only.** They're injected by
  editing one field in a copy of the topology file before the simulation
  starts. The dynamic `FaultInjector` (random fault injection, above) does
  support mid-simulation, time-windowed faults for bandwidth degradation.
- **Packet-loss fault is blocked**, not validated — see above.
- **Collective telemetry is flow-level, not true collective-level.**
  `AstraSim::ncclFlowTag` (the only context available at the ns3-frontend
  hook points used) carries no layer-name or training-iteration field, so
  `iteration_id`/`layer_id` in `collective_telemetry.csv` are empty and
  `collective_type`/`algorithm` are workload-derived constants
  (`ALLREDUCE`/`NcclFlowModel`), not per-row reads. Getting true
  per-collective attribution requires hooking astra-sim's `Sys`/`Workload`
  layer, not just the ns3 network frontend.
- **A pre-existing (non-LIMER) crash** was found in this SimAI build: the
  original 20-layer workload triggers a `double free or corruption` around
  layer 17 on the 2-server topology, reproduced identically with
  monitoring fully disabled. Worked around with a 10-layer workload; not
  root-caused.
- **No runtime NCCL communicator switching, no checkpoint recovery, no
  detection model of any kind.** This is monitoring (+ fault injection for
  testing detection algorithms) only, per scope.
- Nothing in this repo claims simulated switch/NIC counters are equivalent
  to real hardware telemetry — every "overhead" number is the cost of this
  C++ instrumentation inside the ns-3 event loop.

## Next steps (not implemented here, per scope)

- Deep neuro-fuzzy detection model, Gaussian Process uncertainty
  quantification, weakly-supervised online adaptation.
- NCCL communicator topology recovery.
- Root-cause the pre-existing 20-layer crash and the packet-loss hang.
- Hook astra-sim's `Sys`/`Workload` layer for true per-collective
  (not per-flow) `iteration_id`/`layer_id` attribution.
- Extend dynamic `FaultInjector` to packet-loss and other fault types
  (currently bandwidth-degradation only).

## License / attribution

The `patches/` in this repo modify files from
[aliyun/SimAI](https://github.com/aliyun/SimAI) and
[aliyun/ns-3-alibabacloud](https://github.com/aliyun/ns-3-alibabacloud),
both Apache-2.0 licensed. This repo's own original content (scripts, tools,
configs, docs) is licensed Apache-2.0 as well — see `LICENSE`.
