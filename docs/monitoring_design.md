# LIMER Monitoring Design

## Counter model

- **Cumulative counters** (`tx_bytes`, `tx_packets`, `rx_bytes`, `rx_packets`,
  `dropped_packets`, `drop_bytes`, `ecn_marks`, `pfc_events`, `nacks`): plain
  in-memory integers, incremented at the exact source-code sites listed in
  `code_map.md`, never reset during a run. The C++ collector writes the raw
  cumulative value at each periodic sample tick; it does **not** compute
  rates or deltas itself.
- **Instantaneous gauges** (`queue_bytes`, `outstanding_bytes`): read
  directly from live simulator state (`SwitchMmu::egress_bytes`,
  `RdmaQueuePair::GetOnTheFly()`) at sample time — a point-in-time snapshot,
  not accumulated.
- **Interval-derived fields** (`observed_throughput_bps`, `utilization`,
  `effective_throughput_bps`, `max_queue_bytes`, `max_queue_packets`): never
  computed by the C++ collector. `tools/build_monitoring_dataset.py`
  (Phase 7) computes these from consecutive raw rows during windowing. This
  keeps the hot path (inside `Simulator::Schedule` callbacks) to integer
  increments and periodic reads only — no division, no rate math, no
  per-packet disk I/O.
- **Discrete events** (collective start/finish) are written immediately,
  one row per event, directly to `collective_telemetry.csv` — these are
  O(number of collectives), not O(number of packets), so immediate
  `fprintf`+`fflush` per event carries negligible overhead (confirmed by the
  overhead study, Phase 9).

## Sampling loop

One periodic sampler, `TelemetryCollector::Sample()`, scheduled via
`Simulator::Schedule(MicroSeconds(interval), &TelemetryCollector::Sample, ...)`
— the identical pattern already used (but never invoked) by
`monitor_qlen`/`monitor_bw` in `common.h`. On each tick it walks the same
`NodeContainer n` those dormant monitors walk: for `GetNodeType() == 1`
(switch) nodes, write one `switch_telemetry.csv` row per (port, direction);
for `GetNodeType() == 0` (host) nodes, write one `nic_telemetry.csv` row per
NIC. NVSwitch nodes (`GetNodeType() == 2`) are out of scope for M1 (they
carry NVLink intra-node traffic, classified `INTRA_NODE`, not the ACCESS
links LIMER targets).

ns-3's `--enable-mtp` (multithreaded) build is in use here (confirmed in
`ns-3-alibabacloud/simulation/build/astra_ns3/build.sh`'s
`./ns3 configure -d debug --enable-mtp`), so callbacks can run on worker
threads. `TelemetryCollector` therefore wraps its counter increments (not
its periodic file writes, which only happen from the scheduler thread) in
the same `MtpInterface::explicitCriticalSection` pattern already used
throughout `entry.h`/`AstraSimNetwork.cc` for shared-state updates, rather
than introducing a new lock.

## Configuration

| Env var | Default | Effect |
|---|---|---|
| `LIMER_TELEMETRY_ENABLE` | `1` | `0` disables all instrumentation; `TelemetryCollector` becomes a no-op and no LIMER files are written (used for the Step 11 overhead baseline and the on/off parity check) |
| `LIMER_TELEMETRY_INTERVAL_US` | `1000` (1 ms) | Sampling period for the periodic switch/NIC loop |
| `LIMER_TELEMETRY_DIR` | required when enabled | Output directory for the run's `switch_telemetry.csv` / `nic_telemetry.csv` / `collective_telemetry.csv` |
| `LIMER_RUN_ID` | generated if unset | Written into every row's `run_id` column and into `run_manifest.json` |

## Why the stock 2-line `microAllReduce.txt` isn't used for the monitoring
## experiments

The stock example issues exactly 2 collectives and completes in ~1.69 ms of
simulated time (confirmed: baseline `run.log` reports
`all passes finished at time: 1688030`, i.e. ~1.69e6 ns, and
"Total streams injected: 2"). At the spec's default 1 ms sampling interval
that yields only 1-2 samples per link — not enough to plot a queue-depth
time series or an "AllReduce duration vs iteration" trend (which needs more
than one iteration by definition). `limer/configs/microAllReduce_20iter.txt`
(new file, same line format as the stock workload, not a modification of
it) repeats the ALLREDUCE layer entry 20 times with varying message sizes,
giving a run long enough (tens of ms) for meaningful time-series charts at
the default 1 ms cadence, while the Step 11 overhead study still sweeps
0/1/5/10 ms explicitly on top of this same longer workload.

## Determinism / "seed" caveat

`grep` across the entire `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/`
frontend and the ns-3 switch code found no call to `ns3::RngSeedManager`,
no `ECMP_SEED`/`RNG_SEED` config key, and `SwitchNode::SetEcmpSeed()` is
defined but never called (so `m_ecmpSeed` stays at its class-default value
for every run). This SimAI-Simulation build is therefore **fully
deterministic** for a fixed (topology, workload, config) triple — there is
no exposed source of run-to-run randomness at this layer. The healthy
experiments (Phase 5) still tag three runs with seeds `42`/`123`/`2026` in
`run_manifest.json` for schema compliance and forward-compatibility, but
the honest, verified expectation is that their telemetry is bit-for-bit
identical, and cross-run variance is reported as `0` rather than invented.
This is itself a real, useful characteristic of the simulator to record,
not a gap in methodology.

## Topology pitfall found during Phase 5: `gpu_per_server` must be < world size

The first attempt at the 8-GPU experiments used
`gen_Topo_Template.py -topo Spectrum-X -g 8` (all defaults, i.e.
`gpu_per_server=8`), which places **all 8 GPUs on a single server**. A
temporary debug trace (`std::cerr` in `SwitchNode::SendToDev`/
`SwitchNotifyDequeue`, since reverted) confirmed those functions were never
called during the whole run: with every GPU co-located, this AllReduce
workload's real ns-3 traffic stays entirely on the NVLink/NVSwitch
(`INTRA_NODE`) links and never touches the ASW/PSW (`ACCESS`/
`INTER_SWITCH`) fabric at all - `switch_telemetry.csv` was legitimately
all-zero (accurately reporting reality) and Fault A (bandwidth halved on
one ACCESS link) produced zero observable effect for the same structural
reason, not because of any telemetry or fault-injection bug.

Fix: generate the 8-GPU topology with `-gps 4` (`gen_Topo_Template.py -topo
Spectrum-X -g 8 -gps 4 ...`), splitting the 8 GPUs across 2 servers (4
each, 2 NVSwitches). AllReduce now must cross the ASW/PSW fabric between
the two servers, and `switch_telemetry.csv` shows real nonzero tx_bytes/
queue_bytes (verified in `limer/results/smoke_test/`). All Phase 5/6
experiments use this topology
(`Spectrum-X_8g_4gps_100Gbps_A100`).

## Pre-existing crash found with the 20-iteration workload on the 2-server topology

`limer/configs/microAllReduce_20iter.txt` on `Spectrum-X_8g_4gps_100Gbps_A100`
reliably crashes with `double free or corruption` around layer 17/20.
Confirmed via `LIMER_TELEMETRY_ENABLE=0` that this reproduces **identically
with LIMER instrumentation fully disabled** (`limer/results/crash_debug/
run.log`), so this is a pre-existing bug in this SimAI build triggered by
this specific (many-layer workload) x (multi-server topology) combination,
not something introduced by LIMER. Root-causing it is out of scope for M1
(see final report "known limitations" / "next steps").

Workaround used for Phase 5/6: `limer/configs/microAllReduce_10iter.txt`
(new file, same format, 10 ALLREDUCE layers instead of 20) - verified
stable on this topology (exit 0, ~25.25ms simulated duration, 10/10
streams finished, `limer/results/crash_debug/run10.log`). All Phase 5/6
experiments use this workload.

## Dynamic, randomized fault injection (for detection-algorithm testing)

Beyond the two static Fault A/B scenarios above, `limer::FaultInjector`
(same file, `limer_telemetry.h`) supports mid-simulation, randomized
faults, added specifically so a future detection algorithm has labeled
data with faults that start/stop at arbitrary times rather than being
present for the whole run.

**Design**: all randomness is generated ahead of time by
`tools/generate_fault_schedule.py` (seeded, so a given seed always
reproduces the same schedule - this SimAI build has no RNG of its own, see
above). The output is a CSV with exactly `fault_events.csv`'s schema,
which doubles as both the schedule fed into the simulator
(`LIMER_FAULT_SCHEDULE` env var) and the ground-truth label file. The C++
side (`FaultInjector::Init()`) just reads it and calls
`Simulator::Schedule` twice per fault: once at `start_time_ns` to apply,
once at `end_time_ns` to revert. It only calls
`QbbNetDevice::SetDataRate()`/`SetReceiveErrorModel()` - both already
public methods SimAI's own topology parser uses at startup - no new ns-3
mechanism.

**Three real bugs found and fixed while getting this to work end-to-end**
(all verified against actual runs, not just inspection):

1. Python's `csv` module defaults to `\r\n` line endings; the C++ side's
   line-based CSV reader only split on `\n`, leaving a `\r` stuck to the
   last column and silently breaking `ns3::DataRate`'s string parser
   (`"40Gbps\r"` != `"Gbps"` trailer match -> `NS_FATAL_ERROR`, process
   aborted). Fixed on both sides: the generator now writes `\n` only, and
   the C++ parser defensively trims trailing `\r`/whitespace from every
   field regardless.
2. `configured_bandwidth_bps` in `switch_telemetry.csv` was read once from
   the static topology-parse-time value, so it never reflected a
   dynamically-applied fault - a naive `utilization = throughput /
   configured_bandwidth` during a fault window would have used the wrong
   (pre-fault) denominator. Fixed: `SampleSwitch` now reads
   `QbbNetDevice::GetDataRate()` live, every sample.
3. A point-to-point link has two NetDevice endpoints (one per node), both
   normally set to the same DataRate at topology setup. The fault injector
   initially resolved only one side (`GetDeviceForLink`, keyed by the
   lower node id - which for an ACCESS link is always the host side) and
   called `SetDataRate` on it alone, so the switch-side device never
   changed and the fault was invisible to switch-side telemetry entirely.
   Fixed: `GetBothDevicesForLink()` resolves both endpoints, and
   `Apply()`/`Revert()` set both.

**Verified end-to-end** (`limer/results/random_fault/random-fault-seed7/`,
seed 7, 3 non-overlapping bandwidth faults): `configured_bandwidth_bps` on
the targeted link switches from 100Gbps to the fault value exactly within
the scheduled window and back afterward; `queue_bytes` jumps from tens of
bytes to ~450KB during the fault and drains afterward; `tx_bytes` growth
rate visibly slows during the fault. All 8 applicable validation checks
pass (2 skipped - no monitoring-on/off parity JSON was generated for this
specific run, that check is orthogonal to fault injection).

**Usage**: `bash limer/scripts/run_random_fault_monitoring.sh <seed>
<num_faults>`. Packet-loss faults are opt-in only
(`--include-packet-loss` on the generator) since 0.5% loss is known to
hang the simulator (see "Fault B" above) - random packet-loss severities
default to a lower, still-unvalidated range; treat that path as
experimental.
