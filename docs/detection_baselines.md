# Four Detection Baselines and Shared-Schedule Comparison

The experiment follows the revised [RQ2 alarm-exposure definition](research_questions.md):
compare when each layer produces an actionable signal, without assuming that
switch observation must always beat NIC-local observation.

## What is implemented

`tools/compare_detection_baselines.py` replays telemetry causally and emits
alarms before joining any ground truth. It implements:

1. `switch_sparse`: switch-port throughput EWMA, queue high-water, drop/error,
   and latched flap rules. Its packed state format is asserted to be exactly
   64 bytes per port (`<QQIIIIffffIHHII`).
2. `host_telemetry`: NIC progress, outstanding-byte growth, NACK/error, queue,
   and link-transition rules; 72 logical bytes per port.
3. `rdma_timeout`: a 4 ms no-byte-progress proxy while QP bytes are
   outstanding; 32 logical bytes per port.
4. `nccl_watchdog`: a 4 ms no-flow-completion proxy while collective flows are
   active; 24 logical bytes per job. It is job-global and cannot localize a
   physical link.

The two 4 ms values are deliberately compressed test thresholds for this
roughly 35 ms simulated workload. They validate causal timer/censoring logic;
they do **not** represent real retry exhaustion, Work Completion errors, NCCL
defaults, or NCCL RAS behavior. Production-policy projections and real-cluster
measurements must be reported separately from the simulated switch/NIC times.

The detectors do not read `fault_events.csv`, `configured_bandwidth_bps`, the
fault severity, or the injected parameter. Ground truth is joined only after
alarms are finalized. `CENSORED` means no alarm was produced before the next
fault or the configured follow-up deadline.

The implementation is a reference/offline replay of the online state machine,
not P4 or switch-ASIC code. The packed state assertion establishes the state
budget; instruction/pipeline feasibility still requires a hardware target.

## Shared 16-GPU schedule

`tools/generate_detection_benchmark_schedule.py` creates one CSV, consumed by
the simulator once and then shared by all four detectors:

| Fault | ACCESS link | Interval | Model |
|---|---|---:|---|
| 50% bandwidth degradation | L0-20 | 4.1–8.1 ms | 100 to 50 Gbps |
| short packet loss | L11-23 | 11.1–11.6 ms | 1% corruption; failed delivery is counted and recovered after a 50 us link-layer delay |
| intermittent link flap | L2-22 | 16.1–16.3 ms | state latched down while capacity is reduced to 1 Gbps, then restored |

The recovery model is necessary because this SimAI RDMA implementation has no
sender retransmission timeout: a permanently lost final QP packet can stall
forever because no subsequent out-of-order packet exists to trigger a NACK.
The first attempted benchmark reproduced that stall. Link-layer recovery makes
the short fault finite while retaining real error counters and a real 50 us
traffic delay. The flap uses near-zero capacity instead of route removal so it
is intermittent and recoverable without rebuilding routing tables.

Reproduce everything with:

```bash
bash limer/scripts/run_detection_baseline_comparison.sh
```

## Initial measured result

All three fault starts are deliberately offset from the 1 ms snapshot grid.

| Fault | Switch sparse | Host telemetry | RDMA timeout | NCCL watchdog |
|---|---:|---:|---:|---:|
| bandwidth degradation | 0.9 ms, unique Top-1 | 0.9 ms, Top-1 tie across 2 links | censored; switch lead >6.1 ms | censored; switch lead >6.1 ms |
| short packet loss | 0.9 ms, unique Top-1 | 0.9 ms, unique Top-1 | censored; switch lead >4.1 ms | censored; switch lead >4.1 ms |
| link flap | 0.9 ms, unique Top-1 | 0.9 ms, unique Top-1 | censored; switch lead >4.3 ms | censored; switch lead >4.3 ms |

The healthy reference produced zero alarms for all four baselines. The mixed
fault run completed all 10 streams and all 16 ranks. Telemetry validation
reported 15 pass, 0 fail, and 2 parity-only skips.

The mixed-fault run also produced 16 collateral alarms on non-target links or
outside their target windows. They are retained in `detector_alarms.csv`:
congestion and stalled progress can propagate beyond the injected ACCESS link,
so hiding them would overstate point localization. The scored first-alarm
ranking above still places the injected link uniquely first for all three
switch detections and two of three host detections.

This is an implementation validation, not a statistically sufficient research
conclusion. It contains one schedule and three faults. In particular, equal
1 ms sampling makes switch and host detection tie on target-link alarm time;
the current advantage of the switch baseline is localization (3/3 unique
Top-1 versus 2/3 for host telemetry), not lower latency. Severity/duration
sweeps and multiple schedules are required before claiming a general lead.
