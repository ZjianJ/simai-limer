# Current-System End-to-End SLO Baseline

## Purpose and non-intervention rule

This experiment records what the current 16-GPU LIMER/SimAI system can do
before any recovery work. It does not change detector thresholds, train a new
model, tune RDMA timeouts, add a second path, create backup QPs, reroute a
packet, or add collective retry logic.

The target contract is represented by three strict intervals:

```text
hard detection:      t_alarm - t_fault < 1 ms
gray detection:      t_alarm - t_fault < 100 ms
restored progress:   t_progress_on_surviving_port - t_alarm < 1 s
```

The collective in flight must either complete correctly or be safely redone.
The baseline reports `UNVERIFIABLE`, rather than interpreting flow completion
as numerical correctness.

## Reproduction

First generate the locked 300-run source experiment if it is not present:

```bash
bash limer/scripts/run_gray_fault_qghmm_experiment.sh
```

Then run the non-mutating SLO analysis:

```bash
bash limer/scripts/run_current_system_slo_baseline.sh
```

The output directory is `limer/results/current_system_slo_baseline/`:

- `fault_events_evaluated.csv`: one row per selected fault;
- `detection_event_timeline.csv`: one row per fault and current detector;
- `detection_summary.csv`: scheduled- and observable-event metrics;
- `healthy_alarm_summary.csv`: healthy alarm counts over eight locked runs;
- `recovery_observations.csv`: current progress evidence and absent actions;
- `natural_completion_summary.csv`: same-port behavior after automatic clear;
- `capability_matrix.json`: implemented/unimplemented capability audit;
- `baseline_summary.json`: machine-readable headline results;
- `baseline_checks.json`: consistency and no-fabricated-claim checks;
- `baseline_report.md`: concise result table and evidence boundary.

## Event population and observability

The platform selects 82 locked test events from the existing benchmark:

| Type | Scheduled | Observable at target ACCESS port |
|---|---:|---:|
| fail-slow | 43 | 43 |
| transient packet error | 31 | 21 |
| 100 ms link-down interval | 4 | 4 |
| 0.2 ms link flap | 4 | 4 |

A transient schedule is observable only when it actually increments a target
port drop/error counter. Ten low-rate/short schedules did not hit a packet and
are retained in scheduled-event statistics, but not presented as detector
misses under the observable-event contract.

The four `link_down` cases are finite 100 ms down intervals, not permanent
failures. This distinction is important for the recovery result.

## Current results

All numbers below are offline causal signal-exposure times. No detector is
executed in the simulator and no controller-delivery timestamp exists, so they
do not yet establish an actionable end-to-end alarm SLO.

| Detector | Hard observable recall/deadline | Hard max | Gray observable recall/deadline | Gray median/P95 |
|---|---:|---:|---:|---:|
| `switch_sparse` | 100% / 100% | 0.950 ms | 100% / 100% | 0.575 / 1.050 ms |
| `qghmm_quantized` | 100% / 100% | 0.950 ms | 98.44% / 98.44% | 0.650 / 2.455 ms |
| `host_telemetry` | 100% / 100% | 0.950 ms | 87.50% / 87.50% | 0.650 / 2.088 ms |
| 4 ms RDMA proxy | 0% / 0% | — | 0% / 0% | — |
| 4 ms NCCL proxy | 50% / 0% | 50.95 ms | 0% / 0% | — |

On all scheduled gray events, including the ten with no realized packet
error, switch/QG-HMM recalls are respectively 86.49% and 85.14%.

Healthy alarms over eight locked runs are:

| Detector | Total | Per run |
|---|---:|---:|
| `switch_sparse` | 1 | 0.125 |
| `qghmm_quantized` | 290 | 36.25 |
| `host_telemetry` | 0 | 0 |
| RDMA proxy | 0 | 0 |
| NCCL proxy | 0 | 0 |

The current operational reference is therefore `switch_sparse`, not the
learned model: it has the best observable recall, localization, latency, and
healthy alarm rate in this locked baseline.

## Recovery result

Recovery is `FAIL_NOT_IMPLEMENTED` for all 82 events:

- every simulated host has exactly one direct ACCESS link;
- there is no explicit backup-path mapping or pre-established backup QP;
- alarms do not run online and cannot quiesce a port;
- there is no controller cutover event or surviving-port traffic identity;
- the 4 ms RDMA timer is a no-progress proxy, not sender RTO/retry exhaustion;
- there is no collective abort/commit/redo epoch;
- SimAI records flow completion but does not calculate tensor values.

All 82 finite-fault runs eventually complete their streams. This is not a
successful recovery result. In the four link-down cases the schedule restores
the same port after exactly 100 ms; target traffic returns to that same port a
median 0.5 ms later, and the runs complete a median 144.37 ms after fault
onset. No traffic is moved to a named surviving port.

## Interpretation boundary

The current system can expose hard and observable gray fault signals within
the requested numerical windows in offline replay. It cannot yet claim the
requirements themselves because alarm delivery and action are absent. The
one-second target remains an explicit failed baseline rather than being
silently satisfied by automatic fault expiry or unrelated flow completions.
