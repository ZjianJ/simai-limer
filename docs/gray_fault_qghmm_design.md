# Quantized Gaussian HMM for ACCESS Gray Faults

## Scope and class boundary

This experiment learns four states from historical 16-GPU runs:

- `HEALTHY`
- `ACCESS_FAIL_SLOW`
- `TRANSIENT_LINK_ERROR_PROXY`
- `CONGESTION_HOTSPOT`

`LINK_DOWN` and explicit `LINK_FLAP` transitions bypass the Gaussian model and
remain on the event-driven fast path. They appear only in OOD tests. The short
error class uses SimAI's recoverable corruption proxy and is never described
as a complete RDMA sender-retry implementation.

The implementation preserves the existing telemetry schema and event order.
Only an optional ninth fault-schedule field selects the already implemented
link-layer recovery delay (10/50/100 us); schedules without it retain the
50 us default.

## Reproduction

Generate the complete 300-run manifest without starting simulations:

```bash
bash limer/scripts/run_gray_fault_qghmm_experiment.sh --generate-only
```

Run the complete resumable experiment and analysis:

```bash
bash limer/scripts/run_gray_fault_qghmm_experiment.sh
```

The simulation phase can be safely sharded. Each run has its own SimAI config,
raw files, log directory, telemetry directory, and immutable workload/schedule:

```bash
for shard in 0 1 2 3; do
  bash limer/scripts/run_gray_fault_qghmm_experiment.sh \
    --shard-index "$shard" --shard-count 4 --skip-analysis &
done
wait
bash limer/scripts/run_gray_fault_qghmm_experiment.sh --analysis-only
```

For a small plumbing test, set a separate output root so it cannot be confused
with the real experiment:

```bash
LIMER_GRAY_OUT_ROOT=limer/results/gray_fault_qghmm_pilot \
  bash limer/scripts/run_gray_fault_qghmm_experiment.sh --pilot
```

Pilot metrics are explicitly not research results and must not be used to tune
or claim accuracy.

## Benchmark design

The default manifest contains exactly 300 complete runs:

| Scenario | Runs |
|---|---:|
| HEALTHY | 40 |
| ACCESS_FAIL_SLOW | 100 |
| TRANSIENT_LINK_ERROR_PROXY | 70 |
| CONGESTION_HOTSPOT | 60 |
| OOD/stress | 30 |

Fail-slow covers remaining capacities 90/75/50/25%, durations
1/2/5/10/20 ms, step/ramp/intermittent/oscillation shapes, and seven phases
relative to the 1 ms snapshot grid. Transient errors cover four corruption
rates, five durations, and recovery delays of 10/50/100 us. Workload profiles
vary message sizes, issue cadence, compute gaps, and burst shape without
changing configured link capacity.

Four ACCESS links are chosen deterministically as unseen links. No fault on
those links may enter training. OOD runs cover unseen 60% capacity and 3/7 ms
durations, unseen message sizes, dual-link faults, link flap/down, non-ACCESS
faults, and an application-progress-stall workload proxy. The latter is an OOD
progress signal, not a claim of a simulated single-GPU hardware stall. The manifest is the authority for
run-level train/validation/test membership; windows are never randomly split.

## Label isolation and causality

`build_gray_fault_dataset.py` performs two passes:

1. It reads only switch telemetry, static link topology, and causally
   reconstructable collective start/completion state. It computes and freezes
   the feature table.
2. Only after feature construction is complete does it open schedule sidecars
   and attach offline labels, coverage, phase, severity, and observability
   weight.

Inference code copies exactly four identity fields and these eight features:

1. `tx_rate_residual`
2. `rx_rate_residual`
3. `tx_rx_asymmetry`
4. `queue_ewma_residual`
5. `queue_peak_residual`
6. `queue_growth`
7. `drop_error_delta`
8. `peer_rate_gap`

It never copies `fault_events.csv`, configured bandwidth, severity, target
link, injected parameters, labels, or future completion duration. Cumulative
counter deltas, 2/5 ms rolling statistics, EWMA, and queue growth use only the
current and preceding snapshots.

The collective context contains only expected-active, phase, and a compact
message-size profile supplied at dispatch time. This is legitimate host
context, not a future completion feature. Healthy means/scales are learned
only from training-split healthy runs; missing contexts fall back to a global
training-health baseline.

## Models and selection

- M0: unchanged `switch_sparse` from `compare_detection_baselines.py`.
- M1: weighted single-window diagonal Gaussian classifier.
- M2: weighted diagonal Gaussian emission plus a smoothed transition matrix
  learned from complete run/link label sequences.
- M3: two-component weighted diagonal GMM emission with the same learned HMM
  transitions.
- M4: a sparse RBF/Nystrom GP-like classifier used only as an unconstrained
  offline capability upper bound.

M3 is selected only when its validation macro-F1 exceeds M2 by at least two
points. Confidence uses a validation-learned 256-entry margin-to-correctness
table. Familiarity is the maximum absolute emission likelihood; low
familiarity produces `UNKNOWN`, while a small Top-1/Top-2 margin produces
`AMBIGUOUS`.

## Quantized inference and resources

Each normalized feature is clipped and mapped to `int8`. The clip range is
the maximum per-class training-only 99.5th percentile, preventing rare
drop/error events from being erased by the much larger healthy population.
Offline generation
creates an `int16` contribution table for every class/component, feature, and
possible byte value. HMM transitions and biases are also `int16`; the online
path performs table lookup, integer addition, maximum, and comparison only.
For M3, a 256-entry integer LUT approximates the two-component log-sum-exp
correction after the component maximum. Class priors are applied only at a
run/link sequence boundary, while mixture weights remain in every emission.

The full selected four-state M3 uses exactly 64 bytes dynamic state per ACCESS
port, 33,336 bytes of shared constants, 16,640 LUT entries, and 68 lookups,
76 additions, and 23 comparisons per inference. Repeating quantization from
the same locked inputs produces a byte-identical artifact.

This is a software resource accounting result. ASIC stage placement, memory
bank conflicts, and line-rate feasibility still require a named hardware/P4
target.

## Outputs and checks

The full output directory contains the requested manifest, context baselines,
float/quantized models, calibration table, all alarm onsets, confusion and
calibration tables, risk-coverage data, metrics, ablations, and final report.

The pipeline hard-fails on leakage or split violations and reports checks for:

- no ground truth during inference;
- no dynamic bandwidth input;
- run-level splits;
- unseen-link isolation;
- causal windows;
- deterministic quantization;
- float/quantized agreement;
- retention of every alarm onset;
- fault monitoring parity using an automatic on/off replay of one fixed
  fail-slow run;
- 64-byte state budget.

Go/No-Go is computed only by `evaluate_qg_hmm.py` after parameters are locked.
A small pilot is expected to be NO-GO and is useful only for verifying the
experiment machinery.

## Full 300-run result

All 300 immutable run specifications completed after isolated retries of six
non-deterministic upstream SimAI `double free` aborts. The final dataset has
129,088 causal port snapshots; all five dataset-isolation checks and all ten
inference/resource checks pass. Monitoring-on and monitoring-off executions of
the same fail-slow run both finish at exactly 13,798,766 ns.

Validation macro-F1 is M1=0.5955, M2=0.4054, M3=0.5679, and M4=0.5306. The
locked selection rule therefore retains M3 over M2. On the locked test set,
the quantized model obtains 0.4876 ID macro-F1, 0.8920 fail-slow recall, 0.5538
transient recall, 0.2326 congestion recall, 0.8046 unique Top-1 localization,
0.1225 ECE, and 0.3346 OOD AUROC. Float/quantized prediction agreement is
0.9887 and macro-F1 loss is 0.000081.

The decision is **NO-GO**. In particular, false alarms are 36.25 per healthy
run and UNKNOWN recall is 0.0075. M4 also fails to reach the acceptance bar,
so the next iteration should prioritize observability, feature sufficiency,
and the information overlap between normal congestion and faults instead of
adding model complexity. The exact metrics and criterion table are in
`results/gray_fault_qghmm/final_report.md` and `metrics.json`.
