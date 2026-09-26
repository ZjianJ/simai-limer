# P2 true-16 platform qualification

## Current result

The canonical completed qualification is:

```text
limer/results/stage_gates/p2/platform_qualification_v6/
```

`qualification.json` reports `PASS`, and the independently sealed Q1, Q2,
Q3, and Q4 stage manifests all report `PASS`.

The run used one content-addressed simulator runtime closure and one frozen
Python qualification-harness identity for all four stages:

```text
simulator ELF SHA-256:
0eec58c9a85f0fed78ac8a685713171c91c3e107c6788e01c640c02e9cfa3007

runtime closure identity:
1c21c76d9b7801206197c7e848ff71a739932d9377c1aa00916fcfb2cab3a6e1

Python harness identity:
177047a46b3ac12e0e809804fb9ebc372bcb99fb82786e2047eef43507e20ff5
```

The harness binding covers the ten loaded local Python modules used to plan,
run, and validate the qualification. Source-set, byte, or size drift between
stages is fail-stop.

## What Q1--Q4 establish

Q1, Q2, and Q3 are no-stop true-16 completion runs with 1, 10, and 32
AllReduce layers. Q4 is a 520 ms observation-window run with a 550-layer
declared workload. At the observation boundary, Q4 established:

- 304 physical links in the frozen topology/link-map contract;
- 519 exact 1 ms switch snapshots, with 1,120 switch/NVSwitch TX/RX rows per
  snapshot;
- 519 exact 1 ms host/NIC snapshots, with 48 rows per snapshot;
- both ACCESS rails for all 16 ranks (32 ACCESS links) showing positive byte
  growth through the healthy observation interval;
- no ACCESS traffic silence longer than 2 ms on the sampled timeline;
- 461 started and 461 causally committed AllReduce sequences, with no
  collective left in flight at the observation boundary;
- 885,120 completed sender-flow records spanning all 16 ranks;
- 885,120 training source-port allocations and releases, 37,408 real reuses,
  zero active ports at stop, and zero conflicts, exhaustion, or invariant
  errors;
- a real per-pair wrap beyond the 39,152-port allocation interval: maximum
  41,490 allocations for one pair and 2,338 reuses;
- simulator exit code 0, no OOM/OOM-kill delta, and peak RSS 589,824 KiB.

The independently reconstructed route-install sidecar contains 4,624 rows in
1,456 route groups and maps candidates to all 304 physical links. This proves
that the installed route graph covers every reconstructable fabric-to-host
route group under SimAI's no-HOST-transit rule.

## Scope limits

This qualification is a healthy-platform gate, not a detector or recovery
result.

- “Exact monitoring” currently means complete rows at every scheduled 1 ms
  sample from 1 ms through 519 ms. It does not mean continuous-time sampling
  or an observation at every packet event.
- Route-install coverage of all 304 physical links does not prove that this
  particular healthy workload transmitted packets over every candidate link.
  Dynamic ECMP path-choice evidence is a separate P2 gate.
- The qualification does not inject a hard disconnect or gray fault and does
  not measure online alarm-delivery latency.
- It does not establish ECN/PFC congestion-negative scenarios, RDMA WC error
  exposure, backup-QP failover, collective redo, or sub-second recovery.
- A 1 ms polling grid alone cannot guarantee a strict detection latency below
  1 ms. The hard-failure target requires the event-driven link-state path.

Therefore P2 remains in progress after this platform sub-gate. The next P2
entry criteria are executable congestion negatives, dynamic route-choice
evidence, and fully bound workload/role artifacts for the remaining corpus
scenarios.
