# LIMER Research Questions

## RQ1: sparse switch state

**With a fixed per-port state budget, which gray ACCESS faults can be detected
and localized from direct switch observations, and at what false-alarm cost?**

The current reference budget is 64 packed logical bytes per physical port.
RQ1 is evaluated separately for bandwidth degradation, short corruption/loss,
intermittent link flap, throughput anomaly, and queue anomaly. Results must
report detection rate, latency, localization rank, healthy false alarms, and
collateral alarms. A software packing assertion is not evidence of ASIC
pipeline feasibility; that requires a named hardware target.

## RQ2: alarm exposure across the stack

**For each type of ACCESS fault, when do direct switch observation, local NIC
observation, RDMA error exposure, and the collective watchdog respectively
produce an actionable alarm?**

The question is deliberately not "is the switch always faster than the host?"
Switch and NIC observations can tie when they use the same sampling cadence,
and some host-local counters can expose a fault quickly. The expected large
gap is instead between direct/local observation and errors that become visible
only after transport retry exhaustion or prolonged collective non-progress.

Use one ground-truth fault start, `t_fault`, and record four independently
defined exposure times:

| Time | Layer | Actionable-alarm contract | Scope available to an operator |
|---|---|---|---|
| `t_switch` | switch port | A causal port rule crosses threshold and names a physical link/port | reroute, drain, or rate-limit that link |
| `t_nic` | host NIC | A causal local counter/progress rule crosses threshold and names a NIC/link candidate | isolate a NIC/host or request path diagnosis |
| `t_rdma` | RDMA transport | Retry exhaustion, a Work Completion error, or an equivalent transport error is delivered | fail/retry a QP or notify the communication runtime |
| `t_collective` | collective runtime | A communicator/job watchdog or RAS policy declares prolonged non-progress | abort/restart/reconfigure a collective or job |

For layer `k`, report `L_k = t_k - t_fault`. If no alarm occurs before the
evaluation horizon, report it as right-censored with a lower bound; do not turn
it into a numeric latency. Also report localization scope, healthy false
alarms, and fault-propagation/collateral alarms. "Detected internally" and
"delivered to an operator/controller" should be separate timestamps when an
external polling interval exists.

### Three evidence classes

1. **Simulated direct observation.** SimAI currently provides exact event
   counters and 1 ms coherent snapshots for switch and NIC rules. These yield
   measured `t_switch` and `t_nic`.
2. **Compressed proxy.** The implemented 4 ms RDMA and collective no-progress
   timers only exercise the comparison pipeline on a millisecond-scale SimAI
   workload. They are not production defaults and do not simulate retry
   exhaustion, Work Completion errors, or a real NCCL watchdog. Their output
   must be labeled `proxy` or `censored`, never "real RDMA/NCCL latency."
3. **Production-policy projection or real-cluster measurement.** Evaluate the
   actual configured retry/watchdog policy on an extended timeline, or measure
   it on hardware. Keep projected and measured values in separate result
   columns.

NVIDIA's current NCCL documentation gives a concrete scale: the default
`NCCL_IB_TIMEOUT=20` and `NCCL_IB_RETRY_CNT=7` example waits approximately
30 seconds before raising a network error to NCCL. The RAS client query timeout
defaults to 5 seconds, while other RAS decisions use 5–60 second timers. These
are configuration-dependent reference points, not constants to hard-code into
all experiments:

- [NCCL InfiniBand timeout and retry-count documentation](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/env.html#nccl-ib-timeout)
- [NCCL RAS queries and timeout behavior](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/troubleshooting/ras.html)

### Required RQ2 result layout

Each fault instance should produce one row per layer with:

`fault_id, fault_type, target_link_id, layer, evidence_class, alarm_contract,
alarm_time, latency, censoring, localization_scope, action_scope,
configuration`.

This layout allows the same fault schedule to answer both practical questions:
how early a controller could act, and how much diagnostic precision is
available at that time.
