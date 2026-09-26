# LIMER Telemetry Schema

All CSV timestamps use the same ns-3 virtual clock:
`Simulator::Now().GetNanoSeconds()`. They are simulation time, not wall time.
“Live” means the value comes from simulator state or an event-updated counter;
“proxy” is explicitly derived from a related signal; “unavailable” is left
empty rather than fabricated.

## switch_telemetry.csv

One `tx` and one `rx` row are emitted for every physical SWITCH and NVSWITCH
endpoint at every coherent snapshot. Thus INTRA_NODE, ACCESS, and
INTER_SWITCH links are represented.

| Field | Status | Meaning/source |
|---|---|---|
| run_id, timestamp_ns | live | Run identity and snapshot time |
| switch_id, port_id | live | ns-3 node and physical `QbbNetDevice` interface; loopback 0 is excluded |
| link_id, peer_node_id | live | Physical link identity and peer from the topology adjacency map |
| direction | live | `tx` or `rx` endpoint row |
| tx_packets, tx_bytes | live | Cumulative successful channel transmissions on this physical device |
| rx_packets, rx_bytes | live | Cumulative successful receives after receive-error filtering |
| dropped_packets, drop_bytes | live | TX admission/channel drops on `tx` rows; receive/error drops on `rx` rows |
| queue_packets, queue_bytes | live | Current total egress occupancy on `tx` rows |
| max_queue_packets, max_queue_bytes | live | Event-updated high-water since the preceding snapshot |
| max_queue_timestamp_ns | live | Exact simulator time of the interval byte high-water |
| ecn_marks, pfc_events | live | Cumulative switch congestion-control events |
| link_errors | live | Cumulative receive errors from the QBB receive-error model |
| recovered_packets, recovered_bytes | live | Corrupted deliveries recovered by the finite benchmark link-layer recovery path |
| configured_bandwidth_bps | live | Current `QbbNetDevice::GetDataRate()`; reflects dynamic bandwidth faults |
| observed_throughput_bps | live | `tx_bytes` delta divided by exact elapsed simulator time |
| utilization | live | Throughput divided by current configured rate |
| node_type | live | `SWITCH` or `NVSWITCH` |
| link_state | live | Physical/device state including the benchmark forced-down latch: `up` or `down` |
| flap_count, last_link_down_ns, last_link_up_ns, cumulative_link_down_ns | live | Event-latched link transitions and down time; a sub-snapshot flap remains visible |

## nic_telemetry.csv

One row is emitted per physical HOST device. Device 0 is loopback and is not
mistaken for a NIC; each `nic_id` maps to a non-empty `link_id`.

| Field | Status | Meaning/source |
|---|---|---|
| run_id, timestamp_ns | live | Run identity and snapshot time |
| node_id, rank_id, nic_id, link_id | live | Host/rank, exact physical device index, and mapped physical link |
| tx_packets, tx_bytes | live | Per-device cumulative successful transmissions |
| rx_packets, rx_bytes | live | Per-device cumulative successful receives |
| tx/rx dropped packets/bytes, link_errors | live | Per-device channel/receive failure counters |
| recovered_packets, recovered_bytes | live | Per-device finite link-layer recoveries after transient corruption |
| retransmissions | proxy | Same NACK-triggered resend event as `nacks`; not an independent signal |
| nacks | live | Per-port NACK counter from `RdmaHw::ReceiverCheckSeq` |
| outstanding_packets | unavailable | QP state is byte-granular; left empty |
| outstanding_bytes | live | Sum of `RdmaQueuePair::GetOnTheFly()` for QPs mapped to this port |
| effective_throughput_bps | live | Per-device `tx_bytes` delta over exact elapsed time |
| completion_delay_ns | unavailable | No single QP completion belongs unambiguously to a periodic device row |
| rtt_proxy_ns | unavailable | No measured per-device/per-sample RTT exists |
| queue_packets, queue_bytes | live | Current host-device egress occupancy |
| max_queue_packets, max_queue_bytes, max_queue_timestamp_ns | live | Event-updated interval queue high-water and timestamp |
| configured_bandwidth_bps, link_state, utilization | live | Current device rate/state and transmit utilization |
| flap_count, last_link_down_ns, last_link_up_ns, cumulative_link_down_ns | live | Per-device event-latched transition state |

## Optional split-baseline sidecars

When `LIMER_SPLIT_POLICY` is enabled, `split_events.csv` records each actual
chunk QP assignment and ACK-qualified completion: virtual timestamp, policy,
source/destination, source port, selected rail, payload bytes, capacity used,
remaining reserved payload, logical flow ID, chunk offset and original flow
bytes. A QP's source port may be reused only after its completion/cleanup;
the logical flow and offset, not port alone, identify the payload interval.

`split_samples.csv` records per-source/rail cumulative unique ACKed payload,
interval goodput, EWMA, NIC queue, outstanding bytes and the explicit demand
gate. ACK deltas are clipped to the QP's payload size and exclude background
QPs and duplicate ACKs. These are separate from wire-byte TX/RX counters.
The existing minimum 1-ms coherent sampling cadence still applies. See
[split_baselines.md](split_baselines.md) for oracle visibility and queue scope.

## collective_telemetry.csv

These rows describe completed ns-3 flows, not fully attributed training-layer
collectives. The frontend flow tag does not contain workload layer/iteration
context, so `iteration_id` and `layer_id` remain empty.

| Field | Status | Meaning/source |
|---|---|---|
| run_id, collective_id | live | Run and `ncclFlowTag.current_flow_id` |
| iteration_id, layer_id | unavailable | Not carried by this frontend hook |
| collective_type, algorithm | proxy | Workload/model constants `ALLREDUCE` and `NcclFlowModel` |
| rank_id, world_size | live | Completing rank and simulated GPU count |
| message_size_bytes | live | Flow byte count passed to `SendFlow` |
| start_time_ns, finish_time_ns | live | Flow start/completion event times |
| duration_ns | derived | `finish_time_ns - start_time_ns` |
| status | live | `ok` for completed rows |

## rdma_wc_telemetry.csv and background_flow_application.csv

`rdma_wc_telemetry.csv` records QP creation, retry, WC, backup-ready, and
failover events. Its `traffic_class` field is `TRAINING`, `BACKGROUND`, or
`CONTROL`; RDMA/NCCL baselines must filter to `TRAINING` so injected congestion
traffic cannot become a false training alarm.

The terminal transport fields are per-QP live values, not planner metadata:

| Field | Status | Meaning/source |
|---|---|---|
| primary_nic, backup_nic, active_nic | live | Physical host ns-3 device indices bound to the QP; they are not zero-based rail labels |
| retry_count | live | Number of RTO retries already issued for this QP |
| retry_limit | live | Effective retry limit copied into this QP at creation |
| rto_us | live | Effective RTO copied into this QP at creation; this makes the runtime transport setting auditable from raw evidence |

For an executable `incast` or `queue_buildup` control, the independent
`background_flow_application.csv` sidecar records exactly one
`SCHEDULED -> START -> COMPLETE` lifecycle per declared QP. `COMPLETE` is
ACK-qualified and carries first-TX and first-ACK timestamps. A flow still
active at the observation horizon is emitted as `CENSORED` and the P2 runner
refuses to publish that run as complete evidence.

### P2 v5 single-rail background evidence contract

The prepared-v4 corpus is retained as immutable historical output, but it is
not admissible P2 execution evidence. Its congestion flows could hash across
both rails and its nominal 200 ms label interval did not describe the much
shorter interval in which the finite flows actually produced pressure.

Prepared v5 replaces that interpretation with a hash-locked, single-port
contract:

- Host device `ifIndex=2` is Plane A / route bucket 0; `ifIndex=3` is Plane B /
  route bucket 1. `primary_nic` and `active_nic` must contain 2 or 3, while
  `route_bucket` remains 0 or 1.
- Every reserved background source port is selected so both the forward DATA
  4-tuple and reverse ACK 4-tuple produce the declared ns-3 Murmur3 route
  bucket. The two directions therefore use the same physical rail.
- The declared launch window is the schedule interval from the earliest QP
  start through one nanosecond after the latest QP start. The realized pressure
  window is measured independently, from the first DATA transmission through
  the last ACK-qualified completion; queue-buildup also preserves per-wave
  timing. Neither window is inferred from a nominal fault-label duration.
- Every background QP must reach ACK-qualified `SUCCESS` no later than the
  declared 200,000,000 ns completion deadline. Its auditable transport values
  are `rto_us=250000`, `retry_limit=0`, and `retry_count=0`, with no retry,
  failover, standby, or error events.
- Congestion evidence is evaluated only on the declared target ACCESS link and
  physical target port. The paired ACCESS link is retained for topology and
  isolation checks, but its queue peak or throughput cannot satisfy the target
  link's evidence requirement.

These are evidence-admission rules, not a completed result. P2 is not passed
until the v5 runs themselves are executed, hash-bound, and accepted by the
stage evaluator; a prepared corpus or a static source test cannot unlock P3.

## link_map.csv and run_manifest.json

`link_map.csv` is generated from the loaded topology and is the authoritative
one-row-per-physical-link mapping: node types, endpoint ports, class, nominal
bandwidth, and delay. The run manifest records source revisions, topology,
GPU count, workload/configuration, fault identity, requested telemetry
interval, wall times, and exit status. Requested intervals below 1000 us are
raised to the safe 1000 us MTP snapshot cadence and a warning is written to
the run log.

## Label isolation

`fault_events.csv` contains ground truth only. It is never joined into raw
telemetry. `tools/build_monitoring_dataset.py` attaches labels afterward by
physical `link_id` and actual time-window overlap, including
`fault_overlap_fraction` for boundary windows.

The simulator fault-schedule input accepts an optional ninth
`recovery_delay_ns` field for recoverable packet corruption. Omitting it keeps
the 50 us default, so existing schedules are unchanged. This injected value
is schedule-side ground truth and is never exposed as an inference feature.
