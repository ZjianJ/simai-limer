# LIMER Telemetry Schema

All three CSVs and the manifest share one simulation-time origin: ns-3's
`Simulator::Now().GetNanoSeconds()` at the moment `main1()` starts the network
(right after `SetupNetwork()` returns, before `Simulator::Run()`), i.e. tick 0
of the topology/route setup phase. Every `timestamp_ns` in every file is this
same ns-3 virtual clock, not wall-clock time. Sources reference
`limer/docs/code_map.md`.

Status legend: **live** = written from a real counter every sample/event;
**proxy** = a simulator-internal value used as a stand-in for something that
doesn't exist as a discrete hardware counter in this codebase (documented
per-field); **unavailable** = column exists for schema completeness but this
SimAI/ns-3 fork has no underlying signal for it, so it is left empty and
must not be fabricated.

## switch_telemetry.csv

| Field | Type | Status | Source |
|---|---|---|---|
| run_id | string | live | passed in from the run script / `run_manifest.json` |
| timestamp_ns | uint64 | live | `Simulator::Now().GetNanoSeconds()` at sample time |
| switch_id | uint32 | live | `SwitchNode::m_id` / ns-3 node id |
| port_id | uint32 | live | ns-3 interface index (`ifIndex`), matches `SwitchNode::PrintSwitchQlen`'s loop variable |
| link_id | string | live | looked up from `link_map.csv` via (switch_id, port_id) |
| peer_node_id | uint32 | live | far-end node id from `nbr2if` (topology adjacency) |
| direction | enum{tx,rx} | live | one row per direction per sample |
| tx_packets | uint64 | proxy | ns-3 doesn't keep a native per-port packet counter on `SwitchNode`; LIMER adds one alongside `m_txBytes` (increment at the same site, `SwitchNotifyDequeue`) |
| tx_bytes | uint64 | live | `SwitchNode::m_txBytes[port]`, cumulative |
| rx_packets | uint64 | proxy | same rationale as tx_packets, incremented at `SendToDev` ingress |
| rx_bytes | uint64 | live | derived from `m_bytes[inDev][*][*]` accumulation at `SendToDev` |
| dropped_packets | uint64 | live | new counter incremented at both `// Drop` sites in `SwitchNode::SendToDev` |
| drop_bytes | uint64 | live | packet size summed at the same sites |
| queue_packets | uint32 | unavailable | `SwitchMmu` tracks queue occupancy in bytes only (`egress_bytes`), not packet count; left empty rather than estimated |
| queue_bytes | uint64 | live | `SwitchMmu::egress_bytes[port][qIndex]` summed over `qIndex`, instantaneous read at sample time |
| max_queue_packets | uint32 | unavailable | same reason as queue_packets |
| max_queue_bytes | uint64 | live (derived) | computed by the Phase 7 windowing script as max of `queue_bytes` samples within each window, not by the C++ collector |
| ecn_marks | uint64 | live | new counter incremented in `SwitchNode::SwitchNotifyDequeue`'s `egressCongested` branch |
| pfc_events | uint64 | live | new counter incremented in `CheckAndSendPfc`/`CheckAndSendResume` right after `device->SendPfc(...)` |
| link_errors | uint64 | unavailable | this ns-3 fork has no PHY-level bit-error/link-flap model on point-to-point/QBB links (only explicit `ERROR_RATE_PER_LINK` packet drop, already captured as dropped_packets); left empty |
| configured_bandwidth_bps | uint64 | live | `QbbNetDevice::GetDataRate()` read fresh at every sample (not the cached topology-parse-time value) - deliberately live so it reflects a `FaultInjector`-applied bandwidth-degradation fault during its active window; `utilization` below is therefore correct even mid-fault. Falls back to the static topology value if the device somehow isn't a `QbbNetDevice`. |
| observed_throughput_bps | double | live (derived) | computed by the windowing script from `tx_bytes` delta / window width |
| utilization | double | live (derived) | `observed_throughput_bps / configured_bandwidth_bps`, computed by the windowing script |

## nic_telemetry.csv

| Field | Type | Status | Source |
|---|---|---|---|
| run_id | string | live | as above |
| timestamp_ns | uint64 | live | as above |
| node_id | uint32 | live | ns-3 host node id (`GetNodeType() == 0`) |
| rank_id | uint32 | live | astra-sim rank == host node id in this topology (one GPU per host node) |
| nic_id | uint32 | live | ns-3 NIC device index on the host (`gpus_per_server` is 8 but this topology has 1 NIC/GPU, so nic_id == the QbbNetDevice ifIndex) |
| link_id | string | live | looked up from `link_map.csv` (host's ACCESS link) |
| tx_packets | uint64 | proxy | new counter at `RdmaHw`'s packet-send path, no native counter existed. **Caveat found during verification:** this counter lives on the `RdmaHw` object (one per host), not per-NIC, so it is a host-level total repeated identically across every `nic_id` row for that host in a given sample - `tx_bytes`/`rx_bytes` (below) are correctly per-NIC, only the packet counts are not |
| tx_bytes | uint64 | live | `RdmaQueuePair` bytes sent, same source as `RdmaHw::PrintHostBW` |
| rx_packets | uint64 | proxy | new counter, same rationale and same host-level-not-per-NIC caveat as tx_packets |
| rx_bytes | uint64 | live | same source as `RdmaHw::PrintHostBW`'s rx path |
| retransmissions | uint64 | proxy | this RDMA model (go-back-N / IRN-style) doesn't label individual retransmitted packets separately from NACK-triggered resends; LIMER counts triggered NACK responses as the retransmission proxy (see nacks below) — same underlying event, reported in both columns for compatibility with the schema, not two independent measurements |
| nacks | uint64 | live | new counter incremented at `RdmaHw::ReceiverCheckSeq`'s NACK-send branch |
| outstanding_packets | uint32 | unavailable | `RdmaQueuePair::GetOnTheFly()` is byte-granular only; no packet-count equivalent without assuming a fixed packet size, which would be an estimate, not a measurement — left empty |
| outstanding_bytes | uint64 | live | `RdmaQueuePair::GetOnTheFly()`, instantaneous read at sample time |
| effective_throughput_bps | double | live (derived) | computed by the windowing script from tx_bytes delta / window width |
| completion_delay_ns | uint64 | live | per-QP duration from `qp_finish`'s `(Simulator::Now() - q->startTime)`, recorded as a discrete event, not sampled |
| rtt_proxy_ns | uint64 | proxy | this simulator models RTT as a static routing-table quantity (`pairRtt[src][dst]`, used for BDP/window sizing), not a per-packet measured RTT; LIMER reports this static `pairRtt` value as the proxy, clearly distinct from a measured RTT |

## collective_telemetry.csv

| Field | Type | Status | Source |
|---|---|---|---|
| run_id | string | live | as above |
| collective_id | string | live | `AstraSim::ncclFlowTag.current_flow_id` (per code_map.md row C) |
| iteration_id | uint32 | live | astra-sim workload layer iteration counter, passed through `flowTag` context at `SendFlow` |
| layer_id | string | live | astra-sim layer name (e.g. `embedding_layer`), available at the `Sys`/workload level, passed down to the flow-start hook |
| collective_type | string | live | from the workload file (e.g. `ALLREDUCE`) |
| algorithm | string | live | `NcclFlowModel` (this SimAI build's fixed collective implementation, confirmed in baseline `run.log`: "all-reduce Collective implementation: NcclFlowModel") |
| rank_id | uint32 | live | `flowTag.sender_node` / `receiver_node` |
| world_size | uint32 | live | `gpu_num` computed in `AstraSimNetwork.cc:main()` |
| message_size_bytes | uint64 | live | `count` argument to `sim_send`/`SendFlow` |
| start_time_ns | uint64 | live | `Simulator::Now().GetNanoSeconds()` at `SendFlow` call |
| finish_time_ns | uint64 | live | `Simulator::Now().GetNanoSeconds()` at `notify_sender_sending_finished`/`qp_finish` completion |
| duration_ns | uint64 | live (derived) | finish_time_ns - start_time_ns |
| status | enum{ok,timeout} | live | `ok` unless the simulation's own timeout/assert path fires (`SIMULATOR_STOP_TIME` in `SimAI.conf`); no separate fault-injection status leaks in here (see `fault_events.csv` isolation below) |

## run_manifest.json

| Field | Source |
|---|---|
| run_id | generated per run (e.g. `healthy-seed42-<timestamp>`) |
| git_commit | `git rev-parse HEAD` in the SimAI repo at run time |
| submodule_commits | `git submodule status` at run time |
| topology_name | topology file name passed to `-n` |
| gpu_count | `gpu_num` from topology header |
| gpus_per_server | topology header field |
| workload_file | `-w` argument |
| configuration_file | `-c` argument (a `limer/configs/*.conf`, never the stock upstream conf) |
| telemetry_interval_us | `LIMER_TELEMETRY_INTERVAL_US` (default 1000) |
| fault_enabled | true/false + fault_id if applicable |
| random_seed | seed used for this run (see `monitoring_design.md` for what "seed" controls in a deterministic discrete-event simulator) |
| start_wall_time / end_wall_time | wall-clock ISO8601, for the overhead study, not simulation time |
| exit_code | `SimAI_simulator` process exit code |

## Label isolation

`fault_events.csv` (ground truth: fault_id, fault_type, target_link_id,
start_time_ns, end_time_ns, severity, parameter_before, parameter_after) is
written by the run script from the fault *configuration*, never by the C++
collector, and never joined into the three telemetry CSVs above. Label
alignment happens only in the Phase 7 post-processing step
(`tools/build_monitoring_dataset.py`), by timestamp + link_id, keeping the
telemetry files themselves label-free.
