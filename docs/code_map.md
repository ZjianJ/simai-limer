# LIMER Code Map

Read from the actual cloned/checked-out source on branch `limer-monitoring`
(SimAI @ `f5efb5a`, `ns-3-alibabacloud` submodule @ local commit `1484b1a`,
built off upstream `7e3cb5b`), not from GitHub listings. Paths are relative
to the SimAI repo root (`~/limer-simai/SimAI`).

Node types (`Node::GetNodeType()`): `0` = host/GPU, `1` = switch (ASW/PSW),
`2` = NVSwitch.

## A. Switch / access-link layer

| Monitored object | Source file | Class · function | Available fields | Needs modification |
|---|---|---|---|---|
| Egress queue occupancy (per port, per priority queue) | `ns-3-alibabacloud/simulation/src/point-to-point/model/switch-mmu.h` | `SwitchMmu::egress_bytes[port][qIndex]` (uint64, live), `ingress_bytes`, `hdrm_bytes`, `paused[port][qIndex]` | queue_bytes per (switch, port, queue); PFC-paused flag | No — read-only access from a new getter |
| Existing switch queue print loop | `.../switch-node.cc:337` | `SwitchNode::PrintSwitchQlen(FILE*)` | Already sums `egress_bytes` per port every call; text-format only (not CSV, not per-queue LIMER schema) | No — model LIMER's sampler on this pattern, don't touch it |
| Egress tx byte counter (cumulative, per port) | `.../switch-node.h:24`, `.../switch-node.cc:306` | `SwitchNode::m_txBytes[ifIndex]`, incremented in `SwitchNotifyDequeue` | tx_bytes per (switch, port) | No — read-only |
| Existing switch bandwidth print loop | `.../switch-node.cc:359` | `SwitchNode::PrintSwitchBw(FILE*, interval)` | Computes bps delta from `m_txBytes`; text-format only | No |
| Packet drop (admission control reject) | `.../switch-node.cc:107-140` | `SwitchNode::SendToDev(Ptr<Packet>, CustomHeader&)` — two `return; // Drop` sites: (1) routing lookup miss (`idx < 0`, line ~138), (2) MMU ingress/egress admission failure (line ~130) | drop event with (switch, inDev, outDev/idx, qIndex, packet size) | **Yes** — add one counter-increment call at each `// Drop` site (2 one-line additions) |
| ECN marking | `.../switch-node.cc:199-221` | `SwitchNode::SwitchNotifyDequeue(ifIndex, qIndex, p)`, `egressCongested = m_mmu->ShouldSendCN(...)` branch | ecn_marks per (switch, port) | **Yes** — one counter increment inside the existing `if (egressCongested)` block |
| PFC pause/resume send | `.../switch-node.cc:92-105` | `SwitchNode::CheckAndSendPfc` / `CheckAndSendResume`, both call `device->SendPfc(qIndex, 0/1)` | pfc_events per (switch, port); pause vs resume via the 0/1 arg | **Yes** — one counter increment per function, right after `SendPfc(...)` |
| PFC frame format (existing, unused monitor) | `.../common.h:153` (astra-sim ns3 frontend) | `get_pfc(FILE*, Ptr<QbbNetDevice>, uint32_t type)` | Already formats (time, node, node_type, ifindex, type); not wired to any call site currently | No — reference only |
| Link bandwidth/delay/up-down state | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h:122-130` | `struct Interface { idx, up, delay, bw }`, `map<Ptr<Node>, map<Ptr<Node>, Interface>> nbr2if` | Per-link static bandwidth (bps), delay (ns), up/down — this is the authoritative link table built from the topology file | No — read-only, used for `link_map.csv` |
| Topology file parser (node/link roles) | `.../common.h:694-780` (`SetupNetwork`), topology text format confirmed empirically (`limer/results/baseline/topology/Spectrum-X_8g_8gps_100Gbps_A100`) | `SetupNetwork()` reads `node_num gpu_per_server nvswitch_num switch_num link_num gpu_type`, then switch-id list, then `src dst bw delay is_down` per link | Enough to classify every link as ACCESS / INTER_SWITCH / INTRA_NODE structurally, without touching ns-3 internals — see rule below | No — LIMER parses the same topology *file* independently in a small standalone script/tool |

**Link classification rule (derived empirically from the generated topology, confirmed against `gen_Topo_Template.py`'s `Rail_Opti_SingleToR`/Spectrum-X path):** hosts are node ids `[0, node_num - switch_num - nvswitch_num)`; the next `nvswitch_num` ids are NVSwitches; the remaining `switch_num` ids are regular switches (ASW nearest, PSW behind them). For a link `(a, b)`: `INTRA_NODE` if one endpoint is a host and the other is an NVSwitch id; `ACCESS` if one endpoint is a host and the other is a regular-switch id; `INTER_SWITCH` if both endpoints are regular-switch ids; `OTHER` otherwise (not expected to occur in these topologies).

## B. NIC / host (RDMA) layer

| Monitored object | Source file | Class · function | Available fields | Needs modification |
|---|---|---|---|---|
| Per-QP completion (flow completion time) | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/entry.h:299` | `qp_finish(FILE*, Ptr<RdmaQueuePair> q)` | sip, dip, sport, dport, m_size, startTime, duration (`Simulator::Now() - startTime`), standalone_fct — already written to `FCT_OUTPUT_FILE` (confirmed non-empty in baseline run) | No — read as-is; LIMER adds a parallel call at the same callback site, doesn't touch the existing FCT file |
| Per-QP send completion | `.../entry.h:347` | `send_finish(FILE*, Ptr<RdmaQueuePair> q)` | sip, dip, sport, m_size, completion tick | No |
| NACK / out-of-order detection (retransmission signal) | `ns-3-alibabacloud/simulation/src/point-to-point/model/rdma-hw.cc:584` | `RdmaHw::ReceiverCheckSeq(seq, Ptr<RdmaRxQueuePair> q, size)`, NACK branch at line ~598 (`Simulator::Now() >= q->m_nackTimer \|\| q->m_lastNACK != expected`) | nack event per (rx node, qp) | **Yes** — one counter increment in the NACK branch |
| Per-host bandwidth (existing, unused monitor) | `.../rdma-hw.cc` | `RdmaHw::PrintHostBW(FILE*, interval)` | Same tx-byte-delta pattern as `SwitchNode::PrintSwitchBw`, per-NIC | No — model LIMER's sampler on this pattern |
| Per-QP current rate (existing, unused monitor) | `.../rdma-hw.cc` | `RdmaHw::PrintQPRate(FILE*)` | current send rate per (src,dst,sport,dport) | No |
| Per-QP CNP count (existing, unused monitor) | `.../rdma-hw.cc` | `RdmaHw::PrintQPCnpNumber(FILE*)` | CNP (congestion notification) count per QP | No |
| Outstanding bytes per QP | `ns-3-alibabacloud/simulation/src/point-to-point/model/rdma-queue-pair.h:97,105` | `RdmaQueuePair::GetOnTheFly()` (= `snd_nxt - snd_una`, already public), `GetBytesLeft()` | outstanding bytes, remaining bytes, sampled at collector tick | No — already public, read-only |
| Dormant periodic monitor scheduler | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h:216` | `schedule_monitor()` | Wires `monitor_qlen`/`monitor_bw`/`monitor_qp_rate`/`monitor_qp_cnp_number` to `Simulator::Schedule` | Confirmed **no call site** in `main1()`/`SetupNetwork()` in this version — `QLEN_MON_FILE` etc. from `SimAI.conf` are parsed but never produced (verified: baseline run produced no `llama_hpn7_{qlen,bw,rate,cnp}.txt`). LIMER does not turn this on as-is (different schema); it adds its own sampler modeled on the same per-node traversal pattern. |

## C. Collective (astra-sim ↔ ns-3 bridge) layer

| Monitored object | Source file | Class · function | Available fields | Needs modification |
|---|---|---|---|---|
| Per-rank network API (send/recv entry points) | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/AstraSimNetwork.cc:63-211` | `class ASTRASimNetwork : public AstraSim::AstraNetworkAPI` — `sim_send`, `sim_recv`, `sim_schedule`, `sim_get_time` | rank, dst, count (message size), tag, current sim time (`Simulator::Now().GetNanoSeconds()`) | **Yes** — collective start hook belongs in `sim_send`/`sim_recv` request registration (or one level up, see next row) |
| Flow issue (maps astra-sim message to ns-3 RDMA QP) | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/entry.h:107` | `SendFlow(src, dst, maxPacketCount, msg_handler, fun_arg, tag, request)` | `request->flowTag` (has `current_flow_id`, `channel_id`, `sender_node`, `receiver_node`), message size, `AstraSim::Sys::boostedTick()` at send time | **Yes** — collective/flow *start* timestamp recorded here |
| Flow completion notify | `.../entry.h:280` (`notify_sender_sending_finished`), `.../entry.h:299` (`qp_finish`), `.../entry.h:347` (`send_finish`) | notify_* functions fire once all QPs for a flow tag finish | flow/collective finish timestamp, aggregate bytes | **Yes** — collective *finish* timestamp recorded here |
| Existing per-flow "detailed" CSV | Written by astra-sim's `NcclFlowModel` (inside `astra-sim-alibabacloud/astra-sim/`, not ns3-specific — confirmed via baseline run) | n/a (already active) | `ncclFlowModel_EndToEnd.csv` (1002 bytes, non-empty in baseline), `ncclFlowModel_detailed_<rank>.csv` (empty for non-participating ranks, e.g. rank 8 = the NVSwitch's Sys object), `ncclFlowModel_test1_dimension_utilization_0.csv` | No — confirmed working as-is; LIMER's `collective_telemetry.csv` is a normalized, schema-matching parallel output, not a replacement |
| Top-level driver / node roles | `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/AstraSimNetwork.cc:260-335` | `main()` — computes `gpu_num`, `nodes_num`, builds `node2nvswitch`, creates one `ASTRASimNetwork`+`AstraSim::Sys` pair per rank (including the NVSwitch's rank) | rank→node mapping, `gpu_num`, `gpus_per_server` | No — read `node_num`/`switch_num`/`nvswitch_num`/`gpus_per_server` (all extern globals from `common.h`) to build `run_manifest.json`'s topology summary |

## Files LIMER will touch (all additive, env/instrumentation only — see summary below)

| File | Change type |
|---|---|
| `ns-3-alibabacloud/simulation/src/point-to-point/model/switch-node.cc` / `.h` | Add: drop counter calls (2 sites), ECN counter call (1 site), PFC counter calls (2 sites), queue/tx-byte read-only getters |
| `ns-3-alibabacloud/simulation/src/point-to-point/model/switch-mmu.h` | Add: none planned (fields already accessible); revisit if a getter is needed beyond direct member access |
| `ns-3-alibabacloud/simulation/src/point-to-point/model/rdma-hw.cc` / `.h` | Add: NACK counter call (1 site), outstanding-bytes getter if not already public |
| `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/entry.h` | Add: `TelemetryCollector` sampling-loop kickoff in `main1()` after `SetupNetwork(...)`; start/finish hooks in `SendFlow`/`notify_sender_sending_finished`/`qp_finish` |
| `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/AstraSimNetwork.cc` | Add: link-map/topology-role export call right after topology is parsed |
| New: `astra-sim-alibabacloud/astra-sim/network_frontend/ns3/limer_telemetry.h` | New file — `TelemetryCollector` implementation (see `monitoring_design.md`) |

None of the above changes alter packet forwarding, admission-control decisions, RDMA
retransmission/congestion-control logic, or collective scheduling — every insertion is a
counter increment or a read of an already-computed value, gated by
`LIMER_TELEMETRY_ENABLE`. Verified empirically in Phase 4 (see
`limer/results/baseline/monitoring_on_off_parity.json`).
