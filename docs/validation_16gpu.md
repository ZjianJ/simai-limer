# 16-GPU Full-Link Monitoring Validation

## Configuration

- Simulator: SimAI-Simulation/ns-3, 16 ranks, 16 GPUs
- Placement: 4 GPUs per server, 4 servers
- Topology: Spectrum-X, 100 Gbps network links, 2400 Gbps intra-node links
- Workload: 10-layer AllReduce
- Monitoring: coherent 1 ms snapshots plus event-updated cumulative counters
  and interval queue high-water timestamps
- Reproduction: `bash limer/scripts/run_16gpu_validation.sh`

## Observed result

| Item | Result |
|---|---:|
| Physical links in topology | 288 |
| INTRA_NODE / ACCESS / INTER_SWITCH | 16 / 16 / 256 |
| Physical links present in telemetry | 288 / 288 |
| Links carrying non-zero traffic | 288 / 288 |
| Fabric endpoint rows at every snapshot | 544 TX + 544 RX |
| Host physical-device rows at every snapshot | 32 |
| Snapshot times before completion | 25 |
| Flow completion rows | 8,960 |
| Distinct ranks / world size | 16 / 16 |
| Monitoring-on completion tick | 25,252,483 ns |
| Monitoring-off completion tick | 25,252,483 ns |
| Automated checks | 15 pass, 0 fail, 1 skip |

The one skipped check is `fault_target_is_access_link`: this is deliberately a
healthy run and has no fault schedule. All coverage, monotonicity,
counter-conservation, queue-peak, live-rate/state, collective-consistency, and
observer-parity checks passed.

## Verdict and precision boundary

The monitor is suitable as the 16-GPU signal source for subsequent LIMER
detection experiments: every physical link and endpoint is mapped, cumulative
traffic/error counters are updated by actual simulator events, current queue
and link state are sampled coherently, and transient queue peaks keep their
exact event timestamp without changing the simulated completion result.

It is not a literal CSV row for every continuous instant or every queue state
transition. The coherent snapshot floor is 1 ms because a 100 us MTP barrier
was observed to perturb deterministic event ordering. Requests below 1 ms are
therefore raised to 1 ms and logged; sub-ms event counters and queue high-water
timestamps remain exact. A full enqueue/dequeue event trace would require a
separate high-volume trace mode and is not part of this monitor.
