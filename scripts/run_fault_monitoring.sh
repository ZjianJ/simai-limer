#!/bin/bash
# Fault A (static bandwidth degradation) and Fault B (static per-link packet
# loss) on one GPU-ToR access link (rank 3 <-> its ASW, node 3 <-> node 13
# in the 8-GPU Spectrum-X topology - confirmed by reading the generated
# topology file, see limer/results/baseline/topology/).
#
# Both faults are injected purely via a per-link field in a copy of the
# topology file (see limer/tools/make_fault_topology.py and
# astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h's
# SetupNetwork(), which reads "src dst data_rate delay error_rate" per
# link line) - no simulator code changes needed for either fault.
#
# LIMER Phase M1 only supports *static* (whole-run) faults, not a
# mid-simulation transient - see limer/README.md "known limitations".
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

BASE_TOPO="${LIMER_DIR}/results/baseline/topology/Spectrum-X_8g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
FAULT_LINK_SRC=3
FAULT_LINK_DST=13

export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}"

mkdir -p "${LIMER_DIR}/results/fault"
FAULT_EVENTS_CSV="${LIMER_DIR}/results/fault/fault_events.csv"
echo "fault_id,fault_type,target_link_id,start_time_ns,end_time_ns,severity,parameter_before,parameter_after" > "${FAULT_EVENTS_CSV}"

run_case () {
  FAULT_ID="$1"; FAULT_TYPE="$2"; TOPO="$3"; PARAM_BEFORE="$4"; PARAM_AFTER="$5"
  RUN_ID="fault-${FAULT_ID}"
  OUT_DIR="${LIMER_DIR}/results/fault/${RUN_ID}"
  mkdir -p "${OUT_DIR}/raw_simai"
  echo "=== ${RUN_ID} (${FAULT_TYPE}) ==="

  export LIMER_TELEMETRY_ENABLE=1
  export LIMER_TELEMETRY_INTERVAL_US=1000
  export LIMER_TELEMETRY_DIR="${OUT_DIR}"
  export LIMER_RUN_ID="${RUN_ID}"

  START_WALL=$(date -Iseconds)
  set +e
  cd "${OUT_DIR}/raw_simai"
  AS_SEND_LAT=3 AS_NVLS_ENABLE=1 timeout 280 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
    -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" \
    > "${OUT_DIR}/run.log" 2>&1
  EXIT_CODE=$?
  cd - >/dev/null
  set -e
  END_WALL=$(date -Iseconds)

  echo "  exit_code=${EXIT_CODE}"
  if [ "${EXIT_CODE}" -ne 0 ]; then
    echo "  BLOCKED: ${RUN_ID} did not complete (exit ${EXIT_CODE}), see ${OUT_DIR}/run.log"
    echo "${FAULT_ID},BLOCKED-${FAULT_TYPE},L${FAULT_LINK_SRC}-${FAULT_LINK_DST},0,0,n/a,${PARAM_BEFORE},${PARAM_AFTER}" >> "${FAULT_EVENTS_CSV}"
    return
  fi

  SIM_END_NS=$(grep -oE "all passes finished at time: [0-9]+" "${OUT_DIR}/run.log" | grep -oE "[0-9]+" || echo "0")
  cp "${TOPO}" "${OUT_DIR}/topology_used"
  git -C "${ROOT_DIR}" rev-parse HEAD > "${OUT_DIR}/git_commit.txt" 2>/dev/null || true
  git -C "${ROOT_DIR}" submodule status > "${OUT_DIR}/submodule_status.txt" 2>/dev/null || true

  python3 "${SCRIPT_DIR}/../tools/write_run_manifest.py" \
    --run-id "${RUN_ID}" --out "${OUT_DIR}/run_manifest.json" \
    --root-dir "${ROOT_DIR}" --topology-path "${TOPO}" \
    --topology-name "$(basename "${TOPO}")" \
    --workload-file "${WORKLOAD}" --configuration-file "${CONF}" \
    --telemetry-interval-us "${LIMER_TELEMETRY_INTERVAL_US}" \
    --fault-enabled true --fault-id "${FAULT_ID}" --random-seed 42 \
    --start-wall-time "${START_WALL}" --end-wall-time "${END_WALL}" \
    --exit-code "${EXIT_CODE}"

  echo "${FAULT_ID},${FAULT_TYPE},L${FAULT_LINK_SRC}-${FAULT_LINK_DST},0,${SIM_END_NS},static-whole-run,${PARAM_BEFORE},${PARAM_AFTER}" >> "${FAULT_EVENTS_CSV}"
  echo "  output=${OUT_DIR}, sim_end_ns=${SIM_END_NS}"
}

# Fault A: bandwidth halved (100Gbps -> 50Gbps) on the target access link.
TOPO_A="${LIMER_DIR}/results/fault/topology_bandwidth_degradation"
python3 "${SCRIPT_DIR}/../tools/make_fault_topology.py" \
  --in-topo "${BASE_TOPO}" --out-topo "${TOPO_A}" \
  --src "${FAULT_LINK_SRC}" --dst "${FAULT_LINK_DST}" --bandwidth 50Gbps
run_case "bandwidth_degradation" "bandwidth_degradation" "${TOPO_A}" "100Gbps" "50Gbps"

# Fault B: 0.5% per-packet loss on the same link, bandwidth unchanged.
TOPO_B="${LIMER_DIR}/results/fault/topology_packet_loss"
python3 "${SCRIPT_DIR}/../tools/make_fault_topology.py" \
  --in-topo "${BASE_TOPO}" --out-topo "${TOPO_B}" \
  --src "${FAULT_LINK_SRC}" --dst "${FAULT_LINK_DST}" --error-rate 0.005
run_case "packet_loss" "packet_loss" "${TOPO_B}" "error_rate=0.0" "error_rate=0.005"

echo "Fault ground truth: ${FAULT_EVENTS_CSV}"
