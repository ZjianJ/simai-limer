#!/bin/bash
# Reproduce a healthy run and a permanent ACCESS disconnect on a true 16-GPU,
# dual-ToR, dual-plane topology, then audit only runtime evidence.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
OUT_ROOT=${LIMER_TRUE16_OUT_ROOT:-"${LIMER_DIR}/results/true16_hard_fault_e2e"}
TOPO_DIR="${OUT_ROOT}/topology"
TOPO_NAME="Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
TOPO="${TOPO_DIR}/${TOPO_NAME}"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_16rank_hardfault.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
BINARY="${ROOT_DIR}/bin/SimAI_simulator"
TIMEOUT_S=${SIMAI_TRUE16_TIMEOUT_S:-900}
FAULT_START_NS=${LIMER_TRUE16_FAULT_START_NS:-50000}

if [ ! -x "${BINARY}" ]; then
  echo "SimAI binary is missing: ${BINARY}" >&2
  echo "Run: bash ${LIMER_DIR}/scripts/build_simai.sh" >&2
  exit 2
fi

mkdir -p "${TOPO_DIR}"
if [ ! -f "${TOPO}" ]; then
  (
    cd "${TOPO_DIR}"
    python3 "${ROOT_DIR}/astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py" \
      -topo Spectrum-X --dt --dp \
      -g 16 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps \
      -asn 8 -psn 64 -apbw 400Gbps
  )
fi

python3 "${LIMER_DIR}/tools/validate_true16_dualrail.py" \
  --topology "${TOPO}" \
  --expected-gpus 16 \
  --expected-gpus-per-server 4 \
  --out-json "${OUT_ROOT}/topology_validation.json"

run_one() {
  local name=$1
  local schedule=${2:-}
  local out_dir="${OUT_ROOT}/${name}"
  local raw_dir="${out_dir}/raw_simai"
  local log_dir="${out_dir}/astra_log"
  local rc

  mkdir -p "${raw_dir}" "${log_dir}"
  if [ -n "${schedule}" ]; then
    if (
      cd "${raw_dir}"
      ulimit -c 0
      timeout "${TIMEOUT_S}s" env \
        ASTRA_SIM_LOG_DIR="${log_dir}" \
        LIMER_TELEMETRY_ENABLE=1 \
        LIMER_TELEMETRY_INTERVAL_US=100 \
        LIMER_TELEMETRY_DIR="${out_dir}" \
        LIMER_RUN_ID="true16-${name}" \
        LIMER_FAULT_SCHEDULE="${schedule}" \
        LIMER_ALARM_PROCESSING_US=5 \
        LIMER_ALARM_TRANSPORT_US=50 \
        LIMER_ALARM_CONSUME_US=5 \
        LIMER_RDMA_RTO_US=100 \
        LIMER_RDMA_RETRY_LIMIT=7 \
        LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE=1 \
        AS_SEND_LAT=3 AS_NVLS_ENABLE=1 AS_PXN_ENABLE=0 AS_LOG_LEVEL=1 \
        "${BINARY}" -t 16 -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}"
    ) > "${out_dir}/run.log" 2>&1; then
      rc=0
    else
      rc=$?
    fi
  else
    if (
      cd "${raw_dir}"
      ulimit -c 0
      timeout "${TIMEOUT_S}s" env \
        ASTRA_SIM_LOG_DIR="${log_dir}" \
        LIMER_TELEMETRY_ENABLE=1 \
        LIMER_TELEMETRY_INTERVAL_US=100 \
        LIMER_TELEMETRY_DIR="${out_dir}" \
        LIMER_RUN_ID="true16-${name}" \
        LIMER_ALARM_PROCESSING_US=5 \
        LIMER_ALARM_TRANSPORT_US=50 \
        LIMER_ALARM_CONSUME_US=5 \
        LIMER_RDMA_RTO_US=100 \
        LIMER_RDMA_RETRY_LIMIT=7 \
        LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE=1 \
        AS_SEND_LAT=3 AS_NVLS_ENABLE=1 AS_PXN_ENABLE=0 AS_LOG_LEVEL=1 \
        "${BINARY}" -t 16 -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}"
    ) > "${out_dir}/run.log" 2>&1; then
      rc=0
    else
      rc=$?
    fi
  fi
  printf '%s\n' "${rc}" > "${out_dir}/exit_code.txt"
  if [ "${rc}" -ne 0 ]; then
    echo "${name} run failed with exit code ${rc}; see ${out_dir}/run.log" >&2
    return "${rc}"
  fi
}

run_one healthy

FAULT_EVENTS="${OUT_ROOT}/fault_events.csv"
FAULT_SELECTION="${OUT_ROOT}/fault_selection.json"
python3 "${LIMER_DIR}/tools/prepare_true16_hard_fault.py" \
  --link-map "${OUT_ROOT}/healthy/link_map.csv" \
  --healthy-nic "${OUT_ROOT}/healthy/nic_telemetry.csv" \
  --gpu 0 \
  --start-ns "${FAULT_START_NS}" \
  --out-csv "${FAULT_EVENTS}" \
  --out-json "${FAULT_SELECTION}"

run_one hard_disconnect "${FAULT_EVENTS}"

python3 "${LIMER_DIR}/tools/evaluate_true16_hard_fault_e2e.py" \
  --healthy-dir "${OUT_ROOT}/healthy" \
  --fault-dir "${OUT_ROOT}/hard_disconnect" \
  --fault-events "${FAULT_EVENTS}" \
  --fault-selection "${FAULT_SELECTION}" \
  --topology-validation "${OUT_ROOT}/topology_validation.json" \
  --expected-ranks 16 \
  --expected-links 304 \
  --expected-access-links 32 \
  --hard-detection-slo-ns 1000000 \
  --recovery-slo-ns 1000000000 \
  --out-json "${OUT_ROOT}/evaluation.json" \
  --out-md "${OUT_ROOT}/evaluation.md"

echo "true-16 hard-disconnect experiment complete"
echo "Report: ${OUT_ROOT}/evaluation.md"
