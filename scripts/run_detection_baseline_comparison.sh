#!/bin/bash
# Run one healthy reference and one mixed-fault 16-GPU workload, then evaluate
# all four detection baselines against the exact same fault_events.csv.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
OUT_ROOT="${LIMER_DIR}/results/detection_baselines_16gpu"
TOPO_DIR="${OUT_ROOT}/topology"
TOPO="${TOPO_DIR}/Spectrum-X_16g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
HEALTHY_DIR="${OUT_ROOT}/healthy"
FAULT_DIR="${OUT_ROOT}/mixed_fault"
PYTHON_BIN="${LIMER_DIR}/.venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN=python3
fi

mkdir -p "${TOPO_DIR}" "${OUT_ROOT}/astra_log" "${FAULT_DIR}"
if [ ! -f "${TOPO}" ]; then
  cd "${TOPO_DIR}"
  python3 "${ROOT_DIR}/astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py" \
    -topo Spectrum-X -g 16 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps
fi

run_simulation() {
  local run_id=$1 out_dir=$2 schedule=${3:-}
  mkdir -p "${out_dir}/raw_simai"
  cd "${out_dir}/raw_simai"
  local -a env_args=(
    "ASTRA_SIM_LOG_DIR=${OUT_ROOT}/astra_log"
    "LIMER_TELEMETRY_ENABLE=1"
    "LIMER_TELEMETRY_INTERVAL_US=1000"
    "LIMER_TELEMETRY_DIR=${out_dir}"
    "LIMER_RUN_ID=${run_id}"
    "AS_SEND_LAT=3"
    "AS_NVLS_ENABLE=1"
  )
  if [ -n "${schedule}" ]; then
    env_args+=("LIMER_FAULT_SCHEDULE=${schedule}")
  fi
  env "${env_args[@]}" timeout 180 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
    -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" > "${out_dir}/run.log" 2>&1
}

echo "=== healthy reference ==="
run_simulation "detection-healthy-16gpu" "${HEALTHY_DIR}"

echo "=== fixed mixed ACCESS-fault schedule ==="
"${PYTHON_BIN}" "${LIMER_DIR}/tools/generate_detection_benchmark_schedule.py" \
  --link-map "${HEALTHY_DIR}/link_map.csv" --seed 2026 \
  --out-csv "${FAULT_DIR}/fault_events.csv"

echo "=== mixed-fault run ==="
run_simulation "detection-mixed-fault-16gpu" "${FAULT_DIR}" \
  "${FAULT_DIR}/fault_events.csv"

"${PYTHON_BIN}" "${LIMER_DIR}/tools/validate_telemetry.py" \
  --run-dirs "${FAULT_DIR}" --link-map "${FAULT_DIR}/link_map.csv" \
  --fault-events "${FAULT_DIR}/fault_events.csv" \
  --out-json "${OUT_ROOT}/telemetry_validation.json" \
  --out-md "${OUT_ROOT}/telemetry_validation.md"

"${PYTHON_BIN}" "${LIMER_DIR}/tools/compare_detection_baselines.py" \
  --run-dir "${FAULT_DIR}" --healthy-run-dir "${HEALTHY_DIR}" \
  --fault-events "${FAULT_DIR}/fault_events.csv" \
  --out-alarms "${OUT_ROOT}/detector_alarms.csv" \
  --out-comparison "${OUT_ROOT}/latency_comparison.csv" \
  --out-json "${OUT_ROOT}/comparison_summary.json" \
  --out-md "${OUT_ROOT}/latency_comparison.md"

echo "Comparison: ${OUT_ROOT}/latency_comparison.md"
