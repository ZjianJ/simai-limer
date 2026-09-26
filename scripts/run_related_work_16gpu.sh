#!/bin/bash
# Unified mechanism-level evaluation of FANcY, Trumpet, NetBouncer, MP-RDMA,
# Flor, SHIFT, OptCC, and ReCoVer on the locked LIMER fault ledger.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
BASELINE_DIR="${LIMER_RELATED_WORK_BASELINE:-${LIMER_DIR}/results/current_system_slo_baseline}"
OUT_DIR="${LIMER_RELATED_WORK_OUT:-${LIMER_DIR}/results/related_work_16gpu}"
CONFIG="${LIMER_DIR}/configs/related_work_16gpu.json"
SOURCE_WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CORRECTED_WORKLOAD="${LIMER_DIR}/configs/microAllReduce_16rank_10iter.txt"
SMOKE_WORKLOAD="${LIMER_DIR}/configs/microAllReduce_16rank_smoke.txt"
TOPOLOGY="${LIMER_DIR}/results/validation_16gpu/topology/Spectrum-X_16g_4gps_100Gbps_A100"
SIM_CONFIG="${LIMER_DIR}/configs/SimAI.baseline.conf"
DATASET_MANIFEST="${LIMER_DIR}/results/gray_fault_qghmm/dataset_manifest.json"
FEATURE_DATASET="${LIMER_DIR}/results/gray_fault_qghmm/gray_fault_dataset.csv"
HEALTHY_ALARMS="${BASELINE_DIR}/healthy_alarm_summary.csv"
SIMULATOR_BIN="${ROOT_DIR}/bin/SimAI_simulator"
MONITORING_VALIDATION="${LIMER_DIR}/results/validation_16gpu_v2/final_validation.json"
MONITORING_RUN_DIR="${LIMER_DIR}/results/validation_16gpu_v2/healthy_requested_100us_safe"
PYTHON_BIN="${LIMER_DIR}/.venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then PYTHON_BIN=python3; fi

required=(fault_events_evaluated.csv detection_event_timeline.csv capability_matrix.json)
for name in "${required[@]}"; do
  if [ ! -f "${BASELINE_DIR}/${name}" ]; then
    echo "Missing ${BASELINE_DIR}/${name}; generating the locked current-system baseline first." >&2
    bash "${LIMER_DIR}/scripts/run_current_system_slo_baseline.sh"
    break
  fi
done

mkdir -p "${OUT_DIR}"
SMOKE_ARGS=()
SMOKE_DIR="${OUT_DIR}/true16_smoke"
if [ "${LIMER_RELATED_WORK_RUN_TRUE16_SMOKE:-0}" = "1" ]; then
  mkdir -p "${SMOKE_DIR}/raw_simai" "${SMOKE_DIR}/astra_log"
  cd "${SMOKE_DIR}/raw_simai"
  ulimit -c 0 || true
  smoke_ok=0
  for attempt in 1 2 3; do
    attempt_log="${SMOKE_DIR}/run.attempt-${attempt}.log"
    if ASTRA_SIM_LOG_DIR="${SMOKE_DIR}/astra_log" \
       LIMER_TELEMETRY_ENABLE=1 \
       LIMER_TELEMETRY_INTERVAL_US=1000 \
       LIMER_TELEMETRY_DIR="${SMOKE_DIR}" \
       LIMER_RUN_ID="true-16rank-smoke" \
       AS_SEND_LAT=3 AS_NVLS_ENABLE=1 \
      timeout "${LIMER_RELATED_WORK_SMOKE_TIMEOUT_S:-180}" \
       "${SIMULATOR_BIN}" -t 16 \
         -w "${SMOKE_WORKLOAD}" -n "${TOPOLOGY}" -c "${SIM_CONFIG}" \
         > "${attempt_log}" 2>&1; then
      cp "${attempt_log}" "${SMOKE_DIR}/run.log"
      sha256sum "${SMOKE_WORKLOAD}" "${TOPOLOGY}" "${SIM_CONFIG}" \
        "${SIMULATOR_BIN}" > "${SMOKE_DIR}/input.sha256"
      smoke_ok=1
      break
    fi
    cp "${attempt_log}" "${SMOKE_DIR}/run.log"
  done
  if [ "${smoke_ok}" != "1" ]; then
    echo "True-16-rank smoke did not complete; the report will record it as unverified." >&2
  fi
fi
if [ "${LIMER_RELATED_WORK_RUN_TRUE16_SMOKE:-0}" = "1" ] && [ "${smoke_ok:-0}" = "1" ]; then
  SMOKE_ARGS=(--corrected-smoke-log "${SMOKE_DIR}/run.log")
elif [ "${LIMER_RELATED_WORK_REUSE_TRUE16_SMOKE:-0}" = "1" ] \
     && [ -f "${SMOKE_DIR}/run.log" ] \
     && [ -f "${SMOKE_DIR}/input.sha256" ] \
     && sha256sum --quiet --check "${SMOKE_DIR}/input.sha256"; then
  SMOKE_ARGS=(--corrected-smoke-log "${SMOKE_DIR}/run.log")
fi

cd "${ROOT_DIR}"
PYTHONDONTWRITEBYTECODE=1 "${PYTHON_BIN}" \
  "${LIMER_DIR}/tools/evaluate_related_work_16gpu.py" \
  --baseline-dir "${BASELINE_DIR}" \
  --config "${CONFIG}" \
  --source-workload "${SOURCE_WORKLOAD}" \
  --corrected-workload "${CORRECTED_WORKLOAD}" \
  --dataset-manifest "${DATASET_MANIFEST}" \
  --feature-dataset "${FEATURE_DATASET}" \
  --healthy-alarm-summary "${HEALTHY_ALARMS}" \
  --monitoring-validation "${MONITORING_VALIDATION}" \
  --monitoring-run-dir "${MONITORING_RUN_DIR}" \
  --provenance-input "${LIMER_DIR}/results/gray_fault_qghmm/detector_outputs.csv" \
  --provenance-input "${LIMER_DIR}/results/gray_fault_qghmm/detector_alarms.csv" \
  --provenance-input "${LIMER_DIR}/results/gray_fault_qghmm/model_float.json" \
  --provenance-input "${LIMER_DIR}/results/gray_fault_qghmm/model_quantized.json" \
  --provenance-input "${MONITORING_VALIDATION}" \
  --provenance-input "${MONITORING_RUN_DIR}/link_map.csv" \
  --provenance-input "${MONITORING_RUN_DIR}/switch_telemetry.csv" \
  --provenance-input "${MONITORING_RUN_DIR}/nic_telemetry.csv" \
  --smoke-workload "${SMOKE_WORKLOAD}" \
  --smoke-topology "${TOPOLOGY}" \
  --smoke-sim-config "${SIM_CONFIG}" \
  --simulator-bin "${SIMULATOR_BIN}" \
  "${SMOKE_ARGS[@]}" \
  --out-dir "${OUT_DIR}"

echo "Related-work report: ${OUT_DIR}/related_work_report.md"
