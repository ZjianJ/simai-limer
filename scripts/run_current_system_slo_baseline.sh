#!/bin/bash
# Analyze the locked 16-GPU runs without changing detectors or recovery logic.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
SOURCE_ROOT="${LIMER_CURRENT_BASELINE_SOURCE:-${LIMER_DIR}/results/gray_fault_qghmm}"
OUT_DIR="${LIMER_CURRENT_BASELINE_OUT:-${LIMER_DIR}/results/current_system_slo_baseline}"
PYTHON_BIN="${LIMER_DIR}/.venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then PYTHON_BIN=python3; fi

required=(dataset_manifest.json gray_fault_dataset.parquet detector_alarms.csv)
for name in "${required[@]}"; do
  if [ ! -f "${SOURCE_ROOT}/${name}" ]; then
    echo "Missing ${SOURCE_ROOT}/${name}; first run:" >&2
    echo "  bash limer/scripts/run_gray_fault_qghmm_experiment.sh" >&2
    exit 2
  fi
done

PYTHONDONTWRITEBYTECODE=1 "${PYTHON_BIN}" \
  "${LIMER_DIR}/tools/evaluate_current_system_slo_baseline.py" \
  --manifest "${SOURCE_ROOT}/dataset_manifest.json" \
  --dataset "${SOURCE_ROOT}/gray_fault_dataset.parquet" \
  --qghmm-alarms "${SOURCE_ROOT}/detector_alarms.csv" \
  --runs-root "${SOURCE_ROOT}/runs" \
  --out-dir "${OUT_DIR}"

echo "Current-system baseline: ${OUT_DIR}/baseline_report.md"
