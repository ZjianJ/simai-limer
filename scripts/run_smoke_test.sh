#!/bin/bash
# One-command smoke test: 8-GPU topology + the stock 2-collective
# microAllReduce.txt example, monitoring enabled at the default 1ms
# interval. Confirms the instrumented binary still runs to completion.
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

TOPO="${LIMER_DIR}/results/baseline/topology/Spectrum-X_8g_4gps_100Gbps_A100"
if [ ! -f "${TOPO}" ]; then
  mkdir -p "$(dirname "${TOPO}")"
  cd "$(dirname "${TOPO}")"
  python3 "${ROOT_DIR}/astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py" -topo Spectrum-X -g 8 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps
  cd - >/dev/null
fi

OUT_DIR="${LIMER_DIR}/results/smoke_test"
mkdir -p "${OUT_DIR}"

export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}"
export LIMER_TELEMETRY_ENABLE=1
export LIMER_TELEMETRY_INTERVAL_US=1000
export LIMER_TELEMETRY_DIR="${OUT_DIR}"
export LIMER_RUN_ID="smoke-test"

cd "${OUT_DIR}"
AS_SEND_LAT=3 AS_NVLS_ENABLE=1 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
  -w "${ROOT_DIR}/example/microAllReduce.txt" \
  -n "${TOPO}" \
  -c "${LIMER_DIR}/configs/SimAI.baseline.conf" \
  > run.log 2>&1
EXIT_CODE=$?
echo "Smoke test exit code: ${EXIT_CODE}"
echo "Output: ${OUT_DIR}"
ls -la "${OUT_DIR}"
exit ${EXIT_CODE}
