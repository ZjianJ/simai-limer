#!/bin/bash
# 8-GPU microAllReduce_20iter workload, monitoring on, one run per seed in
# {42, 123, 2026}. See limer/docs/monitoring_design.md for why repeated
# runs are expected to be bit-for-bit deterministic on this SimAI build
# (no exposed RNG source at the ns3-frontend layer) - seeds are still
# tagged per-run for schema compliance and forward-compatibility.
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

TOPO="${LIMER_DIR}/results/baseline/topology/Spectrum-X_8g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"

export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}"

for SEED in 42 123 2026; do
  RUN_ID="healthy-seed${SEED}"
  OUT_DIR="${LIMER_DIR}/results/healthy/${RUN_ID}"
  mkdir -p "${OUT_DIR}/raw_simai"
  echo "=== ${RUN_ID} ==="

  export LIMER_TELEMETRY_ENABLE=1
  export LIMER_TELEMETRY_INTERVAL_US=1000
  export LIMER_TELEMETRY_DIR="${OUT_DIR}"
  export LIMER_RUN_ID="${RUN_ID}"

  START_WALL=$(date -Iseconds)
  cd "${OUT_DIR}/raw_simai"
  AS_SEND_LAT=3 AS_NVLS_ENABLE=1 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
    -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" \
    > "${OUT_DIR}/run.log" 2>&1
  EXIT_CODE=$?
  END_WALL=$(date -Iseconds)
  cd - >/dev/null

  cp "${TOPO}" "${OUT_DIR}/topology_used"
  git -C "${ROOT_DIR}" rev-parse HEAD > "${OUT_DIR}/git_commit.txt" 2>/dev/null || true
  git -C "${ROOT_DIR}" submodule status > "${OUT_DIR}/submodule_status.txt" 2>/dev/null || true

  python3 "${SCRIPT_DIR}/../tools/write_run_manifest.py" \
    --run-id "${RUN_ID}" \
    --out "${OUT_DIR}/run_manifest.json" \
    --root-dir "${ROOT_DIR}" \
    --topology-name "$(basename "${TOPO}")" \
    --workload-file "${WORKLOAD}" \
    --configuration-file "${CONF}" \
    --telemetry-interval-us "${LIMER_TELEMETRY_INTERVAL_US}" \
    --fault-enabled false \
    --random-seed "${SEED}" \
    --start-wall-time "${START_WALL}" \
    --end-wall-time "${END_WALL}" \
    --exit-code "${EXIT_CODE}"

  echo "  exit_code=${EXIT_CODE}, output=${OUT_DIR}"
  if [ "${EXIT_CODE}" -ne 0 ]; then
    echo "  WARNING: ${RUN_ID} exited non-zero, see ${OUT_DIR}/run.log"
  fi
done
