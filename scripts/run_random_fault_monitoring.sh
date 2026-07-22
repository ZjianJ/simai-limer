#!/bin/bash
# Randomized, dynamic (mid-simulation) fault injection, for generating
# labeled telemetry to test a detection algorithm against later.
#
# Usage: run_random_fault_monitoring.sh [seed] [num_faults]
#
# Unlike run_fault_monitoring.sh (Fault A/B, static from t=0), this uses
# limer::FaultInjector (astra-sim-alibabacloud/astra-sim/network_frontend/
# ns3/limer_telemetry.h) to flip a random ACCESS link's bandwidth down and
# back up again at random, non-overlapping times mid-run. All randomness
# is generated ahead of time by tools/generate_fault_schedule.py (this
# SimAI build has no RNG of its own - see docs/monitoring_design.md), so
# the same seed always reproduces the same fault timeline.
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

SEED="${1:-7}"
NUM_FAULTS="${2:-3}"
RUN_ID="random-fault-seed${SEED}"

TOPO="${LIMER_DIR}/results/baseline/topology/Spectrum-X_8g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
LINK_MAP="${LIMER_DIR}/results/healthy/healthy-seed42/link_map.csv"
OUT_DIR="${LIMER_DIR}/results/random_fault/${RUN_ID}"

export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}" "${OUT_DIR}/raw_simai"

if [ ! -f "${LINK_MAP}" ]; then
  echo "ERROR: ${LINK_MAP} not found - run run_healthy_monitoring.sh at least once first" >&2
  exit 1
fi

# Sim duration estimate: reuse the known healthy-run completion tick for
# this topology/workload (25253814 ns, see results/healthy/*/run.log) as
# the window faults get placed within. If you change the workload, update
# --sim-duration-ns or pass the real tick from a prior run.
SIM_DURATION_NS="${LIMER_SIM_DURATION_NS:-25253814}"

echo "=== generating fault schedule (seed=${SEED}, num_faults=${NUM_FAULTS}) ==="
python3 "${SCRIPT_DIR}/../tools/generate_fault_schedule.py" \
  --link-map "${LINK_MAP}" \
  --sim-duration-ns "${SIM_DURATION_NS}" \
  --num-faults "${NUM_FAULTS}" \
  --seed "${SEED}" \
  --out-csv "${OUT_DIR}/fault_events.csv"

echo "=== ${RUN_ID} ==="
export LIMER_TELEMETRY_ENABLE=1
export LIMER_TELEMETRY_INTERVAL_US=1000
export LIMER_TELEMETRY_DIR="${OUT_DIR}"
export LIMER_RUN_ID="${RUN_ID}"
export LIMER_FAULT_SCHEDULE="${OUT_DIR}/fault_events.csv"

START_WALL=$(date -Iseconds)
cd "${OUT_DIR}/raw_simai"
AS_SEND_LAT=3 AS_NVLS_ENABLE=1 timeout 280 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
  -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" \
  > "${OUT_DIR}/run.log" 2>&1
EXIT_CODE=$?
cd - >/dev/null
END_WALL=$(date -Iseconds)

echo "exit_code=${EXIT_CODE}"
if [ "${EXIT_CODE}" -ne 0 ]; then
  echo "WARNING: ${RUN_ID} did not complete, see ${OUT_DIR}/run.log"
  exit ${EXIT_CODE}
fi

cp "${TOPO}" "${OUT_DIR}/topology_used"
git -C "${ROOT_DIR}" rev-parse HEAD > "${OUT_DIR}/git_commit.txt" 2>/dev/null || true
git -C "${ROOT_DIR}" submodule status > "${OUT_DIR}/submodule_status.txt" 2>/dev/null || true

python3 "${SCRIPT_DIR}/../tools/write_run_manifest.py" \
  --run-id "${RUN_ID}" --out "${OUT_DIR}/run_manifest.json" \
  --root-dir "${ROOT_DIR}" --topology-path "${TOPO}" \
  --topology-name "$(basename "${TOPO}")" \
  --workload-file "${WORKLOAD}" --configuration-file "${CONF}" \
  --telemetry-interval-us "${LIMER_TELEMETRY_INTERVAL_US}" \
  --fault-enabled true --fault-id "random-schedule-seed${SEED}" --random-seed "${SEED}" \
  --start-wall-time "${START_WALL}" --end-wall-time "${END_WALL}" \
  --exit-code "${EXIT_CODE}"

echo "  output=${OUT_DIR}"
grep "LIMER-FAULT" "${OUT_DIR}/run.log" || true
