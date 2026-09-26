#!/bin/bash
# Reproduce the healthy 16-GPU full-link telemetry and observer-parity checks.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
OUT_ROOT="${LIMER_DIR}/results/validation_16gpu"
TOPO_DIR="${OUT_ROOT}/topology"
TOPO="${TOPO_DIR}/Spectrum-X_16g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"

mkdir -p "${TOPO_DIR}" "${OUT_ROOT}/astra_log"
if [ ! -f "${TOPO}" ]; then
  cd "${TOPO_DIR}"
  python3 "${ROOT_DIR}/astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py" \
    -topo Spectrum-X -g 16 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps
fi

run_one() {
  local name=$1 enabled=$2 requested_interval_us=$3
  local out_dir="${OUT_ROOT}/${name}"
  mkdir -p "${out_dir}/raw_simai"
  cd "${out_dir}/raw_simai"
  ASTRA_SIM_LOG_DIR="${OUT_ROOT}/astra_log" \
  LIMER_TELEMETRY_ENABLE="${enabled}" \
  LIMER_TELEMETRY_INTERVAL_US="${requested_interval_us}" \
  LIMER_TELEMETRY_DIR="${out_dir}" \
  LIMER_RUN_ID="16gpu-${name}" \
  AS_SEND_LAT=3 AS_NVLS_ENABLE=1 \
    "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
      -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" > "${out_dir}/run.log" 2>&1
}

# A 100 us request exercises the safety clamp; the effective coherent cadence
# is 1 ms, while event-updated queue peaks retain their sub-ms timestamps.
run_one monitoring_on 1 100
run_one monitoring_off 0 1000

ON_TICK=$(grep -oE "all passes finished at time: [0-9]+" \
  "${OUT_ROOT}/monitoring_on/run.log" | grep -oE "[0-9]+")
OFF_TICK=$(grep -oE "all passes finished at time: [0-9]+" \
  "${OUT_ROOT}/monitoring_off/run.log" | grep -oE "[0-9]+")
export LIMER_VALIDATION_ON_TICK="${ON_TICK}"
export LIMER_VALIDATION_OFF_TICK="${OFF_TICK}"
export LIMER_VALIDATION_PARITY="${OUT_ROOT}/monitoring_on_off_parity.json"
python3 - <<'PY'
import json
import os

with open(os.environ["LIMER_VALIDATION_PARITY"], "w") as out:
    json.dump({
        "monitoring_on": {
            "all_passes_finished_at_tick": int(os.environ["LIMER_VALIDATION_ON_TICK"])
        },
        "monitoring_off": {
            "all_passes_finished_at_tick": int(os.environ["LIMER_VALIDATION_OFF_TICK"])
        },
    }, out, indent=2)
    out.write("\n")
PY

python3 "${LIMER_DIR}/tools/validate_telemetry.py" \
  --run-dirs "${OUT_ROOT}/monitoring_on" \
  --parity-json "${OUT_ROOT}/monitoring_on_off_parity.json" \
  --out-json "${OUT_ROOT}/validation_report.json" \
  --out-md "${OUT_ROOT}/validation_report.md"

echo "16-GPU validation complete: on=${ON_TICK} ns, off=${OFF_TICK} ns"
echo "Report: ${OUT_ROOT}/validation_report.md"
