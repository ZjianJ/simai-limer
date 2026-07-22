#!/bin/bash
# Monitoring-overhead sweep: same workload/topology, 4 configs
# (disabled, 1ms, 5ms, 10ms sampling). Measures *simulation instrumentation*
# overhead (wall-clock, output size/rows), not real hardware telemetry
# overhead - see limer/README.md.
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

TOPO="${LIMER_DIR}/results/baseline/topology/Spectrum-X_8g_4gps_100Gbps_A100"
WORKLOAD="${LIMER_DIR}/configs/microAllReduce_10iter.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"

export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}"

OUT_ROOT="${LIMER_DIR}/results"
CSV="${OUT_ROOT}/monitoring_overhead.csv"
echo "config,enabled,interval_us,wall_seconds,exit_code,switch_csv_bytes,nic_csv_bytes,collective_csv_bytes,switch_csv_rows,nic_csv_rows,collective_csv_rows,peak_rss_kb,all_passes_finished_at_tick" > "${CSV}"

run_one () {
  NAME="$1"; ENABLED="$2"; INTERVAL="$3"
  OUT_DIR="${OUT_ROOT}/overhead/${NAME}"
  mkdir -p "${OUT_DIR}"
  export LIMER_TELEMETRY_ENABLE="${ENABLED}"
  export LIMER_TELEMETRY_INTERVAL_US="${INTERVAL}"
  export LIMER_TELEMETRY_DIR="${OUT_DIR}"
  export LIMER_RUN_ID="overhead-${NAME}"

  cd "${OUT_DIR}"
  T0=$(date +%s.%N)
  /usr/bin/time -v -o "${OUT_DIR}/time.log" \
    env AS_SEND_LAT=3 AS_NVLS_ENABLE=1 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
    -w "${WORKLOAD}" -n "${TOPO}" -c "${CONF}" \
    > run.log 2>&1
  EXIT_CODE=$?
  T1=$(date +%s.%N)
  cd - >/dev/null
  WALL=$(python3 -c "print(${T1}-${T0})")

  PEAK_RSS=$(grep -oE "Maximum resident set size \(kbytes\): [0-9]+" "${OUT_DIR}/time.log" 2>/dev/null | grep -oE "[0-9]+" || echo "")
  TICK=$(grep -oE "all passes finished at time: [0-9]+" "${OUT_DIR}/run.log" | grep -oE "[0-9]+" || echo "")

  for f in switch_telemetry nic_telemetry collective_telemetry; do
    path="${OUT_DIR}/${f}.csv"
    if [ -f "${path}" ]; then
      eval "${f}_bytes=$(stat -c%s "${path}")"
      eval "${f}_rows=$(($(wc -l < "${path}") - 1))"
    else
      eval "${f}_bytes=0"
      eval "${f}_rows=0"
    fi
  done

  echo "${NAME},${ENABLED},${INTERVAL},${WALL},${EXIT_CODE},${switch_telemetry_bytes},${nic_telemetry_bytes},${collective_telemetry_bytes},${switch_telemetry_rows},${nic_telemetry_rows},${collective_telemetry_rows},${PEAK_RSS},${TICK}" >> "${CSV}"
  echo "  ${NAME}: exit=${EXIT_CODE} wall=${WALL}s tick=${TICK}"
}

run_one "disabled" 0 1000
run_one "1ms" 1 1000
run_one "5ms" 1 5000
run_one "10ms" 1 10000

echo "Overhead results: ${CSV}"
column -s, -t "${CSV}" || cat "${CSV}"
