#!/bin/bash
# P1: reproduce one permanent hard disconnect on each active Plane-B ACCESS
# link, sharing only a strictly validated monitoring-on/off healthy pair.
set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"

OUT_ROOT=${LIMER_TRUE16_MATRIX_OUT_ROOT:-"${LIMER_DIR}/results/true16_hard_fault_matrix"}
TIMEOUT_S=${SIMAI_TRUE16_MATRIX_TIMEOUT_S:-900}
FAULT_START_NS=${LIMER_TRUE16_MATRIX_FAULT_START_NS:-10000}
GPU_SPEC=${LIMER_TRUE16_MATRIX_GPUS:-all}
AGGREGATE_ONLY=0

usage() {
  echo "Usage: $0 [--out-root PATH] [--gpus all|0-15|0,3,7] [--aggregate-only]"
  echo
  echo "Completed GPU evaluations are content-validated and reused."
  echo "Invalid final evidence is never overwritten; select a new --out-root."
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --out-root)
      [ "$#" -ge 2 ] || { echo "--out-root requires a value" >&2; exit 2; }
      OUT_ROOT=$2
      shift 2
      ;;
    --gpus)
      [ "$#" -ge 2 ] || { echo "--gpus requires a value" >&2; exit 2; }
      GPU_SPEC=$2
      shift 2
      ;;
    --aggregate-only)
      AGGREGATE_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "${TIMEOUT_S}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SIMAI_TRUE16_MATRIX_TIMEOUT_S must be a positive integer" >&2
  exit 2
fi
if ! [[ "${FAULT_START_NS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "LIMER_TRUE16_MATRIX_FAULT_START_NS must be a positive integer" >&2
  exit 2
fi

mkdir -p "${OUT_ROOT}"
OUT_ROOT=$(realpath "${OUT_ROOT}")
TOPO_DIR="${OUT_ROOT}/topology"
TOPO_NAME="Spectrum-X_16g_4gps_DualToR_DualPlane_100Gbps_A100"
TOPO="${TOPO_DIR}/${TOPO_NAME}"
HEALTHY_WORKLOAD="${LIMER_DIR}/configs/microAllReduce_16rank_p1_healthy_120ms.txt"
FAULT_WORKLOAD="${LIMER_DIR}/configs/microAllReduce_16rank_smoke.txt"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
CONTRACT="${LIMER_DIR}/configs/experiment_contract.yaml"
BINARY="${ROOT_DIR}/bin/SimAI_simulator"
TOPOLOGY_VALIDATION="${OUT_ROOT}/topology_validation.json"
MATRIX_EVALUATOR="${LIMER_DIR}/tools/evaluate_true16_hard_fault_matrix.py"

if [ ! -x "${BINARY}" ]; then
  echo "SimAI binary is missing or not executable: ${BINARY}" >&2
  echo "Run: bash ${LIMER_DIR}/scripts/build_simai.sh" >&2
  exit 2
fi

mapfile -t GPUS < <(python3 - "${GPU_SPEC}" <<'PY'
import sys

raw = sys.argv[1].strip().lower()
if raw in {"all", "0-15"}:
    values = list(range(16))
else:
    values = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            raise SystemExit("empty GPU token")
        if "-" in token:
            left, right = token.split("-", 1)
            values.extend(range(int(left), int(right) + 1))
        else:
            values.append(int(token))
if not values or any(value < 0 or value >= 16 for value in values):
    raise SystemExit(f"GPU specification must select values in 0..15: {raw}")
if len(values) != len(set(values)):
    raise SystemExit(f"GPU specification contains duplicates: {raw}")
for value in sorted(values):
    print(value)
PY
)
if [ "${#GPUS[@]}" -eq 0 ]; then
  echo "No valid GPUs selected by --gpus ${GPU_SPEC}" >&2
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
  --out-json "${TOPOLOGY_VALIDATION}"

common_matrix_args=(
  --matrix-root "${OUT_ROOT}"
  --topology "${TOPO}"
  --contract "${CONTRACT}"
  --healthy-workload "${HEALTHY_WORKLOAD}"
  --fault-workload "${FAULT_WORKLOAD}"
  --sim-config "${CONF}"
  --simulator-binary "${BINARY}"
  --expected-gpus 16
  --primary-host-port 3
  --sample-interval-ns 1000000
  --minimum-healthy-span-ns 100000000
  --hard-detection-slo-ns 1000000
  --recovery-slo-ns 1000000000
)

run_simai() {
  local run_id=$1
  local telemetry_enabled=$2
  local run_dir=$3
  local workload=$4
  local schedule=${5:-}
  local raw_dir="${run_dir}/raw_simai"
  local log_dir="${run_dir}/astra_log"
  local rc
  local -a environment=(
    "ASTRA_SIM_LOG_DIR=${log_dir}"
    "LIMER_TELEMETRY_ENABLE=${telemetry_enabled}"
    "LIMER_TELEMETRY_INTERVAL_US=1000"
    "LIMER_TELEMETRY_DIR=${run_dir}"
    "LIMER_RUN_ID=${run_id}"
    "LIMER_ALARM_PROCESSING_US=5"
    "LIMER_ALARM_TRANSPORT_US=50"
    "LIMER_ALARM_CONSUME_US=5"
    "LIMER_RDMA_RTO_US=100"
    "LIMER_RDMA_RETRY_LIMIT=7"
    "LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE=1"
    "AS_SEND_LAT=3"
    "AS_NVLS_ENABLE=1"
    "AS_PXN_ENABLE=0"
    "AS_LOG_LEVEL=1"
  )
  if [ -n "${schedule}" ]; then
    environment+=("LIMER_FAULT_SCHEDULE=${schedule}")
  fi
  mkdir -p "${raw_dir}" "${log_dir}"
  set +e
  (
    cd "${raw_dir}"
    ulimit -c 0
    timeout "${TIMEOUT_S}s" env "${environment[@]}" \
      "${BINARY}" -t 16 -w "${workload}" -n "${TOPO}" -c "${CONF}"
  ) > "${run_dir}/run.log" 2>&1
  rc=$?
  set -e
  printf '%s\n' "${rc}" > "${run_dir}/exit_code.txt"
  if [ "${rc}" -ne 0 ]; then
    echo "${run_id} failed with exit code ${rc}; see ${run_dir}/run.log" >&2
    return "${rc}"
  fi
}

validate_healthy() {
  local healthy_root=$1
  local out_json=$2
  local out_md=$3
  python3 "${MATRIX_EVALUATOR}" "${common_matrix_args[@]}" \
    --healthy-long-on-dir "${healthy_root}/long/monitoring_on" \
    --healthy-long-off-dir "${healthy_root}/long/monitoring_off" \
    --healthy-only --out-json "${out_json}" --out-md "${out_md}"
}

if [ "${AGGREGATE_ONLY}" -eq 0 ]; then
  HEALTHY_ROOT="${OUT_ROOT}/healthy"
  if [ -d "${HEALTHY_ROOT}" ]; then
    if ! validate_healthy \
      "${HEALTHY_ROOT}" \
      "${OUT_ROOT}/healthy_validation.json" \
      "${OUT_ROOT}/healthy_validation.md"; then
      echo "Existing healthy evidence failed validation and will not be reused." >&2
      echo "Use a new --out-root; this runner never overwrites invalid final evidence." >&2
      exit 1
    fi
    echo "REUSE validated healthy monitoring-on/off pair"
  else
    HEALTHY_PENDING="${OUT_ROOT}/healthy.pending.$$"
    if [ -e "${HEALTHY_PENDING}" ]; then
      echo "Pending healthy path already exists: ${HEALTHY_PENDING}" >&2
      exit 1
    fi
    mkdir -p "${HEALTHY_PENDING}"
    echo "RUN healthy monitoring-on"
    run_simai \
      "true16-matrix-healthy-on" 1 \
      "${HEALTHY_PENDING}/long/monitoring_on" "${HEALTHY_WORKLOAD}"
    echo "RUN healthy monitoring-off"
    run_simai \
      "true16-matrix-healthy-off" 0 \
      "${HEALTHY_PENDING}/long/monitoring_off" "${HEALTHY_WORKLOAD}"
    if ! validate_healthy \
      "${HEALTHY_PENDING}" \
      "${HEALTHY_PENDING}/validation.json" \
      "${HEALTHY_PENDING}/validation.md"; then
      echo "New healthy pair failed validation; retained at ${HEALTHY_PENDING}" >&2
      exit 1
    fi
    mv "${HEALTHY_PENDING}" "${HEALTHY_ROOT}"
    validate_healthy \
      "${HEALTHY_ROOT}" \
      "${OUT_ROOT}/healthy_validation.json" \
      "${OUT_ROOT}/healthy_validation.md"
  fi

  for gpu in "${GPUS[@]}"; do
    printf -v gpu_name 'gpu_%02d' "${gpu}"
    gpu_dir="${OUT_ROOT}/${gpu_name}"
    fault_dir="${gpu_dir}/hard_disconnect"
    mkdir -p "${gpu_dir}"

    if [ -d "${fault_dir}" ]; then
      if python3 "${MATRIX_EVALUATOR}" "${common_matrix_args[@]}" \
        --check-gpu "${gpu}" \
        --out-json "${gpu_dir}/resume_check.json" \
        --out-md "${gpu_dir}/resume_check.md"; then
        echo "REUSE validated ${gpu_name}"
        continue
      fi
      echo "Existing ${gpu_name} fast-event evidence is invalid and will not be overwritten." >&2
      echo "Use a new --out-root or preserve and move the invalid directory aside." >&2
      exit 1
    fi

    python3 "${LIMER_DIR}/tools/prepare_true16_hard_fault.py" \
      --link-map "${HEALTHY_ROOT}/long/monitoring_on/link_map.csv" \
      --healthy-nic "${HEALTHY_ROOT}/long/monitoring_on/nic_telemetry.csv" \
      --gpu "${gpu}" \
      --host-port 3 \
      --start-ns "${FAULT_START_NS}" \
      --out-csv "${gpu_dir}/fault_events.csv" \
      --out-json "${gpu_dir}/fault_selection.json"

    if [ ! -d "${fault_dir}" ]; then
      fault_pending="${gpu_dir}/hard_disconnect.pending.$$"
      if [ -e "${fault_pending}" ]; then
        echo "Pending fault path already exists: ${fault_pending}" >&2
        exit 1
      fi
      echo "RUN ${gpu_name} permanent Plane-B hard disconnect"
      run_simai \
        "true16-matrix-${gpu_name}-hard" 1 "${fault_pending}" \
        "${FAULT_WORKLOAD}" "${gpu_dir}/fault_events.csv"
      mv "${fault_pending}" "${fault_dir}"
    fi

    python3 "${MATRIX_EVALUATOR}" "${common_matrix_args[@]}" \
      --check-gpu "${gpu}" \
      --out-json "${gpu_dir}/resume_check.json" \
      --out-md "${gpu_dir}/resume_check.md"
  done
fi

# Deliberately returns non-zero while any of the 16 required targets is
# missing.  Partial/sharded invocations still leave a complete FAIL matrix for
# the next resumable invocation to consume.
python3 "${MATRIX_EVALUATOR}" "${common_matrix_args[@]}" \
  --out-json "${OUT_ROOT}/stage_matrix.json" \
  --out-md "${OUT_ROOT}/stage_matrix.md"
