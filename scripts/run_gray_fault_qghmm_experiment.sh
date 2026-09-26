#!/bin/bash
# Generate and run the resumable 16-GPU QG-HMM benchmark.
set -euo pipefail
# Upstream SimAI can abort on unsupported workload shapes. Preserve run.log
# but never emit multi-gigabyte core files into the experiment quota.
ulimit -c 0

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")
LIMER_DIR="${ROOT_DIR}/limer"
OUT_ROOT="${LIMER_GRAY_OUT_ROOT:-${LIMER_DIR}/results/gray_fault_qghmm}"
MANIFEST="${LIMER_GRAY_MANIFEST:-${OUT_ROOT}/dataset_manifest.json}"
TOPO_DIR="${OUT_ROOT}/topology"
TOPO="${TOPO_DIR}/Spectrum-X_16g_4gps_100Gbps_A100"
CONF="${LIMER_DIR}/configs/SimAI.baseline.conf"
PYTHON_BIN="${LIMER_DIR}/.venv/bin/python"
MAX_RUNS=0
SHARD_INDEX=0
SHARD_COUNT=1
SCENARIO=""
PILOT=0
GENERATE_ONLY=0
ANALYSIS_ONLY=0
SKIP_ANALYSIS=0

usage() {
  echo "Usage: $0 [--pilot] [--generate-only|--analysis-only|--skip-analysis] [--max-runs N] [--scenario NAME] [--shard-index I --shard-count N]"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --pilot) PILOT=1; shift ;;
    --generate-only) GENERATE_ONLY=1; shift ;;
    --analysis-only) ANALYSIS_ONLY=1; shift ;;
    --skip-analysis) SKIP_ANALYSIS=1; shift ;;
    --max-runs) MAX_RUNS=$2; shift 2 ;;
    --scenario) SCENARIO=$2; shift 2 ;;
    --shard-index) SHARD_INDEX=$2; shift 2 ;;
    --shard-count) SHARD_COUNT=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN=python3
fi
mkdir -p "${OUT_ROOT}" "${TOPO_DIR}" "${OUT_ROOT}/runs" "${OUT_ROOT}/astra_log"
if [ ! -f "${TOPO}" ]; then
  cd "${TOPO_DIR}"
  python3 "${ROOT_DIR}/astra-sim-alibabacloud/inputs/topo/gen_Topo_Template.py" \
    -topo Spectrum-X -g 16 -gps 4 -gt A100 -bw 100Gbps -nvbw 2400Gbps
fi

if [ ! -f "${MANIFEST}" ]; then
  generator_args=()
  if [ "${PILOT}" -eq 1 ]; then generator_args+=(--pilot); fi
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/generate_gray_fault_benchmark.py" \
    --link-map "${LIMER_DIR}/results/detection_baselines_16gpu/healthy/link_map.csv" \
    --workload-template "${LIMER_DIR}/configs/microAllReduce_10iter.txt" \
    --out-dir "${OUT_ROOT}" --seed 2026 "${generator_args[@]}"
fi
if [ "${GENERATE_ONLY}" -eq 1 ]; then
  echo "Manifest generated: ${MANIFEST}"
  exit 0
fi

mapfile -t RUN_ROWS < <("${PYTHON_BIN}" - "${MANIFEST}" "${SCENARIO}" \
  "${SHARD_INDEX}" "${SHARD_COUNT}" "${MAX_RUNS}" <<'PY'
import hashlib, json, os, sys
manifest_path, scenario = sys.argv[1], sys.argv[2]
shard_index, shard_count, max_runs = map(int, sys.argv[3:])
manifest = json.load(open(manifest_path))
base = os.path.dirname(os.path.abspath(manifest_path))
selected = []
for index, run in enumerate(manifest["runs"]):
    if scenario and run["scenario"] != scenario:
        continue
    if index % shard_count != shard_index:
        continue
    selected.append(run)
if max_runs > 0:
    selected = selected[:max_runs]
for run in selected:
    for field, hash_field in [("workload_path", "workload_sha256"),
                              ("fault_events_path", "fault_events_sha256")]:
        path = os.path.join(base, run[field])
        if hash_field in run:
            actual = hashlib.sha256(open(path, "rb").read()).hexdigest()
            if actual != run[hash_field]:
                raise SystemExit(f"immutable spec hash mismatch: {path}")
    print("\t".join([
        run["run_id"],
        os.path.join(base, run["workload_path"]),
        os.path.join(base, run["fault_events_path"]),
    ]))
PY
)

completed=0
failed=0
telemetry_complete() {
  local run_dir=$1
  [ -s "${run_dir}/switch_telemetry.csv" ] &&
    [ -s "${run_dir}/nic_telemetry.csv" ] &&
    [ -s "${run_dir}/collective_telemetry.csv" ] &&
    [ -s "${run_dir}/link_map.csv" ] &&
    LC_ALL=C grep -q '^run_id,timestamp_ns,switch_id,' "${run_dir}/switch_telemetry.csv" &&
    LC_ALL=C grep -q '^run_id,timestamp_ns,node_id,' "${run_dir}/nic_telemetry.csv" &&
    LC_ALL=C grep -q '^run_id,collective_id,iteration_id,' "${run_dir}/collective_telemetry.csv" &&
    LC_ALL=C grep -q '^link_id,src_node,dst_node,' "${run_dir}/link_map.csv"
}
if [ "${ANALYSIS_ONLY}" -eq 0 ]; then
for row in "${RUN_ROWS[@]}"; do
  IFS=$'\t' read -r run_id workload schedule <<<"${row}"
  run_dir="${OUT_ROOT}/runs/${run_id}"
  if telemetry_complete "${run_dir}" && \
     grep -q "Percentage of finished streams: 100" "${run_dir}/run.log" 2>/dev/null; then
    echo "SKIP complete ${run_id}"
    completed=$((completed + 1))
    continue
  fi
  mkdir -p "${run_dir}/raw_simai" "${run_dir}/astra_log"
  run_conf="${run_dir}/SimAI.conf"
  "${PYTHON_BIN}" - "${CONF}" "${run_conf}" "${run_dir}/raw_simai" <<'PY'
import os, sys
source, destination, raw_dir = sys.argv[1:]
path_keys = {
    "FLOW_FILE", "TRACE_FILE", "TRACE_OUTPUT_FILE", "FCT_OUTPUT_FILE",
    "PFC_OUTPUT_FILE", "QLEN_MON_FILE", "BW_MON_FILE", "RATE_MON_FILE",
    "CNP_MON_FILE",
}
lines = []
for line in open(source):
    fields = line.split(maxsplit=1)
    if fields and fields[0] in path_keys:
        suffix = os.path.basename(fields[1].strip()) if len(fields) > 1 else fields[0].lower()
        line = f"{fields[0]} {os.path.join(raw_dir, suffix)}\n"
    lines.append(line)
with open(destination, "w") as out:
    out.writelines(lines)
PY
  schedule_rows=$(($(wc -l < "${schedule}") - 1))
  env_args=(
    "ASTRA_SIM_LOG_DIR=${run_dir}/astra_log"
    "LIMER_TELEMETRY_ENABLE=1"
    "LIMER_TELEMETRY_INTERVAL_US=1000"
    "LIMER_TELEMETRY_DIR=${run_dir}"
    "LIMER_RUN_ID=${run_id}"
    "AS_SEND_LAT=3"
    "AS_NVLS_ENABLE=1"
  )
  if [ "${schedule_rows}" -gt 0 ]; then
    env_args+=("LIMER_FAULT_SCHEDULE=${schedule}")
  fi
  echo "RUN ${run_id}"
  cd "${run_dir}/raw_simai"
  if env "${env_args[@]}" timeout 240 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
      -w "${workload}" -n "${TOPO}" -c "${run_conf}" > "${run_dir}/run.log" 2>&1; then
    if grep -q "Percentage of finished streams: 100" "${run_dir}/run.log"; then
      completed=$((completed + 1))
    else
      echo "INCOMPLETE ${run_id}: simulator exited but streams did not reach 100%" >&2
      failed=$((failed + 1))
    fi
  else
    echo "FAILED ${run_id}; see ${run_dir}/run.log" >&2
    failed=$((failed + 1))
  fi
done
fi

echo "Gray benchmark shard complete: completed=${completed}, failed=${failed}, selected=${#RUN_ROWS[@]}"
if [ "${failed}" -ne 0 ]; then exit 1; fi

if [ "${SKIP_ANALYSIS}" -eq 0 ] && \
   { [ "${ANALYSIS_ONLY}" -eq 1 ] || { [ "${MAX_RUNS}" -eq 0 ] && [ "${SHARD_COUNT}" -eq 1 ] && [ -z "${SCENARIO}" ]; }; }; then
  PARITY_JSON="${OUT_ROOT}/monitoring_parity_under_fault.json"
  if [ ! -f "${PARITY_JSON}" ]; then
    IFS=$'\t' read -r parity_run parity_workload parity_schedule < <(
      "${PYTHON_BIN}" - "${MANIFEST}" <<'PY'
import json, os, sys
path = os.path.abspath(sys.argv[1])
manifest = json.load(open(path))
base = os.path.dirname(path)
run = next(item for item in manifest["runs"]
           if item["scenario"] == "ACCESS_FAIL_SLOW" and item["split"] == "test")
print("\t".join([run["run_id"], os.path.join(base, run["workload_path"]),
                 os.path.join(base, run["fault_events_path"])]))
PY
    )
    parity_on_dir="${OUT_ROOT}/runs/${parity_run}"
    parity_off_dir="${OUT_ROOT}/parity_fault_monitoring_off"
    mkdir -p "${parity_off_dir}/raw_simai" "${parity_off_dir}/astra_log"
    parity_conf="${parity_off_dir}/SimAI.conf"
    "${PYTHON_BIN}" - "${CONF}" "${parity_conf}" "${parity_off_dir}/raw_simai" <<'PY'
import os, sys
source, destination, raw_dir = sys.argv[1:]
keys = {"FLOW_FILE", "TRACE_FILE", "TRACE_OUTPUT_FILE", "FCT_OUTPUT_FILE",
        "PFC_OUTPUT_FILE", "QLEN_MON_FILE", "BW_MON_FILE", "RATE_MON_FILE",
        "CNP_MON_FILE"}
lines = []
for line in open(source):
    fields = line.split(maxsplit=1)
    if fields and fields[0] in keys:
        line = f"{fields[0]} {os.path.join(raw_dir, os.path.basename(fields[1].strip()))}\n"
    lines.append(line)
with open(destination, "w") as out:
    out.writelines(lines)
PY
    echo "PARITY monitoring-off replay ${parity_run}"
    cd "${parity_off_dir}/raw_simai"
    ASTRA_SIM_LOG_DIR="${parity_off_dir}/astra_log" \
    LIMER_TELEMETRY_ENABLE=0 LIMER_FAULT_SCHEDULE="${parity_schedule}" \
    AS_SEND_LAT=3 AS_NVLS_ENABLE=1 timeout 240 "${ROOT_DIR}/bin/SimAI_simulator" -t 16 \
      -w "${parity_workload}" -n "${TOPO}" -c "${parity_conf}" \
      > "${parity_off_dir}/run.log" 2>&1
    on_tick=$(grep -oE "all passes finished at time: [0-9]+" "${parity_on_dir}/run.log" | tail -n1 | grep -oE "[0-9]+")
    off_tick=$(grep -oE "all passes finished at time: [0-9]+" "${parity_off_dir}/run.log" | tail -n1 | grep -oE "[0-9]+")
    "${PYTHON_BIN}" - "${PARITY_JSON}" "${parity_run}" "${on_tick}" "${off_tick}" <<'PY'
import json, sys
path, run_id, on_tick, off_tick = sys.argv[1:]
with open(path, "w") as out:
    json.dump({"run_id": run_id, "monitoring_on_tick_ns": int(on_tick),
               "monitoring_off_tick_ns": int(off_tick),
               "exact_match": int(on_tick) == int(off_tick)}, out, indent=2)
    out.write("\n")
PY
  fi
  echo "BUILD causal dataset"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/build_gray_fault_dataset.py" \
    --manifest "${MANIFEST}" --runs-root "${OUT_ROOT}/runs" --out-dir "${OUT_ROOT}"
  echo "TRAIN M1/M2/M3/M4"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/train_qg_hmm.py" \
    --dataset "${OUT_ROOT}/gray_fault_dataset.parquet" \
    --dataset-checks "${OUT_ROOT}/dataset_checks.json" \
    --out-model "${OUT_ROOT}/model_float.json"
  echo "QUANTIZE selected QG-HMM"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/quantize_qg_hmm.py" \
    --float-model "${OUT_ROOT}/model_float.json" \
    --dataset "${OUT_ROOT}/gray_fault_dataset.parquet" \
    --out-model "${OUT_ROOT}/model_quantized.json" \
    --out-calibration "${OUT_ROOT}/calibration_lut.json"
  echo "REPLAY float and quantized models"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/replay_qg_hmm.py" \
    --dataset "${OUT_ROOT}/gray_fault_dataset.parquet" \
    --float-model "${OUT_ROOT}/model_float.json" \
    --quantized-model "${OUT_ROOT}/model_quantized.json" \
    --out-outputs "${OUT_ROOT}/detector_outputs.csv" \
    --out-alarms "${OUT_ROOT}/detector_alarms.csv" \
    --out-jsonl "${OUT_ROOT}/detector_alarms.jsonl"
  echo "RUN locked-split ablations"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/evaluate_qghmm_ablations.py" \
    --dataset "${OUT_ROOT}/gray_fault_dataset.parquet" \
    --float-model "${OUT_ROOT}/model_float.json" \
    --outputs "${OUT_ROOT}/detector_outputs.csv" \
    --out-json "${OUT_ROOT}/ablation_metrics.json"
  echo "EVALUATE and compare M0 switch_sparse"
  "${PYTHON_BIN}" "${LIMER_DIR}/tools/evaluate_qg_hmm.py" \
    --dataset "${OUT_ROOT}/gray_fault_dataset.parquet" \
    --outputs "${OUT_ROOT}/detector_outputs.csv" \
    --alarms "${OUT_ROOT}/detector_alarms.csv" \
    --float-model "${OUT_ROOT}/model_float.json" \
    --quantized-model "${OUT_ROOT}/model_quantized.json" \
    --manifest "${MANIFEST}" --runs-root "${OUT_ROOT}/runs" \
    --parity-json "${PARITY_JSON}" \
    --ablations "${OUT_ROOT}/ablation_metrics.json" \
    --out-dir "${OUT_ROOT}"
  echo "Final report: ${OUT_ROOT}/final_report.md"
fi
