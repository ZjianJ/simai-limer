#!/bin/bash
# Build SimAI-Simulation (ns3 backend) with the LIMER env-compat patches
# already applied on this branch. See limer/results/environment.txt and
# limer/docs/code_map.md for why these are needed on this HPC node.
set -euo pipefail
SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_DIR=$(realpath "${SCRIPT_DIR}/../..")

module load gcc-native/12.3
export CC=gcc CXX=g++
export ASTRA_SIM_LOG_DIR="${ASTRA_SIM_LOG_DIR:-$HOME/.astra-sim}"
mkdir -p "${ASTRA_SIM_LOG_DIR}"

cd "${ROOT_DIR}"
./scripts/build.sh -c ns3

echo "Built: ${ROOT_DIR}/bin/SimAI_simulator -> $(readlink -f "${ROOT_DIR}/bin/SimAI_simulator")"
