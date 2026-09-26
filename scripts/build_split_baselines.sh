#!/usr/bin/env bash
# Stage the canonical frontend and build with bounded memory/CPU consumption.
set -euo pipefail
script_dir=$(dirname "$(realpath "$0")")
simai_dir=$(realpath "$script_dir/../..")
ns3_dir="$simai_dir/ns-3-alibabacloud/simulation"
astra_dir="$simai_dir/astra-sim-alibabacloud/astra-sim"
cp "$astra_dir/network_frontend/ns3/"*.h "$ns3_dir/scratch/"
cp "$astra_dir/network_frontend/ns3/AstraSimNetwork.cc" "$ns3_dir/scratch/"
mkdir -p "$ns3_dir/src/applications/astra-sim"
cp -a "$astra_dir/." "$ns3_dir/src/applications/astra-sim/"
cd "$ns3_dir"
./ns3 configure -d debug --enable-mtp --enable-modules='applications;csma;point-to-point;mtp'
cmake --build cmake-cache --target scratch_AstraSimNetwork --parallel "${SIMAI_BUILD_JOBS:-2}"
