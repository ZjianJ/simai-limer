#!/bin/bash
# Pull the latest configs/docs/scripts/tools and regenerate patches/ from a
# SimAI checkout on the limer-monitoring branch. Run this, review `git diff`,
# then commit + push.
set -euo pipefail
SIMAI_ROOT="${1:-${SIMAI_ROOT:-/home/u6ow/zijian.u6ow/limer-simai/SimAI}}"
REPO_DIR=$(cd "$(dirname "$(realpath "$0")")/.." && pwd)

# Pre-LIMER anchor commits: fixed historical points each patch is diffed from.
ASTRA_BASE=f5efb5a
NS3_BASE=1484b1a

echo "Syncing configs/docs/scripts/tools from ${SIMAI_ROOT}/limer ..."
rsync -a --delete "${SIMAI_ROOT}/limer/configs/" "${REPO_DIR}/configs/"
rsync -a --delete "${SIMAI_ROOT}/limer/docs/" "${REPO_DIR}/docs/"
rsync -a --delete --exclude sync_from_simai.sh "${SIMAI_ROOT}/limer/scripts/" "${REPO_DIR}/scripts/"
rsync -a --delete "${SIMAI_ROOT}/limer/tools/" "${REPO_DIR}/tools/"

echo "Regenerating patches against anchor commits (astra: ${ASTRA_BASE}, ns3: ${NS3_BASE}) ..."
git -C "${SIMAI_ROOT}" diff "${ASTRA_BASE}" HEAD -- astra-sim-alibabacloud > "${REPO_DIR}/patches/astra-sim-alibabacloud.patch"
git -C "${SIMAI_ROOT}/ns-3-alibabacloud" diff "${NS3_BASE}" HEAD -- . > "${REPO_DIR}/patches/ns-3-alibabacloud.patch"

echo "Done. Review with: git -C ${REPO_DIR} status / git -C ${REPO_DIR} diff"
echo "Then: git -C ${REPO_DIR} add -A && git -C ${REPO_DIR} commit -m '...' && git -C ${REPO_DIR} push"
