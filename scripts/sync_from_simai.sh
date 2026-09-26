#!/bin/bash
# Pull the latest configs/docs/scripts/tools and regenerate patches/ from a
# SimAI checkout on the limer-monitoring branch. Run this, review `git diff`,
# then commit + push.
set -euo pipefail
SIMAI_ROOT="${1:-${SIMAI_ROOT:-/home/u6ow/zijian.u6ow/limer-simai/SimAI}}"
REPO_DIR=$(cd "$(dirname "$(realpath "$0")")/.." && pwd)

# Pre-LIMER anchor commits: fixed historical points each patch is diffed from.
ASTRA_BASE=f5efb5a
NS3_BASE=7e3cb5b

echo "Syncing configs/docs/scripts/tools from ${SIMAI_ROOT}/limer ..."
rsync -a "${SIMAI_ROOT}/limer/configs/" "${REPO_DIR}/configs/"
rsync -a "${SIMAI_ROOT}/limer/docs/" "${REPO_DIR}/docs/"
rsync -a --exclude sync_from_simai.sh "${SIMAI_ROOT}/limer/scripts/" "${REPO_DIR}/scripts/"
rsync -a --exclude export_simai_patches.py "${SIMAI_ROOT}/limer/tools/" "${REPO_DIR}/tools/"
rsync -a "${SIMAI_ROOT}/limer/tests/" "${REPO_DIR}/tests/"

echo "Regenerating patches against anchor commits (astra: ${ASTRA_BASE}, ns3: ${NS3_BASE}) ..."
python3 "${REPO_DIR}/tools/export_simai_patches.py" \
  --simai-root "${SIMAI_ROOT}" --output "${REPO_DIR}/patches"

echo "Done. Review with: git -C ${REPO_DIR} status / git -C ${REPO_DIR} diff"
echo "Then: git -C ${REPO_DIR} add -A && git -C ${REPO_DIR} commit -m '...' && git -C ${REPO_DIR} push"
