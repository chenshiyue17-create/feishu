#!/usr/bin/env bash
set -euo pipefail

REPO_PARENT="${REPO_PARENT:-/opt}"
REPO_NAME="${REPO_NAME:-xhs_feishu_monitor}"
REPO_DIR="${REPO_PARENT}/${REPO_NAME}"
ENV_FILE="${ENV_FILE:-${REPO_DIR}/.env}"
URLS_FILE="${URLS_FILE:-${REPO_DIR}/input/robam_multi_profile_urls.txt}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8787}"
PYTHON_BIN="${PYTHON_BIN:-${REPO_DIR}/.venv/bin/python}"

cd "${REPO_PARENT}"

if command -v pkill >/dev/null 2>&1; then
  pkill -f "xhs_feishu_monitor.local_stats_app" 2>/dev/null || true
  sleep 1
fi

exec "${PYTHON_BIN}" -m xhs_feishu_monitor.local_stats_app \
  --env-file "${ENV_FILE}" \
  --urls-file "${URLS_FILE}" \
  --host "${HOST}" \
  --port "${PORT}"
