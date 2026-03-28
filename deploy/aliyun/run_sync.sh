#!/usr/bin/env bash
set -euo pipefail

REPO_PARENT="${REPO_PARENT:-/opt}"
REPO_NAME="${REPO_NAME:-xhs_feishu_monitor}"
REPO_DIR="${REPO_PARENT}/${REPO_NAME}"
ENV_FILE="${ENV_FILE:-${REPO_DIR}/.env}"
URLS_FILE="${URLS_FILE:-${REPO_DIR}/input/robam_multi_profile_urls.txt}"
PYTHON_BIN="${PYTHON_BIN:-${REPO_DIR}/.venv/bin/python}"
PROJECT_NAME="${1:-${PROJECT_NAME:-}}"

ARGS=(
  -m xhs_feishu_monitor.profile_batch_to_feishu
  --env-file "${ENV_FILE}"
  --urls-file "${URLS_FILE}"
  --scheduled
)

if [[ -n "${PROJECT_NAME}" ]]; then
  ARGS+=(--project "${PROJECT_NAME}")
fi

cd "${REPO_PARENT}"
exec "${PYTHON_BIN}" "${ARGS[@]}"
