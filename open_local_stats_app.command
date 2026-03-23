#!/bin/zsh
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$APP_DIR/.." && pwd)"
HOST="127.0.0.1"
PORT="8787"
URL="http://${HOST}:${PORT}"
HEALTH_URL="${URL}/api/health"
ENV_FILE="${APP_DIR}/.env"
URLS_FILE="${APP_DIR}/input/robam_multi_profile_urls.txt"
OUTPUT_DIR="${APP_DIR}/output"
OUT_LOG="${OUTPUT_DIR}/local_stats_app.out.log"
ERR_LOG="${OUTPUT_DIR}/local_stats_app.err.log"
PID_FILE="${OUTPUT_DIR}/local_stats_app.pid"

mkdir -p "${OUTPUT_DIR}"

healthcheck() {
  curl -fsS "${HEALTH_URL}" >/dev/null 2>&1
}

if healthcheck; then
  open "${URL}"
  exit 0
fi

if [[ -f "${PID_FILE}" ]]; then
  APP_PID="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ -n "${APP_PID}" ]] && ps -p "${APP_PID}" >/dev/null 2>&1; then
    APP_CMD="$(ps -p "${APP_PID}" -o command= 2>/dev/null || true)"
    if [[ "${APP_CMD}" == *"xhs_feishu_monitor.local_stats_app"* ]]; then
      kill "${APP_PID}" >/dev/null 2>&1 || true
      sleep 1
    fi
  fi
  rm -f "${PID_FILE}"
fi

if lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[ERROR] 端口 ${PORT} 已被其他进程占用，且不是本地统计 app。请先释放端口后再启动。" >&2
  exit 1
fi

cd "${PROJECT_ROOT}"
nohup python3 -m xhs_feishu_monitor.local_stats_app \
  --env-file "${ENV_FILE}" \
  --urls-file "${URLS_FILE}" \
  --host "${HOST}" \
  --port "${PORT}" \
  >"${OUT_LOG}" 2>"${ERR_LOG}" </dev/null &

echo $! > "${PID_FILE}"

for _ in {1..30}; do
  if healthcheck; then
    open "${URL}"
    exit 0
  fi
  sleep 1
done

echo "[ERROR] 本地统计 app 启动超时，请检查日志：${ERR_LOG}" >&2
exit 1
