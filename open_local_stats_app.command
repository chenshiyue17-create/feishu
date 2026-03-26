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
LAUNCHD_LABEL="com.cc.xhs-local-stats-app"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
LOGS_DIR="${HOME}/Library/Logs"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${LAUNCHD_LABEL}.plist"
OUT_LOG="${LOGS_DIR}/${LAUNCHD_LABEL}.out.log"
ERR_LOG="${LOGS_DIR}/${LAUNCHD_LABEL}.err.log"
PID_FILE="${OUTPUT_DIR}/local_stats_app.pid"
PYTHON_BIN="$(command -v python3)"
SHELL_BIN="/bin/zsh"
START_CMD="cd /Users/cc/Documents/New\\ project && exec ${PYTHON_BIN} -m xhs_feishu_monitor.local_stats_app --env-file /Users/cc/Documents/New\\ project/xhs_feishu_monitor/.env --urls-file /Users/cc/Documents/New\\ project/xhs_feishu_monitor/input/robam_multi_profile_urls.txt --host ${HOST} --port ${PORT}"

mkdir -p "${OUTPUT_DIR}"
mkdir -p "${LAUNCH_AGENTS_DIR}"
mkdir -p "${LOGS_DIR}"

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

cat > "${PLIST_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${SHELL_BIN}</string>
    <string>-lc</string>
    <string>${START_CMD}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${PROJECT_ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${OUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${ERR_LOG}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>PATH</key>
    <string>${PATH}</string>
    <key>HOME</key>
    <string>${HOME}</string>
  </dict>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
launchctl enable "gui/$(id -u)/${LAUNCHD_LABEL}" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/$(id -u)/${LAUNCHD_LABEL}" >/dev/null 2>&1 || true

for _ in {1..30}; do
  if healthcheck; then
    lsof -ti tcp:"${PORT}" | head -n 1 > "${PID_FILE}" 2>/dev/null || true
    open "${URL}"
    exit 0
  fi
  sleep 1
done

echo "[ERROR] 本地统计 app 启动超时，请检查日志：${ERR_LOG}" >&2
exit 1
