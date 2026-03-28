#!/usr/bin/env bash
set -euo pipefail

APP_USER="${APP_USER:-$USER}"
REPO_URL="${REPO_URL:-https://github.com/chenshiyue17-create/feishu.git}"
REPO_PARENT="${REPO_PARENT:-/opt}"
REPO_NAME="${REPO_NAME:-xhs_feishu_monitor}"
REPO_DIR="${REPO_PARENT}/${REPO_NAME}"
CACHE_DIR="${CACHE_DIR:-/data/xhs_feishu_monitor/cache}"
LOG_DIR="${LOG_DIR:-/var/log/xhs_feishu_monitor}"

sudo apt update
sudo apt install -y python3 python3-venv python3-pip git curl

sudo mkdir -p "${REPO_PARENT}" "${CACHE_DIR}" "${LOG_DIR}"
if [[ ! -d "${REPO_DIR}/.git" ]]; then
  sudo git clone "${REPO_URL}" "${REPO_DIR}"
fi
sudo chown -R "${APP_USER}:${APP_USER}" "${REPO_DIR}" "${CACHE_DIR}" "${LOG_DIR}"

cd "${REPO_DIR}"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

mkdir -p input
chmod +x deploy/aliyun/run_local_stats.sh deploy/aliyun/run_sync.sh deploy/aliyun/install_aliyun.sh

echo
echo "[OK] 部署基础环境完成"
echo "[TODO] 复制 deploy/aliyun/alicloud.env.template 到 ${REPO_DIR}/.env 并填写真实值"
echo "[TODO] 放置账号清单到 ${REPO_DIR}/input/robam_multi_profile_urls.txt"
echo "[TODO] 安装 systemd 服务或 cron，详见 deploy/aliyun/DEPLOY_ALIYUN.md"
