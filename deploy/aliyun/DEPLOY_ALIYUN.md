# 阿里云轻量服务器部署方案

这套方案按“服务器模式”部署，不再使用本机的：

- `launchd`
- `Chrome 默认资料`
- `/Users/cc/...` 路径

服务器上固定使用：

- `XHS_FETCH_MODE=requests`
- `XHS_COOKIE`
- `systemd` 托管本地看板
- `systemd timer` 或 `cron` 定时采集

## 目录约定

- 项目代码：`/opt/xhs_feishu_monitor`
- 虚拟环境：`/opt/xhs_feishu_monitor/.venv`
- 缓存目录：`/data/xhs_feishu_monitor/cache`
- 日志目录：`/var/log/xhs_feishu_monitor`

## 交付文件

- 环境模板：`deploy/aliyun/alicloud.env.template`
- 本地看板启动脚本：`deploy/aliyun/run_local_stats.sh`
- 定时同步脚本：`deploy/aliyun/run_sync.sh`
- systemd 服务：
  - `deploy/aliyun/systemd/xhs-local-stats.service`
  - `deploy/aliyun/systemd/xhs-sync.service`
  - `deploy/aliyun/systemd/xhs-sync.timer`
- cron 示例：`deploy/aliyun/cron.example`
- 一键安装脚本：`deploy/aliyun/install_aliyun.sh`

## 一键准备

```bash
cd /opt
git clone https://github.com/chenshiyue17-create/feishu.git xhs_feishu_monitor
cd /opt/xhs_feishu_monitor
bash deploy/aliyun/install_aliyun.sh
```

## 环境文件

复制模板：

```bash
cp /opt/xhs_feishu_monitor/deploy/aliyun/alicloud.env.template /opt/xhs_feishu_monitor/.env
```

至少填写：

- `XHS_COOKIE`
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_BITABLE_APP_TOKEN`
- `FEISHU_RANKING_BITABLE_APP_TOKEN`
- `FEISHU_TABLE_ID`

并确认：

- `PROJECT_CACHE_DIR=/data/xhs_feishu_monitor/cache`

## 账号清单

把监控账号清单放到：

```bash
/opt/xhs_feishu_monitor/input/robam_multi_profile_urls.txt
```

## 启动本地看板

最简单的手动启动：

```bash
cd /opt
/opt/xhs_feishu_monitor/.venv/bin/python -m xhs_feishu_monitor.local_stats_app \
  --env-file /opt/xhs_feishu_monitor/.env \
  --urls-file /opt/xhs_feishu_monitor/input/robam_multi_profile_urls.txt \
  --host 0.0.0.0 \
  --port 8787
```

### 用 systemd 托管

复制服务文件：

```bash
sudo cp /opt/xhs_feishu_monitor/deploy/aliyun/systemd/xhs-local-stats.service /etc/systemd/system/xhs-local-stats.service
sudo sed -i "s/User=%i/User=$USER/" /etc/systemd/system/xhs-local-stats.service
sudo systemctl daemon-reload
sudo systemctl enable --now xhs-local-stats.service
sudo systemctl status xhs-local-stats.service
```

## 定时采集

### 推荐：systemd timer

复制服务和定时器：

```bash
sudo cp /opt/xhs_feishu_monitor/deploy/aliyun/systemd/xhs-sync.service /etc/systemd/system/xhs-sync.service
sudo cp /opt/xhs_feishu_monitor/deploy/aliyun/systemd/xhs-sync.timer /etc/systemd/system/xhs-sync.timer
sudo sed -i "s/User=%i/User=$USER/" /etc/systemd/system/xhs-sync.service
sudo systemctl daemon-reload
sudo systemctl enable --now xhs-sync.timer
sudo systemctl list-timers | grep xhs-sync
```

当前定时策略是：

- 只在 `14:00-16:00`
- 每小时触发一次
- 项目内账号由程序内部低突发轮转

### 备选：cron

示例已放在：

```bash
/opt/xhs_feishu_monitor/deploy/aliyun/cron.example
```

## 健康检查

```bash
curl http://127.0.0.1:8787/api/health
curl "http://127.0.0.1:8787/api/dashboard?refresh=1"
```

## 服务器模式下的注意点

1. 不要用 `XHS_CHROME_COOKIE_PROFILE`
2. 不要依赖浏览器交互登录
3. 统一使用 `XHS_COOKIE`
4. 不要把缓存目录放在 `/Users/...`
5. 飞书只做：
   - `小红书日历留底`
   - `每日点赞复盘`
   - `每日评论复盘`

## 对外访问

如果要从公网访问本地看板：

```bash
sudo ufw allow 8787/tcp
```

更稳的做法是再配 `nginx` 反代到 `127.0.0.1:8787`。
