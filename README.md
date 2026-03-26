# XHS Feishu Monitor

把小红书笔记互动数据抓下来，标准化后写入飞书多维表格。

当前默认假设你要监控的是“笔记维度”的点赞、收藏、评论、分享数据，不是店铺订单或广告后台数据。

## 能做什么

- 读取多个小红书监控目标
- 支持三种输入源
  - 直接抓取 `url`
  - 读取本地保存的 `html_file`
  - 读取上游抓取器导出的 `json_file`
- 自动标准化为统一字段
- 写入飞书多维表格
- 支持两种同步模式
  - `upsert`: 按唯一字段更新同一条记录
  - `append`: 每次运行都新增一条快照
- 本地缓存上次数据，自动算互动增量

## 目录

- `xhs_feishu_monitor/cli.py`: 命令行入口
- `xhs_feishu_monitor/xhs.py`: 小红书抓取与解析
- `xhs_feishu_monitor/feishu.py`: 飞书多维表格同步
- `xhs_feishu_monitor/state.py`: 增量状态缓存
- `xhs_feishu_monitor/examples/`: 示例配置

## 安装

```bash
cd /Users/cc/Documents/New\ project
python3 -m pip install -r xhs_feishu_monitor/requirements.txt
```

## 飞书多维表格字段建议

默认字段映射见 [field_map.example.json](/Users/cc/Documents/New project/xhs_feishu_monitor/examples/field_map.example.json)。

建议先在飞书多维表格里建这些列：

- `笔记ID`
- `标题`
- `链接`
- `正文摘要`
- `作者`
- `作者ID`
- `发布时间`
- `抓取时间`
- `点赞数`
- `收藏数`
- `评论数`
- `分享数`
- `点赞增量`
- `收藏增量`
- `评论增量`
- `评论增长率`
- `评论预警`
- `分享增量`
- `监控名称`
- `标签`
- `备注`
- `快照键`

如果你的列名不同，把 [field_map.example.json](/Users/cc/Documents/New project/xhs_feishu_monitor/examples/field_map.example.json) 复制一份后修改，并在 `.env` 里配置 `FEISHU_FIELD_MAP_FILE`。

## 配置

1. 复制环境变量模板：

```bash
cp xhs_feishu_monitor/.env.example xhs_feishu_monitor/.env
```

2. 填这些值：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_BITABLE_APP_TOKEN`
- `FEISHU_TABLE_ID`
- `XHS_COOKIE`

`XHS_COOKIE` 不是必填，但小红书页面经常有登录态限制，实操里通常建议带上。

如果你已经在本机 Chrome 登录过小红书，也可以不手填 `XHS_COOKIE`，直接让程序读取本地资料目录：

```env
XHS_CHROME_COOKIE_PROFILE=xhs_feishu_monitor/.interactive_login_profile
```

程序会自动解密 Chrome 里的小红书 cookie，再走 `requests` 抓取。

如果你要加 IP 池，在 `.env` 里补其中一种：

```env
XHS_PROXY_POOL=http://1.2.3.4:8000,http://5.6.7.8:8000
XHS_PROXY_COOLDOWN_SECONDS=300
```

或者：

```env
XHS_PROXY_POOL_FILE=/absolute/path/to/proxies.txt
XHS_PROXY_COOLDOWN_SECONDS=300
```

`proxies.txt` 一行一个代理，支持 `host:port` 或完整 URL。程序会在 `requests` 抓取时按轮换顺序取代理，失败的代理会进入冷却期，过了 `XHS_PROXY_COOLDOWN_SECONDS` 后再参与轮换。浏览器抓取模式也会复用同一套代理配置；`local_browser` 如果复用默认 Chrome，会受浏览器自身会话和代理行为影响，稳定性不如 `requests/playwright`。

如果你要避免批量采集时瞬时并发过高，建议保留默认的低突发策略：

```env
XHS_BATCH_CONCURRENCY=2
XHS_BATCH_REQUEST_INTERVAL_SECONDS=2
XHS_BATCH_ACCOUNT_DELAY_SECONDS=1
XHS_BATCH_ACCOUNT_JITTER_SECONDS=0.8
XHS_BATCH_CHUNK_SIZE=8
XHS_BATCH_CHUNK_COOLDOWN_SECONDS=12
XHS_BATCH_RETRY_FAILED_ONCE=true
XHS_BATCH_RETRY_DELAY_SECONDS=20
XHS_BATCH_RISK_RETRY_DELAY_SECONDS=45
XHS_BATCH_PROJECT_COOLDOWN_SECONDS=45
```

说明：

- 默认无代理池时，程序会把 `requests` 并发自动收敛到最多 `2`
- 如果配置了代理池，并发上限会随代理数量小幅放开，但仍会控制在低突发范围
- `XHS_BATCH_REQUEST_INTERVAL_SECONDS` 是全局起步间隔，不是单账号重试延迟
- `XHS_BATCH_ACCOUNT_DELAY_SECONDS` 是每个账号采集之间的额外基础延迟
- `XHS_BATCH_ACCOUNT_JITTER_SECONDS` 会在每个账号之间再附加随机抖动，避免请求节奏过于固定
- `XHS_BATCH_CHUNK_SIZE / XHS_BATCH_CHUNK_COOLDOWN_SECONDS` 用于每跑完一小段后主动降速，降低连续打点过密的风险
- `XHS_BATCH_RETRY_FAILED_ONCE` 会把首轮超时、429、风控类失败放到尾部慢速补抓一次
- `XHS_BATCH_RETRY_DELAY_SECONDS` 是进入尾部重试前的等待时间，避免刚失败就立即再次命中风控
- `XHS_BATCH_RISK_RETRY_DELAY_SECONDS` 会给 429 / 403 / 风控 / 反爬 / 空结果 这类高风险失败更长的尾部延迟，并放到最后一轮再抓
- `XHS_BATCH_PROJECT_COOLDOWN_SECONDS` 会在全量 `urls_file` 任务里按项目分组后，给项目与项目之间留出冷却时间，适合 30 到 300 个账号的项目制监控

如果你要让账号主页拿到更准确的“总作品数”，当前版本会优先走 `xhshow` 签名请求访问 `user_posted` 分页接口：

```env
XHS_ENABLE_SIGNED_PROFILE_PAGES=true
XHS_SIGNED_PROFILE_MAX_PAGES=40
```

说明：

- 这个通道只用于账号主页翻页和精确总作品数，不影响普通笔记详情抓取
- 作品详情仍然只保留前 `30` 条，避免飞书和本地前端过重
- 额外翻页抓到的作品只用于补全 `账号总作品数`
- 如果 `xhshow` 没装好、签名失败、登录态无效，程序会自动退回现有下限口径，不会中断主流程

如果你还想在作品表里同步一段“最新评论摘要”，可以开启：

```env
XHS_FETCH_WORK_COMMENT_PREVIEW=true
XHS_WORK_COMMENT_PREVIEW_LIMIT=3
```

说明：

- 这里只抓第一页最新评论做摘要，不做整帖全量评论采集
- 默认最多保留前 `3` 条，适合预警和人工复核
- 这个摘要会尽量复用签名评论接口；拿不到时不会影响主流程

如果你要采集作品评论数并做日增预警，在 `.env` 里补这些值：

```env
XHS_FETCH_WORK_COMMENT_COUNTS=true
COMMENT_ALERT_GROWTH_THRESHOLD_PERCENT=10
COMMENT_ALERT_MIN_PREVIOUS_COUNT=0
FEISHU_NOTIFY_WEBHOOK=
FEISHU_NOTIFY_SECRET=
```

说明：

- `XHS_FETCH_WORK_COMMENT_COUNTS=true` 会在主页列表抓完后，继续打开作品详情补评论数
- `COMMENT_ALERT_GROWTH_THRESHOLD_PERCENT=10` 表示评论数相比上一轮基线增长超过 `10%` 就触发预警
- `FEISHU_NOTIFY_WEBHOOK` 是飞书群机器人的 Webhook；不填就只落预警记录，不发群通知
- `FEISHU_NOTIFY_SECRET` 只有你给机器人开启“签名校验”时才需要填

如果 `requests` 直抓不稳定，可以切到浏览器抓取模式：

```bash
python3 -m pip install playwright
playwright install chromium
```

然后在 `.env` 里加：

```env
XHS_FETCH_MODE=playwright
PLAYWRIGHT_HEADLESS=true
PLAYWRIGHT_WAIT_MS=4000
```

如果你已经有 Playwright 的登录态文件，还可以配置：

```env
PLAYWRIGHT_STORAGE_STATE=/absolute/path/to/storage_state.json
```

如果你要直接调用本机安装的 Chrome 打开小红书并抓取，可以改成：

```env
XHS_FETCH_MODE=local_browser
PLAYWRIGHT_BROWSER_MODE=local_profile
PLAYWRIGHT_CHANNEL=chrome
PLAYWRIGHT_USER_DATA_DIR=xhs_feishu_monitor/.local_chrome_profile
PLAYWRIGHT_PROFILE_DIRECTORY=Default
PLAYWRIGHT_HEADLESS=false
PLAYWRIGHT_WAIT_MS=7000
```

这会打开一个独立的本地 Chrome 实例，并把登录态保存在 `PLAYWRIGHT_USER_DATA_DIR`，适合需要人工登录后重复抓取的场景。

## 监控目标配置

示例见 [targets.example.json](/Users/cc/Documents/New project/xhs_feishu_monitor/examples/targets.example.json)。

每项支持三种写法：

```json
[
  {
    "name": "直接抓页面",
    "url": "https://www.xiaohongshu.com/explore/xxxxxxxxxxxx",
    "tags": ["老板电器", "蒸烤炸"]
  },
  {
    "name": "读取本地 HTML",
    "html_file": "saved/note.html"
  },
  {
    "name": "读取上游 JSON",
    "json_file": "saved/note.json"
  }
]
```

## 运行

## 长期运维入口

如果后续继续扩项目，优先看这两份文档：

- [飞书视图创建清单](/Users/cc/Documents/New%20project/xhs_feishu_monitor/FEISHU_VIEW_TEMPLATES.md)
- [项目新增 SOP](/Users/cc/Documents/New%20project/xhs_feishu_monitor/PROJECT_ONBOARDING_SOP.md)

当前长期建议固定为：

- 本地看板负责实时查看
- 飞书负责日历留底和近 `14` 天复盘
- 飞书长期只保留 3 张表：
  - `小红书日历留底`
  - `每日点赞复盘`
  - `每日评论复盘`
- 后续新增项目只复制视图，不新增新表

先做一次不落库预览：

```bash
python3 -m xhs_feishu_monitor \
  --targets xhs_feishu_monitor/examples/targets.example.json \
  --dry-run
```

正式同步：

```bash
python3 -m xhs_feishu_monitor \
  --targets /absolute/path/to/targets.json \
  --env-file xhs_feishu_monitor/.env
```

先做健康检查，不真正写入飞书：

```bash
python3 -m xhs_feishu_monitor \
  --targets /absolute/path/to/targets.json \
  --env-file xhs_feishu_monitor/.env \
  --check
```

这一步现在会检查三件事：

- 小红书目标能不能抓到互动数据
- 飞书凭证和多维表格访问权限是否正常
- `.env` 里的字段映射在目标数据表里是否真的存在

如果你现在只想先验证小红书抓取链路：

```bash
python3 -m xhs_feishu_monitor \
  --targets xhs_feishu_monitor/examples/targets.example.json \
  --check \
  --skip-feishu-check
```

如果要直接抓“账号页”的账号数据、作品标题和首页作品互动：

```bash
python3 -m xhs_feishu_monitor.profile_report \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --json-out xhs_feishu_monitor/output/profile_report.json
```

如果要把账号页摘要直接写入飞书多维表格：

```bash
python3 -m xhs_feishu_monitor.profile_to_feishu \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields
```

如果要把首页作品逐条同步到单独的数据表：

```bash
python3 -m xhs_feishu_monitor.profile_works_to_feishu \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env
```

如果要额外生成适合仪表盘的“总览 / 趋势 / 榜单 / 日历留底”四张看板表：

```bash
python3 -m xhs_feishu_monitor.profile_dashboard_to_feishu \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env
```

如果你决定用 `NanmiCoder/MediaCrawler` 来抓更完整的小红书内容，再把导出的 `contents.json/jsonl` 接到飞书：

```bash
python3 -m xhs_feishu_monitor.mediacrawler_xhs_to_feishu \
  --contents-file /absolute/path/to/creator_contents_2026xxxx.jsonl \
  --profile-url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard
```

这条链路的定位是：

- `MediaCrawler`: 拿更完整的作品明细、`note_id`、精确互动值、评论
- `xhs_feishu_monitor`: 负责飞书多维表格、看板总览、趋势和榜单同步

注意一件事：

- MediaCrawler 的小红书 `json/jsonl` 文件存储会稳定写出 `contents/comments`
- 但 `creator` 文件存储没有真正落文件
- 所以桥接脚本支持再补一个 `--profile-url`，用当前项目自己的主页解析来补账号摘要

如果要一次同步“账号汇总 + 作品明细”：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields
```

如果要一次同步“账号汇总 + 作品明细 + 看板表”：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard
```

如果要把这套“账号汇总 + 作品明细”装成 5 分钟轮询任务：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --install-launchd \
  --interval-seconds 300 \
  --load-launchd
```

默认会生成：

- `~/Library/LaunchAgents/com.cc.xhs-profile-live-sync.plist`
- `~/Library/Logs/com.cc.xhs-profile-live-sync.out.log`
- `~/Library/Logs/com.cc.xhs-profile-live-sync.err.log`

如果要卸载：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --unload-launchd
```

如果要每次都新增快照，不覆盖旧记录：

```bash
FEISHU_SYNC_MODE=append python3 -m xhs_feishu_monitor --targets /absolute/path/to/targets.json
```

## 定时执行

现在已经内置了 `launchd` 安装能力，适合你当前这台 Mac。

先生成并安装一个每 30 分钟跑一次的任务：

```bash
python3 -m xhs_feishu_monitor \
  --targets /absolute/path/to/targets.json \
  --env-file xhs_feishu_monitor/.env \
  --install-launchd \
  --interval-minutes 30
```

如果要安装后立刻加载：

```bash
python3 -m xhs_feishu_monitor \
  --targets /absolute/path/to/targets.json \
  --env-file xhs_feishu_monitor/.env \
  --install-launchd \
  --interval-minutes 30 \
  --load-launchd
```

如果你要轮询时连看板表一起更新，直接在 `profile_live_sync` 上加 `--sync-dashboard`：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard \
  --install-launchd \
  --interval-seconds 300 \
  --load-launchd
```

如果你不要轮询，而是固定每天 `14:00` 跑一次：

```bash
python3 -m xhs_feishu_monitor.profile_live_sync \
  --url 'https://www.xiaohongshu.com/user/profile/你的用户ID?...' \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard \
  --install-launchd \
  --daily-at 14:00 \
  --load-launchd
```

如果你现在用的是“项目制监控”，并且 `urls_file` 已经按 `项目名<TAB>主页链接` 维护，批量飞书同步任务支持直接按项目错峰安装：

```bash
python3 -m xhs_feishu_monitor.profile_batch_to_feishu \
  --urls-file xhs_feishu_monitor/input/robam_multi_profile_urls.txt \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard \
  --install-launchd \
  --daily-at 14:00 \
  --project-slot-minutes 20 \
  --load-launchd
```

这样会按项目顺序自动拆成多条任务，例如：

- `默认项目`：`14:00`
- `项目B`：`14:20`
- `项目C`：`14:40`

如果你只想给单个项目装定时任务，也可以直接指定：

```bash
python3 -m xhs_feishu_monitor.profile_batch_to_feishu \
  --urls-file xhs_feishu_monitor/input/robam_multi_profile_urls.txt \
  --project 默认项目 \
  --env-file xhs_feishu_monitor/.env \
  --ensure-fields \
  --sync-dashboard \
  --install-launchd \
  --daily-at 14:00 \
  --load-launchd
```

## 评论预警

当前评论预警是按“上一轮成功同步结果”做基线比对。你现在的任务已经固定在每天 `14:00` 跑，所以实际效果就是：

- 第一天：写入作品评论数，建立基线
- 第二天开始：如果同一作品评论数比前一天 `14:00` 的结果增长超过 `10%`，就会标记 `评论预警`
- 如果配置了 `FEISHU_NOTIFY_WEBHOOK`，同一批超阈值作品会直接发到飞书群机器人

评论相关字段会落在 `小红书作品数据` 表里：

- `评论数`
- `评论文本`
- `评论增量`
- `评论增长率`
- `评论预警`

当某天真的触发预警时，还会自动创建并写入一张 `小红书评论预警` 表。

## 作品留底

现在还会额外维护一张 `小红书作品日历留底` 表：

- 唯一键是 `日期 + 作品指纹`
- 每天每条作品只保留 1 条快照
- 可直接在飞书切日历视图，按 `日历日期` 看每天发了哪些内容、当天点赞和评论是多少

`小红书作品数据` 主表也会基于这张日历表，自动补作品级一周对比字段：

- `上周日期文本`
- `上周点赞数` / `点赞周增量` / `点赞周增幅`
- `上周评论数` / `评论周增量` / `评论周增幅`
- `周对比摘要`

规则和账号日历留底一致：优先取 7 天前同一作品的留底；如果那天没有记录，就取更早、且最接近 7 天前的一条。

## 看板布局

参考图那种“顶部指标卡 + 左侧排行榜 + 中间 TOP3 卡片 + 底部趋势图”的布局，我已经整理成了可直接照着搭的说明：

- [dashboard_layout.md](/Users/cc/Documents/New project/xhs_feishu_monitor/dashboard_layout.md)

推荐的数据源对应关系：

- `小红书仪表盘总控`: 顶部指标卡
- `小红书看板总览`: 账号排行和账号结构图
- `小红书看板榜单`: 排行榜和 TOP3 卡片
- `小红书看板趋势`: 底部趋势图
- `小红书日历留底`: 每天每账号 1 条快照，适合直接切飞书日历视图
- `小红书单条作品排行`: 单条点赞排行、单条评论排行、单条第二天增长排行

## 本地统计前端 App

如果你还想在本机直接看一版统计前端，不进飞书也能看总控、排行、趋势和评论预警，可以直接起本地服务：

```bash
python3 -m xhs_feishu_monitor.local_stats_app \
  --env-file xhs_feishu_monitor/.env \
  --urls-file xhs_feishu_monitor/input/robam_multi_profile_urls.txt \
  --host 127.0.0.1 \
  --port 8787 \
  --open-browser
```

启动后打开：

- `http://127.0.0.1:8787`

当前本地 app 直接读取飞书里的这些表做聚合：

- `小红书仪表盘总控`
- `小红书日历留底`
- `小红书单条作品排行`
- `小红书评论预警`（没有也能正常打开）

页面现在包含：

- 监测账号管理：可直接粘贴新的小红书主页链接，立即写入监测清单并触发同步
- 监测清单操作：支持暂停、恢复、删除单个账号；暂停后每天 `14:00` 的任务会自动跳过该账号
- 账号名展示：同步完成后，监测清单会优先显示账号名字，链接作为副信息保留
- 账号独立视角：顶部指标、趋势、榜单、预警都按“当前选中账号”展示，不再把多个账号的数据做无意义累加
- 顶部账号卡片：当前选中账号的粉丝、获赞收藏、可见作品、首页总点赞、首页总评论，另保留监测账号数
- 趋势区：日更走势，按 `小红书日历留底` 每天保留 1 个点；当前口径是每天 `14:00` 留底，并支持 `1天 / 7天` 切换与涨跌提示
- 榜单区：支持 `当前账号 / 所有账号` 两种维度，并按 `点赞 / 评论 / 次日增长` 三列同时展示；榜单项会直接展示作品封面、标题、账号、榜单值和跳转链接
- 榜单区：单条点赞排行、单条评论排行、单条第二天增长排行
- 账号区：每个账号的粉丝、获赞收藏、总点赞、总评论、周对比摘要
- 预警区：最近触发的评论增长提醒
- 链接入口：账号卡、榜单、预警里都可以直接点进小红书主页，账号卡还支持直接跳头部作品

现在本地 app 还会直接维护这份监测清单：

- [robam_multi_profile_urls.txt](/Users/cc/Documents/New%20project/xhs_feishu_monitor/input/robam_multi_profile_urls.txt)

你在页面里新增账号后，这个文件会即时更新；每天 `14:00` 的定时任务也会继续读取同一份清单，所以新增账号会自动进入后续日常采集。

如果你想在 Finder 里直接双击打开，本地也准备了这两个入口：

- 启动脚本：[open_local_stats_app.command](/Users/cc/Documents/New%20project/xhs_feishu_monitor/open_local_stats_app.command)
- Mac 应用：`/Users/cc/Documents/New project/XHS Local Stats App.app`

双击后会自动检查本地服务是否已启动；如果没有，就在后台拉起本地统计前端并自动打开浏览器。

如果你想强制刷新缓存，可以直接访问：

- `http://127.0.0.1:8787/api/dashboard?refresh=1`

## 日历留底

开启 `--sync-dashboard` 后，程序会额外维护一张 `小红书日历留底` 表：

- 唯一键是 `日期 + 账号ID`
- 同一天重复执行只会更新当天这条，不会刷出多条重复日历卡片
- 每天 `14:00` 的定时任务会自动把当天账号快照留底

这张表建议在飞书里新建一个“日历视图”，日期字段选 `日历日期`，卡片标题优先显示 `日历标题`。

现在这张表还会自动补“一周对比”字段：

- `上周日期文本`
- `上周粉丝数` / `粉丝周增量` / `粉丝周增幅`
- `上周获赞收藏数` / `获赞收藏周增量` / `获赞收藏周增幅`
- `上周首页总点赞` / `首页总点赞周增量` / `首页总点赞周增幅`
- `上周首页总评论` / `首页总评论周增量` / `首页总评论周增幅`
- `周对比摘要`

周对比规则是：优先取同一账号“7 天前”的日历留底；如果那天没有记录，就取更早、且最接近 7 天前的一条。历史未满 7 天时，会提示“暂无 7 天前留底”。

`小红书仪表盘总控` 也会基于这张日历表，自动汇总整组账号的一周对比，包括总粉丝、总获赞收藏、总作品、总点赞和总评论的周增量与周增幅。

`小红书单条作品排行` 建议直接建 3 个筛选视图：

- `单条点赞排行`：`榜单类型 = 单条点赞排行`
- `单条评论排行`：`榜单类型 = 单条评论排行`
- `单条第二天增长排行`：`榜单类型 = 单条第二天增长排行`

其中“第二天增长”按 `今天 14:00` 相比 `昨天 14:00` 的同作品互动增量排序；当前没有昨天基线时，这个视图会暂时为空。

## 飞书视图模板

现在飞书侧建议固定成 3 张表：

- `小红书日历留底`
- `项目账号排行榜`
- `项目作品排行榜`

后续新增项目时，不再继续新建数据表，而是在这 3 张表里按 `项目` 和 `榜单类型` 建筛选视图。  
我已经把推荐视图整理成模板，直接按这份文档配置即可：

- [FEISHU_VIEW_TEMPLATES.md](/Users/cc/Documents/New%20project/xhs_feishu_monitor/FEISHU_VIEW_TEMPLATES.md)

默认会生成：

- `~/Library/LaunchAgents/com.cc.xhs-feishu-monitor.plist`
- `~/Library/Logs/com.cc.xhs-feishu-monitor.out.log`
- `~/Library/Logs/com.cc.xhs-feishu-monitor.err.log`

如果要卸载：

```bash
python3 -m xhs_feishu_monitor \
  --targets /absolute/path/to/targets.json \
  --env-file xhs_feishu_monitor/.env \
  --unload-launchd
```

如果你后面要改成服务器跑，再换成 `cron`、容器定时任务或 Codex automation 也可以。

## 说明

- 小红书当前公开开放平台文档主要是电商接口，不是笔记互动监控接口，所以这里采用页面抓取/上游 JSON 适配器方案。
- 页面结构可能变化，`url` 直抓不是永久稳定；一旦页面结构变了，可以先切到 `html_file` 或 `json_file` 模式继续跑。
- 如果页面是前端动态渲染或登录态要求更高，优先改用 `XHS_FETCH_MODE=playwright`。
- 当前默认按“笔记”监控，不含作者主页总粉丝、店铺交易或投流数据。
- 当前公开主页在未登录态下不会稳定返回作品 `note_id`，所以“作品明细”能持续更新标题、类型、封面、点赞文本等公开字段，但单篇作品详情链接和更深层互动字段需要 `XHS_COOKIE` 或更完整的登录态。
