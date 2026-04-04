# XHS Local Stats App

当前这套项目的主线已经固定为：

- 本机登录小红书后采集
- 本地生成看板与缓存
- 服务器只接收缓存并提供网页/手机查看
- 手机端只查看服务器缓存

这份文档只描述**现在实际在用**的链路，不再把历史飞书方案当主路径。

## 当前能做什么

- 采集多个小红书账号主页与作品列表
- 补抓作品详情，拿点赞、评论等核心指标
- 本地生成：
  - 项目总览
  - 项目日历
  - 点赞榜
  - 评论榜
  - 次日增长榜
  - Top 账号
  - Top 内容
  - 评论预警
- 支持：
  - 更新全部项目
  - 更新当前项目
  - 更新当前账号
- 本机缓存推送到服务器
- 服务器网页查看
- 手机页查看
- Mac `launchd` 自动采集/自动上传

## 当前实际组件

- 本地应用：
  - [/Users/cc/Documents/New project/XHS Local Stats App.app](/Users/cc/Documents/New%20project/XHS%20Local%20Stats%20App.app)
- 本地服务入口：
  - [open_local_stats_app.command](/Users/cc/Documents/New%20project/xhs_feishu_monitor/open_local_stats_app.command)
- 本地前端：
  - [local_stats_app/web/index.html](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/index.html)
  - [local_stats_app/web/app.js](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/app.js)
  - [local_stats_app/web/styles.css](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/styles.css)
- 服务器服务：
  - [local_stats_app/server.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/server.py)
- 本地自动任务：
  - [local_daily_sync.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_daily_sync.py)

## 采集链路

当前主采集链路是：

- 你项目自己的控制逻辑：
  - [xhs.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs.py)
- 签名接口能力：
  - [xhs_signed.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs_signed.py)
- 上游签名项目：
  - [`xhshow`](https://github.com/Cloxl/xhshow)

当前实际模式：

- 主通道：
  - `requests + xhshow`
- 备用通道：
  - `playwright`
  - `local_browser`

一句话理解：

- `xhshow` 负责签名接口
- 本项目负责调度、缓存、看板、服务器展示
- `playwright` 只作为浏览器补救通道

## 当前数据口径

### 项目主卡

当前主卡默认显示：

- 项目粉丝总量
- 项目获赞收藏
- 项目评论总量

这些是**当前项目账号汇总值**，不是榜单条数。

### 榜单条数

本地缓存状态里的：

- 点赞榜 `xxx 条`
- 评论榜 `xxx 条`
- 增长榜 `xxx 条`

表示的是**榜单作品条数**，不是项目总量。

### 次日增长榜

增长榜依赖：

- 当前作品缓存
- 前一天作品历史

当前历史文件在项目目录里：

- `tracked_works.json`
- `tracked_work_history.json`
- `ranking_rows.json`

现在缓存重建时会优先从这两份历史重算增长榜，不再只依赖旧 `ranking_rows.json`。

## 完整性规则

当前主线要求是：

- 没登录，不开跑
- 样本账号作品详情不可用，不开跑
- 已经拿到的精确数据优先保留
- 新一轮不完整结果不应该把完整旧值冲掉

评论数当前会明确区分来源：

- `精确值`
- `详情缺失`

不再把旧缓存或评论下限伪装成精确值。

## 本地缓存目录

默认本地缓存目录：

- [/Users/cc/Downloads/飞书缓存](/Users/cc/Downloads/%E9%A3%9E%E4%B9%A6%E7%BC%93%E5%AD%98)

每个项目目录里当前会有：

- `dashboard.json`
- `calendar_rows.json`
- `ranking_rows.json`
- `tracked_works.json`
- `tracked_work_history.json`
- `covers/`

封面会单独保存到：

- `PROJECT_CACHE_DIR/<项目>/covers/`

并且会去重，避免重复下载同一张图。

## 本地运行

安装依赖：

```bash
cd '/Users/cc/Documents/New project'
python3 -m pip install -r xhs_feishu_monitor/requirements.txt
```

当前依赖里最关键的是：

- `requests`
- `xhshow`

如果你要启用浏览器采集补救：

```bash
python3 -m pip install playwright
playwright install chromium
```

启动本地 App：

- [XHS Local Stats App.app](/Users/cc/Documents/New%20project/XHS%20Local%20Stats%20App.app)

或者直接跑本地服务：

```bash
cd '/Users/cc/Documents/New project'
python3 -m xhs_feishu_monitor.local_stats_app \
  --env-file '/Users/cc/Documents/New project/xhs_feishu_monitor/.env' \
  --urls-file '/Users/cc/Documents/New project/xhs_feishu_monitor/input/robam_multi_profile_urls.txt'
```

本地默认地址：

- [http://127.0.0.1:8787](http://127.0.0.1:8787)

## 手动更新入口

当前本地看板里有 3 个手动入口：

- 更新全部项目
- 更新当前项目
- 更新当前账号

规则：

- `更新全部项目`：全量跑全部激活账号
- `更新当前项目`：只跑当前项目
- `更新当前账号`：只跑当前选中账号

自动任务运行中时：

- 会禁用“更新全部项目”
- 避免和自动任务抢同一批账号

## 自动任务

当前自动任务使用 Mac `launchd`。

核心脚本：

- [local_daily_sync.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_daily_sync.py)

当前设计目标：

- 每天下午自动采集
- 本机登录态不可用时不空跑
- 自动采集成功后再自动上传服务器

当前 `launchd` 状态文件：

- [/Users/cc/Documents/New project/xhs_feishu_monitor/.local_daily_sync_status.json](/Users/cc/Documents/New%20project/xhs_feishu_monitor/.local_daily_sync_status.json)

这是运行时文件，不建议提交到 Git。

## 服务器模式

当前服务器不负责采集，职责只有两个：

- 接收本机推送来的缓存
- 提供网页和手机端查看

服务器主服务：

- [local_stats_app/server.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/server.py)

推荐访问地址：

- 主页面：
  - [http://47.87.68.74](http://47.87.68.74)
- 手机页默认项目：
  - [http://47.87.68.74/mobile/index.html?project=默认项目](http://47.87.68.74/mobile/index.html?project=%E9%BB%98%E8%AE%A4%E9%A1%B9%E7%9B%AE)
- 手机页东莞：
  - [http://47.87.68.74/mobile/index.html?project=东莞](http://47.87.68.74/mobile/index.html?project=%E4%B8%9C%E8%8E%9E)

当前服务器通过 `nginx` 代理到本地服务，不再建议直接用 `:8787` 做公网入口。

## 推送服务器

当前推送脚本：

- [profile_cache_push.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_cache_push.py)

本机采集后可直接推送：

```bash
cd '/Users/cc/Documents/New project'
python3 -m xhs_feishu_monitor.profile_cache_push \
  --env-file '/Users/cc/Documents/New project/xhs_feishu_monitor/.env' \
  --urls-file '/Users/cc/Documents/New project/xhs_feishu_monitor/input/robam_multi_profile_urls.txt' \
  --server-url 'http://47.87.68.74'
```

当前 App 内也已经有：

- `推送到服务器`

按钮，不需要每次手动开终端。

## 当前推荐配置

当前建议优先使用：

```env
XHS_FETCH_MODE=requests
XHS_ENABLE_SIGNED_PROFILE_PAGES=true
XHS_SIGNED_PROFILE_MAX_PAGES=40
SERVER_CACHE_PUSH_URL=http://47.87.68.74
```

如果 `requests` 下作品详情不稳，再切：

```env
XHS_FETCH_MODE=playwright
```

如果你要直接复用本机 Chrome：

```env
XHS_FETCH_MODE=local_browser
PLAYWRIGHT_BROWSER_MODE=local_profile
PLAYWRIGHT_CHANNEL=chrome
```

## 当前不再作为主线的内容

仓库里仍然保留了一些历史飞书相关模块，例如：

- [feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/feishu.py)
- [profile_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_to_feishu.py)
- [profile_dashboard_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_dashboard_to_feishu.py)

这些模块目前主要保留为：

- 兼容旧数据结构
- 复用榜单/日历/字段构建逻辑
- 历史导出或桥接脚本

但**当前日常使用主线不是“采集后写飞书”**。

## 仓库地址

- 本项目：
  - [https://github.com/chenshiyue17-create/feishu](https://github.com/chenshiyue17-create/feishu)
- 当前上游签名采集项目：
  - [https://github.com/Cloxl/xhshow](https://github.com/Cloxl/xhshow)

## 一句话操作指南

日常只记这条：

1. 在本机登录小红书
2. 打开本地 [XHS Local Stats App.app](/Users/cc/Documents/New%20project/XHS%20Local%20Stats%20App.app)
3. 点 `更新全部项目`
4. 点 `推送到服务器`
5. 手机打开服务器页查看
