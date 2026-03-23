# 小红书监控与飞书看板技术架构设计文档

## 1. 文档目标

本文档用于说明当前项目的技术架构、模块职责、主数据流、运行方式、稳定性策略和扩展边界，作为后续维护、重构和功能扩展的依据。

本文档聚焦当前已经落地的本地单机架构，不讨论云原生改造或分布式部署。

## 2. 总体架构

系统采用“本地采集 + 本地编排 + 飞书存储 + 本地看板展示”的单机架构。

核心思路：

- 小红书采集在本机完成
- 飞书多维表格作为业务数据展示层与轻量持久化层
- 本地前端作为运营查看与管理入口
- `launchd` 负责定时调度
- 所有状态控制、降级策略、清单管理在本地代码内完成

### 2.1 架构分层

#### 接入层

- 小红书网页
- 小红书签名接口
- 本机 Chrome Cookie / 默认浏览器会话
- 飞书开放平台 API

#### 采集层

- 主页抓取
- 作品详情抓取
- 评论摘要抓取
- 浏览器回退抓取
- 代理池

#### 业务编排层

- 账号报告组装
- 作品报告组装
- 评论预警计算
- 周对比与日历留底
- 飞书数据同步

#### 展示层

- 飞书多维表格
- 飞书仪表盘数据层
- 本地统计前端

#### 调度与运维层

- `launchd`
- 手动同步
- 登录态健康检查
- 进度条与限频
- 本地缓存与回退

## 3. 代码模块划分

### 3.1 配置与基础模型

- [config.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/config.py)
  - 统一环境变量读取
  - 小红书采集、代理池、飞书同步、评论预警等全局配置
- [models.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/models.py)
  - `Target`
  - `NoteSnapshot`

### 3.2 小红书采集层

- [xhs.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs.py)
  - 通用采集核心
  - 支持 `requests / playwright / local_browser / auto`
  - 支持代理池轮换
  - 支持 HTML / JSON / 浏览器态解析
- [xhs_signed.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs_signed.py)
  - 签名接口请求封装
  - 主页分页签名采集
  - 作品详情签名采集
  - 评论摘要签名采集
- [chrome_cookies.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/chrome_cookies.py)
  - 从本机默认 Chrome 读取并解密 Cookie

### 3.3 账号与作品报告层

- [profile_report.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_report.py)
  - 账号主页报告生成
  - 前 30 条作品抽取
  - 精确总作品数补全
- [profile_metrics.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_metrics.py)
  - 作品详情补全
  - 作品评论数与最新评论摘要补全
- [profile_batch_report.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_batch_report.py)
  - 多账号批量采集
  - 并发调度
  - 批量任务入口

### 3.4 飞书同步层

- [feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/feishu.py)
  - 飞书 API 封装
  - 表、字段、记录、upsert、对比更新
- [profile_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_to_feishu.py)
  - 账号总览表同步
- [profile_works_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_works_to_feishu.py)
  - 作品明细表
  - 作品日历留底
- [profile_dashboard_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_dashboard_to_feishu.py)
  - 看板总览
  - 榜单
  - 趋势
  - 日历留底
  - 单条排行
- [profile_batch_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_batch_to_feishu.py)
  - 多账号一体化同步主入口
  - 采集报告合并
  - 保留旧详情
  - 飞书批量写入

### 3.5 预警层

- [comment_alerts.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/comment_alerts.py)
  - 评论增长率计算
  - 评论预警表同步
  - 飞书群机器人通知

### 3.6 本地前端层

- [local_stats_app/server.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/server.py)
  - 本地 HTTP 服务
  - 同步状态机
  - 手动同步控制
  - HTTP 路由与响应
  - 本地覆盖缓存
- [local_stats_app/monitored_accounts.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/monitored_accounts.py)
  - 监测清单读写
  - 项目归属与暂停状态管理
  - 本地元数据缓存
  - 账号展示补全
- [local_stats_app/login_state.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/login_state.py)
  - 登录态自检
  - 浏览器登录唤起
  - 登录等待与恢复采集
- [local_stats_app/data_service.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/data_service.py)
  - 从飞书数据表构建前端 payload
- [local_stats_app/web/index.html](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/index.html)
- [local_stats_app/web/app.js](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/app.js)
- [local_stats_app/web/styles.css](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/web/styles.css)

### 3.7 调度与启动层

- [launchd.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/launchd.py)
  - `launchd` plist 生成与安装
- [profile_live_sync.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_live_sync.py)
  - 单账号实时同步入口
- [open_local_stats_app.command](/Users/cc/Documents/New%20project/xhs_feishu_monitor/open_local_stats_app.command)
  - 本地前端启动脚本

## 4. 主数据流

### 4.1 日常自动同步链路

每天 `14:00`，`launchd` 触发批量同步任务。

主流程如下：

1. 读取监控清单
2. 过滤暂停账号
3. 执行多账号采集
4. 为每个账号生成主页报告
5. 对前 30 条作品补详情、评论数、最新评论摘要
6. 合并历史详情，避免退化覆盖
7. 计算评论增长与预警
8. 生成账号表、作品表、榜单表、趋势表、日历留底表数据
9. 飞书写入前做字段级对比，无变化则跳过
10. 同步完成后，本地前端从飞书和本地覆盖缓存读取最新结果

### 4.2 手动同步链路

手动同步入口来自本地前端。

流程如下：

1. 用户点击“立即更新”
2. 系统检查冷却时间
3. 系统执行登录态前置自检
4. 若登录态不可用，则提示先登录
5. 若可用，则发起采集
6. 先把结果更新到本地前端覆盖缓存
7. 再异步写入飞书

这个设计保证：

- 前端优先看到新数据
- 飞书写慢时不阻塞本地查看

### 4.3 单账号重试链路

当某个账号异常时，监控卡片可单独触发重试。

流程如下：

1. 从清单中取当前账号
2. 执行单账号采集
3. 更新识别状态
4. 刷新本地缓存
5. 按需同步飞书

### 4.4 登录态健康检查链路

登录态检查由本地前端服务触发。

流程如下：

1. 读取当前采集模式和 Cookie 来源
2. 选择样本账号
3. 尝试抓取账号页和作品详情
4. 检查是否命中登录页
5. 检查是否拿到 `note_id` 和评论数
6. 输出 `正常 / 关注 / 异常`

## 5. 小红书采集架构

### 5.1 采集模式设计

当前支持四种模式：

- `requests`
- `playwright`
- `local_browser`
- `auto`

推荐运行方式：

- 长期批量监控使用 `requests + 默认 Chrome Cookie + 签名接口`
- `playwright` 用于回退补救
- `local_browser` 用于人工交互登录与调试

### 5.2 采集能力分层

#### 第一层：公开页与基础 HTML 提取

适合：

- 主页首屏
- 未登录情况下的降级场景

特点：

- 实现简单
- 成本低
- 不稳定
- 详情字段容易缺失

#### 第二层：签名接口采集

当前已接入：

- `user_posted`
- `feed`
- `comment/page`

作用：

- 主页翻页
- 精确总作品数
- 作品详情补全
- 评论摘要补全

#### 第三层：浏览器回退

用于：

- 登录态失效时人工补救
- 签名链路异常时的兜底

### 5.3 签名层设计

签名方案基于 `xhshow`。

当前接入方式：

- 在 [requirements.txt](/Users/cc/Documents/New%20project/xhs_feishu_monitor/requirements.txt) 固定依赖
- 由 [xhs.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs.py) 内部按需加载
- 按 `a1` 或账号维度维护 `SessionManager`

设计原因：

- 直接嵌到现有项目中
- 不引入额外爬虫框架层
- 可复用当前的 Cookie、代理池、降级保护

### 5.4 代理池设计

代理池能力集成在采集层内部。

包括：

- 轮换选择
- 失败冷却
- 最近错误记录
- 最近成功记录
- 本地前端状态面板展示

### 5.5 降级原则

当前采集层必须遵守：

- 降级可以发生
- 降级结果必须可识别
- 降级结果不能覆盖已有完整详情

典型场景：

- 主页拿到 `30+` 但精确总作品数失败
- 签名详情失败，只保留已有 `note_id`
- 评论摘要抓不到，不覆盖已有摘要

## 6. 飞书同步架构

### 6.1 飞书定位

飞书在当前架构中的角色不是“唯一真源数据库”，而是：

- 展示层
- 轻量持久化层
- 团队共享查看层

真实实时运行状态仍掌握在本地任务和本地前端中。

### 6.2 表设计原则

- 账号视图与作品视图分离
- 趋势与榜单分离
- 日历留底与当前快照分离
- 预警独立建表
- 每张表只服务一个清晰用途

### 6.3 更新策略

当前同步策略为：

- 优先 `upsert`
- 同步前做字段对比
- 忽略单纯时间字段变化
- 无变化数据不更新

收益：

- 节省飞书 API 调用
- 降低同步时间
- 降低频繁写入失败风险

### 6.4 数据保留策略

- 账号总览保留当前状态
- 日历留底保留日快照
- 作品日历留底保留按作品的历史点
- 榜单保留当前排名结果
- 预警保留告警事件

## 7. 本地前端架构

### 7.1 设计目标

本地前端承担两个角色：

- 运营查看入口
- 本地控制台

因此它不仅展示数据，也负责：

- 账号管理
- 项目管理
- 手动同步
- 登录态检查
- 代理池状态查看

### 7.2 数据来源策略

本地前端的数据来源有两层：

#### 第一层：飞书表数据

适合：

- 稳定展示
- 历史趋势
- 榜单
- 预警

#### 第二层：本地覆盖缓存

适合：

- 手动更新后的即时结果
- 飞书尚未写完时的前端刷新
- 飞书暂时失败时的短期兜底

### 7.3 项目制设计

项目制是为 30 到 300 个账号规模准备的结构层。

特点：

- 项目是一级视角
- 项目内再看账号
- 同步支持按项目触发
- 榜单与趋势按项目过滤

### 7.4 前端展示口径

当前关键展示口径：

- 顶部业务卡按当前账号展示
- 监测账号数单独作为范围信息
- 趋势图按日更数据展示
- 榜单可切当前账号或所有账号
- 榜单只显示前十

## 8. 调度与运行架构

### 8.1 定时任务

当前调度基于 Mac `launchd`。

默认策略：

- 每天 `14:00` 触发
- 读取本地监控清单
- 跑完整采集与飞书同步

### 8.2 启动入口

主要入口：

- [cli.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/cli.py)
- [profile_batch_report.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_batch_report.py)
- [profile_batch_to_feishu.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_batch_to_feishu.py)
- [profile_live_sync.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/profile_live_sync.py)
- [local_stats_app/server.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/local_stats_app/server.py)

### 8.3 运行状态可视化

当前系统已具备以下本地可观测能力：

- 进度条
- 成功/失败计数
- 预计剩余时间
- 登录态健康
- 代理池状态
- 单账号识别状态

## 9. 稳健性设计

### 9.1 不覆盖完整数据

这是当前最核心的稳健性规则。

当本次抓取退化为：

- 无 `note_id`
- 无作品链接
- 无评论数
- 无最新评论摘要

系统必须优先保留已有完整数据。

### 9.2 本地优先显示

手动同步时：

- 先更新本地前端
- 后更新飞书

这样即使飞书慢或失败，用户也能先看到最新结果。

### 9.3 登录态问题隔离

登录态异常不应表现为“数据突然变成 0”。

系统当前的处理方式：

- 通过健康检查暴露异常
- 在监测列表显示失败原因
- 必要时弹登录窗口
- 保留旧详情，避免误清空

### 9.4 文件写入安全

监测清单采用原子写入，避免中途异常造成清单损坏。

### 9.5 飞书写入节流

通过“写前对比”减少无意义更新，降低：

- 同步时间
- API 调用量
- 写入失败率

## 10. 扩展点设计

### 10.1 采集扩展点

当前最明显的扩展点在 [xhs.py](/Users/cc/Documents/New%20project/xhs_feishu_monitor/xhs.py)：

- 新增签名接口
- 替换签名库
- 加深评论分页
- 补充二级评论

### 10.2 数据扩展点

当前可继续扩展：

- 评论内容情绪标签
- 热词抽取
- 评论关键词预警
- 项目级阈值
- 作品内容分类

### 10.3 展示扩展点

本地前端可继续扩展：

- 项目首页独立大屏
- 评论详情页
- 失败原因统计页
- 账号对比视图

## 11. 当前技术债

### 11.1 单机本地耦合较强

当前系统默认：

- 本机 Chrome
- 本机 `launchd`
- 本机文件清单
- 本机前端服务

这保证了快速可用，但也意味着：

- 不利于多人协作部署
- 不利于跨电脑迁移

### 11.2 采集与业务编排仍部分耦合

`profile_report.py` 当前承担了：

- 主页报告生成
- 作品详情补全
- 评论摘要补全

后续如果评论能力继续加深，建议拆出专门的“作品详情 enrich 层”。

### 11.3 飞书既做展示又做部分持久化

这在当前阶段可接受，但当：

- 历史数据量快速增长
- 需要更复杂查询
- 需要更稳定恢复

时，飞书可能不够。

## 12. 后续重构建议

### 12.1 短期

- 把评论摘要加入预警通知
- 把评论采集封装成独立模块
- 增加采集失败原因统计页

### 12.2 中期

- 把“账号采集 / 作品补全 / 评论补全 / 飞书同步”拆成更清晰的流水线模块
- 增加本地 SQLite 缓存层，减少对飞书历史回读依赖
- 为项目级配置增加单独配置文件

### 12.3 长期

- 支持云端运行
- 支持远程任务执行
- 支持统一任务中心
- 支持跨平台多源内容监控

## 13. 结论

当前架构适合的目标非常明确：

- 本地单机长期运行
- 30 到 300 个账号规模
- 以飞书为团队查看入口
- 以本地前端为操控与即时查看入口

当前最关键的技术路径也已经明确：

- 采集层以 `requests + Chrome Cookie + 签名接口` 为主
- 浏览器层作为回退与登录补救
- 飞书层以对比更新和多表拆分控制复杂度
- 本地前端承担管理、查看、限频、自检与兜底展示

在这个边界内，当前架构是合理且可持续维护的。
