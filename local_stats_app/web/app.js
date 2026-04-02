const state = {
  payload: null,
  monitoring: null,
  systemConfig: null,
  lastServerPush: null,
  activeAccountId: "",
  rankingScope: "all",
  trendWindow: 7,
  calendarMonth: "",
  calendarSelectedDate: "",
  monitorQuery: "",
  monitorFilter: "all",
  monitorProjectFilter: "all",
  monitorViewMode: "cards",
  collapseStableProjects: true,
  monitorPanelExpanded: false,
  monitorPage: 1,
  monitorPageSize: 30,
  pollTimer: null,
  loginAutoRefreshTriggered: false,
};

const rankingConfigs = [
  { type: "单条点赞排行", title: "点赞排行", metricLabel: "点赞" },
  { type: "单条评论排行", title: "评论排行", metricLabel: "评论" },
  { type: "单条第二天增长排行", title: "次日增长", metricLabel: "增长" },
];

function formatNumber(value) {
  return new Intl.NumberFormat("zh-CN").format(Number(value || 0));
}

function formatDateTime(value) {
  if (!value) return "未更新";
  return String(value).replace("T", " ").slice(0, 19);
}

function getLocalCacheSummary() {
  const payload = state.payload || {};
  const rankings = payload.rankings || {};
  return {
    accountCount: Number((payload.accounts || []).length || 0),
    likeCount: Number((rankings["单条点赞排行"] || []).length || 0),
    commentCount: Number((rankings["单条评论排行"] || []).length || 0),
    growthCount: Number((rankings["单条第二天增长排行"] || []).length || 0),
    latestDate: payload.latest_date || "",
    updatedAt: payload.updated_at || payload.generated_at || "",
    stale: Boolean(payload.stale),
  };
}

function formatPushTarget(url) {
  const value = String(url || "").trim();
  if (!value) return "未设置";
  try {
    const parsed = new URL(value);
    return parsed.port ? `${parsed.hostname}:${parsed.port}` : parsed.hostname;
  } catch (_error) {
    return value;
  }
}

function formatClockTime(value) {
  const text = formatDateTime(value);
  return text === "未更新" ? "" : text.slice(11, 16);
}

function parseTimeMs(value) {
  const text = String(value || "").trim();
  if (!text) return 0;
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const result = Date.parse(normalized);
  return Number.isNaN(result) ? 0 : result;
}

function formatLaunchdPhaseLabel(status = {}) {
  const phase = String(status.phase || "").trim();
  const state = String(status.state || "").trim();
  if (phase === "waiting_login" || status.waiting_for_login) return "等待登录";
  if (phase === "waiting_window") return "等待时间窗口";
  if (phase === "collecting") return "采集中";
  if (phase === "uploading") return "上传中";
  if (phase === "preparing") return "准备中";
  if (phase === "finished" && state === "success") return "今日已完成";
  if (phase === "finished" && state === "partial") return "部分完成";
  if (state === "success") return "今日已完成";
  if (state === "partial") return "部分完成";
  if (state === "skipped") return "已跳过";
  return "待命";
}

function buildLaunchdProgressValue(status = {}) {
  const phaseLabel = formatLaunchdPhaseLabel(status);
  const currentProject = String(status.current_project || "").trim();
  const currentIndex = Number(status.current_project_index || 0);
  const currentTotal = Number(status.current_project_total || status.project_count || 0);
  const progressText = currentIndex > 0 && currentTotal > 0 ? `${currentIndex}/${currentTotal}` : "";
  const pieces = [phaseLabel];
  if (currentProject) pieces.push(currentProject);
  if (progressText) pieces.push(progressText);
  return pieces.join(" · ");
}

function buildLaunchdProgressCopy(status = {}, plan = {}) {
  const pieces = [];
  if (status.message) pieces.push(status.message);
  if (status.current_project_scheduled_at) {
    pieces.push(`当前项目计划 ${formatDateTime(status.current_project_scheduled_at)}`);
  }
  if (status.next_run_at) {
    pieces.push(`下一轮 ${formatDateTime(status.next_run_at)}`);
  } else if (plan.next_run_at) {
    pieces.push(`下一轮 ${formatDateTime(plan.next_run_at)}`);
  }
  if (status.last_upload_success_at) {
    pieces.push(`上次自动上传 ${formatDateTime(status.last_upload_success_at)}`);
  } else if (status.last_success_at) {
    pieces.push(`上次自动采集 ${formatDateTime(status.last_success_at)}`);
  }
  return pieces.filter(Boolean).join(" · ");
}

function buildLaunchdDisplayState(status = {}, plan = {}, lastPush = null) {
  const finishedAtMs = parseTimeMs(status.finished_at || status.updated_at || "");
  const lastPushMs = parseTimeMs(lastPush?.server_updated_at || lastPush?.pushed_at || "");
  const manualPushIsNewer = lastPushMs > 0 && lastPushMs >= finishedAtMs;
  if (String(status.phase || "") === "finished" && String(status.state || "") === "partial" && manualPushIsNewer) {
    return {
      value: "已手动补传",
      copy: [
        lastPush?.server_updated_at ? `服务器确认 ${formatDateTime(lastPush.server_updated_at)}` : "",
        plan.next_run_at ? `下一轮 ${formatDateTime(plan.next_run_at)}` : "",
      ].filter(Boolean).join(" · "),
    };
  }
  return {
    value: buildLaunchdProgressValue(status),
    copy: buildLaunchdProgressCopy(status, plan),
  };
}

function buildLaunchdPlanPreview(plan = {}) {
  const items = Array.isArray(plan.projects) ? plan.projects : [];
  if (!items.length) return "当前没有待排程的项目";
  const preview = items.slice(0, 4).map((item) => {
    const clock = formatClockTime(item.next_run_at);
    const name = String(item.name || "未命名项目").trim();
    const activeCount = Number(item.active_count || 0);
    return `${clock || "--:--"} ${name}${activeCount ? `(${formatNumber(activeCount)}个账号)` : ""}`;
  });
  if (items.length > 4) {
    preview.push(`还有 ${formatNumber(items.length - 4)} 个项目`);
  }
  return preview.join(" · ");
}

function loadLastServerPush() {
  try {
    const raw = window.localStorage.getItem("xhs_last_server_push");
    state.lastServerPush = raw ? JSON.parse(raw) : null;
  } catch (_error) {
    state.lastServerPush = null;
  }
}

function persistLastServerPush(pushPayload) {
  state.lastServerPush = pushPayload || null;
  try {
    if (pushPayload) {
      window.localStorage.setItem("xhs_last_server_push", JSON.stringify(pushPayload));
    } else {
      window.localStorage.removeItem("xhs_last_server_push");
    }
  } catch (_error) {
    // ignore storage failures
  }
}

async function loadSystemConfig() {
  const response = await fetch("/api/system-config");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || "系统配置加载失败");
  }
  state.systemConfig = payload;
  renderSystemConfig();
}

function renderSystemConfig() {
  const payload = state.systemConfig || {};
  const config = payload.config || {};
  const cacheSummary = getLocalCacheSummary();
  const autoPushStatus = state.monitoring?.sync_status?.server_cache_push_status || {};
  const launchdStatus = state.monitoring?.sync_status?.launchd_status || {};
  const schedulePlan = state.monitoring?.sync_status?.schedule_plan || {};
  const scheduleDriver = String(state.monitoring?.sync_status?.schedule_driver || "app").trim().toLowerCase() || "app";
  const cacheStateNode = document.getElementById("systemConfigCacheStatus");
  document.getElementById("configXhsCookie").value = config.XHS_COOKIE || "";
  document.getElementById("configProjectCacheDir").value = config.PROJECT_CACHE_DIR || "";
  document.getElementById("configStateFile").value = config.STATE_FILE || "";
  document.getElementById("configServerCachePushUrl").value = config.SERVER_CACHE_PUSH_URL || "";
  document.getElementById("configServerCacheUploadToken").value = config.SERVER_CACHE_UPLOAD_TOKEN || "";
  document.getElementById("configUrlsText").value = payload.urls_text || "";
  document.getElementById("systemConfigSummary").textContent = payload.env_file
    ? `${payload.env_file}${cacheSummary.updatedAt ? ` · 更新 ${formatDateTime(cacheSummary.updatedAt)}` : ""}`
    : "当前未加载配置";
  if (cacheStateNode) {
    const cacheReady = cacheSummary.accountCount > 0 || cacheSummary.likeCount > 0 || cacheSummary.commentCount > 0;
    const lastPush = state.lastServerPush || null;
    const lastPushTarget = lastPush?.server_url || config.SERVER_CACHE_PUSH_URL || "";
    const lastPushText = lastPush?.server_updated_at
      ? `服务器确认 ${formatDateTime(lastPush.server_updated_at)}`
      : lastPush?.pushed_at
        ? `本机发起 ${formatDateTime(lastPush.pushed_at)}`
        : "还没有成功推送记录";
    const autoPushText = scheduleDriver === "launchd"
      ? buildLaunchdDisplayState(launchdStatus, schedulePlan, lastPush).value
      : autoPushStatus?.last_success_at
        ? `上次自动上传 ${formatDateTime(autoPushStatus.last_success_at)}`
        : autoPushStatus?.next_auto_run_at
          ? `下次自动上传 ${formatDateTime(autoPushStatus.next_auto_run_at)}`
          : "每天 14:00 自动上传到服务器";
    const autoPushCopy = scheduleDriver === "launchd"
      ? buildLaunchdDisplayState(launchdStatus, schedulePlan, lastPush).copy || "launchd 会在 14:00 后一次采集全部项目，成功后再自动上传服务器"
      : autoPushStatus?.state === "error"
        ? `自动上传失败：${autoPushStatus.last_error || autoPushStatus.message || "未知错误"}`
        : autoPushStatus?.state === "running"
          ? autoPushStatus.message || "自动上传进行中"
          : `${formatPushTarget(config.SERVER_CACHE_PUSH_URL || "")} · 每天 ${autoPushStatus.daily_at || "14:00"} 自动上传`;
    const schedulePlanValue = scheduleDriver === "launchd"
      ? buildSchedulePlanSummary(schedulePlan)
      : "";
    const schedulePlanCopy = scheduleDriver === "launchd"
      ? buildLaunchdPlanPreview(schedulePlan)
      : "";
    cacheStateNode.innerHTML = `
      <article class="system-config-status-card">
        <div class="system-config-status-label">本地缓存状态</div>
        <div class="system-config-status-value">${cacheReady ? `${formatNumber(cacheSummary.accountCount)} 个账号` : "暂无缓存"}</div>
        <div class="system-config-status-copy">
          ${cacheReady
            ? `${cacheSummary.updatedAt ? `更新 ${formatDateTime(cacheSummary.updatedAt)} · ` : ""}留底 ${cacheSummary.latestDate || "-"} · 点赞榜 ${formatNumber(cacheSummary.likeCount)} 条 / 评论榜 ${formatNumber(cacheSummary.commentCount)} 条 / 增长榜 ${formatNumber(cacheSummary.growthCount)} 条`
            : "如果你刚更新过本地看板，这里应该会显示账号数和榜单条数。"}
        </div>
      </article>
      <article class="system-config-status-card">
        <div class="system-config-status-label">服务器同步</div>
        <div class="system-config-status-value">${lastPushText}</div>
        <div class="system-config-status-copy">${config.SERVER_CACHE_PUSH_URL ? `${formatPushTarget(lastPushTarget)}${lastPush ? ` · ${formatNumber(lastPush.account_count || 0)} 个账号` : ""}` : "先填写 SERVER_CACHE_PUSH_URL，再推送到服务器。"}
        </div>
      </article>
      <article class="system-config-status-card">
        <div class="system-config-status-label">${scheduleDriver === "launchd" ? "自动任务" : "自动上传"}</div>
        <div class="system-config-status-value">${autoPushText}</div>
        <div class="system-config-status-copy">${autoPushCopy}</div>
      </article>
      ${scheduleDriver === "launchd" ? `
      <article class="system-config-status-card">
        <div class="system-config-status-label">今日计划</div>
        <div class="system-config-status-value">${schedulePlanValue || "等待计划生成"}</div>
        <div class="system-config-status-copy">${schedulePlanCopy || "launchd 已接管定时任务，到点后会直接开始整批采集。"}</div>
      </article>
      ` : ""}
    `;
  }
}

async function saveSystemConfig() {
  const response = await fetch("/api/system-config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      config: {
        XHS_COOKIE: document.getElementById("configXhsCookie").value,
        PROJECT_CACHE_DIR: document.getElementById("configProjectCacheDir").value,
        STATE_FILE: document.getElementById("configStateFile").value,
        SERVER_CACHE_PUSH_URL: document.getElementById("configServerCachePushUrl").value,
        SERVER_CACHE_UPLOAD_TOKEN: document.getElementById("configServerCacheUploadToken").value,
      },
      urls_text: document.getElementById("configUrlsText").value,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || "保存配置失败");
  }
  state.systemConfig = payload;
  renderSystemConfig();
  document.getElementById("systemConfigResult").textContent = "配置已保存，本地缓存未自动推送";
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function pushServerCache() {
  const config = state.systemConfig?.config || {};
  const cacheSummary = getLocalCacheSummary();
  const activeAccount = getActiveAccount();
  if (!String(config.SERVER_CACHE_PUSH_URL || "").trim()) {
    throw new Error("请先填写服务器地址，再推送");
  }
  if (cacheSummary.accountCount <= 0) {
    throw new Error("本地还没有可推送的数据，请先更新本地看板");
  }
  const response = await fetch("/api/server-cache-push", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(activeAccount?.account_id ? { account_id: activeAccount.account_id } : {}),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || "推送服务器失败");
  }
  persistLastServerPush({
    pushed_at: new Date().toISOString(),
    server_updated_at: payload.updated_at || "",
    account_count: payload.account_count || cacheSummary.accountCount || 0,
    server_url: String(config.SERVER_CACHE_PUSH_URL || "").trim(),
  });
  await loadMonitoring();
  renderSystemConfig();
  document.getElementById("systemConfigResult").textContent = activeAccount?.account_id
    ? `已把当前账号推送到服务器 · ${activeAccount.account || activeAccount.account_id}`
    : `已推送到服务器 · ${payload.account_count || cacheSummary.accountCount} 个账号 · 点赞榜 ${formatNumber(cacheSummary.likeCount)} 条 · 评论榜 ${formatNumber(cacheSummary.commentCount)} 条`;
}

function formatScheduleWindow(plan = {}) {
  const start = String(plan.window_start || "").trim();
  const end = String(plan.window_end || "").trim();
  if (!start || !end) return "";
  return `${start}-${end}`;
}

function buildSchedulePlanSummary(plan = {}) {
  if (!plan) return "";
  const nextRun = plan.next_run_at ? formatDateTime(plan.next_run_at) : "";
  const totalAccounts = Number(plan.total_accounts || plan.per_run || 0);
  const projectCount = Number(plan.project_count || 0);
  const pieces = [];
  if (nextRun) pieces.push(`下一次 ${nextRun}`);
  if (totalAccounts) pieces.push(`一次采集 ${formatNumber(totalAccounts)} 个账号`);
  if (projectCount) pieces.push(`${formatNumber(projectCount)} 个项目`);
  return pieces.join(" · ");
}

function formatSignedNumber(value) {
  const number = Number(value || 0);
  if (number > 0) return `+${formatNumber(number)}`;
  if (number < 0) return `-${formatNumber(Math.abs(number))}`;
  return "0";
}

function buildCompareSummaryLine(compare = {}) {
  const pieces = [];
  if (compare.account_count_delta || compare.account_count_delta === 0) {
    pieces.push(`账号 ${formatSignedNumber(compare.account_count_delta)}`);
  }
  if (compare.like_count_delta || compare.like_count_delta === 0) {
    pieces.push(`点赞榜 ${formatSignedNumber(compare.like_count_delta)}`);
  }
  if (compare.comment_count_delta || compare.comment_count_delta === 0) {
    pieces.push(`评论榜 ${formatSignedNumber(compare.comment_count_delta)}`);
  }
  return pieces.join(" · ");
}

function formatDurationShort(seconds) {
  const totalSeconds = Math.max(0, Number(seconds || 0));
  if (totalSeconds < 60) return `${Math.ceil(totalSeconds)}秒`;
  const minutes = Math.floor(totalSeconds / 60);
  const remainSeconds = Math.ceil(totalSeconds % 60);
  if (minutes < 60) {
    return remainSeconds ? `${minutes}分${remainSeconds}秒` : `${minutes}分`;
  }
  const hours = Math.floor(minutes / 60);
  const remainMinutes = minutes % 60;
  return remainMinutes ? `${hours}小时${remainMinutes}分` : `${hours}小时`;
}

function getProjectSyncStateText(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "running") return "同步中";
  if (normalized === "success") return "最近成功";
  if (normalized === "error") return "最近失败";
  return "暂无记录";
}

function getProjectSyncBadgeClass(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "running") return "is-running";
  if (normalized === "success") return "is-success";
  if (normalized === "error") return "is-error";
  return "";
}

function truncateMiddle(value, maxLength = 64) {
  const text = String(value || "");
  if (text.length <= maxLength) return text;
  const keep = Math.max(12, Math.floor((maxLength - 3) / 2));
  return `${text.slice(0, keep)}...${text.slice(-keep)}`;
}

function getLoginStateText(status) {
  const normalized = String(status || "idle");
  if (normalized === "ok") return "正常";
  if (normalized === "warning") return "关注";
  if (normalized === "error") return "异常";
  if (normalized === "checking") return "自检中";
  return "待命";
}

function getFetchStateText(status) {
  const normalized = String(status || "checking");
  if (normalized === "ok") return "已识别";
  if (normalized === "warning") return "关注";
  if (normalized === "error") return "异常";
  return "识别中";
}

function buildActionLink(url, label) {
  if (!url) return "";
  return `<a class="action-link" href="${url}" target="_blank" rel="noreferrer">${label}</a>`;
}

function getPreferredItemUrl(item) {
  if (!item) return "";
  return item.note_url || item.profile_url || "";
}

function buildCoverSrc(url) {
  if (!url) return "";
  return `/api/image?url=${encodeURIComponent(url)}`;
}

function buildWindowGrowth(fullSeries, requestedWindow = state.trendWindow) {
  if (!fullSeries.length || fullSeries.length < 2) {
    return null;
  }
  const latest = fullSeries[fullSeries.length - 1];
  let baseline = null;
  let label = "";

  if (requestedWindow <= 1) {
    baseline = fullSeries[fullSeries.length - 2];
    label = "较前1天";
  } else {
    const windowSize = Math.max(2, Math.min(requestedWindow, fullSeries.length));
    baseline = fullSeries[fullSeries.length - windowSize];
    label = fullSeries.length < requestedWindow ? `已积累${windowSize}天` : `近${requestedWindow}天`;
  }

  if (!baseline || baseline.date === latest.date) {
    return null;
  }
  return {
    label,
    start_date: baseline.date,
    end_date: latest.date,
    fans: Number(latest.fans || 0) - Number(baseline.fans || 0),
    interaction: Number(latest.interaction || 0) - Number(baseline.interaction || 0),
    likes: Number(latest.likes || 0) - Number(baseline.likes || 0),
    comments: Number(latest.comments || 0) - Number(baseline.comments || 0),
    works: Number(latest.works || 0) - Number(baseline.works || 0),
  };
}

function buildSeriesWindowContext(fullSeries, requestedWindow = state.trendWindow) {
  if (!fullSeries.length || fullSeries.length < 2) {
    return null;
  }
  const latest = fullSeries[fullSeries.length - 1];
  let baseline = null;
  let label = "";

  if (requestedWindow <= 1) {
    baseline = fullSeries[fullSeries.length - 2];
    label = "较前1天";
  } else {
    const windowSize = Math.max(2, Math.min(requestedWindow, fullSeries.length));
    baseline = fullSeries[fullSeries.length - windowSize];
    label = fullSeries.length < requestedWindow ? `已积累${windowSize}天` : `近${requestedWindow}天`;
  }
  if (!baseline || baseline.date === latest.date) {
    return null;
  }
  return {
    label,
    start_date: baseline.date,
    end_date: latest.date,
  };
}

function buildProjectComparableGrowth(projectName = getSelectedProjectName(), requestedWindow = 7) {
  const accountSeries = state.payload?.account_series || {};
  const fullSeries = getProjectSeries(projectName);
  const windowContext = buildSeriesWindowContext(fullSeries, requestedWindow);
  if (!windowContext) {
    return null;
  }

  const projectAccountIds =
    projectName === "all"
      ? new Set(Object.keys(accountSeries))
      : getProjectAccountIds(projectName);

  const latestByAccount = new Map();
  const baselineByAccount = new Map();
  projectAccountIds.forEach((accountId) => {
    const series = accountSeries[accountId] || [];
    const latestPoint = series.find((item) => String(item.date || "") === windowContext.end_date);
    const baselinePoint = series.find((item) => String(item.date || "") === windowContext.start_date);
    if (latestPoint) latestByAccount.set(accountId, latestPoint);
    if (baselinePoint) baselineByAccount.set(accountId, baselinePoint);
  });

  const comparableAccountIds = [...latestByAccount.keys()].filter((accountId) => baselineByAccount.has(accountId));
  const newAccountIds = [...latestByAccount.keys()].filter((accountId) => !baselineByAccount.has(accountId));
  const lostAccountIds = [...baselineByAccount.keys()].filter((accountId) => !latestByAccount.has(accountId));

  const sumMetric = (collection, key) =>
    comparableAccountIds.reduce((sum, accountId) => sum + Number((collection.get(accountId) || {})[key] || 0), 0);

  const latestFans = sumMetric(latestByAccount, "fans");
  const baselineFans = sumMetric(baselineByAccount, "fans");
  const latestInteraction = sumMetric(latestByAccount, "interaction");
  const baselineInteraction = sumMetric(baselineByAccount, "interaction");
  const latestLikes = sumMetric(latestByAccount, "likes");
  const baselineLikes = sumMetric(baselineByAccount, "likes");
  const latestComments = sumMetric(latestByAccount, "comments");
  const baselineComments = sumMetric(baselineByAccount, "comments");
  const latestWorks = sumMetric(latestByAccount, "works");
  const baselineWorks = sumMetric(baselineByAccount, "works");

  return {
    ...windowContext,
    comparable_ready: comparableAccountIds.length > 0,
    comparable_account_count: comparableAccountIds.length,
    latest_account_count: latestByAccount.size,
    baseline_account_count: baselineByAccount.size,
    new_account_count: newAccountIds.length,
    lost_account_count: lostAccountIds.length,
    fans: latestFans - baselineFans,
    interaction: latestInteraction - baselineInteraction,
    likes: latestLikes - baselineLikes,
    comments: latestComments - baselineComments,
    works: latestWorks - baselineWorks,
  };
}

function buildCoverMarkup(item, { size = "hero", rank = 1 } = {}) {
  const wrapperClass = size === "hero" ? "ranking-hero-cover" : "ranking-mini-cover";
  const imageClass = size === "hero" ? "ranking-hero-cover-image" : "ranking-mini-cover-image";
  const targetUrl = getPreferredItemUrl(item);
  const openTag = targetUrl
    ? `<a class="${wrapperClass}" href="${targetUrl}" target="_blank" rel="noreferrer">`
    : `<div class="${wrapperClass}">`;
  const closeTag = targetUrl ? "</a>" : "</div>";
  const imageMarkup = item.cover_url
    ? `<img class="${imageClass}" src="${buildCoverSrc(item.cover_url)}" alt="作品封面" loading="lazy" referrerpolicy="no-referrer" data-origin-src="${item.cover_url}" onerror="if(this.dataset.retryDirect!=='1' && this.dataset.originSrc){this.dataset.retryDirect='1';this.src=this.dataset.originSrc;return;} this.hidden=true;this.nextElementSibling.hidden=false;" />`
    : "";
  const placeholderMarkup = `<div class="ranking-cover-placeholder"${item.cover_url ? " hidden" : ""}>暂无封面</div>`;
  const rankMarkup = rank === "" || rank === null || rank === undefined ? "" : `<div class="rank-badge">${rank}</div>`;
  return `
    ${openTag}
      ${imageMarkup}
      ${placeholderMarkup}
      ${rankMarkup}
    ${closeTag}
  `;
}

function renderCommentBasisChip(item) {
  const basis = String(item?.comment_basis || "").trim();
  if (!basis) return "";
  const label = basis === "评论预览下限" ? "评论下限" : basis === "旧缓存" ? "旧缓存" : basis === "详情缺失" ? "详情缺失" : basis;
  return `<span class="ranking-basis-chip">${label}</span>`;
}

function buildCommentBasisHint(item) {
  const basis = String(item?.comment_basis || "").trim();
  if (basis === "详情缺失") {
    return "这条作品没有拿到精确评论数，因此不计入评论榜和评论总量";
  }
  if (basis === "评论预览下限") {
    return "该评论数来自评论预览下限，不是精确总数";
  }
  if (basis === "旧缓存") {
    return "该评论数来自上次成功采集的旧缓存，本轮未刷新到新值";
  }
  return "";
}

async function loadDashboard(force = false) {
  const query = force ? "?refresh=1" : "";
  const response = await fetch(`/api/dashboard${query}`);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  state.payload = await response.json();
  ensureActiveAccount();
  renderApp();
}

async function loadMonitoring() {
  const response = await fetch("/api/monitored-accounts");
  if (!response.ok) {
    throw new Error(`监测账号加载失败: ${response.status}`);
  }
  state.monitoring = await response.json();
  const loginState = state.monitoring?.login_state || {};
  if (loginState.state === "ok") {
    state.loginAutoRefreshTriggered = false;
  }
  ensureProjectSelection();
  ensureActiveAccount();
  renderMonitoring();
  renderApp();
  schedulePolling();
  if (
    !state.loginAutoRefreshTriggered
    && !loginState.checking
    && loginState.state === "warning"
    && loginState.detail_ready
    && Number(loginState.comment_count_ready || 0) <= 0
  ) {
    state.loginAutoRefreshTriggered = true;
    checkLoginState({ silent: true }).catch((error) => {
      document.getElementById("addResult").textContent = error.message;
    });
  }
}

function schedulePolling() {
  if (state.pollTimer) {
    clearTimeout(state.pollTimer);
    state.pollTimer = null;
  }
  const syncStatus = state.monitoring?.sync_status || {};
  const loginState = state.monitoring?.login_state || {};
  if (syncStatus.state !== "running" && !syncStatus.pending && !loginState.checking) {
    return;
  }
  state.pollTimer = setTimeout(async () => {
    try {
      await Promise.all([loadMonitoring(), loadDashboard(true)]);
    } catch (error) {
      document.getElementById("addResult").textContent = error.message;
    }
  }, 2000);
}

function renderApp() {
  if (state.systemConfig) {
    renderSystemConfig();
  }
  renderMeta();
  renderOperationsHub();
  renderProjectHome();
  renderProjectCalendar();
  renderAccountFocus();
  renderRankingScopeTabs();
  renderRankingList();
  renderAlerts();
}

function renderOperationsHub() {
  const root = document.getElementById("operationsHub");
  const summaryNode = document.getElementById("operationsSummary");
  if (!root || !summaryNode) return;
  const projectName = getSelectedProjectName();
  const selectedProject = (state.monitoring?.projects || []).find((item) => item.name === projectName) || null;
  const syncStatus = state.monitoring?.sync_status || {};
  const serverPushStatus = syncStatus.server_cache_push_status || {};
  const loginState = state.monitoring?.login_state || {};
  const projectSync = selectedProject?.sync_status || {};
  const manualState = getManualUpdateState(syncStatus);
  const isAllProjects = projectName === "all";
  const projectLabel = isAllProjects ? "全部项目" : projectName;
  const loginLabel =
    loginState.state === "ok"
      ? "登录态正常"
      : loginState.state === "warning"
        ? "详情能力受限"
        : loginState.state === "checking"
          ? "正在自检"
          : loginState.state === "error"
            ? "登录态异常"
            : "等待自检";
  const uploadLabel =
    serverPushStatus.state === "running"
      ? "正在推送服务器"
      : serverPushStatus.state === "waiting_sync"
        ? "采集成功后自动推送"
        : serverPushStatus.state === "waiting_login"
          ? "等待登录后继续自动任务"
          : serverPushStatus.last_error
            ? "服务器上传失败，等待重试"
            : serverPushStatus.last_success_at
              ? `最近上传 ${formatDateTime(serverPushStatus.last_success_at)}`
              : "按计划上传服务器";
  const syncLabel =
    syncStatus.state === "running"
      ? "本地采集中"
      : manualState.cooldownSeconds > 0
        ? `采集冷却 ${formatDurationShort(manualState.cooldownSeconds)}`
        : "可更新本地看板";
  const projectResult =
    projectSync.total_accounts
      ? `${formatNumber(projectSync.total_accounts)} 账号 / ${formatNumber(projectSync.total_works || 0)} 作品`
      : (selectedProject ? `${formatNumber(selectedProject.active_count || selectedProject.total || 0)} 个账号` : "等待项目数据");
  const nextAction =
    loginState.state === "error"
      ? "先点立即自检并完成网页登录"
      : syncStatus.state === "running"
        ? "等待本地看板采集完成"
        : serverPushStatus.state === "running"
          ? "等待服务器上传完成"
          : isAllProjects
            ? "先选一个项目，再更新看板"
            : "建议先更新本地看板，必要时再推送到服务器";

  summaryNode.textContent = `${projectLabel} · ${syncLabel} · ${uploadLabel}`;
  root.innerHTML = `
    <article class="operations-card is-focus">
      <div class="operations-card-label">当前项目</div>
      <div class="operations-card-title">${projectLabel}</div>
      <div class="operations-card-copy">${projectResult}</div>
      <div class="operations-chip-row">
        <span class="operations-chip">${syncLabel}</span>
        <span class="operations-chip">${uploadLabel}</span>
      </div>
    </article>
    <article class="operations-card">
      <div class="operations-card-label">推荐下一步</div>
      <div class="operations-card-title">今日动作</div>
      <div class="operations-card-copy">${nextAction}</div>
      <div class="operations-list">
        <span>1. 更新本地看板</span>
        <span>2. 检查项目概况与榜单</span>
        <span>3. 需要共享时推送到服务器</span>
      </div>
    </article>
    <article class="operations-card">
      <div class="operations-card-label">系统状态</div>
      <div class="operations-card-title">${loginLabel}</div>
      <div class="operations-card-copy">${loginState.message || "优先用自检判断是否需要重新登录。"}</div>
      <div class="operations-chip-row">
        ${loginState.checked_at ? `<span class="operations-chip">上次自检 ${formatDateTime(loginState.checked_at)}</span>` : ""}
        ${serverPushStatus.next_auto_run_at ? `<span class="operations-chip">下次计划 ${formatDateTime(serverPushStatus.next_auto_run_at)}</span>` : ""}
      </div>
    </article>
  `;
}

function ensureActiveAccount() {
  const accounts = getVisibleAccounts();
  if (!accounts.length) {
    state.activeAccountId = "";
    return;
  }
  const exists = accounts.some((item) => item.account_id === state.activeAccountId);
  if (state.activeAccountId && !exists) {
    state.activeAccountId = "";
  }
}

function getActiveAccount() {
  const accounts = getVisibleAccounts();
  if (!state.activeAccountId) return null;
  return accounts.find((item) => item.account_id === state.activeAccountId) || null;
}

function getActiveSeries() {
  const active = getActiveAccount();
  if (!active) return [];
  return (state.payload?.account_series || {})[active.account_id] || [];
}

function getMonitoringEntries() {
  return state.monitoring?.entries || [];
}

function getDashboardAccounts() {
  const accounts = state.payload?.accounts || [];
  const monitoringEntries = getMonitoringEntries();
  if (!accounts.length || !monitoringEntries.length) {
    return accounts;
  }
  const byAccountId = new Map();
  const byProfileUrl = new Map();
  monitoringEntries.forEach((entry) => {
    if (entry.account_id) byAccountId.set(entry.account_id, entry);
    if (entry.profile_url) byProfileUrl.set(entry.profile_url, entry);
    if (entry.url) byProfileUrl.set(entry.url, entry);
  });
  return accounts.map((item) => {
    if (item.project) return item;
    const matchedEntry = byAccountId.get(item.account_id) || byProfileUrl.get(item.profile_url || "");
    return matchedEntry?.project ? { ...item, project: matchedEntry.project } : item;
  });
}

function getSelectedProjectName() {
  return state.monitorProjectFilter || "all";
}

function ensureProjectSelection() {
  if (state.monitorProjectFilter !== "all") return;
  const projects = state.monitoring?.projects || [];
  const preferred = projects.find((item) => Number(item.active || item.total || 0) > 0) || projects[0];
  if (preferred?.name) {
    state.monitorProjectFilter = preferred.name;
  }
}

function getProjectEntries(projectName = getSelectedProjectName()) {
  const entries = getMonitoringEntries();
  if (projectName === "all") {
    return entries;
  }
  return entries.filter((entry) => entry.project === projectName);
}

function getProjectAccountIds(projectName = getSelectedProjectName()) {
  return new Set(
    getProjectEntries(projectName)
      .map((entry) => entry.account_id)
      .filter(Boolean),
  );
}

function getProjectStatus(projectName = getSelectedProjectName()) {
  const projects = state.monitoring?.projects || [];
  if (projectName === "all") {
    return state.monitoring?.sync_status || {};
  }
  return projects.find((item) => item.name === projectName)?.sync_status || {};
}

function getVisibleAccounts() {
  const accounts = getDashboardAccounts();
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    return accounts;
  }
  const accountIds = getProjectAccountIds(projectName);
  return accounts.filter((item) => accountIds.has(item.account_id));
}

function getVisibleAlerts() {
  const allAlerts = state.payload?.alerts || [];
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    return allAlerts;
  }
  const projectAccountIds = getProjectAccountIds(projectName);
  return allAlerts.filter((item) => projectAccountIds.has(item.account_id));
}

function getProjectSeries(projectName = getSelectedProjectName()) {
  const accountSeries = state.payload?.account_series || {};
  const grouped = new Map();
  const accountIds =
    projectName === "all"
      ? new Set(Object.keys(accountSeries))
      : getProjectAccountIds(projectName);
  accountIds.forEach((accountId) => {
    const series = accountSeries[accountId] || [];
    series.forEach((point) => {
      const date = String(point.date || "");
      if (!date) return;
      if (!grouped.has(date)) {
        grouped.set(date, {
          date,
          fans: 0,
          interaction: 0,
          likes: 0,
          comments: 0,
          works: 0,
        });
      }
      const bucket = grouped.get(date);
      bucket.fans += Number(point.fans || 0);
      bucket.interaction += Number(point.interaction || 0);
      bucket.likes += Number(point.likes || 0);
      bucket.comments += Number(point.comments || 0);
      bucket.works += Number(point.works || 0);
    });
  });
  return Array.from(grouped.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function monthKeyFromDate(dateText) {
  return String(dateText || "").slice(0, 7);
}

function formatCalendarMonth(monthKey) {
  if (!monthKey || !monthKey.includes("-")) return "暂无历史";
  const [year, month] = monthKey.split("-");
  return `${year}年${month}月`;
}

function getMonthDateRange(monthKey) {
  if (!monthKey || !monthKey.includes("-")) return null;
  const [yearText, monthText] = monthKey.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  if (!year || !month) return null;
  const firstDay = new Date(year, month - 1, 1);
  const lastDay = new Date(year, month, 0);
  return { firstDay, lastDay };
}

function toIsoDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function buildProjectCalendarState(projectName = getSelectedProjectName()) {
  const accountSeries = state.payload?.account_series || {};
  const accountCards = new Map(getDashboardAccounts().map((item) => [item.account_id, item]));
  const accountIds = projectName === "all" ? new Set(Object.keys(accountSeries)) : getProjectAccountIds(projectName);
  const grouped = new Map();

  accountIds.forEach((accountId) => {
    const series = accountSeries[accountId] || [];
    const card = accountCards.get(accountId) || {};
    series.forEach((point) => {
      const date = String(point.date || "").trim();
      if (!date) return;
      if (!grouped.has(date)) {
        grouped.set(date, {
          date,
          fans: 0,
          interaction: 0,
          likes: 0,
          comments: 0,
          works: 0,
          accounts: [],
        });
      }
      const bucket = grouped.get(date);
      bucket.fans += Number(point.fans || 0);
      bucket.interaction += Number(point.interaction || 0);
      bucket.likes += Number(point.likes || 0);
      bucket.comments += Number(point.comments || 0);
      bucket.works += Number(point.works || 0);
      bucket.accounts.push({
        account_id: accountId,
        account: card.account || card.display_name || accountId,
        profile_url: card.profile_url || "",
        fans: Number(point.fans || 0),
        interaction: Number(point.interaction || 0),
        likes: Number(point.likes || 0),
        comments: Number(point.comments || 0),
        works: Number(point.works || 0),
      });
    });
  });

  const dates = Array.from(grouped.keys()).sort();
  const months = Array.from(new Set(dates.map(monthKeyFromDate))).sort();
  if (!months.length) {
    state.calendarMonth = "";
    state.calendarSelectedDate = "";
    return { months: [], cells: [], selectedDay: null, summary: "", availableDates: [] };
  }

  if (!months.includes(state.calendarMonth)) {
    state.calendarMonth = months[months.length - 1];
  }

  const activeMonthDates = dates.filter((date) => monthKeyFromDate(date) === state.calendarMonth);
  if (!activeMonthDates.length) {
    state.calendarMonth = months[months.length - 1];
  }
  const monthDates = dates.filter((date) => monthKeyFromDate(date) === state.calendarMonth);

  if (!monthDates.includes(state.calendarSelectedDate)) {
    state.calendarSelectedDate = monthDates[monthDates.length - 1] || "";
  }

  const range = getMonthDateRange(state.calendarMonth);
  if (!range) {
    return { months, cells: [], selectedDay: null, summary: "", availableDates: dates };
  }

  const firstWeekday = (range.firstDay.getDay() + 6) % 7;
  const totalDays = range.lastDay.getDate();
  const cells = [];
  for (let index = 0; index < firstWeekday; index += 1) {
    cells.push({ empty: true, key: `empty-start-${index}` });
  }
  for (let day = 1; day <= totalDays; day += 1) {
    const currentDate = new Date(range.firstDay.getFullYear(), range.firstDay.getMonth(), day);
    const dateKey = toIsoDate(currentDate);
    const bucket = grouped.get(dateKey);
    cells.push({
      empty: false,
      date: dateKey,
      day,
      hasData: Boolean(bucket),
      bucket: bucket || null,
      isSelected: state.calendarSelectedDate === dateKey,
    });
  }
  while (cells.length % 7 !== 0) {
    cells.push({ empty: true, key: `empty-end-${cells.length}` });
  }

  const selectedDay = grouped.get(state.calendarSelectedDate) || null;
  const summary = monthDates.length
    ? `${formatCalendarMonth(state.calendarMonth)} 共 ${formatNumber(monthDates.length)} 天有留底，最近留底 ${monthDates[monthDates.length - 1]}`
    : `${formatCalendarMonth(state.calendarMonth)} 暂无留底`;

  if (selectedDay) {
    selectedDay.accounts.sort((left, right) => (right.interaction - left.interaction) || (right.likes - left.likes) || (right.comments - left.comments));
  }

  return {
    months,
    cells,
    selectedDay,
    summary,
    availableDates: dates,
  };
}

function renderProjectCalendar() {
  const titleNode = document.getElementById("projectCalendarTitle");
  const monthNode = document.getElementById("projectCalendarMonth");
  const summaryNode = document.getElementById("projectCalendarSummary");
  const gridNode = document.getElementById("projectCalendar");
  const detailNode = document.getElementById("projectCalendarDetail");
  const prevButton = document.getElementById("calendarPrevMonth");
  const nextButton = document.getElementById("calendarNextMonth");
  const projectName = getSelectedProjectName();

  if (projectName === "all") {
    titleNode.textContent = "项目日历";
    monthNode.textContent = "请选择项目";
    summaryNode.textContent = "进入单个项目后，可按日历查看历史留底。";
    gridNode.innerHTML = `<div class="empty-state">请选择单个项目后查看历史日历。</div>`;
    detailNode.innerHTML = "";
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  const calendarState = buildProjectCalendarState(projectName);
  titleNode.textContent = `${projectName} · 历史日历`;
  monthNode.textContent = formatCalendarMonth(state.calendarMonth);
  summaryNode.textContent = `${calendarState.summary} · 每天展示该项目账号留底总量，可点日期看当天明细。`;

  const monthIndex = calendarState.months.indexOf(state.calendarMonth);
  prevButton.disabled = monthIndex <= 0;
  nextButton.disabled = monthIndex === -1 || monthIndex >= calendarState.months.length - 1;

  if (!calendarState.cells.length) {
    gridNode.innerHTML = `<div class="empty-state">当前项目还没有历史留底。</div>`;
    detailNode.innerHTML = "";
    return;
  }

  const weekLabels = ["一", "二", "三", "四", "五", "六", "日"];
  gridNode.innerHTML = `
    <div class="calendar-weekdays">
      ${weekLabels.map((label) => `<div class="calendar-weekday">${label}</div>`).join("")}
    </div>
    <div class="calendar-cells">
      ${calendarState.cells
        .map((cell) => {
          if (cell.empty) {
            return `<div class="calendar-cell is-empty"></div>`;
          }
          const bucket = cell.bucket || {};
          return `
            <button class="calendar-cell ${cell.hasData ? "has-data" : "is-empty-day"} ${cell.isSelected ? "is-selected" : ""}" data-calendar-date="${cell.date}" type="button">
              <div class="calendar-cell-day">${cell.day}</div>
              ${
                cell.hasData
                  ? `
                <div class="calendar-cell-metrics">
                  <span>粉 ${formatNumber(bucket.fans)}</span>
                  <span>赞 ${formatNumber(bucket.likes)}</span>
                  <span>评 ${formatNumber(bucket.comments)}</span>
                </div>
                <div class="calendar-cell-meta">${formatNumber((bucket.accounts || []).length)} 个账号</div>
              `
                  : `<div class="calendar-cell-empty">无采集</div>`
              }
            </button>
          `;
        })
        .join("")}
    </div>
  `;

  gridNode.querySelectorAll("[data-calendar-date]").forEach((button) => {
    button.addEventListener("click", () => {
      state.calendarSelectedDate = button.dataset.calendarDate || "";
      renderProjectCalendar();
    });
  });

  const selected = calendarState.selectedDay;
  if (!selected) {
    detailNode.innerHTML = "";
    return;
  }

  detailNode.innerHTML = "";
}

function getCalendarRankingSnapshot(projectName, dateText) {
  const history = state.payload?.history_rankings || {};
  if (!dateText) return null;
  const projectHistory = history?.[projectName];
  if (projectHistory && typeof projectHistory === "object" && !Array.isArray(projectHistory)) {
    const snapshot = projectHistory[dateText];
    if (snapshot && typeof snapshot === "object") {
      return snapshot;
    }
  }
  const directSnapshot = history?.[dateText];
  if (directSnapshot && typeof directSnapshot === "object") {
    return directSnapshot;
  }
  return null;
}

function getSelectedRankingSource(projectName = getSelectedProjectName()) {
  const selectedDate = String(state.calendarSelectedDate || "").trim();
  const latestDate = String(state.payload?.latest_date || "").trim();
  const snapshot = selectedDate ? getCalendarRankingSnapshot(projectName, selectedDate) : null;
  const fallbackToCurrent = !snapshot && Boolean(selectedDate) && selectedDate === latestDate;
  const resolvedSnapshot = snapshot || {
    snapshot_time: String(state.payload?.updated_at || state.payload?.generated_at || "").trim(),
    likes: state.payload?.rankings?.["单条点赞排行"] || [],
    comments: state.payload?.rankings?.["单条评论排行"] || [],
    growth: state.payload?.rankings?.["单条第二天增长排行"] || [],
  };
  return {
    date: selectedDate || latestDate,
    snapshot: resolvedSnapshot,
    fallbackToCurrent,
  };
}

function buildCalendarRankingColumn(title, rows, metricLabel) {
  if (!rows.length) {
    return `
      <section class="ranking-column">
        <div class="ranking-column-header">
          <h3>${title}</h3>
          <span class="ranking-column-meta">暂无</span>
        </div>
        <div class="empty-state">当前没有可显示的数据</div>
      </section>
    `;
  }
  return `
    <section class="ranking-column">
      <div class="ranking-column-header">
        <h3>${title}</h3>
        <span class="ranking-column-meta">Top ${formatNumber(rows.length)}</span>
      </div>
      <div class="ranking-mini-list">
        ${rows.map((item, index) => renderRankingMiniItem({ metricLabel }, item, index + 1)).join("")}
      </div>
    </section>
  `;
}

function getProjectTopContentRows(limit = 5) {
  const rankings = state.payload?.rankings || {};
  const sourceRows = rankings["单条点赞排行"] || [];
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    return sourceRows.slice(0, limit);
  }
  const projectAccountIds = getProjectAccountIds(projectName);
  return sourceRows.filter((item) => projectAccountIds.has(item.account_id)).slice(0, limit);
}

function getProjectRankingCount(projectName, rankType) {
  const rows = state.payload?.rankings?.[rankType] || [];
  if (!projectName || projectName === "all") {
    return rows.length;
  }
  const projectAccountIds = getProjectAccountIds(projectName);
  return rows.filter((item) => projectAccountIds.has(item.account_id)).length;
}

function getProjectRankingRows(projectName, rankType) {
  const rows = state.payload?.rankings?.[rankType] || [];
  if (!projectName || projectName === "all") {
    return rows;
  }
  const projectAccountIds = getProjectAccountIds(projectName);
  return rows.filter((item) => projectAccountIds.has(item.account_id));
}

function getProjectCommentSummary(projectName) {
  const rows = getProjectRankingRows(projectName, "单条评论排行");
  const exactRows = rows.filter((item) => String(item.comment_basis || "") === "精确值");
  const nonExactCount = rows.length - exactRows.length;
  const exactTotal = exactRows.reduce((sum, item) => sum + Number(item.metric || 0), 0);
  return {
    rowCount: rows.length,
    exactCount: exactRows.length,
    nonExactCount,
    exactTotal,
  };
}

function getProjectAlertCount(projectName) {
  if (!projectName || projectName === "all") {
    return (state.payload?.alerts || []).length;
  }
  const projectAccountIds = getProjectAccountIds(projectName);
  return (state.payload?.alerts || []).filter((item) => projectAccountIds.has(item.account_id)).length;
}

function getProjectHealth(project) {
  const projectName = String(project?.name || "");
  const syncStatus = project?.sync_status || {};
  const alertCount = getProjectAlertCount(projectName);
  const syncedAccounts = getProjectEntries(projectName).filter((entry) => entry.account_id).length;
  if (String(syncStatus.state || "") === "error") {
    return { level: "risk", label: "高风险", order: 0, alertCount, syncedAccounts };
  }
  if (alertCount > 0) {
    return { level: "warning", label: "预警中", order: 1, alertCount, syncedAccounts };
  }
  if (String(syncStatus.state || "") === "running") {
    return { level: "running", label: "同步中", order: 2, alertCount, syncedAccounts };
  }
  if (syncedAccounts === 0) {
    return { level: "idle", label: "待补数", order: 3, alertCount, syncedAccounts };
  }
  return { level: "healthy", label: "稳定", order: 4, alertCount, syncedAccounts };
}

function getProjectFailureStage(projectSync = {}) {
  const message = String(projectSync.message || "").trim();
  const lastError = String(projectSync.last_error || "").trim();
  const text = `${message} ${lastError}`;
  if (!text) return "";
  if (text.includes("登录态") || text.includes("登录页")) return "登录态异常";
  if (text.includes("抓取失败") || text.includes("没有成功账号")) return "抓取失败";
  if (text.includes("网络") || text.includes("Connection aborted") || text.includes("RemoteDisconnected") || text.includes("timed out")) return "网络异常";
  return "同步失败";
}

function getProjectFailureTime(projectSync = {}) {
  return projectSync.finished_at || projectSync.updated_at || "";
}

function getScopedRankingRows(allRows, active, rankingScope = state.rankingScope, projectName = getSelectedProjectName()) {
  const projectAccountIds = getProjectAccountIds(projectName);
  if (rankingScope === "all") {
    return projectName === "all"
      ? allRows
      : allRows.filter((item) => projectAccountIds.has(item.account_id));
  }
  if (active) {
    return allRows.filter((item) => item.account_id === active.account_id);
  }
  return [];
}

function buildMonitorSearchText(entry) {
  return [
    entry.project,
    entry.display_name,
    entry.account,
    entry.account_id,
    entry.url,
    entry.summary_text,
  ]
    .join(" ")
    .toLowerCase();
}

function getFilteredMonitoringEntries() {
  const query = String(state.monitorQuery || "").trim().toLowerCase();
  const filter = state.monitorFilter || "all";
  const projectFilter = state.monitorProjectFilter || "all";
  return getMonitoringEntries().filter((entry) => {
    if (projectFilter !== "all" && entry.project !== projectFilter) return false;
    if (filter === "active" && !entry.active) return false;
    if (filter === "paused" && entry.active) return false;
    if (query && !buildMonitorSearchText(entry).includes(query)) return false;
    return true;
  });
}

function renderMonitorProjectFilterOptions(projects) {
  const select = document.getElementById("monitorProjectFilter");
  const options = [{ name: "all", label: "全部项目" }].concat(
    projects.map((project) => ({ name: project.name, label: `${project.name} (${formatNumber(project.total)})` })),
  );
  select.innerHTML = options
    .map((option) => `<option value="${option.name}">${option.label}</option>`)
    .join("");
  const exists = options.some((option) => option.name === state.monitorProjectFilter);
  if (!exists) {
    state.monitorProjectFilter = "all";
  }
  select.value = state.monitorProjectFilter;
}

function getMonitorPageData(entries) {
  const pageSize = Math.max(15, Number(state.monitorPageSize || 30));
  const total = entries.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.min(Math.max(1, state.monitorPage || 1), totalPages);
  const startIndex = total === 0 ? 0 : (currentPage - 1) * pageSize;
  const endIndex = Math.min(startIndex + pageSize, total);
  state.monitorPage = currentPage;
  return {
    pageSize,
    total,
    totalPages,
    currentPage,
    startIndex,
    endIndex,
    items: entries.slice(startIndex, endIndex),
  };
}

function resetMonitoringPage() {
  state.monitorPage = 1;
}

function renderAccountFocus() {
  const projectRoot = document.getElementById("projectFilterBar");
  const root = document.getElementById("accountFilterBar");
  const exportButton = document.getElementById("exportAccountRankingButton");
  const exportProjectButton = document.getElementById("exportProjectRankingButton");
  if (!root || !projectRoot) {
    return;
  }
  const projects = state.monitoring?.projects || [];
  const projectName = getSelectedProjectName();
  if (exportButton) {
    exportButton.disabled = projectName === "all" || !getActiveAccount();
  }
  if (exportProjectButton) {
    exportProjectButton.disabled = projectName === "all" || !getVisibleAccounts().length;
  }
  projectRoot.innerHTML = `
    <button class="account-filter-button ${projectName === "all" ? "is-active" : ""}" data-project-name="all">
      全部项目
    </button>
    ${projects
      .map(
        (project) => `
          <button class="account-filter-button ${project.name === projectName ? "is-active" : ""}" data-project-name="${project.name}">
            ${project.name}
          </button>
        `,
      )
      .join("")}
  `;
  projectRoot.querySelectorAll(".account-filter-button").forEach((button) => {
    button.addEventListener("click", () => {
      const nextProject = button.dataset.projectName || "all";
      state.monitorProjectFilter = nextProject;
      state.activeAccountId = "";
      state.rankingScope = "all";
      resetMonitoringPage();
      ensureActiveAccount();
      renderMonitoring();
      renderApp();
    });
  });
  if (projectName === "all") {
    root.innerHTML = "";
    root.hidden = true;
    return;
  }
  root.hidden = false;
  const accounts = getVisibleAccounts();
  const active = getActiveAccount();
  if (!accounts.length || !active) {
    root.innerHTML = `
      <button class="account-filter-button is-active" data-account-id="">
        ${projectName} 总览
      </button>
      ${
        accounts
          .map(
            (item) => `
              <button class="account-filter-button" data-account-id="${item.account_id}">
                ${item.account}
              </button>
            `,
          )
          .join("")
      }
    `;
    root.querySelectorAll(".account-filter-button").forEach((button) => {
      button.addEventListener("click", () => {
        state.activeAccountId = button.dataset.accountId || "";
        state.rankingScope = state.activeAccountId ? "account" : "all";
        renderApp();
      });
    });
    return;
  }
  root.innerHTML = `
    <button class="account-filter-button" data-account-id="">
      ${projectName} 总览
    </button>
    ${accounts
      .map(
        (item) => `
          <button class="account-filter-button ${item.account_id === active.account_id ? "is-active" : ""}" data-account-id="${item.account_id}">
            ${item.account}
          </button>
        `,
      )
      .join("")}
  `;
  root.querySelectorAll(".account-filter-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeAccountId = button.dataset.accountId || "";
      state.rankingScope = state.activeAccountId ? "account" : "all";
      renderApp();
    });
  });
}

async function exportCurrentAccountRankings() {
  const resultNode = document.getElementById("accountExportResult");
  const exportDirNode = document.getElementById("accountRankingExportDir");
  const active = getActiveAccount();
  const projectName = getSelectedProjectName();
  if (!active || !active.account_id) {
    throw new Error("请先选择一个账号后再导出");
  }
  if (projectName === "all") {
    throw new Error("请先进入单个项目后再导出");
  }
  resultNode.textContent = "正在导出当前账号点赞/评论榜单...";
  const response = await fetch("/api/account-rankings/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      account_id: active.account_id,
      project: projectName,
      export_dir: String(exportDirNode?.value || "").trim(),
    }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `导出失败: ${response.status}`);
  }
  if (exportDirNode && !exportDirNode.value && payload.export_dir) {
    exportDirNode.value = payload.account_dir || payload.export_dir;
  }
  resultNode.textContent = `已导出快照 ${payload.snapshot_time} · 点赞 ${payload.like_count} 条 · 评论 ${payload.comment_count} 条 · 目录 ${payload.export_dir}`;
}

async function exportCurrentProjectRankings() {
  const resultNode = document.getElementById("accountExportResult");
  const exportDirNode = document.getElementById("accountRankingExportDir");
  const projectName = getSelectedProjectName();
  const accounts = getVisibleAccounts();
  if (projectName === "all") {
    throw new Error("请先进入单个项目后再导出");
  }
  if (!accounts.length) {
    throw new Error("当前项目没有可导出的账号");
  }
  resultNode.textContent = `正在导出项目「${projectName}」复盘快照...`;
  const response = await fetch("/api/project-rankings/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      project: projectName,
      account_ids: accounts.map((item) => item.account_id).filter(Boolean),
      export_dir: String(exportDirNode?.value || "").trim(),
    }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `项目导出失败: ${response.status}`);
  }
  if (exportDirNode && !exportDirNode.value && payload.project_dir) {
    exportDirNode.value = payload.project_dir;
  }
  resultNode.textContent = `已导出项目快照 ${payload.snapshot_time} · 账号 ${payload.account_count} 个 · 点赞 ${payload.like_count} 条 · 评论 ${payload.comment_count} 条 · 目录 ${payload.export_dir}${payload.compare ? " · 已生成快照对比" : ""}`;
}

function renderProjectHome() {
  const projectName = getSelectedProjectName();
  const entries = getProjectEntries(projectName);
  const accounts = getVisibleAccounts();
  const projectStatus = getProjectStatus(projectName);
  const topAccounts = [...accounts]
    .sort((a, b) => (b.fans - a.fans) || (b.interaction - a.interaction) || (b.likes - a.likes))
    .slice(0, 5);
  const topContents = getProjectTopContentRows(5);
  const projectSeries = getProjectSeries(projectName);
  const projectGrowth = buildProjectComparableGrowth(projectName, 7);

  const titleNode = document.getElementById("projectHomeTitle");
  const summaryNode = document.getElementById("projectHomeSummary");
  const statsNode = document.getElementById("projectHomeStats");
  const topAccountsNode = document.getElementById("projectTopAccounts");
  const topContentsNode = document.getElementById("projectTopContents");
  const trendTitleNode = document.getElementById("projectHomeTrendTitle");
  const trendSummaryNode = document.getElementById("projectHomeTrendSummary");
  const trendChartNode = document.getElementById("projectHomeTrendChart");
  if (
    !titleNode ||
    !summaryNode ||
    !statsNode ||
    !topAccountsNode ||
    !topContentsNode ||
    !trendTitleNode ||
    !trendSummaryNode ||
    !trendChartNode
  ) {
    return;
  }

  if (projectName === "all") {
    titleNode.textContent = "请选择单个项目";
    summaryNode.textContent = "先从上方进入单个项目。";
    statsNode.innerHTML = `<div class="empty-state">请选择单个项目后查看该项目的总览指标。</div>`;
    topAccountsNode.innerHTML = `<div class="empty-state">请选择单个项目后查看项目内 Top 账号。</div>`;
    topContentsNode.innerHTML = `<div class="empty-state">请选择单个项目后查看项目内 Top 内容。</div>`;
    trendTitleNode.textContent = "项目近 7 天成长";
    trendSummaryNode.textContent = "请选择单个项目后查看项目趋势。";
    trendChartNode.innerHTML = `<div class="empty-state">当前不再显示全部项目的混合趋势。</div>`;
    return;
  }
  titleNode.textContent = `项目：${projectName}`;

  const totalFans = accounts.reduce((sum, item) => sum + Number(item.fans || 0), 0);
  const totalInteraction = accounts.reduce((sum, item) => sum + Number(item.interaction || 0), 0);
  const totalComments = accounts.reduce((sum, item) => sum + Number(item.comments || 0), 0);
  const projectCommentSummary = getProjectCommentSummary(projectName);
  const commentTotalValue =
    projectCommentSummary.exactTotal > 0
      ? formatNumber(projectCommentSummary.exactTotal)
      : totalComments > 0
        ? formatNumber(totalComments)
        : projectCommentSummary.rowCount > 0
          ? "缺失"
          : "0";
  let commentTotalHint = "项目内账号首页可见作品评论合计";
  if (projectCommentSummary.exactTotal > 0) {
    commentTotalHint =
      projectCommentSummary.nonExactCount > 0
        ? `按 ${formatNumber(projectCommentSummary.exactCount)} 条精确评论作品汇总；另有 ${formatNumber(projectCommentSummary.nonExactCount)} 条非精确作品未计入`
        : `按 ${formatNumber(projectCommentSummary.exactCount)} 条精确评论作品汇总`;
  } else if (totalComments > 0) {
    commentTotalHint = "项目内账号首页可见作品评论合计";
  } else if (projectCommentSummary.rowCount > 0) {
    commentTotalHint = `当前已有 ${formatNumber(projectCommentSummary.rowCount)} 条评论榜数据，但缺少精确评论总量，主卡未计入`;
  }
  const summaryParts = [];
  summaryParts.push(`监测 ${formatNumber(entries.length)} 个账号`);
  summaryParts.push(`已同步 ${formatNumber(accounts.length)} 个账号`);
  if (projectStatus.last_success_at || projectStatus.finished_at) {
    summaryParts.push(`最近成功 ${formatDateTime(projectStatus.last_success_at || projectStatus.finished_at)}`);
  } else if (projectStatus.updated_at) {
    summaryParts.push(`最近状态 ${formatDateTime(projectStatus.updated_at)}`);
  }
  if (projectGrowth) {
    if (projectGrowth.comparable_ready) {
      summaryParts.push(
        `${projectGrowth.label} 可比账号 ${formatNumber(projectGrowth.comparable_account_count)} 个 · 粉丝 ${formatSignedNumber(projectGrowth.fans)} / 点赞 ${formatSignedNumber(projectGrowth.likes)} / 评论 ${formatSignedNumber(projectGrowth.comments)}`,
      );
    } else {
      summaryParts.push(`${projectGrowth.label} 暂无可比账号`);
    }
  }
  summaryParts.push("本地主视图与手机版对齐");
  summaryNode.textContent = summaryParts.join(" · ");

  const statCards = [
    ["项目粉丝总量", formatNumber(totalFans), "项目内账号当前粉丝总量"],
    ["项目获赞收藏", formatNumber(totalInteraction), "项目内账号当前获赞收藏总量"],
    ["项目评论总量", commentTotalValue, commentTotalHint],
  ];
  statsNode.innerHTML = statCards
    .map(
      ([label, value, hint]) => `
        <article class="portal-card project-home-stat-card">
          <div class="label">${label}</div>
          <div class="value">${value}</div>
          <div class="hint">${hint}</div>
        </article>
      `,
    )
    .join("");

  if (!topAccounts.length) {
    topAccountsNode.innerHTML = `<div class="empty-state">当前项目下暂无已同步账号。</div>`;
  } else {
    topAccountsNode.innerHTML = topAccounts
      .map(
        (item, index) => `
          <article class="project-home-item">
            <div class="project-home-rank">${index + 1}</div>
            <div class="project-home-item-body">
              <div class="project-home-item-title">${item.profile_url ? `<a class="note-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</div>
              <div class="project-home-item-meta">粉丝 ${formatNumber(item.fans)} · 获赞 ${formatNumber(item.interaction)} · 作品 ${item.works_display || formatNumber(item.works)}</div>
            </div>
          </article>
        `,
      )
      .join("");
  }

  if (!topContents.length) {
    topContentsNode.innerHTML = `<div class="empty-state">当前项目下暂无内容排行。</div>`;
  } else {
    topContentsNode.innerHTML = topContents
      .map(
        (item, index) => `
          <article class="project-home-item is-content">
            <div class="project-home-rank">${index + 1}</div>
            <div class="project-home-cover-shell">
              ${buildCoverMarkup(item, { size: "mini", rank: "" })}
            </div>
            <div class="project-home-item-body">
              <div class="project-home-item-title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</div>
              <div class="project-home-item-meta">${item.account} · 点赞 ${formatNumber(item.metric)}</div>
            </div>
          </article>
        `,
      )
      .join("");
  }

  trendTitleNode.textContent = `${projectName} · 近 7 天成长`;
  trendSummaryNode.textContent = projectGrowth
    ? projectGrowth.comparable_ready
      ? `${projectGrowth.start_date} → ${projectGrowth.end_date} · 可比账号 ${formatNumber(projectGrowth.comparable_account_count)} 个 · 粉丝 ${formatSignedNumber(projectGrowth.fans)} / 点赞 ${formatSignedNumber(projectGrowth.likes)} / 评论 ${formatSignedNumber(projectGrowth.comments)}${projectGrowth.new_account_count ? ` · 新增账号 ${formatNumber(projectGrowth.new_account_count)} 个` : ""}`
      : `${projectGrowth.start_date} → ${projectGrowth.end_date} · 暂无可比账号${projectGrowth.new_account_count ? ` · 新增账号 ${formatNumber(projectGrowth.new_account_count)} 个` : ""}`
    : "历史不足，暂不显示项目级周对比";
  trendChartNode.innerHTML = buildProjectTrendMarkup(projectSeries, projectGrowth, projectName);
}

function buildProjectTrendMarkup(series, delta, projectName) {
  if (!series.length) {
    return `<div class="empty-state">${projectName === "all" ? "暂无项目趋势数据。" : "当前项目下暂无趋势数据。"}</div>`;
  }

  const width = 1000;
  const height = 280;
  const pad = { top: 20, right: 30, bottom: 34, left: 46 };
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const fans = series.map((item) => Number(item.fans || 0));
  const comments = series.map((item) => Number(item.comments || 0));
  const step = series.length > 1 ? innerWidth / (series.length - 1) : 0;
  const pointX = (index) => (series.length > 1 ? pad.left + step * index : pad.left + innerWidth / 2);
  const buildPointY = (values) => {
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const span = maxValue - minValue;
    return (value) => {
      if (!span) {
        return pad.top + innerHeight / 2;
      }
      return pad.top + innerHeight - ((value - minValue) / span) * innerHeight;
    };
  };
  const pointYFans = buildPointY(fans);
  const pointYComments = buildPointY(comments);
  const linePath = (values, pointY) =>
    values
      .map((value, index) => {
        const x = pointX(index);
        const y = pointY(value);
        return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
      })
      .join(" ");
  const points = (values, color, pointY) =>
    values
      .map((value, index) => `<circle cx="${pointX(index)}" cy="${pointY(value)}" r="5" fill="${color}" />`)
      .join("");
  const labels = series
    .map((item, index) => `<text class="chart-label" x="${pointX(index)}" y="${height - 10}" text-anchor="middle">${String(item.date).slice(5)}</text>`)
    .join("");

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="projectFansStroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#ffd166" />
          <stop offset="100%" stop-color="#ff9f1c" />
        </linearGradient>
        <linearGradient id="projectCommentsStroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#7fdcc2" />
          <stop offset="100%" stop-color="#52c3a1" />
        </linearGradient>
      </defs>
      <path d="${linePath(fans, pointYFans)}" fill="none" stroke="url(#projectFansStroke)" stroke-width="4" stroke-linecap="round" />
      <path d="${linePath(comments, pointYComments)}" fill="none" stroke="url(#projectCommentsStroke)" stroke-width="4" stroke-linecap="round" />
      ${points(fans, "#ffb347", pointYFans)}
      ${points(comments, "#52c3a1", pointYComments)}
      ${labels}
    </svg>
    <div class="chart-legend">
      <span><span class="legend-dot" style="background:#ff9f1c"></span>项目粉丝 ${formatNumber(fans[0])} → ${formatNumber(fans[fans.length - 1])}</span>
      <span><span class="legend-dot" style="background:#52c3a1"></span>项目评论 ${formatNumber(comments[0])} → ${formatNumber(comments[comments.length - 1])}</span>
    </div>
    ${
      delta
        ? delta.comparable_ready
          ? `<div class="chart-delta-row">
              <span class="delta-chip">${delta.label}</span>
              <span class="delta-chip">可比账号 ${formatNumber(delta.comparable_account_count)} 个</span>
              <span class="delta-chip">粉丝 ${formatSignedNumber(delta.fans)}</span>
              <span class="delta-chip">点赞 ${formatSignedNumber(delta.likes)}</span>
              <span class="delta-chip">评论 ${formatSignedNumber(delta.comments)}</span>
              ${delta.new_account_count ? `<span class="delta-chip">新增账号 ${formatNumber(delta.new_account_count)} 个</span>` : ""}
            </div>`
          : `<div class="chart-delta-row">
              <span class="delta-chip">${delta.label}</span>
              <span class="delta-chip">暂无可比账号</span>
              ${delta.new_account_count ? `<span class="delta-chip">新增账号 ${formatNumber(delta.new_account_count)} 个</span>` : ""}
            </div>`
        : `<div class="chart-delta-row"><span class="delta-chip">历史不足，暂不显示项目级涨跌</span></div>`
    }
    <div class="chart-caption">折线展示项目当前总量趋势，含新增账号；下方涨跌仅统计窗口内连续留底的可比账号，避免新增账号导致增量失真。</div>
  `;
}

function renderMonitoring() {
  const monitoring = state.monitoring || {};
  const syncStatus = monitoring.sync_status || {};
  const profileLookupError = monitoring.profile_lookup_error || "";
  const loginState = monitoring.login_state || {};
  const proxyPool = monitoring.proxy_pool || {};
  const panelNode = document.querySelector(".monitor-panel");
  const toggleButton = document.getElementById("monitorPanelToggle");
  const countNode = document.getElementById("monitorCount");
  const badgeNode = document.getElementById("syncBadge");
  const statusNode = document.getElementById("syncStatusText");
  const fileNode = document.getElementById("urlsFileText");
  const statsNode = document.getElementById("monitorStatsBar");
  const listNode = document.getElementById("monitorList");
  const pageInfoNode = document.getElementById("monitorPageInfo");
  const prevPageButton = document.getElementById("monitorPrevPage");
  const nextPageButton = document.getElementById("monitorNextPage");
  const projects = monitoring.projects || [];

  if (panelNode) {
    panelNode.classList.toggle("is-collapsed", !state.monitorPanelExpanded);
  }
  if (toggleButton) {
    toggleButton.textContent = state.monitorPanelExpanded ? "收起管理" : "展开管理";
  }

  countNode.textContent = `${formatNumber(monitoring.active_count || 0)} 监测中 / ${formatNumber(monitoring.total || 0)} 总数`;
  badgeNode.textContent = syncStatus.state === "running" ? "同步中" : syncStatus.state === "success" ? "已同步" : syncStatus.state === "error" ? "失败" : "待命";
  badgeNode.className = `sync-badge is-${syncStatus.state || "idle"}`;
  statusNode.textContent = syncStatus.message || "当前未开始同步";
  fileNode.textContent = monitoring.urls_file
    ? `监测清单：${monitoring.urls_file} · 暂停 ${formatNumber(monitoring.paused_count || 0)} 个`
    : "";
  renderSyncProgress(syncStatus);
  renderLoginState(loginState);
  renderProxyPool(proxyPool);
  renderManualUpdateState(syncStatus);

  document.getElementById("monitorSearchInput").value = state.monitorQuery;
  document.getElementById("monitorPageSize").value = String(state.monitorPageSize);
  renderMonitorProjectFilterOptions(projects);
  renderProjectCards(projects, syncStatus);
  document.querySelectorAll(".monitor-filter-button").forEach((button) => {
    button.classList.toggle("is-active", (button.dataset.monitorFilter || "all") === state.monitorFilter);
  });

  const entries = getMonitoringEntries();
  if (!entries.length) {
    statsNode.innerHTML = "";
    pageInfoNode.textContent = "第 1 / 1 页";
    prevPageButton.disabled = true;
    nextPageButton.disabled = true;
    listNode.innerHTML = `<div class="empty-state">当前还没有监测账号，贴入主页链接后会立即加入清单。</div>`;
    return;
  }
  const filteredEntries = getFilteredMonitoringEntries();
  const page = getMonitorPageData(filteredEntries);
  const summaryChips = [
    `项目 ${formatNumber(projects.length)}`,
    `全部 ${formatNumber(monitoring.total || 0)}`,
    `监测中 ${formatNumber(monitoring.active_count || 0)}`,
    `已暂停 ${formatNumber(monitoring.paused_count || 0)}`,
    `当前筛选 ${formatNumber(filteredEntries.length)}`,
    `本页 ${formatNumber(page.items.length)}`,
  ];
  if (syncStatus.summary?.total_accounts) {
    summaryChips.push(`最近同步 ${formatNumber(syncStatus.summary.total_accounts)} 账号 / ${formatNumber(syncStatus.summary.total_works || 0)} 作品`);
  }
  if (profileLookupError) {
    summaryChips.push("账号摘要读取失败，已回退本地清单");
  }
  if (loginState.state && loginState.state !== "idle") {
    summaryChips.push(`登录态 ${getLoginStateText(loginState.state)}`);
  }
  statsNode.innerHTML = summaryChips.map((text) => `<span class="monitor-stat-chip">${text}</span>`).join("");
  pageInfoNode.textContent =
    filteredEntries.length > 0
      ? `第 ${formatNumber(page.currentPage)} / ${formatNumber(page.totalPages)} 页 · 显示 ${formatNumber(page.startIndex + 1)}-${formatNumber(page.endIndex)} / ${formatNumber(filteredEntries.length)}`
      : "筛选后暂无结果";
  prevPageButton.disabled = page.currentPage <= 1;
  nextPageButton.disabled = page.currentPage >= page.totalPages;

  if (!filteredEntries.length) {
    listNode.innerHTML = `<div class="empty-state">当前筛选条件下没有账号，换个关键词或状态再试。</div>`;
    return;
  }
  listNode.className = "monitor-list";
  listNode.innerHTML = page.items
    .map(
      (entry) => `
        <article class="monitor-item ${entry.active ? "" : "is-paused"}">
          <div class="monitor-main">
            <div class="monitor-title-row">
              <a class="monitor-chip" href="${entry.profile_url || entry.url}" target="_blank" rel="noreferrer" title="${entry.url}">
                ${entry.display_name || truncateMiddle(entry.url, 82)}
              </a>
              <span class="monitor-project-pill">${entry.project || "默认项目"}</span>
              <span class="monitor-state-pill ${entry.active ? "is-active" : "is-paused"}">${entry.active ? "监测中" : "已暂停"}</span>
              <span class="monitor-fetch-pill is-${entry.fetch_state || "checking"}">${getFetchStateText(entry.fetch_state)}</span>
            </div>
            <div class="monitor-summary-row">
              <span class="monitor-summary-chip">账号ID ${entry.account_id || "-"}</span>
              <span class="monitor-summary-chip">${entry.summary_text || "暂无快照摘要"}</span>
              <span class="monitor-summary-chip is-time" title="${entry.fetch_checked_at || ""}">采集 ${formatDateTime(entry.fetch_checked_at)}</span>
              <span class="monitor-summary-chip ${entry.fetch_state === "error" ? "is-error" : entry.fetch_state === "ok" ? "is-success" : ""}">${entry.fetch_message || "等待首次同步"}</span>
            </div>
            <div class="monitor-link-text" title="${entry.url}">主页 ${truncateMiddle(entry.url, 72)}</div>
          </div>
          <div class="monitor-item-actions">
            <button class="monitor-inline-button monitor-toggle-button" data-url="${entry.url}" data-active="${entry.active ? "0" : "1"}">
              ${entry.active ? "暂停" : "恢复"}
            </button>
            <button class="monitor-inline-button monitor-retry-button" data-url="${entry.url}">
              重试
            </button>
            <button class="monitor-inline-button danger-button monitor-remove-button" data-url="${entry.url}">
              删除
            </button>
          </div>
        </article>
      `,
    )
    .join("");
  listNode.querySelectorAll(".monitor-toggle-button").forEach((button) => {
    button.addEventListener("click", () => {
      toggleMonitoredAccount(button.dataset.url, button.dataset.active === "1").catch((error) => {
        document.getElementById("addResult").textContent = error.message;
      });
    });
  });
  listNode.querySelectorAll(".monitor-retry-button").forEach((button) => {
    button.addEventListener("click", () => {
      retryMonitoredAccount(button.dataset.url).catch((error) => {
        document.getElementById("addResult").textContent = error.message;
      });
    });
  });
  listNode.querySelectorAll(".monitor-remove-button").forEach((button) => {
    button.addEventListener("click", () => {
      removeMonitoredAccount(button.dataset.url).catch((error) => {
        document.getElementById("addResult").textContent = error.message;
      });
    });
  });
}

function renderSyncProgress(syncStatus) {
  const root = document.getElementById("syncProgressCard");
  if (!root) return;
  const progress = syncStatus?.progress || {};
  const percent = Math.max(0, Math.min(100, Number(progress.overall_percent || 0)));
  const phasePercent = Math.max(0, Math.min(100, Number(progress.phase_percent || 0)));
  const detailText = progress.detail_text || syncStatus?.message || "当前未开始同步";
  const elapsedText = progress.elapsed_text ? `已用 ${progress.elapsed_text}` : "";
  const etaText = progress.eta_text ? `预计剩余 ${progress.eta_text}` : syncStatus?.state === "running" ? "预计剩余计算中" : "";
  const syncLastSuccessAt = syncStatus?.last_success_at ? formatDateTime(syncStatus.last_success_at) : "";
  const syncLastFinishedAt = syncStatus?.finished_at ? formatDateTime(syncStatus.finished_at) : "";
  const syncLastError = String(syncStatus?.last_error || "").trim();
  const schedulePlan = syncStatus?.schedule_plan || {};
  const scheduleSummary = buildSchedulePlanSummary(schedulePlan);
  const scheduleProjectChips = (schedulePlan.projects || [])
    .slice(0, 3)
    .map(
      (item) =>
        `${item.name} ${formatDateTime(item.next_run_at).slice(11, 16)} · ${formatNumber(item.active_count || item.per_run || 0)} 个账号`
    );

  if (syncStatus?.state !== "running" && !progress.phase) {
    root.innerHTML = `
      <div class="sync-progress-idle-grid">
        <section class="sync-progress-idle-block">
          <div class="sync-progress-title">看板同步</div>
          <div class="sync-progress-subtitle">当前无活动同步任务</div>
          <div class="sync-progress-meta">
            <span class="sync-progress-chip">状态 待命</span>
            ${scheduleSummary ? `<span class="sync-progress-chip">${scheduleSummary}</span>` : ""}
            ${syncLastSuccessAt ? `<span class="sync-progress-chip is-success">最近成功 ${syncLastSuccessAt}</span>` : ""}
            ${syncLastFinishedAt && !syncLastSuccessAt ? `<span class="sync-progress-chip">最近结束 ${syncLastFinishedAt}</span>` : ""}
            ${syncLastError ? `<span class="sync-progress-chip is-error">${truncateMiddle(syncLastError, 72)}</span>` : ""}
            ${scheduleProjectChips.map((text) => `<span class="sync-progress-chip">${text}</span>`).join("")}
          </div>
        </section>
      </div>
    `;
    return;
  }

  const chips = [
    { text: progress.phase_label || "同步状态", tone: "" },
    { text: progress.total ? `${formatNumber(progress.current || 0)} / ${formatNumber(progress.total || 0)}` : "", tone: "" },
    { text: `成功 ${formatNumber(progress.success_count || 0)}`, tone: "success" },
    { text: `失败 ${formatNumber(progress.failed_count || 0)}`, tone: progress.failed_count ? "error" : "" },
    { text: progress.account ? truncateMiddle(progress.account, 26) : "", tone: "" },
    { text: progress.works ? `${formatNumber(progress.works)} 条作品` : "", tone: "" },
  ].filter((item) => item.text);

  root.innerHTML = `
    <div class="sync-progress-top">
      <div>
        <div class="sync-progress-title">看板同步</div>
        <div class="sync-progress-subtitle">${detailText}</div>
      </div>
      <div class="sync-progress-percent">${formatNumber(percent)}%</div>
    </div>
    <div class="sync-progress-bar-shell">
      <div class="sync-progress-bar" style="width:${percent}%"></div>
    </div>
    <div class="sync-progress-meta">
      <span>阶段进度 ${formatNumber(phasePercent)}%</span>
      ${elapsedText ? `<span>${elapsedText}</span>` : ""}
      ${etaText ? `<span>${etaText}</span>` : ""}
      ${chips.map((item) => `<span class="sync-progress-chip ${item.tone ? `is-${item.tone}` : ""}">${item.text}</span>`).join("")}
    </div>
  `;
}

function renderLoginState(loginState) {
  const root = document.getElementById("loginStateCard");
  if (!root) return;
  const status = loginState?.state || "idle";
  const checkedAt = loginState?.checked_at ? formatDateTime(loginState.checked_at) : "未检查";
  const sampleLabel = loginState?.sample_account || loginState?.sample_user_id || (loginState?.sample_url ? truncateMiddle(loginState.sample_url, 54) : "未选样本");
  const detailText =
    Number(loginState?.work_count || 0) > 0
      ? `作品 ${formatNumber(loginState.work_count)} · note_id ${formatNumber(loginState.note_id_count || 0)}`
      : "等待样本数据";
  const chips = [
    loginState?.cookie_source_label || "未配置登录态",
    loginState?.fetch_mode ? `模式 ${loginState.fetch_mode}` : "",
    loginState?.sample_url ? `样本 ${sampleLabel}` : "",
    detailText,
  ].filter(Boolean);
  const hintText = String((loginState?.hints || [])[0] || "").trim();
  const proxyPool = state.monitoring?.sync_status?.proxy_pool_status || {};
  const mergedHintText = [hintText, buildProxyPoolSummary(proxyPool)].filter(Boolean).join(" · ");
  root.innerHTML = `
    <div class="login-state-top">
      <span class="login-state-badge is-${status}">${getLoginStateText(status)}</span>
      <span class="login-state-meta">${loginState?.checking ? "正在后台自检..." : `上次自检 ${checkedAt}`}</span>
    </div>
    <div class="login-state-message">${loginState?.message || "等待自动自检"}</div>
    <div class="login-state-chip-row">
      ${chips.map((text) => `<span class="login-state-chip">${text}</span>`).join("")}
    </div>
    ${mergedHintText ? `<div class="login-state-hints"><span>${mergedHintText}</span></div>` : ""}
  `;
}

function buildProxyPoolSummary(proxyPool) {
  if (!proxyPool) return "";
  const pieces = [];
  if (proxyPool?.enabled) {
    pieces.push(`IP池 ${formatNumber(proxyPool.total || 0)}`);
    if (proxyPool?.ready_count) pieces.push(`可用 ${formatNumber(proxyPool.ready_count || 0)}`);
    if (proxyPool?.cooling_count) pieces.push(`冷却 ${formatNumber(proxyPool.cooling_count || 0)}`);
    if (proxyPool?.current_ip) pieces.push(`当前IP ${proxyPool.current_ip}`);
    if (proxyPool?.last_error) pieces.push(proxyPool.last_error);
  } else if (proxyPool?.current_ip_error) {
    pieces.push(`IP 检测失败：${proxyPool.current_ip_error}`);
  } else {
    pieces.push("IP池未启用，当前使用本机网络");
  }
  return pieces.filter(Boolean).join(" · ");
}

function renderProxyPool(proxyPool) {
  const root = document.getElementById("proxyPoolCard");
  if (!root) return;
  root.innerHTML = "";
}

function renderProjectCards(projects, syncStatus) {
  const root = document.getElementById("projectOverview");
  if (!projects.length) {
    root.innerHTML = "";
    return;
  }
  const manualUpdateState = getManualUpdateState(syncStatus);
  const sortedProjects = [...projects].sort((left, right) => {
    const leftSelected = getSelectedProjectName() === left.name ? 1 : 0;
    const rightSelected = getSelectedProjectName() === right.name ? 1 : 0;
    if (leftSelected !== rightSelected) return rightSelected - leftSelected;
    const leftHealth = getProjectHealth(left);
    const rightHealth = getProjectHealth(right);
    if (leftHealth.order !== rightHealth.order) return leftHealth.order - rightHealth.order;
    if (leftHealth.alertCount !== rightHealth.alertCount) return rightHealth.alertCount - leftHealth.alertCount;
    if (left.active_count !== right.active_count) return Number(right.active_count || 0) - Number(left.active_count || 0);
    if (left.total !== right.total) return Number(right.total || 0) - Number(left.total || 0);
    return String(left.name || "").localeCompare(String(right.name || ""), "zh-CN");
  });
  const urgentProjects = [];
  const stableProjects = [];

  const buildProjectCardMarkup = (project) => {
      const entries = getProjectEntries(project.name);
      const previewNames = entries
        .map((entry) => entry.display_name)
        .filter(Boolean)
        .slice(0, 3);
      const isActive = getSelectedProjectName() === project.name;
      const projectHealth = getProjectHealth(project);
      const projectSync = project.sync_status || {};
      const projectSyncState = String(projectSync.state || "");
      const projectSyncBadgeClass = getProjectSyncBadgeClass(projectSyncState);
      const projectSyncBadgeText = getProjectSyncStateText(projectSyncState);
      const projectSyncUpdatedAt = projectSync.last_success_at || projectSync.finished_at || projectSync.updated_at || "";
      const projectSyncMessage = String(projectSync.message || "").trim();
      const projectSyncError = String(projectSync.last_error || "").trim();
      const projectFailureStage = getProjectFailureStage(projectSync);
      const projectFailureTime = getProjectFailureTime(projectSync);
      const projectSyncResult = projectSync.total_accounts
        ? `${formatNumber(projectSync.total_accounts)} 账号 / ${formatNumber(projectSync.total_works || 0)} 作品`
        : "";
      const projectAlertCount = projectHealth.alertCount;
      const projectSyncedAccounts = projectHealth.syncedAccounts;
      return `
        <article class="project-card ${isActive ? "is-active" : ""} is-${projectHealth.level}">
          <div class="project-card-top">
            <div>
              <div class="project-card-name">${project.name}</div>
              <div class="project-card-meta">${formatNumber(project.total)} 个账号 · 监测中 ${formatNumber(project.active_count)} · 暂停 ${formatNumber(project.paused_count)}</div>
            </div>
            <div class="project-card-badge-group">
              <span class="project-card-health-pill is-${projectHealth.level}">${projectHealth.label}</span>
              <span class="project-card-badge">${isActive ? "当前项目" : "项目"}</span>
            </div>
          </div>
          <div class="project-card-metrics">
            <span class="project-card-metric-chip">已同步 ${formatNumber(projectSyncedAccounts)}</span>
            <span class="project-card-metric-chip ${projectAlertCount ? "is-warning" : ""}">预警 ${formatNumber(projectAlertCount)}</span>
            ${
              projectSyncState === "error" && projectFailureStage
                ? `<span class="project-card-metric-chip is-danger">阶段 ${projectFailureStage}</span>`
                : ""
            }
            ${
              projectSyncState === "error" && projectFailureTime
                ? `<span class="project-card-metric-chip">失败于 ${formatDateTime(projectFailureTime)}</span>`
                : ""
            }
          </div>
          <div class="project-card-status-row">
            <span class="sync-badge ${projectSyncBadgeClass}">${projectSyncBadgeText}</span>
            ${
              projectSyncUpdatedAt
                ? `<span class="project-card-status-time subtle">${formatDateTime(projectSyncUpdatedAt)}</span>`
                : ""
            }
          </div>
          ${
            projectSyncResult || projectSyncMessage || projectSyncError
              ? `<div class="project-card-status-text subtle">${
                  projectSyncResult || projectSyncMessage || projectSyncError
                }</div>`
              : ""
          }
          ${
            projectSyncError
              ? `<div class="project-card-status-error subtle">${truncateMiddle(projectSyncError, 88)}</div>`
              : ""
          }
          <div class="project-card-preview">${previewNames.length ? previewNames.join(" / ") : "暂无账号"}</div>
          <div class="project-card-actions">
            <button class="monitor-inline-button project-open-button" data-project="${project.name}" type="button">
              ${isActive ? "查看全部项目" : "进入项目"}
            </button>
            <button class="monitor-inline-button project-sync-button" data-project="${project.name}" type="button" ${manualUpdateState.disabled ? "disabled" : ""}>
              ${manualUpdateState.projectButtonText}
            </button>
          </div>
          <div class="project-card-foot subtle">点击后切换到该项目的账号、榜单和同步范围</div>
        </article>
      `;
  };

  sortedProjects.forEach((project) => {
    const markup = buildProjectCardMarkup(project);
    const projectHealth = getProjectHealth(project);
    if (projectHealth.level === "healthy" || projectHealth.level === "idle") {
      stableProjects.push(markup);
    } else {
      urgentProjects.push(markup);
    }
  });

  root.innerHTML = `
    ${
      urgentProjects.length
        ? `
      <section class="project-card-group">
        <div class="project-card-group-header">
          <div class="project-card-group-title">优先处理项目</div>
          <div class="project-card-group-meta">${formatNumber(urgentProjects.length)} 个</div>
        </div>
        <div class="project-card-grid">
          ${urgentProjects.join("")}
        </div>
      </section>
    `
        : ""
    }
    ${
      stableProjects.length
        ? `
      <section class="project-card-group">
        <button class="project-group-toggle" id="stableProjectToggle" type="button">
          <span class="project-card-group-title">稳定项目</span>
          <span class="project-card-group-meta">${formatNumber(stableProjects.length)} 个 · ${state.collapseStableProjects ? "展开" : "收起"}</span>
        </button>
        <div class="project-card-grid ${state.collapseStableProjects ? "is-collapsed" : ""}" id="stableProjectGrid">
          ${stableProjects.join("")}
        </div>
      </section>
    `
        : ""
    }
  `;

  const stableToggle = document.getElementById("stableProjectToggle");
  if (stableToggle) {
    stableToggle.addEventListener("click", () => {
      state.collapseStableProjects = !state.collapseStableProjects;
      renderProjectCards(projects, syncStatus);
    });
  }

  root.querySelectorAll(".project-open-button").forEach((button) => {
    button.addEventListener("click", () => {
      const nextProject = button.dataset.project || "all";
      state.monitorProjectFilter = getSelectedProjectName() === nextProject ? "all" : nextProject;
      state.activeAccountId = "";
      state.rankingScope = "all";
      resetMonitoringPage();
      ensureActiveAccount();
      renderMonitoring();
      renderApp();
    });
  });
  root.querySelectorAll(".project-sync-button").forEach((button) => {
    button.addEventListener("click", () => {
      syncProject(button.dataset.project || "").catch((error) => {
        document.getElementById("addResult").textContent = error.message;
      });
    });
  });
}

function getManualUpdateState(syncStatus) {
  const running = syncStatus?.state === "running";
  const launchdStatus = syncStatus?.launchd_status || {};
  const autoRunning = String(launchdStatus.state || "") === "running";
  let cooldownSeconds = Number(syncStatus?.manual_cooldown_seconds_remaining || 0);
  if (syncStatus?.manual_available_at) {
    const availableAt = new Date(syncStatus.manual_available_at).getTime();
    if (!Number.isNaN(availableAt)) {
      cooldownSeconds = Math.max(0, Math.ceil((availableAt - Date.now()) / 1000));
    }
  }
  const disableAll = running || autoRunning || cooldownSeconds > 0;
  const disableProject = running || cooldownSeconds > 0;
  const disableAccount = running || cooldownSeconds > 0;
  const allButtonText = running || autoRunning ? "采集中..." : cooldownSeconds > 0 ? `冷却 ${formatDurationShort(cooldownSeconds)}` : "更新全部项目";
  const projectButtonText = running ? "采集中" : cooldownSeconds > 0 ? "冷却中" : "更新当前项目";
  const accountButtonText = running ? "采集中" : cooldownSeconds > 0 ? "冷却中" : "更新当前账号";
  const helperText = running
    ? "当前正在手动采集并更新本地看板，为避免重复请求，采集按钮已临时锁定。"
    : autoRunning
      ? "自动任务正在整批采集，已临时锁定“更新全部项目”；如需补采，可继续更新当前项目或当前账号。"
    : cooldownSeconds > 0
      ? `为降低小红书风控，本地采集冷却中，剩余 ${formatDurationShort(cooldownSeconds)}。每天 14:00 自动更新不受影响。`
      : "可以分别更新全部项目、当前项目或当前账号；需要共享时再推送到服务器。";
  return {
    disableAll,
    disableProject,
    disableAccount,
    allButtonText,
    projectButtonText,
    accountButtonText,
    helperText,
    cooldownSeconds,
  };
}

function renderManualUpdateState(syncStatus) {
  const { disableAll, disableProject, disableAccount, allButtonText, projectButtonText, accountButtonText, helperText } = getManualUpdateState(syncStatus);
  const selectedProject = getSelectedProjectName();
  const hasProjectScope = selectedProject !== "all";
  const activeAccount = getActiveAccount();
  const syncButton = document.getElementById("syncNowButton");
  const heroButton = document.getElementById("manualUpdateButton");
  const syncAllButton = document.getElementById("syncAllButton");
  const heroProjectButton = document.getElementById("manualUpdateProjectButton");
  const heroAccountButton = document.getElementById("manualUpdateAccountButton");
  const syncAccountButton = document.getElementById("syncAccountButton");
  const hintNode = document.getElementById("syncCooldownText");
  if (syncButton) {
    syncButton.disabled = disableProject || !hasProjectScope;
    syncButton.textContent = projectButtonText;
    syncButton.title = hasProjectScope ? "" : "先进入一个项目，再更新当前项目";
  }
  if (heroButton) {
    heroButton.disabled = disableAll;
    heroButton.textContent = allButtonText;
  }
  if (syncAllButton) {
    syncAllButton.disabled = disableAll;
    syncAllButton.textContent = allButtonText;
  }
  if (heroProjectButton) {
    heroProjectButton.disabled = disableProject || !hasProjectScope;
    heroProjectButton.textContent = projectButtonText;
    heroProjectButton.title = hasProjectScope ? "" : "先进入一个项目，再更新当前项目";
  }
  if (heroAccountButton) {
    heroAccountButton.disabled = disableAccount || !activeAccount;
    heroAccountButton.textContent = accountButtonText;
    heroAccountButton.title = activeAccount ? "" : "先选择一个账号，再更新当前账号";
  }
  if (syncAccountButton) {
    syncAccountButton.disabled = disableAccount || !activeAccount;
    syncAccountButton.textContent = accountButtonText;
    syncAccountButton.title = activeAccount ? "" : "先选择一个账号，再更新当前账号";
  }
  document.querySelectorAll(".project-sync-button").forEach((button) => {
    button.disabled = disableProject;
    button.textContent = projectButtonText;
  });
  if (hintNode) {
    hintNode.textContent = helperText;
  }
}

function renderMeta() {
  const payload = state.payload;
  const updatedAtNode = document.getElementById("updatedAt");
  const latestDateNode = document.getElementById("latestDate");
  const samplingNoteNode = document.getElementById("samplingNote");
  if (updatedAtNode) {
    updatedAtNode.textContent = `数据更新时间：${formatDateTime(payload.updated_at || payload.generated_at)}${payload.stale ? " · 当前显示缓存" : ""}`;
  }
  if (latestDateNode) {
    latestDateNode.textContent = `最新留底：${payload.latest_date || "-"}`;
  }
  if (samplingNoteNode) {
    samplingNoteNode.textContent = "口径说明：每个账号最多采集前 30 条作品；项目增长默认按可比账号计算；服务器和手机端都只读取这份本地缓存。";
  }
  const active = getActiveAccount();
  const weeklySummaryNode = document.getElementById("weeklySummary");
  if (weeklySummaryNode) {
    weeklySummaryNode.textContent = active?.weekly_summary || "暂无周对比摘要";
  }
  const seriesMeta = payload.series_meta || {};
  const trendModeChipNode = document.getElementById("trendModeChip");
  if (trendModeChipNode) {
    trendModeChipNode.textContent = `${seriesMeta.mode === "daily" ? "日更" : "趋势"}${seriesMeta.update_time ? ` · ${seriesMeta.update_time}` : ""}`;
  }
}

function renderTrendWindowTabs() {
  document.querySelectorAll(".trend-window-button").forEach((button) => {
    const buttonWindow = Number(button.dataset.trendWindow || 7);
    button.classList.toggle("is-active", buttonWindow === state.trendWindow);
  });
}

function renderRankingScopeTabs() {
  document.querySelectorAll(".scope-button").forEach((button) => {
    button.classList.toggle("is-active", (button.dataset.rankingScope || "account") === state.rankingScope);
  });
}

function renderPortalCards() {
  const portalRoot = document.getElementById("portalCards");
  if (!portalRoot) return;
  const active = getActiveAccount();
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    portalRoot.innerHTML = `<div class="empty-state">请选择单个项目后查看该项目或项目内账号的数据卡。</div>`;
    return;
  }
  const fullSeries = active ? getActiveSeries() : getProjectSeries(projectName);
  const windowGrowth = buildWindowGrowth(fullSeries, active ? state.trendWindow : 7);
  if (!active) {
    const accounts = getVisibleAccounts();
    if (!accounts.length) {
      portalRoot.innerHTML = `<div class="empty-state">当前项目下暂无项目指标。</div>`;
      return;
    }
    const totalFans = accounts.reduce((sum, item) => sum + Number(item.fans || 0), 0);
    const totalInteraction = accounts.reduce((sum, item) => sum + Number(item.interaction || 0), 0);
    const totalComments = accounts.reduce((sum, item) => sum + Number(item.comments || 0), 0);
    const projectCommentSummary = getProjectCommentSummary(projectName);
    const projectCommentCardValue =
      projectCommentSummary.exactTotal > 0
        ? formatNumber(projectCommentSummary.exactTotal)
        : totalComments > 0
          ? formatNumber(totalComments)
          : projectCommentSummary.rowCount > 0
            ? "缺失"
            : "0";
    const projectCommentCardHint =
      projectCommentSummary.exactTotal > 0
        ? projectCommentSummary.nonExactCount > 0
          ? `按 ${formatNumber(projectCommentSummary.exactCount)} 条精确评论作品汇总；另有 ${formatNumber(projectCommentSummary.nonExactCount)} 条非精确作品未计入`
          : `按 ${formatNumber(projectCommentSummary.exactCount)} 条精确评论作品汇总`
        : totalComments > 0
          ? "当前范围下首页可见作品评论合计"
          : projectCommentSummary.rowCount > 0
            ? `当前已有 ${formatNumber(projectCommentSummary.rowCount)} 条评论榜数据，但缺少精确评论总量，主卡未计入`
            : "当前范围下首页可见作品评论合计";
    const projectGrowth = buildProjectComparableGrowth(projectName, 7);
    const comparableHint = projectGrowth
      ? projectGrowth.comparable_ready
        ? `${projectGrowth.start_date} → ${projectGrowth.end_date} · 仅统计 ${formatNumber(projectGrowth.comparable_account_count)} 个可比账号`
        : `${projectGrowth.start_date} → ${projectGrowth.end_date} · 暂无可比账号`
      : "历史不足，暂不显示增长";
    const cards = [
      ["项目粉丝总量", formatNumber(totalFans), "当前范围下账号粉丝总量"],
      ["项目获赞收藏", formatNumber(totalInteraction), "当前范围下账号获赞收藏总量"],
      ["项目评论总量", projectCommentCardValue, projectCommentCardHint],
      ["可比账号数", projectGrowth ? formatNumber(projectGrowth.comparable_account_count || 0) : "-", comparableHint],
      ["新增账号数", projectGrowth ? formatNumber(projectGrowth.new_account_count || 0) : "-", projectGrowth ? `${projectGrowth.start_date} → ${projectGrowth.end_date} 新进入统计窗口的账号` : "历史不足，暂不显示新增账号"],
      ["近7天可比粉丝增量", projectGrowth && projectGrowth.comparable_ready ? formatSignedNumber(projectGrowth.fans) : "-", comparableHint],
      ["近7天可比点赞增量", projectGrowth && projectGrowth.comparable_ready ? formatSignedNumber(projectGrowth.likes) : "-", comparableHint],
      ["近7天可比评论增量", projectGrowth && projectGrowth.comparable_ready ? formatSignedNumber(projectGrowth.comments) : "-", comparableHint],
      ["近7天可比作品增量", projectGrowth && projectGrowth.comparable_ready ? formatSignedNumber(projectGrowth.works) : "-", comparableHint],
    ];
    portalRoot.innerHTML = cards
      .map(
        ([label, value, hint]) => `
          <article class="portal-card">
            <div class="label">${label}</div>
            <div class="value">${value}</div>
            <div class="hint">${hint}</div>
          </article>
        `,
      )
      .join("");
    return;
  }
  const growthLabel = windowGrowth?.label || "成长窗口";
  const rangeText = windowGrowth ? `${windowGrowth.start_date} → ${windowGrowth.end_date}` : "历史不足，暂不显示增长";
  const cards = [
    ["当前粉丝", formatNumber(active.fans), "当前账号粉丝规模"],
    ["当前获赞收藏", formatNumber(active.interaction), "当前账号公开页获赞收藏"],
    ["当前评论总数", formatNumber(active.comments), "当前账号首页可见作品评论合计"],
    [`${growthLabel}粉丝增量`, windowGrowth ? formatSignedNumber(windowGrowth.fans) : "-", rangeText],
    [`${growthLabel}获赞增量`, windowGrowth ? formatSignedNumber(windowGrowth.interaction) : "-", rangeText],
    [
      `${growthLabel}作品增量`,
      windowGrowth ? formatSignedNumber(windowGrowth.works) : "-",
      active.works_exact === false ? `${rangeText} · 当前账号作品总量未完全展开，仅看增长变化` : rangeText,
    ],
  ];
  const root = document.getElementById("portalCards");
  root.innerHTML = cards
    .map(
      ([label, value, hint]) => `
        <article class="portal-card">
          <div class="label">${label}</div>
          <div class="value">${value}</div>
          <div class="hint">${hint}</div>
        </article>
      `,
    )
    .join("");
}

function renderTrendChart() {
  const active = getActiveAccount();
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    document.getElementById("trendTitle").textContent = "账号趋势";
    document.getElementById("trendChart").innerHTML = `<div class="empty-state">请选择单个项目后查看趋势，不再显示跨项目混合走势。</div>`;
    return;
  }
  if (!active) {
    const projectSeries = getProjectSeries(projectName);
    const projectDelta = buildWindowGrowth(projectSeries, 7);
    document.getElementById("trendTitle").textContent = projectName === "all" ? "项目成长趋势" : `${projectName} 项目成长趋势`;
    document.getElementById("trendChart").innerHTML = buildProjectTrendMarkup(projectSeries, projectDelta, projectName);
    return;
  }
  const fullSeries = getActiveSeries();
  const displayCount = state.trendWindow === 1 ? 2 : state.trendWindow;
  const series = fullSeries.slice(-displayCount);
  const delta = buildWindowGrowth(fullSeries, state.trendWindow);
  const windowNote =
    state.trendWindow === 1
      ? "1天视图展示最近 2 个留底点，便于看昨天到今天的变化。"
      : `当前窗口：${delta?.label || `已积累${series.length}天`}`;
  const root = document.getElementById("trendChart");
  document.getElementById("trendTitle").textContent = active ? `${active.account} 成长趋势` : "账号趋势";
  if (!series.length) {
    root.innerHTML = `<div class="empty-state">暂无日更趋势数据。</div>`;
    return;
  }

  const width = 1000;
  const height = 280;
  const pad = { top: 20, right: 30, bottom: 34, left: 46 };
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const fans = series.map((item) => Number(item.fans || 0));
  const interaction = series.map((item) => Number(item.interaction || 0));
  const step = series.length > 1 ? innerWidth / (series.length - 1) : 0;
  const pointX = (index) => (series.length > 1 ? pad.left + step * index : pad.left + innerWidth / 2);
  const buildPointY = (values) => {
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const span = maxValue - minValue;
    return (value) => {
      if (!span) {
        return pad.top + innerHeight / 2;
      }
      return pad.top + innerHeight - ((value - minValue) / span) * innerHeight;
    };
  };
  const pointYFans = buildPointY(fans);
  const pointYInteraction = buildPointY(interaction);

  const linePath = (values, pointY) =>
    values
      .map((value, index) => {
        const x = pointX(index);
        const y = pointY(value);
        return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
      })
      .join(" ");

  const points = (values, color, pointY) =>
    values
      .map((value, index) => {
        const x = pointX(index);
        const y = pointY(value);
        return `<circle cx="${x}" cy="${y}" r="5" fill="${color}" />`;
      })
      .join("");

  const labels = series
    .map((item, index) => {
      const x = pointX(index);
      const y = height - 10;
      return `<text class="chart-label" x="${x}" y="${y}" text-anchor="middle">${String(item.date).slice(5)}</text>`;
    })
    .join("");

  root.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="fansStroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#ffd166" />
          <stop offset="100%" stop-color="#ff9f1c" />
        </linearGradient>
        <linearGradient id="interactionStroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#7fdcc2" />
          <stop offset="100%" stop-color="#52c3a1" />
        </linearGradient>
      </defs>
      <path d="${linePath(fans, pointYFans)}" fill="none" stroke="url(#fansStroke)" stroke-width="4" stroke-linecap="round" />
      <path d="${linePath(interaction, pointYInteraction)}" fill="none" stroke="url(#interactionStroke)" stroke-width="4" stroke-linecap="round" />
      ${points(fans, "#ffb347", pointYFans)}
      ${points(interaction, "#52c3a1", pointYInteraction)}
      ${labels}
    </svg>
    <div class="chart-legend">
      <span><span class="legend-dot" style="background:#ff9f1c"></span>粉丝数 ${formatNumber(fans[0])} → ${formatNumber(fans[fans.length - 1])}</span>
      <span><span class="legend-dot" style="background:#52c3a1"></span>获赞收藏 ${formatNumber(interaction[0])} → ${formatNumber(interaction[interaction.length - 1])}</span>
    </div>
    ${
      delta
        ? `
      <div class="chart-delta-row">
        <span class="delta-chip">${delta.label}</span>
        <span class="delta-chip">粉丝 ${formatSignedNumber(delta.fans)}</span>
        <span class="delta-chip">获赞 ${formatSignedNumber(delta.interaction)}</span>
        <span class="delta-chip">作品 ${formatSignedNumber(delta.works)}</span>
      </div>
    `
        : `<div class="chart-delta-row"><span class="delta-chip">历史不足，暂不显示涨跌</span></div>`
    }
    <div class="chart-caption">${state.payload?.series_meta?.note || "趋势图按天留底"} 折线按各指标自身波动区间缩放，更直观看成长方向；作品变化在下方增量展示。${windowNote}</div>
  `;
}

function renderRankingList() {
  const active = getActiveAccount();
  const root = document.getElementById("rankingList");
  const rankingTitle = document.getElementById("rankingTitle");
  const rankingSummaryText = document.getElementById("rankingSummaryText");
  const rankingSummaryBar = document.getElementById("rankingSummaryBar");
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    rankingTitle.textContent = "榜单中心";
    rankingSummaryText.textContent = "请选择单个项目后查看该项目内的榜单，不再显示跨项目混合榜单。";
    rankingSummaryBar.innerHTML = `<div class="empty-state">请选择单个项目后查看榜单摘要。</div>`;
    root.innerHTML = `<div class="empty-state">请选择单个项目后查看项目榜单。</div>`;
    return;
  }
  const rankingSource = getSelectedRankingSource(projectName);
  const rankingDate = rankingSource.date || String(state.payload?.latest_date || "").trim() || "未选择日期";
  const rankings = {
    "单条点赞排行": rankingSource.snapshot?.likes || [],
    "单条评论排行": rankingSource.snapshot?.comments || [],
    "单条第二天增长排行": rankingSource.snapshot?.growth || [],
  };
  rankingTitle.textContent =
    state.rankingScope === "all"
      ? `${rankingDate} · 项目 ${projectName} 排行榜`
      : `${rankingDate} · ${active ? active.account : `项目 ${projectName} 暂无已同步账号`}`;
  rankingSummaryText.textContent =
    state.rankingScope === "all"
      ? `当前查看项目「${projectName}」在 ${rankingDate} 的内容榜单。${rankingSource.fallbackToCurrent ? " 当天历史榜单缺失，已回退到当前榜单。" : ""}`
      : active
        ? `当前查看账号「${active.account}」在 ${rankingDate} 的内容表现。${rankingSource.fallbackToCurrent ? " 当天历史榜单缺失，已回退到当前榜单。" : ""}`
        : `项目「${projectName}」下暂无已同步账号，暂时无法切换到账号维度。`;

  const summaryItems = rankingConfigs
    .map((config) => {
      const scopedRows = getScopedRankingRows(rankings[config.type] || [], active, state.rankingScope, projectName);
      const top = scopedRows[0];
      if (!top) return "";
      return `
        <div class="ranking-summary-chip">
          <div class="ranking-summary-chip-label">${config.title} Top1</div>
          <div class="ranking-summary-chip-title">${top.title}</div>
          <div class="ranking-summary-chip-meta">${top.account} · ${config.metricLabel} ${formatNumber(top.metric)}</div>
        </div>
      `;
    })
    .filter(Boolean);
  rankingSummaryBar.innerHTML = summaryItems.length
    ? summaryItems.join("")
    : `<div class="empty-state">当前范围下暂无榜单数据。</div>`;
  root.innerHTML = rankingConfigs
    .map((config) => renderRankingColumn(config, rankings[config.type] || [], active))
    .join("");
}

function renderRankingColumn(config, allRows, active) {
  const projectName = getSelectedProjectName();
  const filteredRows = getScopedRankingRows(allRows, active, state.rankingScope, projectName);
  const rows = filteredRows.slice(0, 10);
  if (!rows.length) {
    return `
      <section class="ranking-column">
        <div class="ranking-column-header">
          <h3>${config.title}</h3>
          <span class="ranking-column-meta">${state.rankingScope === "all" ? (projectName === "all" ? "全局 Top 10" : "项目 Top 10") : "账号 Top 10"}</span>
        </div>
        <div class="empty-state">暂无数据</div>
      </section>
    `;
  }
  return `
    <section class="ranking-column">
      <div class="ranking-column-header">
        <h3>${config.title}</h3>
        <span class="ranking-column-meta">${state.rankingScope === "all" ? (projectName === "all" ? "全局 Top 10" : "项目 Top 10") : "账号 Top 10"}</span>
      </div>
      ${renderRankingHero(config, rows[0])}
      ${
        rows.length > 1
          ? `
        <div class="ranking-mini-list">
          ${rows
            .slice(1)
            .map((item, index) => renderRankingMiniItem(config, item, index + 2))
            .join("")}
        </div>
      `
          : ""
      }
    </section>
  `;
}

function renderRankingHero(config, item) {
  const primaryUrl = getPreferredItemUrl(item);
  const primaryLabel = item.note_url ? "作品详情" : "账号主页";
  return `
    <article class="ranking-hero">
      ${buildCoverMarkup(item, { size: "hero", rank: 1 })}
      <div class="ranking-hero-body">
        <div class="ranking-hero-meta">
          <div class="ranking-hero-text">
            <p class="ranking-hero-title">${primaryUrl ? `<a class="note-link" href="${primaryUrl}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
            <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</div>
          </div>
          <div class="ranking-hero-metric">
            <div class="metric-label">${config.metricLabel}</div>
            <div class="metric-value">${formatNumber(item.metric)}</div>
          </div>
        </div>
        <div class="subtle">${item.summary || "当前榜单头部内容"}</div>
        ${renderCommentBasisChip(item) ? `<div class="ranking-basis-row">${renderCommentBasisChip(item)}${buildCommentBasisHint(item) ? `<span class="subtle">${buildCommentBasisHint(item)}</span>` : ""}</div>` : ""}
        <div class="action-row">
          ${buildActionLink(item.profile_url, "账号主页")}
          ${buildActionLink(primaryUrl, primaryLabel)}
        </div>
      </div>
    </article>
  `;
}

function renderRankingMiniItem(config, item, rank) {
  const primaryUrl = getPreferredItemUrl(item);
  return `
    <article class="ranking-mini-item">
      ${buildCoverMarkup(item, { size: "mini", rank })}
      <div class="ranking-mini-body">
        <p class="title">${primaryUrl ? `<a class="note-link" href="${primaryUrl}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
        <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</div>
        <div class="ranking-mini-summary">${item.summary || ""}</div>
        ${renderCommentBasisChip(item)}
      </div>
      <div class="ranking-mini-metric">
        <div class="metric-label">${config.metricLabel}</div>
        <div class="metric-value">${formatNumber(item.metric)}</div>
      </div>
    </article>
  `;
}

function renderAccounts() {
  const accounts = getVisibleAccounts();
  const active = getActiveAccount();
  const root = document.getElementById("accountCards");
  if (!accounts.length) {
    root.innerHTML = `<div class="empty-state">暂无账号快照。</div>`;
    return;
  }
  root.innerHTML = accounts
    .map(
      (item) => {
        const dayGrowth = buildWindowGrowth((state.payload?.account_series || {})[item.account_id] || [], 1);
        return `
        <article class="account-card ${active && item.account_id === active.account_id ? "is-active" : ""}" data-account-id="${item.account_id}">
          <p class="title">${item.profile_url ? `<a class="note-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</p>
          <div class="subtle">${item.weekly_summary || "暂无周对比摘要"}</div>
          ${item.comments_note ? `<div class="ranking-basis-row"><span class="ranking-basis-chip">评论下限</span><span class="subtle">${item.comments_note}</span></div>` : ""}
          <div class="action-row">
            ${buildActionLink(item.profile_url, "打开主页")}
            ${buildActionLink(item.top_url, "头部作品")}
          </div>
          <div class="stats">
            <div class="stat"><div class="stat-label">粉丝</div><div class="stat-value">${formatNumber(item.fans)}</div></div>
            <div class="stat"><div class="stat-label">获赞收藏</div><div class="stat-value">${formatNumber(item.interaction)}</div></div>
            <div class="stat"><div class="stat-label">作品</div><div class="stat-value">${item.works_display || formatNumber(item.works)}</div></div>
            <div class="stat"><div class="stat-label">较前1天</div><div class="stat-value stat-value-growth">${dayGrowth ? `粉丝 ${formatSignedNumber(dayGrowth.fans)} / 点赞 ${formatSignedNumber(dayGrowth.likes)} / 评论 ${formatSignedNumber(dayGrowth.comments)}` : "历史不足"}</div></div>
          </div>
        </article>
      `;
      },
    )
    .join("");
  root.querySelectorAll(".account-card").forEach((card) => {
    card.addEventListener("click", (event) => {
      if (event.target.closest("a")) {
        return;
      }
      state.activeAccountId = card.dataset.accountId || "";
      state.rankingScope = state.activeAccountId ? "account" : "all";
      renderApp();
    });
  });
}

function renderAlerts() {
  const active = getActiveAccount();
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    document.getElementById("alertsSummaryText").textContent = "请选择单个项目后查看该项目的互动预警，不再显示跨项目汇总。";
    document.getElementById("alertsSummaryBar").innerHTML = `<div class="empty-state">请选择单个项目后查看预警摘要。</div>`;
    document.getElementById("alertsList").innerHTML = `<div class="empty-state">请选择单个项目后查看项目预警明细。</div>`;
    return;
  }
  const allAlerts = getVisibleAlerts();
  const alerts = active
    ? allAlerts.filter((item) => item.account_id === active.account_id)
    : allAlerts;
  const summaryNode = document.getElementById("alertsSummaryText");
  const summaryBarNode = document.getElementById("alertsSummaryBar");
  const root = document.getElementById("alertsList");
  const totalDelta = alerts.reduce((sum, item) => sum + Number(item.delta || 0), 0);
  summaryNode.textContent = active
    ? `当前账号命中 ${formatNumber(alerts.length)} 条互动预警，累计最高增量 ${formatNumber(totalDelta)}，规则为点赞或评论增加至少 10。`
    : `当前项目「${projectName}」命中 ${formatNumber(alerts.length)} 条互动预警，累计最高增量 ${formatNumber(totalDelta)}，规则为点赞或评论增加至少 10。`;

  const summaryItems = alerts.slice(0, 3).map(
    (item, index) => `
      <div class="alert-summary-chip">
        <div class="alert-summary-chip-label">高危 ${index + 1}</div>
        <div class="alert-summary-chip-title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</div>
        <div class="alert-summary-chip-meta">${item.account} · ${item.alert_type} · 点赞 +${formatNumber(item.like_delta)} · 评论 +${formatNumber(item.comment_delta)}</div>
      </div>
    `,
  );
  summaryBarNode.innerHTML = summaryItems.length
    ? summaryItems.join("")
    : `<div class="empty-state">${active ? "当前账号没有互动预警。" : projectName === "all" ? "暂无互动预警。" : "当前项目下暂无互动预警。"}</div>`;
  if (!alerts.length) {
    root.innerHTML = `<div class="empty-state">${active ? "当前账号没有互动预警。" : projectName === "all" ? "暂无互动预警。" : "当前项目下暂无互动预警。"}</div>`;
    return;
  }
  root.innerHTML = alerts
    .map(
      (item) => `
        <article class="alert-item">
          <p class="title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
          <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account} · ${item.date} · ${item.status || "未发送"} · ${item.alert_type}</div>
          <div class="subtle">点赞 ${formatNumber(item.previous_likes)} → ${formatNumber(item.current_likes)}，+${formatNumber(item.like_delta)}；评论 ${formatNumber(item.previous_comments)} → ${formatNumber(item.current_comments)}，+${formatNumber(item.comment_delta)}</div>
          <div class="action-row">
            ${buildActionLink(item.profile_url, "账号主页")}
            ${buildActionLink(item.note_url, "作品详情")}
          </div>
        </article>
      `,
    )
    .join("");
}

async function addMonitoredAccounts() {
  const textarea = document.getElementById("accountInput");
  const projectInput = document.getElementById("projectInput");
  const resultNode = document.getElementById("addResult");
  const rawText = textarea.value.trim();
  const project = (projectInput.value || "").trim() || (getSelectedProjectName() !== "all" ? getSelectedProjectName() : "");
  if (!rawText) {
    resultNode.textContent = "先粘贴小红书主页链接。";
    return;
  }
  resultNode.textContent = "正在写入监测清单并触发同步...";
  const response = await fetch("/api/monitored-accounts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ raw_text: rawText, project }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `添加失败: ${response.status}`);
  }
  textarea.value = "";
  if (project) {
    state.monitorProjectFilter = project;
    state.activeAccountId = "";
    state.rankingScope = "all";
  }
  if (!project) {
    projectInput.value = "";
  }
  resultNode.textContent = `${payload.message}，当前共 ${payload.total} 个账号。`;
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function syncCurrentList() {
  const resultNode = document.getElementById("addResult");
  const monitoringEntries = getMonitoringEntries();
  const activeEntries = monitoringEntries.filter((entry) => entry && entry.active);
  const totalAccounts = activeEntries.length || Number(state.monitoring?.active_count || 0);
  state.activeAccountId = "";
  state.rankingScope = "all";
  resultNode.textContent = totalAccounts
    ? `正在同步全部项目，共 ${formatNumber(totalAccounts)} 个账号...`
    : "正在同步全部项目...";
  renderApp();
  const response = await fetch("/api/monitored-accounts/sync", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scope: "all" }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `同步失败: ${response.status}`);
  }
  resultNode.textContent =
    payload.message || (totalAccounts ? `已开始同步全部项目，共 ${formatNumber(totalAccounts)} 个账号。` : "已开始同步全部项目。");
  await loadMonitoring();
}

async function syncProject(project) {
  const normalizedProject = String(project || "").trim();
  if (!normalizedProject || normalizedProject === "all") {
    throw new Error("先进入一个项目，再更新当前项目");
  }
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = `正在同步项目「${normalizedProject}」...`;
  const response = await fetch("/api/monitored-accounts/sync-project", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ project: normalizedProject }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `项目同步失败: ${response.status}`);
  }
  state.monitorProjectFilter = normalizedProject;
  state.activeAccountId = "";
  state.rankingScope = "all";
  resultNode.textContent = payload.message || `已开始同步项目「${normalizedProject}」。`;
  await loadMonitoring();
}

async function syncActiveAccount() {
  const active = getActiveAccount();
  if (!active?.profile_url) {
    throw new Error("先点一个账号，再更新当前账号");
  }
  await retryMonitoredAccount(active.profile_url);
}

async function toggleMonitoredAccount(url, active) {
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = active ? "正在恢复账号监测..." : "正在暂停账号监测...";
  const response = await fetch("/api/monitored-accounts/toggle", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url, active }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `操作失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || "已更新监测状态。";
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function bulkToggleFilteredAccounts(active) {
  const filteredEntries = getFilteredMonitoringEntries();
  const resultNode = document.getElementById("addResult");
  if (!filteredEntries.length) {
    resultNode.textContent = "当前筛选结果为空，没有可批量处理的账号。";
    return;
  }
  const actionText = active ? "恢复" : "暂停";
  resultNode.textContent = `正在批量${actionText}筛选结果...`;
  const response = await fetch("/api/monitored-accounts/bulk-toggle", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      active,
      urls: filteredEntries.map((entry) => entry.url),
    }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `${actionText}失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || `已${actionText}筛选结果。`;
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function assignProjectToFilteredAccounts() {
  const filteredEntries = getFilteredMonitoringEntries();
  const projectInput = document.getElementById("monitorAssignProjectInput");
  const resultNode = document.getElementById("addResult");
  const project = (projectInput.value || "").trim();
  if (!filteredEntries.length) {
    resultNode.textContent = "当前筛选结果为空，没有可移动项目的账号。";
    return;
  }
  if (!project) {
    resultNode.textContent = "先输入目标项目名。";
    return;
  }
  resultNode.textContent = `正在把筛选结果移动到项目「${project}」...`;
  const response = await fetch("/api/monitored-accounts/project", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      project,
      urls: filteredEntries.map((entry) => entry.url),
    }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `项目调整失败: ${response.status}`);
  }
  state.monitorProjectFilter = project;
  state.activeAccountId = "";
  state.rankingScope = "all";
  projectInput.value = project;
  resultNode.textContent = payload.message || `已移动到项目「${project}」。`;
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function removeMonitoredAccount(url) {
  if (!window.confirm("确认从监测清单删除这个账号？删除后将不再自动采集。")) {
    return;
  }
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = "正在删除账号并更新监测清单...";
  const response = await fetch("/api/monitored-accounts/remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `删除失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || "已删除账号。";
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function retryMonitoredAccount(url) {
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = "正在重试该账号...";
  const response = await fetch("/api/monitored-accounts/retry", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `重试失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || "已开始重试该账号。";
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function checkLoginState({ silent = false } = {}) {
  const resultNode = document.getElementById("addResult");
  if (!silent) {
    resultNode.textContent = "正在执行登录态自检...";
  }
  const response = await fetch("/api/login-state/check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `登录态自检失败: ${response.status}`);
  }
  if (!silent) {
    resultNode.textContent = payload.message || "已开始登录态自检。";
  }
  await loadMonitoring();
}

async function retryFeishuUpload({ scope = "full", useSelectedProject = true } = {}) {
  throw new Error("旧的外部协作上传入口已移除。");
}

document.getElementById("refreshButton").addEventListener("click", () => loadDashboard(true));
document.getElementById("manualUpdateButton").addEventListener("click", () => {
  syncCurrentList().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("manualUpdateProjectButton").addEventListener("click", () => {
  syncProject(getSelectedProjectName()).catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("manualUpdateAccountButton").addEventListener("click", () => {
  syncActiveAccount().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("addAccountButton").addEventListener("click", () => {
  addMonitoredAccounts().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("syncAllButton").addEventListener("click", () => {
  syncCurrentList().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("syncNowButton").addEventListener("click", () => {
  syncProject(getSelectedProjectName()).catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("syncAccountButton").addEventListener("click", () => {
  syncActiveAccount().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("checkLoginStateButton").addEventListener("click", () => {
  checkLoginState().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("monitorPanelToggle").addEventListener("click", () => {
  state.monitorPanelExpanded = !state.monitorPanelExpanded;
  renderMonitoring();
});
document.getElementById("calendarPrevMonth").addEventListener("click", () => {
  const projectName = getSelectedProjectName();
  if (projectName === "all") return;
  const calendarState = buildProjectCalendarState(projectName);
  const monthIndex = calendarState.months.indexOf(state.calendarMonth);
  if (monthIndex > 0) {
    state.calendarMonth = calendarState.months[monthIndex - 1];
    state.calendarSelectedDate = "";
    renderProjectCalendar();
  }
});
document.getElementById("calendarNextMonth").addEventListener("click", () => {
  const projectName = getSelectedProjectName();
  if (projectName === "all") return;
  const calendarState = buildProjectCalendarState(projectName);
  const monthIndex = calendarState.months.indexOf(state.calendarMonth);
  if (monthIndex >= 0 && monthIndex < calendarState.months.length - 1) {
    state.calendarMonth = calendarState.months[monthIndex + 1];
    state.calendarSelectedDate = "";
    renderProjectCalendar();
  }
});
const exportAccountRankingButton = document.getElementById("exportAccountRankingButton");
if (exportAccountRankingButton) {
  exportAccountRankingButton.addEventListener("click", () => {
    exportCurrentAccountRankings().catch((error) => {
      document.getElementById("accountExportResult").textContent = error.message;
    });
  });
}
const exportProjectRankingButton = document.getElementById("exportProjectRankingButton");
if (exportProjectRankingButton) {
  exportProjectRankingButton.addEventListener("click", () => {
    exportCurrentProjectRankings().catch((error) => {
      document.getElementById("accountExportResult").textContent = error.message;
    });
  });
}
document.getElementById("monitorSearchInput").addEventListener("input", (event) => {
  state.monitorQuery = event.target.value || "";
  resetMonitoringPage();
  renderMonitoring();
});
document.getElementById("monitorProjectFilter").addEventListener("change", (event) => {
  state.monitorProjectFilter = event.target.value || "all";
  state.activeAccountId = "";
  state.rankingScope = "all";
  resetMonitoringPage();
  ensureActiveAccount();
  renderMonitoring();
  renderApp();
});
document.getElementById("monitorPageSize").addEventListener("change", (event) => {
  state.monitorPageSize = Number(event.target.value || 30);
  resetMonitoringPage();
  renderMonitoring();
});
document.querySelectorAll(".monitor-filter-button").forEach((button) => {
  button.addEventListener("click", () => {
    state.monitorFilter = button.dataset.monitorFilter || "all";
    resetMonitoringPage();
    renderMonitoring();
  });
});
document.getElementById("monitorPrevPage").addEventListener("click", () => {
  state.monitorPage = Math.max(1, state.monitorPage - 1);
  renderMonitoring();
});
document.getElementById("monitorNextPage").addEventListener("click", () => {
  state.monitorPage += 1;
  renderMonitoring();
});
document.getElementById("pauseFilteredButton").addEventListener("click", () => {
  bulkToggleFilteredAccounts(false).catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("resumeFilteredButton").addEventListener("click", () => {
  bulkToggleFilteredAccounts(true).catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("assignProjectButton").addEventListener("click", () => {
  assignProjectToFilteredAccounts().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("reloadSystemConfigButton").addEventListener("click", () => {
  loadSystemConfig()
    .then(() => {
      document.getElementById("systemConfigResult").textContent = "已导入当前项目配置";
    })
    .catch((error) => {
      document.getElementById("systemConfigResult").textContent = error.message;
    });
});
document.getElementById("saveSystemConfigButton").addEventListener("click", () => {
  saveSystemConfig().catch((error) => {
    document.getElementById("systemConfigResult").textContent = error.message;
  });
});
document.getElementById("pushServerCacheButton").addEventListener("click", () => {
  document.getElementById("systemConfigResult").textContent = "推送中...";
  pushServerCache().catch((error) => {
    document.getElementById("systemConfigResult").textContent = error.message;
  });
});
document.querySelectorAll(".scope-button").forEach((button) => {
  button.addEventListener("click", () => {
    state.rankingScope = button.dataset.rankingScope || "account";
    renderApp();
  });
});
document.querySelectorAll(".trend-window-button").forEach((button) => {
  button.addEventListener("click", () => {
    state.trendWindow = Number(button.dataset.trendWindow || 7);
    renderApp();
  });
});

loadLastServerPush();

window.setInterval(() => {
  renderManualUpdateState(state.monitoring?.sync_status || {});
}, 1000);

Promise.all([loadDashboard(), loadMonitoring(), loadSystemConfig()]).catch((error) => {
  const statsNode = document.getElementById("projectHomeStats");
  if (statsNode) {
    statsNode.innerHTML = `<div class="empty-state">${error.message}</div>`;
  }
  document.getElementById("monitorList").innerHTML = `<div class="empty-state">${error.message}</div>`;
});
