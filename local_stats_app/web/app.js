const state = {
  payload: null,
  monitoring: null,
  activeAccountId: "",
  rankingScope: "account",
  trendWindow: 7,
  monitorQuery: "",
  monitorFilter: "all",
  monitorProjectFilter: "all",
  monitorPage: 1,
  monitorPageSize: 30,
  pollTimer: null,
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

function formatSignedNumber(value) {
  const number = Number(value || 0);
  if (number > 0) return `+${formatNumber(number)}`;
  if (number < 0) return `-${formatNumber(Math.abs(number))}`;
  return "0";
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

function buildCoverMarkup(item, { size = "hero", rank = 1 } = {}) {
  const wrapperClass = size === "hero" ? "ranking-hero-cover" : "ranking-mini-cover";
  const imageClass = size === "hero" ? "ranking-hero-cover-image" : "ranking-mini-cover-image";
  const openTag = item.note_url
    ? `<a class="${wrapperClass}" href="${item.note_url}" target="_blank" rel="noreferrer">`
    : `<div class="${wrapperClass}">`;
  const closeTag = item.note_url ? "</a>" : "</div>";
  const imageMarkup = item.cover_url
    ? `<img class="${imageClass}" src="${buildCoverSrc(item.cover_url)}" alt="作品封面" loading="lazy" referrerpolicy="no-referrer" onerror="this.hidden=true;this.nextElementSibling.hidden=false;" />`
    : "";
  const placeholderMarkup = `<div class="ranking-cover-placeholder"${item.cover_url ? " hidden" : ""}>暂无封面</div>`;
  return `
    ${openTag}
      ${imageMarkup}
      ${placeholderMarkup}
      <div class="rank-badge">${rank}</div>
    ${closeTag}
  `;
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
  renderMonitoring();
  schedulePolling();
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
  renderMeta();
  renderAccountFocus();
  renderPortalCards();
  renderTrendWindowTabs();
  renderTrendChart();
  renderRankingScopeTabs();
  renderRankingList();
  renderAccounts();
  renderAlerts();
}

function ensureActiveAccount() {
  const accounts = getVisibleAccounts();
  if (!accounts.length) {
    state.activeAccountId = "";
    return;
  }
  const exists = accounts.some((item) => item.account_id === state.activeAccountId);
  if (!state.activeAccountId || !exists) {
    state.activeAccountId = accounts[0].account_id;
  }
}

function getActiveAccount() {
  const accounts = getVisibleAccounts();
  return accounts.find((item) => item.account_id === state.activeAccountId) || accounts[0] || null;
}

function getActiveSeries() {
  const active = getActiveAccount();
  if (!active) return [];
  return (state.payload?.account_series || {})[active.account_id] || [];
}

function getMonitoringEntries() {
  return state.monitoring?.entries || [];
}

function getSelectedProjectName() {
  return state.monitorProjectFilter || "all";
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

function getVisibleAccounts() {
  const accounts = state.payload?.accounts || [];
  const projectName = getSelectedProjectName();
  if (projectName === "all") {
    return accounts;
  }
  const accountIds = getProjectAccountIds(projectName);
  return accounts.filter((item) => accountIds.has(item.account_id));
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
  const root = document.getElementById("accountFilterBar");
  const titleNode = document.getElementById("activeAccountTitle");
  const summaryNode = document.getElementById("activeAccountSummary");
  const accounts = getVisibleAccounts();
  const active = getActiveAccount();
  if (!accounts.length || !active) {
    const projectName = getSelectedProjectName();
    titleNode.textContent = projectName === "all" ? "当前账号" : `项目：${projectName}`;
    summaryNode.textContent = projectName === "all" ? "暂无账号快照" : "当前项目下暂无已同步账号";
    root.innerHTML = `<div class="empty-state">${projectName === "all" ? "暂无可切换账号。" : "当前项目下暂无可切换账号。"}</div>`;
    return;
  }
  titleNode.textContent = active.account;
  summaryNode.textContent =
    getSelectedProjectName() === "all"
      ? active.weekly_summary || "当前账号暂无周对比摘要"
      : `项目：${getSelectedProjectName()} · ${active.weekly_summary || "当前账号暂无周对比摘要"}`;
  root.innerHTML = accounts
    .map(
      (item) => `
        <button class="account-filter-button ${item.account_id === active.account_id ? "is-active" : ""}" data-account-id="${item.account_id}">
          ${item.account}
        </button>
      `,
    )
    .join("");
  root.querySelectorAll(".account-filter-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeAccountId = button.dataset.accountId || "";
      renderApp();
    });
  });
}

function renderMonitoring() {
  const monitoring = state.monitoring || {};
  const syncStatus = monitoring.sync_status || {};
  const profileLookupError = monitoring.profile_lookup_error || "";
  const loginState = monitoring.login_state || {};
  const proxyPool = monitoring.proxy_pool || {};
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
              <span class="monitor-summary-chip ${entry.fetch_state === "error" ? "is-error" : entry.fetch_state === "ok" ? "is-success" : ""}">${entry.fetch_message || "等待首次同步"}</span>
            </div>
            <div class="monitor-link-text">${truncateMiddle(entry.url, 96)}</div>
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

  if (syncStatus?.state !== "running" && !progress.phase) {
    root.innerHTML = `<div class="sync-progress-empty">当前没有进行中的同步任务。</div>`;
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
        <div class="sync-progress-title">同步进度</div>
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
    loginState?.sample_url ? `样本 ${sampleLabel}` : "待选样本账号",
    detailText,
    `上次自检 ${checkedAt}`,
  ].filter(Boolean);
  const hintLines = (loginState?.hints || []).slice(0, 2);
  root.innerHTML = `
    <div class="login-state-top">
      <span class="login-state-badge is-${status}">${getLoginStateText(status)}</span>
      ${loginState?.checking ? '<span class="login-state-meta">正在后台自检...</span>' : ""}
    </div>
    <div class="login-state-message">${loginState?.message || "等待自动自检"}</div>
    <div class="login-state-chip-row">
      ${chips.map((text) => `<span class="login-state-chip">${text}</span>`).join("")}
    </div>
    ${hintLines.length ? `<div class="login-state-hints">${hintLines.map((text) => `<span>${text}</span>`).join("")}</div>` : ""}
  `;
}

function renderProxyPool(proxyPool) {
  const root = document.getElementById("proxyPoolCard");
  if (!root) return;
  if (!proxyPool?.enabled) {
    root.innerHTML = `<div class="proxy-pool-empty">当前未启用 IP 池，采集请求将直接使用本机网络。</div>`;
    return;
  }
  const chips = [
    `总数 ${formatNumber(proxyPool.total || 0)}`,
    `可用 ${formatNumber(proxyPool.ready_count || 0)}`,
    `冷却 ${formatNumber(proxyPool.cooling_count || 0)}`,
    proxyPool.last_selected_proxy ? `最近使用 ${truncateMiddle(proxyPool.last_selected_proxy, 36)}` : "",
    proxyPool.updated_at ? `更新时间 ${formatDateTime(proxyPool.updated_at)}` : "",
  ].filter(Boolean);
  const items = (proxyPool.entries || []).slice(0, 6);
  root.innerHTML = `
    <div class="proxy-pool-top">
      <div class="proxy-pool-title">IP 池状态</div>
      <div class="proxy-pool-subtitle">${proxyPool.last_error ? proxyPool.last_error : "最近没有代理错误"}</div>
    </div>
    <div class="proxy-pool-chip-row">
      ${chips.map((text) => `<span class="proxy-pool-chip">${text}</span>`).join("")}
    </div>
    ${
      items.length
        ? `<div class="proxy-pool-list">
            ${items
              .map(
                (item) => `
                  <div class="proxy-pool-item">
                    <div class="proxy-pool-item-main">
                      <span class="proxy-pool-state is-${item.state}">${item.state === "ready" ? "可用" : `冷却 ${formatDurationShort(item.cooldown_seconds_remaining || 0)}`}</span>
                      <span class="proxy-pool-url">${truncateMiddle(item.proxy_url, 48)}</span>
                    </div>
                    <div class="proxy-pool-item-meta">
                      <span>成功 ${item.last_success_at ? formatDateTime(item.last_success_at) : "暂无"}</span>
                      <span>失败 ${formatNumber(item.failure_count || 0)}</span>
                    </div>
                  </div>
                `,
              )
              .join("")}
          </div>`
        : `<div class="proxy-pool-empty">代理池已启用，但当前还没有采集行为。</div>`
    }
  `;
}

function renderProjectCards(projects, syncStatus) {
  const root = document.getElementById("projectOverview");
  if (!projects.length) {
    root.innerHTML = "";
    return;
  }
  const manualUpdateState = getManualUpdateState(syncStatus);
  root.innerHTML = projects
    .map((project) => {
      const entries = getProjectEntries(project.name);
      const previewNames = entries
        .map((entry) => entry.display_name)
        .filter(Boolean)
        .slice(0, 3);
      const isActive = getSelectedProjectName() === project.name;
      return `
        <article class="project-card ${isActive ? "is-active" : ""}">
          <div class="project-card-top">
            <div>
              <div class="project-card-name">${project.name}</div>
              <div class="project-card-meta">${formatNumber(project.total)} 个账号 · 监测中 ${formatNumber(project.active_count)} · 暂停 ${formatNumber(project.paused_count)}</div>
            </div>
            <span class="project-card-badge">${isActive ? "当前项目" : "项目"}</span>
          </div>
          <div class="project-card-preview">${previewNames.length ? previewNames.join(" / ") : "暂无账号"}</div>
          <div class="project-card-actions">
            <button class="monitor-inline-button project-open-button" data-project="${project.name}" type="button">
              ${isActive ? "查看全部项目" : "进入项目"}
            </button>
            <button class="monitor-inline-button project-sync-button" data-project="${project.name}" type="button" ${manualUpdateState.disabled ? "disabled" : ""}>
              ${manualUpdateState.projectButtonText}
            </button>
          </div>
          ${
            syncStatus.summary?.total_accounts
              ? `<div class="project-card-foot subtle">最近同步结果：${formatNumber(syncStatus.summary.total_accounts)} 账号 / ${formatNumber(syncStatus.summary.total_works || 0)} 作品</div>`
              : ""
          }
        </article>
      `;
    })
    .join("");

  root.querySelectorAll(".project-open-button").forEach((button) => {
    button.addEventListener("click", () => {
      const nextProject = button.dataset.project || "all";
      state.monitorProjectFilter = getSelectedProjectName() === nextProject ? "all" : nextProject;
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
  let cooldownSeconds = Number(syncStatus?.manual_cooldown_seconds_remaining || 0);
  if (syncStatus?.manual_available_at) {
    const availableAt = new Date(syncStatus.manual_available_at).getTime();
    if (!Number.isNaN(availableAt)) {
      cooldownSeconds = Math.max(0, Math.ceil((availableAt - Date.now()) / 1000));
    }
  }
  const disabled = running || cooldownSeconds > 0;
  const buttonText = running ? "更新中..." : cooldownSeconds > 0 ? `冷却 ${formatDurationShort(cooldownSeconds)}` : "立即更新数据";
  const projectButtonText = running ? "更新中" : cooldownSeconds > 0 ? "冷却中" : "更新项目";
  const helperText = running
    ? "当前正在更新数据，为避免重复请求，手动更新按钮已临时锁定。"
    : cooldownSeconds > 0
      ? `为降低小红书风控，手动更新冷却中，剩余 ${formatDurationShort(cooldownSeconds)}。每天 14:00 自动更新不受影响。`
      : "手动更新有冷却保护，避免过于频繁触发小红书检测。每天 14:00 自动更新保持不变。";
  return { disabled, buttonText, projectButtonText, helperText, cooldownSeconds };
}

function renderManualUpdateState(syncStatus) {
  const { disabled, buttonText, projectButtonText, helperText } = getManualUpdateState(syncStatus);
  const syncButton = document.getElementById("syncNowButton");
  const heroButton = document.getElementById("manualUpdateButton");
  const hintNode = document.getElementById("syncCooldownText");
  if (syncButton) {
    syncButton.disabled = disabled;
    syncButton.textContent = buttonText;
  }
  if (heroButton) {
    heroButton.disabled = disabled;
    heroButton.textContent = buttonText;
  }
  document.querySelectorAll(".project-sync-button").forEach((button) => {
    button.disabled = disabled;
    button.textContent = projectButtonText;
  });
  if (hintNode) {
    hintNode.textContent = helperText;
  }
}

function renderMeta() {
  const payload = state.payload;
  document.getElementById("updatedAt").textContent = `数据更新时间：${formatDateTime(payload.updated_at || payload.generated_at)}${payload.stale ? " · 当前显示缓存" : ""}`;
  document.getElementById("latestDate").textContent = `最新留底：${payload.latest_date || "-"}`;
  const active = getActiveAccount();
  document.getElementById("weeklySummary").textContent = active?.weekly_summary || "暂无周对比摘要";
  const seriesMeta = payload.series_meta || {};
  document.getElementById("trendModeChip").textContent = `${seriesMeta.mode === "daily" ? "日更" : "趋势"}${seriesMeta.update_time ? ` · ${seriesMeta.update_time}` : ""}`;
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
  const active = getActiveAccount();
  const fullSeries = getActiveSeries();
  const windowGrowth = buildWindowGrowth(fullSeries, state.trendWindow);
  if (!active) {
    document.getElementById("portalCards").innerHTML = `<div class="empty-state">暂无账号指标。</div>`;
    return;
  }
  const growthLabel = windowGrowth?.label || "成长窗口";
  const rangeText = windowGrowth ? `${windowGrowth.start_date} → ${windowGrowth.end_date}` : "历史不足，暂不显示增长";
  const worksDisplay = active.works_display || formatNumber(active.works);
  const worksHint =
    active.works_exact === false
      ? "当前账号已抓取作品下限，账号总量尚未完全展开"
      : "当前账号总作品数";
  const cards = [
    ["当前粉丝", formatNumber(active.fans), "当前账号粉丝规模"],
    ["当前获赞收藏", formatNumber(active.interaction), "当前账号公开页获赞收藏"],
    ["当前作品数", worksDisplay, worksHint],
    ["当前评论总数", formatNumber(active.comments), "当前账号首页可见作品评论合计"],
    [`${growthLabel}粉丝增量`, windowGrowth ? formatSignedNumber(windowGrowth.fans) : "-", rangeText],
    [`${growthLabel}获赞增量`, windowGrowth ? formatSignedNumber(windowGrowth.interaction) : "-", rangeText],
    [
      `${growthLabel}作品增量`,
      windowGrowth && active.works_exact !== false ? formatSignedNumber(windowGrowth.works) : "-",
      active.works_exact === false ? "当前账号作品总数未完全展开，暂不显示作品增长" : rangeText,
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
  const rankings = state.payload?.rankings || {};
  const active = getActiveAccount();
  const root = document.getElementById("rankingList");
  const rankingTitle = document.getElementById("rankingTitle");
  const projectName = getSelectedProjectName();
  rankingTitle.textContent =
    state.rankingScope === "all"
      ? `榜单中心 · ${projectName === "all" ? "所有账号" : `项目 ${projectName}`}`
      : `榜单中心 · ${active ? active.account : projectName === "all" ? "当前账号" : `项目 ${projectName} 暂无已同步账号`}`;
  root.innerHTML = rankingConfigs
    .map((config) => renderRankingColumn(config, rankings[config.type] || [], active))
    .join("");
}

function renderRankingColumn(config, allRows, active) {
  const projectName = getSelectedProjectName();
  const projectAccountIds = getProjectAccountIds(projectName);
  let filteredRows = [];
  if (state.rankingScope === "all") {
    filteredRows =
      projectName === "all"
        ? allRows
        : allRows.filter((item) => projectAccountIds.has(item.account_id));
  } else if (active) {
    filteredRows = allRows.filter((item) => item.account_id === active.account_id);
  }
  const rows = filteredRows.slice(0, 10);
  if (!rows.length) {
    return `
      <section class="ranking-column">
        <div class="ranking-column-header">
          <h3>${config.title}</h3>
          <span class="ranking-column-meta">Top 10</span>
        </div>
        <div class="empty-state">暂无数据</div>
      </section>
    `;
  }
  return `
    <section class="ranking-column">
      <div class="ranking-column-header">
        <h3>${config.title}</h3>
        <span class="ranking-column-meta">Top 10</span>
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
  return `
    <article class="ranking-hero">
      ${buildCoverMarkup(item, { size: "hero", rank: 1 })}
      <div class="ranking-hero-body">
        <div class="ranking-hero-meta">
          <div class="ranking-hero-text">
            <p class="ranking-hero-title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
            <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</div>
          </div>
          <div class="ranking-hero-metric">
            <div class="metric-label">${config.metricLabel}</div>
            <div class="metric-value">${formatNumber(item.metric)}</div>
          </div>
        </div>
        <div class="subtle">${item.summary || "当前榜单头部内容"}</div>
        <div class="action-row">
          ${buildActionLink(item.profile_url, "账号主页")}
          ${buildActionLink(item.note_url, "作品详情")}
        </div>
      </div>
    </article>
  `;
}

function renderRankingMiniItem(config, item, rank) {
  return `
    <article class="ranking-mini-item">
      ${buildCoverMarkup(item, { size: "mini", rank })}
      <div class="ranking-mini-body">
        <p class="title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
        <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account}</div>
        <div class="ranking-mini-summary">${item.summary || ""}</div>
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
      renderApp();
    });
  });
}

function renderAlerts() {
  const active = getActiveAccount();
  const projectName = getSelectedProjectName();
  const projectAccountIds = getProjectAccountIds(projectName);
  const allAlerts = state.payload?.alerts || [];
  const alerts = active
    ? allAlerts.filter((item) => item.account_id === active.account_id)
    : projectName === "all"
      ? allAlerts
      : allAlerts.filter((item) => projectAccountIds.has(item.account_id));
  const root = document.getElementById("alertsList");
  if (!alerts.length) {
    root.innerHTML = `<div class="empty-state">${active ? "当前账号没有评论预警。" : projectName === "all" ? "暂无评论预警。" : "当前项目下暂无评论预警。"}</div>`;
    return;
  }
  root.innerHTML = alerts
    .map(
      (item) => `
        <article class="alert-item">
          <p class="title">${item.note_url ? `<a class="note-link" href="${item.note_url}" target="_blank" rel="noreferrer">${item.title}</a>` : item.title}</p>
          <div class="subtle">${item.profile_url ? `<a class="note-link subtle-link" href="${item.profile_url}" target="_blank" rel="noreferrer">${item.account}</a>` : item.account} · ${item.date} · ${item.status || "未发送"}</div>
          <div class="subtle">评论 ${formatNumber(item.previous_comments)} → ${formatNumber(item.current_comments)}，+${formatNumber(item.delta)}，${item.rate || 0}%</div>
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
  }
  if (!project) {
    projectInput.value = "";
  }
  resultNode.textContent = `${payload.message}，当前共 ${payload.total} 个账号。`;
  await Promise.all([loadMonitoring(), loadDashboard(true)]);
}

async function syncCurrentList() {
  if (getSelectedProjectName() !== "all") {
    await syncProject(getSelectedProjectName());
    return;
  }
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = "正在触发同步...";
  const response = await fetch("/api/monitored-accounts/sync", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `同步失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || "已开始同步当前监测清单。";
  await loadMonitoring();
}

async function syncProject(project) {
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = `正在同步项目「${project}」...`;
  const response = await fetch("/api/monitored-accounts/sync-project", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ project }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `项目同步失败: ${response.status}`);
  }
  state.monitorProjectFilter = project || "all";
  resultNode.textContent = payload.message || `已开始同步项目「${project}」。`;
  await loadMonitoring();
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

async function checkLoginState() {
  const resultNode = document.getElementById("addResult");
  resultNode.textContent = "正在执行登录态自检...";
  const response = await fetch("/api/login-state/check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `登录态自检失败: ${response.status}`);
  }
  resultNode.textContent = payload.message || "已开始登录态自检。";
  await loadMonitoring();
}

document.getElementById("refreshButton").addEventListener("click", () => loadDashboard(true));
document.getElementById("manualUpdateButton").addEventListener("click", () => {
  syncCurrentList().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("addAccountButton").addEventListener("click", () => {
  addMonitoredAccounts().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("syncNowButton").addEventListener("click", () => {
  syncCurrentList().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("checkLoginStateButton").addEventListener("click", () => {
  checkLoginState().catch((error) => {
    document.getElementById("addResult").textContent = error.message;
  });
});
document.getElementById("monitorSearchInput").addEventListener("input", (event) => {
  state.monitorQuery = event.target.value || "";
  resetMonitoringPage();
  renderMonitoring();
});
document.getElementById("monitorProjectFilter").addEventListener("change", (event) => {
  state.monitorProjectFilter = event.target.value || "all";
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

window.setInterval(() => {
  renderManualUpdateState(state.monitoring?.sync_status || {});
}, 1000);

Promise.all([loadDashboard(), loadMonitoring()]).catch((error) => {
  document.getElementById("portalCards").innerHTML = `<div class="empty-state">${error.message}</div>`;
  document.getElementById("monitorList").innerHTML = `<div class="empty-state">${error.message}</div>`;
});
