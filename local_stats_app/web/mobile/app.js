const params = new URLSearchParams(window.location.search);
const apiBase = (params.get("api_base") || "").replace(/\/$/, "");
let selectedProject = (params.get("project") || "默认项目").trim();
let selectedAccountId = (params.get("account_id") || "all").trim() || "all";
let selectedHistoryDate = "";

function buildDashboardUrl() {
  return `${apiBase}/api/mobile-rankings?project=${encodeURIComponent(selectedProject)}`;
}

function updateUrlProject(project) {
  const next = new URL(window.location.href);
  next.searchParams.set("project", project);
  if (selectedAccountId && selectedAccountId !== "all") {
    next.searchParams.set("account_id", selectedAccountId);
  } else {
    next.searchParams.delete("account_id");
  }
  window.history.replaceState({}, "", next.toString());
}

function formatNumber(value) {
  return new Intl.NumberFormat("zh-CN").format(Number(value || 0));
}

function formatDateTime(value) {
  if (!value) return "未更新";
  return String(value).replace("T", " ").slice(0, 19);
}

function buildMobileStatusSummary(payload) {
  const serverReceivedAt = payload.server_received_at || "";
  const updatedAt = payload.updated_at || payload.generated_at || "";
  const latestDate = payload.latest_date || "";
  const accountCount = Number(payload.account_count || 0);
  const pieces = [];
  if (serverReceivedAt) {
    pieces.push(`服务器接收 ${formatDateTime(serverReceivedAt)}`);
  } else if (updatedAt) {
    pieces.push(`缓存更新 ${formatDateTime(updatedAt)}`);
  }
  if (latestDate) {
    pieces.push(`最新留底 ${latestDate}`);
  }
  if (accountCount) {
    pieces.push(`${formatNumber(accountCount)} 个账号`);
  }
  return pieces.join(" · ");
}

function formatTimeLabel(value) {
  const text = String(value || "").trim();
  if (!text) return "等待加载";
  if (text.includes("T")) return formatDateTime(text);
  return text;
}

function applyVersion(payload) {
  const version = String(payload?.version || "").trim();
  if (version) {
    const node = document.getElementById("versionChip");
    if (node) node.textContent = version;
  }
}

function renderHeadline(payload, detail) {
  document.getElementById("headlineServerTime").textContent = formatTimeLabel(payload.server_received_at || payload.updated_at || payload.generated_at || "");
}

function renderList(rootId, countId, rows, metricLabel) {
  const root = document.getElementById(rootId);
  const count = document.getElementById(countId);
  const data = (rows || []).slice(0, 20);
  count.textContent = String(data.length);
  if (!data.length) {
    root.innerHTML = '<div class="empty-state">当前没有可显示的数据</div>';
    return;
  }
  root.innerHTML = data.map((item) => {
    const href = item.note_url || item.profile_url || "#";
    const safeHref = href === "#" ? "#" : href;
    return `
      <a class="rank-card" href="${safeHref}" target="_blank" rel="noreferrer">
        <div class="rank-index">${item.rank || "-"}</div>
        <div class="rank-body">
          <p class="rank-title">${item.title || "未命名作品"}</p>
          <div class="rank-meta">
            <span>${item.account || "未知账号"}</span>
            <span>${item.comment_is_lower_bound ? "评论为下限口径" : ""}</span>
          </div>
          <div class="rank-metric">${metricLabel} ${formatNumber(item.metric)}</div>
        </div>
      </a>
    `;
  }).join("");
}

function filterRowsByAccount(rows, accountId) {
  const normalized = String(accountId || "").trim();
  if (!normalized || normalized === "all") return (rows || []).slice();
  return (rows || []).filter((item) => String(item.account_id || "").trim() === normalized);
}

function renderCalendar(rows) {
  const select = document.getElementById("calendarDateSelect");
  const count = document.getElementById("calendarCount");
  const selectedLabel = document.getElementById("calendarSelectedDate");
  const data = (rows || []).slice().reverse();
  count.textContent = String(data.length);
  if (!data.length) {
    select.innerHTML = '<option value="">当前没有历史留底</option>';
    select.disabled = true;
    selectedLabel.textContent = "暂无日期";
    return;
  }
  select.disabled = false;
  const preferredDate = String((window.__mobilePayload || {}).latest_date || "").trim();
  if (!selectedHistoryDate || !data.some((item) => item.date === selectedHistoryDate)) {
    selectedHistoryDate = data.some((item) => item.date === preferredDate) ? preferredDate : (data[0].date || "");
  }
  select.innerHTML = data
    .map((item) => `<option value="${item.date || ""}">${item.date || "未知日期"}</option>`)
    .join("");
  select.value = selectedHistoryDate;
  selectedLabel.textContent = selectedHistoryDate || "未选择日期";
}

function renderHistoryDetails(payload) {
  const detail = (payload.history_rankings || {})[selectedHistoryDate] || {};
  renderHeadline(payload, detail);
  document.getElementById("historyDetailDate").textContent = selectedHistoryDate || "-";
  document.getElementById("historyDetailTitle").textContent = selectedHistoryDate ? `${selectedHistoryDate} 排行榜` : "当天排行榜";
  document.getElementById("historyDetailSummary").textContent = selectedHistoryDate
    ? `${detail.snapshot_time || selectedHistoryDate} · ${formatNumber(detail.account_count || 0)} 个账号`
    : "点历史日历中的某一天，查看当天榜单";
  renderList("historyLikesList", "historyLikesCount", detail.likes || [], "点赞");
  renderList("historyCommentsList", "historyCommentsCount", detail.comments || [], "评论");
  renderList("historyGrowthList", "historyGrowthCount", detail.growth || [], "增长");
  renderAccountDetails(payload, detail);
}

function renderProjectOptions(projects) {
  const select = document.getElementById("projectSelect");
  const options = (projects || []).filter(Boolean);
  if (!options.length) {
    select.innerHTML = `<option value="${selectedProject}">${selectedProject}</option>`;
    select.value = selectedProject;
    return;
  }
  if (!options.includes(selectedProject)) {
    selectedProject = options[0];
  }
  select.innerHTML = options
    .map((project) => `<option value="${project}">${project}</option>`)
    .join("");
  select.value = selectedProject;
}

function renderAccountOptions(accounts) {
  const select = document.getElementById("accountSelect");
  const items = (accounts || []).filter((item) => item && item.account_id);
  if (selectedAccountId !== "all" && !items.some((item) => item.account_id === selectedAccountId)) {
    selectedAccountId = "all";
  }
  select.innerHTML = [
    '<option value="all">全部账号</option>',
    ...items.map((item) => `<option value="${item.account_id}">${item.account || item.account_id}</option>`),
  ].join("");
  select.value = selectedAccountId;
}

function renderAccountDetails(payload, detail) {
  const accounts = payload.accounts || [];
  const activeAccount = accounts.find((item) => String(item.account_id || "") === selectedAccountId);
  const label = activeAccount ? (activeAccount.account || activeAccount.account_id) : "全部账号";
  document.getElementById("accountDetailTitle").textContent = activeAccount ? `${label} 账号内榜单` : "账号内榜单";
  document.getElementById("accountDetailBadge").textContent = label;
  document.getElementById("accountDetailSummary").textContent = activeAccount
    ? `${selectedHistoryDate || payload.latest_date || ""} · ${label}`
    : "选择一个账号，查看账号内榜单";
  renderList("accountLikesList", "accountLikesCount", filterRowsByAccount(detail.likes || [], selectedAccountId), "点赞");
  renderList("accountCommentsList", "accountCommentsCount", filterRowsByAccount(detail.comments || [], selectedAccountId), "评论");
  renderList("accountGrowthList", "accountGrowthCount", filterRowsByAccount(detail.growth || [], selectedAccountId), "增长");
}

async function exportLongImage() {
  const button = document.getElementById("exportLongImageButton");
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "导出中...";
  try {
    const htmlToImage = window.htmlToImage;
    if (!htmlToImage || typeof htmlToImage.toPng !== "function") {
      throw new Error("长图组件未加载完成");
    }
    const node = document.querySelector(".app-shell");
    const dataUrl = await htmlToImage.toPng(node, {
      cacheBust: true,
      pixelRatio: 2,
      backgroundColor: "#111318",
      skipFonts: true,
    });
    const selectedDate = selectedHistoryDate || (window.__mobilePayload || {}).latest_date || "latest";
    const link = document.createElement("a");
    link.href = dataUrl;
    link.download = `xhs-mobile-${selectedProject || "project"}-${selectedDate}.png`;
    document.body.appendChild(link);
    link.click();
    link.remove();
  } catch (error) {
    window.alert(`导出失败：${error.message}`);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
}

async function loadDashboard() {
  const statusCard = document.getElementById("statusCard");
  statusCard.textContent = "正在加载榜单...";
  try {
    const response = await fetch(buildDashboardUrl(), { credentials: "omit" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    window.__mobilePayload = payload;
    applyVersion(payload);
    renderProjectOptions(payload.projects || []);
    renderAccountOptions(payload.accounts || []);
    selectedProject = payload.project || selectedProject;
    updateUrlProject(selectedProject);
    document.getElementById("pageTitle").textContent = `${selectedProject} 排行榜`;
    document.getElementById("pageSummary").textContent =
      `${buildMobileStatusSummary(payload) || `缓存更新 ${formatDateTime(payload.updated_at || payload.generated_at)}`} · 本机采集后推送到服务器，手机端只查看这份缓存`;
    statusCard.textContent =
      `当前项目：${selectedProject}。手机端只读取服务器缓存，不采集、不上传。点击榜单卡片会直接跳转到小红书作品或账号主页。`;
    const rankings = payload.rankings || {};
    renderCalendar(payload.calendar || []);
    renderHistoryDetails(payload);
    renderList("likesList", "likesCount", rankings.likes || [], "点赞");
    renderList("commentsList", "commentsCount", rankings.comments || [], "评论");
    renderList("growthList", "growthCount", rankings.growth || [], "增长");
  } catch (error) {
    statusCard.textContent = `加载失败：${error.message}`;
    ["calendarList", "likesList", "commentsList", "growthList", "historyLikesList", "historyCommentsList", "historyGrowthList"].forEach((id) => {
      document.getElementById(id).innerHTML = '<div class="empty-state">加载失败</div>';
    });
  }
}

document.getElementById("refreshButton").addEventListener("click", loadDashboard);
document.getElementById("exportLongImageButton").addEventListener("click", exportLongImage);
document.getElementById("calendarDateSelect").addEventListener("change", (event) => {
  selectedHistoryDate = String(event.target.value || "").trim();
  document.getElementById("calendarSelectedDate").textContent = selectedHistoryDate || "未选择日期";
  renderHistoryDetails(window.__mobilePayload || {});
});
document.getElementById("projectSelect").addEventListener("change", (event) => {
  selectedProject = String(event.target.value || "").trim() || selectedProject;
  selectedAccountId = "all";
  loadDashboard();
});
document.getElementById("accountSelect").addEventListener("change", (event) => {
  selectedAccountId = String(event.target.value || "").trim() || "all";
  updateUrlProject(selectedProject);
  renderHistoryDetails(window.__mobilePayload || {});
});
loadDashboard();
