const params = new URLSearchParams(window.location.search);
const apiBase = (params.get("api_base") || "").replace(/\/$/, "");
let selectedProject = (params.get("project") || "默认项目").trim();
let calendarExpanded = false;
let selectedHistoryDate = "";

function buildDashboardUrl() {
  return `${apiBase}/api/mobile-rankings?project=${encodeURIComponent(selectedProject)}`;
}

function updateUrlProject(project) {
  const next = new URL(window.location.href);
  next.searchParams.set("project", project);
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

function renderCalendar(rows) {
  const root = document.getElementById("calendarList");
  const count = document.getElementById("calendarCount");
  const data = (rows || []).slice().reverse();
  count.textContent = String(data.length);
  root.classList.toggle("is-collapsed", !calendarExpanded);
  if (!data.length) {
    root.innerHTML = '<div class="empty-state">当前没有历史留底</div>';
    return;
  }
  if (!selectedHistoryDate || !data.some((item) => item.date === selectedHistoryDate)) {
    selectedHistoryDate = data[0].date || "";
  }
  root.innerHTML = data.map((item) => `
    <button class="calendar-card ${item.date === selectedHistoryDate ? "is-active" : ""}" type="button" data-history-date="${item.date || ""}">
      <p class="calendar-date">${item.date || "未知日期"}</p>
      <div class="calendar-meta">
        <span class="calendar-chip">账号 ${formatNumber(item.accounts)}</span>
        <span class="calendar-chip">点赞 ${formatNumber(item.likes)}</span>
        <span class="calendar-chip">评论 ${formatNumber(item.comments)}</span>
        <span class="calendar-chip">作品 ${formatNumber(item.works)}</span>
      </div>
    </button>
  `).join("");
  root.querySelectorAll("[data-history-date]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedHistoryDate = String(button.getAttribute("data-history-date") || "").trim();
      renderCalendar(rows);
      renderHistoryDetails(window.__mobilePayload || {});
    });
  });
}

function renderHistoryDetails(payload) {
  const detail = (payload.history_rankings || {})[selectedHistoryDate] || {};
  document.getElementById("historyDetailDate").textContent = selectedHistoryDate || "-";
  document.getElementById("historyDetailTitle").textContent = selectedHistoryDate ? `${selectedHistoryDate} 排行榜` : "当天排行榜";
  document.getElementById("historyDetailSummary").textContent = selectedHistoryDate
    ? `${detail.snapshot_time || selectedHistoryDate} · ${formatNumber(detail.account_count || 0)} 个账号`
    : "点历史日历中的某一天，查看当天榜单";
  renderList("historyLikesList", "historyLikesCount", detail.likes || [], "点赞");
  renderList("historyCommentsList", "historyCommentsCount", detail.comments || [], "评论");
  renderList("historyGrowthList", "historyGrowthCount", detail.growth || [], "增长");
}

function bindCalendarToggle() {
  const button = document.getElementById("calendarToggleButton");
  button.addEventListener("click", () => {
    calendarExpanded = !calendarExpanded;
    document.getElementById("calendarList").classList.toggle("is-collapsed", !calendarExpanded);
  });
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

async function loadDashboard() {
  const statusCard = document.getElementById("statusCard");
  statusCard.textContent = "正在加载榜单...";
  try {
    const response = await fetch(buildDashboardUrl(), { credentials: "omit" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    window.__mobilePayload = payload;
    renderProjectOptions(payload.projects || []);
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
document.getElementById("projectSelect").addEventListener("change", (event) => {
  selectedProject = String(event.target.value || "").trim() || selectedProject;
  loadDashboard();
});
bindCalendarToggle();
loadDashboard();
