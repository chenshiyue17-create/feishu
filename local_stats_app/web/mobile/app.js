const params = new URLSearchParams(window.location.search);
const apiBase = (params.get("api_base") || "").replace(/\/$/, "");
const selectedProject = (params.get("project") || "默认项目").trim();
const dashboardUrl = `${apiBase}/api/mobile-rankings?project=${encodeURIComponent(selectedProject)}`;
let calendarExpanded = false;

function formatNumber(value) {
  return new Intl.NumberFormat("zh-CN").format(Number(value || 0));
}

function formatDateTime(value) {
  if (!value) return "未更新";
  return String(value).replace("T", " ").slice(0, 19);
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
  root.innerHTML = data.map((item) => `
    <div class="calendar-card">
      <p class="calendar-date">${item.date || "未知日期"}</p>
      <div class="calendar-meta">
        <span class="calendar-chip">账号 ${formatNumber(item.accounts)}</span>
        <span class="calendar-chip">点赞 ${formatNumber(item.likes)}</span>
        <span class="calendar-chip">评论 ${formatNumber(item.comments)}</span>
        <span class="calendar-chip">作品 ${formatNumber(item.works)}</span>
      </div>
    </div>
  `).join("");
}

function bindCalendarToggle() {
  const button = document.getElementById("calendarToggleButton");
  button.addEventListener("click", () => {
    calendarExpanded = !calendarExpanded;
    document.getElementById("calendarList").classList.toggle("is-collapsed", !calendarExpanded);
  });
}

async function loadDashboard() {
  const statusCard = document.getElementById("statusCard");
  statusCard.textContent = "正在加载榜单...";
  try {
    const response = await fetch(dashboardUrl, { credentials: "omit" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    document.getElementById("pageTitle").textContent = `${selectedProject} 排行榜`;
    document.getElementById("pageSummary").textContent =
      `更新时间 ${formatDateTime(payload.updated_at || payload.generated_at)} · 含历史日历与 3 个主榜单`;
    statusCard.textContent =
      `数据已加载：${formatDateTime(payload.updated_at || payload.generated_at)}。点击榜单卡片会直接跳转到小红书作品或账号主页。`;
    const rankings = payload.rankings || {};
    renderCalendar(payload.calendar || []);
    renderList("likesList", "likesCount", rankings.likes || [], "点赞");
    renderList("commentsList", "commentsCount", rankings.comments || [], "评论");
    renderList("growthList", "growthCount", rankings.growth || [], "增长");
  } catch (error) {
    statusCard.textContent = `加载失败：${error.message}`;
    ["calendarList", "likesList", "commentsList", "growthList"].forEach((id) => {
      document.getElementById(id).innerHTML = '<div class="empty-state">加载失败</div>';
    });
  }
}

document.getElementById("refreshButton").addEventListener("click", loadDashboard);
bindCalendarToggle();
loadDashboard();
