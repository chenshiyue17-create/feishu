const params = new URLSearchParams(window.location.search);
const apiBase = (params.get("api_base") || "").replace(/\/$/, "");
let selectedProject = (params.get("project") || "默认项目").trim();
let selectedAccountId = (params.get("account_id") || "all").trim() || "all";
let selectedHistoryDate = "";
let serverClockTimer = null;

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
  const latestDate = payload.latest_date || "";
  const accountCount = Number(payload.account_count || 0);
  const pieces = [];
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

function padNumber(value) {
  return String(value).padStart(2, "0");
}

function formatDateObject(value) {
  return [
    value.getFullYear(),
    padNumber(value.getMonth() + 1),
    padNumber(value.getDate()),
  ].join("-") + " " + [
    padNumber(value.getHours()),
    padNumber(value.getMinutes()),
    padNumber(value.getSeconds()),
  ].join(":");
}

function parseServerDate(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function startServerClock(value) {
  const node = document.getElementById("headlineServerTime");
  if (serverClockTimer) {
    window.clearInterval(serverClockTimer);
    serverClockTimer = null;
  }
  const baseDate = parseServerDate(value);
  if (!baseDate) {
    node.textContent = formatTimeLabel(value);
    return;
  }
  const startedAt = Date.now();
  const render = () => {
    const elapsed = Date.now() - startedAt;
    const current = new Date(baseDate.getTime() + elapsed);
    node.textContent = formatDateObject(current);
  };
  render();
  serverClockTimer = window.setInterval(render, 1000);
}

function applyVersion(payload) {
  const version = String(payload?.version || "").trim();
  if (version) {
    const node = document.getElementById("versionChip");
    if (node) node.textContent = version;
  }
}

function renderHeadline(payload, detail) {
  startServerClock(payload.server_time || payload.server_received_at || payload.updated_at || payload.generated_at || "");
}

function renderList(rootId, countId, rows, metricLabel, options = {}) {
  const root = document.getElementById(rootId);
  const count = document.getElementById(countId);
  const data = (rows || []).slice(0, 20);
  const displayRows = options.reindexRank
    ? data.map((item, index) => ({ ...item, rank: index + 1 }))
    : data;
  count.textContent = String(data.length);
  if (!data.length) {
    root.innerHTML = '<div class="empty-state">当前没有可显示的数据</div>';
    return;
  }
  root.innerHTML = displayRows.map((item) => {
    const href = item.note_url || item.profile_url || "#";
    const safeHref = href === "#" ? "#" : href;
    const basisLabel = String(item.comment_basis || "").trim() === "评论预览下限"
      ? "评论下限"
      : String(item.comment_basis || "").trim() === "旧缓存"
        ? "旧缓存"
        : "";
    return `
      <a class="rank-card" href="${safeHref}" target="_blank" rel="noreferrer">
        <div class="rank-index">${item.rank || "-"}</div>
        <div class="rank-body">
          <p class="rank-title">${item.title || "未命名作品"}</p>
          <div class="rank-meta">
            <span>${item.account || "未知账号"}</span>
            <span>${basisLabel}</span>
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
  const data = (rows || []).slice().reverse();
  if (!data.length) {
    select.innerHTML = '<option value="">当前没有历史留底</option>';
    select.disabled = true;
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
}

function renderHistoryDetails(payload) {
  const detail = (payload.history_rankings || {})[selectedHistoryDate] || {};
  const accounts = payload.accounts || [];
  const activeAccount = accounts.find((item) => String(item.account_id || "") === selectedAccountId);
  const scopeLabel = activeAccount
    ? (activeAccount.account || activeAccount.account_id || "账号")
    : (selectedProject || payload.project || "项目");
  const likesRows = filterRowsByAccount(detail.likes || [], selectedAccountId);
  const commentsRows = filterRowsByAccount(detail.comments || [], selectedAccountId);
  const growthRows = filterRowsByAccount(detail.growth || [], selectedAccountId);
  const scopeAccountCount = activeAccount ? (likesRows.length || commentsRows.length || growthRows.length ? 1 : 0) : Number(detail.account_count || 0);
  renderHeadline(payload, detail);
  document.getElementById("historyDetailTitle").textContent = selectedHistoryDate
    ? `${selectedHistoryDate} ${scopeLabel} 排行榜`
    : `${scopeLabel} 排行榜`;
  document.getElementById("historyDetailSummary").textContent = selectedHistoryDate
    ? `${detail.snapshot_time || selectedHistoryDate} · ${formatNumber(scopeAccountCount)} 个账号`
    : "点历史日历中的某一天，查看当天榜单";
  renderList("historyLikesList", "historyLikesCount", likesRows, "点赞", { reindexRank: Boolean(activeAccount) });
  renderList("historyCommentsList", "historyCommentsCount", commentsRows, "评论", { reindexRank: Boolean(activeAccount) });
  renderList("historyGrowthList", "historyGrowthCount", growthRows, "增长", { reindexRank: Boolean(activeAccount) });
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

async function exportLongImage() {
  const button = document.getElementById("exportLongImageButton");
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "导出中...";
  try {
    const html2canvas = window.html2canvas;
    if (document.fonts && typeof document.fonts.ready?.then === "function") {
      await document.fonts.ready;
    }
    if (!html2canvas || typeof html2canvas !== "function") {
      throw new Error("长图组件未加载完成");
    }
    const node = document.querySelector(".app-shell");
    const width = Math.ceil(node.getBoundingClientRect().width);
    const canvas = await html2canvas(node, {
      backgroundColor: "#111318",
      scale: Math.min(window.devicePixelRatio || 2, 3),
      useCORS: true,
      logging: false,
      width,
      windowWidth: width,
      scrollX: 0,
      scrollY: -window.scrollY,
      onclone: (clonedDocument) => {
        const clonedNode = clonedDocument.querySelector(".app-shell");
        if (clonedNode) {
          clonedNode.style.width = `${width}px`;
        }
      },
    });
    const dataUrl = canvas.toDataURL("image/png");
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
      `${buildMobileStatusSummary(payload) || `缓存更新 ${formatDateTime(payload.updated_at || payload.generated_at)}`}`;
    statusCard.textContent =
      `仅查看服务器缓存，不采集、不上传。点击榜单卡片可直接跳转到小红书作品或账号主页。`;
    renderCalendar(payload.calendar || []);
    renderHistoryDetails(payload);
  } catch (error) {
    statusCard.textContent = `加载失败：${error.message}`;
    ["historyLikesList", "historyCommentsList", "historyGrowthList"].forEach((id) => {
      document.getElementById(id).innerHTML = '<div class="empty-state">加载失败</div>';
    });
  }
}

document.getElementById("refreshButton").addEventListener("click", loadDashboard);
document.getElementById("exportLongImageButton").addEventListener("click", exportLongImage);
document.getElementById("calendarDateSelect").addEventListener("change", (event) => {
  selectedHistoryDate = String(event.target.value || "").trim();
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
