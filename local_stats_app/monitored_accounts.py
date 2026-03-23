from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..profile_batch_report import normalize_profile_url, normalize_profile_urls


DEFAULT_PROJECT_NAME = "默认项目"
MONITORED_PAUSED_PREFIX = "# PAUSED "
PROFILE_USER_ID_PATTERN = re.compile(r"/user/profile/([0-9a-z]+)", re.IGNORECASE)


def extract_link(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    if isinstance(value, list):
        for item in value:
            link = extract_link(item)
            if link:
                return link
    return ""


def extract_profile_user_id(url: str) -> str:
    match = PROFILE_USER_ID_PATTERN.search(str(url or ""))
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def build_metric_text(*, fans: Any = "", interaction: Any = "", works: Any = "") -> str:
    parts: List[str] = []
    fans_text = str(fans or "").strip()
    interaction_text = str(interaction or "").strip()
    works_text = str(works or "").strip()
    if fans_text:
        parts.append(f"粉丝 {fans_text}")
    if interaction_text:
        parts.append(f"获赞 {interaction_text}")
    if works_text:
        parts.append(f"作品 {works_text}")
    return " · ".join(parts)


def normalize_project_name(value: Any) -> str:
    text = str(value or "").strip()
    return text or DEFAULT_PROJECT_NAME


def classify_monitored_fetch_state(*, error_text: Any = "", has_snapshot: bool = False) -> tuple[str, str]:
    if has_snapshot:
        return "ok", "已获取账号快照"
    text = str(error_text or "").strip()
    lowered = text.lower()
    if not text:
        return "checking", "等待首次同步"
    if "/login" in lowered or "登录跳转" in text or "登录页" in text:
        return "error", "命中登录页，当前登录态不可用"
    if "空结果" in text:
        return "error", "账号页返回空结果"
    if "timeout" in lowered or "超时" in text:
        return "error", "请求超时，稍后重试"
    if "429" in lowered or "403" in lowered or "反爬" in text or "风控" in text:
        return "error", "可能触发风控，建议稍后重试"
    return "error", text[:42]


def is_login_redirect_url(url: Any) -> bool:
    text = str(url or "").strip().lower()
    return "/login" in text and "xiaohongshu.com" in text


def pick_profile_url(default_url: str, *candidates: Any) -> str:
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text and not is_login_redirect_url(text):
            return text
    return str(default_url or "").strip()


def build_profile_name_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    index: Dict[str, Dict[str, str]] = {}
    for row in rows:
        account_id = str(row.get("账号ID") or "").strip()
        if not account_id:
            continue
        works_text = str(row.get("作品数展示") or row.get("账号总作品数") or "").strip()
        if not works_text:
            visible_works = str(row.get("首页可见作品数") or "").strip()
            if visible_works.isdigit() and int(visible_works) >= 30:
                works_text = "30+"
            else:
                works_text = visible_works
        index[account_id] = {
            "account": str(row.get("账号") or "").strip(),
            "profile_url": extract_link(row.get("内容链接")),
            "fans_text": str(row.get("粉丝数") or row.get("粉丝数文本") or "").strip(),
            "interaction_text": str(row.get("获赞收藏文本") or "").strip(),
            "works_text": works_text,
        }
    return index


def build_dashboard_account_index(accounts: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    index: Dict[str, Dict[str, str]] = {}
    for item in accounts:
        account_id = str(item.get("account_id") or "").strip()
        if not account_id:
            continue
        fans_value = item.get("fans")
        interaction_value = item.get("interaction")
        works_value = item.get("works")
        works_display = str(item.get("works_display") or works_value or "").strip()
        if works_display.isdigit() and int(works_display) >= 30 and not str(item.get("works_display") or "").strip():
            works_display = "30+"
        index[account_id] = {
            "account": str(item.get("account") or "").strip(),
            "profile_url": str(item.get("profile_url") or "").strip(),
            "fans_text": "" if fans_value in ("", None) else str(fans_value),
            "interaction_text": "" if interaction_value in ("", None) else str(interaction_value),
            "works_text": works_display,
        }
    return index


def enrich_monitored_entries(
    entries: List[Dict[str, Any]],
    profile_rows: List[Dict[str, Any]],
    metadata_index: Optional[Dict[str, Dict[str, str]]] = None,
    dashboard_account_index: Optional[Dict[str, Dict[str, str]]] = None,
) -> List[Dict[str, Any]]:
    profile_index = build_profile_name_index(profile_rows)
    metadata_index = metadata_index or {}
    dashboard_account_index = dashboard_account_index or {}
    enriched: List[Dict[str, Any]] = []
    for entry in entries:
        url = str(entry.get("url") or "").strip()
        account_id = extract_profile_user_id(url)
        profile_meta = profile_index.get(account_id) or {}
        cached_meta = metadata_index.get(url) or {}
        dashboard_meta = dashboard_account_index.get(account_id) or {}
        account_name = (
            profile_meta.get("account")
            or cached_meta.get("account")
            or dashboard_meta.get("account")
            or account_id
            or ""
        )
        fans_text = profile_meta.get("fans_text") or cached_meta.get("fans_text") or dashboard_meta.get("fans_text") or ""
        interaction_text = (
            profile_meta.get("interaction_text")
            or cached_meta.get("interaction_text")
            or dashboard_meta.get("interaction_text")
            or ""
        )
        works_text = profile_meta.get("works_text") or cached_meta.get("works_text") or dashboard_meta.get("works_text") or ""
        fetch_state, fetch_message = classify_monitored_fetch_state(
            error_text=cached_meta.get("fetch_message") or "",
            has_snapshot=bool(account_name and (fans_text or interaction_text or works_text or dashboard_meta)),
        )
        if cached_meta.get("fetch_state") == "checking" and fetch_state == "ok":
            fetch_state = "ok"
            fetch_message = "已获取账号快照"
        elif cached_meta.get("fetch_state") in {"checking", "error", "warning"} and not (fans_text or interaction_text or works_text or dashboard_meta):
            fetch_state = str(cached_meta.get("fetch_state") or fetch_state)
            fetch_message = str(cached_meta.get("fetch_message") or fetch_message)
        summary_text = build_metric_text(
            fans=fans_text,
            interaction=interaction_text,
            works=works_text,
        )
        if not summary_text:
            summary_text = "等待首次同步"
        enriched.append(
            {
                "url": url,
                "active": bool(entry.get("active", True)),
                "project": normalize_project_name(entry.get("project")),
                "account_id": account_id,
                "account": account_name,
                "display_name": account_name or url,
                "profile_url": pick_profile_url(
                    url,
                    profile_meta.get("profile_url"),
                    cached_meta.get("profile_url"),
                    dashboard_meta.get("profile_url"),
                ),
                "fans_text": fans_text,
                "interaction_text": interaction_text,
                "works_text": works_text,
                "summary_text": summary_text,
                "fetch_state": fetch_state,
                "fetch_message": fetch_message or "等待首次同步",
                "fetch_checked_at": str(cached_meta.get("fetch_checked_at") or "").strip(),
            }
        )
    return enriched


def resolve_text_path(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def resolve_metadata_cache_path(urls_file: str) -> Path:
    path = resolve_text_path(urls_file)
    suffix = path.suffix or ".txt"
    return path.with_name(f"{path.stem}{suffix}.meta.json")


def load_monitored_metadata(urls_file: str) -> Dict[str, Dict[str, str]]:
    path = resolve_metadata_cache_path(urls_file)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized: Dict[str, Dict[str, str]] = {}
    for raw_url, raw_meta in payload.items():
        url = normalize_profile_url(str(raw_url or ""))
        if not url or not isinstance(raw_meta, dict):
            continue
        normalized[url] = {
            "account": str(raw_meta.get("account") or "").strip(),
            "account_id": str(raw_meta.get("account_id") or "").strip(),
            "profile_url": str(raw_meta.get("profile_url") or "").strip(),
            "fans_text": str(raw_meta.get("fans_text") or "").strip(),
            "interaction_text": str(raw_meta.get("interaction_text") or "").strip(),
            "works_text": str(raw_meta.get("works_text") or "").strip(),
            "fetch_state": str(raw_meta.get("fetch_state") or "").strip(),
            "fetch_message": str(raw_meta.get("fetch_message") or "").strip(),
            "fetch_checked_at": str(raw_meta.get("fetch_checked_at") or "").strip(),
        }
    return normalized


def update_monitored_metadata(urls_file: str, items: List[Dict[str, Any]]) -> Path:
    path = resolve_metadata_cache_path(urls_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = load_monitored_metadata(urls_file)
    for item in items:
        url = normalize_profile_url(str(item.get("url") or item.get("profile_url") or ""))
        if not url:
            continue
        current = metadata.get(url) or {}
        account_value = str(item.get("account") or "").strip() if "account" in item else current.get("account", "")
        account_id_value = str(item.get("account_id") or "").strip() if "account_id" in item else current.get("account_id", "")
        profile_url_value = (
            str(item.get("profile_url") or "").strip()
            if "profile_url" in item
            else str(current.get("profile_url") or url).strip()
        )
        fans_value = str(item.get("fans_text") or "").strip() if "fans_text" in item else current.get("fans_text", "")
        interaction_value = (
            str(item.get("interaction_text") or "").strip()
            if "interaction_text" in item
            else current.get("interaction_text", "")
        )
        works_value = str(item.get("works_text") or "").strip() if "works_text" in item else current.get("works_text", "")
        fetch_state_value = (
            str(item.get("fetch_state") or "").strip()
            if "fetch_state" in item
            else current.get("fetch_state", "")
        )
        fetch_message_value = (
            str(item.get("fetch_message") or "").strip()
            if "fetch_message" in item
            else current.get("fetch_message", "")
        )
        fetch_checked_at_value = (
            str(item.get("fetch_checked_at") or "").strip()
            if "fetch_checked_at" in item
            else current.get("fetch_checked_at", "")
        )
        metadata[url] = {
            "account": account_value,
            "account_id": account_id_value,
            "profile_url": profile_url_value or url,
            "fans_text": fans_value,
            "interaction_text": interaction_value,
            "works_text": works_value,
            "fetch_state": fetch_state_value,
            "fetch_message": fetch_message_value,
            "fetch_checked_at": fetch_checked_at_value,
        }
    payload = json.dumps(metadata, ensure_ascii=False, indent=2)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)
    return path


def parse_monitored_entries(urls_file: str) -> List[Dict[str, Any]]:
    path = resolve_text_path(urls_file)
    if not path.exists():
        return []
    entries: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        active = True
        candidate = line
        if line.startswith(MONITORED_PAUSED_PREFIX):
            active = False
            candidate = line[len(MONITORED_PAUSED_PREFIX) :].strip()
        elif line.startswith("#"):
            continue
        project = DEFAULT_PROJECT_NAME
        if "\t" in candidate:
            raw_project, candidate = candidate.split("\t", 1)
            project = normalize_project_name(raw_project)
        normalized = normalize_profile_url(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        entries.append({"url": normalized, "active": active, "project": project})
    return entries


def load_monitored_urls(urls_file: str) -> List[str]:
    return [entry["url"] for entry in parse_monitored_entries(urls_file) if entry.get("active")]


def merge_monitored_entries(
    existing_entries: List[Dict[str, Any]],
    *,
    raw_text: str = "",
    urls: Optional[List[str]] = None,
    project: str = DEFAULT_PROJECT_NAME,
) -> tuple[List[Dict[str, Any]], List[str], List[str]]:
    merged = [
        {
            "url": normalize_profile_url(str(entry.get("url") or "")),
            "active": bool(entry.get("active", True)),
            "project": normalize_project_name(entry.get("project")),
        }
        for entry in existing_entries
        if normalize_profile_url(str(entry.get("url") or ""))
    ]
    index = {entry["url"]: entry for entry in merged}
    added: List[str] = []
    reactivated: List[str] = []
    normalized_project = normalize_project_name(project)
    for normalized in normalize_profile_urls(list(urls or []), raw_text, None):
        existing = index.get(normalized)
        if existing is None:
            entry = {"url": normalized, "active": True, "project": normalized_project}
            merged.append(entry)
            index[normalized] = entry
            added.append(normalized)
            continue
        if not existing.get("active"):
            existing["active"] = True
            reactivated.append(normalized)
    return merged, added, reactivated


def merge_monitored_urls(
    existing_urls: List[str],
    *,
    raw_text: str = "",
    urls: Optional[List[str]] = None,
) -> tuple[List[str], List[str]]:
    merged_entries, added, _ = merge_monitored_entries(
        [{"url": url, "active": True, "project": DEFAULT_PROJECT_NAME} for url in existing_urls],
        raw_text=raw_text,
        urls=urls,
    )
    return [entry["url"] for entry in merged_entries if entry.get("active")], added


def write_monitored_entries(urls_file: str, entries: List[Dict[str, Any]]) -> Path:
    path = resolve_text_path(urls_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    seen: set[str] = set()
    for entry in entries:
        normalized = normalize_profile_url(str(entry.get("url") or ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        project = normalize_project_name(entry.get("project"))
        line = f"{project}\t{normalized}"
        if entry.get("active", True):
            lines.append(line)
        else:
            lines.append(f"{MONITORED_PAUSED_PREFIX}{line}")
    payload = "\n".join(lines)
    if payload:
        payload += "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)
    return path


def write_monitored_urls(urls_file: str, urls: List[str]) -> Path:
    return write_monitored_entries(
        urls_file,
        [{"url": url, "active": True, "project": DEFAULT_PROJECT_NAME} for url in urls],
    )


def build_project_summaries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        project = normalize_project_name(entry.get("project"))
        bucket = grouped.setdefault(project, {"name": project, "total": 0, "active_count": 0, "paused_count": 0})
        bucket["total"] += 1
        if entry.get("active"):
            bucket["active_count"] += 1
        else:
            bucket["paused_count"] += 1
    return [grouped[key] for key in sorted(grouped, key=lambda item: (item != DEFAULT_PROJECT_NAME, item))]
