from __future__ import annotations

import argparse
import csv
import copy
import gzip
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import replace
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..chrome_cookies import (
    export_xiaohongshu_cookie_header,
    is_default_chrome_profile_root,
    resolve_chrome_profile_directory,
    resolve_chrome_profile_root,
)
from ..config import APP_VERSION, DEFAULT_SERVER_CACHE_PUSH_URL, load_settings, normalize_server_cache_push_url
from ..local_daily_sync_status import load_local_daily_sync_status
from ..profile_cache_push import push_local_cache_to_server
from ..profile_batch_report import normalize_profile_url
from ..profile_batch_to_feishu import (
    load_reports_for_sync,
)
from ..profile_dashboard_to_feishu import (
    build_single_work_ranking_fields,
    build_single_work_rankings,
    compute_dashboard_metrics,
    extract_snapshot_date,
    parse_exact_number,
    rank_profile_works,
)
from ..project_sync_status import attach_project_sync_statuses, update_project_sync_status
from ..project_cache import (
    load_cached_dashboard_payload,
    rebuild_dashboard_cache_from_project_dirs,
    repair_dashboard_cache_from_exports,
    resolve_project_cache_dir,
    write_project_cache_bundle,
)
from ..profile_report import build_profile_report, load_profile_report_payload
from ..profile_to_feishu import PROFILE_TABLE_NAME
from ..profile_works_to_feishu import WORKS_TABLE_NAME
from ..xhs import build_proxy_pool_status
from .data_service import build_dashboard_payload_from_tables
from . import login_state as login_state_module
from .login_state import (
    LOGIN_STATE_IDLE_PAYLOAD,
    LOGIN_WAIT_POLL_SECONDS,
    LOGIN_WAIT_TIMEOUT_SECONDS,
    build_login_state_payload,
    login_state_requires_interactive_login,
    open_xiaohongshu_login_window as _open_xiaohongshu_login_window_impl,
    run_login_state_self_check as _run_login_state_self_check_impl,
    wait_for_xiaohongshu_login as _wait_for_xiaohongshu_login_impl,
)
from .monitored_accounts import (
    DEFAULT_PROJECT_NAME,
    build_dashboard_account_index,
    build_metric_text,
    build_profile_name_index,
    build_project_summaries,
    classify_monitored_fetch_state,
    enrich_monitored_entries,
    extract_link,
    extract_profile_user_id,
    is_login_redirect_url,
    load_monitored_metadata,
    load_monitored_urls,
    merge_monitored_entries,
    merge_monitored_urls,
    normalize_project_name,
    parse_monitored_entries,
    pick_profile_url,
    resolve_text_path,
    resolve_metadata_cache_path,
    update_monitored_metadata,
    write_monitored_metadata,
    write_monitored_entries,
    write_monitored_urls,
)


PORTAL_TABLE_NAME = "小红书仪表盘总控"
CALENDAR_TABLE_NAME = "小红书日历留底"
RANKING_TABLE_NAME = "小红书单条作品排行"
ALERT_TABLE_NAME = "小红书评论预警"
WEB_DIR = Path(__file__).resolve().parent / "web"
DEFAULT_URLS_FILE = "xhs_feishu_monitor/input/robam_multi_profile_urls.txt"
DEFAULT_ACCOUNT_RANKING_EXPORT_DIR = "/Users/cc/Downloads/飞书缓存/账号榜单导出"
SYSTEM_CONFIG_KEYS = ("XHS_COOKIE", "PROJECT_CACHE_DIR", "STATE_FILE", "SERVER_CACHE_PUSH_URL", "SERVER_CACHE_UPLOAD_TOKEN")
SYSTEM_CONFIG_HELPER_KEYS: tuple[str, ...] = ()
LEGACY_SYSTEM_CONFIG_PREFIXES = ("FEISHU_",)
SYSTEM_CONFIG_DEFAULTS = {
    "SERVER_CACHE_PUSH_URL": DEFAULT_SERVER_CACHE_PUSH_URL,
}

DASHBOARD_SERIES_META = {
    "mode": "daily",
    "update_time": "14:00",
    "source": "小红书日历留底",
    "note": "趋势图按天留底，每个账号每天保留 1 个点。",
}

AUTO_SERVER_CACHE_PUSH_DAILY_AT = "14:00"
AUTO_SERVER_CACHE_PUSH_RETRY_SECONDS = 15 * 60
AUTO_SERVER_CACHE_PUSH_POLL_SECONDS = 20
AUTO_PROJECT_SYNC_RETRY_SECONDS = 10 * 60
LEGACY_FEISHU_ERROR_MARKERS = (
    "缺少飞书配置",
    "飞书上传失败",
    "tenant_access_token",
    "fieldnamenotfound",
    "rolepermnotallow",
    "feishu_app_id",
    "feishu_app_secret",
    "feishu_bitable_app_token",
    "feishu_table_id",
)


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def contains_legacy_feishu_error(*, message: str = "", error: str = "") -> bool:
    combined_text = f"{message} {error}".lower()
    return any(marker in combined_text for marker in LEGACY_FEISHU_ERROR_MARKERS)


def build_empty_dashboard_payload(*, load_error: str = "") -> Dict[str, Any]:
    return {
        "generated_at": iso_now(),
        "latest_date": "",
        "updated_at": "",
        "series_meta": copy.deepcopy(DASHBOARD_SERIES_META),
        "portal": {
            "updated_at": "",
            "accounts": 0,
            "fans": 0,
            "interaction": 0,
            "works": 0,
            "likes": 0,
            "comments": 0,
            "average_likes": 0,
            "average_comments": 0,
            "weekly_summary": "",
            "top_title": "",
            "top_account": "",
            "top_like": 0,
            "top_url": "",
        },
        "series": [],
        "account_series": {},
        "accounts": [],
        "rankings": {},
        "alerts": [],
        "stale": True,
        "local_override": False,
        "cache_age_seconds": 0,
        "load_error": load_error,
    }


def load_system_config(env_file: str, urls_file: str) -> Dict[str, Any]:
    env_path = Path(env_file).expanduser().resolve()
    urls_path = resolve_text_path(urls_file).expanduser().resolve()
    env_values: Dict[str, str] = {}
    env_lines: List[str] = []
    if env_path.exists():
        env_lines = env_path.read_text(encoding="utf-8").splitlines()
        for raw in env_lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_values[key.strip()] = value.strip()
    urls_text = urls_path.read_text(encoding="utf-8") if urls_path.exists() else ""
    config = {key: env_values.get(key, SYSTEM_CONFIG_DEFAULTS.get(key, "")) for key in SYSTEM_CONFIG_KEYS}
    config = _normalize_system_config_updates(config)
    return {
        "ok": True,
        "env_file": str(env_path),
        "urls_file": str(urls_path),
        "config": config,
        "urls_text": urls_text,
        "updated_at": iso_now(),
    }


def _normalize_system_config_updates(updates: Dict[str, str]) -> Dict[str, str]:
    normalized = dict(updates)
    normalized["SERVER_CACHE_PUSH_URL"] = normalize_server_cache_push_url(normalized.get("SERVER_CACHE_PUSH_URL") or "")
    return normalized


def _filter_legacy_system_config_lines(lines: List[str]) -> List[str]:
    filtered: List[str] = []
    for raw in lines:
        line = raw.rstrip("\n")
        stripped = line.strip()
        if stripped and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key.startswith(LEGACY_SYSTEM_CONFIG_PREFIXES):
                continue
        filtered.append(line)
    return filtered


def save_system_config(env_file: str, urls_file: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    env_path = Path(env_file).expanduser().resolve()
    urls_path = resolve_text_path(urls_file).expanduser().resolve()
    env_path.parent.mkdir(parents=True, exist_ok=True)
    urls_path.parent.mkdir(parents=True, exist_ok=True)

    current_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    current_lines = _filter_legacy_system_config_lines(current_lines)
    updates = payload.get("config") or {}
    if not isinstance(updates, dict):
        raise ValueError("config 必须是对象")
    allowed_input_keys = set(SYSTEM_CONFIG_KEYS) | set(SYSTEM_CONFIG_HELPER_KEYS)
    normalized = {str(key): str(value or "") for key, value in updates.items() if str(key) in allowed_input_keys}
    normalized = _normalize_system_config_updates(normalized)

    kept_keys = set(normalized.keys())
    new_lines: List[str] = []
    seen: set[str] = set()
    for raw in current_lines:
        line = raw.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key, _value = line.split("=", 1)
        key = key.strip()
        if key in normalized:
            new_lines.append(f"{key}={normalized[key]}")
            seen.add(key)
        else:
            new_lines.append(line)
    for key in SYSTEM_CONFIG_KEYS:
        if key in normalized and key not in seen:
            new_lines.append(f"{key}={normalized[key]}")
    env_path.write_text("\n".join(new_lines).rstrip() + "\n", encoding="utf-8")

    urls_text = str(payload.get("urls_text") or "")
    urls_path.write_text(urls_text, encoding="utf-8")
    return load_system_config(str(env_path), str(urls_path))


def _parse_clock_minutes(value: str, default: str) -> int:
    text = str(value or default).strip() or default
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
    except Exception:
        hour, minute = map(int, default.split(":", 1))
    return hour * 60 + minute


def _compute_next_schedule_time(*, now: datetime, start_minutes: int, end_minutes: int, interval_minutes: int) -> datetime:
    current_minutes = now.hour * 60 + now.minute
    interval_minutes = max(1, int(interval_minutes or 30))
    if start_minutes == end_minutes:
        slot_minutes = ((current_minutes // interval_minutes) + 1) * interval_minutes
        day_offset, minute_of_day = divmod(slot_minutes, 24 * 60)
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return base.replace(hour=minute_of_day // 60, minute=minute_of_day % 60) + timedelta(days=day_offset)
    if start_minutes <= current_minutes < end_minutes:
        relative = current_minutes - start_minutes
        next_relative = ((relative // interval_minutes) + 1) * interval_minutes
        next_minutes = start_minutes + next_relative
        if next_minutes < end_minutes:
            return now.replace(hour=next_minutes // 60, minute=next_minutes % 60, second=0, microsecond=0)
    if current_minutes < start_minutes:
        return now.replace(hour=start_minutes // 60, minute=start_minutes % 60, second=0, microsecond=0)
    next_day = now + timedelta(days=1)
    return next_day.replace(hour=start_minutes // 60, minute=start_minutes % 60, second=0, microsecond=0)


def build_collection_schedule_plan(*, settings, entries: List[Dict[str, Any]], now: Optional[datetime] = None) -> Dict[str, Any]:
    current = now or datetime.now().astimezone()
    enabled = bool(getattr(settings, "xhs_spread_schedule_enabled", True))
    interval_minutes = max(1, int(getattr(settings, "xhs_batch_schedule_interval_minutes", 30) or 30))
    window_start = str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00")
    window_end = str(getattr(settings, "xhs_batch_window_end", "16:00") or "16:00")
    start_minutes = _parse_clock_minutes(window_start, "14:00")
    end_minutes = _parse_clock_minutes(window_end, "16:00")
    slot_offset_seconds = max(0, int(getattr(settings, "xhs_batch_slot_offset_seconds", 300) or 0))
    min_accounts = max(1, int(getattr(settings, "xhs_batch_min_accounts_per_run", 1) or 1))
    max_accounts = max(min_accounts, int(getattr(settings, "xhs_batch_max_accounts_per_run", min_accounts) or min_accounts))
    per_run = min_accounts if min_accounts == max_accounts else max_accounts
    active_entries = [dict(item) for item in (entries or []) if item.get("active")]
    grouped: Dict[str, int] = {}
    order: List[str] = []
    for item in active_entries:
        project_name = normalize_project_name(str(item.get("project") or DEFAULT_PROJECT_NAME))
        if project_name not in grouped:
            grouped[project_name] = 0
            order.append(project_name)
        grouped[project_name] += 1
    next_run_at = _compute_next_schedule_time(
        now=current,
        start_minutes=start_minutes,
        end_minutes=end_minutes,
        interval_minutes=interval_minutes,
    )
    duration_minutes = (end_minutes - start_minutes) % (24 * 60)
    if duration_minutes == 0:
        duration_minutes = 24 * 60
    slots_per_day = max(1, (duration_minutes + interval_minutes - 1) // interval_minutes)
    projects = []
    for index, project_name in enumerate(order):
        active_count = grouped.get(project_name, 0)
        project_per_run = min(
            active_count,
            max(
                min_accounts,
                min(max_accounts, (active_count + slots_per_day - 1) // slots_per_day),
            ),
        ) if active_count else 0
        project_run_at = next_run_at + timedelta(seconds=index * slot_offset_seconds)
        projects.append(
            {
                "name": project_name,
                "active_count": active_count,
                "per_run": project_per_run,
                "next_run_at": project_run_at.isoformat(timespec="seconds"),
                "slot_offset_seconds": index * slot_offset_seconds,
            }
        )
    return {
        "enabled": enabled,
        "interval_minutes": interval_minutes,
        "window_start": window_start,
        "window_end": window_end,
        "next_run_at": next_run_at.isoformat(timespec="seconds"),
        "per_run": max((item["per_run"] for item in projects), default=per_run),
        "slots_per_day": slots_per_day,
        "project_count": len(projects),
        "projects": projects,
    }


def build_auto_project_schedule(*, settings, entries: List[Dict[str, Any]], now: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
    current = now or datetime.now().astimezone()
    if not bool(getattr(settings, "xhs_spread_schedule_enabled", True)):
        return {}
    project_url_map: Dict[str, List[str]] = {}
    for entry in entries:
        if not entry.get("active"):
            continue
        normalized_project = normalize_project_name(entry.get("project"))
        url = str(entry.get("url") or "").strip()
        if not url:
            continue
        project_url_map.setdefault(normalized_project, []).append(url)
    if not project_url_map:
        return {}
    start_minutes = _parse_clock_minutes(str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00"), "14:00")
    end_minutes = _parse_clock_minutes(str(getattr(settings, "xhs_batch_window_end", "15:00") or "15:00"), "15:00")
    start_at = current.replace(hour=start_minutes // 60, minute=start_minutes % 60, second=0, microsecond=0)
    if end_minutes <= start_minutes:
        end_at = (start_at + timedelta(days=1)).replace(hour=end_minutes // 60, minute=end_minutes % 60, second=0, microsecond=0)
    else:
        end_at = current.replace(hour=end_minutes // 60, minute=end_minutes % 60, second=0, microsecond=0)
    duration_seconds = max(60, int((end_at - start_at).total_seconds()))
    project_names = sorted(project_url_map)
    slot_gap_seconds = max(5, duration_seconds // max(1, len(project_names)))
    schedule: Dict[str, Dict[str, Any]] = {}
    for index, project_name in enumerate(project_names):
        schedule[project_name] = {
            "urls": list(project_url_map.get(project_name) or []),
            "scheduled_at": start_at + timedelta(seconds=index * slot_gap_seconds),
            "slot_gap_seconds": slot_gap_seconds,
            "project_index": index,
        }
    return schedule


def _sync_login_state_module_dependencies() -> None:
    login_state_module.load_settings = load_settings
    login_state_module.export_xiaohongshu_cookie_header = export_xiaohongshu_cookie_header
    login_state_module.is_default_chrome_profile_root = is_default_chrome_profile_root
    login_state_module.resolve_chrome_profile_directory = resolve_chrome_profile_directory
    login_state_module.resolve_chrome_profile_root = resolve_chrome_profile_root
    login_state_module.build_profile_report = build_profile_report
    login_state_module.load_profile_report_payload = load_profile_report_payload
    login_state_module.subprocess = subprocess
    login_state_module.webbrowser = webbrowser


def run_login_state_self_check(*, env_file: str, sample_url: str = "") -> Dict[str, Any]:
    _sync_login_state_module_dependencies()
    return _run_login_state_self_check_impl(env_file=env_file, sample_url=sample_url)


def open_xiaohongshu_login_window(*, settings, target_url: str = "") -> bool:
    _sync_login_state_module_dependencies()
    return _open_xiaohongshu_login_window_impl(settings=settings, target_url=target_url)


def wait_for_xiaohongshu_login(
    *,
    env_file: str,
    settings,
    sample_url: str,
    on_wait: Optional[Callable[[Dict[str, Any]], None]] = None,
    timeout_seconds: int = LOGIN_WAIT_TIMEOUT_SECONDS,
    poll_seconds: int = LOGIN_WAIT_POLL_SECONDS,
) -> Dict[str, Any]:
    _sync_login_state_module_dependencies()
    return _wait_for_xiaohongshu_login_impl(
        env_file=env_file,
        settings=settings,
        sample_url=sample_url,
        on_wait=on_wait,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        run_self_check=run_login_state_self_check,
        open_login_window=open_xiaohongshu_login_window,
    )


def build_dashboard_account_point(report: Dict[str, Any]) -> Dict[str, Any]:
    profile = report.get("profile") or {}
    metrics = compute_dashboard_metrics(report)
    works_value = profile.get("total_work_count")
    if works_value in ("", None):
        works_value = metrics["visible_work_count"]
    latest_comment_value = latest_report_comment_value(report)
    return {
        "date": extract_snapshot_date(str(report.get("captured_at") or "")),
        "fans": parse_exact_number(profile.get("fans_count_text")) or 0,
        "interaction": parse_exact_number(profile.get("interaction_count_text")) or 0,
        "likes": int(metrics["total_likes"]),
        "comments": latest_comment_value,
        "works": int(works_value or 0),
    }


def build_dashboard_account_card(report: Dict[str, Any], existing_card: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    profile = report.get("profile") or {}
    metrics = compute_dashboard_metrics(report)
    ranked = rank_profile_works(report.get("works") or [])
    top_work = ranked[0] if ranked else {}
    works_value = profile.get("total_work_count")
    if works_value in ("", None):
        works_value = metrics["visible_work_count"]
    works_display = str(profile.get("work_count_display_text") or works_value or "").strip()
    card = dict(existing_card or {})
    card.update(
        {
            "account_id": str(profile.get("profile_user_id") or "").strip(),
            "account": str(profile.get("nickname") or "").strip(),
            "date": extract_snapshot_date(str(report.get("captured_at") or "")),
            "fans": parse_exact_number(profile.get("fans_count_text")) or 0,
            "interaction": parse_exact_number(profile.get("interaction_count_text")) or 0,
            "works": int(works_value or 0),
            "works_display": works_display,
            "works_exact": bool(profile.get("work_count_exact", not works_display.endswith("+"))),
            "likes": int(metrics["total_likes"]),
            "comments": latest_report_comment_value(report, fallback=existing_card.get("comments") if existing_card else 0),
            "profile_url": str(profile.get("profile_url") or "").strip(),
            "top_title": str(top_work.get("title_copy") or "").strip(),
            "top_like": int(metrics["top_like_count"]),
            "top_url": str(top_work.get("note_url") or (existing_card or {}).get("top_url") or "").strip(),
        }
    )
    return card


def merge_account_series_points(
    base_series: Dict[str, List[Dict[str, Any]]],
    reports: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    merged: Dict[str, List[Dict[str, Any]]] = {
        account_id: [dict(point) for point in points]
        for account_id, points in (base_series or {}).items()
        if account_id
    }
    for report in reports:
        profile = report.get("profile") or {}
        account_id = str(profile.get("profile_user_id") or "").strip()
        if not account_id:
            continue
        existing_points = [dict(item) for item in merged.get(account_id, [])]
        fallback_point = existing_points[-1] if existing_points else {}
        point = build_dashboard_account_point_with_fallback(report, fallback_point=fallback_point)
        series = [dict(item) for item in existing_points if str(item.get("date") or "").strip() != point["date"]]
        series.append(point)
        series.sort(key=lambda item: str(item.get("date") or ""))
        merged[account_id] = series
    return merged


def latest_report_comment_value(report: Dict[str, Any], *, fallback: Any = 0) -> int:
    metrics = compute_dashboard_metrics(report)
    if report_has_comment_data(report):
        return int(metrics["total_comments"])
    return int(fallback or 0)


def report_has_comment_data(report: Dict[str, Any]) -> bool:
    return any(work.get("comment_count") is not None for work in (report.get("works") or []))


def report_has_detail_links(report: Dict[str, Any]) -> bool:
    return any(str(work.get("note_url") or work.get("note_id") or "").strip() for work in (report.get("works") or []))


def build_dashboard_account_point_with_fallback(report: Dict[str, Any], *, fallback_point: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    point = build_dashboard_account_point(report)
    if not report_has_comment_data(report):
        point["comments"] = int((fallback_point or {}).get("comments") or 0)
    return point


def rebuild_daily_series_from_account_series(account_series: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for points in account_series.values():
        for point in points:
            date_text = str(point.get("date") or "").strip()
            if not date_text:
                continue
            bucket = grouped.setdefault(
                date_text,
                {
                    "date": date_text,
                    "fans": 0,
                    "likes": 0,
                    "comments": 0,
                    "works": 0,
                    "accounts": 0,
                },
            )
            bucket["fans"] += int(point.get("fans") or 0)
            bucket["likes"] += int(point.get("likes") or 0)
            bucket["comments"] += int(point.get("comments") or 0)
            bucket["works"] += int(point.get("works") or 0)
            bucket["accounts"] += 1
    return [grouped[key] for key in sorted(grouped)]


def build_ranking_item_from_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    comment_basis = str(fields.get("评论数口径") or fields.get("单选") or "").strip()
    return {
        "rank": int(fields.get("排名") or 0),
        "account_id": str(fields.get("账号ID") or "").strip(),
        "account": str(fields.get("账号") or "").strip(),
        "title": str(fields.get("标题文案") or "").strip(),
        "metric": fields.get("排序值"),
        "summary": str(fields.get("榜单摘要") or "").strip(),
        "comment_basis": comment_basis,
        "comment_is_lower_bound": comment_basis == "评论预览下限",
        "profile_url": extract_link(fields.get("主页链接")),
        "note_url": extract_link(fields.get("作品链接")),
        "cover_url": extract_link(fields.get("封面图")),
        "tracking_status": str(fields.get("追踪状态") or "").strip(),
        "first_seen_date": str(fields.get("首次入池日期") or "").strip(),
    }


def _safe_export_name(value: str, fallback: str) -> str:
    text = str(value or "").strip()
    cleaned = "".join(char if char.isalnum() or "\u4e00" <= char <= "\u9fff" or char in ("-", "_") else "-" for char in text)
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned or fallback


def _write_rows_to_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        if not fieldnames:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_account_ranking_review_markdown(
    *,
    project_name: str,
    account_name: str,
    account_id: str,
    snapshot_label: str,
    like_rows: List[Dict[str, Any]],
    comment_rows: List[Dict[str, Any]],
) -> str:
    lines = [
        f"# {account_name} 榜单复盘",
        "",
        f"- 项目：{project_name}",
        f"- 账号：{account_name}",
        f"- 账号ID：{account_id}",
        f"- 快照时间：{snapshot_label}",
        f"- 点赞榜条数：{len(like_rows)}",
        f"- 评论榜条数：{len(comment_rows)}",
        "",
        "## 点赞 Top 5",
    ]
    if like_rows:
        for row in like_rows[:5]:
            lines.append(f"- TOP{row['排名']}：{row['标题']}｜点赞 {row['数值']}")
    else:
        lines.append("- 暂无点赞榜数据")
    lines.extend(["", "## 评论 Top 5"])
    if comment_rows:
        for row in comment_rows[:5]:
            basis = f"｜{row['评论口径']}" if row.get("评论口径") else ""
            lines.append(f"- TOP{row['排名']}：{row['标题']}｜评论 {row['数值']}{basis}")
    else:
        lines.append("- 暂无评论榜数据")
    return "\n".join(lines) + "\n"


def _build_account_export_rows(
    *,
    rankings: Dict[str, List[Dict[str, Any]]],
    account: Dict[str, Any],
    account_id: str,
    account_name: str,
    project_name: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    def build_export_rows(items: List[Dict[str, Any]], metric_key: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for item in items:
            rows.append(
                {
                    "项目": project_name,
                    "账号ID": account_id,
                    "账号": account_name,
                    "排名": int(item.get("rank") or 0),
                    "标题": str(item.get("title") or "").strip(),
                    "数值": item.get("metric"),
                    "指标": metric_key,
                    "摘要": str(item.get("summary") or "").strip(),
                    "作品链接": str(item.get("note_url") or "").strip(),
                    "主页链接": str(item.get("profile_url") or account.get("profile_url") or "").strip(),
                    "封面图": str(item.get("cover_url") or "").strip(),
                    "评论口径": str(item.get("comment_basis") or "").strip(),
                    "追踪状态": str(item.get("tracking_status") or "").strip(),
                    "首次入池日期": str(item.get("first_seen_date") or "").strip(),
                }
            )
        return rows

    like_rows = build_export_rows(
        [item for item in (rankings.get("单条点赞排行") or []) if str(item.get("account_id") or "").strip() == account_id],
        "点赞",
    )
    comment_rows = build_export_rows(
        [item for item in (rankings.get("单条评论排行") or []) if str(item.get("account_id") or "").strip() == account_id],
        "评论",
    )
    return like_rows, comment_rows


def _export_account_rankings_to_snapshot(
    *,
    rankings: Dict[str, List[Dict[str, Any]]],
    account: Dict[str, Any],
    account_id: str,
    account_name: str,
    project_name: str,
    account_dir: Path,
    target_dir: Path,
    snapshot_label: str,
    snapshot_slug: str,
) -> Dict[str, Any]:
    like_rows, comment_rows = _build_account_export_rows(
        rankings=rankings,
        account=account,
        account_id=account_id,
        account_name=account_name,
        project_name=project_name,
    )
    if not like_rows and not comment_rows:
        raise ValueError("当前账号暂无可导出的点赞或评论榜单数据")

    like_csv_path = target_dir / f"{snapshot_slug}-点赞排行.csv"
    comment_csv_path = target_dir / f"{snapshot_slug}-评论排行.csv"
    like_json_path = target_dir / f"{snapshot_slug}-点赞排行.json"
    comment_json_path = target_dir / f"{snapshot_slug}-评论排行.json"
    summary_path = target_dir / "导出摘要.json"
    review_markdown_path = target_dir / "复盘摘要.md"
    latest_summary_path = account_dir / "最近一次导出.json"

    _write_rows_to_csv(like_csv_path, like_rows)
    _write_rows_to_csv(comment_csv_path, comment_rows)
    like_json_path.write_text(json.dumps(like_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    comment_json_path.write_text(json.dumps(comment_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {
        "project": project_name,
        "account_id": account_id,
        "account": account_name,
        "export_dir": str(target_dir),
        "account_dir": str(account_dir),
        "snapshot_time": snapshot_label,
        "snapshot_slug": snapshot_slug,
        "like_count": len(like_rows),
        "comment_count": len(comment_rows),
        "updated_at": iso_now(),
        "files": {
            "like_csv": str(like_csv_path),
            "comment_csv": str(comment_csv_path),
            "like_json": str(like_json_path),
            "comment_json": str(comment_json_path),
            "review_markdown": str(review_markdown_path),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    review_markdown_path.write_text(
        _build_account_ranking_review_markdown(
            project_name=project_name,
            account_name=account_name,
            account_id=account_id,
            snapshot_label=snapshot_label,
            like_rows=like_rows,
            comment_rows=comment_rows,
        ),
        encoding="utf-8",
    )
    latest_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    summary["latest_summary_path"] = str(latest_summary_path)
    return summary


def _build_project_review_markdown(
    *,
    project_name: str,
    snapshot_label: str,
    summaries: List[Dict[str, Any]],
) -> str:
    lines = [
        f"# {project_name} 项目复盘",
        "",
        f"- 快照时间：{snapshot_label}",
        f"- 账号数量：{len(summaries)}",
        f"- 点赞榜总条数：{sum(int(item.get('like_count') or 0) for item in summaries)}",
        f"- 评论榜总条数：{sum(int(item.get('comment_count') or 0) for item in summaries)}",
        "",
        "## 账号索引",
    ]
    if not summaries:
        lines.append("- 当前项目暂无可导出的账号榜单")
    else:
        for item in summaries:
            lines.append(
                f"- {item.get('account')}｜点赞 {item.get('like_count')} 条｜评论 {item.get('comment_count')} 条｜目录 {item.get('export_dir')}"
            )
    return "\n".join(lines) + "\n"


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_rows_json_if_exists(path_text: str) -> List[Dict[str, Any]]:
    path = Path(str(path_text or "").strip())
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return [dict(item) for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _build_ranking_compare(current_rows: List[Dict[str, Any]], previous_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def row_key(row: Dict[str, Any]) -> str:
        note_url = str(row.get("作品链接") or "").strip()
        title = str(row.get("标题") or "").strip()
        return note_url or title

    current_index = {row_key(item): item for item in current_rows if row_key(item)}
    previous_index = {row_key(item): item for item in previous_rows if row_key(item)}
    new_entries: List[Dict[str, Any]] = []
    dropped_entries: List[Dict[str, Any]] = []
    moved_up: List[Dict[str, Any]] = []
    moved_down: List[Dict[str, Any]] = []

    for key, current_item in current_index.items():
        previous_item = previous_index.get(key)
        if previous_item is None:
            new_entries.append(
                {
                    "title": str(current_item.get("标题") or "").strip(),
                    "current_rank": int(current_item.get("排名") or 0),
                }
            )
            continue
        current_rank = int(current_item.get("排名") or 0)
        previous_rank = int(previous_item.get("排名") or 0)
        delta = previous_rank - current_rank
        if delta > 0:
            moved_up.append(
                {
                    "title": str(current_item.get("标题") or "").strip(),
                    "current_rank": current_rank,
                    "previous_rank": previous_rank,
                    "rank_change": delta,
                }
            )
        elif delta < 0:
            moved_down.append(
                {
                    "title": str(current_item.get("标题") or "").strip(),
                    "current_rank": current_rank,
                    "previous_rank": previous_rank,
                    "rank_change": delta,
                }
            )

    for key, previous_item in previous_index.items():
        if key in current_index:
            continue
        dropped_entries.append(
            {
                "title": str(previous_item.get("标题") or "").strip(),
                "previous_rank": int(previous_item.get("排名") or 0),
            }
        )

    moved_up.sort(key=lambda item: (int(item.get("current_rank") or 0), str(item.get("title") or "")))
    moved_down.sort(key=lambda item: (int(item.get("current_rank") or 9999), str(item.get("title") or "")))
    new_entries.sort(key=lambda item: (int(item.get("current_rank") or 9999), str(item.get("title") or "")))
    dropped_entries.sort(key=lambda item: (int(item.get("previous_rank") or 9999), str(item.get("title") or "")))
    return {
        "current_count": len(current_rows),
        "previous_count": len(previous_rows),
        "new_entries": new_entries,
        "dropped_entries": dropped_entries,
        "moved_up": moved_up,
        "moved_down": moved_down,
    }


def _build_project_compare_payload(
    *,
    project_name: str,
    current_summary: Dict[str, Any],
    previous_summary: Dict[str, Any],
) -> Dict[str, Any]:
    previous_accounts = {
        str(item.get("account_id") or "").strip(): item
        for item in (previous_summary.get("accounts") or [])
        if str(item.get("account_id") or "").strip()
    }
    current_accounts = {
        str(item.get("account_id") or "").strip(): item
        for item in (current_summary.get("accounts") or [])
        if str(item.get("account_id") or "").strip()
    }
    added_account_ids = [account_id for account_id in current_accounts if account_id not in previous_accounts]
    removed_account_ids = [account_id for account_id in previous_accounts if account_id not in current_accounts]
    changed_accounts: List[Dict[str, Any]] = []
    for account_id, current_item in current_accounts.items():
        previous_item = previous_accounts.get(account_id)
        if not previous_item:
            continue
        like_delta = int(current_item.get("like_count") or 0) - int(previous_item.get("like_count") or 0)
        comment_delta = int(current_item.get("comment_count") or 0) - int(previous_item.get("comment_count") or 0)
        like_compare = _build_ranking_compare(
            _load_rows_json_if_exists(str(((current_item.get("files") or {}).get("like_json") or ""))),
            _load_rows_json_if_exists(str(((previous_item.get("files") or {}).get("like_json") or ""))),
        )
        comment_compare = _build_ranking_compare(
            _load_rows_json_if_exists(str(((current_item.get("files") or {}).get("comment_json") or ""))),
            _load_rows_json_if_exists(str(((previous_item.get("files") or {}).get("comment_json") or ""))),
        )
        if (
            like_delta
            or comment_delta
            or like_compare["new_entries"]
            or like_compare["dropped_entries"]
            or like_compare["moved_up"]
            or like_compare["moved_down"]
            or comment_compare["new_entries"]
            or comment_compare["dropped_entries"]
            or comment_compare["moved_up"]
            or comment_compare["moved_down"]
        ):
            changed_accounts.append(
                {
                    "account_id": account_id,
                    "account": str(current_item.get("account") or previous_item.get("account") or "").strip(),
                    "like_delta": like_delta,
                    "comment_delta": comment_delta,
                    "like_compare": like_compare,
                    "comment_compare": comment_compare,
                    "current_export_dir": str(current_item.get("export_dir") or "").strip(),
                    "previous_export_dir": str(previous_item.get("export_dir") or "").strip(),
                }
            )
    changed_accounts.sort(
        key=lambda item: (abs(int(item.get("comment_delta") or 0)), abs(int(item.get("like_delta") or 0)), str(item.get("account") or "")),
        reverse=True,
    )
    return {
        "project": project_name,
        "current_snapshot_time": str(current_summary.get("snapshot_time") or "").strip(),
        "previous_snapshot_time": str(previous_summary.get("snapshot_time") or "").strip(),
        "current_snapshot_slug": str(current_summary.get("snapshot_slug") or "").strip(),
        "previous_snapshot_slug": str(previous_summary.get("snapshot_slug") or "").strip(),
        "current_account_count": int(current_summary.get("account_count") or 0),
        "previous_account_count": int(previous_summary.get("account_count") or 0),
        "current_like_count": int(current_summary.get("like_count") or 0),
        "previous_like_count": int(previous_summary.get("like_count") or 0),
        "current_comment_count": int(current_summary.get("comment_count") or 0),
        "previous_comment_count": int(previous_summary.get("comment_count") or 0),
        "account_count_delta": int(current_summary.get("account_count") or 0) - int(previous_summary.get("account_count") or 0),
        "like_count_delta": int(current_summary.get("like_count") or 0) - int(previous_summary.get("like_count") or 0),
        "comment_count_delta": int(current_summary.get("comment_count") or 0) - int(previous_summary.get("comment_count") or 0),
        "added_accounts": [
            {
                "account_id": account_id,
                "account": str((current_accounts.get(account_id) or {}).get("account") or "").strip(),
            }
            for account_id in added_account_ids
        ],
        "removed_accounts": [
            {
                "account_id": account_id,
                "account": str((previous_accounts.get(account_id) or {}).get("account") or "").strip(),
            }
            for account_id in removed_account_ids
        ],
        "changed_accounts": changed_accounts,
    }


def _build_project_compare_markdown(compare_payload: Dict[str, Any]) -> str:
    lines = [
        f"# {compare_payload.get('project') or '项目'} 快照对比",
        "",
        f"- 当前快照：{compare_payload.get('current_snapshot_time') or '-'}",
        f"- 对比快照：{compare_payload.get('previous_snapshot_time') or '-'}",
        f"- 账号数变化：{format_signed_number_for_export(compare_payload.get('account_count_delta'))}",
        f"- 点赞榜条数变化：{format_signed_number_for_export(compare_payload.get('like_count_delta'))}",
        f"- 评论榜条数变化：{format_signed_number_for_export(compare_payload.get('comment_count_delta'))}",
        "",
        "## 新增账号",
    ]
    added_accounts = compare_payload.get("added_accounts") or []
    if added_accounts:
        for item in added_accounts:
            lines.append(f"- {item.get('account')}（{item.get('account_id')}）")
    else:
        lines.append("- 无")
    lines.extend(["", "## 退出快照账号"])
    removed_accounts = compare_payload.get("removed_accounts") or []
    if removed_accounts:
        for item in removed_accounts:
            lines.append(f"- {item.get('account')}（{item.get('account_id')}）")
    else:
        lines.append("- 无")
    lines.extend(["", "## 变化账号"])
    changed_accounts = compare_payload.get("changed_accounts") or []
    if changed_accounts:
        for item in changed_accounts[:20]:
            lines.append(
                f"- {item.get('account')}｜点赞 {format_signed_number_for_export(item.get('like_delta'))}｜评论 {format_signed_number_for_export(item.get('comment_delta'))}"
            )
            like_compare = item.get("like_compare") or {}
            comment_compare = item.get("comment_compare") or {}
            if like_compare.get("new_entries"):
                lines.append(f"  点赞新进榜：{', '.join(entry.get('title') or '' for entry in like_compare['new_entries'][:3])}")
            if like_compare.get("moved_up"):
                lines.append(f"  点赞升榜：{', '.join(entry.get('title') or '' for entry in like_compare['moved_up'][:3])}")
            if like_compare.get("moved_down"):
                lines.append(f"  点赞降榜：{', '.join(entry.get('title') or '' for entry in like_compare['moved_down'][:3])}")
            if like_compare.get("dropped_entries"):
                lines.append(f"  点赞掉榜：{', '.join(entry.get('title') or '' for entry in like_compare['dropped_entries'][:3])}")
            if comment_compare.get("new_entries"):
                lines.append(f"  评论新进榜：{', '.join(entry.get('title') or '' for entry in comment_compare['new_entries'][:3])}")
            if comment_compare.get("moved_up"):
                lines.append(f"  评论升榜：{', '.join(entry.get('title') or '' for entry in comment_compare['moved_up'][:3])}")
            if comment_compare.get("moved_down"):
                lines.append(f"  评论降榜：{', '.join(entry.get('title') or '' for entry in comment_compare['moved_down'][:3])}")
            if comment_compare.get("dropped_entries"):
                lines.append(f"  评论掉榜：{', '.join(entry.get('title') or '' for entry in comment_compare['dropped_entries'][:3])}")
    else:
        lines.append("- 无明显变化")
    return "\n".join(lines) + "\n"


def format_signed_number_for_export(value: Any) -> str:
    number = int(value or 0)
    if number > 0:
        return f"+{number}"
    return str(number)


def load_latest_project_export_summary(*, project_name: str, export_dir: str = "") -> Dict[str, Any]:
    normalized_project_name = str(project_name or "").strip()
    if not normalized_project_name:
        return {}
    root_dir = Path(str(export_dir or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).strip() or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).expanduser().resolve()
    project_dir = root_dir / _safe_export_name(normalized_project_name, "未分组")
    summary = _load_json_if_exists(project_dir / "最近一次项目导出.json")
    return summary if isinstance(summary, dict) else {}


def export_single_account_rankings(
    *,
    payload: Dict[str, Any],
    account_id: str,
    project: str = "",
    export_dir: str = "",
) -> Dict[str, Any]:
    normalized_account_id = str(account_id or "").strip()
    if not normalized_account_id:
        raise ValueError("缺少账号ID，无法导出榜单")

    accounts = payload.get("accounts") or []
    rankings = payload.get("rankings") or {}
    account = next((item for item in accounts if str(item.get("account_id") or "").strip() == normalized_account_id), {})
    account_name = str(account.get("account") or normalized_account_id).strip() or normalized_account_id
    project_name = str(project or "未分组").strip() or "未分组"
    root_dir = Path(str(export_dir or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).strip() or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).expanduser().resolve()
    account_dir = root_dir / _safe_export_name(project_name, "未分组") / _safe_export_name(account_name, normalized_account_id)
    snapshot_time = datetime.now().astimezone()
    snapshot_label = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_slug = snapshot_time.strftime("%Y-%m-%d_%H%M%S")
    target_dir = account_dir / snapshot_slug
    like_rows, comment_rows = _build_account_export_rows(
        rankings=rankings,
        account=account,
        account_id=normalized_account_id,
        account_name=account_name,
        project_name=project_name,
    )
    if not like_rows and not comment_rows:
        raise ValueError("当前账号暂无可导出的点赞或评论榜单数据")
    target_dir.mkdir(parents=True, exist_ok=True)

    return _export_account_rankings_to_snapshot(
        rankings=rankings,
        account=account,
        account_id=normalized_account_id,
        account_name=account_name,
        project_name=project_name,
        account_dir=account_dir,
        target_dir=target_dir,
        snapshot_label=snapshot_label,
        snapshot_slug=snapshot_slug,
    )


def export_project_rankings(
    *,
    payload: Dict[str, Any],
    project: str,
    account_ids: List[str],
    export_dir: str = "",
) -> Dict[str, Any]:
    project_name = str(project or "").strip()
    if not project_name:
        raise ValueError("缺少项目名，无法导出项目快照")
    normalized_account_ids = [str(item or "").strip() for item in account_ids if str(item or "").strip()]
    if not normalized_account_ids:
        raise ValueError("当前项目没有可导出的账号")

    accounts = payload.get("accounts") or []
    account_index = {str(item.get("account_id") or "").strip(): dict(item) for item in accounts if str(item.get("account_id") or "").strip()}
    rankings = payload.get("rankings") or {}
    root_dir = Path(str(export_dir or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).strip() or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR).expanduser().resolve()
    project_dir = root_dir / _safe_export_name(project_name, "未分组")
    latest_project_summary_json = project_dir / "最近一次项目导出.json"
    previous_project_summary = _load_json_if_exists(latest_project_summary_json)
    snapshot_time = datetime.now().astimezone()
    snapshot_label = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_slug = snapshot_time.strftime("%Y-%m-%d_%H%M%S")
    snapshot_dir = project_dir / snapshot_slug

    exportable_accounts: List[Dict[str, Any]] = []
    for account_id in normalized_account_ids:
        account = account_index.get(account_id, {})
        account_name = str(account.get("account") or account_id).strip() or account_id
        like_rows, comment_rows = _build_account_export_rows(
            rankings=rankings,
            account=account,
            account_id=account_id,
            account_name=account_name,
            project_name=project_name,
        )
        if not like_rows and not comment_rows:
            continue
        exportable_accounts.append(
            {
                "account_id": account_id,
                "account": account,
                "account_name": account_name,
            }
        )
    if not exportable_accounts:
        raise ValueError("当前项目暂无可导出的账号榜单数据")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    exported_summaries: List[Dict[str, Any]] = []
    index_rows: List[Dict[str, Any]] = []
    for exportable in exportable_accounts:
        account_id = str(exportable.get("account_id") or "")
        account = dict(exportable.get("account") or {})
        account_name = str(exportable.get("account_name") or account_id).strip() or account_id
        account_dir = project_dir / _safe_export_name(account_name, account_id)
        target_dir = snapshot_dir / _safe_export_name(account_name, account_id)
        account_dir.mkdir(parents=True, exist_ok=True)
        target_dir.mkdir(parents=True, exist_ok=True)
        summary = _export_account_rankings_to_snapshot(
            rankings=rankings,
            account=account,
            account_id=account_id,
            account_name=account_name,
            project_name=project_name,
            account_dir=account_dir,
            target_dir=target_dir,
            snapshot_label=snapshot_label,
            snapshot_slug=snapshot_slug,
        )
        exported_summaries.append(summary)
        index_rows.append(
            {
                "项目": project_name,
                "账号ID": account_id,
                "账号": account_name,
                "快照时间": snapshot_label,
                "点赞榜条数": int(summary.get("like_count") or 0),
                "评论榜条数": int(summary.get("comment_count") or 0),
                "快照目录": str(summary.get("export_dir") or ""),
                "最近一次导出": str(summary.get("latest_summary_path") or ""),
            }
        )

    project_index_csv = snapshot_dir / "项目账号索引.csv"
    project_index_json = snapshot_dir / "项目账号索引.json"
    project_review_markdown = snapshot_dir / "项目复盘摘要.md"
    project_summary_json = snapshot_dir / "项目导出摘要.json"
    project_compare_json = snapshot_dir / "项目快照对比.json"
    project_compare_markdown = snapshot_dir / "项目快照对比.md"

    _write_rows_to_csv(project_index_csv, index_rows)
    project_index_json.write_text(json.dumps(index_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    project_review_markdown.write_text(
        _build_project_review_markdown(
            project_name=project_name,
            snapshot_label=snapshot_label,
            summaries=exported_summaries,
        ),
        encoding="utf-8",
    )
    project_summary = {
        "project": project_name,
        "snapshot_time": snapshot_label,
        "snapshot_slug": snapshot_slug,
        "project_dir": str(project_dir),
        "export_dir": str(snapshot_dir),
        "account_count": len(exported_summaries),
        "like_count": sum(int(item.get("like_count") or 0) for item in exported_summaries),
        "comment_count": sum(int(item.get("comment_count") or 0) for item in exported_summaries),
        "files": {
            "project_index_csv": str(project_index_csv),
            "project_index_json": str(project_index_json),
            "project_review_markdown": str(project_review_markdown),
        },
        "accounts": exported_summaries,
        "updated_at": iso_now(),
    }
    if previous_project_summary and str(previous_project_summary.get("snapshot_slug") or "").strip() != snapshot_slug:
        compare_payload = _build_project_compare_payload(
            project_name=project_name,
            current_summary=project_summary,
            previous_summary=previous_project_summary,
        )
        project_compare_json.write_text(json.dumps(compare_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        project_compare_markdown.write_text(_build_project_compare_markdown(compare_payload), encoding="utf-8")
        project_summary["compare"] = compare_payload
        project_summary["files"]["project_compare_json"] = str(project_compare_json)
        project_summary["files"]["project_compare_markdown"] = str(project_compare_markdown)
    project_summary_json.write_text(json.dumps(project_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_project_summary_json.write_text(json.dumps(project_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    project_summary["summary_path"] = str(project_summary_json)
    project_summary["latest_summary_path"] = str(latest_project_summary_json)
    return project_summary


def build_local_ranking_updates(reports: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    updates: Dict[str, List[Dict[str, Any]]] = {}
    ranking_groups = build_single_work_rankings(reports=reports, history_index={})
    for rank_type in ("单条点赞排行", "单条评论排行"):
        rows: List[Dict[str, Any]] = []
        for rank, item in enumerate(ranking_groups.get(rank_type, []), start=1):
            rows.append(build_ranking_item_from_fields(build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank)))
        updates[rank_type] = rows
    return updates


def ranking_match_key(item: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("account_id") or "").strip(),
        str(item.get("title") or "").strip(),
        str(item.get("cover_url") or "").strip(),
    )


def merge_ranking_row_with_existing(new_row: Dict[str, Any], existing_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged_row = dict(new_row)
    matched = None
    target_key = ranking_match_key(new_row)
    for item in existing_rows:
        if ranking_match_key(item) == target_key:
            matched = item
            break
    if matched is None:
        for item in existing_rows:
            if (
                str(item.get("account_id") or "").strip() == target_key[0]
                and str(item.get("title") or "").strip() == target_key[1]
            ):
                matched = item
                break
    if not matched:
        return merged_row
    for field in ("profile_url", "note_url", "cover_url"):
        if not str(merged_row.get(field) or "").strip() and str(matched.get(field) or "").strip():
            merged_row[field] = matched[field]
    if not str(merged_row.get("summary") or "").strip() and str(matched.get("summary") or "").strip():
        merged_row["summary"] = matched["summary"]
    return merged_row


def ranking_sort_key(rank_type: str, item: Dict[str, Any]) -> tuple[Any, ...]:
    metric = item.get("metric")
    try:
        metric_value = float(metric)
    except (TypeError, ValueError):
        metric_value = 0.0
    return (
        metric_value,
        str(item.get("title") or ""),
        str(item.get("account") or ""),
    )


def merge_rankings(
    base_rankings: Dict[str, List[Dict[str, Any]]],
    *,
    reports: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    merged: Dict[str, List[Dict[str, Any]]] = {
        rank_type: [dict(item) for item in rows]
        for rank_type, rows in (base_rankings or {}).items()
    }
    report_by_account_id = {
        str((report.get("profile") or {}).get("profile_user_id") or "").strip(): report
        for report in reports
        if str((report.get("profile") or {}).get("profile_user_id") or "").strip()
    }
    synced_account_ids = {
        account_id
        for account_id in report_by_account_id
    }
    ranking_updates = build_local_ranking_updates(reports)
    for rank_type, new_rows in ranking_updates.items():
        existing_rank_rows = [dict(item) for item in merged.get(rank_type, [])]
        preserved_existing_rows: List[Dict[str, Any]] = []
        for item in existing_rank_rows:
            account_id = str(item.get("account_id") or "").strip()
            if account_id not in synced_account_ids:
                preserved_existing_rows.append(item)
                continue
            report = report_by_account_id.get(account_id) or {}
            if rank_type in {"单条评论排行", "单条第二天增长排行"} and not report_has_comment_data(report):
                preserved_existing_rows.append(item)
        merged_new_rows = [merge_ranking_row_with_existing(item, existing_rank_rows) for item in new_rows]
        preserved_existing_rows.extend(merged_new_rows)
        existing_rows = preserved_existing_rows
        existing_rows.sort(key=lambda item: ranking_sort_key(rank_type, item), reverse=True)
        merged[rank_type] = existing_rows
    for rank_type in ("单条点赞排行", "单条评论排行", "单条第二天增长排行"):
        merged.setdefault(rank_type, [])
    return merged


def build_portal_from_accounts_and_rankings(
    *,
    accounts: List[Dict[str, Any]],
    rankings: Dict[str, List[Dict[str, Any]]],
    base_portal: Optional[Dict[str, Any]] = None,
    updated_at: str = "",
) -> Dict[str, Any]:
    portal = dict(base_portal or {})
    total_works = sum(int(item.get("works") or 0) for item in accounts)
    total_likes = sum(int(item.get("likes") or 0) for item in accounts)
    total_comments = sum(int(item.get("comments") or 0) for item in accounts)
    portal.update(
        {
            "updated_at": updated_at or str(portal.get("updated_at") or ""),
            "accounts": len(accounts),
            "fans": sum(int(item.get("fans") or 0) for item in accounts),
            "interaction": sum(int(item.get("interaction") or 0) for item in accounts),
            "works": total_works,
            "likes": total_likes,
            "comments": total_comments,
            "average_likes": round(total_likes / total_works, 2) if total_works else 0,
            "average_comments": round(total_comments / total_works, 2) if total_works else 0,
        }
    )
    top_rows = rankings.get("单条点赞排行") or []
    if top_rows:
        portal["top_title"] = str(top_rows[0].get("title") or "").strip()
        portal["top_account"] = str(top_rows[0].get("account") or "").strip()
        portal["top_like"] = int(float(top_rows[0].get("metric") or 0))
        portal["top_url"] = str(top_rows[0].get("note_url") or "").strip()
    return portal


def build_dashboard_payload_with_reports(
    *,
    base_payload: Optional[Dict[str, Any]],
    reports: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = copy.deepcopy(base_payload or {})
    account_series = merge_account_series_points(payload.get("account_series") or {}, reports)
    account_cards = {
        str(item.get("account_id") or "").strip(): dict(item)
        for item in (payload.get("accounts") or [])
        if str(item.get("account_id") or "").strip()
    }
    for report in reports:
        account_id = str((report.get("profile") or {}).get("profile_user_id") or "").strip()
        if not account_id:
            continue
        account_cards[account_id] = build_dashboard_account_card(report, existing_card=account_cards.get(account_id))
    accounts = list(account_cards.values())
    accounts.sort(
        key=lambda item: (
            int(item.get("fans") or 0),
            int(item.get("likes") or 0),
            int(item.get("comments") or 0),
            str(item.get("account") or ""),
        ),
        reverse=True,
    )

    rankings = merge_rankings(payload.get("rankings") or {}, reports=reports)
    latest_report_date = max((extract_snapshot_date(str(report.get("captured_at") or "")) for report in reports), default="")
    latest_report_time = max((str(report.get("captured_at") or "").strip() for report in reports), default="")
    latest_date = max(str(payload.get("latest_date") or "").strip(), latest_report_date)
    updated_at = max(str(payload.get("updated_at") or "").strip(), latest_report_time)

    merged_payload = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "latest_date": latest_date,
        "updated_at": updated_at,
        "series_meta": copy.deepcopy(payload.get("series_meta") or DASHBOARD_SERIES_META),
        "portal": build_portal_from_accounts_and_rankings(
            accounts=accounts,
            rankings=rankings,
            base_portal=payload.get("portal") or {},
            updated_at=updated_at,
        ),
        "series": rebuild_daily_series_from_account_series(account_series),
        "account_series": account_series,
        "accounts": accounts,
        "rankings": rankings,
        "alerts": copy.deepcopy(payload.get("alerts") or []),
    }
    merged_payload["series_meta"].setdefault("mode", DASHBOARD_SERIES_META["mode"])
    merged_payload["series_meta"].setdefault("update_time", DASHBOARD_SERIES_META["update_time"])
    merged_payload["series_meta"].setdefault("source", DASHBOARD_SERIES_META["source"])
    merged_payload["series_meta"].setdefault("note", DASHBOARD_SERIES_META["note"])
    return merged_payload


def build_mobile_rankings_payload(
    *,
    dashboard_payload: Dict[str, Any],
    monitored_entries: List[Dict[str, Any]],
    project: str = "",
) -> Dict[str, Any]:
    normalized_projects = sorted(
        {
            normalize_project_name(str(item.get("project") or DEFAULT_PROJECT_NAME))
            for item in (monitored_entries or [])
            if normalize_project_name(str(item.get("project") or DEFAULT_PROJECT_NAME))
        }
    )
    normalized_project = normalize_project_name(project) if str(project or "").strip() else ""
    if normalized_projects and normalized_project not in normalized_projects:
        normalized_project = normalized_projects[0]
    rankings = dashboard_payload.get("rankings") or {}
    all_history_rankings = dashboard_payload.get("history_rankings") or {}
    if normalized_project:
        project_account_ids = {
            str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
            for item in (monitored_entries or [])
            if normalize_project_name(str(item.get("project") or DEFAULT_PROJECT_NAME)) == normalized_project
            and str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
        }
    else:
        project_account_ids = {
            str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
            for item in (monitored_entries or [])
            if str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
        }
    project_accounts: List[Dict[str, Any]] = []
    for item in dashboard_payload.get("accounts") or []:
        if not isinstance(item, dict):
            continue
        account_id = str(item.get("account_id") or "").strip()
        if not account_id or (project_account_ids and account_id not in project_account_ids):
            continue
        project_accounts.append(
            {
                "account_id": account_id,
                "account": str(item.get("account") or account_id).strip() or account_id,
                "profile_url": str(item.get("profile_url") or "").strip(),
            }
        )
    if not project_accounts:
        account_name_index = {
            str(item.get("account_id") or "").strip(): str(item.get("account") or "").strip()
            for item in (dashboard_payload.get("accounts") or [])
            if isinstance(item, dict) and str(item.get("account_id") or "").strip()
        }
        for item in (monitored_entries or []):
            if not isinstance(item, dict):
                continue
            account_id = str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
            if not account_id or (project_account_ids and account_id not in project_account_ids):
                continue
            project_accounts.append(
                {
                    "account_id": account_id,
                    "account": account_name_index.get(account_id) or account_id,
                    "profile_url": str(item.get("url") or "").strip(),
                }
            )
    project_accounts.sort(key=lambda item: str(item.get("account") or ""))

    def filter_rows(rank_type: str) -> List[Dict[str, Any]]:
        rows = rankings.get(rank_type) or []
        if not normalized_project:
            return list(rows)
        return [dict(item) for item in rows if str(item.get("account_id") or "").strip() in project_account_ids]

    daily_history: List[Dict[str, Any]] = []
    account_series = dashboard_payload.get("account_series") or {}
    per_date: Dict[str, Dict[str, Any]] = {}
    for account_id in project_account_ids:
        for point in account_series.get(account_id) or []:
            date_text = str(point.get("date") or "").strip()
            if not date_text:
                continue
            bucket = per_date.setdefault(
                date_text,
                {
                    "date": date_text,
                    "fans": 0,
                    "interaction": 0,
                    "likes": 0,
                    "comments": 0,
                    "works": 0,
                    "accounts": 0,
                },
            )
            bucket["fans"] += int(point.get("fans") or 0)
            bucket["interaction"] += int(point.get("interaction") or 0)
            bucket["likes"] += int(point.get("likes") or 0)
            bucket["comments"] += int(point.get("comments") or 0)
            bucket["works"] += int(point.get("works") or 0)
            bucket["accounts"] += 1
    daily_history = sorted(per_date.values(), key=lambda item: str(item.get("date") or ""))
    project_history_rankings: Dict[str, Dict[str, Any]] = {}
    if normalized_project:
        raw_project_history = all_history_rankings.get(normalized_project) or {}
        if isinstance(raw_project_history, dict):
            project_history_rankings = {
                str(date_text): dict(item)
                for date_text, item in raw_project_history.items()
                if isinstance(item, dict)
            }
    latest_date = str(dashboard_payload.get("latest_date") or "").strip()
    if latest_date and latest_date not in project_history_rankings:
        project_history_rankings[latest_date] = {
            "date": latest_date,
            "snapshot_time": str(dashboard_payload.get("updated_at") or dashboard_payload.get("generated_at") or "").strip(),
            "snapshot_slug": "latest-cache",
            "account_count": len(project_account_ids),
            "likes": filter_rows("单条点赞排行"),
            "comments": filter_rows("单条评论排行"),
            "growth": filter_rows("单条第二天增长排行"),
        }

    return {
        "ok": True,
        "version": APP_VERSION,
        "project": normalized_project or "all",
        "projects": normalized_projects,
        "view_mode": "server_cache_only",
        "server_time": iso_now(),
        "updated_at": str(dashboard_payload.get("updated_at") or dashboard_payload.get("generated_at") or "").strip(),
        "generated_at": str(dashboard_payload.get("generated_at") or "").strip(),
        "server_received_at": str(dashboard_payload.get("server_received_at") or "").strip(),
        "latest_date": latest_date,
        "account_count": len(project_account_ids),
        "accounts": project_accounts,
        "rankings": {
            "likes": filter_rows("单条点赞排行"),
            "comments": filter_rows("单条评论排行"),
            "growth": filter_rows("单条第二天增长排行"),
        },
        "calendar": daily_history,
        "history_rankings": project_history_rankings,
    }


def refresh_project_export_snapshots(
    *,
    payload: Dict[str, Any],
    reports: List[Dict[str, Any]],
    fallback_project: str = "",
    export_dir: str = "",
) -> List[Dict[str, Any]]:
    grouped_account_ids: Dict[str, List[str]] = {}
    normalized_fallback_project = normalize_project_name(fallback_project) if str(fallback_project or "").strip() else ""
    for report in reports:
        profile = report.get("profile") or {}
        account_id = str(profile.get("profile_user_id") or "").strip()
        if not account_id:
            continue
        project_name = normalize_project_name(
            str(report.get("project") or normalized_fallback_project or DEFAULT_PROJECT_NAME)
        )
        grouped_account_ids.setdefault(project_name, [])
        if account_id not in grouped_account_ids[project_name]:
            grouped_account_ids[project_name].append(account_id)

    summaries: List[Dict[str, Any]] = []
    for project_name, account_ids in grouped_account_ids.items():
        if not account_ids:
            continue
        try:
            summaries.append(
                export_project_rankings(
                    payload=payload,
                    project=project_name,
                    account_ids=account_ids,
                    export_dir=export_dir,
                )
            )
        except Exception:
            continue
    return summaries


class DashboardStore:
    def __init__(self, *, env_file: str, cache_seconds: int = 30, local_override_ttl_seconds: int = 900) -> None:
        self.env_file = env_file
        self.cache_seconds = cache_seconds
        self.local_override_ttl_seconds = max(cache_seconds, int(local_override_ttl_seconds or 0))
        self._lock = threading.Lock()
        self._cached_at = 0.0
        self._local_override_at = 0.0
        self._payload: Dict[str, Any] = {}
        self._local_override_payload: Dict[str, Any] = {}
        self._last_error = ""

    def get_payload(self, *, force: bool = False) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            if self._local_override_payload:
                if (now - self._local_override_at) < self.local_override_ttl_seconds:
                    payload = copy.deepcopy(self._local_override_payload)
                    payload["stale"] = False
                    payload["local_override"] = True
                    payload["cache_age_seconds"] = int(now - self._local_override_at)
                    payload["load_error"] = ""
                    return payload
                self._local_override_payload = {}
                self._local_override_at = 0.0
            if not force and self._payload and (now - self._cached_at) < self.cache_seconds:
                payload = copy.deepcopy(self._payload)
                payload["stale"] = False
                payload["local_override"] = False
                payload["cache_age_seconds"] = int(now - self._cached_at)
                payload["load_error"] = ""
                return payload
        try:
            payload = load_dashboard_payload(self.env_file)
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)
                if self._payload:
                    payload = copy.deepcopy(self._payload)
                    payload["stale"] = True
                    payload["local_override"] = False
                    payload["cache_age_seconds"] = int(now - self._cached_at)
                    payload["load_error"] = self._last_error
                    return payload
                return build_empty_dashboard_payload(load_error=self._last_error)
        with self._lock:
            self._payload = payload
            self._cached_at = time.time()
            self._last_error = ""
            cloned = copy.deepcopy(self._payload)
        cloned["stale"] = False
        cloned["local_override"] = False
        cloned["cache_age_seconds"] = 0
        cloned["load_error"] = ""
        return cloned

    def get_cached_payload(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._payload)

    def peek_payload(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            if self._local_override_payload and (now - self._local_override_at) < self.local_override_ttl_seconds:
                return copy.deepcopy(self._local_override_payload)
            return copy.deepcopy(self._payload)

    def set_local_override(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._local_override_payload = copy.deepcopy(payload or {})
            self._local_override_at = time.time() if self._local_override_payload else 0.0

    def commit_local_override(self) -> bool:
        with self._lock:
            if not self._local_override_payload:
                return False
            self._payload = copy.deepcopy(self._local_override_payload)
            self._cached_at = time.time()
            self._local_override_payload = {}
            self._local_override_at = 0.0
            self._last_error = ""
            return True

    def invalidate(self, *, clear_override: bool = False) -> None:
        with self._lock:
            self._payload = {}
            self._cached_at = 0.0
            self._last_error = ""
            if clear_override:
                self._local_override_payload = {}
                self._local_override_at = 0.0


class LoginStateStore:
    def __init__(self, *, env_file: str, urls_file: str, cache_seconds: int = 600) -> None:
        self.env_file = env_file
        self.urls_file = urls_file
        self.cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._payload: Dict[str, Any] = {}
        self._cached_at = 0.0
        self._running = False
        self._sample_url = ""

    def get_payload(self, *, force: bool = False, sample_url: str = "") -> Dict[str, Any]:
        normalized_sample_url = normalize_profile_url(sample_url)
        now = time.time()
        with self._lock:
            needs_refresh = force or not self._payload or (now - self._cached_at) >= self.cache_seconds
            if needs_refresh and not self._running:
                self._running = True
                self._sample_url = normalized_sample_url or self._resolve_sample_url_locked()
                threading.Thread(target=self._run_check, daemon=True).start()
            payload = copy.deepcopy(self._payload) if self._payload else build_login_state_payload()
            payload["checking"] = self._running
            payload["cache_age_seconds"] = int(now - self._cached_at) if self._cached_at else 0
            if self._running and not payload.get("checked_at"):
                payload["state"] = "checking"
                payload["message"] = "正在检查登录态与样本账号抓取能力..."
        return payload

    def set_payload(self, payload: Dict[str, Any], *, running: bool = False, sample_url: str = "") -> None:
        with self._lock:
            self._payload = copy.deepcopy(payload or {})
            self._cached_at = time.time() if self._payload else 0.0
            self._running = bool(running)
            if sample_url:
                self._sample_url = normalize_profile_url(sample_url)

    def _resolve_sample_url_locked(self) -> str:
        entries = parse_monitored_entries(self.urls_file)
        for entry in entries:
            if entry.get("active"):
                return str(entry.get("url") or "")
        if entries:
            return str(entries[0].get("url") or "")
        return ""

    def _run_check(self) -> None:
        sample_url = ""
        with self._lock:
            sample_url = self._sample_url
        try:
            settings = load_settings(self.env_file)
            payload = run_login_state_self_check(env_file=self.env_file, sample_url=sample_url)
            if login_state_requires_interactive_login(payload):
                window_opened = open_xiaohongshu_login_window(
                    settings=settings,
                    target_url=sample_url or "https://www.xiaohongshu.com/",
                )
                payload = dict(payload)
                payload["login_window_opened"] = window_opened
                payload["message"] = (
                    "检测到小红书未登录，已弹出网页登录窗口，完成登录后再点一次“立即自检”即可。"
                    if window_opened
                    else "检测到小红书未登录，请先完成登录后再点一次“立即自检”。"
                )
                hints = list(payload.get("hints") or [])
                if window_opened:
                    hints.insert(0, "当前已弹出网页登录窗口；登录完成后再自检一次即可刷新状态。")
                else:
                    hints.insert(0, "未能自动打开网页登录窗口，请先手动登录。")
                payload["hints"] = hints[:3]
        except Exception as exc:
            payload = build_login_state_payload(
                state="error",
                message=f"登录态自检失败：{exc}",
                checked_at=iso_now(),
                sample_url=sample_url,
                degraded=True,
                hints=["可先手动同步一次，确认当前抓取链路是否仍可用。"],
            )
        with self._lock:
            self._payload = payload
            self._cached_at = time.time()
            self._running = False


def build_sync_progress(
    *,
    phase: str,
    current: int,
    total: int,
    account: str = "",
    works: int = 0,
    status: str = "",
    success_count: int = 0,
    failed_count: int = 0,
    started_at: str = "",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    safe_total = max(1, int(total or 0))
    safe_current = max(0, min(int(current or 0), safe_total))
    normalized_status = str(status or "").strip().lower()
    phase_label = "准备中"
    overall_percent = 0
    detail_text = "准备开始同步"
    if phase == "login":
        phase_label = "等待网页登录"
        phase_percent = 0
        overall_percent = 0
        detail_text = status or "检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。"
    elif phase == "collect":
        phase_label = "抓取账号数据"
        if normalized_status == "running" and safe_current > 0:
            effective_current = max(0.0, safe_current - 0.5)
            phase_percent = round((effective_current / safe_total) * 100)
            overall_percent = round((effective_current / safe_total) * 50)
            detail_text = f"正在抓取第 {safe_current}/{safe_total} 个账号"
        else:
            phase_percent = round((safe_current / safe_total) * 100)
            overall_percent = round((safe_current / safe_total) * 50)
            detail_text = f"正在抓取账号 {safe_current}/{safe_total}"
    elif phase == "sync":
        phase_label = "推送服务器缓存"
        phase_percent = round((safe_current / safe_total) * 100)
        overall_percent = 50 + round((safe_current / safe_total) * 50)
        detail_text = f"正在推送服务器缓存 {safe_current}/{safe_total}"
    else:
        phase_percent = 0

    if account:
        detail_text += f" · {account}"
    if works:
        detail_text += f" · {works} 条作品"
    if status and status != detail_text and normalized_status not in {"running", "success", "failed"}:
        detail_text += f" · {status}"

    timing = build_progress_timing(started_at=started_at, overall_percent=overall_percent, now=now)

    return {
        "phase": phase,
        "phase_label": phase_label,
        "current": safe_current,
        "total": safe_total,
        "phase_percent": phase_percent,
        "overall_percent": overall_percent,
        "account": account,
        "works": max(0, int(works or 0)),
        "status": status,
        "success_count": max(0, int(success_count or 0)),
        "failed_count": max(0, int(failed_count or 0)),
        "detail_text": detail_text,
        "elapsed_seconds": timing["elapsed_seconds"],
        "elapsed_text": timing["elapsed_text"],
        "eta_seconds": timing["eta_seconds"],
        "eta_text": timing["eta_text"],
    }


def build_progress_timing(*, started_at: str, overall_percent: int, now: Optional[datetime] = None) -> Dict[str, Any]:
    if not started_at:
        return {"elapsed_seconds": 0, "elapsed_text": "", "eta_seconds": 0, "eta_text": ""}
    current_time = now or datetime.now().astimezone()
    try:
        started = datetime.fromisoformat(started_at)
    except ValueError:
        return {"elapsed_seconds": 0, "elapsed_text": "", "eta_seconds": 0, "eta_text": ""}
    elapsed_seconds = max(0, int((current_time - started).total_seconds()))
    eta_seconds = 0
    if 0 < overall_percent < 100 and elapsed_seconds > 0:
        remaining_ratio = (100 - overall_percent) / overall_percent
        eta_seconds = max(1, int(round(elapsed_seconds * remaining_ratio)))
    return {
        "elapsed_seconds": elapsed_seconds,
        "elapsed_text": format_duration_text(elapsed_seconds) if elapsed_seconds else "",
        "eta_seconds": eta_seconds,
        "eta_text": format_duration_text(eta_seconds) if eta_seconds else "",
    }


def format_duration_text(seconds: int) -> str:
    total_seconds = max(0, int(seconds or 0))
    if total_seconds < 60:
        return f"{total_seconds}秒"
    minutes, remaining_seconds = divmod(total_seconds, 60)
    if minutes < 60:
        if remaining_seconds:
            return f"{minutes}分{remaining_seconds}秒"
        return f"{minutes}分"
    hours, remaining_minutes = divmod(minutes, 60)
    if remaining_minutes:
        return f"{hours}小时{remaining_minutes}分"
    return f"{hours}小时"


class MonitoringSyncStore:
    def __init__(
        self,
        *,
        env_file: str,
        urls_file: str,
        dashboard_store: DashboardStore,
        login_state_store: Optional[LoginStateStore] = None,
        manual_sync_cooldown_seconds: int = 1200,
        profile_table_name: str = PROFILE_TABLE_NAME,
        works_table_name: str = WORKS_TABLE_NAME,
        ensure_fields: bool = True,
        sync_dashboard: bool = True,
    ) -> None:
        self.env_file = env_file
        self.urls_file = urls_file
        self.dashboard_store = dashboard_store
        self.login_state_store = login_state_store
        self.manual_sync_cooldown_seconds = max(0, int(manual_sync_cooldown_seconds or 0))
        self.profile_table_name = profile_table_name
        self.works_table_name = works_table_name
        self.ensure_fields = ensure_fields
        self.sync_dashboard = sync_dashboard
        self._lock = threading.Lock()
        self._running = False
        self._pending_resync = False
        self._current_sync_urls: List[str] = []
        self._current_sync_project = ""
        self._pending_sync_urls: List[str] = []
        self._manual_last_requested_at = 0.0
        self._current_sync_mode = ""
        self._auto_project_success_dates: Dict[str, str] = {}
        self._auto_project_last_attempt_at: Dict[str, float] = {}
        self._profile_lookup_cache_rows: List[Dict[str, Any]] = []
        self._profile_lookup_cache_error = ""
        self._profile_lookup_cache_loaded_at = 0.0
        self._status: Dict[str, Any] = {
            "state": "idle",
            "message": "待命",
            "started_at": "",
            "finished_at": "",
            "last_success_at": "",
            "last_error": "",
            "pending": False,
            "progress": {},
            "summary": {},
        }
        self._server_push_running = False
        self._last_auto_server_push_success_date = ""
        self._last_auto_server_push_attempt_at = 0.0
        self._server_push_status: Dict[str, Any] = {
            "state": "idle",
            "message": "每天 14:00-15:00 自动全量采集，成功后自动上传服务器",
            "started_at": "",
            "finished_at": "",
            "last_success_at": "",
            "last_error": "",
            "mode": "",
            "daily_at": AUTO_SERVER_CACHE_PUSH_DAILY_AT,
            "next_auto_run_at": "",
        }
        with self._lock:
            self._update_server_push_schedule_locked()
        threading.Thread(target=self._auto_collection_loop, daemon=True).start()

    def get_payload(self) -> Dict[str, Any]:
        with self._lock:
            entries = parse_monitored_entries(self.urls_file)
            metadata_index = load_monitored_metadata(self.urls_file)
            settings = load_settings(self.env_file)
            profile_rows, profile_lookup_error = self._get_profile_lookup_rows_locked()
            dashboard_payload = self.dashboard_store.peek_payload()
            dashboard_account_index = build_dashboard_account_index(dashboard_payload.get("accounts") or [])
            enriched_entries = enrich_monitored_entries(
                entries,
                profile_rows,
                metadata_index,
                dashboard_account_index=dashboard_account_index,
            )
            enriched_entries.sort(
                key=lambda item: (
                    str(item.get("project") or ""),
                    0 if item.get("active") else 1,
                    str(item.get("account") or item.get("account_id") or item.get("url") or "").lower(),
                )
            )
            active_entries = [entry for entry in enriched_entries if entry.get("active")]
            sample_url = ""
            if active_entries:
                sample_url = str(active_entries[0].get("url") or "")
            elif enriched_entries:
                sample_url = str(enriched_entries[0].get("url") or "")
            project_summaries = attach_project_sync_statuses(build_project_summaries(enriched_entries), urls_file=self.urls_file)
            for item in project_summaries:
                item["latest_export"] = load_latest_project_export_summary(
                    project_name=str(item.get("name") or ""),
                    export_dir="",
                )
            sync_status = self._status_snapshot_locked()
            schedule_driver = str(getattr(settings, "xhs_schedule_driver", "app") or "app").strip().lower() or "app"
            sync_status["schedule_driver"] = schedule_driver
            if schedule_driver == "launchd":
                current = datetime.now().astimezone()
                next_run = self._daily_clock_datetime(current, str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00"))
                if next_run <= current:
                    next_run = self._daily_clock_datetime(current + timedelta(days=1), str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00"))
                sync_status["server_cache_push_status"] = {
                    **(sync_status.get("server_cache_push_status") or {}),
                    "daily_at": str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00"),
                    "next_auto_run_at": next_run.isoformat(timespec="seconds"),
                    "message": "当前由 launchd 在 14:00-15:00 自动采集并在成功后上传服务器",
                    "mode": "launchd",
                }
            sync_status["launchd_status"] = load_local_daily_sync_status(
                env_file=self.env_file,
                state_file_path=str(getattr(settings, "state_file", "") or ""),
            )
            sync_status["schedule_plan"] = build_collection_schedule_plan(
                settings=settings,
                entries=enriched_entries,
            )
            return {
                "urls_file": str(resolve_text_path(self.urls_file)),
                "total": len(enriched_entries),
                "active_count": len(active_entries),
                "paused_count": len(enriched_entries) - len(active_entries),
                "urls": [entry["url"] for entry in active_entries],
                "entries": enriched_entries,
                "projects": project_summaries,
                "profile_lookup_error": profile_lookup_error,
                "login_state": (
                    self.login_state_store.get_payload(sample_url=sample_url)
                    if self.login_state_store
                    else build_login_state_payload()
                ),
                "proxy_pool": build_proxy_pool_status(settings),
                "sync_status": sync_status,
            }

    def _get_profile_lookup_rows_locked(self) -> tuple[List[Dict[str, Any]], str]:
        now = time.time()
        cache_ttl_seconds = 180
        if now - self._profile_lookup_cache_loaded_at <= cache_ttl_seconds:
            return list(self._profile_lookup_cache_rows), str(self._profile_lookup_cache_error or "")
        try:
            profile_rows = load_profile_table_rows(self.env_file)
            self._profile_lookup_cache_rows = list(profile_rows)
            self._profile_lookup_cache_error = ""
        except Exception as exc:
            self._profile_lookup_cache_rows = []
            self._profile_lookup_cache_error = str(exc)
        self._profile_lookup_cache_loaded_at = now
        return list(self._profile_lookup_cache_rows), str(self._profile_lookup_cache_error or "")

    def _build_manual_cooldown_locked(self) -> Dict[str, Any]:
        now = time.time()
        remaining_seconds = 0
        available_at = ""
        if self.manual_sync_cooldown_seconds > 0 and self._manual_last_requested_at > 0:
            remaining_seconds = max(
                0,
                int(round((self._manual_last_requested_at + self.manual_sync_cooldown_seconds) - now)),
            )
            if remaining_seconds > 0:
                available_at = datetime.fromtimestamp(
                    self._manual_last_requested_at + self.manual_sync_cooldown_seconds
                ).astimezone().isoformat(timespec="seconds")
        return {
            "manual_cooldown_seconds_remaining": remaining_seconds,
            "manual_cooldown_text": format_duration_text(remaining_seconds) if remaining_seconds else "",
            "manual_available_at": available_at,
            "manual_sync_locked": remaining_seconds > 0,
        }

    def _status_snapshot_locked(self) -> Dict[str, Any]:
        snapshot = dict(self._status)
        if contains_legacy_feishu_error(
            message=str(snapshot.get("message") or ""),
            error=str(snapshot.get("last_error") or ""),
        ):
            snapshot["last_error"] = ""
            if str(snapshot.get("state") or "") == "error":
                snapshot["state"] = "idle"
                snapshot["message"] = "待命"
                snapshot["progress"] = {}
                snapshot["summary"] = {}
        snapshot.update(self._build_manual_cooldown_locked())
        snapshot["server_cache_push_status"] = dict(self._server_push_status)
        snapshot["upload_status"] = {
            "state": "disabled",
            "message": "旧的外部协作上传入口已移除，当前只保留本地缓存与服务器查看。",
            "scope": "disabled",
            "started_at": "",
            "finished_at": "",
            "last_success_at": "",
            "last_error": "",
            "pending": False,
            "progress": {},
            "summary": {},
            "has_retry_payload": False,
            "has_cached_payload": False,
        }
        return snapshot

    @staticmethod
    def _daily_clock_datetime(now: datetime, daily_at: str) -> datetime:
        hour_text, minute_text = str(daily_at or AUTO_SERVER_CACHE_PUSH_DAILY_AT).split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _compute_next_auto_server_push_locked(self, now: Optional[datetime] = None) -> datetime:
        current = now or datetime.now().astimezone()
        settings = load_settings(self.env_file)
        entries = parse_monitored_entries(self.urls_file)
        plan = self._build_auto_project_plan(settings=settings, entries=entries, now=current)
        if not plan:
            return self._daily_clock_datetime(current + timedelta(days=1), AUTO_SERVER_CACHE_PUSH_DAILY_AT)
        today_text = current.date().isoformat()
        pending_runs: List[datetime] = []
        for project_name, payload in plan.items():
            success_date = self._auto_project_success_dates.get(project_name, "")
            if success_date == today_text:
                continue
            scheduled_at: datetime = payload["scheduled_at"]
            last_attempt_at = float(self._auto_project_last_attempt_at.get(project_name) or 0.0)
            if current < scheduled_at:
                pending_runs.append(scheduled_at)
                continue
            if last_attempt_at > 0:
                retry_at = datetime.fromtimestamp(last_attempt_at, tz=current.tzinfo) + timedelta(seconds=AUTO_PROJECT_SYNC_RETRY_SECONDS)
                pending_runs.append(max(current, retry_at))
            else:
                pending_runs.append(current)
        if pending_runs:
            return min(pending_runs)
        if self._last_auto_server_push_success_date == today_text:
            return self._daily_clock_datetime(current + timedelta(days=1), AUTO_SERVER_CACHE_PUSH_DAILY_AT)
        if self._last_auto_server_push_attempt_at > 0:
            retry_at = datetime.fromtimestamp(self._last_auto_server_push_attempt_at, tz=current.tzinfo) + timedelta(seconds=AUTO_SERVER_CACHE_PUSH_RETRY_SECONDS)
            return max(current, retry_at)
        return current

    def _update_server_push_schedule_locked(self, now: Optional[datetime] = None) -> None:
        next_run = self._compute_next_auto_server_push_locked(now)
        self._server_push_status["daily_at"] = AUTO_SERVER_CACHE_PUSH_DAILY_AT
        self._server_push_status["next_auto_run_at"] = next_run.isoformat(timespec="seconds")

    @staticmethod
    def _build_project_url_map(entries: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        grouped: Dict[str, List[str]] = {}
        for entry in entries:
            if not entry.get("active"):
                continue
            normalized = normalize_project_name(entry.get("project"))
            url = str(entry.get("url") or "").strip()
            if not url:
                continue
            grouped.setdefault(normalized, []).append(url)
        return grouped

    def _build_auto_project_plan(self, *, settings, entries: List[Dict[str, Any]], now: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        return build_auto_project_schedule(settings=settings, entries=entries, now=now)

    def _pick_due_auto_project_locked(self, now: datetime) -> tuple[str, List[str]]:
        settings = load_settings(self.env_file)
        entries = parse_monitored_entries(self.urls_file)
        plan = self._build_auto_project_plan(settings=settings, entries=entries, now=now)
        self._update_server_push_schedule_locked(now)
        if not plan:
            self._server_push_status["message"] = "当前没有可自动采集的项目，采集成功后仍会自动上传"
            return "", []
        today_text = now.date().isoformat()
        for project_name, payload in plan.items():
            if self._auto_project_success_dates.get(project_name, "") == today_text:
                continue
            scheduled_at: datetime = payload["scheduled_at"]
            if now < scheduled_at:
                continue
            last_attempt_at = float(self._auto_project_last_attempt_at.get(project_name) or 0.0)
            if last_attempt_at and now.timestamp() - last_attempt_at < AUTO_PROJECT_SYNC_RETRY_SECONDS:
                continue
            self._auto_project_last_attempt_at[project_name] = now.timestamp()
            self._server_push_status.update(
                {
                    "state": "waiting_sync",
                    "message": f"项目「{project_name}」自动采集中，完成后再上传服务器",
                    "mode": "auto",
                    "last_error": "",
                }
            )
            return project_name, list(payload["urls"])
        return "", []

    def push_server_cache(self, *, auto: bool = False, account_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        started_at = iso_now()
        normalized_account_ids = [
            str(item or "").strip()
            for item in (account_ids or [])
            if str(item or "").strip()
        ]
        with self._lock:
            if self._server_push_running:
                if auto:
                    return {"ok": False, "message": "已有服务器缓存上传任务正在执行"}
                raise ValueError("当前已有服务器缓存上传任务在执行，请稍后再试")
            self._server_push_running = True
            self._server_push_status.update(
                {
                    "state": "running",
                    "message": (
                        "自动采集已完成，正在上传到服务器"
                        if auto
                        else f"正在上传 {len(normalized_account_ids)} 个账号到服务器"
                        if normalized_account_ids
                        else "手动上传到服务器中"
                    ),
                    "started_at": started_at,
                    "finished_at": "",
                    "last_error": "",
                    "last_success_at": str(self._server_push_status.get("last_success_at") or ""),
                    "mode": "auto" if auto else "manual",
                }
            )
            if auto:
                self._last_auto_server_push_attempt_at = time.time()
            self._update_server_push_schedule_locked()
        try:
            result = push_current_cache_to_server(
                env_file=self.env_file,
                urls_file=self.urls_file,
                account_ids=normalized_account_ids,
            )
            finished_at = iso_now()
            with self._lock:
                self._server_push_status.update(
                    {
                        "state": "success",
                        "message": (
                            "服务器缓存自动上传完成"
                            if auto
                            else f"已把 {len(normalized_account_ids)} 个账号增量上传到服务器"
                            if normalized_account_ids
                            else "服务器缓存手动上传完成"
                        ),
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "last_success_at": finished_at,
                        "last_error": "",
                        "mode": "auto" if auto else "manual",
                    }
                )
                if auto:
                    self._last_auto_server_push_success_date = datetime.now().astimezone().date().isoformat()
                self._update_server_push_schedule_locked()
            return result
        except Exception as exc:
            finished_at = iso_now()
            with self._lock:
                self._server_push_status.update(
                    {
                        "state": "error",
                        "message": f"服务器缓存上传失败：{exc}",
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "last_success_at": str(self._server_push_status.get("last_success_at") or ""),
                        "last_error": str(exc),
                        "mode": "auto" if auto else "manual",
                    }
                )
                self._update_server_push_schedule_locked()
            raise
        finally:
            with self._lock:
                self._server_push_running = False

    def _auto_collection_loop(self) -> None:
        while True:
            time.sleep(AUTO_SERVER_CACHE_PUSH_POLL_SECONDS)
            try:
                settings = load_settings(self.env_file)
                if str(getattr(settings, "xhs_schedule_driver", "app") or "app").strip().lower() == "launchd":
                    with self._lock:
                        self._server_push_status.update(
                            {
                                "state": "idle",
                                "message": "当前由 launchd 在 14:00-15:00 自动采集并在成功后上传服务器",
                                "mode": "launchd",
                                "last_error": "",
                            }
                        )
                        self._update_server_push_schedule_locked(datetime.now().astimezone())
                    continue
                server_url = str(getattr(settings, "server_cache_push_url", "") or "").strip()
                now = datetime.now().astimezone()
                with self._lock:
                    if not server_url:
                        self._server_push_status.update(
                            {
                                "state": "idle",
                                "message": "已开启每天 14:00-15:00 自动采集；填写服务器地址后会在采集成功后自动上传",
                                "mode": "",
                                "last_error": "",
                            }
                        )
                        self._update_server_push_schedule_locked(now)
                        continue
                    if self._running:
                        self._update_server_push_schedule_locked(now)
                        continue
                    project_name, urls = self._pick_due_auto_project_locked(now)
                    if not project_name or not urls:
                        continue
                    self._request_sync_locked(
                        reason=f"按计划自动同步项目「{project_name}」的 {len(urls)} 个账号",
                        urls=urls,
                        project=project_name,
                        mode="auto",
                    )
            except Exception:
                continue

    def _has_cached_upload_payload_locked(self) -> bool:
        return False

    @staticmethod
    def _normalize_upload_scope(scope: str) -> str:
        normalized = str(scope or "").strip().lower()
        if normalized in {"calendar", "rankings", "full"}:
            return normalized
        return "full"

    @staticmethod
    def _describe_upload_scope(scope: str, *, project: str = "") -> str:
        normalized_scope = MonitoringSyncStore._normalize_upload_scope(scope)
        project_name = str(project or "").strip()
        target_text = f"项目「{project_name}」" if project_name else "全部项目"
        if normalized_scope == "calendar":
            return f"{target_text}日历留底"
        if normalized_scope == "rankings":
            return f"{target_text}排行榜"
        return f"{target_text}缓存数据"

    def _start_upload_job_locked(self, upload_job: Dict[str, Any]) -> None:
        reports = [dict(item) for item in (upload_job.get("reports") or []) if isinstance(item, dict)]
        estimated_total = max(1, int(upload_job.get("estimated_total") or len(reports) or 1))
        started_at = iso_now()
        upload_scope = self._normalize_upload_scope(str(upload_job.get("upload_scope") or "full"))
        upload_project = str(upload_job.get("project") or "").strip()
        self._upload_running = True
        self._upload_status = {
            "state": "running",
            "message": f"{self._describe_upload_scope(upload_scope, project=upload_project)}上传中",
            "scope": upload_scope,
            "started_at": started_at,
            "finished_at": "",
            "last_success_at": self._upload_status.get("last_success_at", ""),
            "last_error": "",
            "pending": False,
            "progress": build_sync_progress(
                phase="sync",
                current=0,
                total=estimated_total,
                success_count=0,
                failed_count=0,
                started_at=started_at,
            ),
            "summary": {},
        }
        threading.Thread(target=self._upload_loop, args=(upload_job,), daemon=True).start()

    def _stage_feishu_upload(self, *, reports: List[Dict[str, Any]], settings, project: str) -> str:
        upload_job = {
            "reports": [copy.deepcopy(report) for report in reports],
            "settings": copy.deepcopy(settings),
            "project": str(project or "").strip(),
        }
        with self._lock:
            self._last_upload_retry_job = copy.deepcopy(upload_job)
            if self._upload_running:
                self._pending_upload_job = upload_job
                self._upload_status["pending"] = True
                self._upload_status["message"] = "已准备新的缓存上传任务，当前上传完成后可手动再次上传"
                return "staged_pending"
            self._upload_status.update(
                {
                    "state": "idle",
                    "message": "本地看板已更新，可按需继续推送缓存",
                    "finished_at": iso_now(),
                    "last_error": "",
                    "pending": False,
                    "progress": {},
                    "summary": {},
                }
            )
        return "staged"

    def retry_feishu_upload(self, *, project: str = "", scope: str = "full") -> Dict[str, Any]:
        return {
            "ok": False,
            "message": "旧的外部协作上传入口已移除，当前只保留本地缓存和手机查看。",
            "sync_status": self._status_snapshot_locked(),
        }

    def _handle_upload_progress_update(self, payload: Dict[str, Any]) -> None:
        progress = build_sync_progress(
            phase="sync",
            current=int(payload.get("current") or 0),
            total=int(payload.get("total") or 0),
            account=str(payload.get("account") or payload.get("url") or ""),
            works=int(payload.get("works") or 0),
            status=str(payload.get("status") or ""),
            success_count=int(payload.get("success_count") or 0),
            failed_count=int(payload.get("failed_count") or 0),
            started_at=str(self._upload_status.get("started_at") or ""),
        )
        with self._lock:
            if self._upload_status.get("state") not in {"running", "queued"}:
                return
            self._upload_status["state"] = "running"
            self._upload_status["progress"] = progress
            self._upload_status["message"] = progress.get("detail_text") or self._upload_status.get("message", "")

    def _upload_loop(self, upload_job: Dict[str, Any]) -> None:
        finished_at = iso_now()
        try:
            reports = [dict(item) for item in (upload_job.get("reports") or []) if isinstance(item, dict)]
            settings = upload_job.get("settings")
            upload_mode = str(upload_job.get("mode") or "").strip()
            upload_scope = self._normalize_upload_scope(str(upload_job.get("upload_scope") or "full"))
            upload_project = str(upload_job.get("project") or "").strip()
            latest_only = bool(upload_job.get("latest_only"))
            if upload_mode == "cache":
                summary = sync_cached_project_rankings_to_feishu(
                    settings=settings,
                    project=upload_project,
                    progress_callback=self._handle_upload_progress_update,
                    upload_calendar=upload_scope in {"full", "calendar"},
                    upload_rankings=upload_scope in {"full", "rankings"},
                    latest_only=latest_only,
                )
            else:
                summary = sync_reports_to_feishu(
                    reports=reports,
                    settings=settings,
                    profile_table_name=self.profile_table_name,
                    works_table_name=self.works_table_name,
                    ensure_fields=self.ensure_fields,
                    sync_dashboard=self.sync_dashboard,
                    progress_callback=self._handle_upload_progress_update,
                )
            finished_at = iso_now()
            timing = build_progress_timing(
                started_at=str(self._upload_status.get("started_at") or ""),
                overall_percent=100,
            )
            success_count = int(summary.get("successful_accounts") or summary.get("project_count") or 0)
            total_count = int(summary.get("total_accounts") or summary.get("project_count") or upload_job.get("estimated_total") or len(reports) or 1)
            total_works = int(summary.get("total_works") or 0)
            result = {
                "state": "success",
                "message": f"{self._describe_upload_scope(upload_scope, project=upload_project)}上传完成，账号 {success_count} 个，作品 {total_works} 条",
                "scope": upload_scope,
                "started_at": "",
                "finished_at": finished_at,
                "last_success_at": finished_at,
                "last_error": "",
                "pending": False,
                "progress": {
                    "phase": "done",
                    "phase_label": "缓存上传完成",
                    "current": success_count,
                    "total": total_count,
                    "phase_percent": 100,
                    "overall_percent": 100,
                    "account": "",
                    "works": total_works,
                    "status": "success",
                    "success_count": success_count,
                    "failed_count": int(summary.get("failed_accounts") or 0),
                    "detail_text": f"已完成 {success_count} 个账号缓存上传",
                    "elapsed_seconds": timing["elapsed_seconds"],
                    "elapsed_text": timing["elapsed_text"],
                    "eta_seconds": 0,
                    "eta_text": "",
                },
                "summary": {**summary, "project": upload_project},
            }
        except Exception as exc:
            result = {
                "state": "error",
                "message": f"缓存上传失败：{exc}",
                "scope": self._normalize_upload_scope(str(upload_job.get("upload_scope") or "full")),
                "started_at": "",
                "finished_at": finished_at,
                "last_success_at": self._upload_status.get("last_success_at", ""),
                "last_error": str(exc),
                "pending": False,
                "progress": dict(self._upload_status.get("progress") or {}),
                "summary": {},
            }

        next_job: Dict[str, Any] = {}
        with self._lock:
            self._upload_status = result
            next_job = dict(self._pending_upload_job)
            self._pending_upload_job = {}
            if next_job:
                self._start_upload_job_locked(next_job)
            else:
                self._upload_running = False

    def bulk_update_account_state(self, *, urls: List[str], active: bool) -> Dict[str, Any]:
        normalized_urls: List[str] = []
        seen: set[str] = set()
        for url in urls:
            normalized = normalize_profile_url(url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_urls.append(normalized)
        if not normalized_urls:
            return {"ok": False, "message": "没有可更新的账号主页链接。"}

        with self._lock:
            entries = parse_monitored_entries(self.urls_file)
            index = {entry["url"]: entry for entry in entries}
            matched = 0
            changed = 0
            for normalized in normalized_urls:
                target = index.get(normalized)
                if target is None:
                    continue
                matched += 1
                if bool(target.get("active", True)) == active:
                    continue
                target["active"] = active
                changed += 1

            if matched == 0:
                return {"ok": False, "message": "筛选结果里没有可匹配的监测账号。"}
            if changed == 0:
                state_text = "监测中" if active else "已暂停"
                return {
                    "ok": True,
                    "message": f"筛选结果中的 {matched} 个账号当前都已是{state_text}。",
                    "changed_count": 0,
                    "matched_count": matched,
                    "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }

            path = write_monitored_entries(self.urls_file, entries)
            active_count = sum(1 for entry in entries if entry.get("active"))
            started = False
            if active_count > 0:
                action_text = "恢复" if active else "暂停"
                started = self._request_sync_locked(reason=f"{action_text} {changed} 个账号，开始同步")
            return {
                "ok": True,
                "message": f"已{'恢复' if active else '暂停'} {changed} 个账号",
                "urls_file": str(path),
                "changed_count": changed,
                "matched_count": matched,
                "total": len(entries),
                "active_count": active_count,
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def add_accounts(self, *, raw_text: str = "", urls: Optional[List[str]] = None, project: str = DEFAULT_PROJECT_NAME) -> Dict[str, Any]:
        with self._lock:
            existing_entries = parse_monitored_entries(self.urls_file)
            merged_entries, added_urls, reactivated_urls = merge_monitored_entries(
                existing_entries,
                raw_text=raw_text,
                urls=urls,
                project=project,
            )
            if not added_urls and not reactivated_urls:
                return {
                    "ok": False,
                    "message": "没有识别到新的账号主页链接，或这些账号已在监测清单里。",
                    "added_urls": [],
                    "reactivated_urls": [],
                    "total": len(merged_entries),
                "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }
            path = write_monitored_entries(self.urls_file, merged_entries)
            summary_parts = []
            if added_urls:
                summary_parts.append(f"新增 {len(added_urls)} 个账号")
            if reactivated_urls:
                summary_parts.append(f"恢复 {len(reactivated_urls)} 个账号")
            warmup_urls = list(dict.fromkeys([*added_urls, *reactivated_urls]))
            if warmup_urls:
                update_monitored_metadata(
                    self.urls_file,
                    [
                        {
                            "url": url,
                            "account_id": extract_profile_user_id(url),
                            "fetch_state": "checking",
                            "fetch_message": "正在识别账号信息",
                            "fetch_checked_at": iso_now(),
                        }
                        for url in warmup_urls
                    ],
                )
                threading.Thread(target=self._warm_monitored_metadata, args=(warmup_urls,), daemon=True).start()
            started = self._request_sync_locked(reason="，".join(summary_parts) + "，开始同步")
            return {
                "ok": True,
                "message": "，".join(summary_parts) + "到监测清单",
                "urls_file": str(path),
                "added_urls": added_urls,
                "reactivated_urls": reactivated_urls,
                "total": len(merged_entries),
                "active_count": sum(1 for entry in merged_entries if entry.get("active")),
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def _warm_monitored_metadata(self, urls: List[str]) -> None:
        try:
            settings = load_settings(self.env_file)
        except Exception:
            return
        warmed_items: List[Dict[str, Any]] = []
        for url in urls:
            try:
                payload = load_profile_report_payload(settings=settings, profile_url=url)
                report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
                profile = report.get("profile") or {}
                warmed_items.append(
                    {
                        "url": url,
                        "profile_url": pick_profile_url(
                            url,
                            profile.get("profile_url"),
                            payload.get("final_url"),
                        ),
                        "account": str(profile.get("nickname") or ""),
                        "account_id": str(profile.get("profile_user_id") or extract_profile_user_id(url)),
                        "fans_text": str(profile.get("fans_count_text") or ""),
                        "interaction_text": str(profile.get("interaction_count_text") or ""),
                        "works_text": str(
                            profile.get("work_count_display_text")
                            or profile.get("total_work_count")
                            or profile.get("visible_work_count")
                            or len(report.get("works") or [])
                        ),
                        "fetch_state": "ok",
                        "fetch_message": "已获取账号快照",
                        "fetch_checked_at": iso_now(),
                    }
                )
            except Exception as exc:
                fetch_state, fetch_message = classify_monitored_fetch_state(error_text=str(exc), has_snapshot=False)
                warmed_items.append(
                    {
                        "url": url,
                        "account_id": extract_profile_user_id(url),
                        "fetch_state": fetch_state,
                        "fetch_message": fetch_message,
                        "fetch_checked_at": iso_now(),
                    }
                )
        if warmed_items:
            update_monitored_metadata(self.urls_file, warmed_items)

    def assign_project(self, *, urls: List[str], project: str) -> Dict[str, Any]:
        normalized_project = normalize_project_name(project)
        normalized_urls: List[str] = []
        seen: set[str] = set()
        for url in urls:
            normalized = normalize_profile_url(url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_urls.append(normalized)
        if not normalized_urls:
            return {"ok": False, "message": "没有可移动项目的账号主页链接。"}

        with self._lock:
            entries = parse_monitored_entries(self.urls_file)
            index = {entry["url"]: entry for entry in entries}
            matched = 0
            changed = 0
            for normalized in normalized_urls:
                target = index.get(normalized)
                if target is None:
                    continue
                matched += 1
                if normalize_project_name(target.get("project")) == normalized_project:
                    continue
                target["project"] = normalized_project
                changed += 1

            if matched == 0:
                return {"ok": False, "message": "筛选结果里没有可匹配的监测账号。"}
            if changed == 0:
                return {
                    "ok": True,
                    "message": f"筛选结果中的 {matched} 个账号当前都已在项目「{normalized_project}」。",
                    "changed_count": 0,
                    "matched_count": matched,
                    "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }

            path = write_monitored_entries(self.urls_file, entries)
            started = self._request_sync_locked(reason=f"调整 {changed} 个账号的项目归属，开始同步")
            return {
                "ok": True,
                "message": f"已将 {changed} 个账号移动到项目「{normalized_project}」",
                "urls_file": str(path),
                "changed_count": changed,
                "matched_count": matched,
                "total": len(entries),
                "active_count": sum(1 for entry in entries if entry.get('active')),
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def update_account_state(self, *, url: str, active: bool) -> Dict[str, Any]:
        with self._lock:
            normalized = normalize_profile_url(url)
            if not normalized:
                return {"ok": False, "message": "账号主页链接不能为空。"}
            entries = parse_monitored_entries(self.urls_file)
            target = next((entry for entry in entries if entry["url"] == normalized), None)
            if target is None:
                return {"ok": False, "message": "监测清单里没有找到这个账号。"}
            if bool(target.get("active")) == active:
                state_text = "启用中" if active else "已暂停"
                return {"ok": True, "message": f"该账号当前已是{state_text}。", "sync_started": False}
            target["active"] = active
            path = write_monitored_entries(self.urls_file, entries)
            active_count = sum(1 for entry in entries if entry.get("active"))
            started = False
            if active_count > 0:
                action_text = "恢复" if active else "暂停"
                started = self._request_sync_locked(reason=f"{action_text} 1 个账号，开始同步")
            return {
                "ok": True,
                "message": "已恢复账号监测" if active else "已暂停账号监测",
                "urls_file": str(path),
                "total": len(entries),
                "active_count": active_count,
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def remove_account(self, *, url: str) -> Dict[str, Any]:
        with self._lock:
            normalized = normalize_profile_url(url)
            if not normalized:
                return {"ok": False, "message": "账号主页链接不能为空。"}
            entries = parse_monitored_entries(self.urls_file)
            filtered = [entry for entry in entries if entry["url"] != normalized]
            if len(filtered) == len(entries):
                return {"ok": False, "message": "监测清单里没有找到这个账号。"}
            path = write_monitored_entries(self.urls_file, filtered)
            active_count = sum(1 for entry in filtered if entry.get("active"))
            started = False
            if active_count > 0:
                started = self._request_sync_locked(reason="删除 1 个账号，开始同步")
            return {
                "ok": True,
                "message": "已从监测清单删除账号",
                "urls_file": str(path),
                "removed_url": normalized,
                "total": len(filtered),
                "active_count": active_count,
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def retry_account(self, *, url: str) -> Dict[str, Any]:
        with self._lock:
            normalized = normalize_profile_url(url)
            if not normalized:
                return {"ok": False, "message": "账号主页链接不能为空。"}
            entries = parse_monitored_entries(self.urls_file)
            target = next((entry for entry in entries if entry["url"] == normalized), None)
            if target is None:
                return {"ok": False, "message": "监测清单里没有找到这个账号。"}
            update_monitored_metadata(
                self.urls_file,
                [
                    {
                        "url": normalized,
                        "account_id": extract_profile_user_id(normalized),
                        "fetch_state": "checking",
                        "fetch_message": "正在重试抓取账号信息",
                        "fetch_checked_at": iso_now(),
                    }
                ],
            )
            started = self._request_sync_locked(reason="重试 1 个账号", urls=[normalized])
            return {
                "ok": True,
                "message": "已开始重试该账号" if started else "当前任务完成后会自动重试该账号",
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def request_sync(self, *, project: str = "") -> Dict[str, Any]:
        with self._lock:
            entries = parse_monitored_entries(self.urls_file)
            normalized_project = normalize_project_name(project) if str(project or "").strip() else ""
            urls = [
                entry["url"]
                for entry in entries
                if entry.get("active") and (not normalized_project or normalize_project_name(entry.get("project")) == normalized_project)
            ]
            if not urls:
                return {
                    "ok": False,
                    "message": (
                        f"项目「{normalized_project}」当前没有可同步账号。"
                        if normalized_project
                        else "当前监测清单为空，先添加账号主页链接。"
                    ),
                    "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }
            if self._running:
                return {
                    "ok": False,
                    "message": "当前已有更新任务在跑，先等这一轮完成。",
                    "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }
            cooldown = self._build_manual_cooldown_locked()
            if cooldown["manual_sync_locked"]:
                return {
                    "ok": False,
                    "message": f"手动更新过于频繁，请在 {cooldown['manual_cooldown_text']} 后再试。",
                    "sync_started": False,
                    "sync_status": self._status_snapshot_locked(),
                }
            reason = (
                f"立即同步项目「{normalized_project}」的 {len(urls)} 个账号"
                if normalized_project
                else f"立即同步 {len(urls)} 个账号"
            )
            started = self._request_sync_locked(reason=reason, urls=urls, project=normalized_project, mode="manual")
            if started:
                self._manual_last_requested_at = time.time()
            return {
                "ok": True,
                "message": (
                    f"已开始同步项目「{normalized_project}」"
                    if normalized_project and started
                    else "已开始同步当前监测清单"
                    if started
                    else "当前已有同步任务在跑，先等这一轮完成。"
                ),
                "sync_started": started,
                "sync_status": self._status_snapshot_locked(),
            }

    def _request_sync_locked(self, *, reason: str, urls: Optional[List[str]] = None, project: str = "", mode: str = "manual") -> bool:
        if self._running:
            self._pending_resync = True
            self._pending_sync_urls = list(urls or [])
            self._status["pending"] = True
            self._status["message"] = f"{reason}，当前任务完成后自动重跑"
            return False
        self._running = True
        self._pending_resync = False
        self._current_sync_urls = list(urls or [])
        self._current_sync_project = str(project or "").strip()
        self._current_sync_mode = str(mode or "manual").strip() or "manual"
        self._pending_sync_urls = []
        started_at = iso_now()
        if self._current_sync_project:
            update_project_sync_status(
                urls_file=self.urls_file,
                project=self._current_sync_project,
                state="running",
                message=reason,
                started_at=started_at,
            )
        self._status = {
            "state": "running",
            "message": reason,
            "started_at": started_at,
            "finished_at": "",
            "last_success_at": self._status.get("last_success_at", ""),
            "last_error": "",
            "pending": False,
            "progress": build_sync_progress(
                phase="collect",
                current=0,
                total=max(1, len(self._current_sync_urls) or 1),
                success_count=0,
                failed_count=0,
                started_at=started_at,
            ),
            "summary": {},
        }
        threading.Thread(target=self._sync_loop, daemon=True).start()
        return True

    def _set_running_progress(self, progress: Dict[str, Any], *, message: str = "") -> None:
        with self._lock:
            if self._status.get("state") != "running":
                return
            self._status["progress"] = dict(progress or {})
            if message:
                self._status["message"] = message

    def _publish_login_state(self, payload: Dict[str, Any], *, running: bool, sample_url: str) -> None:
        if not self.login_state_store:
            return
        self.login_state_store.set_payload(payload, running=running, sample_url=sample_url)

    def _ensure_login_ready_for_sync(self, *, settings, sample_url: str, mode: str = "manual") -> None:
        normalized_sample_url = normalize_profile_url(sample_url)
        if not normalized_sample_url:
            return

        def on_wait(payload: Dict[str, Any]) -> None:
            waiting_message = (
                "检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。"
                if payload.get("login_window_opened")
                else "检测到小红书未登录，但未能自动打开网页登录，请先手动登录后重试。"
            )
            waiting_payload = dict(payload)
            waiting_payload["message"] = waiting_message
            self._publish_login_state(waiting_payload, running=True, sample_url=normalized_sample_url)
            if str(mode or "").strip() == "auto":
                with self._lock:
                    self._server_push_status.update(
                        {
                            "state": "waiting_login",
                            "message": waiting_message,
                            "mode": "auto",
                            "last_error": "",
                        }
                    )
            progress = build_sync_progress(
                phase="login",
                current=0,
                total=1,
                status=waiting_message,
                success_count=0,
                failed_count=0,
                started_at=str(self._status.get("started_at") or ""),
            )
            self._set_running_progress(progress, message=progress.get("detail_text") or waiting_message)

        payload = wait_for_xiaohongshu_login(
            env_file=self.env_file,
            settings=settings,
            sample_url=normalized_sample_url,
            on_wait=on_wait,
            timeout_seconds=0 if str(mode or "").strip() == "auto" else LOGIN_WAIT_TIMEOUT_SECONDS,
        )
        self._publish_login_state(payload, running=False, sample_url=normalized_sample_url)
        if login_state_requires_interactive_login(payload):
            if payload.get("login_window_opened"):
                raise RuntimeError(
                    f"检测到小红书未登录，已弹出网页登录窗口；等待 {format_duration_text(LOGIN_WAIT_TIMEOUT_SECONDS)} 仍未完成登录，请登录后再试。"
                )
            raise RuntimeError("检测到小红书登录态异常，且未能自动打开网页登录，请先手动登录后再试。")

    def _sync_loop(self) -> None:
        while True:
            finished_at = iso_now()
            should_auto_push = False
            try:
                settings = load_settings(self.env_file)
                current_urls = list(self._current_sync_urls)
                self._ensure_login_ready_for_sync(
                    settings=settings,
                    sample_url=current_urls[0] if current_urls else "",
                    mode=self._current_sync_mode,
                )
                collect_progress = build_sync_progress(
                    phase="collect",
                    current=0,
                    total=max(1, len(current_urls) or 1),
                    success_count=0,
                    failed_count=0,
                    started_at=str(self._status.get("started_at") or ""),
                )
                self._set_running_progress(collect_progress, message="登录完成，开始抓取账号数据")
                reports = load_reports_for_sync(
                    settings=settings,
                    explicit_urls=current_urls,
                    raw_text="",
                    urls_file=None if current_urls else self.urls_file,
                    project=self._current_sync_project,
                    report_json=None,
                    progress_callback=self._handle_progress_update,
                )
                update_monitored_metadata(
                    self.urls_file,
                    [
                        {
                            "url": report.get("source_url") or (report.get("profile") or {}).get("profile_url") or "",
                            "profile_url": (report.get("profile") or {}).get("profile_url") or "",
                            "account": (report.get("profile") or {}).get("nickname") or "",
                            "account_id": (report.get("profile") or {}).get("profile_user_id") or "",
                            "fans_text": (report.get("profile") or {}).get("fans_count_text") or "",
                            "interaction_text": (report.get("profile") or {}).get("interaction_count_text") or "",
                            "works_text": (
                                (report.get("profile") or {}).get("work_count_display_text")
                                or (report.get("profile") or {}).get("total_work_count")
                                or (report.get("profile") or {}).get("visible_work_count")
                                or len(report.get("works") or [])
                            ),
                            "fetch_state": "ok",
                            "fetch_message": "已获取账号快照",
                            "fetch_checked_at": iso_now(),
                        }
                        for report in reports
                    ],
                )
                try:
                    write_project_cache_bundle(reports=reports, settings=settings)
                    cached_payload = load_cached_dashboard_payload(settings)
                    self.dashboard_store.set_local_override(cached_payload)
                except Exception:
                    pass
                if not self.dashboard_store.commit_local_override():
                    self.dashboard_store.invalidate()
                local_summary = {
                    "total_accounts": len(reports),
                    "successful_accounts": len(reports),
                    "failed_accounts": 0,
                    "total_works": sum(len(report.get("works") or []) for report in reports),
                }
                finished_progress = build_progress_timing(
                    started_at=self._status.get("started_at", ""),
                    overall_percent=100,
                )
                result = {
                    "state": "success",
                    "message": f"本地缓存已更新，账号 {local_summary.get('total_accounts', 0)} 个，作品 {local_summary.get('total_works', 0)} 条",
                    "started_at": "",
                    "finished_at": finished_at,
                    "last_success_at": finished_at,
                    "last_error": "",
                    "pending": False,
                    "progress": {
                        "phase": "done",
                        "phase_label": "同步完成",
                        "current": local_summary.get("total_accounts", 0),
                        "total": local_summary.get("total_accounts", 0),
                        "phase_percent": 100,
                        "overall_percent": 100,
                        "account": "",
                        "works": local_summary.get("total_works", 0),
                        "status": "success",
                        "success_count": local_summary.get("successful_accounts", 0),
                        "failed_count": 0,
                        "detail_text": f"已完成 {local_summary.get('total_accounts', 0)} 个账号看板更新",
                        "elapsed_seconds": finished_progress["elapsed_seconds"],
                        "elapsed_text": finished_progress["elapsed_text"],
                        "eta_seconds": 0,
                        "eta_text": "",
                    },
                    "summary": dict(local_summary),
                }
            except Exception as exc:
                result = {
                    "state": "error",
                    "message": str(exc),
                    "started_at": "",
                    "finished_at": finished_at,
                    "last_success_at": self._status.get("last_success_at", ""),
                    "last_error": str(exc),
                    "pending": False,
                    "progress": dict(self._status.get("progress") or {}),
                    "summary": {},
                }

            with self._lock:
                if self._pending_resync:
                    self._pending_resync = False
                    self._current_sync_urls = list(self._pending_sync_urls)
                    self._pending_sync_urls = []
                    started_at = iso_now()
                    if self._current_sync_project:
                        update_project_sync_status(
                            urls_file=self.urls_file,
                            project=self._current_sync_project,
                            state="running",
                            message="检测到新的监测账号，继续同步最新清单",
                            started_at=started_at,
                        )
                    self._status = {
                        "state": "running",
                        "message": "检测到新的监测账号，继续同步最新清单",
                        "started_at": started_at,
                        "finished_at": result.get("finished_at", ""),
                        "last_success_at": result.get("last_success_at", self._status.get("last_success_at", "")),
                        "last_error": result.get("last_error", ""),
                        "pending": False,
                        "progress": build_sync_progress(
                            phase="collect",
                            current=0,
                            total=max(1, len(self._current_sync_urls) or 1),
                            success_count=0,
                            failed_count=0,
                            started_at=started_at,
                        ),
                        "summary": result.get("summary", {}),
                    }
                    continue
                current_project = self._current_sync_project
                current_mode = self._current_sync_mode
                self._running = False
                self._current_sync_urls = []
                self._current_sync_project = ""
                self._current_sync_mode = ""
                self._pending_sync_urls = []
                self._status = result
                if current_mode == "auto" and current_project:
                    today_text = datetime.now().astimezone().date().isoformat()
                    if result.get("state") == "success":
                        self._auto_project_success_dates[current_project] = today_text
                        active_projects = sorted(self._build_project_url_map(parse_monitored_entries(self.urls_file)))
                        all_collected = bool(active_projects) and all(
                            self._auto_project_success_dates.get(project_name, "") == today_text
                            for project_name in active_projects
                        )
                        if all_collected and self._last_auto_server_push_success_date != today_text:
                            should_auto_push = True
                    self._update_server_push_schedule_locked()
                if current_project:
                    if result.get("state") == "success":
                        summary = result.get("summary") or {}
                        update_project_sync_status(
                            urls_file=self.urls_file,
                            project=current_project,
                            state="success",
                            message=result.get("message", ""),
                            finished_at=result.get("finished_at", ""),
                            total_accounts=int(summary.get("total_accounts") or 0),
                            total_works=int(summary.get("total_works") or 0),
                        )
                    else:
                        update_project_sync_status(
                            urls_file=self.urls_file,
                            project=current_project,
                            state="error",
                            message=result.get("message", ""),
                            finished_at=result.get("finished_at", ""),
                            last_error=result.get("last_error", ""),
                        )
            if should_auto_push:
                try:
                    self.push_server_cache(auto=True)
                except Exception:
                    pass
            break

    def _handle_progress_update(self, payload: Dict[str, Any]) -> None:
        phase = str(payload.get("phase") or "").strip()
        if phase not in {"collect", "sync"}:
            return
        if phase == "collect":
            status = str(payload.get("status") or "").strip()
            if status == "success":
                update_monitored_metadata(
                    self.urls_file,
                    [
                        {
                            "url": payload.get("url") or "",
                            "profile_url": payload.get("profile_url") or payload.get("url") or "",
                            "account": payload.get("account") or "",
                            "account_id": payload.get("account_id") or "",
                            "fans_text": payload.get("fans_text") or "",
                            "interaction_text": payload.get("interaction_text") or "",
                            "works_text": payload.get("works_text") or payload.get("works") or "",
                            "fetch_state": "ok",
                            "fetch_message": "已获取账号快照",
                            "fetch_checked_at": iso_now(),
                        }
                    ],
                )
            elif status == "failed":
                fetch_state, fetch_message = classify_monitored_fetch_state(
                    error_text=payload.get("error") or "",
                    has_snapshot=False,
                )
                update_monitored_metadata(
                    self.urls_file,
                    [
                        {
                            "url": payload.get("url") or "",
                            "account_id": payload.get("account_id") or extract_profile_user_id(str(payload.get("url") or "")),
                            "fetch_state": fetch_state,
                            "fetch_message": fetch_message,
                            "fetch_checked_at": iso_now(),
                        }
                    ],
                )
        progress = build_sync_progress(
            phase=phase,
            current=int(payload.get("current") or 0),
            total=int(payload.get("total") or 0),
            account=str(payload.get("account") or payload.get("url") or ""),
            works=int(payload.get("works") or 0),
            status=str(payload.get("status") or ""),
            success_count=int(payload.get("success_count") or 0),
            failed_count=int(payload.get("failed_count") or 0),
            started_at=str(self._status.get("started_at") or ""),
        )
        with self._lock:
            if self._status.get("state") != "running":
                return
            self._status["progress"] = progress
            self._status["message"] = progress.get("detail_text") or self._status.get("message", "")


def _payload_account_ids(payload: Dict[str, Any]) -> set[str]:
    return {
        str((item or {}).get("account_id") or "").strip()
        for item in (payload.get("accounts") or [])
        if str((item or {}).get("account_id") or "").strip()
    }


def _normalize_dashboard_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = copy.deepcopy(payload or {})
    accounts = []
    for item in normalized.get("accounts") or []:
        row = dict(item or {})
        account_name = str(row.get("account_name") or row.get("account") or row.get("display_name") or "").strip()
        account_id = str(row.get("account_id") or "").strip()
        row["account"] = account_name or account_id
        row["account_name"] = account_name or account_id
        row["display_name"] = str(row.get("display_name") or account_name or account_id).strip() or account_id
        accounts.append(row)
    normalized["accounts"] = accounts
    return normalized


def _load_dashboard_payload_local_only(env_file: str) -> Dict[str, Any]:
    settings = load_settings(env_file)
    cached_payload = load_cached_dashboard_payload(settings)
    metadata_path = resolve_text_path(DEFAULT_URLS_FILE + ".meta.json")
    monitored_metadata: Dict[str, Any] = {}
    if metadata_path.exists():
        try:
            monitored_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            monitored_metadata = {}
    expected_account_ids = {
        str((item or {}).get("account_id") or "").strip()
        for item in monitored_metadata.values()
        if str((item or {}).get("account_id") or "").strip()
    }
    expected_accounts = max(1, len(expected_account_ids) or (len(monitored_metadata) // 2))
    cached_account_ids = _payload_account_ids(cached_payload) if cached_payload else set()
    if cached_payload and len(cached_payload.get("accounts") or []) >= expected_accounts and cached_account_ids.issuperset(expected_account_ids):
        return _normalize_dashboard_payload(cached_payload)
    try:
        rebuilt_payload = rebuild_dashboard_cache_from_project_dirs(settings)
        rebuilt_account_ids = _payload_account_ids(rebuilt_payload)
        if rebuilt_payload and (
            not expected_account_ids
            or rebuilt_account_ids.issuperset(expected_account_ids)
            or len(rebuilt_payload.get("accounts") or []) >= expected_accounts
        ):
            return _normalize_dashboard_payload(rebuilt_payload)
    except Exception:
        pass
    try:
        repaired_payload = repair_dashboard_cache_from_exports(
            settings=settings,
            monitored_metadata=monitored_metadata,
        )
        repaired_account_ids = _payload_account_ids(repaired_payload)
        if repaired_payload and (
            not expected_account_ids
            or repaired_account_ids.issuperset(expected_account_ids)
            or len(repaired_payload.get("accounts") or []) >= expected_accounts
        ):
            return _normalize_dashboard_payload(repaired_payload)
    except Exception:
        pass
    if cached_payload:
        return _normalize_dashboard_payload(cached_payload)
    return build_empty_dashboard_payload(load_error="本地暂无可用缓存")


def save_uploaded_server_cache(*, env_file: str, urls_file: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    dashboard_payload = dict(payload.get("dashboard_payload") or {})
    monitored_entries = payload.get("monitored_entries") or []
    monitored_metadata = payload.get("monitored_metadata") or {}
    merge_mode = str(payload.get("merge_mode") or "replace").strip().lower()
    partial_account_ids = {
        str(item or "").strip()
        for item in (payload.get("account_ids") or [])
        if str(item or "").strip()
    }
    if not isinstance(dashboard_payload, dict) or not dashboard_payload:
        raise ValueError("dashboard_payload 不能为空")
    if monitored_entries and not isinstance(monitored_entries, list):
        raise ValueError("monitored_entries 必须是数组")
    if monitored_metadata and not isinstance(monitored_metadata, dict):
        raise ValueError("monitored_metadata 必须是对象")

    settings = load_settings(env_file)
    cache_dir = resolve_project_cache_dir(settings)
    cache_dir.mkdir(parents=True, exist_ok=True)
    dashboard_payload = _merge_uploaded_dashboard_payload(
        settings=settings,
        incoming_payload=dashboard_payload,
        account_ids=partial_account_ids,
        merge_mode=merge_mode,
    )
    dashboard_payload["server_received_at"] = iso_now()
    dashboard_path = cache_dir / "dashboard_all.json"
    dashboard_path.write_text(json.dumps(dashboard_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if monitored_entries:
        if merge_mode == "partial" and partial_account_ids:
            existing_entries = parse_monitored_entries(urls_file)
            merged_entries = {
                str(item.get("url") or "").strip(): dict(item)
                for item in existing_entries
                if str(item.get("url") or "").strip()
            }
            for item in monitored_entries:
                normalized_url = normalize_profile_url(str((item or {}).get("url") or ""))
                if not normalized_url:
                    continue
                merged_entries[normalized_url] = {
                    "url": normalized_url,
                    "active": bool((item or {}).get("active", True)),
                    "project": normalize_project_name((item or {}).get("project")),
                }
            write_monitored_entries(urls_file, list(merged_entries.values()))
        else:
            write_monitored_entries(urls_file, monitored_entries)
    if monitored_metadata:
        if merge_mode == "partial" and partial_account_ids:
            update_monitored_metadata(
                urls_file,
                [
                    {
                        "url": url,
                        **dict(meta or {}),
                    }
                    for url, meta in monitored_metadata.items()
                    if isinstance(meta, dict)
                ],
            )
        else:
            write_monitored_metadata(urls_file, monitored_metadata)

    return {
        "ok": True,
        "dashboard_path": str(dashboard_path),
        "urls_file": str(resolve_text_path(urls_file)),
        "metadata_path": str(resolve_metadata_cache_path(urls_file)),
        "account_count": len(dashboard_payload.get("accounts") or []),
        "project_count": len({str(item.get("project") or "").strip() for item in monitored_entries if isinstance(item, dict)}),
        "updated_at": iso_now(),
    }


def push_current_cache_to_server(*, env_file: str, urls_file: str, account_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    settings = load_settings(env_file)
    server_url = str(getattr(settings, "server_cache_push_url", "") or "").strip()
    if not server_url:
        raise ValueError("缺少 SERVER_CACHE_PUSH_URL，请先在系统配置里填写服务器地址")
    token = str(getattr(settings, "server_cache_upload_token", "") or "").strip()
    result = push_local_cache_to_server(
        env_file=env_file,
        urls_file=urls_file,
        server_url=server_url,
        token=token,
        account_ids=account_ids,
    )
    return {"ok": True, **result}


def load_dashboard_payload(env_file: str) -> Dict[str, Any]:
    return _normalize_dashboard_payload(_load_dashboard_payload_local_only(env_file))


def load_profile_table_rows(env_file: str) -> List[Dict[str, Any]]:
    return []


def list_table_ids(client: FeishuBitableClient) -> Dict[str, str]:
    table_ids: Dict[str, str] = {}
    for table in client.list_tables():
        name = str(table.get("name") or "").strip()
        table_id = str(table.get("table_id") or "").strip()
        if name and table_id:
            table_ids[name] = table_id
    return table_ids


def fetch_table_rows(
    settings,
    table_ids: Dict[str, str],
    table_name: str,
    *,
    required: bool = True,
) -> List[Dict[str, Any]]:
    table_id = table_ids.get(table_name, "")
    if not table_id:
        if required:
            raise ValueError(f"缺少飞书数据表: {table_name}")
        return []
    client = FeishuBitableClient(replace(settings, feishu_table_id=table_id))
    return [record.get("fields") or {} for record in client.list_records(page_size=500)]


def _merge_history_rankings(
    *,
    existing_history: Dict[str, Any],
    incoming_history: Dict[str, Any],
    account_ids: set[str],
) -> Dict[str, Any]:
    if not account_ids:
        return copy.deepcopy(incoming_history or existing_history or {})
    merged = copy.deepcopy(existing_history or {})
    for project_name, raw_project_history in (incoming_history or {}).items():
        if not isinstance(raw_project_history, dict):
            continue
        target_project_history = copy.deepcopy(merged.get(project_name) or {})
        for date_text, snapshot in raw_project_history.items():
            if not isinstance(snapshot, dict):
                continue
            existing_snapshot = copy.deepcopy(target_project_history.get(date_text) or {})
            merged_snapshot = copy.deepcopy(existing_snapshot)
            for rank_key in ("likes", "comments", "growth"):
                incoming_rows = [dict(item) for item in (snapshot.get(rank_key) or [])]
                existing_rows = [dict(item) for item in (existing_snapshot.get(rank_key) or [])]
                preserved = [
                    item for item in existing_rows
                    if str(item.get("account_id") or "").strip() not in account_ids
                ]
                merged_snapshot[rank_key] = preserved + incoming_rows
            for key, value in snapshot.items():
                if key not in {"likes", "comments", "growth"}:
                    merged_snapshot[key] = copy.deepcopy(value)
            target_project_history[date_text] = merged_snapshot
        merged[project_name] = target_project_history
    return merged


def _merge_uploaded_dashboard_payload(*, settings, incoming_payload: Dict[str, Any], account_ids: set[str], merge_mode: str) -> Dict[str, Any]:
    normalized_mode = str(merge_mode or "replace").strip().lower()
    if normalized_mode != "partial" or not account_ids:
        return dict(incoming_payload)
    existing_payload = load_cached_dashboard_payload(settings)
    if not existing_payload:
        return dict(incoming_payload)

    existing_accounts = {
        str(item.get("account_id") or "").strip(): dict(item)
        for item in (existing_payload.get("accounts") or [])
        if str(item.get("account_id") or "").strip()
    }
    for item in (incoming_payload.get("accounts") or []):
        account_id = str(item.get("account_id") or "").strip()
        if not account_id:
            continue
        existing_accounts[account_id] = dict(item)
    merged_account_series = {
        str(account_id or "").strip(): [dict(point) for point in points]
        for account_id, points in (existing_payload.get("account_series") or {}).items()
        if str(account_id or "").strip()
    }
    for account_id, points in (incoming_payload.get("account_series") or {}).items():
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            continue
        merged_account_series[normalized_account_id] = [dict(point) for point in points]
    merged_rankings = copy.deepcopy(existing_payload.get("rankings") or {})
    for rank_type, rows in (incoming_payload.get("rankings") or {}).items():
        existing_rows = [dict(item) for item in (merged_rankings.get(rank_type) or [])]
        preserved_rows = [
            item for item in existing_rows
            if str(item.get("account_id") or "").strip() not in account_ids
        ]
        incoming_rows = [dict(item) for item in (rows or [])]
        combined_rows = preserved_rows + incoming_rows
        combined_rows.sort(key=lambda item: ranking_sort_key(rank_type, item), reverse=True)
        merged_rankings[rank_type] = combined_rows
    existing_alerts = [dict(item) for item in (existing_payload.get("alerts") or [])]
    preserved_alerts = [
        item for item in existing_alerts
        if str(item.get("account_id") or "").strip() not in account_ids
    ]
    incoming_alerts = [dict(item) for item in (incoming_payload.get("alerts") or [])]
    merged_alerts = preserved_alerts + incoming_alerts
    merged_payload = {
        **copy.deepcopy(existing_payload),
        **copy.deepcopy(incoming_payload),
        "accounts": list(existing_accounts.values()),
        "account_series": merged_account_series,
        "rankings": merged_rankings,
        "alerts": merged_alerts,
    }
    merged_payload["history_rankings"] = _merge_history_rankings(
        existing_history=existing_payload.get("history_rankings") or {},
        incoming_history=incoming_payload.get("history_rankings") or {},
        account_ids=account_ids,
    )
    merged_payload["portal"] = build_portal_from_accounts_and_rankings(
        accounts=merged_payload["accounts"],
        rankings=merged_rankings,
        base_portal=merged_payload.get("portal") or {},
        updated_at=str(merged_payload.get("updated_at") or incoming_payload.get("updated_at") or existing_payload.get("updated_at") or ""),
    )
    merged_payload["series"] = rebuild_daily_series_from_account_series(merged_account_series)
    merged_payload["generated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    merged_payload["latest_date"] = str(
        incoming_payload.get("latest_date")
        or existing_payload.get("latest_date")
        or ""
    ).strip()
    merged_payload["updated_at"] = str(
        incoming_payload.get("updated_at")
        or incoming_payload.get("generated_at")
        or existing_payload.get("updated_at")
        or existing_payload.get("generated_at")
        or ""
    ).strip()
    return merged_payload


def build_handler(
    *,
    dashboard_store: DashboardStore,
    monitoring_store: MonitoringSyncStore,
    login_state_store: LoginStateStore,
):
    class LocalStatsHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(WEB_DIR), **kwargs)

        def send_json_response(self, status_code: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def read_json_body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            content_encoding = str(self.headers.get("Content-Encoding") or "").lower()
            if "gzip" in content_encoding:
                try:
                    raw = gzip.decompress(raw)
                except Exception as exc:
                    raise ValueError(f"请求体解压失败: {exc}") from exc
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("请求体必须是 JSON 对象")
            return payload

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/api/dashboard":
                force = "refresh=1" in self.path
                payload = dashboard_store.get_payload(force=force)
                self.send_json_response(HTTPStatus.OK, payload)
                return
            if path == "/api/mobile-rankings":
                parsed = urllib.parse.urlparse(self.path)
                params = urllib.parse.parse_qs(parsed.query)
                project = str((params.get("project") or [""])[0]).strip()
                payload = build_mobile_rankings_payload(
                    dashboard_payload=_load_dashboard_payload_local_only(monitoring_store.env_file),
                    monitored_entries=monitoring_store.get_payload().get("entries") or [],
                    project=project,
                )
                self.send_json_response(HTTPStatus.OK, payload)
                return
            if path == "/api/system-config":
                self.send_json_response(HTTPStatus.OK, load_system_config(monitoring_store.env_file, monitoring_store.urls_file))
                return
            if path == "/api/health":
                self.send_json_response(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "xhs_local_stats_app",
                        "version": APP_VERSION,
                        "pid": os.getpid(),
                        "time": iso_now(),
                    },
                )
                return
            if path == "/api/image":
                self.serve_remote_image()
                return
            if path == "/api/monitored-accounts":
                self.send_json_response(HTTPStatus.OK, monitoring_store.get_payload())
                return
            if path == "/api/login-state":
                force = "refresh=1" in self.path
                monitoring_payload = monitoring_store.get_payload()
                entries = monitoring_payload.get("entries") or []
                sample_url = str((next((item for item in entries if item.get("active")), {}) or {}).get("url") or "")
                payload = login_state_store.get_payload(force=force, sample_url=sample_url)
                self.send_json_response(HTTPStatus.OK, payload)
                return
            return super().do_GET()

        def serve_remote_image(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            target_url = str((params.get("url") or [""])[0]).strip()
            if not target_url.startswith(("http://", "https://")):
                self.send_json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "message": "无效图片地址"})
                return
            request = urllib.request.Request(
                target_url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://www.xiaohongshu.com/",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    body = response.read()
                    content_type = response.getheader("Content-Type") or "image/jpeg"
            except Exception:
                self.send_json_response(HTTPStatus.BAD_GATEWAY, {"ok": False, "message": "图片加载失败"})
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            try:
                if path == "/api/monitored-accounts":
                    payload = self.read_json_body()
                    result = monitoring_store.add_accounts(
                        raw_text=str(payload.get("raw_text") or ""),
                        urls=[str(item) for item in (payload.get("urls") or []) if str(item).strip()],
                        project=str(payload.get("project") or ""),
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/toggle":
                    payload = self.read_json_body()
                    result = monitoring_store.update_account_state(
                        url=str(payload.get("url") or ""),
                        active=bool(payload.get("active")),
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/bulk-toggle":
                    payload = self.read_json_body()
                    result = monitoring_store.bulk_update_account_state(
                        urls=[str(item) for item in (payload.get("urls") or []) if str(item).strip()],
                        active=bool(payload.get("active")),
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/project":
                    payload = self.read_json_body()
                    result = monitoring_store.assign_project(
                        urls=[str(item) for item in (payload.get("urls") or []) if str(item).strip()],
                        project=str(payload.get("project") or ""),
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/remove":
                    payload = self.read_json_body()
                    result = monitoring_store.remove_account(url=str(payload.get("url") or ""))
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/retry":
                    payload = self.read_json_body()
                    result = monitoring_store.retry_account(url=str(payload.get("url") or ""))
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/retry-upload":
                    payload = self.read_json_body()
                    result = monitoring_store.retry_feishu_upload(
                        project=str(payload.get("project") or ""),
                        scope=str(payload.get("scope") or "full"),
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/account-rankings/export":
                    payload = self.read_json_body()
                    dashboard_payload = dashboard_store.peek_payload() or dashboard_store.get_payload(force=False)
                    result = export_single_account_rankings(
                        payload=dashboard_payload,
                        account_id=str(payload.get("account_id") or ""),
                        project=str(payload.get("project") or ""),
                        export_dir=str(payload.get("export_dir") or ""),
                    )
                    self.send_json_response(HTTPStatus.OK, {"ok": True, **result})
                    return
                if path == "/api/project-rankings/export":
                    payload = self.read_json_body()
                    dashboard_payload = dashboard_store.peek_payload() or dashboard_store.get_payload(force=False)
                    result = export_project_rankings(
                        payload=dashboard_payload,
                        project=str(payload.get("project") or ""),
                        account_ids=[str(item) for item in (payload.get("account_ids") or []) if str(item).strip()],
                        export_dir=str(payload.get("export_dir") or ""),
                    )
                    self.send_json_response(HTTPStatus.OK, {"ok": True, **result})
                    return
                if path == "/api/monitored-accounts/sync":
                    result = monitoring_store.request_sync()
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/sync-project":
                    payload = self.read_json_body()
                    result = monitoring_store.request_sync(project=str(payload.get("project") or ""))
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/system-config":
                    payload = self.read_json_body()
                    result = save_system_config(monitoring_store.env_file, monitoring_store.urls_file, payload)
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/server-cache-push":
                    payload = self.read_json_body()
                    result = monitoring_store.push_server_cache(
                        auto=False,
                        account_ids=[str(payload.get("account_id") or "").strip()] if str(payload.get("account_id") or "").strip() else [],
                    )
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/server-cache-upload":
                    settings = load_settings(monitoring_store.env_file)
                    expected_token = str(getattr(settings, "server_cache_upload_token", "") or "").strip()
                    provided_token = str(self.headers.get("X-Upload-Token") or "").strip()
                    if expected_token and provided_token != expected_token:
                        self.send_json_response(HTTPStatus.FORBIDDEN, {"ok": False, "message": "上传令牌无效"})
                        return
                    payload = self.read_json_body()
                    result = save_uploaded_server_cache(
                        env_file=monitoring_store.env_file,
                        urls_file=monitoring_store.urls_file,
                        payload=payload,
                    )
                    dashboard_payload = payload.get("dashboard_payload") or {}
                    dashboard_store.set_local_override(dashboard_payload)
                    monitoring_store._profile_lookup_cache_rows = []  # noqa: SLF001
                    monitoring_store._profile_lookup_cache_error = ""  # noqa: SLF001
                    monitoring_store._profile_lookup_cache_loaded_at = 0.0  # noqa: SLF001
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/login-state/check":
                    monitoring_payload = monitoring_store.get_payload()
                    entries = monitoring_payload.get("entries") or []
                    sample_url = str((next((item for item in entries if item.get("active")), {}) or {}).get("url") or "")
                    login_state = login_state_store.get_payload(force=True, sample_url=sample_url)
                    self.send_json_response(
                        HTTPStatus.OK,
                        {"ok": True, "message": "已开始登录态自检", "login_state": login_state},
                    )
                    return
                self.send_json_response(HTTPStatus.NOT_FOUND, {"ok": False, "message": "接口不存在"})
            except ValueError as exc:
                self.send_json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "message": str(exc)})
            except Exception as exc:
                self.send_json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "message": str(exc)})

    return LocalStatsHandler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动本地统计前端 app。")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--urls-file", default=DEFAULT_URLS_FILE)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--cache-seconds", type=int, default=30)
    parser.add_argument("--open-browser", action="store_true")
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    dashboard_store = DashboardStore(env_file=args.env_file, cache_seconds=args.cache_seconds)
    login_state_store = LoginStateStore(env_file=args.env_file, urls_file=args.urls_file)
    monitoring_store = MonitoringSyncStore(
        env_file=args.env_file,
        urls_file=args.urls_file,
        dashboard_store=dashboard_store,
        login_state_store=login_state_store,
        manual_sync_cooldown_seconds=max(0, int(getattr(settings, "xhs_manual_sync_cooldown_minutes", 20) or 0)) * 60,
    )
    handler = build_handler(
        dashboard_store=dashboard_store,
        monitoring_store=monitoring_store,
        login_state_store=login_state_store,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    url = f"http://{args.host}:{args.port}"
    print(f"[OK] local_stats_app={url}")
    if args.open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
