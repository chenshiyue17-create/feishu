from __future__ import annotations

import argparse
import copy
import json
import os
import re
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import replace
from datetime import datetime
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
from ..config import load_settings
from ..feishu import FeishuBitableClient
from ..profile_batch_report import normalize_profile_url, normalize_profile_urls
from ..profile_batch_to_feishu import load_reports_for_sync, sync_reports_to_feishu
from ..profile_dashboard_to_feishu import (
    build_single_work_ranking_fields,
    build_single_work_rankings,
    compute_dashboard_metrics,
    extract_snapshot_date,
    parse_exact_number,
    rank_profile_works,
)
from ..profile_report import build_profile_report, load_profile_report_payload
from ..profile_to_feishu import PROFILE_TABLE_NAME
from ..profile_works_to_feishu import WORKS_TABLE_NAME
from ..xhs import build_proxy_pool_status
from .data_service import build_dashboard_payload_from_tables


PORTAL_TABLE_NAME = "小红书仪表盘总控"
CALENDAR_TABLE_NAME = "小红书日历留底"
RANKING_TABLE_NAME = "小红书单条作品排行"
ALERT_TABLE_NAME = "小红书评论预警"
WEB_DIR = Path(__file__).resolve().parent / "web"
DEFAULT_URLS_FILE = "xhs_feishu_monitor/input/robam_multi_profile_urls.txt"
MONITORED_PAUSED_PREFIX = "# PAUSED "
DEFAULT_PROJECT_NAME = "默认项目"
PROFILE_USER_ID_PATTERN = re.compile(r"/user/profile/([0-9a-z]+)", re.IGNORECASE)
LOGIN_STATE_IDLE_PAYLOAD = {
    "state": "idle",
    "message": "等待自动自检",
    "checked_at": "",
    "cache_age_seconds": 0,
    "checking": False,
    "fetch_mode": "",
    "cookie_source": "none",
    "cookie_source_label": "未配置登录态",
    "cookie_ready": False,
    "detail_ready": False,
    "degraded": False,
    "sample_url": "",
    "sample_account": "",
    "sample_user_id": "",
    "work_count": 0,
    "note_id_count": 0,
    "comment_count_ready": 0,
    "hints": [],
}

LOGIN_WAIT_TIMEOUT_SECONDS = 180
LOGIN_WAIT_POLL_SECONDS = 5

DASHBOARD_SERIES_META = {
    "mode": "daily",
    "update_time": "14:00",
    "source": "小红书日历留底",
    "note": "趋势图按天留底，每个账号每天保留 1 个点。",
}


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


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
    return {
        "rank": int(fields.get("排名") or 0),
        "account_id": str(fields.get("账号ID") or "").strip(),
        "account": str(fields.get("账号") or "").strip(),
        "title": str(fields.get("标题文案") or "").strip(),
        "metric": fields.get("排序值"),
        "summary": str(fields.get("榜单摘要") or "").strip(),
        "profile_url": extract_link(fields.get("主页链接")),
        "note_url": extract_link(fields.get("作品链接")),
        "cover_url": extract_link(fields.get("封面图")),
    }


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
            raise
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


def detect_cookie_source(settings) -> tuple[str, str]:
    if str(getattr(settings, "xhs_cookie", "") or "").strip():
        return "manual_cookie", "手动 Cookie"
    profile_root = str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip()
    if profile_root:
        if is_default_chrome_profile_root(profile_root):
            return "chrome_profile", "Chrome 默认资料"
        profile_name = Path(profile_root).name or "Chrome"
        return "chrome_profile", f"Chrome 登录态 · {profile_name}"
    return "none", "未配置登录态"


def build_login_state_payload(**overrides: Any) -> Dict[str, Any]:
    payload = dict(LOGIN_STATE_IDLE_PAYLOAD)
    payload.update(overrides)
    hints = payload.get("hints") or []
    payload["hints"] = [str(item) for item in hints if str(item).strip()]
    return payload


def login_state_requires_interactive_login(payload: Dict[str, Any]) -> bool:
    state = str(payload.get("state") or "").strip()
    message = str(payload.get("message") or "").strip().lower()
    if state == "warning" and str(payload.get("cookie_source") or "").strip() == "chrome_profile":
        if not bool(payload.get("detail_ready")) and any(
            keyword in message
            for keyword in (
                "公开页摘要",
                "note_id",
                "详细数据",
                "退化",
            )
        ):
            return True
    if state != "error":
        return False
    return any(
        keyword in message
        for keyword in (
            "登录态",
            "登录页",
            "/login",
            "空结果",
            "未解析到任何作品",
            "反爬页",
        )
    )


def run_login_state_self_check(*, env_file: str, sample_url: str = "") -> Dict[str, Any]:
    settings = load_settings(env_file)
    fetch_mode = str(getattr(settings, "xhs_fetch_mode", "") or "").strip().lower() or "requests"
    cookie_source, cookie_source_label = detect_cookie_source(settings)
    checked_at = iso_now()
    hints: List[str] = []

    if fetch_mode != "requests":
        message = f"当前抓取模式为 {fetch_mode}，自动自检先只校验配置；样本抓取建议通过手动同步确认。"
        if fetch_mode == "local_browser":
            hints.append("local_browser 模式会直接调用本机浏览器，不适合频繁后台自检。")
        return build_login_state_payload(
            state="warning",
            message=message,
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=True,
            sample_url=sample_url,
            hints=hints,
        )

    cookie_ready = False
    if cookie_source == "manual_cookie":
        cookie_ready = True
    elif cookie_source == "chrome_profile":
        try:
            cookie_ready = bool(
                export_xiaohongshu_cookie_header(
                    settings.xhs_chrome_cookie_profile,
                    resolve_chrome_profile_directory(settings.playwright_profile_directory),
                ).strip()
            )
        except Exception as exc:
            return build_login_state_payload(
                state="error",
                message=f"Chrome 登录态读取失败：{exc}",
                checked_at=checked_at,
                fetch_mode=fetch_mode,
                cookie_source=cookie_source,
                cookie_source_label=cookie_source_label,
                cookie_ready=False,
                sample_url=sample_url,
                hints=[
                    "重新用本机 Chrome 登录小红书后，再点一次“立即自检”。",
                    "确认 XHS_CHROME_COOKIE_PROFILE 仍指向可用的登录目录。",
                ],
            )
    else:
        hints.append("未配置 XHS_COOKIE 或 Chrome 登录态目录，当前只能依赖公开页能力。")

    if not sample_url:
        return build_login_state_payload(
            state="warning" if cookie_source == "none" else "ok",
            message="已完成登录态配置检查；待添加监测账号后会继续做样本抓取自检。",
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=cookie_ready,
            hints=hints,
        )

    try:
        payload = load_profile_report_payload(settings=settings, profile_url=sample_url)
        report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    except Exception as exc:
        return build_login_state_payload(
            state="error",
            message=f"样本账号抓取失败：{exc}",
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=cookie_ready,
            sample_url=sample_url,
            hints=[
                "先在浏览器里打开小红书确认当前登录态仍有效。",
                "如果刚重新登录，点一次“立即自检”刷新状态。",
            ],
        )

    profile = report.get("profile") or {}
    works = report.get("works") or []
    sample_account = str(profile.get("nickname") or "").strip()
    sample_user_id = str(profile.get("profile_user_id") or "").strip()
    work_count = len(works)
    note_id_count = sum(1 for item in works if str(item.get("note_id") or "").strip())
    comment_count_ready = sum(1 for item in works if item.get("comment_count") is not None)
    detail_ready = note_id_count > 0
    has_profile_core = bool(sample_account or sample_user_id or profile.get("fans_count_text"))

    if not has_profile_core and work_count == 0:
        state = "error"
        message = "样本账号返回了空结果，登录态可能已过期，或当前请求命中了反爬页。"
        hints.extend(
            [
                "先在本机 Chrome 打开小红书主页，确认账号仍处于登录状态。",
                "如果当前是 Chrome 登录态模式，建议重新登录后再点“立即自检”。",
            ]
        )
    elif work_count == 0:
        state = "error"
        message = "样本账号未解析到任何作品，详细数据链路当前不可用。"
        hints.extend(
            [
                "当前账号大概率退化成公开页或反爬结果，建议重新登录后复检。",
                "如持续为空，优先检查 XHS_CHROME_COOKIE_PROFILE 对应的登录目录是否正确。",
            ]
        )
    elif not detail_ready:
        state = "warning"
        message = "样本账号只拿到公开页摘要，未拿到 note_id，作品详情与评论数据已退化。"
        hints.extend(
            [
                "当前还能看账号摘要，但详细作品数据能力不足。",
                "重新登录本机 Chrome 后再点“立即自检”，通常能恢复 note_id 抓取。",
            ]
        )
    elif cookie_source == "none":
        state = "warning"
        message = "样本账号抓取正常，但当前没有稳定登录态来源，详细数据能力可能随时退化。"
        hints.append("建议改用本机 Chrome 登录态目录，长期稳定性会更高。")
    else:
        state = "ok"
        message = "登录态正常，样本账号已拿到作品明细能力。"
        hints.append("如果后面看见 note_id 或评论字段突然清空，直接点“立即自检”确认登录态。")

    return build_login_state_payload(
        state=state,
        message=message,
        checked_at=checked_at,
        fetch_mode=fetch_mode,
        cookie_source=cookie_source,
        cookie_source_label=cookie_source_label,
        cookie_ready=cookie_ready,
        detail_ready=detail_ready,
        degraded=state in {"warning", "error"},
        sample_url=sample_url,
        sample_account=sample_account,
        sample_user_id=sample_user_id,
        work_count=work_count,
        note_id_count=note_id_count,
        comment_count_ready=comment_count_ready,
        hints=hints,
    )


def open_xiaohongshu_login_window(*, settings, target_url: str = "") -> bool:
    url = str(target_url or "").strip() or "https://www.xiaohongshu.com/"
    chrome_profile_root = str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip()
    profile_directory = resolve_chrome_profile_directory(getattr(settings, "playwright_profile_directory", "") or "Default")
    if chrome_profile_root:
        try:
            if is_default_chrome_profile_root(chrome_profile_root):
                subprocess.Popen(
                    ["open", "-a", "Google Chrome", url],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return True
            subprocess.Popen(
                [
                    "open",
                    "-na",
                    "Google Chrome",
                    "--args",
                    f"--user-data-dir={resolve_chrome_profile_root(chrome_profile_root)}",
                    f"--profile-directory={profile_directory}",
                    url,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            pass
    try:
        return bool(webbrowser.open(url))
    except Exception:
        return False


def wait_for_xiaohongshu_login(
    *,
    env_file: str,
    settings,
    sample_url: str,
    on_wait: Optional[Callable[[Dict[str, Any]], None]] = None,
    timeout_seconds: int = LOGIN_WAIT_TIMEOUT_SECONDS,
    poll_seconds: int = LOGIN_WAIT_POLL_SECONDS,
) -> Dict[str, Any]:
    payload = run_login_state_self_check(env_file=env_file, sample_url=sample_url)
    if not login_state_requires_interactive_login(payload):
        return payload
    if not str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip():
        return payload

    window_opened = open_xiaohongshu_login_window(settings=settings, target_url=sample_url or "https://www.xiaohongshu.com/")
    waiting_payload = dict(payload)
    waiting_payload["login_window_opened"] = window_opened
    waiting_payload["message"] = (
        "检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。"
        if window_opened
        else "检测到小红书未登录，但未能自动打开网页登录，请先手动登录后重试。"
    )
    if on_wait is not None:
        on_wait(waiting_payload)
    if not window_opened:
        return waiting_payload

    deadline = time.time() + max(1, int(timeout_seconds or 1))
    while time.time() < deadline:
        time.sleep(max(1, int(poll_seconds or 1)))
        payload = run_login_state_self_check(env_file=env_file, sample_url=sample_url)
        payload["login_window_opened"] = True
        if not login_state_requires_interactive_login(payload):
            return payload
        if on_wait is not None:
            on_wait(payload)
    payload["login_window_opened"] = True
    return payload


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
            payload = run_login_state_self_check(env_file=self.env_file, sample_url=sample_url)
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
        merged = {
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
        metadata[url] = merged
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
        phase_percent = round((safe_current / safe_total) * 100)
        overall_percent = round((safe_current / safe_total) * 50)
        detail_text = f"正在抓取账号 {safe_current}/{safe_total}"
    elif phase == "sync":
        phase_label = "写入飞书"
        phase_percent = round((safe_current / safe_total) * 100)
        overall_percent = 50 + round((safe_current / safe_total) * 50)
        detail_text = f"正在写入飞书 {safe_current}/{safe_total}"
    else:
        phase_percent = 0

    if account:
        detail_text += f" · {account}"
    if works:
        detail_text += f" · {works} 条作品"
    if status and phase == "collect":
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
        self._pending_sync_urls: List[str] = []
        self._manual_last_requested_at = 0.0
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

    def get_payload(self) -> Dict[str, Any]:
        with self._lock:
            entries = parse_monitored_entries(self.urls_file)
            metadata_index = load_monitored_metadata(self.urls_file)
            settings = load_settings(self.env_file)
            profile_lookup_error = ""
            try:
                profile_rows = load_profile_table_rows(self.env_file)
            except Exception as exc:
                profile_rows = []
                profile_lookup_error = str(exc)
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
            return {
                "urls_file": str(resolve_text_path(self.urls_file)),
                "total": len(enriched_entries),
                "active_count": len(active_entries),
                "paused_count": len(enriched_entries) - len(active_entries),
                "urls": [entry["url"] for entry in active_entries],
                "entries": enriched_entries,
                "projects": build_project_summaries(enriched_entries),
                "profile_lookup_error": profile_lookup_error,
                "login_state": (
                    self.login_state_store.get_payload(sample_url=sample_url)
                    if self.login_state_store
                    else build_login_state_payload()
                ),
                "proxy_pool": build_proxy_pool_status(settings),
                "sync_status": self._status_snapshot_locked(),
            }

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
        snapshot.update(self._build_manual_cooldown_locked())
        return snapshot

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
            started = self._request_sync_locked(reason=reason, urls=urls)
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

    def _request_sync_locked(self, *, reason: str, urls: Optional[List[str]] = None) -> bool:
        if self._running:
            self._pending_resync = True
            self._pending_sync_urls = list(urls or [])
            self._status["pending"] = True
            self._status["message"] = f"{reason}，当前任务完成后自动重跑"
            return False
        self._running = True
        self._pending_resync = False
        self._current_sync_urls = list(urls or [])
        self._pending_sync_urls = []
        started_at = iso_now()
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

    def _ensure_login_ready_for_sync(self, *, settings, sample_url: str) -> None:
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
            try:
                settings = load_settings(self.env_file)
                settings.validate_for_sync()
                current_urls = list(self._current_sync_urls)
                self._ensure_login_ready_for_sync(
                    settings=settings,
                    sample_url=current_urls[0] if current_urls else "",
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
                    self.dashboard_store.set_local_override(
                        build_dashboard_payload_with_reports(
                            base_payload=self.dashboard_store.get_cached_payload(),
                            reports=reports,
                        )
                    )
                except Exception:
                    pass
                summary = sync_reports_to_feishu(
                    reports=reports,
                    settings=settings,
                    profile_table_name=self.profile_table_name,
                    works_table_name=self.works_table_name,
                    ensure_fields=self.ensure_fields,
                    sync_dashboard=self.sync_dashboard,
                    progress_callback=self._handle_progress_update,
                )
                if not self.dashboard_store.commit_local_override():
                    self.dashboard_store.invalidate()
                finished_progress = build_progress_timing(
                    started_at=self._status.get("started_at", ""),
                    overall_percent=100,
                )
                result = {
                    "state": "success",
                    "message": f"同步完成，账号 {summary.get('total_accounts', 0)} 个，作品 {summary.get('total_works', 0)} 条",
                    "started_at": "",
                    "finished_at": finished_at,
                    "last_success_at": finished_at,
                    "last_error": "",
                    "pending": False,
                    "progress": {
                        "phase": "done",
                        "phase_label": "同步完成",
                        "current": summary.get("total_accounts", 0),
                        "total": summary.get("total_accounts", 0),
                        "phase_percent": 100,
                        "overall_percent": 100,
                        "account": "",
                        "works": summary.get("total_works", 0),
                        "status": "success",
                        "success_count": summary.get("total_accounts", 0),
                        "failed_count": 0,
                        "detail_text": f"已完成 {summary.get('total_accounts', 0)} 个账号同步",
                        "elapsed_seconds": finished_progress["elapsed_seconds"],
                        "elapsed_text": finished_progress["elapsed_text"],
                        "eta_seconds": 0,
                        "eta_text": "",
                    },
                    "summary": summary,
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
                self._running = False
                self._current_sync_urls = []
                self._pending_sync_urls = []
                self._status = result
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


def load_dashboard_payload(env_file: str) -> Dict[str, Any]:
    settings = load_settings(env_file)
    base_client = FeishuBitableClient(settings)
    table_ids = list_table_ids(base_client)

    portal_rows = fetch_table_rows(settings, table_ids, PORTAL_TABLE_NAME)
    calendar_rows = fetch_table_rows(settings, table_ids, CALENDAR_TABLE_NAME)
    ranking_rows = fetch_table_rows(settings, table_ids, RANKING_TABLE_NAME)
    alert_rows = fetch_table_rows(settings, table_ids, ALERT_TABLE_NAME, required=False)

    return build_dashboard_payload_from_tables(
        portal_rows=portal_rows,
        calendar_rows=calendar_rows,
        ranking_rows=ranking_rows,
        alert_rows=alert_rows,
    )


def load_profile_table_rows(env_file: str) -> List[Dict[str, Any]]:
    settings = load_settings(env_file)
    base_client = FeishuBitableClient(settings)
    table_ids = list_table_ids(base_client)
    return fetch_table_rows(settings, table_ids, PROFILE_TABLE_NAME, required=False)


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
            if path == "/api/health":
                self.send_json_response(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "xhs_local_stats_app",
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
                self.send_error(HTTPStatus.BAD_REQUEST, "无效图片地址")
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
                self.send_error(HTTPStatus.BAD_GATEWAY, "图片加载失败")
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
                if path == "/api/monitored-accounts/sync":
                    result = monitoring_store.request_sync()
                    self.send_json_response(HTTPStatus.OK, result)
                    return
                if path == "/api/monitored-accounts/sync-project":
                    payload = self.read_json_body()
                    result = monitoring_store.request_sync(project=str(payload.get("project") or ""))
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
                self.send_error(HTTPStatus.NOT_FOUND, "接口不存在")
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
