from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from .local_stats_app.data_service import build_dashboard_payload_from_tables
from .profile_metrics import build_recent_comments_summary, extract_note_reference_from_url
from .profile_dashboard_to_feishu import (
    build_dashboard_calendar_fields,
    build_dashboard_portal_fields,
    build_single_work_ranking_fields,
    build_single_work_rankings,
)
from .profile_works_to_feishu import build_work_calendar_history_index, build_work_fingerprint
from .xhs import XHSCollector


DEFAULT_PROJECT_CACHE_DIR = "/Users/cc/Downloads/飞书缓存"
DEFAULT_DASHBOARD_CACHE_FILE = "dashboard_all.json"
TRACKED_WORKS_CACHE_FILE = "tracked_works.json"
TRACKED_WORK_HISTORY_CACHE_FILE = "tracked_work_history.json"
DEFAULT_EXPORT_CACHE_DIR = "账号榜单导出"
DEFAULT_PROJECT_COVER_DIR = "covers"


def resolve_project_cache_dir(settings) -> Path:
    raw = str(getattr(settings, "project_cache_dir", "") or DEFAULT_PROJECT_CACHE_DIR).strip()
    return Path(raw).expanduser().resolve()


def load_cached_dashboard_payload(settings) -> Dict[str, Any]:
    path = resolve_project_cache_dir(settings) / DEFAULT_DASHBOARD_CACHE_FILE
    if not path.exists():
        return {}
    payload = _read_json(path, {})
    return payload if isinstance(payload, dict) else {}


def rebuild_dashboard_cache_from_project_dirs(settings) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    combined_calendar_rows_source: List[Dict[str, Any]] = []
    combined_project_ranking_rows: List[Dict[str, Any]] = []
    combined_alert_rows: List[Dict[str, Any]] = []

    for project_dir in sorted(path for path in cache_dir.iterdir() if path.is_dir() and path.name != DEFAULT_EXPORT_CACHE_DIR):
        combined_calendar_rows_source.extend(_read_json(project_dir / "calendar_rows.json", []))
        combined_project_ranking_rows.extend(_read_json(project_dir / "ranking_rows.json", []))
        project_dashboard = _read_json(project_dir / "dashboard.json", {})
        combined_alert_rows.extend((project_dashboard or {}).get("alerts") or [])

    if not combined_calendar_rows_source and not combined_project_ranking_rows and not combined_alert_rows:
        return {}

    combined_calendar_rows = _merge_calendar_rows([], combined_calendar_rows_source)
    combined_ranking_rows = _sort_ranking_rows(combined_project_ranking_rows)
    combined_payload = build_dashboard_payload_from_tables(
        portal_rows=[build_dashboard_portal_fields(_build_stub_reports_from_calendar_rows(combined_calendar_rows))] if combined_calendar_rows else [],
        calendar_rows=combined_calendar_rows,
        ranking_rows=combined_ranking_rows,
        alert_rows=_sort_alert_rows(combined_alert_rows),
    )
    _write_json(cache_dir / DEFAULT_DASHBOARD_CACHE_FILE, combined_payload)
    _write_json(cache_dir / "calendar_rows_all.json", combined_calendar_rows)
    _write_json(cache_dir / "ranking_rows_all.json", combined_ranking_rows)
    _write_csv(cache_dir / "全部项目-账号日历留底.csv", combined_calendar_rows)
    _write_csv(cache_dir / "全部项目-单条排行榜.csv", combined_ranking_rows)
    return combined_payload


def repair_dashboard_cache_from_exports(*, settings, monitored_metadata: Dict[str, Any]) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    export_root = cache_dir / DEFAULT_EXPORT_CACHE_DIR
    if not export_root.exists():
        return {}

    project_payloads: Dict[str, Dict[str, Any]] = {}
    combined_calendar_rows: List[Dict[str, Any]] = []
    combined_ranking_rows: List[Dict[str, Any]] = []
    for project_dir in sorted(path for path in export_root.iterdir() if path.is_dir()):
        summary_path = project_dir / "最近一次项目导出.json"
        if not summary_path.exists():
            continue
        summary = _read_json(summary_path, {})
        if not isinstance(summary, dict):
            continue
        project_name = str(summary.get("project") or project_dir.name).strip() or project_dir.name
        accounts = summary.get("accounts") if isinstance(summary.get("accounts"), list) else []
        if not accounts:
            continue
        calendar_rows = _build_calendar_rows_from_export_summary(
            project_name=project_name,
            summary=summary,
            monitored_metadata=monitored_metadata,
        )
        ranking_rows = _build_ranking_rows_from_export_summary(summary)
        if not calendar_rows and not ranking_rows:
            continue
        payload = build_dashboard_payload_from_tables(
            portal_rows=[build_dashboard_portal_fields(_build_stub_reports_from_calendar_rows(calendar_rows))] if calendar_rows else [],
            calendar_rows=calendar_rows,
            ranking_rows=ranking_rows,
            alert_rows=[],
        )
        target_project_dir = cache_dir / _slugify_project_name(project_name)
        target_project_dir.mkdir(parents=True, exist_ok=True)
        _write_json(target_project_dir / "dashboard.json", payload)
        _write_json(target_project_dir / "calendar_rows.json", calendar_rows)
        _write_json(target_project_dir / "ranking_rows.json", ranking_rows)
        _write_csv(target_project_dir / "账号日历留底.csv", calendar_rows)
        _write_csv(target_project_dir / "单条排行榜.csv", ranking_rows)
        project_payloads[project_name] = payload
        combined_calendar_rows.extend(calendar_rows)
        combined_ranking_rows.extend(ranking_rows)

    if not project_payloads:
        return {}

    combined_payload = build_dashboard_payload_from_tables(
        portal_rows=[build_dashboard_portal_fields(_build_stub_reports_from_calendar_rows(combined_calendar_rows))] if combined_calendar_rows else [],
        calendar_rows=_merge_calendar_rows([], combined_calendar_rows),
        ranking_rows=_sort_ranking_rows(combined_ranking_rows),
        alert_rows=[],
    )
    _write_json(cache_dir / DEFAULT_DASHBOARD_CACHE_FILE, combined_payload)
    _write_json(cache_dir / "calendar_rows_all.json", _merge_calendar_rows([], combined_calendar_rows))
    _write_json(cache_dir / "ranking_rows_all.json", _sort_ranking_rows(combined_ranking_rows))
    _write_csv(cache_dir / "全部项目-账号日历留底.csv", _merge_calendar_rows([], combined_calendar_rows))
    _write_csv(cache_dir / "全部项目-单条排行榜.csv", _sort_ranking_rows(combined_ranking_rows))
    return combined_payload


def write_project_cache_bundle(*, reports: List[Dict[str, Any]], settings) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    cache_dir.mkdir(parents=True, exist_ok=True)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for report in reports:
        project_name = str(report.get("project") or "默认项目").strip() or "默认项目"
        grouped.setdefault(project_name, []).append(report)

    project_paths: Dict[str, str] = {}
    for project_name, project_reports in grouped.items():
        project_dir = cache_dir / _slugify_project_name(project_name)
        project_dir.mkdir(parents=True, exist_ok=True)
        current_account_ids = {
            str((report.get("profile") or {}).get("profile_user_id") or "").strip()
            for report in project_reports
            if str((report.get("profile") or {}).get("profile_user_id") or "").strip()
        }
        tracked_state = _build_project_tracked_work_state(
            project_name=project_name,
            project_reports=project_reports,
            project_dir=project_dir,
            settings=settings,
        )
        _persist_project_cover_assets(project_dir=project_dir, tracked_state=tracked_state)
        tracked_account_ids = {
            str(item.get("account_id") or "").strip()
            for item in ((tracked_state.get("payload") or {}).get("items") or [])
            if str(item.get("account_id") or "").strip()
        }
        valid_account_ids = current_account_ids | tracked_account_ids
        existing_calendar_rows = _read_json(project_dir / "calendar_rows.json", [])
        if valid_account_ids:
            existing_calendar_rows = _filter_calendar_rows_by_account_ids(existing_calendar_rows, valid_account_ids)
        calendar_rows = _merge_calendar_rows(existing_calendar_rows, [build_dashboard_calendar_fields(report) for report in project_reports])
        ranking_rows = _build_ranking_rows_from_items(
            tracked_state["ranking_items"],
            history_index=tracked_state["history_index"],
        )
        alert_rows = tracked_state["alert_rows"]
        payload = build_dashboard_payload_from_tables(
            portal_rows=[build_dashboard_portal_fields(project_reports)] if project_reports else [],
            calendar_rows=calendar_rows,
            ranking_rows=ranking_rows,
            alert_rows=alert_rows,
        )
        _write_json(project_dir / "dashboard.json", payload)
        _write_json(project_dir / "calendar_rows.json", calendar_rows)
        _write_json(project_dir / "ranking_rows.json", ranking_rows)
        _write_json(project_dir / TRACKED_WORKS_CACHE_FILE, tracked_state["payload"])
        _write_json(project_dir / TRACKED_WORK_HISTORY_CACHE_FILE, tracked_state["history_payload"])
        _write_csv(project_dir / "账号日历留底.csv", calendar_rows)
        _write_csv(project_dir / "单条排行榜.csv", ranking_rows)
        project_paths[project_name] = str(project_dir)

    combined_payload = rebuild_dashboard_cache_from_project_dirs(settings)

    return {
        "cache_dir": str(cache_dir),
        "projects": project_paths,
        "combined_dashboard_path": str(cache_dir / DEFAULT_DASHBOARD_CACHE_FILE),
    }


def _persist_project_cover_assets(*, project_dir: Path, tracked_state: Dict[str, Any]) -> None:
    cover_dir = project_dir / DEFAULT_PROJECT_COVER_DIR
    payload_items = list((tracked_state.get("payload") or {}).get("items") or [])
    ranking_items = list(tracked_state.get("ranking_items") or [])
    if not payload_items and not ranking_items:
        return

    local_path_by_fingerprint: Dict[str, str] = {}
    for entry in payload_items:
        cover_url = str(entry.get("cover_url") or "").strip()
        fingerprint = str(entry.get("fingerprint") or entry.get("raw_fingerprint") or "").strip()
        if not cover_url or not fingerprint:
            continue
        local_path = _save_cover_asset(cover_dir=cover_dir, fingerprint=fingerprint, cover_url=cover_url)
        if not local_path:
            continue
        entry["local_cover_path"] = local_path
        local_path_by_fingerprint[fingerprint] = local_path

    for item in ranking_items:
        fingerprint = str(item.get("fingerprint") or item.get("baseline_fingerprint") or "").strip()
        local_path = local_path_by_fingerprint.get(fingerprint, "")
        if local_path:
            item["local_cover_path"] = local_path


def _save_cover_asset(*, cover_dir: Path, fingerprint: str, cover_url: str) -> str:
    cover_dir.mkdir(parents=True, exist_ok=True)
    stem = hashlib.sha1(f"{fingerprint}|{cover_url}".encode("utf-8")).hexdigest()
    existing = next(iter(sorted(cover_dir.glob(f"{stem}.*"))), None)
    if existing is not None:
        return str(existing)

    request = urllib.request.Request(
        cover_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.xiaohongshu.com/",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read()
            content_type = ""
            headers = getattr(response, "headers", None)
            if headers is not None:
                content_type = str(getattr(headers, "get_content_type", lambda: "")() or headers.get("Content-Type") or "").strip()
    except Exception:
        return ""
    if not payload:
        return ""

    suffix = _guess_cover_suffix(cover_url=cover_url, content_type=content_type)
    target_path = cover_dir / f"{stem}{suffix}"
    try:
        target_path.write_bytes(payload)
    except Exception:
        return ""
    return str(target_path)


def _guess_cover_suffix(*, cover_url: str, content_type: str) -> str:
    parsed = urllib.parse.urlparse(str(cover_url or "").strip())
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return suffix
    guessed = mimetypes.guess_extension(str(content_type or "").split(";", 1)[0].strip() or "")
    if guessed in {".jpg", ".jpe"}:
        return ".jpg"
    if guessed in {".jpeg", ".png", ".webp", ".gif"}:
        return guessed
    return ".jpg"


def _build_ranking_rows(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _build_ranking_rows_from_items(_build_tracked_ranking_items_from_reports(reports))


def _build_ranking_rows_from_items(
    items: List[Dict[str, Any]],
    *,
    history_index: Optional[Dict[str, List[Any]]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for rank_type, ranked_items in build_single_work_rankings(
        reports=[],
        items=items,
        history_index=history_index or {},
    ).items():
        for rank, item in enumerate(ranked_items, start=1):
            rows.append(build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank))
    return _sort_ranking_rows(rows)


def _merge_calendar_rows(existing_rows: Any, new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing_rows if isinstance(existing_rows, list) else []:
        key = str((row or {}).get("日历键") or "").strip()
        if key:
            merged[key] = dict(row)
    for row in new_rows:
        key = str((row or {}).get("日历键") or "").strip()
        if key:
            merged[key] = dict(row)
    return sorted(
        merged.values(),
        key=lambda item: (
            str(item.get("日期文本") or ""),
            str(item.get("账号ID") or ""),
        ),
    )


def _filter_calendar_rows_by_account_ids(rows: Any, account_ids: set[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    if not account_ids:
        return [dict(row) for row in rows if isinstance(row, dict)]
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get("账号ID") or "").strip()
        if account_id and account_id in account_ids:
            filtered.append(dict(row))
    return filtered


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _stringify_cell(row.get(key)) for key in fieldnames})


def _stringify_cell(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or value.get("text") or "").strip()
    if isinstance(value, list):
        return " | ".join(_stringify_cell(item) for item in value if _stringify_cell(item))
    return str(value or "").strip()


def _build_calendar_rows_from_export_summary(
    *,
    project_name: str,
    summary: Dict[str, Any],
    monitored_metadata: Dict[str, Any],
) -> List[Dict[str, Any]]:
    snapshot_text = str(summary.get("snapshot_time") or "").strip()
    snapshot_iso = _normalize_snapshot_iso(snapshot_text) or str(summary.get("updated_at") or "").strip()
    snapshot_date = _extract_snapshot_date(snapshot_iso or snapshot_text)
    rows: List[Dict[str, Any]] = []
    for account_summary in summary.get("accounts") or []:
        account_id = str(account_summary.get("account_id") or "").strip()
        account_name = str(account_summary.get("account") or "").strip()
        if not account_id:
            continue
        like_rows = _read_json(Path(str((account_summary.get("files") or {}).get("like_json") or "")), [])
        comment_rows = _read_json(Path(str((account_summary.get("files") or {}).get("comment_json") or "")), [])
        profile_url = _pick_account_profile_url(account_summary, like_rows, comment_rows)
        metadata = _find_metadata_for_account(monitored_metadata, account_id=account_id, profile_url=profile_url)
        works_text = str((metadata or {}).get("works_text") or "").strip()
        works_value = _to_int(_strip_plus(works_text))
        fans_value = _to_int((metadata or {}).get("fans_text"))
        interaction_value = _to_int((metadata or {}).get("interaction_text"))
        row = {
            "日历键": f"{snapshot_date}|{account_id}",
            "日期文本": snapshot_date,
            "账号ID": account_id,
            "账号": account_name or str((metadata or {}).get("account") or "").strip(),
            "粉丝数": fans_value,
            "获赞收藏数": interaction_value,
            "账号总作品数": works_value,
            "作品数展示": works_text,
            "首页总点赞": sum(_to_int(item.get("数值")) for item in like_rows if isinstance(item, dict)),
            "首页总评论": sum(_to_int(item.get("数值")) for item in comment_rows if isinstance(item, dict)),
            "主页链接": {"text": account_name or str((metadata or {}).get("account") or "").strip(), "link": profile_url},
            "头部作品标题": str((like_rows[0] if like_rows else {}).get("标题") or "").strip(),
            "头部作品点赞": _to_int((like_rows[0] if like_rows else {}).get("数值")),
            "头部作品链接": {"text": "头部作品", "link": str((like_rows[0] if like_rows else {}).get("作品链接") or "").strip()},
            "周对比摘要": "",
            "项目": project_name,
            "数据更新时间": snapshot_iso or snapshot_text,
        }
        rows.append(row)
    return rows


def _build_ranking_rows_from_export_summary(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for account_summary in summary.get("accounts") or []:
        for rank_type, field_name in (("单条点赞排行", "like_json"), ("单条评论排行", "comment_json")):
            source_path = Path(str((account_summary.get("files") or {}).get(field_name) or ""))
            source_rows = _read_json(source_path, [])
            if not isinstance(source_rows, list):
                continue
            for item in source_rows:
                if not isinstance(item, dict):
                    continue
                comment_basis = str(item.get("评论口径") or "").strip()
                rows.append(
                    {
                        "榜单键": f"{rank_type}|{account_summary.get('account_id','')}|{item.get('标题','')}|{item.get('作品链接','')}",
                        "榜单类型": rank_type,
                        "排名": _to_int(item.get("排名")),
                        "卡片标签": f"TOP{_to_int(item.get('排名'))}",
                        "账号ID": str(item.get("账号ID") or account_summary.get("account_id") or "").strip(),
                        "账号": str(item.get("账号") or account_summary.get("account") or "").strip(),
                        "标题文案": str(item.get("标题") or "").strip(),
                        "排序值": _to_int(item.get("数值")),
                        "榜单摘要": str(item.get("摘要") or "").strip(),
                        "封面图": {"text": "封面图", "link": str(item.get("封面图") or "").strip()},
                        "主页链接": {"text": str(item.get("账号") or account_summary.get("account") or "").strip(), "link": str(item.get("主页链接") or "").strip()},
                        "作品链接": {"text": "作品详情", "link": str(item.get("作品链接") or "").strip()},
                        "评论数口径": comment_basis,
                        "单选": comment_basis,
                        "追踪状态": str(item.get("追踪状态") or "").strip(),
                        "首次入池日期": str(item.get("首次入池日期") or "").strip(),
                    }
                )
    return _sort_ranking_rows(rows)


def _build_stub_reports_from_calendar_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    reports: List[Dict[str, Any]] = []
    for row in rows:
        captured_at = str(row.get("数据更新时间") or row.get("日期文本") or "").strip()
        if captured_at.isdigit():
            timestamp = int(captured_at)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            captured_at = datetime.fromtimestamp(timestamp).astimezone().isoformat(timespec="seconds")
        elif captured_at and "T" not in captured_at and len(captured_at) == 10:
            captured_at = f"{captured_at}T14:00:00+08:00"
        reports.append(
            {
                "captured_at": captured_at,
                "profile": {
                    "profile_user_id": str(row.get("账号ID") or "").strip(),
                    "nickname": str(row.get("账号") or "").strip(),
                    "fans_count_text": str(row.get("粉丝数") or "").strip(),
                    "interaction_count_text": str(row.get("获赞收藏数") or "").strip(),
                    "profile_url": _extract_link_value(row.get("主页链接")),
                    "work_count_display_text": str(row.get("作品数展示") or "").strip(),
                    "total_work_count": _to_int(row.get("账号总作品数")),
                    "visible_work_count": _to_int(row.get("账号总作品数")),
                },
                "works": [],
            }
        )
    return reports


def _normalize_snapshot_iso(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" in text:
        return text
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").astimezone().isoformat(timespec="seconds")
    except Exception:
        return text


def _find_metadata_for_account(monitored_metadata: Dict[str, Any], *, account_id: str, profile_url: str) -> Dict[str, Any]:
    normalized_profile_url = str(profile_url or "").strip()
    for url, item in (monitored_metadata or {}).items():
        if str((item or {}).get("account_id") or "").strip() == account_id:
            return dict(item or {})
        if normalized_profile_url and str((item or {}).get("profile_url") or "").strip() == normalized_profile_url:
            return dict(item or {})
        if normalized_profile_url and str(url or "").strip() == normalized_profile_url:
            return dict(item or {})
    return {}


def _pick_account_profile_url(account_summary: Dict[str, Any], like_rows: List[Dict[str, Any]], comment_rows: List[Dict[str, Any]]) -> str:
    for rows in (like_rows, comment_rows):
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            link = str(item.get("主页链接") or "").strip()
            if link:
                return link
    return ""


def _strip_plus(value: str) -> str:
    return str(value or "").strip().replace("+", "")


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace(",", "")
    return int(text) if text.lstrip("-").isdigit() else 0


def _extract_link_value(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    return str(value or "").strip()


def _slugify_project_name(project_name: str) -> str:
    text = str(project_name or "").strip()
    cleaned = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", text)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "默认项目"


def _sort_ranking_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda item: (
            str(item.get("榜单类型") or ""),
            int(item.get("排名") or 0),
            str(item.get("账号") or ""),
            str(item.get("标题文案") or ""),
        ),
    )


def _build_project_tracked_work_state(
    *,
    project_name: str,
    project_reports: List[Dict[str, Any]],
    project_dir: Path,
    settings,
    collector_factory=None,
) -> Dict[str, Any]:
    existing_payload = _read_json(project_dir / TRACKED_WORKS_CACHE_FILE, {})
    existing_history_payload = _read_json(project_dir / TRACKED_WORK_HISTORY_CACHE_FILE, [])
    existing_items = existing_payload.get("items") if isinstance(existing_payload, dict) else []
    previous_items_by_key: Dict[str, Dict[str, Any]] = {}
    tracked_items: Dict[str, Dict[str, Any]] = {}
    current_account_ids = {
        str((report.get("profile") or {}).get("profile_user_id") or "").strip()
        for report in project_reports
        if str((report.get("profile") or {}).get("profile_user_id") or "").strip()
    }
    for raw_item in existing_items if isinstance(existing_items, list) else []:
        item = dict(raw_item or {})
        account_id = str(item.get("account_id") or "").strip()
        if not account_id:
            continue
        tracked_key = str(item.get("tracked_key") or "").strip() or _resolve_tracked_work_key(item)
        tracked_items[tracked_key] = item
        previous_items_by_key[tracked_key] = dict(item)

    latest_captured_at = max(
        (str(report.get("captured_at") or "").strip() for report in project_reports),
        default=datetime.now().astimezone().isoformat(timespec="seconds"),
    )
    latest_snapshot_date = _extract_snapshot_date(latest_captured_at)
    seen_keys: set[str] = set()

    for report in project_reports:
        profile = report.get("profile") or {}
        for raw_work in report.get("works") or []:
            current_entry = _build_tracked_work_entry(report=report, profile=profile, work=raw_work)
            matched_key = _find_matching_tracked_key(
                tracked_items=tracked_items,
                note_id=str(current_entry.get("note_id") or ""),
                fingerprint=str(current_entry.get("fingerprint") or ""),
            )
            final_key = _resolve_tracked_work_key(current_entry)
            existing_entry = tracked_items.pop(matched_key, None) if matched_key and matched_key != final_key else tracked_items.get(final_key)
            merged_entry = _merge_tracked_work_entry(
                existing_entry=existing_entry,
                current_entry=current_entry,
            )
            tracked_items[final_key] = merged_entry
            seen_keys.add(final_key)

    tracking_window_days = max(1, int(getattr(settings, "feishu_review_upload_days", 14) or 14))
    active_cutoff = datetime.now().astimezone().date() - timedelta(days=tracking_window_days - 1)
    active_collector_factory = collector_factory or XHSCollector
    collector = active_collector_factory(settings) if tracked_items else None

    for tracked_key in list(tracked_items.keys()):
        entry = dict(tracked_items.get(tracked_key) or {})
        if not _tracked_work_is_active(entry=entry, active_cutoff=active_cutoff):
            tracked_items.pop(tracked_key, None)
            continue
        if str(entry.get("account_id") or "").strip() not in current_account_ids:
            continue
        if tracked_key in seen_keys:
            continue
        refreshed_entry = _refresh_tracked_work_entry(
            entry=entry,
            collector=collector,
            captured_at=latest_captured_at,
            snapshot_date=latest_snapshot_date,
            settings=settings,
        )
        tracked_items[tracked_key] = refreshed_entry

    ranking_items = _build_tracked_ranking_items(
        tracked_items=list(tracked_items.values()),
        snapshot_date=latest_snapshot_date,
        active_cutoff=active_cutoff,
    )
    history_payload = _merge_tracked_history_payload(
        existing_history_payload=existing_history_payload,
        previous_items=list(previous_items_by_key.values()),
        current_items=list(tracked_items.values()),
        active_cutoff=active_cutoff,
    )
    history_index = build_work_calendar_history_index(history_payload)
    alert_rows = _build_alert_rows_from_tracked_items(
        current_items=tracked_items,
        previous_items=previous_items_by_key,
        captured_at=latest_captured_at,
        settings=settings,
    )
    payload = {
        "project": project_name,
        "updated_at": latest_captured_at,
        "tracking_window_days": tracking_window_days,
        "items": sorted(
            tracked_items.values(),
            key=lambda item: (
                str(item.get("account") or ""),
                str(item.get("title_copy") or ""),
                str(item.get("tracked_key") or ""),
            ),
        ),
    }
    return {
        "payload": payload,
        "ranking_items": ranking_items,
        "history_payload": history_payload,
        "history_index": history_index,
        "alert_rows": alert_rows,
    }


def _build_tracked_history_records(*, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for item in items:
        snapshot_date = str(item.get("snapshot_date") or "").strip()
        if not snapshot_date:
            continue
        like_count = _to_optional_int(item.get("like_count"))
        comment_count = _to_optional_int(item.get("comment_count"))
        candidate_fingerprints = [
            str(item.get("fingerprint") or "").strip(),
            str(item.get("raw_fingerprint") or "").strip(),
        ]
        seen: set[str] = set()
        for fingerprint in candidate_fingerprints:
            if not fingerprint or fingerprint in seen:
                continue
            seen.add(fingerprint)
            records.append(
                {
                    "fields": {
                        "作品指纹": fingerprint,
                        "日期文本": snapshot_date,
                        "点赞数": like_count,
                        "评论数": comment_count,
                    }
                }
            )
    return records


def _merge_tracked_history_payload(
    *,
    existing_history_payload: Any,
    previous_items: List[Dict[str, Any]],
    current_items: List[Dict[str, Any]],
    active_cutoff,
) -> List[Dict[str, Any]]:
    merged: Dict[tuple[str, str], Dict[str, Any]] = {}
    for record in existing_history_payload if isinstance(existing_history_payload, list) else []:
        if not isinstance(record, dict):
            continue
        fields = record.get("fields") or {}
        fingerprint = str(fields.get("作品指纹") or "").strip()
        snapshot_date = str(fields.get("日期文本") or fields.get("日历日期") or "").strip()
        if not fingerprint or not snapshot_date:
            continue
        merged[(fingerprint, snapshot_date)] = {"fields": dict(fields)}

    for record in _build_tracked_history_records(items=previous_items) + _build_tracked_history_records(items=current_items):
        fields = record.get("fields") or {}
        fingerprint = str(fields.get("作品指纹") or "").strip()
        snapshot_date = str(fields.get("日期文本") or "").strip()
        if not fingerprint or not snapshot_date:
            continue
        merged[(fingerprint, snapshot_date)] = {"fields": dict(fields)}

    min_date = active_cutoff.isoformat() if active_cutoff is not None else ""
    filtered = []
    for (_, snapshot_date), record in merged.items():
        if min_date and snapshot_date < min_date:
            continue
        filtered.append(record)
    filtered.sort(
        key=lambda record: (
            str((record.get("fields") or {}).get("日期文本") or ""),
            str((record.get("fields") or {}).get("作品指纹") or ""),
        )
    )
    return filtered


def _build_alert_rows_from_tracked_items(
    *,
    current_items: Dict[str, Dict[str, Any]],
    previous_items: Dict[str, Dict[str, Any]],
    captured_at: str,
    settings,
) -> List[Dict[str, Any]]:
    alert_rows: List[Dict[str, Any]] = []
    threshold = max(1, int(getattr(settings, "interaction_alert_delta_threshold", 10) or 10))
    min_previous = max(0, int(getattr(settings, "comment_alert_min_previous_count", 0) or 0))
    alert_date = _extract_snapshot_date(captured_at)

    for tracked_key, current in current_items.items():
        previous = dict(previous_items.get(tracked_key) or {})
        if not previous:
            continue
        current_like = _to_optional_int(current.get("like_count"))
        previous_like = _to_optional_int(previous.get("like_count"))
        current_comment = _to_optional_int(current.get("comment_count"))
        previous_comment = _to_optional_int(previous.get("comment_count"))
        like_delta = (
            (current_like - previous_like)
            if current_like is not None and previous_like is not None
            else 0
        )
        comment_delta = (
            (current_comment - previous_comment)
            if current_comment is not None and previous_comment is not None
            else 0
        )

        triggered_types: List[str] = []
        if (
            current_like is not None
            and previous_like is not None
            and current_like > previous_like
            and previous_like >= min_previous
            and like_delta >= threshold
        ):
            triggered_types.append("点赞")
        if (
            current_comment is not None
            and previous_comment is not None
            and current_comment > previous_comment
            and previous_comment >= min_previous
            and comment_delta >= threshold
        ):
            triggered_types.append("评论")
        if not triggered_types:
            continue

        growth_rate = 0.0
        if previous_comment and current_comment is not None and previous_comment > 0:
            growth_rate = round((comment_delta / previous_comment) * 100, 2)

        account_name = str(current.get("account") or "").strip()
        title = str(current.get("title_copy") or "").strip()
        profile_url = str(current.get("profile_url") or "").strip()
        note_url = str(current.get("note_url") or "").strip()
        fingerprint = str(current.get("fingerprint") or current.get("raw_fingerprint") or tracked_key).strip()

        row: Dict[str, Any] = {
            "预警键": f"{alert_date}|{fingerprint}",
            "预警日期": alert_date,
            "预警类型": f"{'+'.join(triggered_types)}预警",
            "账号ID": str(current.get("account_id") or "").strip(),
            "账号": account_name,
            "作品指纹": fingerprint,
            "标题文案": title,
            "当前点赞数": current_like or 0,
            "基准点赞数": previous_like or 0,
            "点赞增量": like_delta,
            "当前评论数": current_comment or 0,
            "基准评论数": previous_comment or 0,
            "评论增量": comment_delta,
            "评论增长率": growth_rate,
            "通知状态": "待发送",
        }
        if profile_url:
            row["主页链接"] = {"text": account_name or "账号主页", "link": profile_url}
        if note_url:
            row["作品链接"] = {"text": title or "作品详情", "link": note_url}
        alert_rows.append(row)

    return _sort_alert_rows(alert_rows)


def _sort_alert_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda item: (
            str(item.get("预警日期") or ""),
            int(item.get("评论增量") or 0),
            int(item.get("点赞增量") or 0),
            str(item.get("账号") or ""),
            str(item.get("标题文案") or ""),
        ),
        reverse=True,
    )


def _build_tracked_ranking_items_from_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for report in reports:
        profile = report.get("profile") or {}
        for work in report.get("works") or []:
            entry = _build_tracked_work_entry(report=report, profile=profile, work=work)
            items.append(_build_ranking_item_from_tracked_entry(entry=entry, snapshot_date=str(entry.get("snapshot_date") or "")))
    return items


def _build_tracked_work_entry(*, report: Dict[str, Any], profile: Dict[str, Any], work: Dict[str, Any]) -> Dict[str, Any]:
    account_id = str(profile.get("profile_user_id") or "").strip()
    fingerprint = build_work_fingerprint(
        profile_user_id=account_id,
        title=str(work.get("title_copy") or ""),
        cover_url=str(work.get("cover_url") or ""),
    )
    note_id = str(work.get("note_id") or "").strip()
    note_url = str(work.get("note_url") or "").strip()
    xsec_token = str(work.get("xsec_token") or "").strip()
    derived_note_id, derived_xsec_token = extract_note_reference_from_url(note_url)
    if not note_id and derived_note_id:
        note_id = derived_note_id
    if not xsec_token and derived_xsec_token:
        xsec_token = derived_xsec_token
    captured_at = str(report.get("captured_at") or "").strip() or datetime.now().astimezone().isoformat(timespec="seconds")
    snapshot_date = _extract_snapshot_date(captured_at)
    comment_count = _to_optional_int(work.get("comment_count"))
    comment_count_text = str(work.get("comment_count_text") or "").strip()
    like_count = _to_optional_int(work.get("like_count")) or 0
    like_count_text = str(work.get("like_count_text") or like_count).strip()
    return {
        "tracked_key": _build_tracked_key(note_id=note_id, fingerprint=fingerprint),
        "fingerprint": f"note:{note_id}" if note_id else fingerprint,
        "raw_fingerprint": fingerprint,
        "note_id": note_id,
        "note_url": note_url,
        "xsec_token": xsec_token,
        "account_id": account_id,
        "account": str(profile.get("nickname") or "").strip(),
        "profile_url": str(profile.get("profile_url") or "").strip(),
        "title_copy": str(work.get("title_copy") or "").strip(),
        "note_type": str(work.get("note_type") or "").strip(),
        "cover_url": str(work.get("cover_url") or "").strip(),
        "like_count": like_count,
        "like_count_text": like_count_text or str(like_count),
        "comment_count": comment_count,
        "comment_count_text": comment_count_text or (str(comment_count) if comment_count is not None else ""),
        "comment_count_basis": str(work.get("comment_count_basis") or "").strip(),
        "comment_count_is_lower_bound": bool(work.get("comment_count_is_lower_bound")),
        "recent_comments_summary": str(work.get("recent_comments_summary") or "").strip(),
        "captured_at": captured_at,
        "snapshot_date": snapshot_date,
        "first_seen_at": captured_at,
        "last_seen_at": captured_at,
        "last_refreshed_at": captured_at,
        "source": "top30",
    }


def _merge_tracked_work_entry(*, existing_entry: Optional[Dict[str, Any]], current_entry: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing_entry or {})
    merged.update(current_entry)
    existing_comment_count = _to_optional_int((existing_entry or {}).get("comment_count"))
    existing_comment_basis = str((existing_entry or {}).get("comment_count_basis") or "").strip()
    existing_is_lower_bound = bool((existing_entry or {}).get("comment_count_is_lower_bound"))
    current_comment_count = _to_optional_int(current_entry.get("comment_count"))
    current_comment_basis = str(current_entry.get("comment_count_basis") or "").strip()
    current_is_lower_bound = bool(current_entry.get("comment_count_is_lower_bound"))
    existing_has_exact_comment = existing_comment_count is not None and not existing_is_lower_bound and existing_comment_basis != "详情缺失"
    current_has_exact_comment = current_comment_count is not None and not current_is_lower_bound and current_comment_basis != "详情缺失"
    if existing_has_exact_comment and not current_has_exact_comment:
        merged["comment_count"] = existing_comment_count
        merged["comment_count_text"] = str((existing_entry or {}).get("comment_count_text") or existing_comment_count)
        merged["comment_count_basis"] = existing_comment_basis or "精确值"
        merged["comment_count_is_lower_bound"] = False
        if str((existing_entry or {}).get("recent_comments_summary") or "").strip():
            merged["recent_comments_summary"] = str((existing_entry or {}).get("recent_comments_summary") or "").strip()
    merged["tracked_key"] = _resolve_tracked_work_key(merged)
    merged["first_seen_at"] = str((existing_entry or {}).get("first_seen_at") or current_entry.get("captured_at") or "").strip()
    merged["last_seen_at"] = str(current_entry.get("captured_at") or "").strip()
    merged["last_refreshed_at"] = str(current_entry.get("captured_at") or "").strip()
    merged["source"] = "top30"
    return merged


def _refresh_tracked_work_entry(*, entry: Dict[str, Any], collector: Any, captured_at: str, snapshot_date: str, settings) -> Dict[str, Any]:
    refreshed = dict(entry or {})
    if collector is None:
        refreshed["captured_at"] = captured_at
        refreshed["snapshot_date"] = snapshot_date
        return refreshed
    note_id = str(refreshed.get("note_id") or "").strip()
    note_url = str(refreshed.get("note_url") or "").strip()
    xsec_token = str(refreshed.get("xsec_token") or "").strip()
    derived_note_id, derived_xsec_token = extract_note_reference_from_url(note_url)
    if not note_id and derived_note_id:
        note_id = derived_note_id
        refreshed["note_id"] = derived_note_id
    if not xsec_token and derived_xsec_token:
        xsec_token = derived_xsec_token
        refreshed["xsec_token"] = derived_xsec_token
    if note_id:
        try:
            snapshot = collector.collect_note_detail(
                note_id=note_id,
                note_url=note_url,
                xsec_token=xsec_token,
                xsec_source="pc_user",
            )
        except Exception:
            snapshot = None
        if snapshot is not None:
            if snapshot.note_id:
                refreshed["note_id"] = snapshot.note_id
            if snapshot.note_url:
                refreshed["note_url"] = snapshot.note_url
            if snapshot.like_count is not None:
                refreshed["like_count"] = int(snapshot.like_count)
                refreshed["like_count_text"] = str(snapshot.like_count)
            if snapshot.comment_count is not None:
                refreshed["comment_count"] = int(snapshot.comment_count)
                refreshed["comment_count_text"] = str(snapshot.comment_count)
                refreshed["comment_count_is_lower_bound"] = False
                refreshed["comment_count_basis"] = "精确值"
    if _to_optional_int(refreshed.get("comment_count")) is None:
        existing_comment_count = _to_optional_int(entry.get("comment_count"))
        existing_basis = str(entry.get("comment_count_basis") or "").strip()
        existing_is_lower_bound = bool(entry.get("comment_count_is_lower_bound"))
        if existing_comment_count is not None and not existing_is_lower_bound and existing_basis != "详情缺失":
            refreshed["comment_count"] = existing_comment_count
            refreshed["comment_count_text"] = str(entry.get("comment_count_text") or existing_comment_count)
            refreshed["comment_count_is_lower_bound"] = False
            refreshed["comment_count_basis"] = existing_basis or "精确值"
            if str(entry.get("recent_comments_summary") or "").strip():
                refreshed["recent_comments_summary"] = str(entry.get("recent_comments_summary") or "").strip()
        else:
            refreshed["comment_count"] = None
            refreshed["comment_count_text"] = ""
            refreshed["comment_count_is_lower_bound"] = False
            refreshed["comment_count_basis"] = "详情缺失"
    refreshed["tracked_key"] = _resolve_tracked_work_key(refreshed)
    refreshed["captured_at"] = captured_at
    refreshed["snapshot_date"] = snapshot_date
    refreshed["last_refreshed_at"] = captured_at
    refreshed["source"] = "tracked"
    return refreshed


def _build_tracked_ranking_items(
    *,
    tracked_items: List[Dict[str, Any]],
    snapshot_date: str,
    active_cutoff,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for entry in tracked_items:
        if not _tracked_work_is_active(entry=entry, active_cutoff=active_cutoff):
            continue
        items.append(_build_ranking_item_from_tracked_entry(entry=entry, snapshot_date=snapshot_date))
    return items


def _build_ranking_item_from_tracked_entry(*, entry: Dict[str, Any], snapshot_date: str) -> Dict[str, Any]:
    like_count = _to_optional_int(entry.get("like_count")) or 0
    comment_count = _to_optional_int(entry.get("comment_count"))
    return {
        "snapshot_date": snapshot_date or str(entry.get("snapshot_date") or ""),
        "captured_at": str(entry.get("captured_at") or ""),
        "account_id": str(entry.get("account_id") or ""),
        "account": str(entry.get("account") or ""),
        "profile_url": str(entry.get("profile_url") or ""),
        "fingerprint": str(entry.get("fingerprint") or ""),
        "baseline_fingerprint": str(entry.get("raw_fingerprint") or entry.get("fingerprint") or ""),
        "title_copy": str(entry.get("title_copy") or ""),
        "note_type": str(entry.get("note_type") or ""),
        "cover_url": str(entry.get("cover_url") or ""),
        "note_url": str(entry.get("note_url") or ""),
        "like_count": like_count,
        "comment_count": comment_count,
        "comment_count_is_lower_bound": bool(entry.get("comment_count_is_lower_bound")),
        "xsec_token": str(entry.get("xsec_token") or ""),
        "tracking_status": _build_tracking_status(entry=entry, snapshot_date=snapshot_date),
        "first_seen_date": _extract_snapshot_date(str(entry.get("first_seen_at") or "")) or str(entry.get("snapshot_date") or ""),
    }


def _find_matching_tracked_key(*, tracked_items: Dict[str, Dict[str, Any]], note_id: str, fingerprint: str) -> str:
    normalized_note_id = str(note_id or "").strip()
    normalized_fingerprint = str(fingerprint or "").strip()
    for key, entry in tracked_items.items():
        if normalized_note_id and str(entry.get("note_id") or "").strip() == normalized_note_id:
            return key
        if normalized_fingerprint and str(entry.get("raw_fingerprint") or entry.get("fingerprint") or "").strip() == normalized_fingerprint:
            return key
    return ""


def _tracked_work_is_active(*, entry: Dict[str, Any], active_cutoff) -> bool:
    snapshot_date = _parse_iso_date(str(entry.get("last_seen_at") or entry.get("captured_at") or ""))
    if snapshot_date is None:
        snapshot_date = _parse_iso_date(str(entry.get("snapshot_date") or ""))
    return snapshot_date is not None and snapshot_date >= active_cutoff


def _resolve_tracked_work_key(entry: Dict[str, Any]) -> str:
    return _build_tracked_key(
        note_id=str(entry.get("note_id") or "").strip(),
        fingerprint=str(entry.get("raw_fingerprint") or entry.get("fingerprint") or "").strip(),
    )


def _build_tracked_key(*, note_id: str, fingerprint: str) -> str:
    if str(note_id or "").strip():
        return f"note:{str(note_id).strip()}"
    return f"fp:{str(fingerprint or '').strip()}"


def _extract_snapshot_date(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 10:
        return text[:10]
    return ""


def _parse_iso_date(value: str):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if "T" in text:
            return datetime.fromisoformat(text).date()
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _to_optional_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace(",", "")
    if text.lstrip("-").isdigit():
        return int(text)
    return None


def _build_tracking_status(*, entry: Dict[str, Any], snapshot_date: str) -> str:
    first_seen_date = _extract_snapshot_date(str(entry.get("first_seen_at") or "")) or str(entry.get("snapshot_date") or "")
    current_date = str(snapshot_date or entry.get("snapshot_date") or "").strip()
    if first_seen_date and current_date and first_seen_date == current_date:
        return "新入池"
    return "连续追踪"
