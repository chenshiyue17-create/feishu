from __future__ import annotations

import argparse
import gzip
import json
import urllib.request
from datetime import date
from pathlib import Path
from typing import List, Optional

from .config import load_settings
from .local_stats_app.monitored_accounts import extract_profile_user_id, load_monitored_metadata, parse_monitored_entries
from .profile_dashboard_to_feishu import build_single_work_ranking_fields, build_single_work_rankings
from .profile_works_to_feishu import build_work_calendar_history_index
from .project_cache import (
    load_cached_dashboard_payload,
    rebuild_dashboard_cache_from_project_dirs,
    repair_dashboard_cache_from_exports,
)

DEFAULT_ACCOUNT_RANKING_EXPORT_DIR = "/Users/cc/Downloads/飞书缓存/账号榜单导出"


def _load_json_if_exists(path: Path) -> dict | list | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _to_int(value: object) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _normalize_upload_dashboard_payload(payload: dict) -> dict:
    normalized = json.loads(json.dumps(payload or {}, ensure_ascii=False))
    account_series = {
        str(account_id or "").strip(): [dict(point or {}) for point in (points or [])]
        for account_id, points in (normalized.get("account_series") or {}).items()
        if str(account_id or "").strip()
    }
    for points in account_series.values():
        last_fans = 0
        last_interaction = 0
        for point in points:
            fans_value = _to_int(point.get("fans"))
            interaction_value = _to_int(point.get("interaction"))
            if fans_value > 0:
                last_fans = fans_value
            elif last_fans > 0:
                point["fans"] = last_fans
            if interaction_value > 0:
                last_interaction = interaction_value
            elif last_interaction > 0:
                point["interaction"] = last_interaction
    normalized["account_series"] = account_series
    accounts = []
    for item in normalized.get("accounts") or []:
        row = dict(item or {})
        account_id = str(row.get("account_id") or "").strip()
        series_points = account_series.get(account_id) or []
        latest_exact_fans = next((_to_int(point.get("fans")) for point in reversed(series_points) if _to_int(point.get("fans")) > 0), 0)
        latest_exact_interaction = next(
            (_to_int(point.get("interaction")) for point in reversed(series_points) if _to_int(point.get("interaction")) > 0),
            0,
        )
        if _to_int(row.get("fans")) <= 0 and latest_exact_fans > 0:
            row["fans"] = latest_exact_fans
        if _to_int(row.get("interaction")) <= 0 and latest_exact_interaction > 0:
            row["interaction"] = latest_exact_interaction
        accounts.append(row)
    normalized["accounts"] = accounts
    return normalized


def _build_snapshot_rank_rows(rows: list[dict], *, metric_label: str) -> list[dict]:
    normalized: list[dict] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        title = str(item.get("标题") or item.get("title") or "").strip()
        account = str(item.get("账号") or item.get("account") or "").strip()
        metric = _to_int(item.get("数值") or item.get("metric"))
        normalized.append(
            {
                "title": title or account or "未命名内容",
                "account": account,
                "account_id": str(item.get("账号ID") or item.get("account_id") or "").strip(),
                "metric": metric,
                "summary": str(item.get("摘要") or item.get("榜单摘要") or "").strip(),
                "note_url": str(item.get("作品链接") or item.get("note_url") or "").strip(),
                "profile_url": str(item.get("主页链接") or item.get("profile_url") or "").strip(),
                "metric_text": f"{metric_label} {metric}",
            }
        )
    normalized.sort(key=lambda item: (int(item.get("metric") or 0), str(item.get("title") or "")), reverse=True)
    for index, item in enumerate(normalized, start=1):
        item["rank"] = index
    return normalized[:20]


def _build_snapshot_growth_rows(compare_payload: dict) -> list[dict]:
    rows: list[dict] = []
    for item in compare_payload.get("changed_accounts") or []:
        if not isinstance(item, dict):
            continue
        like_delta = _to_int(item.get("like_delta"))
        comment_delta = _to_int(item.get("comment_delta"))
        metric = like_delta + comment_delta
        if metric <= 0:
            continue
        rows.append(
            {
                "title": str(item.get("account") or "未知账号").strip() or "未知账号",
                "account": str(item.get("account") or "").strip(),
                "account_id": str(item.get("account_id") or "").strip(),
                "metric": metric,
                "summary": f"点赞 +{like_delta} · 评论 +{comment_delta}",
                "metric_text": f"互动增长 {metric}",
                "like_delta": like_delta,
                "comment_delta": comment_delta,
            }
        )
    rows.sort(key=lambda item: (int(item.get("metric") or 0), str(item.get("title") or "")), reverse=True)
    for index, item in enumerate(rows, start=1):
        item["rank"] = index
    return rows[:20]


def _build_ranking_item_from_fields(fields: dict) -> dict:
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
        "profile_url": str((fields.get("主页链接") or {}).get("link") or fields.get("profile_url") or "").strip() if isinstance(fields.get("主页链接"), dict) else str(fields.get("profile_url") or "").strip(),
        "note_url": str((fields.get("作品链接") or {}).get("link") or fields.get("note_url") or "").strip() if isinstance(fields.get("作品链接"), dict) else str(fields.get("note_url") or "").strip(),
        "cover_url": str((fields.get("封面图") or {}).get("link") or fields.get("cover_url") or "").strip() if isinstance(fields.get("封面图"), dict) else str(fields.get("cover_url") or "").strip(),
        "tracking_status": str(fields.get("追踪状态") or "").strip(),
        "first_seen_date": str(fields.get("首次入池日期") or "").strip(),
    }


def _load_cache_history_rankings(cache_root: Path, account_ids: set[str]) -> dict:
    history_payload: dict[str, dict[str, dict]] = {}
    for project_dir in sorted(path for path in cache_root.iterdir() if path.is_dir()):
        if project_dir.name == "账号榜单导出":
            continue
        tracked_payload = _load_json_if_exists(project_dir / "tracked_works.json")
        history_rows = _load_json_if_exists(project_dir / "tracked_work_history.json")
        if not isinstance(tracked_payload, dict) or not isinstance(history_rows, list):
            continue
        tracked_items = tracked_payload.get("items") or []
        if not isinstance(tracked_items, list) or not tracked_items:
            continue
        metadata_by_fingerprint: dict[str, dict] = {}
        for item in tracked_items:
            if not isinstance(item, dict):
                continue
            account_id = str(item.get("account_id") or "").strip()
            if account_ids and account_id not in account_ids:
                continue
            for fingerprint_key in (
                str(item.get("fingerprint") or "").strip(),
                str(item.get("raw_fingerprint") or "").strip(),
                str(item.get("tracked_key") or "").removeprefix("fp:").strip(),
            ):
                if fingerprint_key:
                    metadata_by_fingerprint[fingerprint_key] = dict(item)
        if not metadata_by_fingerprint:
            continue

        history_index = build_work_calendar_history_index([dict(item) for item in history_rows if isinstance(item, dict)])
        items_by_date: dict[str, list[dict]] = {}
        for row in history_rows:
            if not isinstance(row, dict):
                continue
            fields = row.get("fields") or {}
            fingerprint = str(fields.get("作品指纹") or "").strip()
            snapshot_date = str(fields.get("日期文本") or "").strip()
            if not fingerprint or not snapshot_date:
                continue
            metadata = metadata_by_fingerprint.get(fingerprint)
            if not metadata:
                continue
            comment_count = fields.get("评论数")
            items_by_date.setdefault(snapshot_date, []).append(
                {
                    "snapshot_date": snapshot_date,
                    "captured_at": f"{snapshot_date}T23:59:59+08:00",
                    "account_id": str(metadata.get("account_id") or "").strip(),
                    "account": str(metadata.get("account") or "").strip(),
                    "profile_url": str(metadata.get("profile_url") or "").strip(),
                    "fingerprint": fingerprint,
                    "title_copy": str(metadata.get("title_copy") or "").strip(),
                    "note_type": str(metadata.get("note_type") or "").strip(),
                    "cover_url": str(metadata.get("cover_url") or "").strip(),
                    "note_url": str(metadata.get("note_url") or "").strip(),
                    "like_count": _to_int(fields.get("点赞数")),
                    "comment_count": int(comment_count) if isinstance(comment_count, (int, float)) else None,
                    "comment_count_is_lower_bound": False,
                    "comment_count_basis": "精确值" if isinstance(comment_count, (int, float)) else "",
                    "tracking_status": str(metadata.get("tracking_status") or metadata.get("source") or "").strip(),
                    "first_seen_date": str(metadata.get("first_seen_at") or "").split("T", 1)[0].strip(),
                }
            )

        project_history: dict[str, dict] = {}
        for snapshot_date in sorted(items_by_date.keys()):
            date_items = items_by_date[snapshot_date]
            ranking_groups = build_single_work_rankings(reports=[], items=date_items, history_index=history_index)

            def build_rows(rank_type: str) -> list[dict]:
                rows: list[dict] = []
                for rank, item in enumerate(ranking_groups.get(rank_type, []), start=1):
                    rows.append(_build_ranking_item_from_fields(build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank)))
                return rows

            project_history[snapshot_date] = {
                "date": snapshot_date,
                "snapshot_time": f"{snapshot_date} 23:59:59",
                "snapshot_slug": snapshot_date,
                "account_count": len({str(item.get('account_id') or '').strip() for item in date_items if str(item.get('account_id') or '').strip()}),
                "likes": build_rows("单条点赞排行"),
                "comments": build_rows("单条评论排行"),
                "growth": build_rows("单条第二天增长排行"),
            }
        if project_history:
            history_payload[project_dir.name] = project_history
    return history_payload


def load_project_snapshot_history(export_root: str = DEFAULT_ACCOUNT_RANKING_EXPORT_DIR) -> dict:
    root = Path(str(export_root or DEFAULT_ACCOUNT_RANKING_EXPORT_DIR)).expanduser().resolve()
    if not root.exists():
        return {}

    history_payload: dict[str, dict[str, dict]] = {}
    for project_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        project_name = project_dir.name
        project_history: dict[str, dict] = {}
        snapshot_dirs = sorted(
            [path for path in project_dir.iterdir() if path.is_dir() and path.name[:4].isdigit()],
            key=lambda path: path.name,
            reverse=True,
        )
        for snapshot_dir in snapshot_dirs:
            summary = _load_json_if_exists(snapshot_dir / "项目导出摘要.json")
            if not isinstance(summary, dict):
                continue
            snapshot_time = str(summary.get("snapshot_time") or "").strip()
            snapshot_date = snapshot_time.split(" ")[0] if snapshot_time else snapshot_dir.name.split("_")[0]
            if not snapshot_date or snapshot_date in project_history:
                continue

            like_rows: list[dict] = []
            comment_rows: list[dict] = []
            for account_summary in summary.get("accounts") or []:
                if not isinstance(account_summary, dict):
                    continue
                files = account_summary.get("files") or {}
                account_like_rows = _load_json_if_exists(Path(str(files.get("like_json") or "")))
                account_comment_rows = _load_json_if_exists(Path(str(files.get("comment_json") or "")))
                if isinstance(account_like_rows, list):
                    like_rows.extend(account_like_rows)
                if isinstance(account_comment_rows, list):
                    comment_rows.extend(account_comment_rows)

            compare_payload = summary.get("compare")
            if not isinstance(compare_payload, dict):
                compare_path = Path(str((summary.get("files") or {}).get("project_compare_json") or ""))
                compare_loaded = _load_json_if_exists(compare_path)
                compare_payload = compare_loaded if isinstance(compare_loaded, dict) else {}

            project_history[snapshot_date] = {
                "date": snapshot_date,
                "snapshot_time": snapshot_time,
                "snapshot_slug": str(summary.get("snapshot_slug") or snapshot_dir.name),
                "account_count": _to_int(summary.get("account_count")),
                "likes": _build_snapshot_rank_rows(like_rows, metric_label="点赞"),
                "comments": _build_snapshot_rank_rows(comment_rows, metric_label="评论"),
                "growth": _build_snapshot_growth_rows(compare_payload),
            }

        if project_history:
            history_payload[project_name] = project_history
    return history_payload


def _load_dashboard_payload(env_file: str, urls_file: str) -> dict:
    settings = load_settings(env_file)
    payload = load_cached_dashboard_payload(settings)
    if payload:
        return _normalize_upload_dashboard_payload(payload)
    rebuilt = rebuild_dashboard_cache_from_project_dirs(settings)
    if rebuilt:
        return _normalize_upload_dashboard_payload(rebuilt)
    repaired = repair_dashboard_cache_from_exports(settings=settings, monitored_metadata=load_monitored_metadata(urls_file))
    if repaired:
        return _normalize_upload_dashboard_payload(repaired)
    raise ValueError("本地暂无可上传的缓存，请先完成一次采集")


def _filter_history_rankings(history_payload: dict, account_ids: set[str]) -> dict:
    if not isinstance(history_payload, dict) or not account_ids:
        return history_payload if isinstance(history_payload, dict) else {}
    filtered: dict = {}
    for project_name, project_history in history_payload.items():
        if not isinstance(project_history, dict):
            continue
        date_rows: dict = {}
        for date_text, snapshot in project_history.items():
            if not isinstance(snapshot, dict):
                continue
            date_rows[date_text] = {
                **snapshot,
                "likes": [dict(item) for item in (snapshot.get("likes") or []) if str(item.get("account_id") or "").strip() in account_ids],
                "comments": [dict(item) for item in (snapshot.get("comments") or []) if str(item.get("account_id") or "").strip() in account_ids],
                "growth": [dict(item) for item in (snapshot.get("growth") or []) if str(item.get("account_id") or "").strip() in account_ids],
            }
        filtered[project_name] = date_rows
    return filtered


def _filter_dashboard_payload_by_monitored_entries(dashboard_payload: dict, monitored_entries: list[dict]) -> dict:
    if not isinstance(dashboard_payload, dict):
        return {}
    normalized_entries = [dict(item or {}) for item in (monitored_entries or []) if isinstance(item, dict)]
    allowed_account_ids = {
        str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
        for item in normalized_entries
        if str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip()
    }
    if not allowed_account_ids:
        return dict(dashboard_payload)

    filtered = dict(dashboard_payload)
    filtered["accounts"] = [
        dict(item)
        for item in (dashboard_payload.get("accounts") or [])
        if str(item.get("account_id") or "").strip() in allowed_account_ids
    ]
    filtered["account_series"] = {
        str(account_id): [dict(point) for point in points]
        for account_id, points in (dashboard_payload.get("account_series") or {}).items()
        if str(account_id or "").strip() in allowed_account_ids
    }
    filtered["rankings"] = {
        str(rank_type): [
            dict(item)
            for item in (rows or [])
            if str(item.get("account_id") or "").strip() in allowed_account_ids
        ]
        for rank_type, rows in (dashboard_payload.get("rankings") or {}).items()
    }
    filtered["alerts"] = [
        dict(item)
        for item in (dashboard_payload.get("alerts") or [])
        if str(item.get("account_id") or "").strip() in allowed_account_ids
    ]
    return filtered


def _build_upload_payload(*, env_file: str, urls_file: str, account_ids: Optional[List[str]] = None) -> dict:
    dashboard_payload = dict(_load_dashboard_payload(env_file, urls_file))
    settings = load_settings(env_file)
    monitored_entries = parse_monitored_entries(urls_file)
    monitored_metadata = load_monitored_metadata(urls_file)
    dashboard_payload = _filter_dashboard_payload_by_monitored_entries(dashboard_payload, monitored_entries)
    normalized_account_ids = {
        str(item or "").strip()
        for item in (account_ids or [])
        if str(item or "").strip()
    }
    if normalized_account_ids:
        dashboard_payload["accounts"] = [
            dict(item)
            for item in (dashboard_payload.get("accounts") or [])
            if str(item.get("account_id") or "").strip() in normalized_account_ids
        ]
        dashboard_payload["account_series"] = {
            account_id: [dict(point) for point in points]
            for account_id, points in (dashboard_payload.get("account_series") or {}).items()
            if str(account_id or "").strip() in normalized_account_ids
        }
        dashboard_payload["rankings"] = {
            rank_type: [
                dict(item)
                for item in (rows or [])
                if str(item.get("account_id") or "").strip() in normalized_account_ids
            ]
            for rank_type, rows in (dashboard_payload.get("rankings") or {}).items()
        }
        dashboard_payload["alerts"] = [
            dict(item)
            for item in (dashboard_payload.get("alerts") or [])
            if str(item.get("account_id") or "").strip() in normalized_account_ids
        ]
        monitored_entries = [
            dict(item)
            for item in monitored_entries
            if str(item.get("account_id") or extract_profile_user_id(str(item.get("url") or "")) or "").strip() in normalized_account_ids
        ]
        monitored_metadata = {
            url: dict(meta)
            for url, meta in (monitored_metadata or {}).items()
            if str((meta or {}).get("account_id") or extract_profile_user_id(url) or "").strip() in normalized_account_ids
        }
    history_payload = _load_cache_history_rankings(Path(str(settings.project_cache_dir)).expanduser().resolve(), normalized_account_ids)
    if not history_payload:
        history_payload = load_project_snapshot_history()
    dashboard_payload["history_rankings"] = _filter_history_rankings(history_payload, normalized_account_ids)
    return {
        "dashboard_payload": dashboard_payload,
        "monitored_entries": monitored_entries,
        "monitored_metadata": monitored_metadata,
        "merge_mode": "partial" if normalized_account_ids else "replace",
        "account_ids": sorted(normalized_account_ids),
    }


def push_local_cache_to_server(*, env_file: str, urls_file: str, server_url: str, token: str = "", account_ids: Optional[List[str]] = None) -> dict:
    payload = _build_upload_payload(env_file=env_file, urls_file=urls_file, account_ids=account_ids)

    request_body = json.dumps(
        payload,
        ensure_ascii=False,
    ).encode("utf-8")
    compressed_body = gzip.compress(request_body)
    server_url = str(server_url or "").rstrip("/")
    request = urllib.request.Request(
        f"{server_url}/api/server-cache-upload",
        data=compressed_body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Content-Encoding": "gzip",
            **({"X-Upload-Token": str(token or "").strip()} if str(token or "").strip() else {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.loads(response.read().decode("utf-8"))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把本地缓存上传到服务器，供网页和手机端查看。")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--urls-file", default="xhs_feishu_monitor/input/robam_multi_profile_urls.txt")
    parser.add_argument("--server-url", required=True, help="服务器基础地址，例如 http://47.87.68.74")
    parser.add_argument("--token", default="", help="可选上传令牌，对应 SERVER_CACHE_UPLOAD_TOKEN")
    parser.add_argument("--account-id", action="append", default=[], help="仅上传指定账号，可重复传多个")
    args = parser.parse_args(argv)

    payload = push_local_cache_to_server(
        env_file=args.env_file,
        urls_file=args.urls_file,
        server_url=args.server_url,
        token=args.token,
        account_ids=list(args.account_id or []),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
