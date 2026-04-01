from __future__ import annotations

import argparse
import gzip
import json
import urllib.request
from pathlib import Path
from typing import List, Optional

from .config import load_settings
from .local_stats_app.monitored_accounts import extract_profile_user_id, load_monitored_metadata, parse_monitored_entries
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
        return payload
    rebuilt = rebuild_dashboard_cache_from_project_dirs(settings)
    if rebuilt:
        return rebuilt
    repaired = repair_dashboard_cache_from_exports(settings=settings, monitored_metadata=load_monitored_metadata(urls_file))
    if repaired:
        return repaired
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
