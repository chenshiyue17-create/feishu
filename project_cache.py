from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from .local_stats_app.data_service import build_dashboard_payload_from_tables
from .profile_dashboard_to_feishu import (
    build_dashboard_calendar_fields,
    build_dashboard_portal_fields,
    build_single_work_ranking_fields,
    build_single_work_rankings,
)


DEFAULT_PROJECT_CACHE_DIR = "/Users/cc/Downloads/飞书缓存"
DEFAULT_DASHBOARD_CACHE_FILE = "dashboard_all.json"


def resolve_project_cache_dir(settings) -> Path:
    raw = str(getattr(settings, "project_cache_dir", "") or DEFAULT_PROJECT_CACHE_DIR).strip()
    return Path(raw).expanduser().resolve()


def load_cached_dashboard_payload(settings) -> Dict[str, Any]:
    path = resolve_project_cache_dir(settings) / DEFAULT_DASHBOARD_CACHE_FILE
    if not path.exists():
        return {}
    payload = _read_json(path, {})
    return payload if isinstance(payload, dict) else {}


def write_project_cache_bundle(*, reports: List[Dict[str, Any]], settings) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    cache_dir.mkdir(parents=True, exist_ok=True)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for report in reports:
        project_name = str(report.get("project") or "默认项目").strip() or "默认项目"
        grouped.setdefault(project_name, []).append(report)

    combined_calendar_rows = _merge_calendar_rows(
        _read_json(cache_dir / "calendar_rows_all.json", []),
        [build_dashboard_calendar_fields(report) for report in reports],
    )
    combined_ranking_rows = _build_ranking_rows(reports)
    combined_payload = build_dashboard_payload_from_tables(
        portal_rows=[build_dashboard_portal_fields(reports)] if reports else [],
        calendar_rows=combined_calendar_rows,
        ranking_rows=combined_ranking_rows,
        alert_rows=[],
    )

    _write_json(cache_dir / DEFAULT_DASHBOARD_CACHE_FILE, combined_payload)
    _write_json(cache_dir / "calendar_rows_all.json", combined_calendar_rows)
    _write_json(cache_dir / "ranking_rows_all.json", combined_ranking_rows)
    _write_csv(cache_dir / "全部项目-账号日历留底.csv", combined_calendar_rows)
    _write_csv(cache_dir / "全部项目-单条排行榜.csv", combined_ranking_rows)

    project_paths: Dict[str, str] = {}
    for project_name, project_reports in grouped.items():
        project_dir = cache_dir / _slugify_project_name(project_name)
        project_dir.mkdir(parents=True, exist_ok=True)
        calendar_rows = _merge_calendar_rows(
            _read_json(project_dir / "calendar_rows.json", []),
            [build_dashboard_calendar_fields(report) for report in project_reports],
        )
        ranking_rows = _build_ranking_rows(project_reports)
        payload = build_dashboard_payload_from_tables(
            portal_rows=[build_dashboard_portal_fields(project_reports)] if project_reports else [],
            calendar_rows=calendar_rows,
            ranking_rows=ranking_rows,
            alert_rows=[],
        )
        _write_json(project_dir / "dashboard.json", payload)
        _write_json(project_dir / "calendar_rows.json", calendar_rows)
        _write_json(project_dir / "ranking_rows.json", ranking_rows)
        _write_csv(project_dir / "账号日历留底.csv", calendar_rows)
        _write_csv(project_dir / "单条排行榜.csv", ranking_rows)
        project_paths[project_name] = str(project_dir)

    return {
        "cache_dir": str(cache_dir),
        "projects": project_paths,
        "combined_dashboard_path": str(cache_dir / DEFAULT_DASHBOARD_CACHE_FILE),
    }


def _build_ranking_rows(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    history_index: Dict[str, List[Any]] = {}
    for rank_type, ranked_items in build_single_work_rankings(reports=reports, history_index=history_index).items():
        for rank, item in enumerate(ranked_items, start=1):
            rows.append(build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank))
    return rows


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


def _slugify_project_name(project_name: str) -> str:
    text = str(project_name or "").strip()
    cleaned = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", text)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "默认项目"
