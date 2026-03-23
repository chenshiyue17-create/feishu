from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_PROJECT_STATUS_FILE = "xhs_feishu_monitor/output/project_sync_status.json"


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def resolve_project_status_path(urls_file: str = "", *, fallback: str = DEFAULT_PROJECT_STATUS_FILE) -> Path:
    if str(urls_file or "").strip():
        path = Path(urls_file).expanduser().resolve()
        suffix = path.suffix or ".txt"
        return path.with_name(f"{path.stem}{suffix}.project_status.json")
    return Path(fallback).expanduser().resolve()


def load_project_sync_statuses(urls_file: str = "") -> Dict[str, Dict[str, Any]]:
    path = resolve_project_status_path(urls_file)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    statuses: Dict[str, Dict[str, Any]] = {}
    for project, raw in payload.items():
        if not isinstance(raw, dict):
            continue
        statuses[str(project)] = {
            "project": str(raw.get("project") or project),
            "state": str(raw.get("state") or ""),
            "message": str(raw.get("message") or ""),
            "started_at": str(raw.get("started_at") or ""),
            "finished_at": str(raw.get("finished_at") or ""),
            "last_success_at": str(raw.get("last_success_at") or ""),
            "last_error": str(raw.get("last_error") or ""),
            "total_accounts": int(raw.get("total_accounts") or 0),
            "total_works": int(raw.get("total_works") or 0),
            "updated_at": str(raw.get("updated_at") or ""),
        }
    return statuses


def write_project_sync_statuses(urls_file: str, statuses: Dict[str, Dict[str, Any]]) -> Path:
    path = resolve_project_status_path(urls_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(statuses, ensure_ascii=False, indent=2)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)
    return path


def update_project_sync_status(
    *,
    urls_file: str,
    project: str,
    state: str,
    message: str = "",
    started_at: str = "",
    finished_at: str = "",
    total_accounts: int = 0,
    total_works: int = 0,
    last_error: str = "",
) -> Path:
    project_name = str(project or "").strip()
    if not project_name:
        return resolve_project_status_path(urls_file)
    statuses = load_project_sync_statuses(urls_file)
    current = dict(statuses.get(project_name) or {})
    current.update(
        {
            "project": project_name,
            "state": state,
            "message": message,
            "started_at": started_at or current.get("started_at", ""),
            "finished_at": finished_at or current.get("finished_at", ""),
            "updated_at": iso_now(),
            "total_accounts": max(0, int(total_accounts or 0)),
            "total_works": max(0, int(total_works or 0)),
            "last_error": last_error or current.get("last_error", ""),
        }
    )
    if state == "success":
        current["last_success_at"] = finished_at or current.get("updated_at", "")
        current["last_error"] = ""
    statuses[project_name] = current
    return write_project_sync_statuses(urls_file, statuses)


def attach_project_sync_statuses(
    projects: List[Dict[str, Any]],
    *,
    urls_file: str,
) -> List[Dict[str, Any]]:
    statuses = load_project_sync_statuses(urls_file)
    attached: List[Dict[str, Any]] = []
    for item in projects:
        project_name = str(item.get("name") or "").strip()
        enriched = dict(item)
        if project_name and project_name in statuses:
            enriched["sync_status"] = dict(statuses[project_name])
        else:
            enriched["sync_status"] = {}
        attached.append(enriched)
    return attached
