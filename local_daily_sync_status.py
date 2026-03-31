from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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


def resolve_local_daily_sync_status_path(*, env_file: str, state_file_path: str = "") -> Path:
    env_dir = Path(env_file).expanduser().resolve().parent
    if state_file_path:
        raw_state_path = Path(state_file_path).expanduser()
        state_path = raw_state_path if raw_state_path.is_absolute() else (env_dir / raw_state_path)
        return state_path.resolve().parent / ".local_daily_sync_status.json"
    return env_dir / ".local_daily_sync_status.json"


def build_default_local_daily_sync_status() -> Dict[str, Any]:
    return {
        "state": "idle",
        "phase": "idle",
        "message": "",
        "started_at": "",
        "finished_at": "",
        "last_success_at": "",
        "last_error": "",
        "next_run_at": "",
        "project_count": 0,
        "successful_projects": 0,
        "failed_projects": 0,
        "current_project": "",
        "current_project_index": 0,
        "current_project_total": 0,
        "current_project_scheduled_at": "",
        "waiting_for_login": False,
        "upload_state": "",
        "upload_message": "",
        "last_upload_success_at": "",
        "last_upload_error": "",
        "updated_at": "",
    }


def _contains_legacy_feishu_error(*, message: str = "", error: str = "") -> bool:
    combined_text = f"{message} {error}".lower()
    return any(marker in combined_text for marker in LEGACY_FEISHU_ERROR_MARKERS)


def load_local_daily_sync_status(*, env_file: str, state_file_path: str = "") -> Dict[str, Any]:
    path = resolve_local_daily_sync_status_path(env_file=env_file, state_file_path=state_file_path)
    if not path.exists():
        return build_default_local_daily_sync_status()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return build_default_local_daily_sync_status()
    normalized = {
        **build_default_local_daily_sync_status(),
        **(payload if isinstance(payload, dict) else {}),
    }
    if _contains_legacy_feishu_error(
        message=str(normalized.get("message") or ""),
        error=str(normalized.get("last_error") or ""),
    ):
        normalized["last_error"] = ""
        if str(normalized.get("state") or "") == "error":
            normalized["state"] = "idle"
            normalized["phase"] = "idle"
            normalized["message"] = ""
    if _contains_legacy_feishu_error(
        message=str(normalized.get("upload_message") or ""),
        error=str(normalized.get("last_upload_error") or ""),
    ):
        normalized["last_upload_error"] = ""
        if str(normalized.get("upload_state") or "") == "error":
            normalized["upload_state"] = ""
            normalized["upload_message"] = ""
    return normalized


def write_local_daily_sync_status(
    *,
    env_file: str,
    state_file_path: str = "",
    payload: Dict[str, Any],
) -> str:
    path = resolve_local_daily_sync_status_path(env_file=env_file, state_file_path=state_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_payload = {
        **build_default_local_daily_sync_status(),
        **(payload or {}),
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(normalized_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
