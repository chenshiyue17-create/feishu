from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


DEFAULT_FIELD_MAP = {
    "note_id": "笔记ID",
    "note_title": "标题",
    "note_url": "链接",
    "description": "正文摘要",
    "author_name": "作者",
    "author_id": "作者ID",
    "published_at": "发布时间",
    "captured_at": "抓取时间",
    "like_count": "点赞数",
    "collect_count": "收藏数",
    "comment_count": "评论数",
    "share_count": "分享数",
    "like_delta": "点赞增量",
    "collect_delta": "收藏增量",
    "comment_delta": "评论增量",
    "share_delta": "分享增量",
    "source_name": "监控名称",
    "tags": "标签",
    "remark": "备注",
    "snapshot_key": "快照键",
    "raw_json": "原始数据",
}


@dataclass
class Settings:
    xhs_cookie: str = ""
    xhs_fetch_mode: str = "requests"
    xhs_user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    xhs_timeout_seconds: int = 20
    xhs_retry_attempts: int = 3
    xhs_retry_delay_seconds: int = 2
    xhs_batch_concurrency: int = 4
    xhs_manual_sync_cooldown_minutes: int = 20
    xhs_fetch_work_comment_counts: bool = True
    xhs_chrome_cookie_profile: str = ""
    xhs_proxy_pool: List[str] = field(default_factory=list)
    xhs_proxy_cooldown_seconds: int = 300
    xhs_extra_headers: Dict[str, str] = field(default_factory=dict)
    playwright_browser_mode: str = "launch"
    playwright_channel: str = ""
    playwright_executable_path: str = ""
    playwright_user_data_dir: str = ""
    playwright_profile_directory: str = "Default"
    playwright_storage_state: str = ""
    playwright_headless: bool = True
    playwright_wait_ms: int = 4000
    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_bitable_app_token: str = ""
    feishu_table_id: str = ""
    feishu_sync_mode: str = "upsert"
    feishu_unique_field: str = DEFAULT_FIELD_MAP["note_id"]
    feishu_field_map: Dict[str, str] = field(default_factory=lambda: dict(DEFAULT_FIELD_MAP))
    include_raw_json: bool = False
    feishu_notify_webhook: str = ""
    feishu_notify_secret: str = ""
    comment_alert_growth_threshold_percent: float = 10.0
    comment_alert_min_previous_count: int = 0
    verify_tls: bool = True
    state_file: str = "xhs_feishu_monitor/.state.json"

    def validate_for_sync(self) -> None:
        missing = []
        if not self.feishu_app_id:
            missing.append("FEISHU_APP_ID")
        if not self.feishu_app_secret:
            missing.append("FEISHU_APP_SECRET")
        if not self.feishu_bitable_app_token:
            missing.append("FEISHU_BITABLE_APP_TOKEN")
        if not self.feishu_table_id:
            missing.append("FEISHU_TABLE_ID")
        if missing:
            raise ValueError("缺少飞书配置: " + ", ".join(missing))
        if self.feishu_sync_mode not in {"append", "upsert"}:
            raise ValueError("FEISHU_SYNC_MODE 只支持 append 或 upsert")


def load_settings(env_file: Optional[str] = None) -> Settings:
    env_path = Path(env_file).expanduser() if env_file else None
    env_values = _load_env_file(env_path) if env_path and env_path.exists() else {}
    base_dir = env_path.parent if env_path else Path.cwd()
    default_state_file = ".state.json" if env_path else "xhs_feishu_monitor/.state.json"

    field_map_path = _first_non_empty(
        os.getenv("FEISHU_FIELD_MAP_FILE"),
        env_values.get("FEISHU_FIELD_MAP_FILE"),
    )
    extra_headers_raw = _first_non_empty(
        os.getenv("XHS_EXTRA_HEADERS_JSON"),
        env_values.get("XHS_EXTRA_HEADERS_JSON"),
    )
    proxy_pool_raw = _first_non_empty(
        os.getenv("XHS_PROXY_POOL"),
        env_values.get("XHS_PROXY_POOL"),
    )
    proxy_pool_file = _first_non_empty(
        os.getenv("XHS_PROXY_POOL_FILE"),
        env_values.get("XHS_PROXY_POOL_FILE"),
    )

    settings = Settings(
        xhs_cookie=_env("XHS_COOKIE", env_values),
        xhs_fetch_mode=(_env("XHS_FETCH_MODE", env_values) or "requests").strip().lower(),
        xhs_user_agent=_env("XHS_USER_AGENT", env_values) or Settings.xhs_user_agent,
        xhs_timeout_seconds=_env_int("XHS_TIMEOUT_SECONDS", env_values, default=20),
        xhs_retry_attempts=_env_int("XHS_RETRY_ATTEMPTS", env_values, default=3),
        xhs_retry_delay_seconds=_env_int("XHS_RETRY_DELAY_SECONDS", env_values, default=2),
        xhs_batch_concurrency=_env_int("XHS_BATCH_CONCURRENCY", env_values, default=4),
        xhs_manual_sync_cooldown_minutes=_env_int("XHS_MANUAL_SYNC_COOLDOWN_MINUTES", env_values, default=20),
        xhs_fetch_work_comment_counts=_env_bool("XHS_FETCH_WORK_COMMENT_COUNTS", env_values, default=True),
        xhs_chrome_cookie_profile=_resolve_optional_path(_env("XHS_CHROME_COOKIE_PROFILE", env_values), base_dir),
        xhs_proxy_pool=_load_proxy_pool(proxy_pool_raw, proxy_pool_file, base_dir),
        xhs_proxy_cooldown_seconds=_env_int("XHS_PROXY_COOLDOWN_SECONDS", env_values, default=300),
        xhs_extra_headers=_load_json_object(extra_headers_raw, "XHS_EXTRA_HEADERS_JSON"),
        playwright_browser_mode=(_env("PLAYWRIGHT_BROWSER_MODE", env_values) or "launch").strip().lower(),
        playwright_channel=_env("PLAYWRIGHT_CHANNEL", env_values).strip(),
        playwright_executable_path=_resolve_optional_path(_env("PLAYWRIGHT_EXECUTABLE_PATH", env_values), base_dir),
        playwright_user_data_dir=_resolve_optional_path(_env("PLAYWRIGHT_USER_DATA_DIR", env_values), base_dir),
        playwright_profile_directory=_env("PLAYWRIGHT_PROFILE_DIRECTORY", env_values) or "Default",
        playwright_storage_state=_resolve_optional_path(_env("PLAYWRIGHT_STORAGE_STATE", env_values), base_dir),
        playwright_headless=_env_bool("PLAYWRIGHT_HEADLESS", env_values, default=True),
        playwright_wait_ms=_env_int("PLAYWRIGHT_WAIT_MS", env_values, default=4000),
        feishu_app_id=_env("FEISHU_APP_ID", env_values),
        feishu_app_secret=_env("FEISHU_APP_SECRET", env_values),
        feishu_bitable_app_token=_env("FEISHU_BITABLE_APP_TOKEN", env_values),
        feishu_table_id=_env("FEISHU_TABLE_ID", env_values),
        feishu_sync_mode=(_env("FEISHU_SYNC_MODE", env_values) or "upsert").strip().lower(),
        feishu_unique_field=_env("FEISHU_UNIQUE_FIELD", env_values) or DEFAULT_FIELD_MAP["note_id"],
        feishu_field_map=_load_field_map(field_map_path, base_dir),
        include_raw_json=_env_bool("FEISHU_INCLUDE_RAW_JSON", env_values, default=False),
        feishu_notify_webhook=_env("FEISHU_NOTIFY_WEBHOOK", env_values),
        feishu_notify_secret=_env("FEISHU_NOTIFY_SECRET", env_values),
        comment_alert_growth_threshold_percent=_env_float("COMMENT_ALERT_GROWTH_THRESHOLD_PERCENT", env_values, default=10.0),
        comment_alert_min_previous_count=_env_int("COMMENT_ALERT_MIN_PREVIOUS_COUNT", env_values, default=0),
        verify_tls=_env_bool("VERIFY_TLS", env_values, default=True),
        state_file=_resolve_path(_env("STATE_FILE", env_values) or default_state_file, base_dir),
    )
    return settings


def _load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cleaned = value.strip().strip("'").strip('"')
        values[key.strip()] = cleaned
    return values


def _load_field_map(path_text: Optional[str], base_dir: Path) -> Dict[str, str]:
    if not path_text:
        return dict(DEFAULT_FIELD_MAP)
    resolved = Path(_resolve_path(path_text, base_dir)).expanduser()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("FEISHU_FIELD_MAP_FILE 必须是 JSON 对象")
    field_map = dict(DEFAULT_FIELD_MAP)
    for key, value in payload.items():
        if value is None:
            field_map.pop(str(key), None)
            continue
        field_map[str(key)] = str(value)
    return field_map


def _load_json_object(raw: Optional[str], env_name: str) -> Dict[str, str]:
    if not raw:
        return {}
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"{env_name} 必须是 JSON 对象")
    return {str(key): str(value) for key, value in payload.items()}


def _load_proxy_pool(raw: Optional[str], path_text: Optional[str], base_dir: Path) -> List[str]:
    values: List[str] = []
    if raw:
        values.extend(_split_proxy_lines(raw))
    if path_text:
        resolved = Path(_resolve_path(path_text, base_dir)).expanduser()
        if resolved.exists():
            values.extend(_split_proxy_lines(resolved.read_text(encoding="utf-8")))
    deduped: List[str] = []
    seen = set()
    for value in values:
        normalized = _normalize_proxy_url(value)
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _split_proxy_lines(raw: str) -> List[str]:
    pieces: List[str] = []
    for line in raw.replace(",", "\n").splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        pieces.append(cleaned)
    return pieces


def _normalize_proxy_url(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    if "://" not in cleaned:
        return f"http://{cleaned}"
    return cleaned


def _resolve_path(path_text: str, base_dir: Path) -> str:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def _resolve_optional_path(path_text: str, base_dir: Path) -> str:
    if not path_text:
        return ""
    return _resolve_path(path_text, base_dir)


def _env(name: str, env_values: Dict[str, str]) -> str:
    return os.getenv(name) or env_values.get(name, "")


def _env_int(name: str, env_values: Dict[str, str], default: int) -> int:
    raw = _env(name, env_values)
    if not raw:
        return default
    return int(raw)


def _env_bool(name: str, env_values: Dict[str, str], default: bool) -> bool:
    raw = _env(name, env_values)
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, env_values: Dict[str, str], default: float) -> float:
    raw = _env(name, env_values)
    if not raw:
        return default
    return float(raw)


def _first_non_empty(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value:
            return value
    return None
