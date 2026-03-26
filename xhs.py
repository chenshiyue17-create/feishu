from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from .chrome_cookies import export_xiaohongshu_cookie_header, resolve_chrome_profile_directory
from .config import Settings
from .models import NoteSnapshot, Target
from .xhs_signed import XHSSignedSession, extract_profile_posted_page_payload


NEXT_DATA_PATTERN = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
JSON_LD_PATTERN = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
TITLE_PATTERN = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_PATTERN = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?P<name>[^"\']+)["\'][^>]+content=["\'](?P<content>[^"\']*)["\'][^>]*>',
    re.IGNORECASE,
)
NOTE_ID_FROM_URL_PATTERN = re.compile(r"(?:explore|discovery/item)/([a-zA-Z0-9_-]{6,})")
ASSIGNMENT_MARKERS = (
    "window.__INITIAL_STATE__",
    "window.__INITIAL_SSR_STATE__",
    "window.__INITIAL_DATA__",
    "window.__REDUX_STATE__",
    "__INITIAL_STATE__",
    "__INITIAL_SSR_STATE__",
)

NOTE_ID_KEYS = ("noteid", "note_id", "noteidstr", "itemid", "item_id", "id")
TITLE_KEYS = ("title", "notetitle", "displaytitle", "sharetitle", "seo_title")
DESCRIPTION_KEYS = ("desc", "description", "content", "notedesc", "notecontent")
AUTHOR_NAME_KEYS = ("nickname", "nick_name", "username", "author_name", "screen_name", "name")
AUTHOR_ID_KEYS = ("userid", "user_id", "authorid", "author_id", "uid", "ownerid")
LIKE_KEYS = ("likecount", "likedcount", "likes", "diggcount", "upcount")
COLLECT_KEYS = ("collectcount", "favoritecount", "favcount", "collectedcount")
COMMENT_KEYS = ("commentcount", "commentscount", "commentnum", "replycount")
SHARE_KEYS = ("sharecount", "shares", "forwardcount", "share_num")
PUBLISHED_KEYS = ("publishtime", "publish_time", "time", "updatetime", "createtime", "createdat", "pubtime")
URL_KEYS = ("url", "sharelink", "noteurl", "jumpurl")
PROFILE_URL_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?xiaohongshu\.com/user/profile/[0-9a-z]+", re.IGNORECASE)
PROFILE_POSTED_API_FRAGMENT = "/api/sns/web/v1/user_posted"
PROXY_STATUS_LOCK = threading.Lock()
PROXY_RUNTIME_STATE: Dict[str, Dict[str, Any]] = {}
PROXY_RUNTIME_META: Dict[str, Any] = {
    "last_selected_proxy": "",
    "last_error": "",
    "updated_at": "",
}
PUBLIC_IP_STATUS: Dict[str, Any] = {
    "ip": "",
    "checked_at": "",
    "error": "",
    "cached_at_monotonic": 0.0,
}
PUBLIC_IP_CACHE_SECONDS = 600
PUBLIC_IP_LOOKUP_URLS = (
    "https://api64.ipify.org",
    "https://api.ipify.org",
)


class XHSCollector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self._chrome_cookie_header: Optional[str] = None
        self._proxy_index = 0
        self._proxy_cooldowns: Dict[str, float] = {}
        self._signed_session: Optional[XHSSignedSession] = None

    def collect(self, target: Target) -> NoteSnapshot:
        errors: List[str] = []
        for mode in self._resolve_fetch_modes(target):
            try:
                payload, final_url = self._load_payload(target, mode)
                snapshot = _normalize_snapshot(payload, target, final_url)
                snapshot.source_name = target.name or snapshot.source_name
                snapshot.tags = list(dict.fromkeys([*snapshot.tags, *target.tags]))
                snapshot.remark = target.remark or snapshot.remark
                snapshot.captured_at = datetime.now().astimezone().isoformat(timespec="seconds")
                if not _has_metrics(snapshot):
                    raise ValueError("未提取到互动数据")
                return snapshot
            except Exception as exc:
                errors.append(f"{mode}: {exc}")
        raise ValueError(f"{target.display_name} 抓取失败: " + " | ".join(errors))

    def _load_payload(self, target: Target, mode: str) -> Tuple[Any, str]:
        if target.json_file:
            path = Path(target.json_file)
            return json.loads(path.read_text(encoding="utf-8")), target.url or ""
        if target.html_file:
            path = Path(target.html_file)
            return path.read_text(encoding="utf-8"), target.url or ""
        if not target.url:
            raise ValueError(f"{target.display_name} 缺少 url/html_file/json_file")
        if mode in {"playwright", "local_browser"}:
            return self._load_payload_via_playwright(target)
        headers = {
            "User-Agent": self.settings.xhs_user_agent,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://www.xiaohongshu.com/",
        }
        cookie_header = self._resolve_cookie_header()
        if cookie_header:
            headers["Cookie"] = cookie_header
        headers.update(self.settings.xhs_extra_headers)
        proxy_url = self._pick_proxy_url()
        try:
            response = self.session.get(
                target.url,
                headers=headers,
                timeout=self.settings.xhs_timeout_seconds,
                allow_redirects=True,
                verify=self.settings.verify_tls,
                proxies=_build_requests_proxies(proxy_url),
            )
        except requests.RequestException as exc:
            self._mark_proxy_failed(proxy_url, error_text=str(exc))
            raise
        self._mark_proxy_success(proxy_url)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return response.json(), response.url
        return response.text, response.url

    def _load_payload_via_playwright(self, target: Target) -> Tuple[Any, str]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "未安装 playwright。先执行 `python3 -m pip install playwright` 和 `playwright install chromium`"
            ) from exc

        storage_state = self.settings.playwright_storage_state or None
        proxy_url = self._pick_proxy_url()
        with sync_playwright() as playwright:
            context = None
            try:
                context, cleanup = self._open_playwright_context(
                    playwright=playwright,
                    storage_state=storage_state,
                    proxy_url=proxy_url,
                )
                cookies = parse_cookie_header(self.settings.xhs_cookie, target.url or "")
                if cookies:
                    context.add_cookies(cookies)
                page = context.new_page()
                profile_pages: List[Dict[str, Any]] = []
                if _is_profile_url(target.url or ""):
                    page.on("response", lambda response: _collect_profile_posted_page(response=response, bucket=profile_pages))
                page.goto(target.url or "", wait_until="domcontentloaded", timeout=self.settings.xhs_timeout_seconds * 1000)
                page.wait_for_timeout(self.settings.playwright_wait_ms)
                if _is_profile_url(target.url or ""):
                    _scroll_profile_page_until_stable(page=page, settings=self.settings)
                    runtime_state = _extract_runtime_initial_state(page)
                    final_payload: Any
                    if isinstance(runtime_state, dict) and runtime_state:
                        final_payload = _merge_profile_runtime_pages(runtime_state, profile_pages)
                    else:
                        html_text = page.content()
                        final_payload = _merge_profile_runtime_pages(extract_initial_state(html_text), profile_pages)
                else:
                    final_payload = page.content()
                final_url = page.url
                page.close()
                cleanup()
                self._mark_proxy_success(proxy_url)
                return final_payload, final_url
            except Exception as exc:
                self._mark_proxy_failed(proxy_url, error_text=str(exc))
                if context is not None:
                    try:
                        context.close()
                    except Exception:
                        pass
                raise

    def _open_playwright_context(self, *, playwright, storage_state: Optional[str], proxy_url: str = ""):
        browser_mode = (self.settings.playwright_browser_mode or "launch").strip().lower()
        if browser_mode not in {"launch", "local_profile"}:
            raise ValueError("PLAYWRIGHT_BROWSER_MODE 只支持 launch 或 local_profile")
        if browser_mode == "local_profile":
            context_kwargs: Dict[str, Any] = {
                "user_data_dir": resolve_local_browser_user_data_dir(self.settings),
                "headless": self.settings.playwright_headless,
                "channel": self.settings.playwright_channel or "chrome",
                "executable_path": self.settings.playwright_executable_path or None,
                "args": _build_local_browser_args(self.settings),
                "user_agent": self.settings.xhs_user_agent,
                "extra_http_headers": self.settings.xhs_extra_headers,
                "viewport": {"width": 1440, "height": 960},
            }
            if proxy_url:
                context_kwargs["proxy"] = {"server": proxy_url}
            context = playwright.chromium.launch_persistent_context(
                **context_kwargs,
            )
            return context, context.close

        launch_kwargs: Dict[str, Any] = {
            "headless": self.settings.playwright_headless,
            "channel": self.settings.playwright_channel or None,
            "executable_path": self.settings.playwright_executable_path or None,
        }
        if proxy_url:
            launch_kwargs["proxy"] = {"server": proxy_url}
        browser = playwright.chromium.launch(**launch_kwargs)
        context_kwargs: Dict[str, Any] = {
            "user_agent": self.settings.xhs_user_agent,
            "extra_http_headers": self.settings.xhs_extra_headers,
        }
        if storage_state:
            context_kwargs["storage_state"] = storage_state
        context = browser.new_context(**context_kwargs)
        return context, browser.close

    def _resolve_fetch_modes(self, target: Target) -> List[str]:
        if target.json_file or target.html_file:
            return ["file"]
        fetch_mode = (self.settings.xhs_fetch_mode or "requests").strip().lower()
        if fetch_mode not in {"requests", "playwright", "local_browser", "auto"}:
            raise ValueError("XHS_FETCH_MODE 只支持 requests、playwright、local_browser、auto")
        if fetch_mode == "auto":
            return ["requests", "playwright"]
        return [fetch_mode]

    def _resolve_cookie_header(self) -> str:
        if self.settings.xhs_cookie:
            return self.settings.xhs_cookie
        if self.settings.xhs_chrome_cookie_profile:
            if not self._chrome_cookie_header:
                self._chrome_cookie_header = export_xiaohongshu_cookie_header(
                    self.settings.xhs_chrome_cookie_profile,
                    resolve_chrome_profile_directory(self.settings.playwright_profile_directory),
                )
            return self._chrome_cookie_header
        return ""

    def _pick_proxy_url(self) -> str:
        pool = list(self.settings.xhs_proxy_pool or [])
        if not pool:
            return ""
        now = time.monotonic()
        available = [proxy for proxy in pool if self._proxy_cooldowns.get(proxy, 0.0) <= now]
        candidates = available or pool
        selected = candidates[self._proxy_index % len(candidates)]
        self._proxy_index += 1
        _record_proxy_selected(selected)
        return selected

    def _mark_proxy_failed(self, proxy_url: str, error_text: str = "") -> None:
        if not proxy_url:
            return
        cooldown = max(0, int(self.settings.xhs_proxy_cooldown_seconds or 0))
        if cooldown <= 0:
            self._proxy_cooldowns.pop(proxy_url, None)
            _record_proxy_failed(proxy_url, error_text=error_text, cooldown_until=0.0)
            return
        cooldown_until = time.monotonic() + cooldown
        self._proxy_cooldowns[proxy_url] = cooldown_until
        _record_proxy_failed(proxy_url, error_text=error_text, cooldown_until=cooldown_until)

    def _mark_proxy_success(self, proxy_url: str) -> None:
        if not proxy_url:
            return
        self._proxy_cooldowns.pop(proxy_url, None)
        _record_proxy_success(proxy_url)

    def fetch_profile_posted_pages(self, *, profile_url: str, initial_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self._get_signed_session().fetch_profile_posted_pages(
            profile_url=profile_url,
            initial_state=initial_state,
        )

    def collect_note_detail(
        self,
        *,
        note_id: str,
        note_url: str = "",
        xsec_token: str = "",
        xsec_source: str = "pc_user",
    ) -> Optional[NoteSnapshot]:
        note_card = self._get_signed_session().fetch_note_detail(
            note_id=note_id,
            note_url=note_url,
            xsec_token=xsec_token,
            xsec_source=xsec_source,
        )
        if not isinstance(note_card, dict) or not note_card:
            return None
        snapshot = _snapshot_from_node(note_card, note_url, note_id)
        if note_id and not snapshot.note_id:
            snapshot.note_id = note_id
        if note_url and not snapshot.note_url:
            snapshot.note_url = note_url
        return snapshot

    def fetch_note_comments_preview(
        self,
        *,
        note_id: str,
        xsec_token: str,
        note_url: str = "",
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        return self._get_signed_session().fetch_note_comments_preview(
            note_id=note_id,
            xsec_token=xsec_token,
            note_url=note_url,
            limit=limit,
        )

    def _get_signed_session(self) -> XHSSignedSession:
        if self._signed_session is None:
            self._signed_session = XHSSignedSession(
                settings=self.settings,
                http_session=self.session,
                resolve_cookie_header=self._resolve_cookie_header,
                build_requests_proxies=_build_requests_proxies,
                pick_proxy_url=self._pick_proxy_url,
                mark_proxy_failed=self._mark_proxy_failed,
                mark_proxy_success=self._mark_proxy_success,
            )
        return self._signed_session


def _normalize_snapshot(payload: Any, target: Target, final_url: str) -> NoteSnapshot:
    if isinstance(payload, dict) and _looks_normalized(payload):
        snapshot = NoteSnapshot.from_normalized_dict(payload)
        if final_url and not snapshot.note_url:
            snapshot.note_url = final_url
        return snapshot

    if isinstance(payload, str):
        snapshot = _normalize_from_html(payload, target, final_url)
        if snapshot:
            return snapshot
        if payload.lstrip().startswith("{"):
            return _normalize_snapshot(json.loads(payload), target, final_url)
        raise ValueError(f"{target.display_name} HTML 中未找到可解析的数据")

    if isinstance(payload, dict):
        snapshot = _normalize_from_json(payload, target, final_url)
        if snapshot:
            return snapshot
        raise ValueError(f"{target.display_name} JSON 中未找到可识别的笔记结构")

    if isinstance(payload, list):
        snapshot = _find_best_snapshot([payload], target, final_url)
        if snapshot:
            return snapshot
        raise ValueError(f"{target.display_name} JSON 数组中未找到可识别的笔记结构")

    raise ValueError(f"{target.display_name} 数据类型不支持: {type(payload).__name__}")


def _normalize_from_html(html_text: str, target: Target, final_url: str) -> Optional[NoteSnapshot]:
    candidates: List[Any] = []
    for match in NEXT_DATA_PATTERN.finditer(html_text):
        script_payload = match.group(1).strip()
        if script_payload:
            parsed = _parse_json_or_js_object(script_payload)
            if parsed is None:
                continue
            candidates.append(parsed)
    for match in JSON_LD_PATTERN.finditer(html_text):
        script_payload = match.group(1).strip()
        if script_payload:
            try:
                candidates.append(json.loads(script_payload))
            except json.JSONDecodeError:
                continue
    for marker in ASSIGNMENT_MARKERS:
        start = 0
        while True:
            marker_index = html_text.find(marker, start)
            if marker_index < 0:
                break
            json_text = _extract_assigned_json(html_text, marker_index + len(marker))
            start = marker_index + len(marker)
            if not json_text:
                continue
            parsed = _parse_json_or_js_object(json_text)
            if parsed is None:
                continue
            candidates.append(parsed)

    best_snapshot = _find_best_snapshot(candidates, target, final_url)
    if best_snapshot:
        return best_snapshot

    title = _extract_meta(html_text, "og:title") or _extract_meta(html_text, "title")
    description = _extract_meta(html_text, "description")
    if not title and not description:
        title_match = TITLE_PATTERN.search(html_text)
        title = _collapse_whitespace(title_match.group(1)) if title_match else ""
    if title or description:
        return NoteSnapshot(
            note_id=_extract_note_id_from_url(final_url or target.url or ""),
            note_title=title or "",
            note_url=final_url or target.url or "",
            description=description or "",
            source_name=target.name,
            raw_payload={"fallback": "meta_only"},
        )
    return None


def _normalize_from_json(payload: Dict[str, Any], target: Target, final_url: str) -> Optional[NoteSnapshot]:
    return _find_best_snapshot([payload], target, final_url)


def _find_best_snapshot(objects: Iterable[Any], target: Target, final_url: str) -> Optional[NoteSnapshot]:
    best_score = -1
    best_snapshot: Optional[NoteSnapshot] = None
    fallback_note_id = _extract_note_id_from_url(final_url or target.url or "")

    for root in objects:
        for node in _iter_dict_nodes(root):
            snapshot = _snapshot_from_node(node, final_url or target.url or "", fallback_note_id)
            score = _score_snapshot(snapshot, node)
            if score > best_score:
                best_score = score
                best_snapshot = snapshot

    if best_snapshot and best_score >= 4:
        if target.name and not best_snapshot.source_name:
            best_snapshot.source_name = target.name
        return best_snapshot
    return None


def _snapshot_from_node(node: Dict[str, Any], note_url: str, fallback_note_id: str) -> NoteSnapshot:
    note_id = _extract_note_id(node) or fallback_note_id
    snapshot = NoteSnapshot(
        note_id=note_id,
        note_title=_collapse_whitespace(_stringify(_deep_find_first(node, TITLE_KEYS))),
        note_url=_stringify(_deep_find_first(node, URL_KEYS)) or note_url,
        description=_collapse_whitespace(_stringify(_deep_find_first(node, DESCRIPTION_KEYS))),
        author_name=_collapse_whitespace(_stringify(_deep_find_first(node, AUTHOR_NAME_KEYS))),
        author_id=_stringify(_deep_find_first(node, AUTHOR_ID_KEYS)),
        published_at=_normalize_timestamp(_deep_find_first(node, PUBLISHED_KEYS)),
        like_count=_coerce_count(_deep_find_first(node, LIKE_KEYS)),
        collect_count=_coerce_count(_deep_find_first(node, COLLECT_KEYS)),
        comment_count=_coerce_count(_deep_find_first(node, COMMENT_KEYS)),
        share_count=_coerce_count(_deep_find_first(node, SHARE_KEYS)),
        raw_payload=node,
    )
    if not snapshot.note_url:
        snapshot.note_url = note_url
    return snapshot


def _score_snapshot(snapshot: NoteSnapshot, node: Dict[str, Any]) -> int:
    score = 0
    if snapshot.note_id:
        score += 3
    if snapshot.note_title:
        score += 2
    if snapshot.description:
        score += 1
    if snapshot.author_name:
        score += 1
    if snapshot.like_count is not None:
        score += 2
    if snapshot.collect_count is not None:
        score += 1
    if snapshot.comment_count is not None:
        score += 1
    if snapshot.share_count is not None:
        score += 1
    if snapshot.published_at:
        score += 1
    node_keys = {str(key).lower() for key in node.keys()}
    if any(key in node_keys for key in ("interactinfo", "stat", "stats", "engagement", "note")):
        score += 1
    if not snapshot.note_title and not snapshot.description:
        score -= 2
    return score


def _deep_find_first(payload: Any, aliases: Iterable[str]) -> Any:
    alias_set = {alias.lower() for alias in aliases}
    queue: List[Any] = [payload]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for key, value in current.items():
                key_text = str(key).lower()
                if key_text in alias_set:
                    return value
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            queue.extend(item for item in current if isinstance(item, (dict, list)))
    return None


def _extract_note_id(payload: Dict[str, Any]) -> str:
    candidate = _deep_find_first(payload, NOTE_ID_KEYS)
    text = _stringify(candidate)
    if _looks_like_note_id(text):
        return text
    return ""


def _iter_dict_nodes(payload: Any) -> Iterator[Dict[str, Any]]:
    queue: List[Any] = [payload]
    visited: set[int] = set()
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            if id(current) in visited:
                continue
            visited.add(id(current))
            yield current
            for value in current.values():
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            queue.extend(item for item in current if isinstance(item, (dict, list)))


def _extract_assigned_json(text: str, start_index: int) -> str:
    equals_index = text.find("=", start_index)
    if equals_index < 0:
        return ""
    brace_index = text.find("{", equals_index)
    if brace_index < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for index in range(brace_index, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[brace_index : index + 1]
    return ""


def _normalize_timestamp(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10**12:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().isoformat(timespec="seconds")
    text = str(value).strip()
    if text.isdigit():
        return _normalize_timestamp(int(text))
    iso_candidate = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate).astimezone().isoformat(timespec="seconds")
    except ValueError:
        return text


def _coerce_count(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1
    if text.endswith("万"):
        multiplier = 10000
        text = text[:-1]
    elif text.lower().endswith("k"):
        multiplier = 1000
        text = text[:-1]
    elif text.lower().endswith("m"):
        multiplier = 1000000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def parse_cookie_header(cookie_header: str, target_url: str) -> List[Dict[str, Any]]:
    header = (cookie_header or "").strip()
    if not header:
        return []
    parsed = urlparse(target_url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "https://www.xiaohongshu.com"
    cookie = SimpleCookie()
    cookie.load(header)
    cookies: List[Dict[str, Any]] = []
    for key, morsel in cookie.items():
        cookies.append(
            {
                "name": key,
                "value": morsel.value,
                "url": origin,
            }
        )
    return cookies


def _build_requests_proxies(proxy_url: str) -> Optional[Dict[str, str]]:
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def build_proxy_pool_status(settings: Settings) -> Dict[str, Any]:
    pool = list(settings.xhs_proxy_pool or [])
    now = time.monotonic()
    entries: List[Dict[str, Any]] = []
    latest_error = ""
    public_ip_status = _get_public_ip_status(now=now)
    with PROXY_STATUS_LOCK:
        for proxy_url in pool:
            runtime = dict(PROXY_RUNTIME_STATE.get(proxy_url) or {})
            cooldown_until = float(runtime.get("cooldown_until_monotonic") or 0.0)
            cooling = cooldown_until > now
            cooldown_remaining = max(0, int(round(cooldown_until - now))) if cooling else 0
            last_error = str(runtime.get("last_error") or "")
            if not latest_error and last_error:
                latest_error = last_error
            entries.append(
                {
                    "proxy_url": proxy_url,
                    "state": "cooling" if cooling else "ready",
                    "cooldown_seconds_remaining": cooldown_remaining,
                    "last_used_at": str(runtime.get("last_used_at") or ""),
                    "last_success_at": str(runtime.get("last_success_at") or ""),
                    "last_failure_at": str(runtime.get("last_failure_at") or ""),
                    "last_error": last_error,
                    "failure_count": int(runtime.get("failure_count") or 0),
                    "consecutive_failures": int(runtime.get("consecutive_failures") or 0),
                }
            )
        last_selected_proxy = str(PROXY_RUNTIME_META.get("last_selected_proxy") or "")
        updated_at = str(PROXY_RUNTIME_META.get("updated_at") or "")
        meta_last_error = str(PROXY_RUNTIME_META.get("last_error") or "")
    if not latest_error:
        latest_error = meta_last_error
    ready_count = sum(1 for item in entries if item["state"] == "ready")
    cooling_count = len(entries) - ready_count
    entries.sort(key=lambda item: (0 if item["state"] == "ready" else 1, item["proxy_url"]))
    return {
        "enabled": bool(pool),
        "total": len(entries),
        "ready_count": ready_count,
        "cooling_count": cooling_count,
        "last_selected_proxy": last_selected_proxy,
        "last_error": latest_error,
        "updated_at": updated_at,
        "current_ip": str(public_ip_status.get("ip") or ""),
        "current_ip_checked_at": str(public_ip_status.get("checked_at") or ""),
        "current_ip_error": str(public_ip_status.get("error") or ""),
        "entries": entries,
    }


def _get_public_ip_status(*, now: Optional[float] = None) -> Dict[str, Any]:
    current_now = float(now if now is not None else time.monotonic())
    with PROXY_STATUS_LOCK:
        cached_at = float(PUBLIC_IP_STATUS.get("cached_at_monotonic") or 0.0)
        if cached_at and (current_now - cached_at) < PUBLIC_IP_CACHE_SECONDS:
            return dict(PUBLIC_IP_STATUS)
    ip_value = ""
    error_text = ""
    for lookup_url in PUBLIC_IP_LOOKUP_URLS:
        try:
            response = requests.get(lookup_url, timeout=3)
            response.raise_for_status()
            ip_value = str(response.text or "").strip()
            if ip_value:
                error_text = ""
                break
        except requests.RequestException as exc:
            error_text = str(exc)
    status = {
        "ip": ip_value,
        "checked_at": _iso_now(),
        "error": "" if ip_value else error_text,
        "cached_at_monotonic": current_now,
    }
    with PROXY_STATUS_LOCK:
        PUBLIC_IP_STATUS.update(status)
        return dict(PUBLIC_IP_STATUS)


def _record_proxy_selected(proxy_url: str) -> None:
    if not proxy_url:
        return
    with PROXY_STATUS_LOCK:
        state = PROXY_RUNTIME_STATE.setdefault(proxy_url, {})
        state["last_used_at"] = _iso_now()
        PROXY_RUNTIME_META["last_selected_proxy"] = proxy_url
        PROXY_RUNTIME_META["updated_at"] = _iso_now()


def _record_proxy_success(proxy_url: str) -> None:
    if not proxy_url:
        return
    with PROXY_STATUS_LOCK:
        state = PROXY_RUNTIME_STATE.setdefault(proxy_url, {})
        state["last_used_at"] = _iso_now()
        state["last_success_at"] = _iso_now()
        state["last_error"] = ""
        state["consecutive_failures"] = 0
        state["cooldown_until_monotonic"] = 0.0
        PROXY_RUNTIME_META["updated_at"] = _iso_now()


def _record_proxy_failed(proxy_url: str, *, error_text: str, cooldown_until: float) -> None:
    if not proxy_url:
        return
    with PROXY_STATUS_LOCK:
        state = PROXY_RUNTIME_STATE.setdefault(proxy_url, {})
        state["last_used_at"] = _iso_now()
        state["last_failure_at"] = _iso_now()
        state["last_error"] = str(error_text or "")
        state["failure_count"] = int(state.get("failure_count") or 0) + 1
        state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
        state["cooldown_until_monotonic"] = float(cooldown_until or 0.0)
        PROXY_RUNTIME_META["last_error"] = str(error_text or "")
        PROXY_RUNTIME_META["updated_at"] = _iso_now()


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def resolve_local_browser_user_data_dir(settings: Settings) -> str:
    if settings.playwright_user_data_dir:
        return settings.playwright_user_data_dir
    return str((Path(__file__).resolve().parent / ".local_chrome_profile").resolve())


def _build_local_browser_args(settings: Settings) -> List[str]:
    profile_directory = str(settings.playwright_profile_directory or "").strip()
    if not profile_directory:
        return []
    return [f"--profile-directory={profile_directory}"]


def _is_profile_url(url: str) -> bool:
    return bool(PROFILE_URL_PATTERN.search(str(url or "").strip()))


def _collect_profile_posted_page(*, response, bucket: List[Dict[str, Any]]) -> None:
    if PROFILE_POSTED_API_FRAGMENT not in str(response.url or ""):
        return
    if response.status != 200:
        return
    try:
        payload = response.json()
    except Exception:
        return
    page_payload = _extract_profile_posted_page_payload(payload)
    if page_payload is not None:
        bucket.append(page_payload)


def _extract_runtime_initial_state(page) -> Dict[str, Any]:
    try:
        payload = page.evaluate(
            """
() => {
  const unwrap = (value) => {
    if (value && typeof value === 'object' && '_value' in value) {
      return unwrap(value._value);
    }
    if (Array.isArray(value)) {
      return value.map((item) => unwrap(item));
    }
    if (value && typeof value === 'object') {
      const out = {};
      for (const [key, item] of Object.entries(value)) {
        out[key] = unwrap(item);
      }
      return out;
    }
    return value;
  };
  return unwrap(window.__INITIAL_STATE__ || {});
}
"""
        )
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _scroll_profile_page_until_stable(*, page, settings: Settings) -> None:
    wait_ms = max(1200, min(int(settings.playwright_wait_ms or 4000), 2500))
    stagnant_rounds = 0
    previous_count = -1
    previous_height = -1
    for _ in range(8):
        runtime_state = _extract_runtime_initial_state(page)
        current_count = _count_profile_runtime_notes(runtime_state)
        has_more = _profile_runtime_has_more(runtime_state)
        current_height = 0
        try:
            current_height = int(page.evaluate("() => document.body ? document.body.scrollHeight : 0"))
            page.evaluate("() => window.scrollTo(0, document.body ? document.body.scrollHeight : 0)")
        except Exception:
            break
        page.wait_for_timeout(wait_ms)
        runtime_state = _extract_runtime_initial_state(page)
        next_count = _count_profile_runtime_notes(runtime_state)
        next_height = 0
        try:
            next_height = int(page.evaluate("() => document.body ? document.body.scrollHeight : 0"))
        except Exception:
            next_height = current_height
        if not _profile_runtime_has_more(runtime_state):
            break
        if next_count <= max(previous_count, current_count) and next_height <= max(previous_height, current_height):
            stagnant_rounds += 1
            if stagnant_rounds >= 2:
                break
        else:
            stagnant_rounds = 0
        previous_count = next_count
        previous_height = next_height
        if not has_more and not _profile_runtime_has_more(runtime_state):
            break


def _count_profile_runtime_notes(initial_state: Dict[str, Any]) -> int:
    user = initial_state.get("user") or {}
    notes = user.get("notes") or []
    count = 0
    for bucket in notes:
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if _looks_like_profile_note_item(item):
                count += 1
    return count


def _profile_runtime_has_more(initial_state: Dict[str, Any]) -> bool:
    user = initial_state.get("user") or {}
    note_queries = user.get("noteQueries") or []
    for item in note_queries:
        if not isinstance(item, dict):
            continue
        if bool(item.get("hasMore")):
            return True
    return False


def _extract_profile_posted_page_payload(payload: Any) -> Optional[Dict[str, Any]]:
    for node in _iter_dict_nodes(payload):
        items = _extract_profile_posted_items(node)
        if not items:
            continue
        return {
            "items": items,
            "cursor": _stringify(
                node.get("cursor")
                or node.get("nextCursor")
                or node.get("next_cursor")
            ),
            "user_id": _stringify(
                node.get("userId")
                or node.get("userid")
                or node.get("user_id")
            ),
            "page": _coerce_count(node.get("page") or node.get("pageNum") or node.get("page_num")),
            "num": _coerce_count(node.get("num") or node.get("pageSize") or node.get("page_size")),
            "has_more": _coerce_profile_has_more(node),
        }
    return None


def _extract_profile_posted_items(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [
        node.get("notes"),
        node.get("noteList"),
        node.get("items"),
        node.get("list"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, list):
            continue
        items = [item for item in candidate if _looks_like_profile_note_item(item)]
        if items:
            return items
    return []


def _coerce_profile_has_more(node: Dict[str, Any]) -> Optional[bool]:
    if "hasMore" in node:
        return bool(node.get("hasMore"))
    if "has_more" in node:
        return bool(node.get("has_more"))
    return None


def _looks_like_profile_note_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    note_card = item.get("noteCard")
    if isinstance(note_card, dict):
        return bool(
            note_card.get("noteId")
            or note_card.get("displayTitle")
            or note_card.get("title")
            or note_card.get("user")
        )
    return bool(item.get("id") or item.get("noteId") or item.get("note_id"))


def _profile_note_identity(item: Dict[str, Any]) -> str:
    note_card = item.get("noteCard") or {}
    return _stringify(
        note_card.get("noteId")
        or item.get("id")
        or item.get("noteId")
        or item.get("note_id")
        or item.get("xsecToken")
    )


def _merge_profile_runtime_pages(initial_state: Dict[str, Any], profile_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(initial_state, dict):
        return {}
    if not profile_pages:
        return initial_state
    user = initial_state.setdefault("user", {})
    notes = user.get("notes")
    if not isinstance(notes, list):
        notes = []
        user["notes"] = notes
    if not notes or not isinstance(notes[0], list):
        notes.insert(0, [])
    first_bucket = notes[0]
    existing_keys = {
        key
        for key in (_profile_note_identity(item) for item in first_bucket if isinstance(item, dict))
        if key
    }
    for page_payload in profile_pages:
        for item in page_payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            note_key = _profile_note_identity(item)
            if note_key and note_key in existing_keys:
                continue
            first_bucket.append(item)
            if note_key:
                existing_keys.add(note_key)

    note_queries = user.get("noteQueries")
    if not isinstance(note_queries, list):
        note_queries = []
        user["noteQueries"] = note_queries
    if not note_queries or not isinstance(note_queries[0], dict):
        note_queries.insert(0, {})
    latest = profile_pages[-1]
    query = note_queries[0]
    if latest.get("cursor"):
        query["cursor"] = latest["cursor"]
    if latest.get("user_id"):
        query["userId"] = latest["user_id"]
    if latest.get("page") is not None:
        query["page"] = latest["page"]
    if latest.get("num") is not None:
        query["num"] = latest["num"]
    if latest.get("has_more") is not None:
        query["hasMore"] = latest["has_more"]
    return initial_state


def _has_metrics(snapshot: NoteSnapshot) -> bool:
    return any(
        value is not None
        for value in (
            snapshot.like_count,
            snapshot.collect_count,
            snapshot.comment_count,
            snapshot.share_count,
        )
    )


def extract_initial_state(html_text: str) -> Dict[str, Any]:
    for marker in ASSIGNMENT_MARKERS:
        start = 0
        while True:
            marker_index = html_text.find(marker, start)
            if marker_index < 0:
                break
            object_text = _extract_assigned_json(html_text, marker_index + len(marker))
            start = marker_index + len(marker)
            if not object_text:
                continue
            parsed = _parse_json_or_js_object(object_text)
            if isinstance(parsed, dict):
                return parsed
    raise ValueError("HTML 中未找到可解析的 __INITIAL_STATE__ 数据")


def _parse_json_or_js_object(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return _parse_js_object_with_node(text)


def _parse_js_object_with_node(text: str) -> Optional[Any]:
    script = """
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(0, 'utf8');
const payload = vm.runInNewContext('(' + source + ')', {});
process.stdout.write(JSON.stringify(payload));
"""
    try:
        result = subprocess.run(
            ["node", "-e", script],
            input=text,
            text=True,
            capture_output=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def _extract_meta(html_text: str, name: str) -> str:
    lowered = name.lower()
    for match in META_PATTERN.finditer(html_text):
        meta_name = match.group("name").strip().lower()
        if meta_name == lowered:
            return _collapse_whitespace(match.group("content"))
    return ""


def _extract_note_id_from_url(url: str) -> str:
    if not url:
        return ""
    match = NOTE_ID_FROM_URL_PATTERN.search(url)
    if match:
        return match.group(1)
    return ""


def _looks_normalized(payload: Dict[str, Any]) -> bool:
    keys = {str(key) for key in payload.keys()}
    return "note_id" in keys or "note_title" in keys or "like_count" in keys


def _looks_like_note_id(value: str) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"[a-zA-Z0-9_-]{6,}", value))


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()
