from __future__ import annotations

import re
from http.cookies import SimpleCookie
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests

from .config import Settings


PROFILE_URL_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?xiaohongshu\.com/user/profile/([0-9a-z]+)", re.IGNORECASE)
PROFILE_POSTED_API_FRAGMENT = "/api/sns/web/v1/user_posted"
NOTE_FEED_API_FRAGMENT = "/api/sns/web/v1/feed"
NOTE_COMMENT_PAGE_API_FRAGMENT = "/api/sns/web/v2/comment/page"
API_HOST = "https://edith.xiaohongshu.com"
AUTHOR_ID_KEYS = ("userid", "user_id", "authorid", "author_id", "uid", "ownerid")


class XHSSignedSession:
    def __init__(
        self,
        *,
        settings: Settings,
        http_session: requests.Session,
        resolve_cookie_header,
        build_requests_proxies,
        pick_proxy_url,
        mark_proxy_failed,
        mark_proxy_success,
    ) -> None:
        self.settings = settings
        self.http_session = http_session
        self.resolve_cookie_header = resolve_cookie_header
        self.build_requests_proxies = build_requests_proxies
        self.pick_proxy_url = pick_proxy_url
        self.mark_proxy_failed = mark_proxy_failed
        self.mark_proxy_success = mark_proxy_success
        self._xhshow_client: Any = None
        self._session_managers: Dict[str, Any] = {}

    def fetch_profile_posted_pages(self, *, profile_url: str, initial_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not getattr(self.settings, "xhs_enable_signed_profile_pages", True):
            return []
        cookie_header = self.resolve_cookie_header()
        cookie_dict = cookie_dict_from_header(cookie_header)
        if not cookie_dict.get("a1"):
            return []
        user_id = extract_profile_user_id(initial_state, profile_url)
        if not user_id:
            return []
        client = self._load_xhshow_client()
        if client is None:
            return []

        xsec_token, xsec_source = extract_profile_security_params(profile_url)
        if not xsec_source:
            xsec_source = "pc_user"

        max_pages = max(1, int(getattr(self.settings, "xhs_signed_profile_max_pages", 40) or 40))
        cursor = ""
        seen_cursors: set[str] = set()
        pages: List[Dict[str, Any]] = []

        for _ in range(max_pages):
            params: Dict[str, Any] = {
                "num": 30,
                "cursor": cursor,
                "user_id": user_id,
                "xsec_source": xsec_source,
            }
            if xsec_token:
                params["xsec_token"] = xsec_token
            page_payload = self._request_signed_get(
                path=PROFILE_POSTED_API_FRAGMENT,
                params=params,
                cookie_header=cookie_header,
                cookie_dict=cookie_dict,
                referer=profile_url,
                session_key=str(cookie_dict.get("a1") or user_id),
            )
            normalized = extract_profile_posted_page_payload(page_payload)
            if normalized is None:
                break
            pages.append(normalized)
            next_cursor = str(normalized.get("cursor") or "").strip()
            has_more = bool(normalized.get("has_more"))
            if not has_more or not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return pages

    def fetch_note_detail(
        self,
        *,
        note_id: str,
        note_url: str = "",
        xsec_token: str = "",
        xsec_source: str = "pc_user",
    ) -> Optional[Dict[str, Any]]:
        note_id = str(note_id or "").strip()
        if not note_id:
            return None
        cookie_header = self.resolve_cookie_header()
        cookie_dict = cookie_dict_from_header(cookie_header)
        if not cookie_dict.get("a1"):
            return None
        payload: Dict[str, Any] = {
            "source_note_id": note_id,
            "image_formats": ["jpg", "webp", "avif"],
            "extra": {"need_body_topic": 1},
            "xsec_source": str(xsec_source or "pc_user").strip() or "pc_user",
        }
        if xsec_token:
            payload["xsec_token"] = xsec_token
        response_payload = self._request_signed_post(
            path=NOTE_FEED_API_FRAGMENT,
            payload=payload,
            cookie_header=cookie_header,
            cookie_dict=cookie_dict,
            referer=note_url,
            session_key=str(cookie_dict.get("a1") or note_id),
        )
        return extract_feed_note_card(response_payload)

    def fetch_note_comments_preview(
        self,
        *,
        note_id: str,
        xsec_token: str,
        note_url: str = "",
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        note_id = str(note_id or "").strip()
        xsec_token = str(xsec_token or "").strip()
        limit = max(0, int(limit or 0))
        if not note_id or not xsec_token or limit <= 0:
            return []
        cookie_header = self.resolve_cookie_header()
        cookie_dict = cookie_dict_from_header(cookie_header)
        if not cookie_dict.get("a1"):
            return []
        params: Dict[str, Any] = {
            "note_id": note_id,
            "cursor": "",
            "top_comment_id": "",
            "image_formats": "jpg,webp,avif",
            "xsec_token": xsec_token,
        }
        response_payload = self._request_signed_get(
            path=NOTE_COMMENT_PAGE_API_FRAGMENT,
            params=params,
            cookie_header=cookie_header,
            cookie_dict=cookie_dict,
            referer=note_url,
            session_key=str(cookie_dict.get("a1") or note_id),
        )
        comments = extract_comment_items(response_payload)
        previews = [normalize_comment_preview(item) for item in comments]
        return [item for item in previews if item][:limit]

    def _request_signed_get(
        self,
        *,
        path: str,
        params: Dict[str, Any],
        cookie_header: str,
        cookie_dict: Dict[str, str],
        referer: str,
        session_key: str,
    ) -> Any:
        client = self._load_xhshow_client()
        if client is None:
            raise RuntimeError("xhshow 未安装")
        session_manager = self._session_managers.get(session_key)
        if session_manager is None:
            session_manager = client.SessionManager()
            self._session_managers[session_key] = session_manager
        signed_headers = client.sign_headers_get(
            uri=path,
            cookies=cookie_dict,
            params=params,
            session=session_manager,
        )
        headers = self._build_base_headers(cookie_header=cookie_header, referer=referer, signed_headers=signed_headers)
        proxy_url = self.pick_proxy_url()
        try:
            response = self.http_session.get(
                f"{API_HOST}{path}",
                headers=headers,
                params=params,
                timeout=self.settings.xhs_timeout_seconds,
                verify=self.settings.verify_tls,
                proxies=self.build_requests_proxies(proxy_url),
            )
        except requests.RequestException as exc:
            self.mark_proxy_failed(proxy_url, error_text=str(exc))
            raise
        if response.status_code >= 400:
            self.mark_proxy_failed(proxy_url, error_text=f"HTTP {response.status_code}")
            response.raise_for_status()
        self.mark_proxy_success(proxy_url)
        return unwrap_xhs_api_payload(response.json())

    def _request_signed_post(
        self,
        *,
        path: str,
        payload: Dict[str, Any],
        cookie_header: str,
        cookie_dict: Dict[str, str],
        referer: str,
        session_key: str,
    ) -> Any:
        client = self._load_xhshow_client()
        if client is None:
            raise RuntimeError("xhshow 未安装")
        session_manager = self._session_managers.get(session_key)
        if session_manager is None:
            session_manager = client.SessionManager()
            self._session_managers[session_key] = session_manager
        signed_headers = client.sign_headers_post(
            uri=path,
            cookies=cookie_dict,
            payload=payload,
            session=session_manager,
        )
        headers = self._build_base_headers(cookie_header=cookie_header, referer=referer, signed_headers=signed_headers)
        headers["Content-Type"] = "application/json;charset=UTF-8"
        proxy_url = self.pick_proxy_url()
        try:
            response = self.http_session.post(
                f"{API_HOST}{path}",
                headers=headers,
                json=payload,
                timeout=self.settings.xhs_timeout_seconds,
                verify=self.settings.verify_tls,
                proxies=self.build_requests_proxies(proxy_url),
            )
        except requests.RequestException as exc:
            self.mark_proxy_failed(proxy_url, error_text=str(exc))
            raise
        if response.status_code >= 400:
            self.mark_proxy_failed(proxy_url, error_text=f"HTTP {response.status_code}")
            response.raise_for_status()
        self.mark_proxy_success(proxy_url)
        return unwrap_xhs_api_payload(response.json())

    def _build_base_headers(self, *, cookie_header: str, referer: str, signed_headers: Dict[str, str]) -> Dict[str, str]:
        headers = {
            "User-Agent": self.settings.xhs_user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Origin": "https://www.xiaohongshu.com",
            "Referer": referer or "https://www.xiaohongshu.com/",
            "Cookie": cookie_header,
            **signed_headers,
        }
        headers.update(self.settings.xhs_extra_headers)
        return headers

    def _load_xhshow_client(self) -> Any:
        if self._xhshow_client is not None:
            return self._xhshow_client
        try:
            from xhshow import SessionManager, Xhshow
        except ImportError:
            return None

        class _ClientBundle:
            def __init__(self) -> None:
                self._client = Xhshow()
                self.SessionManager = SessionManager

            def sign_headers_get(self, **kwargs):
                return self._client.sign_headers_get(**kwargs)

            def sign_headers_post(self, **kwargs):
                return self._client.sign_headers_post(**kwargs)

        self._xhshow_client = _ClientBundle()
        return self._xhshow_client


def cookie_dict_from_header(cookie_header: str) -> Dict[str, str]:
    header = str(cookie_header or "").strip()
    if not header:
        return {}
    cookie = SimpleCookie()
    cookie.load(header)
    return {key: morsel.value for key, morsel in cookie.items()}


def extract_profile_security_params(profile_url: str) -> Tuple[str, str]:
    parsed = urlparse(str(profile_url or "").strip())
    query = parse_qs(parsed.query)
    return (
        str((query.get("xsec_token") or [""])[0] or "").strip(),
        str((query.get("xsec_source") or [""])[0] or "").strip(),
    )


def extract_profile_user_id(initial_state: Dict[str, Any], profile_url: str) -> str:
    user = initial_state.get("user") or {}
    user_page_data = user.get("userPageData") or {}
    basic_info = user_page_data.get("basicInfo") or {}
    candidate = stringify(basic_info.get("userId") or basic_info.get("userid") or basic_info.get("user_id"))
    if candidate:
        return candidate
    notes = user.get("notes") or []
    for bucket in notes:
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if not isinstance(item, dict):
                continue
            note_card = item.get("noteCard") or {}
            candidate = stringify(
                deep_find_first(note_card, AUTHOR_ID_KEYS)
                or item.get("userId")
                or item.get("user_id")
            )
            if candidate:
                return candidate
    matched = PROFILE_URL_PATTERN.search(str(profile_url or "").strip())
    if matched:
        return str(matched.group(1) or "").strip()
    return ""


def extract_profile_posted_page_payload(payload: Any) -> Optional[Dict[str, Any]]:
    for node in iter_dict_nodes(payload):
        items = extract_profile_posted_items(node)
        if not items:
            continue
        return {
            "items": items,
            "cursor": stringify(node.get("cursor") or node.get("nextCursor") or node.get("next_cursor")),
            "user_id": stringify(node.get("userId") or node.get("userid") or node.get("user_id")),
            "page": coerce_count(node.get("page") or node.get("pageNum") or node.get("page_num")),
            "num": coerce_count(node.get("num") or node.get("pageSize") or node.get("page_size")),
            "has_more": coerce_profile_has_more(node),
        }
    return None


def unwrap_xhs_api_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def extract_feed_note_card(payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    items = payload.get("items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            note_card = item.get("note_card") or item.get("noteCard")
            if isinstance(note_card, dict) and note_card:
                return note_card
    return None


def extract_comment_items(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    comments = payload.get("comments")
    if isinstance(comments, list):
        return [item for item in comments if isinstance(item, dict)]
    return []


def normalize_comment_preview(payload: Dict[str, Any]) -> Dict[str, Any]:
    content = collapse_whitespace(stringify(payload.get("content") or payload.get("text") or payload.get("comment")))
    user_info = payload.get("user_info") or payload.get("userInfo") or payload.get("user") or {}
    nickname = collapse_whitespace(
        stringify(user_info.get("nickname") or user_info.get("nick_name") or user_info.get("name"))
    )
    if not content:
        return {}
    return {
        "comment_id": stringify(payload.get("id") or payload.get("comment_id") or payload.get("commentId")),
        "nickname": nickname,
        "content": content,
        "like_count": coerce_count(payload.get("like_count") or payload.get("likeCount")),
        "created_at": normalize_timestamp(payload.get("create_time") or payload.get("createTime") or payload.get("time")),
    }


def extract_profile_posted_items(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [node.get("notes"), node.get("noteList"), node.get("items"), node.get("list")]
    for candidate in candidates:
        if not isinstance(candidate, list):
            continue
        items = [item for item in candidate if looks_like_profile_note_item(item)]
        if items:
            return items
    return []


def coerce_profile_has_more(node: Dict[str, Any]) -> Optional[bool]:
    if "hasMore" in node:
        return bool(node.get("hasMore"))
    if "has_more" in node:
        return bool(node.get("has_more"))
    return None


def looks_like_profile_note_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    note_card = item.get("noteCard")
    if isinstance(note_card, dict):
        return bool(note_card.get("noteId") or note_card.get("displayTitle") or note_card.get("title") or note_card.get("user"))
    return bool(item.get("id") or item.get("noteId") or item.get("note_id"))


def deep_find_first(payload: Any, aliases: tuple[str, ...]) -> Any:
    alias_set = {alias.lower() for alias in aliases}
    queue: List[Any] = [payload]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for key, value in current.items():
                if str(key).lower() in alias_set:
                    return value
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            queue.extend(item for item in current if isinstance(item, (dict, list)))
    return None


def iter_dict_nodes(payload: Any):
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


def coerce_count(value: Any) -> Optional[int]:
    if value is None or value == "" or isinstance(value, bool):
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


def normalize_timestamp(value: Any) -> str:
    from datetime import datetime, timezone

    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10**12:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().isoformat(timespec="seconds")
    text = str(value).strip()
    if text.isdigit():
        return normalize_timestamp(int(text))
    iso_candidate = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate).astimezone().isoformat(timespec="seconds")
    except ValueError:
        return text


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()
