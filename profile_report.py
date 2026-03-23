from __future__ import annotations

import argparse
import json
import re
import time
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .chrome_cookies import is_default_chrome_profile_root
from .config import load_settings
from .models import Target
from .profile_metrics import enrich_profile_report_with_note_metrics as _enrich_profile_report_with_note_metrics
from .xhs import XHSCollector, _coerce_count, extract_initial_state


PROFILE_URL_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?xiaohongshu\.com/user/profile/([0-9a-z]+)", re.IGNORECASE)
DEFAULT_PROFILE_WORK_PAGE_SIZE = 30


@dataclass
class ProfileWorkItem:
    profile_user_id: str
    nickname: str
    title_copy: str
    note_type: str
    like_count: Optional[int]
    like_count_text: str
    comment_count: Optional[int]
    comment_count_text: str
    cover_url: str
    xsec_token: str
    note_id: str
    note_url: str
    index: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_type": "work",
            "profile_user_id": self.profile_user_id,
            "nickname": self.nickname,
            "title_copy": self.title_copy,
            "note_type": self.note_type,
            "like_count": self.like_count,
            "like_count_text": self.like_count_text,
            "comment_count": self.comment_count,
            "comment_count_text": self.comment_count_text,
            "cover_url": self.cover_url,
            "xsec_token": self.xsec_token,
            "note_id": self.note_id,
            "note_url": self.note_url,
            "index": self.index,
        }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="导出小红书账号页摘要和作品标题清单。")
    parser.add_argument("--url", required=True, help="账号主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--json-out", help="输出 JSON 文件路径")
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    payload = load_profile_report_payload(settings=settings, profile_url=args.url)
    report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    report = enrich_profile_report_with_note_metrics(report=report, settings=settings)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json_out:
        path = Path(args.json_out).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(output, encoding="utf-8")
        print(f"[OK] 已写出 {path}")
    print(output)
    return 0


def load_profile_report_payload(*, settings, profile_url: str) -> Dict[str, Any]:
    target = Target(name="profile", url=_normalize_profile_url_for_fetch(profile_url))
    attempts = max(1, int(getattr(settings, "xhs_retry_attempts", 3) or 1))
    retry_delay_seconds = max(0, int(getattr(settings, "xhs_retry_delay_seconds", 2) or 0))
    errors: List[str] = []
    setting_variants = _build_profile_fetch_setting_variants(settings)
    collectors = []
    for active_settings in setting_variants:
        collector = XHSCollector(active_settings)
        collectors.append(
            (
                active_settings,
                collector,
                collector._resolve_fetch_modes(target),
            )
        )

    for attempt in range(1, attempts + 1):
        best_payload: Optional[Dict[str, Any]] = None
        best_report_preview: Optional[Dict[str, Any]] = None
        for variant_index, (active_settings, collector, fetch_modes) in enumerate(collectors):
            variant_label = str(getattr(active_settings, "xhs_fetch_mode", "") or "requests")
            for mode in fetch_modes:
                try:
                    payload, final_url = collector._load_payload(target, mode)
                    if isinstance(payload, dict):
                        initial_state = payload
                    else:
                        initial_state = extract_initial_state(str(payload))
                    report_preview = build_profile_report(initial_state=initial_state, profile_url=final_url)
                    if _should_expand_profile_work_count(report_preview):
                        try:
                            profile_pages = collector.fetch_profile_posted_pages(
                                profile_url=profile_url or final_url,
                                initial_state=initial_state,
                            )
                        except Exception as exc:
                            errors.append(f"attempt {attempt}/{attempts} {variant_label}/{mode} signed-pages: {exc}")
                            profile_pages = []
                        if profile_pages:
                            initial_state = _merge_profile_pages_into_initial_state(
                                initial_state=initial_state,
                                profile_pages=profile_pages,
                            )
                            report_preview = build_profile_report(initial_state=initial_state, profile_url=final_url)
                    if _should_retry_profile_payload(report_preview=report_preview, final_url=final_url):
                        raise ValueError("账号页返回空结果或登录跳转")
                    candidate_payload = {
                        "initial_state": initial_state,
                        "final_url": final_url,
                    }
                    if report_preview.get("profile", {}).get("work_count_exact", False):
                        return candidate_payload
                    if best_report_preview is None or _profile_report_is_better(report_preview, best_report_preview):
                        best_payload = candidate_payload
                        best_report_preview = report_preview
                except Exception as exc:
                    errors.append(f"attempt {attempt}/{attempts} {variant_label}/{mode}: {exc}")
            if variant_index + 1 < len(setting_variants):
                continue
        if best_payload is not None:
            return best_payload
        if attempt < attempts and retry_delay_seconds:
            time.sleep(retry_delay_seconds)

    raise ValueError("账号页抓取失败: " + " | ".join(errors))


def _normalize_profile_url_for_fetch(profile_url: str) -> str:
    text = str(profile_url or "").strip()
    if not text:
        return text
    match = PROFILE_URL_PATTERN.search(text)
    if not match:
        return text
    return f"https://www.xiaohongshu.com/user/profile/{match.group(1)}"


def _build_profile_fetch_setting_variants(settings) -> List[Any]:
    variants = [settings]
    current_mode = str(getattr(settings, "xhs_fetch_mode", "requests") or "requests").strip().lower()
    if current_mode != "requests":
        return variants

    playwright_user_data_dir = str(getattr(settings, "playwright_user_data_dir", "") or "").strip()
    playwright_storage_state = str(getattr(settings, "playwright_storage_state", "") or "").strip()
    chrome_cookie_profile = str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip()
    if not (playwright_user_data_dir or playwright_storage_state or chrome_cookie_profile):
        return variants

    fallback = copy(settings)
    setattr(fallback, "xhs_fetch_mode", "playwright")
    if chrome_cookie_profile and is_default_chrome_profile_root(chrome_cookie_profile) and not playwright_storage_state:
        setattr(fallback, "playwright_browser_mode", "launch")
        setattr(fallback, "playwright_user_data_dir", "")
    elif chrome_cookie_profile and not playwright_user_data_dir:
        setattr(fallback, "playwright_user_data_dir", chrome_cookie_profile)
        setattr(fallback, "playwright_browser_mode", "local_profile")
    elif not getattr(fallback, "playwright_browser_mode", ""):
        setattr(fallback, "playwright_browser_mode", "launch")
    variants.append(fallback)
    return variants


def _should_retry_profile_payload(*, report_preview: Dict[str, Any], final_url: str) -> bool:
    profile = report_preview.get("profile") or {}
    works = report_preview.get("works") or []
    has_profile_content = bool(
        str(profile.get("profile_user_id") or "").strip()
        or str(profile.get("nickname") or "").strip()
        or str(profile.get("fans_count_text") or "").strip()
        or str(profile.get("interaction_count_text") or "").strip()
        or works
    )
    normalized_final_url = str(final_url or "").strip().lower()
    return ("/login" in normalized_final_url and "xiaohongshu.com" in normalized_final_url) or not has_profile_content


def _should_expand_profile_work_count(report_preview: Dict[str, Any]) -> bool:
    profile = report_preview.get("profile") or {}
    visible_work_count = int(profile.get("visible_work_count") or 0)
    return bool(profile.get("profile_user_id")) and not bool(profile.get("work_count_exact")) and visible_work_count > 0


def _merge_profile_pages_into_initial_state(*, initial_state: Dict[str, Any], profile_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    from .xhs import _merge_profile_runtime_pages

    if not profile_pages:
        return initial_state
    return _merge_profile_runtime_pages(initial_state, profile_pages)


def build_profile_report(*, initial_state: Dict[str, Any], profile_url: str) -> Dict[str, Any]:
    user = initial_state.get("user") or {}
    user_page_data = user.get("userPageData") or {}
    basic_info = user_page_data.get("basicInfo") or {}
    interactions = user_page_data.get("interactions") or []
    tags = user_page_data.get("tags") or []
    works_raw = _extract_profile_cards(user.get("notes") or [], limit=DEFAULT_PROFILE_WORK_PAGE_SIZE)
    loaded_work_count = _count_profile_cards(user.get("notes") or [])
    profile_user_id = _first_non_empty(
        basic_info.get("userId"),
        _nested_get(works_raw, [0, "noteCard", "user", "userId"]),
        _nested_get(works_raw, [0, "user", "userId"]),
    )
    nickname = str(basic_info.get("nickname") or _first_non_empty(
        _nested_get(works_raw, [0, "noteCard", "user", "nickname"]),
        _nested_get(works_raw, [0, "noteCard", "user", "nickName"]),
    ) or "")

    work_items: List[ProfileWorkItem] = []
    for index, card_entry in enumerate(works_raw):
        note_card = card_entry.get("noteCard") or {}
        title_copy = str(note_card.get("displayTitle") or note_card.get("title") or "").strip()
        interact = note_card.get("interactInfo") or {}
        like_text = str(interact.get("likedCount") or "").strip()
        comment_text = _first_non_empty(
            interact.get("commentCount"),
            interact.get("commentedCount"),
            interact.get("commentsCount"),
            interact.get("commentNum"),
            interact.get("replyCount"),
        )
        xsec_token = str(note_card.get("xsecToken") or card_entry.get("xsecToken") or "").strip()
        note_id = str(note_card.get("noteId") or card_entry.get("id") or "").strip()
        work_items.append(
            ProfileWorkItem(
                profile_user_id=profile_user_id,
                nickname=nickname,
                title_copy=title_copy,
                note_type=str(note_card.get("type") or "").strip(),
                like_count=_coerce_count(like_text),
                like_count_text=like_text,
                comment_count=_coerce_count(comment_text),
                comment_count_text=str(comment_text or "").strip(),
                cover_url=_cover_url(note_card),
                xsec_token=xsec_token,
                note_id=note_id,
                note_url=_build_note_url(note_id=note_id, xsec_token=xsec_token),
                index=index,
            )
        )

    interaction_map = {str(item.get("type") or ""): str(item.get("count") or "") for item in interactions if isinstance(item, dict)}
    work_count_fields = _build_profile_work_count_fields(
        user=user,
        visible_work_count=len(work_items),
        loaded_work_count=loaded_work_count,
    )
    captured_at = datetime.now().astimezone().isoformat(timespec="seconds")
    return {
        "captured_at": captured_at,
        "profile": {
            "record_type": "account",
            "profile_url": profile_url,
            "profile_user_id": profile_user_id,
            "nickname": nickname,
            "red_id": str(basic_info.get("redId") or "").strip(),
            "desc": str(basic_info.get("desc") or "").strip(),
            "ip_location": str(basic_info.get("ipLocation") or "").strip(),
            "avatar_url": str(basic_info.get("images") or basic_info.get("imageb") or "").strip(),
            "gender": basic_info.get("gender"),
            "follows_count_text": interaction_map.get("follows", ""),
            "fans_count_text": interaction_map.get("fans", ""),
            "interaction_count_text": interaction_map.get("interaction", ""),
            "visible_work_count": len(work_items),
            "total_work_count": work_count_fields["total_work_count"],
            "work_count_display_text": work_count_fields["work_count_display_text"],
            "work_count_exact": work_count_fields["work_count_exact"],
            "work_count_has_more": work_count_fields["work_count_has_more"],
            "tags": [item for item in tags if isinstance(item, dict)],
        },
        "works": [item.to_dict() for item in work_items],
    }


def enrich_profile_report_with_note_metrics(*, report: Dict[str, Any], settings) -> Dict[str, Any]:
    return _enrich_profile_report_with_note_metrics(
        report=report,
        settings=settings,
        collector_factory=XHSCollector,
    )


def _extract_profile_cards(notes: List[Any], *, limit: int = DEFAULT_PROFILE_WORK_PAGE_SIZE) -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    for bucket in notes:
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if not isinstance(item, dict):
                continue
            note_card = item.get("noteCard")
            if isinstance(note_card, dict) and (note_card.get("displayTitle") or note_card.get("title")):
                cards.append(item)
                if len(cards) >= limit:
                    return cards[:limit]
        if cards:
            break
    return cards[:limit]


def _count_profile_cards(notes: List[Any]) -> int:
    count = 0
    for bucket in notes:
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if not isinstance(item, dict):
                continue
            note_card = item.get("noteCard")
            if isinstance(note_card, dict) and (
                note_card.get("displayTitle")
                or note_card.get("title")
                or note_card.get("noteId")
                or note_card.get("user")
            ):
                count += 1
    return count


def _build_profile_work_count_fields(*, user: Dict[str, Any], visible_work_count: int, loaded_work_count: int) -> Dict[str, Any]:
    note_queries = user.get("noteQueries") or []
    has_query_metadata = False
    has_more = False
    page_size = DEFAULT_PROFILE_WORK_PAGE_SIZE

    for item in note_queries:
        if not isinstance(item, dict):
            continue
        has_query_metadata = True
        try:
            page_size = max(page_size, int(item.get("num") or 0))
        except (TypeError, ValueError):
            pass
        if bool(item.get("hasMore")):
            has_more = True

    if has_query_metadata:
        work_count_exact = not has_more
    else:
        work_count_exact = visible_work_count < page_size

    total_work_count: Optional[int]
    if work_count_exact:
        total_work_count = max(visible_work_count, loaded_work_count)
        work_count_display_text = str(total_work_count)
    else:
        total_work_count = None
        lower_bound = max(visible_work_count, loaded_work_count)
        work_count_display_text = f"{lower_bound}+" if lower_bound > 0 else ""

    return {
        "total_work_count": total_work_count,
        "work_count_display_text": work_count_display_text,
        "work_count_exact": work_count_exact,
        "work_count_has_more": has_more,
    }


def _profile_report_is_better(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    left_profile = left.get("profile") or {}
    right_profile = right.get("profile") or {}
    left_exact = bool(left_profile.get("work_count_exact", False))
    right_exact = bool(right_profile.get("work_count_exact", False))
    if left_exact != right_exact:
        return left_exact
    left_total = left_profile.get("total_work_count") or 0
    right_total = right_profile.get("total_work_count") or 0
    if left_total != right_total:
        return left_total > right_total
    left_visible = left_profile.get("visible_work_count") or 0
    right_visible = right_profile.get("visible_work_count") or 0
    return left_visible > right_visible


def _cover_url(note_card: Dict[str, Any]) -> str:
    cover = note_card.get("cover") or {}
    if not isinstance(cover, dict):
        return ""
    return str(cover.get("urlDefault") or cover.get("urlPre") or cover.get("url") or "").strip()


def _build_note_url(*, note_id: str, xsec_token: str) -> str:
    if note_id:
        if xsec_token:
            return f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}&xsec_source=pc_user"
        return f"https://www.xiaohongshu.com/explore/{note_id}"
    return ""


def _nested_get(items: List[Dict[str, Any]], path: List[Any]) -> Any:
    if not items:
        return ""
    current: Any = items
    for key in path:
        if isinstance(current, list):
            if not isinstance(key, int) or key >= len(current):
                return ""
            current = current[key]
            continue
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    return current


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
