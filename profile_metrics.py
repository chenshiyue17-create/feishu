from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from .models import Target
from .xhs import XHSCollector


def enrich_profile_report_with_note_metrics(*, report: Dict[str, Any], settings, collector_factory=XHSCollector) -> Dict[str, Any]:
    if not getattr(settings, "xhs_fetch_work_comment_counts", True):
        return report
    works = report.get("works") or []
    if not works:
        return report

    metric_limit = max(0, int(getattr(settings, "xhs_work_metric_limit", 0) or 0))
    target_works = works[:metric_limit] if metric_limit > 0 else works
    _enrich_works_with_note_metrics(
        works=target_works,
        settings=settings,
        collector_factory=collector_factory,
    )
    return report


def _resolve_note_metric_concurrency(*, settings, work_count: int) -> int:
    if work_count <= 1:
        return work_count
    fetch_mode = str(getattr(settings, "xhs_fetch_mode", "requests") or "requests").strip().lower()
    if fetch_mode in {"playwright", "local_browser"}:
        return 1
    configured = int(getattr(settings, "xhs_batch_concurrency", 6) or 6)
    return max(1, min(configured, work_count, 8))


def _enrich_works_with_note_metrics(*, works: List[Dict[str, Any]], settings, collector_factory) -> None:
    concurrency = _resolve_note_metric_concurrency(settings=settings, work_count=len(works))
    if concurrency <= 1:
        collector = collector_factory(settings)
        for work in works:
            enriched = _enrich_single_work_with_note_metrics(
                work=work,
                collector=collector,
                settings=settings,
            )
            work.clear()
            work.update(enriched)
        return

    def _run(index: int, work: Dict[str, Any]) -> tuple[int, Dict[str, Any]]:
        collector = collector_factory(settings)
        return index, _enrich_single_work_with_note_metrics(
            work=work,
            collector=collector,
            settings=settings,
        )

    future_to_index = {}
    results: Dict[int, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for index, work in enumerate(works):
            future = executor.submit(_run, index, work)
            future_to_index[future] = index
        for future in as_completed(future_to_index):
            index, enriched = future.result()
            results[index] = enriched

    for index, work in enumerate(works):
        enriched = results.get(index)
        if enriched is None:
            continue
        work.clear()
        work.update(enriched)


def _enrich_single_work_with_note_metrics(*, work: Dict[str, Any], collector, settings) -> Dict[str, Any]:
    enriched = dict(work)
    signed_snapshot = None
    comment_preview: List[Dict[str, Any]] = []
    note_url = str(enriched.get("note_url") or "").strip()
    note_id = str(enriched.get("note_id") or "").strip()
    xsec_token = str(enriched.get("xsec_token") or "").strip()
    derived_note_id, derived_xsec_token = extract_note_reference_from_url(note_url)
    if derived_note_id and not note_id:
        note_id = derived_note_id
        enriched["note_id"] = derived_note_id
    if derived_xsec_token and not xsec_token:
        xsec_token = derived_xsec_token
        enriched["xsec_token"] = derived_xsec_token
    if note_id:
        try:
            signed_snapshot = collector.collect_note_detail(
                note_id=note_id,
                note_url=note_url,
                xsec_token=xsec_token,
                xsec_source="pc_user",
            )
        except Exception:
            signed_snapshot = None
    if signed_snapshot is not None:
        if signed_snapshot.note_id:
            enriched["note_id"] = signed_snapshot.note_id
        if signed_snapshot.note_url:
            enriched["note_url"] = signed_snapshot.note_url
        if signed_snapshot.comment_count is not None:
            enriched["comment_count"] = signed_snapshot.comment_count
            enriched["comment_count_text"] = str(signed_snapshot.comment_count)
            enriched["comment_count_basis"] = "精确值"
            enriched["comment_count_is_lower_bound"] = False
            return enriched
    note_url = str(enriched.get("note_url") or "").strip()
    if not note_url:
        if comment_preview and enriched.get("comment_count") is None:
            preview_count = len(comment_preview)
            enriched["comment_count"] = preview_count
            enriched["comment_count_text"] = f"{preview_count}+"
            enriched["comment_count_basis"] = "评论预览下限"
            enriched["comment_count_is_lower_bound"] = True
        elif enriched.get("comment_count") is None:
            enriched["comment_count_basis"] = "详情缺失"
            enriched["comment_count_is_lower_bound"] = False
        return enriched
    try:
        snapshot = collector.collect(Target(name="work-detail", url=note_url))
    except Exception:
        snapshot = None
    if snapshot is not None and snapshot.comment_count is not None:
        enriched["comment_count"] = snapshot.comment_count
        enriched["comment_count_text"] = str(snapshot.comment_count)
        enriched["comment_count_basis"] = "精确值"
        enriched["comment_count_is_lower_bound"] = False
        return enriched
    if getattr(settings, "xhs_fetch_work_comment_preview", True) and note_id and xsec_token:
        try:
            comment_preview = collector.fetch_note_comments_preview(
                note_id=note_id,
                xsec_token=xsec_token,
                note_url=note_url,
                limit=int(getattr(settings, "xhs_work_comment_preview_limit", 3) or 3),
            )
        except Exception:
            comment_preview = []
        if comment_preview:
            enriched["recent_comments"] = comment_preview
            enriched["recent_comments_summary"] = build_recent_comments_summary(comment_preview)
    if comment_preview and enriched.get("comment_count") is None:
        preview_count = len(comment_preview)
        enriched["comment_count"] = preview_count
        enriched["comment_count_text"] = f"{preview_count}+"
        enriched["comment_count_basis"] = "评论预览下限"
        enriched["comment_count_is_lower_bound"] = True
        return enriched
    if enriched.get("comment_count") is None:
        enriched["comment_count_basis"] = "详情缺失"
        enriched["comment_count_is_lower_bound"] = False
    return enriched


def extract_note_reference_from_url(note_url: str) -> tuple[str, str]:
    parsed = urlparse(str(note_url or "").strip())
    path_parts = [part for part in parsed.path.split("/") if part]
    note_id = ""
    if len(path_parts) >= 2 and path_parts[0] == "explore":
        note_id = str(path_parts[1] or "").strip()
    query = parse_qs(parsed.query)
    xsec_token = str((query.get("xsec_token") or [""])[0] or "").strip()
    return note_id, xsec_token


def build_recent_comments_summary(comments: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for item in comments:
        if not isinstance(item, dict):
            continue
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        nickname = str(item.get("nickname") or "").strip()
        parts.append(f"{nickname}: {content}" if nickname else content)
    return " | ".join(parts[:3])
