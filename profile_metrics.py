from __future__ import annotations

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

    collector = collector_factory(settings)
    metric_limit = max(0, int(getattr(settings, "xhs_work_metric_limit", 0) or 0))
    target_works = works[:metric_limit] if metric_limit > 0 else works
    for work in target_works:
        signed_snapshot = None
        note_url = str(work.get("note_url") or "").strip()
        note_id = str(work.get("note_id") or "").strip()
        xsec_token = str(work.get("xsec_token") or "").strip()
        derived_note_id, derived_xsec_token = extract_note_reference_from_url(note_url)
        if derived_note_id and not note_id:
            note_id = derived_note_id
            work["note_id"] = derived_note_id
        if derived_xsec_token and not xsec_token:
            xsec_token = derived_xsec_token
            work["xsec_token"] = derived_xsec_token
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
                work["note_id"] = signed_snapshot.note_id
            if signed_snapshot.note_url:
                work["note_url"] = signed_snapshot.note_url
            if signed_snapshot.comment_count is not None:
                work["comment_count"] = signed_snapshot.comment_count
                work["comment_count_text"] = str(signed_snapshot.comment_count)
                work["comment_count_basis"] = "精确值"
                work["comment_count_is_lower_bound"] = False
                continue
        note_url = str(work.get("note_url") or "").strip()
        if not note_url:
            work["comment_count_basis"] = "详情缺失"
            work["comment_count_is_lower_bound"] = False
            work["comment_count"] = None
            work["comment_count_text"] = ""
            continue
        try:
            snapshot = collector.collect(Target(name="work-detail", url=note_url))
        except Exception:
            snapshot = None
        if snapshot is not None and snapshot.comment_count is not None:
            work["comment_count"] = snapshot.comment_count
            work["comment_count_text"] = str(snapshot.comment_count)
            work["comment_count_basis"] = "精确值"
            work["comment_count_is_lower_bound"] = False
            continue
        work["comment_count_basis"] = "详情缺失"
        work["comment_count_is_lower_bound"] = False
        work["comment_count"] = None
        work["comment_count_text"] = ""
    return report


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
