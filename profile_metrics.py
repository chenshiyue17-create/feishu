from __future__ import annotations

from typing import Any, Dict, List

from .models import Target
from .xhs import XHSCollector


def enrich_profile_report_with_note_metrics(*, report: Dict[str, Any], settings, collector_factory=XHSCollector) -> Dict[str, Any]:
    if not getattr(settings, "xhs_fetch_work_comment_counts", True):
        return report
    works = report.get("works") or []
    if not works:
        return report

    collector = collector_factory(settings)
    for work in works:
        signed_snapshot = None
        note_id = str(work.get("note_id") or "").strip()
        xsec_token = str(work.get("xsec_token") or "").strip()
        note_url = str(work.get("note_url") or "").strip()
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
                    work["recent_comments"] = comment_preview
                    work["recent_comments_summary"] = build_recent_comments_summary(comment_preview)
            if signed_snapshot.comment_count is not None:
                work["comment_count"] = signed_snapshot.comment_count
                work["comment_count_text"] = str(signed_snapshot.comment_count)
                continue
        note_url = str(work.get("note_url") or "").strip()
        if not note_url:
            continue
        try:
            snapshot = collector.collect(Target(name="work-detail", url=note_url))
        except Exception:
            continue
        if snapshot.comment_count is not None:
            work["comment_count"] = snapshot.comment_count
            work["comment_count_text"] = str(snapshot.comment_count)
    return report


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
