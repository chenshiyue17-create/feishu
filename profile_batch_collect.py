from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import load_settings
from .profile_batch_report import (
    collect_profile_reports_with_progress,
    compute_slots_per_day,
    normalize_profile_url_entries,
    normalize_profile_url,
    select_spread_batch_entries,
)
from .profile_batch_to_feishu import normalize_batch_item_to_report
from .project_cache import write_project_cache_bundle
from .project_sync_status import update_project_sync_status


DEFAULT_COLLECTION_RESUME_DIRNAME = ".collection_resume"


def _resolve_collect_url_entries(
    *,
    explicit_urls: List[str],
    raw_text: str,
    urls_file: Optional[str],
    project: str,
    scheduled: bool,
    settings,
) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    url_entries = normalize_profile_url_entries(explicit_urls, raw_text, urls_file)
    project_name = str(project or "").strip()
    if project_name:
        url_entries = [
            {
                **item,
                "project": str(item.get("project") or "").strip() or project_name,
            }
            for item in url_entries
        ]
        url_entries = [item for item in url_entries if str(item.get("project") or "").strip() == project_name]
    sampling_meta: Dict[str, Any] = {}
    if scheduled:
        url_entries, sampling_meta = select_spread_batch_entries(
            url_entries=url_entries,
            settings=settings,
            project=project_name,
        )
    return url_entries, sampling_meta


def _slugify_resume_scope(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "-", str(text or "").strip()).strip("-")
    return cleaned or "default"


def _resolve_collection_resume_path(
    *,
    settings,
    project: str,
    scheduled: bool,
    urls_file: Optional[str],
) -> Path:
    base_dir = Path(str(getattr(settings, "project_cache_dir", "") or "/Users/cc/Downloads/飞书缓存")).expanduser().resolve()
    scope_text = str(project or "").strip()
    if not scope_text and urls_file:
        scope_text = Path(urls_file).expanduser().resolve().stem
    filename = f"{_slugify_resume_scope(scope_text or 'adhoc')}-{'scheduled' if scheduled else 'manual'}.json"
    return base_dir / DEFAULT_COLLECTION_RESUME_DIRNAME / filename


def _report_resume_key(report: Dict[str, Any]) -> str:
    profile = report.get("profile") or {}
    return (
        normalize_profile_url(str(report.get("source_url") or profile.get("profile_url") or ""))
        or str(profile.get("profile_user_id") or "").strip()
    )


def _merge_resume_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for report in reports:
        key = _report_resume_key(report)
        if not key:
            continue
        if key not in merged:
            order.append(key)
        merged[key] = dict(report)
    return [merged[key] for key in order]


def _load_collection_resume_reports(
    *,
    path: Path,
    date_text: str,
    project: str,
    scheduled: bool,
    active_urls: List[str],
) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    if str(payload.get("date") or "") != date_text:
        return []
    if bool(payload.get("scheduled")) != bool(scheduled):
        return []
    if str(payload.get("project") or "").strip() != str(project or "").strip():
        return []
    active_url_set = {normalize_profile_url(url) for url in active_urls if normalize_profile_url(url)}
    reports = _merge_resume_reports(list(payload.get("successful_reports") or []))
    if not active_url_set:
        return reports
    return [report for report in reports if _report_resume_key(report) in active_url_set]


def _write_collection_resume_reports(
    *,
    path: Path,
    date_text: str,
    project: str,
    scheduled: bool,
    successful_reports: List[Dict[str, Any]],
    last_error: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_text,
        "project": str(project or "").strip(),
        "scheduled": bool(scheduled),
        "successful_reports": _merge_resume_reports(successful_reports),
        "last_error": str(last_error or "").strip(),
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _clear_collection_resume_reports(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _format_failed_report_logs(reports: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, str]]:
    failed_logs: List[Dict[str, str]] = []
    for item in reports:
        if item.get("status") == "success":
            continue
        failed_logs.append(
            {
                "url": str(item.get("requested_url") or item.get("final_url") or "").strip(),
                "account": str((item.get("profile") or {}).get("nickname") or "").strip(),
                "error": str(item.get("error") or item.get("message") or "unknown error").strip(),
            }
        )
        if len(failed_logs) >= limit:
            break
    return failed_logs


def collect_profiles_to_local_cache(
    *,
    env_file: str,
    settings,
    explicit_urls: Optional[List[str]] = None,
    raw_text: str = "",
    urls_file: Optional[str] = None,
    project: str = "",
    scheduled: bool = False,
    slot_offset_seconds: int = 0,
) -> Dict[str, Any]:
    if scheduled and slot_offset_seconds > 0:
        time.sleep(max(0, int(slot_offset_seconds)))

    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    date_text = datetime.now().astimezone().date().isoformat()
    if project:
        update_project_sync_status(
            urls_file=urls_file or "",
            project=project,
            state="running",
            message=f"项目「{project}」开始采集",
            started_at=started_at,
        )

    try:
        url_entries, sampling_meta = _resolve_collect_url_entries(
            explicit_urls=list(explicit_urls or []),
            raw_text=raw_text,
            urls_file=urls_file,
            project=project,
            scheduled=scheduled,
            settings=settings,
        )
        if scheduled and not url_entries:
            summary = {
                "status": "skipped",
                "message": "当前时段无需采集，已按随机轮转策略跳过",
                "window_start": sampling_meta.get("window_start") or getattr(settings, "xhs_batch_window_start", "14:00"),
                "window_end": sampling_meta.get("window_end") or getattr(settings, "xhs_batch_window_end", "15:00"),
                "slots_per_day": int(sampling_meta.get("slots_per_day") or compute_slots_per_day(settings)),
            }
            if project:
                update_project_sync_status(
                    urls_file=urls_file or "",
                    project=project,
                    state="success",
                    message="当前时段无需采集，已按随机轮转策略跳过",
                    started_at=started_at,
                    finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                )
            return summary

        urls = [item["url"] for item in url_entries]
        if not urls:
            raise ValueError("没有找到可用的小红书账号主页链接")

        resume_path = _resolve_collection_resume_path(
            settings=settings,
            project=project,
            scheduled=scheduled,
            urls_file=urls_file,
        )
        resumed_reports = _load_collection_resume_reports(
            path=resume_path,
            date_text=date_text,
            project=project,
            scheduled=scheduled,
            active_urls=urls,
        )
        successful_reports = _merge_resume_reports(list(resumed_reports))
        successful_url_set = {_report_resume_key(report) for report in successful_reports if _report_resume_key(report)}
        pending_entries = [item for item in url_entries if item["url"] not in successful_url_set]
        resumed_count = len(successful_reports)

        if project and resumed_count:
            update_project_sync_status(
                urls_file=urls_file or "",
                project=project,
                state="running",
                message=f"项目「{project}」断点继续，已跳过 {resumed_count} 个已完成账号",
                started_at=started_at,
            )

        if not pending_entries and successful_reports:
            cache_summary = write_project_cache_bundle(reports=successful_reports, settings=settings)
            _clear_collection_resume_reports(resume_path)
            finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
            summary = {
                "status": "success",
                "total_accounts": len(url_entries),
                "successful_accounts": len(successful_reports),
                "failed_accounts": 0,
                "resumed_accounts": resumed_count,
                "pending_accounts": 0,
                "total_works": sum(len((item.get("works") or [])) for item in successful_reports),
                "cache_dir": cache_summary.get("cache_dir"),
                "projects": cache_summary.get("projects"),
                "resume_path": str(resume_path),
            }
            if project:
                update_project_sync_status(
                    urls_file=urls_file or "",
                    project=project,
                    state="success",
                    message=f"项目「{project}」断点采集完成",
                    started_at=started_at,
                    finished_at=finished_at,
                    total_accounts=int(summary.get("successful_accounts") or 0),
                    total_works=int(summary.get("total_works") or 0),
                )
            return summary

        if pending_entries:
            _write_collection_resume_reports(
                path=resume_path,
                date_text=date_text,
                project=project,
                scheduled=scheduled,
                successful_reports=successful_reports,
            )

        new_successful_reports: List[Dict[str, Any]] = []
        failed_reports: List[Dict[str, Any]] = []

        def _persist_resume_progress(payload: Dict[str, Any]) -> None:
            nonlocal successful_reports, new_successful_reports
            if str(payload.get("phase") or "").strip() != "collect":
                return
            if str(payload.get("status") or "").strip() != "success":
                return
            raw_item = payload.get("raw_item")
            if not isinstance(raw_item, dict):
                return
            report = normalize_batch_item_to_report(
                raw_item,
                project=str(raw_item.get("project") or project or "").strip(),
            )
            merged_reports = _merge_resume_reports(successful_reports + [report])
            if len(merged_reports) == len(successful_reports):
                return
            successful_reports = merged_reports
            new_successful_reports = _merge_resume_reports(new_successful_reports + [report])
            _write_collection_resume_reports(
                path=resume_path,
                date_text=date_text,
                project=project,
                scheduled=scheduled,
                successful_reports=successful_reports,
            )

        reports = collect_profile_reports_with_progress(
            urls=[item["url"] for item in pending_entries],
            url_entries=pending_entries,
            settings=settings,
            progress_callback=_persist_resume_progress,
        )

        successful_reports = _merge_resume_reports(
            successful_reports
            + [
                normalize_batch_item_to_report(
                    item,
                    project=str(item.get("project") or project or "").strip(),
                )
                for item in reports
                if item.get("status") == "success"
            ]
        )
        failed_reports = [item for item in reports if item.get("status") != "success"]
        if not successful_reports:
            failed_logs = _format_failed_report_logs(failed_reports)
            if failed_logs:
                print(json.dumps({"status": "failed", "errors": failed_logs}, ensure_ascii=False, indent=2))
            raise ValueError("批量抓取没有成功结果，未写入本地缓存")

        cache_summary = write_project_cache_bundle(reports=successful_reports, settings=settings)
        if failed_reports:
            _write_collection_resume_reports(
                path=resume_path,
                date_text=date_text,
                project=project,
                scheduled=scheduled,
                successful_reports=successful_reports,
                last_error=str((failed_reports[0] or {}).get("error") or ""),
            )
        else:
            _clear_collection_resume_reports(resume_path)
        finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
        summary = {
            "status": "partial" if failed_reports else "success",
            "total_accounts": len(url_entries),
            "successful_accounts": len(successful_reports),
            "failed_accounts": len(failed_reports),
            "resumed_accounts": resumed_count,
            "pending_accounts": len(failed_reports),
            "total_works": sum(len((item.get("works") or [])) for item in successful_reports),
            "cache_dir": cache_summary.get("cache_dir"),
            "projects": cache_summary.get("projects"),
            "resume_path": str(resume_path),
        }
        if project:
            update_project_sync_status(
                urls_file=urls_file or "",
                project=project,
                state="partial" if failed_reports else "success",
                message=f"项目「{project}」采集完成，已保留断点等待补齐" if failed_reports else f"项目「{project}」采集完成",
                started_at=started_at,
                finished_at=finished_at,
                total_accounts=int(summary.get("successful_accounts") or 0),
                total_works=int(summary.get("total_works") or 0),
                last_error=str((failed_reports[0] or {}).get("error") or "") if failed_reports else "",
            )
        return summary
    except Exception as exc:
        if project:
            update_project_sync_status(
                urls_file=urls_file or "",
                project=project,
                state="error",
                message=f"项目「{project}」采集失败",
                started_at=started_at,
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                last_error=str(exc),
            )
        raise


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="批量采集多个小红书账号并写入本地缓存。")
    parser.add_argument("--url", action="append", default=[], help="单个账号主页链接，可重复传入")
    parser.add_argument("--urls-file", help="每行一个账号主页链接的文本文件")
    parser.add_argument("--raw-text", help="一段原始文本，脚本会自动提取其中的小红书主页链接")
    parser.add_argument("--project", help="只采集指定项目")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--scheduled", action="store_true", help="供定时任务调用：按日内分批策略随机抽样采集")
    parser.add_argument("--slot-offset-seconds", type=int, default=0, help="定时任务项目错峰启动延迟秒数")
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    summary = collect_profiles_to_local_cache(
        env_file=args.env_file,
        settings=settings,
        explicit_urls=args.url,
        raw_text=args.raw_text or "",
        urls_file=args.urls_file,
        project=args.project or "",
        scheduled=args.scheduled,
        slot_offset_seconds=args.slot_offset_seconds,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
