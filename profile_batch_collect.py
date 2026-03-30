from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import load_settings
from .profile_batch_report import (
    collect_profile_reports_with_progress,
    compute_slots_per_day,
    normalize_profile_url_entries,
    select_spread_batch_entries,
)
from .project_cache import write_project_cache_bundle
from .project_sync_status import update_project_sync_status


def _collect_reports(
    *,
    settings,
    explicit_urls: List[str],
    raw_text: str,
    urls_file: Optional[str],
    project: str,
    scheduled: bool,
) -> List[Dict[str, Any]]:
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
    if scheduled:
        url_entries, _sampling_meta = select_spread_batch_entries(
            url_entries=url_entries,
            settings=settings,
            project=project_name,
        )
        if not url_entries:
            return []
    urls = [item["url"] for item in url_entries]
    if not urls:
        raise ValueError("没有找到可用的小红书账号主页链接")
    return collect_profile_reports_with_progress(
        urls=urls,
        url_entries=url_entries,
        settings=settings,
    )


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
    if args.scheduled and args.slot_offset_seconds > 0:
        time.sleep(max(0, int(args.slot_offset_seconds)))

    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    if args.project:
        update_project_sync_status(
            urls_file=args.urls_file or "",
            project=args.project,
            state="running",
            message=f"项目「{args.project}」开始采集",
            started_at=started_at,
        )

    try:
        reports = _collect_reports(
            settings=settings,
            explicit_urls=args.url,
            raw_text=args.raw_text or "",
            urls_file=args.urls_file,
            project=args.project or "",
            scheduled=args.scheduled,
        )
        if args.scheduled and not reports:
            if args.project:
                update_project_sync_status(
                    urls_file=args.urls_file or "",
                    project=args.project,
                    state="success",
                    message="当前时段无需采集，已按随机轮转策略跳过",
                    started_at=started_at,
                    finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                )
            print(
                json.dumps(
                    {
                        "status": "skipped",
                        "message": "当前时段无需采集，已按随机轮转策略跳过",
                        "window_start": getattr(settings, "xhs_batch_window_start", "14:00"),
                        "window_end": getattr(settings, "xhs_batch_window_end", "16:00"),
                        "slots_per_day": compute_slots_per_day(settings),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        successful_reports = [item for item in reports if item.get("status") == "success"]
        failed_reports = [item for item in reports if item.get("status") != "success"]
        if not successful_reports:
            failed_logs = _format_failed_report_logs(reports)
            if failed_logs:
                print(json.dumps({"status": "failed", "errors": failed_logs}, ensure_ascii=False, indent=2))
            raise ValueError("批量抓取没有成功结果，未写入本地缓存")

        cache_summary = write_project_cache_bundle(reports=successful_reports, settings=settings)
        finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
        summary = {
            "status": "success",
            "total_accounts": len(reports),
            "successful_accounts": len(successful_reports),
            "failed_accounts": len(failed_reports),
            "total_works": sum(len((item.get("works") or [])) for item in successful_reports),
            "cache_dir": cache_summary.get("cache_dir"),
            "projects": cache_summary.get("projects"),
        }
        if args.project:
            update_project_sync_status(
                urls_file=args.urls_file or "",
                project=args.project,
                state="success",
                message=f"项目「{args.project}」采集完成",
                started_at=started_at,
                finished_at=finished_at,
                total_accounts=int(summary.get("successful_accounts") or 0),
                total_works=int(summary.get("total_works") or 0),
            )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        if args.project:
            update_project_sync_status(
                urls_file=args.urls_file or "",
                project=args.project,
                state="error",
                message=f"项目「{args.project}」采集失败",
                started_at=started_at,
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                last_error=str(exc),
            )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
