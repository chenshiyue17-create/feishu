from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .config import load_settings
from .comment_alerts import build_work_comment_fields, sync_comment_alerts
from .feishu import FeishuBitableClient, fields_match
from .launchd import (
    build_launch_agent_plist,
    build_launch_environment,
    default_paths,
    install_launch_agent,
    unload_launch_agent,
    wrap_program_arguments_for_login_shell,
)
from .profile_batch_report import (
    collect_profile_reports_with_progress,
    compute_slots_per_day,
    load_url_entries_file,
    normalize_profile_url,
    normalize_profile_url_entries,
    normalize_profile_urls,
    select_spread_batch_entries,
)
from .project_cache import resolve_project_cache_dir, write_project_cache_bundle
from .project_sync_status import update_project_sync_status
from .profile_dashboard_to_feishu import (
    DASHBOARD_CALENDAR_FIELDS,
    DASHBOARD_CALENDAR_TABLE_NAME,
    DASHBOARD_SINGLE_WORK_RANKING_FIELDS,
    build_record_state_index,
    build_single_work_ranking_fields,
    build_single_work_rankings,
    ensure_named_table,
    sync_dashboard_portal,
    sync_dashboard_tables,
    sync_single_work_ranking_table,
    to_ms,
)
from .profile_live_sync import parse_daily_time
from .profile_to_feishu import (
    PROFILE_FIELD_SPECS,
    PROFILE_TABLE_NAME,
    build_profile_feishu_fields,
    dedupe_profile_records,
    ensure_profile_table,
)
from .profile_works_to_feishu import (
    WORKS_TABLE_FIELDS,
    WORKS_CALENDAR_FIELDS,
    WORKS_CALENDAR_TABLE_NAME,
    WORKS_TABLE_NAME,
    build_work_calendar_fields,
    build_work_calendar_history_index,
    build_work_feishu_fields,
    build_work_fingerprint,
    build_work_weekly_fields,
    dedupe_work_records,
    ensure_works_calendar_table,
    ensure_works_table,
    extract_snapshot_date,
    select_work_weekly_baseline,
)

DEFAULT_BATCH_SYNC_LABEL = "com.cc.xhs-profile-batch-report"
NOTE_ID_FROM_URL_PATTERN = re.compile(r"/(?:explore|discovery/item)/([A-Za-z0-9_-]+)")
PROFILE_ID_FROM_URL_PATTERN = re.compile(r"/user/profile/([0-9a-z]+)", re.IGNORECASE)
LIKE_RANK_TYPE = "单条点赞排行"
COMMENT_RANK_TYPE = "单条评论排行"
PROJECT_WORK_RANKING_TABLE_NAME = "项目作品排行榜"
SINGLE_TABLE_RANKING_TABLE_NAMES = (PROJECT_WORK_RANKING_TABLE_NAME, "小红书单条作品排行", "数据表")
PROJECT_ACCOUNT_RANKING_TABLE_NAME = "项目账号排行榜"
PRODUCT_METRIC_NOTE = "口径：每个账号最多采集前30条作品；项目增长按可比账号计算；飞书仅做留底和协作展示"
ACCOUNT_RANKING_USAGE_NOTE = "用途：按项目查看账号点赞排行和评论排行"
EXPORT_REVIEW_ROOT_DIR = "/Users/cc/Downloads/飞书缓存/账号榜单导出"
DAILY_LIKE_REVIEW_TABLE_NAME = "每日点赞复盘"
DAILY_COMMENT_REVIEW_TABLE_NAME = "每日评论复盘"
PROJECT_DASHBOARD_VIEW_SPECS = (
    (DAILY_LIKE_REVIEW_TABLE_NAME, "今日点赞榜", "grid"),
    (DAILY_COMMENT_REVIEW_TABLE_NAME, "今日评论榜", "grid"),
)
PROJECT_DASHBOARD_AUX_VIEW_SPECS = (
    (DASHBOARD_CALENDAR_TABLE_NAME, "日历", "calendar"),
    (DASHBOARD_CALENDAR_TABLE_NAME, "最新留底", "grid"),
    (DAILY_LIKE_REVIEW_TABLE_NAME, "点赞复盘", "grid"),
    (DAILY_COMMENT_REVIEW_TABLE_NAME, "评论复盘", "grid"),
)
LEGACY_FEISHU_TABLE_NAMES = {
    "__codex_probe__",
    "小红书账号总览",
    "小红书作品数据",
    "小红书看板总览",
    "小红书看板趋势",
    "小红书看板榜单",
    "小红书仪表盘总控",
    "小红书日历留底",
    "小红书作品日历留底",
    "小红书单条作品排行",
    "东莞-点赞排行",
    "东莞-评论排行",
    "默认项目-点赞排行",
    "默认项目-评论排行",
    "项目账号排行榜",
    "项目作品排行榜",
}
DAILY_REVIEW_COMMON_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "复盘键", "type": 1},
    {"field_name": "日期", "type": 5, "property": {"date_formatter": "yyyy-MM-dd"}},
    {"field_name": "日期文本", "type": 1},
    {"field_name": "快照时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "快照批次", "type": 1},
    {"field_name": "项目", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "账号内排名", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "标题", "type": 1},
    {"field_name": "摘要", "type": 1},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "封面图", "type": 15},
    {"field_name": "追踪状态", "type": 1},
    {"field_name": "首次入池日期", "type": 1},
    {"field_name": "快照目录", "type": 1},
    {"field_name": "口径说明", "type": 1},
]
DAILY_LIKE_REVIEW_FIELDS: List[Dict[str, Any]] = [
    *DAILY_REVIEW_COMMON_FIELDS,
    {"field_name": "点赞数", "type": 2, "property": {"formatter": "0"}},
]
DAILY_COMMENT_REVIEW_FIELDS: List[Dict[str, Any]] = [
    *DAILY_REVIEW_COMMON_FIELDS,
    {"field_name": "评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论口径", "type": 1},
]
PROJECT_ACCOUNT_RANKING_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "项目账号榜单键", "type": 1},
    {"field_name": "项目", "type": 1},
    {"field_name": "榜单类型", "type": 1},
    {"field_name": "排名", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "排序值", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "日历日期", "type": 5, "property": {"date_formatter": "yyyy-MM-dd"}},
    {"field_name": "日期文本", "type": 1},
    {"field_name": "粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总评论", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "账号总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "作品数展示", "type": 1},
    {"field_name": "头部作品标题", "type": 1},
    {"field_name": "头部作品点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "TOP3作品摘要", "type": 1},
    {"field_name": "口径说明", "type": 1},
    {"field_name": "数据用途", "type": 1},
]


def is_feishu_forbidden_error(exc: Exception) -> bool:
    message = str(exc or "")
    return "403" in message and "Forbidden" in message


def is_feishu_record_not_found_error(exc: Exception) -> bool:
    return "1254043" in str(exc or "") or "RecordIdNotFound" in str(exc or "")


def clear_feishu_table_records(*, settings, table_id: str) -> int:
    client = FeishuBitableClient(replace(settings, feishu_table_id=table_id))
    deleted = 0
    # Deleting while paging can cause page-token scans to skip rows as the table mutates.
    # Always re-read from the head until the table is empty.
    while True:
        batch = client.list_records(page_size=500, field_names=[])
        if not batch:
            break
        deleted_in_round = 0
        for item in batch:
            record_id = str(item.get("record_id") or "").strip()
            if not record_id:
                continue
            try:
                client.delete_record(record_id)
                deleted += 1
                deleted_in_round += 1
            except Exception as exc:
                if is_feishu_record_not_found_error(exc):
                    continue
                raise
        if deleted_in_round <= 0:
            break
    return deleted


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把多个小红书账号批量同步到飞书多维表格。")
    parser.add_argument("--url", action="append", default=[], help="单个账号主页链接，可重复传入")
    parser.add_argument("--urls-file", help="每行一个账号主页链接的文本文件")
    parser.add_argument("--raw-text", help="一段原始文本，脚本会自动提取其中的小红书主页链接")
    parser.add_argument("--project", help="只同步指定项目")
    parser.add_argument("--report-json", help="可选：直接读取 profile_batch_report 导出的 JSON 文件")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--profile-table-name", default=PROFILE_TABLE_NAME)
    parser.add_argument("--works-table-name", default=WORKS_TABLE_NAME)
    parser.add_argument("--ensure-fields", action="store_true", help="自动补齐账号汇总和作品明细字段")
    parser.add_argument("--sync-dashboard", action="store_true", help="额外同步飞书看板总览、趋势和榜单数据")
    parser.add_argument("--dry-run", action="store_true", help="只输出将要同步的账号和作品数量，不写入飞书")
    parser.add_argument("--install-launchd", action="store_true", help="生成并安装 launchd 定时任务")
    parser.add_argument("--load-launchd", action="store_true", help="安装后立刻加载 launchd 任务")
    parser.add_argument("--unload-launchd", action="store_true", help="卸载 launchd 任务")
    parser.add_argument("--daily-at", default="14:00", help="每天固定执行时间，格式 HH:MM")
    parser.add_argument("--project-slot-minutes", type=int, default=20, help="按项目安装错峰任务时，每个项目之间的分钟间隔")
    parser.add_argument("--scheduled", action="store_true", help="供定时任务调用：按日内分批策略随机抽样采集")
    parser.add_argument("--slot-offset-seconds", type=int, default=0, help="定时任务项目错峰启动延迟秒数")
    parser.add_argument("--launchd-label", default=DEFAULT_BATCH_SYNC_LABEL, help="launchd 任务标签")
    parser.add_argument("--launchd-plist", help="launchd plist 路径")
    parser.add_argument("--stdout-log-path", help="stdout 日志路径")
    parser.add_argument("--stderr-log-path", help="stderr 日志路径")
    args = parser.parse_args(argv)

    if args.unload_launchd:
        unloaded = unload_batch_sync_launchd(label=args.launchd_label, plist_path=args.launchd_plist)
        if unloaded:
            for path in unloaded:
                print(f"[OK] unloaded launchd plist={path}")
        else:
            print("[OK] no launchd plist matched")
        return 0

    if args.install_launchd:
        if args.report_json:
            parser.error("--install-launchd 不能和 --report-json 一起使用，定时任务需要实时抓取账号链接")
        urls = normalize_profile_urls(args.url, args.raw_text or "", args.urls_file)
        if not urls:
            parser.error("安装 launchd 前需要提供 --url / --urls-file / --raw-text")
        install_batch_sync_launchd(
            urls=urls,
            urls_file=args.urls_file,
            raw_text=args.raw_text or "",
            project=args.project or "",
            env_file=args.env_file,
            profile_table_name=args.profile_table_name,
            works_table_name=args.works_table_name,
            ensure_fields=args.ensure_fields,
            sync_dashboard=args.sync_dashboard,
            daily_at=args.daily_at,
            project_slot_minutes=args.project_slot_minutes,
            label=args.launchd_label,
            plist_path=args.launchd_plist,
            stdout_log_path=args.stdout_log_path,
            stderr_log_path=args.stderr_log_path,
            load_after_install=args.load_launchd,
        )
        return 0

    settings = load_settings(args.env_file)
    if args.scheduled and args.slot_offset_seconds > 0:
        time.sleep(max(0, int(args.slot_offset_seconds)))
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    if args.project:
        update_project_sync_status(
            urls_file=args.urls_file or "",
            project=args.project,
            state="running",
            message=f"项目「{args.project}」开始同步",
            started_at=started_at,
        )
    try:
        reports = load_reports_for_sync(
            settings=settings,
            explicit_urls=args.url,
            raw_text=args.raw_text or "",
            urls_file=args.urls_file,
            project=args.project or "",
            report_json=args.report_json,
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
                        "window_start": getattr(settings, "xhs_batch_window_start", "09:00"),
                        "window_end": getattr(settings, "xhs_batch_window_end", "21:00"),
                        "slots_per_day": compute_slots_per_day(settings),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        if args.dry_run:
            print(json.dumps(build_dry_run_summary(reports), ensure_ascii=False, indent=2))
            return 0

        settings.validate_for_sync()
        summary = sync_reports_to_feishu(
            reports=reports,
            settings=settings,
            profile_table_name=args.profile_table_name,
            works_table_name=args.works_table_name,
            ensure_fields=args.ensure_fields,
            sync_dashboard=args.sync_dashboard,
        )
        if args.project:
            update_project_sync_status(
                urls_file=args.urls_file or "",
                project=args.project,
                state="success",
                message=f"项目「{args.project}」同步完成",
                started_at=started_at,
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                total_accounts=int(summary.get("total_accounts") or 0),
                total_works=int(summary.get("total_works") or 0),
            )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        if args.project:
            error_message = build_project_sync_error_message(project=args.project, error=exc)
            update_project_sync_status(
                urls_file=args.urls_file or "",
                project=args.project,
                state="error",
                message=error_message,
                started_at=started_at,
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                last_error=str(exc),
            )
        raise


def build_project_sync_error_message(*, project: str, error: Exception) -> str:
    project_name = str(project or "").strip() or "未分组"
    text = str(error or "").strip()
    if "FieldNameNotFound" in text:
        return f"项目「{project_name}」飞书上传失败：排行榜表缺少字段"
    if "RolePermNotAllow" in text or "403" in text:
        return f"项目「{project_name}」飞书上传失败：当前应用没有足够权限"
    if "tenant_access_token" in text or "缺少飞书配置" in text:
        return f"项目「{project_name}」飞书上传失败：凭证或配置异常"
    if "批量抓取没有成功结果" in text:
        return f"项目「{project_name}」抓取失败：本轮没有成功账号"
    if "登录页" in text or "未配置登录态" in text or "当前登录态不可用" in text:
        return f"项目「{project_name}」抓取失败：登录态异常"
    if "timed out" in text or "Connection aborted" in text or "RemoteDisconnected" in text:
        return f"项目「{project_name}」抓取或上传失败：网络连接异常"
    return f"项目「{project_name}」同步失败"


def load_reports_for_sync(
    *,
    settings,
    explicit_urls: List[str],
    raw_text: str,
    urls_file: Optional[str],
    project: str,
    report_json: Optional[str],
    scheduled: bool = False,
    force_full: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    if report_json:
        return load_reports_from_json(report_json)

    url_entries = normalize_profile_url_entries(explicit_urls, raw_text, urls_file)
    if str(project or "").strip():
        project_name = str(project).strip()
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
            project=project,
        )
        if not url_entries:
            return []
    project_by_url = {
        normalize_profile_url(str(item.get("url") or "")): str(item.get("project") or "").strip()
        for item in url_entries
        if str(item.get("url") or "").strip()
    }
    urls = [item["url"] for item in url_entries]
    if not urls:
        raise ValueError("没有找到可用的小红书账号主页链接")
    date_text = datetime.now().astimezone().date().isoformat()
    resume_path = _resolve_batch_resume_path(
        settings=settings,
        project=project,
        urls_file=urls_file,
        scheduled=scheduled,
    )
    resumed_reports = (
        []
        if force_full
        else _load_batch_resume_reports(
            path=resume_path,
            date_text=date_text,
            project=project,
            scheduled=scheduled,
            active_urls=urls,
        )
    )
    resumed_url_keys = {_batch_resume_key(report) for report in resumed_reports if _batch_resume_key(report)}
    pending_entries = [item for item in url_entries if item["url"] not in resumed_url_keys]

    merged_resumed_reports = list(resumed_reports)

    def _persist_resume_progress(payload: Dict[str, Any]) -> None:
        nonlocal merged_resumed_reports
        if progress_callback is not None:
            progress_callback(payload)
        if str(payload.get("phase") or "").strip() != "collect":
            return
        if str(payload.get("status") or "").strip() != "success":
            return
        raw_item = payload.get("raw_item")
        if not isinstance(raw_item, dict):
            return
        report = normalize_batch_item_to_report(
            raw_item,
            project=project_by_url.get(
                normalize_profile_url(
                    str(
                        raw_item.get("requested_url")
                        or raw_item.get("final_url")
                        or (raw_item.get("profile") or {}).get("profile_url")
                        or ""
                    )
                ),
                "",
            ),
        )
        next_reports = _merge_batch_resume_reports(merged_resumed_reports + [report])
        if len(next_reports) == len(merged_resumed_reports):
            return
        merged_resumed_reports = next_reports
        if not force_full:
            _write_batch_resume_reports(
                path=resume_path,
                date_text=date_text,
                project=project,
                scheduled=scheduled,
                reports=merged_resumed_reports,
            )

    items = []
    if pending_entries:
        items = collect_profile_reports_with_progress(
            urls=[item["url"] for item in pending_entries],
            url_entries=pending_entries,
            settings=settings,
            progress_callback=_persist_resume_progress,
        )
    elif progress_callback is not None:
        progress_callback(
            {
                "phase": "collect",
                "current": len(merged_resumed_reports),
                "total": len(url_entries),
                "status": "success",
                "account": "",
                "works": 0,
                "success_count": len(merged_resumed_reports),
                "failed_count": 0,
            }
        )
    reports: List[Dict[str, Any]] = []
    reports.extend(merged_resumed_reports)
    for item in items:
        if item.get("status") != "success":
            continue
        report = normalize_batch_item_to_report(
            item,
            project=project_by_url.get(
                normalize_profile_url(
                    str(
                        item.get("requested_url")
                        or item.get("final_url")
                        or (item.get("profile") or {}).get("profile_url")
                        or ""
                    )
                ),
                "",
            ),
        )
        if not _report_matches_requested_profile(report):
            continue
        reports.append(report)
    reports = _merge_batch_resume_reports(reports)
    if reports and not force_full:
        _write_batch_resume_reports(
            path=resume_path,
            date_text=date_text,
            project=project,
            scheduled=scheduled,
            reports=reports,
        )
    if not reports and scheduled:
        return []
    if not reports:
        raise ValueError("批量抓取没有成功结果，无法同步到飞书")
    return reports


def _resolve_batch_resume_path(*, settings, project: str, urls_file: Optional[str], scheduled: bool) -> Path:
    scope_name = str(project or "").strip()
    if not scope_name and urls_file:
        scope_name = Path(urls_file).expanduser().resolve().stem
    slug = slugify_project_name(scope_name or "adhoc")
    mode = "scheduled" if scheduled else "manual"
    return resolve_project_cache_dir(settings) / ".collection_resume" / f"{slug}-{mode}-reports.json"


def _batch_resume_key(report: Dict[str, Any]) -> str:
    profile = report.get("profile") or {}
    return (
        normalize_profile_url(str(report.get("source_url") or profile.get("profile_url") or ""))
        or str(profile.get("profile_user_id") or "").strip()
    )


def _merge_batch_resume_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    ordered_keys: List[str] = []
    for report in reports:
        key = _batch_resume_key(report)
        if not key:
            continue
        if key not in merged:
            ordered_keys.append(key)
        merged[key] = dict(report)
    return [merged[key] for key in ordered_keys]


def _load_batch_resume_reports(
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
    reports = _merge_batch_resume_reports(list(payload.get("reports") or []))
    if not active_url_set:
        return reports
    return [report for report in reports if _batch_resume_key(report) in active_url_set]


def _write_batch_resume_reports(
    *,
    path: Path,
    date_text: str,
    project: str,
    scheduled: bool,
    reports: List[Dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_text,
        "project": str(project or "").strip(),
        "scheduled": bool(scheduled),
        "reports": _merge_batch_resume_reports(reports),
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_profile_user_id_from_url(url: str) -> str:
    match = PROFILE_ID_FROM_URL_PATTERN.search(str(url or ""))
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _report_matches_requested_profile(report: Dict[str, Any]) -> bool:
    profile = report.get("profile") or {}
    source_url = str(report.get("source_url") or "").strip()
    requested_account_id = _extract_profile_user_id_from_url(source_url)
    actual_account_id = str(profile.get("profile_user_id") or "").strip()
    if requested_account_id and actual_account_id and requested_account_id != actual_account_id:
        return False
    return True


def load_reports_from_json(path_text: str) -> List[Dict[str, Any]]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        items = payload.get("items") or []
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError(f"不支持的批量报告结构: {path}")

    reports: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict) or item.get("status") != "success":
            continue
        report = normalize_batch_item_to_report(item)
        reports.append(report)
    if not reports:
        raise ValueError("批量报告里没有可同步的成功记录")
    return reports


def normalize_batch_item_to_report(item: Dict[str, Any], *, project: str = "") -> Dict[str, Any]:
    profile = dict(item.get("profile") or {})
    works = [dict(work) for work in (item.get("works") or []) if isinstance(work, dict)]
    captured_at = str(item.get("captured_at") or "").strip() or datetime.now().astimezone().isoformat(timespec="seconds")
    final_url = str(item.get("final_url") or item.get("requested_url") or profile.get("profile_url") or "").strip()
    if final_url and not profile.get("profile_url"):
        profile["profile_url"] = final_url
    return {
        "captured_at": captured_at,
        "source_url": str(item.get("requested_url") or final_url or profile.get("profile_url") or "").strip(),
        "project": str(project or item.get("project") or "").strip(),
        "profile": profile,
        "works": works,
    }


def build_dry_run_summary(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    items = []
    total_works = 0
    for report in reports:
        profile = report.get("profile") or {}
        works = report.get("works") or []
        total_works += len(works)
        items.append(
            {
                "账号": profile.get("nickname") or profile.get("profile_user_id") or "",
                "账号ID": profile.get("profile_user_id") or "",
                "粉丝数": profile.get("fans_count_text") or "",
                "获赞收藏": profile.get("interaction_count_text") or "",
                "作品数": len(works),
                "头部作品": [work.get("title_copy") or "" for work in works[:3]],
            }
        )
    return {
        "total_accounts": len(reports),
        "total_works": total_works,
        "items": items,
    }


def sync_reports_to_feishu(
    *,
    reports: List[Dict[str, Any]],
    settings,
    profile_table_name: str,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    total_reports = len(reports)
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": total_reports,
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
            }
        )
    cache_result = write_project_cache_bundle(reports=reports, settings=settings)

    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    project_ranking_results = sync_project_ranking_tables(
        reports=reports,
        settings=ranking_settings,
    )

    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": total_reports,
                "total": total_reports,
                "account": "",
                "works": sum(len(report.get("works") or []) for report in reports),
                "success_count": total_reports,
                "failed_count": 0,
            }
        )

    return {
        "profile_table_id": "",
        "works_table_id": "",
        "works_calendar_table_id": "",
        "total_accounts": total_reports,
        "successful_accounts": total_reports,
        "failed_accounts": 0,
        "total_works": sum(len(report.get("works") or []) for report in reports),
        "deduped_profiles": 0,
        "deduped_works": 0,
        "dashboard_synced": 0,
        "dashboard_portal": None,
        "single_work_rankings": project_ranking_results,
        "comment_alerts": {"skipped": True, "reason": "已切换为仅上传排行榜"},
        "skipped_profiles": 0,
        "skipped_works": 0,
        "skipped_work_calendars": 0,
        "cache": cache_result,
        "items": [
            {
                "账号": (report.get("profile") or {}).get("nickname") or "",
                "账号ID": (report.get("profile") or {}).get("profile_user_id") or "",
                "summary_action": "cached",
                "works_synced": len(report.get("works") or []),
                "sample_works": [],
            }
            for report in reports
        ],
    }


def sync_project_ranking_tables(
    *,
    reports: List[Dict[str, Any]],
    settings,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    return sync_project_rankings_into_single_table(reports=reports, settings=settings, progress_callback=progress_callback)


def sync_project_rankings_into_single_table(
    *,
    reports: List[Dict[str, Any]],
    settings,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    grouped_reports: Dict[str, List[Dict[str, Any]]] = {}
    for report in reports:
        project_name = str(report.get("project") or "").strip() or "未分组"
        grouped_reports.setdefault(project_name, []).append(report)

    grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
    rank_limit = max(1, int(getattr(settings, "feishu_ranking_upload_limit", 30) or 30))
    for project_name, project_reports in sorted(grouped_reports.items(), key=lambda item: item[0]):
        rankings = build_single_work_rankings(reports=project_reports, history_index={})
        project_rows: List[Dict[str, Any]] = []
        for rank_type in (LIKE_RANK_TYPE, COMMENT_RANK_TYPE):
            ranked_items = (rankings.get(rank_type) or [])[:rank_limit]
            for rank, item in enumerate(ranked_items, start=1):
                project_rows.append(build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank))
        grouped_rows[project_name] = project_rows

    return sync_project_ranking_rows_into_single_table(grouped_rows=grouped_rows, settings=settings, progress_callback=progress_callback)


def has_cached_project_rankings(*, settings, project: str = "") -> bool:
    return has_cached_project_upload_payload(settings=settings, project=project, include_calendar=True, include_rankings=True)


def has_cached_project_upload_payload(
    *,
    settings,
    project: str = "",
    include_calendar: bool = True,
    include_rankings: bool = True,
) -> bool:
    cache_dir = resolve_project_cache_dir(settings)
    if not cache_dir.exists():
        return False
    target_slug = slugify_project_name(project) if str(project or "").strip() else ""
    for path in cache_dir.iterdir():
        if not path.is_dir():
            continue
        if target_slug and path.name != target_slug:
            continue
        has_calendar = include_calendar and (path / "calendar_rows.json").exists()
        has_rankings = include_rankings and (path / "ranking_rows.json").exists()
        if has_calendar or has_rankings:
            return True
    if include_rankings and has_export_review_snapshots(project=project):
        return True
    return False


def resolve_export_review_root(export_dir: str = "") -> Path:
    raw = str(export_dir or EXPORT_REVIEW_ROOT_DIR).strip() or EXPORT_REVIEW_ROOT_DIR
    return Path(raw).expanduser().resolve()


def has_export_review_snapshots(*, project: str = "", export_dir: str = "") -> bool:
    root = resolve_export_review_root(export_dir)
    if not root.exists():
        return False
    target_project = str(project or "").strip()
    for summary_path in sorted(root.glob("*/*/项目导出摘要.json")):
        payload = load_json_file(summary_path)
        if not isinstance(payload, dict):
            continue
        project_name = str(payload.get("project") or summary_path.parent.parent.name).strip()
        if target_project and project_name != target_project:
            continue
        return True
    return False


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_export_review_key(
    *,
    snapshot_slug: str,
    project: str,
    account_id: str,
    metric_label: str,
    work_url: str,
    title: str,
) -> str:
    source = "|".join(
        [
            str(snapshot_slug or "").strip(),
            str(project or "").strip(),
            str(account_id or "").strip(),
            str(metric_label or "").strip(),
            str(work_url or "").strip(),
            str(title or "").strip(),
        ]
    )
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    return f"{snapshot_slug}|{project}|{account_id}|{metric_label}|{digest}"


def _link_field(text: str, url: Any) -> Dict[str, str] | str:
    url_text = str(url or "").strip()
    if not url_text:
        return ""
    return {"text": str(text or url_text).strip() or url_text, "link": url_text}


def load_export_review_rows(
    *,
    project: str = "",
    export_dir: str = "",
    settings: Any = None,
    latest_only: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    root = resolve_export_review_root(export_dir)
    target_project = str(project or "").strip()
    grouped_rows: Dict[str, List[Dict[str, Any]]] = {
        DAILY_LIKE_REVIEW_TABLE_NAME: [],
        DAILY_COMMENT_REVIEW_TABLE_NAME: [],
    }
    if not root.exists():
        return grouped_rows

    runtime_settings = settings or load_settings("xhs_feishu_monitor/.env")
    review_days = max(1, int(getattr(runtime_settings, "feishu_review_upload_days", 14) or 14))
    per_account_limit = max(1, int(getattr(runtime_settings, "feishu_review_per_account_limit", 10) or 10))
    today = datetime.now().astimezone().date()
    latest_daily_summaries: Dict[tuple[str, str], tuple[str, Path, Dict[str, Any]]] = {}

    for summary_path in sorted(root.glob("*/*/项目导出摘要.json")):
        try:
            payload = load_json_file(summary_path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        project_name = str(payload.get("project") or summary_path.parent.parent.name).strip() or "未分组"
        if target_project and project_name != target_project:
            continue
        snapshot_slug = str(payload.get("snapshot_slug") or summary_path.parent.name).strip()
        snapshot_date_text = snapshot_slug.split("_", 1)[0] if snapshot_slug else str(payload.get("snapshot_time") or "")[:10]
        try:
            snapshot_date = datetime.strptime(snapshot_date_text, "%Y-%m-%d").date()
        except ValueError:
            continue
        if snapshot_date < (today - timedelta(days=review_days - 1)):
            continue
        key = (project_name, snapshot_date_text)
        previous = latest_daily_summaries.get(key)
        if previous and previous[0] >= snapshot_slug:
            continue
        latest_daily_summaries[key] = (snapshot_slug, summary_path, payload)

    selected_daily_summaries = latest_daily_summaries
    if latest_only:
        latest_per_project: Dict[str, tuple[str, str, Path, Dict[str, Any]]] = {}
        for (project_name, snapshot_date_text), (snapshot_slug, summary_path, payload) in latest_daily_summaries.items():
            current = latest_per_project.get(project_name)
            candidate = (snapshot_date_text, snapshot_slug, summary_path, payload)
            if current is None or candidate[:2] > current[:2]:
                latest_per_project[project_name] = candidate
        selected_daily_summaries = {
            (project_name, snapshot_date_text): (snapshot_slug, summary_path, payload)
            for project_name, (snapshot_date_text, snapshot_slug, summary_path, payload) in latest_per_project.items()
        }

    for (_project_name, _snapshot_date_text), (snapshot_slug, summary_path, payload) in sorted(selected_daily_summaries.items(), key=lambda item: (item[0][0], item[0][1])):
        project_name = str(payload.get("project") or summary_path.parent.parent.name).strip() or "未分组"
        snapshot_time = str(payload.get("snapshot_time") or "").strip()
        snapshot_ms = to_ms(snapshot_time)
        snapshot_date_text = snapshot_slug.split("_", 1)[0] if snapshot_slug else snapshot_time[:10]
        snapshot_date_ms = to_ms(f"{snapshot_date_text}T00:00:00+08:00") if snapshot_date_text else 0
        export_dir_text = str(payload.get("export_dir") or summary_path.parent).strip()
        for account_summary in payload.get("accounts") or []:
            if not isinstance(account_summary, dict):
                continue
            account_id = str(account_summary.get("account_id") or "").strip()
            account_name = str(account_summary.get("account") or account_id).strip() or account_id
            files = account_summary.get("files") or {}
            for table_name, metric_label, value_field, file_key in (
                (DAILY_LIKE_REVIEW_TABLE_NAME, "点赞", "点赞数", "like_json"),
                (DAILY_COMMENT_REVIEW_TABLE_NAME, "评论", "评论数", "comment_json"),
            ):
                file_path = Path(str(files.get(file_key) or "").strip())
                if not file_path.exists():
                    continue
                try:
                    ranking_rows = load_json_file(file_path)
                except Exception:
                    continue
                if not isinstance(ranking_rows, list):
                    continue
                for row in ranking_rows[:per_account_limit]:
                    if not isinstance(row, dict):
                        continue
                    work_url = str(row.get("作品链接") or "").strip()
                    title = str(row.get("标题") or "").strip()
                    review_key = build_export_review_key(
                        snapshot_slug=snapshot_slug,
                        project=project_name,
                        account_id=account_id,
                        metric_label=metric_label,
                        work_url=work_url,
                        title=title,
                    )
                    value = to_optional_int(row.get("数值")) or 0
                    fields: Dict[str, Any] = {
                        "复盘键": review_key,
                        "日期": snapshot_date_ms,
                        "日期文本": snapshot_date_text,
                        "快照时间": snapshot_ms,
                        "快照批次": snapshot_slug,
                        "项目": project_name,
                        "账号ID": account_id,
                        "账号": account_name,
                        "账号内排名": to_optional_int(row.get("排名")) or 0,
                        "标题": title,
                        "摘要": str(row.get("摘要") or "").strip(),
                        "作品链接": _link_field(title or "作品详情", work_url),
                        "主页链接": _link_field(account_name or "账号主页", row.get("主页链接")),
                        "封面图": _link_field("封面图", row.get("封面图")),
                        "追踪状态": str(row.get("追踪状态") or "").strip(),
                        "首次入池日期": str(row.get("首次入池日期") or "").strip(),
                        "快照目录": export_dir_text,
                        "口径说明": PRODUCT_METRIC_NOTE,
                        value_field: value,
                    }
                    if table_name == DAILY_COMMENT_REVIEW_TABLE_NAME:
                        fields["评论口径"] = str(row.get("评论口径") or "未知").strip() or "未知"
                    grouped_rows[table_name].append({key: value for key, value in fields.items() if value not in ("", None)})
    return grouped_rows


def sync_export_review_tables_to_feishu(
    *,
    settings,
    project: str = "",
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    latest_only: bool = False,
) -> Dict[str, Any]:
    grouped_rows = load_export_review_rows(project=project, settings=settings, latest_only=latest_only)
    total_rows = sum(len(rows) for rows in grouped_rows.values())
    if total_rows <= 0:
        if project:
            raise ValueError(f"项目「{project}」当前没有可上传的导出复盘快照")
        raise ValueError("当前没有可上传的导出复盘快照")

    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取复盘表结构",
            }
        )

    summary: Dict[str, Any] = {
        "daily_like_review_created": 0,
        "daily_like_review_updated": 0,
        "daily_like_review_skipped": 0,
        "daily_like_review_deleted": 0,
        "daily_comment_review_created": 0,
        "daily_comment_review_updated": 0,
        "daily_comment_review_skipped": 0,
        "daily_comment_review_deleted": 0,
    }
    processed = 0
    success_count = 0
    managed_projects = {str(project or "").strip() or "未分组" for rows in grouped_rows.values() for project in [None] if False}
    for rows in grouped_rows.values():
        for row in rows:
            managed_projects.add(str(row.get("项目") or "").strip() or "未分组")

    for table_name, field_specs, value_prefix in (
        (DAILY_LIKE_REVIEW_TABLE_NAME, DAILY_LIKE_REVIEW_FIELDS, "daily_like_review"),
        (DAILY_COMMENT_REVIEW_TABLE_NAME, DAILY_COMMENT_REVIEW_FIELDS, "daily_comment_review"),
    ):
        rows = grouped_rows.get(table_name) or []
        table_id = ensure_named_table(
            tables_client=tables_client,
            table_name=table_name,
            default_view_name=table_name,
            fields=field_specs,
        )
        client = FeishuBitableClient(replace(ranking_settings, feishu_table_id=table_id))
        client.ensure_fields(field_specs)
        supported_field_names = {
            str(item.get("field_name") or "").strip()
            for item in client.list_fields()
            if str(item.get("field_name") or "").strip()
        }
        desired_supported_field_names = {
            str(spec.get("field_name") or "").strip()
            for spec in field_specs
            if str(spec.get("field_name") or "").strip() in supported_field_names
        }
        state_index = build_record_state_index(
            client,
            unique_field="复盘键",
            field_names=["复盘键", *sorted(desired_supported_field_names)],
        )
        desired_fields: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            fields = {key: value for key, value in dict(row).items() if not supported_field_names or key in supported_field_names}
            row_key = str(fields.get("复盘键") or "").strip()
            if row_key:
                desired_fields[row_key] = fields

        for row_key, fields in desired_fields.items():
            existing = state_index.pop(row_key, None)
            record_id = str((existing or {}).get("record_id") or "").strip()
            if record_id:
                if fields_match((existing or {}).get("fields") or {}, fields, ignore_fields=["快照时间"]):
                    summary[f"{value_prefix}_skipped"] += 1
                else:
                    client.update_record(record_id, fields)
                    summary[f"{value_prefix}_updated"] += 1
                    success_count += 1
            else:
                client.create_record(fields)
                summary[f"{value_prefix}_created"] += 1
                success_count += 1
            processed += 1
            if progress_callback is not None:
                progress_callback(
                    {
                        "phase": "sync",
                        "current": processed,
                        "total": max(1, total_rows),
                        "account": str(fields.get("账号") or fields.get("项目") or ""),
                        "works": 0,
                        "success_count": success_count,
                        "failed_count": 0,
                        "status": f"写入{table_name}",
                    }
                )

        for row_key, existing in state_index.items():
            record_id = str((existing or {}).get("record_id") or "").strip()
            fields = (existing or {}).get("fields") or {}
            project_name = str(fields.get("项目") or "").strip() or "未分组"
            if not record_id or project_name not in managed_projects:
                continue
            client.delete_record(record_id)
            summary[f"{value_prefix}_deleted"] += 1

        summary[f"{value_prefix}_table_name"] = table_name
        summary[f"{value_prefix}_table_id"] = table_id

    summary["single_work_ranking_created"] = summary["daily_like_review_created"] + summary["daily_comment_review_created"]
    summary["single_work_ranking_updated"] = summary["daily_like_review_updated"] + summary["daily_comment_review_updated"]
    summary["single_work_ranking_skipped"] = summary["daily_like_review_skipped"] + summary["daily_comment_review_skipped"]
    summary["single_work_ranking_deleted"] = summary["daily_like_review_deleted"] + summary["daily_comment_review_deleted"]
    summary["projects"] = sorted(
        {
            str(row.get("项目") or "").strip()
            for rows in grouped_rows.values()
            for row in rows
            if str(row.get("项目") or "").strip()
        }
    )
    summary["project_count"] = len(summary["projects"])
    return summary


def ensure_project_dashboard_views(*, settings, projects: List[str]) -> Dict[str, Any]:
    normalized_projects = sorted({str(item or "").strip() for item in projects if str(item or "").strip()})
    if not normalized_projects:
        return {"projects": [], "view_count": 0, "views": []}
    ranking_app_token = str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
    base_app_token = str(getattr(settings, "feishu_bitable_app_token", "") or "").strip()
    if not ranking_app_token and not base_app_token:
        return {"projects": normalized_projects, "view_count": 0, "views": []}

    ranking_settings = (
        replace(settings, feishu_bitable_app_token=ranking_app_token)
        if ranking_app_token
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    table_ids = {
        str(item.get("name") or "").strip(): str(item.get("table_id") or "").strip()
        for item in tables_client.list_tables()
        if str(item.get("name") or "").strip() and str(item.get("table_id") or "").strip()
    }
    ensured_views: List[str] = []
    for project_name in normalized_projects:
        for table_name, suffix, view_type in (*PROJECT_DASHBOARD_VIEW_SPECS, *PROJECT_DASHBOARD_AUX_VIEW_SPECS):
            table_id = table_ids.get(table_name)
            if not table_id:
                continue
            view_name = f"{project_name}-{suffix}"
            tables_client.ensure_view(view_name=view_name, view_type=view_type, table_id=table_id)
            ensured_views.append(view_name)
    return {
        "projects": normalized_projects,
        "view_count": len(ensured_views),
        "views": ensured_views,
        "primary_views": [f"{project_name}-{suffix}" for project_name in normalized_projects for _table_name, suffix, _view_type in PROJECT_DASHBOARD_VIEW_SPECS],
    }


def rebuild_feishu_review_tables_from_exports(
    *,
    settings,
    project: str = "",
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    target_keep_names = {DAILY_LIKE_REVIEW_TABLE_NAME, DAILY_COMMENT_REVIEW_TABLE_NAME}
    deleted_tables = 0
    cleared_tables = 0
    cleared_records = 0
    if not str(project or "").strip():
        for table in tables_client.list_tables():
            table_name = str(table.get("name") or "").strip()
            table_id = str(table.get("table_id") or "").strip()
            if not table_id or table_name in target_keep_names:
                continue
            if table_name in LEGACY_FEISHU_TABLE_NAMES:
                try:
                    tables_client.delete_table(table_id)
                    deleted_tables += 1
                except Exception as exc:
                    if not is_feishu_forbidden_error(exc):
                        raise
                    cleared_records += clear_feishu_table_records(settings=ranking_settings, table_id=table_id)
                    cleared_tables += 1
    summary = sync_export_review_tables_to_feishu(
        settings=settings,
        project=project,
        progress_callback=progress_callback,
    )
    summary["deleted_legacy_tables"] = deleted_tables
    summary["cleared_legacy_tables"] = cleared_tables
    summary["cleared_legacy_records"] = cleared_records
    return summary


def sync_cached_project_rankings_to_feishu(
    *,
    settings,
    project: str = "",
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    upload_calendar: bool = True,
    upload_rankings: bool = True,
    latest_only: bool = False,
) -> Dict[str, Any]:
    if not upload_calendar and not upload_rankings:
        raise ValueError("至少选择一种飞书上传内容")

    calendar_summary: Dict[str, Any] = {}
    if upload_calendar:
        calendar_summary = sync_cached_project_calendar_to_feishu(
            settings=settings,
            project=project,
            progress_callback=progress_callback,
        )
    ranking_summary: Dict[str, Any] = {}
    if upload_rankings:
        ranking_summary = sync_export_review_tables_to_feishu(
            settings=settings,
            project=project,
            progress_callback=progress_callback,
            latest_only=latest_only,
        )

    if not ranking_summary and not calendar_summary.get("calendar_project_count"):
        if project:
            raise ValueError(f"项目「{project}」当前没有可上传的复盘快照或日历留底")
        raise ValueError("当前没有可上传的复盘快照或日历留底")
    dashboard_projects = sorted(
        {
            *[str(item).strip() for item in calendar_summary.get("calendar_projects", []) if str(item).strip()],
            *[str(item).strip() for item in ranking_summary.get("projects", []) if str(item).strip()],
            *([str(project).strip()] if str(project or "").strip() else []),
        }
    )
    dashboard_summary = ensure_project_dashboard_views(settings=settings, projects=dashboard_projects)
    return {**calendar_summary, **ranking_summary, "dashboard_views": dashboard_summary}


def sync_cached_project_calendar_to_feishu(
    *,
    settings,
    project: str = "",
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    target_project = str(project or "").strip()
    target_slug = slugify_project_name(target_project) if target_project else ""
    grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
    if not cache_dir.exists():
        return {
            "calendar_table_name": DASHBOARD_CALENDAR_TABLE_NAME,
            "calendar_project_count": 0,
            "calendar_created": 0,
            "calendar_updated": 0,
            "calendar_skipped": 0,
        }

    for path in sorted(cache_dir.iterdir(), key=lambda item: item.name):
        if not path.is_dir():
            continue
        if target_slug and path.name != target_slug:
            continue
        calendar_path = path / "calendar_rows.json"
        if not calendar_path.exists():
            continue
        payload = json.loads(calendar_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            continue
        project_name = target_project or path.name
        rows = [dict(item) for item in payload if isinstance(item, dict) and str(item.get("日历键") or "").strip()]
        if rows:
            grouped_rows[project_name] = rows

    if not grouped_rows:
        return {
            "calendar_table_name": DASHBOARD_CALENDAR_TABLE_NAME,
            "calendar_project_count": 0,
            "calendar_created": 0,
            "calendar_updated": 0,
            "calendar_skipped": 0,
            "calendar_projects": [],
        }

    total_rows = sum(len(rows) for rows in grouped_rows.values())
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取日历留底表结构",
            }
        )
    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=DASHBOARD_CALENDAR_TABLE_NAME,
        fields=DASHBOARD_CALENDAR_FIELDS,
        default_view_name="日历留底",
    )

    client = FeishuBitableClient(replace(ranking_settings, feishu_table_id=table_id))
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取日历留底字段",
            }
        )
    client.ensure_fields(DASHBOARD_CALENDAR_FIELDS)
    supported_field_names = {
        str(item.get("field_name") or "").strip()
        for item in client.list_fields()
        if str(item.get("field_name") or "").strip()
    }
    desired_supported_field_names = {
        "日历键",
        "账号",
        "备注",
        "数据更新时间",
    }
    desired_supported_field_names = [field_name for field_name in desired_supported_field_names if field_name in supported_field_names]
    record_index = build_record_id_index(client, unique_field="日历键")
    record_state_index = build_record_state_index(
        client,
        unique_field="日历键",
        field_names=desired_supported_field_names,
    )

    created = 0
    updated = 0
    skipped = 0
    processed = 0
    for project_name, rows in sorted(grouped_rows.items(), key=lambda item: item[0]):
        for raw_fields in rows:
            fields = dict(raw_fields)
            if supported_field_names and "项目" not in supported_field_names and "备注" in supported_field_names:
                remark = str(fields.get("备注") or "").strip()
                project_remark = f"项目：{project_name}"
                if project_remark not in remark:
                    fields["备注"] = f"{remark} | {project_remark}".strip(" |") if remark else project_remark
            if "备注" in supported_field_names:
                remark = str(fields.get("备注") or "").strip()
                if PRODUCT_METRIC_NOTE not in remark:
                    fields["备注"] = f"{remark} | {PRODUCT_METRIC_NOTE}".strip(" |") if remark else PRODUCT_METRIC_NOTE
            if supported_field_names:
                fields = {key: value for key, value in fields.items() if key in supported_field_names}
            action, _record_id = upsert_record_with_index(
                client=client,
                record_index=record_index,
                record_state_index=record_state_index,
                unique_field="日历键",
                unique_value=fields.get("日历键"),
                fields=fields,
                compare_ignore_fields=["数据更新时间"],
            )
            if action == "created":
                created += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1
            processed += 1
            if progress_callback is not None:
                progress_callback(
                    {
                        "phase": "sync",
                        "current": processed,
                        "total": max(1, total_rows),
                        "account": str(fields.get("账号") or project_name),
                        "works": 0,
                        "success_count": created + updated,
                        "failed_count": 0,
                        "status": "写入日历留底",
                    }
                )

    return {
        "calendar_table_name": DASHBOARD_CALENDAR_TABLE_NAME,
        "calendar_table_id": table_id,
        "calendar_project_count": len(grouped_rows),
        "calendar_projects": sorted(grouped_rows.keys()),
        "calendar_created": created,
        "calendar_updated": updated,
        "calendar_skipped": skipped,
    }


def sync_cached_project_account_rankings_to_feishu(
    *,
    settings,
    project: str = "",
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    cache_dir = resolve_project_cache_dir(settings)
    target_project = str(project or "").strip()
    target_slug = slugify_project_name(target_project) if target_project else ""
    grouped_calendar_rows: Dict[str, List[Dict[str, Any]]] = {}
    if not cache_dir.exists():
        return {
            "project_account_ranking_table_name": PROJECT_ACCOUNT_RANKING_TABLE_NAME,
            "project_account_ranking_project_count": 0,
            "project_account_ranking_created": 0,
            "project_account_ranking_updated": 0,
            "project_account_ranking_skipped": 0,
            "project_account_ranking_deleted": 0,
        }

    for path in sorted(cache_dir.iterdir(), key=lambda item: item.name):
        if not path.is_dir():
            continue
        if target_slug and path.name != target_slug:
            continue
        calendar_path = path / "calendar_rows.json"
        if not calendar_path.exists():
            continue
        payload = json.loads(calendar_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            continue
        project_name = target_project or path.name
        rows = [dict(item) for item in payload if isinstance(item, dict) and str(item.get("账号ID") or "").strip()]
        if rows:
            grouped_calendar_rows[project_name] = rows

    if not grouped_calendar_rows:
        return {
            "project_account_ranking_table_name": PROJECT_ACCOUNT_RANKING_TABLE_NAME,
            "project_account_ranking_project_count": 0,
            "project_account_ranking_created": 0,
            "project_account_ranking_updated": 0,
            "project_account_ranking_skipped": 0,
            "project_account_ranking_deleted": 0,
        }

    grouped_rows = build_project_account_ranking_rows(grouped_calendar_rows)
    total_rows = sum(len(rows) for rows in grouped_rows.values())
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取项目账号榜表结构",
            }
        )
    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=PROJECT_ACCOUNT_RANKING_TABLE_NAME,
        default_view_name="项目账号排行",
        fields=PROJECT_ACCOUNT_RANKING_FIELDS,
    )
    client = FeishuBitableClient(replace(ranking_settings, feishu_table_id=table_id))
    client.ensure_fields(PROJECT_ACCOUNT_RANKING_FIELDS)
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取项目账号榜字段",
            }
        )
    supported_field_names = {
        str(item.get("field_name") or "").strip()
        for item in client.list_fields()
        if str(item.get("field_name") or "").strip()
    }
    desired_supported_field_names = {
        str(spec.get("field_name") or "").strip()
        for spec in PROJECT_ACCOUNT_RANKING_FIELDS
        if str(spec.get("field_name") or "").strip() in supported_field_names
    }
    record_index = build_record_id_index(client, unique_field="项目账号榜单键")
    record_state_index = build_record_state_index(
        client,
        unique_field="项目账号榜单键",
        field_names=["项目账号榜单键", *sorted(desired_supported_field_names)],
    )

    desired_keys: set[str] = set()
    created = 0
    updated = 0
    skipped = 0
    processed = 0
    for project_name, rows in sorted(grouped_rows.items(), key=lambda item: item[0]):
        for raw_fields in rows:
            fields = {key: value for key, value in dict(raw_fields).items() if not supported_field_names or key in supported_field_names}
            row_key = str(fields.get("项目账号榜单键") or "").strip()
            if not row_key:
                continue
            desired_keys.add(row_key)
            action, _record_id = upsert_record_with_index(
                client=client,
                record_index=record_index,
                record_state_index=record_state_index,
                unique_field="项目账号榜单键",
                unique_value=row_key,
                fields=fields,
                compare_ignore_fields=["数据更新时间"],
            )
            if action == "created":
                created += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1
            processed += 1
            if progress_callback is not None:
                progress_callback(
                    {
                        "phase": "sync",
                        "current": processed,
                        "total": max(1, total_rows),
                        "account": str(fields.get("账号") or project_name),
                        "works": 0,
                        "success_count": created + updated,
                        "failed_count": 0,
                        "status": "写入项目账号榜",
                    }
                )

    deleted = 0
    for row_key, existing in record_state_index.items():
        record_id = str((existing or {}).get("record_id") or "").strip()
        fields = (existing or {}).get("fields") or {}
        project_name = str(fields.get("项目") or "").strip()
        if not record_id or row_key in desired_keys:
            continue
        if project_name and project_name in grouped_rows:
            client.delete_record(record_id)
            deleted += 1

    return {
        "project_account_ranking_table_name": PROJECT_ACCOUNT_RANKING_TABLE_NAME,
        "project_account_ranking_table_id": table_id,
        "project_account_ranking_project_count": len(grouped_rows),
        "project_account_ranking_created": created,
        "project_account_ranking_updated": updated,
        "project_account_ranking_skipped": skipped,
        "project_account_ranking_deleted": deleted,
    }


def build_project_account_ranking_rows(grouped_calendar_rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
    for project_name, calendar_rows in grouped_calendar_rows.items():
        latest_by_account: Dict[str, Dict[str, Any]] = {}
        for row in calendar_rows:
            account_id = str(row.get("账号ID") or "").strip()
            if not account_id:
                continue
            current_date = str(row.get("日期文本") or "")
            current_updated = to_optional_int(row.get("数据更新时间")) or 0
            existing = latest_by_account.get(account_id) or {}
            existing_date = str(existing.get("日期文本") or "")
            existing_updated = to_optional_int(existing.get("数据更新时间")) or 0
            if current_date > existing_date or (current_date == existing_date and current_updated >= existing_updated):
                latest_by_account[account_id] = dict(row)

        latest_rows = list(latest_by_account.values())
        ranking_specs = {
            "点赞排行": "首页总点赞",
            "评论排行": "首页总评论",
        }
        rank_maps = {ranking_type: build_dense_rank_map(latest_rows, value_field=value_field) for ranking_type, value_field in ranking_specs.items()}

        project_account_rows: List[Dict[str, Any]] = []
        for ranking_type, value_field in ranking_specs.items():
            ranking_rows = sorted(
                latest_rows,
                key=lambda item: (
                    -(to_optional_int(item.get(value_field)) or 0),
                    str(item.get("账号") or item.get("账号ID") or ""),
                ),
            )
            rank_map = rank_maps[ranking_type]
            for row in ranking_rows:
                account_id = str(row.get("账号ID") or "").strip()
                rank = rank_map.get(account_id) or 0
                fields = {
                    "项目账号榜单键": f"{project_name}|{ranking_type}|{account_id}",
                    "项目": project_name,
                    "榜单类型": ranking_type,
                    "排名": rank,
                    "排序值": to_optional_int(row.get(value_field)) or 0,
                    "账号ID": account_id,
                    "账号": str(row.get("账号") or "").strip(),
                    "主页链接": row.get("主页链接"),
                    "数据更新时间": row.get("数据更新时间"),
                    "日历日期": row.get("日历日期"),
                    "日期文本": row.get("日期文本"),
                    "粉丝数": row.get("粉丝数") or 0,
                    "获赞收藏数": row.get("获赞收藏数") or 0,
                    "首页总点赞": row.get("首页总点赞") or 0,
                    "首页总评论": row.get("首页总评论") or 0,
                    "账号总作品数": row.get("账号总作品数") or 0,
                    "作品数展示": row.get("作品数展示") or "",
                    "头部作品标题": row.get("头部作品标题") or "",
                    "头部作品点赞": row.get("头部作品点赞") or 0,
                    "TOP3作品摘要": row.get("TOP3作品摘要") or "",
                    "口径说明": PRODUCT_METRIC_NOTE,
                    "数据用途": ACCOUNT_RANKING_USAGE_NOTE,
                }
                project_account_rows.append({key: value for key, value in fields.items() if value not in ("", None)})
        grouped_rows[project_name] = project_account_rows
    return grouped_rows


def build_dense_rank_map(rows: List[Dict[str, Any]], *, value_field: str) -> Dict[str, int]:
    ranked = sorted(
        rows,
        key=lambda item: (
            -(to_optional_int(item.get(value_field)) or 0),
            str(item.get("账号") or item.get("账号ID") or ""),
        ),
    )
    rank_map: Dict[str, int] = {}
    current_rank = 0
    previous_value: Optional[int] = None
    for index, row in enumerate(ranked, start=1):
        account_id = str(row.get("账号ID") or "").strip()
        if not account_id:
            continue
        current_value = to_optional_int(row.get(value_field)) or 0
        if previous_value is None or current_value != previous_value:
            current_rank = index
            previous_value = current_value
        rank_map[account_id] = current_rank
    return rank_map


def sync_project_ranking_rows_into_single_table(
    *,
    grouped_rows: Dict[str, List[Dict[str, Any]]],
    settings,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    total_rows = sum(len(rows) for rows in grouped_rows.values())
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取飞书表结构",
            }
        )
    ranking_settings = (
        replace(settings, feishu_bitable_app_token=settings.feishu_ranking_bitable_app_token)
        if str(getattr(settings, "feishu_ranking_bitable_app_token", "") or "").strip()
        else settings
    )
    tables_client = FeishuBitableClient(ranking_settings)
    tables = tables_client.list_tables()
    table_id = ""
    table_name = ""
    for preferred_name in SINGLE_TABLE_RANKING_TABLE_NAMES:
        for table in tables:
            if str(table.get("name") or "").strip() == preferred_name:
                table_id = str(table.get("table_id") or "").strip()
                table_name = preferred_name
                break
        if table_id:
            break
    if not table_id:
        table_id = ensure_named_table(
            tables_client=tables_client,
            table_name=PROJECT_WORK_RANKING_TABLE_NAME,
            default_view_name="项目作品排行",
            fields=DASHBOARD_SINGLE_WORK_RANKING_FIELDS,
        )
        table_name = PROJECT_WORK_RANKING_TABLE_NAME

    client = FeishuBitableClient(replace(ranking_settings, feishu_table_id=table_id))
    if table_name == PROJECT_WORK_RANKING_TABLE_NAME:
        client.ensure_fields(DASHBOARD_SINGLE_WORK_RANKING_FIELDS)
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取飞书字段",
            }
        )
    supported_field_names = {
        str(item.get("field_name") or "").strip()
        for item in client.list_fields()
        if str(item.get("field_name") or "").strip()
    }
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "sync",
                "current": 0,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": 0,
                "failed_count": 0,
                "status": "读取飞书旧记录",
            }
        )
    desired_supported_field_names = {
        str(spec.get("field_name") or "").strip()
        for spec in DASHBOARD_SINGLE_WORK_RANKING_FIELDS
        if str(spec.get("field_name") or "").strip() in supported_field_names
    }
    state_index = build_record_state_index(
        client,
        unique_field="榜单键",
        field_names=["榜单键", *sorted(desired_supported_field_names)],
    )
    desired_fields: Dict[str, Dict[str, Any]] = {}

    for project_name, project_rows in sorted(grouped_rows.items(), key=lambda item: item[0]):
        for raw_fields in project_rows:
            fields = dict(raw_fields or {})
            raw_rank_type = str(fields.get("榜单类型") or "").strip()
            if raw_rank_type not in {LIKE_RANK_TYPE, COMMENT_RANK_TYPE}:
                continue
            standard_rank_type = strip_single_work_prefix(raw_rank_type)
            row_key = str(fields.get("榜单键") or "").strip()
            if not row_key:
                continue
            fields["榜单类型"] = standard_rank_type
            fields["榜单键"] = f"{project_name}|{row_key}"
            fields["文本"] = project_name
            ranking_note = f"{ACCOUNT_RANKING_USAGE_NOTE}；{PRODUCT_METRIC_NOTE}"
            if "榜单摘要" in supported_field_names:
                summary = str(fields.get("榜单摘要") or "").strip()
                if PRODUCT_METRIC_NOTE not in summary:
                    fields["榜单摘要"] = f"{summary} | {PRODUCT_METRIC_NOTE}".strip(" |") if summary else ranking_note
            if supported_field_names and "文本" not in supported_field_names:
                if "卡片标签" in supported_field_names:
                    card_label = str(fields.get("卡片标签") or "").strip()
                    fields["卡片标签"] = f"{project_name} · {card_label}" if card_label else project_name
                elif "榜单摘要" in supported_field_names:
                    summary = str(fields.get("榜单摘要") or "").strip()
                    fields["榜单摘要"] = f"{project_name} | {summary}" if summary else project_name
            if supported_field_names:
                fields = {key: value for key, value in fields.items() if key in supported_field_names}
            desired_fields[str(fields["榜单键"])] = fields

    created = 0
    updated = 0
    skipped = 0
    deleted = 0
    processed = 0
    for row_key, fields in desired_fields.items():
        existing = state_index.pop(row_key, None)
        record_id = str((existing or {}).get("record_id") or "").strip()
        if record_id:
            if fields_match((existing or {}).get("fields") or {}, fields, ignore_fields=["数据更新时间"]):
                skipped += 1
                processed += 1
                if progress_callback is not None:
                    progress_callback(
                        {
                            "phase": "sync",
                            "current": processed,
                            "total": max(1, total_rows),
                            "account": str(fields.get("文本") or ""),
                            "works": 0,
                            "success_count": created + updated,
                            "failed_count": 0,
                            "status": "写入排行榜",
                        }
                    )
                continue
            try:
                client.update_record(record_id, fields)
                updated += 1
            except Exception as exc:
                if is_feishu_forbidden_error(exc):
                    skipped += 1
                    processed += 1
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "phase": "sync",
                                "current": processed,
                                "total": max(1, total_rows),
                                "account": str(fields.get("文本") or ""),
                                "works": 0,
                                "success_count": created + updated,
                                "failed_count": 0,
                                "status": "更新受限，已跳过该记录",
                            }
                        )
                    continue
                raise
            processed += 1
            if progress_callback is not None:
                progress_callback(
                    {
                        "phase": "sync",
                        "current": processed,
                        "total": max(1, total_rows),
                        "account": str(fields.get("文本") or ""),
                        "works": 0,
                        "success_count": created + updated,
                        "failed_count": 0,
                        "status": "写入排行榜",
                    }
                )
            continue
        try:
            client.create_record(fields)
            created += 1
        except Exception as exc:
            if is_feishu_forbidden_error(exc):
                skipped += 1
                processed += 1
                if progress_callback is not None:
                    progress_callback(
                        {
                            "phase": "sync",
                            "current": processed,
                            "total": max(1, total_rows),
                            "account": str(fields.get("文本") or ""),
                            "works": 0,
                            "success_count": created + updated,
                            "failed_count": 0,
                            "status": "创建受限，已跳过该记录",
                        }
                    )
                continue
            raise
        processed += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "sync",
                    "current": processed,
                    "total": max(1, total_rows),
                    "account": str(fields.get("文本") or ""),
                    "works": 0,
                    "success_count": created + updated,
                    "failed_count": 0,
                    "status": "写入排行榜",
                }
            )

    managed_type_suffixes = {"点赞排行", "评论排行"}
    managed_raw_types = {LIKE_RANK_TYPE, COMMENT_RANK_TYPE, "单条第二天增长排行"}
    managed_projects = {str(project_name or "").strip() or "未分组" for project_name in grouped_rows}
    if progress_callback is not None and state_index:
        progress_callback(
            {
                "phase": "sync",
                "current": processed,
                "total": max(1, total_rows),
                "account": "",
                "works": 0,
                "success_count": created + updated,
                "failed_count": 0,
                "status": "清理旧记录",
            }
        )
    for row_key, existing in state_index.items():
        fields = (existing or {}).get("fields") or {}
        rank_type = str(fields.get("榜单类型") or "").strip()
        project_name = str(fields.get("文本") or "").strip() or "未分组"
        record_id = str((existing or {}).get("record_id") or "").strip()
        if not record_id:
            continue
        if project_name not in managed_projects:
            continue
        if rank_type in managed_raw_types or rank_type in managed_type_suffixes or any(rank_type.endswith(suffix) for suffix in managed_type_suffixes):
            try:
                client.delete_record(record_id)
                deleted += 1
            except Exception as exc:
                if is_feishu_forbidden_error(exc):
                    skipped += 1
                    continue
                raise

    return {
        "mode": "single_table_partitioned",
        "table_name": table_name,
        "table_id": table_id,
        "project_count": len(grouped_rows),
        "single_work_ranking_created": created,
        "single_work_ranking_updated": updated,
        "single_work_ranking_skipped": skipped,
        "single_work_ranking_deleted": deleted,
    }


def build_project_ranking_table_name(*, project_name: str, rank_label: str) -> str:
    project_text = str(project_name or "").strip() or "未分组"
    return f"{project_text}-{rank_label}"


def strip_single_work_prefix(rank_type: str) -> str:
    text = str(rank_type or "").strip()
    if text.startswith("单条"):
        return text[2:]
    return text


def build_record_id_index(client: FeishuBitableClient, *, unique_field: str) -> Dict[str, str]:
    index: Dict[str, str] = {}
    for record in client.list_records(page_size=500, field_names=[unique_field]):
        fields = record.get("fields") or {}
        unique_value = normalize_unique_value(fields.get(unique_field))
        record_id = str(record.get("record_id") or "").strip()
        if unique_value and record_id and unique_value not in index:
            index[unique_value] = record_id
    return index


def build_record_state_index(
    client: FeishuBitableClient,
    *,
    unique_field: str,
    field_names: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    normalized_field_names = None
    if field_names:
        seen: set[str] = set()
        normalized_field_names = []
        for field_name in [unique_field, *field_names]:
            normalized = str(field_name or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_field_names.append(normalized)
    for record in client.list_records(page_size=500, field_names=normalized_field_names):
        fields = record.get("fields") or {}
        unique_value = normalize_unique_value(fields.get(unique_field))
        record_id = str(record.get("record_id") or "").strip()
        if unique_value and record_id and unique_value not in index:
            index[unique_value] = {
                "record_id": record_id,
                "fields": dict(fields),
            }
    return index


def upsert_record_with_index(
    *,
    client: FeishuBitableClient,
    record_index: Dict[str, str],
    record_state_index: Optional[Dict[str, Dict[str, Any]]] = None,
    unique_field: str,
    unique_value: Any,
    fields: Dict[str, Any],
    compare_ignore_fields: Optional[List[str]] = None,
) -> tuple[str, str]:
    normalized = normalize_unique_value(unique_value)
    if not normalized:
        raise ValueError(f"缺少唯一字段值: {unique_field}")
    record_id = record_index.get(normalized, "")
    if record_id:
        existing_fields = ((record_state_index or {}).get(normalized) or {}).get("fields") or {}
        if existing_fields and fields_match(existing_fields, fields, ignore_fields=compare_ignore_fields):
            return "skipped", record_id
        client.update_record(record_id, fields)
        if record_state_index is not None:
            record_state_index[normalized] = {
                "record_id": record_id,
                "fields": dict(fields),
            }
        return "updated", record_id
    record_id = client.create_record(fields)
    record_index[normalized] = record_id
    if record_state_index is not None:
        record_state_index[normalized] = {
            "record_id": record_id,
            "fields": dict(fields),
        }
    return "created", record_id


def normalize_unique_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if "text" in value:
            return str(value.get("text") or "").strip()
        if "link" in value:
            return str(value.get("link") or "").strip()
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, list):
        return "|".join(normalize_unique_value(item) for item in value)
    return str(value).strip()


def merge_report_with_existing_work_details(
    *,
    report: Dict[str, Any],
    works_records: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    profile = dict(report.get("profile") or {})
    profile_user_id = str(profile.get("profile_user_id") or "").strip()
    merged_works: List[Dict[str, Any]] = []
    for raw_work in report.get("works") or []:
        work = dict(raw_work or {})
        fingerprint = build_work_fingerprint(
            profile_user_id=profile_user_id,
            title=str(work.get("title_copy") or ""),
            cover_url=str(work.get("cover_url") or ""),
        )
        existing_fields = (works_records.get(fingerprint) or {}).get("fields") or {}
        existing_note_url = extract_link_value(existing_fields.get("作品链接"))
        if existing_note_url and not str(work.get("note_url") or "").strip():
            work["note_url"] = existing_note_url
        if existing_note_url and not str(work.get("note_id") or "").strip():
            note_id = extract_note_id_from_url(existing_note_url)
            if note_id:
                work["note_id"] = note_id
        merged_works.append(work)

    merged_report = dict(report)
    merged_report["profile"] = profile
    merged_report["works"] = merged_works
    return merged_report


def extract_link_value(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    if isinstance(value, list):
        for item in value:
            link = extract_link_value(item)
            if link:
                return link
    return str(value or "").strip()


def extract_note_id_from_url(url: str) -> str:
    match = NOTE_ID_FROM_URL_PATTERN.search(str(url or "").strip())
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def to_optional_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace(",", "")
    if text.lstrip("-").isdigit():
        return int(text)
    return None


def parse_calendar_key_date(value: str) -> datetime.date:
    return datetime.fromisoformat(str(value or "").split("|", 1)[0]).date()


def install_batch_sync_launchd(
    *,
    urls: List[str],
    urls_file: Optional[str],
    raw_text: str,
    project: str,
    env_file: str,
    profile_table_name: str,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
    daily_at: str,
    project_slot_minutes: int,
    label: str,
    plist_path: Optional[str],
    stdout_log_path: Optional[str],
    stderr_log_path: Optional[str],
    load_after_install: bool,
) -> None:
    schedule_settings = load_settings(env_file)
    project_specs = build_project_launchd_specs(
        urls_file=urls_file,
        explicit_project=project,
        daily_at=daily_at,
        project_slot_minutes=project_slot_minutes,
        base_label=label,
        slot_offset_seconds=max(0, int(getattr(schedule_settings, "xhs_batch_slot_offset_seconds", 300) or 0)),
    )
    use_spread_schedule = bool(getattr(schedule_settings, "xhs_spread_schedule_enabled", True))
    interval_seconds = max(
        300,
        int(max(1, int(getattr(schedule_settings, "xhs_batch_schedule_interval_minutes", 30) or 30)) * 60),
    )
    if len(project_specs) > 1:
        working_directory = str(Path(__file__).resolve().parent.parent)
        for spec in project_specs:
            resolved_paths = resolve_launchd_paths(label=spec["label"])
            program_arguments = build_batch_sync_program_arguments(
                urls=urls,
                urls_file=urls_file,
                raw_text=raw_text,
                project=spec["project"],
                env_file=env_file,
                profile_table_name=profile_table_name,
                works_table_name=works_table_name,
                ensure_fields=ensure_fields,
                sync_dashboard=sync_dashboard,
                scheduled=use_spread_schedule,
                slot_offset_seconds=int(spec.get("slot_offset_seconds") or 0),
            )
            plist_bytes = build_launch_agent_plist(
                label=spec["label"],
                program_arguments=wrap_program_arguments_for_login_shell(
                    program_arguments=program_arguments,
                    working_directory=working_directory,
                ),
                working_directory=working_directory,
                interval_seconds=interval_seconds if use_spread_schedule else None,
                start_calendar_interval=None if use_spread_schedule else parse_daily_time(spec["daily_at"]),
                stdout_log_path=resolved_paths["stdout_log_path"],
                stderr_log_path=resolved_paths["stderr_log_path"],
                environment_variables=build_launch_environment(),
            )
            install_launch_agent(
                plist_bytes=plist_bytes,
                label=spec["label"],
                plist_path=resolved_paths["plist_path"],
                load_after_install=load_after_install,
            )
            print(f"[OK] installed launchd label={spec['label']}")
            print(f"[OK] project={spec['project']}")
            print(f"[OK] plist={resolved_paths['plist_path']}")
            print(f"[OK] stdout_log={resolved_paths['stdout_log_path']}")
            print(f"[OK] stderr_log={resolved_paths['stderr_log_path']}")
            if use_spread_schedule:
                print(f"[OK] interval_minutes={getattr(schedule_settings, 'xhs_batch_schedule_interval_minutes', 30)}")
                print(
                    f"[OK] window={getattr(schedule_settings, 'xhs_batch_window_start', '09:00')}"
                    f"-{getattr(schedule_settings, 'xhs_batch_window_end', '21:00')}"
                )
                print(f"[OK] slot_offset_seconds={int(spec.get('slot_offset_seconds') or 0)}")
            else:
                print(f"[OK] daily_at={spec['daily_at']}")
        return

    resolved_paths = resolve_launchd_paths(
        label=label,
        plist_path=plist_path,
        stdout_log_path=stdout_log_path,
        stderr_log_path=stderr_log_path,
    )
    program_arguments = build_batch_sync_program_arguments(
        urls=urls,
        urls_file=urls_file,
        raw_text=raw_text,
        project=project,
        env_file=env_file,
        profile_table_name=profile_table_name,
        works_table_name=works_table_name,
        ensure_fields=ensure_fields,
        sync_dashboard=sync_dashboard,
        scheduled=use_spread_schedule,
        slot_offset_seconds=0,
    )
    working_directory = str(Path(__file__).resolve().parent.parent)
    plist_bytes = build_launch_agent_plist(
        label=label,
        program_arguments=wrap_program_arguments_for_login_shell(
            program_arguments=program_arguments,
            working_directory=working_directory,
        ),
        working_directory=working_directory,
        interval_seconds=interval_seconds if use_spread_schedule else None,
        start_calendar_interval=None if use_spread_schedule else parse_daily_time(daily_at),
        stdout_log_path=resolved_paths["stdout_log_path"],
        stderr_log_path=resolved_paths["stderr_log_path"],
        environment_variables=build_launch_environment(),
    )
    install_launch_agent(
        plist_bytes=plist_bytes,
        label=label,
        plist_path=resolved_paths["plist_path"],
        load_after_install=load_after_install,
    )
    print(f"[OK] installed launchd label={label}")
    print(f"[OK] plist={resolved_paths['plist_path']}")
    print(f"[OK] stdout_log={resolved_paths['stdout_log_path']}")
    print(f"[OK] stderr_log={resolved_paths['stderr_log_path']}")
    if use_spread_schedule:
        print(f"[OK] interval_minutes={getattr(schedule_settings, 'xhs_batch_schedule_interval_minutes', 30)}")
        print(
            f"[OK] window={getattr(schedule_settings, 'xhs_batch_window_start', '09:00')}"
            f"-{getattr(schedule_settings, 'xhs_batch_window_end', '21:00')}"
        )
    else:
        print(f"[OK] daily_at={daily_at}")


def build_batch_sync_program_arguments(
    *,
    urls: List[str],
    urls_file: Optional[str],
    raw_text: str,
    project: str,
    env_file: str,
    profile_table_name: str,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
    scheduled: bool = False,
    slot_offset_seconds: int = 0,
) -> List[str]:
    argv = [
        sys.executable,
        "-m",
        "xhs_feishu_monitor.profile_batch_to_feishu",
        "--env-file",
        str(Path(env_file).expanduser().resolve()),
    ]
    if urls_file:
        argv.extend(["--urls-file", str(Path(urls_file).expanduser().resolve())])
    elif raw_text:
        argv.extend(["--raw-text", raw_text])
    else:
        for url in urls:
            argv.extend(["--url", url])
    if str(project or "").strip():
        argv.extend(["--project", str(project).strip()])
    if scheduled:
        argv.append("--scheduled")
    if slot_offset_seconds > 0:
        argv.extend(["--slot-offset-seconds", str(int(slot_offset_seconds))])
    if profile_table_name != PROFILE_TABLE_NAME:
        argv.extend(["--profile-table-name", profile_table_name])
    if works_table_name != WORKS_TABLE_NAME:
        argv.extend(["--works-table-name", works_table_name])
    if ensure_fields:
        argv.append("--ensure-fields")
    if sync_dashboard:
        argv.append("--sync-dashboard")
    return argv


def resolve_launchd_paths(
    *,
    label: str,
    plist_path: Optional[str] = None,
    stdout_log_path: Optional[str] = None,
    stderr_log_path: Optional[str] = None,
) -> Dict[str, str]:
    defaults = default_paths(label)
    return {
        "plist_path": str(Path(plist_path or defaults["plist_path"]).expanduser().resolve()),
        "stdout_log_path": str(Path(stdout_log_path or defaults["stdout_log_path"]).expanduser().resolve()),
        "stderr_log_path": str(Path(stderr_log_path or defaults["stderr_log_path"]).expanduser().resolve()),
    }


def build_project_launchd_specs(
    *,
    urls_file: Optional[str],
    explicit_project: str,
    daily_at: str,
    project_slot_minutes: int,
    base_label: str,
    slot_offset_seconds: int = 0,
) -> List[Dict[str, str]]:
    if str(explicit_project or "").strip():
        return [{"project": str(explicit_project).strip(), "daily_at": daily_at, "label": base_label}]
    if not urls_file:
        return []
    projects = extract_ordered_projects_from_urls_file(urls_file)
    if len(projects) <= 1 or project_slot_minutes <= 0:
        return []
    return [
        {
            "project": project,
            "daily_at": offset_daily_time(daily_at, index * project_slot_minutes),
            "label": f"{base_label}.{slugify_project_name(project)}",
            "slot_offset_seconds": index * max(0, int(slot_offset_seconds or 0)),
        }
        for index, project in enumerate(projects)
    ]


def extract_ordered_projects_from_urls_file(urls_file: str) -> List[str]:
    projects: List[str] = []
    seen = set()
    for entry in load_url_entries_file(urls_file):
        project = str(entry.get("project") or "").strip()
        if not project or project in seen:
            continue
        seen.add(project)
        projects.append(project)
    return projects


def offset_daily_time(daily_at: str, offset_minutes: int) -> str:
    schedule = parse_daily_time(daily_at)
    total_minutes = (int(schedule["Hour"]) * 60 + int(schedule["Minute"]) + int(offset_minutes or 0)) % (24 * 60)
    hour, minute = divmod(total_minutes, 60)
    return f"{hour:02d}:{minute:02d}"


def slugify_project_name(project: str) -> str:
    text = str(project or "").strip().lower()
    cleaned = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "-", text).strip("-")
    return cleaned or "project"


def unload_batch_sync_launchd(*, label: str, plist_path: Optional[str] = None) -> List[str]:
    if plist_path:
        resolved = str(Path(plist_path).expanduser().resolve())
        unload_launch_agent(plist_path=resolved)
        return [resolved]
    defaults = resolve_launchd_paths(label=label)
    launch_agents_dir = Path(defaults["plist_path"]).parent
    matched = sorted(launch_agents_dir.glob(f"{label}*.plist"))
    unloaded: List[str] = []
    for path in matched:
        unload_launch_agent(plist_path=str(path))
        unloaded.append(str(path))
    return unloaded


if __name__ == "__main__":
    raise SystemExit(main())
