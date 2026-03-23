from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import replace
from datetime import datetime
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
    load_url_entries_file,
    normalize_profile_url_entries,
    normalize_profile_urls,
)
from .project_sync_status import update_project_sync_status
from .profile_dashboard_to_feishu import (
    sync_dashboard_portal,
    sync_dashboard_tables,
    sync_single_work_ranking_table,
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
        )
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
            update_project_sync_status(
                urls_file=args.urls_file or "",
                project=args.project,
                state="error",
                message=f"项目「{args.project}」同步失败",
                started_at=started_at,
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                last_error=str(exc),
            )
        raise


def load_reports_for_sync(
    *,
    settings,
    explicit_urls: List[str],
    raw_text: str,
    urls_file: Optional[str],
    project: str,
    report_json: Optional[str],
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    if report_json:
        return load_reports_from_json(report_json)

    url_entries = normalize_profile_url_entries(explicit_urls, raw_text, urls_file)
    if str(project or "").strip():
        project_name = str(project).strip()
        url_entries = [item for item in url_entries if str(item.get("project") or "").strip() == project_name]
    urls = [item["url"] for item in url_entries]
    if not urls:
        raise ValueError("没有找到可用的小红书账号主页链接")
    items = collect_profile_reports_with_progress(
        urls=urls,
        url_entries=url_entries,
        settings=settings,
        progress_callback=progress_callback,
    )
    reports = [normalize_batch_item_to_report(item) for item in items if item.get("status") == "success"]
    if not reports:
        raise ValueError("批量抓取没有成功结果，无法同步到飞书")
    return reports


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

    reports = [normalize_batch_item_to_report(item) for item in items if isinstance(item, dict) and item.get("status") == "success"]
    if not reports:
        raise ValueError("批量报告里没有可同步的成功记录")
    return reports


def normalize_batch_item_to_report(item: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(item.get("profile") or {})
    works = [dict(work) for work in (item.get("works") or []) if isinstance(work, dict)]
    captured_at = str(item.get("captured_at") or "").strip() or datetime.now().astimezone().isoformat(timespec="seconds")
    final_url = str(item.get("final_url") or item.get("requested_url") or profile.get("profile_url") or "").strip()
    if final_url and not profile.get("profile_url"):
        profile["profile_url"] = final_url
    return {
        "captured_at": captured_at,
        "source_url": str(item.get("requested_url") or final_url or profile.get("profile_url") or "").strip(),
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

    tables_client = FeishuBitableClient(settings)
    profile_table_id = ensure_profile_table(tables_client=tables_client, table_name=profile_table_name)
    works_table_id = ensure_works_table(
        tables_client=tables_client,
        settings=settings,
        table_name=works_table_name,
    )
    works_calendar_table_id = ensure_works_calendar_table(
        tables_client=tables_client,
        settings=settings,
        table_name=WORKS_CALENDAR_TABLE_NAME,
    )

    profile_client = FeishuBitableClient(replace(settings, feishu_table_id=profile_table_id))
    works_client = FeishuBitableClient(replace(settings, feishu_table_id=works_table_id))
    works_calendar_client = FeishuBitableClient(replace(settings, feishu_table_id=works_calendar_table_id))

    if ensure_fields:
        profile_client.ensure_fields(PROFILE_FIELD_SPECS)
        works_client.ensure_fields(WORKS_TABLE_FIELDS)
        works_calendar_client.ensure_fields(WORKS_CALENDAR_FIELDS)

    deduped_profiles = dedupe_profile_records(profile_client)
    deduped_works = dedupe_work_records(works_client)
    profile_records = build_record_state_index(profile_client, unique_field="账号ID")
    profile_index = {key: item["record_id"] for key, item in profile_records.items()}
    works_records = build_record_state_index(works_client, unique_field="作品指纹")
    works_index = {key: item["record_id"] for key, item in works_records.items()}
    works_calendar_records = build_record_state_index(works_calendar_client, unique_field="日历键")
    works_calendar_index = {key: item["record_id"] for key, item in works_calendar_records.items()}
    works_calendar_history = build_work_calendar_history_index(
        works_calendar_client.list_records(
            page_size=500,
            field_names=["日期文本", "日历日期", "作品指纹", "点赞数", "点赞文本", "评论数", "评论文本"],
        )
    )
    reports = [merge_report_with_existing_work_details(report=report, works_records=works_records) for report in reports]

    account_results: List[Dict[str, Any]] = []
    dashboard_synced = 0
    synced_works = 0
    skipped_profiles = 0
    skipped_works = 0
    skipped_work_calendars = 0
    dashboard_portal_result = None
    single_work_ranking_result = None
    comment_alert_candidates: List[Dict[str, Any]] = []
    for index, report in enumerate(reports, start=1):
        profile_fields = build_profile_feishu_fields(report)
        profile_action, profile_record_id = upsert_record_with_index(
            client=profile_client,
            record_index=profile_index,
            record_state_index=profile_records,
            unique_field="账号ID",
            unique_value=profile_fields["账号ID"],
            fields=profile_fields,
            compare_ignore_fields=["上报时间"],
        )
        if profile_action == "skipped":
            skipped_profiles += 1

        work_results = []
        snapshot_date = extract_snapshot_date(report.get("captured_at") or "")
        for work in report["works"]:
            work_fields = build_work_feishu_fields(report=report, work=work)
            fingerprint = str(work_fields.get("作品指纹") or "").strip()
            weekly_baseline = select_work_weekly_baseline(
                history_index=works_calendar_history,
                fingerprint=fingerprint,
                snapshot_date=snapshot_date,
            )
            work_fields.update(build_work_weekly_fields(current_fields=work_fields, baseline_fields=weekly_baseline))
            previous_fields = (works_records.get(fingerprint) or {}).get("fields") or {}
            comment_fields, comment_alert = build_work_comment_fields(
                report=report,
                work=work,
                previous_fields=previous_fields,
                settings=settings,
            )
            work_fields.update(comment_fields)
            work_action, work_record_id = upsert_record_with_index(
                client=works_client,
                record_index=works_index,
                record_state_index=works_records,
                unique_field="作品指纹",
                unique_value=work_fields["作品指纹"],
                fields=work_fields,
                compare_ignore_fields=["抓取时间"],
            )
            if work_action == "skipped":
                skipped_works += 1
            if fingerprint:
                works_records[fingerprint] = {
                    "record_id": work_record_id,
                    "fields": dict(work_fields),
                }
            calendar_fields = build_work_calendar_fields(report=report, work=work)
            calendar_key = str(calendar_fields.get("日历键") or "").strip()
            calendar_action, calendar_record_id = upsert_record_with_index(
                client=works_calendar_client,
                record_index=works_calendar_index,
                record_state_index=works_calendar_records,
                unique_field="日历键",
                unique_value=calendar_key,
                fields=calendar_fields,
                compare_ignore_fields=["数据更新时间"],
            )
            if calendar_action == "skipped":
                skipped_work_calendars += 1
            if fingerprint and calendar_key:
                today_entry = (parse_calendar_key_date(calendar_key), dict(calendar_fields))
                works_calendar_history.setdefault(fingerprint, [])
                works_calendar_history[fingerprint] = [
                    entry for entry in works_calendar_history[fingerprint] if entry[0] != today_entry[0]
                ]
                works_calendar_history[fingerprint].append(today_entry)
                works_calendar_history[fingerprint].sort(key=lambda item: item[0], reverse=True)
            synced_works += 1
            if comment_alert:
                comment_alert_candidates.append(comment_alert)
            work_results.append(
                {
                    "action": work_action,
                    "record_id": work_record_id,
                    "title": work_fields["标题文案"],
                    "calendar_action": calendar_action,
                    "calendar_record_id": calendar_record_id,
                }
            )

        dashboard_result = None
        if sync_dashboard:
            dashboard_result = sync_dashboard_tables(report=report, settings=settings)
            dashboard_synced += 1

        account_results.append(
            {
                "账号": report["profile"].get("nickname") or "",
                "账号ID": report["profile"].get("profile_user_id") or "",
                "summary_action": profile_action,
                "summary_record_id": profile_record_id,
                "works_synced": len(report["works"]),
                "dashboard": dashboard_result,
                "sample_works": work_results[:3],
            }
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "sync",
                    "current": index,
                    "total": total_reports,
                    "account": str(report["profile"].get("nickname") or report["profile"].get("profile_user_id") or ""),
                    "account_id": str(report["profile"].get("profile_user_id") or ""),
                    "works": len(report["works"]),
                    "synced_works": synced_works,
                    "success_count": index,
                    "failed_count": 0,
                }
            )

    if sync_dashboard and reports:
        dashboard_portal_result = sync_dashboard_portal(reports=reports, settings=settings)
        single_work_ranking_result = sync_single_work_ranking_table(reports=reports, settings=settings)

    comment_alert_result = sync_comment_alerts(settings=settings, alerts=comment_alert_candidates)

    return {
        "profile_table_id": profile_table_id,
        "works_table_id": works_table_id,
        "works_calendar_table_id": works_calendar_table_id,
        "total_accounts": len(reports),
        "total_works": synced_works,
        "deduped_profiles": deduped_profiles,
        "deduped_works": deduped_works,
        "dashboard_synced": dashboard_synced,
        "dashboard_portal": dashboard_portal_result,
        "single_work_rankings": single_work_ranking_result,
        "comment_alerts": comment_alert_result,
        "skipped_profiles": skipped_profiles,
        "skipped_works": skipped_works,
        "skipped_work_calendars": skipped_work_calendars,
        "items": account_results,
    }


def build_record_id_index(client: FeishuBitableClient, *, unique_field: str) -> Dict[str, str]:
    index: Dict[str, str] = {}
    for record in client.list_records(page_size=500, field_names=[unique_field]):
        fields = record.get("fields") or {}
        unique_value = normalize_unique_value(fields.get(unique_field))
        record_id = str(record.get("record_id") or "").strip()
        if unique_value and record_id and unique_value not in index:
            index[unique_value] = record_id
    return index


def build_record_state_index(client: FeishuBitableClient, *, unique_field: str) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for record in client.list_records(page_size=500):
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
        existing_comment_count = to_optional_int(existing_fields.get("评论数"))
        existing_comment_text = str(existing_fields.get("评论文本") or "").strip()
        existing_comment_summary = str(existing_fields.get("最新评论摘要") or "").strip()

        if existing_note_url and not str(work.get("note_url") or "").strip():
            work["note_url"] = existing_note_url
        if existing_note_url and not str(work.get("note_id") or "").strip():
            note_id = extract_note_id_from_url(existing_note_url)
            if note_id:
                work["note_id"] = note_id
        if existing_comment_count is not None and work.get("comment_count") is None:
            work["comment_count"] = existing_comment_count
        if existing_comment_text and not str(work.get("comment_count_text") or "").strip():
            work["comment_count_text"] = existing_comment_text
        elif existing_comment_count is not None and not str(work.get("comment_count_text") or "").strip():
            work["comment_count_text"] = str(existing_comment_count)
        if existing_comment_summary and not str(work.get("recent_comments_summary") or "").strip():
            work["recent_comments_summary"] = existing_comment_summary
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
    project_specs = build_project_launchd_specs(
        urls_file=urls_file,
        explicit_project=project,
        daily_at=daily_at,
        project_slot_minutes=project_slot_minutes,
        base_label=label,
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
            )
            plist_bytes = build_launch_agent_plist(
                label=spec["label"],
                program_arguments=wrap_program_arguments_for_login_shell(
                    program_arguments=program_arguments,
                    working_directory=working_directory,
                ),
                working_directory=working_directory,
                start_calendar_interval=parse_daily_time(spec["daily_at"]),
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
    )
    working_directory = str(Path(__file__).resolve().parent.parent)
    plist_bytes = build_launch_agent_plist(
        label=label,
        program_arguments=wrap_program_arguments_for_login_shell(
            program_arguments=program_arguments,
            working_directory=working_directory,
        ),
        working_directory=working_directory,
        start_calendar_interval=parse_daily_time(daily_at),
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
