from __future__ import annotations

import argparse
import re
import sys
from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional

from .config import load_settings
from .feishu import FeishuBitableClient
from .profile_dashboard_to_feishu import sync_dashboard_tables
from .launchd import (
    build_launch_environment,
    build_launch_agent_plist,
    default_paths,
    install_launch_agent,
    unload_launch_agent,
    wrap_program_arguments_for_login_shell,
)
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload
from .profile_to_feishu import (
    PROFILE_FIELD_SPECS,
    PROFILE_TABLE_NAME,
    build_profile_feishu_fields,
    dedupe_profile_records,
    ensure_profile_table,
)
from .profile_works_to_feishu import (
    WORKS_TABLE_NAME,
    WORKS_TABLE_FIELDS,
    build_work_feishu_fields,
    dedupe_work_records,
    ensure_works_table,
)


DEFAULT_LIVE_SYNC_LABEL = "com.cc.xhs-profile-live-sync"
DEFAULT_INTERVAL_SECONDS = 300


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="同步小红书账号汇总和作品明细到飞书。")
    parser.add_argument("--url", help="小红书账号主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--profile-table-name", default=PROFILE_TABLE_NAME)
    parser.add_argument("--works-table-name", default=WORKS_TABLE_NAME)
    parser.add_argument("--ensure-fields", action="store_true", help="自动补齐账号汇总和作品明细字段")
    parser.add_argument("--sync-dashboard", action="store_true", help="额外同步飞书看板总览、趋势和榜单数据")
    parser.add_argument("--install-launchd", action="store_true", help="生成并安装 launchd 轮询任务")
    parser.add_argument("--load-launchd", action="store_true", help="安装后立刻加载 launchd 任务")
    parser.add_argument("--unload-launchd", action="store_true", help="卸载 launchd 任务")
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS, help="轮询间隔秒数")
    parser.add_argument("--daily-at", help="每天固定执行时间，格式 HH:MM，例如 14:00")
    parser.add_argument("--launchd-label", default=DEFAULT_LIVE_SYNC_LABEL, help="launchd 任务标签")
    parser.add_argument("--launchd-plist", help="launchd plist 路径")
    parser.add_argument("--stdout-log-path", help="stdout 日志路径")
    parser.add_argument("--stderr-log-path", help="stderr 日志路径")
    args = parser.parse_args(argv)

    if args.unload_launchd:
        plist_path = resolve_launchd_paths(label=args.launchd_label, plist_path=args.launchd_plist)["plist_path"]
        unload_launch_agent(plist_path=plist_path)
        print(f"[OK] unloaded launchd plist={plist_path}")
        return 0

    if not args.url:
        parser.error("--url 为必填，除非只执行 --unload-launchd")

    settings = load_settings(args.env_file)
    settings.validate_for_sync()

    if args.install_launchd:
        install_profile_launchd(
            url=args.url,
            env_file=args.env_file,
            profile_table_name=args.profile_table_name,
            works_table_name=args.works_table_name,
            ensure_fields=args.ensure_fields,
            sync_dashboard=args.sync_dashboard,
            interval_seconds=args.interval_seconds,
            daily_at=args.daily_at,
            label=args.launchd_label,
            plist_path=args.launchd_plist,
            stdout_log_path=args.stdout_log_path,
            stderr_log_path=args.stderr_log_path,
            load_after_install=args.load_launchd,
        )
        return 0

    payload = load_profile_report_payload(settings=settings, profile_url=args.url)
    report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    report = enrich_profile_report_with_note_metrics(report=report, settings=settings)

    tables_client = FeishuBitableClient(settings)
    summary_table_id = ensure_profile_table(
        tables_client=tables_client,
        table_name=args.profile_table_name,
    )
    summary_settings = replace(settings, feishu_table_id=summary_table_id)
    summary_client = FeishuBitableClient(summary_settings)
    if args.ensure_fields:
        summary_client.ensure_fields(PROFILE_FIELD_SPECS)
    deduped_profile_count = dedupe_profile_records(summary_client)

    summary_fields = build_profile_feishu_fields(report)
    summary_action, summary_record_id = summary_client.upsert_record(
        unique_field="账号ID",
        unique_value=summary_fields["账号ID"],
        fields=summary_fields,
    )

    works_table_id = ensure_works_table(
        tables_client=tables_client,
        settings=settings,
        table_name=args.works_table_name,
    )
    work_settings = replace(settings, feishu_table_id=works_table_id)
    works_client = FeishuBitableClient(work_settings)
    if args.ensure_fields:
        works_client.ensure_fields(WORKS_TABLE_FIELDS)
    deduped_work_count = dedupe_work_records(works_client)

    synced_work_count = 0
    for work in report["works"]:
        work_fields = build_work_feishu_fields(report=report, work=work)
        work_action, work_record_id = works_client.upsert_record(
            unique_field="作品指纹",
            unique_value=work_fields["作品指纹"],
            fields=work_fields,
        )
        synced_work_count += 1
        print(f"[OK] work {work_action} record_id={work_record_id} title={work_fields['标题文案']}")

    print(f"[OK] summary {summary_action} record_id={summary_record_id}")
    print(f"[OK] profile_table={args.profile_table_name} table_id={summary_table_id}")
    print(f"[OK] deduped_profiles={deduped_profile_count}")
    print(f"[OK] works_table={args.works_table_name} table_id={works_table_id}")
    print(f"[OK] synced_works={synced_work_count}")
    print(f"[OK] deduped_works={deduped_work_count}")
    if args.sync_dashboard:
        dashboard_result = sync_dashboard_tables(report=report, settings=settings)
        print(
            "[OK] dashboard "
            f"overview={dashboard_result['overview_action']}:{dashboard_result['overview_record_id']} "
            f"trend={dashboard_result['trend_action']}:{dashboard_result['trend_record_id']} "
            f"calendar={dashboard_result['calendar_action']}:{dashboard_result['calendar_record_id']} "
            f"ranking_created={dashboard_result['ranking_created']} "
            f"ranking_updated={dashboard_result['ranking_updated']} "
            f"ranking_deleted={dashboard_result['ranking_deleted']}"
        )
    return 0


def install_profile_launchd(
    *,
    url: str,
    env_file: str,
    profile_table_name: str,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
    interval_seconds: int,
    daily_at: Optional[str],
    label: str,
    plist_path: Optional[str],
    stdout_log_path: Optional[str],
    stderr_log_path: Optional[str],
    load_after_install: bool,
) -> None:
    if not daily_at and interval_seconds < 60:
        raise ValueError("interval_seconds 不能小于 60")

    resolved_paths = resolve_launchd_paths(
        label=label,
        plist_path=plist_path,
        stdout_log_path=stdout_log_path,
        stderr_log_path=stderr_log_path,
    )
    program_arguments = build_live_sync_program_arguments(
        url=url,
        env_file=env_file,
        profile_table_name=profile_table_name,
        works_table_name=works_table_name,
        ensure_fields=ensure_fields,
        sync_dashboard=sync_dashboard,
    )
    working_directory = str(Path(__file__).resolve().parent.parent)
    start_calendar_interval = parse_daily_time(daily_at) if daily_at else None
    plist_bytes = build_launch_agent_plist(
        label=label,
        program_arguments=wrap_program_arguments_for_login_shell(
            program_arguments=program_arguments,
            working_directory=working_directory,
        ),
        working_directory=working_directory,
        interval_seconds=None if start_calendar_interval else interval_seconds,
        start_calendar_interval=start_calendar_interval,
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
    if start_calendar_interval:
        print(f"[OK] daily_at={daily_at}")
    else:
        print(f"[OK] interval_seconds={interval_seconds}")


def build_live_sync_program_arguments(
    *,
    url: str,
    env_file: str,
    profile_table_name: str,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
) -> List[str]:
    argv = [
        sys.executable,
        "-m",
        "xhs_feishu_monitor.profile_live_sync",
        "--url",
        url,
        "--env-file",
        str(Path(env_file).expanduser().resolve()),
    ]
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
) -> dict:
    defaults = default_paths(label)
    return {
        "plist_path": str(Path(plist_path or defaults["plist_path"]).expanduser().resolve()),
        "stdout_log_path": str(Path(stdout_log_path or defaults["stdout_log_path"]).expanduser().resolve()),
        "stderr_log_path": str(Path(stderr_log_path or defaults["stderr_log_path"]).expanduser().resolve()),
    }


def parse_daily_time(value: Optional[str]) -> Dict[str, int]:
    text = str(value or "").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not match:
        raise ValueError("daily_at 格式必须是 HH:MM，例如 14:00")
    return {
        "Hour": int(match.group(1)),
        "Minute": int(match.group(2)),
    }


if __name__ == "__main__":
    raise SystemExit(main())
