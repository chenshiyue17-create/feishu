from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import load_settings
from .launchd import (
    build_launch_agent_plist,
    build_launch_environment,
    default_paths,
    install_launch_agent,
    unload_launch_agent,
    wrap_program_arguments_for_login_shell,
)
from .local_stats_app.monitored_accounts import parse_monitored_entries
from .local_stats_app.server import (
    build_auto_project_schedule,
    login_state_requires_interactive_login,
    push_current_cache_to_server,
    wait_for_xiaohongshu_login,
)
from .profile_batch_collect import collect_profiles_to_local_cache
from .local_daily_sync_status import (
    load_local_daily_sync_status,
    write_local_daily_sync_status,
)


DEFAULT_LOCAL_DAILY_SYNC_LABEL = "com.cc.xhs-local-daily-sync"
LEGACY_FEISHU_LABEL_PREFIX = "com.cc.xhs-profile-batch-report"


def _parse_daily_time(value: str) -> Dict[str, int]:
    raw_value = str(value or "14:00").strip()
    try:
        hour_text, minute_text = raw_value.split(":", 1)
    except ValueError:
        hour_text, minute_text = "14", "00"
    return {
        "Hour": max(0, min(23, int(hour_text))),
        "Minute": max(0, min(59, int(minute_text))),
    }


def _compute_next_daily_window_start(*, settings, now: datetime) -> str:
    clock = _parse_daily_time(str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00"))
    target = now.replace(hour=clock["Hour"], minute=clock["Minute"], second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return target.isoformat(timespec="seconds")


def build_local_daily_sync_program_arguments(*, env_file: str, urls_file: str) -> List[str]:
    return [
        sys.executable,
        "-m",
        "xhs_feishu_monitor.local_daily_sync",
        "--env-file",
        str(Path(env_file).expanduser().resolve()),
        "--urls-file",
        str(Path(urls_file).expanduser().resolve()),
    ]


def _upsert_env_value(*, env_file: str, key: str, value: str) -> None:
    path = Path(env_file).expanduser().resolve()
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    updated = False
    new_lines: List[str] = []
    for line in lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"{key}={value}")
    path.write_text("\n".join(new_lines).rstrip() + "\n", encoding="utf-8")


def cleanup_legacy_feishu_launchd_jobs(*, remove_logs: bool = True) -> Dict[str, List[str]]:
    home_dir = Path.home()
    launch_agents_dir = home_dir / "Library" / "LaunchAgents"
    logs_dir = home_dir / "Library" / "Logs"
    removed_plists: List[str] = []
    removed_logs: List[str] = []

    for plist_path in sorted(launch_agents_dir.glob(f"{LEGACY_FEISHU_LABEL_PREFIX}*.plist")):
        unload_launch_agent(plist_path=str(plist_path))
        try:
            plist_path.unlink()
        except FileNotFoundError:
            pass
        removed_plists.append(str(plist_path))

    if remove_logs:
        for log_path in sorted(logs_dir.glob(f"{LEGACY_FEISHU_LABEL_PREFIX}*.log")):
            try:
                log_path.unlink()
            except FileNotFoundError:
                pass
            removed_logs.append(str(log_path))

    return {
        "plists": removed_plists,
        "logs": removed_logs,
    }


def install_local_daily_sync_launchd(
    *,
    env_file: str,
    urls_file: str,
    label: str = DEFAULT_LOCAL_DAILY_SYNC_LABEL,
    plist_path: Optional[str] = None,
    stdout_log_path: Optional[str] = None,
    stderr_log_path: Optional[str] = None,
    load_after_install: bool = True,
    set_schedule_driver: bool = True,
) -> Dict[str, str]:
    cleanup_legacy_feishu_launchd_jobs()
    settings = load_settings(env_file)
    defaults = default_paths(label)
    resolved_paths = {
        "plist_path": str(Path(plist_path or defaults["plist_path"]).expanduser().resolve()),
        "stdout_log_path": str(Path(stdout_log_path or defaults["stdout_log_path"]).expanduser().resolve()),
        "stderr_log_path": str(Path(stderr_log_path or defaults["stderr_log_path"]).expanduser().resolve()),
    }
    Path(resolved_paths["stdout_log_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(resolved_paths["stderr_log_path"]).parent.mkdir(parents=True, exist_ok=True)
    working_directory = str(Path(__file__).resolve().parent.parent)
    program_arguments = build_local_daily_sync_program_arguments(env_file=env_file, urls_file=urls_file)
    plist_bytes = build_launch_agent_plist(
        label=label,
        program_arguments=wrap_program_arguments_for_login_shell(
            program_arguments=program_arguments,
            working_directory=working_directory,
        ),
        working_directory=working_directory,
        start_calendar_interval=_parse_daily_time(str(getattr(settings, "xhs_batch_window_start", "14:00") or "14:00")),
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
    if set_schedule_driver:
        _upsert_env_value(env_file=env_file, key="XHS_SCHEDULE_DRIVER", value="launchd")
    return resolved_paths


def uninstall_local_daily_sync_launchd(
    *,
    env_file: str,
    label: str = DEFAULT_LOCAL_DAILY_SYNC_LABEL,
    plist_path: Optional[str] = None,
    set_schedule_driver: bool = True,
) -> str:
    cleanup_legacy_feishu_launchd_jobs()
    defaults = default_paths(label)
    resolved_plist_path = str(Path(plist_path or defaults["plist_path"]).expanduser().resolve())
    unload_launch_agent(plist_path=resolved_plist_path)
    if set_schedule_driver:
        _upsert_env_value(env_file=env_file, key="XHS_SCHEDULE_DRIVER", value="app")
    return resolved_plist_path


def _sleep_until(target_time: datetime) -> None:
    while True:
        remaining = (target_time - datetime.now().astimezone()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 30))


def run_local_daily_sync(*, env_file: str, urls_file: str) -> Dict[str, Any]:
    settings = load_settings(env_file)
    state_file_path = str(getattr(settings, "state_file", "") or "")
    persisted_status = load_local_daily_sync_status(env_file=env_file, state_file_path=state_file_path)
    entries = parse_monitored_entries(urls_file)
    plan = build_auto_project_schedule(settings=settings, entries=entries)
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    runtime_status = {
        **persisted_status,
        "state": "running",
        "phase": "preparing",
        "message": "等待项目时间窗口并准备自动采集",
        "started_at": started_at,
        "finished_at": "",
        "next_run_at": _compute_next_daily_window_start(settings=settings, now=datetime.now().astimezone()),
        "project_count": len(plan),
        "successful_projects": 0,
        "failed_projects": 0,
        "current_project": "",
        "current_project_index": 0,
        "current_project_total": len(plan),
        "current_project_scheduled_at": "",
        "waiting_for_login": False,
        "upload_state": "",
        "upload_message": "",
        "last_error": "",
        "last_upload_error": "",
    }
    write_local_daily_sync_status(env_file=env_file, state_file_path=state_file_path, payload=runtime_status)
    if not plan:
        result = {
            "status": "skipped",
            "message": "当前没有可自动采集的项目",
            "project_count": 0,
        }
        write_local_daily_sync_status(
            env_file=env_file,
            state_file_path=state_file_path,
            payload={
                **runtime_status,
                "state": "skipped",
                "phase": "idle",
                "message": result["message"],
                "finished_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "current_project": "",
                "current_project_index": 0,
                "current_project_scheduled_at": "",
                "waiting_for_login": False,
            },
        )
        return result

    project_results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    ordered_plan = sorted(plan.items(), key=lambda item: item[1]["scheduled_at"])
    total_projects = len(ordered_plan)
    for project_position, (project_name, payload) in enumerate(ordered_plan, start=1):
        scheduled_at = payload["scheduled_at"]
        urls = list(payload.get("urls") or [])
        if not urls:
            continue
        write_local_daily_sync_status(
            env_file=env_file,
            state_file_path=state_file_path,
            payload={
                **runtime_status,
                "state": "running",
                "phase": "waiting_window",
                "message": f"等待项目「{project_name}」进入采集时间",
                "current_project": project_name,
                "current_project_index": project_position,
                "current_project_total": total_projects,
                "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                "successful_projects": len(project_results),
                "failed_projects": len(failures),
                "waiting_for_login": False,
            },
        )
        _sleep_until(scheduled_at)
        login_payload = wait_for_xiaohongshu_login(
            env_file=env_file,
            settings=settings,
            sample_url=urls[0],
            on_wait=lambda payload, project_name=project_name, project_position=project_position, scheduled_at=scheduled_at: write_local_daily_sync_status(
                env_file=env_file,
                state_file_path=state_file_path,
                payload={
                    **runtime_status,
                    "state": "running",
                    "phase": "waiting_login",
                    "message": str(payload.get("message") or "等待小红书登录恢复"),
                    "current_project": project_name,
                    "current_project_index": project_position,
                    "current_project_total": total_projects,
                    "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "successful_projects": len(project_results),
                    "failed_projects": len(failures),
                    "waiting_for_login": True,
                },
            ),
            timeout_seconds=0,
        )
        if login_state_requires_interactive_login(login_payload):
            failures.append(
                {
                    "project": project_name,
                    "error": login_payload.get("message") or "登录态未恢复",
                }
            )
            write_local_daily_sync_status(
                env_file=env_file,
                state_file_path=state_file_path,
                payload={
                    **runtime_status,
                    "state": "partial",
                    "phase": "waiting_login",
                    "message": f"项目「{project_name}」登录态未恢复，已跳过本项目",
                    "current_project": project_name,
                    "current_project_index": project_position,
                    "current_project_total": total_projects,
                    "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "successful_projects": len(project_results),
                    "failed_projects": len(failures),
                    "last_error": login_payload.get("message") or "登录态未恢复",
                    "waiting_for_login": True,
                },
            )
            continue
        write_local_daily_sync_status(
            env_file=env_file,
            state_file_path=state_file_path,
            payload={
                **runtime_status,
                "state": "running",
                "phase": "collecting",
                "message": f"项目「{project_name}」正在采集",
                "current_project": project_name,
                "current_project_index": project_position,
                "current_project_total": total_projects,
                "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                "successful_projects": len(project_results),
                "failed_projects": len(failures),
                "waiting_for_login": False,
            },
        )
        try:
            summary = collect_profiles_to_local_cache(
                env_file=env_file,
                settings=settings,
                explicit_urls=urls,
                raw_text="",
                urls_file=None,
                project=project_name,
                scheduled=False,
            )
            if str(summary.get("status") or "").strip() == "partial":
                raise RuntimeError(
                    f"项目「{project_name}」仍有 {int(summary.get('pending_accounts') or 0)} 个账号未完成，已保留断点等待继续"
                )
            project_results.append(
                {
                    "project": project_name,
                    "scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    **summary,
                }
            )
            write_local_daily_sync_status(
                env_file=env_file,
                state_file_path=state_file_path,
                payload={
                    **runtime_status,
                    "state": "running",
                    "phase": "collecting",
                    "message": f"项目「{project_name}」采集完成",
                    "current_project": project_name,
                    "current_project_index": project_position,
                    "current_project_total": total_projects,
                    "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "successful_projects": len(project_results),
                    "failed_projects": len(failures),
                    "waiting_for_login": False,
                },
            )
        except Exception as exc:
            failures.append(
                {
                    "project": project_name,
                    "scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "error": str(exc),
                }
            )
            write_local_daily_sync_status(
                env_file=env_file,
                state_file_path=state_file_path,
                payload={
                    **runtime_status,
                    "state": "partial",
                    "phase": "collecting",
                    "message": f"项目「{project_name}」采集失败",
                    "current_project": project_name,
                    "current_project_index": project_position,
                    "current_project_total": total_projects,
                    "current_project_scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "successful_projects": len(project_results),
                    "failed_projects": len(failures),
                    "last_error": str(exc),
                    "waiting_for_login": False,
                },
            )

    if failures:
        result = {
            "status": "partial",
            "message": "存在项目采集失败，本轮不自动上传服务器",
            "project_count": len(plan),
            "successful_projects": len(project_results),
            "failed_projects": len(failures),
            "projects": project_results,
            "failures": failures,
        }
        write_local_daily_sync_status(
            env_file=env_file,
            state_file_path=state_file_path,
            payload={
                **runtime_status,
                "state": "partial",
                "phase": "finished",
                "message": result["message"],
                "finished_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "current_project": "",
                "current_project_index": 0,
                "current_project_scheduled_at": "",
                "successful_projects": len(project_results),
                "failed_projects": len(failures),
                "last_error": str((failures[0] or {}).get("error") or ""),
                "upload_state": "skipped",
                "upload_message": "本轮存在采集失败，未自动上传服务器",
                "waiting_for_login": False,
            },
        )
        return result

    write_local_daily_sync_status(
        env_file=env_file,
        state_file_path=state_file_path,
        payload={
            **runtime_status,
            "state": "running",
            "phase": "uploading",
            "message": "自动采集已完成，正在上传服务器",
            "current_project": "",
            "current_project_index": total_projects,
            "current_project_total": total_projects,
            "current_project_scheduled_at": "",
            "successful_projects": len(project_results),
            "failed_projects": 0,
            "upload_state": "running",
            "upload_message": "自动上传服务器进行中",
            "waiting_for_login": False,
        },
    )
    try:
        upload_result = push_current_cache_to_server(env_file=env_file, urls_file=urls_file)
    except Exception as exc:
        result = {
            "status": "partial",
            "message": "本地自动采集完成，但上传服务器失败",
            "project_count": len(project_results),
            "projects": project_results,
            "upload_error": str(exc),
        }
        write_local_daily_sync_status(
            env_file=env_file,
            state_file_path=state_file_path,
            payload={
                **runtime_status,
                "state": "partial",
                "phase": "uploading",
                "message": result["message"],
                "finished_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "current_project": "",
                "current_project_index": 0,
                "current_project_scheduled_at": "",
                "successful_projects": len(project_results),
                "failed_projects": 0,
                "upload_state": "error",
                "upload_message": "上传服务器失败",
                "last_upload_error": str(exc),
                "waiting_for_login": False,
            },
        )
        return result

    finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
    result = {
        "status": "success",
        "message": "本地自动采集完成，并已上传服务器",
        "project_count": len(project_results),
        "projects": project_results,
        "upload": upload_result,
    }
    write_local_daily_sync_status(
        env_file=env_file,
        state_file_path=state_file_path,
        payload={
            **runtime_status,
            "state": "success",
            "phase": "finished",
            "message": result["message"],
            "finished_at": finished_at,
            "last_success_at": finished_at,
            "current_project": "",
            "current_project_index": 0,
            "current_project_scheduled_at": "",
            "successful_projects": len(project_results),
            "failed_projects": 0,
            "upload_state": "success",
            "upload_message": "自动上传服务器成功",
            "last_upload_success_at": str(upload_result.get("updated_at") or finished_at),
            "last_upload_error": "",
            "last_error": "",
            "waiting_for_login": False,
        },
    )
    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="每天 14:00 由 launchd 拉起本地全量采集并在成功后上传服务器。")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--urls-file", default="xhs_feishu_monitor/input/robam_multi_profile_urls.txt")
    parser.add_argument("--install-launchd", action="store_true", help="安装本地 launchd 定时任务")
    parser.add_argument("--unload-launchd", action="store_true", help="卸载本地 launchd 定时任务")
    parser.add_argument("--load-launchd", action="store_true", help="安装后立即加载 launchd 任务")
    parser.add_argument("--cleanup-legacy-launchd", action="store_true", help="清理旧的飞书 launchd 任务与日志")
    parser.add_argument("--launchd-label", default=DEFAULT_LOCAL_DAILY_SYNC_LABEL, help="launchd 任务标签")
    parser.add_argument("--launchd-plist", help="自定义 launchd plist 路径")
    parser.add_argument("--stdout-log", help="launchd stdout 日志路径")
    parser.add_argument("--stderr-log", help="launchd stderr 日志路径")
    args = parser.parse_args(argv)

    if args.unload_launchd:
        plist_path = uninstall_local_daily_sync_launchd(
            env_file=args.env_file,
            label=args.launchd_label,
            plist_path=args.launchd_plist,
        )
        print(f"[OK] 已卸载 launchd 任务: {args.launchd_label}")
        print(f"[OK] plist 路径: {plist_path}")
        print("[OK] 已把 XHS_SCHEDULE_DRIVER 切回 app")
        return 0

    if args.cleanup_legacy_launchd:
        result = cleanup_legacy_feishu_launchd_jobs()
        print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
        return 0

    if args.install_launchd:
        paths = install_local_daily_sync_launchd(
            env_file=args.env_file,
            urls_file=args.urls_file,
            label=args.launchd_label,
            plist_path=args.launchd_plist,
            stdout_log_path=args.stdout_log,
            stderr_log_path=args.stderr_log,
            load_after_install=args.load_launchd,
        )
        print(f"[OK] 已生成 launchd 任务: {args.launchd_label}")
        print(f"[OK] plist 路径: {paths['plist_path']}")
        print(f"[OK] stdout 日志: {paths['stdout_log_path']}")
        print(f"[OK] stderr 日志: {paths['stderr_log_path']}")
        print("[OK] 已把 XHS_SCHEDULE_DRIVER 切到 launchd")
        return 0

    result = run_local_daily_sync(env_file=args.env_file, urls_file=args.urls_file)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") in {"success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
