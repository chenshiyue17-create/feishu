from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
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


DEFAULT_LOCAL_DAILY_SYNC_LABEL = "com.cc.xhs-local-daily-sync"


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
    entries = parse_monitored_entries(urls_file)
    plan = build_auto_project_schedule(settings=settings, entries=entries)
    if not plan:
        return {
            "status": "skipped",
            "message": "当前没有可自动采集的项目",
            "project_count": 0,
        }

    project_results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for project_name, payload in sorted(plan.items(), key=lambda item: item[1]["scheduled_at"]):
        scheduled_at = payload["scheduled_at"]
        urls = list(payload.get("urls") or [])
        if not urls:
            continue
        _sleep_until(scheduled_at)
        login_payload = wait_for_xiaohongshu_login(
            env_file=env_file,
            settings=settings,
            sample_url=urls[0],
            timeout_seconds=0,
        )
        if login_state_requires_interactive_login(login_payload):
            failures.append(
                {
                    "project": project_name,
                    "error": login_payload.get("message") or "登录态未恢复",
                }
            )
            continue
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
            project_results.append(
                {
                    "project": project_name,
                    "scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    **summary,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "project": project_name,
                    "scheduled_at": scheduled_at.isoformat(timespec="seconds"),
                    "error": str(exc),
                }
            )

    if failures:
        return {
            "status": "partial",
            "message": "存在项目采集失败，本轮不自动上传服务器",
            "project_count": len(plan),
            "successful_projects": len(project_results),
            "failed_projects": len(failures),
            "projects": project_results,
            "failures": failures,
        }

    upload_result = push_current_cache_to_server(env_file=env_file, urls_file=urls_file)
    return {
        "status": "success",
        "message": "本地自动采集完成，并已上传服务器",
        "project_count": len(project_results),
        "projects": project_results,
        "upload": upload_result,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="每天 14:00 由 launchd 拉起本地全量采集并在成功后上传服务器。")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--urls-file", default="xhs_feishu_monitor/input/robam_multi_profile_urls.txt")
    parser.add_argument("--install-launchd", action="store_true", help="安装本地 launchd 定时任务")
    parser.add_argument("--unload-launchd", action="store_true", help="卸载本地 launchd 定时任务")
    parser.add_argument("--load-launchd", action="store_true", help="安装后立即加载 launchd 任务")
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
