from __future__ import annotations

import os
import plistlib
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


DEFAULT_LABEL = "com.cc.xhs-feishu-monitor"


def build_launch_agent_plist(
    *,
    label: str,
    program_arguments: List[str],
    working_directory: str,
    interval_seconds: Optional[int] = None,
    start_calendar_interval: Optional[Dict[str, int]] = None,
    stdout_log_path: str,
    stderr_log_path: str,
    environment_variables: Optional[Dict[str, str]] = None,
) -> bytes:
    payload = {
        "Label": label,
        "ProgramArguments": program_arguments,
        "WorkingDirectory": working_directory,
        "RunAtLoad": True,
        "StandardOutPath": stdout_log_path,
        "StandardErrorPath": stderr_log_path,
    }
    if interval_seconds is not None:
        payload["StartInterval"] = interval_seconds
    if start_calendar_interval:
        payload["StartCalendarInterval"] = start_calendar_interval
    if environment_variables:
        payload["EnvironmentVariables"] = environment_variables
    return plistlib.dumps(payload, sort_keys=True)


def install_launch_agent(
    *,
    plist_bytes: bytes,
    label: str,
    plist_path: str,
    load_after_install: bool,
) -> None:
    path = Path(plist_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plist_bytes)
    if load_after_install:
        load_launch_agent(label=label, plist_path=str(path))


def load_launch_agent(*, label: str, plist_path: str) -> None:
    uid = str(os.getuid())
    path = str(Path(plist_path).expanduser().resolve())
    subprocess.run(
        ["launchctl", "bootout", f"gui/{uid}", path],
        check=False,
        capture_output=True,
        text=True,
    )
    subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", path], check=True)
    subprocess.run(["launchctl", "enable", f"gui/{uid}/{label}"], check=False)
    subprocess.run(["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"], check=False)


def unload_launch_agent(*, plist_path: str) -> None:
    uid = str(os.getuid())
    path = str(Path(plist_path).expanduser().resolve())
    subprocess.run(["launchctl", "bootout", f"gui/{uid}", path], check=False)


def default_paths(label: str) -> Dict[str, str]:
    launch_agents_dir = Path("~/Library/LaunchAgents").expanduser()
    logs_dir = Path("~/Library/Logs").expanduser()
    safe_label = label.replace("/", "-")
    return {
        "plist_path": str((launch_agents_dir / f"{safe_label}.plist").resolve()),
        "stdout_log_path": str((logs_dir / f"{safe_label}.out.log").resolve()),
        "stderr_log_path": str((logs_dir / f"{safe_label}.err.log").resolve()),
    }


def build_launch_environment() -> Dict[str, str]:
    environment = {
        "PYTHONUNBUFFERED": "1",
        "HOME": str(Path.home()),
        "USER": os.getenv("USER", ""),
        "LOGNAME": os.getenv("LOGNAME", ""),
        "SHELL": os.getenv("SHELL", "/bin/zsh"),
        "PATH": os.getenv("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
        "LANG": os.getenv("LANG", "C.UTF-8"),
    }
    for name in (
        "TMPDIR",
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ):
        value = os.getenv(name, "")
        if value:
            environment[name] = value
    return {key: value for key, value in environment.items() if value}


def wrap_program_arguments_for_login_shell(
    *,
    program_arguments: List[str],
    working_directory: str,
    shell: str = "/bin/zsh",
) -> List[str]:
    command = f"cd {shlex.quote(working_directory)} && {shlex.join(program_arguments)}"
    return [shell, "-lc", command]


def build_sync_program_arguments(
    *,
    targets_path: str,
    env_file_path: str,
    state_file_path: Optional[str] = None,
) -> List[str]:
    argv = [
        sys.executable,
        "-m",
        "xhs_feishu_monitor",
        "--targets",
        str(Path(targets_path).expanduser().resolve()),
        "--env-file",
        str(Path(env_file_path).expanduser().resolve()),
    ]
    if state_file_path:
        argv.extend(
            [
                "--state-file",
                str(Path(state_file_path).expanduser().resolve()),
            ]
        )
    return argv
