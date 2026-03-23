from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .config import load_settings
from .launchd import (
    build_launch_environment,
    build_launch_agent_plist,
    default_paths,
    install_launch_agent,
    unload_launch_agent,
    wrap_program_arguments_for_login_shell,
)
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload
from .profile_live_sync import parse_daily_time


DEFAULT_BATCH_LABEL = "com.cc.xhs-profile-batch-report"


PROFILE_URL_START_PATTERN = re.compile(
    r"(?:(?:https?://)?(?:www\.)?xiaohongshu\.com/user/profile/[0-9a-z]+)",
    re.IGNORECASE,
)
PROFILE_URL_PATTERN = re.compile(
    r"(?:(?:https?://)?(?:www\.)?xiaohongshu\.com/user/profile/(?P<user_id>[0-9a-z]+))",
    re.IGNORECASE,
)


class BatchThrottle:
    def __init__(
        self,
        *,
        request_interval_seconds: float,
        chunk_size: int,
        chunk_cooldown_seconds: float,
    ) -> None:
        self.request_interval_seconds = max(0.0, float(request_interval_seconds or 0.0))
        self.chunk_size = max(0, int(chunk_size or 0))
        self.chunk_cooldown_seconds = max(0.0, float(chunk_cooldown_seconds or 0.0))
        self._lock = threading.Lock()
        self._next_available_at = 0.0
        self._started_count = 0

    def wait(self) -> None:
        sleep_seconds = 0.0
        with self._lock:
            now = time.time()
            sleep_seconds = max(0.0, self._next_available_at - now)
            release_at = max(now, self._next_available_at)
            self._started_count += 1
            if (
                self.chunk_size > 0
                and self.chunk_cooldown_seconds > 0
                and self._started_count > 1
                and (self._started_count - 1) % self.chunk_size == 0
            ):
                release_at += self.chunk_cooldown_seconds
            if self.request_interval_seconds > 0:
                release_at += self.request_interval_seconds
            self._next_available_at = release_at
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def is_retryable_batch_error(error_text: Any) -> bool:
    text = str(error_text or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if any(keyword in lowered for keyword in ("/login", "login", "cookie", "not logged", "unauthorized")):
        return False
    return any(
        keyword in lowered
        for keyword in (
            "timeout",
            "timed out",
            "connection reset",
            "temporarily unavailable",
            "rate limit",
            "too many requests",
            "403",
            "429",
            "超时",
            "风控",
            "反爬",
            "空结果",
        )
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="批量导出多个小红书账号主页摘要。")
    parser.add_argument("--url", action="append", default=[], help="单个账号主页链接，可重复传入")
    parser.add_argument("--urls-file", help="每行一个账号主页链接的文本文件")
    parser.add_argument("--raw-text", help="一段原始文本，脚本会自动提取其中的小红书主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--json-out", help="输出 JSON 文件路径")
    parser.add_argument("--csv-out", help="输出 CSV 文件路径")
    parser.add_argument("--install-launchd", action="store_true", help="生成并安装 launchd 定时任务")
    parser.add_argument("--load-launchd", action="store_true", help="安装后立刻加载 launchd 任务")
    parser.add_argument("--unload-launchd", action="store_true", help="卸载 launchd 任务")
    parser.add_argument("--daily-at", default="14:00", help="每天固定执行时间，格式 HH:MM")
    parser.add_argument("--launchd-label", default=DEFAULT_BATCH_LABEL, help="launchd 任务标签")
    parser.add_argument("--launchd-plist", help="launchd plist 路径")
    parser.add_argument("--stdout-log-path", help="stdout 日志路径")
    parser.add_argument("--stderr-log-path", help="stderr 日志路径")
    args = parser.parse_args(argv)

    if args.unload_launchd:
        plist_path = resolve_launchd_paths(label=args.launchd_label, plist_path=args.launchd_plist)["plist_path"]
        unload_launch_agent(plist_path=plist_path)
        print(f"[OK] unloaded launchd plist={plist_path}")
        return 0

    url_entries = normalize_profile_url_entries(args.url, args.raw_text or "", args.urls_file)
    urls = [item["url"] for item in url_entries]
    if not urls:
        raise ValueError("没有找到可用的小红书账号主页链接")

    if args.install_launchd:
        install_batch_launchd(
            urls=urls,
            urls_file=args.urls_file,
            raw_text=args.raw_text or "",
            env_file=args.env_file,
            json_out=args.json_out,
            csv_out=args.csv_out,
            daily_at=args.daily_at,
            label=args.launchd_label,
            plist_path=args.launchd_plist,
            stdout_log_path=args.stdout_log_path,
            stderr_log_path=args.stderr_log_path,
            load_after_install=args.load_launchd,
        )
        return 0

    settings = load_settings(args.env_file)
    reports = collect_profile_reports_with_progress(urls=urls, url_entries=url_entries, settings=settings)
    output = {
        "total": len(reports),
        "success": sum(1 for item in reports if item.get("status") == "success"),
        "failed": sum(1 for item in reports if item.get("status") != "success"),
        "items": reports,
    }

    rendered = json.dumps(output, ensure_ascii=False, indent=2)
    if args.json_out:
        path = Path(args.json_out).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        print(f"[OK] wrote {path}")
    if args.csv_out:
        write_batch_csv(args.csv_out, reports)
    print(rendered)
    return 0


def normalize_profile_urls(explicit_urls: List[str], raw_text: str, urls_file: Optional[str] = None) -> List[str]:
    return [item["url"] for item in normalize_profile_url_entries(explicit_urls, raw_text, urls_file)]


def normalize_profile_url_entries(
    explicit_urls: List[str],
    raw_text: str,
    urls_file: Optional[str] = None,
) -> List[Dict[str, str]]:
    normalized: List[str] = []
    entries: List[Dict[str, str]] = []
    seen: set[str] = set()
    candidates = [{"url": item, "project": ""} for item in explicit_urls if str(item).strip()]
    candidates.extend(load_url_entries_file(urls_file))
    candidates.extend({"url": item, "project": ""} for item in extract_profile_urls(raw_text))
    for item in candidates:
        fixed = normalize_profile_url(str(item.get("url") or ""))
        if not fixed or fixed in seen:
            continue
        seen.add(fixed)
        project = str(item.get("project") or "").strip()
        entries.append({"url": fixed, "project": project})
    return entries


def extract_profile_urls(raw_text: str) -> List[str]:
    text = str(raw_text or "").strip()
    if not text:
        return []
    starts = [match.start() for match in PROFILE_URL_START_PATTERN.finditer(text)]
    if not starts:
        return []

    urls: List[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(text)
        chunk = text[start:end].strip()
        if chunk:
            urls.append(chunk)
    return urls


def normalize_profile_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = "https://" + text.lstrip("/")
    text = text.strip(" ,，;；\n\t")
    match = PROFILE_URL_PATTERN.search(text)
    if not match:
        return text
    user_id = str(match.group("user_id") or "").strip()
    if not user_id:
        return text
    return f"https://www.xiaohongshu.com/user/profile/{user_id}"


def load_urls_file(path_text: Optional[str]) -> List[str]:
    return [item["url"] for item in load_url_entries_file(path_text)]


def load_url_entries_file(path_text: Optional[str]) -> List[Dict[str, str]]:
    if not path_text:
        return []
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    entries: List[Dict[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        candidate = line
        project = ""
        if "\t" in candidate:
            project, candidate = candidate.split("\t", 1)
        entries.append({"url": candidate.strip(), "project": project.strip()})
    return entries


def collect_profile_reports(
    *,
    urls: List[str],
    settings,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    return collect_profile_reports_with_progress(
        urls=urls,
        url_entries=None,
        settings=settings,
        progress_callback=progress_callback,
    )


def collect_profile_reports_with_progress(
    *,
    urls: List[str],
    url_entries: Optional[List[Dict[str, str]]] = None,
    settings,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    normalized_entries = _normalize_batch_entries(urls=urls, url_entries=url_entries)
    ordered_urls = [item["url"] for item in normalized_entries]
    max_workers = resolve_batch_concurrency(settings)
    throttle = build_batch_throttle(settings)
    retry_failed_once = bool(getattr(settings, "xhs_batch_retry_failed_once", True))
    retry_delay_seconds = max(0.0, float(getattr(settings, "xhs_batch_retry_delay_seconds", 20.0) or 0.0))
    project_cooldown_seconds = max(0.0, float(getattr(settings, "xhs_batch_project_cooldown_seconds", 45.0) or 0.0))
    if max_workers == 1 or len(ordered_urls) <= 1:
        results = []
        total = len(ordered_urls)
        success_count = 0
        failed_count = 0
        current = 0
        project_batches = build_project_batches(normalized_entries)
        for group_index, group in enumerate(project_batches):
            for entry in group:
                current += 1
                url = entry["url"]
                item = _collect_single_profile_report_with_throttle(url=url, settings=settings, throttle=throttle)
                item = _retry_failed_item_if_needed(
                    item=item,
                    url=url,
                    settings=settings,
                    throttle=throttle,
                    retry_failed_once=retry_failed_once,
                    retry_delay_seconds=retry_delay_seconds,
                )
                results.append(item)
                if item.get("status") == "success":
                    success_count += 1
                else:
                    failed_count += 1
                _emit_collect_progress(
                    progress_callback=progress_callback,
                    current=current,
                    total=total,
                    item=item,
                    success_count=success_count,
                    failed_count=failed_count,
                )
            if group_index < len(project_batches) - 1:
                _sleep_between_project_batches(project_cooldown_seconds=project_cooldown_seconds)
        return results

    indexed_results: Dict[int, Dict[str, Any]] = {}
    pending_retry_indexes: List[int] = []
    completed = 0
    total = len(ordered_urls)
    success_count = 0
    failed_count = 0
    url_to_index = {item["url"]: index for index, item in enumerate(normalized_entries)}
    project_batches = build_project_batches(normalized_entries)
    for group_index, group in enumerate(project_batches):
        group_urls = [item["url"] for item in group]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_collect_single_profile_report_with_throttle, url=url, settings=settings, throttle=throttle): url
                for url in group_urls
            }
            for future in as_completed(future_map):
                item = future.result()
                index = url_to_index[future_map[future]]
                indexed_results[index] = item
                if retry_failed_once and item.get("status") != "success" and is_retryable_batch_error(item.get("error")):
                    pending_retry_indexes.append(index)
                    continue
                completed += 1
                if item.get("status") == "success":
                    success_count += 1
                else:
                    failed_count += 1
                _emit_collect_progress(
                    progress_callback=progress_callback,
                    current=completed,
                    total=total,
                    item=item,
                    success_count=success_count,
                    failed_count=failed_count,
                )
        if group_index < len(project_batches) - 1:
            _sleep_between_project_batches(project_cooldown_seconds=project_cooldown_seconds)
    for index in sorted(pending_retry_indexes):
        retried = _retry_failed_item_if_needed(
            item=indexed_results[index],
            url=ordered_urls[index],
            settings=settings,
            throttle=throttle,
            retry_failed_once=retry_failed_once,
            retry_delay_seconds=retry_delay_seconds,
        )
        indexed_results[index] = retried
        completed += 1
        if retried.get("status") == "success":
            success_count += 1
        else:
            failed_count += 1
        _emit_collect_progress(
            progress_callback=progress_callback,
            current=completed,
            total=total,
            item=retried,
            success_count=success_count,
            failed_count=failed_count,
        )
    return [indexed_results[index] for index in range(len(ordered_urls))]


def _normalize_batch_entries(*, urls: List[str], url_entries: Optional[List[Dict[str, str]]]) -> List[Dict[str, str]]:
    if url_entries:
        normalized: List[Dict[str, str]] = []
        for item in url_entries:
            url = normalize_profile_url(str(item.get("url") or ""))
            if not url:
                continue
            normalized.append({"url": url, "project": str(item.get("project") or "").strip()})
        return normalized
    return [{"url": normalize_profile_url(url), "project": ""} for url in urls if normalize_profile_url(url)]


def build_project_batches(url_entries: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    order: List[str] = []
    for item in url_entries:
        project = str(item.get("project") or "").strip()
        if project not in grouped:
            grouped[project] = []
            order.append(project)
        grouped[project].append(item)
    return [grouped[project] for project in order]


def _sleep_between_project_batches(*, project_cooldown_seconds: float) -> None:
    if project_cooldown_seconds <= 0:
        return
    time.sleep(project_cooldown_seconds)


def _emit_collect_progress(
    *,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]],
    current: int,
    total: int,
    item: Dict[str, Any],
    success_count: int,
    failed_count: int,
) -> None:
    if progress_callback is None:
        return
    profile = item.get("profile") or {}
    progress_callback(
        {
            "phase": "collect",
            "current": current,
            "total": total,
            "status": item.get("status") or "",
            "url": str(item.get("requested_url") or ""),
            "profile_url": str(item.get("final_url") or item.get("requested_url") or ""),
            "account": str(profile.get("nickname") or profile.get("profile_user_id") or ""),
            "account_id": str(profile.get("profile_user_id") or ""),
            "fans_text": str(profile.get("fans_count_text") or ""),
            "interaction_text": str(profile.get("interaction_count_text") or ""),
            "works_text": str(
                profile.get("work_count_display_text")
                or profile.get("total_work_count")
                or profile.get("visible_work_count")
                or len(item.get("works") or [])
            ),
            "works": len(item.get("works") or []),
            "error": str(item.get("error") or ""),
            "success_count": max(0, int(success_count or 0)),
            "failed_count": max(0, int(failed_count or 0)),
        }
    )


def resolve_batch_concurrency(settings) -> int:
    fetch_mode = str(getattr(settings, "xhs_fetch_mode", "requests") or "requests").strip().lower()
    configured = max(1, int(getattr(settings, "xhs_batch_concurrency", 2) or 1))
    if fetch_mode in {"playwright", "local_browser"}:
        return 1
    proxy_pool = list(getattr(settings, "xhs_proxy_pool", []) or [])
    if len(proxy_pool) <= 1:
        return min(configured, 2)
    return min(configured, min(4, len(proxy_pool)))


def build_batch_throttle(settings) -> BatchThrottle:
    return BatchThrottle(
        request_interval_seconds=float(getattr(settings, "xhs_batch_request_interval_seconds", 2.0) or 0.0),
        chunk_size=int(getattr(settings, "xhs_batch_chunk_size", 8) or 0),
        chunk_cooldown_seconds=float(getattr(settings, "xhs_batch_chunk_cooldown_seconds", 12.0) or 0.0),
    )


def _collect_single_profile_report_with_throttle(*, url: str, settings, throttle: Optional[BatchThrottle]) -> Dict[str, Any]:
    if throttle is not None:
        throttle.wait()
    return _collect_single_profile_report(url=url, settings=settings)


def _retry_failed_item_if_needed(
    *,
    item: Dict[str, Any],
    url: str,
    settings,
    throttle: Optional[BatchThrottle],
    retry_failed_once: bool,
    retry_delay_seconds: float,
) -> Dict[str, Any]:
    if not retry_failed_once or item.get("status") == "success" or not is_retryable_batch_error(item.get("error")):
        return item
    if retry_delay_seconds > 0:
        time.sleep(retry_delay_seconds)
    retried = _collect_single_profile_report_with_throttle(url=url, settings=settings, throttle=throttle)
    retried["retried"] = True
    retried["retry_error"] = str(item.get("error") or "")
    retried["retry_status"] = "success" if retried.get("status") == "success" else "failed"
    return retried


def _collect_single_profile_report(*, url: str, settings) -> Dict[str, Any]:
    try:
        payload = load_profile_report_payload(settings=settings, profile_url=url)
        report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
        report = enrich_profile_report_with_note_metrics(report=report, settings=settings)
        return {
            "status": "success",
            "requested_url": url,
            "final_url": payload["final_url"],
            "profile": report["profile"],
            "works": report["works"],
        }
    except Exception as exc:
        return {
            "status": "failed",
            "requested_url": url,
            "error": str(exc),
        }


def write_batch_csv(path_text: str, reports: List[Dict[str, Any]]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "status",
        "requested_url",
        "final_url",
        "nickname",
        "profile_user_id",
        "red_id",
        "ip_location",
        "follows_count_text",
        "fans_count_text",
        "interaction_count_text",
        "visible_work_count",
        "total_work_count",
        "work_count_display_text",
        "top_titles",
        "error",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in reports:
            profile = item.get("profile") or {}
            works = item.get("works") or []
            writer.writerow(
                {
                    "status": item.get("status"),
                    "requested_url": item.get("requested_url", ""),
                    "final_url": item.get("final_url", ""),
                    "nickname": profile.get("nickname", ""),
                    "profile_user_id": profile.get("profile_user_id", ""),
                    "red_id": profile.get("red_id", ""),
                    "ip_location": profile.get("ip_location", ""),
                    "follows_count_text": profile.get("follows_count_text", ""),
                    "fans_count_text": profile.get("fans_count_text", ""),
                    "interaction_count_text": profile.get("interaction_count_text", ""),
                    "visible_work_count": profile.get("visible_work_count", ""),
                    "total_work_count": profile.get("total_work_count", ""),
                    "work_count_display_text": profile.get("work_count_display_text", ""),
                    "top_titles": " | ".join(work.get("title_copy", "") for work in works[:3]),
                    "error": item.get("error", ""),
                }
            )
    print(f"[OK] wrote {path}")


def install_batch_launchd(
    *,
    urls: List[str],
    urls_file: Optional[str],
    raw_text: str,
    env_file: str,
    json_out: Optional[str],
    csv_out: Optional[str],
    daily_at: str,
    label: str,
    plist_path: Optional[str],
    stdout_log_path: Optional[str],
    stderr_log_path: Optional[str],
    load_after_install: bool,
) -> None:
    resolved_paths = resolve_launchd_paths(
        label=label,
        plist_path=plist_path,
        stdout_log_path=stdout_log_path,
        stderr_log_path=stderr_log_path,
    )
    program_arguments = build_batch_program_arguments(
        urls=urls,
        urls_file=urls_file,
        raw_text=raw_text,
        env_file=env_file,
        json_out=json_out,
        csv_out=csv_out,
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


def build_batch_program_arguments(
    *,
    urls: List[str],
    urls_file: Optional[str],
    raw_text: str,
    env_file: str,
    json_out: Optional[str],
    csv_out: Optional[str],
) -> List[str]:
    argv = [
        sys.executable,
        "-m",
        "xhs_feishu_monitor.profile_batch_report",
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
    if json_out:
        argv.extend(["--json-out", str(Path(json_out).expanduser().resolve())])
    if csv_out:
        argv.extend(["--csv-out", str(Path(csv_out).expanduser().resolve())])
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


if __name__ == "__main__":
    raise SystemExit(main())
