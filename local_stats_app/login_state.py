from __future__ import annotations

import subprocess
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from ..chrome_cookies import (
    export_xiaohongshu_cookie_header,
    is_default_chrome_profile_root,
    resolve_chrome_profile_directory,
    resolve_chrome_profile_root,
)
from ..config import load_settings
from ..profile_batch_report import normalize_profile_url
from ..profile_report import (
    build_profile_report,
    enrich_profile_report_with_note_metrics,
    load_profile_report_payload,
)


LOGIN_STATE_IDLE_PAYLOAD = {
    "state": "idle",
    "message": "等待自动自检",
    "checked_at": "",
    "cache_age_seconds": 0,
    "checking": False,
    "fetch_mode": "",
    "cookie_source": "none",
    "cookie_source_label": "未配置登录态",
    "cookie_ready": False,
    "detail_ready": False,
    "degraded": False,
    "sample_url": "",
    "sample_account": "",
    "sample_user_id": "",
    "work_count": 0,
    "note_id_count": 0,
    "comment_count_ready": 0,
    "hints": [],
}

LOGIN_WAIT_TIMEOUT_SECONDS = 180
LOGIN_WAIT_POLL_SECONDS = 5
LOGIN_STATE_SAMPLE_WORK_LIMIT = 3


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def detect_cookie_source(settings) -> tuple[str, str]:
    if str(getattr(settings, "xhs_cookie", "") or "").strip():
        return "manual_cookie", "手动 Cookie"
    profile_root = str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip()
    if profile_root:
        if is_default_chrome_profile_root(profile_root):
            return "chrome_profile", "Chrome 默认资料"
        profile_name = Path(profile_root).name or "Chrome"
        return "chrome_profile", f"Chrome 登录态 · {profile_name}"
    return "none", "未配置登录态"


def build_login_state_payload(**overrides: Any) -> Dict[str, Any]:
    payload = dict(LOGIN_STATE_IDLE_PAYLOAD)
    payload.update(overrides)
    hints = payload.get("hints") or []
    payload["hints"] = [str(item) for item in hints if str(item).strip()]
    return payload


def login_state_requires_interactive_login(payload: Dict[str, Any]) -> bool:
    state = str(payload.get("state") or "").strip()
    message = str(payload.get("message") or "").strip().lower()
    if state != "error":
        return False
    return any(
        keyword in message
        for keyword in (
            "登录态",
            "登录页",
            "/login",
            "空结果",
            "未解析到任何作品",
            "反爬页",
        )
    )


def login_state_allows_collection_start(payload: Dict[str, Any]) -> bool:
    if login_state_requires_interactive_login(payload):
        return False
    return bool(payload.get("detail_ready"))


def explain_collection_start_block(payload: Dict[str, Any]) -> str:
    if login_state_requires_interactive_login(payload):
        if payload.get("login_window_opened"):
            return "检测到小红书未登录，已弹出网页登录窗口；完成登录前不会开始采集，避免空跑。"
        return "检测到小红书登录态异常，当前不会开始采集；请先完成登录后再试。"
    if not bool(payload.get("detail_ready")):
        return "样本账号还拿不到作品详情，当前不会开始采集，避免空跑。"
    return ""


def is_transient_self_check_failure(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    return any(
        keyword in text
        for keyword in (
            "__initial_state__",
            "err_connection_closed",
            "connection closed",
            "remote end closed connection",
            "remote disconnected",
            "connection aborted",
            "timed out",
            "timeout",
            "page.goto",
            "html 中未找到可解析的",
            "返回空结果或登录跳转",
        )
    ) and not any(
        keyword in text
        for keyword in (
            "当前登录态不可用",
            "登录态",
            "/login",
            "登录页",
        )
    )


def run_login_state_self_check(*, env_file: str, sample_url: str = "") -> Dict[str, Any]:
    settings = load_settings(env_file)
    fetch_mode = str(getattr(settings, "xhs_fetch_mode", "") or "").strip().lower() or "requests"
    cookie_source, cookie_source_label = detect_cookie_source(settings)
    checked_at = iso_now()
    hints: List[str] = []

    if fetch_mode != "requests":
        message = f"当前抓取模式为 {fetch_mode}，自动自检先只校验配置；样本抓取建议通过手动同步确认。"
        if fetch_mode == "local_browser":
            hints.append("local_browser 模式会直接调用本机浏览器，不适合频繁后台自检。")
        return build_login_state_payload(
            state="warning",
            message=message,
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=True,
            sample_url=sample_url,
            hints=hints,
        )

    cookie_ready = False
    if cookie_source == "manual_cookie":
        cookie_ready = True
    elif cookie_source == "chrome_profile":
        try:
            cookie_ready = bool(
                export_xiaohongshu_cookie_header(
                    settings.xhs_chrome_cookie_profile,
                    resolve_chrome_profile_directory(settings.playwright_profile_directory),
                ).strip()
            )
        except Exception as exc:
            return build_login_state_payload(
                state="error",
                message=f"Chrome 登录态读取失败：{exc}",
                checked_at=checked_at,
                fetch_mode=fetch_mode,
                cookie_source=cookie_source,
                cookie_source_label=cookie_source_label,
                cookie_ready=False,
                sample_url=sample_url,
                hints=[
                    "重新用本机 Chrome 登录小红书后，再点一次“立即自检”。",
                    "确认 XHS_CHROME_COOKIE_PROFILE 仍指向可用的登录目录。",
                ],
            )
    else:
        hints.append("未配置 XHS_COOKIE 或 Chrome 登录态目录，当前只能依赖公开页能力。")

    if not sample_url:
        return build_login_state_payload(
            state="warning" if cookie_source == "none" else "ok",
            message="已完成登录态配置检查；待添加监测账号后会继续做样本抓取自检。",
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=cookie_ready,
            hints=hints,
        )

    try:
        payload = load_profile_report_payload(settings=settings, profile_url=sample_url)
        report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
        if report.get("works"):
            sample_report = {
                "captured_at": report.get("captured_at"),
                "profile": dict(report.get("profile") or {}),
                "works": [dict(item) for item in (report.get("works") or [])[:LOGIN_STATE_SAMPLE_WORK_LIMIT]],
            }
            enrich_profile_report_with_note_metrics(report=sample_report, settings=settings)
            sample_works = sample_report.get("works") or []
            if sample_works:
                work_index = {str(item.get("note_id") or ""): item for item in sample_works if str(item.get("note_id") or "").strip()}
                merged_works = []
                for work in report.get("works") or []:
                    note_id = str(work.get("note_id") or "").strip()
                    if note_id and note_id in work_index:
                        merged_works.append({**work, **work_index[note_id]})
                    else:
                        merged_works.append(work)
                report["works"] = merged_works
    except Exception as exc:
        if is_transient_self_check_failure(str(exc)):
            return build_login_state_payload(
                state="warning",
                message=f"样本账号抓取异常：{exc}",
                checked_at=checked_at,
                fetch_mode=fetch_mode,
                cookie_source=cookie_source,
                cookie_source_label=cookie_source_label,
                cookie_ready=cookie_ready,
                sample_url=sample_url,
                hints=[
                    "这次更像网络波动或页面结构变化，暂时不能据此判断登录态失效。",
                    "可先手动同步一次；若本地看板仍能更新，登录态通常仍可用。",
                ],
            )
        return build_login_state_payload(
            state="error",
            message=f"样本账号抓取失败：{exc}",
            checked_at=checked_at,
            fetch_mode=fetch_mode,
            cookie_source=cookie_source,
            cookie_source_label=cookie_source_label,
            cookie_ready=cookie_ready,
            sample_url=sample_url,
            hints=[
                "先在浏览器里打开小红书确认当前登录态仍有效。",
                "如果刚重新登录，点一次“立即自检”刷新状态。",
            ],
        )

    profile = report.get("profile") or {}
    works = report.get("works") or []
    sample_account = str(profile.get("nickname") or "").strip()
    sample_user_id = str(profile.get("profile_user_id") or "").strip()
    work_count = len(works)
    note_id_count = sum(1 for item in works if str(item.get("note_id") or "").strip())
    comment_count_ready = sum(1 for item in works if item.get("comment_count") is not None)
    detail_ready = note_id_count > 0
    has_profile_core = bool(sample_account or sample_user_id or profile.get("fans_count_text"))

    if not has_profile_core and work_count == 0:
        state = "error"
        message = "样本账号返回了空结果，登录态可能已过期，或当前请求命中了反爬页。"
        hints.extend(
            [
                "先在本机 Chrome 打开小红书主页，确认账号仍处于登录状态。",
                "如果当前是 Chrome 登录态模式，建议重新登录后再点“立即自检”。",
            ]
        )
    elif work_count == 0:
        state = "error"
        message = "样本账号未解析到任何作品，详细数据链路当前不可用。"
        hints.extend(
            [
                "当前账号大概率退化成公开页或反爬结果，建议重新登录后复检。",
                "如持续为空，优先检查 XHS_CHROME_COOKIE_PROFILE 对应的登录目录是否正确。",
            ]
        )
    elif not detail_ready:
        state = "warning"
        message = "样本账号只拿到公开页摘要，作品详情与评论抓取能力暂时受限。"
        hints.extend(
            [
                "当前账号摘要仍可用，只是详情和评论抓取能力暂时不足。",
                "可先继续采集；若后续多次都拿不到 note_id，再重新登录后复检。",
            ]
        )
    elif comment_count_ready <= 0:
        state = "warning"
        message = "样本账号已拿到作品详情，但精确评论数仍不可用，当前不允许开始正式采集。"
        hints.extend(
            [
                "当前不是没登录，而是评论详情链路还没恢复；现在开始正式采集会空跑。",
                "建议先重新登录并再做一次自检，确认样本作品能拿到精确评论数后再开始。",
            ]
        )
    elif cookie_source == "none":
        state = "warning"
        message = "样本账号抓取正常，但当前没有稳定登录态来源，详细数据能力可能随时退化。"
        hints.append("建议改用本机 Chrome 登录态目录，长期稳定性会更高。")
    else:
        state = "ok"
        message = "登录态正常，样本账号已拿到作品明细能力。"
        hints.append("如果后面看见 note_id 或评论字段突然清空，直接点“立即自检”确认登录态。")

    return build_login_state_payload(
        state=state,
        message=message,
        checked_at=checked_at,
        fetch_mode=fetch_mode,
        cookie_source=cookie_source,
        cookie_source_label=cookie_source_label,
        cookie_ready=cookie_ready,
        detail_ready=detail_ready,
        degraded=state in {"warning", "error"},
        sample_url=sample_url,
        sample_account=sample_account,
        sample_user_id=sample_user_id,
        work_count=work_count,
        note_id_count=note_id_count,
        comment_count_ready=comment_count_ready,
        hints=hints,
    )


def open_xiaohongshu_login_window(*, settings, target_url: str = "") -> bool:
    url = str(target_url or "").strip() or "https://www.xiaohongshu.com/"
    chrome_profile_root = str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip()
    profile_directory = resolve_chrome_profile_directory(getattr(settings, "playwright_profile_directory", "") or "Default")
    if chrome_profile_root:
        try:
            if is_default_chrome_profile_root(chrome_profile_root):
                subprocess.Popen(
                    ["open", "-a", "Google Chrome", url],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return True
            subprocess.Popen(
                [
                    "open",
                    "-na",
                    "Google Chrome",
                    "--args",
                    f"--user-data-dir={resolve_chrome_profile_root(chrome_profile_root)}",
                    f"--profile-directory={profile_directory}",
                    url,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            pass
    try:
        return bool(webbrowser.open(url))
    except Exception:
        return False


def wait_for_xiaohongshu_login(
    *,
    env_file: str,
    settings,
    sample_url: str,
    on_wait: Callable[[Dict[str, Any]], None] | None = None,
    timeout_seconds: int = LOGIN_WAIT_TIMEOUT_SECONDS,
    poll_seconds: int = LOGIN_WAIT_POLL_SECONDS,
    run_self_check: Callable[..., Dict[str, Any]] | None = None,
    open_login_window: Callable[..., bool] | None = None,
) -> Dict[str, Any]:
    run_self_check_fn = run_self_check or run_login_state_self_check
    open_login_window_fn = open_login_window or open_xiaohongshu_login_window

    payload = run_self_check_fn(env_file=env_file, sample_url=sample_url)
    if not login_state_requires_interactive_login(payload):
        return payload
    can_open_window = bool(str(getattr(settings, "xhs_chrome_cookie_profile", "") or "").strip())
    window_opened = False
    if can_open_window:
        window_opened = open_login_window_fn(settings=settings, target_url=sample_url or "https://www.xiaohongshu.com/")
    waiting_payload = dict(payload)
    waiting_payload["login_window_opened"] = window_opened
    waiting_payload["message"] = (
        "检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。"
        if window_opened
        else "检测到小红书未登录，请先完成登录；自动任务会在登录恢复后继续。"
    )
    if on_wait is not None:
        on_wait(waiting_payload)
    if timeout_seconds > 0 and not window_opened:
        return waiting_payload

    deadline = None if int(timeout_seconds or 0) <= 0 else time.time() + max(1, int(timeout_seconds or 1))
    while deadline is None or time.time() < deadline:
        time.sleep(max(1, int(poll_seconds or 1)))
        payload = run_self_check_fn(env_file=env_file, sample_url=sample_url)
        payload["login_window_opened"] = window_opened
        if not login_state_requires_interactive_login(payload):
            return payload
        if on_wait is not None:
            on_wait(payload)
    payload["login_window_opened"] = window_opened
    return payload
