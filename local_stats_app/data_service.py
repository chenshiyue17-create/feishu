from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional


def extract_link(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    if isinstance(value, list):
        for item in value:
            link = extract_link(item)
            if link:
                return link
    return ""


def extract_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("text") or "").strip()
    if isinstance(value, list):
        for item in value:
            text = extract_text(item)
            if text:
                return text
    return str(value or "").strip()


def to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace(",", "")
    if text.lstrip("-").isdigit():
        return int(text)
    return 0


def to_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip().replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def build_work_count_display(row: Dict[str, Any]) -> str:
    display_text = str(row.get("作品数展示") or "").strip()
    if display_text:
        return display_text
    total_text = str(row.get("账号总作品数") or "").strip()
    if total_text:
        return total_text
    visible_value = to_int(row.get("首页可见作品数"))
    if visible_value >= 30:
        return "30+"
    if visible_value > 0:
        return str(visible_value)
    return ""


def build_work_count_value(row: Dict[str, Any]) -> int:
    total_value = to_int(row.get("账号总作品数"))
    if total_value:
        return total_value
    visible_value = to_int(row.get("首页可见作品数"))
    if visible_value >= 30:
        return 30
    return visible_value


def pick_latest_date(calendar_rows: List[Dict[str, Any]]) -> str:
    latest = ""
    for row in calendar_rows:
        date_text = str(row.get("日期文本") or "").strip()
        if date_text and date_text > latest:
            latest = date_text
    return latest


def build_daily_series(calendar_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "date": "",
            "fans": 0,
            "likes": 0,
            "comments": 0,
            "works": 0,
            "accounts": 0,
        }
    )
    seen_accounts: Dict[str, set[str]] = defaultdict(set)

    for row in calendar_rows:
        date_text = str(row.get("日期文本") or "").strip()
        account_id = str(row.get("账号ID") or "").strip()
        if not date_text:
            continue
        bucket = grouped[date_text]
        bucket["date"] = date_text
        bucket["fans"] += to_int(row.get("粉丝数"))
        bucket["likes"] += to_int(row.get("首页总点赞"))
        bucket["comments"] += to_int(row.get("首页总评论"))
        bucket["works"] += to_int(row.get("首页可见作品数"))
        if account_id and account_id not in seen_accounts[date_text]:
            seen_accounts[date_text].add(account_id)
            bucket["accounts"] += 1

    return [grouped[key] for key in sorted(grouped)]


def build_account_series_map(calendar_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for row in calendar_rows:
        date_text = str(row.get("日期文本") or "").strip()
        account_id = str(row.get("账号ID") or "").strip()
        if not date_text or not account_id:
            continue
        grouped[account_id][date_text] = {
            "date": date_text,
            "fans": to_int(row.get("粉丝数")),
            "interaction": to_int(row.get("获赞收藏数")),
            "likes": to_int(row.get("首页总点赞")),
            "comments": to_int(row.get("首页总评论")),
            "works": build_work_count_value(row),
        }
    return {
        account_id: [series_by_date[key] for key in sorted(series_by_date)]
        for account_id, series_by_date in grouped.items()
    }


def build_account_cards(calendar_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest_by_account: Dict[str, Dict[str, Any]] = {}
    for row in calendar_rows:
        account_id = str(row.get("账号ID") or "").strip()
        date_text = str(row.get("日期文本") or "").strip()
        if not account_id or not date_text:
            continue
        current = latest_by_account.get(account_id)
        if current and str(current.get("date") or "").strip() >= date_text:
            continue
        latest_by_account[account_id] = {
            "account_id": account_id,
            "account": str(row.get("账号") or "").strip(),
            "date": date_text,
            "fans": to_int(row.get("粉丝数")),
            "interaction": to_int(row.get("获赞收藏数")),
            "works": build_work_count_value(row),
            "works_display": build_work_count_display(row),
            "works_exact": not build_work_count_display(row).endswith("+"),
            "likes": to_int(row.get("首页总点赞")),
            "comments": to_int(row.get("首页总评论")),
            "weekly_summary": str(row.get("周对比摘要") or "").strip(),
            "profile_url": extract_link(row.get("主页链接")),
            "top_title": str(row.get("头部作品标题") or "").strip(),
            "top_like": to_int(row.get("头部作品点赞")),
            "top_url": extract_link(row.get("头部作品链接")),
        }
    cards = list(latest_by_account.values())
    cards.sort(key=lambda item: (item["fans"], item["likes"], item["comments"], item["account"]), reverse=True)
    return cards


def build_rankings(ranking_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in ranking_rows:
        rank_type = str(row.get("榜单类型") or "").strip()
        if not rank_type:
            continue
        comment_basis = str(row.get("评论数口径") or row.get("单选") or "").strip()
        grouped[rank_type].append(
            {
                "rank": to_int(row.get("排名")),
                "account_id": str(row.get("账号ID") or "").strip(),
                "account": str(row.get("账号") or "").strip(),
                "title": str(row.get("标题文案") or "").strip(),
                "metric": row.get("排序值"),
                "summary": str(row.get("榜单摘要") or "").strip(),
                "comment_basis": comment_basis,
                "comment_is_lower_bound": comment_basis == "评论预览下限",
                "profile_url": extract_link(row.get("主页链接")),
                "note_url": extract_link(row.get("作品链接")),
                "cover_url": extract_link(row.get("封面图")),
            }
        )
    result: Dict[str, List[Dict[str, Any]]] = {}
    for rank_type, rows in grouped.items():
        rows.sort(key=lambda item: (item["rank"], item["title"]))
        result[rank_type] = rows
    return result


def build_alerts(alert_rows: List[Dict[str, Any]], *, top_n: int = 10) -> List[Dict[str, Any]]:
    alerts = []
    for row in alert_rows:
        like_delta = to_int(row.get("点赞增量"))
        comment_delta = to_int(row.get("评论增量"))
        alerts.append(
            {
                "date": str(row.get("预警日期") or "").strip(),
                "alert_type": str(row.get("预警类型") or "互动预警").strip(),
                "account_id": str(row.get("账号ID") or "").strip(),
                "account": str(row.get("账号") or "").strip(),
                "title": str(row.get("标题文案") or "").strip(),
                "current_likes": to_int(row.get("当前点赞数")),
                "previous_likes": to_int(row.get("基准点赞数")),
                "like_delta": like_delta,
                "current_comments": to_int(row.get("当前评论数")),
                "previous_comments": to_int(row.get("基准评论数")),
                "delta": max(like_delta, comment_delta),
                "comment_delta": comment_delta,
                "rate": to_float(row.get("评论增长率")),
                "status": str(row.get("通知状态") or "").strip(),
                "profile_url": extract_link(row.get("主页链接")),
                "note_url": extract_link(row.get("作品链接")),
            }
        )
    alerts.sort(key=lambda item: (item["date"], item["delta"], item["comment_delta"], item["like_delta"]), reverse=True)
    return alerts[:top_n]


def build_portal_card(portal_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not portal_rows:
        return {}
    row = max(portal_rows, key=lambda item: to_int(item.get("数据更新时间")))
    return {
        "updated_at": row.get("数据更新时间"),
        "accounts": to_int(row.get("监控账号数")),
        "fans": to_int(row.get("总粉丝数")),
        "interaction": to_int(row.get("总获赞收藏数")),
        "works": to_int(row.get("总作品数")),
        "likes": to_int(row.get("总点赞数")),
        "comments": to_int(row.get("总评论数")),
        "average_likes": row.get("平均点赞数") or 0,
        "average_comments": row.get("平均评论数") or 0,
        "weekly_summary": str(row.get("周对比摘要") or "").strip(),
        "top_title": str(row.get("头部作品标题") or "").strip(),
        "top_account": str(row.get("头部作品账号") or "").strip(),
        "top_like": to_int(row.get("头部作品点赞")),
        "top_url": extract_link(row.get("头部作品链接")),
    }


def to_iso_from_ms(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value) / 1000).astimezone().isoformat(timespec="seconds")
    text = str(value or "").strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000).astimezone().isoformat(timespec="seconds")
    return text


def build_dashboard_payload_from_tables(
    *,
    portal_rows: List[Dict[str, Any]],
    calendar_rows: List[Dict[str, Any]],
    ranking_rows: List[Dict[str, Any]],
    alert_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    portal = build_portal_card(portal_rows)
    series = build_daily_series(calendar_rows)
    account_series = build_account_series_map(calendar_rows)
    accounts = build_account_cards(calendar_rows)
    latest_date = pick_latest_date(calendar_rows)
    rankings = build_rankings(ranking_rows)
    alerts = build_alerts(alert_rows or [])
    updated_at = to_iso_from_ms(portal.get("updated_at")) if portal.get("updated_at") else ""
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "latest_date": latest_date,
        "updated_at": updated_at,
        "series_meta": {
            "mode": "daily",
            "update_time": "14:00",
            "source": "小红书日历留底",
            "note": "趋势图按天留底，每个账号每天保留 1 个点。",
        },
        "portal": portal,
        "series": series,
        "account_series": account_series,
        "accounts": accounts,
        "rankings": rankings,
        "alerts": alerts,
    }
