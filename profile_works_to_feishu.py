from __future__ import annotations

import argparse
import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .config import Settings, load_settings
from .feishu import FeishuBitableClient
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload


WORKS_TABLE_NAME = "小红书作品数据"
WORKS_TABLE_VIEW = "作品明细"
WORKS_CALENDAR_TABLE_NAME = "小红书作品日历留底"
WORKS_CALENDAR_TABLE_VIEW = "作品日历留底"
WORKS_TABLE_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "作品指纹", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "标题文案", "type": 1},
    {"field_name": "展示序号", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "作品类型", "type": 1},
    {"field_name": "点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞文本", "type": 1},
    {"field_name": "评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论文本", "type": 1},
    {"field_name": "评论增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论增长率", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "评论预警", "type": 1},
    {"field_name": "上周日期文本", "type": 1},
    {"field_name": "上周点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "上周评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "周对比摘要", "type": 1},
    {"field_name": "封面图", "type": 15},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "xsec_token", "type": 1},
    {"field_name": "抓取时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "备注", "type": 1},
]

WORKS_CALENDAR_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "日历键", "type": 1},
    {"field_name": "日历日期", "type": 5, "property": {"date_formatter": "yyyy-MM-dd"}},
    {"field_name": "日期文本", "type": 1},
    {"field_name": "作品指纹", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "标题文案", "type": 1},
    {"field_name": "作品类型", "type": 1},
    {"field_name": "点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞文本", "type": 1},
    {"field_name": "评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论文本", "type": 1},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "封面图", "type": 15},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把小红书账号首页作品同步到飞书多维表格单独数据表。")
    parser.add_argument("--url", required=True, help="小红书账号主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--table-name", default=WORKS_TABLE_NAME)
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    settings.validate_for_sync()
    payload = load_profile_report_payload(settings=settings, profile_url=args.url)
    report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    report = enrich_profile_report_with_note_metrics(report=report, settings=settings)

    tables_client = FeishuBitableClient(settings)
    table_id = ensure_works_table(tables_client=tables_client, settings=settings, table_name=args.table_name)

    work_settings = replace(settings, feishu_table_id=table_id)
    works_client = FeishuBitableClient(work_settings)
    works_client.ensure_fields(WORKS_TABLE_FIELDS)
    deleted = dedupe_work_records(works_client)

    synced = 0
    for work in report["works"]:
        fields = build_work_feishu_fields(report=report, work=work)
        action, record_id = works_client.upsert_record(
            unique_field="作品指纹",
            unique_value=fields["作品指纹"],
            fields=fields,
        )
        synced += 1
        print(f"[OK] {action} work_record_id={record_id} title={fields['标题文案']}")
    print(f"[OK] 表={args.table_name} table_id={table_id} synced={synced} deduped={deleted}")
    return 0


def ensure_works_table(*, tables_client: FeishuBitableClient, settings: Settings, table_name: str) -> str:
    tables = tables_client.list_tables()
    for table in tables:
        if str(table.get("name") or "").strip() == table_name:
            return str(table.get("table_id") or "")

    created = tables_client.create_table(
        table_name=table_name,
        default_view_name=WORKS_TABLE_VIEW,
        fields=WORKS_TABLE_FIELDS,
    )
    table_id = str(created.get("table_id") or "")
    if not table_id:
        raise ValueError(f"创建数据表失败: {table_name}")
    return table_id


def ensure_works_calendar_table(*, tables_client: FeishuBitableClient, settings: Settings, table_name: str = WORKS_CALENDAR_TABLE_NAME) -> str:
    tables = tables_client.list_tables()
    for table in tables:
        if str(table.get("name") or "").strip() == table_name:
            return str(table.get("table_id") or "")

    created = tables_client.create_table(
        table_name=table_name,
        default_view_name=WORKS_CALENDAR_TABLE_VIEW,
        fields=WORKS_CALENDAR_FIELDS,
    )
    table_id = str(created.get("table_id") or "")
    if not table_id:
        raise ValueError(f"创建数据表失败: {table_name}")
    return table_id


def build_work_feishu_fields(*, report: Dict[str, Any], work: Dict[str, Any]) -> Dict[str, Any]:
    profile = report["profile"]
    captured_at = report["captured_at"]
    fingerprint = build_work_fingerprint(
        profile_user_id=profile.get("profile_user_id") or "",
        title=work.get("title_copy") or "",
        cover_url=work.get("cover_url") or "",
    )
    note_url = work.get("note_url") or ""
    fields: Dict[str, Any] = {
        "作品指纹": fingerprint,
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "标题文案": work.get("title_copy") or "",
        "展示序号": int(work.get("index") or 0) + 1,
        "作品类型": work.get("note_type") or "",
        "点赞文本": work.get("like_count_text") or "",
        "评论文本": work.get("comment_count_text") or "",
        "主页链接": {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile.get("profile_url") or "",
        },
        "xsec_token": work.get("xsec_token") or "",
        "抓取时间": to_ms(captured_at),
        "备注": build_work_remark(work),
    }
    if isinstance(work.get("like_count"), (int, float)):
        fields["点赞数"] = int(work["like_count"])
    if isinstance(work.get("comment_count"), (int, float)):
        fields["评论数"] = int(work["comment_count"])
    if work.get("cover_url"):
        fields["封面图"] = {"text": "封面图", "link": work["cover_url"]}
    if note_url:
        fields["作品链接"] = {"text": "作品链接", "link": note_url}

    return {key: value for key, value in fields.items() if value not in ("", None)}


def build_work_calendar_fields(*, report: Dict[str, Any], work: Dict[str, Any]) -> Dict[str, Any]:
    fields = build_work_feishu_fields(report=report, work=work)
    snapshot_date = extract_snapshot_date(report.get("captured_at") or "")
    calendar_fields: Dict[str, Any] = {
        "日历键": f"{snapshot_date}|{fields['作品指纹']}",
        "日历日期": to_ms(report.get("captured_at") or ""),
        "日期文本": snapshot_date,
        "作品指纹": fields["作品指纹"],
        "账号ID": fields.get("账号ID") or "",
        "账号": fields.get("账号") or "",
        "标题文案": fields.get("标题文案") or "",
        "作品类型": fields.get("作品类型") or "",
        "点赞数": fields.get("点赞数"),
        "点赞文本": fields.get("点赞文本") or "",
        "评论数": fields.get("评论数"),
        "评论文本": fields.get("评论文本") or "",
        "作品链接": fields.get("作品链接"),
        "封面图": fields.get("封面图"),
        "数据更新时间": fields.get("抓取时间"),
    }
    return {key: value for key, value in calendar_fields.items() if value not in ("", None)}


def build_work_weekly_fields(*, current_fields: Dict[str, Any], baseline_fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not baseline_fields:
        return {"周对比摘要": "暂无 7 天前留底，作品周对比将在积累满 7 天后显示"}

    result: Dict[str, Any] = {}
    baseline_date = str(baseline_fields.get("日期文本") or "").strip()
    if baseline_date:
        result["上周日期文本"] = baseline_date
    summary_items: List[str] = []
    append_weekly_change(
        result=result,
        label="点赞",
        current_value=to_optional_int(current_fields.get("点赞数", current_fields.get("点赞文本"))),
        previous_value=to_optional_int(baseline_fields.get("点赞数", baseline_fields.get("点赞文本"))),
        previous_field="上周点赞数",
        delta_field="点赞周增量",
        rate_field="点赞周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="评论",
        current_value=to_optional_int(current_fields.get("评论数", current_fields.get("评论文本"))),
        previous_value=to_optional_int(baseline_fields.get("评论数", baseline_fields.get("评论文本"))),
        previous_field="上周评论数",
        delta_field="评论周增量",
        rate_field="评论周增幅",
        summary_items=summary_items,
    )
    if baseline_date and summary_items:
        result["周对比摘要"] = f"对比 {baseline_date} | {' | '.join(summary_items)}"
    elif baseline_date:
        result["周对比摘要"] = f"对比 {baseline_date}"
    else:
        result["周对比摘要"] = "暂无可用周对比"
    return result


def build_work_calendar_history_index(records: List[Dict[str, Any]]) -> Dict[str, List[tuple[date, Dict[str, Any]]]]:
    index: Dict[str, List[tuple[date, Dict[str, Any]]]] = {}
    for record in records:
        fields = record.get("fields") or {}
        fingerprint = str(fields.get("作品指纹") or "").strip()
        if not fingerprint:
            continue
        snapshot_date = parse_iso_date(fields.get("日期文本") or fields.get("日历日期"))
        if snapshot_date is None:
            continue
        index.setdefault(fingerprint, []).append((snapshot_date, fields))
    for entries in index.values():
        entries.sort(key=lambda item: item[0], reverse=True)
    return index


def select_work_weekly_baseline(
    *,
    history_index: Dict[str, List[tuple[date, Dict[str, Any]]]],
    fingerprint: str,
    snapshot_date: str,
) -> Optional[Dict[str, Any]]:
    target_date = parse_iso_date(snapshot_date)
    if not fingerprint or target_date is None:
        return None
    expected_date = target_date - timedelta(days=7)
    for candidate_date, fields in history_index.get(fingerprint, []):
        if candidate_date <= expected_date:
            return fields
    return None


def build_work_fingerprint(*, profile_user_id: str, title: str, cover_url: str) -> str:
    raw = f"{profile_user_id}|{title.strip()}|{normalize_cover_asset_key(cover_url)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def to_ms(iso_text: str) -> int:
    return int(datetime.fromisoformat(iso_text).timestamp() * 1000)


def build_work_remark(work: Dict[str, Any]) -> str:
    remarks: List[str] = ["公开主页卡片抓取"]
    if not work.get("note_id"):
        remarks.append("note_id 缺失")
    if not work.get("note_url"):
        remarks.append("作品链接缺失")
    return "；".join(remarks)


def normalize_cover_asset_key(cover_url: str) -> str:
    text = str(cover_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    last_segment = parsed.path.rsplit("/", 1)[-1]
    if not last_segment:
        return text
    return last_segment.split("!", 1)[0].strip()


def dedupe_work_records(works_client: FeishuBitableClient) -> int:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for record in works_client.list_records(page_size=500):
        fields = record.get("fields") or {}
        fingerprint = build_work_fingerprint(
            profile_user_id=str(fields.get("账号ID") or "").strip(),
            title=str(fields.get("标题文案") or "").strip(),
            cover_url=_extract_hyperlink(fields.get("封面图")),
        )
        if not fingerprint:
            continue
        entry = {
            "record_id": str(record.get("record_id") or "").strip(),
            "fingerprint": str(fields.get("作品指纹") or "").strip(),
            "captured_at": fields.get("抓取时间"),
        }
        groups.setdefault(fingerprint, []).append(entry)

    deleted = 0
    for fingerprint, entries in groups.items():
        entries.sort(key=_record_sort_key, reverse=True)
        keeper = entries[0]
        if keeper["record_id"] and keeper["fingerprint"] != fingerprint:
            works_client.update_record(keeper["record_id"], {"作品指纹": fingerprint})
        for entry in entries[1:]:
            if not entry["record_id"]:
                continue
            works_client.delete_record(entry["record_id"])
            deleted += 1
    return deleted


def _extract_hyperlink(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    if isinstance(value, list):
        for item in value:
            link = _extract_hyperlink(item)
            if link:
                return link
    return str(value or "").strip()


def _record_sort_key(entry: Dict[str, Any]) -> tuple[int, str]:
    captured_at = entry.get("captured_at")
    if isinstance(captured_at, (int, float)):
        return int(captured_at), str(entry.get("record_id") or "")
    if isinstance(captured_at, str) and captured_at.isdigit():
        return int(captured_at), str(entry.get("record_id") or "")
    return 0, str(entry.get("record_id") or "")


def extract_snapshot_date(iso_text: str) -> str:
    text = str(iso_text or "").strip()
    if "T" in text:
        return text.split("T", 1)[0]
    if len(text) >= 10:
        return text[:10]
    return datetime.now().astimezone().date().isoformat()


def parse_iso_date(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text) / 1000).date()
        except (OverflowError, ValueError):
            return None
    if "T" in text:
        text = text.split("T", 1)[0]
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def to_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace(",", "")
    if text.lstrip("-").isdigit():
        return int(text)
    return None


def compute_growth_rate(*, current_value: int, previous_value: int) -> Optional[float]:
    if previous_value <= 0:
        return None
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def append_weekly_change(
    *,
    result: Dict[str, Any],
    label: str,
    current_value: Optional[int],
    previous_value: Optional[int],
    previous_field: str,
    delta_field: str,
    rate_field: str,
    summary_items: List[str],
) -> None:
    if previous_value is None:
        return
    result[previous_field] = previous_value
    if current_value is None:
        return
    delta = current_value - previous_value
    result[delta_field] = delta
    rate = compute_growth_rate(current_value=current_value, previous_value=previous_value)
    if rate is not None:
        result[rate_field] = rate
        summary_items.append(f"{label} {delta:+d} ({rate:+.2f}%)")
    else:
        summary_items.append(f"{label} {delta:+d}")


if __name__ == "__main__":
    raise SystemExit(main())
