from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import load_settings
from .feishu import FeishuBitableClient
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload


PROFILE_TABLE_NAME = "小红书账号总览"
PROFILE_FIELD_SPECS: List[Dict[str, Any]] = [
    {"field_name": "账号", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "小红书号", "type": 1},
    {"field_name": "IP属地", "type": 1},
    {"field_name": "账号简介", "type": 1},
    {"field_name": "关注数文本", "type": 1},
    {"field_name": "粉丝数文本", "type": 1},
    {"field_name": "粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏文本", "type": 1},
    {"field_name": "首页可见作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "账号总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "作品数展示", "type": 1},
    {"field_name": "作品标题文案", "type": 1},
    {"field_name": "首页作品摘要", "type": 1},
    {"field_name": "平均点赞数", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "内容链接", "type": 15},
    {"field_name": "上报时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "备注", "type": 1},
]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="抓取小红书账号页并同步到飞书多维表格。")
    parser.add_argument("--url", required=True, help="小红书账号主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--table-name", default=PROFILE_TABLE_NAME)
    parser.add_argument("--ensure-fields", action="store_true", help="自动补齐缺失字段")
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    settings.validate_for_sync()
    payload = load_profile_report_payload(settings=settings, profile_url=args.url)
    report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    report = enrich_profile_report_with_note_metrics(report=report, settings=settings)

    tables_client = FeishuBitableClient(settings)
    table_id = ensure_profile_table(tables_client=tables_client, table_name=args.table_name)
    client = tables_client if table_id == settings.feishu_table_id else FeishuBitableClient(
        replace(settings, feishu_table_id=table_id)
    )
    if args.ensure_fields:
        client.ensure_fields(PROFILE_FIELD_SPECS)
    dedupe_profile_records(client)

    existing_fields = {str(item.get("field_name") or "").strip() for item in client.list_fields()}
    missing = [spec["field_name"] for spec in PROFILE_FIELD_SPECS if spec["field_name"] not in existing_fields]
    if missing:
        raise ValueError("飞书表缺少字段: " + ", ".join(missing))

    fields = build_profile_feishu_fields(report)
    action, record_id = client.upsert_record(
        unique_field="账号ID",
        unique_value=fields["账号ID"],
        fields=fields,
    )
    print(f"[OK] {action} record_id={record_id}")
    print(f"[OK] 账号={fields['账号']}")
    print(f"[OK] 表={args.table_name} table_id={table_id}")
    print(f"[OK] 作品数展示={report['profile'].get('work_count_display_text') or report['profile']['visible_work_count']}")
    return 0


def ensure_profile_table(*, tables_client: FeishuBitableClient, table_name: str) -> str:
    tables = tables_client.list_tables()
    for table in tables:
        if str(table.get("name") or "").strip() == table_name:
            return str(table.get("table_id") or "")

    created = tables_client.create_table(
        table_name=table_name,
        default_view_name="账号总览",
        fields=PROFILE_FIELD_SPECS,
    )
    table_id = str(created.get("table_id") or "")
    if not table_id:
        raise ValueError(f"创建数据表失败: {table_name}")
    return table_id


def build_profile_feishu_fields(report: Dict[str, Any]) -> Dict[str, Any]:
    profile = report["profile"]
    works = report["works"]
    average_like = _average([item.get("like_count") for item in works])
    title_copy = "\n".join(item.get("title_copy") or "" for item in works if item.get("title_copy"))
    work_summary = "\n".join(
        f"{index + 1}. {item.get('title_copy') or '无标题'} | 点赞: {item.get('like_count_text') or '未知'} | 类型: {item.get('note_type') or '未知'}"
        for index, item in enumerate(works)
    )

    fields: Dict[str, Any] = {
        "账号": profile.get("nickname") or profile.get("profile_user_id") or "",
        "账号ID": profile.get("profile_user_id") or "",
        "小红书号": profile.get("red_id") or "",
        "IP属地": profile.get("ip_location") or "",
        "账号简介": profile.get("desc") or "",
        "关注数文本": profile.get("follows_count_text") or "",
        "粉丝数文本": profile.get("fans_count_text") or "",
        "获赞收藏文本": profile.get("interaction_count_text") or "",
        "首页可见作品数": profile.get("visible_work_count") or 0,
        "账号总作品数": profile.get("total_work_count"),
        "作品数展示": profile.get("work_count_display_text") or str(profile.get("visible_work_count") or 0),
        "作品标题文案": title_copy,
        "首页作品摘要": work_summary,
        "平均点赞数": average_like,
        "内容链接": {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile.get("profile_url") or "",
        },
        "上报时间": _to_ms(report.get("captured_at") or ""),
        "备注": _build_remark(profile=profile, works=works),
    }

    numeric_fans = _parse_exact_number(profile.get("fans_count_text") or "")
    if numeric_fans is not None:
        fields["粉丝数"] = numeric_fans

    filtered: Dict[str, Any] = {}
    for key, value in fields.items():
        if value is None or value == "":
            continue
        filtered[key] = value
    return filtered


def dedupe_profile_records(client: FeishuBitableClient) -> int:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for record in client.list_records(page_size=500):
        fields = record.get("fields") or {}
        account_id = str(fields.get("账号ID") or "").strip()
        if not account_id:
            continue
        entry = {
            "record_id": str(record.get("record_id") or "").strip(),
            "reported_at": fields.get("上报时间"),
        }
        groups.setdefault(account_id, []).append(entry)

    deleted = 0
    for entries in groups.values():
        entries.sort(key=_profile_record_sort_key, reverse=True)
        for entry in entries[1:]:
            if not entry["record_id"]:
                continue
            client.delete_record(entry["record_id"])
            deleted += 1
    return deleted


def _average(values: List[Any]) -> Optional[float]:
    numbers = [float(value) for value in values if isinstance(value, (int, float))]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 2)


def _parse_exact_number(text: str) -> Optional[int]:
    raw = str(text or "").strip().replace(",", "")
    if not raw or not raw.isdigit():
        return None
    return int(raw)


def _to_ms(iso_text: str) -> int:
    return int(datetime.fromisoformat(iso_text).timestamp() * 1000)


def _build_remark(*, profile: Dict[str, Any], works: List[Dict[str, Any]]) -> str:
    parts = [
        "公开主页抓取",
        "粉丝/关注/获赞收藏为公开页展示值",
    ]
    if not profile.get("work_count_exact", True):
        parts.append(f"作品数展示为已抓取下限 {profile.get('work_count_display_text') or profile.get('visible_work_count')}")
    if any(not item.get("note_id") for item in works):
        parts.append("首页作品卡片未返回 note_id，作品链接未补齐")
    if profile.get("fans_count_text") and not _parse_exact_number(profile.get("fans_count_text") or ""):
        parts.append(f"粉丝数仅显示为 {profile.get('fans_count_text')}")
    return "；".join(parts)


def _profile_record_sort_key(entry: Dict[str, Any]) -> tuple[int, str]:
    reported_at = entry.get("reported_at")
    if isinstance(reported_at, (int, float)):
        return int(reported_at), str(entry.get("record_id") or "")
    if isinstance(reported_at, str) and reported_at.isdigit():
        return int(reported_at), str(entry.get("record_id") or "")
    return 0, str(entry.get("record_id") or "")


if __name__ == "__main__":
    raise SystemExit(main())
