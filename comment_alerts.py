from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from .config import Settings
from .feishu import FeishuBitableClient, fields_match
from .profile_dashboard_to_feishu import ensure_named_table
from .profile_works_to_feishu import build_work_fingerprint


COMMENT_ALERT_TABLE_NAME = "小红书评论预警"
COMMENT_ALERT_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "预警键", "type": 1},
    {"field_name": "预警日期", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "作品指纹", "type": 1},
    {"field_name": "标题文案", "type": 1},
    {"field_name": "当前评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "基准评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论增长率", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "抓取时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "通知状态", "type": 1},
    {"field_name": "备注", "type": 1},
]


def build_work_comment_fields(
    *,
    report: Dict[str, Any],
    work: Dict[str, Any],
    previous_fields: Optional[Dict[str, Any]],
    settings: Settings,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    current_comment_count = to_optional_int(work.get("comment_count"))
    current_comment_text = str(work.get("comment_count_text") or "").strip()
    extra_fields: Dict[str, Any] = {}
    if current_comment_count is not None:
        extra_fields["评论数"] = current_comment_count
    if current_comment_text:
        extra_fields["评论文本"] = current_comment_text

    previous_comment_count = to_optional_int((previous_fields or {}).get("评论数"))
    if current_comment_count is None or previous_comment_count is None:
        return extra_fields, None

    comment_delta = current_comment_count - previous_comment_count
    extra_fields["评论增量"] = comment_delta
    if previous_comment_count > 0:
        growth_rate = round((comment_delta / previous_comment_count) * 100, 2)
        extra_fields["评论增长率"] = growth_rate
    else:
        growth_rate = None

    if not should_trigger_comment_alert(
        current_comment_count=current_comment_count,
        previous_comment_count=previous_comment_count,
        growth_rate=growth_rate,
        settings=settings,
    ):
        return extra_fields, None

    threshold_text = format_threshold(settings.comment_alert_growth_threshold_percent)
    extra_fields["评论预警"] = f"评论日增>{threshold_text}%"
    return extra_fields, build_comment_alert_record(
        report=report,
        work=work,
        current_comment_count=current_comment_count,
        previous_comment_count=previous_comment_count,
        comment_delta=comment_delta,
        growth_rate=growth_rate or 0.0,
    )


def should_trigger_comment_alert(
    *,
    current_comment_count: int,
    previous_comment_count: int,
    growth_rate: Optional[float],
    settings: Settings,
) -> bool:
    if growth_rate is None:
        return False
    if current_comment_count <= previous_comment_count:
        return False
    if previous_comment_count < int(settings.comment_alert_min_previous_count or 0):
        return False
    return growth_rate > float(settings.comment_alert_growth_threshold_percent or 0)


def build_comment_alert_record(
    *,
    report: Dict[str, Any],
    work: Dict[str, Any],
    current_comment_count: int,
    previous_comment_count: int,
    comment_delta: int,
    growth_rate: float,
) -> Dict[str, Any]:
    profile = report.get("profile") or {}
    fingerprint = build_work_fingerprint(
        profile_user_id=profile.get("profile_user_id") or "",
        title=work.get("title_copy") or "",
        cover_url=work.get("cover_url") or "",
    )
    captured_at = str(report.get("captured_at") or "")
    alert_date = captured_at.split("T", 1)[0] if "T" in captured_at else captured_at[:10]
    fields: Dict[str, Any] = {
        "预警键": f"{alert_date}|{fingerprint}",
        "预警日期": alert_date,
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "作品指纹": fingerprint,
        "标题文案": work.get("title_copy") or "",
        "当前评论数": current_comment_count,
        "基准评论数": previous_comment_count,
        "评论增量": comment_delta,
        "评论增长率": growth_rate,
        "抓取时间": to_ms(captured_at),
        "备注": f"基准 {previous_comment_count} -> 当前 {current_comment_count}",
    }
    if profile.get("profile_url"):
        fields["主页链接"] = {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile["profile_url"],
        }
    if work.get("note_url"):
        fields["作品链接"] = {
            "text": work.get("title_copy") or "作品链接",
            "link": work["note_url"],
        }
    return fields


def sync_comment_alerts(
    *,
    settings: Settings,
    alerts: List[Dict[str, Any]],
    table_name: str = COMMENT_ALERT_TABLE_NAME,
) -> Dict[str, Any]:
    if not alerts:
        return {
            "alert_table_id": "",
            "alerts_created": 0,
            "alerts_updated": 0,
            "alerts_sent": 0,
            "alerts_pending": 0,
        }

    tables_client = FeishuBitableClient(settings)
    table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=table_name,
        default_view_name="评论预警",
        fields=COMMENT_ALERT_FIELDS,
    )
    client = FeishuBitableClient(replace(settings, feishu_table_id=table_id))
    client.ensure_fields(COMMENT_ALERT_FIELDS)

    existing: Dict[str, Dict[str, Any]] = {}
    for record in client.list_records(page_size=500):
        fields = record.get("fields") or {}
        alert_key = str(fields.get("预警键") or "").strip()
        if not alert_key:
            continue
        existing[alert_key] = {
            "record_id": str(record.get("record_id") or "").strip(),
            "status": str(fields.get("通知状态") or "").strip(),
            "fields": dict(fields),
        }

    created = 0
    updated = 0
    pending_notifications: List[Dict[str, Any]] = []
    synced_records: List[Tuple[str, Dict[str, Any]]] = []
    for alert in alerts:
        alert_key = str(alert.get("预警键") or "").strip()
        previous_status = (existing.get(alert_key) or {}).get("status", "")
        fields = dict(alert)
        if settings.feishu_notify_webhook:
            if previous_status == "已发送":
                fields["通知状态"] = "已发送"
            else:
                fields["通知状态"] = "待发送"
                pending_notifications.append(fields)
        else:
            fields["通知状态"] = "未配置Webhook"

        record_id = (existing.get(alert_key) or {}).get("record_id", "")
        if record_id:
            existing_fields = (existing.get(alert_key) or {}).get("fields") or {}
            if fields_match(existing_fields, fields, ignore_fields=["抓取时间"]):
                synced_records.append((record_id, fields))
            else:
                client.update_record(record_id, fields)
                updated += 1
                synced_records.append((record_id, fields))
        else:
            record_id = client.create_record(fields)
            created += 1
            synced_records.append((record_id, fields))

    sent = 0
    if pending_notifications and settings.feishu_notify_webhook:
        try:
            send_comment_alert_notification(settings=settings, alerts=pending_notifications)
        except Exception:
            for record_id, fields in synced_records:
                if fields.get("通知状态") != "待发送":
                    continue
                client.update_record(record_id, {"通知状态": "发送失败"})
        else:
            sent = len(pending_notifications)
            for record_id, fields in synced_records:
                if fields.get("通知状态") != "待发送":
                    continue
                client.update_record(record_id, {"通知状态": "已发送"})

    pending = sum(1 for _, fields in synced_records if fields.get("通知状态") == "待发送")
    return {
        "alert_table_id": table_id,
        "alerts_created": created,
        "alerts_updated": updated,
        "alerts_sent": sent,
        "alerts_pending": pending,
    }


def send_comment_alert_notification(*, settings: Settings, alerts: List[Dict[str, Any]]) -> None:
    if not settings.feishu_notify_webhook or not alerts:
        return

    threshold_text = format_threshold(settings.comment_alert_growth_threshold_percent)
    lines = [
        f"小红书评论增长预警",
        f"本次共有 {len(alerts)} 条作品评论增长超过 {threshold_text}%",
    ]
    for index, alert in enumerate(alerts[:10], start=1):
        lines.append(
            (
                f"{index}. {alert.get('账号') or '未知账号'} | {alert.get('标题文案') or '无标题'} | "
                f"{alert.get('基准评论数', 0)} -> {alert.get('当前评论数', 0)} | "
                f"+{alert.get('评论增量', 0)} | +{alert.get('评论增长率', 0)}%"
            )
        )
        link = extract_hyperlink(alert.get("作品链接"))
        if link:
            lines.append(link)
    if len(alerts) > 10:
        lines.append(f"其余 {len(alerts) - 10} 条请查看飞书多维表格《{COMMENT_ALERT_TABLE_NAME}》")

    payload: Dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": "\n".join(lines)},
    }
    if settings.feishu_notify_secret:
        timestamp = str(int(time.time()))
        sign = build_feishu_webhook_sign(timestamp=timestamp, secret=settings.feishu_notify_secret)
        payload["timestamp"] = timestamp
        payload["sign"] = sign

    response = requests.post(
        settings.feishu_notify_webhook,
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") not in (0, None):
        raise ValueError(f"飞书通知失败: {data.get('msg')}")


def build_feishu_webhook_sign(*, timestamp: str, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    digest = hmac.new(string_to_sign, digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def to_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def to_ms(iso_text: str) -> int:
    return int(datetime.fromisoformat(iso_text).timestamp() * 1000)


def extract_hyperlink(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("link") or "").strip()
    return str(value or "").strip()


def format_threshold(value: float) -> str:
    text = f"{float(value):.2f}"
    return text.rstrip("0").rstrip(".")
