from __future__ import annotations

import argparse
import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from .config import Settings, load_settings
from .feishu import FeishuBitableClient, fields_match
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload
from .profile_works_to_feishu import (
    WORKS_CALENDAR_FIELDS,
    WORKS_CALENDAR_TABLE_NAME,
    build_work_calendar_history_index,
    build_work_fingerprint,
)


DASHBOARD_OVERVIEW_TABLE_NAME = "小红书看板总览"
DASHBOARD_TREND_TABLE_NAME = "小红书看板趋势"
DASHBOARD_RANKING_TABLE_NAME = "小红书看板榜单"
DASHBOARD_SINGLE_WORK_RANKING_TABLE_NAME = "小红书单条作品排行"
DASHBOARD_PORTAL_TABLE_NAME = "小红书仪表盘总控"
DASHBOARD_CALENDAR_TABLE_NAME = "小红书日历留底"

DASHBOARD_OVERVIEW_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "看板键", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "账号简介", "type": 1},
    {"field_name": "IP属地", "type": 1},
    {"field_name": "关注数文本", "type": 1},
    {"field_name": "关注数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "粉丝数文本", "type": 1},
    {"field_name": "粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏文本", "type": 1},
    {"field_name": "获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页可见作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "账号总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "作品数展示", "type": 1},
    {"field_name": "首页视频数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页图文数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "视频占比", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "首页总点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页平均点赞", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "头部作品标题", "type": 1},
    {"field_name": "头部作品点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "TOP3作品摘要", "type": 1},
    {"field_name": "内容类型分布", "type": 1},
    {"field_name": "备注", "type": 1},
]

DASHBOARD_TREND_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "快照ID", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "首页可见作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页平均点赞", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "首页视频数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页图文数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "视频占比", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "头部作品标题", "type": 1},
    {"field_name": "头部作品点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "粉丝数文本", "type": 1},
    {"field_name": "粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏文本", "type": 1},
    {"field_name": "获赞收藏数", "type": 2, "property": {"formatter": "0"}},
]

DASHBOARD_RANKING_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "榜单键", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "排名", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "卡片标签", "type": 1},
    {"field_name": "作品指纹", "type": 1},
    {"field_name": "标题文案", "type": 1},
    {"field_name": "作品类型", "type": 1},
    {"field_name": "点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞文本", "type": 1},
    {"field_name": "封面图", "type": 15},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "xsec_token", "type": 1},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "作品摘要", "type": 1},
]

DASHBOARD_SINGLE_WORK_RANKING_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "榜单键", "type": 1},
    {"field_name": "项目", "type": 1},
    {"field_name": "榜单类型", "type": 1},
    {"field_name": "排名", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "卡片标签", "type": 1},
    {"field_name": "排序值", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "作品指纹", "type": 1},
    {"field_name": "标题文案", "type": 1},
    {"field_name": "作品类型", "type": 1},
    {"field_name": "点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论数口径", "type": 1},
    {"field_name": "对比日期文本", "type": 1},
    {"field_name": "昨日点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "昨日评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "点赞次日增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "评论次日增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "互动次日增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "互动次日增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "封面图", "type": 15},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "作品链接", "type": 15},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "榜单摘要", "type": 1},
]

DASHBOARD_PORTAL_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "账号组键", "type": 1},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "监控账号数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周日期文本", "type": 1},
    {"field_name": "上周总粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总粉丝周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总粉丝周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "总获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周总获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总获赞收藏周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总获赞收藏周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总作品周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总作品周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "总点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周总点赞数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总点赞周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总点赞周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "平均点赞数", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "已采评论作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周总评论数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总评论周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "总评论周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "平均评论数", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "视频作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "图文作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "视频占比", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "头部作品标题", "type": 1},
    {"field_name": "头部作品账号", "type": 1},
    {"field_name": "头部作品点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "头部作品链接", "type": 15},
    {"field_name": "TOP3作品摘要", "type": 1},
    {"field_name": "账号分布摘要", "type": 1},
    {"field_name": "周对比摘要", "type": 1},
    {"field_name": "备注", "type": 1},
]

DASHBOARD_CALENDAR_FIELDS: List[Dict[str, Any]] = [
    {"field_name": "日历键", "type": 1},
    {"field_name": "日历日期", "type": 5, "property": {"date_formatter": "yyyy-MM-dd"}},
    {"field_name": "日期文本", "type": 1},
    {"field_name": "日历标题", "type": 1},
    {"field_name": "账号ID", "type": 1},
    {"field_name": "账号", "type": 1},
    {"field_name": "主页链接", "type": 15},
    {"field_name": "数据更新时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "粉丝数文本", "type": 1},
    {"field_name": "粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周日期文本", "type": 1},
    {"field_name": "上周粉丝数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "粉丝周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "粉丝周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "获赞收藏文本", "type": 1},
    {"field_name": "获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "上周获赞收藏数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "获赞收藏周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "首页可见作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "账号总作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "作品数展示", "type": 1},
    {"field_name": "首页总点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页平均点赞", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "上周首页总点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总点赞周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总点赞周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "已采评论作品数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总评论", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页平均评论", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "上周首页总评论", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总评论周增量", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "首页总评论周增幅", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "头部作品标题", "type": 1},
    {"field_name": "头部作品点赞", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "头部作品链接", "type": 15},
    {"field_name": "TOP3作品摘要", "type": 1},
    {"field_name": "周对比摘要", "type": 1},
    {"field_name": "备注", "type": 1},
]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把小红书账号主页数据同步成飞书看板数据表。")
    parser.add_argument("--url", required=True, help="小红书账号主页链接")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--overview-table-name", default=DASHBOARD_OVERVIEW_TABLE_NAME)
    parser.add_argument("--trend-table-name", default=DASHBOARD_TREND_TABLE_NAME)
    parser.add_argument("--ranking-table-name", default=DASHBOARD_RANKING_TABLE_NAME)
    parser.add_argument("--calendar-table-name", default=DASHBOARD_CALENDAR_TABLE_NAME)
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    settings.validate_for_sync()
    payload = load_profile_report_payload(settings=settings, profile_url=args.url)
    report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
    report = enrich_profile_report_with_note_metrics(report=report, settings=settings)

    results = sync_dashboard_tables(
        report=report,
        settings=settings,
        overview_table_name=args.overview_table_name,
        trend_table_name=args.trend_table_name,
        ranking_table_name=args.ranking_table_name,
        calendar_table_name=args.calendar_table_name,
    )
    print(
        "[OK] overview "
        f"{results['overview_action']} record_id={results['overview_record_id']} table_id={results['overview_table_id']}"
    )
    print(
        "[OK] trend "
        f"{results['trend_action']} record_id={results['trend_record_id']} table_id={results['trend_table_id']}"
    )
    print(
        "[OK] ranking "
        f"created={results['ranking_created']} updated={results['ranking_updated']} deleted={results['ranking_deleted']} "
        f"table_id={results['ranking_table_id']}"
    )
    print(
        "[OK] calendar "
        f"{results['calendar_action']} record_id={results['calendar_record_id']} table_id={results['calendar_table_id']}"
    )
    return 0


def sync_dashboard_tables(
    *,
    report: Dict[str, Any],
    settings: Settings,
    overview_table_name: str = DASHBOARD_OVERVIEW_TABLE_NAME,
    trend_table_name: str = DASHBOARD_TREND_TABLE_NAME,
    ranking_table_name: str = DASHBOARD_RANKING_TABLE_NAME,
    calendar_table_name: str = DASHBOARD_CALENDAR_TABLE_NAME,
) -> Dict[str, Any]:
    tables_client = FeishuBitableClient(settings)

    overview_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=overview_table_name,
        default_view_name="总览",
        fields=DASHBOARD_OVERVIEW_FIELDS,
    )
    trend_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=trend_table_name,
        default_view_name="趋势",
        fields=DASHBOARD_TREND_FIELDS,
    )
    ranking_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=ranking_table_name,
        default_view_name="榜单",
        fields=DASHBOARD_RANKING_FIELDS,
    )
    calendar_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=calendar_table_name,
        default_view_name="日历留底",
        fields=DASHBOARD_CALENDAR_FIELDS,
    )

    overview_client = FeishuBitableClient(replace(settings, feishu_table_id=overview_table_id))
    trend_client = FeishuBitableClient(replace(settings, feishu_table_id=trend_table_id))
    ranking_client = FeishuBitableClient(replace(settings, feishu_table_id=ranking_table_id))
    calendar_client = FeishuBitableClient(replace(settings, feishu_table_id=calendar_table_id))

    overview_client.ensure_fields(DASHBOARD_OVERVIEW_FIELDS)
    trend_client.ensure_fields(DASHBOARD_TREND_FIELDS)
    ranking_client.ensure_fields(DASHBOARD_RANKING_FIELDS)
    calendar_client.ensure_fields(DASHBOARD_CALENDAR_FIELDS)

    overview_fields = build_dashboard_overview_fields(report)
    overview_state = build_record_state_index(overview_client, unique_field="看板键")
    overview_action, overview_record_id = upsert_record_if_changed(
        client=overview_client,
        state_index=overview_state,
        unique_field="看板键",
        unique_value=overview_fields["看板键"],
        fields=overview_fields,
        compare_ignore_fields=["数据更新时间"],
    )

    trend_fields = build_dashboard_trend_fields(report)
    trend_records = trend_client.list_records(page_size=500)
    trend_action, trend_record_id = sync_dashboard_trend(
        trend_client=trend_client,
        trend_records=trend_records,
        trend_fields=trend_fields,
    )

    calendar_action, calendar_record_id = sync_dashboard_calendar(
        calendar_client=calendar_client,
        report=report,
    )

    ranking_result = sync_dashboard_ranking(
        ranking_client=ranking_client,
        report=report,
    )
    return {
        "overview_action": overview_action,
        "overview_record_id": overview_record_id,
        "overview_table_id": overview_table_id,
        "trend_action": trend_action,
        "trend_record_id": trend_record_id,
        "trend_table_id": trend_table_id,
        "calendar_action": calendar_action,
        "calendar_record_id": calendar_record_id,
        "calendar_table_id": calendar_table_id,
        "ranking_table_id": ranking_table_id,
        **ranking_result,
    }


def sync_dashboard_portal(
    *,
    reports: List[Dict[str, Any]],
    settings: Settings,
    portal_table_name: str = DASHBOARD_PORTAL_TABLE_NAME,
) -> Dict[str, Any]:
    tables_client = FeishuBitableClient(settings)
    portal_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=portal_table_name,
        default_view_name="总控",
        fields=DASHBOARD_PORTAL_FIELDS,
    )
    portal_client = FeishuBitableClient(replace(settings, feishu_table_id=portal_table_id))
    portal_client.ensure_fields(DASHBOARD_PORTAL_FIELDS)
    calendar_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=DASHBOARD_CALENDAR_TABLE_NAME,
        default_view_name="日历留底",
        fields=DASHBOARD_CALENDAR_FIELDS,
    )
    calendar_client = FeishuBitableClient(replace(settings, feishu_table_id=calendar_table_id))
    portal_fields = build_dashboard_portal_fields(
        reports,
        weekly_baseline=select_portal_weekly_baseline(
            records=calendar_client.list_records(
                page_size=500,
                field_names=[
                    "账号ID",
                    "日期文本",
                    "日历日期",
                    "粉丝数",
                    "获赞收藏数",
                    "首页可见作品数",
                    "首页总点赞",
                    "首页总评论",
                ],
            ),
            reports=reports,
        ),
    )
    portal_state = build_record_state_index(portal_client, unique_field="账号组键")
    action, record_id = upsert_record_if_changed(
        client=portal_client,
        state_index=portal_state,
        unique_field="账号组键",
        unique_value=portal_fields["账号组键"],
        fields=portal_fields,
        compare_ignore_fields=["数据更新时间"],
    )
    return {
        "portal_action": action,
        "portal_record_id": record_id,
        "portal_table_id": portal_table_id,
    }


def sync_single_work_ranking_table(
    *,
    reports: List[Dict[str, Any]],
    settings: Settings,
    table_name: str = DASHBOARD_SINGLE_WORK_RANKING_TABLE_NAME,
    history_index: Optional[Dict[str, List[Any]]] = None,
    included_rank_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    tables_client = FeishuBitableClient(settings)
    ranking_table_id = ensure_named_table(
        tables_client=tables_client,
        table_name=table_name,
        default_view_name="单条排行",
        fields=DASHBOARD_SINGLE_WORK_RANKING_FIELDS,
    )
    ranking_client = FeishuBitableClient(replace(settings, feishu_table_id=ranking_table_id))
    ranking_client.ensure_fields(DASHBOARD_SINGLE_WORK_RANKING_FIELDS)
    works_calendar_table_id = ""
    ranking_history_index = history_index or {}
    if history_index is None:
        works_calendar_table_id = ensure_named_table(
            tables_client=tables_client,
            table_name=WORKS_CALENDAR_TABLE_NAME,
            default_view_name="作品日历留底",
            fields=WORKS_CALENDAR_FIELDS,
        )
        works_calendar_client = FeishuBitableClient(replace(settings, feishu_table_id=works_calendar_table_id))
        ranking_history_index = build_work_calendar_history_index(
            works_calendar_client.list_records(
                page_size=500,
                field_names=["日期文本", "日历日期", "作品指纹", "点赞数", "评论数"],
            )
        )

    allowed_rank_types = {str(item or "").strip() for item in (included_rank_types or []) if str(item or "").strip()}
    desired_fields: Dict[str, Dict[str, Any]] = {}
    for rank_type, ranked_items in build_single_work_rankings(reports=reports, history_index=ranking_history_index).items():
        if allowed_rank_types and rank_type not in allowed_rank_types:
            continue
        for rank, item in enumerate(ranked_items, start=1):
            fields = build_single_work_ranking_fields(item=item, rank_type=rank_type, rank=rank)
            desired_fields[str(fields["榜单键"])] = fields

    existing_rows = build_record_state_index(ranking_client, unique_field="榜单键")

    created = 0
    updated = 0
    for row_key, fields in desired_fields.items():
        existing = existing_rows.pop(row_key, None)
        record_id = str((existing or {}).get("record_id") or "").strip()
        if record_id:
            if fields_match((existing or {}).get("fields") or {}, fields, ignore_fields=["数据更新时间"]):
                continue
            ranking_client.update_record(record_id, fields)
            updated += 1
            continue
        ranking_client.create_record(fields)
        created += 1

    deleted = 0
    for record_id in existing_rows.values():
        if not record_id:
            continue
        ranking_client.delete_record(record_id)
        deleted += 1

    return {
        "single_work_ranking_table_id": ranking_table_id,
        "single_work_ranking_created": created,
        "single_work_ranking_updated": updated,
        "single_work_ranking_deleted": deleted,
    }


def ensure_named_table(
    *,
    tables_client: FeishuBitableClient,
    table_name: str,
    default_view_name: str,
    fields: List[Dict[str, Any]],
) -> str:
    tables = tables_client.list_tables()
    for table in tables:
        if str(table.get("name") or "").strip() == table_name:
            return str(table.get("table_id") or "")

    created = tables_client.create_table(
        table_name=table_name,
        default_view_name=default_view_name,
        fields=fields,
    )
    table_id = str(created.get("table_id") or "")
    if not table_id:
        raise ValueError(f"创建看板数据表失败: {table_name}")
    return table_id


def build_record_state_index(client: FeishuBitableClient, *, unique_field: str) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for record in client.list_records(page_size=500):
        fields = record.get("fields") or {}
        unique_value = str(fields.get(unique_field) or "").strip()
        record_id = str(record.get("record_id") or "").strip()
        if unique_value and record_id and unique_value not in index:
            index[unique_value] = {"record_id": record_id, "fields": dict(fields)}
    return index


def upsert_record_if_changed(
    *,
    client: FeishuBitableClient,
    state_index: Dict[str, Dict[str, Any]],
    unique_field: str,
    unique_value: Any,
    fields: Dict[str, Any],
    compare_ignore_fields: Optional[List[str]] = None,
) -> tuple[str, str]:
    row_key = str(unique_value or "").strip()
    if not row_key:
        raise ValueError(f"缺少唯一字段值: {unique_field}")
    existing = state_index.get(row_key)
    if existing:
        record_id = str(existing.get("record_id") or "").strip()
        if record_id and fields_match((existing.get("fields") or {}), fields, ignore_fields=compare_ignore_fields):
            return "skipped", record_id
        client.update_record(record_id, fields)
        state_index[row_key] = {"record_id": record_id, "fields": dict(fields)}
        return "updated", record_id
    record_id = client.create_record(fields)
    state_index[row_key] = {"record_id": record_id, "fields": dict(fields)}
    return "created", record_id


def sync_dashboard_trend(
    *,
    trend_client: FeishuBitableClient,
    trend_records: List[Dict[str, Any]],
    trend_fields: Dict[str, Any],
) -> tuple[str, str]:
    snapshot_key = str(trend_fields.get("快照ID") or "").strip()
    account_id = str(trend_fields.get("账号ID") or "").strip()
    snapshot_record_id = ""
    latest_record_id = ""
    latest_fields: Dict[str, Any] = {}
    latest_updated_at = -1
    for record in trend_records:
        fields = record.get("fields") or {}
        record_id = str(record.get("record_id") or "").strip()
        if str(fields.get("快照ID") or "").strip() == snapshot_key:
            snapshot_record_id = record_id
        if str(fields.get("账号ID") or "").strip() != account_id:
            continue
        updated_at = to_optional_int(fields.get("数据更新时间")) or 0
        if updated_at >= latest_updated_at:
            latest_updated_at = updated_at
            latest_record_id = record_id
            latest_fields = dict(fields)
    if latest_record_id and fields_match(latest_fields, trend_fields, ignore_fields=["快照ID", "数据更新时间"]):
        return "skipped", latest_record_id
    if snapshot_record_id:
        trend_client.update_record(snapshot_record_id, trend_fields)
        return "updated", snapshot_record_id
    record_id = trend_client.create_record(trend_fields)
    return "created", record_id


def build_dashboard_overview_fields(report: Dict[str, Any]) -> Dict[str, Any]:
    profile = report["profile"]
    ranked = rank_profile_works(report["works"])
    metrics = compute_dashboard_metrics(report)
    top_work = ranked[0] if ranked else {}
    work_count_display = profile.get("work_count_display_text") or str(metrics["visible_work_count"])

    fields: Dict[str, Any] = {
        "看板键": profile.get("profile_user_id") or "",
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "数据更新时间": to_ms(report.get("captured_at") or ""),
        "主页链接": {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile.get("profile_url") or "",
        },
        "账号简介": profile.get("desc") or "",
        "IP属地": profile.get("ip_location") or "",
        "关注数文本": profile.get("follows_count_text") or "",
        "关注数": parse_exact_number(profile.get("follows_count_text")),
        "粉丝数文本": profile.get("fans_count_text") or "",
        "粉丝数": parse_exact_number(profile.get("fans_count_text")),
        "获赞收藏文本": profile.get("interaction_count_text") or "",
        "获赞收藏数": parse_exact_number(profile.get("interaction_count_text")),
        "首页可见作品数": metrics["visible_work_count"],
        "账号总作品数": profile.get("total_work_count"),
        "作品数展示": work_count_display,
        "首页视频数": metrics["video_count"],
        "首页图文数": metrics["image_count"],
        "视频占比": metrics["video_ratio"],
        "首页总点赞": metrics["total_likes"],
        "首页平均点赞": metrics["average_likes"],
        "头部作品标题": top_work.get("title_copy") or "",
        "头部作品点赞": metrics["top_like_count"],
        "TOP3作品摘要": build_top3_summary(ranked[:3]),
        "内容类型分布": f"视频 {metrics['video_count']} | 图文 {metrics['image_count']}",
        "备注": build_dashboard_remark(report),
    }
    return {key: value for key, value in fields.items() if value not in ("", None)}


def build_dashboard_trend_fields(report: Dict[str, Any]) -> Dict[str, Any]:
    profile = report["profile"]
    ranked = rank_profile_works(report["works"])
    metrics = compute_dashboard_metrics(report)
    captured_at = report.get("captured_at") or ""
    snapshot_id = f"{profile.get('profile_user_id') or 'profile'}|{captured_at}"
    top_work = ranked[0] if ranked else {}
    fields: Dict[str, Any] = {
        "快照ID": snapshot_id,
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "数据更新时间": to_ms(captured_at),
        "首页可见作品数": metrics["visible_work_count"],
        "首页总点赞": metrics["total_likes"],
        "首页平均点赞": metrics["average_likes"],
        "首页视频数": metrics["video_count"],
        "首页图文数": metrics["image_count"],
        "视频占比": metrics["video_ratio"],
        "头部作品标题": top_work.get("title_copy") or "",
        "头部作品点赞": metrics["top_like_count"],
        "粉丝数文本": profile.get("fans_count_text") or "",
        "粉丝数": parse_exact_number(profile.get("fans_count_text")),
        "获赞收藏文本": profile.get("interaction_count_text") or "",
        "获赞收藏数": parse_exact_number(profile.get("interaction_count_text")),
    }
    return {key: value for key, value in fields.items() if value not in ("", None)}


def build_dashboard_portal_fields(
    reports: List[Dict[str, Any]],
    *,
    weekly_baseline: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metrics = compute_dashboard_portal_metrics(reports)
    top_work = metrics["top_work"]
    fields: Dict[str, Any] = {
        "账号组键": metrics["account_group_key"],
        "数据更新时间": to_ms(metrics["captured_at"]),
        "监控账号数": metrics["account_count"],
        "总粉丝数": metrics["total_fans"],
        "总获赞收藏数": metrics["total_interaction"],
        "总作品数": metrics["total_works"],
        "总点赞数": metrics["total_likes"],
        "平均点赞数": metrics["average_likes"],
        "已采评论作品数": metrics["tracked_comment_work_count"],
        "总评论数": metrics["total_comments"],
        "平均评论数": metrics["average_comments"],
        "视频作品数": metrics["video_count"],
        "图文作品数": metrics["image_count"],
        "视频占比": metrics["video_ratio"],
        "头部作品标题": top_work.get("title_copy") or "",
        "头部作品账号": top_work.get("nickname") or "",
        "头部作品点赞": top_work.get("like_count") or 0,
        "TOP3作品摘要": build_global_top3_summary(metrics["top3_works"]),
        "账号分布摘要": build_account_distribution_summary(reports),
        "备注": build_dashboard_portal_remark(reports),
    }
    fields.update(
        build_portal_weekly_comparison_fields(
            current_metrics=metrics,
            weekly_baseline=weekly_baseline,
        )
    )
    if top_work.get("note_url"):
        fields["头部作品链接"] = {
            "text": top_work.get("title_copy") or "头部作品",
            "link": top_work["note_url"],
        }
    return {key: value for key, value in fields.items() if value not in ("", None)}


def sync_dashboard_calendar(
    *,
    calendar_client: FeishuBitableClient,
    report: Dict[str, Any],
) -> tuple[str, str]:
    existing_records = calendar_client.list_records(page_size=500)
    baseline_fields = select_weekly_baseline(
        records=existing_records,
        account_id=str((report.get("profile") or {}).get("profile_user_id") or "").strip(),
        snapshot_date=extract_snapshot_date(str(report.get("captured_at") or "")),
    )
    calendar_fields = build_dashboard_calendar_fields(report, baseline_fields=baseline_fields)
    existing_state = build_record_state_index(calendar_client, unique_field="日历键")
    return upsert_record_if_changed(
        client=calendar_client,
        state_index=existing_state,
        unique_field="日历键",
        unique_value=calendar_fields["日历键"],
        fields=calendar_fields,
        compare_ignore_fields=["数据更新时间"],
    )


def build_dashboard_calendar_fields(
    report: Dict[str, Any],
    *,
    baseline_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    profile = report["profile"]
    ranked = rank_profile_works(report["works"])
    metrics = compute_dashboard_metrics(report)
    captured_at = str(report.get("captured_at") or "")
    snapshot_date = extract_snapshot_date(captured_at)
    top_work = ranked[0] if ranked else {}
    work_count_display = profile.get("work_count_display_text") or str(metrics["visible_work_count"])
    title_parts = [
        profile.get("nickname") or profile.get("profile_user_id") or "未知账号",
        f"作品 {work_count_display}",
        f"总赞 {metrics['total_likes']}",
    ]
    if metrics["tracked_comment_work_count"]:
        title_parts.append(f"总评 {metrics['total_comments']}")

    fields: Dict[str, Any] = {
        "日历键": f"{snapshot_date}|{profile.get('profile_user_id') or ''}",
        "日历日期": to_ms(captured_at),
        "日期文本": snapshot_date,
        "日历标题": " | ".join(title_parts),
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "数据更新时间": to_ms(captured_at),
        "粉丝数文本": profile.get("fans_count_text") or "",
        "粉丝数": parse_exact_number(profile.get("fans_count_text")),
        "获赞收藏文本": profile.get("interaction_count_text") or "",
        "获赞收藏数": parse_exact_number(profile.get("interaction_count_text")),
        "首页可见作品数": metrics["visible_work_count"],
        "账号总作品数": profile.get("total_work_count"),
        "作品数展示": work_count_display,
        "首页总点赞": metrics["total_likes"],
        "首页平均点赞": metrics["average_likes"],
        "已采评论作品数": metrics["tracked_comment_work_count"],
        "首页总评论": metrics["total_comments"],
        "首页平均评论": metrics["average_comments"],
        "头部作品标题": top_work.get("title_copy") or "",
        "头部作品点赞": metrics["top_like_count"],
        "TOP3作品摘要": build_top3_summary(ranked[:3]),
        "备注": "每日留底，建议在飞书切换为日历视图查看",
    }
    fields.update(
        build_weekly_comparison_fields(
            current_fields=fields,
            baseline_fields=baseline_fields,
        )
    )
    if profile.get("profile_url"):
        fields["主页链接"] = {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile["profile_url"],
        }
    if top_work.get("note_url"):
        fields["头部作品链接"] = {
            "text": top_work.get("title_copy") or "头部作品",
            "link": top_work["note_url"],
        }
    return {key: value for key, value in fields.items() if value not in ("", None)}


def build_weekly_comparison_fields(
    *,
    current_fields: Dict[str, Any],
    baseline_fields: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not baseline_fields:
        return {
            "周对比摘要": "暂无 7 天前留底，周对比将在积累满 7 天后显示",
        }

    result: Dict[str, Any] = {}
    baseline_date = str(baseline_fields.get("日期文本") or "").strip()
    if baseline_date:
        result["上周日期文本"] = baseline_date

    summary_items: List[str] = []
    append_weekly_change(
        result=result,
        label="粉丝",
        current_value=to_optional_int(current_fields.get("粉丝数")),
        previous_value=to_optional_int(baseline_fields.get("粉丝数")),
        previous_field="上周粉丝数",
        delta_field="粉丝周增量",
        rate_field="粉丝周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="获赞收藏",
        current_value=to_optional_int(current_fields.get("获赞收藏数")),
        previous_value=to_optional_int(baseline_fields.get("获赞收藏数")),
        previous_field="上周获赞收藏数",
        delta_field="获赞收藏周增量",
        rate_field="获赞收藏周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总赞",
        current_value=to_optional_int(current_fields.get("首页总点赞")),
        previous_value=to_optional_int(baseline_fields.get("首页总点赞")),
        previous_field="上周首页总点赞",
        delta_field="首页总点赞周增量",
        rate_field="首页总点赞周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总评",
        current_value=to_optional_int(current_fields.get("首页总评论")),
        previous_value=to_optional_int(baseline_fields.get("首页总评论")),
        previous_field="上周首页总评论",
        delta_field="首页总评论周增量",
        rate_field="首页总评论周增幅",
        summary_items=summary_items,
    )
    result["周对比摘要"] = (
        f"对比 {baseline_date} | {' | '.join(summary_items)}"
        if baseline_date and summary_items
        else f"对比 {baseline_date}" if baseline_date else "暂无可用周对比"
    )
    return result


def build_portal_weekly_comparison_fields(
    *,
    current_metrics: Dict[str, Any],
    weekly_baseline: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not weekly_baseline:
        return {
            "周对比摘要": "暂无 7 天前留底，整组账号周对比将在积累满 7 天后显示",
        }

    result: Dict[str, Any] = {}
    baseline_date = str(weekly_baseline.get("baseline_date_text") or "").strip()
    if baseline_date:
        result["上周日期文本"] = baseline_date

    summary_items: List[str] = []
    append_weekly_change(
        result=result,
        label="总粉丝",
        current_value=to_optional_int(current_metrics.get("total_fans")),
        previous_value=to_optional_int(weekly_baseline.get("total_fans")),
        previous_field="上周总粉丝数",
        delta_field="总粉丝周增量",
        rate_field="总粉丝周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总获赞收藏",
        current_value=to_optional_int(current_metrics.get("total_interaction")),
        previous_value=to_optional_int(weekly_baseline.get("total_interaction")),
        previous_field="上周总获赞收藏数",
        delta_field="总获赞收藏周增量",
        rate_field="总获赞收藏周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总作品",
        current_value=to_optional_int(current_metrics.get("total_works")),
        previous_value=to_optional_int(weekly_baseline.get("total_works")),
        previous_field="上周总作品数",
        delta_field="总作品周增量",
        rate_field="总作品周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总点赞",
        current_value=to_optional_int(current_metrics.get("total_likes")),
        previous_value=to_optional_int(weekly_baseline.get("total_likes")),
        previous_field="上周总点赞数",
        delta_field="总点赞周增量",
        rate_field="总点赞周增幅",
        summary_items=summary_items,
    )
    append_weekly_change(
        result=result,
        label="总评论",
        current_value=to_optional_int(current_metrics.get("total_comments")),
        previous_value=to_optional_int(weekly_baseline.get("total_comments")),
        previous_field="上周总评论数",
        delta_field="总评论周增量",
        rate_field="总评论周增幅",
        summary_items=summary_items,
    )
    covered_accounts = int(weekly_baseline.get("covered_accounts") or 0)
    expected_accounts = int(weekly_baseline.get("expected_accounts") or 0)
    coverage_text = f"基线覆盖 {covered_accounts}/{expected_accounts} 账号"
    if summary_items:
        result["周对比摘要"] = f"对比 {baseline_date} | {coverage_text} | {' | '.join(summary_items)}"
    else:
        result["周对比摘要"] = f"对比 {baseline_date} | {coverage_text}"
    return result


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
        return
    summary_items.append(f"{label} {delta:+d}")


def select_weekly_baseline(
    *,
    records: List[Dict[str, Any]],
    account_id: str,
    snapshot_date: str,
) -> Optional[Dict[str, Any]]:
    target_date = parse_iso_date(snapshot_date)
    if not account_id or target_date is None:
        return None
    expected_date = target_date - timedelta(days=7)
    candidates: List[tuple[date, Dict[str, Any]]] = []
    for record in records:
        fields = record.get("fields") or {}
        if str(fields.get("账号ID") or "").strip() != account_id:
            continue
        candidate_date = parse_iso_date(fields.get("日期文本") or fields.get("日历日期"))
        if candidate_date is None or candidate_date > expected_date:
            continue
        candidates.append((candidate_date, fields))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def select_portal_weekly_baseline(
    *,
    records: List[Dict[str, Any]],
    reports: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    metrics = compute_dashboard_portal_metrics(reports)
    snapshot_date = extract_snapshot_date(metrics["captured_at"])
    expected_accounts = len(reports)
    target_date = parse_iso_date(snapshot_date)
    if target_date is None:
        return None
    expected_date = target_date - timedelta(days=7)

    total_fans = 0
    total_interaction = 0
    total_works = 0
    total_likes = 0
    total_comments = 0
    covered_accounts = 0
    baseline_dates: List[str] = []

    for report in reports:
        account_id = str((report.get("profile") or {}).get("profile_user_id") or "").strip()
        if not account_id:
            continue
        candidates: List[tuple[date, Dict[str, Any]]] = []
        for record in records:
            fields = record.get("fields") or {}
            if str(fields.get("账号ID") or "").strip() != account_id:
                continue
            candidate_date = parse_iso_date(fields.get("日期文本") or fields.get("日历日期"))
            if candidate_date is None or candidate_date > expected_date:
                continue
            candidates.append((candidate_date, fields))
        if not candidates:
            continue
        candidates.sort(key=lambda item: item[0], reverse=True)
        selected_date, selected_fields = candidates[0]
        baseline_dates.append(selected_date.isoformat())
        total_fans += to_optional_int(selected_fields.get("粉丝数")) or 0
        total_interaction += to_optional_int(selected_fields.get("获赞收藏数")) or 0
        total_works += to_optional_int(selected_fields.get("首页可见作品数")) or 0
        total_likes += to_optional_int(selected_fields.get("首页总点赞")) or 0
        total_comments += to_optional_int(selected_fields.get("首页总评论")) or 0
        covered_accounts += 1

    if covered_accounts == 0:
        return None

    baseline_dates.sort()
    if len(set(baseline_dates)) == 1:
        baseline_date_text = baseline_dates[0]
    else:
        baseline_date_text = f"{baseline_dates[0]}~{baseline_dates[-1]}"
    return {
        "baseline_date_text": baseline_date_text,
        "covered_accounts": covered_accounts,
        "expected_accounts": expected_accounts,
        "total_fans": total_fans,
        "total_interaction": total_interaction,
        "total_works": total_works,
        "total_likes": total_likes,
        "total_comments": total_comments,
    }


def build_single_work_rankings(
    *,
    reports: List[Dict[str, Any]],
    history_index: Dict[str, List[tuple[date, Dict[str, Any]]]],
) -> Dict[str, List[Dict[str, Any]]]:
    items = build_single_work_items(reports)

    like_rank = sorted(
        items,
        key=lambda item: (
            item["like_count"],
            item["comment_count"] if item["comment_count"] is not None else -1,
            item["title_copy"],
            item["account"],
        ),
        reverse=True,
    )
    comment_rank = sorted(
        [item for item in items if item["comment_count"] is not None],
        key=lambda item: (
            item["comment_count"],
            item["like_count"],
            item["title_copy"],
            item["account"],
        ),
        reverse=True,
    )
    growth_rank = []
    for item in items:
        baseline = select_previous_day_work_baseline(
            history_index=history_index,
            fingerprint=item["fingerprint"],
            snapshot_date=item["snapshot_date"],
        )
        if not baseline:
            continue
        previous_like = to_optional_int(baseline.get("点赞数"))
        previous_comment = to_optional_int(baseline.get("评论数"))
        if previous_like is None and previous_comment is None:
            continue
        like_delta = item["like_count"] - (previous_like or 0)
        comment_delta = (item["comment_count"] or 0) - (previous_comment or 0)
        engagement_delta = like_delta + comment_delta
        previous_total = (previous_like or 0) + (previous_comment or 0)
        item_with_growth = dict(item)
        item_with_growth.update(
            {
                "baseline_date_text": str(baseline.get("日期文本") or ""),
                "previous_like_count": previous_like,
                "previous_comment_count": previous_comment,
                "like_day_delta": like_delta,
                "comment_day_delta": comment_delta,
                "engagement_day_delta": engagement_delta,
                "engagement_day_rate": compute_growth_rate(
                    current_value=item["like_count"] + (item["comment_count"] or 0),
                    previous_value=previous_total,
                )
                if previous_total > 0
                else None,
            }
        )
        if engagement_delta > 0:
            growth_rank.append(item_with_growth)

    growth_rank.sort(
        key=lambda item: (
            item["engagement_day_delta"],
            item["comment_day_delta"],
            item["like_day_delta"],
            item["title_copy"],
        ),
        reverse=True,
    )

    return {
        "单条点赞排行": like_rank,
        "单条评论排行": comment_rank,
        "单条第二天增长排行": growth_rank,
    }


def build_single_work_items(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for report in reports:
        profile = report.get("profile") or {}
        captured_at = str(report.get("captured_at") or "")
        snapshot_date = extract_snapshot_date(captured_at)
        for work in report.get("works") or []:
            fingerprint = build_work_fingerprint(
                profile_user_id=profile.get("profile_user_id") or "",
                title=work.get("title_copy") or "",
                cover_url=work.get("cover_url") or "",
            )
            items.append(
                {
                    "snapshot_date": snapshot_date,
                    "captured_at": captured_at,
                    "account_id": profile.get("profile_user_id") or "",
                    "account": profile.get("nickname") or "",
                    "profile_url": profile.get("profile_url") or "",
                    "fingerprint": fingerprint,
                    "title_copy": work.get("title_copy") or "",
                    "note_type": work.get("note_type") or "",
                    "cover_url": work.get("cover_url") or "",
                    "note_url": work.get("note_url") or "",
                    "like_count": work_numeric_like(work),
                    "comment_count": work_numeric_comment(work),
                    "comment_count_is_lower_bound": bool(work.get("comment_count_is_lower_bound")),
                    "xsec_token": work.get("xsec_token") or "",
                }
            )
    return items


def select_previous_day_work_baseline(
    *,
    history_index: Dict[str, List[tuple[date, Dict[str, Any]]]],
    fingerprint: str,
    snapshot_date: str,
) -> Optional[Dict[str, Any]]:
    target_date = parse_iso_date(snapshot_date)
    if target_date is None:
        return None
    expected_date = target_date - timedelta(days=1)
    for candidate_date, fields in history_index.get(fingerprint, []):
        if candidate_date == expected_date:
            return fields
    return None


def build_single_work_ranking_fields(*, item: Dict[str, Any], rank_type: str, rank: int) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "榜单键": f"{rank_type}|{item['fingerprint']}",
        "榜单类型": rank_type,
        "排名": rank,
        "卡片标签": rank_label(rank),
        "账号ID": item["account_id"],
        "账号": item["account"],
        "作品指纹": item["fingerprint"],
        "标题文案": item["title_copy"],
        "作品类型": item["note_type"],
        "点赞数": item["like_count"],
        "数据更新时间": to_ms(item["captured_at"]),
    }
    if item.get("comment_count") is not None:
        fields["评论数"] = item["comment_count"]
        comment_basis = "评论预览下限" if item.get("comment_count_is_lower_bound") else "精确值"
        fields["评论数口径"] = comment_basis
        # 当前排行榜单表没有新增字段权限时，复用现有空闲列承载这条信息。
        fields["单选"] = comment_basis
    if rank_type == "单条点赞排行":
        fields["排序值"] = item["like_count"]
        fields["榜单摘要"] = f"点赞 {item['like_count']}"
    elif rank_type == "单条评论排行":
        fields["排序值"] = item.get("comment_count") or 0
        fields["榜单摘要"] = f"评论 {item.get('comment_count') or 0}"
    else:
        fields["排序值"] = item.get("engagement_day_delta") or 0
        if item.get("baseline_date_text"):
            fields["对比日期文本"] = item["baseline_date_text"]
        if item.get("previous_like_count") is not None:
            fields["昨日点赞数"] = item["previous_like_count"]
        if item.get("previous_comment_count") is not None:
            fields["昨日评论数"] = item["previous_comment_count"]
        fields["点赞次日增量"] = item.get("like_day_delta") or 0
        fields["评论次日增量"] = item.get("comment_day_delta") or 0
        fields["互动次日增量"] = item.get("engagement_day_delta") or 0
        if item.get("engagement_day_rate") is not None:
            fields["互动次日增幅"] = item["engagement_day_rate"]
        fields["榜单摘要"] = (
            f"次日互动 +{item.get('engagement_day_delta') or 0}"
            f" | 点赞 +{item.get('like_day_delta') or 0}"
            f" | 评论 +{item.get('comment_day_delta') or 0}"
        )
    if item.get("cover_url"):
        fields["封面图"] = {"text": "封面图", "link": item["cover_url"]}
    if item.get("profile_url"):
        fields["主页链接"] = {"text": item["account"] or "小红书主页", "link": item["profile_url"]}
    if item.get("note_url"):
        fields["作品链接"] = {"text": "作品链接", "link": item["note_url"]}
    return {key: value for key, value in fields.items() if value not in ("", None)}


def sync_dashboard_ranking(
    *,
    ranking_client: FeishuBitableClient,
    report: Dict[str, Any],
) -> Dict[str, int]:
    profile = report["profile"]
    account_id = str(profile.get("profile_user_id") or "").strip()
    ranked = rank_profile_works(report["works"])
    desired_fields: Dict[str, Dict[str, Any]] = {}
    for rank, work in enumerate(ranked, start=1):
        fields = build_dashboard_ranking_fields(report=report, work=work, rank=rank)
        desired_fields[str(fields["榜单键"])] = fields

    existing_rows: Dict[str, Dict[str, Any]] = {}
    for record in ranking_client.list_records(page_size=500):
        fields = record.get("fields") or {}
        if str(fields.get("账号ID") or "").strip() != account_id:
            continue
        row_key = str(fields.get("榜单键") or "").strip()
        if row_key:
            existing_rows[row_key] = {
                "record_id": str(record.get("record_id") or "").strip(),
                "fields": dict(fields),
            }

    created = 0
    updated = 0
    for row_key, fields in desired_fields.items():
        existing = existing_rows.pop(row_key, None)
        record_id = str((existing or {}).get("record_id") or "").strip()
        if record_id:
            if fields_match((existing or {}).get("fields") or {}, fields, ignore_fields=["数据更新时间"]):
                continue
            ranking_client.update_record(record_id, fields)
            updated += 1
            continue
        ranking_client.create_record(fields)
        created += 1

    deleted = 0
    for record_id in existing_rows.values():
        if not record_id:
            continue
        ranking_client.delete_record(record_id)
        deleted += 1

    return {
        "ranking_created": created,
        "ranking_updated": updated,
        "ranking_deleted": deleted,
    }


def build_dashboard_ranking_fields(*, report: Dict[str, Any], work: Dict[str, Any], rank: int) -> Dict[str, Any]:
    profile = report["profile"]
    fingerprint = build_work_fingerprint(
        profile_user_id=profile.get("profile_user_id") or "",
        title=work.get("title_copy") or "",
        cover_url=work.get("cover_url") or "",
    )
    fields: Dict[str, Any] = {
        "榜单键": f"{profile.get('profile_user_id') or ''}|{fingerprint}",
        "账号ID": profile.get("profile_user_id") or "",
        "账号": profile.get("nickname") or "",
        "排名": rank,
        "卡片标签": rank_label(rank),
        "作品指纹": fingerprint,
        "标题文案": work.get("title_copy") or "",
        "作品类型": work.get("note_type") or "",
        "点赞数": work_numeric_like(work),
        "点赞文本": work.get("like_count_text") or "",
        "主页链接": {
            "text": profile.get("nickname") or "小红书主页",
            "link": profile.get("profile_url") or "",
        },
        "xsec_token": work.get("xsec_token") or "",
        "数据更新时间": to_ms(report.get("captured_at") or ""),
        "作品摘要": f"#{rank} | 点赞 {work.get('like_count_text') or work_numeric_like(work)} | {work.get('note_type') or '未知'}",
    }
    if work.get("cover_url"):
        fields["封面图"] = {"text": "封面图", "link": work["cover_url"]}
    if work.get("note_url"):
        fields["作品链接"] = {"text": "作品链接", "link": work["note_url"]}
    return {key: value for key, value in fields.items() if value not in ("", None)}


def compute_dashboard_metrics(report: Dict[str, Any]) -> Dict[str, float]:
    works = report.get("works") or []
    visible_work_count = len(works)
    video_count = sum(1 for work in works if str(work.get("note_type") or "").strip().lower() == "video")
    image_count = max(0, visible_work_count - video_count)
    like_values = [work_numeric_like(work) for work in works]
    comment_values = []
    for work in works:
        comment_value = work_numeric_comment(work)
        if comment_value is not None:
            comment_values.append(comment_value)
    total_likes = sum(like_values)
    total_comments = sum(comment_values)
    average_likes = round(total_likes / visible_work_count, 2) if visible_work_count else 0.0
    average_comments = round(total_comments / len(comment_values), 2) if comment_values else 0.0
    top_like_count = max(like_values) if like_values else 0
    video_ratio = round((video_count / visible_work_count) * 100, 2) if visible_work_count else 0.0
    return {
        "visible_work_count": visible_work_count,
        "video_count": video_count,
        "image_count": image_count,
        "video_ratio": video_ratio,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "average_comments": average_comments,
        "tracked_comment_work_count": len(comment_values),
        "average_likes": average_likes,
        "top_like_count": top_like_count,
    }


def compute_dashboard_portal_metrics(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    account_ids = sorted(str((report.get("profile") or {}).get("profile_user_id") or "").strip() for report in reports)
    account_ids = [item for item in account_ids if item]
    account_group_key = hashlib.sha1("|".join(account_ids).encode("utf-8")).hexdigest() if account_ids else "empty"
    total_fans = 0
    total_interaction = 0
    all_works: List[Dict[str, Any]] = []
    captured_at = ""
    for report in reports:
        profile = report.get("profile") or {}
        total_fans += parse_exact_number(profile.get("fans_count_text")) or 0
        total_interaction += parse_exact_number(profile.get("interaction_count_text")) or 0
        current_captured_at = str(report.get("captured_at") or "").strip()
        if current_captured_at and current_captured_at > captured_at:
            captured_at = current_captured_at
        for work in report.get("works") or []:
            merged = dict(work)
            merged["nickname"] = profile.get("nickname") or ""
            all_works.append(merged)
    total_works = len(all_works)
    video_count = sum(1 for work in all_works if str(work.get("note_type") or "").strip().lower() == "video")
    image_count = max(0, total_works - video_count)
    total_likes = sum(work_numeric_like(work) for work in all_works)
    total_comments = 0
    tracked_comment_work_count = 0
    for work in all_works:
        comment_value = work_numeric_comment(work)
        if comment_value is None:
            continue
        total_comments += comment_value
        tracked_comment_work_count += 1
    average_likes = round(total_likes / total_works, 2) if total_works else 0.0
    average_comments = round(total_comments / tracked_comment_work_count, 2) if tracked_comment_work_count else 0.0
    video_ratio = round((video_count / total_works) * 100, 2) if total_works else 0.0
    ranked = sorted(
        all_works,
        key=lambda work: (
            work_numeric_like(work),
            str(work.get("title_copy") or ""),
            str(work.get("nickname") or ""),
        ),
        reverse=True,
    )
    return {
        "account_group_key": account_group_key,
        "account_count": len(reports),
        "captured_at": captured_at or datetime.now().astimezone().isoformat(timespec="seconds"),
        "total_fans": total_fans,
        "total_interaction": total_interaction,
        "total_works": total_works,
        "video_count": video_count,
        "image_count": image_count,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "tracked_comment_work_count": tracked_comment_work_count,
        "average_comments": average_comments,
        "average_likes": average_likes,
        "video_ratio": video_ratio,
        "top_work": ranked[0] if ranked else {},
        "top3_works": ranked[:3],
    }


def rank_profile_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        works,
        key=lambda work: (
            work_numeric_like(work),
            str(work.get("like_count_text") or ""),
            -(int(work.get("index") or 0)),
            str(work.get("title_copy") or ""),
        ),
        reverse=True,
    )


def work_numeric_like(work: Dict[str, Any]) -> int:
    value = work.get("like_count")
    if isinstance(value, (int, float)):
        return int(value)
    text = str(work.get("like_count_text") or "").strip().replace(",", "")
    if text.isdigit():
        return int(text)
    return 0


def work_numeric_comment(work: Dict[str, Any]) -> Optional[int]:
    value = work.get("comment_count")
    if isinstance(value, (int, float)):
        return int(value)
    text = str(work.get("comment_count_text") or "").strip().replace(",", "")
    if text.isdigit():
        return int(text)
    return None


def build_top3_summary(works: List[Dict[str, Any]]) -> str:
    lines = [
        f"{index}. {work.get('title_copy') or '无标题'} | 点赞 {work.get('like_count_text') or work_numeric_like(work)}"
        for index, work in enumerate(works, start=1)
    ]
    return "\n".join(lines)


def build_global_top3_summary(works: List[Dict[str, Any]]) -> str:
    lines = [
        (
            f"{index}. {work.get('title_copy') or '无标题'} | "
            f"{work.get('nickname') or '未知账号'} | 点赞 {work.get('like_count_text') or work_numeric_like(work)}"
        )
        for index, work in enumerate(works, start=1)
    ]
    return "\n".join(lines)


def build_account_distribution_summary(reports: List[Dict[str, Any]]) -> str:
    lines = []
    for report in reports:
        profile = report.get("profile") or {}
        metrics = compute_dashboard_metrics(report)
        work_count_display = profile.get("work_count_display_text") or str(metrics["visible_work_count"])
        lines.append(
            (
                f"{profile.get('nickname') or profile.get('profile_user_id') or '未知账号'}"
                f" | 粉丝 {profile.get('fans_count_text') or '0'}"
                f" | 作品 {work_count_display}"
                f" | 总赞 {metrics['total_likes']}"
            )
        )
    return "\n".join(lines)


def build_dashboard_remark(report: Dict[str, Any]) -> str:
    profile = report["profile"]
    remarks = [
        "看板指标基于公开主页可见作品",
        "建议在飞书仪表盘中配置指标卡、榜单和趋势图",
    ]
    if not profile.get("work_count_exact", True):
        remarks.append(f"作品数展示为已抓取下限 {profile.get('work_count_display_text') or profile.get('visible_work_count')}")
    if any(not item.get("note_id") for item in report.get("works") or []):
        remarks.append("公开页未返回 note_id，作品链接可能缺失")
    if profile.get("fans_count_text"):
        remarks.append(f"粉丝展示值为 {profile.get('fans_count_text')}")
    return "；".join(remarks)


def build_dashboard_portal_remark(reports: List[Dict[str, Any]]) -> str:
    notes = [
        "总控指标基于已监控账号的最新一次抓取结果",
        "顶部指标卡建议直接使用本表",
    ]
    if any(any(not item.get("note_id") for item in report.get("works") or []) for report in reports):
        notes.append("部分作品缺少 note_id，个别作品链接可能为空")
    return "；".join(notes)


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


def rank_label(rank: int) -> str:
    if rank == 1:
        return "TOP1"
    if rank == 2:
        return "TOP2"
    if rank == 3:
        return "TOP3"
    return "观察中"


def to_ms(iso_text: str) -> int:
    return int(datetime.fromisoformat(iso_text).timestamp() * 1000)


def parse_exact_number(value: Any) -> Optional[int]:
    text = str(value or "").strip().replace(",", "")
    if not text or not text.isdigit():
        return None
    return int(text)


if __name__ == "__main__":
    raise SystemExit(main())
