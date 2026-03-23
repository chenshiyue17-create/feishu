from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import load_settings
from .feishu import FeishuBitableClient
from .profile_dashboard_to_feishu import sync_dashboard_tables
from .profile_report import build_profile_report, enrich_profile_report_with_note_metrics, load_profile_report_payload
from .profile_to_feishu import PROFILE_FIELD_SPECS, build_profile_feishu_fields, dedupe_profile_records
from .profile_works_to_feishu import (
    WORKS_TABLE_FIELDS,
    WORKS_TABLE_NAME,
    build_work_feishu_fields,
    dedupe_work_records,
    ensure_works_table,
)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把 MediaCrawler 导出的小红书内容明细同步到飞书。")
    parser.add_argument("--contents-file", required=True, help="MediaCrawler 导出的 xhs contents.json/jsonl 文件")
    parser.add_argument("--profile-url", help="可选：小红书账号主页链接，用于补充账号摘要")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--works-table-name", default=WORKS_TABLE_NAME)
    parser.add_argument("--ensure-fields", action="store_true", help="自动补齐飞书字段")
    parser.add_argument("--sync-dashboard", action="store_true", help="额外同步看板总览、趋势和榜单数据")
    parser.add_argument("--dry-run", action="store_true", help="只输出归一化结果，不写入飞书")
    parser.add_argument("--json-out", help="dry-run 时输出 JSON 文件")
    args = parser.parse_args(argv)

    contents = load_mediacrawler_records(args.contents_file)
    settings = load_settings(args.env_file)
    profile_context = None
    if args.profile_url:
        payload = load_profile_report_payload(settings=settings, profile_url=args.profile_url)
        profile_context = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
        profile_context = enrich_profile_report_with_note_metrics(report=profile_context, settings=settings)

    report = build_report_from_mediacrawler(
        content_items=contents,
        profile_url=args.profile_url or "",
        profile_context=profile_context,
    )
    if args.dry_run:
        output = json.dumps(report, ensure_ascii=False, indent=2)
        if args.json_out:
            path = Path(args.json_out).expanduser().resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(output, encoding="utf-8")
            print(f"[OK] wrote {path}")
        print(output)
        return 0

    settings.validate_for_sync()
    sync_report_to_feishu(
        report=report,
        settings=settings,
        works_table_name=args.works_table_name,
        ensure_fields=args.ensure_fields,
        sync_dashboard=args.sync_dashboard,
    )
    return 0


def load_mediacrawler_records(path_text: str) -> List[Dict[str, Any]]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        items = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                items.append(payload)
        return items
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    raise ValueError(f"不支持的 MediaCrawler 文件结构: {path}")


def build_report_from_mediacrawler(
    *,
    content_items: List[Dict[str, Any]],
    profile_url: str = "",
    profile_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items = [item for item in content_items if isinstance(item, dict) and item.get("note_id")]
    if not items:
        raise ValueError("MediaCrawler 内容文件里没有可用的 note_id 记录")

    profile_seed = profile_context.get("profile") if profile_context else {}
    sorted_items = sorted(
        items,
        key=lambda item: (
            _coerce_int(item.get("time")),
            _coerce_int(item.get("last_update_time")),
            _coerce_int(item.get("liked_count")),
            str(item.get("note_id") or ""),
        ),
        reverse=True,
    )
    first = sorted_items[0]
    profile_user_id = _first_text(
        profile_seed.get("profile_user_id") if profile_seed else "",
        first.get("user_id"),
    )
    nickname = _first_text(
        profile_seed.get("nickname") if profile_seed else "",
        first.get("nickname"),
    )
    avatar_url = _first_text(
        profile_seed.get("avatar_url") if profile_seed else "",
        first.get("avatar"),
    )
    ip_location = _first_text(
        profile_seed.get("ip_location") if profile_seed else "",
        first.get("ip_location"),
    )
    captured_at = datetime.now().astimezone().isoformat(timespec="seconds")

    works: List[Dict[str, Any]] = []
    for index, item in enumerate(sorted_items):
        note_id = str(item.get("note_id") or "").strip()
        xsec_token = str(item.get("xsec_token") or "").strip()
        note_url = str(item.get("note_url") or "").strip()
        if not note_url and note_id:
            note_url = f"https://www.xiaohongshu.com/explore/{note_id}"
        works.append(
            {
                "record_type": "work",
                "profile_user_id": profile_user_id,
                "nickname": nickname,
                "title_copy": str(item.get("title") or item.get("desc") or "").strip(),
                "note_type": str(item.get("type") or "").strip(),
                "like_count": _coerce_int(item.get("liked_count")),
                "like_count_text": str(item.get("liked_count") or "").strip(),
                "cover_url": pick_cover_url(item),
                "xsec_token": xsec_token,
                "note_id": note_id,
                "note_url": note_url,
                "index": index,
                "collect_count": _coerce_int(item.get("collected_count")),
                "comment_count": _coerce_int(item.get("comment_count")),
                "share_count": _coerce_int(item.get("share_count")),
            }
        )

    profile = {
        "record_type": "account",
        "profile_url": profile_url or profile_seed.get("profile_url", ""),
        "profile_user_id": profile_user_id,
        "nickname": nickname,
        "red_id": profile_seed.get("red_id", ""),
        "desc": profile_seed.get("desc", ""),
        "ip_location": ip_location,
        "avatar_url": avatar_url,
        "gender": profile_seed.get("gender"),
        "follows_count_text": profile_seed.get("follows_count_text", ""),
        "fans_count_text": profile_seed.get("fans_count_text", ""),
        "interaction_count_text": profile_seed.get("interaction_count_text", ""),
        "visible_work_count": len(works),
        "total_work_count": len(works),
        "work_count_display_text": str(len(works)),
        "work_count_exact": True,
        "work_count_has_more": False,
        "tags": profile_seed.get("tags", []),
    }
    return {
        "captured_at": captured_at,
        "profile": profile,
        "works": works,
    }


def sync_report_to_feishu(
    *,
    report: Dict[str, Any],
    settings,
    works_table_name: str,
    ensure_fields: bool,
    sync_dashboard: bool,
) -> None:
    summary_client = FeishuBitableClient(settings)
    if ensure_fields:
        summary_client.ensure_fields(PROFILE_FIELD_SPECS)
    deduped_profiles = dedupe_profile_records(summary_client)
    summary_fields = build_profile_feishu_fields(report)
    summary_action, summary_record_id = summary_client.upsert_record(
        unique_field="账号ID",
        unique_value=summary_fields["账号ID"],
        fields=summary_fields,
    )

    works_table_id = ensure_works_table(
        tables_client=summary_client,
        settings=settings,
        table_name=works_table_name,
    )
    works_client = FeishuBitableClient(replace(settings, feishu_table_id=works_table_id))
    if ensure_fields:
        works_client.ensure_fields(WORKS_TABLE_FIELDS)
    deduped_works = dedupe_work_records(works_client)

    synced_works = 0
    for work in report["works"]:
        fields = build_work_feishu_fields(report=report, work=work)
        action, record_id = works_client.upsert_record(
            unique_field="作品指纹",
            unique_value=fields["作品指纹"],
            fields=fields,
        )
        synced_works += 1
        print(f"[OK] work {action} record_id={record_id} title={fields['标题文案']}")

    print(f"[OK] summary {summary_action} record_id={summary_record_id}")
    print(f"[OK] deduped_profiles={deduped_profiles}")
    print(f"[OK] works_table={works_table_name} table_id={works_table_id}")
    print(f"[OK] synced_works={synced_works}")
    print(f"[OK] deduped_works={deduped_works}")
    if sync_dashboard:
        dashboard_result = sync_dashboard_tables(report=report, settings=settings)
        print(
            "[OK] dashboard "
            f"overview={dashboard_result['overview_action']}:{dashboard_result['overview_record_id']} "
            f"trend={dashboard_result['trend_action']}:{dashboard_result['trend_record_id']} "
            f"ranking_created={dashboard_result['ranking_created']} "
            f"ranking_updated={dashboard_result['ranking_updated']} "
            f"ranking_deleted={dashboard_result['ranking_deleted']}"
        )


def pick_cover_url(item: Dict[str, Any]) -> str:
    image_list = str(item.get("image_list") or "").strip()
    if image_list:
        return image_list.split(",", 1)[0].strip()
    video_url = str(item.get("video_url") or "").strip()
    if video_url:
        return video_url
    return ""


def _coerce_int(value: Any) -> int:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return 0
    if text.endswith("万"):
        try:
            return int(float(text[:-1]) * 10000)
        except ValueError:
            return 0
    if text.isdigit():
        return int(text)
    try:
        return int(float(text))
    except ValueError:
        return 0


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
