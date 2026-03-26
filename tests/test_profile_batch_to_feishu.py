from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.profile_batch_to_feishu import (
    build_dense_rank_map,
    build_dry_run_summary,
    build_export_review_key,
    build_project_launchd_specs,
    build_project_account_ranking_rows,
    build_project_sync_error_message,
    build_batch_sync_program_arguments,
    has_cached_project_rankings,
    is_feishu_forbidden_error,
    build_record_id_index,
    build_record_state_index,
    sync_cached_project_account_rankings_to_feishu,
    sync_cached_project_calendar_to_feishu,
    sync_cached_project_rankings_to_feishu,
    sync_project_rankings_into_single_table,
    offset_daily_time,
    merge_report_with_existing_work_details,
    load_export_review_rows,
    load_reports_from_json,
    normalize_batch_item_to_report,
    normalize_unique_value,
    sync_export_review_tables_to_feishu,
    resolve_launchd_paths,
    slugify_project_name,
    upsert_record_with_index,
)
from xhs_feishu_monitor.profile_works_to_feishu import build_work_fingerprint


class ProfileBatchToFeishuTest(unittest.TestCase):
    def test_normalize_batch_item_to_report_fills_profile_url_and_captured_at(self) -> None:
        report = normalize_batch_item_to_report(
            {
                "status": "success",
                "requested_url": "https://www.xiaohongshu.com/user/profile/u1",
                "profile": {"nickname": "测试账号", "profile_user_id": "u1"},
                "works": [{"title_copy": "作品A"}],
            }
        )
        self.assertTrue(report["captured_at"])
        self.assertEqual(report["profile"]["profile_url"], "https://www.xiaohongshu.com/user/profile/u1")

    def test_load_reports_from_json_filters_success_items(self) -> None:
        payload = {
            "items": [
                {"status": "success", "profile": {"profile_user_id": "u1"}, "works": []},
                {"status": "failed", "error": "x"},
            ]
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "report.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            reports = load_reports_from_json(str(path))
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["profile"]["profile_user_id"], "u1")

    def test_build_dry_run_summary(self) -> None:
        summary = build_dry_run_summary(
            [
                {
                    "profile": {"nickname": "账号A", "profile_user_id": "u1", "fans_count_text": "100", "interaction_count_text": "999"},
                    "works": [{"title_copy": "作品1"}, {"title_copy": "作品2"}],
                }
            ]
        )
        self.assertEqual(summary["total_accounts"], 1)
        self.assertEqual(summary["total_works"], 2)
        self.assertEqual(summary["items"][0]["头部作品"], ["作品1", "作品2"])

    def test_normalize_unique_value_handles_links_and_lists(self) -> None:
        self.assertEqual(normalize_unique_value({"text": "主页", "link": "https://x"}), "主页")
        self.assertEqual(normalize_unique_value(["u1", {"link": "https://x"}]), "u1|https://x")

    def test_build_record_id_index_and_upsert_record_with_index(self) -> None:
        client = _FakeClient(
            records=[
                {"record_id": "rec_1", "fields": {"账号ID": "u1"}},
            ]
        )
        index = build_record_id_index(client, unique_field="账号ID")
        self.assertEqual(index, {"u1": "rec_1"})

        action, record_id = upsert_record_with_index(
            client=client,
            record_index=index,
            unique_field="账号ID",
            unique_value="u1",
            fields={"账号ID": "u1", "账号": "账号A"},
        )
        self.assertEqual((action, record_id), ("updated", "rec_1"))
        self.assertEqual(client.updated, [("rec_1", {"账号ID": "u1", "账号": "账号A"})])

        action, record_id = upsert_record_with_index(
            client=client,
            record_index=index,
            unique_field="账号ID",
            unique_value="u2",
            fields={"账号ID": "u2", "账号": "账号B"},
        )
        self.assertEqual((action, record_id), ("created", "rec_2"))
        self.assertEqual(client.created, [{"账号ID": "u2", "账号": "账号B"}])
        self.assertEqual(index["u2"], "rec_2")

    def test_upsert_record_with_index_skips_when_only_timestamp_changes(self) -> None:
        client = _FakeClient(
            records=[
                {"record_id": "rec_1", "fields": {"账号ID": "u1", "账号": "账号A", "上报时间": 111}},
            ]
        )
        state_index = build_record_state_index(client, unique_field="账号ID")
        index = {"u1": "rec_1"}
        action, record_id = upsert_record_with_index(
            client=client,
            record_index=index,
            record_state_index=state_index,
            unique_field="账号ID",
            unique_value="u1",
            fields={"账号ID": "u1", "账号": "账号A", "上报时间": 222},
            compare_ignore_fields=["上报时间"],
        )
        self.assertEqual((action, record_id), ("skipped", "rec_1"))
        self.assertEqual(client.updated, [])

    def test_upsert_record_with_index_updates_when_business_fields_change(self) -> None:
        client = _FakeClient(
            records=[
                {"record_id": "rec_1", "fields": {"账号ID": "u1", "账号": "账号A", "上报时间": 111}},
            ]
        )
        state_index = build_record_state_index(client, unique_field="账号ID")
        index = {"u1": "rec_1"}
        action, record_id = upsert_record_with_index(
            client=client,
            record_index=index,
            record_state_index=state_index,
            unique_field="账号ID",
            unique_value="u1",
            fields={"账号ID": "u1", "账号": "账号B", "上报时间": 222},
            compare_ignore_fields=["上报时间"],
        )
        self.assertEqual((action, record_id), ("updated", "rec_1"))
        self.assertEqual(client.updated, [("rec_1", {"账号ID": "u1", "账号": "账号B", "上报时间": 222})])

    def test_build_batch_sync_program_arguments(self) -> None:
        argv = build_batch_sync_program_arguments(
            urls=["https://www.xiaohongshu.com/user/profile/u1"],
            urls_file=None,
            raw_text="",
            project="项目A",
            env_file="xhs_feishu_monitor/.env",
            profile_table_name="账号总览表",
            works_table_name="作品明细表",
            ensure_fields=True,
            sync_dashboard=True,
        )
        self.assertEqual(argv[1:3], ["-m", "xhs_feishu_monitor.profile_batch_to_feishu"])
        self.assertIn("--url", argv)
        self.assertIn("--profile-table-name", argv)
        self.assertIn("--works-table-name", argv)
        self.assertIn("--ensure-fields", argv)
        self.assertIn("--sync-dashboard", argv)
        self.assertIn("--project", argv)

    def test_offset_daily_time(self) -> None:
        self.assertEqual(offset_daily_time("14:00", 20), "14:20")
        self.assertEqual(offset_daily_time("23:50", 20), "00:10")

    def test_slugify_project_name(self) -> None:
        self.assertEqual(slugify_project_name("上海 团购"), "上海-团购")

    def test_build_project_launchd_specs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "urls.txt"
            path.write_text(
                "项目A\thttps://www.xiaohongshu.com/user/profile/u1\n"
                "项目B\thttps://www.xiaohongshu.com/user/profile/u2\n",
                encoding="utf-8",
            )
            specs = build_project_launchd_specs(
                urls_file=str(path),
                explicit_project="",
                daily_at="14:00",
                project_slot_minutes=20,
                base_label="com.cc.test",
            )
        self.assertEqual(
            specs,
            [
                {"project": "项目A", "daily_at": "14:00", "label": "com.cc.test.项目a"},
                {"project": "项目B", "daily_at": "14:20", "label": "com.cc.test.项目b"},
            ],
        )

    def test_merge_report_with_existing_work_details_preserves_note_url_and_comments(self) -> None:
        report = {
            "profile": {"profile_user_id": "u1"},
            "works": [
                {
                    "title_copy": "作品A",
                    "cover_url": "https://img.example.com/a.jpg",
                    "note_url": "",
                    "note_id": "",
                    "comment_count": None,
                    "comment_count_text": "",
                    "recent_comments_summary": "",
                }
            ],
        }
        fingerprint = build_work_fingerprint(
            profile_user_id="u1",
            title="作品A",
            cover_url="https://img.example.com/a.jpg",
        )
        works_records = {
            fingerprint: {
                "record_id": "rec_1",
                "fields": {
                    "作品链接": {"link": "https://www.xiaohongshu.com/explore/abc123"},
                    "评论数": 18,
                    "评论文本": "18",
                    "最新评论摘要": "用户A: 老评论",
                },
            }
        }
        merged = merge_report_with_existing_work_details(report=report, works_records=works_records)
        self.assertEqual(merged["works"][0]["note_url"], "https://www.xiaohongshu.com/explore/abc123")
        self.assertEqual(merged["works"][0]["note_id"], "abc123")
        self.assertEqual(merged["works"][0]["comment_count"], 18)
        self.assertEqual(merged["works"][0]["comment_count_text"], "18")
        self.assertEqual(merged["works"][0]["recent_comments_summary"], "用户A: 老评论")

    def test_resolve_launchd_paths(self) -> None:
        paths = resolve_launchd_paths(label="com.cc.test-profile-batch-sync")
        self.assertTrue(paths["plist_path"].endswith("com.cc.test-profile-batch-sync.plist"))
        self.assertTrue(paths["stdout_log_path"].endswith("com.cc.test-profile-batch-sync.out.log"))
        self.assertTrue(paths["stderr_log_path"].endswith("com.cc.test-profile-batch-sync.err.log"))

    def test_build_project_sync_error_message(self) -> None:
        self.assertEqual(
            build_project_sync_error_message(project="东莞", error=ValueError("飞书接口错误 1254045: FieldNameNotFound")),
            "项目「东莞」飞书上传失败：排行榜表缺少字段",
        )
        self.assertEqual(
            build_project_sync_error_message(project="东莞", error=ValueError("批量抓取没有成功结果，无法同步到飞书")),
            "项目「东莞」抓取失败：本轮没有成功账号",
        )
        self.assertEqual(
            build_project_sync_error_message(project="东莞", error=ValueError("命中登录页，当前登录态不可用")),
            "项目「东莞」抓取失败：登录态异常",
        )

    def test_build_dense_rank_map(self) -> None:
        rows = [
            {"账号ID": "u1", "账号": "账号A", "粉丝数": 100},
            {"账号ID": "u2", "账号": "账号B", "粉丝数": 200},
            {"账号ID": "u3", "账号": "账号C", "粉丝数": 100},
        ]
        rank_map = build_dense_rank_map(rows, value_field="粉丝数")
        self.assertEqual(rank_map, {"u2": 1, "u1": 2, "u3": 2})

    def test_build_project_account_ranking_rows(self) -> None:
        grouped_rows = build_project_account_ranking_rows(
            {
                "东莞": [
                    {
                        "账号ID": "u1",
                        "账号": "账号A",
                        "粉丝数": 100,
                        "获赞收藏数": 200,
                        "首页总点赞": 20,
                        "首页总评论": 8,
                        "日期文本": "2026-03-25",
                    },
                    {
                        "账号ID": "u2",
                        "账号": "账号B",
                        "粉丝数": 180,
                        "获赞收藏数": 120,
                        "首页总点赞": 30,
                        "首页总评论": 5,
                        "日期文本": "2026-03-25",
                    },
                ]
            }
        )
        self.assertEqual(len(grouped_rows["东莞"]), 4)
        like_row_u2 = next(item for item in grouped_rows["东莞"] if item["账号ID"] == "u2" and item["榜单类型"] == "点赞排行")
        comment_row_u1 = next(item for item in grouped_rows["东莞"] if item["账号ID"] == "u1" and item["榜单类型"] == "评论排行")
        self.assertEqual(like_row_u2["项目账号榜单键"], "东莞|点赞排行|u2")
        self.assertEqual(like_row_u2["排名"], 1)
        self.assertEqual(like_row_u2["排序值"], 30)
        self.assertIn("前30条作品", like_row_u2["口径说明"])
        self.assertEqual(comment_row_u1["项目账号榜单键"], "东莞|评论排行|u1")
        self.assertEqual(comment_row_u1["排名"], 1)
        self.assertEqual(comment_row_u1["排序值"], 8)

    def test_has_cached_project_rankings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "飞书缓存"
            (cache_dir / "默认项目").mkdir(parents=True, exist_ok=True)
            (cache_dir / "默认项目" / "ranking_rows.json").write_text("[]", encoding="utf-8")
            export_dir = Path(temp_dir) / "账号榜单导出" / "东莞" / "2026-03-25_212805"
            export_dir.mkdir(parents=True, exist_ok=True)
            (export_dir / "项目导出摘要.json").write_text(json.dumps({"project": "东莞"}, ensure_ascii=False), encoding="utf-8")
            settings = SimpleNamespace(project_cache_dir=str(cache_dir), feishu_ranking_bitable_app_token="")
            self.assertTrue(has_cached_project_rankings(settings=settings))
            self.assertTrue(has_cached_project_rankings(settings=settings, project="默认项目"))
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.resolve_export_review_root", return_value=Path(temp_dir) / "账号榜单导出"):
                self.assertTrue(has_cached_project_rankings(settings=settings, project="东莞"))

    def test_sync_cached_project_rankings_to_feishu_prefers_export_reviews(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "飞书缓存"
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "calendar_rows.json").write_text(
                json.dumps([{"日历键": "2026-03-25|u1", "账号": "账号A"}], ensure_ascii=False),
                encoding="utf-8",
            )
            (project_dir / "ranking_rows.json").write_text(
                json.dumps(
                    [
                        {
                            "榜单键": "单条点赞排行|fp1",
                            "榜单类型": "单条点赞排行",
                            "排名": 1,
                            "账号ID": "u1",
                            "账号": "账号A",
                            "作品指纹": "fp1",
                            "标题文案": "作品A",
                            "点赞数": 88,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            settings = SimpleNamespace(project_cache_dir=str(cache_dir), feishu_ranking_bitable_app_token="")
            with (
                patch(
                    "xhs_feishu_monitor.profile_batch_to_feishu.sync_cached_project_calendar_to_feishu",
                    return_value={"calendar_project_count": 1},
                ) as sync_calendar,
                patch(
                    "xhs_feishu_monitor.profile_batch_to_feishu.sync_export_review_tables_to_feishu",
                    return_value={"project_count": 1, "single_work_ranking_created": 2},
                ) as sync_reviews,
            ):
                result = sync_cached_project_rankings_to_feishu(settings=settings, project="默认项目")
        self.assertEqual(result["calendar_project_count"], 1)
        self.assertEqual(result["project_count"], 1)
        sync_calendar.assert_called_once()
        sync_reviews.assert_called_once()

    def test_load_export_review_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "账号榜单导出" / "默认项目" / "2026-03-25_212805"
            account_dir = root / "账号A"
            account_dir.mkdir(parents=True, exist_ok=True)
            like_path = account_dir / "2026-03-25_212805-点赞排行.json"
            comment_path = account_dir / "2026-03-25_212805-评论排行.json"
            like_path.write_text(
                json.dumps(
                    [{"项目": "默认项目", "账号ID": "u1", "账号": "账号A", "排名": 1, "标题": "作品A", "数值": 88, "摘要": "点赞 88", "作品链接": "https://x/1", "主页链接": "https://x/u1", "封面图": "https://img/1"}],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            comment_path.write_text(
                json.dumps(
                    [{"项目": "默认项目", "账号ID": "u1", "账号": "账号A", "排名": 1, "标题": "作品A", "数值": 16, "摘要": "评论 16", "作品链接": "https://x/1", "主页链接": "https://x/u1", "封面图": "https://img/1", "评论口径": "精确值"}],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (root / "项目导出摘要.json").write_text(
                json.dumps(
                    {
                        "project": "默认项目",
                        "snapshot_time": "2026-03-25 21:28:05",
                        "snapshot_slug": "2026-03-25_212805",
                        "export_dir": str(root),
                        "accounts": [
                            {
                                "account_id": "u1",
                                "account": "账号A",
                                "files": {"like_json": str(like_path), "comment_json": str(comment_path)},
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            settings = SimpleNamespace(feishu_review_upload_days=14, feishu_review_per_account_limit=10)
            rows = load_export_review_rows(project="默认项目", export_dir=str(Path(temp_dir) / "账号榜单导出"), settings=settings)
        self.assertEqual(len(rows["每日点赞复盘"]), 1)
        self.assertEqual(len(rows["每日评论复盘"]), 1)
        self.assertEqual(rows["每日点赞复盘"][0]["点赞数"], 88)
        self.assertEqual(rows["每日评论复盘"][0]["评论口径"], "精确值")

    def test_sync_export_review_tables_to_feishu(self) -> None:
        client = _FakeRankingClient(tables=[], records=[], fields=[])
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "账号榜单导出" / "默认项目" / "2026-03-25_212805"
            account_dir = root / "账号A"
            account_dir.mkdir(parents=True, exist_ok=True)
            like_path = account_dir / "2026-03-25_212805-点赞排行.json"
            comment_path = account_dir / "2026-03-25_212805-评论排行.json"
            like_path.write_text(json.dumps([{"项目": "默认项目", "账号ID": "u1", "账号": "账号A", "排名": 1, "标题": "作品A", "数值": 88}], ensure_ascii=False), encoding="utf-8")
            comment_path.write_text(json.dumps([{"项目": "默认项目", "账号ID": "u1", "账号": "账号A", "排名": 1, "标题": "作品A", "数值": 16, "评论口径": "精确值"}], ensure_ascii=False), encoding="utf-8")
            (root / "项目导出摘要.json").write_text(
                json.dumps({"project": "默认项目", "snapshot_time": "2026-03-25 21:28:05", "snapshot_slug": "2026-03-25_212805", "export_dir": str(root), "accounts": [{"account_id": "u1", "account": "账号A", "files": {"like_json": str(like_path), "comment_json": str(comment_path)}}]}, ensure_ascii=False),
                encoding="utf-8",
            )
            settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token", feishu_review_upload_days=14, feishu_review_per_account_limit=10)
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client, client, client]):
                with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                    with patch("xhs_feishu_monitor.profile_batch_to_feishu.resolve_export_review_root", return_value=Path(temp_dir) / "账号榜单导出"):
                        summary = sync_export_review_tables_to_feishu(settings=settings, project="默认项目")
        self.assertEqual(summary["daily_like_review_created"], 1)
        self.assertEqual(summary["daily_comment_review_created"], 1)
        self.assertEqual(client.created_table_name, "每日评论复盘")
        self.assertEqual(len(client.created), 2)

    def test_load_export_review_rows_limits_days_and_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "账号榜单导出" / "默认项目"
            latest = root / "2026-03-25_100000"
            old = root / "2026-03-01_100000"
            for snapshot_root in (latest, old):
                account_dir = snapshot_root / "账号A"
                account_dir.mkdir(parents=True, exist_ok=True)
                like_path = account_dir / f"{snapshot_root.name}-点赞排行.json"
                comment_path = account_dir / f"{snapshot_root.name}-评论排行.json"
                rows = [{"项目": "默认项目", "账号ID": "u1", "账号": "账号A", "排名": index + 1, "标题": f"作品{index}", "数值": index + 1} for index in range(12)]
                like_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                comment_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                (snapshot_root / "项目导出摘要.json").write_text(
                    json.dumps({"project": "默认项目", "snapshot_time": snapshot_root.name.replace("_", " "), "snapshot_slug": snapshot_root.name, "export_dir": str(snapshot_root), "accounts": [{"account_id": "u1", "account": "账号A", "files": {"like_json": str(like_path), "comment_json": str(comment_path)}}]}, ensure_ascii=False),
                    encoding="utf-8",
                )
            settings = SimpleNamespace(feishu_review_upload_days=3, feishu_review_per_account_limit=10)
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.datetime") as mocked_datetime:
                from datetime import datetime as real_datetime
                mocked_datetime.now.return_value = real_datetime(2026, 3, 25, 12, 0, 0)
                mocked_datetime.strptime = real_datetime.strptime
                rows = load_export_review_rows(project="默认项目", export_dir=str(Path(temp_dir) / "账号榜单导出"), settings=settings)
        self.assertEqual(len(rows["每日点赞复盘"]), 10)
        self.assertEqual(len(rows["每日评论复盘"]), 10)

    def test_sync_cached_project_calendar_to_feishu_uses_project_cache(self) -> None:
        client = _FakeRankingClient(
            tables=[{"name": "小红书日历留底", "table_id": "tbl_calendar"}],
            records=[],
            fields=[
                {"field_name": "日历键"},
                {"field_name": "账号"},
                {"field_name": "备注"},
                {"field_name": "数据更新时间"},
            ],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "飞书缓存"
            project_dir = cache_dir / "东莞"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "calendar_rows.json").write_text(
                json.dumps(
                    [{"日历键": "2026-03-25|u1", "账号": "账号A", "备注": ""}],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            settings = SimpleNamespace(project_cache_dir=str(cache_dir), feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
                with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                    result = sync_cached_project_calendar_to_feishu(settings=settings, project="东莞")
        self.assertEqual(result["calendar_project_count"], 1)
        self.assertEqual(len(client.created), 1)
        self.assertIn("项目：东莞", client.created[0]["备注"])
        self.assertIn("前30条作品", client.created[0]["备注"])

    def test_sync_cached_project_account_rankings_to_feishu_uses_project_cache(self) -> None:
        client = _FakeRankingClient(
            tables=[],
            records=[],
            fields=[{"field_name": spec["field_name"]} for spec in [
                {"field_name": "项目账号榜单键"},
                {"field_name": "项目"},
                {"field_name": "榜单类型"},
                {"field_name": "排名"},
                {"field_name": "排序值"},
                {"field_name": "账号ID"},
                {"field_name": "账号"},
                {"field_name": "首页总点赞"},
                {"field_name": "首页总评论"},
                {"field_name": "口径说明"},
                {"field_name": "数据用途"},
                {"field_name": "数据更新时间"},
            ]],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "飞书缓存"
            project_dir = cache_dir / "东莞"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "calendar_rows.json").write_text(
                json.dumps(
                    [
                        {"账号ID": "u1", "账号": "账号A", "粉丝数": 100, "获赞收藏数": 120, "首页总点赞": 15, "首页总评论": 9, "日期文本": "2026-03-25"},
                        {"账号ID": "u2", "账号": "账号B", "粉丝数": 180, "获赞收藏数": 80, "首页总点赞": 30, "首页总评论": 3, "日期文本": "2026-03-25"},
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            settings = SimpleNamespace(project_cache_dir=str(cache_dir), feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
                with patch("xhs_feishu_monitor.profile_batch_to_feishu.ensure_named_table", return_value="tbl_project_account"):
                    with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                        result = sync_cached_project_account_rankings_to_feishu(settings=settings, project="东莞")
        self.assertEqual(result["project_account_ranking_project_count"], 1)
        self.assertEqual(len(client.created), 4)
        created_like_u2 = next(item for item in client.created if item["账号ID"] == "u2" and item["榜单类型"] == "点赞排行")
        created_comment_u1 = next(item for item in client.created if item["账号ID"] == "u1" and item["榜单类型"] == "评论排行")
        self.assertEqual(created_like_u2["排名"], 1)
        self.assertEqual(created_like_u2["排序值"], 30)
        self.assertIn("留底和协作展示", created_like_u2["口径说明"])
        self.assertEqual(created_comment_u1["排名"], 1)

    def test_sync_cached_project_rankings_to_feishu_can_upload_calendar_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "飞书缓存"
            project_dir = cache_dir / "东莞"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "calendar_rows.json").write_text(
                json.dumps([{"日历键": "2026-03-25|u1"}], ensure_ascii=False),
                encoding="utf-8",
            )
            settings = SimpleNamespace(project_cache_dir=str(cache_dir), feishu_ranking_bitable_app_token="")
            with (
                patch(
                    "xhs_feishu_monitor.profile_batch_to_feishu.sync_cached_project_calendar_to_feishu",
                    return_value={"calendar_project_count": 1},
                ) as sync_calendar,
                patch(
                    "xhs_feishu_monitor.profile_batch_to_feishu.sync_export_review_tables_to_feishu",
                    return_value={"project_count": 0},
                ) as sync_reviews,
            ):
                result = sync_cached_project_rankings_to_feishu(settings=settings, project="东莞", upload_rankings=False)
        self.assertEqual(result["calendar_project_count"], 1)
        sync_calendar.assert_called_once()
        sync_reviews.assert_not_called()

    def test_sync_project_rankings_into_single_table_does_not_delete_other_projects(self) -> None:
        client = _FakeRankingClient(
            tables=[{"name": "项目作品排行榜", "table_id": "tbl_data"}],
            records=[
                {"record_id": "rec_default", "fields": {"榜单键": "默认项目|u1|a", "榜单类型": "点赞排行", "文本": "默认项目"}},
                {"record_id": "rec_dongguan", "fields": {"榜单键": "东莞|u2|b", "榜单类型": "点赞排行", "文本": "东莞"}},
            ],
        )
        reports = [
            {
                "project": "东莞",
                "captured_at": "2026-03-24T10:00:00+08:00",
                "profile": {"profile_user_id": "u2", "nickname": "账号B"},
                "works": [
                    {
                        "title_copy": "作品B",
                        "cover_url": "https://img.example.com/b.jpg",
                        "like_count": 20,
                        "comment_count": 5,
                    }
                ],
            }
        ]
        settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
        with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                summary = sync_project_rankings_into_single_table(reports=reports, settings=settings)
        self.assertFalse(any(record_id == "rec_default" for record_id in client.deleted))
        self.assertEqual(summary["project_count"], 1)

    def test_sync_project_rankings_into_single_table_filters_unknown_fields(self) -> None:
        client = _FakeRankingClient(
            tables=[{"name": "小红书单条作品排行", "table_id": "tbl_data"}],
            records=[],
            fields=[
                {"field_name": "榜单键"},
                {"field_name": "榜单类型"},
                {"field_name": "文本"},
                {"field_name": "排名"},
                {"field_name": "账号ID"},
                {"field_name": "账号"},
                {"field_name": "标题文案"},
                {"field_name": "评论数"},
                {"field_name": "单选"},
                {"field_name": "数据更新时间"},
                {"field_name": "排序值"},
                {"field_name": "榜单摘要"},
            ],
        )
        reports = [
            {
                "project": "东莞",
                "captured_at": "2026-03-24T10:00:00+08:00",
                "profile": {"profile_user_id": "u2", "nickname": "账号B"},
                "works": [
                    {
                        "title_copy": "作品B",
                        "cover_url": "https://img.example.com/b.jpg",
                        "like_count": 20,
                        "comment_count": 5,
                        "comment_count_is_lower_bound": True,
                    }
                ],
            }
        ]
        settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
        with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                sync_project_rankings_into_single_table(reports=reports, settings=settings)
        self.assertEqual(len(client.created), 2)
        comment_row = next(item for item in client.created if item["榜单类型"] == "评论排行")
        self.assertEqual(comment_row["单选"], "评论预览下限")
        self.assertNotIn("评论数口径", comment_row)

    def test_sync_project_rankings_into_single_table_falls_back_project_to_card_label(self) -> None:
        client = _FakeRankingClient(
            tables=[{"name": "小红书单条作品排行", "table_id": "tbl_data"}],
            records=[],
            fields=[
                {"field_name": "榜单键"},
                {"field_name": "榜单类型"},
                {"field_name": "排名"},
                {"field_name": "卡片标签"},
                {"field_name": "账号ID"},
                {"field_name": "账号"},
                {"field_name": "标题文案"},
                {"field_name": "点赞数"},
                {"field_name": "数据更新时间"},
                {"field_name": "排序值"},
                {"field_name": "榜单摘要"},
            ],
        )
        reports = [
            {
                "project": "东莞",
                "captured_at": "2026-03-24T10:00:00+08:00",
                "profile": {"profile_user_id": "u2", "nickname": "账号B"},
                "works": [
                    {
                        "title_copy": "作品B",
                        "cover_url": "https://img.example.com/b.jpg",
                        "like_count": 20,
                        "comment_count": 5,
                    }
                ],
            }
        ]
        settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
        with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                sync_project_rankings_into_single_table(reports=reports, settings=settings)
        self.assertEqual(len(client.created), 2)
        like_row = next(item for item in client.created if item["榜单类型"] == "点赞排行")
        self.assertEqual(like_row["卡片标签"], "东莞 · TOP1")
        self.assertNotIn("文本", like_row)

    def test_sync_project_rankings_into_single_table_creates_project_table_when_missing(self) -> None:
        first_client = _FakeRankingClient(
            tables=[],
            records=[],
            fields=[],
        )
        second_client = _FakeRankingClient(
            tables=[],
            records=[],
            fields=[{"field_name": "榜单键"}, {"field_name": "榜单类型"}, {"field_name": "数据更新时间"}],
        )
        reports = [
            {
                "project": "东莞",
                "captured_at": "2026-03-24T10:00:00+08:00",
                "profile": {"profile_user_id": "u2", "nickname": "账号B"},
                "works": [{"title_copy": "作品B", "cover_url": "https://img.example.com/b.jpg", "like_count": 20, "comment_count": 5}],
            }
        ]
        settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
        with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[first_client, second_client]):
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                summary = sync_project_rankings_into_single_table(reports=reports, settings=settings)
        self.assertEqual(summary["table_name"], "项目作品排行榜")
        self.assertEqual(first_client.created_table_name, "项目作品排行榜")

    def test_build_record_state_index_can_limit_fields(self) -> None:
        client = _FakeClient(
            records=[
                {"record_id": "rec_1", "fields": {"账号ID": "u1", "账号": "账号A", "多余字段": "x"}},
            ]
        )
        state_index = build_record_state_index(client, unique_field="账号ID", field_names=["账号"])
        self.assertEqual(state_index["u1"]["fields"], {"账号ID": "u1", "账号": "账号A"})

    def test_sync_project_rankings_into_single_table_skips_forbidden_updates(self) -> None:
        fingerprint = build_work_fingerprint(
            profile_user_id="u2",
            title="新标题",
            cover_url="https://img.example.com/b.jpg",
        )
        client = _FakeRankingClient(
            tables=[{"name": "小红书单条作品排行", "table_id": "tbl_data"}],
            records=[
                {
                    "record_id": "rec_1",
                    "fields": {
                        "榜单键": f"东莞|单条点赞排行|{fingerprint}",
                        "榜单类型": "点赞排行",
                        "文本": "东莞",
                        "标题文案": "旧标题",
                    },
                }
            ],
        )
        client.raise_on_update = RuntimeError("403 Client Error: Forbidden for url: https://open.feishu.cn/x")
        reports = [
            {
                "project": "东莞",
                "captured_at": "2026-03-24T10:00:00+08:00",
                "profile": {"profile_user_id": "u2", "nickname": "账号B"},
                "works": [
                    {
                        "title_copy": "新标题",
                        "cover_url": "https://img.example.com/b.jpg",
                        "like_count": 20,
                        "comment_count": 5,
                    }
                ],
            }
        ]
        settings = SimpleNamespace(feishu_ranking_bitable_app_token="", feishu_bitable_app_token="token")
        with patch("xhs_feishu_monitor.profile_batch_to_feishu.FeishuBitableClient", side_effect=[client, client]):
            with patch("xhs_feishu_monitor.profile_batch_to_feishu.replace", side_effect=lambda value, **kwargs: value):
                summary = sync_project_rankings_into_single_table(reports=reports, settings=settings)
        self.assertGreaterEqual(summary["single_work_ranking_skipped"], 1)

    def test_is_feishu_forbidden_error(self) -> None:
        self.assertTrue(is_feishu_forbidden_error(RuntimeError("403 Client Error: Forbidden for url: https://open.feishu.cn")))
        self.assertFalse(is_feishu_forbidden_error(RuntimeError("500 Server Error")))


class _FakeClient:
    def __init__(self, *, records: list[dict]) -> None:
        self.records = records
        self.updated: list[tuple[str, dict]] = []
        self.created: list[dict] = []
        self.last_field_names = None

    def list_records(self, *, page_size: int = 100, field_names=None):  # noqa: ANN001
        self.last_field_names = field_names
        if not field_names:
            return self.records
        filtered = []
        for record in self.records:
            fields = record.get("fields") or {}
            filtered_fields = {key: value for key, value in fields.items() if key in field_names}
            filtered.append({"record_id": record.get("record_id"), "fields": filtered_fields})
        return filtered

    def update_record(self, record_id: str, fields: dict) -> None:
        self.updated.append((record_id, fields))

    def create_record(self, fields: dict) -> str:
        self.created.append(fields)
        return f"rec_{len(self.created) + 1}"


class _FakeRankingClient(_FakeClient):
    def __init__(self, *, tables: list[dict], records: list[dict], fields: list[dict] | None = None) -> None:
        super().__init__(records=records)
        self.tables = tables
        self.fields = fields or []
        self.deleted: list[str] = []
        self.raise_on_update: Exception | None = None
        self.created_table_name: str = ""

    def list_tables(self):  # noqa: ANN201
        return self.tables

    def list_fields(self):  # noqa: ANN201
        return self.fields

    def create_table(self, *, table_name: str, default_view_name: str = "", fields=None):  # noqa: ANN001, ANN201
        self.created_table_name = table_name
        self.tables.append({"name": table_name, "table_id": "tbl_created"})
        if fields:
            self.fields = [{"field_name": str(item.get("field_name") or "")} for item in fields]
        return {"table_id": "tbl_created"}

    def ensure_fields(self, field_specs):  # noqa: ANN001, ANN201
        existing = {str(item.get("field_name") or "").strip(): item for item in self.fields}
        for spec in field_specs:
            field_name = str(spec.get("field_name") or "").strip()
            if field_name and field_name not in existing:
                existing[field_name] = {"field_name": field_name}
        self.fields = list(existing.values())
        return existing

    def update_record(self, record_id: str, fields: dict) -> None:
        if self.raise_on_update is not None:
            raise self.raise_on_update
        super().update_record(record_id, fields)

    def delete_record(self, record_id: str) -> None:
        self.deleted.append(record_id)


if __name__ == "__main__":
    unittest.main()
