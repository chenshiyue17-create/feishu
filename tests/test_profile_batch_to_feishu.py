from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from xhs_feishu_monitor.profile_batch_to_feishu import (
    build_dry_run_summary,
    build_batch_sync_program_arguments,
    build_record_id_index,
    build_record_state_index,
    merge_report_with_existing_work_details,
    load_reports_from_json,
    normalize_batch_item_to_report,
    normalize_unique_value,
    resolve_launchd_paths,
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
                },
            }
        }
        merged = merge_report_with_existing_work_details(report=report, works_records=works_records)
        self.assertEqual(merged["works"][0]["note_url"], "https://www.xiaohongshu.com/explore/abc123")
        self.assertEqual(merged["works"][0]["note_id"], "abc123")
        self.assertEqual(merged["works"][0]["comment_count"], 18)
        self.assertEqual(merged["works"][0]["comment_count_text"], "18")

    def test_resolve_launchd_paths(self) -> None:
        paths = resolve_launchd_paths(label="com.cc.test-profile-batch-sync")
        self.assertTrue(paths["plist_path"].endswith("com.cc.test-profile-batch-sync.plist"))
        self.assertTrue(paths["stdout_log_path"].endswith("com.cc.test-profile-batch-sync.out.log"))
        self.assertTrue(paths["stderr_log_path"].endswith("com.cc.test-profile-batch-sync.err.log"))


class _FakeClient:
    def __init__(self, *, records: list[dict]) -> None:
        self.records = records
        self.updated: list[tuple[str, dict]] = []
        self.created: list[dict] = []

    def list_records(self, *, page_size: int = 100, field_names=None):  # noqa: ANN001
        return self.records

    def update_record(self, record_id: str, fields: dict) -> None:
        self.updated.append((record_id, fields))

    def create_record(self, fields: dict) -> str:
        self.created.append(fields)
        return f"rec_{len(self.created) + 1}"


if __name__ == "__main__":
    unittest.main()
