from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.profile_batch_collect import main


class ProfileBatchCollectTest(unittest.TestCase):
    def test_main_collects_without_feishu_sync_loader(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("PROJECT_CACHE_DIR=/tmp/cache\n", encoding="utf-8")
            urls_path.write_text("默认项目\thttps://www.xiaohongshu.com/user/profile/u1\n", encoding="utf-8")

            fake_reports = [
                {
                    "status": "success",
                    "project": "默认项目",
                    "profile": {"profile_user_id": "u1"},
                    "works": [{"id": "w1"}],
                }
            ]

            with patch("xhs_feishu_monitor.profile_batch_collect.load_settings", return_value=SimpleNamespace()), \
                 patch("xhs_feishu_monitor.profile_batch_collect.normalize_profile_url_entries", return_value=[{"url": "https://www.xiaohongshu.com/user/profile/u1", "project": "默认项目"}]), \
                 patch("xhs_feishu_monitor.profile_batch_collect.collect_profile_reports_with_progress", return_value=fake_reports), \
                 patch("xhs_feishu_monitor.profile_batch_collect.write_project_cache_bundle", return_value={"cache_dir": "/tmp/cache", "projects": {"默认项目": "/tmp/cache/默认项目"}}), \
                 patch("xhs_feishu_monitor.profile_batch_collect.load_reports_for_sync", side_effect=AssertionError("should not be called"), create=True):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = main(["--env-file", str(env_path), "--urls-file", str(urls_path)])

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["successful_accounts"], 1)
        self.assertEqual(payload["total_works"], 1)

    def test_main_prints_failed_reasons_when_all_collects_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("PROJECT_CACHE_DIR=/tmp/cache\n", encoding="utf-8")
            urls_path.write_text("默认项目\thttps://www.xiaohongshu.com/user/profile/u1\n", encoding="utf-8")

            failed_reports = [
                {
                    "status": "error",
                    "requested_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "profile": {"nickname": "账号A"},
                    "error": "cookie invalid",
                }
            ]

            with patch("xhs_feishu_monitor.profile_batch_collect.load_settings", return_value=SimpleNamespace()), \
                 patch("xhs_feishu_monitor.profile_batch_collect.normalize_profile_url_entries", return_value=[{"url": "https://www.xiaohongshu.com/user/profile/u1", "project": "默认项目"}]), \
                 patch("xhs_feishu_monitor.profile_batch_collect.collect_profile_reports_with_progress", return_value=failed_reports):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    with self.assertRaisesRegex(ValueError, "未写入本地缓存"):
                        main(["--env-file", str(env_path), "--urls-file", str(urls_path)])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["errors"][0]["account"], "账号A")
        self.assertEqual(payload["errors"][0]["error"], "cookie invalid")
