from __future__ import annotations

import plistlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.local_daily_sync import (
    build_local_daily_sync_program_arguments,
    install_local_daily_sync_launchd,
    run_local_daily_sync,
    uninstall_local_daily_sync_launchd,
)
from xhs_feishu_monitor.local_daily_sync_status import load_local_daily_sync_status


SHANGHAI_TZ = timezone(timedelta(hours=8))


class LocalDailySyncTest(unittest.TestCase):
    def test_build_local_daily_sync_program_arguments(self) -> None:
        argv = build_local_daily_sync_program_arguments(
            env_file="/tmp/.env",
            urls_file="/tmp/urls.txt",
        )
        self.assertIn("-m", argv)
        self.assertIn("xhs_feishu_monitor.local_daily_sync", argv)
        self.assertIn(str(Path("/tmp/.env").resolve()), argv)
        self.assertIn(str(Path("/tmp/urls.txt").resolve()), argv)

    def test_install_local_daily_sync_launchd_updates_env_driver(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("PROJECT_CACHE_DIR=/tmp/cache\n", encoding="utf-8")
            urls_path.write_text("默认项目\thttps://www.xiaohongshu.com/user/profile/u1\n", encoding="utf-8")
            captured: dict[str, object] = {}

            def _capture_install(**kwargs):
                captured.update(kwargs)

            with patch(
                "xhs_feishu_monitor.local_daily_sync.load_settings",
                return_value=SimpleNamespace(xhs_batch_window_start="14:00"),
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.install_launch_agent",
                side_effect=_capture_install,
            ):
                paths = install_local_daily_sync_launchd(
                    env_file=str(env_path),
                    urls_file=str(urls_path),
                    label="com.cc.test-local-daily-sync",
                    load_after_install=False,
                )

            plist_payload = plistlib.loads(captured["plist_bytes"])
            self.assertEqual(plist_payload["Label"], "com.cc.test-local-daily-sync")
            self.assertEqual(plist_payload["StartCalendarInterval"]["Hour"], 14)
            self.assertEqual(plist_payload["StartCalendarInterval"]["Minute"], 0)
            self.assertIn("xhs_feishu_monitor.local_daily_sync", plist_payload["ProgramArguments"][2])
            self.assertEqual(captured["label"], "com.cc.test-local-daily-sync")
            self.assertEqual(captured["plist_path"], paths["plist_path"])
            self.assertIn("XHS_SCHEDULE_DRIVER=launchd", env_path.read_text(encoding="utf-8"))

    def test_uninstall_local_daily_sync_launchd_restores_app_driver(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("XHS_SCHEDULE_DRIVER=launchd\n", encoding="utf-8")

            with patch("xhs_feishu_monitor.local_daily_sync.unload_launch_agent") as unload_mock:
                plist_path = uninstall_local_daily_sync_launchd(
                    env_file=str(env_path),
                    label="com.cc.test-local-daily-sync",
                    plist_path=str(Path(temp_dir) / "test.plist"),
                )

            unload_mock.assert_called_once_with(plist_path=plist_path)
            self.assertIn("XHS_SCHEDULE_DRIVER=app", env_path.read_text(encoding="utf-8"))

    def test_run_local_daily_sync_collects_all_projects_then_pushes(self) -> None:
        now = datetime(2026, 3, 31, 14, 0, tzinfo=SHANGHAI_TZ)
        plan = {
            "东莞": {
                "urls": ["https://www.xiaohongshu.com/user/profile/u2"],
                "scheduled_at": now,
            },
            "默认项目": {
                "urls": ["https://www.xiaohongshu.com/user/profile/u1"],
                "scheduled_at": now + timedelta(seconds=30),
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            state_path = Path(temp_dir) / ".state.json"
            env_path.write_text(f"STATE_FILE={state_path}\n", encoding="utf-8")

            with patch(
                "xhs_feishu_monitor.local_daily_sync.load_settings",
                return_value=SimpleNamespace(state_file=str(state_path)),
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.parse_monitored_entries",
                return_value=[{"project": "默认项目", "url": "u1", "active": True}],
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.build_auto_project_schedule",
                return_value=plan,
            ), patch(
                "xhs_feishu_monitor.local_daily_sync._sleep_until"
            ) as sleep_mock, patch(
                "xhs_feishu_monitor.local_daily_sync.wait_for_xiaohongshu_login",
                return_value={"state": "ok", "message": "ok"},
            ) as wait_mock, patch(
                "xhs_feishu_monitor.local_daily_sync.login_state_requires_interactive_login",
                return_value=False,
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.collect_profiles_to_local_cache",
                side_effect=[
                    {"status": "success", "successful_accounts": 1},
                    {"status": "success", "successful_accounts": 2},
                ],
            ) as collect_mock, patch(
                "xhs_feishu_monitor.local_daily_sync.push_current_cache_to_server",
                return_value={"ok": True, "account_count": 3},
            ) as push_mock:
                result = run_local_daily_sync(env_file=str(env_path), urls_file="/tmp/urls.txt")
            persisted = load_local_daily_sync_status(
                env_file=str(env_path),
                state_file_path=str(state_path),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["project_count"], 2)
        self.assertEqual(len(result["projects"]), 2)
        self.assertEqual(sleep_mock.call_count, 2)
        self.assertEqual(wait_mock.call_count, 2)
        self.assertEqual(wait_mock.call_args.kwargs["timeout_seconds"], 0)
        self.assertEqual(collect_mock.call_count, 2)
        push_mock.assert_called_once_with(env_file=str(env_path), urls_file="/tmp/urls.txt")
        self.assertEqual(persisted["state"], "success")
        self.assertTrue(persisted["last_success_at"])
        self.assertEqual(persisted["upload_state"], "success")

    def test_run_local_daily_sync_skips_upload_when_any_project_fails(self) -> None:
        now = datetime(2026, 3, 31, 14, 0, tzinfo=SHANGHAI_TZ)
        plan = {
            "默认项目": {
                "urls": ["https://www.xiaohongshu.com/user/profile/u1"],
                "scheduled_at": now,
            },
            "东莞": {
                "urls": ["https://www.xiaohongshu.com/user/profile/u2"],
                "scheduled_at": now + timedelta(seconds=10),
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            state_path = Path(temp_dir) / ".state.json"
            env_path.write_text(f"STATE_FILE={state_path}\n", encoding="utf-8")

            with patch(
                "xhs_feishu_monitor.local_daily_sync.load_settings",
                return_value=SimpleNamespace(state_file=str(state_path)),
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.parse_monitored_entries",
                return_value=[{"project": "默认项目", "url": "u1", "active": True}],
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.build_auto_project_schedule",
                return_value=plan,
            ), patch(
                "xhs_feishu_monitor.local_daily_sync._sleep_until"
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.wait_for_xiaohongshu_login",
                return_value={"state": "ok", "message": "ok"},
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.login_state_requires_interactive_login",
                return_value=False,
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.collect_profiles_to_local_cache",
                side_effect=[
                    {"status": "success", "successful_accounts": 1},
                    RuntimeError("东莞采集失败"),
                ],
            ), patch(
                "xhs_feishu_monitor.local_daily_sync.push_current_cache_to_server"
            ) as push_mock:
                result = run_local_daily_sync(env_file=str(env_path), urls_file="/tmp/urls.txt")
            persisted = load_local_daily_sync_status(
                env_file=str(env_path),
                state_file_path=str(state_path),
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["successful_projects"], 1)
        self.assertEqual(result["failed_projects"], 1)
        self.assertIn("东莞采集失败", result["failures"][0]["error"])
        push_mock.assert_not_called()
        self.assertEqual(persisted["state"], "partial")
        self.assertEqual(persisted["upload_state"], "skipped")


if __name__ == "__main__":
    unittest.main()
