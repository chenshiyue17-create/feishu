from __future__ import annotations

import unittest

from xhs_feishu_monitor.profile_live_sync import (
    build_live_sync_program_arguments,
    parse_daily_time,
    resolve_launchd_paths,
)


class ProfileLiveSyncTest(unittest.TestCase):
    def test_build_live_sync_program_arguments(self) -> None:
        argv = build_live_sync_program_arguments(
            url="https://www.xiaohongshu.com/user/profile/u1",
            env_file="xhs_feishu_monitor/.env",
            profile_table_name="账号总览表",
            works_table_name="作品明细表",
            ensure_fields=True,
            sync_dashboard=True,
        )
        self.assertEqual(argv[1:3], ["-m", "xhs_feishu_monitor.profile_live_sync"])
        self.assertIn("--ensure-fields", argv)
        self.assertIn("--profile-table-name", argv)
        self.assertIn("--works-table-name", argv)
        self.assertIn("--sync-dashboard", argv)

    def test_resolve_launchd_paths(self) -> None:
        paths = resolve_launchd_paths(label="com.cc.test-profile-live-sync")
        self.assertTrue(paths["plist_path"].endswith("com.cc.test-profile-live-sync.plist"))
        self.assertTrue(paths["stdout_log_path"].endswith("com.cc.test-profile-live-sync.out.log"))
        self.assertTrue(paths["stderr_log_path"].endswith("com.cc.test-profile-live-sync.err.log"))

    def test_parse_daily_time(self) -> None:
        self.assertEqual(parse_daily_time("14:00"), {"Hour": 14, "Minute": 0})


if __name__ == "__main__":
    unittest.main()
