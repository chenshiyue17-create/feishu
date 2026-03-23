from __future__ import annotations

import plistlib
import unittest
from pathlib import Path

from xhs_feishu_monitor.cli import _build_parser, summarize_field_mapping
from xhs_feishu_monitor.launchd import (
    build_launch_environment,
    build_launch_agent_plist,
    build_sync_program_arguments,
    wrap_program_arguments_for_login_shell,
)


class LaunchdTest(unittest.TestCase):
    def test_summarize_field_mapping(self) -> None:
        summary = summarize_field_mapping(
            fields=[
                {"field_name": "笔记ID", "is_hidden": False},
                {"field_name": "标题", "is_hidden": True},
                {"field_name": "链接", "is_hidden": False},
            ],
            expected_field_names=["笔记ID", "标题", "作者"],
        )
        self.assertEqual(summary["existing"], ["标题", "笔记ID"])
        self.assertEqual(summary["missing"], ["作者"])
        self.assertEqual(summary["hidden"], ["标题"])

    def test_parser_accepts_check_arguments(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "--targets",
                "targets.json",
                "--check",
                "--skip-feishu-check",
                "--check-limit",
                "2",
            ]
        )
        self.assertTrue(args.check)
        self.assertTrue(args.skip_feishu_check)
        self.assertEqual(args.check_limit, 2)

    def test_build_sync_program_arguments(self) -> None:
        targets_path = str(Path("/tmp/targets.json").resolve())
        env_file_path = str(Path("/tmp/.env").resolve())
        state_file_path = str(Path("/tmp/.state.json").resolve())
        argv = build_sync_program_arguments(
            targets_path=targets_path,
            env_file_path=env_file_path,
            state_file_path=state_file_path,
        )
        self.assertIn("-m", argv)
        self.assertIn("xhs_feishu_monitor", argv)
        self.assertIn(targets_path, argv)
        self.assertIn(env_file_path, argv)
        self.assertIn(state_file_path, argv)

    def test_build_launch_agent_plist(self) -> None:
        content = build_launch_agent_plist(
            label="com.cc.test",
            program_arguments=["/usr/bin/python3", "-m", "xhs_feishu_monitor"],
            working_directory="/Users/cc/Documents/New project",
            interval_seconds=1800,
            stdout_log_path="/tmp/test.out.log",
            stderr_log_path="/tmp/test.err.log",
            environment_variables={"PYTHONUNBUFFERED": "1"},
        )
        payload = plistlib.loads(content)
        self.assertEqual(payload["Label"], "com.cc.test")
        self.assertEqual(payload["StartInterval"], 1800)
        self.assertEqual(payload["ProgramArguments"][0], "/usr/bin/python3")
        self.assertEqual(payload["StandardOutPath"], "/tmp/test.out.log")
        self.assertEqual(payload["EnvironmentVariables"]["PYTHONUNBUFFERED"], "1")

    def test_build_launch_agent_plist_with_calendar_interval(self) -> None:
        content = build_launch_agent_plist(
            label="com.cc.test.daily",
            program_arguments=["/usr/bin/python3", "-m", "xhs_feishu_monitor.profile_live_sync"],
            working_directory="/Users/cc/Documents/New project",
            start_calendar_interval={"Hour": 14, "Minute": 0},
            stdout_log_path="/tmp/test.daily.out.log",
            stderr_log_path="/tmp/test.daily.err.log",
        )
        payload = plistlib.loads(content)
        self.assertEqual(payload["Label"], "com.cc.test.daily")
        self.assertEqual(payload["StartCalendarInterval"]["Hour"], 14)
        self.assertEqual(payload["StartCalendarInterval"]["Minute"], 0)
        self.assertNotIn("StartInterval", payload)

    def test_wrap_program_arguments_for_login_shell(self) -> None:
        argv = wrap_program_arguments_for_login_shell(
            program_arguments=["/usr/bin/python3", "-m", "xhs_feishu_monitor.profile_batch_report", "--env-file", "/tmp/.env"],
            working_directory="/Users/cc/Documents/New project",
        )
        self.assertEqual(argv[:2], ["/bin/zsh", "-lc"])
        self.assertIn("cd '/Users/cc/Documents/New project'", argv[2])
        self.assertIn("/usr/bin/python3 -m xhs_feishu_monitor.profile_batch_report", argv[2])

    def test_build_launch_environment(self) -> None:
        environment = build_launch_environment()
        self.assertEqual(environment["PYTHONUNBUFFERED"], "1")
        self.assertTrue(environment["HOME"])
        self.assertTrue(environment["PATH"])


if __name__ == "__main__":
    unittest.main()
