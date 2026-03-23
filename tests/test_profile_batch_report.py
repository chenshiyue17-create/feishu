from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.profile_batch_report import (
    build_batch_program_arguments,
    collect_profile_reports,
    extract_profile_urls,
    load_urls_file,
    normalize_profile_url,
    normalize_profile_urls,
    resolve_batch_concurrency,
)


class ProfileBatchReportTest(unittest.TestCase):
    def test_extract_profile_urls_from_pasted_text(self) -> None:
        raw = (
            "https://www.xiaohongshu.com/user/profile/aaa111?xsec_token=token1&xsec_source=pc_search"
            "xiaohongshu.com/user/profile/bbb222?xsec_token=token2&xsec_source=pc_comment "
            "https://www.xiaohongshu.com/user/profile/ccc333?xsec_token=token3&xsec_source=pc_feed"
        )
        urls = extract_profile_urls(raw)
        self.assertEqual(len(urls), 3)
        self.assertTrue(urls[1].startswith("xiaohongshu.com/user/profile/bbb222"))

    def test_normalize_profile_url(self) -> None:
        self.assertEqual(
            normalize_profile_url("xiaohongshu.com/user/profile/aaa111?xsec_token=t&xsec_source=pc_search"),
            "https://www.xiaohongshu.com/user/profile/aaa111",
        )

    def test_normalize_profile_urls_deduplicates(self) -> None:
        urls = normalize_profile_urls(
            ["https://www.xiaohongshu.com/user/profile/aaa111?xsec_token=t&xsec_source=pc_search"],
            "https://www.xiaohongshu.com/user/profile/aaa111?xsec_token=another&xsec_source=app_share",
        )
        self.assertEqual(len(urls), 1)
        self.assertEqual(urls[0], "https://www.xiaohongshu.com/user/profile/aaa111")

    def test_load_urls_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "urls.txt"
            path.write_text(
                "# comment\n"
                "项目A\thttps://www.xiaohongshu.com/user/profile/aaa111?xsec_token=t1&xsec_source=pc_search\n"
                "\n"
                "# PAUSED 项目B\thttps://www.xiaohongshu.com/user/profile/paused000?xsec_token=tp&xsec_source=pc_search\n"
                "项目B\thttps://www.xiaohongshu.com/user/profile/bbb222?xsec_token=t2&xsec_source=pc_search\n",
                encoding="utf-8",
            )
            urls = load_urls_file(str(path))
        self.assertEqual(len(urls), 2)
        self.assertTrue(urls[0].startswith("https://www.xiaohongshu.com/user/profile/aaa111"))

    def test_build_batch_program_arguments(self) -> None:
        argv = build_batch_program_arguments(
            urls=["https://www.xiaohongshu.com/user/profile/aaa111?xsec_token=t&xsec_source=pc_search"],
            urls_file="xhs_feishu_monitor/input/robam_multi_profile_urls.txt",
            raw_text="",
            env_file="xhs_feishu_monitor/.env",
            json_out="xhs_feishu_monitor/output/robam_multi_profile_report.json",
            csv_out="xhs_feishu_monitor/output/robam_multi_profile_report.csv",
        )
        self.assertIn("xhs_feishu_monitor.profile_batch_report", argv)
        self.assertIn("--urls-file", argv)
        self.assertIn("--json-out", argv)
        self.assertIn("--csv-out", argv)

    def test_resolve_batch_concurrency_disables_parallel_for_browser_modes(self) -> None:
        self.assertEqual(resolve_batch_concurrency(SimpleNamespace(xhs_fetch_mode="local_browser", xhs_batch_concurrency=6)), 1)
        self.assertEqual(resolve_batch_concurrency(SimpleNamespace(xhs_fetch_mode="playwright", xhs_batch_concurrency=6)), 1)
        self.assertEqual(resolve_batch_concurrency(SimpleNamespace(xhs_fetch_mode="requests", xhs_batch_concurrency=6)), 6)

    def test_collect_profile_reports_keeps_input_order_under_parallel_mode(self) -> None:
        def fake_collect_single_profile_report(*, url, settings):
            if url.endswith("u1"):
                time.sleep(0.03)
            return {"status": "success", "requested_url": url, "profile": {}, "works": []}

        settings = SimpleNamespace(xhs_fetch_mode="requests", xhs_batch_concurrency=4)
        urls = ["https://www.xiaohongshu.com/user/profile/u1", "https://www.xiaohongshu.com/user/profile/u2"]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            results = collect_profile_reports(urls=urls, settings=settings)
        self.assertEqual([item["requested_url"] for item in results], urls)

    def test_collect_profile_reports_reports_progress(self) -> None:
        progress_events = []

        def fake_collect_single_profile_report(*, url, settings):
            return {
                "status": "success",
                "requested_url": url,
                "profile": {"nickname": f"账号-{url[-2:]}"},
                "works": [{"title_copy": "作品A"}],
            }

        settings = SimpleNamespace(xhs_fetch_mode="requests", xhs_batch_concurrency=1)
        urls = ["https://www.xiaohongshu.com/user/profile/u1", "https://www.xiaohongshu.com/user/profile/u2"]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            collect_profile_reports(
                urls=urls,
                settings=settings,
                progress_callback=progress_events.append,
            )
        self.assertEqual([event["current"] for event in progress_events], [1, 2])
        self.assertTrue(all(event["phase"] == "collect" for event in progress_events))
        self.assertEqual(progress_events[-1]["success_count"], 2)
        self.assertEqual(progress_events[-1]["failed_count"], 0)


if __name__ == "__main__":
    unittest.main()
