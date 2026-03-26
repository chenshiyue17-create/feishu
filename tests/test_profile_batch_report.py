from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.profile_batch_report import (
    build_batch_throttle,
    build_project_batches,
    build_batch_program_arguments,
    collect_profile_reports,
    collect_profile_reports_with_progress,
    extract_profile_urls,
    is_retryable_batch_error,
    is_slow_tail_retry_error,
    load_url_entries_file,
    load_urls_file,
    normalize_profile_url,
    normalize_profile_url_entries,
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

    def test_load_url_entries_file_preserves_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "urls.txt"
            path.write_text(
                "项目A\thttps://www.xiaohongshu.com/user/profile/aaa111?xsec_token=t1\n"
                "项目B\thttps://www.xiaohongshu.com/user/profile/bbb222?xsec_token=t2\n",
                encoding="utf-8",
            )
            entries = load_url_entries_file(str(path))
        self.assertEqual(entries[0]["project"], "项目A")
        self.assertTrue(entries[1]["url"].startswith("https://www.xiaohongshu.com/user/profile/bbb222"))

    def test_normalize_profile_url_entries_deduplicates_and_keeps_project(self) -> None:
        entries = normalize_profile_url_entries(
            [],
            "",
            None,
        )
        self.assertEqual(entries, [])
        entries = normalize_profile_url_entries(
            ["https://www.xiaohongshu.com/user/profile/aaa111?xsec_token=t"],
            "",
            None,
        )
        self.assertEqual(entries[0]["project"], "")
        self.assertEqual(entries[0]["url"], "https://www.xiaohongshu.com/user/profile/aaa111")

    def test_build_project_batches_preserves_project_order(self) -> None:
        batches = build_project_batches(
            [
                {"url": "https://www.xiaohongshu.com/user/profile/u1", "project": "项目A"},
                {"url": "https://www.xiaohongshu.com/user/profile/u2", "project": "项目B"},
                {"url": "https://www.xiaohongshu.com/user/profile/u3", "project": "项目A"},
            ]
        )
        self.assertEqual([item["url"] for item in batches[0]], ["https://www.xiaohongshu.com/user/profile/u1", "https://www.xiaohongshu.com/user/profile/u3"])
        self.assertEqual([item["url"] for item in batches[1]], ["https://www.xiaohongshu.com/user/profile/u2"])

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
        self.assertEqual(resolve_batch_concurrency(SimpleNamespace(xhs_fetch_mode="requests", xhs_batch_concurrency=6)), 2)

    def test_resolve_batch_concurrency_expands_with_proxy_pool(self) -> None:
        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=6,
            xhs_proxy_pool=["http://1", "http://2", "http://3"],
        )
        self.assertEqual(resolve_batch_concurrency(settings), 3)

    def test_build_batch_throttle_waits_between_requests(self) -> None:
        throttle = build_batch_throttle(
            SimpleNamespace(
                xhs_batch_request_interval_seconds=2.0,
                xhs_batch_account_delay_seconds=0.0,
                xhs_batch_account_jitter_seconds=0.0,
                xhs_batch_chunk_size=0,
                xhs_batch_chunk_cooldown_seconds=0.0,
            )
        )
        with patch("xhs_feishu_monitor.profile_batch_report.time.time", side_effect=[100.0, 100.1]):
            with patch("xhs_feishu_monitor.profile_batch_report.time.sleep") as sleep_mock:
                throttle.wait()
                throttle.wait()
        sleep_mock.assert_called_once()
        self.assertAlmostEqual(sleep_mock.call_args.args[0], 1.9, places=2)

    def test_build_batch_throttle_adds_account_delay_and_jitter(self) -> None:
        throttle = build_batch_throttle(
            SimpleNamespace(
                xhs_batch_request_interval_seconds=0.0,
                xhs_batch_account_delay_seconds=1.0,
                xhs_batch_account_jitter_seconds=0.5,
                xhs_batch_chunk_size=0,
                xhs_batch_chunk_cooldown_seconds=0.0,
            )
        )
        with patch("xhs_feishu_monitor.profile_batch_report.time.time", side_effect=[100.0, 100.1]):
            with patch("xhs_feishu_monitor.profile_batch_report.random.uniform", side_effect=[0.5, 0.5]):
                with patch("xhs_feishu_monitor.profile_batch_report.time.sleep") as sleep_mock:
                    throttle.wait()
                    throttle.wait()
        sleep_mock.assert_called_once()
        self.assertAlmostEqual(sleep_mock.call_args.args[0], 1.4, places=2)

    def test_is_retryable_batch_error_filters_login_failures(self) -> None:
        self.assertTrue(is_retryable_batch_error("请求超时"))
        self.assertTrue(is_retryable_batch_error("429 too many requests"))
        self.assertFalse(is_retryable_batch_error("命中登录页，当前登录态不可用"))

    def test_is_slow_tail_retry_error_identifies_risky_failures(self) -> None:
        self.assertTrue(is_slow_tail_retry_error("429 too many requests"))
        self.assertTrue(is_slow_tail_retry_error("风控拦截"))
        self.assertFalse(is_slow_tail_retry_error("请求超时"))

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

        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=1,
            xhs_batch_request_interval_seconds=0.0,
            xhs_batch_account_delay_seconds=0.0,
            xhs_batch_account_jitter_seconds=0.0,
            xhs_batch_chunk_size=0,
            xhs_batch_chunk_cooldown_seconds=0.0,
            xhs_batch_retry_failed_once=False,
            xhs_batch_retry_delay_seconds=0.0,
            xhs_batch_project_cooldown_seconds=0.0,
        )
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

    def test_collect_profile_reports_preserves_project_on_results(self) -> None:
        def fake_collect_single_profile_report(*, url, settings):
            return {
                "status": "success",
                "requested_url": url,
                "profile": {"nickname": f"账号-{url[-2:]}"},
                "works": [],
            }

        settings = SimpleNamespace(xhs_fetch_mode="requests", xhs_batch_concurrency=1)
        url_entries = [
            {"url": "https://www.xiaohongshu.com/user/profile/u1", "project": "项目A"},
            {"url": "https://www.xiaohongshu.com/user/profile/u2", "project": "项目B"},
        ]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            results = collect_profile_reports_with_progress(
                urls=[item["url"] for item in url_entries],
                settings=settings,
                url_entries=url_entries,
            )
        self.assertEqual([item["project"] for item in results], ["项目A", "项目B"])

    def test_collect_profile_reports_waits_between_projects(self) -> None:
        def fake_collect_single_profile_report(*, url, settings):
            return {"status": "success", "requested_url": url, "profile": {}, "works": []}

        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=1,
            xhs_batch_request_interval_seconds=0.0,
            xhs_batch_account_delay_seconds=0.0,
            xhs_batch_account_jitter_seconds=0.0,
            xhs_batch_chunk_size=0,
            xhs_batch_chunk_cooldown_seconds=0.0,
            xhs_batch_retry_failed_once=False,
            xhs_batch_retry_delay_seconds=0.0,
            xhs_batch_project_cooldown_seconds=5.0,
        )
        url_entries = [
            {"url": "https://www.xiaohongshu.com/user/profile/u1", "project": "项目A"},
            {"url": "https://www.xiaohongshu.com/user/profile/u2", "project": "项目B"},
        ]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            with patch("xhs_feishu_monitor.profile_batch_report.time.sleep") as sleep_mock:
                collect_profile_reports(
                    urls=[item["url"] for item in url_entries],
                    settings=settings,
                )
                from xhs_feishu_monitor.profile_batch_report import collect_profile_reports_with_progress

                collect_profile_reports_with_progress(
                    urls=[item["url"] for item in url_entries],
                    url_entries=url_entries,
                    settings=settings,
                )
        self.assertTrue(any(call.args[0] == 5.0 for call in sleep_mock.call_args_list))

    def test_collect_profile_reports_retries_transient_failures_once(self) -> None:
        call_counts = {}

        def fake_collect_single_profile_report(*, url, settings):
            count = call_counts.get(url, 0) + 1
            call_counts[url] = count
            if count == 1:
                return {"status": "failed", "requested_url": url, "error": "请求超时"}
            return {"status": "success", "requested_url": url, "profile": {"nickname": "账号A"}, "works": []}

        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=1,
            xhs_batch_request_interval_seconds=0.0,
            xhs_batch_account_delay_seconds=0.0,
            xhs_batch_account_jitter_seconds=0.0,
            xhs_batch_chunk_size=0,
            xhs_batch_chunk_cooldown_seconds=0.0,
            xhs_batch_retry_failed_once=True,
            xhs_batch_retry_delay_seconds=0.0,
            xhs_batch_risk_retry_delay_seconds=0.0,
        )
        urls = ["https://www.xiaohongshu.com/user/profile/u1"]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            results = collect_profile_reports(urls=urls, settings=settings)
        self.assertEqual(call_counts[urls[0]], 2)
        self.assertEqual(results[0]["status"], "success")
        self.assertTrue(results[0]["retried"])

    def test_collect_profile_reports_does_not_retry_login_failures(self) -> None:
        call_counts = {}

        def fake_collect_single_profile_report(*, url, settings):
            call_counts[url] = call_counts.get(url, 0) + 1
            return {"status": "failed", "requested_url": url, "error": "命中登录页，当前登录态不可用"}

        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=1,
            xhs_batch_request_interval_seconds=0.0,
            xhs_batch_account_delay_seconds=0.0,
            xhs_batch_account_jitter_seconds=0.0,
            xhs_batch_chunk_size=0,
            xhs_batch_chunk_cooldown_seconds=0.0,
            xhs_batch_retry_failed_once=True,
            xhs_batch_retry_delay_seconds=0.0,
            xhs_batch_risk_retry_delay_seconds=0.0,
        )
        urls = ["https://www.xiaohongshu.com/user/profile/u1"]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            results = collect_profile_reports(urls=urls, settings=settings)
        self.assertEqual(call_counts[urls[0]], 1)
        self.assertEqual(results[0]["status"], "failed")

    def test_collect_profile_reports_uses_longer_delay_for_risky_retry(self) -> None:
        call_counts = {}

        def fake_collect_single_profile_report(*, url, settings):
            count = call_counts.get(url, 0) + 1
            call_counts[url] = count
            if count == 1:
                return {"status": "failed", "requested_url": url, "error": "429 too many requests"}
            return {"status": "success", "requested_url": url, "profile": {"nickname": "账号A"}, "works": []}

        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            xhs_batch_concurrency=1,
            xhs_batch_request_interval_seconds=0.0,
            xhs_batch_account_delay_seconds=0.0,
            xhs_batch_account_jitter_seconds=0.0,
            xhs_batch_chunk_size=0,
            xhs_batch_chunk_cooldown_seconds=0.0,
            xhs_batch_retry_failed_once=True,
            xhs_batch_retry_delay_seconds=5.0,
            xhs_batch_risk_retry_delay_seconds=30.0,
        )
        urls = ["https://www.xiaohongshu.com/user/profile/u1"]
        with patch("xhs_feishu_monitor.profile_batch_report._collect_single_profile_report", side_effect=fake_collect_single_profile_report):
            with patch("xhs_feishu_monitor.profile_batch_report.time.sleep") as sleep_mock:
                results = collect_profile_reports(urls=urls, settings=settings)
        self.assertEqual(call_counts[urls[0]], 2)
        self.assertTrue(any(call.args[0] == 30.0 for call in sleep_mock.call_args_list))
        self.assertEqual(results[0]["retry_tier"], "slow_tail")
        self.assertEqual(results[0]["retry_delay_seconds"], 30.0)


if __name__ == "__main__":
    unittest.main()
