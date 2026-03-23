from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.chrome_cookies import DEFAULT_CHROME_PROFILE_ROOT
from xhs_feishu_monitor.models import NoteSnapshot
from xhs_feishu_monitor.profile_report import (
    _build_profile_fetch_setting_variants,
    _should_expand_profile_work_count,
    build_profile_report,
    enrich_profile_report_with_note_metrics,
    load_profile_report_payload,
)


class ProfileReportTest(unittest.TestCase):
    def test_build_profile_report(self) -> None:
        initial_state = {
            "user": {
                "userPageData": {
                    "basicInfo": {
                        "nickname": "老板电器-全国福利官🐙",
                        "redId": "94341532229",
                        "desc": "关注我了解最新老板电器活动",
                        "ipLocation": "上海",
                        "images": "https://example.com/avatar.jpg",
                        "gender": 1,
                    },
                    "interactions": [
                        {"type": "follows", "count": "10+"},
                        {"type": "fans", "count": "10+"},
                        {"type": "interaction", "count": "10+"},
                    ],
                    "tags": [{"tagType": "info"}],
                },
                "notes": [
                    [
                        {
                            "id": "note_001",
                            "xsecToken": "token_001",
                            "noteCard": {
                                "displayTitle": "7709-U2新品来袭",
                                "type": "video",
                                "xsecToken": "token_001",
                                "noteId": "note_001",
                                "user": {"userId": "68e9b6cc000000003702d55d"},
                                "interactInfo": {"likedCount": "9", "commentCount": "3"},
                                "cover": {"urlDefault": "https://example.com/cover.jpg"},
                            },
                        }
                    ]
                ],
            }
        }
        report = build_profile_report(
            initial_state=initial_state,
            profile_url="https://www.xiaohongshu.com/user/profile/68e9b6cc000000003702d55d",
        )
        self.assertEqual(report["profile"]["nickname"], "老板电器-全国福利官🐙")
        self.assertEqual(report["profile"]["red_id"], "94341532229")
        self.assertEqual(report["profile"]["visible_work_count"], 1)
        self.assertEqual(report["profile"]["total_work_count"], 1)
        self.assertEqual(report["profile"]["work_count_display_text"], "1")
        self.assertTrue(report["profile"]["work_count_exact"])
        self.assertEqual(report["works"][0]["title_copy"], "7709-U2新品来袭")
        self.assertEqual(report["works"][0]["like_count"], 9)
        self.assertEqual(report["works"][0]["comment_count"], 3)
        self.assertEqual(
            report["works"][0]["note_url"],
            "https://www.xiaohongshu.com/explore/note_001?xsec_token=token_001&xsec_source=pc_user",
        )

    def test_build_profile_report_marks_work_count_as_lower_bound_when_has_more(self) -> None:
        initial_state = {
            "user": {
                "userPageData": {
                    "basicInfo": {
                        "nickname": "测试账号",
                    },
                },
                "noteQueries": [{"num": 30, "hasMore": True}],
                "notes": [
                    [
                        {
                            "id": "note_001",
                            "xsecToken": "token_001",
                            "noteCard": {
                                "displayTitle": "作品A",
                                "type": "video",
                                "xsecToken": "token_001",
                                "noteId": "note_001",
                                "user": {"userId": "u1"},
                                "interactInfo": {"likedCount": "9"},
                                "cover": {"urlDefault": "https://example.com/cover.jpg"},
                            },
                        },
                        {
                            "id": "note_002",
                            "xsecToken": "token_002",
                            "noteCard": {
                                "displayTitle": "作品B",
                                "type": "normal",
                                "xsecToken": "token_002",
                                "noteId": "note_002",
                                "user": {"userId": "u1"},
                                "interactInfo": {"likedCount": "5"},
                                "cover": {"urlDefault": "https://example.com/cover2.jpg"},
                            },
                        },
                    ]
                ],
            }
        }
        report = build_profile_report(
            initial_state=initial_state,
            profile_url="https://www.xiaohongshu.com/user/profile/u1",
        )
        self.assertEqual(report["profile"]["visible_work_count"], 2)
        self.assertIsNone(report["profile"]["total_work_count"])
        self.assertEqual(report["profile"]["work_count_display_text"], "2+")
        self.assertFalse(report["profile"]["work_count_exact"])

    def test_build_profile_report_limits_works_to_first_30_items(self) -> None:
        notes = []
        for index in range(32):
            notes.append(
                {
                    "id": f"note_{index}",
                    "noteCard": {
                        "displayTitle": f"作品{index}",
                        "type": "normal",
                        "noteId": f"note_{index}",
                        "user": {"userId": "u1"},
                        "interactInfo": {"likedCount": str(index)},
                    },
                }
            )
        initial_state = {
            "user": {
                "userPageData": {"basicInfo": {"nickname": "测试账号"}},
                "noteQueries": [{"num": 30, "hasMore": True}],
                "notes": [notes],
            }
        }
        report = build_profile_report(
            initial_state=initial_state,
            profile_url="https://www.xiaohongshu.com/user/profile/u1",
        )
        self.assertEqual(report["profile"]["visible_work_count"], 30)
        self.assertEqual(report["profile"]["work_count_display_text"], "32+")
        self.assertEqual(len(report["works"]), 30)

    def test_build_profile_report_keeps_first_30_details_but_uses_exact_total_count(self) -> None:
        notes = []
        for index in range(45):
            notes.append(
                {
                    "id": f"note_{index}",
                    "noteCard": {
                        "displayTitle": f"作品{index}",
                        "type": "normal",
                        "noteId": f"note_{index}",
                        "user": {"userId": "u1"},
                        "interactInfo": {"likedCount": str(index)},
                    },
                }
            )
        initial_state = {
            "user": {
                "userPageData": {"basicInfo": {"nickname": "测试账号"}},
                "noteQueries": [{"num": 30, "hasMore": False}],
                "notes": [notes],
            }
        }
        report = build_profile_report(
            initial_state=initial_state,
            profile_url="https://www.xiaohongshu.com/user/profile/u1",
        )
        self.assertEqual(report["profile"]["visible_work_count"], 30)
        self.assertEqual(report["profile"]["total_work_count"], 45)
        self.assertEqual(report["profile"]["work_count_display_text"], "45")
        self.assertTrue(report["profile"]["work_count_exact"])
        self.assertEqual(len(report["works"]), 30)

    def test_should_expand_profile_work_count_only_for_lower_bound_reports(self) -> None:
        self.assertTrue(
            _should_expand_profile_work_count(
                {"profile": {"profile_user_id": "u1", "visible_work_count": 30, "work_count_exact": False}}
            )
        )
        self.assertFalse(
            _should_expand_profile_work_count(
                {"profile": {"profile_user_id": "u1", "visible_work_count": 30, "work_count_exact": True}}
            )
        )

    def test_enrich_profile_report_with_note_metrics(self) -> None:
        report = {
            "captured_at": "2026-03-17T20:00:00+08:00",
            "profile": {"profile_user_id": "u1"},
            "works": [
                {
                    "title_copy": "作品A",
                    "note_id": "note_001",
                    "xsec_token": "token_001",
                    "note_url": "https://www.xiaohongshu.com/explore/note_001",
                }
            ],
        }

        class FakeCollector:
            def __init__(self, _settings) -> None:
                pass

            def collect_note_detail(self, **_kwargs):
                return NoteSnapshot(note_id="note_001", comment_count=28)

            def fetch_note_comments_preview(self, **_kwargs):
                return [
                    {"nickname": "用户A", "content": "第一条评论"},
                    {"nickname": "用户B", "content": "第二条评论"},
                ]

            def collect(self, _target):
                raise AssertionError("signed detail path should be used first")

        with patch("xhs_feishu_monitor.profile_report.XHSCollector", FakeCollector):
            enriched = enrich_profile_report_with_note_metrics(
                report=report,
                settings=SimpleNamespace(xhs_fetch_work_comment_counts=True),
            )

        self.assertEqual(enriched["works"][0]["comment_count"], 28)
        self.assertEqual(enriched["works"][0]["comment_count_text"], "28")
        self.assertEqual(
            enriched["works"][0]["recent_comments_summary"],
            "用户A: 第一条评论 | 用户B: 第二条评论",
        )

    def test_enrich_profile_report_with_note_metrics_falls_back_to_public_note_page(self) -> None:
        report = {
            "captured_at": "2026-03-17T20:00:00+08:00",
            "profile": {"profile_user_id": "u1"},
            "works": [
                {
                    "title_copy": "作品A",
                    "note_id": "note_001",
                    "xsec_token": "token_001",
                    "note_url": "https://www.xiaohongshu.com/explore/note_001",
                }
            ],
        }

        class FakeCollector:
            def __init__(self, _settings) -> None:
                pass

            def collect_note_detail(self, **_kwargs):
                return None

            def collect(self, _target):
                return NoteSnapshot(comment_count=31)

        with patch("xhs_feishu_monitor.profile_report.XHSCollector", FakeCollector):
            enriched = enrich_profile_report_with_note_metrics(
                report=report,
                settings=SimpleNamespace(xhs_fetch_work_comment_counts=True),
            )

        self.assertEqual(enriched["works"][0]["comment_count"], 31)
        self.assertEqual(enriched["works"][0]["comment_count_text"], "31")

    def test_load_profile_report_payload_retries_after_initial_state_failure(self) -> None:
        settings = SimpleNamespace(xhs_retry_attempts=2, xhs_retry_delay_seconds=0)
        html = """
        <html><body><script>
        window.__INITIAL_STATE__ = {"user":{"userPageData":{"basicInfo":{"nickname":"测试账号"}}}};
        </script></body></html>
        """

        class FakeCollector:
            def __init__(self, _settings) -> None:
                self.calls = 0

            def _resolve_fetch_modes(self, _target):
                return ["requests"]

            def _load_payload(self, _target, _mode):
                self.calls += 1
                if self.calls == 1:
                    return "<html><body>blocked</body></html>", "https://example.com/blocked"
                return html, "https://example.com/profile"

        with patch("xhs_feishu_monitor.profile_report.XHSCollector", FakeCollector):
            payload = load_profile_report_payload(
                settings=settings,
                profile_url="https://www.xiaohongshu.com/user/profile/test",
            )

        self.assertEqual(payload["final_url"], "https://example.com/profile")
        self.assertEqual(payload["initial_state"]["user"]["userPageData"]["basicInfo"]["nickname"], "测试账号")

    def test_load_profile_report_payload_falls_back_to_playwright_when_requests_redirects_to_login(self) -> None:
        settings = SimpleNamespace(
            xhs_retry_attempts=1,
            xhs_retry_delay_seconds=0,
            xhs_fetch_mode="requests",
            playwright_user_data_dir="/tmp/xhs-browser",
            playwright_storage_state="",
            playwright_browser_mode="local_profile",
            xhs_chrome_cookie_profile="",
        )
        html = """
        <html><body><script>
        window.__INITIAL_STATE__ = {"user":{"userPageData":{"basicInfo":{"nickname":"回退账号"}},"notes":[[{"noteCard":{"displayTitle":"作品A","user":{"userId":"u1"},"interactInfo":{"likedCount":"5"}}}]]}};
        </script></body></html>
        """

        class FakeCollector:
            def __init__(self, active_settings) -> None:
                self.settings = active_settings

            def _resolve_fetch_modes(self, _target):
                return [self.settings.xhs_fetch_mode]

            def _load_payload(self, _target, _mode):
                if self.settings.xhs_fetch_mode == "requests":
                    return "<html><body>login</body></html>", "https://www.xiaohongshu.com/login?redirectPath=test"
                return html, "https://www.xiaohongshu.com/user/profile/u1"

        with patch("xhs_feishu_monitor.profile_report.XHSCollector", FakeCollector):
            payload = load_profile_report_payload(
                settings=settings,
                profile_url="https://www.xiaohongshu.com/user/profile/u1?xsec_token=abc",
            )

        self.assertEqual(payload["final_url"], "https://www.xiaohongshu.com/user/profile/u1")
        self.assertEqual(payload["initial_state"]["user"]["userPageData"]["basicInfo"]["nickname"], "回退账号")

    def test_load_profile_report_payload_uses_signed_pages_to_complete_exact_work_count(self) -> None:
        settings = SimpleNamespace(xhs_retry_attempts=1, xhs_retry_delay_seconds=0)
        initial_state = {
            "user": {
                "userPageData": {"basicInfo": {"nickname": "测试账号", "userId": "u1"}},
                "noteQueries": [{"num": 30, "hasMore": True}],
                "notes": [[
                    {
                        "id": f"note_{index}",
                        "noteCard": {
                            "displayTitle": f"作品{index}",
                            "type": "normal",
                            "noteId": f"note_{index}",
                            "user": {"userId": "u1"},
                            "interactInfo": {"likedCount": str(index)},
                        },
                    }
                    for index in range(30)
                ]],
            }
        }

        class FakeCollector:
            def __init__(self, _settings) -> None:
                pass

            def _resolve_fetch_modes(self, _target):
                return ["requests"]

            def _load_payload(self, _target, _mode):
                return initial_state, "https://www.xiaohongshu.com/user/profile/u1"

            def fetch_profile_posted_pages(self, *, profile_url, initial_state):
                self.profile_url = profile_url
                self.initial_state = initial_state
                return [
                    {
                        "items": [
                            {
                                "id": f"note_{index}",
                                "noteCard": {
                                    "displayTitle": f"作品{index}",
                                    "type": "normal",
                                    "noteId": f"note_{index}",
                                    "user": {"userId": "u1"},
                                    "interactInfo": {"likedCount": str(index)},
                                },
                            }
                            for index in range(30, 45)
                        ],
                        "cursor": "",
                        "user_id": "u1",
                        "page": 2,
                        "num": 30,
                        "has_more": False,
                    }
                ]

        with patch("xhs_feishu_monitor.profile_report.XHSCollector", FakeCollector):
            payload = load_profile_report_payload(
                settings=settings,
                profile_url="https://www.xiaohongshu.com/user/profile/u1?xsec_token=demo&xsec_source=pc_search",
            )

        report = build_profile_report(initial_state=payload["initial_state"], profile_url=payload["final_url"])
        self.assertEqual(report["profile"]["visible_work_count"], 30)
        self.assertEqual(report["profile"]["total_work_count"], 45)
        self.assertEqual(report["profile"]["work_count_display_text"], "45")
        self.assertTrue(report["profile"]["work_count_exact"])

    def test_build_profile_fetch_setting_variants_uses_launch_fallback_for_default_chrome_profile(self) -> None:
        settings = SimpleNamespace(
            xhs_fetch_mode="requests",
            playwright_user_data_dir="/tmp/xhs-browser",
            playwright_storage_state="",
            playwright_browser_mode="local_profile",
            xhs_chrome_cookie_profile=DEFAULT_CHROME_PROFILE_ROOT,
        )
        variants = _build_profile_fetch_setting_variants(settings)
        self.assertEqual(len(variants), 2)
        self.assertEqual(variants[0], settings)
        self.assertEqual(variants[1].xhs_fetch_mode, "playwright")
        self.assertEqual(variants[1].playwright_browser_mode, "launch")
        self.assertEqual(variants[1].playwright_user_data_dir, "")


if __name__ == "__main__":
    unittest.main()
