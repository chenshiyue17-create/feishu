from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch

from xhs_feishu_monitor.config import Settings, load_settings
from xhs_feishu_monitor.models import Target
from xhs_feishu_monitor.xhs import (
    PUBLIC_IP_STATUS,
    build_proxy_pool_status,
    _merge_profile_runtime_pages,
    _normalize_snapshot,
    parse_cookie_header,
    resolve_local_browser_user_data_dir,
    XHSCollector,
)


WINDOW_STATE_HTML = """
<html>
  <head><title>demo</title></head>
  <body>
    <script>
      window.__INITIAL_STATE__ = {
        "note": {
          "noteId": "abc123def456",
          "title": "蒸烤炸一体机真实体验",
          "desc": "小厨房也能一步到位",
          "user": {
            "nickname": "老板厨房顾问",
            "userId": "user_123"
          },
          "interactInfo": {
            "likeCount": "1.2万",
            "collectCount": 345,
            "commentCount": "67",
            "shareCount": "8"
          },
          "publishTime": 1710662400
        }
      };
    </script>
  </body>
</html>
"""

NEXT_DATA_HTML = """
<html>
  <body>
    <script id="__NEXT_DATA__" type="application/json">
      {
        "props": {
          "pageProps": {
            "noteInfo": {
              "item": {
                "id": "note_xyz123",
                "title": "开放式厨房怎么选烟机",
                "description": "先看控烟，再看颜值",
                "author": {
                  "nickname": "阿橱",
                  "id": "author_001"
                },
                "stat": {
                  "likedCount": 987,
                  "favoriteCount": "1.5万",
                  "commentCount": 23,
                  "shareCount": 6
                },
                "publishTime": 1710662400000
              }
            }
          }
        }
      }
    </script>
  </body>
</html>
"""


class NormalizeSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.target = Target(name="测试目标", url="https://www.xiaohongshu.com/explore/abc123def456")
        self.settings = Settings()

    def test_normalize_window_state_html(self) -> None:
        snapshot = _normalize_snapshot(WINDOW_STATE_HTML, self.target, self.target.url or "")
        self.assertEqual(snapshot.note_id, "abc123def456")
        self.assertEqual(snapshot.note_title, "蒸烤炸一体机真实体验")
        self.assertEqual(snapshot.author_name, "老板厨房顾问")
        self.assertEqual(snapshot.like_count, 12000)
        self.assertEqual(snapshot.collect_count, 345)
        self.assertEqual(snapshot.comment_count, 67)
        self.assertEqual(snapshot.share_count, 8)

    def test_normalize_next_data_html(self) -> None:
        snapshot = _normalize_snapshot(NEXT_DATA_HTML, self.target, "https://www.xiaohongshu.com/explore/note_xyz123")
        self.assertEqual(snapshot.note_id, "note_xyz123")
        self.assertEqual(snapshot.note_title, "开放式厨房怎么选烟机")
        self.assertEqual(snapshot.author_name, "阿橱")
        self.assertEqual(snapshot.like_count, 987)
        self.assertEqual(snapshot.collect_count, 15000)
        self.assertEqual(snapshot.comment_count, 23)
        self.assertEqual(snapshot.share_count, 6)

    def test_parse_cookie_header(self) -> None:
        cookies = parse_cookie_header(
            "a=1; web_session=abc123; gid=xyz",
            "https://www.xiaohongshu.com/explore/abc123def456",
        )
        self.assertEqual(len(cookies), 3)
        self.assertEqual(cookies[0]["url"], "https://www.xiaohongshu.com")
        self.assertEqual(cookies[1]["name"], "web_session")
        self.assertEqual(cookies[1]["value"], "abc123")

    def test_resolve_local_browser_user_data_dir(self) -> None:
        settings = Settings(playwright_user_data_dir="/tmp/xhs-local-browser")
        self.assertEqual(resolve_local_browser_user_data_dir(settings), "/tmp/xhs-local-browser")

    def test_resolve_fetch_modes_supports_local_browser(self) -> None:
        collector = XHSCollector(Settings(xhs_fetch_mode="local_browser"))
        modes = collector._resolve_fetch_modes(Target(name="账号页", url="https://www.xiaohongshu.com/user/profile/demo"))
        self.assertEqual(modes, ["local_browser"])

    def test_resolve_cookie_header_uses_profile_directory(self) -> None:
        collector = XHSCollector(
            Settings(
                xhs_chrome_cookie_profile="/tmp/chrome-profile",
                playwright_profile_directory="Profile 2",
            )
        )
        with patch("xhs_feishu_monitor.xhs.export_xiaohongshu_cookie_header", return_value="a=b") as export_mock:
            header = collector._resolve_cookie_header()
        self.assertEqual(header, "a=b")
        export_mock.assert_called_once_with("/tmp/chrome-profile", "Profile 2")

    def test_merge_profile_runtime_pages_appends_unique_items_and_updates_query(self) -> None:
        initial_state = {
            "user": {
                "notes": [[{"id": "note_001", "noteCard": {"noteId": "note_001", "displayTitle": "作品1"}}]],
                "noteQueries": [{"num": 30, "cursor": "c1", "page": 1, "hasMore": True}],
            }
        }
        merged = _merge_profile_runtime_pages(
            initial_state,
            [
                {
                    "items": [
                        {"id": "note_001", "noteCard": {"noteId": "note_001", "displayTitle": "作品1"}},
                        {"id": "note_002", "noteCard": {"noteId": "note_002", "displayTitle": "作品2"}},
                    ],
                    "cursor": "c2",
                    "user_id": "u1",
                    "page": 2,
                    "num": 30,
                    "has_more": False,
                }
            ],
        )
        self.assertEqual(len(merged["user"]["notes"][0]), 2)
        self.assertEqual(merged["user"]["noteQueries"][0]["cursor"], "c2")
        self.assertEqual(merged["user"]["noteQueries"][0]["page"], 2)
        self.assertFalse(merged["user"]["noteQueries"][0]["hasMore"])

    def test_proxy_pool_rotates_and_skips_cooling_proxy(self) -> None:
        collector = XHSCollector(
            Settings(
                xhs_proxy_pool=["http://p1:8000", "http://p2:8000"],
                xhs_proxy_cooldown_seconds=60,
            )
        )
        self.assertEqual(collector._pick_proxy_url(), "http://p1:8000")
        self.assertEqual(collector._pick_proxy_url(), "http://p2:8000")
        with patch("xhs_feishu_monitor.xhs.time.monotonic", return_value=100.0):
            collector._mark_proxy_failed("http://p1:8000")
            self.assertEqual(collector._pick_proxy_url(), "http://p2:8000")
        with patch("xhs_feishu_monitor.xhs.time.monotonic", return_value=200.0):
            picked = collector._pick_proxy_url()
        self.assertIn(picked, {"http://p1:8000", "http://p2:8000"})

    def test_load_settings_supports_proxy_pool_env_and_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = f"{temp_dir}/.env"
            proxy_path = f"{temp_dir}/proxies.txt"
            with open(proxy_path, "w", encoding="utf-8") as handle:
                handle.write("# comment\n10.0.0.1:8080\nhttp://10.0.0.2:8080\n")
            with open(env_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "XHS_PROXY_POOL=10.0.0.3:8080,https://10.0.0.4:8080\n"
                    "XHS_PROXY_POOL_FILE=proxies.txt\n"
                    "XHS_PROXY_COOLDOWN_SECONDS=120\n"
                )
            settings = load_settings(env_path)
        self.assertEqual(
            settings.xhs_proxy_pool,
            [
                "http://10.0.0.3:8080",
                "https://10.0.0.4:8080",
                "http://10.0.0.1:8080",
                "http://10.0.0.2:8080",
            ],
        )
        self.assertEqual(settings.xhs_proxy_cooldown_seconds, 120)

    def test_build_proxy_pool_status_reports_cooling_and_last_error(self) -> None:
        collector = XHSCollector(
            Settings(
                xhs_proxy_pool=["http://p1:8000", "http://p2:8000"],
                xhs_proxy_cooldown_seconds=60,
            )
        )
        with patch("xhs_feishu_monitor.xhs.time.monotonic", return_value=100.0):
            collector._mark_proxy_failed("http://p1:8000", error_text="proxy timeout")
            status = build_proxy_pool_status(collector.settings)
        self.assertTrue(status["enabled"])
        self.assertEqual(status["total"], 2)
        self.assertEqual(status["cooling_count"], 1)
        self.assertEqual(status["ready_count"], 1)
        self.assertIn("proxy timeout", status["last_error"])

    def test_build_proxy_pool_status_reports_current_ip_when_pool_disabled(self) -> None:
        with patch("xhs_feishu_monitor.xhs.requests.get") as get_mock:
            get_mock.return_value.status_code = 200
            get_mock.return_value.text = "1.2.3.4"
            get_mock.return_value.raise_for_status.return_value = None
            PUBLIC_IP_STATUS.update({"ip": "", "checked_at": "", "error": "", "cached_at_monotonic": 0.0})
            status = build_proxy_pool_status(Settings())
        self.assertFalse(status["enabled"])
        self.assertEqual(status["current_ip"], "1.2.3.4")
        self.assertTrue(status["current_ip_checked_at"])

    def test_fetch_profile_posted_pages_uses_signed_api_and_returns_paginated_payloads(self) -> None:
        collector = XHSCollector(
            Settings(
                xhs_cookie="a1=test_a1; web_session=demo",
                xhs_enable_signed_profile_pages=True,
                xhs_signed_profile_max_pages=3,
            )
        )

        class FakeSignedSession:
            def fetch_profile_posted_pages(self, *, profile_url, initial_state):
                self.profile_url = profile_url
                self.initial_state = initial_state
                return [
                    {
                        "items": [
                            {"id": "note_001", "noteCard": {"noteId": "note_001", "displayTitle": "作品1"}},
                            {"id": "note_002", "noteCard": {"noteId": "note_002", "displayTitle": "作品2"}},
                        ],
                        "cursor": "",
                        "has_more": False,
                        "user_id": "u1",
                    }
                ]

        initial_state = {
            "user": {
                "userPageData": {"basicInfo": {"userId": "u1"}},
            }
        }

        with patch.object(collector, "_get_signed_session", return_value=FakeSignedSession()) as session_mock:
            pages = collector.fetch_profile_posted_pages(
                profile_url="https://www.xiaohongshu.com/user/profile/u1?xsec_token=demo&xsec_source=pc_search",
                initial_state=initial_state,
            )

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]["items"]), 2)
        self.assertEqual(pages[0]["user_id"], "u1")
        self.assertFalse(pages[0]["has_more"])
        session_mock.assert_called_once()

    def test_collect_note_detail_uses_signed_feed_api(self) -> None:
        collector = XHSCollector(Settings(xhs_cookie="a1=test_a1; web_session=demo"))

        class FakeSignedSession:
            def fetch_note_detail(self, *, note_id, note_url="", xsec_token="", xsec_source="pc_user"):
                self.note_id = note_id
                self.note_url = note_url
                self.xsec_token = xsec_token
                return {
                    "noteId": "note_001",
                    "title": "作品详情",
                    "interactInfo": {"commentCount": "12", "likedCount": "9"},
                }

        with patch.object(collector, "_get_signed_session", return_value=FakeSignedSession()) as session_mock:
            snapshot = collector.collect_note_detail(
                note_id="note_001",
                note_url="https://www.xiaohongshu.com/explore/note_001",
                xsec_token="token_001",
            )

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.note_id, "note_001")
        self.assertEqual(snapshot.comment_count, 12)
        self.assertEqual(snapshot.like_count, 9)
        session_mock.assert_called_once()

    def test_fetch_note_comments_preview_uses_signed_comment_api(self) -> None:
        collector = XHSCollector(Settings(xhs_cookie="a1=test_a1; web_session=demo"))

        class FakeSignedSession:
            def fetch_note_comments_preview(self, *, note_id, xsec_token, note_url="", limit=3):
                self.note_id = note_id
                self.note_url = note_url
                self.xsec_token = xsec_token
                self.limit = limit
                return [
                    {"nickname": "用户A", "content": "第一条评论"},
                    {"nickname": "用户B", "content": "第二条评论"},
                ]

        with patch.object(collector, "_get_signed_session", return_value=FakeSignedSession()) as session_mock:
            comments = collector.fetch_note_comments_preview(
                note_id="note_001",
                xsec_token="token_001",
                note_url="https://www.xiaohongshu.com/explore/note_001",
                limit=2,
            )

        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["nickname"], "用户A")
        self.assertEqual(comments[0]["content"], "第一条评论")
        session_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
