from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

from xhs_feishu_monitor.chrome_cookies import DEFAULT_CHROME_PROFILE_ROOT
from xhs_feishu_monitor.config import Settings
from xhs_feishu_monitor.local_stats_app.data_service import (
    build_account_cards,
    build_account_series_map,
    build_alerts,
    build_dashboard_payload_from_tables,
    build_daily_series,
    build_portal_card,
    build_rankings,
)
from xhs_feishu_monitor.local_stats_app.server import (
    DEFAULT_PROJECT_NAME,
    DashboardStore,
    LoginStateStore,
    MonitoringSyncStore,
    build_empty_dashboard_payload,
    build_mobile_rankings_payload,
    _load_dashboard_payload_local_only,
    build_dashboard_account_index,
    build_dashboard_payload_with_reports,
    build_login_state_payload,
    classify_monitored_fetch_state,
    build_sync_progress,
    build_profile_name_index,
    load_monitored_metadata,
    enrich_monitored_entries,
    export_project_rankings,
    export_single_account_rankings,
    extract_profile_user_id,
    login_state_requires_interactive_login,
    load_monitored_urls,
    merge_monitored_entries,
    merge_monitored_urls,
    open_xiaohongshu_login_window,
    parse_monitored_entries,
    run_login_state_self_check,
    refresh_project_export_snapshots,
    save_uploaded_server_cache,
    push_current_cache_to_server,
    update_monitored_metadata,
    wait_for_xiaohongshu_login,
    write_monitored_entries,
    write_monitored_urls,
    load_system_config,
    save_system_config,
)
from xhs_feishu_monitor.local_stats_app.login_state import is_transient_self_check_failure
from xhs_feishu_monitor.local_daily_sync_status import write_local_daily_sync_status
from xhs_feishu_monitor.project_sync_status import update_project_sync_status


class LocalStatsAppTest(unittest.TestCase):
    def test_load_and_save_system_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("XHS_COOKIE=old_cookie\nPROJECT_CACHE_DIR=/old/cache\n", encoding="utf-8")
            urls_path.write_text("https://www.xiaohongshu.com/user/profile/u1\n", encoding="utf-8")
            payload = load_system_config(str(env_path), str(urls_path))
            self.assertEqual(payload["config"]["XHS_COOKIE"], "old_cookie")
            self.assertIn("u1", payload["urls_text"])

            result = save_system_config(
                str(env_path),
                str(urls_path),
                {
                    "config": {
                        "XHS_COOKIE": "new_cookie",
                        "PROJECT_CACHE_DIR": "/data/cache",
                        "SERVER_CACHE_PUSH_URL": "http://127.0.0.1:8787",
                        "SERVER_CACHE_UPLOAD_TOKEN": "token-1",
                    },
                    "urls_text": "https://www.xiaohongshu.com/user/profile/u2\n",
                },
            )
            self.assertEqual(result["config"]["XHS_COOKIE"], "new_cookie")
            self.assertEqual(result["config"]["PROJECT_CACHE_DIR"], "/data/cache")
            self.assertEqual(result["config"]["SERVER_CACHE_PUSH_URL"], "http://127.0.0.1:8787")
            self.assertEqual(result["config"]["SERVER_CACHE_UPLOAD_TOKEN"], "token-1")
            self.assertIn("u2", urls_path.read_text(encoding="utf-8"))

    def test_load_system_config_uses_default_server_push_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("XHS_COOKIE=old_cookie\n", encoding="utf-8")
            payload = load_system_config(str(env_path), str(urls_path))
            self.assertEqual(payload["config"]["SERVER_CACHE_PUSH_URL"], "http://47.87.68.74")

    def test_load_system_config_normalizes_legacy_8787_server_push_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("SERVER_CACHE_PUSH_URL=http://47.87.68.74:8787\n", encoding="utf-8")
            payload = load_system_config(str(env_path), str(urls_path))
            self.assertEqual(payload["config"]["SERVER_CACHE_PUSH_URL"], "http://47.87.68.74")

    def test_save_system_config_restores_default_server_push_url_when_blank(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("XHS_COOKIE=old_cookie\n", encoding="utf-8")

            result = save_system_config(
                str(env_path),
                str(urls_path),
                {
                    "config": {
                        "XHS_COOKIE": "new_cookie",
                        "SERVER_CACHE_PUSH_URL": "",
                    },
                    "urls_text": "",
                },
            )

            self.assertEqual(result["config"]["SERVER_CACHE_PUSH_URL"], "http://47.87.68.74")
            self.assertIn("SERVER_CACHE_PUSH_URL=http://47.87.68.74", env_path.read_text(encoding="utf-8"))

    def test_save_system_config_normalizes_legacy_8787_server_push_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("XHS_COOKIE=old_cookie\n", encoding="utf-8")

            result = save_system_config(
                str(env_path),
                str(urls_path),
                {
                    "config": {
                        "XHS_COOKIE": "new_cookie",
                        "SERVER_CACHE_PUSH_URL": "http://47.87.68.74:8787",
                    },
                    "urls_text": "",
                },
            )

            self.assertEqual(result["config"]["SERVER_CACHE_PUSH_URL"], "http://47.87.68.74")
            self.assertIn("SERVER_CACHE_PUSH_URL=http://47.87.68.74", env_path.read_text(encoding="utf-8"))

    def test_save_system_config_strips_legacy_feishu_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text(
                "XHS_COOKIE=old_cookie\n"
                "FEISHU_APP_ID=bad\n"
                "FEISHU_APP_SECRET=bad\n"
                "PROJECT_CACHE_DIR=/old/cache\n",
                encoding="utf-8",
            )

            save_system_config(
                str(env_path),
                str(urls_path),
                {
                    "config": {
                        "XHS_COOKIE": "new_cookie",
                        "PROJECT_CACHE_DIR": "/data/cache",
                        "STATE_FILE": "/data/state.json",
                    },
                    "urls_text": "",
                },
            )

            text = env_path.read_text(encoding="utf-8")
            self.assertIn("XHS_COOKIE=new_cookie", text)
            self.assertIn("PROJECT_CACHE_DIR=/data/cache", text)
            self.assertNotIn("FEISHU_APP_ID=", text)
            self.assertNotIn("FEISHU_APP_SECRET=", text)

    def test_save_uploaded_server_cache_writes_dashboard_and_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            cache_dir = Path(temp_dir) / "cache"
            env_path.write_text(
                f"PROJECT_CACHE_DIR={cache_dir}\nSTATE_FILE={Path(temp_dir) / '.state.json'}\n",
                encoding="utf-8",
            )

            result = save_uploaded_server_cache(
                env_file=str(env_path),
                urls_file=str(urls_path),
                payload={
                    "dashboard_payload": {
                        "accounts": [{"account_id": "u1", "account": "账号A"}],
                        "rankings": {},
                        "account_series": {},
                    },
                    "monitored_entries": [
                        {
                            "url": "https://www.xiaohongshu.com/user/profile/u1",
                            "active": True,
                            "project": "默认项目",
                        }
                    ],
                    "monitored_metadata": {
                        "https://www.xiaohongshu.com/user/profile/u1": {
                            "account": "账号A",
                            "account_id": "u1",
                            "fetch_state": "ok",
                        }
                    },
                },
            )

            self.assertTrue((cache_dir / "dashboard_all.json").exists())
            self.assertIn("默认项目", urls_path.read_text(encoding="utf-8"))
            self.assertTrue(Path(result["metadata_path"]).exists())
            self.assertEqual(result["account_count"], 1)

    def test_save_uploaded_server_cache_partial_merge_preserves_other_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            cache_dir = Path(temp_dir) / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            env_path.write_text(
                f"PROJECT_CACHE_DIR={cache_dir}\nSTATE_FILE={Path(temp_dir) / '.state.json'}\n",
                encoding="utf-8",
            )
            urls_path.write_text(
                "默认项目\thttps://www.xiaohongshu.com/user/profile/u1\n"
                "默认项目\thttps://www.xiaohongshu.com/user/profile/u2\n",
                encoding="utf-8",
            )
            (cache_dir / "dashboard_all.json").write_text(
                json.dumps(
                    {
                        "accounts": [
                            {"account_id": "u1", "account": "账号A", "likes": 10},
                            {"account_id": "u2", "account": "账号B", "likes": 20},
                        ],
                        "rankings": {
                            "单条点赞排行": [
                                {"account_id": "u1", "metric": 10, "title": "A"},
                                {"account_id": "u2", "metric": 20, "title": "B"},
                            ]
                        },
                        "account_series": {"u1": [{"date": "2026-03-31", "likes": 10}], "u2": [{"date": "2026-03-31", "likes": 20}]},
                        "alerts": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            save_uploaded_server_cache(
                env_file=str(env_path),
                urls_file=str(urls_path),
                payload={
                    "merge_mode": "partial",
                    "account_ids": ["u1"],
                    "dashboard_payload": {
                        "accounts": [{"account_id": "u1", "account": "账号A", "likes": 99}],
                        "rankings": {"单条点赞排行": [{"account_id": "u1", "metric": 99, "title": "A-new"}]},
                        "account_series": {"u1": [{"date": "2026-03-31", "likes": 99}]},
                        "alerts": [],
                    },
                    "monitored_entries": [
                        {
                            "url": "https://www.xiaohongshu.com/user/profile/u1",
                            "active": True,
                            "project": "默认项目",
                        }
                    ],
                    "monitored_metadata": {
                        "https://www.xiaohongshu.com/user/profile/u1": {
                            "account": "账号A",
                            "account_id": "u1",
                            "fetch_state": "ok",
                        }
                    },
                },
            )

            payload = json.loads((cache_dir / "dashboard_all.json").read_text(encoding="utf-8"))
            accounts = {item["account_id"]: item for item in payload["accounts"]}
            self.assertEqual(accounts["u1"]["likes"], 99)
            self.assertEqual(accounts["u2"]["likes"], 20)
            ranking_rows = payload["rankings"]["单条点赞排行"]
            self.assertTrue(any(str(item.get("account_id")) == "u2" for item in ranking_rows))

    def test_push_current_cache_to_server_uses_configured_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text(
                "SERVER_CACHE_PUSH_URL=http://127.0.0.1:8787\n"
                "SERVER_CACHE_UPLOAD_TOKEN=token-1\n",
                encoding="utf-8",
            )
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.push_local_cache_to_server",
                return_value={"ok": True, "account_count": 3},
            ) as push_mock:
                result = push_current_cache_to_server(env_file=str(env_path), urls_file=str(urls_path))
        self.assertTrue(result["ok"])
        push_mock.assert_called_once()

    def test_push_current_cache_to_server_accepts_account_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            urls_path = Path(temp_dir) / "urls.txt"
            env_path.write_text("SERVER_CACHE_PUSH_URL=http://127.0.0.1\n", encoding="utf-8")
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.push_local_cache_to_server",
                return_value={"ok": True, "account_count": 1},
            ) as push_mock:
                push_current_cache_to_server(env_file=str(env_path), urls_file=str(urls_path), account_ids=["u1"])
        self.assertEqual(push_mock.call_args.kwargs["account_ids"], ["u1"])

    def test_build_daily_series(self) -> None:
        rows = [
            {"日期文本": "2026-03-17", "账号ID": "u1", "粉丝数": 100, "首页总点赞": 20, "首页总评论": 5, "首页可见作品数": 2},
            {"日期文本": "2026-03-17", "账号ID": "u2", "粉丝数": 200, "首页总点赞": 30, "首页总评论": 8, "首页可见作品数": 3},
        ]
        series = build_daily_series(rows)
        self.assertEqual(series[0]["fans"], 300)
        self.assertEqual(series[0]["likes"], 50)
        self.assertEqual(series[0]["comments"], 13)

    def test_build_mobile_rankings_payload_filters_by_project(self) -> None:
        payload = build_mobile_rankings_payload(
            dashboard_payload={
                "updated_at": "2026-03-28T12:00:00+08:00",
                "account_series": {
                    "u1": [
                        {"date": "2026-03-27", "fans": 10, "interaction": 20, "likes": 3, "comments": 1, "works": 5},
                    ],
                    "u2": [
                        {"date": "2026-03-27", "fans": 50, "interaction": 60, "likes": 7, "comments": 2, "works": 8},
                    ],
                },
                "rankings": {
                    "单条点赞排行": [
                        {"rank": 1, "account_id": "u1", "title": "A"},
                        {"rank": 2, "account_id": "u2", "title": "B"},
                    ],
                    "单条评论排行": [
                        {"rank": 1, "account_id": "u1", "title": "A"},
                    ],
                    "单条第二天增长排行": [
                        {"rank": 1, "account_id": "u2", "title": "B"},
                    ],
                },
            },
            monitored_entries=[
                {"account_id": "u1", "project": "默认项目"},
                {"account_id": "u2", "project": "东莞"},
            ],
            project="东莞",
        )
        self.assertEqual(payload["project"], "东莞")
        self.assertEqual(payload["projects"], ["东莞", "默认项目"])
        self.assertEqual(payload["view_mode"], "server_cache_only")
        self.assertEqual(payload["accounts"][0]["account_id"], "u2")
        self.assertEqual(len(payload["rankings"]["likes"]), 1)
        self.assertEqual(payload["rankings"]["likes"][0]["account_id"], "u2")
        self.assertEqual(len(payload["rankings"]["comments"]), 0)
        self.assertEqual(len(payload["rankings"]["growth"]), 1)
        self.assertEqual(len(payload["calendar"]), 1)
        self.assertEqual(payload["calendar"][0]["likes"], 7)

    def test_build_mobile_rankings_payload_extracts_account_id_from_url(self) -> None:
        payload = build_mobile_rankings_payload(
            dashboard_payload={
                "updated_at": "2026-03-30T18:34:28+08:00",
                "latest_date": "2026-03-30",
                "account_series": {
                    "572eb4666a6a6940862da761": [
                        {"date": "2026-03-30", "fans": 10, "interaction": 20, "likes": 3, "comments": 1, "works": 5},
                    ],
                },
                "rankings": {
                    "单条点赞排行": [
                        {"rank": 1, "account_id": "572eb4666a6a6940862da761", "title": "A"},
                    ],
                    "单条评论排行": [],
                    "单条第二天增长排行": [],
                },
            },
            monitored_entries=[
                {"url": "https://www.xiaohongshu.com/user/profile/572eb4666a6a6940862da761", "project": "默认项目"},
            ],
            project="默认项目",
        )
        self.assertEqual(payload["project"], "默认项目")
        self.assertEqual(len(payload["rankings"]["likes"]), 1)
        self.assertEqual(payload["rankings"]["likes"][0]["account_id"], "572eb4666a6a6940862da761")
        self.assertEqual(len(payload["calendar"]), 1)

    def test_build_mobile_rankings_payload_includes_history_rankings(self) -> None:
        payload = build_mobile_rankings_payload(
            dashboard_payload={
                "rankings": {},
                "account_series": {
                    "u1": [
                        {"date": "2026-03-30", "fans": 10, "interaction": 20, "likes": 3, "comments": 1, "works": 5},
                    ],
                },
                "history_rankings": {
                    "默认项目": {
                        "2026-03-30": {
                            "date": "2026-03-30",
                            "snapshot_time": "2026-03-30 18:01:18",
                            "account_count": 1,
                            "likes": [{"rank": 1, "account_id": "u1", "title": "作品A", "metric": 30}],
                            "comments": [{"rank": 1, "account_id": "u1", "title": "作品A", "metric": 8}],
                            "growth": [{"rank": 1, "account_id": "u1", "title": "账号A", "metric": 12}],
                        }
                    }
                },
            },
            monitored_entries=[{"account_id": "u1", "project": "默认项目"}],
            project="默认项目",
        )
        self.assertIn("2026-03-30", payload["history_rankings"])
        self.assertEqual(payload["history_rankings"]["2026-03-30"]["likes"][0]["metric"], 30)

    def test_build_mobile_rankings_payload_falls_back_to_latest_rankings_when_history_missing(self) -> None:
        payload = build_mobile_rankings_payload(
            dashboard_payload={
                "latest_date": "2026-03-31",
                "updated_at": "2026-03-31T05:06:50+08:00",
                "rankings": {
                    "单条点赞排行": [{"rank": 1, "account_id": "u1", "title": "作品A", "metric": 158}],
                    "单条评论排行": [{"rank": 1, "account_id": "u1", "title": "作品A", "metric": 88}],
                    "单条第二天增长排行": [{"rank": 1, "account_id": "u1", "title": "账号A", "metric": 22}],
                },
                "account_series": {
                    "u1": [
                        {"date": "2026-03-31", "fans": 10, "interaction": 20, "likes": 3, "comments": 1, "works": 5},
                    ],
                },
                "history_rankings": {
                    "默认项目": {
                        "2026-03-30": {
                            "date": "2026-03-30",
                            "snapshot_time": "2026-03-30 18:01:18",
                            "account_count": 1,
                            "likes": [{"rank": 1, "account_id": "u1", "title": "旧作品", "metric": 30}],
                            "comments": [],
                            "growth": [],
                        }
                    }
                },
            },
            monitored_entries=[{"account_id": "u1", "project": "默认项目"}],
            project="默认项目",
        )
        self.assertIn("2026-03-31", payload["history_rankings"])
        self.assertEqual(payload["history_rankings"]["2026-03-31"]["likes"][0]["metric"], 158)

    def test_build_mobile_rankings_payload_falls_back_to_first_project(self) -> None:
        payload = build_mobile_rankings_payload(
            dashboard_payload={"rankings": {}, "account_series": {}},
            monitored_entries=[
                {"account_id": "u1", "project": "默认项目"},
                {"account_id": "u2", "project": "东莞"},
            ],
            project="不存在",
        )
        self.assertEqual(payload["project"], "东莞")
        self.assertEqual(payload["projects"], ["东莞", "默认项目"])

    def test_dashboard_store_returns_empty_payload_when_loading_fails_without_cache(self) -> None:
        store = DashboardStore(env_file="/tmp/missing.env", cache_seconds=30)
        with patch("xhs_feishu_monitor.local_stats_app.server.load_dashboard_payload", side_effect=ValueError("invalid param")):
            payload = store.get_payload(force=True)
        self.assertEqual(payload["accounts"], [])
        self.assertTrue(payload["stale"])
        self.assertEqual(payload["load_error"], "invalid param")

    def test_load_dashboard_payload_local_only_returns_empty_without_feishu(self) -> None:
        with patch("xhs_feishu_monitor.local_stats_app.server.load_settings"), \
             patch("xhs_feishu_monitor.local_stats_app.server.load_cached_dashboard_payload", return_value={}), \
             patch("xhs_feishu_monitor.local_stats_app.server.rebuild_dashboard_cache_from_project_dirs", side_effect=ValueError("bad cache")), \
             patch("xhs_feishu_monitor.local_stats_app.server.repair_dashboard_cache_from_exports", side_effect=ValueError("bad export")):
            payload = _load_dashboard_payload_local_only("/tmp/test.env")
        self.assertEqual(payload["accounts"], [])
        self.assertTrue(payload["stale"])

    def test_build_account_series_map(self) -> None:
        rows = [
            {"日期文本": "2026-03-17", "账号ID": "u1", "粉丝数": 100, "获赞收藏数": 200, "首页总点赞": 20, "首页总评论": 5, "首页可见作品数": 2},
            {"日期文本": "2026-03-18", "账号ID": "u1", "粉丝数": 120, "获赞收藏数": 230, "首页总点赞": 25, "首页总评论": 7, "首页可见作品数": 3},
        ]
        series_map = build_account_series_map(rows)
        self.assertEqual(series_map["u1"][0]["fans"], 100)
        self.assertEqual(series_map["u1"][1]["comments"], 7)

    def test_build_account_cards(self) -> None:
        rows = [
            {"日期文本": "2026-03-16", "账号ID": "u1", "账号": "旧账号", "粉丝数": 50},
            {
                "日期文本": "2026-03-17",
                "账号ID": "u1",
                "账号": "账号A",
                "粉丝数": 100,
                "获赞收藏数": 200,
                "账号总作品数": 12,
                "作品数展示": "12+",
                "首页总点赞": 20,
                "首页总评论": 5,
                "首页可见作品数": 2,
                "周对比摘要": "周增",
                "主页链接": {"link": "https://a"},
                "头部作品链接": {"link": "https://note-a"},
            },
        ]
        cards = build_account_cards(rows)
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["account"], "账号A")
        self.assertEqual(cards[0]["fans"], 100)
        self.assertEqual(cards[0]["profile_url"], "https://a")
        self.assertEqual(cards[0]["top_url"], "https://note-a")
        self.assertEqual(cards[0]["works"], 12)
        self.assertEqual(cards[0]["works_display"], "12+")
        self.assertFalse(cards[0]["works_exact"])

    def test_build_account_cards_keeps_latest_per_account(self) -> None:
        rows = [
            {"日期文本": "2026-03-17", "账号ID": "u1", "账号": "账号A", "粉丝数": 100},
            {"日期文本": "2026-03-18", "账号ID": "u2", "账号": "账号B", "粉丝数": 120},
        ]
        cards = build_account_cards(rows)
        self.assertEqual({item["account_id"] for item in cards}, {"u1", "u2"})

    def test_build_account_cards_clamps_legacy_visible_works_to_first_30(self) -> None:
        rows = [
            {
                "日期文本": "2026-03-18",
                "账号ID": "u1",
                "账号": "账号A",
                "粉丝数": 100,
                "获赞收藏数": 200,
                "首页总点赞": 20,
                "首页总评论": 5,
                "首页可见作品数": 32,
            }
        ]
        cards = build_account_cards(rows)
        self.assertEqual(cards[0]["works"], 30)
        self.assertEqual(cards[0]["works_display"], "30+")
        self.assertFalse(cards[0]["works_exact"])

    def test_build_rankings(self) -> None:
        rows = [
            {"榜单类型": "单条点赞排行", "排名": 2, "标题文案": "作品B", "账号": "账号B", "排序值": 50},
            {
                "榜单类型": "单条点赞排行",
                "排名": 1,
                "账号ID": "u1",
                "标题文案": "作品A",
                "账号": "账号A",
                "排序值": 90,
                "主页链接": {"link": "https://profile-a"},
                "封面图": {"link": "https://img.example.com/a.jpg"},
            },
        ]
        rankings = build_rankings(rows)
        self.assertEqual(rankings["单条点赞排行"][0]["title"], "作品A")
        self.assertEqual(rankings["单条点赞排行"][0]["account_id"], "u1")
        self.assertEqual(rankings["单条点赞排行"][0]["profile_url"], "https://profile-a")
        self.assertEqual(rankings["单条点赞排行"][0]["cover_url"], "https://img.example.com/a.jpg")

    def test_build_alerts_sorts_by_numeric_delta(self) -> None:
        alerts = build_alerts(
            [
                {"预警日期": "2026-03-18", "标题文案": "作品A", "预警类型": "评论预警", "点赞增量": 1, "评论增量": 20},
                {"预警日期": "2026-03-18", "账号ID": "u2", "标题文案": "作品B", "预警类型": "点赞预警", "点赞增量": 12, "评论增量": 10, "主页链接": {"link": "https://profile-b"}},
            ]
        )
        self.assertEqual(alerts[0]["title"], "作品A")
        self.assertEqual(alerts[1]["account_id"], "u2")
        self.assertEqual(alerts[0]["delta"], 20)
        self.assertEqual(alerts[1]["profile_url"], "https://profile-b")

    def test_build_portal_card_uses_latest_update(self) -> None:
        portal = build_portal_card(
            [
                {"监控账号数": 2, "总粉丝数": 100, "数据更新时间": 1000},
                {"监控账号数": 5, "总粉丝数": 300, "数据更新时间": 2000},
            ]
        )
        self.assertEqual(portal["accounts"], 5)
        self.assertEqual(portal["fans"], 300)

    def test_build_dashboard_payload_from_tables(self) -> None:
        payload = build_dashboard_payload_from_tables(
            portal_rows=[{"监控账号数": 5, "总粉丝数": 1000, "总评论数": 300}],
            calendar_rows=[{"日期文本": "2026-03-17", "账号ID": "u1", "账号": "账号A", "粉丝数": 100, "首页总点赞": 20, "首页总评论": 5, "首页可见作品数": 2}],
            ranking_rows=[{"榜单类型": "单条点赞排行", "排名": 1, "标题文案": "作品A", "账号": "账号A", "排序值": 20}],
            alert_rows=[],
        )
        self.assertEqual(payload["portal"]["accounts"], 5)
        self.assertEqual(payload["accounts"][0]["account"], "账号A")
        self.assertIn("u1", payload["account_series"])
        self.assertEqual(payload["series_meta"]["mode"], "daily")
        self.assertEqual(payload["series_meta"]["update_time"], "14:00")
        self.assertEqual(payload["rankings"]["单条点赞排行"][0]["title"], "作品A")

    def test_extract_profile_user_id(self) -> None:
        self.assertEqual(
            extract_profile_user_id("https://www.xiaohongshu.com/user/profile/66c8e177000000001d0331c0?xsec_token=abc"),
            "66c8e177000000001d0331c0",
        )

    def test_enrich_monitored_entries_uses_account_name(self) -> None:
        enriched = enrich_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            [
                {
                    "账号ID": "u1",
                    "账号": "账号A",
                    "内容链接": {"link": "https://www.xiaohongshu.com/user/profile/u1?from=feishu"},
                    "粉丝数": 123,
                    "获赞收藏文本": "456",
                    "作品数展示": "32+",
                    "首页可见作品数": 7,
                }
            ],
        )
        self.assertEqual(enriched[0]["account"], "账号A")
        self.assertEqual(enriched[0]["display_name"], "账号A")
        self.assertEqual(enriched[0]["project"], "项目A")
        self.assertEqual(enriched[0]["profile_url"], "https://www.xiaohongshu.com/user/profile/u1?from=feishu")
        self.assertEqual(enriched[0]["summary_text"], "粉丝 123 · 获赞 456 · 作品 32+")

    def test_enrich_monitored_entries_uses_local_metadata_fallback(self) -> None:
        enriched = enrich_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            [],
            {
                "https://www.xiaohongshu.com/user/profile/u1": {
                    "account": "缓存账号",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1?from=cache",
                    "fans_text": "88",
                    "interaction_text": "666",
                    "works_text": "9",
                    "fetch_state": "ok",
                    "fetch_message": "已获取账号快照",
                }
            },
        )
        self.assertEqual(enriched[0]["account"], "缓存账号")
        self.assertEqual(enriched[0]["display_name"], "缓存账号")
        self.assertEqual(enriched[0]["summary_text"], "粉丝 88 · 获赞 666 · 作品 9")
        self.assertEqual(enriched[0]["fetch_state"], "ok")

    def test_enrich_monitored_entries_uses_dashboard_fallback(self) -> None:
        enriched = enrich_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            [],
            {},
            dashboard_account_index=build_dashboard_account_index(
                [
                    {
                        "account_id": "u1",
                        "account": "看板账号",
                        "profile_url": "https://www.xiaohongshu.com/user/profile/u1?from=dashboard",
                        "fans": 321,
                        "interaction": 654,
                        "works": 12,
                    }
                ]
            ),
        )
        self.assertEqual(enriched[0]["account"], "看板账号")
        self.assertEqual(enriched[0]["summary_text"], "粉丝 321 · 获赞 654 · 作品 12")

    def test_enrich_monitored_entries_shows_pending_summary_when_missing(self) -> None:
        enriched = enrich_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            [],
        )
        self.assertEqual(enriched[0]["summary_text"], "等待首次同步")

    def test_enrich_monitored_entries_ignores_login_redirect_profile_url(self) -> None:
        enriched = enrich_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            [],
            {
                "https://www.xiaohongshu.com/user/profile/u1": {
                    "profile_url": "https://www.xiaohongshu.com/login?redirectPath=abc",
                }
            },
        )
        self.assertEqual(enriched[0]["profile_url"], "https://www.xiaohongshu.com/user/profile/u1")

    def test_update_monitored_metadata_persists_account_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            urls_file = f"{temp_dir}/urls.txt"
            write_monitored_entries(
                urls_file,
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            update_monitored_metadata(
                urls_file,
                [
                    {
                        "url": "https://www.xiaohongshu.com/user/profile/u1",
                        "account": "账号A",
                        "account_id": "u1",
                        "profile_url": "https://www.xiaohongshu.com/user/profile/u1?from=cache",
                        "fetch_state": "error",
                        "fetch_message": "命中登录页，当前登录态不可用",
                    }
                ],
            )
            metadata = load_monitored_metadata(urls_file)
        self.assertEqual(metadata["https://www.xiaohongshu.com/user/profile/u1"]["account"], "账号A")
        self.assertEqual(metadata["https://www.xiaohongshu.com/user/profile/u1"]["fetch_state"], "error")

    def test_classify_monitored_fetch_state_login_redirect(self) -> None:
        state, message = classify_monitored_fetch_state(error_text="账号页返回空结果或登录跳转: /login", has_snapshot=False)
        self.assertEqual(state, "error")
        self.assertIn("登录态", message)

    def test_login_state_requires_interactive_login(self) -> None:
        self.assertTrue(
            login_state_requires_interactive_login(
                {"state": "error", "message": "样本账号返回了空结果，登录态可能已过期"}
            )
        )
        self.assertTrue(
            login_state_requires_interactive_login(
                {
                    "state": "warning",
                    "cookie_source": "chrome_profile",
                    "detail_ready": False,
                    "message": "样本账号只拿到公开页摘要，未拿到 note_id，作品详情与评论数据已退化。",
                }
            )
        )
        self.assertFalse(
            login_state_requires_interactive_login(
                {"state": "warning", "message": "样本账号只拿到公开页摘要"}
            )
        )

    def test_is_transient_self_check_failure(self) -> None:
        self.assertTrue(is_transient_self_check_failure("HTML 中未找到可解析的 __INITIAL_STATE__ 数据"))
        self.assertTrue(is_transient_self_check_failure("Page.goto: net::ERR_CONNECTION_CLOSED"))
        self.assertFalse(is_transient_self_check_failure("命中登录页，当前登录态不可用"))

    def test_merge_monitored_urls_adds_and_dedupes(self) -> None:
        merged, added = merge_monitored_urls(
            ["https://www.xiaohongshu.com/user/profile/u1"],
            raw_text="https://www.xiaohongshu.com/user/profile/u1 https://www.xiaohongshu.com/user/profile/u2",
        )
        self.assertEqual(
            merged,
            [
                "https://www.xiaohongshu.com/user/profile/u1",
                "https://www.xiaohongshu.com/user/profile/u2",
            ],
        )
        self.assertEqual(added, ["https://www.xiaohongshu.com/user/profile/u2"])

    def test_merge_monitored_entries_reactivates_paused(self) -> None:
        merged, added, reactivated = merge_monitored_entries(
            [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": False, "project": "项目A"}],
            raw_text="https://www.xiaohongshu.com/user/profile/u1 https://www.xiaohongshu.com/user/profile/u2",
            project="项目B",
        )
        self.assertEqual(added, ["https://www.xiaohongshu.com/user/profile/u2"])
        self.assertEqual(reactivated, ["https://www.xiaohongshu.com/user/profile/u1"])
        self.assertTrue(all(item["active"] for item in merged))
        self.assertEqual(merged[0]["project"], "项目A")
        self.assertEqual(merged[1]["project"], "项目B")

    def test_write_and_load_monitored_urls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_urls(
                f"{temp_dir}/urls.txt",
                [
                    "https://www.xiaohongshu.com/user/profile/u1",
                    "xiaohongshu.com/user/profile/u2",
                ],
            )
            urls = load_monitored_urls(str(path))
        self.assertEqual(
            urls,
            [
                "https://www.xiaohongshu.com/user/profile/u1",
                "https://www.xiaohongshu.com/user/profile/u2",
            ],
        )

    def test_write_and_parse_monitored_entries_with_paused(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": False},
                ],
            )
            entries = parse_monitored_entries(str(path))
            urls = load_monitored_urls(str(path))
        self.assertEqual(
            entries,
            [
                {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": DEFAULT_PROJECT_NAME},
                {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": False, "project": DEFAULT_PROJECT_NAME},
            ],
        )
        self.assertEqual(urls, ["https://www.xiaohongshu.com/user/profile/u1"])

    def test_write_and_parse_monitored_entries_with_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": False, "project": "项目B"},
                ],
            )
            entries = parse_monitored_entries(str(path))
        self.assertEqual(
            entries,
            [
                {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": False, "project": "项目B"},
            ],
        )

    def test_bulk_update_account_state_updates_multiple_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True},
                    {"url": "https://www.xiaohongshu.com/user/profile/u3", "active": False},
                ],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            with patch.object(store, "_request_sync_locked", return_value=True):
                result = store.bulk_update_account_state(
                    urls=[
                        "https://www.xiaohongshu.com/user/profile/u1",
                        "https://www.xiaohongshu.com/user/profile/u2",
                    ],
                    active=False,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["changed_count"], 2)
            self.assertEqual(
                parse_monitored_entries(str(path)),
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": False, "project": DEFAULT_PROJECT_NAME},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": False, "project": DEFAULT_PROJECT_NAME},
                    {"url": "https://www.xiaohongshu.com/user/profile/u3", "active": False, "project": DEFAULT_PROJECT_NAME},
                ],
            )

    def test_assign_project_updates_multiple_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True, "project": "项目A"},
                ],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            with patch.object(store, "_request_sync_locked", return_value=True):
                result = store.assign_project(
                    urls=["https://www.xiaohongshu.com/user/profile/u1", "https://www.xiaohongshu.com/user/profile/u2"],
                    project="项目B",
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["changed_count"], 2)
            self.assertEqual(
                parse_monitored_entries(str(path)),
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目B"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True, "project": "项目B"},
                ],
            )

    def test_request_sync_can_scope_to_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True, "project": "项目B"},
                ],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            captured = {}

            def fake_request_sync_locked(*, reason, urls=None, project="", mode="manual"):
                captured["reason"] = reason
                captured["urls"] = urls or []
                captured["project"] = project
                captured["mode"] = mode
                return True

            with patch.object(store, "_request_sync_locked", side_effect=fake_request_sync_locked):
                result = store.request_sync(project="项目A")
            self.assertTrue(result["ok"])
            self.assertEqual(captured["urls"], ["https://www.xiaohongshu.com/user/profile/u1"])
            self.assertEqual(captured["project"], "项目A")
            self.assertEqual(captured["mode"], "manual")
            self.assertIn("项目「项目A」", captured["reason"])

    def test_retry_account_triggers_single_account_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            captured = {}

            def fake_request_sync_locked(*, reason, urls=None, project=""):
                captured["reason"] = reason
                captured["urls"] = urls or []
                captured["project"] = project
                return True

            with patch.object(store, "_request_sync_locked", side_effect=fake_request_sync_locked):
                result = store.retry_account(url="https://www.xiaohongshu.com/user/profile/u1?xsec_token=abc")

            metadata = load_monitored_metadata(str(path))
            self.assertTrue(result["ok"])
            self.assertEqual(captured["urls"], ["https://www.xiaohongshu.com/user/profile/u1"])
            self.assertEqual(captured["project"], "")
            self.assertEqual(metadata["https://www.xiaohongshu.com/user/profile/u1"]["fetch_state"], "checking")
            self.assertIn("重试", metadata["https://www.xiaohongshu.com/user/profile/u1"]["fetch_message"])

    def test_get_payload_attaches_project_sync_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True, "project": "项目B"},
                ],
            )
            update_project_sync_status(
                urls_file=str(path),
                project="项目A",
                state="success",
                message="同步完成",
                finished_at="2026-03-23T14:00:00+08:00",
                total_accounts=1,
                total_works=30,
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            payload = store.get_payload()
            projects = {item["name"]: item for item in payload["projects"]}
            self.assertEqual(projects["项目A"]["sync_status"]["state"], "success")
            self.assertEqual(projects["项目A"]["sync_status"]["total_accounts"], 1)
            self.assertEqual(projects["项目A"]["sync_status"]["message"], "同步完成")
            self.assertEqual(projects["项目B"]["sync_status"], {})

    def test_get_payload_sanitizes_legacy_feishu_project_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            update_project_sync_status(
                urls_file=str(path),
                project="项目A",
                state="success",
                message="项目「项目A」采集完成",
                finished_at="2026-03-31T16:50:00+08:00",
                total_accounts=1,
                total_works=30,
            )
            update_project_sync_status(
                urls_file=str(path),
                project="项目A",
                state="error",
                message="缺少飞书配置: FEISHU_APP_ID, FEISHU_APP_SECRET",
                finished_at="2026-03-31T17:03:20+08:00",
                last_error="缺少飞书配置: FEISHU_APP_ID, FEISHU_APP_SECRET",
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            payload = store.get_payload()

        project_sync = payload["projects"][0]["sync_status"]
        self.assertEqual(project_sync["state"], "success")
        self.assertEqual(project_sync["message"], "本地缓存已更新")
        self.assertEqual(project_sync["last_error"], "")

    def test_request_sync_blocks_manual_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
                manual_sync_cooldown_seconds=600,
            )
            store._manual_last_requested_at = time.time()
            result = store.request_sync(project="项目A")
        self.assertFalse(result["ok"])
        self.assertIn("手动更新过于频繁", result["message"])
        self.assertTrue(result["sync_status"]["manual_sync_locked"])

    def test_status_snapshot_exposes_upload_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
        )
        payload = store.get_payload()
        self.assertIn("upload_status", payload["sync_status"])
        self.assertEqual(payload["sync_status"]["upload_status"]["state"], "disabled")
        self.assertFalse(payload["sync_status"]["upload_status"]["has_retry_payload"])

    def test_status_snapshot_sanitizes_legacy_feishu_runtime_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            store._status.update(
                {
                    "state": "error",
                    "message": "缺少飞书配置: FEISHU_APP_ID, FEISHU_APP_SECRET",
                    "last_error": "缺少飞书配置: FEISHU_APP_ID, FEISHU_APP_SECRET",
                    "progress": {"phase": "collect"},
                    "summary": {"total_accounts": 1},
                }
            )

            payload = store.get_payload()

        self.assertEqual(payload["sync_status"]["state"], "idle")
        self.assertEqual(payload["sync_status"]["message"], "待命")
        self.assertEqual(payload["sync_status"]["last_error"], "")
        self.assertEqual(payload["sync_status"]["progress"], {})

    def test_status_snapshot_exposes_server_cache_push_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            payload = store.get_payload()
        push_status = payload["sync_status"]["server_cache_push_status"]
        self.assertEqual(push_status["daily_at"], "14:00")
        self.assertTrue(push_status["next_auto_run_at"])

    def test_status_snapshot_exposes_launchd_runtime_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            state_path = Path(temp_dir) / ".state.json"
            env_path.write_text(
                f"STATE_FILE={state_path}\nXHS_SCHEDULE_DRIVER=launchd\nXHS_BATCH_WINDOW_START=14:00\n",
                encoding="utf-8",
            )
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            write_local_daily_sync_status(
                env_file=str(env_path),
                state_file_path=str(state_path),
                payload={
                    "state": "running",
                    "phase": "collecting",
                    "message": "项目「项目A」正在采集",
                    "current_project": "项目A",
                    "current_project_index": 1,
                    "current_project_total": 2,
                    "current_project_scheduled_at": "2026-03-31T14:20:00+08:00",
                    "next_run_at": "2026-04-01T14:00:00+08:00",
                    "last_success_at": "2026-03-31T14:33:00+08:00",
                    "last_upload_success_at": "2026-03-31T14:35:00+08:00",
                },
            )
            store = MonitoringSyncStore(
                env_file=str(env_path),
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=str(env_path)),
            )
            payload = store.get_payload()

        self.assertEqual(payload["sync_status"]["schedule_driver"], "launchd")
        self.assertEqual(payload["sync_status"]["launchd_status"]["state"], "running")
        self.assertEqual(payload["sync_status"]["launchd_status"]["phase"], "collecting")
        self.assertEqual(payload["sync_status"]["launchd_status"]["current_project"], "项目A")
        self.assertEqual(payload["sync_status"]["launchd_status"]["current_project_index"], 1)
        self.assertEqual(payload["sync_status"]["launchd_status"]["next_run_at"], "2026-04-01T14:00:00+08:00")
        self.assertEqual(payload["sync_status"]["launchd_status"]["last_upload_success_at"], "2026-03-31T14:35:00+08:00")

    def test_push_server_cache_updates_manual_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.push_current_cache_to_server",
                return_value={"ok": True, "account_count": 1, "updated_at": "2026-03-31T14:00:05+08:00"},
            ) as push_mock:
                result = store.push_server_cache(auto=False)
        self.assertTrue(result["ok"])
        push_mock.assert_called_once()
        payload = store.get_payload()
        push_status = payload["sync_status"]["server_cache_push_status"]
        self.assertEqual(push_status["state"], "success")
        self.assertEqual(push_status["mode"], "manual")
        self.assertTrue(push_status["last_success_at"])

    def test_compute_next_auto_server_push_retries_after_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            now = datetime.fromisoformat("2026-03-31T14:05:00+08:00")
            store._auto_project_last_attempt_at["项目A"] = datetime.fromisoformat("2026-03-31T14:02:00+08:00").timestamp()
            next_run = store._compute_next_auto_server_push_locked(now)
        self.assertEqual(next_run.isoformat(timespec="seconds"), "2026-03-31T14:12:00+08:00")

    def test_status_snapshot_exposes_schedule_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [
                    {"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"},
                    {"url": "https://www.xiaohongshu.com/user/profile/u2", "active": True, "project": "项目B"},
                ],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            with patch("xhs_feishu_monitor.local_stats_app.server.load_settings") as load_settings_mock:
                load_settings_mock.return_value = Settings(
                    xhs_spread_schedule_enabled=True,
                    xhs_batch_schedule_interval_minutes=30,
                    xhs_batch_window_start="14:00",
                    xhs_batch_window_end="16:00",
                    xhs_batch_min_accounts_per_run=1,
                    xhs_batch_max_accounts_per_run=1,
                    xhs_batch_slot_offset_seconds=300,
                )
                payload = store.get_payload()
        schedule_plan = payload["sync_status"]["schedule_plan"]
        self.assertTrue(schedule_plan["enabled"])
        self.assertEqual(schedule_plan["window_start"], "14:00")
        self.assertEqual(schedule_plan["window_end"], "16:00")
        self.assertEqual(schedule_plan["per_run"], 1)
        self.assertEqual(len(schedule_plan["projects"]), 2)

    def test_sync_loop_updates_dashboard_before_feishu_upload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            dashboard_store = DashboardStore(env_file=f"{temp_dir}/.env")
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=dashboard_store,
            )
            store._running = True
            store._current_sync_urls = ["https://www.xiaohongshu.com/user/profile/u1"]
            store._current_sync_project = "项目A"
            store._status = {
                "state": "running",
                "message": "开始同步",
                "started_at": "2026-03-23T18:00:00+08:00",
                "finished_at": "",
                "last_success_at": "",
                "last_error": "",
                "pending": False,
                "progress": {},
                "summary": {},
            }
            reports = [
                {
                    "captured_at": "2026-03-23T18:05:00+08:00",
                    "source_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "profile": {
                        "profile_user_id": "u1",
                        "nickname": "账号A",
                        "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                        "fans_count_text": "100",
                        "interaction_count_text": "200",
                    },
                    "works": [{"title_copy": "作品A"}],
                }
            ]
            settings = SimpleNamespace(validate_for_sync=lambda: None)
            with (
                patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings),
                patch.object(store, "_ensure_login_ready_for_sync", return_value=None),
                patch("xhs_feishu_monitor.local_stats_app.server.load_reports_for_sync", return_value=reports) as load_reports,
                patch(
                    "xhs_feishu_monitor.local_stats_app.server.build_dashboard_payload_with_reports",
                    return_value={"generated_at": "2026-03-23T18:05:00+08:00", "latest_date": "2026-03-23"},
                ),
            ):
                store._sync_loop()
        self.assertEqual(store._status["state"], "success")
        self.assertIn("本地缓存已更新", store._status["message"])
        self.assertEqual(load_reports.call_args.kwargs["project"], "项目A")

    def test_upload_progress_does_not_override_dashboard_success_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            store._status["state"] = "success"
            store._status["message"] = "看板已更新"
            store._upload_status = {
                "state": "running",
                "message": "飞书后台上传中",
                "started_at": "2026-03-23T18:00:00+08:00",
                "finished_at": "",
                "last_success_at": "",
                "last_error": "",
                "pending": False,
                "progress": {},
                "summary": {},
            }
            store._handle_upload_progress_update(
                {
                    "phase": "sync",
                    "current": 1,
                    "total": 3,
                    "account": "账号A",
                    "works": 30,
                    "success_count": 1,
                    "failed_count": 0,
                }
            )
        self.assertEqual(store._status["state"], "success")
        self.assertEqual(store._status["message"], "看板已更新")
        self.assertEqual(store._upload_status["progress"]["phase"], "sync")

    def test_retry_feishu_upload_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            result = store.retry_feishu_upload()
        self.assertFalse(result["ok"])
        self.assertIn("旧的外部协作上传入口已移除", result["message"])

    def test_dashboard_store_uses_stale_cache_when_refresh_fails(self) -> None:
        store = DashboardStore(env_file="/tmp/test.env", cache_seconds=0)
        with patch(
            "xhs_feishu_monitor.local_stats_app.server.load_dashboard_payload",
            side_effect=[
                {"generated_at": "2026-03-18T13:00:00+08:00", "latest_date": "2026-03-18"},
                RuntimeError("boom"),
            ],
        ):
            first = store.get_payload(force=True)
            second = store.get_payload(force=True)
        self.assertFalse(first["stale"])
        self.assertTrue(second["stale"])
        self.assertEqual(second["load_error"], "boom")
        self.assertEqual(second["latest_date"], "2026-03-18")

    def test_dashboard_store_prefers_local_override_even_on_force_refresh(self) -> None:
        store = DashboardStore(env_file="/tmp/test.env", cache_seconds=0)
        store.set_local_override({"generated_at": "2026-03-18T14:00:00+08:00", "latest_date": "2026-03-18"})
        with patch("xhs_feishu_monitor.local_stats_app.server.load_dashboard_payload", side_effect=AssertionError("unexpected refresh")):
            payload = store.get_payload(force=True)
        self.assertTrue(payload["local_override"])
        self.assertEqual(payload["latest_date"], "2026-03-18")

    def test_build_dashboard_payload_with_reports_updates_frontend_first(self) -> None:
        base_payload = {
            "generated_at": "2026-03-17T14:00:00+08:00",
            "latest_date": "2026-03-17",
            "updated_at": "2026-03-17T14:00:00+08:00",
            "series_meta": {"mode": "daily", "update_time": "14:00"},
            "portal": {"accounts": 1},
            "series": [{"date": "2026-03-17", "fans": 80, "likes": 10, "comments": 2, "works": 1, "accounts": 1}],
            "account_series": {
                "u2": [{"date": "2026-03-17", "fans": 80, "interaction": 120, "likes": 10, "comments": 2, "works": 1}]
            },
            "accounts": [
                {
                    "account_id": "u2",
                    "account": "旧账号",
                    "date": "2026-03-17",
                    "fans": 80,
                    "interaction": 120,
                    "works": 1,
                    "likes": 10,
                    "comments": 2,
                    "weekly_summary": "旧周对比",
                    "profile_url": "https://profile-u2",
                    "top_title": "旧作品",
                    "top_like": 10,
                    "top_url": "https://note-u2",
                }
            ],
            "rankings": {
                "单条点赞排行": [
                    {
                        "rank": 1,
                        "account_id": "u2",
                        "account": "旧账号",
                        "title": "旧作品",
                        "metric": 10,
                        "summary": "点赞 10",
                        "profile_url": "https://profile-u2",
                        "note_url": "https://note-u2",
                        "cover_url": "https://img-u2",
                    }
                ],
                "单条评论排行": [],
                "单条第二天增长排行": [
                    {
                        "rank": 1,
                        "account_id": "u2",
                        "account": "旧账号",
                        "title": "旧作品",
                        "metric": 3,
                        "summary": "次日互动 +3",
                        "profile_url": "https://profile-u2",
                        "note_url": "https://note-u2",
                        "cover_url": "https://img-u2",
                    }
                ],
            },
            "alerts": [{"account_id": "u2", "title": "旧预警"}],
        }
        reports = [
            {
                "captured_at": "2026-03-18T14:00:00+08:00",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://profile-u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                },
                "works": [
                    {
                        "title_copy": "作品A",
                        "note_type": "image",
                        "like_count": 20,
                        "comment_count": 5,
                        "cover_url": "https://img-u1",
                        "note_url": "https://note-u1",
                    }
                ],
            }
        ]

        payload = build_dashboard_payload_with_reports(base_payload=base_payload, reports=reports)

        self.assertEqual(payload["latest_date"], "2026-03-18")
        self.assertEqual(payload["updated_at"], "2026-03-18T14:00:00+08:00")
        self.assertIn("u1", payload["account_series"])
        self.assertEqual(payload["account_series"]["u1"][-1]["fans"], 100)
        self.assertEqual(payload["accounts"][0]["account_id"], "u1")
        self.assertEqual(payload["rankings"]["单条点赞排行"][0]["account_id"], "u1")
        self.assertEqual(payload["rankings"]["单条评论排行"][0]["account_id"], "u1")
        self.assertEqual(payload["rankings"]["单条第二天增长排行"][0]["account_id"], "u2")
        self.assertEqual(payload["alerts"][0]["title"], "旧预警")

    def test_build_dashboard_payload_with_reports_preserves_existing_detail_when_report_degrades(self) -> None:
        base_payload = {
            "generated_at": "2026-03-17T14:00:00+08:00",
            "latest_date": "2026-03-17",
            "updated_at": "2026-03-17T14:00:00+08:00",
            "series_meta": {"mode": "daily", "update_time": "14:00"},
            "portal": {"accounts": 1},
            "series": [{"date": "2026-03-17", "fans": 80, "likes": 10, "comments": 12, "works": 1, "accounts": 1}],
            "account_series": {
                "u1": [{"date": "2026-03-17", "fans": 80, "interaction": 120, "likes": 10, "comments": 12, "works": 1}]
            },
            "accounts": [
                {
                    "account_id": "u1",
                    "account": "旧账号",
                    "date": "2026-03-17",
                    "fans": 80,
                    "interaction": 120,
                    "works": 1,
                    "likes": 10,
                    "comments": 12,
                    "weekly_summary": "旧周对比",
                    "profile_url": "https://profile-u1",
                    "top_title": "旧作品",
                    "top_like": 10,
                    "top_url": "https://note-u1",
                }
            ],
            "rankings": {
                "单条点赞排行": [
                    {
                        "rank": 1,
                        "account_id": "u1",
                        "account": "旧账号",
                        "title": "作品A",
                        "metric": 10,
                        "summary": "点赞 10",
                        "profile_url": "https://profile-u1",
                        "note_url": "https://note-u1",
                        "cover_url": "https://img-u1",
                    }
                ],
                "单条评论排行": [
                    {
                        "rank": 1,
                        "account_id": "u1",
                        "account": "旧账号",
                        "title": "作品A",
                        "metric": 12,
                        "summary": "评论 12",
                        "profile_url": "https://profile-u1",
                        "note_url": "https://note-u1",
                        "cover_url": "https://img-u1",
                    }
                ],
                "单条第二天增长排行": [],
            },
            "alerts": [],
        }
        reports = [
            {
                "captured_at": "2026-03-18T14:00:00+08:00",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://profile-u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                },
                "works": [
                    {
                        "title_copy": "作品A",
                        "note_type": "image",
                        "like_count": 20,
                        "comment_count": None,
                        "cover_url": "https://img-u1",
                        "note_url": "",
                        "note_id": "",
                    }
                ],
            }
        ]

        payload = build_dashboard_payload_with_reports(base_payload=base_payload, reports=reports)

        self.assertEqual(payload["accounts"][0]["comments"], 12)
        self.assertEqual(payload["accounts"][0]["top_url"], "https://note-u1")
        self.assertEqual(payload["rankings"]["单条点赞排行"][0]["note_url"], "https://note-u1")
        self.assertEqual(payload["rankings"]["单条评论排行"][0]["account_id"], "u1")

    def test_monitoring_payload_falls_back_when_profile_lookup_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            with patch("xhs_feishu_monitor.local_stats_app.server.load_profile_table_rows", side_effect=RuntimeError("lookup failed")):
                payload = store.get_payload()
        self.assertEqual(payload["profile_lookup_error"], "lookup failed")
        self.assertEqual(payload["entries"][0]["project"], "项目A")
        self.assertEqual(payload["entries"][0]["display_name"], "u1")

    def test_monitoring_payload_uses_dashboard_override_for_new_account(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            dashboard_store = DashboardStore(env_file=f"{temp_dir}/.env")
            dashboard_store.set_local_override(
                {
                    "accounts": [
                        {
                            "account_id": "u1",
                            "account": "本地新账号",
                            "profile_url": "https://profile-u1",
                            "fans": 100,
                            "interaction": 200,
                            "works": 9,
                        }
                    ]
                }
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=dashboard_store,
            )
            with patch("xhs_feishu_monitor.local_stats_app.server.load_profile_table_rows", return_value=[]):
                payload = store.get_payload()
        self.assertEqual(payload["entries"][0]["display_name"], "本地新账号")
        self.assertEqual(payload["entries"][0]["summary_text"], "粉丝 100 · 获赞 200 · 作品 9")

    def test_run_login_state_self_check_errors_when_chrome_cookie_export_fails(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        with patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings):
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.export_xiaohongshu_cookie_header",
                side_effect=RuntimeError("未找到 Chrome Cookies 数据库"),
            ):
                payload = run_login_state_self_check(
                    env_file="/tmp/test.env",
                    sample_url="https://www.xiaohongshu.com/user/profile/u1",
                )
        self.assertEqual(payload["state"], "error")
        self.assertEqual(payload["cookie_source"], "chrome_profile")
        self.assertIn("Chrome 登录态读取失败", payload["message"])

    def test_run_login_state_self_check_warns_when_note_ids_missing(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        with patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings):
            with patch("xhs_feishu_monitor.local_stats_app.server.export_xiaohongshu_cookie_header", return_value="a=b"):
                with patch(
                    "xhs_feishu_monitor.local_stats_app.server.load_profile_report_payload",
                    return_value={"initial_state": {}, "final_url": "https://www.xiaohongshu.com/user/profile/u1"},
                ):
                    with patch(
                        "xhs_feishu_monitor.local_stats_app.server.build_profile_report",
                        return_value={
                            "profile": {"nickname": "账号A", "profile_user_id": "u1", "fans_count_text": "123"},
                            "works": [{"note_id": ""}, {"note_id": ""}],
                        },
                    ):
                        payload = run_login_state_self_check(
                            env_file="/tmp/test.env",
                            sample_url="https://www.xiaohongshu.com/user/profile/u1",
                        )
        self.assertEqual(payload["state"], "warning")
        self.assertFalse(payload["detail_ready"])
        self.assertEqual(payload["work_count"], 2)
        self.assertEqual(payload["note_id_count"], 0)
        self.assertEqual(payload["sample_account"], "账号A")

    def test_run_login_state_self_check_warns_when_sample_fetch_is_transient_failure(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        with patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings):
            with patch("xhs_feishu_monitor.local_stats_app.server.export_xiaohongshu_cookie_header", return_value="a=b"):
                with patch(
                    "xhs_feishu_monitor.local_stats_app.server.load_profile_report_payload",
                    side_effect=RuntimeError("HTML 中未找到可解析的 __INITIAL_STATE__ 数据"),
                ):
                    payload = run_login_state_self_check(
                        env_file="/tmp/test.env",
                        sample_url="https://www.xiaohongshu.com/user/profile/u1",
                    )
        self.assertEqual(payload["state"], "warning")
        self.assertIn("样本账号抓取异常", payload["message"])
        self.assertFalse(login_state_requires_interactive_login(payload))

    def test_run_login_state_self_check_ok_when_note_ids_exist(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        with patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings):
            with patch("xhs_feishu_monitor.local_stats_app.server.export_xiaohongshu_cookie_header", return_value="a=b"):
                with patch(
                    "xhs_feishu_monitor.local_stats_app.server.load_profile_report_payload",
                    return_value={"initial_state": {}, "final_url": "https://www.xiaohongshu.com/user/profile/u1"},
                ):
                    with patch(
                        "xhs_feishu_monitor.local_stats_app.server.build_profile_report",
                        return_value={
                            "profile": {"nickname": "账号A", "profile_user_id": "u1", "fans_count_text": "123"},
                            "works": [{"note_id": "n1"}, {"note_id": "n2"}],
                        },
                    ):
                        payload = run_login_state_self_check(
                            env_file="/tmp/test.env",
                            sample_url="https://www.xiaohongshu.com/user/profile/u1",
                        )
        self.assertEqual(payload["state"], "ok")
        self.assertTrue(payload["detail_ready"])
        self.assertEqual(payload["note_id_count"], 2)

    def test_wait_for_xiaohongshu_login_opens_window_and_returns_after_success(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        wait_events = []
        check_results = [
            build_login_state_payload(state="error", message="样本账号返回了空结果，登录态可能已过期"),
            build_login_state_payload(state="error", message="样本账号返回了空结果，登录态可能已过期"),
            build_login_state_payload(state="ok", message="登录态正常，样本账号已拿到作品明细能力。"),
        ]
        with patch(
            "xhs_feishu_monitor.local_stats_app.server.run_login_state_self_check",
            side_effect=check_results,
        ):
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.open_xiaohongshu_login_window",
                return_value=True,
            ) as open_mock:
                with patch("xhs_feishu_monitor.local_stats_app.server.time.sleep", return_value=None):
                    payload = wait_for_xiaohongshu_login(
                        env_file="/tmp/test.env",
                        settings=settings,
                        sample_url="https://www.xiaohongshu.com/user/profile/u1",
                        on_wait=lambda item: wait_events.append(dict(item)),
                        timeout_seconds=10,
                        poll_seconds=1,
                    )
        self.assertEqual(payload["state"], "ok")
        self.assertTrue(open_mock.called)
        self.assertGreaterEqual(len(wait_events), 1)
        self.assertIn("网页登录窗口", wait_events[0]["message"])
        self.assertTrue(wait_events[0]["login_window_opened"])

    def test_wait_for_xiaohongshu_login_waits_without_timeout_for_auto_mode(self) -> None:
        settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
        check_results = [
            build_login_state_payload(state="error", message="样本账号返回了空结果，登录态可能已过期"),
            build_login_state_payload(state="error", message="样本账号返回了空结果，登录态可能已过期"),
            build_login_state_payload(state="ok", message="登录态正常，样本账号已拿到作品明细能力。"),
        ]
        with patch(
            "xhs_feishu_monitor.local_stats_app.server.run_login_state_self_check",
            side_effect=check_results,
        ):
            with patch(
                "xhs_feishu_monitor.local_stats_app.server.open_xiaohongshu_login_window",
                return_value=True,
            ):
                with patch("xhs_feishu_monitor.local_stats_app.server.time.sleep", return_value=None):
                    payload = wait_for_xiaohongshu_login(
                        env_file="/tmp/test.env",
                        settings=settings,
                        sample_url="https://www.xiaohongshu.com/user/profile/u1",
                        timeout_seconds=0,
                        poll_seconds=1,
                    )
        self.assertEqual(payload["state"], "ok")
        self.assertTrue(payload["login_window_opened"])

    def test_open_xiaohongshu_login_window_uses_default_chrome(self) -> None:
        settings = Settings(
            xhs_fetch_mode="requests",
            xhs_chrome_cookie_profile=DEFAULT_CHROME_PROFILE_ROOT,
        )
        with patch("xhs_feishu_monitor.local_stats_app.server.subprocess.Popen") as popen_mock:
            opened = open_xiaohongshu_login_window(
                settings=settings,
                target_url="https://www.xiaohongshu.com/user/profile/u1",
            )
        self.assertTrue(opened)
        self.assertEqual(
            popen_mock.call_args.args[0],
            ["open", "-a", "Google Chrome", "https://www.xiaohongshu.com/user/profile/u1"],
        )

    def test_monitoring_payload_includes_login_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            login_state_store = LoginStateStore(env_file=f"{temp_dir}/.env", urls_file=str(path), cache_seconds=999)
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
                login_state_store=login_state_store,
            )
            with patch.object(
                login_state_store,
                "get_payload",
                return_value={"state": "ok", "message": "登录态正常", "checking": False},
            ):
                payload = store.get_payload()
        self.assertEqual(payload["login_state"]["state"], "ok")
        self.assertEqual(payload["login_state"]["message"], "登录态正常")

    def test_login_state_store_run_check_opens_login_window_on_login_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            login_state_store = LoginStateStore(env_file=f"{temp_dir}/.env", urls_file=str(path), cache_seconds=999)
            login_state_store._sample_url = "https://www.xiaohongshu.com/user/profile/u1"
            settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile=DEFAULT_CHROME_PROFILE_ROOT)

            def fake_wait_for_login(**kwargs):
                kwargs["on_wait"](
                    build_login_state_payload(
                        state="error",
                        message="检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。",
                        login_window_opened=True,
                    )
                )
                return build_login_state_payload(
                    state="ok",
                    message="登录态正常，样本账号已拿到作品明细能力。",
                    login_window_opened=True,
                )

            with patch("xhs_feishu_monitor.local_stats_app.server.load_settings", return_value=settings):
                with patch(
                    "xhs_feishu_monitor.local_stats_app.server.wait_for_xiaohongshu_login",
                    side_effect=fake_wait_for_login,
                ) as wait_mock:
                    login_state_store._run_check()

            self.assertTrue(wait_mock.called)
            payload = login_state_store.get_payload()
            self.assertEqual(payload["state"], "ok")
            self.assertTrue(payload["login_window_opened"])

    def test_auto_sync_waits_for_login_without_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_monitored_entries(
                f"{temp_dir}/urls.txt",
                [{"url": "https://www.xiaohongshu.com/user/profile/u1", "active": True, "project": "项目A"}],
            )
            store = MonitoringSyncStore(
                env_file=f"{temp_dir}/.env",
                urls_file=str(path),
                dashboard_store=DashboardStore(env_file=f"{temp_dir}/.env"),
            )
            settings = Settings(xhs_fetch_mode="requests", xhs_chrome_cookie_profile="/tmp/profile")
            with patch("xhs_feishu_monitor.local_stats_app.server.wait_for_xiaohongshu_login") as wait_mock:
                wait_mock.return_value = build_login_state_payload(state="ok", message="登录态正常")
                store._ensure_login_ready_for_sync(
                    settings=settings,
                    sample_url="https://www.xiaohongshu.com/user/profile/u1",
                    mode="auto",
                )
        self.assertEqual(wait_mock.call_args.kwargs["timeout_seconds"], 0)

    def test_export_single_account_rankings_writes_like_and_comment_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            summary = export_single_account_rankings(
                payload={
                    "accounts": [
                        {
                            "account_id": "u1",
                            "account": "账号A",
                            "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                        }
                    ],
                    "rankings": {
                        "单条点赞排行": [
                            {
                                "rank": 1,
                                "account_id": "u1",
                                "account": "账号A",
                                "title": "作品1",
                                "metric": 99,
                                "summary": "点赞榜",
                                "note_url": "https://www.xiaohongshu.com/discovery/item/n1",
                                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                            }
                        ],
                        "单条评论排行": [
                            {
                                "rank": 1,
                                "account_id": "u1",
                                "account": "账号A",
                                "title": "作品2",
                                "metric": 12,
                                "summary": "评论榜",
                                "comment_basis": "评论预览下限",
                            }
                        ],
                    },
                },
                account_id="u1",
                project="项目A",
                export_dir=temp_dir,
            )
            self.assertEqual(summary["like_count"], 1)
            self.assertEqual(summary["comment_count"], 1)
            self.assertTrue((Path(summary["files"]["like_csv"])).exists())
            self.assertTrue((Path(summary["files"]["comment_csv"])).exists())
            self.assertTrue((Path(summary["summary_path"])).exists())
            self.assertTrue((Path(summary["files"]["review_markdown"])).exists())
            self.assertTrue((Path(summary["latest_summary_path"])).exists())
            self.assertIn("项目A", summary["export_dir"])
            self.assertRegex(Path(summary["export_dir"]).name, r"^\d{4}-\d{2}-\d{2}_\d{6}$")

    def test_export_project_rankings_writes_project_snapshot_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            summary = export_project_rankings(
                payload={
                    "accounts": [
                        {"account_id": "u1", "account": "账号A", "profile_url": "https://www.xiaohongshu.com/user/profile/u1"},
                        {"account_id": "u2", "account": "账号B", "profile_url": "https://www.xiaohongshu.com/user/profile/u2"},
                    ],
                    "rankings": {
                        "单条点赞排行": [
                            {"rank": 1, "account_id": "u1", "account": "账号A", "title": "作品1", "metric": 99},
                            {"rank": 2, "account_id": "u2", "account": "账号B", "title": "作品2", "metric": 88},
                        ],
                        "单条评论排行": [
                            {"rank": 1, "account_id": "u1", "account": "账号A", "title": "作品3", "metric": 12},
                        ],
                    },
                },
                project="项目A",
                account_ids=["u1", "u2"],
                export_dir=temp_dir,
            )
            self.assertEqual(summary["account_count"], 2)
            self.assertEqual(summary["like_count"], 2)
            self.assertEqual(summary["comment_count"], 1)
            self.assertTrue(Path(summary["files"]["project_index_csv"]).exists())
            self.assertTrue(Path(summary["files"]["project_review_markdown"]).exists())
            self.assertTrue(Path(summary["summary_path"]).exists())
            self.assertTrue(Path(summary["latest_summary_path"]).exists())
            self.assertRegex(Path(summary["export_dir"]).name, r"^\d{4}-\d{2}-\d{2}_\d{6}$")

    def test_export_project_rankings_generates_compare_files_on_second_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            export_project_rankings(
                payload={
                    "accounts": [{"account_id": "u1", "account": "账号A"}],
                    "rankings": {
                        "单条点赞排行": [{"rank": 1, "account_id": "u1", "title": "作品1", "metric": 9}],
                        "单条评论排行": [{"rank": 1, "account_id": "u1", "title": "评论作品1", "metric": 3}],
                    },
                },
                project="项目A",
                account_ids=["u1"],
                export_dir=temp_dir,
            )
            time.sleep(1)
            summary = export_project_rankings(
                payload={
                    "accounts": [{"account_id": "u1", "account": "账号A"}],
                    "rankings": {
                        "单条点赞排行": [
                            {"rank": 1, "account_id": "u1", "title": "作品2", "metric": 12},
                            {"rank": 2, "account_id": "u1", "title": "作品1", "metric": 11},
                        ],
                        "单条评论排行": [{"rank": 1, "account_id": "u1", "title": "评论作品1", "metric": 5}],
                    },
                },
                project="项目A",
                account_ids=["u1"],
                export_dir=temp_dir,
            )
            self.assertIn("compare", summary)
            self.assertTrue(Path(summary["files"]["project_compare_json"]).exists())
            self.assertTrue(Path(summary["files"]["project_compare_markdown"]).exists())
            self.assertEqual(summary["compare"]["like_count_delta"], 1)
            self.assertEqual(summary["compare"]["comment_count_delta"], 0)
            self.assertEqual(summary["compare"]["changed_accounts"][0]["account"], "账号A")
            self.assertEqual(summary["compare"]["changed_accounts"][0]["like_compare"]["new_entries"][0]["title"], "作品2")
            self.assertEqual(summary["compare"]["changed_accounts"][0]["like_compare"]["moved_down"][0]["title"], "作品1")
            self.assertEqual(summary["compare"]["changed_accounts"][0]["comment_compare"]["new_entries"], [])

    def test_export_single_account_rankings_without_rows_creates_no_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "暂无可导出"):
                export_single_account_rankings(
                    payload={
                        "accounts": [{"account_id": "u1", "account": "账号A"}],
                        "rankings": {
                            "单条点赞排行": [],
                            "单条评论排行": [],
                        },
                    },
                    account_id="u1",
                    project="项目A",
                    export_dir=temp_dir,
                )
            project_dir = Path(temp_dir) / "项目A"
            self.assertFalse(project_dir.exists())

    def test_export_project_rankings_without_rows_creates_no_snapshot_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "暂无可导出"):
                export_project_rankings(
                    payload={
                        "accounts": [{"account_id": "u1", "account": "账号A"}],
                        "rankings": {
                            "单条点赞排行": [],
                            "单条评论排行": [],
                        },
                    },
                    project="项目A",
                    account_ids=["u1"],
                    export_dir=temp_dir,
                )
            project_dir = Path(temp_dir) / "项目A"
            self.assertFalse(project_dir.exists())

    def test_refresh_project_export_snapshots_writes_latest_snapshot_for_current_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = {
                "accounts": [
                    {"account_id": "u1", "account": "账号A", "profile_url": "https://www.xiaohongshu.com/user/profile/u1"},
                    {"account_id": "u2", "account": "账号B", "profile_url": "https://www.xiaohongshu.com/user/profile/u2"},
                ],
                "rankings": {
                    "单条点赞排行": [
                        {"rank": 1, "account_id": "u1", "account": "账号A", "title": "作品1", "metric": 99},
                        {"rank": 2, "account_id": "u2", "account": "账号B", "title": "作品2", "metric": 88},
                    ],
                    "单条评论排行": [
                        {"rank": 1, "account_id": "u1", "account": "账号A", "title": "作品3", "metric": 12},
                    ],
                },
            }
            reports = [
                {"profile": {"profile_user_id": "u1"}, "project": "项目A"},
                {"profile": {"profile_user_id": "u2"}, "project": "项目A"},
            ]
            summaries = refresh_project_export_snapshots(
                payload=payload,
                reports=reports,
                export_dir=temp_dir,
            )
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0]["project"], "项目A")
            latest_summary = Path(temp_dir) / "项目A" / "最近一次项目导出.json"
            self.assertTrue(latest_summary.exists())

    def test_build_sync_progress_calculates_phase_and_overall_percent(self) -> None:
        collect_progress = build_sync_progress(
            phase="collect",
            current=2,
            total=4,
            account="账号A",
            works=3,
            status="success",
            success_count=2,
            failed_count=0,
            started_at="2026-03-18T14:00:00+08:00",
            now=datetime.fromisoformat("2026-03-18T14:01:00+08:00"),
        )
        sync_progress = build_sync_progress(
            phase="sync",
            current=3,
            total=4,
            account="账号A",
            works=3,
            success_count=3,
            failed_count=1,
            started_at="2026-03-18T14:00:00+08:00",
            now=datetime.fromisoformat("2026-03-18T14:01:00+08:00"),
        )
        self.assertEqual(collect_progress["overall_percent"], 25)
        self.assertEqual(sync_progress["overall_percent"], 88)
        self.assertIn("账号A", sync_progress["detail_text"])
        self.assertEqual(sync_progress["elapsed_text"], "1分")
        self.assertTrue(sync_progress["eta_seconds"] > 0)
        self.assertEqual(sync_progress["success_count"], 3)
        self.assertEqual(sync_progress["failed_count"], 1)

    def test_build_sync_progress_supports_login_phase(self) -> None:
        progress = build_sync_progress(
            phase="login",
            current=0,
            total=1,
            status="检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。",
            started_at="2026-03-18T14:00:00+08:00",
            now=datetime.fromisoformat("2026-03-18T14:01:00+08:00"),
        )
        self.assertEqual(progress["phase_label"], "等待网页登录")
        self.assertEqual(progress["overall_percent"], 0)
        self.assertEqual(progress["detail_text"], "检测到小红书未登录，已弹出网页登录窗口，完成登录后会自动继续采集。")
        self.assertEqual(progress["elapsed_text"], "1分")

    def test_build_sync_progress_supports_collect_running_phase(self) -> None:
        progress = build_sync_progress(
            phase="collect",
            current=1,
            total=10,
            account="项目A",
            status="running",
            started_at="2026-03-31T15:40:00+08:00",
            now=datetime.fromisoformat("2026-03-31T15:40:30+08:00"),
        )
        self.assertEqual(progress["phase_label"], "抓取账号数据")
        self.assertEqual(progress["detail_text"], "正在抓取第 1/10 个账号 · 项目A")
        self.assertEqual(progress["overall_percent"], 2)


if __name__ == "__main__":
    unittest.main()
