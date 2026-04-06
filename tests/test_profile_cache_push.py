from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from xhs_feishu_monitor.profile_cache_push import (
    _build_upload_payload,
    _dedupe_ranking_rows,
    _filter_dashboard_payload_by_monitored_entries,
    _normalize_upload_dashboard_payload,
)


class ProfileCachePushTest(unittest.TestCase):
    def test_normalize_upload_dashboard_payload_restores_missing_exact_profile_metrics_from_series(self) -> None:
        payload = _normalize_upload_dashboard_payload(
            {
                "accounts": [
                    {"account_id": "u1", "account": "账号A", "fans": 0, "interaction": 0, "works": 30},
                ],
                "account_series": {
                    "u1": [
                        {"date": "2026-04-04", "fans": 174, "interaction": 893, "likes": 10, "comments": 1, "works": 30},
                        {"date": "2026-04-05", "fans": 0, "interaction": 0, "likes": 12, "comments": 0, "works": 30},
                    ]
                },
            }
        )
        self.assertEqual(payload["accounts"][0]["fans"], 174)
        self.assertEqual(payload["accounts"][0]["interaction"], 893)
        self.assertEqual(payload["account_series"]["u1"][-1]["fans"], 174)
        self.assertEqual(payload["account_series"]["u1"][-1]["interaction"], 893)

    def test_filter_dashboard_payload_by_monitored_entries_removes_stale_accounts(self) -> None:
        payload = {
            "accounts": [
                {"account_id": "u1", "account": "旧账号"},
                {"account_id": "u2", "account": "账号B"},
            ],
            "account_series": {
                "u1": [{"date": "2026-04-01", "likes": 1}],
                "u2": [{"date": "2026-04-01", "likes": 2}],
            },
            "rankings": {
                "单条点赞排行": [
                    {"account_id": "u1", "title": "旧作品"},
                    {"account_id": "u2", "title": "作品B"},
                ],
                "单条评论排行": [
                    {"account_id": "u1", "title": "旧作品"},
                ],
            },
            "alerts": [
                {"account_id": "u1", "title": "旧预警"},
                {"account_id": "u2", "title": "预警B"},
            ],
        }
        monitored_entries = [
            {"url": "https://www.xiaohongshu.com/user/profile/u2", "project": "默认项目", "active": True},
        ]

        result = _filter_dashboard_payload_by_monitored_entries(payload, monitored_entries)

        self.assertEqual([item["account_id"] for item in result["accounts"]], ["u2"])
        self.assertEqual(list(result["account_series"]), ["u2"])
        self.assertEqual([item["account_id"] for item in result["rankings"]["单条点赞排行"]], ["u2"])
        self.assertEqual(result["rankings"]["单条评论排行"], [])
        self.assertEqual([item["account_id"] for item in result["alerts"]], ["u2"])

    def test_build_upload_payload_prefers_cache_history_rankings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cache_dir = root / "cache"
            (cache_dir / "dashboard_all.json").parent.mkdir(parents=True, exist_ok=True)
            (cache_dir / "dashboard_all.json").write_text(
                json.dumps(
                    {
                        "updated_at": "2026-04-05T20:00:00+08:00",
                        "latest_date": "2026-04-05",
                        "accounts": [{"account_id": "u1", "account": "账号A"}],
                        "account_series": {"u1": [{"date": "2026-04-05", "fans": 10, "interaction": 20, "likes": 7, "comments": 5, "works": 1}]},
                        "rankings": {"单条点赞排行": [], "单条评论排行": [], "单条第二天增长排行": []},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "dashboard.json").write_text(
                json.dumps(
                    {
                        "updated_at": "2026-04-05T20:00:00+08:00",
                        "latest_date": "2026-04-05",
                        "accounts": [{"account_id": "u1", "account": "账号A"}],
                        "account_series": {"u1": [{"date": "2026-04-05", "fans": 10, "interaction": 20, "likes": 7, "comments": 5, "works": 1}]},
                        "rankings": {"单条点赞排行": [], "单条评论排行": [], "单条第二天增长排行": []},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (project_dir / "tracked_works.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "fingerprint": "fp-1",
                                "account_id": "u1",
                                "account": "账号A",
                                "profile_url": "https://example.com/u1",
                                "title_copy": "作品A",
                                "note_type": "normal",
                                "cover_url": "https://example.com/a.jpg",
                                "note_url": "https://example.com/note/a",
                                "first_seen_at": "2026-04-04T10:00:00+08:00",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (project_dir / "tracked_work_history.json").write_text(
                json.dumps(
                    [
                        {"fields": {"作品指纹": "fp-1", "日期文本": "2026-04-04", "点赞数": 5, "评论数": 3}},
                        {"fields": {"作品指纹": "fp-1", "日期文本": "2026-04-05", "点赞数": 7, "评论数": 5}},
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            env_path = root / ".env"
            env_path.write_text(f"PROJECT_CACHE_DIR={cache_dir}\nSTATE_FILE={root / '.state.json'}\nSERVER_CACHE_PUSH_URL=http://example.com\n", encoding="utf-8")
            urls_path = root / "urls.txt"
            urls_path.write_text("默认项目 https://www.xiaohongshu.com/user/profile/u1\n", encoding="utf-8")

            payload = _build_upload_payload(env_file=str(env_path), urls_file=str(urls_path))

        self.assertIn("默认项目", payload["dashboard_payload"]["history_rankings"])
        project_history = payload["dashboard_payload"]["history_rankings"]["默认项目"]
        self.assertIn("2026-04-04", project_history)
        self.assertIn("2026-04-05", project_history)
        self.assertEqual(project_history["2026-04-05"]["comments"][0]["metric"], 5)
        self.assertEqual(project_history["2026-04-05"]["growth"][0]["metric"], 4)

    def test_dedupe_ranking_rows_prefers_linked_and_higher_metric_row(self) -> None:
        rows = _dedupe_ranking_rows(
            [
                {
                    "rank": 2,
                    "account_id": "u1",
                    "account": "账号A",
                    "title": "同一作品",
                    "metric": 94,
                    "cover_url": "https://example.com/a.jpg!old",
                    "note_url": "",
                },
                {
                    "rank": 1,
                    "account_id": "u1",
                    "account": "账号A",
                    "title": "同一作品",
                    "metric": 95,
                    "cover_url": "https://example.com/a.jpg!new",
                    "note_url": "https://example.com/note/1",
                },
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric"], 95)
        self.assertEqual(rows[0]["note_url"], "https://example.com/note/1")


if __name__ == "__main__":
    unittest.main()
