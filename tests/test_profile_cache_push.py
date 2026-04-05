from __future__ import annotations

import unittest

from xhs_feishu_monitor.profile_cache_push import (
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


if __name__ == "__main__":
    unittest.main()
