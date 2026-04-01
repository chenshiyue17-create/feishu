from __future__ import annotations

import unittest

from xhs_feishu_monitor.profile_cache_push import _filter_dashboard_payload_by_monitored_entries


class ProfileCachePushTest(unittest.TestCase):
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
