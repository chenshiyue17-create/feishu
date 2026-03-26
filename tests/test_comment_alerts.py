from __future__ import annotations

import unittest
from types import SimpleNamespace

from xhs_feishu_monitor.comment_alerts import (
    build_comment_alert_record,
    build_feishu_webhook_sign,
    build_work_comment_fields,
    should_trigger_comment_alert,
)


class CommentAlertsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = SimpleNamespace(
            interaction_alert_delta_threshold=10,
            comment_alert_growth_threshold_percent=10.0,
            comment_alert_min_previous_count=0,
        )
        self.report = {
            "captured_at": "2026-03-18T14:00:00+08:00",
            "profile": {
                "profile_user_id": "u1",
                "nickname": "账号A",
                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
            },
        }
        self.work = {
            "title_copy": "作品A",
            "cover_url": "https://img.example.com/asset_a!nc_n_webp",
            "note_url": "https://www.xiaohongshu.com/explore/n1",
            "comment_count": 22,
            "comment_count_text": "22",
        }

    def test_should_trigger_comment_alert(self) -> None:
        self.assertTrue(
            should_trigger_comment_alert(
                current_comment_count=30,
                previous_comment_count=20,
                growth_rate=10.01,
                settings=self.settings,
            )
        )
        self.assertFalse(
            should_trigger_comment_alert(
                current_comment_count=29,
                previous_comment_count=20,
                growth_rate=45.0,
                settings=self.settings,
            )
        )

    def test_build_work_comment_fields(self) -> None:
        fields, alert = build_work_comment_fields(
            report=self.report,
            work=self.work,
            previous_fields={"评论数": 20},
            settings=self.settings,
        )
        self.assertEqual(fields["评论数"], 22)
        self.assertEqual(fields["评论增量"], 2)
        self.assertEqual(fields["评论增长率"], 10.0)
        self.assertIsNone(alert)

        self.work["comment_count"] = 30
        self.work["comment_count_text"] = "30"
        fields, alert = build_work_comment_fields(
            report=self.report,
            work=self.work,
            previous_fields={"评论数": 20},
            settings=self.settings,
        )
        self.assertEqual(fields["评论预警"], "评论日增>=10")
        self.assertIsNotNone(alert)
        self.assertEqual(alert["预警类型"], "评论预警")

    def test_build_comment_alert_record(self) -> None:
        alert = build_comment_alert_record(
            report=self.report,
            work=self.work,
            current_like_count=15,
            previous_like_count=3,
            like_delta=12,
            current_comment_count=24,
            previous_comment_count=20,
            comment_delta=4,
            growth_rate=20.0,
            alert_type="点赞预警",
        )
        self.assertEqual(alert["账号"], "账号A")
        self.assertEqual(alert["预警类型"], "点赞预警")
        self.assertEqual(alert["点赞增量"], 12)
        self.assertEqual(alert["当前评论数"], 24)
        self.assertEqual(alert["评论增长率"], 20.0)
        self.assertIn("主页链接", alert)
        self.assertIn("作品链接", alert)

    def test_build_feishu_webhook_sign(self) -> None:
        sign = build_feishu_webhook_sign(timestamp="1000", secret="abc")
        self.assertTrue(sign)


if __name__ == "__main__":
    unittest.main()
