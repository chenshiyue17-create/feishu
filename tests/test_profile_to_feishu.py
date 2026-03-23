from __future__ import annotations

import unittest

from xhs_feishu_monitor.profile_to_feishu import build_profile_feishu_fields


class ProfileToFeishuTest(unittest.TestCase):
    def test_build_profile_feishu_fields(self) -> None:
        report = {
            "captured_at": "2026-03-17T16:40:50+08:00",
            "profile": {
                "profile_url": "https://www.xiaohongshu.com/user/profile/demo",
                "profile_user_id": "user_demo_001",
                "nickname": "老板电器-全国福利官🐙",
                "red_id": "94341532229",
                "desc": "账号简介",
                "ip_location": "上海",
                "follows_count_text": "10+",
                "fans_count_text": "10+",
                "interaction_count_text": "10+",
                "visible_work_count": 2,
                "total_work_count": None,
                "work_count_display_text": "2+",
                "work_count_exact": False,
            },
            "works": [
                {"title_copy": "标题1", "like_count": 10, "like_count_text": "10", "note_type": "video", "note_id": ""},
                {"title_copy": "标题2", "like_count": 4, "like_count_text": "4", "note_type": "normal", "note_id": ""},
            ],
        }
        fields = build_profile_feishu_fields(report)
        self.assertEqual(fields["账号"], "老板电器-全国福利官🐙")
        self.assertEqual(fields["账号ID"], "user_demo_001")
        self.assertEqual(fields["平均点赞数"], 7.0)
        self.assertEqual(fields["首页可见作品数"], 2)
        self.assertEqual(fields["作品数展示"], "2+")
        self.assertNotIn("账号总作品数", fields)
        self.assertIn("标题1", fields["作品标题文案"])
        self.assertIn("标题2", fields["首页作品摘要"])


if __name__ == "__main__":
    unittest.main()
