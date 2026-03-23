from __future__ import annotations

import unittest
from datetime import date

from xhs_feishu_monitor.profile_works_to_feishu import (
    build_work_calendar_fields,
    build_work_feishu_fields,
    build_work_fingerprint,
    build_work_weekly_fields,
    normalize_cover_asset_key,
    select_work_weekly_baseline,
)


class ProfileWorksToFeishuTest(unittest.TestCase):
    def test_build_work_fingerprint_stable(self) -> None:
        a = build_work_fingerprint(profile_user_id="u1", title="标题", cover_url="https://a")
        b = build_work_fingerprint(profile_user_id="u1", title="标题", cover_url="https://a")
        self.assertEqual(a, b)

    def test_cover_asset_key_ignores_rotating_prefix(self) -> None:
        first = normalize_cover_asset_key(
            "https://sns-webpic-qc.xhscdn.com/202603171640/hash_a/1040g00831tp2p124n80g5q79mr6dtlat05j06qo!nc_n_nwebp_mw_1"
        )
        second = normalize_cover_asset_key(
            "https://sns-webpic-qc.xhscdn.com/202603171725/hash_b/1040g00831tp2p124n80g5q79mr6dtlat05j06qo!nc_n_webp_mw_1"
        )
        self.assertEqual(first, second)

    def test_build_work_feishu_fields(self) -> None:
        report = {
            "captured_at": "2026-03-17T16:40:50+08:00",
            "profile": {
                "profile_user_id": "u1",
                "nickname": "账号A",
                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
            },
        }
        work = {
            "title_copy": "标题文案",
            "index": 1,
            "note_type": "video",
            "like_count": 9,
            "like_count_text": "9",
            "comment_count": 12,
            "comment_count_text": "12",
            "cover_url": "https://img.example.com/1.jpg",
            "xsec_token": "token",
            "note_id": "",
            "note_url": "",
        }
        fields = build_work_feishu_fields(report=report, work=work)
        self.assertEqual(fields["账号"], "账号A")
        self.assertEqual(fields["展示序号"], 2)
        self.assertEqual(fields["点赞数"], 9)
        self.assertEqual(fields["评论数"], 12)
        self.assertEqual(fields["评论文本"], "12")
        self.assertIn("note_id 缺失", fields["备注"])

    def test_build_work_calendar_fields(self) -> None:
        report = {
            "captured_at": "2026-03-17T16:40:50+08:00",
            "profile": {
                "profile_user_id": "u1",
                "nickname": "账号A",
                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
            },
        }
        work = {
            "title_copy": "标题文案",
            "index": 1,
            "note_type": "video",
            "like_count": 9,
            "like_count_text": "9",
            "comment_count": 12,
            "comment_count_text": "12",
            "cover_url": "https://img.example.com/1.jpg",
            "xsec_token": "token",
            "note_id": "abc",
            "note_url": "https://www.xiaohongshu.com/explore/abc",
        }
        fields = build_work_calendar_fields(report=report, work=work)
        self.assertEqual(fields["日历键"][:10], "2026-03-17")
        self.assertEqual(fields["点赞数"], 9)
        self.assertEqual(fields["评论数"], 12)

    def test_build_work_weekly_fields(self) -> None:
        current_fields = {"点赞数": 30, "评论数": 18}
        baseline_fields = {"日期文本": "2026-03-10", "点赞数": 20, "评论数": 9}
        weekly = build_work_weekly_fields(current_fields=current_fields, baseline_fields=baseline_fields)
        self.assertEqual(weekly["上周日期文本"], "2026-03-10")
        self.assertEqual(weekly["上周点赞数"], 20)
        self.assertEqual(weekly["点赞周增量"], 10)
        self.assertEqual(weekly["上周评论数"], 9)
        self.assertEqual(weekly["评论周增量"], 9)
        self.assertIn("对比 2026-03-10", weekly["周对比摘要"])

    def test_select_work_weekly_baseline(self) -> None:
        history_index = {
            "fingerprint-a": [
                (date(2026, 3, 11), {"日期文本": "2026-03-11", "点赞数": 12}),
                (date(2026, 3, 10), {"日期文本": "2026-03-10", "点赞数": 10}),
            ]
        }
        baseline = select_work_weekly_baseline(
            history_index=history_index,
            fingerprint="fingerprint-a",
            snapshot_date="2026-03-17",
        )
        self.assertEqual(baseline["日期文本"], "2026-03-10")


if __name__ == "__main__":
    unittest.main()
