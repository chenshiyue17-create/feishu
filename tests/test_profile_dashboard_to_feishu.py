from __future__ import annotations

import unittest
from datetime import date

from xhs_feishu_monitor.profile_dashboard_to_feishu import (
    build_single_work_ranking_fields,
    build_single_work_rankings,
    build_dashboard_calendar_fields,
    build_dashboard_overview_fields,
    build_dashboard_portal_fields,
    build_dashboard_ranking_fields,
    build_dashboard_trend_fields,
    compute_dashboard_metrics,
    compute_dashboard_portal_metrics,
    parse_exact_number,
    rank_profile_works,
    select_portal_weekly_baseline,
    select_previous_day_work_baseline,
    select_weekly_baseline,
)
from xhs_feishu_monitor.profile_works_to_feishu import build_work_fingerprint


class ProfileDashboardToFeishuTest(unittest.TestCase):
    def setUp(self) -> None:
        self.report = {
            "captured_at": "2026-03-17T18:30:00+08:00",
            "profile": {
                "profile_user_id": "u1",
                "nickname": "账号A",
                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                "desc": "简介",
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
                {
                    "title_copy": "作品1",
                    "note_type": "video",
                    "like_count": 9,
                    "like_count_text": "9",
                    "comment_count": 3,
                    "comment_count_text": "3",
                    "cover_url": "https://img.example.com/first/asset_a!nc_n_webp_mw_1",
                    "note_url": "",
                    "xsec_token": "token1",
                    "index": 0,
                    "note_id": "",
                },
                {
                    "title_copy": "作品2",
                    "note_type": "normal",
                    "like_count": 12,
                    "like_count_text": "12",
                    "comment_count": 6,
                    "comment_count_text": "6",
                    "cover_url": "https://img.example.com/second/asset_b!nc_n_webp_mw_1",
                    "note_url": "https://www.xiaohongshu.com/explore/abc",
                    "xsec_token": "token2",
                    "index": 1,
                    "note_id": "abc",
                },
            ],
        }

    def test_compute_dashboard_metrics(self) -> None:
        metrics = compute_dashboard_metrics(self.report)
        self.assertEqual(metrics["visible_work_count"], 2)
        self.assertEqual(metrics["video_count"], 1)
        self.assertEqual(metrics["image_count"], 1)
        self.assertEqual(metrics["total_likes"], 21)
        self.assertEqual(metrics["total_comments"], 9)
        self.assertEqual(metrics["average_likes"], 10.5)
        self.assertEqual(metrics["average_comments"], 4.5)
        self.assertEqual(metrics["top_like_count"], 12)

    def test_rank_profile_works(self) -> None:
        ranked = rank_profile_works(self.report["works"])
        self.assertEqual(ranked[0]["title_copy"], "作品2")
        self.assertEqual(ranked[1]["title_copy"], "作品1")

    def test_build_dashboard_overview_fields(self) -> None:
        fields = build_dashboard_overview_fields(self.report)
        self.assertEqual(fields["看板键"], "u1")
        self.assertEqual(fields["首页总点赞"], 21)
        self.assertEqual(fields["头部作品标题"], "作品2")
        self.assertEqual(fields["视频占比"], 50.0)
        self.assertEqual(fields["作品数展示"], "2+")
        self.assertIn("TOP3作品摘要", fields)

    def test_build_dashboard_trend_fields(self) -> None:
        fields = build_dashboard_trend_fields(self.report)
        self.assertEqual(fields["账号ID"], "u1")
        self.assertEqual(fields["首页平均点赞"], 10.5)
        self.assertEqual(fields["视频占比"], 50.0)
        self.assertIn("快照ID", fields)

    def test_build_dashboard_calendar_fields(self) -> None:
        fields = build_dashboard_calendar_fields(self.report)
        self.assertEqual(fields["日历键"], "2026-03-17|u1")
        self.assertEqual(fields["首页总评论"], 9)
        self.assertEqual(fields["已采评论作品数"], 2)
        self.assertEqual(fields["作品数展示"], "2+")
        self.assertNotIn("账号总作品数", fields)
        self.assertIn("日历标题", fields)
        self.assertEqual(fields["周对比摘要"], "暂无 7 天前留底，周对比将在积累满 7 天后显示")

    def test_build_single_work_ranking_fields_uses_comment_basis_from_item(self) -> None:
        item = {
            "fingerprint": "note:abc",
            "account_id": "u1",
            "account": "账号A",
            "title_copy": "作品1",
            "note_type": "video",
            "like_count": 9,
            "comment_count": 2,
            "comment_count_is_lower_bound": False,
            "comment_count_basis": "旧缓存",
            "captured_at": "2026-03-17T18:30:00+08:00",
        }
        fields = build_single_work_ranking_fields(item=item, rank_type="单条评论排行", rank=1)
        self.assertEqual(fields["评论数口径"], "旧缓存")
        self.assertEqual(fields["单选"], "旧缓存")

    def test_build_dashboard_calendar_fields_with_weekly_baseline(self) -> None:
        baseline = {
            "日期文本": "2026-03-10",
            "粉丝数": 300,
            "获赞收藏数": 600,
            "首页总点赞": 15,
            "首页总评论": 4,
        }
        report = {
            **self.report,
            "profile": {
                **self.report["profile"],
                "fans_count_text": "321",
                "interaction_count_text": "654",
            },
        }
        fields = build_dashboard_calendar_fields(report, baseline_fields=baseline)
        self.assertEqual(fields["上周日期文本"], "2026-03-10")
        self.assertEqual(fields["上周粉丝数"], 300)
        self.assertEqual(fields["粉丝周增量"], 21)
        self.assertEqual(fields["上周获赞收藏数"], 600)
        self.assertEqual(fields["获赞收藏周增量"], 54)
        self.assertEqual(fields["上周首页总点赞"], 15)
        self.assertEqual(fields["首页总点赞周增量"], 6)
        self.assertEqual(fields["上周首页总评论"], 4)
        self.assertEqual(fields["首页总评论周增量"], 5)
        self.assertIn("对比 2026-03-10", fields["周对比摘要"])

    def test_select_weekly_baseline(self) -> None:
        records = [
            {"fields": {"账号ID": "u1", "日期文本": "2026-03-11", "粉丝数": 310}},
            {"fields": {"账号ID": "u1", "日期文本": "2026-03-10", "粉丝数": 300}},
            {"fields": {"账号ID": "u1", "日期文本": "2026-03-08", "粉丝数": 280}},
            {"fields": {"账号ID": "u2", "日期文本": "2026-03-10", "粉丝数": 900}},
        ]
        baseline = select_weekly_baseline(records=records, account_id="u1", snapshot_date="2026-03-17")
        self.assertEqual(baseline["日期文本"], "2026-03-10")

    def test_build_dashboard_ranking_fields(self) -> None:
        ranked = rank_profile_works(self.report["works"])
        fields = build_dashboard_ranking_fields(report=self.report, work=ranked[0], rank=1)
        self.assertEqual(fields["卡片标签"], "TOP1")
        self.assertEqual(fields["点赞数"], 12)
        self.assertIn("作品链接", fields)

    def test_build_single_work_rankings(self) -> None:
        first_fp = build_work_fingerprint(
            profile_user_id="u1",
            title="作品1",
            cover_url="https://img.example.com/first/asset_a!nc_n_webp_mw_1",
        )
        second_fp = build_work_fingerprint(
            profile_user_id="u1",
            title="作品2",
            cover_url="https://img.example.com/second/asset_b!nc_n_webp_mw_1",
        )
        history_index = {
            first_fp: [
                (date(2026, 3, 16), {"日期文本": "2026-03-16", "点赞数": 8, "评论数": 4}),
            ],
            second_fp: [
                (date(2026, 3, 16), {"日期文本": "2026-03-16", "点赞数": 7, "评论数": 2}),
            ],
        }
        rankings = build_single_work_rankings(reports=[self.report], history_index=history_index)
        self.assertEqual(rankings["单条点赞排行"][0]["title_copy"], "作品2")
        self.assertEqual(rankings["单条评论排行"][0]["title_copy"], "作品2")
        self.assertEqual(rankings["单条第二天增长排行"][0]["title_copy"], "作品2")
        self.assertEqual(rankings["单条第二天增长排行"][0]["engagement_day_delta"], 9)

    def test_select_previous_day_work_baseline(self) -> None:
        history_index = {
            "fp-1": [
                (date(2026, 3, 16), {"日期文本": "2026-03-16", "点赞数": 8}),
                (date(2026, 3, 15), {"日期文本": "2026-03-15", "点赞数": 7}),
            ]
        }
        baseline = select_previous_day_work_baseline(
            history_index=history_index,
            fingerprint="fp-1",
            snapshot_date="2026-03-17",
        )
        self.assertEqual(baseline["日期文本"], "2026-03-16")

    def test_build_single_work_ranking_fields(self) -> None:
        item = {
            "captured_at": "2026-03-17T18:30:00+08:00",
            "account_id": "u1",
            "account": "账号A",
            "fingerprint": "fp-1",
            "title_copy": "作品A",
            "note_type": "video",
            "like_count": 20,
            "comment_count": 10,
            "cover_url": "https://img.example.com/a.jpg",
            "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
            "note_url": "https://www.xiaohongshu.com/explore/a",
            "baseline_date_text": "2026-03-16",
            "previous_like_count": 11,
            "previous_comment_count": 4,
            "like_day_delta": 9,
            "comment_day_delta": 6,
            "engagement_day_delta": 15,
            "engagement_day_rate": 100.0,
        }
        fields = build_single_work_ranking_fields(item=item, rank_type="单条第二天增长排行", rank=1)
        self.assertEqual(fields["榜单类型"], "单条第二天增长排行")
        self.assertEqual(fields["互动次日增量"], 15)
        self.assertEqual(fields["对比日期文本"], "2026-03-16")

    def test_parse_exact_number(self) -> None:
        self.assertEqual(parse_exact_number("1,234"), 1234)
        self.assertIsNone(parse_exact_number("10+"))

    def test_compute_dashboard_portal_metrics(self) -> None:
        second_report = {
            **self.report,
            "profile": {
                **self.report["profile"],
                "profile_user_id": "u2",
                "nickname": "账号B",
                "fans_count_text": "120",
                "interaction_count_text": "450",
            },
            "works": [
                {
                    "title_copy": "作品3",
                    "note_type": "normal",
                    "like_count": 20,
                    "like_count_text": "20",
                    "comment_count": 10,
                    "comment_count_text": "10",
                    "cover_url": "https://img.example.com/third/asset_c!nc_n_webp_mw_1",
                    "note_url": "https://www.xiaohongshu.com/explore/xyz",
                    "xsec_token": "token3",
                    "index": 0,
                    "note_id": "xyz",
                }
            ],
        }
        metrics = compute_dashboard_portal_metrics([self.report, second_report])
        self.assertEqual(metrics["account_count"], 2)
        self.assertEqual(metrics["total_works"], 3)
        self.assertEqual(metrics["total_likes"], 41)
        self.assertEqual(metrics["total_comments"], 19)
        self.assertEqual(metrics["top_work"]["title_copy"], "作品3")

    def test_build_dashboard_portal_fields(self) -> None:
        report = {
            **self.report,
            "profile": {
                **self.report["profile"],
                "fans_count_text": "321",
                "interaction_count_text": "654",
            },
        }
        fields = build_dashboard_portal_fields([report])
        self.assertEqual(fields["监控账号数"], 1)
        self.assertEqual(fields["总粉丝数"], 321)
        self.assertEqual(fields["总获赞收藏数"], 654)
        self.assertEqual(fields["头部作品标题"], "作品2")
        self.assertEqual(fields["头部作品点赞"], 12)
        self.assertEqual(fields["总评论数"], 9)
        self.assertEqual(fields["周对比摘要"], "暂无 7 天前留底，整组账号周对比将在积累满 7 天后显示")

    def test_build_dashboard_portal_fields_with_weekly_baseline(self) -> None:
        weekly_baseline = {
            "baseline_date_text": "2026-03-10",
            "covered_accounts": 2,
            "expected_accounts": 2,
            "total_fans": 400,
            "total_interaction": 800,
            "total_works": 2,
            "total_likes": 30,
            "total_comments": 10,
        }
        second_report = {
            **self.report,
            "profile": {
                **self.report["profile"],
                "profile_user_id": "u2",
                "nickname": "账号B",
                "fans_count_text": "120",
                "interaction_count_text": "450",
            },
            "works": [
                {
                    "title_copy": "作品3",
                    "note_type": "normal",
                    "like_count": 20,
                    "like_count_text": "20",
                    "comment_count": 10,
                    "comment_count_text": "10",
                    "cover_url": "https://img.example.com/third/asset_c!nc_n_webp_mw_1",
                    "note_url": "https://www.xiaohongshu.com/explore/xyz",
                    "xsec_token": "token3",
                    "index": 0,
                    "note_id": "xyz",
                }
            ],
        }
        fields = build_dashboard_portal_fields([self.report, second_report], weekly_baseline=weekly_baseline)
        self.assertEqual(fields["上周日期文本"], "2026-03-10")
        self.assertEqual(fields["上周总粉丝数"], 400)
        self.assertEqual(fields["总粉丝周增量"], -280)
        self.assertEqual(fields["上周总点赞数"], 30)
        self.assertEqual(fields["总点赞周增量"], 11)
        self.assertEqual(fields["上周总评论数"], 10)
        self.assertEqual(fields["总评论周增量"], 9)
        self.assertIn("基线覆盖 2/2 账号", fields["周对比摘要"])

    def test_select_portal_weekly_baseline(self) -> None:
        second_report = {
            **self.report,
            "profile": {
                **self.report["profile"],
                "profile_user_id": "u2",
                "nickname": "账号B",
                "fans_count_text": "120",
                "interaction_count_text": "450",
            },
            "works": [
                {
                    "title_copy": "作品3",
                    "note_type": "normal",
                    "like_count": 20,
                    "like_count_text": "20",
                    "comment_count": 10,
                    "comment_count_text": "10",
                    "cover_url": "https://img.example.com/third/asset_c!nc_n_webp_mw_1",
                    "note_url": "https://www.xiaohongshu.com/explore/xyz",
                    "xsec_token": "token3",
                    "index": 0,
                    "note_id": "xyz",
                }
            ],
        }
        records = [
            {"fields": {"账号ID": "u1", "日期文本": "2026-03-10", "粉丝数": 300, "获赞收藏数": 600, "首页可见作品数": 2, "首页总点赞": 15, "首页总评论": 4}},
            {"fields": {"账号ID": "u2", "日期文本": "2026-03-10", "粉丝数": 100, "获赞收藏数": 200, "首页可见作品数": 1, "首页总点赞": 10, "首页总评论": 6}},
            {"fields": {"账号ID": "u2", "日期文本": "2026-03-11", "粉丝数": 110, "获赞收藏数": 250, "首页可见作品数": 1, "首页总点赞": 12, "首页总评论": 8}},
        ]
        baseline = select_portal_weekly_baseline(records=records, reports=[self.report, second_report])
        self.assertEqual(baseline["baseline_date_text"], "2026-03-10")
        self.assertEqual(baseline["total_fans"], 400)
        self.assertEqual(baseline["total_likes"], 25)
        self.assertEqual(baseline["total_comments"], 10)


if __name__ == "__main__":
    unittest.main()
