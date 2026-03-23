from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from xhs_feishu_monitor.mediacrawler_xhs_to_feishu import (
    build_report_from_mediacrawler,
    load_mediacrawler_records,
    pick_cover_url,
)


class MediaCrawlerXhsToFeishuTest(unittest.TestCase):
    def test_load_mediacrawler_records_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "contents.jsonl"
            path.write_text(
                json.dumps({"note_id": "n1", "title": "标题1"}, ensure_ascii=False) + "\n"
                + json.dumps({"note_id": "n2", "title": "标题2"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            records = load_mediacrawler_records(str(path))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["note_id"], "n1")

    def test_pick_cover_url(self) -> None:
        self.assertEqual(
            pick_cover_url({"image_list": "https://img1.jpg,https://img2.jpg"}),
            "https://img1.jpg",
        )
        self.assertEqual(
            pick_cover_url({"video_url": "https://video.mp4"}),
            "https://video.mp4",
        )

    def test_build_report_from_mediacrawler(self) -> None:
        report = build_report_from_mediacrawler(
            content_items=[
                {
                    "note_id": "note_1",
                    "title": "标题1",
                    "desc": "描述1",
                    "type": "video",
                    "liked_count": "9",
                    "collected_count": "2",
                    "comment_count": "1",
                    "share_count": "0",
                    "image_list": "https://img1.jpg,https://img2.jpg",
                    "note_url": "https://www.xiaohongshu.com/explore/note_1",
                    "user_id": "user_1",
                    "nickname": "账号A",
                    "avatar": "https://avatar.jpg",
                    "ip_location": "上海",
                    "xsec_token": "token_1",
                    "time": 100,
                },
                {
                    "note_id": "note_2",
                    "title": "标题2",
                    "type": "normal",
                    "liked_count": "12",
                    "image_list": "https://img3.jpg",
                    "user_id": "user_1",
                    "nickname": "账号A",
                    "xsec_token": "token_2",
                    "time": 200,
                },
            ],
            profile_url="https://www.xiaohongshu.com/user/profile/user_1",
            profile_context={
                "profile": {
                    "profile_user_id": "user_1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/user_1",
                    "desc": "账号简介",
                    "fans_count_text": "10+",
                }
            },
        )
        self.assertEqual(report["profile"]["profile_user_id"], "user_1")
        self.assertEqual(report["profile"]["visible_work_count"], 2)
        self.assertEqual(report["profile"]["total_work_count"], 2)
        self.assertEqual(report["profile"]["work_count_display_text"], "2")
        self.assertEqual(report["works"][0]["title_copy"], "标题2")
        self.assertEqual(report["works"][0]["like_count"], 12)
        self.assertEqual(report["works"][1]["cover_url"], "https://img1.jpg")


if __name__ == "__main__":
    unittest.main()
