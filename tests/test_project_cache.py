from __future__ import annotations

import json
import tempfile
import unittest
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from xhs_feishu_monitor.models import NoteSnapshot
from xhs_feishu_monitor.project_cache import rebuild_dashboard_cache_from_project_dirs, write_project_cache_bundle


class FakeCollector:
    def __init__(self, _settings) -> None:
        pass

    def collect_note_detail(self, *, note_id, note_url="", xsec_token="", xsec_source="pc_user"):
        if note_id == "old-note":
            return NoteSnapshot(
                note_id="old-note",
                note_url=note_url or "https://www.xiaohongshu.com/explore/old-note",
                like_count=25,
                comment_count=13,
            )
        return None

    def fetch_note_comments_preview(self, *, note_id, xsec_token, note_url="", limit=3):
        if note_id == "old-note":
            return [
                {"nickname": "用户A", "content": "第一条评论"},
                {"nickname": "用户B", "content": "第二条评论"},
            ][:limit]
        return []


class ProjectCacheTest(unittest.TestCase):
    def test_write_project_cache_bundle_saves_cover_assets_locally(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
            )
            report = {
                "captured_at": "2026-04-01T18:20:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "作品A",
                        "note_type": "video",
                        "like_count": 12,
                        "like_count_text": "12",
                        "comment_count": 5,
                        "comment_count_text": "5",
                        "comment_count_basis": "精确值",
                        "comment_count_is_lower_bound": False,
                        "cover_url": "https://img.example.com/cover-a",
                        "note_url": "https://www.xiaohongshu.com/explore/note-a?xsec_token=t&xsec_source=pc_user",
                        "xsec_token": "t",
                        "note_id": "note-a",
                        "index": 0,
                    }
                ],
            }

            class FakeImageResponse:
                def __init__(self, payload: bytes) -> None:
                    self._payload = payload
                    self.headers = Message()
                    self.headers["Content-Type"] = "image/jpeg"

                def read(self) -> bytes:
                    return self._payload

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb) -> None:
                    return None

            with patch("xhs_feishu_monitor.project_cache.urllib.request.urlopen", return_value=FakeImageResponse(b"jpeg-bytes")):
                write_project_cache_bundle(reports=[report], settings=settings)

            project_dir = Path(tmpdir) / "默认项目"
            cover_files = list((project_dir / "covers").glob("*"))
            self.assertEqual(len(cover_files), 1)
            self.assertEqual(cover_files[0].read_bytes(), b"jpeg-bytes")
            tracked_payload = json.loads((project_dir / "tracked_works.json").read_text(encoding="utf-8"))
            saved_item = (tracked_payload.get("items") or [])[0]
            self.assertEqual(Path(saved_item["local_cover_path"]).resolve(), cover_files[0].resolve())

    def test_write_project_cache_bundle_reuses_same_cover_asset_without_redownloading(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
            )
            report = {
                "captured_at": "2026-04-01T18:20:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "作品A",
                        "note_type": "video",
                        "like_count": 12,
                        "like_count_text": "12",
                        "comment_count": 5,
                        "comment_count_text": "5",
                        "comment_count_basis": "精确值",
                        "comment_count_is_lower_bound": False,
                        "cover_url": "https://ci.xiaohongshu.com/abc/asset-a!nc_n_webp_mw_1",
                        "note_url": "https://www.xiaohongshu.com/explore/note-a?xsec_token=t&xsec_source=pc_user",
                        "xsec_token": "t",
                        "note_id": "note-a",
                        "index": 0,
                    },
                    {
                        "title_copy": "作品B",
                        "note_type": "video",
                        "like_count": 15,
                        "like_count_text": "15",
                        "comment_count": 8,
                        "comment_count_text": "8",
                        "comment_count_basis": "精确值",
                        "comment_count_is_lower_bound": False,
                        "cover_url": "https://sns-avatar-qc.xhscdn.com/other/asset-a!nc_n_webp_mw_1",
                        "note_url": "https://www.xiaohongshu.com/explore/note-b?xsec_token=t&xsec_source=pc_user",
                        "xsec_token": "t",
                        "note_id": "note-b",
                        "index": 1,
                    },
                ],
            }

            class FakeImageResponse:
                def __init__(self, payload: bytes) -> None:
                    self._payload = payload
                    self.headers = Message()
                    self.headers["Content-Type"] = "image/jpeg"

                def read(self) -> bytes:
                    return self._payload

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb) -> None:
                    return None

            with patch(
                "xhs_feishu_monitor.project_cache.urllib.request.urlopen",
                return_value=FakeImageResponse(b"jpeg-bytes"),
            ) as urlopen_mock:
                write_project_cache_bundle(reports=[report], settings=settings)

            project_dir = Path(tmpdir) / "默认项目"
            cover_files = list((project_dir / "covers").glob("*"))
            self.assertEqual(len(cover_files), 1)
            self.assertEqual(urlopen_mock.call_count, 1)
            tracked_payload = json.loads((project_dir / "tracked_works.json").read_text(encoding="utf-8"))
            items = tracked_payload.get("items") or []
            self.assertEqual(len(items), 2)
            self.assertEqual(
                {Path(item["local_cover_path"]).resolve() for item in items},
                {cover_files[0].resolve()},
            )

    def test_write_project_cache_bundle_preserves_existing_exact_comment_count_when_new_run_missing_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
            )
            cache_dir = Path(tmpdir)
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "tracked_works.json").write_text(
                json.dumps(
                    {
                        "project": "默认项目",
                        "updated_at": "2026-04-01T14:00:00+08:00",
                        "tracking_window_days": 14,
                        "items": [
                            {
                                "tracked_key": "note:keep-note",
                                "fingerprint": "note:keep-note",
                                "raw_fingerprint": "u1-keep-fingerprint",
                                "note_id": "keep-note",
                                "note_url": "https://www.xiaohongshu.com/explore/keep-note?xsec_token=keep-token&xsec_source=pc_user",
                                "xsec_token": "keep-token",
                                "account_id": "u1",
                                "account": "账号A",
                                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                                "title_copy": "老作品",
                                "note_type": "video",
                                "cover_url": "https://img.example.com/keep.jpg",
                                "like_count": 20,
                                "like_count_text": "20",
                                "comment_count": 11,
                                "comment_count_text": "11",
                                "comment_count_basis": "精确值",
                                "comment_count_is_lower_bound": False,
                                "recent_comments_summary": "用户A: 老评论",
                                "captured_at": "2026-04-01T14:00:00+08:00",
                                "snapshot_date": "2026-04-01",
                                "first_seen_at": "2026-03-31T14:00:00+08:00",
                                "last_seen_at": "2026-04-01T14:00:00+08:00",
                                "last_refreshed_at": "2026-04-01T14:00:00+08:00",
                                "source": "tracked",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            report = {
                "captured_at": "2026-04-01T17:00:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "老作品",
                        "note_type": "video",
                        "like_count": 22,
                        "like_count_text": "22",
                        "comment_count": None,
                        "comment_count_text": "",
                        "comment_count_basis": "详情缺失",
                        "comment_count_is_lower_bound": False,
                        "cover_url": "https://img.example.com/keep.jpg",
                        "note_url": "https://www.xiaohongshu.com/explore/keep-note?xsec_token=keep-token&xsec_source=pc_user",
                        "xsec_token": "keep-token",
                        "note_id": "keep-note",
                        "index": 0,
                    }
                ],
            }

            class MissingCommentCollector:
                def __init__(self, _settings) -> None:
                    pass

                def collect_note_detail(self, *, note_id, note_url="", xsec_token="", xsec_source="pc_user"):
                    return NoteSnapshot(note_id=note_id, note_url=note_url, like_count=22, comment_count=None)

            with patch("xhs_feishu_monitor.project_cache.XHSCollector", MissingCommentCollector):
                write_project_cache_bundle(reports=[report], settings=settings)

            tracked_payload = json.loads((project_dir / "tracked_works.json").read_text(encoding="utf-8"))
            tracked_items = {str(item.get("tracked_key") or ""): item for item in tracked_payload.get("items") or []}
            kept = tracked_items["note:keep-note"]
            self.assertEqual(kept["comment_count"], 11)
            self.assertEqual(kept["comment_count_basis"], "精确值")
            self.assertEqual(kept["comment_count_text"], "11")
            self.assertFalse(kept["comment_count_is_lower_bound"])

    def test_rebuild_dashboard_cache_from_project_dirs_merges_all_projects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(project_cache_dir=tmpdir)
            cache_dir = Path(tmpdir)
            default_dir = cache_dir / "默认项目"
            dongguan_dir = cache_dir / "东莞"
            default_dir.mkdir(parents=True, exist_ok=True)
            dongguan_dir.mkdir(parents=True, exist_ok=True)
            (default_dir / "calendar_rows.json").write_text(
                json.dumps(
                    [
                        {
                            "日历键": "2026-03-28|u1",
                            "日期文本": "2026-03-28",
                            "账号ID": "u1",
                            "账号": "账号A",
                            "粉丝数": 100,
                            "获赞收藏数": 200,
                            "账号总作品数": 30,
                            "作品数展示": "30+",
                            "首页总点赞": 20,
                            "首页总评论": 5,
                            "主页链接": {"link": "https://example.com/u1"},
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (dongguan_dir / "calendar_rows.json").write_text(
                json.dumps(
                    [
                        {
                            "日历键": "2026-03-28|u2",
                            "日期文本": "2026-03-28",
                            "账号ID": "u2",
                            "账号": "账号B",
                            "粉丝数": 80,
                            "获赞收藏数": 150,
                            "账号总作品数": 30,
                            "作品数展示": "30+",
                            "首页总点赞": 12,
                            "首页总评论": 3,
                            "主页链接": {"link": "https://example.com/u2"},
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            payload = rebuild_dashboard_cache_from_project_dirs(settings)
            account_ids = {item.get("account_id") for item in payload.get("accounts") or []}
            self.assertEqual(account_ids, {"u1", "u2"})

    def test_write_project_cache_bundle_filters_stale_project_calendar_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
                interaction_alert_delta_threshold=10,
                comment_alert_min_previous_count=0,
            )
            cache_dir = Path(tmpdir)
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "calendar_rows.json").write_text(
                json.dumps(
                    [
                        {"日历键": "2026-03-25|u1", "日期文本": "2026-03-25", "账号ID": "u1", "账号": "账号A", "粉丝数": 10},
                        {"日历键": "2026-03-25|u2", "日期文本": "2026-03-25", "账号ID": "u2", "账号": "账号B", "粉丝数": 20},
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            report = {
                "captured_at": "2026-03-26T14:00:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [],
            }

            write_project_cache_bundle(reports=[report], settings=settings)

            project_payload = json.loads((project_dir / "dashboard.json").read_text(encoding="utf-8"))
            account_ids = {item.get("account_id") for item in project_payload.get("accounts") or []}
            self.assertEqual(account_ids, {"u1"})

    def test_write_project_cache_bundle_generates_alert_rows_from_tracked_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
                interaction_alert_delta_threshold=10,
                comment_alert_min_previous_count=0,
            )
            cache_dir = Path(tmpdir)
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "tracked_works.json").write_text(
                json.dumps(
                    {
                        "project": "默认项目",
                        "updated_at": "2026-03-25T14:00:00+08:00",
                        "tracking_window_days": 14,
                        "items": [
                            {
                                "tracked_key": "note:old-note",
                                "fingerprint": "note:old-note",
                                "raw_fingerprint": "u1-old-fingerprint",
                                "note_id": "old-note",
                                "note_url": "https://www.xiaohongshu.com/explore/old-note?xsec_token=old-token&xsec_source=pc_user",
                                "xsec_token": "old-token",
                                "account_id": "u1",
                                "account": "账号A",
                                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                                "title_copy": "老作品",
                                "note_type": "video",
                                "cover_url": "https://img.example.com/old.jpg",
                                "like_count": 20,
                                "like_count_text": "20",
                                "comment_count": 11,
                                "comment_count_text": "11",
                                "comment_count_is_lower_bound": False,
                                "recent_comments_summary": "",
                                "captured_at": "2026-03-25T14:00:00+08:00",
                                "snapshot_date": "2026-03-25",
                                "first_seen_at": "2026-03-20T14:00:00+08:00",
                                "last_seen_at": "2026-03-25T14:00:00+08:00",
                                "last_refreshed_at": "2026-03-25T14:00:00+08:00",
                                "source": "tracked",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            report = {
                "captured_at": "2026-03-26T14:00:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "老作品",
                        "note_type": "video",
                        "like_count": 35,
                        "like_count_text": "35",
                        "comment_count": 26,
                        "comment_count_text": "26",
                        "cover_url": "https://img.example.com/old.jpg",
                        "note_url": "https://www.xiaohongshu.com/explore/old-note?xsec_token=old-token&xsec_source=pc_user",
                        "xsec_token": "old-token",
                        "note_id": "old-note",
                        "index": 0,
                    }
                ],
            }

            write_project_cache_bundle(reports=[report], settings=settings)

            project_payload = json.loads((project_dir / "dashboard.json").read_text(encoding="utf-8"))
            alerts = project_payload.get("alerts") or []
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["account_id"], "u1")
            self.assertEqual(alerts[0]["like_delta"], 15)
            self.assertEqual(alerts[0]["comment_delta"], 15)
            growth_rows = [
                row
                for row in json.loads((project_dir / "ranking_rows.json").read_text(encoding="utf-8"))
                if str(row.get("榜单类型") or "") == "单条第二天增长排行"
            ]
            self.assertEqual(len(growth_rows), 1)
            self.assertEqual(growth_rows[0]["账号ID"], "u1")
            self.assertEqual(growth_rows[0]["互动次日增量"], 30)

    def test_write_project_cache_bundle_keeps_tracked_work_after_top30_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=True,
                xhs_work_comment_preview_limit=3,
            )
            cache_dir = Path(tmpdir)
            project_dir = cache_dir / "默认项目"
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "tracked_works.json").write_text(
                json.dumps(
                    {
                        "project": "默认项目",
                        "updated_at": "2026-03-25T14:00:00+08:00",
                        "tracking_window_days": 14,
                        "items": [
                            {
                                "tracked_key": "note:old-note",
                                "fingerprint": "note:old-note",
                                "raw_fingerprint": "u1-old-fingerprint",
                                "note_id": "old-note",
                                "note_url": "https://www.xiaohongshu.com/explore/old-note?xsec_token=old-token&xsec_source=pc_user",
                                "xsec_token": "old-token",
                                "account_id": "u1",
                                "account": "账号A",
                                "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                                "title_copy": "老作品",
                                "note_type": "video",
                                "cover_url": "https://img.example.com/old.jpg",
                                "like_count": 20,
                                "like_count_text": "20",
                                "comment_count": 11,
                                "comment_count_text": "11",
                                "comment_count_is_lower_bound": False,
                                "recent_comments_summary": "",
                                "captured_at": "2026-03-25T14:00:00+08:00",
                                "snapshot_date": "2026-03-25",
                                "first_seen_at": "2026-03-20T14:00:00+08:00",
                                "last_seen_at": "2026-03-25T14:00:00+08:00",
                                "last_refreshed_at": "2026-03-25T14:00:00+08:00",
                                "source": "tracked",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            report = {
                "captured_at": "2026-03-26T14:00:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "新作品",
                        "note_type": "normal",
                        "like_count": 5,
                        "like_count_text": "5",
                        "comment_count": 1,
                        "comment_count_text": "1",
                        "cover_url": "https://img.example.com/new.jpg",
                        "note_url": "https://www.xiaohongshu.com/explore/new-note?xsec_token=new-token&xsec_source=pc_user",
                        "xsec_token": "new-token",
                        "note_id": "new-note",
                        "index": 0,
                    }
                ],
            }

            with patch("xhs_feishu_monitor.project_cache.XHSCollector", FakeCollector):
                write_project_cache_bundle(reports=[report], settings=settings)

            ranking_rows = json.loads((project_dir / "ranking_rows.json").read_text(encoding="utf-8"))
            like_titles = {
                str(row.get("标题文案") or "")
                for row in ranking_rows
                if str(row.get("榜单类型") or "") == "单条点赞排行"
            }
            self.assertIn("新作品", like_titles)
            self.assertIn("老作品", like_titles)

            tracked_payload = json.loads((project_dir / "tracked_works.json").read_text(encoding="utf-8"))
            tracked_items = {str(item.get("tracked_key") or ""): item for item in tracked_payload.get("items") or []}
            refreshed_old = tracked_items["note:old-note"]
            self.assertEqual(refreshed_old["like_count"], 25)
            self.assertEqual(refreshed_old["comment_count"], 13)
            self.assertEqual(refreshed_old["recent_comments_summary"], "")
            old_like_row = next(
                row
                for row in ranking_rows
                if str(row.get("榜单类型") or "") == "单条点赞排行" and str(row.get("标题文案") or "") == "老作品"
            )
            new_like_row = next(
                row
                for row in ranking_rows
                if str(row.get("榜单类型") or "") == "单条点赞排行" and str(row.get("标题文案") or "") == "新作品"
            )
            self.assertEqual(old_like_row["追踪状态"], "连续追踪")
            self.assertEqual(old_like_row["首次入池日期"], "2026-03-20")
            self.assertEqual(new_like_row["追踪状态"], "新入池")
            self.assertEqual(new_like_row["首次入池日期"], "2026-03-26")

    def test_write_project_cache_bundle_keeps_growth_history_across_same_day_reruns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                project_cache_dir=tmpdir,
                feishu_review_upload_days=14,
                xhs_fetch_work_comment_preview=False,
                xhs_work_comment_preview_limit=0,
                interaction_alert_delta_threshold=10,
                comment_alert_min_previous_count=0,
            )
            day_one_report = {
                "captured_at": "2026-03-31T14:00:00+08:00",
                "project": "默认项目",
                "profile": {
                    "profile_user_id": "u1",
                    "nickname": "账号A",
                    "profile_url": "https://www.xiaohongshu.com/user/profile/u1",
                    "fans_count_text": "100",
                    "interaction_count_text": "200",
                    "work_count_display_text": "30+",
                    "visible_work_count": 30,
                },
                "works": [
                    {
                        "title_copy": "老作品",
                        "note_type": "video",
                        "like_count": 20,
                        "like_count_text": "20",
                        "comment_count": 11,
                        "comment_count_text": "11",
                        "cover_url": "https://img.example.com/old.jpg",
                        "note_url": "https://www.xiaohongshu.com/explore/old-note?xsec_token=old-token&xsec_source=pc_user",
                        "xsec_token": "old-token",
                        "note_id": "old-note",
                        "index": 0,
                    }
                ],
            }
            day_two_report = {
                **day_one_report,
                "captured_at": "2026-04-01T09:00:00+08:00",
                "works": [
                    {
                        **day_one_report["works"][0],
                        "like_count": 35,
                        "like_count_text": "35",
                        "comment_count": 26,
                        "comment_count_text": "26",
                    }
                ],
            }
            same_day_rerun = {
                **day_one_report,
                "captured_at": "2026-04-01T18:00:00+08:00",
                "works": [
                    {
                        **day_one_report["works"][0],
                        "like_count": 36,
                        "like_count_text": "36",
                        "comment_count": 27,
                        "comment_count_text": "27",
                    }
                ],
            }

            write_project_cache_bundle(reports=[day_one_report], settings=settings)
            write_project_cache_bundle(reports=[day_two_report], settings=settings)
            write_project_cache_bundle(reports=[same_day_rerun], settings=settings)

            ranking_rows = json.loads(
                (Path(tmpdir) / "默认项目" / "ranking_rows.json").read_text(encoding="utf-8")
            )
            growth_rows = [
                row for row in ranking_rows if str(row.get("榜单类型") or "") == "单条第二天增长排行"
            ]
            self.assertEqual(len(growth_rows), 1)
            self.assertEqual(growth_rows[0]["互动次日增量"], 32)
