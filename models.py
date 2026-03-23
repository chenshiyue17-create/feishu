from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Target:
    name: str = ""
    url: Optional[str] = None
    html_file: Optional[str] = None
    json_file: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    remark: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Target":
        known_keys = {"name", "url", "html_file", "json_file", "tags", "remark"}
        tags = payload.get("tags") or []
        if isinstance(tags, str):
            tags = [item.strip() for item in tags.split(",") if item.strip()]
        return cls(
            name=str(payload.get("name") or "").strip(),
            url=_clean_optional_string(payload.get("url")),
            html_file=_clean_optional_string(payload.get("html_file")),
            json_file=_clean_optional_string(payload.get("json_file")),
            tags=list(tags),
            remark=str(payload.get("remark") or "").strip(),
            extra={key: value for key, value in payload.items() if key not in known_keys},
        )

    @property
    def display_name(self) -> str:
        return self.name or self.url or self.html_file or self.json_file or "未命名目标"


@dataclass
class NoteSnapshot:
    note_id: str = ""
    note_title: str = ""
    note_url: str = ""
    description: str = ""
    author_name: str = ""
    author_id: str = ""
    published_at: str = ""
    captured_at: str = ""
    like_count: Optional[int] = None
    collect_count: Optional[int] = None
    comment_count: Optional[int] = None
    share_count: Optional[int] = None
    like_delta: Optional[int] = None
    collect_delta: Optional[int] = None
    comment_delta: Optional[int] = None
    share_delta: Optional[int] = None
    source_name: str = ""
    tags: List[str] = field(default_factory=list)
    remark: str = ""
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_normalized_dict(cls, payload: Dict[str, Any]) -> "NoteSnapshot":
        tags = payload.get("tags") or []
        if isinstance(tags, str):
            tags = [item.strip() for item in tags.split(",") if item.strip()]
        return cls(
            note_id=str(payload.get("note_id") or payload.get("id") or "").strip(),
            note_title=str(payload.get("note_title") or payload.get("title") or "").strip(),
            note_url=str(payload.get("note_url") or payload.get("url") or "").strip(),
            description=str(payload.get("description") or payload.get("desc") or "").strip(),
            author_name=str(payload.get("author_name") or payload.get("author") or "").strip(),
            author_id=str(payload.get("author_id") or "").strip(),
            published_at=str(payload.get("published_at") or "").strip(),
            captured_at=str(payload.get("captured_at") or "").strip(),
            like_count=_coerce_optional_int(payload.get("like_count")),
            collect_count=_coerce_optional_int(payload.get("collect_count")),
            comment_count=_coerce_optional_int(payload.get("comment_count")),
            share_count=_coerce_optional_int(payload.get("share_count")),
            like_delta=_coerce_optional_int(payload.get("like_delta")),
            collect_delta=_coerce_optional_int(payload.get("collect_delta")),
            comment_delta=_coerce_optional_int(payload.get("comment_delta")),
            share_delta=_coerce_optional_int(payload.get("share_delta")),
            source_name=str(payload.get("source_name") or payload.get("name") or "").strip(),
            tags=list(tags),
            remark=str(payload.get("remark") or "").strip(),
            raw_payload=payload.get("raw_payload") or {},
        )

    def identity_key(self) -> str:
        if self.note_id:
            return self.note_id
        if self.note_url:
            return self.note_url
        return self.note_title

    def snapshot_key(self) -> str:
        if not self.identity_key():
            return ""
        captured = self.captured_at.replace("-", "").replace(":", "").replace("T", "").replace("+", "")
        captured = captured.replace(".", "").replace("Z", "")
        return f"{self.identity_key()}::{captured}"

    def to_standard_dict(self, include_raw_json: bool = False) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "note_id": self.note_id,
            "note_title": self.note_title,
            "note_url": self.note_url,
            "description": self.description,
            "author_name": self.author_name,
            "author_id": self.author_id,
            "published_at": self.published_at,
            "captured_at": self.captured_at,
            "like_count": self.like_count,
            "collect_count": self.collect_count,
            "comment_count": self.comment_count,
            "share_count": self.share_count,
            "like_delta": self.like_delta,
            "collect_delta": self.collect_delta,
            "comment_delta": self.comment_delta,
            "share_delta": self.share_delta,
            "source_name": self.source_name,
            "tags": self.tags,
            "remark": self.remark,
            "snapshot_key": self.snapshot_key(),
        }
        if include_raw_json:
            payload["raw_json"] = self.raw_payload
        return payload


def _clean_optional_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value).strip())
    except ValueError:
        return None
