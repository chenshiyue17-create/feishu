from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from .models import NoteSnapshot


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._state: Dict[str, Dict[str, Any]] = {}
        if self.path.exists():
            self._state = json.loads(self.path.read_text(encoding="utf-8"))

    def calculate_deltas(self, snapshot: NoteSnapshot) -> None:
        previous = self._state.get(snapshot.identity_key(), {})
        snapshot.like_delta = _delta(snapshot.like_count, previous.get("like_count"))
        snapshot.collect_delta = _delta(snapshot.collect_count, previous.get("collect_count"))
        snapshot.comment_delta = _delta(snapshot.comment_count, previous.get("comment_count"))
        snapshot.share_delta = _delta(snapshot.share_count, previous.get("share_count"))

    def commit(self, snapshot: NoteSnapshot) -> None:
        identity = snapshot.identity_key()
        if not identity:
            return
        self._state[identity] = {
            "note_id": snapshot.note_id,
            "note_url": snapshot.note_url,
            "captured_at": snapshot.captured_at,
            "like_count": snapshot.like_count,
            "collect_count": snapshot.collect_count,
            "comment_count": snapshot.comment_count,
            "share_count": snapshot.share_count,
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _delta(current: Any, previous: Any) -> Optional[int]:
    if current is None:
        return None
    if previous is None:
        return 0
    try:
        return int(current) - int(previous)
    except (TypeError, ValueError):
        return None
