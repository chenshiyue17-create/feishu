from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import requests

from .config import Settings
from .models import NoteSnapshot


class FeishuBitableClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self._tenant_access_token: Optional[str] = None

    def sync_snapshot(self, snapshot: NoteSnapshot) -> Tuple[str, str]:
        fields = self._build_fields(snapshot)
        if self.settings.feishu_sync_mode == "append":
            record_id = self.create_record(fields)
            return "created", record_id

        unique_field = self.settings.feishu_unique_field
        unique_value = fields.get(unique_field)
        if not unique_value:
            raise ValueError(f"upsert 模式缺少唯一字段 {unique_field}")

        record_id = self.find_record_id(unique_field, unique_value)
        if record_id:
            self.update_record(record_id, fields)
            return "updated", record_id

        record_id = self.create_record(fields)
        return "created", record_id

    def find_record_id(self, field_name: str, field_value: Any) -> str:
        target = _normalize_cell_value(field_value)
        for item in self.list_records(field_names=[field_name]):
            fields = item.get("fields") or {}
            if _normalize_cell_value(fields.get(field_name)) == target:
                return str(item.get("record_id") or "")
        return ""

    def probe_table(self, field_names: Optional[list[str]] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"page_size": 1}
        if field_names:
            params["field_names"] = json.dumps(field_names, ensure_ascii=False)
        data = self._request("GET", self._records_url(), params=params)
        inner = data.get("data") or {}
        return {
            "total": inner.get("total", 0),
            "has_more": inner.get("has_more", False),
            "sample_count": len(inner.get("items") or []),
        }

    def list_fields(self) -> list[Dict[str, Any]]:
        items: list[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            data = self._request("GET", self._fields_url(), params=params)
            inner = data.get("data") or {}
            batch = inner.get("items") or []
            items.extend(item for item in batch if isinstance(item, dict))
            if not inner.get("has_more"):
                break
            page_token = str(inner.get("page_token") or "")
            if not page_token:
                break
        return items

    def list_records(
        self,
        *,
        page_size: int = 100,
        field_names: Optional[list[str]] = None,
    ) -> list[Dict[str, Any]]:
        items: list[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if field_names:
                params["field_names"] = json.dumps(field_names, ensure_ascii=False)
            if page_token:
                params["page_token"] = page_token
            data = self._request("GET", self._records_url(), params=params)
            inner = data.get("data") or {}
            batch = inner.get("items") or []
            items.extend(item for item in batch if isinstance(item, dict))
            if not inner.get("has_more"):
                break
            page_token = str(inner.get("page_token") or "")
            if not page_token:
                break
        return items

    def list_tables(self) -> list[Dict[str, Any]]:
        items: list[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            data = self._request("GET", self._tables_url(), params=params)
            inner = data.get("data") or {}
            batch = inner.get("items") or []
            items.extend(item for item in batch if isinstance(item, dict))
            if not inner.get("has_more"):
                break
            page_token = str(inner.get("page_token") or "")
            if not page_token:
                break
        return items

    def create_table(
        self,
        *,
        table_name: str,
        default_view_name: str = "",
        fields: Optional[list[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        table_payload: Dict[str, Any] = {"name": table_name}
        if default_view_name:
            table_payload["default_view_name"] = default_view_name
        if fields:
            table_payload["fields"] = fields
        data = self._request("POST", self._tables_url(), json={"table": table_payload})
        return data.get("data") or {}

    def create_field(
        self,
        *,
        field_name: str,
        field_type: int,
        property_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "field_name": field_name,
            "type": field_type,
        }
        if property_payload:
            payload["property"] = property_payload
        data = self._request("POST", self._fields_url(), json=payload)
        return (data.get("data") or {}).get("field") or {}

    def ensure_fields(self, field_specs: list[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        existing = {str(item.get("field_name") or "").strip(): item for item in self.list_fields()}
        ensured: Dict[str, Dict[str, Any]] = dict(existing)
        for spec in field_specs:
            field_name = str(spec.get("field_name") or "").strip()
            if not field_name or field_name in ensured:
                continue
            created = self.create_field(
                field_name=field_name,
                field_type=int(spec["type"]),
                property_payload=spec.get("property"),
            )
            ensured[field_name] = created
        return ensured

    def create_record(self, fields: Dict[str, Any]) -> str:
        data = self._request("POST", self._records_url(), json={"fields": fields})
        record_id = ((data.get("data") or {}).get("record") or {}).get("record_id")
        if not record_id:
            raise ValueError("飞书返回成功但没有 record_id")
        return str(record_id)

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> None:
        self._request("PUT", f"{self._records_url()}/{record_id}", json={"fields": fields})

    def delete_record(self, record_id: str) -> None:
        self._request("DELETE", f"{self._records_url()}/{record_id}")

    def upsert_record(self, unique_field: str, unique_value: Any, fields: Dict[str, Any]) -> Tuple[str, str]:
        record_id = self.find_record_id(unique_field, unique_value)
        if record_id:
            self.update_record(record_id, fields)
            return "updated", record_id
        record_id = self.create_record(fields)
        return "created", record_id

    def _build_fields(self, snapshot: NoteSnapshot) -> Dict[str, Any]:
        standard = snapshot.to_standard_dict(include_raw_json=self.settings.include_raw_json)
        fields: Dict[str, Any] = {}
        for key, field_name in self.settings.feishu_field_map.items():
            if key not in standard:
                continue
            value = standard[key]
            if value is None or value == "":
                continue
            if isinstance(value, list):
                value = ",".join(str(item) for item in value if str(item).strip())
            elif isinstance(value, dict):
                value = json.dumps(value, ensure_ascii=False)
            fields[field_name] = value
        return fields

    def _request(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._get_tenant_access_token()}"
        headers["Content-Type"] = "application/json; charset=utf-8"
        response = self.session.request(method, url, headers=headers, timeout=20, **kwargs)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") not in (None, 0):
            raise ValueError(f"飞书接口错误 {payload.get('code')}: {payload.get('msg')}")
        return payload

    def _get_tenant_access_token(self) -> str:
        if self._tenant_access_token:
            return self._tenant_access_token
        response = self.session.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            headers={"Content-Type": "application/json; charset=utf-8"},
            json={
                "app_id": self.settings.feishu_app_id,
                "app_secret": self.settings.feishu_app_secret,
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise ValueError(f"获取 tenant_access_token 失败: {payload.get('msg')}")
        self._tenant_access_token = str(payload["tenant_access_token"])
        return self._tenant_access_token

    def _records_url(self) -> str:
        return (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.settings.feishu_bitable_app_token}/tables/{self.settings.feishu_table_id}/records"
        )

    def _fields_url(self) -> str:
        return (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.settings.feishu_bitable_app_token}/tables/{self.settings.feishu_table_id}/fields"
        )

    def _tables_url(self) -> str:
        return (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.settings.feishu_bitable_app_token}/tables"
        )


def _normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if "text" in value:
            return str(value.get("text") or "").strip()
        if "link" in value:
            return str(value.get("link") or "").strip()
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, list):
        return "|".join(_normalize_cell_value(item) for item in value)
    return str(value).strip()


def normalize_field_value_for_compare(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        normalized: Dict[str, Any] = {}
        for key in sorted(value):
            cleaned = normalize_field_value_for_compare(value[key])
            if cleaned is None:
                continue
            normalized[str(key)] = cleaned
        return normalized or None
    if isinstance(value, list):
        normalized_items = [normalize_field_value_for_compare(item) for item in value]
        cleaned_items = [item for item in normalized_items if item is not None]
        return cleaned_items or None
    return str(value).strip() or None


def fields_match(
    existing_fields: Dict[str, Any],
    desired_fields: Dict[str, Any],
    *,
    ignore_fields: Optional[list[str]] = None,
) -> bool:
    ignored = {str(item).strip() for item in (ignore_fields or []) if str(item).strip()}
    for field_name, desired_value in desired_fields.items():
        if field_name in ignored:
            continue
        if normalize_field_value_for_compare(existing_fields.get(field_name)) != normalize_field_value_for_compare(desired_value):
            return False
    return True
