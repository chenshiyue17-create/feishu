from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


CLASH_GROUP_TYPES = {"Selector", "URLTest", "Fallback", "LoadBalance", "Relay"}
CLASH_NON_PROXY_NAMES = {"DIRECT", "REJECT", "GLOBAL"}
DEFAULT_SELECTOR_CANDIDATES = ("🔰 手动选择", "GLOBAL", "🚀 节点选择", "Proxy", "节点选择")


class ClashSwitchError(RuntimeError):
    pass


@dataclass
class ClashSwitchResult:
    selector: str
    node: str
    delay_ms: int
    proxy_url: str

    def to_progress_event(self) -> Dict[str, Any]:
        return {
            "phase": "clash",
            "status": "success",
            "selector": self.selector,
            "node": self.node,
            "delay_ms": self.delay_ms,
            "proxy_url": self.proxy_url,
            "message": f"Clash 已切换到 {self.node}，延迟 {self.delay_ms}ms",
        }


class ClashNodeRotator:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.enabled = bool(getattr(settings, "xhs_clash_enabled", False))
        self.proxy_url = str(getattr(settings, "xhs_clash_proxy_url", "") or "").strip()
        self.controller_url = str(getattr(settings, "xhs_clash_controller_url", "") or "").strip().rstrip("/")
        self.unix_socket = str(getattr(settings, "xhs_clash_unix_socket", "") or "").strip()
        self.secret = str(getattr(settings, "xhs_clash_secret", "") or "").strip()
        self.selector_name = str(getattr(settings, "xhs_clash_selector", "") or "").strip()
        self.max_delay_ms = max(1, int(getattr(settings, "xhs_clash_max_delay_ms", 500) or 500))
        self.delay_test_url = str(getattr(settings, "xhs_clash_delay_test_url", "") or "").strip() or "http://cp.cloudflare.com/generate_204"
        self._node_index = 0

    def switch_next(self) -> Optional[ClashSwitchResult]:
        if not self.enabled:
            return None
        if not self.proxy_url:
            raise ClashSwitchError("XHS_CLASH_PROXY_URL 未配置")
        proxies = self._load_proxies()
        selector_name, selector = self._resolve_selector(proxies)
        candidates = self._resolve_fast_candidates(proxies=proxies, selector=selector)
        if not candidates:
            raise ClashSwitchError(f"没有延迟 <= {self.max_delay_ms}ms 的 Clash 节点")
        node_name, delay_ms = candidates[self._node_index % len(candidates)]
        self._node_index += 1
        self._select_node(selector_name=selector_name, node_name=node_name)
        return ClashSwitchResult(
            selector=selector_name,
            node=node_name,
            delay_ms=delay_ms,
            proxy_url=self.proxy_url,
        )

    def _load_proxies(self) -> Dict[str, Any]:
        payload = self._request_json("GET", "/proxies")
        proxies = payload.get("proxies") if isinstance(payload, dict) else None
        if not isinstance(proxies, dict) or not proxies:
            raise ClashSwitchError("Clash 控制接口未返回节点列表")
        return proxies

    def _resolve_selector(self, proxies: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        candidates = [self.selector_name] if self.selector_name else []
        candidates.extend(name for name in DEFAULT_SELECTOR_CANDIDATES if name not in candidates)
        for name in candidates:
            item = proxies.get(name)
            if isinstance(item, dict) and isinstance(item.get("all"), list):
                return name, item
        for name, item in proxies.items():
            if isinstance(item, dict) and item.get("type") == "Selector" and isinstance(item.get("all"), list):
                return name, item
        raise ClashSwitchError("未找到可切换的 Clash Selector")

    def _resolve_fast_candidates(self, *, proxies: Dict[str, Any], selector: Dict[str, Any]) -> List[tuple[str, int]]:
        raw_names = [str(name) for name in (selector.get("all") or []) if str(name or "").strip()]
        names = [name for name in raw_names if self._is_proxy_node(name=name, proxies=proxies)]
        candidates: List[tuple[str, int]] = []
        for name in names:
            delay_ms = self._latest_delay_ms(proxies.get(name) or {})
            if delay_ms is not None and 0 < delay_ms <= self.max_delay_ms:
                candidates.append((name, delay_ms))
        if candidates:
            return candidates
        measured: List[tuple[str, int]] = []
        for name in names[:30]:
            delay_ms = self._measure_delay(name)
            if delay_ms is not None and 0 < delay_ms <= self.max_delay_ms:
                measured.append((name, delay_ms))
        return measured

    def _is_proxy_node(self, *, name: str, proxies: Dict[str, Any]) -> bool:
        if name in CLASH_NON_PROXY_NAMES:
            return False
        item = proxies.get(name)
        if not isinstance(item, dict):
            return False
        if item.get("type") in CLASH_GROUP_TYPES:
            return False
        return True

    def _latest_delay_ms(self, item: Dict[str, Any]) -> Optional[int]:
        values: List[int] = []
        for source in (item.get("history"), *(extra.get("history") for extra in (item.get("extra") or {}).values() if isinstance(extra, dict))):
            if not isinstance(source, list):
                continue
            for record in source:
                if not isinstance(record, dict):
                    continue
                delay = record.get("delay")
                if isinstance(delay, (int, float)) and delay > 0:
                    values.append(int(delay))
        return values[-1] if values else None

    def _measure_delay(self, node_name: str) -> Optional[int]:
        encoded_name = urllib.parse.quote(node_name, safe="")
        encoded_url = urllib.parse.quote(self.delay_test_url, safe="")
        timeout_ms = max(1000, self.max_delay_ms + 500)
        try:
            payload = self._request_json(
                "GET",
                f"/proxies/{encoded_name}/delay?timeout={timeout_ms}&url={encoded_url}",
                timeout_seconds=max(1.0, timeout_ms / 1000 + 0.5),
            )
        except Exception:
            return None
        delay = payload.get("delay") if isinstance(payload, dict) else None
        return int(delay) if isinstance(delay, (int, float)) and delay > 0 else None

    def _select_node(self, *, selector_name: str, node_name: str) -> None:
        encoded_selector = urllib.parse.quote(selector_name, safe="")
        self._request("PUT", f"/proxies/{encoded_selector}", body={"name": node_name}, timeout_seconds=3.0)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 3.0,
    ) -> Dict[str, Any]:
        raw = self._request(method, path, body=body, timeout_seconds=timeout_seconds)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ClashSwitchError("Clash 控制接口返回非 JSON") from exc
        if not isinstance(payload, dict):
            raise ClashSwitchError("Clash 控制接口返回格式异常")
        return payload

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Dict[str, Any]],
        timeout_seconds: float,
    ) -> bytes:
        if self.controller_url:
            return self._request_http(method, path, body=body, timeout_seconds=timeout_seconds)
        return self._request_unix_socket(method, path, body=body, timeout_seconds=timeout_seconds)

    def _request_http(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Dict[str, Any]],
        timeout_seconds: float,
    ) -> bytes:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"{self.controller_url}{path}",
            data=data,
            method=method,
            headers=self._headers(json_body=body is not None),
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except urllib.error.URLError as exc:
            raise ClashSwitchError(f"Clash 控制接口不可用: {exc}") from exc

    def _request_unix_socket(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Dict[str, Any]],
        timeout_seconds: float,
    ) -> bytes:
        if not self.unix_socket or not Path(self.unix_socket).exists():
            raise ClashSwitchError("Clash Unix socket 不存在")
        command = [
            "curl",
            "--silent",
            "--show-error",
            "--max-time",
            str(max(1.0, timeout_seconds)),
            "--request",
            method,
            "--unix-socket",
            self.unix_socket,
        ]
        for key, value in self._headers(json_body=body is not None).items():
            command.extend(["--header", f"{key}: {value}"])
        if body is not None:
            command.extend(["--data", json.dumps(body, ensure_ascii=False)])
        command.append(f"http://mihomo{path}")
        result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise ClashSwitchError(result.stderr.decode("utf-8", errors="ignore").strip() or "Clash Unix socket 请求失败")
        return result.stdout

    def _headers(self, *, json_body: bool) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.secret:
            headers["Authorization"] = f"Bearer {self.secret}"
        if json_body:
            headers["Content-Type"] = "application/json"
        return headers
