from __future__ import annotations

import unittest
from types import SimpleNamespace

from xhs_feishu_monitor.clash_verge import ClashNodeRotator


class FakeClashNodeRotator(ClashNodeRotator):
    def __init__(self, settings, payload):
        super().__init__(settings)
        self.payload = payload
        self.selected = []

    def _request_json(self, method, path, *, body=None, timeout_seconds=3.0):
        if method == "GET" and path == "/proxies":
            return self.payload
        raise AssertionError(f"unexpected request {method} {path}")

    def _request(self, method, path, *, body=None, timeout_seconds=3.0):
        if method == "PUT":
            self.selected.append(body["name"])
            return b""
        raise AssertionError(f"unexpected request {method} {path}")


class ClashNodeRotatorTest(unittest.TestCase):
    def test_switch_next_uses_fast_selector_node(self) -> None:
        rotator = FakeClashNodeRotator(
            SimpleNamespace(
                xhs_clash_enabled=True,
                xhs_clash_proxy_url="http://127.0.0.1:7897",
                xhs_clash_controller_url="http://127.0.0.1:9090",
                xhs_clash_unix_socket="",
                xhs_clash_secret="",
                xhs_clash_selector="🔰 手动选择",
                xhs_clash_max_delay_ms=500,
                xhs_clash_delay_test_url="http://cp.cloudflare.com/generate_204",
            ),
            {
                "proxies": {
                    "🔰 手动选择": {"type": "Selector", "all": ["slow", "fast", "DIRECT"]},
                    "slow": {"type": "Shadowsocks", "history": [{"delay": 900}]},
                    "fast": {"type": "Shadowsocks", "history": [{"delay": 120}]},
                    "DIRECT": {"type": "Direct", "history": [{"delay": 1}]},
                }
            },
        )

        result = rotator.switch_next()

        self.assertEqual(result.node, "fast")
        self.assertEqual(result.delay_ms, 120)
        self.assertEqual(rotator.selected, ["fast"])


if __name__ == "__main__":
    unittest.main()
