from __future__ import annotations

import unittest

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from xhs_feishu_monitor.chrome_cookies import build_cookie_header, decrypt_chrome_cookie, derive_chrome_cookie_key


def _encrypt_cookie_value(plaintext: bytes, *, key: bytes) -> bytes:
    pad_length = 16 - (len(plaintext) % 16)
    padded = plaintext + bytes([pad_length]) * pad_length
    cipher = Cipher(algorithms.AES(key), modes.CBC(b" " * 16), backend=default_backend())
    encryptor = cipher.encryptor()
    return b"v10" + encryptor.update(padded) + encryptor.finalize()


class ChromeCookiesTest(unittest.TestCase):
    def test_decrypt_chrome_cookie_removes_integrity_prefix(self) -> None:
        key = derive_chrome_cookie_key("test-password")
        plaintext = b"x" * 32 + b"web_session_value"
        encrypted = _encrypt_cookie_value(plaintext, key=key)
        self.assertEqual(
            decrypt_chrome_cookie(encrypted, key=key, db_version=24),
            "web_session_value",
        )

    def test_build_cookie_header_deduplicates_names(self) -> None:
        key = derive_chrome_cookie_key("test-password")
        rows = [
            (".xiaohongshu.com", "a1", _encrypt_cookie_value(b"x" * 32 + b"value_1", key=key), ""),
            (".xiaohongshu.com", "a1", _encrypt_cookie_value(b"x" * 32 + b"value_2", key=key), ""),
            (".xiaohongshu.com", "web_session", _encrypt_cookie_value(b"x" * 32 + b"session_1", key=key), ""),
        ]
        header = build_cookie_header(rows=rows, key=key, db_version=24)
        self.assertEqual(header, "a1=value_1; web_session=session_1")


if __name__ == "__main__":
    unittest.main()
