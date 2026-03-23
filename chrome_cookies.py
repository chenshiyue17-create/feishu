from __future__ import annotations

import hashlib
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


DEFAULT_KEYCHAIN_SERVICES = (
    "Chrome",
    "Google Chrome Safe Storage",
    "Chrome Safe Storage",
    "Google Chrome",
    "Chromium Safe Storage",
)
DEFAULT_CHROME_PROFILE_ROOT = str((Path.home() / "Library/Application Support/Google/Chrome").resolve())
DEFAULT_CHROME_PROFILE_MARKERS = {"default", "system", "default_profile", "chrome_default"}


def resolve_chrome_profile_root(profile_root: str) -> str:
    text = str(profile_root or "").strip()
    if not text or text.lower() in DEFAULT_CHROME_PROFILE_MARKERS:
        return DEFAULT_CHROME_PROFILE_ROOT
    return str(Path(text).expanduser().resolve())


def resolve_chrome_profile_directory(profile_directory: str) -> str:
    text = str(profile_directory or "").strip()
    return text or "Default"


def is_default_chrome_profile_root(profile_root: str) -> bool:
    return resolve_chrome_profile_root(profile_root) == DEFAULT_CHROME_PROFILE_ROOT


def export_xiaohongshu_cookie_header(profile_root: str, profile_directory: str = "Default") -> str:
    password = find_chrome_safe_storage_password()
    key = derive_chrome_cookie_key(password)
    db_version, rows = read_chrome_cookie_rows(
        profile_root,
        host_pattern="%xiaohongshu%",
        profile_directory=profile_directory,
    )
    return build_cookie_header(rows=rows, key=key, db_version=db_version)


def find_chrome_safe_storage_password(services: Sequence[str] = DEFAULT_KEYCHAIN_SERVICES) -> str:
    for service in services:
        result = subprocess.run(
            ["security", "find-generic-password", "-wa", service],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    raise RuntimeError("未找到 Chrome Safe Storage 密码，请先登录本机 Chrome。")


def derive_chrome_cookie_key(password: str) -> bytes:
    return hashlib.pbkdf2_hmac("sha1", password.encode("utf-8"), b"saltysalt", 1003, dklen=16)


def read_chrome_cookie_rows(
    profile_root: str,
    *,
    host_pattern: str,
    profile_directory: str = "Default",
) -> Tuple[int, List[Tuple[str, str, bytes, str]]]:
    cookies_db = Path(resolve_chrome_profile_root(profile_root)) / resolve_chrome_profile_directory(profile_directory) / "Cookies"
    if not cookies_db.exists():
        raise FileNotFoundError(f"未找到 Chrome Cookies 数据库: {cookies_db}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_db = Path(temp_dir) / "Cookies"
        shutil.copy2(cookies_db, temp_db)
        connection = sqlite3.connect(temp_db)
        cursor = connection.cursor()
        cursor.execute("select value from meta where key='version'")
        db_version = int((cursor.fetchone() or ["0"])[0])
        cursor.execute(
            """
            select host_key, name, encrypted_value, value
            from cookies
            where host_key like ?
            order by host_key, name
            """,
            (host_pattern,),
        )
        rows = cursor.fetchall()
        connection.close()
    return db_version, rows


def build_cookie_header(*, rows: Iterable[Tuple[str, str, bytes, str]], key: bytes, db_version: int) -> str:
    seen: set[str] = set()
    cookie_parts: List[str] = []
    for _host, name, encrypted_value, value in rows:
        raw_value = value or decrypt_chrome_cookie(encrypted_value, key=key, db_version=db_version)
        if not raw_value or name in seen:
            continue
        seen.add(name)
        cookie_parts.append(f"{name}={raw_value}")
    return "; ".join(cookie_parts)


def decrypt_chrome_cookie(encrypted_value: bytes, *, key: bytes, db_version: int) -> str:
    if not encrypted_value:
        return ""
    if encrypted_value.startswith((b"v10", b"v11")):
        data = encrypted_value[3:]
        cipher = Cipher(algorithms.AES(key), modes.CBC(b" " * 16), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(data) + decryptor.finalize()
        pad_length = decrypted[-1]
        if 1 <= pad_length <= 16:
            decrypted = decrypted[:-pad_length]
        if db_version >= 24 and len(decrypted) > 32:
            decrypted = decrypted[32:]
        return decrypted.decode("utf-8", errors="ignore")
    return encrypted_value.decode("utf-8", errors="ignore")
