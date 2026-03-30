from __future__ import annotations

import argparse
import json
import urllib.request
from typing import List, Optional

from .config import load_settings
from .local_stats_app.monitored_accounts import load_monitored_metadata, parse_monitored_entries
from .project_cache import (
    load_cached_dashboard_payload,
    rebuild_dashboard_cache_from_project_dirs,
    repair_dashboard_cache_from_exports,
)


def _load_dashboard_payload(env_file: str, urls_file: str) -> dict:
    settings = load_settings(env_file)
    payload = load_cached_dashboard_payload(settings)
    if payload:
        return payload
    rebuilt = rebuild_dashboard_cache_from_project_dirs(settings)
    if rebuilt:
        return rebuilt
    repaired = repair_dashboard_cache_from_exports(settings=settings, monitored_metadata=load_monitored_metadata(urls_file))
    if repaired:
        return repaired
    raise ValueError("本地暂无可上传的缓存，请先完成一次采集")


def push_local_cache_to_server(*, env_file: str, urls_file: str, server_url: str, token: str = "") -> dict:
    dashboard_payload = _load_dashboard_payload(env_file, urls_file)
    monitored_entries = parse_monitored_entries(urls_file)
    monitored_metadata = load_monitored_metadata(urls_file)

    request_body = json.dumps(
        {
            "dashboard_payload": dashboard_payload,
            "monitored_entries": monitored_entries,
            "monitored_metadata": monitored_metadata,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    server_url = str(server_url or "").rstrip("/")
    request = urllib.request.Request(
        f"{server_url}/api/server-cache-upload",
        data=request_body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            **({"X-Upload-Token": str(token or "").strip()} if str(token or "").strip() else {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="把本地缓存上传到服务器，供网页和手机端查看。")
    parser.add_argument("--env-file", default="xhs_feishu_monitor/.env")
    parser.add_argument("--urls-file", default="xhs_feishu_monitor/input/robam_multi_profile_urls.txt")
    parser.add_argument("--server-url", required=True, help="服务器基础地址，例如 http://47.87.68.74:8787")
    parser.add_argument("--token", default="", help="可选上传令牌，对应 SERVER_CACHE_UPLOAD_TOKEN")
    args = parser.parse_args(argv)

    payload = push_local_cache_to_server(
        env_file=args.env_file,
        urls_file=args.urls_file,
        server_url=args.server_url,
        token=args.token,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
