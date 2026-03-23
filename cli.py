from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import load_settings
from .feishu import FeishuBitableClient
from .launchd import (
    DEFAULT_LABEL,
    build_launch_environment,
    build_launch_agent_plist,
    build_sync_program_arguments,
    default_paths,
    install_launch_agent,
    unload_launch_agent,
    wrap_program_arguments_for_login_shell,
)
from .models import NoteSnapshot, Target
from .state import StateStore
from .xhs import XHSCollector


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(args.env_file)
    if args.state_file:
        settings.state_file = str(Path(args.state_file).expanduser().resolve())
    targets_path = str(Path(args.targets).expanduser().resolve())

    if args.install_launchd or args.unload_launchd:
        return _handle_launchd(args, targets_path, settings.state_file)
    if args.check:
        return _handle_check(args, settings, targets_path)

    targets = _load_targets(targets_path)
    collector = XHSCollector(settings)
    state_store = StateStore(settings.state_file)
    client = None if args.dry_run else FeishuBitableClient(settings)

    if client:
        settings.validate_for_sync()

    snapshots: List[NoteSnapshot] = []
    results: List[Tuple[str, str, str]] = []
    errors: List[Tuple[str, str]] = []

    for target in targets:
        try:
            snapshot = collector.collect(target)
            state_store.calculate_deltas(snapshot)
            snapshots.append(snapshot)

            if args.dry_run:
                results.append((target.display_name, "dry-run", "未写入飞书"))
                continue

            action, record_id = client.sync_snapshot(snapshot)
            state_store.commit(snapshot)
            results.append((target.display_name, action, record_id))
        except Exception as exc:
            errors.append((target.display_name, str(exc)))
            if args.strict:
                break

    if not args.dry_run and results:
        state_store.save()

    if args.print_json or args.dry_run:
        payload = [
            snapshot.to_standard_dict(include_raw_json=settings.include_raw_json) for snapshot in snapshots
        ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    for name, action, detail in results:
        print(f"[OK] {name} -> {action} ({detail})")
    for name, detail in errors:
        print(f"[ERR] {name} -> {detail}")

    if errors:
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="抓取小红书笔记数据并同步到飞书多维表格。",
    )
    parser.add_argument(
        "--targets",
        required=True,
        help="监控目标 JSON 文件路径，支持 url/html_file/json_file 三种来源。",
    )
    parser.add_argument(
        "--env-file",
        default="xhs_feishu_monitor/.env",
        help="环境变量文件路径，默认读取 xhs_feishu_monitor/.env。",
    )
    parser.add_argument(
        "--state-file",
        help="状态缓存文件路径，用于计算互动增量。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只抓取并打印标准化结果，不写入飞书，也不更新本地状态。",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="无论是否写入飞书，都打印标准化后的 JSON。",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="遇到第一个错误就立即停止。",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="只做健康检查，不写入飞书，也不更新本地状态。",
    )
    parser.add_argument(
        "--skip-xhs-check",
        action="store_true",
        help="健康检查时跳过小红书抓取检查。",
    )
    parser.add_argument(
        "--skip-feishu-check",
        action="store_true",
        help="健康检查时跳过飞书连接和表访问检查。",
    )
    parser.add_argument(
        "--check-limit",
        type=int,
        default=3,
        help="健康检查时最多抽查多少个监控目标，默认 3。",
    )
    parser.add_argument(
        "--install-launchd",
        action="store_true",
        help="生成并安装 launchd 定时任务。",
    )
    parser.add_argument(
        "--unload-launchd",
        action="store_true",
        help="卸载 launchd 定时任务。",
    )
    parser.add_argument(
        "--launchd-label",
        default=DEFAULT_LABEL,
        help=f"launchd 任务标签，默认 {DEFAULT_LABEL}。",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=30,
        help="定时运行间隔，单位分钟，默认 30。",
    )
    parser.add_argument(
        "--launchd-plist",
        help="自定义 plist 输出路径，默认写入 ~/Library/LaunchAgents。",
    )
    parser.add_argument(
        "--load-launchd",
        action="store_true",
        help="安装 plist 后立即通过 launchctl 加载。",
    )
    parser.add_argument(
        "--stdout-log",
        help="launchd 标准输出日志文件路径。",
    )
    parser.add_argument(
        "--stderr-log",
        help="launchd 错误日志文件路径。",
    )
    return parser


def _load_targets(path_text: str) -> List[Target]:
    path = Path(path_text).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("targets") or []
    if not isinstance(payload, list):
        raise ValueError("targets 文件必须是数组，或包含 targets 数组的对象")

    targets: List[Target] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("targets 数组中的每一项都必须是对象")
        target = Target.from_dict(item)
        target.html_file = _resolve_optional_path(target.html_file, path.parent)
        target.json_file = _resolve_optional_path(target.json_file, path.parent)
        targets.append(target)
    if not targets:
        raise ValueError("targets 文件为空")
    return targets


def _resolve_optional_path(path_text: Optional[str], base_dir: Path) -> Optional[str]:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def _handle_launchd(args: argparse.Namespace, targets_path: str, state_file_path: str) -> int:
    defaults = default_paths(args.launchd_label)
    plist_path = str(Path(args.launchd_plist or defaults["plist_path"]).expanduser().resolve())

    if args.unload_launchd:
        unload_launch_agent(plist_path=plist_path)
        print(f"[OK] 已卸载 launchd 任务: {args.launchd_label}")
        print(f"[OK] plist 路径: {plist_path}")
        return 0

    if args.interval_minutes <= 0:
        raise ValueError("--interval-minutes 必须大于 0")

    env_file_path = str(Path(args.env_file).expanduser().resolve())
    stdout_log = str(Path(args.stdout_log or defaults["stdout_log_path"]).expanduser().resolve())
    stderr_log = str(Path(args.stderr_log or defaults["stderr_log_path"]).expanduser().resolve())
    Path(stdout_log).parent.mkdir(parents=True, exist_ok=True)
    Path(stderr_log).parent.mkdir(parents=True, exist_ok=True)
    project_root = str(Path(__file__).resolve().parent.parent)
    program_arguments = build_sync_program_arguments(
        targets_path=targets_path,
        env_file_path=env_file_path,
        state_file_path=state_file_path,
    )
    plist_bytes = build_launch_agent_plist(
        label=args.launchd_label,
        program_arguments=wrap_program_arguments_for_login_shell(
            program_arguments=program_arguments,
            working_directory=project_root,
        ),
        working_directory=project_root,
        interval_seconds=args.interval_minutes * 60,
        stdout_log_path=stdout_log,
        stderr_log_path=stderr_log,
        environment_variables=build_launch_environment(),
    )
    install_launch_agent(
        plist_bytes=plist_bytes,
        label=args.launchd_label,
        plist_path=plist_path,
        load_after_install=args.load_launchd,
    )
    print(f"[OK] 已生成 launchd 任务: {args.launchd_label}")
    print(f"[OK] plist 路径: {plist_path}")
    print(f"[OK] stdout 日志: {stdout_log}")
    print(f"[OK] stderr 日志: {stderr_log}")
    if args.load_launchd:
        print("[OK] 已尝试加载 launchd 任务")
    else:
        print("[OK] 如需立即加载，追加 --load-launchd")
    return 0


def _handle_check(args: argparse.Namespace, settings, targets_path: str) -> int:
    failures = 0
    if args.check_limit <= 0:
        raise ValueError("--check-limit 必须大于 0")

    if not args.skip_xhs_check:
        collector = XHSCollector(settings)
        targets = _load_targets(targets_path)[: args.check_limit]
        print("[CHECK] 小红书抓取")
        for target in targets:
            try:
                snapshot = collector.collect(target)
                print(
                    "[OK]",
                    target.display_name,
                    f"点赞={snapshot.like_count} 收藏={snapshot.collect_count} 评论={snapshot.comment_count} 分享={snapshot.share_count}",
                )
            except Exception as exc:
                failures += 1
                print(f"[ERR] {target.display_name} -> {exc}")

    if not args.skip_feishu_check:
        print("[CHECK] 飞书多维表格")
        try:
            settings.validate_for_sync()
            client = FeishuBitableClient(settings)
            field_names = list(dict.fromkeys(settings.feishu_field_map.values()))
            probe = client.probe_table(field_names=field_names[:50])
            fields = client.list_fields()
            summary = summarize_field_mapping(fields=fields, expected_field_names=field_names)
            print(
                "[OK] 飞书连接正常",
                f"total={probe['total']}",
                f"sample_count={probe['sample_count']}",
                f"has_more={probe['has_more']}",
            )
            print(
                "[OK] 字段检查",
                f"existing={len(summary['existing'])}",
                f"missing={len(summary['missing'])}",
                f"hidden={len(summary['hidden'])}",
            )
            if summary["missing"]:
                failures += 1
                print("[ERR] 缺少字段 -> " + ", ".join(summary["missing"]))
            else:
                print("[OK] 字段映射齐全")
        except Exception as exc:
            failures += 1
            print(f"[ERR] 飞书检查失败 -> {exc}")

    if args.skip_xhs_check and args.skip_feishu_check:
        raise ValueError("不能同时跳过小红书检查和飞书检查")

    return 1 if failures else 0


def summarize_field_mapping(
    *,
    fields: List[Dict[str, object]],
    expected_field_names: List[str],
) -> Dict[str, List[str]]:
    existing_names = []
    hidden_names = []
    for item in fields:
        field_name = str(item.get("field_name") or "").strip()
        if not field_name:
            continue
        existing_names.append(field_name)
        if bool(item.get("is_hidden")):
            hidden_names.append(field_name)

    existing_set = set(existing_names)
    hidden_set = set(hidden_names)
    expected_unique = []
    for name in expected_field_names:
        cleaned = str(name).strip()
        if cleaned and cleaned not in expected_unique:
            expected_unique.append(cleaned)

    missing = [name for name in expected_unique if name not in existing_set]
    present = [name for name in expected_unique if name in existing_set]
    hidden = [name for name in expected_unique if name in hidden_set]
    return {
        "existing": sorted(present),
        "missing": sorted(missing),
        "hidden": sorted(hidden),
    }
