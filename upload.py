#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from modules.config import AppConfig, DEFAULT_NETWORKS
from modules.workflow import run_upload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload firmware to S3-compatible storage with compression + SHA256 dedup."
    )
    parser.add_argument(
        "--network",
        action="append",
        choices=list(DEFAULT_NETWORKS),
        help="Limit upload to one or more networks. Default: both.",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Firmware root directory. Default from env FIRMWARE_SOURCE_ROOT or ./firmware.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and compute hashes, but do not upload or update manifests.",
    )
    parser.add_argument(
        "--allow-file-changes",
        action="store_true",
        help=(
            "Allow replacing manifest mapping when an existing network file path now has a different SHA256. "
            "Default is reject."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable progress logs and only print final summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        config = AppConfig.from_env(source_root=args.source_root)
        networks = args.network or list(config.networks)

        summary = run_upload(
            config=config,
            networks=networks,
            dry_run=args.dry_run,
            allow_file_changes=args.allow_file_changes,
            progress=not args.quiet,
        )
        print(
            "upload complete: "
            f"scanned_files={summary.scanned_files}, "
            f"uploaded_objects={summary.uploaded_objects}, "
            f"reused_objects={summary.reused_objects}, "
            f"updated_networks={summary.updated_networks}, "
            f"dry_run={args.dry_run}"
        )
    except Exception as exc:
        print(f"upload failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
