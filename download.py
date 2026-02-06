#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from modules.config import AppConfig, DEFAULT_NETWORKS
from modules.workflow import run_download


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and restore firmware files from S3-compatible storage."
    )
    parser.add_argument(
        "--method",
        choices=["auto", "https", "s3"],
        default="auto",
        help="Download backend. auto=HTTPS first then S3 fallback.",
    )
    parser.add_argument(
        "--network",
        choices=list(DEFAULT_NETWORKS),
        help="Target network for --file or --all.",
    )
    parser.add_argument(
        "--file",
        dest="file_rel_path",
        help="Relative path inside network manifest, e.g. 5219/1/otaM_xxx.bin.",
    )
    parser.add_argument(
        "--hash",
        dest="sha256_hex",
        help="Download by SHA256 object hash.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Restore all mapped files (for --network or both networks).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("downloads"),
        help="Output root directory.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path for --hash or --file single-file mode.",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Not used by download logic, but kept for config symmetry.",
    )
    args = parser.parse_args()
    _validate_args(args, parser)
    return args


def _validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    selected_modes = sum(bool(x) for x in [args.sha256_hex, args.file_rel_path, args.all])
    if selected_modes != 1:
        parser.error("Specify exactly one mode: --hash OR --file (+ --network) OR --all")
    if args.file_rel_path and not args.network:
        parser.error("--file requires --network")
    if args.output and args.all:
        parser.error("--output cannot be used with --all")


def main() -> None:
    args = parse_args()
    try:
        config = AppConfig.from_env(source_root=args.source_root)

        summary = run_download(
            config=config,
            method=args.method,
            network=args.network,
            file_rel_path=args.file_rel_path,
            sha256_hex=args.sha256_hex,
            download_all=args.all,
            output_dir=args.output_dir,
            output_path=args.output,
        )
        print(
            "download complete: "
            f"downloaded_objects={summary.downloaded_objects}, "
            f"restored_files={summary.restored_files}"
        )
    except Exception as exc:
        print(f"download failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
