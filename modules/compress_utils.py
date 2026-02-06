from __future__ import annotations

import gzip
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Tuple


def gzip_compress_to_temp(src_path: Path, compresslevel: int = 9) -> Tuple[Path, int]:
    """Compress src_path to a temp gzip file and return (temp_path, compressed_size)."""
    fd, tmp_name = tempfile.mkstemp(prefix="ota-", suffix=".gz")
    tmp_path = Path(tmp_name)
    with os.fdopen(fd, "wb") as tmp_file:
        with src_path.open("rb") as src, gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=tmp_file,
            compresslevel=compresslevel,
            mtime=0,
        ) as gz:
            shutil.copyfileobj(src, gz, length=1024 * 1024)
    return tmp_path, tmp_path.stat().st_size


def gunzip_to_file_and_hash(src_gz: Path, dst_path: Path) -> str:
    """Decompress gzip file to dst_path and return SHA256 of decompressed bytes."""
    digest = hashlib.sha256()
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src_gz, "rb") as src, dst_path.open("wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            dst.write(chunk)
    return digest.hexdigest()
