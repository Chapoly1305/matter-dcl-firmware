from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence


DEFAULT_NETWORKS = ("ota-mainnet", "ota-testnet")
PROJECT_DEFAULT_BUCKET = "matter-firmware"
PROJECT_DEFAULT_S3_ENDPOINT_URL = (
    "https://665360de327d92bb87e8e2c2c866c7a1.r2.cloudflarestorage.com"
)
PROJECT_DEFAULT_REGION = "auto"
PROJECT_DEFAULT_HTTPS_BASE_URL = "https://matter-firmware.chapoly1305.com"


@dataclass(frozen=True)
class AppConfig:
    source_root: Path
    cache_dir: Path
    bucket: str
    endpoint_url: Optional[str]
    region: str
    access_key_id: Optional[str]
    secret_access_key: Optional[str]
    session_token: Optional[str]
    https_base_url: Optional[str]
    profile_name: Optional[str]
    request_timeout_seconds: int
    global_manifest_key: str
    manifest_prefix: str
    object_prefix: str
    addressing_style: str
    hash_processes: int
    compress_processes: int
    upload_threads: int
    head_threads: int
    download_threads: int

    @property
    def networks(self) -> Sequence[str]:
        return DEFAULT_NETWORKS

    @classmethod
    def from_env(
        cls,
        source_root: Optional[Path] = None,
        cache_dir: Optional[Path] = None,
    ) -> "AppConfig":
        root = source_root or Path(os.getenv("FIRMWARE_SOURCE_ROOT", "firmware"))
        cache = cache_dir or Path(os.getenv("CACHE_DIR", ".cache"))
        cpu_count = max(1, os.cpu_count() or 1)

        return cls(
            source_root=root,
            cache_dir=cache,
            bucket=os.getenv("S3_BUCKET", PROJECT_DEFAULT_BUCKET).strip(),
            endpoint_url=_none_if_empty(
                os.getenv("S3_ENDPOINT_URL", PROJECT_DEFAULT_S3_ENDPOINT_URL)
            ),
            region=os.getenv("S3_REGION", PROJECT_DEFAULT_REGION),
            access_key_id=_none_if_empty(os.getenv("S3_ACCESS_KEY_ID")),
            secret_access_key=_none_if_empty(os.getenv("S3_SECRET_ACCESS_KEY")),
            session_token=_none_if_empty(os.getenv("S3_SESSION_TOKEN")),
            https_base_url=_strip_trailing_slash(
                os.getenv("HTTPS_BASE_URL", PROJECT_DEFAULT_HTTPS_BASE_URL)
            ),
            profile_name=_none_if_empty(os.getenv("AWS_PROFILE")),
            request_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
            global_manifest_key=os.getenv(
                "GLOBAL_MANIFEST_KEY", "manifests/global_manifest.json"
            ),
            manifest_prefix=os.getenv("MANIFEST_PREFIX", "manifests"),
            object_prefix=os.getenv("OBJECT_PREFIX", "objects/sha256"),
            addressing_style=os.getenv("S3_ADDRESSING_STYLE", "auto"),
            hash_processes=_env_int("HASH_PROCESSES", cpu_count),
            compress_processes=_env_int("COMPRESS_PROCESSES", cpu_count),
            upload_threads=_env_int("UPLOAD_THREADS", cpu_count),
            head_threads=_env_int("HEAD_THREADS", cpu_count),
            download_threads=_env_int("DOWNLOAD_THREADS", cpu_count),
        )

    def ensure_s3_ready(self) -> None:
        if not self.bucket:
            raise ValueError("Missing required env: S3_BUCKET")

    def ensure_https_ready(self) -> None:
        if not self.https_base_url:
            raise ValueError("Missing required env for HTTPS mode: HTTPS_BASE_URL")


def _none_if_empty(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _strip_trailing_slash(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return value.rstrip("/")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return max(1, parsed)
