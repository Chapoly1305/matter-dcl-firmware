from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .storage import StorageClient


def now_iso8601() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_object_key(config: AppConfig, sha256_hex: str) -> str:
    return f"{config.object_prefix}/{sha256_hex[:2]}/{sha256_hex}.gz"


def global_manifest_default() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": now_iso8601(),
        "objects": {},
    }


def network_manifest_default(network: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "network": network,
        "updated_at": now_iso8601(),
        "files": {},
    }


class ManifestRepository:
    def __init__(self, config: AppConfig, storage: StorageClient) -> None:
        self.config = config
        self.storage = storage
        self.cache_root = config.cache_dir

    def load_global_manifest(self) -> dict[str, Any]:
        key = self.config.global_manifest_key
        data = self._load_remote_then_cache(key)
        if data is None:
            data = global_manifest_default()
        data.setdefault("schema_version", 1)
        data.setdefault("objects", {})
        return data

    def save_global_manifest(self, data: dict[str, Any]) -> None:
        key = self.config.global_manifest_key
        data["updated_at"] = now_iso8601()
        self.storage.write_json_s3(key, data)
        self._save_cache(key, data)

    def load_network_manifest(self, network: str) -> dict[str, Any]:
        key = self.network_manifest_key(network)
        data = self._load_remote_then_cache(key)
        if data is None:
            data = network_manifest_default(network)
        data.setdefault("schema_version", 1)
        data["network"] = network
        data.setdefault("files", {})
        return data

    def save_network_manifest(self, network: str, data: dict[str, Any]) -> None:
        key = self.network_manifest_key(network)
        data["network"] = network
        data["updated_at"] = now_iso8601()
        self.storage.write_json_s3(key, data)
        self._save_cache(key, data)

    def network_manifest_key(self, network: str) -> str:
        return f"{self.config.manifest_prefix}/{network}_manifest.json"

    def _load_remote_then_cache(self, key: str) -> dict[str, Any] | None:
        if self.storage.can_use_s3:
            try:
                data = self.storage.read_json_s3(key)
            except Exception:
                data = None
            if data is not None:
                self._save_cache(key, data)
                return data
            # If S3 backend is available, keep it as authoritative for manifests.
            # Do not force HTTPS manifest fetches (they may be blocked by CDN/domain policy).
            return self._load_cache(key)
        if self.config.https_base_url:
            try:
                data = self.storage.read_json_https(key)
            except Exception:
                data = None
            if data is not None:
                self._save_cache(key, data)
                return data
        return self._load_cache(key)

    def _cache_path(self, key: str) -> Path:
        return self.cache_root / key

    def _save_cache(self, key: str, data: dict[str, Any]) -> None:
        path = self._cache_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

    def _load_cache(self, key: str) -> dict[str, Any] | None:
        path = self._cache_path(key)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
