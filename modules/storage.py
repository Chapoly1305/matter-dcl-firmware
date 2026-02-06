from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional

from .config import AppConfig


class ObjectNotFoundError(FileNotFoundError):
    pass


class StorageClient:
    def __init__(self, config: AppConfig, require_s3: bool = False) -> None:
        self.config = config
        self._s3_client = None
        self._client_error = None
        self._botocore_exceptions = None
        self._init_s3_client(require_s3=require_s3)

    @property
    def has_s3(self) -> bool:
        return self._s3_client is not None

    @property
    def can_use_s3(self) -> bool:
        return self.has_s3 and bool(self.config.bucket)

    def _init_s3_client(self, require_s3: bool) -> None:
        try:
            import boto3
            from botocore.config import Config as BotoConfig
            from botocore.exceptions import ClientError
        except ImportError as exc:
            if require_s3:
                raise RuntimeError(
                    "Missing dependency boto3/botocore. Install them to use S3 uploads."
                ) from exc
            return
        except Exception as exc:
            if require_s3:
                raise RuntimeError(
                    "Failed to initialize boto3/botocore. Check Python SSL/OpenSSL environment."
                ) from exc
            return

        self._client_error = ClientError
        self._botocore_exceptions = (ClientError,)

        kwargs = {
            "service_name": "s3",
            "endpoint_url": self.config.endpoint_url,
            "region_name": self.config.region,
            "config": BotoConfig(
                signature_version="s3v4",
                read_timeout=self.config.request_timeout_seconds,
                connect_timeout=self.config.request_timeout_seconds,
                s3={
                    "payload_signing_enabled": True,
                    "addressing_style": self.config.addressing_style,
                },
            ),
        }

        if self.config.access_key_id and self.config.secret_access_key:
            kwargs["aws_access_key_id"] = self.config.access_key_id
            kwargs["aws_secret_access_key"] = self.config.secret_access_key
            if self.config.session_token:
                kwargs["aws_session_token"] = self.config.session_token
            session = boto3.session.Session()
        else:
            session = boto3.session.Session(profile_name=self.config.profile_name)

        try:
            self._s3_client = session.client(**kwargs)
        except Exception as exc:
            if require_s3:
                raise RuntimeError(
                    "Failed to create S3 client. Check credentials/endpoint/SSL environment."
                ) from exc
            self._s3_client = None

    def object_exists(self, object_key: str) -> bool:
        self._require_s3()
        try:
            self._s3_client.head_object(Bucket=self.config.bucket, Key=object_key)
            return True
        except self._client_error as exc:  # type: ignore[misc]
            if _is_not_found_error(exc):
                return False
            raise

    def upload_file(
        self,
        local_path: Path,
        object_key: str,
        content_type: str = "application/octet-stream",
        content_encoding: Optional[str] = None,
    ) -> None:
        self._require_s3()
        extra = {"ContentType": content_type}
        if content_encoding:
            extra["ContentEncoding"] = content_encoding
        with local_path.open("rb") as body:
            self._s3_client.put_object(
                Bucket=self.config.bucket,
                Key=object_key,
                Body=body,
                **extra,
            )

    def download_file(self, object_key: str, dst_path: Path) -> None:
        self._require_s3()
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            response = self._s3_client.get_object(Bucket=self.config.bucket, Key=object_key)
        except self._client_error as exc:  # type: ignore[misc]
            if _is_not_found_error(exc):
                raise ObjectNotFoundError(object_key) from exc
            raise
        body = response["Body"]
        with dst_path.open("wb") as f:
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

    def read_json_s3(self, object_key: str) -> Optional[dict[str, Any]]:
        self._require_s3()
        try:
            response = self._s3_client.get_object(Bucket=self.config.bucket, Key=object_key)
        except self._client_error as exc:  # type: ignore[misc]
            if _is_not_found_error(exc):
                return None
            raise
        return json.loads(response["Body"].read().decode("utf-8"))

    def write_json_s3(self, object_key: str, data: dict[str, Any]) -> None:
        self._require_s3()
        payload = json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
        self._s3_client.put_object(
            Bucket=self.config.bucket,
            Key=object_key,
            Body=payload,
            ContentType="application/json",
        )

    def download_https(self, object_key: str, dst_path: Path) -> None:
        url = self.build_https_url(object_key)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.config.request_timeout_seconds) as resp:
                if resp.status >= 400:
                    raise ObjectNotFoundError(object_key)
                with dst_path.open("wb") as f:
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise ObjectNotFoundError(object_key) from exc
            raise

    def read_json_https(self, object_key: str) -> Optional[dict[str, Any]]:
        url = self.build_https_url(object_key)
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.config.request_timeout_seconds) as resp:
                if resp.status >= 400:
                    return None
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

    def build_https_url(self, object_key: str) -> str:
        self.config.ensure_https_ready()
        quoted = urllib.parse.quote(object_key, safe="/")
        return f"{self.config.https_base_url}/{quoted}"

    def _require_s3(self) -> None:
        if self._s3_client is None:
            raise RuntimeError("S3 client is not available.")
        if not self.config.bucket:
            raise RuntimeError("S3 bucket is not configured. Set S3_BUCKET.")


def _is_not_found_error(exc: Exception) -> bool:
    code = ""
    status_code = None
    if hasattr(exc, "response"):
        response = getattr(exc, "response", {}) or {}
        code = str((response.get("Error") or {}).get("Code", "")).strip()
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status_code == 404
