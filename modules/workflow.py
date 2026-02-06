from __future__ import annotations

import concurrent.futures
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .compress_utils import gunzip_to_file_and_hash, gzip_compress_to_temp
from .config import AppConfig
from .hash_utils import sha256_file
from .manifest import ManifestRepository, build_object_key, now_iso8601
from .storage import ObjectNotFoundError, StorageClient


@dataclass
class UploadSummary:
    scanned_files: int = 0
    uploaded_objects: int = 0
    reused_objects: int = 0
    updated_networks: int = 0


@dataclass
class DownloadSummary:
    restored_files: int = 0
    downloaded_objects: int = 0


@dataclass
class DownloadItem:
    sha256: str
    object_key: str
    compression: str
    output_path: Path


@dataclass
class UploadFileRecord:
    network: str
    rel_path: str
    abs_path: Path


@dataclass
class HashedFileRecord:
    network: str
    rel_path: str
    abs_path: Path
    sha256: str
    original_size: int


def run_upload(config: AppConfig, networks: Sequence[str], dry_run: bool = False) -> UploadSummary:
    config.ensure_s3_ready()
    storage = StorageClient(config, require_s3=True)
    manifests = ManifestRepository(config, storage)
    summary = UploadSummary()

    global_manifest = manifests.load_global_manifest()
    object_map: Dict[str, Dict[str, Any]] = global_manifest.setdefault("objects", {})
    file_records = _collect_upload_file_records(config, networks)
    summary.scanned_files = len(file_records)
    if not file_records:
        return summary

    hashed_records = _hash_files_parallel(file_records, config.hash_processes)
    network_entries: Dict[str, Dict[str, Dict[str, Any]]] = {}
    hash_to_source_path: Dict[str, Path] = {}
    hash_to_original_size: Dict[str, int] = {}

    for record in hashed_records:
        object_key = build_object_key(config, record.sha256)
        hash_to_source_path.setdefault(record.sha256, record.abs_path)
        hash_to_original_size.setdefault(record.sha256, record.original_size)
        network_entries.setdefault(record.network, {})[record.rel_path] = {
            "sha256": record.sha256,
            "object_key": object_key,
            "compression": "gzip",
            "original_size": record.original_size,
            "compressed_size": None,
        }

    all_hashes = sorted(hash_to_source_path.keys())
    known_hashes = {h for h in all_hashes if h in object_map}
    unknown_hashes = [h for h in all_hashes if h not in known_hashes]

    unknown_exists = _check_unknown_hashes_exist(
        storage=storage,
        config=config,
        hashes=unknown_hashes,
    )
    missing_hashes = [h for h in unknown_hashes if not unknown_exists.get(h, False)]
    summary.reused_objects = len(all_hashes) - len(missing_hashes)
    summary.uploaded_objects = len(missing_hashes)

    uploaded_sizes: Dict[str, int] = {}
    if missing_hashes and not dry_run:
        uploaded_sizes = _compress_and_upload_missing_hashes(
            storage=storage,
            config=config,
            hash_to_source_path=hash_to_source_path,
            missing_hashes=missing_hashes,
        )

    for sha256_hex in all_hashes:
        object_key = build_object_key(config, sha256_hex)
        object_entry = object_map.get(sha256_hex) or {}
        compressed_size = uploaded_sizes.get(sha256_hex, object_entry.get("compressed_size"))
        object_map[sha256_hex] = {
            "sha256": sha256_hex,
            "object_key": object_key,
            "compression": "gzip",
            "original_size": hash_to_original_size[sha256_hex],
            "compressed_size": compressed_size,
            "uploaded_at": object_entry.get("uploaded_at") or now_iso8601(),
        }

    for network, entries in network_entries.items():
        for rel_path, entry in entries.items():
            entry["compressed_size"] = object_map[entry["sha256"]].get("compressed_size")
        network_manifest = manifests.load_network_manifest(network)
        network_manifest["files"] = entries
        if not dry_run:
            manifests.save_network_manifest(network, network_manifest)
        summary.updated_networks += 1

    if not dry_run and summary.updated_networks > 0:
        manifests.save_global_manifest(global_manifest)

    return summary


def run_download(
    config: AppConfig,
    method: str,
    network: str | None = None,
    file_rel_path: str | None = None,
    sha256_hex: str | None = None,
    download_all: bool = False,
    output_dir: Path = Path("downloads"),
    output_path: Path | None = None,
) -> DownloadSummary:
    storage = StorageClient(config, require_s3=False)
    manifests = ManifestRepository(config, storage)
    summary = DownloadSummary()

    global_manifest = manifests.load_global_manifest()
    objects = global_manifest.get("objects", {})

    plan = _build_download_plan(
        config=config,
        manifests=manifests,
        objects=objects,
        network=network,
        file_rel_path=file_rel_path,
        sha256_hex=sha256_hex,
        download_all=download_all,
        output_dir=output_dir,
        output_path=output_path,
    )
    if not plan:
        raise ValueError("No files found in manifests for selected scope.")

    grouped: Dict[str, List[DownloadItem]] = {}
    for item in plan:
        grouped.setdefault(item.sha256, []).append(item)

    for hash_value, items in grouped.items():
        one = items[0]
        if one.compression != "gzip":
            raise ValueError(f"Unsupported compression '{one.compression}' for {hash_value}")

        with tempfile.TemporaryDirectory(prefix="ota-dl-") as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            gz_path = tmp_dir_path / f"{hash_value}.gz"
            raw_path = tmp_dir_path / f"{hash_value}.bin"

            _download_object(storage, method, one.object_key, gz_path)
            summary.downloaded_objects += 1

            actual_hash = gunzip_to_file_and_hash(gz_path, raw_path)
            if actual_hash != hash_value:
                raise ValueError(
                    f"SHA256 mismatch for object {one.object_key}: expected {hash_value}, got {actual_hash}"
                )

            for item in items:
                item.output_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(raw_path, item.output_path)
                summary.restored_files += 1

    return summary


def _build_download_plan(
    config: AppConfig,
    manifests: ManifestRepository,
    objects: dict[str, dict[str, Any]],
    network: str | None,
    file_rel_path: str | None,
    sha256_hex: str | None,
    download_all: bool,
    output_dir: Path,
    output_path: Path | None,
) -> List[DownloadItem]:
    items: List[DownloadItem] = []

    if sha256_hex:
        obj = objects.get(sha256_hex)
        if not obj:
            raise KeyError(f"Hash not found in global manifest: {sha256_hex}")
        target = output_path or (output_dir / f"{sha256_hex}.bin")
        items.append(
            DownloadItem(
                sha256=sha256_hex,
                object_key=obj["object_key"],
                compression=obj.get("compression", "gzip"),
                output_path=target,
            )
        )
        return items

    if file_rel_path:
        if not network:
            raise ValueError("--file requires --network")
        net_manifest = manifests.load_network_manifest(network)
        file_entry = net_manifest.get("files", {}).get(file_rel_path)
        if not file_entry:
            raise KeyError(f"File not found in {network} manifest: {file_rel_path}")

        hash_value = file_entry["sha256"]
        obj = objects.get(hash_value, file_entry)
        target = output_path or (output_dir / network / file_rel_path)
        items.append(
            DownloadItem(
                sha256=hash_value,
                object_key=obj["object_key"],
                compression=obj.get("compression", "gzip"),
                output_path=target,
            )
        )
        return items

    if not download_all:
        raise ValueError("Specify one mode: --hash, --file+--network, or --all")

    target_networks = [network] if network else list(config.networks)
    for net in target_networks:
        net_manifest = manifests.load_network_manifest(net)
        for rel_path, file_entry in sorted(net_manifest.get("files", {}).items()):
            hash_value = file_entry["sha256"]
            obj = objects.get(hash_value, file_entry)
            items.append(
                DownloadItem(
                    sha256=hash_value,
                    object_key=obj["object_key"],
                    compression=obj.get("compression", "gzip"),
                    output_path=output_dir / net / rel_path,
                )
            )
    return items


def _download_object(
    storage: StorageClient,
    method: str,
    object_key: str,
    destination: Path,
) -> None:
    if method == "https":
        storage.download_https(object_key, destination)
        return

    if method == "s3":
        storage.download_file(object_key, destination)
        return

    if method == "auto":
        if storage.config.https_base_url:
            try:
                storage.download_https(object_key, destination)
                return
            except Exception:
                if storage.can_use_s3:
                    storage.download_file(object_key, destination)
                    return
                raise
        if storage.can_use_s3:
            storage.download_file(object_key, destination)
            return
        raise RuntimeError("No download backend available. Configure HTTPS_BASE_URL or S3 access.")

    raise ValueError(f"Unsupported method: {method}")


def _iter_network_files(network_root: Path) -> Iterable[tuple[str, Path]]:
    files = [p for p in network_root.rglob("*") if p.is_file()]
    for path in sorted(files, key=lambda p: p.relative_to(network_root).as_posix()):
        rel_path = path.relative_to(network_root).as_posix()
        yield rel_path, path


def _collect_upload_file_records(config: AppConfig, networks: Sequence[str]) -> List[UploadFileRecord]:
    records: List[UploadFileRecord] = []
    for network in networks:
        network_root = config.source_root / network
        if not network_root.exists():
            continue
        for rel_path, abs_path in _iter_network_files(network_root):
            records.append(UploadFileRecord(network=network, rel_path=rel_path, abs_path=abs_path))
    return records


def _hash_files_parallel(
    file_records: Sequence[UploadFileRecord],
    workers: int,
) -> List[HashedFileRecord]:
    if not file_records:
        return []

    paths = [str(record.abs_path) for record in file_records]
    if workers <= 1 or len(file_records) < 2:
        hashed = [_hash_file_task(path) for path in paths]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            hashed = list(executor.map(_hash_file_task, paths, chunksize=8))

    result: List[HashedFileRecord] = []
    for record, (sha256_hex, original_size) in zip(file_records, hashed):
        result.append(
            HashedFileRecord(
                network=record.network,
                rel_path=record.rel_path,
                abs_path=record.abs_path,
                sha256=sha256_hex,
                original_size=original_size,
            )
        )
    return result


def _check_unknown_hashes_exist(
    storage: StorageClient,
    config: AppConfig,
    hashes: Sequence[str],
) -> Dict[str, bool]:
    if not hashes:
        return {}

    def check_one(sha256_hex: str) -> tuple[str, bool]:
        object_key = build_object_key(config, sha256_hex)
        return sha256_hex, storage.object_exists(object_key)

    results: Dict[str, bool] = {}
    workers = max(1, min(config.head_threads, len(hashes)))
    if workers <= 1:
        for sha256_hex in hashes:
            key, exists = check_one(sha256_hex)
            results[key] = exists
        return results

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_hash = {executor.submit(check_one, h): h for h in hashes}
        for future in concurrent.futures.as_completed(future_to_hash):
            key, exists = future.result()
            results[key] = exists
    return results


def _compress_and_upload_missing_hashes(
    storage: StorageClient,
    config: AppConfig,
    hash_to_source_path: Dict[str, Path],
    missing_hashes: Sequence[str],
) -> Dict[str, int]:
    compressed_sizes: Dict[str, int] = {}
    if not missing_hashes:
        return compressed_sizes

    compress_workers = max(1, min(config.compress_processes, len(missing_hashes)))
    upload_workers = max(1, min(config.upload_threads, len(missing_hashes)))

    upload_futures: List[concurrent.futures.Future[tuple[str, int]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=upload_workers) as upload_executor:
        if compress_workers <= 1:
            for sha256_hex in missing_hashes:
                src_path = hash_to_source_path[sha256_hex]
                tmp_path, compressed_size = gzip_compress_to_temp(src_path)
                upload_futures.append(
                    upload_executor.submit(
                        _upload_compressed_file,
                        storage,
                        tmp_path,
                        build_object_key(config, sha256_hex),
                        sha256_hex,
                        compressed_size,
                    )
                )
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=compress_workers) as compress_executor:
                compression_jobs = {
                    compress_executor.submit(_compress_file_task, str(hash_to_source_path[sha256_hex])): sha256_hex
                    for sha256_hex in missing_hashes
                }
                for future in concurrent.futures.as_completed(compression_jobs):
                    sha256_hex = compression_jobs[future]
                    tmp_file_str, compressed_size = future.result()
                    upload_futures.append(
                        upload_executor.submit(
                            _upload_compressed_file,
                            storage,
                            Path(tmp_file_str),
                            build_object_key(config, sha256_hex),
                            sha256_hex,
                            compressed_size,
                        )
                    )

        for future in concurrent.futures.as_completed(upload_futures):
            sha256_hex, compressed_size = future.result()
            compressed_sizes[sha256_hex] = compressed_size

    return compressed_sizes


def _upload_compressed_file(
    storage: StorageClient,
    local_gz_path: Path,
    object_key: str,
    sha256_hex: str,
    compressed_size: int,
) -> tuple[str, int]:
    try:
        storage.upload_file(
            local_path=local_gz_path,
            object_key=object_key,
            content_type="application/octet-stream",
        )
        return sha256_hex, compressed_size
    finally:
        local_gz_path.unlink(missing_ok=True)


def _hash_file_task(path_str: str) -> tuple[str, int]:
    path = Path(path_str)
    return sha256_file(path), path.stat().st_size


def _compress_file_task(path_str: str) -> tuple[str, int]:
    path = Path(path_str)
    gz_path, compressed_size = gzip_compress_to_temp(path)
    return str(gz_path), compressed_size
