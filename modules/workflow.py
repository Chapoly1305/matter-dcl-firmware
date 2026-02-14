from __future__ import annotations

import concurrent.futures
import shutil
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence

from .compress_utils import gunzip_to_file_and_hash, gzip_compress_to_temp
from .config import AppConfig
from .hash_utils import sha256_file
from .manifest import ManifestRepository, build_object_key, now_iso8601
from .storage import StorageClient

ADDITIONAL_LOGS_NETWORK = "additional-logs"


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
    skipped_existing_files: int = 0


@dataclass
class DownloadItem:
    sha256: str
    object_key: str
    compression: str
    output_path: Path


@dataclass
class DownloadTask:
    sha256: str
    object_key: str
    compression: str
    items_to_restore: List[DownloadItem]


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


DownloadProgressCallback = Callable[[int, int | None], None]


def run_upload(
    config: AppConfig,
    networks: Sequence[str],
    additional_logs_root: Path | None = None,
    dry_run: bool = False,
    allow_file_changes: bool = False,
    progress: bool = True,
) -> UploadSummary:
    reporter = _make_upload_reporter(progress)
    started_at = time.monotonic()
    reporter(
        f"start: networks={','.join(networks)}, dry_run={dry_run}, source_root={config.source_root}"
    )
    config.ensure_s3_ready()
    storage = StorageClient(config, require_s3=True)
    manifests = ManifestRepository(config, storage)
    summary = UploadSummary()

    global_manifest = manifests.load_global_manifest()
    object_map: Dict[str, Dict[str, Any]] = global_manifest.setdefault("objects", {})
    reporter(f"loaded global manifest: objects={len(object_map)}")
    file_records = _collect_upload_file_records(config, networks)
    if additional_logs_root is not None:
        if additional_logs_root.exists() and not additional_logs_root.is_dir():
            raise ValueError(f"additional logs path is not a directory: {additional_logs_root}")
        additional_records = _collect_additional_logs_file_records(additional_logs_root)
        file_records.extend(additional_records)
        if additional_records:
            reporter(
                f"included additional logs: root={additional_logs_root}, files={len(additional_records)}"
            )
    summary.scanned_files = len(file_records)
    reporter(f"scan complete: files={summary.scanned_files}")
    if not file_records:
        reporter(f"nothing to do. elapsed={time.monotonic() - started_at:.2f}s")
        return summary

    reporter(f"hashing: workers={config.hash_processes}")
    hashed_records = _hash_files_parallel(file_records, config.hash_processes, reporter=reporter)
    reporter(f"hashing complete: records={len(hashed_records)}")

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

    reporter("loading network manifests")
    manifest_networks = sorted(network_entries.keys())
    network_manifests = {
        network: manifests.load_network_manifest(network) for network in manifest_networks
    }

    changed_paths = _find_changed_file_paths(network_entries=network_entries, network_manifests=network_manifests)
    if changed_paths and not allow_file_changes:
        sample_limit = 10
        sample = changed_paths[:sample_limit]
        details = "\n".join(
            f"- {network}/{rel_path}: {old_sha} -> {new_sha}"
            for network, rel_path, old_sha, new_sha in sample
        )
        extra = ""
        if len(changed_paths) > sample_limit:
            extra = f"\n... and {len(changed_paths) - sample_limit} more"
        raise ValueError(
            "Detected SHA256 changes for existing mapped file paths. "
            "Upload is rejected by default to avoid accidental replacement. "
            "Use --allow-file-changes to permit replacing mapped hashes.\n"
            f"{details}{extra}"
        )
    if changed_paths and allow_file_changes:
        reporter(f"detected changed file mappings: count={len(changed_paths)} (allowed by flag)")

    all_hashes = sorted(hash_to_source_path.keys())
    known_hashes = {h for h in all_hashes if h in object_map}
    unknown_hashes = [h for h in all_hashes if h not in known_hashes]
    reporter(
        f"dedupe summary: unique_hashes={len(all_hashes)}, "
        f"known_hashes={len(known_hashes)}, unknown_hashes={len(unknown_hashes)}"
    )

    unknown_exists = _check_unknown_hashes_exist(
        storage=storage,
        config=config,
        hashes=unknown_hashes,
        reporter=reporter,
    )
    missing_hashes = [h for h in unknown_hashes if not unknown_exists.get(h, False)]
    summary.reused_objects = len(all_hashes) - len(missing_hashes)
    summary.uploaded_objects = len(missing_hashes)
    reporter(
        f"object existence check complete: to_upload={summary.uploaded_objects}, "
        f"reused={summary.reused_objects}"
    )

    uploaded_sizes: Dict[str, int] = {}
    if missing_hashes and not dry_run:
        reporter(
            f"compress+upload: items={len(missing_hashes)}, "
            f"compress_workers={config.compress_processes}, upload_workers={config.upload_threads}"
        )
        uploaded_sizes = _compress_and_upload_missing_hashes(
            storage=storage,
            config=config,
            hash_to_source_path=hash_to_source_path,
            missing_hashes=missing_hashes,
            reporter=reporter,
        )
        reporter("compress+upload complete")
    elif dry_run:
        reporter("dry-run: skipping upload and manifest writes")

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
        network_manifest = network_manifests[network]
        network_manifest["files"] = entries
        if not dry_run:
            manifests.save_network_manifest(network, network_manifest)
            reporter(f"saved network manifest: network={network}, files={len(entries)}")
        else:
            reporter(f"would save network manifest: network={network}, files={len(entries)}")
        summary.updated_networks += 1

    if not dry_run and summary.updated_networks > 0:
        manifests.save_global_manifest(global_manifest)
        reporter(f"saved global manifest: objects={len(object_map)}")

    reporter(f"done. elapsed={time.monotonic() - started_at:.2f}s")
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

    existing_matches = _check_existing_download_targets_parallel(plan, config.hash_processes)
    total_objects = len({item.sha256 for item in plan})
    grouped: Dict[str, List[DownloadItem]] = {}
    for item, is_match in zip(plan, existing_matches):
        if is_match:
            summary.skipped_existing_files += 1
            continue
        grouped.setdefault(item.sha256, []).append(item)
    grouped_items = list(grouped.items())
    tasks: List[DownloadTask] = []

    for hash_value, items in grouped_items:
        one = items[0]
        if one.compression != "gzip":
            raise ValueError(f"Unsupported compression '{one.compression}' for {hash_value}")
        tasks.append(
            DownloadTask(
                sha256=hash_value,
                object_key=one.object_key,
                compression=one.compression,
                items_to_restore=items,
            )
        )

    print(
        "download plan: "
        f"objects={total_objects}, files={len(plan)}, "
        f"scheduled_objects={len(tasks)}, skipped_existing_files={summary.skipped_existing_files}",
        flush=True,
    )
    if not tasks:
        return summary

    workers = max(1, min(config.download_threads, len(tasks)))
    progress = _DownloadProgressBar(total_objects=len(tasks))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_hash = {
            executor.submit(
                _download_and_restore_task,
                storage,
                method,
                task,
                progress.make_callback(task.sha256),
            ): task.sha256
            for task in tasks
        }
        try:
            for future in concurrent.futures.as_completed(future_to_hash):
                hash_value = future_to_hash[future]
                restored_files = future.result()
                summary.downloaded_objects += 1
                summary.restored_files += restored_files
                progress.mark_done(hash_value)
        except Exception:
            for future in future_to_hash:
                future.cancel()
            progress.finish()
            raise

    progress.finish()

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


def _existing_file_matches_hash(path: Path, expected_sha256: str) -> bool:
    if not path.exists() or not path.is_file():
        return False
    return sha256_file(path) == expected_sha256


def _check_existing_download_targets_parallel(
    items: Sequence[DownloadItem],
    workers: int,
) -> List[bool]:
    if not items:
        return []

    tasks = [(str(item.output_path), item.sha256) for item in items]
    if workers <= 1 or len(tasks) < 2:
        return [_check_download_target_task(task) for task in tasks]

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(_check_download_target_task, tasks, chunksize=8))


def _check_download_target_task(task: tuple[str, str]) -> bool:
    path_str, expected_sha256 = task
    return _existing_file_matches_hash(Path(path_str), expected_sha256)


def _download_object(
    storage: StorageClient,
    method: str,
    object_key: str,
    destination: Path,
    progress: DownloadProgressCallback | None = None,
) -> None:
    if method == "https":
        storage.download_https(object_key, destination, progress=progress)
        return

    if method == "s3":
        storage.download_file(object_key, destination, progress=progress)
        return

    if method == "auto":
        if storage.config.https_base_url:
            try:
                storage.download_https(object_key, destination, progress=progress)
                return
            except Exception:
                if storage.can_use_s3:
                    storage.download_file(object_key, destination, progress=progress)
                    return
                raise
        if storage.can_use_s3:
            storage.download_file(object_key, destination, progress=progress)
            return
        raise RuntimeError("No download backend available. Configure HTTPS_BASE_URL or S3 access.")

    raise ValueError(f"Unsupported method: {method}")


def _download_and_restore_task(
    storage: StorageClient,
    method: str,
    task: DownloadTask,
    progress: DownloadProgressCallback | None = None,
) -> int:
    with tempfile.TemporaryDirectory(prefix="ota-dl-") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        gz_path = tmp_dir_path / f"{task.sha256}.gz"
        raw_path = tmp_dir_path / f"{task.sha256}.bin"

        _download_object(
            storage,
            method,
            task.object_key,
            gz_path,
            progress=progress,
        )
        actual_hash = gunzip_to_file_and_hash(gz_path, raw_path)
        if actual_hash != task.sha256:
            raise ValueError(
                f"SHA256 mismatch for object {task.object_key}: expected {task.sha256}, got {actual_hash}"
            )

        for item in task.items_to_restore:
            item.output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(raw_path, item.output_path)
        return len(task.items_to_restore)


def _iter_network_files(network_root: Path) -> Iterable[tuple[str, Path]]:
    files = [p for p in network_root.rglob("*") if p.is_file()]
    for path in sorted(files, key=lambda p: p.relative_to(network_root).as_posix()):
        rel_path = path.relative_to(network_root).as_posix()
        yield rel_path, path


class _DownloadProgressBar:
    def __init__(self, total_objects: int, width: int = 32) -> None:
        self.total_objects = max(1, total_objects)
        self.width = max(10, width)
        self._lock = threading.Lock()
        self._completed_objects = 0
        self._done_tasks: set[str] = set()
        self._downloaded_by_task: Dict[str, int] = {}
        self._total_by_task: Dict[str, int] = {}
        self._last_render = 0.0
        self._max_progress = 0.0
        self._last_line_len = 0
        self._printed = False

    def make_callback(self, task_id: str) -> DownloadProgressCallback:
        def report(downloaded: int, total: int | None) -> None:
            with self._lock:
                self._downloaded_by_task[task_id] = max(0, downloaded)
                if total is not None and total > 0:
                    self._total_by_task[task_id] = total
                self._render_locked(force=False)

        return report

    def mark_done(self, task_id: str) -> None:
        with self._lock:
            if task_id not in self._done_tasks:
                self._done_tasks.add(task_id)
                self._completed_objects += 1
            total = self._total_by_task.get(task_id)
            if total is not None:
                self._downloaded_by_task[task_id] = max(self._downloaded_by_task.get(task_id, 0), total)
            self._render_locked(force=True)

    def finish(self) -> None:
        with self._lock:
            self._render_locked(force=True)
            if self._printed:
                print(file=sys.stdout, flush=True)

    def _render_locked(self, force: bool) -> None:
        now = time.monotonic()
        if not force and now - self._last_render < 0.1:
            return

        downloaded = sum(self._downloaded_by_task.values())
        known_total = sum(self._total_by_task.values())
        partial_objects = 0.0
        for task_id, task_downloaded in self._downloaded_by_task.items():
            if task_id in self._done_tasks:
                continue
            task_total = self._total_by_task.get(task_id)
            if task_total is None or task_total <= 0:
                continue
            partial_objects += min(0.999, max(0.0, task_downloaded / task_total))

        object_progress = min(
            1.0, (self._completed_objects + partial_objects) / self.total_objects
        )
        bar_progress = max(self._max_progress, object_progress)
        self._max_progress = bar_progress
        filled = min(self.width, max(0, int(bar_progress * self.width)))
        bar = "#" * filled + "-" * (self.width - filled)

        if known_total > 0:
            detail = (
                f"{bar_progress * 100:6.2f}% | objects {self._completed_objects}/{self.total_objects} | "
                f"bytes {_format_size(downloaded)}/{_format_size(known_total)}"
            )
        else:
            detail = (
                f"{object_progress * 100:6.2f}% | objects {self._completed_objects}/{self.total_objects} | "
                f"downloaded {_format_size(downloaded)}"
            )

        line = f"[{bar}] {detail}"
        pad = " " * max(0, self._last_line_len - len(line))
        print(f"\r{line}{pad}", end="", file=sys.stdout, flush=True)
        self._last_line_len = len(line)
        self._printed = True
        self._last_render = now


def _format_size(num_bytes: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(max(0, num_bytes))
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def _collect_upload_file_records(config: AppConfig, networks: Sequence[str]) -> List[UploadFileRecord]:
    records: List[UploadFileRecord] = []
    for network in networks:
        network_root = config.source_root / network
        if not network_root.exists():
            continue
        for rel_path, abs_path in _iter_network_files(network_root):
            records.append(UploadFileRecord(network=network, rel_path=rel_path, abs_path=abs_path))
    return records


def _collect_additional_logs_file_records(additional_logs_root: Path) -> List[UploadFileRecord]:
    if not additional_logs_root.exists():
        return []
    records: List[UploadFileRecord] = []
    for rel_path, abs_path in _iter_network_files(additional_logs_root):
        records.append(
            UploadFileRecord(
                network=ADDITIONAL_LOGS_NETWORK,
                rel_path=rel_path,
                abs_path=abs_path,
            )
        )
    return records


def _hash_files_parallel(
    file_records: Sequence[UploadFileRecord],
    workers: int,
    reporter: Callable[[str], None] | None = None,
) -> List[HashedFileRecord]:
    if not file_records:
        return []

    total = len(file_records)
    progress_step = _progress_step(total)
    paths = [str(record.abs_path) for record in file_records]
    if workers <= 1 or len(file_records) < 2:
        hashed = []
        for idx, path in enumerate(paths, start=1):
            hashed.append(_hash_file_task(path))
            if reporter and (idx % progress_step == 0 or idx == total):
                reporter(f"hash progress: {idx}/{total}")
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            hashed = []
            for idx, result in enumerate(executor.map(_hash_file_task, paths, chunksize=8), start=1):
                hashed.append(result)
                if reporter and (idx % progress_step == 0 or idx == total):
                    reporter(f"hash progress: {idx}/{total}")

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
    reporter: Callable[[str], None] | None = None,
) -> Dict[str, bool]:
    if not hashes:
        return {}

    total = len(hashes)
    progress_step = _progress_step(total)

    def check_one(sha256_hex: str) -> tuple[str, bool]:
        object_key = build_object_key(config, sha256_hex)
        return sha256_hex, storage.object_exists(object_key)

    results: Dict[str, bool] = {}
    workers = max(1, min(config.head_threads, len(hashes)))
    if workers <= 1:
        for idx, sha256_hex in enumerate(hashes, start=1):
            key, exists = check_one(sha256_hex)
            results[key] = exists
            if reporter and (idx % progress_step == 0 or idx == total):
                reporter(f"remote check progress: {idx}/{total}")
        return results

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_hash = {executor.submit(check_one, h): h for h in hashes}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_hash):
            key, exists = future.result()
            results[key] = exists
            completed += 1
            if reporter and (completed % progress_step == 0 or completed == total):
                reporter(f"remote check progress: {completed}/{total}")
    return results


def _compress_and_upload_missing_hashes(
    storage: StorageClient,
    config: AppConfig,
    hash_to_source_path: Dict[str, Path],
    missing_hashes: Sequence[str],
    reporter: Callable[[str], None] | None = None,
) -> Dict[str, int]:
    compressed_sizes: Dict[str, int] = {}
    if not missing_hashes:
        return compressed_sizes

    total = len(missing_hashes)
    progress_step = _progress_step(total)
    compress_workers = max(1, min(config.compress_processes, len(missing_hashes)))
    upload_workers = max(1, min(config.upload_threads, len(missing_hashes)))

    upload_futures: List[concurrent.futures.Future[tuple[str, int]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=upload_workers) as upload_executor:
        if compress_workers <= 1:
            for idx, sha256_hex in enumerate(missing_hashes, start=1):
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
                if reporter and (idx % progress_step == 0 or idx == total):
                    reporter(f"compression progress: {idx}/{total}")
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=compress_workers) as compress_executor:
                compression_jobs = {
                    compress_executor.submit(_compress_file_task, str(hash_to_source_path[sha256_hex])): sha256_hex
                    for sha256_hex in missing_hashes
                }
                compressed_done = 0
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
                    compressed_done += 1
                    if reporter and (compressed_done % progress_step == 0 or compressed_done == total):
                        reporter(f"compression progress: {compressed_done}/{total}")

        uploaded_done = 0
        for future in concurrent.futures.as_completed(upload_futures):
            sha256_hex, compressed_size = future.result()
            compressed_sizes[sha256_hex] = compressed_size
            uploaded_done += 1
            if reporter and (uploaded_done % progress_step == 0 or uploaded_done == total):
                reporter(f"upload progress: {uploaded_done}/{total}")

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


def _find_changed_file_paths(
    network_entries: Dict[str, Dict[str, Dict[str, Any]]],
    network_manifests: Dict[str, Dict[str, Any]],
) -> List[tuple[str, str, str, str]]:
    changed: List[tuple[str, str, str, str]] = []
    for network, entries in network_entries.items():
        existing_files = network_manifests.get(network, {}).get("files", {})
        for rel_path, new_entry in entries.items():
            old_entry = existing_files.get(rel_path)
            if not isinstance(old_entry, dict):
                continue
            old_sha = str(old_entry.get("sha256", "")).strip()
            new_sha = str(new_entry.get("sha256", "")).strip()
            if old_sha and new_sha and old_sha != new_sha:
                changed.append((network, rel_path, old_sha, new_sha))
    return changed


def _make_upload_reporter(enabled: bool) -> Callable[[str], None]:
    if not enabled:
        return _noop_reporter

    def report(message: str) -> None:
        print(f"[upload] {message}", flush=True)

    return report


def _noop_reporter(_: str) -> None:
    return None


def _progress_step(total: int) -> int:
    if total <= 0:
        return 1
    return max(1, total // 20)
