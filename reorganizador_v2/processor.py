"""Core processing logic for extractor v2."""

from __future__ import annotations

import csv
import logging
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence

from . import config, file_utils
from .sinks.csv_sink import CsvSink
from .sinks.sqlite_sink import SQLiteSink
from .sinks.sqlserver_sink import SqlServerSink

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessingOptions:
    source_root: Path
    dest_root: Optional[Path] = None
    organize_by: str = "type-date"
    move_files: bool = False
    dry_run: bool = False
    hash_algorithm: str = config.DEFAULT_HASH
    incremental: bool = True
    verify_hash: bool = True
    threads: int = config.DEFAULT_THREADS
    processes: int = 0
    conflict: str = "rename"
    dedup: bool = False
    resume: bool = False


@dataclass(slots=True)
class FileMetadata:
    path: Path
    file_name: str
    extension: str
    mime_type: str
    size_bytes: int
    created_time: str
    modified_time: str
    accessed_time: str
    ctime_ts: float
    mtime_ts: float
    atime_ts: float


@dataclass(slots=True)
class CacheEntry:
    hash_value: Optional[str] = None
    hash_algo: Optional[str] = None
    size_bytes: Optional[int] = None
    modified_ts: Optional[float] = None
    hash_verified: Optional[str] = None
    dest_path: Optional[str] = None
    dest_hash: Optional[str] = None
    verified: bool = False

    def matches(self, size: int, modified_ts: float, algorithm: str) -> bool:
        if not self.hash_value:
            return False
        if self.hash_algo and self.hash_algo.lower() != algorithm.lower():
            return False
        if self.size_bytes is not None and self.size_bytes != size:
            return False
        if self.modified_ts is not None and abs(self.modified_ts - modified_ts) > 1:
            return False
        return True


@dataclass(slots=True)
class ProcessingRecord:
    metadata: FileMetadata
    action: str
    action_status: str
    hash_value: Optional[str]
    hash_algo: str
    hash_value_dst: Optional[str]
    hash_verified: str
    dest_path: Optional[str]
    gestor: Optional[str] = None
    proyecto: Optional[str] = None
    error: Optional[str] = None
    verified: bool = False
    duration_hash: float = 0.0

    def to_row(self, record_id: int) -> Dict[str, object]:
        return {
            "id": record_id,
            "file_name": self.metadata.file_name,
            "extension": self.metadata.extension,
            "mime_type": self.metadata.mime_type,
            "size_bytes": self.metadata.size_bytes,
            "created_time": self.metadata.created_time,
            "modified_time": self.metadata.modified_time,
            "accessed_time": self.metadata.accessed_time,
            "hash_algo": self.hash_algo,
            "hash_value": self.hash_value,
            "hash_value_dst": self.hash_value_dst,
            "hash_verified": self.hash_verified,
            "src_path": str(self.metadata.path),
            "dst_path": self.dest_path,
            "gestor": self.gestor,
            "proyecto": self.proyecto,
            "action": self.action,
            "action_status": self.action_status,
            "error": self.error,
            "verified": self.verified,
        }


class MetadataCache:
    def __init__(self) -> None:
        self._entries: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()

    def load_from_sqlite(self, sink: SQLiteSink) -> None:
        try:
            rows = sink.fetch_existing_cache()
        except Exception as exc:
            LOGGER.warning("Unable to load SQLite cache: %s", exc)
            return
        for src_path, row in rows.items():
            data = dict(row)
            self._entries[src_path] = CacheEntry(
                hash_value=data.get("hash_value"),
                hash_algo=data.get("hash_algo"),
                size_bytes=data.get("size_bytes"),
                modified_ts=_parse_timestamp(data.get("modified_time")),
                hash_verified=data.get("hash_verified"),
                dest_path=data.get("dst_path"),
                dest_hash=data.get("hash_value_dst"),
                verified=bool(data.get("verified")),
            )

    def load_from_csv(self, csv_path: Path) -> None:
        if not csv_path.exists():
            return
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    src_path = row.get("src_path")
                    if not src_path:
                        continue
                    entry = self._entries.get(src_path, CacheEntry())
                    entry.hash_value = entry.hash_value or row.get("hash_value")
                    entry.hash_algo = entry.hash_algo or row.get("hash_algo")
                    entry.size_bytes = entry.size_bytes if entry.size_bytes is not None else _safe_int(row.get("size_bytes"))
                    entry.modified_ts = entry.modified_ts if entry.modified_ts is not None else _parse_timestamp(row.get("modified_time"))
                    entry.hash_verified = entry.hash_verified or row.get("hash_verified")
                    entry.dest_path = entry.dest_path or row.get("dst_path")
                    entry.dest_hash = entry.dest_hash or row.get("hash_value_dst")
                    entry.verified = entry.verified if entry.verified is not False else (row.get("verified") in {"1", "true", "True", "ok"})
                    self._entries[src_path] = entry
        except Exception as exc:
            LOGGER.warning("Unable to load CSV cache: %s", exc)

    def get(self, path: Path) -> Optional[CacheEntry]:
        return self._entries.get(str(path))

    def update(self, record: ProcessingRecord) -> None:
        self._entries[str(record.metadata.path)] = CacheEntry(
            hash_value=record.hash_value,
            hash_algo=record.hash_algo,
            size_bytes=record.metadata.size_bytes,
            modified_ts=record.metadata.mtime_ts,
            hash_verified=record.hash_verified,
            dest_path=record.dest_path,
            dest_hash=record.hash_value_dst,
            verified=record.verified,
        )


def _hash_worker(path_str: str, algorithm: str) -> file_utils.HashResult:
    return file_utils.compute_hash(Path(path_str), algorithm)


class HashCalculator:
    def __init__(self, algorithm: str, processes: int = 0) -> None:
        self.algorithm = algorithm.lower()
        self._executor: Optional[ProcessPoolExecutor] = None
        if processes and processes > 1:
            self._executor = ProcessPoolExecutor(max_workers=processes)

    def compute(self, path: Path) -> file_utils.HashResult:
        if not self._executor:
            return file_utils.compute_hash(path, self.algorithm)
        future = self._executor.submit(_hash_worker, str(path), self.algorithm)
        return future.result()

    def shutdown(self) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)


class FileProcessor:
    def __init__(self, options: ProcessingOptions, cache: MetadataCache) -> None:
        self.options = options
        self.cache = cache
        self.hash_calculator = HashCalculator(options.hash_algorithm, options.processes)
        self._dedup_map: dict[str, Path] = {}

    def gather_metadata(self, path: Path) -> FileMetadata:
        stats = path.stat()
        return FileMetadata(
            path=path,
            file_name=path.name,
            extension=path.suffix.lower(),
            mime_type=file_utils.guess_mime(path),
            size_bytes=stats.st_size,
            created_time=file_utils.format_datetime(stats.st_ctime),
            modified_time=file_utils.format_datetime(stats.st_mtime),
            accessed_time=file_utils.format_datetime(stats.st_atime),
            ctime_ts=stats.st_ctime,
            mtime_ts=stats.st_mtime,
            atime_ts=stats.st_atime,
        )

    def process_path(self, path: Path) -> ProcessingRecord:
        metadata = self.gather_metadata(path)
        cache_entry = self.cache.get(path)
        gestor, proyecto = file_utils.extract_manager_project(
            path=path, source_root=self.options.source_root
        )

        action = "scan"
        dest_path: Optional[Path] = None
        dest_path_str: Optional[str] = None

        if self.options.dest_root:
            action = "move" if self.options.move_files else "copy"
            candidate = file_utils.build_destination_path(
                src=path, dest_root=self.options.dest_root,
                source_root=self.options.source_root,
                mode=self.options.organize_by, modified_time=metadata.mtime_ts,
            )
            dest_path = candidate
            dest_path_str = str(dest_path)

        # --- Incremental skip ---
        cached_dest_path = Path(cache_entry.dest_path) if cache_entry and cache_entry.dest_path else None
        should_skip = (
            self.options.incremental
            and not self.options.move_files
            and cache_entry is not None
            and cached_dest_path is not None
            and cached_dest_path.exists()
            and cache_entry.matches(metadata.size_bytes, metadata.mtime_ts, self.options.hash_algorithm)
        )

        if should_skip and self.options.conflict != "overwrite":
            dest_path = cached_dest_path
            dest_path_str = cache_entry.dest_path
            record = ProcessingRecord(
                metadata=metadata, action="skip", action_status="skipped",
                hash_value=cache_entry.hash_value,
                hash_algo=cache_entry.hash_algo or self.options.hash_algorithm,
                hash_value_dst=cache_entry.dest_hash,
                hash_verified=cache_entry.hash_verified or "cached",
                dest_path=cache_entry.dest_path, gestor=gestor, proyecto=proyecto,
                error=None, verified=cache_entry.verified,
            )
            return record

        # --- Hash ---
        use_cached_hash = (
            self.options.incremental
            and cache_entry is not None
            and cache_entry.matches(metadata.size_bytes, metadata.mtime_ts, self.options.hash_algorithm)
        )
        if use_cached_hash:
            hash_result = file_utils.HashResult(
                algorithm=cache_entry.hash_algo or self.options.hash_algorithm,
                value=cache_entry.hash_value, duration_seconds=0.0,
            )
        else:
            hash_result = self.hash_calculator.compute(path)

        hash_value_dst = cache_entry.dest_hash if cache_entry else None
        hash_verified = cache_entry.hash_verified if cache_entry else "pending"
        verified_flag = cache_entry.verified if cache_entry else False

        error: Optional[str] = None
        action_status = "ok"

        if dest_path and action != "scan":
            # --- Conflict resolution ---
            if dest_path.exists() and file_utils.should_overwrite(
                dest_path, metadata.mtime_ts, self.options.conflict
            ):
                dest_path.unlink(missing_ok=True)

            if dest_path.exists() and self.options.conflict == "skip" and not self.options.move_files:
                action = "skip"
                action_status = "skipped"
            elif not self.options.dry_run:
                if dest_path.exists() and self.options.conflict == file_utils.ConflictStrategy.RENAME:
                    dest_path = file_utils.ensure_unique_path(dest_path)
                    dest_path_str = str(dest_path)
                try:
                    # --- Dedup via hardlink ---
                    if (
                        self.options.dedup
                        and not self.options.move_files
                        and hash_result.value
                        and hash_result.value in self._dedup_map
                    ):
                        existing_dest = self._dedup_map[hash_result.value]
                        try:
                            file_utils.safe_makedirs(dest_path.parent)
                            os.link(str(existing_dest), file_utils._win_path(dest_path))
                            dedup_action = "hardlink"
                        except OSError:
                            file_utils.copy_file(path, dest_path)
                            dedup_action = "copy"
                    elif self.options.move_files:
                        file_utils.move_file(path, dest_path)
                        dedup_action = "move"
                    else:
                        file_utils.copy_file(path, dest_path)
                        dedup_action = "copy"

                    if dedup_action != "move":
                        action = dedup_action

                except Exception as exc:
                    error = str(exc)
                    action_status = "error"
                    LOGGER.exception("Failed processing %s: %s", path, exc)

        if dest_path is None:
            hash_verified = "n/a"
        elif self.options.dry_run:
            hash_verified = "dry-run"
        elif action_status == "ok" and self.options.verify_hash and hash_result.value and dest_path.exists():
            dst_result = self.hash_calculator.compute(dest_path)
            hash_value_dst = dst_result.value
            verified_flag = hash_value_dst == hash_result.value
            hash_verified = "ok" if verified_flag else "mismatch"
            if not verified_flag:
                action_status = "error"
                error = "Hash verification failed: destination hash does not match source hash."
        elif action_status == "ok" and self.options.verify_hash and self.options.hash_algorithm == "none":
            hash_verified = "n/a"
        elif action_status == "ok" and not self.options.verify_hash:
            hash_verified = "skipped"

        record = ProcessingRecord(
            metadata=metadata, action=action, action_status=action_status,
            hash_value=hash_result.value, hash_algo=hash_result.algorithm,
            hash_value_dst=hash_value_dst, hash_verified=hash_verified,
            dest_path=dest_path_str, gestor=gestor, proyecto=proyecto,
            error=error, verified=verified_flag,
            duration_hash=hash_result.duration_seconds,
        )

        if error is None:
            self.cache.update(record)
            if self.options.dedup and hash_result.value and dest_path:
                self._dedup_map[hash_result.value] = dest_path

        return record

    def shutdown(self) -> None:
        self.hash_calculator.shutdown()


class SinkManager:
    def __init__(
        self,
        csv_sink: Optional[CsvSink] = None,
        sqlite_sink: Optional[SQLiteSink] = None,
        sqlserver_sink: Optional[SqlServerSink] = None,
        batch_size: int = 100,
    ) -> None:
        self.csv_sink = csv_sink
        self.sqlite_sink = sqlite_sink
        self.sqlserver_sink = sqlserver_sink
        self.batch_size = batch_size
        self._buffer: List[Dict[str, object]] = []
        self._counter = 1
        self._lock = threading.Lock()

    def append(self, record: ProcessingRecord) -> None:
        row = record.to_row(self._counter)
        with self._lock:
            self._buffer.append(row)
            self._counter += 1
            if len(self._buffer) >= self.batch_size:
                self._flush_locked()

    def _flush_locked(self) -> None:
        rows = list(self._buffer)
        self._buffer.clear()
        if not rows:
            return
        if self.csv_sink:
            self.csv_sink.append_records(rows)
        if self.sqlite_sink:
            self.sqlite_sink.insert_records(rows)
            # Checkpoint para resume tras interrupcion.
            last_row = rows[-1]
            if last_row.get('src_path'):
                self.sqlite_sink.save_checkpoint(str(last_row['src_path']))
        if self.sqlserver_sink:
            self.sqlserver_sink.insert_records(rows)

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        self.flush()
        if self.sqlserver_sink:
            self.sqlserver_sink.close()
        if self.sqlite_sink:
            self.sqlite_sink.close()


class BatchProcessor:
    def __init__(self, file_processor: FileProcessor, sink_manager: SinkManager) -> None:
        self.file_processor = file_processor
        self.sink_manager = sink_manager

    def process_paths(
        self,
        paths: Sequence[Path],
        progress_callback: Optional[Callable[[ProcessingRecord], None]] = None,
    ) -> Iterator[ProcessingRecord]:
        with ThreadPoolExecutor(max_workers=self.file_processor.options.threads) as executor:
            future_map = {
                executor.submit(self.file_processor.process_path, path): path for path in paths
            }
            for future in as_completed(future_map):
                record = future.result()
                self.sink_manager.append(record)
                if record.action == "move" and record.action_status == "ok":
                    self.sink_manager.flush()
                if progress_callback:
                    progress_callback(record)
                yield record

    def close(self) -> None:
        self.file_processor.shutdown()
        self.sink_manager.close()


def _parse_timestamp(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").timestamp()
    except ValueError:
        return None


def _safe_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None
