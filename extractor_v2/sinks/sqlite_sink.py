"""SQLite sink implementation."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence

from .. import config

LOGGER = logging.getLogger(__name__)


SQL_COLUMNS = [
    "file_name",
    "extension",
    "mime_type",
    "size_bytes",
    "created_time",
    "modified_time",
    "accessed_time",
    "hash_algo",
    "hash_value",
    "hash_value_dst",
    "hash_verified",
    "src_path",
    "dst_path",
    "gestor",
    "proyecto",
    "action",
    "action_status",
    "error",
    "verified",
]


class SQLiteSink:
    """Persists metadata into a SQLite database."""

    def __init__(self, path: Path, timeout: int = 30) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), timeout=timeout, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                extension TEXT,
                mime_type TEXT,
                size_bytes INTEGER,
                created_time TEXT,
                modified_time TEXT,
                accessed_time TEXT,
                hash_algo TEXT,
                hash_value TEXT,
                hash_value_dst TEXT,
                hash_verified TEXT,
                src_path TEXT UNIQUE,
                dst_path TEXT,
                gestor TEXT,
                proyecto TEXT,
                action TEXT,
                action_status TEXT,
                error TEXT,
                verified INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_hash_value ON files(hash_value)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_dst_path ON files(dst_path)"
        )
        self._conn.commit()

        # ensure new columns exist for legacy DBs
        cursor.execute("PRAGMA table_info(files)")
        columns = {row["name"] for row in cursor.fetchall()}
        migrations: List[tuple[str, str]] = []
        if "verified" not in columns:
            migrations.append(("verified", "INTEGER DEFAULT 0"))
        if "gestor" not in columns:
            migrations.append(("gestor", "TEXT"))
        if "proyecto" not in columns:
            migrations.append(("proyecto", "TEXT"))

        for column, ddl in migrations:
            LOGGER.info("Adding missing '%s' column to SQLite database.", column)
            cursor.execute(f"ALTER TABLE files ADD COLUMN {column} {ddl}")
        self._conn.commit()

    def insert_records(self, records: Iterable[Mapping[str, object]], retries: int = 3) -> None:
        payload = [self._row_from_record(record) for record in records]
        if not payload:
            return

        query = f"""
            INSERT INTO files ({", ".join(SQL_COLUMNS)})
            VALUES ({", ".join(["?"] * len(SQL_COLUMNS))})
            ON CONFLICT(src_path) DO UPDATE SET
                file_name=excluded.file_name,
                extension=excluded.extension,
                mime_type=excluded.mime_type,
                size_bytes=excluded.size_bytes,
                created_time=excluded.created_time,
                modified_time=excluded.modified_time,
                accessed_time=excluded.accessed_time,
                hash_algo=excluded.hash_algo,
                hash_value=excluded.hash_value,
                hash_value_dst=excluded.hash_value_dst,
                hash_verified=excluded.hash_verified,
                dst_path=excluded.dst_path,
                gestor=excluded.gestor,
                proyecto=excluded.proyecto,
                action=excluded.action,
                action_status=excluded.action_status,
                error=excluded.error,
                verified=excluded.verified
        """

        attempt = 0
        while True:
            try:
                with self._lock:
                    self._conn.executemany(query, payload)
                    self._conn.commit()
                break
            except sqlite3.DatabaseError as exc:
                self._conn.rollback()
                if attempt >= retries:
                    raise
                sleep_for = 2**attempt
                LOGGER.warning(
                    "SQLite insert failed (%s). Retrying in %ss...", exc, sleep_for
                )
                time.sleep(sleep_for)
                attempt += 1

    def _row_from_record(self, record: Mapping[str, object]) -> List[object]:
        row: List[object] = []
        for column in SQL_COLUMNS:
            value = record.get(column)
            if column == "verified":
                value = 1 if bool(value) else 0
            row.append(value)
        return row

    def fetch_existing_cache(self) -> dict[str, sqlite3.Row]:
        """Return current rows keyed by src_path for incremental runs."""
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT
                src_path,
                hash_value,
                hash_algo,
                size_bytes,
                modified_time,
                hash_verified,
                verified,
                hash_value_dst,
                dst_path
            FROM files
            """
        )
        return {row["src_path"]: row for row in cursor.fetchall()}

    def close(self) -> None:
        with self._lock:
            self._conn.close()
