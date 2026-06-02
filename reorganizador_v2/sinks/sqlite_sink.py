"""SQLite sink implementation."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence

from .. import config

LOGGER = logging.getLogger(__name__)

SQL_COLUMNS = config.SQL_COLUMNS


class SQLiteSink:
    """Persists metadata into a SQLite database."""

    def __init__(self, path: Path, timeout: int = 30) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), timeout=timeout, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._ensure_schema()
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA synchronous=NORMAL')

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
                year TEXT,
                presupuesto_detectado TEXT,
                cliente TEXT,
                sede_hotel_direccion TEXT,
                referencia TEXT,
                origen_asignacion TEXT,
                clave_interna TEXT,
                tipo_documento TEXT,
                match_status TEXT,
                match_confidence REAL,
                match_source TEXT,
                match_reason TEXT,
                texto_detectado TEXT,
                duplicado_anio_presupuesto TEXT,
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
        
        # Tabla de checkpoints para resume tras interrupcion.
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                src_path TEXT UNIQUE,
                saved_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.commit()
        self._conn.commit()

        # Comprueba que las columnas nuevas existan en bases antiguas.
        cursor.execute("PRAGMA table_info(files)")
        columns = {row["name"] for row in cursor.fetchall()}
        column_types = {
            "size_bytes": "INTEGER",
            "match_confidence": "REAL",
            "verified": "INTEGER DEFAULT 0",
        }
        migrations: List[tuple[str, str]] = [
            (column, column_types.get(column, "TEXT"))
            for column in SQL_COLUMNS
            if column not in columns
        ]

        for column, ddl in migrations:
            # Aplica migraciones simples solo cuando hacen falta para no romper instalaciones previas.
            LOGGER.info("Adding missing '%s' column to SQLite database.", column)
            cursor.execute(f"ALTER TABLE files ADD COLUMN {column} {ddl}")
        self._conn.commit()

    def insert_records(self, records: Iterable[Mapping[str, object]], retries: int = 3) -> None:
        payload = [self._row_from_record(record) for record in records]
        if not payload:
            return

        update_columns = [column for column in SQL_COLUMNS if column != "src_path"]
        update_assignments = ",\n                ".join(
            f"{column}=excluded.{column}" for column in update_columns
        )

        query = f"""
            INSERT INTO files ({", ".join(SQL_COLUMNS)})
            VALUES ({", ".join(["?"] * len(SQL_COLUMNS))})
            ON CONFLICT(src_path) DO UPDATE SET
                {update_assignments}
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

    
    def save_checkpoint(self, src_path: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO checkpoints (src_path) VALUES (?)
                ON CONFLICT(src_path) DO UPDATE SET
                    src_path = excluded.src_path,
                    saved_at = datetime('now')
                """,
                (src_path,),
            )
            self._conn.commit()

    def get_checkpoint(self) -> Optional[str]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT src_path FROM checkpoints ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        return row["src_path"] if row else None
    def close(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()
