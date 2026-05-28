"""SQL Server sink implementation."""

from __future__ import annotations

import logging
import threading
import time
from typing import Iterable, List, Mapping, Sequence

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pyodbc = None

from .. import config

LOGGER = logging.getLogger(__name__)

SQL_COLUMNS = config.SQL_COLUMNS


class SqlServerSink:
    """Persist metadata in SQL Server with retry handling."""

    def __init__(self, connection_string: str, table: str = "dbo.files") -> None:
        if pyodbc is None:
            raise RuntimeError("pyodbc is required for SQL Server support.")
        self.table = table
        self._conn = pyodbc.connect(connection_string, autocommit=False)
        self._lock = threading.Lock()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            IF OBJECT_ID('{self.table}', 'U') IS NULL
            BEGIN
                CREATE TABLE {self.table} (
                    id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    file_name NVARCHAR(260),
                    extension NVARCHAR(32),
                    mime_type NVARCHAR(128),
                    size_bytes BIGINT,
                    created_time DATETIME NULL,
                    modified_time DATETIME NULL,
                    accessed_time DATETIME NULL,
                    hash_algo NVARCHAR(16),
                    hash_value NVARCHAR(128),
                    hash_value_dst NVARCHAR(128),
                    hash_verified NVARCHAR(32),
                    src_path NVARCHAR(512) UNIQUE,
                    dst_path NVARCHAR(512),
                    gestor NVARCHAR(128),
                    proyecto NVARCHAR(128),
                    year NVARCHAR(16),
                    presupuesto_detectado NVARCHAR(64),
                    cliente NVARCHAR(260),
                    sede_hotel_direccion NVARCHAR(260),
                    referencia NVARCHAR(512),
                    origen_asignacion NVARCHAR(64),
                    clave_interna NVARCHAR(128),
                    tipo_documento NVARCHAR(32),
                    match_status NVARCHAR(64),
                    match_confidence FLOAT,
                    match_source NVARCHAR(64),
                    match_reason NVARCHAR(512),
                    texto_detectado NVARCHAR(512),
                    duplicado_anio_presupuesto NVARCHAR(16),
                    action NVARCHAR(32),
                    action_status NVARCHAR(32),
                    error NVARCHAR(MAX),
                    verified BIT DEFAULT(0)
                );
                CREATE INDEX IX_files_hash_value ON {self.table}(hash_value);
            END
            """
        )
        self._conn.commit()
        column_types = {
            "gestor": "NVARCHAR(128)",
            "proyecto": "NVARCHAR(128)",
            "year": "NVARCHAR(16)",
            "presupuesto_detectado": "NVARCHAR(64)",
            "cliente": "NVARCHAR(260)",
            "sede_hotel_direccion": "NVARCHAR(260)",
            "referencia": "NVARCHAR(512)",
            "origen_asignacion": "NVARCHAR(64)",
            "clave_interna": "NVARCHAR(128)",
            "tipo_documento": "NVARCHAR(32)",
            "match_status": "NVARCHAR(64)",
            "match_confidence": "FLOAT",
            "match_source": "NVARCHAR(64)",
            "match_reason": "NVARCHAR(512)",
            "texto_detectado": "NVARCHAR(512)",
            "duplicado_anio_presupuesto": "NVARCHAR(16)",
            "verified": "BIT DEFAULT(0)",
        }
        for column, ddl in column_types.items():
            cursor.execute(
                f"""
                IF COL_LENGTH('{self.table}', '{column}') IS NULL
                    ALTER TABLE {self.table} ADD {column} {ddl};
                """
            )
        # Mantiene el esquema actualizado aunque la tabla viniera de otra version.
        self._conn.commit()

    def insert_records(
        self,
        records: Iterable[Mapping[str, object]],
        retries: int = 3,
    ) -> None:
        payload = [self._row_from_record(record) for record in records]
        if not payload:
            return

        update_columns = [column for column in SQL_COLUMNS if column != "src_path"]
        update_assignments = ",\n                ".join(
            f"{column} = source.{column}" for column in update_columns
        )

        query = f"""
            MERGE {self.table} WITH (HOLDLOCK) AS target
            USING (VALUES ({', '.join(['?'] * len(SQL_COLUMNS))})) AS source
            ({', '.join(SQL_COLUMNS)})
            ON target.src_path = source.src_path
            WHEN MATCHED THEN UPDATE SET
                {update_assignments}
            WHEN NOT MATCHED THEN INSERT
                ({', '.join(SQL_COLUMNS)})
                VALUES ({', '.join(['source.' + col for col in SQL_COLUMNS])});
        """

        attempt = 0
        while True:
            try:
                with self._lock:
                    cursor = self._conn.cursor()
                    cursor.fast_executemany = True
                    cursor.executemany(query, payload)
                    self._conn.commit()
                break
            except pyodbc.Error as exc:
                self._conn.rollback()
                if attempt >= retries:
                    raise
                wait_seconds = 2**attempt
                LOGGER.warning(
                    "SQL Server insert failed (%s). Retrying in %ss...", exc, wait_seconds
                )
                time.sleep(wait_seconds)
                attempt += 1

    def _row_from_record(self, record: Mapping[str, object]) -> List[object]:
        row: List[object] = []
        for column in SQL_COLUMNS:
            value = record.get(column)
            if column == "verified":
                value = 1 if bool(value) else 0
            row.append(value)
        return row

    def close(self) -> None:
        with self._lock:
            self._conn.close()
