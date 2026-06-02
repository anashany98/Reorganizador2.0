"""Tests for SQL Server sink without requiring a real database."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from reorganizador_v2.sinks.sqlserver_sink import SqlServerSink


class TestRowFromRecord:
    """Tests for _row_from_record, the pure method that builds row values."""

    @pytest.fixture
    def sink(self) -> SqlServerSink:
        with patch(
            "reorganizador_v2.sinks.sqlserver_sink.pyodbc"
        ) as mock_pyodbc:
            mock_pyodbc.connect.return_value = MagicMock()
            return SqlServerSink("mock_connection_string")

    def test_maps_columns_in_order(self, sink: SqlServerSink) -> None:
        record = {
            "file_name": "test.pdf",
            "extension": ".pdf",
            "mime_type": "application/pdf",
            "size_bytes": 1234,
            "created_time": "2025-01-01",
            "modified_time": "2025-01-02",
            "accessed_time": "2025-01-03",
            "hash_algo": "sha256",
            "hash_value": "abc123",
            "hash_value_dst": "abc123",
            "hash_verified": "ok",
            "src_path": "C:/src/test.pdf",
            "dst_path": "C:/dst/test.pdf",
            "gestor": "MAR",
            "proyecto": "250076",
            "year": "2025",
            "presupuesto_detectado": "250076",
            "cliente": "MELIA",
            "sede_hotel_direccion": "HOTEL",
            "referencia": "REF001",
            "origen_asignacion": "direccion",
            "clave_interna": "INT001",
            "tipo_documento": "PDF",
            "match_status": "OK_NUMERO_UNICO",
            "match_confidence": 0.95,
            "match_source": "filename",
            "match_reason": "exact",
            "texto_detectado": "250076",
            "duplicado_anio_presupuesto": "",
            "action": "copy",
            "action_status": "ok",
            "error": "",
            "verified": True,
        }

        row = sink._row_from_record(record)

        assert row[0] == "test.pdf"
        assert row[1] == ".pdf"
        assert row[2] == "application/pdf"
        assert row[-1] == 1  # verified: bool True -> int 1

    def test_verified_converts_falsy_to_zero(self, sink: SqlServerSink) -> None:
        record = {"verified": False}
        row = sink._row_from_record(record)
        # verified is the last column (index 33 in SQL_COLUMNS)
        assert row[-1] == 0

    def test_verified_converts_truthy_to_one(self, sink: SqlServerSink) -> None:
        record = {"verified": 1}
        row = sink._row_from_record(record)
        assert row[-1] == 1

    def test_missing_key_returns_none(self, sink: SqlServerSink) -> None:
        row = sink._row_from_record({})
        assert all(v is None or v == 0 for v in row)

    def test_verified_defaults_to_zero_when_missing(self, sink: SqlServerSink) -> None:
        row = sink._row_from_record({"file_name": "only.txt"})
        assert row[-1] == 0


class TestInitWithoutPyodbc:
    """Tests for the constructor when pyodbc is not available."""

    def test_raises_runtime_error_without_pyodbc(self) -> None:
        with patch.dict(sys.modules, {"pyodbc": None}):
            with patch(
                "reorganizador_v2.sinks.sqlserver_sink.pyodbc", None
            ):
                with pytest.raises(RuntimeError, match="pyodbc"):
                    SqlServerSink("dummy_connection_string")
