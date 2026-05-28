"""Regression tests for the scan workflow."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from openpyxl import Workbook

from reorganizador_v2 import main


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _run_scan(
    tmp_path: Path,
    source: Path,
    dest: Path,
    *extra_args: str,
    batch_size: int = 500,
) -> Path:
    db_path = tmp_path / "metadatos.db"
    csv_path = tmp_path / "metadatos.csv"
    main.main(
        [
            "--log-level",
            "ERROR",
            "scan",
            "--source",
            str(source),
            "--dest",
            str(dest),
            "--organize-by",
            "flat",
            "--hash-algo",
            "sha256",
            "--threads",
            "1",
            "--processes",
            "0",
            "--batch-size",
            str(batch_size),
            "--sqlite-db",
            str(db_path),
            "--csv-out",
            str(csv_path),
            *extra_args,
        ]
    )
    return db_path


def _fetch_rows(db_path: Path) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return list(conn.execute("SELECT * FROM files ORDER BY file_name"))


def _write_factusol_mapping(path: Path) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "Mapping_FactuSOL"
    ws.append(
        [
            "Anio",
            "Presupuesto",
            "Cliente",
            "Sede_Hotel_Direccion",
            "Referencia",
            "OrigenAsignacion",
            "ClaveInterna",
            "DuplicadoAnioPresupuesto",
        ]
    )
    ws.append(
        [
            "2025",
            "250076",
            "MELIA HOTELS INTERNATIONAL S.A",
            "GRAN MELIA VICTORIA",
            "GRAN MELIA VICTORIA CORTINAS HABITACIONES",
            "direccion_entrega",
            "2025-250076-00123",
            "NO",
        ]
    )
    wb.save(path)
    return path


def test_scan_flushes_final_partial_batch_to_sinks(tmp_path: Path) -> None:
    source = tmp_path / "src"
    dest = tmp_path / "dest"
    _write_text(source / "a.txt", "alpha")
    _write_text(source / "b.txt", "beta")

    db_path = _run_scan(tmp_path, source, dest, batch_size=500)

    rows = _fetch_rows(db_path)
    assert [row["file_name"] for row in rows] == ["a.txt", "b.txt"]
    assert (tmp_path / "metadatos.csv").read_text(encoding="utf-8").count("\n") == 3


def test_scan_is_idempotent_when_destination_already_exists(tmp_path: Path) -> None:
    source = tmp_path / "src"
    dest = tmp_path / "dest"
    _write_text(source / "a.txt", "alpha")
    _write_text(source / "b.txt", "beta")

    db_path = _run_scan(tmp_path, source, dest, batch_size=1)
    _run_scan(tmp_path, source, dest, batch_size=1)

    copied_names = sorted(path.name for path in dest.rglob("*") if path.is_file())
    rows = _fetch_rows(db_path)
    assert copied_names == ["a.txt", "b.txt"]
    assert [row["action"] for row in rows] == ["skip", "skip"]


def test_scan_records_verified_destination_hashes(tmp_path: Path) -> None:
    source = tmp_path / "src"
    dest = tmp_path / "dest"
    _write_text(source / "a.txt", "alpha")

    db_path = _run_scan(tmp_path, source, dest, batch_size=1)

    [row] = _fetch_rows(db_path)
    assert row["hash_value"]
    assert row["hash_value_dst"] == row["hash_value"]
    assert row["hash_verified"] == "ok"
    assert row["verified"] == 1


def test_scan_dedup_does_not_error_on_duplicate_content(tmp_path: Path) -> None:
    source = tmp_path / "src"
    dest = tmp_path / "dest"
    _write_text(source / "a.txt", "same")
    _write_text(source / "b.txt", "same")

    db_path = _run_scan(tmp_path, source, dest, "--dedup", batch_size=1)

    rows = _fetch_rows(db_path)
    assert [row["action_status"] for row in rows] == ["ok", "ok"]
    assert sorted(path.name for path in dest.rglob("*") if path.is_file()) == [
        "a.txt",
        "b.txt",
    ]


def test_factusol_dry_run_records_destination_without_copying(tmp_path: Path) -> None:
    source = tmp_path / "src"
    dest = tmp_path / "dest"
    mapping_excel = _write_factusol_mapping(tmp_path / "factusol.xlsx")
    _write_text(source / "Gestores" / "2025" / "MAR" / "P-250076.pdf", "alpha")
    db_path = tmp_path / "factusol.db"
    csv_path = tmp_path / "factusol.csv"

    main.main(
        [
            "--log-level",
            "ERROR",
            "scan",
            "--source",
            str(source),
            "--dest",
            str(dest),
            "--organize-by",
            "factusol-client-budget",
            "--mapping-excel",
            str(mapping_excel),
            "--years",
            "2025",
            "--hash-algo",
            "sha256",
            "--threads",
            "1",
            "--processes",
            "0",
            "--batch-size",
            "1",
            "--sqlite-db",
            str(db_path),
            "--csv-out",
            str(csv_path),
            "--dry-run",
        ]
    )

    [row] = _fetch_rows(db_path)
    assert not any(path.is_file() for path in dest.rglob("*"))
    assert row["year"] == "2025"
    assert row["presupuesto_detectado"] == "250076"
    assert row["cliente"] == "MELIA HOTELS INTERNATIONAL S.A"
    assert row["sede_hotel_direccion"] == "GRAN MELIA VICTORIA"
    assert row["tipo_documento"] == "PDF"
    assert row["match_status"] in {"OK_NORMALIZADO", "OK_COMPACTO"}
    assert "Presupuesto 250076" in row["dst_path"]
    assert row["hash_verified"] == "dry-run"
