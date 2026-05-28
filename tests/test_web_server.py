"""Tests for the local FastAPI server wiring."""

from __future__ import annotations

import asyncio
from pathlib import Path

from openpyxl import Workbook

from web.server import app
from web.server import browse
from web.server import preview_source


def test_preview_endpoint_is_registered_once() -> None:
    preview_routes = [
        route
        for route in app.routes
        if getattr(route, "path", None) == "/api/preview"
        and "GET" in getattr(route, "methods", set())
    ]

    assert len(preview_routes) == 1


def test_preview_project_filter_limits_all_reported_totals(tmp_path: Path) -> None:
    source = tmp_path / "Gestores"
    matching = source / "2025" / "MAR" / "250076" / "a.txt"
    other = source / "2025" / "ANA" / "250077" / "b.pdf"
    matching.parent.mkdir(parents=True)
    other.parent.mkdir(parents=True)
    matching.write_text("alpha", encoding="utf-8")
    other.write_text("beta-beta", encoding="utf-8")

    result = asyncio.run(preview_source(source=str(source), projects="250076"))

    assert result["total_files"] == 1
    assert result["pending"] == 1
    assert result["total_size_bytes"] == len("alpha")
    assert result["extensions"] == {"txt": 1}
    assert result["gestores"] == {"MAR": 1}
    assert result["proyectos"] == {"250076": 1}


def test_preview_factusol_returns_rows_and_match_counters(tmp_path: Path) -> None:
    source = tmp_path / "Gestores"
    dest = tmp_path / "Organizados"
    file_path = source / "2025" / "MAR" / "P-250076.pdf"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("alpha", encoding="utf-8")

    mapping_excel = tmp_path / "factusol.xlsx"
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
            "CORTINAS HABITACIONES",
            "direccion_entrega",
            "2025-250076-00123",
            "NO",
        ]
    )
    wb.save(mapping_excel)

    result = asyncio.run(
        preview_source(
            source=str(source),
            projects="",
            mapping_excel=str(mapping_excel),
            years="2025",
            dest=str(dest),
            unmatched_dir="_REVISION",
        )
    )

    assert result["match_counters"]["OK"] == 1
    assert result["match_counters"]["SIN_NUMERO_PRESUPUESTO"] == 0
    assert result["items"][0]["presupuesto_detectado"] == "250076"
    assert result["items"][0]["cliente"] == "MELIA HOTELS INTERNATIONAL S.A"
    assert result["items"][0]["sede_hotel_direccion"] == "GRAN MELIA VICTORIA"
    assert result["items"][0]["tipo_documento"] == "PDF"
    assert "Presupuesto 250076" in result["items"][0]["dst_path"]


def test_browse_can_include_excel_files_for_mapping_picker(tmp_path: Path) -> None:
    folder = tmp_path / "inputs"
    folder.mkdir()
    (folder / "factusol_mapping_app_simplificado.xlsx").write_text("fake", encoding="utf-8")
    (folder / "notas.txt").write_text("ignore", encoding="utf-8")
    (folder / "subdir").mkdir()

    result = asyncio.run(
        browse(
            path=str(folder),
            include_files=True,
            file_extensions=".xlsx,.xls,.xlsm",
        )
    )

    items = {(item["name"], item["type"]) for item in result["items"]}
    assert ("subdir", "dir") in items
    assert ("factusol_mapping_app_simplificado.xlsx", "file") in items
    assert ("notas.txt", "file") not in items


def test_preview_accepts_multiple_source_folders(tmp_path: Path) -> None:
    source_a = tmp_path / "Gestores" / "2025"
    source_b = tmp_path / "Gestores" / "2026"
    file_a = source_a / "MAR" / "250076" / "a.pdf"
    file_b = source_b / "ANA" / "260001" / "b.xlsx"
    file_a.parent.mkdir(parents=True)
    file_b.parent.mkdir(parents=True)
    file_a.write_text("alpha", encoding="utf-8")
    file_b.write_text("beta", encoding="utf-8")

    result = asyncio.run(
        preview_source(
            source=str(source_a),
            sources=str(source_b),
        )
    )

    assert result["total_files"] == 2
    assert result["extensions"] == {"pdf": 1, "xlsx": 1}


def test_preview_accepts_sources_without_primary_source(tmp_path: Path) -> None:
    source_a = tmp_path / "Gestores" / "2025"
    source_b = tmp_path / "Gestores" / "2026"
    file_a = source_a / "MAR" / "250076" / "a.pdf"
    file_b = source_b / "ANA" / "260001" / "b.xlsx"
    file_a.parent.mkdir(parents=True)
    file_b.parent.mkdir(parents=True)
    file_a.write_text("alpha", encoding="utf-8")
    file_b.write_text("beta", encoding="utf-8")

    result = asyncio.run(
        preview_source(
            source="",
            sources=f"{source_a};{source_b}",
        )
    )

    assert result["total_files"] == 2
    assert result["extensions"] == {"pdf": 1, "xlsx": 1}
