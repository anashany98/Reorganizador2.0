"""Tests for FactuSOL mapping, detection and destination paths."""

from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import Workbook

from reorganizador_v2.factusol_mapping import (
    build_factusol_client_budget_destination_path,
    categorize_document_type,
    compact_text,
    detect_budget_candidates,
    load_mapping,
    resolve_budget_match,
    sanitize_path_part,
)


HEADERS = [
    "Anio",
    "Presupuesto",
    "Cliente",
    "Sede_Hotel_Direccion",
    "Referencia",
    "OrigenAsignacion",
    "ClaveInterna",
    "DuplicadoAnioPresupuesto",
]


def _write_mapping(path: Path, rows: list[dict[str, str]], headers: list[str] | None = None) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "Mapping_FactuSOL"
    selected_headers = headers or HEADERS
    ws.append(selected_headers)
    for row in rows:
        ws.append([row.get(header, "") for header in selected_headers])
    wb.save(path)
    return path


def _base_rows() -> list[dict[str, str]]:
    return [
        {
            "Anio": "2025",
            "Presupuesto": "250076",
            "Cliente": "MELIA HOTELS INTERNATIONAL S.A",
            "Sede_Hotel_Direccion": "GRAN MELIA VICTORIA",
            "Referencia": "GRAN MELIA VICTORIA CORTINAS HABITACIONES",
            "OrigenAsignacion": "direccion_entrega",
            "ClaveInterna": "2025-250076-00123",
            "DuplicadoAnioPresupuesto": "NO",
        },
        {
            "Anio": "2025",
            "Presupuesto": "250077",
            "Cliente": "HOTEL PORT DE SOLLER",
            "Sede_Hotel_Direccion": "BIKINI ISLAND",
            "Referencia": "BIKINI ISLAND CORTINAS BALCONES",
            "OrigenAsignacion": "direccion_entrega",
            "ClaveInterna": "2025-250077-00124",
            "DuplicadoAnioPresupuesto": "NO",
        },
        {
            "Anio": "2025",
            "Presupuesto": "250120",
            "Cliente": "HOTEL CAN PERE S.L",
            "Sede_Hotel_Direccion": "",
            "Referencia": "SIN DIRECCION",
            "OrigenAsignacion": "nombre_comercial",
            "ClaveInterna": "2025-250120-00125",
            "DuplicadoAnioPresupuesto": "NO",
        },
        {
            "Anio": "2025",
            "Presupuesto": "250001",
            "Cliente": "MELIA HOTELS INTERNATIONAL S.A",
            "Sede_Hotel_Direccion": "GRAN MELIA VICTORIA",
            "Referencia": "CORTINAS HABITACIONES GRAN MELIA VICTORIA",
            "OrigenAsignacion": "direccion_entrega",
            "ClaveInterna": "2025-250001-00126",
            "DuplicadoAnioPresupuesto": "SI",
        },
        {
            "Anio": "2025",
            "Presupuesto": "250001",
            "Cliente": "OTRO CLIENTE S.A",
            "Sede_Hotel_Direccion": "OTRO HOTEL",
            "Referencia": "MAMPARAS OTRO HOTEL",
            "OrigenAsignacion": "direccion_entrega",
            "ClaveInterna": "2025-250001-00127",
            "DuplicadoAnioPresupuesto": "SI",
        },
    ]


@pytest.fixture()
def mapping_index(tmp_path: Path):
    return load_mapping(_write_mapping(tmp_path / "factusol.xlsx", _base_rows()))


def test_load_mapping_reads_mapping_sheet_and_builds_year_budget_index(tmp_path: Path) -> None:
    excel_path = _write_mapping(tmp_path / "factusol.xlsx", _base_rows())

    index = load_mapping(excel_path)

    assert ("2025", "250076") in index.mapping_by_year_budget
    assert index.mapping_by_year_budget[("2025", "250076")][0].cliente == "MELIA HOTELS INTERNATIONAL S.A"
    assert len(index.mapping_by_year_budget[("2025", "250001")]) == 2


def test_load_mapping_validates_required_columns(tmp_path: Path) -> None:
    excel_path = _write_mapping(
        tmp_path / "factusol.xlsx",
        _base_rows(),
        headers=[header for header in HEADERS if header != "Cliente"],
    )

    with pytest.raises(ValueError, match="Cliente"):
        load_mapping(excel_path)


def test_load_mapping_uses_cliente_when_sede_is_empty(tmp_path: Path) -> None:
    index = load_mapping(_write_mapping(tmp_path / "factusol.xlsx", _base_rows()))

    record = index.mapping_by_year_budget[("2025", "250120")][0]

    assert record.sede_hotel_direccion == "HOTEL CAN PERE S.L"


def test_windows_folder_name_sanitization() -> None:
    assert sanitize_path_part("MELIA HOTELS / PALMA:*") == "MELIA HOTELS PALMA"
    assert sanitize_path_part(" \t\r\n. ") == "_SIN_NOMBRE"
    assert len(sanitize_path_part("A" * 200)) == 80


def test_compact_text_removes_accents_and_separators() -> None:
    assert compact_text("P-250076 Meliá") == "P250076MELIA"


def test_detect_budget_exact(mapping_index) -> None:
    candidates = detect_budget_candidates(Path("C:/Gestores/2025/MAR/250076.pdf"), mapping_index)

    assert candidates[0].detected_budget == "250076"
    assert candidates[0].reason == "OK_EXACTO"


@pytest.mark.parametrize("name", ["P250076.pdf", "A250076.docx", "250076A.xlsx"])
def test_detect_budget_with_letters(mapping_index, name: str) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR") / name, mapping_index)

    assert match.status == "OK_COMPACTO"
    assert match.presupuesto_detectado == "250076"


@pytest.mark.parametrize("name", ["P-250076.pdf", "MELIA.250076.pdf", "250076_MELIA.pdf"])
def test_detect_budget_with_separators(mapping_index, name: str) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR") / name, mapping_index)

    assert match.status in {"OK_NORMALIZADO", "OK_COMPACTO"}
    assert match.presupuesto_detectado == "250076"


@pytest.mark.parametrize("name", ["25-0076.pdf", "25.0076.pdf", "25_0076.pdf"])
def test_detect_budget_recomposed(mapping_index, name: str) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR") / name, mapping_index)

    assert match.status == "OK_RECOMPUESTO"
    assert match.presupuesto_detectado == "250076"


def test_resolve_without_budget_number(mapping_index) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR/MELIA FOTOS.zip"), mapping_index)

    assert match.status == "SIN_NUMERO_PRESUPUESTO"
    assert match.record is None


def test_resolve_budget_not_found_in_excel(mapping_index) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR/250999.pdf"), mapping_index)

    assert match.status == "NO_ENCONTRADO_EN_EXCEL"
    assert match.presupuesto_detectado == "250999"


def test_resolve_multiple_budget_numbers_is_ambiguous(mapping_index) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR/250076 250077.pdf"), mapping_index)

    assert match.status == "AMBIGUO"
    assert match.record is None


def test_resolve_duplicate_budget_with_clear_sede_and_reference(mapping_index) -> None:
    match = resolve_budget_match(
        Path("C:/Gestores/2025/MAR/GRAN MELIA VICTORIA cortinas/PRE-250001.pdf"),
        mapping_index,
    )

    assert match.status == "OK_COMPACTO"
    assert match.record is not None
    assert match.record.sede_hotel_direccion == "GRAN MELIA VICTORIA"


def test_resolve_duplicate_budget_without_context_is_ambiguous(mapping_index) -> None:
    match = resolve_budget_match(Path("C:/Gestores/2025/MAR/250001.pdf"), mapping_index)

    assert match.status == "AMBIGUO"
    assert match.record is None


def test_build_destination_path_for_ok_match(mapping_index, tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    src = source_root / "2025" / "MAR" / "250076" / "presupuesto.pdf"
    match = resolve_budget_match(src, mapping_index, allowed_years={"2025"})

    dest = build_factusol_client_budget_destination_path(
        src=src,
        dest_root=tmp_path / "dest",
        source_root=source_root,
        match_result=match,
    )

    assert dest == (
        tmp_path
        / "dest"
        / "2025"
        / "MELIA HOTELS INTERNATIONAL S.A"
        / "GRAN MELIA VICTORIA"
        / "Presupuesto 250076"
        / "PDF"
        / "presupuesto.pdf"
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a.pdf", "PDF"),
        ("a.xls", "EXCEL"),
        ("a.xlsx", "EXCEL"),
        ("a.xlsm", "EXCEL"),
        ("a.csv", "EXCEL"),
        ("a.jpg", "IMAGENES"),
        ("a.jpeg", "IMAGENES"),
        ("a.png", "IMAGENES"),
        ("a.tif", "IMAGENES"),
        ("a.tiff", "IMAGENES"),
        ("a.webp", "IMAGENES"),
        ("a.heic", "IMAGENES"),
        ("a.msg", "CORREOS"),
        ("a.eml", "CORREOS"),
        ("a.pst", "CORREOS"),
        ("a.ost", "CORREOS"),
        ("a.dwg", "PLANOS"),
        ("a.dxf", "PLANOS"),
        ("a.skp", "PLANOS"),
        ("a.rvt", "PLANOS"),
        ("a.ifc", "PLANOS"),
        ("a.pln", "PLANOS"),
        ("a.3dm", "PLANOS"),
        ("a.doc", "WORD"),
        ("a.docx", "WORD"),
        ("a.zip", "ZIP"),
        ("a.rar", "ZIP"),
        ("a.7z", "ZIP"),
        ("a.bin", "OTROS"),
    ],
)
def test_categorize_document_types(name: str, expected: str) -> None:
    assert categorize_document_type(Path(name)) == expected
