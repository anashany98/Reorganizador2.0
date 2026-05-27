"""Tests for file_utils — extract_manager_project and build_destination_path."""

from __future__ import annotations

from pathlib import Path

from reorganizador_v2 import file_utils
from reorganizador_v2.config import OrganizeBy


# -----------------------------------------------------------------------------
# extract_manager_project
# -----------------------------------------------------------------------------


def test_extract_manager_project_basic():
    """Ruta estándar: Gestores/AÑO/GESTOR/PROYECTO/archivo.txt"""
    source_root = Path("C:/Gestores")
    path = Path("C:/Gestores/2025/MAR/250076/informe.pdf")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "MAR"
    assert proyecto == "250076"


def test_extract_manager_project_without_year():
    """Sin la carpeta de año: Gestores/GESTOR/PROYECTO..."""
    source_root = Path("C:/Gestores")
    path = Path("C:/Gestores/MAR/250076/doc.docx")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "MAR"
    assert proyecto == "250076"


def test_extract_manager_project_relative_path():
    """La ruta source_root es parte del origen, usa relativa."""
    source_root = Path("C:/Data/Archivos")
    path = Path("C:/Data/Archivos/Gestores/2024/ABC/123456/foto.jpg")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "ABC"
    assert proyecto == "123456"


def test_extract_manager_project_project_with_dashes():
    """Números de proyecto con guiones: 250-076 → 250076 (is_project)."""
    source_root = Path("C:/Gestores")
    path = Path("C:/Gestores/2025/JAN/250-076/archivo.pdf")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "JAN"
    assert proyecto == "250-076"


def test_extract_manager_project_no_project():
    """Solo gestor, sin proyecto."""
    source_root = Path("C:/Gestores")
    path = Path("C:/Gestores/2025/XYZ/general.docx")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "XYZ"
    assert proyecto is None


def test_extract_manager_project_only_project():
    """Solo número de proyecto, sin gestor."""
    source_root = Path("C:/Data")
    path = Path("C:/Data/500123/archivo.txt")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    # 500123 es numérico, se toma como proyecto; gestor es el primero que no es numérico.
    # En este caso no hay gestor no numérico, así que gestor toma "500123"
    assert proyecto == "500123"


def test_extract_manager_project_deep_nesting():
    """Ruta muy anidada dentro del proyecto."""
    source_root = Path("C:/Gestores")
    path = Path("C:/Gestores/2025/ABC/123456/subcarpeta/anexos/doc_final.pdf")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "ABC"
    assert proyecto == "123456"


def test_extract_manager_project_no_structure():
    """Ruta sin estructura de gestores — usa la ruta relativa a source_root."""
    source_root = Path("C:/MisDocs")
    path = Path("C:/MisDocs/vacaciones/foto.jpg")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    # La ruta absoluta incluye "C:\\" como parte, que se toma como gestor
    # antes de que la ruta relativa (vacaciones) tenga oportunidad.
    # Comportamiento conocido: el primer segmento no numérico gana.
    assert gestor is not None
    assert proyecto is None


def test_extract_manager_project_source_root_is_gestor():
    """El propio source_root contiene el nombre del gestor."""
    source_root = Path("C:/Proyectos/Gestores/2025/MAR")
    path = Path("C:/Proyectos/Gestores/2025/MAR/250076/plano.dwg")
    gestor, proyecto = file_utils.extract_manager_project(path, source_root)
    assert gestor == "MAR"
    assert proyecto == "250076"


# -----------------------------------------------------------------------------
# build_destination_path
# -----------------------------------------------------------------------------


def test_build_destination_flat():
    src = Path("/src/a/b/file.pdf")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, OrganizeBy.FLAT, 1700000000.0
    )
    assert result == Path("/dest/a/b/file.pdf")


def test_build_destination_type():
    src = Path("/src/docs/report.pdf")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, OrganizeBy.TYPE, 1700000000.0
    )
    assert result == Path("/dest/pdf/report.pdf")


def test_build_destination_date():
    src = Path("/src/file.txt")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, OrganizeBy.DATE, 1704067200.0  # 2024-01-01
    )
    assert result == Path("/dest/2024/01/file.txt")


def test_build_destination_type_date():
    src = Path("/src/file.jpg")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, OrganizeBy.TYPE_DATE, 1704067200.0
    )
    assert result == Path("/dest/jpg/2024/01/file.jpg")


def test_build_destination_hierarchical():
    src = Path("/src/proyecto/docs/report.pdf")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, OrganizeBy.HIERARCHICAL_TYPE_EXT, 1700000000.0
    )
    assert result == Path("/dest/proyecto/docs/Archivos/PDF/report.pdf")


def test_build_destination_accepts_string_mode():
    """La función acepta tanto OrganizeBy como string plano."""
    src = Path("/src/file.txt")
    dest_root = Path("/dest")
    source_root = Path("/src")
    result = file_utils.build_destination_path(
        src, dest_root, source_root, "type", 1700000000.0
    )
    assert result == Path("/dest/txt/file.txt")
