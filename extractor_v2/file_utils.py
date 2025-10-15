"""Utility helpers for filesystem interactions and hashing."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from . import config


@dataclass(slots=True)
class HashResult:
    """Represents a hashing outcome."""

    algorithm: str
    value: Optional[str]
    duration_seconds: float


class HashComputationError(RuntimeError):
    """Raised when a hash cannot be computed."""


# Categorias de extension usadas por el modo de organizacion jerarquica.
ARCHIVO_EXTENSIONS = {
    "pdf",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "txt",
    "csv",
    "xml",
}
IMAGEN_EXTENSIONS = {
    "jpg",
    "jpeg",
    "png",
    "tif",
    "tiff",
    "bmp",
    "gif",
}
CORREO_EXTENSIONS = {
    "msg",
    "eml",
    "pst",
}


def safe_makedirs(path: Path) -> None:
    """Create a directory and parents when absent."""
    path.mkdir(parents=True, exist_ok=True)


def iter_files(root: Path) -> Iterable[Path]:
    """Yield files recursively from a root path."""
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            yield Path(dirpath, filename)


def format_datetime(timestamp: float) -> str:
    """Format numeric timestamp into ISO-like string."""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    except (OverflowError, ValueError):
        return ""


def guess_mime(path: Path) -> str:
    """Infer MIME type from filename, fallback to octet-stream."""
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def ensure_unique_path(path: Path) -> Path:
    """Generate a unique sibling path by suffixing a counter."""
    if not path.exists():
        return path

    parent = path.parent
    stem = path.stem
    suffix = path.suffix
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def categorize_file_by_extension(path: Path) -> Tuple[str, str]:
    """Return the logical category and extension folder for *path*.

    The category is one of ``Archivos``, ``Imagenes``, ``Correos`` or ``Otros``
    and the extension label is the file suffix in uppercase (``NOEXT`` when the
    file has no suffix). This information is used by the
    ``hierarchical-type-ext`` organization strategy to append category/extension
    folders without flattening the original hierarchy.
    """

    extension = path.suffix.lower().lstrip(".")
    if not extension:
        return "Otros", "NOEXT"

    if extension in ARCHIVO_EXTENSIONS:
        category = "Archivos"
    elif extension in IMAGEN_EXTENSIONS:
        category = "Imagenes"
    elif extension in CORREO_EXTENSIONS:
        category = "Correos"
    else:
        category = "Otros"

    return category, extension.upper()


def extract_manager_project(path: Path, source_root: Path) -> Tuple[Optional[str], Optional[str]]:
    """Infer gestor and proyecto identifiers from *path* relative to *source_root*."""

    def is_year(part: str) -> bool:
        return len(part) == 4 and part.isdigit()

    def is_project(part: str) -> bool:
        normalized = part.replace("-", "").replace("_", "")
        return normalized.isdigit()

    def strip_prefix(parts: Sequence[str]) -> List[str]:
        items = [segment for segment in parts if segment]
        if items and is_year(items[0]):
            items = items[1:]
        return items

    def segments_after_marker(parts: Sequence[str]) -> List[str]:
        for idx, segment in enumerate(parts):
            if segment.lower() == "gestores":
                return list(parts[idx + 1 :])
        return list(parts)

    def extract_from_segments(segments: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
        gestor: Optional[str] = None
        proyecto: Optional[str] = None
        for segment in segments:
            if not segment:
                continue
            if gestor is None and not is_project(segment):
                gestor = segment
                continue
            if gestor is not None and proyecto is None and is_project(segment):
                proyecto = segment
                continue
        if gestor is None and segments:
            gestor = segments[0]
        if proyecto is None:
            for segment in segments:
                if is_project(segment):
                    proyecto = segment
                    break
        return gestor, proyecto

    gestor: Optional[str] = None
    proyecto: Optional[str] = None

    # 1) Primero intenta con la ruta absoluta (util cuando se ejecuta fuera de source_root).
    from_path = strip_prefix(segments_after_marker(path.parts))
    gestor_path, proyecto_path = extract_from_segments(from_path)
    gestor = gestor_path or gestor
    proyecto = proyecto_path or proyecto

    try:
        # 2) Luego intenta con la ruta relativa al source_root configurado.
        relative_parts = strip_prefix(path.relative_to(source_root).parts)
    except ValueError:
        relative_parts = []
    gestor_rel, proyecto_rel = extract_from_segments(relative_parts)
    gestor = gestor or gestor_rel
    proyecto = proyecto or proyecto_rel

    # 3) Por ultimo usa la informacion que pueda traer la ruta del propio source_root.
    from_root = strip_prefix(segments_after_marker(source_root.parts))
    gestor_root, proyecto_root = extract_from_segments(from_root)
    gestor = gestor or gestor_root
    proyecto = proyecto or proyecto_root

    return gestor, proyecto


def build_destination_path(
    src: Path,
    dest_root: Path,
    source_root: Path,
    mode: str,
    modified_time: float,
) -> Path:
    """Compose destination path respecting the selected organization mode.

    Parameters
    ----------
    src:
        Source file.
    dest_root:
        Destination root folder.
    source_root:
        Original traversal root, used to compute relative path.
    mode:
        Strategy: ``type``, ``date``, ``type-date``, ``flat`` or
        ``hierarchical-type-ext``.
    modified_time:
        mtime in seconds; used when grouping by date.
    """

    relative = src.relative_to(source_root)
    # Mantiene la jerarquia original antes de sumar carpetas de organizacion.
    base_parent = dest_root / relative.parent
    extension = (src.suffix[1:] or "noext").lower()
    if mode == "type":
        dest = base_parent / extension / src.name
    elif mode == "date":
        dest = base_parent / src.name
    elif mode == "type-date":
        dest = base_parent / extension / src.name
    elif mode == "hierarchical-type-ext":
        # Mantiene toda la jerarquia original y agrega la categoria y la extension.
        category, ext_label = categorize_file_by_extension(src)
        dest = base_parent / category / ext_label / src.name
    else:
        dest = dest_root / relative

    safe_makedirs(dest.parent)
    return dest


def compute_hash(path: Path, algorithm: str = config.DEFAULT_HASH) -> HashResult:
    """Compute a hash for the provided file."""
    start = time.perf_counter()
    algo = algorithm.lower()
    if algo not in config.SUPPORTED_HASH_ALGOS and algo != "none":
        raise HashComputationError(f"Hash algorithm '{algorithm}' is not supported.")

    if algo == "none":
        return HashResult(algorithm=algorithm, value=None, duration_seconds=0.0)

    hasher = hashlib.new(algo)
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(config.CHUNK_SIZE):
                hasher.update(chunk)
    except OSError as exc:
        raise HashComputationError(f"Unable to hash {path}: {exc}") from exc

    return HashResult(
        algorithm=algo,
        value=hasher.hexdigest(),
        duration_seconds=time.perf_counter() - start,
    )


def copy_file(src: Path, dest: Path) -> None:
    """Copy a file preserving metadata."""
    safe_makedirs(dest.parent)
    shutil.copy2(src, dest)


def move_file(src: Path, dest: Path) -> None:
    """Move a file preserving metadata."""
    safe_makedirs(dest.parent)
    shutil.move(src, dest)


def file_signature(path: Path) -> Tuple[int, float]:
    """Return a lightweight signature (size, mtime) to detect changes."""
    stats = path.stat()
    return stats.st_size, stats.st_mtime


def verify_hash_match(src_hash: str | None, dest_path: Path, algorithm: str) -> bool:
    """Re-hash destination file and compare against a known value."""
    if not src_hash:
        return False
    dest_result = compute_hash(dest_path, algorithm)
    return dest_result.value == src_hash
