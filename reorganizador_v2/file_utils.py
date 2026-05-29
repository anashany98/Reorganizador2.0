"""Utility helpers for filesystem interactions and hashing."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from datetime import datetime
from . import config
from .config import OrganizeBy


@dataclass(slots=True)
class HashResult:
    algorithm: str
    value: Optional[str]
    duration_seconds: float


class HashComputationError(RuntimeError):
    """Raised when a hash cannot be computed."""


class ConflictStrategy:
    RENAME = "rename"
    OVERWRITE = "overwrite"
    SKIP = "skip"
    OVERWRITE_IF_NEWER = "overwrite-if-newer"

    @classmethod
    def choices(cls) -> list[str]:
        return [cls.RENAME, cls.OVERWRITE, cls.SKIP, cls.OVERWRITE_IF_NEWER]


_WIN32 = sys.platform == "win32"

ARCHIVO_EXTENSIONS = {"pdf", "doc", "docx", "xls", "xlsx", "txt", "csv", "xml"}
IMAGEN_EXTENSIONS = {"jpg", "jpeg", "png", "tif", "tiff", "bmp", "gif"}
CORREO_EXTENSIONS = {"msg", "eml", "pst"}


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _win_path(p: Path) -> str:
    s = str(p)
    if _WIN32 and len(s) > 1 and not s.startswith("\\\\?\\"):
        s = "\\\\?\\" + s
    return s


def _preserve_creation_time(src: Path, dest: Path) -> None:
    if not _WIN32:
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        src_stat = src.stat()
        ctime_ns = int(src_stat.st_ctime * 10_000_000) + 116444736000000000
        ct = ctypes.c_ulonglong(ctime_ns)
        handle = kernel32.CreateFileW(
            _win_path(dest), 0x40000000, 0, None, 3, 0x02000000, None
        )
        if handle and handle != -1:
            ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ct), None, None)
            kernel32.CloseHandle(handle)
    except Exception:
        pass


def safe_makedirs(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# Files to skip (system/hidden/temporary)
_SKIP_FILES_PREFIXES = ('._', '.~', '.DS_Store', 'Thumbs.db', 'desktop.ini')
_SKIP_FILES_EXACT = {'.DS_Store', 'Thumbs.db', 'desktop.ini'}


def _should_skip_file(name: str) -> bool:
    """Check if file should be skipped (system/hidden/temporary files)."""
    if name in _SKIP_FILES_EXACT:
        return True
    for prefix in _SKIP_FILES_PREFIXES:
        if name.startswith(prefix):
            return True
    return False


def _scandir_recursive(root: Path) -> Iterable[Path]:
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    yield from _scandir_recursive(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    name = entry.name
                    if _should_skip_file(name):
                        continue
                    yield Path(entry.path)
    except PermissionError:
        return


def iter_files(root: Path) -> Iterable[Path]:
    yield from _scandir_recursive(root)


def format_datetime(timestamp: float) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    except (OverflowError, ValueError):
        return ""


def guess_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def ensure_unique_path(path: Path) -> Path:
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


# ---------------------------------------------------------------------------
# Copy / Move / Hardlink
# ---------------------------------------------------------------------------


def copy_file(src: Path, dest: Path) -> None:
    safe_makedirs(dest.parent)
    shutil.copy2(_win_path(src), _win_path(dest))
    _preserve_creation_time(src, dest)


def move_file(src: Path, dest: Path) -> None:
    safe_makedirs(dest.parent)
    shutil.move(_win_path(src), _win_path(dest))


def hardlink_or_copy_file(src: Path, dest: Path) -> str:
    safe_makedirs(dest.parent)
    try:
        os.link(_win_path(src), _win_path(dest))
        return "hardlink"
    except OSError:
        shutil.copy2(_win_path(src), _win_path(dest))
        return "copy"


def should_overwrite(dest: Path, src_mtime: float, strategy: str) -> bool:
    if not dest.exists():
        return False
    if strategy == ConflictStrategy.OVERWRITE:
        return True
    if strategy == ConflictStrategy.SKIP:
        return False
    if strategy == ConflictStrategy.OVERWRITE_IF_NEWER:
        return src_mtime > dest.stat().st_mtime
    return False


# ---------------------------------------------------------------------------
# Category helpers
# ---------------------------------------------------------------------------


def categorize_file_by_extension(path: Path) -> Tuple[str, str]:
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


# ---------------------------------------------------------------------------
# Gestor / Proyecto extraction
# ---------------------------------------------------------------------------


def extract_manager_project(path: Path, source_root: Path) -> Tuple[Optional[str], Optional[str]]:
    def is_year(part: str) -> bool:
        return len(part) == 4 and part.isdigit()

    def is_project(part: str) -> bool:
        return part.replace("-", "").replace("_", "").isdigit()

    def strip_prefix(parts: Sequence[str]) -> List[str]:
        items = [s for s in parts if s]
        if items and is_year(items[0]):
            items = items[1:]
        return items

    def segments_after_marker(parts: Sequence[str]) -> List[str]:
        for idx, s in enumerate(parts):
            if s.lower() == "gestores":
                return list(parts[idx + 1:])
        return list(parts)

    def extract_from_segments(segments: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
        gestor: Optional[str] = None
        proyecto: Optional[str] = None
        for s in segments:
            if not s:
                continue
            if gestor is None and not is_project(s):
                gestor = s
                continue
            if gestor is not None and proyecto is None and is_project(s):
                proyecto = s
                continue
        if gestor is None and segments:
            gestor = segments[0]
        if proyecto is None:
            for s in segments:
                if is_project(s):
                    proyecto = s
                    break
        return gestor, proyecto

    gestor: Optional[str] = None
    proyecto: Optional[str] = None
    from_path = strip_prefix(segments_after_marker(path.parts))
    gestor_path, proyecto_path = extract_from_segments(from_path)
    gestor = gestor_path or gestor
    proyecto = proyecto_path or proyecto
    try:
        relative_parts = strip_prefix(path.relative_to(source_root).parts)
    except ValueError:
        relative_parts = []
    gestor_rel, proyecto_rel = extract_from_segments(relative_parts)
    gestor = gestor or gestor_rel
    proyecto = proyecto or proyecto_rel
    from_root = strip_prefix(segments_after_marker(source_root.parts))
    gestor_root, proyecto_root = extract_from_segments(from_root)
    gestor = gestor or gestor_root
    proyecto = proyecto or proyecto_root
    return gestor, proyecto


# ---------------------------------------------------------------------------
# Destination path builder
# ---------------------------------------------------------------------------


def build_destination_path(
    src: Path, dest_root: Path, source_root: Path,
    mode: str | OrganizeBy, modified_time: float,
) -> Path:
    if isinstance(mode, OrganizeBy):
        mode = mode.value
    relative = src.relative_to(source_root)
    base_parent = dest_root / relative.parent
    extension = (src.suffix[1:] or "noext").lower()

    if mode == OrganizeBy.DATE:
        dt = datetime.fromtimestamp(modified_time)
        dest = dest_root / str(dt.year) / f"{dt.month:02d}" / src.name
    elif mode == OrganizeBy.TYPE_DATE:
        dt = datetime.fromtimestamp(modified_time)
        dest = dest_root / extension / str(dt.year) / f"{dt.month:02d}" / src.name
    elif mode == OrganizeBy.TYPE:
        dest = dest_root / extension / src.name
    elif mode == OrganizeBy.HIERARCHICAL_TYPE_EXT:
        category, ext_label = categorize_file_by_extension(src)
        dest = base_parent / category / ext_label / src.name
    elif mode == OrganizeBy.PROJECT_TYPE:
        parts = list(relative.parts)
        clean = []
        skip_gestor = False
        for p in parts:
            if skip_gestor:
                skip_gestor = False
                continue
            clean.append(p)
            if len(p) == 4 and p.isdigit():
                skip_gestor = True
        category, ext_label = categorize_file_by_extension(src)
        dest = dest_root / Path(*clean).parent / category / ext_label / src.name
    else:
        dest = dest_root / relative

    return dest


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------


def compute_hash(path: Path, algorithm: str = config.DEFAULT_HASH) -> HashResult:
    start = time.perf_counter()
    algo = algorithm.lower()
    if algo not in config.SUPPORTED_HASH_ALGOS and algo != "none":
        raise HashComputationError(f"Hash algorithm '{algorithm}' is not supported.")
    if algo == "none":
        return HashResult(algorithm=algorithm, value=None, duration_seconds=0.0)
    try:
        if algo == "xxhash":
            import xxhash
            hasher = xxhash.xxh64()
        else:
            hasher = hashlib.new(algo)
        with path.open("rb") as stream:
            while chunk := stream.read(config.CHUNK_SIZE):
                hasher.update(chunk)
    except (OSError, ImportError) as exc:
        raise HashComputationError(f"Unable to hash {path} with {algo}: {exc}") from exc
    return HashResult(algorithm=algo, value=hasher.hexdigest(),
                      duration_seconds=time.perf_counter() - start)


def file_signature(path: Path) -> Tuple[int, float]:
    stats = path.stat()
    return stats.st_size, stats.st_mtime


def verify_hash_match(src_hash: str | None, dest_path: Path, algorithm: str) -> bool:
    if not src_hash:
        return False
    dest_result = compute_hash(dest_path, algorithm)
    return dest_result.value == src_hash


# ---------------------------------------------------------------------------
# Project filter
# ---------------------------------------------------------------------------


def parse_project_filter(raw: str | None) -> set[str] | None:
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    source: list[str] = []
    candidate = Path(raw)
    if candidate.exists() and candidate.is_file():
        text = candidate.read_text(encoding="utf-8", errors="replace")
        source = [line.strip() for line in text.replace(",", "\n").splitlines() if line.strip()]
    else:
        source = [item.strip() for item in raw.split(",") if item.strip()]
    if not source:
        return None
    result: set[str] = set()
    for item in source:
        n = item.replace("-", "").replace("_", "").replace(" ", "")
        if n:
            result.add(n)
    return result if result else None


def path_matches_project_filter(path: Path, source_root: Path, projects: set[str]) -> bool:
    _, proyecto = extract_manager_project(path, source_root)
    if not proyecto:
        return False
    return proyecto.replace("-", "").replace("_", "").replace(" ", "") in projects
