"""Application-wide configuration and logging helpers."""

from __future__ import annotations

import logging
import os
import sys
from enum import Enum
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

os.environ.setdefault("PYTHONIOENCODING", "utf-8")

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# -----------------------------------------------------------------------------
# .env support (opcional)
# -----------------------------------------------------------------------------

try:
    from dotenv import load_dotenv  # type: ignore

    _ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE)
    del _ENV_FILE
except ImportError:
    pass  # python-dotenv no instalado, se ignoran los archivos .env

# -----------------------------------------------------------------------------
# General constants
# -----------------------------------------------------------------------------

OS_CPU_COUNT = os.cpu_count() or 4
DEFAULT_THREADS: int = max(4, int(OS_CPU_COUNT * 0.75))
DEFAULT_PROCESSES: int = max(2, int(OS_CPU_COUNT * 0.5))
CHUNK_SIZE: int = 8 * 1024 * 1024  # 8 MiB
DEFAULT_HASH: str = "xxhash"
SUPPORTED_HASH_ALGOS = {"sha1", "sha256", "md5", "xxhash"}
HASH_CACHE_TTL_SECONDS: int = 24 * 3600


class OrganizeBy(str, Enum):
    """Estrategias de organización en destino."""

    FLAT = "flat"
    TYPE = "type"
    DATE = "date"
    TYPE_DATE = "type-date"
    HIERARCHICAL_TYPE_EXT = 'hierarchical-type-ext'
    PROJECT_TYPE = 'project-type'
    FACTUSOL_CLIENT_BUDGET = "factusol-client-budget"

    @classmethod
    def choices(cls) -> list[str]:
        return [member.value for member in cls]


# Columnas compartidas entre los sinks SQLite y SQL Server.
# Si se añade una columna nueva, solo hay que tocarla aquí.
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
    "year",
    "presupuesto_detectado",
    "cliente",
    "sede_hotel_direccion",
    "referencia",
    "origen_asignacion",
    "clave_interna",
    "tipo_documento",
    "match_status",
    "match_confidence",
    "match_source",
    "match_reason",
    "texto_detectado",
    "duplicado_anio_presupuesto",
    "action",
    "action_status",
    "error",
    "verified",
]

CSV_HEADERS = [
    "id",
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
    "year",
    "presupuesto_detectado",
    "cliente",
    "sede_hotel_direccion",
    "referencia",
    "origen_asignacion",
    "clave_interna",
    "tipo_documento",
    "match_status",
    "match_confidence",
    "match_source",
    "match_reason",
    "texto_detectado",
    "duplicado_anio_presupuesto",
    "action",
    "action_status",
    "error",
    "verified",
]

LOG_DIR = Path("logs")
LOG_FILE_MAX_BYTES = 5 * 1024 * 1024
LOG_FILE_BACKUPS = 3
LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------


def setup_logging(level: str | int = "INFO", log_dir: Optional[Path] = None) -> Path:
    """
    Configure root logging with rotating file handler and rich console handler.

    Parameters
    ----------
    level:
        Logging level, accepts numeric or string names.
    log_dir:
        Custom directory for log files. Defaults to ``logs`` in the CWD.

    Returns
    -------
    Path
        The path to the active log file.
    """
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    resolved_dir = (log_dir or LOG_DIR).resolve()
    resolved_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_dir / f"{datetime.now():%Y-%m-%d}.log"

    numeric_level = (
        logging._nameToLevel.get(str(level).upper(), logging.INFO)
        if isinstance(level, str)
        else int(level)
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=LOG_FILE_MAX_BYTES,
        backupCount=LOG_FILE_BACKUPS,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    rich_console = Console(force_terminal=True, color_system="auto", soft_wrap=True)
    console_handler = RichHandler(
        console=rich_console,
        rich_tracebacks=True,
        show_level=False,
        show_path=False,
    )
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logging.basicConfig(
        level=numeric_level,
        handlers=[file_handler, console_handler],
    )

    # Set noise-prone libraries to a higher level by default.
    logging.getLogger("watchdog.observers.inotify_buffer").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)

    logging.debug("Logging configured at level %s", logging.getLevelName(numeric_level))
    logging.debug("Log file located at %s", log_path)

    return log_path
