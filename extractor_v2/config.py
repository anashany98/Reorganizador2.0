"""Application-wide configuration and logging helpers."""

from __future__ import annotations

import logging
import os
import sys
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
# General constants
# -----------------------------------------------------------------------------

OS_CPU_COUNT = os.cpu_count() or 4
DEFAULT_THREADS: int = min(8, OS_CPU_COUNT)
DEFAULT_PROCESSES: int = max(1, min(4, OS_CPU_COUNT))
CHUNK_SIZE: int = 1024 * 1024  # 1 MiB
DEFAULT_HASH: str = "sha256"
SUPPORTED_HASH_ALGOS = {"sha1", "sha256", "md5"}
HASH_CACHE_TTL_SECONDS: int = 24 * 3600

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
