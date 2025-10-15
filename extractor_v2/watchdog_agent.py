"""Watchdog agent for continuous monitoring."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from .processor import FileProcessor, SinkManager

LOGGER = logging.getLogger(__name__)


class NuevoArchivoHandler(FileSystemEventHandler):
    """Watchdog handler that triggers processing on new or updated files."""

    def __init__(self, processor: FileProcessor, sink_manager: SinkManager) -> None:
        super().__init__()
        self.processor = processor
        self.sink_manager = sink_manager
        self._lock = threading.Lock()

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle_event(event, reason="created")

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle_event(event, reason="modified")

    def _handle_event(self, event: FileSystemEvent, reason: str) -> None:
        if event.is_directory:
            return
        path = Path(event.src_path)
        if not path.exists():
            return
        try:
            with self._lock:
                record = self.processor.process_path(path)
                self.sink_manager.append(record)
                self.sink_manager.flush()

            suffix = path.suffix.lower()
            if suffix in {".tif", ".tiff"}:
                LOGGER.info("Nuevo TIFF detectado (%s): %s", reason, path)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Fallo procesando %s: %s", path, exc)


class WatchdogAgent:
    """Encapsulates watchdog observer lifecycle."""

    def __init__(self, processor: FileProcessor, sink_manager: SinkManager) -> None:
        self.processor = processor
        self.sink_manager = sink_manager
        self._observer = Observer()

    def start(self, source: Path, recursive: bool = True) -> None:
        handler = NuevoArchivoHandler(self.processor, self.sink_manager)
        self._observer.schedule(handler, str(source), recursive=recursive)
        self._observer.start()
        LOGGER.info("Monitorizando cambios en %s", source)

    def run_forever(self) -> None:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            LOGGER.info("Watchdog detenido por el usuario.")
        finally:
            self.stop()

    def stop(self) -> None:
        self._observer.stop()
        self._observer.join()
        self.sink_manager.close()
        self.processor.shutdown()
