"""CSV sink for metadata storage."""

from __future__ import annotations

import csv
import threading
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .. import config


class CsvSink:
    """Thread-safe CSV writer."""

    def __init__(self, path: Path, headers: Sequence[str] | None = None) -> None:
        self.path = path
        self.headers = list(headers or config.CSV_HEADERS)
        self._lock = threading.Lock()
        self._ensure_parent()
        self._initialize_file()

    def _ensure_parent(self) -> None:
        if self.path.parent:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def _initialize_file(self) -> None:
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=self.headers)
                writer.writeheader()
            return

        with self.path.open("r", newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            existing_headers = list(reader.fieldnames or [])
            if not existing_headers or all(header in existing_headers for header in self.headers):
                if existing_headers:
                    self.headers = existing_headers
                return
            existing_rows = list(reader)

        merged_headers = existing_headers + [
            header for header in self.headers if header not in existing_headers
        ]
        self.headers = merged_headers
        with self.path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=self.headers)
            writer.writeheader()
            writer.writerows(existing_rows)

    def append_records(self, records: Iterable[Mapping[str, object]]) -> None:
        rows = list(records)
        if not rows:
            return

        with self._lock, self.path.open("a", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=self.headers)
            writer.writerows(rows)
