"""Tests for the local FastAPI server wiring."""

from __future__ import annotations

import asyncio
from pathlib import Path

from web.server import app
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
