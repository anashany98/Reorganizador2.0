import logging
import os
import sys
import threading
from pathlib import Path
from typing import Literal

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = APP_ROOT.parent
STATIC_ROOT = APP_ROOT / "static"
WEB_CSV_PATH = PROJECT_ROOT / "web_metadatos.csv"
WEB_DB_PATH = PROJECT_ROOT / "web_metadatos.db"

# Allow importing the package when the server is launched directly.
sys.path.append(str(PROJECT_ROOT))

from reorganizador_v2 import config as app_config, factusol_mapping, file_utils
from reorganizador_v2.config import OrganizeBy
from reorganizador_v2.processor import (
    BatchProcessor,
    FileProcessor,
    MetadataCache,
    ProcessingOptions,
    ProcessingRecord,
    SinkManager,
)
from reorganizador_v2.sinks import excel_audit
from reorganizador_v2.sinks.csv_sink import CsvSink
from reorganizador_v2.sinks.sqlite_sink import SQLiteSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("web_server")

app = FastAPI(title="Reorganizador 2.0 API")
app.mount("/static", StaticFiles(directory=STATIC_ROOT), name="static")


class JobState:
    def __init__(self) -> None:
        self.active = False
        self.total = 0
        self.processed = 0
        self.skipped = 0
        self.errors = 0
        self.current_file = ""
        self.match_ok = 0
        self.match_sin_numero = 0
        self.match_no_encontrado = 0
        self.match_ambiguo = 0
        self.lock = threading.Lock()
        self.cancel_event = threading.Event()

    def reset(self) -> None:
        with self.lock:
            self.active = False
            self.total = 0
            self.processed = 0
            self.skipped = 0
            self.errors = 0
            self.current_file = ""
            self.match_ok = 0
            self.match_sin_numero = 0
            self.match_no_encontrado = 0
            self.match_ambiguo = 0
            self.cancel_event.clear()

    def set_preparing(self) -> None:
        with self.lock:
            self.active = True
            self.total = 0
            self.processed = 0
            self.skipped = 0
            self.errors = 0
            self.current_file = "Analizando archivos..."
            self.match_ok = 0
            self.match_sin_numero = 0
            self.match_no_encontrado = 0
            self.match_ambiguo = 0

    def start(self, total: int) -> None:
        with self.lock:
            self.active = True
            self.total = total

    def update(self, record: ProcessingRecord) -> None:
        with self.lock:
            self.current_file = record.metadata.file_name
            if record.action_status == "error":
                self.errors += 1
            elif record.action == "skip":
                self.skipped += 1
            else:
                self.processed += 1
            if record.match_status:
                if record.match_status.startswith("OK_"):
                    self.match_ok += 1
                elif record.match_status == "SIN_NUMERO_PRESUPUESTO":
                    self.match_sin_numero += 1
                elif record.match_status == "NO_ENCONTRADO_EN_EXCEL":
                    self.match_no_encontrado += 1
                elif record.match_status == "AMBIGUO":
                    self.match_ambiguo += 1

    def finish(self) -> None:
        with self.lock:
            self.active = False
            self.current_file = ""


job_state = JobState()


class ScanConfig(BaseModel):
    source: str
    dest: str = ""
    organize_by: OrganizeBy = OrganizeBy.TYPE_DATE
    move: bool = False
    dry_run: bool = True
    hash_algo: Literal["sha1", "sha256", "md5", "xxhash", "none"] = app_config.DEFAULT_HASH
    min_size_mb: float = 0
    extensions: str = ""
    project_filter: str = ""
    mapping_excel: str = ""
    years: str = ""
    unmatched_dir: str = "_REVISION"
    require_budget_match: bool = False
    threads: int = 0
    processes: int = 0
    conflict: str = 'rename'
    dedup: bool = False


def _build_processing_options(config_data: ScanConfig, source: Path, dest: Path | None) -> ProcessingOptions:
    factusol_index = None
    if config_data.organize_by == OrganizeBy.FACTUSOL_CLIENT_BUDGET:
        if not config_data.mapping_excel:
            raise ValueError("El flujo FactuSOL requiere seleccionar el Excel simplificado.")
        factusol_index = factusol_mapping.load_mapping(Path(config_data.mapping_excel))

    return ProcessingOptions(
        source_root=source,
        dest_root=dest,
        organize_by=config_data.organize_by,
        move_files=config_data.move,
        dry_run=config_data.dry_run,
        hash_algorithm=config_data.hash_algo,
        incremental=True,
        verify_hash=not config_data.dry_run,
        threads=config_data.threads or app_config.DEFAULT_THREADS,
        processes=config_data.processes or app_config.DEFAULT_PROCESSES,
        factusol_index=factusol_index,
        allowed_years=_parse_years(config_data.years),
        unmatched_dir=config_data.unmatched_dir or "_REVISION",
        require_budget_match=config_data.require_budget_match,
        conflict=config_data.conflict,
        dedup=config_data.dedup,
    )


def run_scan_task(config_data: ScanConfig) -> None:
    """Execute the scan logic in a background thread."""
    logger.info("Starting scan task: %s", config_data.model_dump())

    try:
        source = Path(config_data.source).resolve()
        dest = Path(config_data.dest).resolve() if config_data.dest else None

        if dest:
            dest.mkdir(parents=True, exist_ok=True)

        options = _build_processing_options(config_data, source, dest)
        csv_sink = CsvSink(WEB_CSV_PATH)
        sqlite_sink = SQLiteSink(WEB_DB_PATH)

        cache = MetadataCache()
        cache.load_from_sqlite(sqlite_sink)

        processor = FileProcessor(options, cache)
        sink_manager = SinkManager(
            csv_sink=csv_sink,
            sqlite_sink=sqlite_sink,
            batch_size=1000,
        )
        batch_processor = BatchProcessor(processor, sink_manager)

        raw_files = file_utils.iter_files(source)
        filtered_files = []

        ext_set = {
            extension.strip().lower().replace(".", "")
            for extension in config_data.extensions.split(",")
            if extension.strip()
        }
        min_bytes = int(config_data.min_size_mb * 1024 * 1024)

        for path in raw_files:
            if job_state.cancel_event.is_set():
                break

            if min_bytes > 0:
                try:
                    if path.stat().st_size < min_bytes:
                        continue
                except OSError:
                    continue

            if ext_set and path.suffix.lower().lstrip(".") not in ext_set:
                continue

            filtered_files.append(path)

        # Filtro por número de proyecto (opcional).
        project_set = file_utils.parse_project_filter(config_data.project_filter)
        if project_set:
            before = len(filtered_files)
            filtered_files = [
                p for p in filtered_files
                if file_utils.path_matches_project_filter(p, source, project_set)
            ]
            logger.info(
                "Project filter: %d → %d files (%d projects)",
                before, len(filtered_files), len(project_set),
            )

        job_state.start(len(filtered_files))

        def _on_record(record: ProcessingRecord) -> None:
            job_state.update(record)

        try:
            for _ in batch_processor.process_paths(filtered_files, progress_callback=_on_record):
                if job_state.cancel_event.is_set():
                    logger.info("Scan cancelled by user.")
                    break
        finally:
            batch_processor.close()
            job_state.finish()
            logger.info("Scan task finished.")

    except Exception as exc:
        logger.exception("Scan failed: %s", exc)
        job_state.finish()


@app.get("/")
async def read_root() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@app.get("/api/browse")
async def browse(path: str = "") -> dict:
    """List directories in the given path."""
    try:
        candidate = Path(path).resolve() if path else Path.home().resolve()
        if not candidate.exists() or not candidate.is_dir():
            raise HTTPException(status_code=404, detail="Path not found or not a directory")

        items = []
        try:
            for item in candidate.iterdir():
                if item.is_dir() and not item.name.startswith("."):
                    items.append(
                        {
                            "name": item.name,
                            "path": str(item),
                            "type": "dir",
                        }
                    )
        except PermissionError:
            pass

        items.sort(key=lambda entry: entry["name"].lower())

        return {
            "current": str(candidate),
            "parent": str(candidate.parent) if candidate.parent != candidate else None,
            "items": items,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Browse error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/scan")
async def start_scan(config: ScanConfig, background_tasks: BackgroundTasks) -> dict:
    if job_state.active:
        raise HTTPException(status_code=400, detail="Job already running")

    if not os.path.exists(config.source):
        raise HTTPException(status_code=404, detail="Source directory not found")

    job_state.reset()
    job_state.set_preparing()
    background_tasks.add_task(run_scan_task, config)
    return {"message": "Scan started", "job_id": "job_1"}


@app.post("/api/stop")
async def stop_scan() -> dict:
    job_state.cancel_event.set()
    return {"message": "Cancellation requested"}


@app.get("/api/status")
async def get_status() -> dict:
    with job_state.lock:
        processed_total = job_state.processed + job_state.skipped + job_state.errors
        percent = 0 if job_state.total == 0 else round((processed_total / job_state.total) * 100)
        return {
            "active": job_state.active,
            "total": job_state.total,
            "processed": processed_total,
            "percent": percent,
            "current_file": job_state.current_file,
            "stats": {
                "processed": job_state.processed,
                "skipped": job_state.skipped,
                "errors": job_state.errors,
            },
            "match_counters": {
                "OK": job_state.match_ok,
                "SIN_NUMERO_PRESUPUESTO": job_state.match_sin_numero,
                "NO_ENCONTRADO_EN_EXCEL": job_state.match_no_encontrado,
                "AMBIGUO": job_state.match_ambiguo,
            },
        }


@app.get("/api/history")
async def get_history(page: int = 1, page_size: int = 50) -> dict:
    if not WEB_DB_PATH.exists():
        return {"items": [], "total": 0, "page": page, "page_size": page_size}

    try:
        import sqlite3

        with sqlite3.connect(WEB_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
            if not cursor.fetchone():
                return {"items": [], "total": 0, "page": page, "page_size": page_size}

            cursor.execute("SELECT COUNT(*) FROM files")
            total = cursor.fetchone()[0]

            offset = max(0, (page - 1) * page_size)
            cursor.execute(
                """
                SELECT
                    COALESCE(modified_time, created_time, '') AS created_time,
                    file_name,
                    action,
                    action_status,
                    gestor,
                    proyecto,
                    year,
                    presupuesto_detectado,
                    cliente,
                    sede_hotel_direccion,
                    referencia,
                    tipo_documento,
                    match_status,
                    match_confidence,
                    dst_path,
                    src_path
                FROM files
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (page_size, offset),
            )
            return {
                "items": [dict(row) for row in cursor.fetchall()],
                "total": total,
                "page": page,
                "page_size": page_size,
            }
    except Exception as exc:
        logger.exception("History error: %s", exc)
        return {"items": [], "total": 0, "error": str(exc)}


@app.get("/api/report")
async def get_report() -> dict:
    """Resumen post-scan agrupado por gestor y proyecto."""
    if not WEB_DB_PATH.exists():
        return {"gestores": [], "proyectos": [], "total_files": 0}

    try:
        import sqlite3

        with sqlite3.connect(WEB_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
            if not cursor.fetchone():
                return {"gestores": [], "proyectos": [], "total_files": 0}

            cursor.execute("SELECT COUNT(*) FROM files")
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT
                    COALESCE(gestor, 'Sin gestor') AS gestor,
                    COUNT(*) AS total,
                    SUM(CASE WHEN action_status = 'error' THEN 1 ELSE 0 END) AS errores,
                    SUM(CASE WHEN action = 'copy' THEN 1 ELSE 0 END) AS copiados,
                    SUM(CASE WHEN action = 'move' THEN 1 ELSE 0 END) AS movidos,
                    SUM(CASE WHEN action = 'skip' THEN 1 ELSE 0 END) AS omitidos
                FROM files
                GROUP BY gestor
                ORDER BY total DESC
                """
            )
            gestores = [dict(row) for row in cursor.fetchall()]

            cursor.execute(
                """
                SELECT
                    COALESCE(gestor, 'Sin gestor') AS gestor,
                    COALESCE(proyecto, 'Sin proyecto') AS proyecto,
                    COUNT(*) AS total,
                    SUM(CASE WHEN action_status = 'error' THEN 1 ELSE 0 END) AS errores
                FROM files
                GROUP BY gestor, proyecto
                ORDER BY total DESC
                LIMIT 30
                """
            )
            proyectos = [dict(row) for row in cursor.fetchall()]

            return {
                "gestores": gestores,
                "proyectos": proyectos,
                "total_files": total,
            }
    except Exception as exc:
        logger.exception("Report error: %s", exc)
        return {"gestores": [], "proyectos": [], "total_files": 0, "error": str(exc)}



@app.get("/api/audit")
async def download_audit():
    """Descarga el Excel de auditoria generado desde la ultima ejecucion."""
    from fastapi.responses import FileResponse
    excel_path = PROJECT_ROOT / "auditoria.xlsx"
    if not WEB_DB_PATH.exists():
        raise HTTPException(status_code=404, detail="No hay datos de escaneo todavia.")
    try:
        excel_audit.generate_audit_excel(
            db_path=WEB_DB_PATH,
            output_path=excel_path,
        )
    except Exception as exc:
        logger.exception("Audit generation failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return FileResponse(
        path=excel_path,
        filename="auditoria_reorganizador.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/api/preview")
async def preview_source(
    source: str = "",
    projects: str = "",
    mapping_excel: str = "",
    years: str = "",
    dest: str = "",
    unmatched_dir: str = "_REVISION",
) -> dict:
    """Pre-escaneo informativo sin procesar archivos."""
    from pathlib import Path
    import sqlite3

    src = Path(source).resolve() if source else None
    if not src or not src.exists():
        return {"error": "Origen no valido", "total_files": 0}

    all_files = list(file_utils.iter_files(src))
    if not all_files:
        return {"error": "", "total_files": 0, "pending": 0, "total_size_bytes": 0,
                "extensions": {}, "gestores": {}, "proyectos": {}}

    # Filtro de proyectos
    project_set = file_utils.parse_project_filter(projects)
    if project_set:
        all_files = [f for f in all_files if file_utils.path_matches_project_filter(f, src, project_set)]

    total = len(all_files)
    total_bytes = 0
    ext_count: dict[str, int] = {}
    gestor_count: dict[str, int] = {}
    proyecto_count: dict[str, int] = {}

    for f in all_files:
        try:
            total_bytes += f.stat().st_size
        except OSError:
            pass
        ext = f.suffix.lower().lstrip(".") or "sin_ext"
        ext_count[ext] = ext_count.get(ext, 0) + 1
        gestor, proyecto = file_utils.extract_manager_project(f, src)
        if gestor:
            gestor_count[gestor] = gestor_count.get(gestor, 0) + 1
        if proyecto:
            proyecto_count[proyecto] = proyecto_count.get(proyecto, 0) + 1

    # Ya procesados (incremental)
    already = 0
    if WEB_DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(WEB_DB_PATH))
            cursor = conn.cursor()
            cursor.execute("SELECT src_path FROM files")
            cached = {row[0] for row in cursor.fetchall()}
            already = sum(1 for f in all_files if str(f) in cached)
            conn.close()
        except Exception:
            pass

    result = {
        "total_files": total,
        "processed_already": already,
        "pending": max(0, total - already),
        "total_size_bytes": total_bytes,
        "extensions": dict(sorted(ext_count.items(), key=lambda x: -x[1])[:15]),
        "gestores": dict(sorted(gestor_count.items(), key=lambda x: -x[1])[:15]),
        "proyectos": dict(sorted(proyecto_count.items(), key=lambda x: -x[1])[:15]),
    }
    if mapping_excel:
        result.update(_build_factusol_preview(all_files, src, mapping_excel, years, dest, unmatched_dir))
    return result


def _parse_years(raw: str | None) -> set[str] | None:
    if not raw or not raw.strip():
        return None
    years = {item.strip() for item in raw.split(",") if item.strip()}
    return years or None


def _build_factusol_preview(
    files: list[Path],
    source_root: Path,
    mapping_excel: str,
    years: str,
    dest: str,
    unmatched_dir: str,
) -> dict:
    index = factusol_mapping.load_mapping(Path(mapping_excel))
    allowed_years = _parse_years(years)
    dest_root = Path(dest).resolve() if dest else Path("DESTINO").resolve()
    counters = {
        "OK": 0,
        "SIN_NUMERO_PRESUPUESTO": 0,
        "NO_ENCONTRADO_EN_EXCEL": 0,
        "AMBIGUO": 0,
        "ERRORES": 0,
    }
    items = []
    for file_path in files[:500]:
        try:
            match = factusol_mapping.resolve_budget_match(
                path=file_path,
                mapping_index=index,
                allowed_years=allowed_years,
            )
            match.tipo_documento = factusol_mapping.categorize_document_type(file_path)
            dst_path = factusol_mapping.build_factusol_client_budget_destination_path(
                src=file_path,
                dest_root=dest_root,
                source_root=source_root,
                match_result=match,
                unmatched_dir=unmatched_dir or "_REVISION",
            )
            if match.status.startswith("OK_"):
                counters["OK"] += 1
            elif match.status in counters:
                counters[match.status] += 1
            items.append(
                {
                    "file_name": file_path.name,
                    "src_path": str(file_path),
                    "presupuesto_detectado": match.presupuesto_detectado or "",
                    "cliente": match.cliente,
                    "sede_hotel_direccion": match.sede_hotel_direccion,
                    "referencia": match.referencia,
                    "tipo_documento": match.tipo_documento,
                    "match_status": match.status,
                    "match_confidence": round(match.confidence, 4),
                    "dst_path": str(dst_path),
                }
            )
        except Exception as exc:
            counters["ERRORES"] += 1
            items.append(
                {
                    "file_name": file_path.name,
                    "src_path": str(file_path),
                    "presupuesto_detectado": "",
                    "cliente": "",
                    "sede_hotel_direccion": "",
                    "referencia": "",
                    "tipo_documento": "",
                    "match_status": "ERROR",
                    "match_confidence": 0,
                    "dst_path": "",
                    "error": str(exc),
                }
            )
    return {"items": items, "match_counters": counters}

def main() -> None:
    import uvicorn

    host = os.getenv("REORGANIZADOR_HOST", "127.0.0.1")
    port = int(os.getenv("REORGANIZADOR_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
