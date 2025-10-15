"""Command line interface for extractor v2."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from rich.console import Console
from concurrent.futures import ThreadPoolExecutor

from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from . import config, file_utils
from .processor import (
    BatchProcessor,
    FileProcessor,
    MetadataCache,
    ProcessingOptions,
    ProcessingRecord,
    SinkManager,
)
from .sinks.csv_sink import CsvSink
from .sinks.sqlite_sink import SQLiteSink
from .sinks.sqlserver_sink import SqlServerSink
from .watchdog_agent import WatchdogAgent

console = Console()


@dataclass
class ProcessingStats:
    """Aggregate statistics for reporting."""

    processed: int = 0
    skipped: int = 0
    errors: int = 0

    def update(self, record: ProcessingRecord) -> None:
        if record.action_status == "error":
            self.errors += 1
        elif record.action == "skip":
            self.skipped += 1
        else:
            self.processed += 1

    def as_table(self) -> Table:
        table = Table(title="Resumen de procesamiento")
        table.add_column("Estado", justify="left")
        table.add_column("Cantidad", justify="right")
        table.add_row("Procesados", str(self.processed))
        table.add_row("Omitidos", str(self.skipped))
        table.add_row("Errores", str(self.errors))
        return table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extractor_v2",
        description="Organiza archivos, extrae metadatos y sincroniza hashes.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nivel de logging (DEBUG, INFO, WARNING, ERROR).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # scan command
    scan_parser = subparsers.add_parser(
        "scan",
        help="Procesa una carpeta de forma puntual.",
    )
    scan_parser.add_argument("--source", required=True, help="Carpeta origen a explorar.")
    scan_parser.add_argument("--dest", help="Carpeta destino para organizar/copiar.")
    scan_parser.add_argument(
        "--organize-by",
        default="type-date",
        choices=["flat", "type", "date", "type-date"],
        help="Estrategia de organización en destino.",
    )
    scan_parser.add_argument(
        "--move",
        action="store_true",
        help="Mover archivos en vez de copiarlos.",
    )
    scan_parser.add_argument("--dry-run", action="store_true", help="Simula sin copiar/mover.")
    scan_parser.add_argument(
        "--hash-algo",
        default=config.DEFAULT_HASH,
        choices=sorted(config.SUPPORTED_HASH_ALGOS | {"none"}),
        help="Algoritmo de hash.",
    )
    scan_parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Desactiva el procesamiento incremental.",
    )
    scan_parser.add_argument(
        "--no-verify",
        action="store_true",
        help="No verificar hash de la copia.",
    )
    scan_parser.add_argument(
        "--threads",
        type=int,
        default=config.DEFAULT_THREADS,
        help="Cantidad de hilos para procesamiento.",
    )
    scan_parser.add_argument(
        "--processes",
        type=int,
        default=0,
        help="Cantidad de procesos para hashing (0 desactiva).",
    )
    scan_parser.add_argument(
        "--csv-out",
        default="metadatos.csv",
        help="Ruta del CSV incremental.",
    )
    scan_parser.add_argument(
        "--sqlite-db",
        default="metadatos.db",
        help="Ruta de la base de datos SQLite.",
    )
    scan_parser.add_argument(
        "--sqlserver-conn",
        help="Cadena de conexión para SQL Server (opcional).",
    )
    scan_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Lote de escritura en sinks.",
    )

    # watch command
    watch_parser = subparsers.add_parser(
        "watch",
        help="Monitoriza en continuo con Watchdog.",
    )
    watch_parser.add_argument("--source", required=True, help="Carpeta a monitorizar.")
    watch_parser.add_argument("--dest", help="Carpeta destino para copias.")
    watch_parser.add_argument(
        "--organize-by",
        default="type-date",
        choices=["flat", "type", "date", "type-date"],
        help="Estrategia de organización en destino.",
    )
    watch_parser.add_argument(
        "--move",
        action="store_true",
        help="Mover en vez de copiar durante la monitorización.",
    )
    watch_parser.add_argument(
        "--hash-algo",
        default=config.DEFAULT_HASH,
        choices=sorted(config.SUPPORTED_HASH_ALGOS | {"none"}),
        help="Algoritmo de hash.",
    )
    watch_parser.add_argument(
        "--threads",
        type=int,
        default=config.DEFAULT_THREADS,
        help="Hilos para eventos concurrentes.",
    )
    watch_parser.add_argument(
        "--processes",
        type=int,
        default=0,
        help="Procesos para hashing.",
    )
    watch_parser.add_argument("--csv-out", default="metadatos.csv")
    watch_parser.add_argument("--sqlite-db", default="metadatos.db")
    watch_parser.add_argument("--sqlserver-conn")

    # verify command
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verifica coincidencia de hashes entre origen y copia.",
    )
    verify_parser.add_argument(
        "--sqlite-db",
        help="Ruta a la base SQLite con metadatos.",
    )
    verify_parser.add_argument(
        "--csv",
        help="Ruta a CSV de metadatos (alternativo a SQLite).",
    )
    verify_parser.add_argument(
        "--hash-algo",
        choices=sorted(config.SUPPORTED_HASH_ALGOS | {"none"}),
        help="Algoritmo a recalcular (por defecto usa el de cada registro o sha256).",
    )
    verify_parser.add_argument(
        "--threads",
        type=int,
        default=config.DEFAULT_THREADS,
        help="Número de hilos para verificación.",
    )

    return parser


def run_scan(args: argparse.Namespace) -> None:
    """Procesa la carpeta origen una única vez con las opciones del comando `scan`."""
    log_file = config.setup_logging(args.log_level)
    console.print(f"[green]Logs guardados en[/green] {log_file}")

    source = Path(args.source).resolve()
    if not source.exists():
        raise FileNotFoundError(f"La ruta origen no existe: {source}")

    dest = Path(args.dest).resolve() if args.dest else None
    if dest:
        dest.mkdir(parents=True, exist_ok=True)

    options = ProcessingOptions(
        source_root=source,
        dest_root=dest,
        organize_by=args.organize_by,
        move_files=args.move,
        dry_run=args.dry_run,
        hash_algorithm=args.hash_algo,
        incremental=not args.no_incremental,
        verify_hash=not args.no_verify,
        threads=max(1, args.threads),
        processes=max(0, args.processes),
    )

    csv_sink = CsvSink(Path(args.csv_out)) if args.csv_out else None
    sqlite_sink = SQLiteSink(Path(args.sqlite_db)) if args.sqlite_db else None
    sqlserver_sink = (
        SqlServerSink(args.sqlserver_conn) if args.sqlserver_conn else None
    )

    cache = MetadataCache()
    if sqlite_sink:
        cache.load_from_sqlite(sqlite_sink)
    if args.csv_out:
        cache.load_from_csv(Path(args.csv_out))

    processor = FileProcessor(options, cache)
    sink_manager = SinkManager(
        csv_sink=csv_sink,
        sqlite_sink=sqlite_sink,
        sqlserver_sink=sqlserver_sink,
        batch_size=args.batch_size,
    )
    batch_processor = BatchProcessor(processor, sink_manager)

    files = [path for path in file_utils.iter_files(source)]
    stats = ProcessingStats()

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=True,
    )

    with progress:
        task_id = progress.add_task("Procesando archivos", total=len(files))

        def _on_record(rec: ProcessingRecord) -> None:
            stats.update(rec)
            progress.advance(task_id)

        try:
            for _record in batch_processor.process_paths(files, progress_callback=_on_record):
                pass
        finally:
            batch_processor.close()

    console.print(stats.as_table())


def run_watch(args: argparse.Namespace) -> None:
    """Mantiene una monitorización continua del origen y replica cambios en destino."""
    log_file = config.setup_logging(args.log_level)
    console.print(f"[green]Logs guardados en[/green] {log_file}")

    source = Path(args.source).resolve()
    if not source.exists():
        raise FileNotFoundError(f"La ruta origen no existe: {source}")

    dest = Path(args.dest).resolve() if args.dest else None
    if dest:
        dest.mkdir(parents=True, exist_ok=True)

    options = ProcessingOptions(
        source_root=source,
        dest_root=dest,
        organize_by=args.organize_by,
        move_files=args.move,
        hash_algorithm=args.hash_algo,
        incremental=True,
        verify_hash=True,
        threads=max(1, args.threads),
        processes=max(0, args.processes),
    )

    csv_sink = CsvSink(Path(args.csv_out)) if args.csv_out else None
    sqlite_sink = SQLiteSink(Path(args.sqlite_db)) if args.sqlite_db else None
    sqlserver_sink = (
        SqlServerSink(args.sqlserver_conn) if args.sqlserver_conn else None
    )

    cache = MetadataCache()
    if sqlite_sink:
        cache.load_from_sqlite(sqlite_sink)
    if args.csv_out:
        cache.load_from_csv(Path(args.csv_out))

    processor = FileProcessor(options, cache)
    sink_manager = SinkManager(
        csv_sink=csv_sink,
        sqlite_sink=sqlite_sink,
        sqlserver_sink=sqlserver_sink,
        batch_size=1,
    )

    agent = WatchdogAgent(processor, sink_manager)
    agent.start(source)
    agent.run_forever()


def run_verify(args: argparse.Namespace) -> None:
    """Chequea que cada copia conserve el mismo hash que el archivo original."""
    log_file = config.setup_logging(args.log_level)
    console.print(f"[green]Logs guardados en[/green] {log_file}")

    entries = _load_verify_entries(args.sqlite_db, args.csv)
    if not entries:
        console.print("[yellow]No se encontraron registros para verificar.[/yellow]")
        return

    stats = {"ok": 0, "missing": 0, "mismatch": 0}
    algorithm_override = args.hash_algo

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=True,
    )

    with progress, ThreadPoolExecutor(max_workers=max(1, args.threads)) as executor:
        task_id = progress.add_task("Verificando hashes", total=len(entries))

        futures = [
            executor.submit(_verify_entry, entry, algorithm_override) for entry in entries
        ]

        for entry, future in zip(entries, futures):
            result = future.result()
            entry["verification_result"] = result
            stats[result] += 1
            progress.advance(task_id)

    table = Table(title="Resultado de verificación")
    table.add_column("Estado")
    table.add_column("Cantidad", justify="right")
    for key in ("ok", "missing", "mismatch"):
        table.add_row(key.capitalize(), str(stats[key]))
    console.print(table)

    if args.sqlite_db:
        _update_verification_status(Path(args.sqlite_db), entries)


def _load_verify_entries(sqlite_db: Optional[str], csv_path: Optional[str]) -> List[dict]:
    entries: List[dict] = []
    if sqlite_db:
        db_path = Path(sqlite_db)
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT src_path, dst_path, hash_value, hash_value_dst, hash_algo
                    FROM files
                    WHERE dst_path IS NOT NULL
                    """
                )
                for src_path, dst_path, hash_value, hash_value_dst, hash_algo in cursor.fetchall():
                    entries.append(
                        {
                            "src_path": src_path,
                            "dst_path": dst_path,
                            "hash_value": hash_value,
                            "hash_value_dst": hash_value_dst,
                            "hash_algo": hash_algo,
                        }
                    )
            finally:
                conn.close()
    if csv_path:
        path = Path(csv_path)
        if path.exists():
            import csv as csv_module

            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv_module.DictReader(handle)
                for row in reader:
                    if row.get("dst_path"):
                        entries.append(
                            {
                                "src_path": row.get("src_path"),
                                "dst_path": row.get("dst_path"),
                                "hash_value": row.get("hash_value"),
                                "hash_value_dst": row.get("hash_value_dst"),
                                "hash_algo": row.get("hash_algo"),
                            }
                        )
    return entries


def _verify_entry(entry: dict, algorithm_override: Optional[str]) -> str:
    from .file_utils import compute_hash

    src_path = Path(entry["src_path"])
    dst_path = Path(entry["dst_path"])

    if not src_path.exists() or not dst_path.exists():
        return "missing"

    algorithm = algorithm_override or entry.get("hash_algo") or config.DEFAULT_HASH

    src_result = compute_hash(src_path, algorithm)
    dst_result = compute_hash(dst_path, algorithm)

    return "ok" if src_result.value == dst_result.value else "mismatch"


def _update_verification_status(sqlite_db: Path, entries: Sequence[dict]) -> None:
    try:
        conn = sqlite3.connect(str(sqlite_db))
        cursor = conn.cursor()
        for entry in entries:
            src_path = entry.get("src_path")
            if not src_path:
                continue
            result = entry.get("verification_result")
            if result is None:
                continue
            verified_flag = 1 if result == "ok" else 0
            cursor.execute(
                """
                UPDATE files
                SET verified = ?, hash_verified = ?
                WHERE src_path = ?
                """,
                (verified_flag, result, src_path),
            )
        conn.commit()
    except Exception as exc:  # pragma: no cover - defensive
        logging.getLogger(__name__).warning("No se pudo actualizar SQLite: %s", exc)
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "scan":
            run_scan(args)
        elif args.command == "watch":
            run_watch(args)
        elif args.command == "verify":
            run_verify(args)
        else:
            parser.print_help()
    except Exception as exc:
        console.print(f"[red]Error:[/red] {exc}")
        logging.getLogger(__name__).exception("Fallo en la ejecución principal")
        raise


if __name__ == "__main__":
    main()
