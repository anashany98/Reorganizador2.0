"""FactuSOL mapping support for budget-based document organization."""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence


OK_STATUSES = {"OK_EXACTO", "OK_NORMALIZADO", "OK_COMPACTO", "OK_RECOMPUESTO"}
STATUS_SIN_NUMERO = "SIN_NUMERO_PRESUPUESTO"
STATUS_NO_ENCONTRADO = "NO_ENCONTRADO_EN_EXCEL"
STATUS_AMBIGUO = "AMBIGUO"

REQUIRED_COLUMNS = [
    "Anio",
    "Presupuesto",
    "Cliente",
    "Sede_Hotel_Direccion",
    "Referencia",
    "OrigenAsignacion",
    "ClaveInterna",
]

OPTIONAL_COLUMNS = ["DuplicadoAnioPresupuesto"]

_SEPARATOR_RE = re.compile(r"[-./\\_()\[\]#]+")
_SPACES_RE = re.compile(r"\s+")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_INVALID_PATH_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
_NUMBER_RE = re.compile(r"\d+")


@dataclass(slots=True)
class FactusolBudgetRecord:
    anio: str
    presupuesto: str
    cliente: str
    sede_hotel_direccion: str
    referencia: str
    origen_asignacion: str
    clave_interna: str
    duplicado_anio_presupuesto: str = ""


@dataclass(slots=True)
class BudgetCandidate:
    raw_text: str
    normalized_text: str
    compact_text: str
    detected_budget: str
    confidence: float
    source_level: str
    reason: str


@dataclass(slots=True)
class MatchResult:
    status: str
    record: FactusolBudgetRecord | None = None
    candidate: BudgetCandidate | None = None
    year: str | None = None
    presupuesto_detectado: str | None = None
    cliente: str = ""
    sede_hotel_direccion: str = ""
    referencia: str = ""
    origen_asignacion: str = ""
    clave_interna: str = ""
    tipo_documento: str = ""
    confidence: float = 0.0
    source_level: str = ""
    reason: str = ""
    texto_detectado: str = ""
    duplicado_anio_presupuesto: str = ""

    @property
    def is_ok(self) -> bool:
        return self.status in OK_STATUSES and self.record is not None


@dataclass(slots=True)
class FactusolMappingIndex:
    records: list[FactusolBudgetRecord]
    mapping_by_year_budget: dict[tuple[str, str], list[FactusolBudgetRecord]] = field(default_factory=dict)
    records_by_budget: dict[str, list[FactusolBudgetRecord]] = field(default_factory=dict)
    years: set[str] = field(default_factory=set)
    budgets: set[str] = field(default_factory=set)
    duplicate_keys: set[tuple[str, str]] = field(default_factory=set)

    @classmethod
    def from_records(cls, records: Iterable[FactusolBudgetRecord]) -> "FactusolMappingIndex":
        materialized = list(records)
        by_key: dict[tuple[str, str], list[FactusolBudgetRecord]] = defaultdict(list)
        by_budget: dict[str, list[FactusolBudgetRecord]] = defaultdict(list)
        years: set[str] = set()
        budgets: set[str] = set()

        for record in materialized:
            key = (record.anio, record.presupuesto)
            by_key[key].append(record)
            by_budget[record.presupuesto].append(record)
            years.add(record.anio)
            budgets.add(record.presupuesto)

        duplicates = {key for key, values in by_key.items() if len(values) > 1}
        return cls(
            records=materialized,
            mapping_by_year_budget=dict(by_key),
            records_by_budget=dict(by_budget),
            years=years,
            budgets=budgets,
            duplicate_keys=duplicates,
        )

    def is_duplicate(self, record: FactusolBudgetRecord) -> bool:
        return (record.anio, record.presupuesto) in self.duplicate_keys


class FactusolMappingLoader:
    """Loads the simplified FactuSOL Excel mapping."""

    sheet_name = "Mapping_FactuSOL"

    def load_mapping(self, excel_path: Path) -> FactusolMappingIndex:
        try:
            from openpyxl import load_workbook
        except ImportError as exc:  # pragma: no cover - dependency is declared.
            raise RuntimeError("openpyxl no esta instalado. pip install openpyxl") from exc

        if not excel_path.exists():
            raise FileNotFoundError(f"Excel de mapping no encontrado: {excel_path}")

        workbook = load_workbook(excel_path, read_only=True, data_only=True)
        try:
            if self.sheet_name not in workbook.sheetnames:
                raise ValueError(f"El Excel debe contener la hoja '{self.sheet_name}'.")

            sheet = workbook[self.sheet_name]
            rows = sheet.iter_rows(values_only=True)
            try:
                header_row = next(rows)
            except StopIteration as exc:
                raise ValueError("La hoja Mapping_FactuSOL esta vacia.") from exc

            headers = [_cell_to_text(value) for value in header_row]
            header_positions = {name: idx for idx, name in enumerate(headers) if name}
            missing = [column for column in REQUIRED_COLUMNS if column not in header_positions]
            if missing:
                raise ValueError(
                    "Faltan columnas obligatorias en Mapping_FactuSOL: "
                    + ", ".join(missing)
                )

            records: list[FactusolBudgetRecord] = []
            for row in rows:
                data = {
                    column: _cell_to_text(row[header_positions[column]])
                    if header_positions[column] < len(row)
                    else ""
                    for column in REQUIRED_COLUMNS
                }
                for column in OPTIONAL_COLUMNS:
                    idx = header_positions.get(column)
                    data[column] = _cell_to_text(row[idx]) if idx is not None and idx < len(row) else ""

                if not any(data.values()):
                    continue

                cliente = data["Cliente"].strip()
                sede = data["Sede_Hotel_Direccion"].strip() or cliente
                records.append(
                    FactusolBudgetRecord(
                        anio=data["Anio"].strip(),
                        presupuesto=data["Presupuesto"].strip(),
                        cliente=cliente,
                        sede_hotel_direccion=sede,
                        referencia=data["Referencia"].strip(),
                        origen_asignacion=data["OrigenAsignacion"].strip(),
                        clave_interna=data["ClaveInterna"].strip(),
                        duplicado_anio_presupuesto=data.get("DuplicadoAnioPresupuesto", "").strip(),
                    )
                )
        finally:
            workbook.close()

        return FactusolMappingIndex.from_records(records)


def load_mapping(excel_path: Path) -> FactusolMappingIndex:
    return FactusolMappingLoader().load_mapping(excel_path)


def normalize_text(value: str) -> str:
    raw = "" if value is None else str(value)
    without_accents = _strip_accents(raw).upper()
    separated = _SEPARATOR_RE.sub(" ", without_accents)
    separated = _CONTROL_RE.sub(" ", separated)
    return _SPACES_RE.sub(" ", separated).strip()


def compact_text(value: str) -> str:
    return normalize_text(value).replace(" ", "")


def sanitize_path_part(value: str, max_len: int = 80) -> str:
    text = _strip_accents("" if value is None else str(value))
    text = _CONTROL_RE.sub(" ", text)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = _INVALID_PATH_CHARS_RE.sub(" ", text)
    text = _SPACES_RE.sub(" ", text).strip(" .")
    if max_len > 0:
        text = text[:max_len].rstrip(" .")
    return text or "_SIN_NOMBRE"


def detect_year_from_path(path: Path, allowed_years: set[str]) -> str | None:
    if not allowed_years:
        return None
    for part in path.parts:
        normalized = normalize_text(part)
        if normalized in allowed_years:
            return normalized
    for number in _NUMBER_RE.findall(normalize_text(str(path))):
        if number in allowed_years:
            return number
    return None


def detect_budget_candidates(path: Path, mapping_index: FactusolMappingIndex) -> list[BudgetCandidate]:
    candidates: list[BudgetCandidate] = []
    seen: set[tuple[str, str, str]] = set()

    for raw_text, source_level, source_rank in _path_source_texts(path):
        if not raw_text:
            continue
        normalized = normalize_text(raw_text)
        compact = compact_text(raw_text)
        for budget in mapping_index.budgets:
            reason = _match_reason_for_budget(raw_text, budget)
            if not reason:
                continue
            key = (budget, source_level, normalized)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                BudgetCandidate(
                    raw_text=raw_text,
                    normalized_text=normalized,
                    compact_text=compact,
                    detected_budget=budget,
                    confidence=_confidence_for(reason, source_rank),
                    source_level=source_level,
                    reason=reason,
                )
            )

    candidates.sort(key=lambda item: item.confidence, reverse=True)
    return candidates


def resolve_budget_match(
    path: Path,
    mapping_index: FactusolMappingIndex,
    allowed_years: set[str] | None = None,
) -> MatchResult:
    effective_years = {str(year).strip() for year in allowed_years or mapping_index.years if str(year).strip()}
    detected_year = detect_year_from_path(path, effective_years)
    candidates = detect_budget_candidates(path, mapping_index)

    if not candidates:
        unknown = _unknown_budget_numbers(path, mapping_index)
        if unknown:
            return MatchResult(
                status=STATUS_NO_ENCONTRADO,
                year=detected_year,
                presupuesto_detectado=unknown[0],
                reason=STATUS_NO_ENCONTRADO,
                texto_detectado=unknown[0],
            )
        return MatchResult(
            status=STATUS_SIN_NUMERO,
            year=detected_year,
            reason=STATUS_SIN_NUMERO,
        )

    best_by_budget: dict[str, BudgetCandidate] = {}
    for candidate in candidates:
        existing = best_by_budget.get(candidate.detected_budget)
        if existing is None or candidate.confidence > existing.confidence:
            best_by_budget[candidate.detected_budget] = candidate

    ranked_candidates = sorted(best_by_budget.values(), key=lambda item: item.confidence, reverse=True)
    if len(ranked_candidates) > 1:
        best = ranked_candidates[0]
        second = ranked_candidates[1]
        if best.confidence - second.confidence < 0.15:
            return MatchResult(
                status=STATUS_AMBIGUO,
                year=detected_year,
                presupuesto_detectado=", ".join(candidate.detected_budget for candidate in ranked_candidates),
                candidate=best,
                confidence=best.confidence,
                source_level=best.source_level,
                reason="Varios presupuestos candidatos con confianza similar.",
                texto_detectado=best.raw_text,
            )

    candidate = ranked_candidates[0]
    records = _records_for_candidate(candidate.detected_budget, mapping_index, detected_year, effective_years)
    if not records:
        return MatchResult(
            status=STATUS_NO_ENCONTRADO,
            candidate=candidate,
            year=detected_year,
            presupuesto_detectado=candidate.detected_budget,
            confidence=candidate.confidence,
            source_level=candidate.source_level,
            reason=STATUS_NO_ENCONTRADO,
            texto_detectado=candidate.raw_text,
        )

    if len(records) == 1:
        return _ok_result(
            status=candidate.reason,
            record=records[0],
            candidate=candidate,
            mapping_index=mapping_index,
            confidence=candidate.confidence,
        )

    resolved = _resolve_duplicate_record(path, records)
    if resolved is None:
        return MatchResult(
            status=STATUS_AMBIGUO,
            candidate=candidate,
            year=detected_year,
            presupuesto_detectado=candidate.detected_budget,
            confidence=candidate.confidence,
            source_level=candidate.source_level,
            reason="Duplicado Anio+Presupuesto sin contexto suficiente.",
            texto_detectado=candidate.raw_text,
            duplicado_anio_presupuesto="SI",
        )

    record, score, margin = resolved
    confidence = min(1.0, candidate.confidence + min(score, 5.0) / 20.0)
    return _ok_result(
        status=candidate.reason,
        record=record,
        candidate=candidate,
        mapping_index=mapping_index,
        confidence=confidence,
        reason=f"Duplicado resuelto por contexto; margen {margin:.2f}.",
    )


def categorize_document_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return "PDF"
    if ext in {".xls", ".xlsx", ".xlsm", ".csv"}:
        return "EXCEL"
    if ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp", ".heic"}:
        return "IMAGENES"
    if ext in {".msg", ".eml", ".pst", ".ost"}:
        return "CORREOS"
    if ext in {".dwg", ".dxf", ".skp", ".rvt", ".ifc", ".pln", ".3dm"}:
        return "PLANOS"
    if ext in {".doc", ".docx"}:
        return "WORD"
    if ext in {".zip", ".rar", ".7z"}:
        return "ZIP"
    return "OTROS"


def build_factusol_client_budget_destination_path(
    src: Path,
    dest_root: Path,
    source_root: Path,
    match_result: MatchResult,
    unmatched_dir: str = "_REVISION",
) -> Path:
    if match_result.is_ok and match_result.record is not None:
        record = match_result.record
        sede = record.sede_hotel_direccion or record.cliente
        return (
            dest_root
            / sanitize_path_part(record.anio)
            / sanitize_path_part(record.cliente)
            / sanitize_path_part(sede)
            / sanitize_path_part(f"Presupuesto {record.presupuesto}")
            / categorize_document_type(src)
            / src.name
        )

    review_root = dest_root / sanitize_path_part(unmatched_dir)
    relative_parent = _relative_parent(src, source_root)

    if match_result.status == STATUS_SIN_NUMERO:
        return _append_relative(review_root / "_SIN_NUMERO_PRESUPUESTO", relative_parent) / src.name
    if match_result.status == STATUS_NO_ENCONTRADO:
        year = sanitize_path_part(match_result.year or "DESCONOCIDO")
        return _append_relative(review_root / "_NO_ENCONTRADO_EN_EXCEL" / year, relative_parent) / src.name
    return _append_relative(review_root / "_AMBIGUO", relative_parent) / src.name


def _cell_to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _strip_accents(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def _path_source_texts(path: Path) -> list[tuple[str, str, int]]:
    items: list[tuple[str, str, int]] = []
    parent_name = path.parent.name
    if parent_name:
        items.append((parent_name, "parent_folder", 0))
    if path.stem:
        items.append((path.stem, "filename", 1))
    for parent in path.parents[1:5]:
        if parent.name:
            items.append((parent.name, "ancestor_folder", 2))
    tail_parts = path.parts[-7:] if len(path.parts) > 7 else path.parts
    if tail_parts:
        items.append((" ".join(tail_parts), "relative_path", 3))
    items.append((str(path), "full_path", 4))
    return items


def _match_reason_for_budget(raw_text: str, budget: str) -> str | None:
    normalized = normalize_text(raw_text)
    compact = normalized.replace(" ", "")
    budget_norm = normalize_text(budget)
    budget_compact = budget_norm.replace(" ", "")

    if normalized == budget_norm:
        return "OK_EXACTO"
    if _is_recomposed_match(normalized, budget_compact):
        return "OK_RECOMPUESTO"
    if budget_compact and budget_compact in compact and compact != budget_compact:
        return "OK_COMPACTO"
    if budget_norm in normalized.split():
        return "OK_NORMALIZADO"
    if compact == budget_compact and normalized != budget_norm:
        return "OK_RECOMPUESTO"
    return None


def _is_recomposed_match(normalized_text: str, budget_compact: str) -> bool:
    numbers = _NUMBER_RE.findall(normalized_text)
    if len(numbers) < 2:
        return False
    for start in range(len(numbers)):
        combined = ""
        for number in numbers[start:]:
            combined += number
            if combined == budget_compact:
                return True
            if len(combined) >= len(budget_compact):
                break
    return False


def _confidence_for(reason: str, source_rank: int) -> float:
    base = {
        "OK_EXACTO": 0.99,
        "OK_NORMALIZADO": 0.94,
        "OK_COMPACTO": 0.90,
        "OK_RECOMPUESTO": 0.88,
    }[reason]
    return max(0.10, base - (source_rank * 0.05))


def _unknown_budget_numbers(path: Path, mapping_index: FactusolMappingIndex) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    allowed_years = mapping_index.years
    for raw_text, _, _ in _path_source_texts(path):
        normalized = normalize_text(raw_text)
        numbers = _NUMBER_RE.findall(normalized)
        for number in numbers:
            if len(number) >= 5 and number not in mapping_index.budgets and number not in allowed_years:
                if number not in seen:
                    seen.add(number)
                    found.append(number)
        for recomposed in _recomposed_numbers(numbers):
            if recomposed not in mapping_index.budgets and recomposed not in allowed_years:
                if recomposed not in seen:
                    seen.add(recomposed)
                    found.append(recomposed)
    return found


def _recomposed_numbers(numbers: Sequence[str]) -> Iterable[str]:
    for start in range(len(numbers)):
        combined = ""
        for number in numbers[start:]:
            combined += number
            if len(combined) >= 5:
                yield combined
            if len(combined) >= 8:
                break


def _records_for_candidate(
    budget: str,
    mapping_index: FactusolMappingIndex,
    detected_year: str | None,
    allowed_years: set[str],
) -> list[FactusolBudgetRecord]:
    records = list(mapping_index.records_by_budget.get(budget, []))
    if allowed_years:
        records = [record for record in records if record.anio in allowed_years]
    if detected_year:
        records = [record for record in records if record.anio == detected_year]
    return records


def _resolve_duplicate_record(
    path: Path,
    records: Sequence[FactusolBudgetRecord],
) -> tuple[FactusolBudgetRecord, float, float] | None:
    path_norm = normalize_text(str(path))
    path_compact = path_norm.replace(" ", "")
    scored = sorted(
        ((record, _score_record(record, path_norm, path_compact)) for record in records),
        key=lambda item: item[1],
        reverse=True,
    )
    if not scored or scored[0][1] < 2.0:
        return None
    if len(scored) > 1:
        margin = scored[0][1] - scored[1][1]
        if margin < 1.5:
            return None
    else:
        margin = scored[0][1]
    return scored[0][0], scored[0][1], margin


def _score_record(record: FactusolBudgetRecord, path_norm: str, path_compact: str) -> float:
    weighted_fields = [
        (record.sede_hotel_direccion, 4.0),
        (record.referencia, 4.0),
        (record.cliente, 2.0),
    ]
    score = 0.0
    path_tokens = set(_meaningful_tokens(path_norm))
    for value, weight in weighted_fields:
        normalized = normalize_text(value)
        compact = normalized.replace(" ", "")
        if not normalized:
            continue
        if normalized in path_norm:
            score += weight
            continue
        if compact and compact in path_compact:
            score += weight * 0.9
            continue
        tokens = set(_meaningful_tokens(normalized))
        if tokens:
            overlap = len(tokens & path_tokens) / len(tokens)
            score += weight * overlap * 0.6
    return score


def _meaningful_tokens(value: str) -> list[str]:
    return [
        token
        for token in normalize_text(value).split()
        if len(token) > 2 and not token.isdigit()
    ]


def _ok_result(
    status: str,
    record: FactusolBudgetRecord,
    candidate: BudgetCandidate,
    mapping_index: FactusolMappingIndex,
    confidence: float,
    reason: str | None = None,
) -> MatchResult:
    return MatchResult(
        status=status,
        record=record,
        candidate=candidate,
        year=record.anio,
        presupuesto_detectado=record.presupuesto,
        cliente=record.cliente,
        sede_hotel_direccion=record.sede_hotel_direccion or record.cliente,
        referencia=record.referencia,
        origen_asignacion=record.origen_asignacion,
        clave_interna=record.clave_interna,
        confidence=confidence,
        source_level=candidate.source_level,
        reason=reason or candidate.reason,
        texto_detectado=candidate.raw_text,
        duplicado_anio_presupuesto="SI" if mapping_index.is_duplicate(record) else "NO",
    )


def _relative_parent(src: Path, source_root: Path) -> tuple[str, ...]:
    try:
        relative = src.relative_to(source_root)
    except ValueError:
        relative = Path(src.name)
    return tuple(sanitize_path_part(part) for part in relative.parts[:-1])


def _append_relative(base: Path, parts: Sequence[str]) -> Path:
    result = base
    for part in parts:
        result = result / part
    return result
