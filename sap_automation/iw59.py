from __future__ import annotations

import csv
import calendar
import importlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .contracts import ObjectManifest
from .sap_helpers import (
    resolve_first_existing,
    resolve_first_existing_with_id,
    set_first_existing_text,
    set_text as _sap_set_text,
    wait_for_file,
    wait_not_busy as _sap_wait_not_busy,
)


_IW59_SOURCE_COLUMN_SCHEMAS: dict[str, dict[str, tuple[str, ...]]] = {
    "CA": {
        "note": ("nota",),
        "status": ("statusuar",),
        "texto_code_parte_obj": ("texto_code_parte_obj",),
        "ptob": ("ptob",),
    },
    "RL": {
        "note": ("nota",),
        "status": ("statusuar",),
        "texto_code_parte_obj": ("txt_cod_part_ob", "texto_code_parte_obj"),
        "ptob": ("ptob",),
    },
    "WB": {
        "note": ("nota",),
        "status": ("statusuar",),
        "texto_code_parte_obj": ("texto_code_parte_obj", "texto"),
        "ptob": ("ptob", "cdd"),
    },
}
_IW59_NOTE_FIELD_CANDIDATES: tuple[str, ...] = (
    "nota",
    "notification",
    "notification_number",
    "numero_nota",
    "número_nota",
    "qmnum",
)
_IW59_STATUS_FIELD_CANDIDATES: tuple[str, ...] = (
    "statusuar",
    "status_usuario",
    "status user",
    "user_status",
    "status_sistema_usuario",
)
_IW59_BRS_FIELD_CANDIDATES: tuple[str, ...] = (
    "brs",
    "br",
    "modified_by",
    "modificado_por",
)


def _resolve_iw59_field(fieldnames: list[str], candidates: tuple[str, ...]) -> str:
    normalized = {
        str(item or "").strip().casefold(): str(item or "").strip()
        for item in fieldnames
        if str(item or "").strip()
    }
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return ""


def collect_iw59_notes_from_ca_csv(
    csv_path: Path,
    *,
    allowed_status_values: set[str] | None = None,
    source_object_code: str = "",
) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = [str(item or "").strip() for item in (reader.fieldnames or [])]

    source_schema = _IW59_SOURCE_COLUMN_SCHEMAS.get(str(source_object_code).strip().upper(), {})
    note_field = _resolve_iw59_field(
        fieldnames,
        source_schema.get("note", _IW59_NOTE_FIELD_CANDIDATES),
    ) or _resolve_iw59_field(fieldnames, _IW59_NOTE_FIELD_CANDIDATES)
    status_field = _resolve_iw59_field(
        fieldnames,
        source_schema.get("status", _IW59_STATUS_FIELD_CANDIDATES),
    ) or _resolve_iw59_field(fieldnames, _IW59_STATUS_FIELD_CANDIDATES)
    if not note_field or not status_field:
        return []

    normalized_status_filter = {
        str(item).strip().upper() for item in (allowed_status_values or set()) if str(item).strip()
    }
    notes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        note = str(row.get(note_field, "") or "").strip()
        status_value = str(row.get(status_field, "") or "").strip()
        if (
            not note
            or not status_value
            or note in seen
            or (
                normalized_status_filter
                and status_value.strip().upper() not in normalized_status_filter
            )
        ):
            continue
        seen.add(note)
        notes.append(note)
    return notes


def chunk_iw59_notes(notes: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    return [notes[index : index + chunk_size] for index in range(0, len(notes), chunk_size)]


def partition_iw59_values(values: list[str], partition_count: int) -> list[list[str]]:
    if partition_count <= 0:
        raise ValueError("partition_count must be greater than zero.")
    if not values:
        return []
    base_size, remainder = divmod(len(values), partition_count)
    partitions: list[list[str]] = []
    start = 0
    for index in range(partition_count):
        current_size = base_size + (1 if index < remainder else 0)
        if current_size <= 0:
            continue
        end = start + current_size
        partitions.append(values[start:end])
        start = end
    return [partition for partition in partitions if partition]


def collect_iw59_brs_from_csv(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = [str(item or "").strip() for item in (reader.fieldnames or [])]

    brs_field = _resolve_iw59_field(fieldnames, _IW59_BRS_FIELD_CANDIDATES)
    if not brs_field:
        return []

    brs_values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        br = str(row.get(brs_field, "") or "").strip().upper()
        if not br or br in seen:
            continue
        seen.add(br)
        brs_values.append(br)
    return brs_values


def concatenate_text_exports(source_paths: list[Path], destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as destination:
        for index, source_path in enumerate(source_paths):
            text = source_path.read_text(encoding="utf-8", errors="ignore")
            if index > 0 and text and not text.startswith("\n"):
                destination.write("\n")
            destination.write(text)


def build_ca_note_enrichment_map(csv_path: Path, *, source_object_code: str = "") -> dict[str, dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [str(item or "").strip() for item in (reader.fieldnames or [])]
        source_schema = _IW59_SOURCE_COLUMN_SCHEMAS.get(str(source_object_code).strip().upper(), {})
        note_field = _resolve_iw59_field(
            fieldnames,
            source_schema.get("note", _IW59_NOTE_FIELD_CANDIDATES),
        ) or _resolve_iw59_field(fieldnames, _IW59_NOTE_FIELD_CANDIDATES)
        if not note_field:
            return {}
        enrichment_fields = {
            "texto_code_parte_obj": _resolve_iw59_field(
                fieldnames,
                source_schema.get("texto_code_parte_obj", ("texto_code_parte_obj",)),
            ),
            "ptob": _resolve_iw59_field(
                fieldnames,
                source_schema.get("ptob", ("ptob",)),
            ),
        }

        mapping: dict[str, dict[str, str]] = {}
        for row in reader:
            note = str(row.get(note_field, "") or "").strip()
            if not note or note in mapping:
                continue
            payload = {
                field_name: str(row.get(source_field, "") or "").strip()
                for field_name, source_field in enrichment_fields.items()
                if source_field
            }
            if any(payload.values()):
                mapping[note] = payload
        return mapping


def concatenate_delimited_exports(
    source_paths: list[Path],
    destination_path: Path,
    *,
    ca_note_enrichment_map: dict[str, dict[str, str]] | None = None,
) -> int:
    compat = importlib.import_module("sap_gui_export_compat")
    source_header: list[str] | None = None
    output_header: list[str] | None = None
    rows_written = 0
    enrichment_map = ca_note_enrichment_map or {}
    enrichment_fields = ["texto_code_parte_obj", "ptob"]
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for source_path in source_paths:
            _, delimiter, rows = compat._read_delimited_text(source_path)
            if not rows:
                continue
            current_header = [str(item).strip() for item in rows[0]]
            normalized_current_header = [item.casefold() for item in current_header]
            note_index = normalized_current_header.index("nota") if "nota" in normalized_current_header else -1
            enrichment_indexes = {
                field_name: normalized_current_header.index(field_name)
                for field_name in enrichment_fields
                if field_name in normalized_current_header
            }

            if source_header is None:
                source_header = current_header
                output_header = list(current_header)
                if enrichment_map:
                    normalized_output_header = [item.casefold() for item in output_header]
                    for field_name in enrichment_fields:
                        if field_name not in normalized_output_header:
                            output_header.append(field_name)
                writer.writerow(output_header)

            data_rows = rows[1:] if current_header == source_header else rows
            for row in data_rows:
                output_row = list(row)
                note = (
                    str(output_row[note_index]).strip()
                    if note_index >= 0 and note_index < len(output_row)
                    else ""
                )
                enrichment_values = enrichment_map.get(note, {})
                normalized_output_header = [item.casefold() for item in (output_header or [])]
                for field_name in enrichment_fields:
                    if field_name not in normalized_output_header:
                        continue
                    output_index = normalized_output_header.index(field_name)
                    while len(output_row) <= output_index:
                        output_row.append("")
                    field_value = enrichment_values.get(field_name, "")
                    if field_name in enrichment_indexes:
                        if field_value:
                            output_row[output_index] = field_value
                    else:
                        output_row[output_index] = field_value
                writer.writerow(output_row)
                rows_written += 1
    return rows_written


def compute_iw59_manu_modified_date_range(today: date | None = None) -> tuple[str, str]:
    return compute_iw59_modified_date_range(today=today)


def compute_easter_sunday(year: int) -> date:
    if year <= 0:
        raise ValueError("year must be greater than zero.")
    century = year // 100
    year_of_century = year % 100
    leap_correction = century // 4
    century_remainder = century % 4
    moon_correction = (century + 8) // 25
    epact_correction = (century - moon_correction + 1) // 3
    golden_number = (19 * (year % 19) + century - leap_correction - epact_correction + 15) % 30
    year_correction = year_of_century // 4
    year_remainder = year_of_century % 4
    weekday_correction = (32 + 2 * century_remainder + 2 * year_correction - golden_number - year_remainder) % 7
    month_factor = (year % 19 + 11 * golden_number + 22 * weekday_correction) // 451
    month = (golden_number + weekday_correction - 7 * month_factor + 114) // 31
    day = ((golden_number + weekday_correction - 7 * month_factor + 114) % 31) + 1
    return date(year, month, day)


def build_brazil_business_holidays(year: int) -> set[date]:
    easter = compute_easter_sunday(year)
    fixed_holidays = {
        date(year, 1, 1),
        date(year, 4, 21),
        date(year, 5, 1),
        date(year, 9, 7),
        date(year, 10, 12),
        date(year, 11, 2),
        date(year, 11, 15),
        date(year, 12, 25),
    }
    if year >= 2024:
        fixed_holidays.add(date(year, 11, 20))
    movable_observances = {
        easter - timedelta(days=48),  # Carnival Monday
        easter - timedelta(days=47),  # Carnival Tuesday
        easter - timedelta(days=2),   # Good Friday
        easter + timedelta(days=60),  # Corpus Christi
    }
    return fixed_holidays | movable_observances


def is_business_day(day: date, *, holidays: set[date] | None = None) -> bool:
    return day.weekday() < 5 and day not in (holidays or set())


def compute_nth_business_day_of_month(
    *,
    current_day: date,
    nth: int,
    holidays: set[date] | None = None,
) -> date:
    if nth <= 0:
        raise ValueError("nth must be greater than zero.")
    day = current_day.replace(day=1)
    business_day_count = 0
    while day.month == current_day.month:
        if is_business_day(day, holidays=holidays):
            business_day_count += 1
            if business_day_count == nth:
                return day
        day += timedelta(days=1)
    raise ValueError(f"Could not resolve the {nth}th business day for month={current_day.month}.")


def compute_iw59_modified_date_range(
    today: date | None = None,
    *,
    transition_business_day: int = 5,
    holidays: set[date] | None = None,
) -> tuple[str, str]:
    if transition_business_day <= 0:
        raise ValueError("transition_business_day must be greater than zero.")
    current_day = today or date.today()
    resolved_holidays = holidays or build_brazil_business_holidays(current_day.year)
    month_start = current_day.replace(day=1)
    transition_day = compute_nth_business_day_of_month(
        current_day=current_day,
        nth=transition_business_day,
        holidays=resolved_holidays,
    )
    if current_day <= transition_day:
        previous_month_end = month_start - timedelta(days=1)
        previous_month_start = previous_month_end.replace(day=1)
        return previous_month_start.strftime("%d.%m.%Y"), previous_month_end.strftime("%d.%m.%Y")
    return month_start.strftime("%d.%m.%Y"), current_day.strftime("%d.%m.%Y")


def resolve_reference_month_start(
    today: date | None = None,
    *,
    transition_business_day: int = 5,
    holidays: set[date] | None = None,
) -> date:
    if transition_business_day <= 0:
        raise ValueError("transition_business_day must be greater than zero.")
    current_day = today or date.today()
    resolved_holidays = holidays or build_brazil_business_holidays(current_day.year)
    month_start = current_day.replace(day=1)
    transition_day = compute_nth_business_day_of_month(
        current_day=current_day,
        nth=transition_business_day,
        holidays=resolved_holidays,
    )
    if current_day <= transition_day:
        previous_month_end = month_start - timedelta(days=1)
        return previous_month_end.replace(day=1)
    return month_start


def compute_kelly_modified_date_ranges(
    today: date | None = None,
    *,
    transition_business_day: int = 5,
    holidays: set[date] | None = None,
) -> list[tuple[str, str]]:
    reference_month_start = resolve_reference_month_start(
        today=today,
        transition_business_day=transition_business_day,
        holidays=holidays,
    )
    last_day = calendar.monthrange(reference_month_start.year, reference_month_start.month)[1]
    windows = (
        (1, 10),
        (11, 20),
        (21, last_day),
    )
    return [
        (
            reference_month_start.replace(day=start_day).strftime("%d.%m.%Y"),
            reference_month_start.replace(day=end_day).strftime("%d.%m.%Y"),
        )
        for start_day, end_day in windows
        if start_day <= end_day <= last_day
    ]


def resolve_iw59_modified_date_policy(
    *,
    iw59_cfg: dict[str, Any],
    demandante_cfg: dict[str, Any],
) -> tuple[bool, int]:
    use_modified_date_range = bool(
        demandante_cfg.get(
            "use_modified_date_range",
            iw59_cfg.get("use_modified_date_range", True),
        )
    )
    transition_business_day = int(
        demandante_cfg.get(
            "transition_business_day",
            iw59_cfg.get("transition_business_day", 5),
        )
    )
    if transition_business_day <= 0:
        raise ValueError("IW59 transition_business_day must be greater than zero.")
    return use_modified_date_range, transition_business_day


@dataclass(frozen=True)
class Iw59ExportResult:
    status: str
    source_object_code: str = ""
    reason: str = ""
    total_notes: int = 0
    chunk_size: int = 0
    chunk_count: int = 0
    chunk_files: list[str] = field(default_factory=list)
    combined_txt_path: str = ""
    combined_csv_path: str = ""
    metadata_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Iw59BatchResult:
    status: str
    run_id: str
    reference: str
    demandante: str
    results: list[dict[str, Any]] = field(default_factory=list)
    metadata_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Iw59ExportAdapter:
    def skip(self, *, reason: str, source_object_code: str = "") -> Iw59ExportResult:
        return Iw59ExportResult(
            status="skipped",
            source_object_code=str(source_object_code).strip().upper(),
            reason=reason,
        )

    def execute(
        self,
        *,
        output_root: Path,
        run_id: str,
        reference: str,
        demandante: str,
        source_manifest: ObjectManifest,
        session: Any,
        logger: Any,
        config: dict[str, Any],
    ) -> Iw59ExportResult:
        source_object_code = str(source_manifest.object_code).strip().upper()
        source_slug = source_object_code.casefold()
        source_path = Path(source_manifest.canonical_csv_path)
        if not source_path.exists():
            return self.skip(
                reason=f"IW59 skipped because {source_object_code} canonical CSV is missing.",
                source_object_code=source_object_code,
            )

        iw59_cfg = config.get("iw59", {})
        if not bool(iw59_cfg.get("enabled", True)):
            return self.skip(
                reason="IW59 skipped because iw59.enabled=false.",
                source_object_code=source_object_code,
            )

        transaction_code = str(iw59_cfg.get("transaction_code", "IW59")).strip() or "IW59"
        multi_select_button_id = str(
            iw59_cfg.get("multi_select_button_id", "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH")
        ).strip()
        back_button_id = str(iw59_cfg.get("back_button_id", "wnd[0]/tbar[0]/btn[3]")).strip()
        wait_timeout_seconds = float(config.get("global", {}).get("wait_timeout_seconds", 60.0))
        unwind_min_presses = int(iw59_cfg.get("pre_iw59_unwind_min_presses", 3))
        unwind_max_presses = int(iw59_cfg.get("pre_iw59_unwind_max_presses", 8))
        demandante_name = str(demandante or "").strip().upper() or "IGOR"
        demandante_cfg = (
            iw59_cfg.get("demandantes", {}).get(demandante_name, {})
            if isinstance(iw59_cfg.get("demandantes", {}), dict)
            else {}
        )
        chunk_size = int(demandante_cfg.get("chunk_size", iw59_cfg.get("chunk_size", 20000)))
        allowed_status_values = {
            str(item).strip().upper()
            for item in demandante_cfg.get("allowed_status_values", [])
            if str(item).strip()
        }
        notes = collect_iw59_notes_from_ca_csv(
            source_path,
            allowed_status_values=allowed_status_values or None,
            source_object_code=source_object_code,
        )
        if not notes:
            if allowed_status_values:
                return self.skip(
                    reason=(
                        f"IW59 skipped because {source_object_code} did not produce notes matching statusuar filter: "
                        + ", ".join(sorted(allowed_status_values))
                    ),
                    source_object_code=source_object_code,
                )
            return self.skip(
                reason=f"IW59 skipped because {source_object_code} did not produce notes with statusuar.",
                source_object_code=source_object_code,
            )

        base_dir = output_root.expanduser().resolve() / "runs" / run_id / "iw59"
        raw_dir = base_dir / "raw"
        normalized_dir = base_dir / "normalized"
        metadata_dir = base_dir / "metadata"
        raw_dir.mkdir(parents=True, exist_ok=True)
        normalized_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        chunks = chunk_iw59_notes(notes, chunk_size)
        logger.info(
            "Starting IW59 extraction run_id=%s source_object=%s total_notes=%s chunk_size=%s chunk_count=%s status_filter=%s",
            run_id,
            source_object_code,
            len(notes),
            chunk_size,
            len(chunks),
            sorted(allowed_status_values),
        )

        chunk_paths: list[Path] = []
        for chunk_index, chunk_notes in enumerate(chunks, start=1):
            chunk_path = raw_dir / f"iw59_{source_slug}_{reference}_{run_id}_{chunk_index:03d}.txt"
            logger.info(
                "Running IW59 chunk source_object=%s index=%s/%s notes=%s output=%s",
                source_object_code,
                chunk_index,
                len(chunks),
                len(chunk_notes),
                chunk_path,
            )
            self._run_chunk(
                session=session,
                notes=chunk_notes,
                output_path=chunk_path,
                logger=logger,
                demandante=demandante_name,
                iw59_cfg=iw59_cfg,
                demandante_cfg=demandante_cfg,
                transaction_code=transaction_code,
                multi_select_button_id=multi_select_button_id,
                back_button_id=back_button_id,
                wait_timeout_seconds=wait_timeout_seconds,
                unwind_min_presses=unwind_min_presses,
                unwind_max_presses=unwind_max_presses,
            )
            chunk_paths.append(chunk_path)

        combined_txt_path = raw_dir / f"iw59_{source_slug}_{reference}_{run_id}_combined.txt"
        combined_csv_path = normalized_dir / f"iw59_{source_slug}_{reference}_{run_id}.csv"
        metadata_path = metadata_dir / f"iw59_{source_slug}_{reference}_{run_id}.manifest.json"
        source_note_enrichment_map = build_ca_note_enrichment_map(
            source_path,
            source_object_code=source_object_code,
        )

        concatenate_text_exports(chunk_paths, combined_txt_path)
        rows_written = concatenate_delimited_exports(
            chunk_paths,
            combined_csv_path,
            ca_note_enrichment_map=source_note_enrichment_map,
        )
        metadata = {
            "run_id": run_id,
            "reference": reference,
            "demandante": demandante_name,
            "source_object_code": source_object_code,
            "status": "success",
            "total_notes": len(notes),
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "chunk_files": [str(item) for item in chunk_paths],
            "combined_txt_path": str(combined_txt_path),
            "combined_csv_path": str(combined_csv_path),
            "rows_written": rows_written,
            "source_enrichment_mapped_notes": len(source_note_enrichment_map),
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "IW59 extraction finished source_object=%s total_notes=%s chunk_count=%s combined_txt=%s combined_csv=%s",
            source_object_code,
            len(notes),
            len(chunks),
            combined_txt_path,
            combined_csv_path,
        )
        return Iw59ExportResult(
            status="success",
            source_object_code=source_object_code,
            total_notes=len(notes),
            chunk_size=chunk_size,
            chunk_count=len(chunks),
            chunk_files=[str(item) for item in chunk_paths],
            combined_txt_path=str(combined_txt_path),
            combined_csv_path=str(combined_csv_path),
            metadata_path=str(metadata_path),
        )

    def execute_modified_by_brs(
        self,
        *,
        output_root: Path,
        run_id: str,
        demandante: str,
        session: Any,
        logger: Any,
        config: dict[str, Any],
        input_csv_path: Path | None = None,
    ) -> Iw59ExportResult:
        iw59_cfg = config.get("iw59", {})
        if not bool(iw59_cfg.get("enabled", True)):
            return self.skip(reason="IW59 skipped because iw59.enabled=false.", source_object_code="KELLY")

        demandante_name = str(demandante or "").strip().upper() or "KELLY"
        demandante_cfg = (
            iw59_cfg.get("demandantes", {}).get(demandante_name, {})
            if isinstance(iw59_cfg.get("demandantes", {}), dict)
            else {}
        )
        transaction_code = str(iw59_cfg.get("transaction_code", "IW59")).strip() or "IW59"
        multi_select_button_id = str(
            demandante_cfg.get(
                "multi_select_button_id",
                iw59_cfg.get("multi_select_button_id", "wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH"),
            )
        ).strip()
        selection_entry_field_id = str(
            demandante_cfg.get("selection_entry_field_id", "wnd[0]/usr/ctxtAENAM-LOW")
        ).strip()
        selection_summary_ids = list(
            demandante_cfg.get(
                "selection_summary_ids",
                [
                    "wnd[0]/usr/txt%_AENAM_%_APP_%-TEXT",
                    "wnd[0]/usr/txt%_AENAM_%_APP_%-TO_TEXT",
                ],
            )
        )
        back_button_id = str(iw59_cfg.get("back_button_id", "wnd[0]/tbar[0]/btn[3]")).strip()
        wait_timeout_seconds = float(config.get("global", {}).get("wait_timeout_seconds", 60.0))
        unwind_min_presses = int(iw59_cfg.get("pre_iw59_unwind_min_presses", 3))
        unwind_max_presses = int(iw59_cfg.get("pre_iw59_unwind_max_presses", 8))
        chunk_size = int(demandante_cfg.get("chunk_size", 100))
        transition_business_day = int(
            demandante_cfg.get("transition_business_day", iw59_cfg.get("transition_business_day", 5))
        )
        if transition_business_day <= 0:
            raise ValueError("IW59 KELLY transition_business_day must be greater than zero.")

        configured_input_path = str(demandante_cfg.get("input_csv_path", "")).strip()
        resolved_input_path = (input_csv_path or Path(configured_input_path or "brs_filtrados.csv")).expanduser().resolve()
        if not resolved_input_path.exists():
            raise FileNotFoundError(f"IW59 KELLY input CSV not found: {resolved_input_path}")

        brs_values = collect_iw59_brs_from_csv(resolved_input_path)
        if not brs_values:
            return self.skip(
                reason=f"IW59 skipped because KELLY input CSV did not produce BRS values: {resolved_input_path}",
                source_object_code="KELLY",
            )

        base_dir = output_root.expanduser().resolve() / "runs" / run_id / "iw59"
        raw_dir = base_dir / "raw"
        normalized_dir = base_dir / "normalized"
        metadata_dir = base_dir / "metadata"
        raw_dir.mkdir(parents=True, exist_ok=True)
        normalized_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        chunk_groups = chunk_iw59_notes(brs_values, chunk_size)
        holiday_years = {current_year for current_year in range(date.today().year - 1, 2041)}
        holidays: set[date] = set()
        for current_year in holiday_years:
            holidays.update(build_brazil_business_holidays(current_year))
        modified_date_ranges = compute_kelly_modified_date_ranges(
            transition_business_day=transition_business_day,
            holidays=holidays,
        )
        reference_month_start = resolve_reference_month_start(
            transition_business_day=transition_business_day,
            holidays=holidays,
        )
        reference_label = reference_month_start.strftime("%Y%m")
        logger.info(
            "Starting IW59 KELLY extraction run_id=%s total_brs=%s chunk_size=%s chunk_count=%s modified_windows=%s reference=%s input=%s",
            run_id,
            len(brs_values),
            chunk_size,
            len(chunk_groups),
            len(modified_date_ranges),
            reference_label,
            resolved_input_path,
        )

        chunk_paths: list[Path] = []
        total_requests = 0
        for window_index, (modified_from, modified_to) in enumerate(modified_date_ranges, start=1):
            logger.info(
                "IW59 KELLY window start index=%s/%s from=%s to=%s chunk_count=%s",
                window_index,
                len(modified_date_ranges),
                modified_from,
                modified_to,
                len(chunk_groups),
            )
            for chunk_index, chunk_brs in enumerate(chunk_groups, start=1):
                total_requests += 1
                chunk_path = raw_dir / (
                    f"iw59_kelly_{reference_label}_{run_id}_w{window_index:02d}_{chunk_index:03d}.txt"
                )
                logger.info(
                    "Running IW59 KELLY chunk window=%s/%s index=%s/%s brs=%s from=%s to=%s output=%s",
                    window_index,
                    len(modified_date_ranges),
                    chunk_index,
                    len(chunk_groups),
                    len(chunk_brs),
                    modified_from,
                    modified_to,
                    chunk_path,
                )
                self._run_chunk_modified_by(
                    session=session,
                    brs_values=chunk_brs,
                    output_path=chunk_path,
                    logger=logger,
                    demandante=demandante_name,
                    iw59_cfg=iw59_cfg,
                    demandante_cfg=demandante_cfg,
                    transaction_code=transaction_code,
                    multi_select_button_id=multi_select_button_id,
                    selection_entry_field_id=selection_entry_field_id,
                    selection_summary_ids=selection_summary_ids,
                    back_button_id=back_button_id,
                    wait_timeout_seconds=wait_timeout_seconds,
                    unwind_min_presses=unwind_min_presses,
                    unwind_max_presses=unwind_max_presses,
                    modified_from=modified_from,
                    modified_to=modified_to,
                )
                chunk_paths.append(chunk_path)

        combined_txt_path = raw_dir / f"iw59_kelly_{reference_label}_{run_id}_combined.txt"
        combined_csv_path = normalized_dir / f"iw59_kelly_{reference_label}_{run_id}.csv"
        metadata_path = metadata_dir / f"iw59_kelly_{reference_label}_{run_id}.manifest.json"
        concatenate_text_exports(chunk_paths, combined_txt_path)
        rows_written = concatenate_delimited_exports(chunk_paths, combined_csv_path)
        metadata = {
            "run_id": run_id,
            "reference": reference_label,
            "demandante": demandante_name,
            "source_object_code": "KELLY",
            "selection_mode": "modified_by",
            "status": "success",
            "input_csv_path": str(resolved_input_path),
            "total_brs": len(brs_values),
            "chunk_size": chunk_size,
            "chunk_count": len(chunk_groups),
            "modified_date_range_count": len(modified_date_ranges),
            "modified_date_ranges": [
                {"index": index, "from": item[0], "to": item[1]}
                for index, item in enumerate(modified_date_ranges, start=1)
            ],
            "request_count": total_requests,
            "chunk_files": [str(item) for item in chunk_paths],
            "combined_txt_path": str(combined_txt_path),
            "combined_csv_path": str(combined_csv_path),
            "rows_written": rows_written,
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "IW59 KELLY extraction finished total_brs=%s request_count=%s combined_csv=%s",
            len(brs_values),
            total_requests,
            combined_csv_path,
        )
        return Iw59ExportResult(
            status="success",
            source_object_code="KELLY",
            total_notes=len(brs_values),
            chunk_size=chunk_size,
            chunk_count=total_requests,
            chunk_files=[str(item) for item in chunk_paths],
            combined_txt_path=str(combined_txt_path),
            combined_csv_path=str(combined_csv_path),
            metadata_path=str(metadata_path),
        )

    def _run_chunk(
        self,
        *,
        session: Any,
        notes: list[str],
        output_path: Path,
        logger: Any,
        demandante: str,
        iw59_cfg: dict[str, Any],
        demandante_cfg: dict[str, Any],
        transaction_code: str,
        multi_select_button_id: str,
        back_button_id: str,
        wait_timeout_seconds: float,
        unwind_min_presses: int,
        unwind_max_presses: int,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        self._unwind_before_iw59(
            session=session,
            back_button_id=back_button_id,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
            min_presses=unwind_min_presses,
            max_presses=unwind_max_presses,
        )
        logger.info("IW59 entering transaction transaction=%s", transaction_code.upper())
        main_window = session.findById("wnd[0]")
        main_window.maximize()
        okcd = session.findById("wnd[0]/tbar[0]/okcd")
        okcd.text = transaction_code.lower()
        main_window.sendVKey(0)
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._prepare_selection_filters(
            session=session,
            logger=logger,
            demandante=demandante,
            iw59_cfg=iw59_cfg,
            demandante_cfg=demandante_cfg,
        )

        logger.info("IW59 opening notification multiselect notes=%s", len(notes))
        session.findById(multi_select_button_id).press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        paste_succeeded = self._paste_notes_via_clipboard(
            session=session,
            notes=notes,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        if not paste_succeeded:
            logger.warning(
                "IW59 clipboard paste failed or incomplete, falling back to file upload notes=%s",
                len(notes),
            )
            self._paste_notes_via_file_upload(
                session=session,
                notes=notes,
                logger=logger,
                output_path=output_path,
                wait_timeout_seconds=wait_timeout_seconds,
            )

        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        session.findById("wnd[1]/tbar[0]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        logger.info(
            "IW59 note multiselect applied notes=%s summary=%s",
            len(notes),
            self._note_selection_summary(session),
        )

        logger.info("IW59 executing selection for chunk output=%s", output_path)
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._ensure_results_screen_ready(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        self._open_export_dialog(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        local_file_radio = session.findById(
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]"
        )
        local_file_radio.select()
        local_file_radio.setFocus()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_PATH",
                "wnd[2]/usr/ctxtDY_PATH",
                "wnd[1]/usr/txtDY_PATH",
                "wnd[2]/usr/txtDY_PATH",
            ],
            value=str(output_path.parent),
        )
        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_FILENAME",
                "wnd[2]/usr/ctxtDY_FILENAME",
                "wnd[1]/usr/txtDY_FILENAME",
                "wnd[2]/usr/txtDY_FILENAME",
            ],
            value=output_path.name,
        )
        self._resolve_first_existing(
            session=session,
            ids=[
                "wnd[1]/tbar[0]/btn[0]",
                "wnd[2]/tbar[0]/btn[0]",
            ],
        ).press()
        self._wait_for_file(output_path=output_path, timeout_seconds=wait_timeout_seconds)
        logger.info("IW59 chunk exported output=%s notes=%s", output_path, len(notes))

    def _run_chunk_modified_by(
        self,
        *,
        session: Any,
        brs_values: list[str],
        output_path: Path,
        logger: Any,
        demandante: str,
        iw59_cfg: dict[str, Any],
        demandante_cfg: dict[str, Any],
        transaction_code: str,
        multi_select_button_id: str,
        selection_entry_field_id: str,
        selection_summary_ids: list[str],
        back_button_id: str,
        wait_timeout_seconds: float,
        unwind_min_presses: int,
        unwind_max_presses: int,
        modified_from: str,
        modified_to: str,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        self._unwind_before_iw59(
            session=session,
            back_button_id=back_button_id,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
            min_presses=unwind_min_presses,
            max_presses=unwind_max_presses,
        )
        logger.info("IW59 entering transaction transaction=%s", transaction_code.upper())
        main_window = session.findById("wnd[0]")
        main_window.maximize()
        okcd = session.findById("wnd[0]/tbar[0]/okcd")
        okcd.text = transaction_code.lower()
        main_window.sendVKey(0)
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._prepare_selection_filters(
            session=session,
            logger=logger,
            demandante=demandante,
            iw59_cfg=iw59_cfg,
            demandante_cfg=demandante_cfg,
            modified_date_range=(modified_from, modified_to),
        )

        selection_field = session.findById(selection_entry_field_id)
        selection_field.setFocus()
        selection_field.caretPosition = 0

        logger.info("IW59 opening modified-by multiselect brs=%s", len(brs_values))
        session.findById(multi_select_button_id).press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        paste_succeeded = self._paste_selection_values_via_clipboard(
            session=session,
            values=brs_values,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
            selection_label="BRS",
        )
        if not paste_succeeded:
            logger.warning(
                "IW59 clipboard paste failed or incomplete for BRS, falling back to file upload brs=%s",
                len(brs_values),
            )
            self._paste_selection_values_via_file_upload(
                session=session,
                values=brs_values,
                logger=logger,
                output_path=output_path,
                wait_timeout_seconds=wait_timeout_seconds,
                selection_label="BRS",
            )

        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        session.findById("wnd[1]/tbar[0]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        logger.info(
            "IW59 modified-by multiselect applied brs=%s summary=%s",
            len(brs_values),
            self._selection_summary(session, selection_summary_ids),
        )

        logger.info("IW59 executing selection for chunk output=%s", output_path)
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._ensure_results_screen_ready(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        self._open_export_dialog(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        local_file_radio = session.findById(
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]"
        )
        local_file_radio.select()
        local_file_radio.setFocus()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_PATH",
                "wnd[2]/usr/ctxtDY_PATH",
                "wnd[1]/usr/txtDY_PATH",
                "wnd[2]/usr/txtDY_PATH",
            ],
            value=str(output_path.parent),
        )
        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_FILENAME",
                "wnd[2]/usr/ctxtDY_FILENAME",
                "wnd[1]/usr/txtDY_FILENAME",
                "wnd[2]/usr/txtDY_FILENAME",
            ],
            value=output_path.name,
        )
        self._resolve_first_existing(
            session=session,
            ids=[
                "wnd[1]/tbar[0]/btn[0]",
                "wnd[2]/tbar[0]/btn[0]",
            ],
        ).press()
        self._wait_for_file(output_path=output_path, timeout_seconds=wait_timeout_seconds)
        logger.info("IW59 chunk exported output=%s brs=%s", output_path, len(brs_values))

    def _prepare_selection_filters(
        self,
        *,
        session: Any,
        logger: Any,
        demandante: str,
        iw59_cfg: dict[str, Any],
        demandante_cfg: dict[str, Any],
        modified_date_range: tuple[str, str] | None = None,
    ) -> None:
        if modified_date_range is not None:
            modified_from, modified_to = modified_date_range
            logger.info(
                "IW59 selection prep clearing note dates and applying explicit modified date range demandante=%s from=%s to=%s",
                str(demandante).strip().upper(),
                modified_from,
                modified_to,
            )
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUV", value="")
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUB", value="")
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-LOW", value=modified_from)
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-HIGH", value=modified_to)
            status_field = session.findById("wnd[0]/usr/ctxtSTAI1-LOW")
            status_field.setFocus()
            status_field.caretPosition = 0
            return

        use_modified_date_range, transition_business_day = resolve_iw59_modified_date_policy(
            iw59_cfg=iw59_cfg,
            demandante_cfg=demandante_cfg,
        )
        if not use_modified_date_range:
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUV", value="")
            self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUB", value="")
            return

        modified_from, modified_to = compute_iw59_modified_date_range(
            transition_business_day=transition_business_day,
        )
        logger.info(
            "IW59 selection prep clearing note dates and applying modified date range demandante=%s transition_business_day=%s from=%s to=%s",
            str(demandante).strip().upper(),
            transition_business_day,
            modified_from,
            modified_to,
        )
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUV", value="")
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUB", value="")
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-LOW", value=modified_from)
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-HIGH", value=modified_to)
        status_field = session.findById("wnd[0]/usr/ctxtSTAI1-LOW")
        status_field.setFocus()
        status_field.caretPosition = 0

    def _ensure_results_screen_ready(
        self,
        *,
        session: Any,
        logger: Any,
        wait_timeout_seconds: float,
    ) -> None:
        deadline = time.monotonic() + min(max(wait_timeout_seconds, 1.0), 8.0)
        last_status_bar = ""
        last_visible_controls: list[str] = []
        while time.monotonic() <= deadline:
            if self._has_export_surface(session):
                logger.info(
                    "IW59 execute reached exportable results screen status_bar=%s",
                    self._status_bar_text(session),
                )
                return
            last_status_bar = self._status_bar_text(session)
            last_visible_controls = self._visible_controls(session=session, limit=60)
            if self._is_selection_screen(session):
                time.sleep(0.25)
                continue
            time.sleep(0.25)

        status_bar = last_status_bar or self._status_bar_text(session)
        popup_title = self._safe_text(session, "wnd[1]")
        popup_status = self._safe_text(session, "wnd[1]/usr")
        details = [
            "IW59 execute did not leave selection screen.",
            f"status_bar={status_bar!r}",
            f"note_selection={self._note_selection_summary(session)!r}",
        ]
        if popup_title:
            details.append(f"popup_title={popup_title!r}")
        if popup_status:
            details.append(f"popup_text={popup_status!r}")
        if last_visible_controls:
            details.append(f"visible_controls={last_visible_controls}")
        raise RuntimeError(" ".join(details))

    def _has_export_surface(self, session: Any) -> bool:
        export_menu_ids = [
            "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
        ]
        return any(self._exists(session, item_id) for item_id in export_menu_ids) or any(
            self._exists(session, item_id) for item_id in self._candidate_export_shell_ids(session=session)
        )

    def _is_selection_screen(self, session: Any) -> bool:
        selection_markers = [
            "wnd[0]/usr/ctxtMONITOR",
            "wnd[0]/usr/ctxtVARIANT",
            "wnd[0]/usr/btn%_PAGESTAT_%_APP_%-VALU_PUSH",
            "wnd[0]/usr/ctxtPAGESTAT-LOW",
            "wnd[0]/usr/ctxtANLNR-LOW",
        ]
        return any(self._exists(session, item_id) for item_id in selection_markers)

    def _note_selection_summary(self, session: Any) -> str:
        return self._selection_summary(
            session,
            [
                "wnd[0]/usr/txt%_QMNUM_%_APP_%-TEXT",
                "wnd[0]/usr/txt%_QMNUM_%_APP_%-TO_TEXT",
            ],
        )

    def _selection_summary(self, session: Any, ids: list[str]) -> str:
        values = [self._safe_text(session, item_id) for item_id in ids]
        return " | ".join([item for item in values if item]) or ""

    def _paste_selection_values_via_clipboard(
        self,
        *,
        session: Any,
        values: list[str],
        logger: Any,
        wait_timeout_seconds: float,
        selection_label: str,
    ) -> bool:
        compat = importlib.import_module("sap_gui_export_compat")
        clipboard_count = self._copy_notes_to_clipboard(values, logger=logger)
        logger.info(
            "IW59 clipboard ready %s_requested=%s clipboard_verified=%s",
            selection_label.casefold(),
            len(values),
            clipboard_count,
        )
        if clipboard_count < len(values):
            return False
        time.sleep(0.2)
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        pasted_count = self._count_multiselect_entries(session)
        logger.info(
            "IW59 clipboard pasted into multiselect pasted_entries=%s expected=%s",
            pasted_count,
            len(values),
        )
        if pasted_count is not None and pasted_count < len(values):
            logger.warning(
                "IW59 clipboard multiselect mismatch pasted=%s expected=%s",
                pasted_count,
                len(values),
            )
            return False
        return True

    def _paste_selection_values_via_file_upload(
        self,
        *,
        session: Any,
        values: list[str],
        logger: Any,
        output_path: Path,
        wait_timeout_seconds: float,
        selection_label: str,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        upload_dir = output_path.parent
        upload_dir.mkdir(parents=True, exist_ok=True)
        upload_file = upload_dir / f"_iw59_multiselect_{output_path.stem}.txt"
        upload_file.write_text("\r\n".join(values) + "\r\n", encoding="utf-8")
        logger.info(
            "IW59 file upload fallback %s=%s file=%s",
            selection_label.casefold(),
            len(values),
            upload_file,
        )
        try:
            self._delete_multiselect_entries(session=session, logger=logger)
        except Exception:
            logger.debug("IW59 could not clear previous multiselect entries, continuing")
        session.findById("wnd[1]/tbar[0]/btn[23]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[2]/usr/ctxtDY_PATH",
                "wnd[2]/usr/txtDY_PATH",
                "wnd[1]/usr/ctxtDY_PATH",
                "wnd[1]/usr/txtDY_PATH",
            ],
            value=str(upload_dir),
        )
        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[2]/usr/ctxtDY_FILENAME",
                "wnd[2]/usr/txtDY_FILENAME",
                "wnd[1]/usr/ctxtDY_FILENAME",
                "wnd[1]/usr/txtDY_FILENAME",
            ],
            value=upload_file.name,
        )
        self._resolve_first_existing(
            session=session,
            ids=[
                "wnd[2]/tbar[0]/btn[0]",
                "wnd[1]/tbar[0]/btn[0]",
            ],
        ).press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        pasted_count = self._count_multiselect_entries(session)
        logger.info(
            "IW59 file upload completed pasted_entries=%s expected=%s file=%s",
            pasted_count,
            len(values),
            upload_file,
        )
        try:
            upload_file.unlink(missing_ok=True)
        except Exception:
            pass

    def _paste_notes_via_clipboard(
        self,
        *,
        session: Any,
        notes: list[str],
        logger: Any,
        wait_timeout_seconds: float,
    ) -> bool:
        return self._paste_selection_values_via_clipboard(
            session=session,
            values=notes,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
            selection_label="notes",
        )

    def _paste_notes_via_file_upload(
        self,
        *,
        session: Any,
        notes: list[str],
        logger: Any,
        output_path: Path,
        wait_timeout_seconds: float,
    ) -> None:
        self._paste_selection_values_via_file_upload(
            session=session,
            values=notes,
            logger=logger,
            output_path=output_path,
            wait_timeout_seconds=wait_timeout_seconds,
            selection_label="notes",
        )

    def _delete_multiselect_entries(self, *, session: Any, logger: Any) -> None:
        try:
            grid = session.findById(
                "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE"
            )
            if int(grid.RowCount) > 0:
                grid.SelectAllRows()
                session.findById("wnd[1]/tbar[0]/btn[2]").press()
        except Exception as exc:
            logger.debug("IW59 delete multiselect entries skipped reason=%s", exc)

    def _copy_notes_to_clipboard(self, notes: list[str], *, logger: Any, max_retries: int = 3) -> int:
        import win32clipboard  # type: ignore

        payload = "\r\n".join(notes)
        expected_count = len(notes)
        for attempt in range(1, max_retries + 1):
            win32clipboard.OpenClipboard()
            try:
                win32clipboard.EmptyClipboard()
                win32clipboard.SetClipboardText(payload, win32clipboard.CF_UNICODETEXT)
            finally:
                win32clipboard.CloseClipboard()
            time.sleep(0.15)
            verified_count = self._verify_clipboard_notes(expected_count)
            if verified_count == expected_count:
                logger.info(
                    "Clipboard verified attempt=%s expected=%s actual=%s",
                    attempt,
                    expected_count,
                    verified_count,
                )
                return verified_count
            logger.warning(
                "Clipboard mismatch attempt=%s expected=%s actual=%s retrying",
                attempt,
                expected_count,
                verified_count,
            )
            time.sleep(0.3)
        logger.error(
            "Clipboard verification failed after %s attempts expected=%s last_actual=%s proceeding anyway",
            max_retries,
            expected_count,
            verified_count,
        )
        return verified_count

    @staticmethod
    def _verify_clipboard_notes(expected_count: int) -> int:
        import win32clipboard  # type: ignore

        win32clipboard.OpenClipboard()
        try:
            raw = win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT) or ""
        except Exception:
            return 0
        finally:
            win32clipboard.CloseClipboard()
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        return len(lines)

    def _count_multiselect_entries(self, session: Any) -> int | None:
        try:
            grid = session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE")
            return int(grid.RowCount)
        except Exception:
            pass
        try:
            title_text = self._safe_text(session, "wnd[1]")
            if title_text:
                import re
                match = re.search(r"(\d+)\s*(?:Entries|Entr|entrad)", title_text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return None

    def _open_export_dialog(
        self,
        *,
        session: Any,
        logger: Any,
        wait_timeout_seconds: float,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        menu_ids = [
            "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
        ]
        try:
            _, export_item = self._resolve_first_existing_with_id(
                session=session,
                ids=menu_ids,
            )
        except RuntimeError as menu_error:
            logger.info(
                "IW59 export menu unavailable, trying ALV toolbar fallback error=%s",
                menu_error,
            )
        else:
            export_item.select()
            compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
            logger.info("IW59 export opened via menu")
            return

        shell_id, shell = self._resolve_first_existing_with_id(
            session=session,
            ids=self._candidate_export_shell_ids(session=session),
        )
        shell.pressToolbarContextButton("&MB_EXPORT")
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        shell.selectContextMenuItem("&PC")
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        logger.info("IW59 export opened via ALV toolbar fallback shell_id=%s", shell_id)

    def _candidate_export_shell_ids(self, *, session: Any) -> list[str]:
        candidates = [
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID1/shellcont/shell",
            "wnd[0]/usr/cntlGRID/shellcont/shell",
            "wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell",
            "wnd[0]/usr/cntlRESULT_LIST/shellcont/shell",
            "wnd[0]/usr/cntlRESULT/shellcont/shell",
            "wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID_CONT0050/shellcont/shell",
            "wnd[0]/usr/cntlCC_ALV/shellcont/shell",
        ]
        compat = importlib.import_module("sap_gui_export_compat")
        try:
            visible_ids = compat._collect_visible_control_ids(session=session, limit=80)
        except Exception:
            visible_ids = []
        for item_id in visible_ids:
            normalized = str(item_id or "").strip()
            if not normalized or normalized in candidates:
                continue
            lowered = normalized.lower()
            if "shellcont/shell" not in lowered and not lowered.endswith("/shell"):
                continue
            if "/usr/" not in lowered:
                continue
            candidates.append(normalized)
        return candidates

    def _resolve_first_existing(self, *, session: Any, ids: list[str]) -> Any:
        return resolve_first_existing(session, ids)

    def _resolve_first_existing_with_id(self, *, session: Any, ids: list[str]) -> tuple[str, Any]:
        return resolve_first_existing_with_id(session, ids)

    def _set_first_existing_text(self, *, session: Any, ids: list[str], value: str) -> None:
        set_first_existing_text(session, ids, value)

    def _set_text(self, *, session: Any, item_id: str, value: str) -> None:
        _sap_set_text(session, item_id, value)

    def _wait_for_file(self, *, output_path: Path, timeout_seconds: float) -> None:
        wait_for_file(output_path, timeout_seconds=timeout_seconds)

    def _visible_controls(self, *, session: Any, limit: int) -> list[str]:
        compat = importlib.import_module("sap_gui_export_compat")
        try:
            return list(compat._collect_visible_control_ids(session=session, limit=limit))
        except Exception:
            return []

    def _unwind_before_iw59(
        self,
        *,
        session: Any,
        back_button_id: str,
        logger: Any,
        wait_timeout_seconds: float,
        min_presses: int,
        max_presses: int,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        previous_signature = self._session_signature(session)
        total_presses = 0
        stable_hits = 0
        logger.info(
            "IW59 pre-unwind start min_presses=%s max_presses=%s",
            min_presses,
            max_presses,
        )
        for _ in range(max(1, max_presses)):
            pressed = self._press_back(session=session, back_button_id=back_button_id)
            if not pressed:
                if total_presses >= min_presses:
                    break
                continue
            total_presses += 1
            compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
            current_signature = self._session_signature(session)
            changed = current_signature != previous_signature
            stable_hits = 0 if changed else stable_hits + 1
            logger.info(
                "IW59 pre-unwind press=%s changed=%s stable_hits=%s signature=%s",
                total_presses,
                changed,
                stable_hits,
                current_signature,
            )
            if total_presses >= min_presses and stable_hits >= 1:
                break
            previous_signature = current_signature
        logger.info(
            "IW59 pre-unwind finished presses=%s final_signature=%s",
            total_presses,
            self._session_signature(session),
        )

    def _press_back(self, *, session: Any, back_button_id: str) -> bool:
        try:
            session.findById(back_button_id).press()
            return True
        except Exception:
            try:
                session.findById("wnd[0]").sendVKey(3)
                return True
            except Exception:
                return False

    def _session_signature(self, session: Any) -> str:
        parts = [
            self._safe_text(session, "wnd[0]"),
            self._safe_text(session, "wnd[0]/tbar[0]/okcd"),
            self._safe_text(session, "wnd[0]/sbar"),
            "qmnum=" + str(self._exists(session, "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH")),
            "otgrup=" + str(self._exists(session, "wnd[0]/usr/ctxtOTGRP-LOW")),
        ]
        return "|".join(parts)

    def _safe_text(self, session: Any, item_id: str) -> str:
        try:
            item = session.findById(item_id)
        except Exception:
            return ""
        for attr_name in ("Text", "text"):
            try:
                return str(getattr(item, attr_name, "")).strip()
            except Exception:
                continue
        return ""

    def _status_bar_text(self, session: Any) -> str:
        return self._safe_text(session, "wnd[0]/sbar")

    def _exists(self, session: Any, item_id: str) -> bool:
        try:
            session.findById(item_id)
            return True
        except Exception:
            return False
