from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from python_calamine import load_workbook as load_calamine_workbook
from pyxlsb import open_workbook

from .config import load_export_config
from .lotes_sp_lat_lng_repository import (
    CoordinateCsvIngestError,
    LotesLatLngIngestResult,
    LotesLatLngRepository,
)
from .sm import _resolve_db_url

logger = logging.getLogger("sap_automation.lotes_sp_lat_lng_tool")

_TARGET_COLUMNS = {
    "instalacao": "Instalação",
    "lat_da_instalacao": "Latitude inst",
    "lng_da_instalacao": "Longitude inst",
    "lat_da_leitura": "Latitude lido",
    "lng_da_leitura": "Longitude lido",
}


@dataclass(frozen=True)
class LotesLatLngResult:
    status: str
    source_dir: str
    files_processed: list[str]
    rows_read: int
    rows_written: int
    duplicate_installations_removed: int
    rows_with_any_coordinates: int
    rows_without_coordinates: int
    output_csv_path: str
    manifest_path: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract INSTALACAO and latitude/longitude columns from lotes_viny XLSB files and optionally ingest them.",
    )
    parser.add_argument(
        "--source-dir",
        default="lotes_sp_lat_lng/lotes_viny",
        help="Directory containing Lote *.xlsb files.",
    )
    parser.add_argument(
        "--lote-start",
        type=int,
        default=1,
        help="Initial lote number to include.",
    )
    parser.add_argument(
        "--lote-end",
        type=int,
        default=20,
        help="Final lote number to include.",
    )
    parser.add_argument(
        "--output-csv-path",
        default="output/spreadsheet/instalacoes_coordenadas_sp.csv",
        help="Destination CSV path.",
    )
    parser.add_argument(
        "--manifest-path",
        default="output/spreadsheet/instalacoes_coordenadas_sp.manifest.json",
        help="Destination manifest JSON path.",
    )
    parser.add_argument(
        "--config-path",
        default="sap_iw69_batch_config.json",
        help="Config path used only when --ingest is enabled.",
    )
    parser.add_argument(
        "--output-root",
        default="output",
        help="Output root used only to resolve DB URL when --ingest is enabled.",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Deprecated alias for --mode extract-and-ingest.",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Deprecated alias for --mode ingest.",
    )
    parser.add_argument(
        "--mode",
        choices=["extract", "ingest", "extract-and-ingest"],
        default="extract",
        help="Execution mode. Use the same CLI shape and change only this flag.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="CLI log level: DEBUG, INFO, WARNING, ERROR.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    _configure_cli_logging(args.log_level)
    if args.ingest and args.ingest_only:
        raise SystemExit("--ingest and --ingest-only are mutually exclusive.")
    mode = str(args.mode)
    if args.ingest:
        mode = "extract-and-ingest"
    if args.ingest_only:
        mode = "ingest"

    logger.info(
        "Starting SP_LAT_LNG CLI mode=%s source_dir=%s lote_start=%s lote_end=%s output_csv=%s",
        mode,
        args.source_dir,
        args.lote_start,
        args.lote_end,
        args.output_csv_path,
    )

    payload: dict[str, Any] = {}
    if mode in {"extract", "extract-and-ingest"}:
        result = extract_lotes_lat_lng(
            source_dir=Path(args.source_dir),
            lote_start=args.lote_start,
            lote_end=args.lote_end,
            output_csv_path=Path(args.output_csv_path),
            manifest_path=Path(args.manifest_path),
        )
        payload.update(result.to_dict())
        exit_code = 0 if result.status == "success" else 1
    else:
        logger.info("Skipping extraction because mode=%s", mode)
        exit_code = 0

    if mode in {"ingest", "extract-and-ingest"}:
        ingest_result = ingest_lotes_lat_lng(
            csv_path=Path(args.output_csv_path),
            config_path=Path(args.config_path),
            output_root=Path(args.output_root),
        )
        payload["ingest"] = ingest_result.to_dict()
        if ingest_result.status != "success":
            exit_code = 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


def extract_lotes_lat_lng(
    *,
    source_dir: Path,
    lote_start: int,
    lote_end: int,
    output_csv_path: Path,
    manifest_path: Path,
) -> LotesLatLngResult:
    resolved_source_dir = source_dir.expanduser().resolve()
    if not resolved_source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {resolved_source_dir}")
    logger.info(
        "Scanning lote XLSB files source_dir=%s lote_start=%s lote_end=%s",
        resolved_source_dir,
        lote_start,
        lote_end,
    )
    files = _select_lote_files(resolved_source_dir, lote_start=lote_start, lote_end=lote_end)
    if not files:
        raise RuntimeError(f"No lote XLSB files found between {lote_start} and {lote_end} in {resolved_source_dir}")
    logger.info("Selected lote files count=%s", len(files))

    rows_read = 0
    consolidated: dict[str, dict[str, str]] = {}
    duplicate_installations_removed = 0
    for path in files:
        logger.info("Reading lote workbook path=%s", path)
        file_rows = _extract_rows_from_xlsb(path)
        logger.info("Workbook parsed path=%s rows=%s", path, len(file_rows))
        rows_read += len(file_rows)
        for row in file_rows:
            instalacao = row["instalacao"]
            if instalacao in consolidated:
                duplicate_installations_removed += 1
                consolidated[instalacao] = _merge_coordinate_rows(consolidated[instalacao], row)
                continue
            consolidated[instalacao] = row

    resolved_output_csv_path = output_csv_path.expanduser().resolve()
    resolved_output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing consolidated coordinate CSV path=%s rows=%s", resolved_output_csv_path, len(consolidated))
    with resolved_output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_TARGET_COLUMNS))
        writer.writeheader()
        for instalacao in sorted(consolidated):
            writer.writerow(consolidated[instalacao])

    rows_without_coordinates = len(consolidated) - sum(
        1 for row in consolidated.values() if _row_has_any_coordinates(row)
    )

    result = LotesLatLngResult(
        status="success",
        source_dir=str(resolved_source_dir),
        files_processed=[str(path) for path in files],
        rows_read=rows_read,
        rows_written=len(consolidated),
        duplicate_installations_removed=duplicate_installations_removed,
        rows_with_any_coordinates=sum(1 for row in consolidated.values() if _row_has_any_coordinates(row)),
        rows_without_coordinates=rows_without_coordinates,
        output_csv_path=str(resolved_output_csv_path),
        manifest_path=str(manifest_path.expanduser().resolve()),
    )
    resolved_manifest_path = manifest_path.expanduser().resolve()
    resolved_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_manifest_path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "Extraction finished files=%s rows_read=%s rows_written=%s duplicates_removed=%s with_coordinates=%s without_coordinates=%s manifest=%s",
        len(files),
        rows_read,
        len(consolidated),
        duplicate_installations_removed,
        result.rows_with_any_coordinates,
        result.rows_without_coordinates,
        resolved_manifest_path,
    )
    return result


def ingest_lotes_lat_lng(
    *,
    csv_path: Path,
    config_path: Path,
    output_root: Path,
) -> LotesLatLngIngestResult:
    try:
        resolved_csv_path = csv_path.expanduser().resolve()
        resolved_output_root = output_root.expanduser().resolve()
        resolved_output_root.mkdir(parents=True, exist_ok=True)
        logger.info("Starting coordinate ingest csv_path=%s config_path=%s output_root=%s", resolved_csv_path, config_path, resolved_output_root)
        config = load_export_config(config_path)
        db_url = _resolve_db_url(config, resolved_output_root, logger)
        repository = LotesLatLngRepository(db_url)
        rows_ingested = repository.save_csv(resolved_csv_path)
        logger.info("Coordinate ingest finished rows_ingested=%s csv_path=%s", rows_ingested, resolved_csv_path)
        return LotesLatLngIngestResult(
            status="success",
            rows_ingested=rows_ingested,
            source_csv_path=str(resolved_csv_path),
        )
    except CoordinateCsvIngestError as exc:
        logger.exception("Failed to ingest coordinate CSV after partial progress.")
        return LotesLatLngIngestResult(
            status="failed",
            rows_ingested=exc.rows_ingested,
            source_csv_path=str(csv_path.expanduser().resolve()),
            error=str(exc),
        )
    except Exception as exc:
        logger.exception("Failed to ingest coordinate CSV.")
        return LotesLatLngIngestResult(
            status="failed",
            rows_ingested=0,
            source_csv_path=str(csv_path.expanduser().resolve()),
            error=str(exc),
        )


def _select_lote_files(source_dir: Path, *, lote_start: int, lote_end: int) -> list[Path]:
    files: list[tuple[int, str, Path]] = []
    for path in source_dir.glob("*.xlsb"):
        lote_number = _extract_lote_number(path.name)
        if lote_number is None or lote_number < lote_start or lote_number > lote_end:
            continue
        files.append((lote_number, path.name, path))
    logger.debug("Matched lote XLSB files=%s", [path.name for _, _, path in sorted(files)])
    return [path for _, _, path in sorted(files)]


def _extract_lote_number(filename: str) -> int | None:
    match = re.search(r"lote\s+(\d+)", filename, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_rows_from_xlsb(path: Path, *, sheet_name: str = "Planilha2") -> list[dict[str, str]]:
    try:
        return _extract_rows_from_xlsb_calamine(path, sheet_name=sheet_name)
    except Exception as exc:
        logger.warning("Calamine XLSB reader failed, falling back to pyxlsb path=%s error=%s", path, exc)
        return _extract_rows_from_xlsb_pyxlsb(path, sheet_name=sheet_name)


def _extract_rows_from_xlsb_calamine(path: Path, *, sheet_name: str = "Planilha2") -> list[dict[str, str]]:
    workbook = load_calamine_workbook(path)
    if sheet_name not in workbook.sheet_names:
        raise RuntimeError(f"Workbook {path} does not contain expected sheet {sheet_name!r}.")
    sheet = workbook.get_sheet_by_name(sheet_name)
    try:
        return _extract_rows_from_values(sheet.iter_rows(), path=path, sheet_name=sheet_name)
    finally:
        with suppress(Exception):
            workbook.close()


def _extract_rows_from_xlsb_pyxlsb(path: Path, *, sheet_name: str = "Planilha2") -> list[dict[str, str]]:
    with open_workbook(path) as workbook:
        if sheet_name not in workbook.sheets:
            raise RuntimeError(f"Workbook {path} does not contain expected sheet {sheet_name!r}.")
        with workbook.get_sheet(sheet_name) as sheet:
            return _extract_rows_from_values(
                ([cell.v for cell in row] for row in sheet.rows()),
                path=path,
                sheet_name=sheet_name,
            )


def _extract_rows_from_values(rows: Any, *, path: Path, sheet_name: str) -> list[dict[str, str]]:
    iterator = iter(rows)
    header_cells: list[Any] | None = None
    header_scan_limit = 10
    for row_index in range(1, header_scan_limit + 1):
        try:
            row = next(iterator)
        except StopIteration:
            break
        normalized_row = {_normalize_header(str(value or "").strip()) for value in row if str(value or "").strip()}
        if {_normalize_header(value) for value in _TARGET_COLUMNS.values()}.issubset(normalized_row):
            header_cells = list(row)
            logger.debug("Found coordinate header path=%s sheet=%s row_index=%s", path, sheet_name, row_index)
            break
    if header_cells is None:
        raise RuntimeError(f"Workbook {path} sheet {sheet_name!r} is missing expected coordinate columns.")
    header = [_normalize_header(str(value or "").strip()) for value in header_cells]
    indexes = {
        key: header.index(_normalize_header(column_name))
        for key, column_name in _TARGET_COLUMNS.items()
    }
    result: list[dict[str, str]] = []
    for row in iterator:
        values = list(row)
        payload = {key: _normalize_cell_value(values[indexes[key]] if indexes[key] < len(values) else "") for key in _TARGET_COLUMNS}
        payload["instalacao"] = _normalize_installation(payload["instalacao"])
        if payload["instalacao"]:
            result.append(payload)
    logger.debug("Extracted coordinate rows path=%s rows=%s", path, len(result))
    return result


def _normalize_header(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text == "-":
        return ""
    if text.endswith(".0") and text.replace(".", "", 1).replace("-", "", 1).isdigit():
        return text[:-2]
    return text


def _normalize_installation(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return "".join(ch for ch in text if ch.isdigit())


def _merge_coordinate_rows(current: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(current)
    for key in _TARGET_COLUMNS:
        if key == "instalacao":
            continue
        if not merged.get(key) and incoming.get(key):
            merged[key] = incoming[key]
    return merged


def _row_has_any_coordinates(row: dict[str, str]) -> bool:
    return any(
        str(row.get(key, "") or "").strip()
        for key in ("lat_da_instalacao", "lng_da_instalacao", "lat_da_leitura", "lng_da_leitura")
    )


def _configure_cli_logging(log_level: str) -> None:
    resolved_level = getattr(logging, str(log_level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    with suppress(Exception):
        logging.getLogger("pyxlsb").setLevel(max(logging.WARNING, resolved_level))


if __name__ == "__main__":
    raise SystemExit(main())
