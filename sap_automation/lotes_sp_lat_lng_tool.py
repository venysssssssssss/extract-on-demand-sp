from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pyxlsb import open_workbook

from .config import load_export_config
from .lotes_sp_lat_lng_repository import (
    LotesLatLngIngestResult,
    LotesLatLngRepository,
    read_coordinate_csv,
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
        help="Also ingest the consolidated CSV into INSTALACOES_COORDENADAS_SP.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = extract_lotes_lat_lng(
        source_dir=Path(args.source_dir),
        lote_start=args.lote_start,
        lote_end=args.lote_end,
        output_csv_path=Path(args.output_csv_path),
        manifest_path=Path(args.manifest_path),
    )
    payload = result.to_dict()
    if args.ingest:
        ingest_result = ingest_lotes_lat_lng(
            csv_path=Path(args.output_csv_path),
            config_path=Path(args.config_path),
            output_root=Path(args.output_root),
        )
        payload["ingest"] = ingest_result.to_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if result.status == "success" else 1


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
    files = _select_lote_files(resolved_source_dir, lote_start=lote_start, lote_end=lote_end)
    if not files:
        raise RuntimeError(f"No lote XLSB files found between {lote_start} and {lote_end} in {resolved_source_dir}")

    rows_read = 0
    consolidated: dict[str, dict[str, str]] = {}
    duplicate_installations_removed = 0
    for path in files:
        file_rows = _extract_rows_from_xlsb(path)
        rows_read += len(file_rows)
        for row in file_rows:
            instalacao = row["instalacao"]
            if instalacao in consolidated:
                duplicate_installations_removed += 1
                consolidated[instalacao] = _merge_coordinate_rows(consolidated[instalacao], row)
                continue
            consolidated[instalacao] = row

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_TARGET_COLUMNS))
        writer.writeheader()
        for instalacao in sorted(consolidated):
            writer.writerow(consolidated[instalacao])

    result = LotesLatLngResult(
        status="success",
        source_dir=str(resolved_source_dir),
        files_processed=[str(path) for path in files],
        rows_read=rows_read,
        rows_written=len(consolidated),
        duplicate_installations_removed=duplicate_installations_removed,
        output_csv_path=str(output_csv_path.expanduser().resolve()),
        manifest_path=str(manifest_path.expanduser().resolve()),
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def ingest_lotes_lat_lng(
    *,
    csv_path: Path,
    config_path: Path,
    output_root: Path,
) -> LotesLatLngIngestResult:
    try:
        config = load_export_config(config_path)
        db_url = _resolve_db_url(config, output_root, logger)
        rows = read_coordinate_csv(csv_path)
        repository = LotesLatLngRepository(db_url)
        rows_ingested = repository.save_rows(rows)
        return LotesLatLngIngestResult(
            status="success",
            rows_ingested=rows_ingested,
            source_csv_path=str(csv_path.expanduser().resolve()),
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
    return [path for _, _, path in sorted(files)]


def _extract_lote_number(filename: str) -> int | None:
    match = re.search(r"lote\s+(\d+)", filename, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_rows_from_xlsb(path: Path, *, sheet_name: str = "Planilha2") -> list[dict[str, str]]:
    with open_workbook(path) as workbook:
        if sheet_name not in workbook.sheets:
            raise RuntimeError(f"Workbook {path} does not contain expected sheet {sheet_name!r}.")
        with workbook.get_sheet(sheet_name) as sheet:
            iterator = sheet.rows()
            header_cells: list[Any] | None = None
            for _ in range(10):
                try:
                    row = next(iterator)
                except StopIteration:
                    break
                normalized_row = {_normalize_header(str(cell.v or "").strip()) for cell in row if str(cell.v or "").strip()}
                if {_normalize_header(value) for value in _TARGET_COLUMNS.values()}.issubset(normalized_row):
                    header_cells = row
                    break
            if header_cells is None:
                raise RuntimeError(f"Workbook {path} sheet {sheet_name!r} is missing expected coordinate columns.")
            header = [_normalize_header(str(cell.v or "").strip()) for cell in header_cells]
            indexes = {
                key: header.index(_normalize_header(column_name))
                for key, column_name in _TARGET_COLUMNS.items()
            }
            result: list[dict[str, str]] = []
            for row in iterator:
                values = [cell.v for cell in row]
                payload = {key: _normalize_cell_value(values[indexes[key]] if indexes[key] < len(values) else "") for key in _TARGET_COLUMNS}
                payload["instalacao"] = _normalize_installation(payload["instalacao"])
                if payload["instalacao"]:
                    result.append(payload)
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


if __name__ == "__main__":
    raise SystemExit(main())
