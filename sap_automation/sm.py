from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .config import load_export_config, resolve_sm_config
from .execution import SessionProvider
from .runtime_logging import configure_run_logger
from .sm_repository import SmRepository

logger = logging.getLogger("sap_automation.sm")


@dataclass(frozen=True)
class SmManifest:
    run_id: str
    status: str
    processed_chunks: int
    total_chunks: int
    rows_extracted_sqvi1: int
    rows_extracted_sqvi2: int
    manifest_path: str
    error: str = ""
    target_month: int = 0
    target_year: int = 0
    distribuidora: str = ""
    installations_count: int = 0
    final_rows: int = 0
    final_csv_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SmIngestResult:
    run_id: str
    status: str
    rows_ingested: int
    csv_path: str = ""
    source_csv_path: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _read_sap_txt_as_dicts(file_path: Path) -> list[dict[str, str]]:
    """Read a SAP-exported delimited TXT preserving its header labels."""
    if not file_path.exists():
        return []

    import sap_gui_export_compat

    _, _, rows = sap_gui_export_compat._read_delimited_text(file_path)
    if not rows:
        return []

    header_index = _find_sm_header_row_index(rows)
    if header_index < 0:
        logger.error("Could not find SM header row in SAP TXT: %s", file_path)
        return []

    header = [item.strip() for item in rows[header_index]]
    parsed_rows: list[dict[str, str]] = []
    for row in rows[header_index + 1 :]:
        parsed_row = _build_row_dict_from_header(header=header, row=row)
        if parsed_row:
            parsed_rows.append(parsed_row)
    return parsed_rows


def _find_sm_header_row_index(rows: list[list[str]]) -> int:
    import sap_gui_export_compat

    required_header_tokens = {
        "doc_impr",
        "nota",
        "montante",
        "dtfxcalcfat",
        "dtfxcalc_fat",
        "dt_fx_calc_fat",
        "vencido",
        "dt_lcto",
    }
    best_index = -1
    best_score = 0
    for index, row in enumerate(rows):
        normalized_cells = {
            sap_gui_export_compat._normalize_header_name(str(cell))
            for cell in row
            if str(cell).strip()
        }
        score = len(normalized_cells.intersection(required_header_tokens))
        if "doc_impr" in normalized_cells:
            score += 10
        if score > best_score:
            best_index = index
            best_score = score
    return best_index if best_score > 0 else -1


def _build_row_dict_from_header(*, header: list[str], row: list[str]) -> dict[str, str]:
    import sap_gui_export_compat

    parsed: dict[str, str] = {}
    non_empty_values = 0
    for index, column_name in enumerate(header):
        cleaned_column_name = str(column_name or "").strip()
        if not cleaned_column_name:
            continue
        value = str(row[index]).strip() if index < len(row) else ""
        if sap_gui_export_compat._normalize_header_name(value) == sap_gui_export_compat._normalize_header_name(cleaned_column_name):
            return {}
        if value:
            non_empty_values += 1
        parsed[cleaned_column_name] = value
    return parsed if non_empty_values else {}


def _extract_column_from_txt(file_path: Path, column_name: str) -> list[str]:
    """Read a SAP-exported delimited TXT and extract all values from a specific column."""
    parsed_rows = _read_sap_txt_as_dicts(file_path)
    if not parsed_rows:
        return []

    import sap_gui_export_compat

    header = list(parsed_rows[0].keys())
    # Normalize requested column name to match against normalized header
    target_norm = sap_gui_export_compat._normalize_header_name(column_name)
    matched_key = ""
    for h in header:
        if sap_gui_export_compat._normalize_header_name(h) == target_norm:
            matched_key = h
            break

    if not matched_key:
        logger.error("Column %s (norm=%s) not found in header: %s", column_name, target_norm, header)
        return []

    values: list[str] = []
    for row in parsed_rows:
        val = str(row.get(matched_key, "")).strip()
        if val:
            values.append(val)
    return values


def _resolve_target_period(month: int | None, year: int | None, today: date | None = None) -> tuple[int, int]:
    reference_date = today or date.today()
    return month or reference_date.month, year or reference_date.year


def _deduplicate_tokens(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if token and token not in seen:
            seen.add(token)
            normalized.append(token)
    return normalized


def _first_by_normalized_key(row: dict[str, Any], candidate_names: list[str]) -> str:
    import sap_gui_export_compat

    normalized_candidates = {
        sap_gui_export_compat._normalize_header_name(candidate)
        for candidate in candidate_names
    }
    for key, value in row.items():
        if sap_gui_export_compat._normalize_header_name(str(key)) in normalized_candidates:
            token = str(value or "").strip()
            if token:
                return token
    return ""


def _build_sqvi1_index(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        doc_impr = _first_by_normalized_key(row, ["Doc.impr.", "Doc impr", "doc_impr"])
        if doc_impr:
            indexed.setdefault(doc_impr, []).append(row)
    return indexed


def _build_sm_final_rows(
    *,
    chunk_index: int,
    sqvi1_rows: list[dict[str, str]],
    sqvi2_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    sqvi1_by_doc = _build_sqvi1_index(sqvi1_rows)
    matched_docs: set[str] = set()
    final_rows: list[dict[str, Any]] = []

    for sqvi2_row in sqvi2_rows:
        doc_impr = _first_by_normalized_key(sqvi2_row, ["Doc.impr.", "Doc impr", "doc_impr"])
        sqvi1_row = sqvi1_by_doc.get(doc_impr, [{}])[0] if doc_impr else {}
        if doc_impr:
            matched_docs.add(doc_impr)
        final_rows.append(
            _build_sm_final_row(
                chunk_index=_row_chunk_index(sqvi2_row, fallback=chunk_index),
                sqvi1_row=sqvi1_row,
                sqvi2_row=sqvi2_row,
            )
        )

    for sqvi1_row in sqvi1_rows:
        doc_impr = _first_by_normalized_key(sqvi1_row, ["Doc.impr.", "Doc impr", "doc_impr"])
        if doc_impr and doc_impr in matched_docs:
            continue
        final_rows.append(
            _build_sm_final_row(
                chunk_index=_row_chunk_index(sqvi1_row, fallback=chunk_index),
                sqvi1_row=sqvi1_row,
                sqvi2_row={},
                extraction_status="sqvi2_missing",
            )
        )

    return final_rows


def _build_sm_final_row(
    *,
    chunk_index: int,
    sqvi1_row: dict[str, Any],
    sqvi2_row: dict[str, Any],
    extraction_status: str = "success",
) -> dict[str, Any]:
    doc_impr = _first_by_normalized_key(sqvi2_row, ["Doc.impr.", "Doc impr", "doc_impr"]) or _first_by_normalized_key(
        sqvi1_row, ["Doc.impr.", "Doc impr", "doc_impr"]
    )
    return {
        "chunk_index": chunk_index,
        "nota": _first_by_normalized_key(sqvi1_row, ["Nota"]),
        "doc_impr": doc_impr,
        "montante": _first_by_normalized_key(sqvi1_row, ["Montante", "Monta nte"]),
        "dt_fx_calc_fat": _first_by_normalized_key(sqvi1_row, ["DtFxCálcFat", "DtFxCalcFat", "Dt Fx Cálc Fat"]),
        "vencido": _first_by_normalized_key(sqvi2_row, ["vencido"]),
        "dt_lcto": _first_by_normalized_key(sqvi2_row, ["Dt.lçto.", "Dt.lcto.", "Dt lçto", "Dt lcto"]),
        "extraction_status": extraction_status,
        "sqvi1": _without_internal_keys(sqvi1_row),
        "sqvi2": _without_internal_keys(sqvi2_row),
    }


def _row_chunk_index(row: dict[str, Any], *, fallback: int) -> int:
    try:
        return int(row.get("__sm_chunk_index") or fallback)
    except (TypeError, ValueError):
        return fallback


def _without_internal_keys(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not str(key).startswith("__sm_")}


def _tag_rows_with_chunk(rows: list[dict[str, str]], *, chunk_index: int) -> list[dict[str, str]]:
    tagged_rows: list[dict[str, str]] = []
    for row in rows:
        tagged = dict(row)
        tagged["__sm_chunk_index"] = str(chunk_index)
        tagged_rows.append(tagged)
    return tagged_rows


def _write_sm_final_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "nota",
        "doc_impr",
        "montante",
        "dt_fx_calc_fat",
        "vencido",
        "dt_lcto",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            csv_row = {
                "nota": row.get("nota", ""),
                "doc_impr": row.get("doc_impr", ""),
                "montante": row.get("montante", ""),
                "dt_fx_calc_fat": row.get("dt_fx_calc_fat", ""),
                "vencido": row.get("vencido", ""),
                "dt_lcto": row.get("dt_lcto", ""),
            }
            if _has_only_nota(csv_row):
                continue
            writer.writerow(
                csv_row
            )


def _has_only_nota(row: dict[str, Any]) -> bool:
    nota = str(row.get("nota") or "").strip()
    if not nota:
        return False
    return not any(
        str(row.get(column) or "").strip()
        for column in ("doc_impr", "montante", "dt_fx_calc_fat", "vencido", "dt_lcto")
    )


def _read_sm_final_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"SM final CSV not found: {path}")

    results: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            results.append(
                {
                    "chunk_index": row.get("chunk_index", ""),
                    "nota": row.get("sqvi1_nota") or row.get("nota", ""),
                    "doc_impr": row.get("doc_impr", ""),
                    "montante": row.get("sqvi1_montante") or row.get("montante", ""),
                    "dt_fx_calc_fat": row.get("sqvi1_dt_fx_calc_fat") or row.get("dt_fx_calc_fat", ""),
                    "vencido": row.get("sqvi2_vencido") or row.get("vencido", ""),
                    "dt_lcto": row.get("sqvi2_dt_lcto") or row.get("dt_lcto", ""),
                    "extraction_status": row.get("extraction_status", "success"),
                    "sqvi1": {
                        "Nota": row.get("sqvi1_nota") or row.get("nota", ""),
                        "Doc.impr.": row.get("doc_impr", ""),
                        "Montante": row.get("sqvi1_montante") or row.get("montante", ""),
                        "DtFxCálcFat": row.get("sqvi1_dt_fx_calc_fat") or row.get("dt_fx_calc_fat", ""),
                    },
                    "sqvi2": {
                        "Doc.impr.": row.get("doc_impr", ""),
                        "vencido": row.get("sqvi2_vencido") or row.get("vencido", ""),
                        "Dt.lçto.": row.get("sqvi2_dt_lcto") or row.get("dt_lcto", ""),
                    },
                }
            )
    return results


def _loads_json_dict(value: str | None) -> dict[str, Any]:
    token = str(value or "").strip()
    if not token:
        return {}
    parsed = json.loads(token)
    if not isinstance(parsed, dict):
        raise ValueError("SM CSV payload column must contain a JSON object.")
    return parsed


def _resolve_sm_final_csv_path(
    *,
    output_root: Path,
    final_csv_path: Path | None,
    source_run_id: str | None,
    run_id: str,
) -> Path:
    if final_csv_path is not None:
        return final_csv_path.expanduser().resolve()
    resolved_source_run_id = str(source_run_id or run_id).strip()
    return output_root.expanduser().resolve() / "runs" / resolved_source_run_id / "sm" / "SM_DADOS_FATURA.csv"


def _resolve_db_url(config: dict[str, Any], output_root: Path, logger: logging.Logger) -> str:
    db_url = config.get("global", {}).get("database_url")
    if not db_url:
        import os
        sql_host = os.environ.get("ENEL_SQL_HOST")
        sql_user = os.environ.get("ENEL_SQL_USER")
        sql_pass = os.environ.get("ENEL_SQL_PASSWORD")
        sql_db = os.environ.get("ENEL_SQL_DATABASE", "ENEL")
        sql_port = os.environ.get("ENEL_SQL_PORT", "1433")
        sql_driver = os.environ.get("ENEL_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

        if sql_host and sql_user and sql_pass:
            import urllib
            # Add TrustServerCertificate=yes to handle SSL trust issues common in local/internal environments
            params = urllib.parse.quote_plus(
                f"DRIVER={{{sql_driver}}};SERVER={sql_host},{sql_port};"
                f"DATABASE={sql_db};UID={sql_user};PWD={sql_pass};"
                "TrustServerCertificate=yes;"
            )
            db_url = f"mssql+pyodbc:///?odbc_connect={params}"
            logger.info("Constructed SQL Server URL for host=%s (TrustServerCertificate=yes)", sql_host)
        else:
            db_url = os.environ.get("DATABASE_URL")
    
    if not db_url:
        output_root_resolved = output_root.expanduser().resolve()
        db_url = f"sqlite+pysqlite:///{(output_root_resolved / 'control_plane.db').as_posix()}"
        logger.info("Using default SQLite database: %s", db_url)
    return db_url


def ingest_sm_results(
    *,
    run_id: str,
    results: list[dict[str, Any]] | None = None,
    output_root: Path,
    config_path: Path,
    fetch_only: bool = False,
    final_csv_path: Path | None = None,
    source_run_id: str | None = None,
    month: int | None = None,
    year: int | None = None,
    distribuidora: str = "São Paulo",
) -> SmIngestResult:
    """Save raw results to the database OR fetch installations to CSV."""
    config = load_export_config(config_path)
    ingest_logger = logging.getLogger(f"sap_automation.sm.ingest.{run_id}")
    
    try:
        db_url = _resolve_db_url(config, output_root, ingest_logger)
        repository = SmRepository(db_url)
        target_month, target_year = _resolve_target_period(month, year)

        if fetch_only:
            installations = repository.get_installations_to_process(
                month=target_month, year=target_year, distribuidora=distribuidora
            )
            installations = _deduplicate_tokens(installations)
            csv_path = output_root / f"SM_INSTALLATIONS_{run_id}.csv"
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["ID_RECLAMAÇÃO"])
                for inst in installations:
                    writer.writerow([inst])
            return SmIngestResult(run_id=run_id, status="success", rows_ingested=len(installations), csv_path=str(csv_path))

        if results is None:
            resolved_csv_path = _resolve_sm_final_csv_path(
                output_root=output_root,
                final_csv_path=final_csv_path,
                source_run_id=source_run_id,
                run_id=run_id,
            )
            results = _read_sm_final_csv(resolved_csv_path)
            source_csv_path = str(resolved_csv_path)
        else:
            source_csv_path = ""

        repository.save_final_results(
            run_id,
            results,
            month=target_month,
            year=target_year,
            distribuidora=distribuidora,
        )
        return SmIngestResult(
            run_id=run_id,
            status="success",
            rows_ingested=len(results),
            source_csv_path=source_csv_path,
        )
    except Exception as exc:
        ingest_logger.exception("Operation failed.")
        return SmIngestResult(run_id=run_id, status="failed", rows_ingested=0, error=str(exc))


def run_sm_demandante(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    month: int | None = None,
    year: int | None = None,
    distribuidora: str = "São Paulo",
    installations: list[str] | None = None,
    installations_csv_path: Path | None = None,
    skip_ingest: bool = False,
    session_provider: SessionProvider | None = None,
) -> SmManifest:
    """Execute the Sala Mercado flow: DB/CSV -> SQVI 1 -> Column Extract -> SQVI 2 -> DB."""
    from .service import create_session_provider
    from .execution import StepExecutor

    resolved_output_root = output_root.expanduser().resolve() / "runs" / run_id / "sm"
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    final_csv_path = resolved_output_root / "SM_DADOS_FATURA.csv"
    manifest_path = resolved_output_root / "sm_manifest.json"
    target_month, target_year = _resolve_target_period(month, year)

    config = load_export_config(config_path)
    run_logger, log_path = configure_run_logger(output_root=resolved_output_root, run_id=run_id)

    # Repository for DB access (only if needed)
    repository = None
    if (installations is None and installations_csv_path is None) or not skip_ingest:
        db_url = _resolve_db_url(config, output_root, run_logger)
        repository = SmRepository(db_url)

    # Resolve Installations
    if installations is None:
        if installations_csv_path:
            run_logger.info("Reading installations from CSV: %s", installations_csv_path)
            with open(installations_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                installations = [row["ID_RECLAMAÇÃO"] for row in reader if row.get("ID_RECLAMAÇÃO")]
        else:
            if not repository:
                raise RuntimeError("Repository not available to fetch installations.")
            try:
                installations = repository.get_installations_to_process(
                    month=target_month, year=target_year, distribuidora=distribuidora
                )
            except Exception as e:
                run_logger.error("Failed to fetch installations from DB: %s", e)
                raise RuntimeError(f"Database query failed: {e}") from e

    installations = _deduplicate_tokens(installations)

    if not installations:
        run_logger.warning("No installations found to process for %s in %s/%s", distribuidora, target_month, target_year)
        manifest = SmManifest(
            run_id=run_id,
            status="skipped",
            processed_chunks=0,
            total_chunks=0,
            rows_extracted_sqvi1=0,
            rows_extracted_sqvi2=0,
            manifest_path=str(manifest_path),
            target_month=target_month,
            target_year=target_year,
            distribuidora=distribuidora,
            installations_count=0,
            final_rows=0,
            final_csv_path=str(final_csv_path),
        )
        manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    run_logger.info(
        "Found %d installations to process distribuidora=%s reference=%04d-%02d.",
        len(installations),
        distribuidora,
        target_year,
        target_month,
    )

    # Chunking
    sm_config = resolve_sm_config(config, demandante)
    chunk_size = int(sm_config.get("chunk_size", 5000))
    chunks = [installations[i : i + chunk_size] for i in range(0, len(installations), chunk_size)]
    total_chunks = len(chunks)

    session = (session_provider or create_session_provider(config)).get_session(config=config, logger=run_logger)
    executor = StepExecutor()

    total_sqvi1_rows = 0
    total_sqvi2_rows = 0
    processed_chunks = 0
    final_results: list[dict[str, Any]] = []
    all_sqvi1_rows: list[dict[str, str]] = []
    all_sqvi2_rows: list[dict[str, str]] = []
    all_doc_impr: list[str] = []

    try:
        for index, chunk in enumerate(chunks, start=1):
            run_logger.info("Processing SQVI 1 chunk %d/%d (installations=%d)", index, total_chunks, len(chunk))
            
            # SQVI 1
            sqvi1_cfg = sm_config["sqvi_1"]
            context_sqvi1 = {
                "transaction_code": sm_config["transaction_code"],
                "chunk_index": str(index),
                "export_filename": sqvi1_cfg["export_filename"].format(chunk_index=index),
                "output_dir": str(resolved_output_root),
            }
            
            # Use clipboard to pass chunk values
            executor.execute(
                session=session,
                steps=[{"action": "set_clipboard_text", "values": chunk}] + sqvi1_cfg["steps"],
                context=context_sqvi1,
                default_timeout_seconds=float(config["global"].get("wait_timeout_seconds", 60.0)),
                logger=run_logger,
            )

            sqvi1_file = resolved_output_root / context_sqvi1["export_filename"]
            sqvi1_rows = _tag_rows_with_chunk(_read_sap_txt_as_dicts(sqvi1_file), chunk_index=index)
            doc_impr_list = _deduplicate_tokens(_extract_column_from_txt(sqvi1_file, "Doc.impr."))
            total_sqvi1_rows += len(sqvi1_rows)
            all_sqvi1_rows.extend(sqvi1_rows)
            all_doc_impr.extend(doc_impr_list)
            processed_chunks += 1
            
            if not doc_impr_list:
                run_logger.warning("No Doc.impr. found in SQVI 1 results for chunk %d", index)
            else:
                run_logger.info("SQVI 1 chunk %d extracted doc_impr=%d rows=%d", index, len(doc_impr_list), len(sqvi1_rows))

        all_doc_impr = _deduplicate_tokens(all_doc_impr)
        if not all_doc_impr:
            run_logger.warning("No Doc.impr. found in any SQVI 1 result. SQVI 2 will be skipped.")

        doc_chunks = [all_doc_impr[i : i + chunk_size] for i in range(0, len(all_doc_impr), chunk_size)]
        for index, doc_chunk in enumerate(doc_chunks, start=1):
            run_logger.info(
                "Processing SQVI 2 chunk %d/%d (doc_impr=%d total_doc_impr=%d)",
                index,
                len(doc_chunks),
                len(doc_chunk),
                len(all_doc_impr),
            )
            sqvi2_cfg = sm_config["sqvi_2"]
            context_sqvi2 = {
                "transaction_code": sm_config["transaction_code"],
                "chunk_index": str(index),
                "export_filename": sqvi2_cfg["export_filename"].format(chunk_index=index),
                "output_dir": str(resolved_output_root),
            }

            executor.execute(
                session=session,
                steps=[{"action": "set_clipboard_text", "values": doc_chunk}] + sqvi2_cfg["steps"],
                context=context_sqvi2,
                default_timeout_seconds=float(config["global"].get("wait_timeout_seconds", 60.0)),
                logger=run_logger,
            )

            sqvi2_file = resolved_output_root / context_sqvi2["export_filename"]
            
            # Read and parse final results
            sqvi2_rows = _tag_rows_with_chunk(_read_sap_txt_as_dicts(sqvi2_file), chunk_index=index)
            total_sqvi2_rows += len(sqvi2_rows)
            all_sqvi2_rows.extend(sqvi2_rows)

        final_results = _build_sm_final_rows(
            chunk_index=0,
            sqvi1_rows=all_sqvi1_rows,
            sqvi2_rows=all_sqvi2_rows,
        )

        # Save to DB
        _write_sm_final_csv(final_csv_path, final_results)
        if not skip_ingest and repository:
            repository.save_final_results(
                run_id,
                final_results,
                month=target_month,
                year=target_year,
                distribuidora=distribuidora,
            )
        
        status = "success"

    except Exception as exc:
        run_logger.exception("SM flow failed.")
        if all_sqvi1_rows or all_sqvi2_rows or final_results:
            final_results = final_results or _build_sm_final_rows(
                chunk_index=0,
                sqvi1_rows=all_sqvi1_rows,
                sqvi2_rows=all_sqvi2_rows,
            )
            _write_sm_final_csv(final_csv_path, final_results)
        status = "failed"
        error_msg = str(exc)
    else:
        error_msg = ""

    manifest = SmManifest(
        run_id=run_id,
        status=status,
        processed_chunks=processed_chunks,
        total_chunks=total_chunks,
        rows_extracted_sqvi1=total_sqvi1_rows,
        rows_extracted_sqvi2=total_sqvi2_rows,
        manifest_path=str(manifest_path),
        error=error_msg,
        target_month=target_month,
        target_year=target_year,
        distribuidora=distribuidora,
        installations_count=len(installations),
        final_rows=len(final_results),
        final_csv_path=str(final_csv_path),
    )

    with open(manifest.manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest.to_dict(), f, ensure_ascii=False, indent=2)

    return manifest
