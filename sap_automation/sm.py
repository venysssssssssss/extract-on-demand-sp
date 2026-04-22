from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SmIngestResult:
    run_id: str
    status: str
    rows_ingested: int
    csv_path: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _extract_column_from_txt(file_path: Path, column_name: str) -> list[str]:
    """Read a SAP-exported delimited TXT and extract all values from a specific column."""
    if not file_path.exists():
        return []

    import sap_gui_export_compat

    _, _, rows = sap_gui_export_compat._read_delimited_text(file_path)
    if not rows:
        return []

    header = [item.strip() for item in rows[0]]
    # Normalize requested column name to match against normalized header
    target_norm = sap_gui_export_compat._normalize_header_name(column_name)
    col_index = -1
    for i, h in enumerate(header):
        if sap_gui_export_compat._normalize_header_name(h) == target_norm:
            col_index = i
            break

    if col_index == -1:
        logger.error("Column %s (norm=%s) not found in header: %s", column_name, target_norm, header)
        return []

    values: list[str] = []
    for row in rows[1:]:
        if len(row) > col_index:
            val = str(row[col_index]).strip()
            if val:
                values.append(val)
    return values


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
            params = urllib.parse.quote_plus(f"DRIVER={{{sql_driver}}};SERVER={sql_host},{sql_port};DATABASE={sql_db};UID={sql_user};PWD={sql_pass}")
            db_url = f"mssql+pyodbc:///?odbc_connect={params}"
            logger.info("Constructed SQL Server URL for host=%s", sql_host)
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

        if fetch_only:
            installations = repository.get_installations_to_process(
                month=month, year=year, distribuidora=distribuidora
            )
            csv_path = output_root / f"SM_INSTALLATIONS_{run_id}.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["ID_RECLAMAÇÃO"])
                for inst in installations:
                    writer.writerow([inst])
            return SmIngestResult(run_id=run_id, status="success", rows_ingested=len(installations), csv_path=str(csv_path))

        if results is None:
             raise ValueError("Results are required for ingestion when fetch_only is false.")

        repository.save_final_results(run_id, results)
        return SmIngestResult(run_id=run_id, status="success", rows_ingested=len(results))
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
    import sap_gui_export_compat

    resolved_output_root = output_root.expanduser().resolve() / "runs" / run_id / "sm"
    resolved_output_root.mkdir(parents=True, exist_ok=True)

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
                    month=month, year=year, distribuidora=distribuidora
                )
            except Exception as e:
                run_logger.error("Failed to fetch installations from DB: %s", e)
                raise RuntimeError(f"Database query failed: {e}") from e

    if not installations:
        run_logger.warning("No installations found to process for %s in %s/%s", distribuidora, month, year)
        return SmManifest(
            run_id=run_id,
            status="skipped",
            processed_chunks=0,
            total_chunks=0,
            rows_extracted_sqvi1=0,
            rows_extracted_sqvi2=0,
            manifest_path=str(resolved_output_root / "sm_manifest.json"),
        )

    run_logger.info("Found %d installations to process.", len(installations))

    # Chunking
    sm_config = resolve_sm_config(config, demandante)
    chunk_size = int(sm_config.get("chunk_size", 5000))
    chunks = [installations[i : i + chunk_size] for i in range(0, len(installations), chunk_size)]
    total_chunks = len(chunks)

    session = (session_provider or create_session_provider(config)).get_session(config=config, logger=run_logger)
    executor = StepExecutor()

    total_sqvi1_rows = 0
    total_sqvi2_rows = 0
    final_results: list[dict[str, Any]] = []

    try:
        for index, chunk in enumerate(chunks, start=1):
            run_logger.info("Processing chunk %d/%d (size=%d)", index, total_chunks, len(chunk))
            
            # SQVI 1
            sqvi1_cfg = sm_config["sqvi_1"]
            context_sqvi1 = {
                "transaction_code": sm_config["transaction_code"],
                "chunk_index": str(index),
                "export_filename": sqvi1_cfg["export_filename"].format(chunk_index=index),
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
            # Extract Doc.impr. (normalized to doc_impr)
            doc_impr_list = _extract_column_from_txt(sqvi1_file, "Doc.impr.")
            total_sqvi1_rows += len(doc_impr_list)
            
            if not doc_impr_list:
                run_logger.warning("No Doc.impr. found in SQVI 1 results for chunk %d", index)
                continue

            # SQVI 2
            sqvi2_cfg = sm_config["sqvi_2"]
            context_sqvi2 = {
                "transaction_code": sm_config["transaction_code"],
                "chunk_index": str(index),
                "export_filename": sqvi2_cfg["export_filename"].format(chunk_index=index),
            }

            executor.execute(
                session=session,
                steps=[{"action": "set_clipboard_text", "values": doc_impr_list}] + sqvi2_cfg["steps"],
                context=context_sqvi2,
                default_timeout_seconds=float(config["global"].get("wait_timeout_seconds", 60.0)),
                logger=run_logger,
            )

            sqvi2_file = resolved_output_root / context_sqvi2["export_filename"]
            
            # Read and parse final results
            _, _, sqvi2_rows = sap_gui_export_compat._read_delimited_text(sqvi2_file)
            if sqvi2_rows:
                header = sqvi2_rows[0]
                for row_data in sqvi2_rows[1:]:
                    res_dict = dict(zip(header, row_data, strict=False))
                    res_dict["chunk_index"] = index
                    final_results.append(res_dict)
                total_sqvi2_rows += len(sqvi2_rows) - 1

        # Save to DB
        if not skip_ingest and repository:
            repository.save_final_results(run_id, final_results)
        
        status = "success"

    except Exception as exc:
        run_logger.exception("SM flow failed.")
        status = "failed"
        error_msg = str(exc)
    else:
        error_msg = ""

    manifest = SmManifest(
        run_id=run_id,
        status=status,
        processed_chunks=len(chunks), # This should be tracking successful chunks if we want more detail
        total_chunks=total_chunks,
        rows_extracted_sqvi1=total_sqvi1_rows,
        rows_extracted_sqvi2=total_sqvi2_rows,
        manifest_path=str(resolved_output_root / "sm_manifest.json"),
        error=error_msg,
    )

    with open(manifest.manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest.to_dict(), f, ensure_ascii=False, indent=2)

    return manifest
