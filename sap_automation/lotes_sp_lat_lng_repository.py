from __future__ import annotations

import csv
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Column, MetaData, String, Table, create_engine, delete, insert
from sqlalchemy.exc import DBAPIError

logger = logging.getLogger("sap_automation.lotes_sp_lat_lng_repository")

DEFAULT_CSV_CHUNK_SIZE = 2000
MIN_ADAPTIVE_CHUNK_SIZE = 250
MAX_TRANSIENT_RETRIES = 3

metadata = MetaData()

instalacoes_coordenadas_sp = Table(
    "INSTALACOES_COORDENADAS_SP",
    metadata,
    Column("instalacao", String(128), primary_key=True),
    Column("lat_da_instalacao", String(64)),
    Column("lng_da_instalacao", String(64)),
    Column("lat_da_leitura", String(64)),
    Column("lng_da_leitura", String(64)),
    extend_existing=True,
)


@dataclass(frozen=True)
class LotesLatLngIngestResult:
    status: str
    rows_ingested: int
    source_csv_path: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CoordinateCsvIngestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        rows_read: int,
        rows_ingested: int,
        duplicates_removed: int,
    ) -> None:
        super().__init__(message)
        self.rows_read = rows_read
        self.rows_ingested = rows_ingested
        self.duplicates_removed = duplicates_removed


class LotesLatLngRepository:
    def __init__(self, engine_url: str) -> None:
        self.engine_url = engine_url
        engine_kwargs: dict[str, Any] = {}
        if str(engine_url).startswith("mssql+pyodbc"):
            engine_kwargs["fast_executemany"] = True
        self.engine = create_engine(
            engine_url,
            pool_pre_ping=True,
            pool_recycle=1800,
            **engine_kwargs,
        )
        logger.info("Initialized coordinate repository dialect=%s", self.engine.dialect.name)

    def save_rows(self, rows: list[dict[str, str]]) -> int:
        normalized_rows = _normalize_coordinate_rows(rows)
        logger.info(
            "Preparing coordinate ingest input_rows=%s normalized_rows=%s",
            len(rows),
            len(normalized_rows),
        )
        metadata.create_all(self.engine, tables=[instalacoes_coordenadas_sp])
        with self.engine.begin() as conn:
            if normalized_rows:
                keys = [row["instalacao"] for row in normalized_rows]
                deleted_rows = 0
                for chunk in _chunked(keys, size=1000):
                    result = conn.execute(
                        delete(instalacoes_coordenadas_sp).where(instalacoes_coordenadas_sp.c.instalacao.in_(chunk))
                    )
                    deleted_rows += int(result.rowcount or 0)
                logger.info("Deleted existing coordinate rows count=%s", deleted_rows)
                for chunk in _chunked(normalized_rows, size=1000):
                    conn.execute(insert(instalacoes_coordenadas_sp), chunk)
        logger.info("Saved %d rows to INSTALACOES_COORDENADAS_SP", len(normalized_rows))
        return len(normalized_rows)

    def save_csv(self, path: Path, *, chunk_size: int = DEFAULT_CSV_CHUNK_SIZE) -> int:
        resolved_path = path.expanduser().resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Coordinate CSV not found: {resolved_path}")
        metadata.create_all(self.engine, tables=[instalacoes_coordenadas_sp])
        rows_ingested = 0
        rows_read = 0
        duplicates_removed = 0
        seen: set[str] = set()
        batch: list[dict[str, str]] = []
        logger.info(
            "Starting coordinate CSV ingest path=%s chunk_size=%s min_adaptive_chunk_size=%s max_retries=%s",
            resolved_path,
            chunk_size,
            MIN_ADAPTIVE_CHUNK_SIZE,
            MAX_TRANSIENT_RETRIES,
        )
        try:
            with resolved_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    rows_read += 1
                    normalized_row = _normalize_coordinate_row(row)
                    instalacao = normalized_row.get("instalacao", "")
                    if not instalacao:
                        continue
                    if instalacao in seen:
                        duplicates_removed += 1
                        continue
                    seen.add(instalacao)
                    batch.append(normalized_row)
                    if len(batch) >= chunk_size:
                        rows_ingested += self._save_batch_resilient(batch)
                        logger.info(
                            "Coordinate CSV ingest progress rows_read=%s rows_ingested=%s duplicates_removed=%s",
                            rows_read,
                            rows_ingested,
                            duplicates_removed,
                        )
                        batch = []
                if batch:
                    rows_ingested += self._save_batch_resilient(batch)
        except Exception as exc:
            message = (
                "Coordinate CSV ingest failed after "
                f"rows_read={rows_read} rows_ingested={rows_ingested} duplicates_removed={duplicates_removed}: {exc}"
            )
            logger.exception(message)
            raise CoordinateCsvIngestError(
                message,
                rows_read=rows_read,
                rows_ingested=rows_ingested,
                duplicates_removed=duplicates_removed,
            ) from exc
        logger.info(
            "Coordinate CSV ingest finished rows_read=%s rows_ingested=%s duplicates_removed=%s",
            rows_read,
            rows_ingested,
            duplicates_removed,
        )
        return rows_ingested

    def _save_batch_resilient(self, rows: list[dict[str, str]], *, attempt: int = 1) -> int:
        try:
            with self.engine.begin() as conn:
                return _save_batch(conn, rows)
        except DBAPIError as exc:
            if not _is_transient_database_error(exc):
                raise

            logger.warning(
                "Transient coordinate batch ingest failure rows=%s attempt=%s/%s error=%s",
                len(rows),
                attempt,
                MAX_TRANSIENT_RETRIES,
                exc,
            )
            self.engine.dispose()

            if len(rows) > MIN_ADAPTIVE_CHUNK_SIZE:
                midpoint = len(rows) // 2
                logger.warning(
                    "Retrying coordinate batch with adaptive split original_rows=%s left_rows=%s right_rows=%s",
                    len(rows),
                    midpoint,
                    len(rows) - midpoint,
                )
                return self._save_batch_resilient(rows[:midpoint], attempt=1) + self._save_batch_resilient(
                    rows[midpoint:],
                    attempt=1,
                )

            if attempt < MAX_TRANSIENT_RETRIES:
                sleep_seconds = min(30, attempt * 5)
                logger.warning(
                    "Retrying coordinate batch after reconnect rows=%s sleep_seconds=%s",
                    len(rows),
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                return self._save_batch_resilient(rows, attempt=attempt + 1)

            raise


def read_coordinate_csv(path: Path) -> list[dict[str, str]]:
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Coordinate CSV not found: {resolved_path}")
    with resolved_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    logger.info("Read coordinate CSV path=%s rows=%s", resolved_path, len(rows))
    return rows


def _normalize_coordinate_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        normalized_row = _normalize_coordinate_row(row)
        instalacao = normalized_row["instalacao"]
        if not instalacao or instalacao in seen:
            continue
        seen.add(instalacao)
        normalized.append(normalized_row)
    return normalized


def _chunked(values: list[Any], *, size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _normalize_coordinate_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "instalacao": str(row.get("instalacao", "") or "").strip(),
        "lat_da_instalacao": str(row.get("lat_da_instalacao", "") or "").strip(),
        "lng_da_instalacao": str(row.get("lng_da_instalacao", "") or "").strip(),
        "lat_da_leitura": str(row.get("lat_da_leitura", "") or "").strip(),
        "lng_da_leitura": str(row.get("lng_da_leitura", "") or "").strip(),
    }


def _save_batch(conn: Any, rows: list[dict[str, str]]) -> int:
    keys = [row["instalacao"] for row in rows]
    for chunk in _chunked(keys, size=1000):
        conn.execute(delete(instalacoes_coordenadas_sp).where(instalacoes_coordenadas_sp.c.instalacao.in_(chunk)))
    conn.execute(insert(instalacoes_coordenadas_sp), rows)
    return len(rows)


def _is_transient_database_error(exc: DBAPIError) -> bool:
    if getattr(exc, "connection_invalidated", False):
        return True
    text = str(exc).upper()
    transient_tokens = (
        "08S01",
        "COMMUNICATION LINK FAILURE",
        "FALHA DE VÍNCULO DE COMUNICAÇÃO",
        "FALHA DE VINCULO DE COMUNICACAO",
        "HYT00",
        "HYT01",
        "TIMEOUT",
        "DEADLOCK",
        "40001",
    )
    return any(token in text for token in transient_tokens)
