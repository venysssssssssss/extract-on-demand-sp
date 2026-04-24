from __future__ import annotations

import csv
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Column, MetaData, String, Table, create_engine, delete, insert

logger = logging.getLogger("sap_automation.lotes_sp_lat_lng_repository")

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


class LotesLatLngRepository:
    def __init__(self, engine_url: str) -> None:
        self.engine = create_engine(engine_url)
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
        instalacao = str(row.get("instalacao", "") or "").strip()
        if not instalacao or instalacao in seen:
            continue
        seen.add(instalacao)
        normalized.append(
            {
                "instalacao": instalacao,
                "lat_da_instalacao": str(row.get("lat_da_instalacao", "") or "").strip(),
                "lng_da_instalacao": str(row.get("lng_da_instalacao", "") or "").strip(),
                "lat_da_leitura": str(row.get("lat_da_leitura", "") or "").strip(),
                "lng_da_leitura": str(row.get("lng_da_leitura", "") or "").strip(),
            }
        )
    return normalized


def _chunked(values: list[Any], *, size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
