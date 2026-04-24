from __future__ import annotations

import csv
import logging
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Column, MetaData, String, Table, create_engine, delete, insert, select

logger = logging.getLogger("sap_automation.medidor_repository")

metadata = MetaData()

sm_dados_medidor_sp = Table(
    "SM_DADOS_MEDIDOR_SP",
    metadata,
    Column("num_instalacao", String(128), primary_key=True),
    Column("tp_medidor", String(128)),
    extend_existing=True,
)


@dataclass(frozen=True)
class MedidorIngestResult:
    run_id: str
    status: str
    rows_ingested: int
    source_csv_path: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MedidorRepository:
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
        self.table_reincidencia_name = os.environ.get("ENEL_SQL_TABLE", "TBL_REINCIDENCIA_SM")
        logger.info("Initialized MEDIDOR repository dialect=%s", self.engine.dialect.name)

    def get_installations_by_alimentador(
        self,
        *,
        distribuidora: str = "São Paulo",
        source_column: str = "ALIMENTADOR",
    ) -> list[str]:
        table = Table(
            self.table_reincidencia_name,
            MetaData(),
            Column(source_column, String(128)),
            Column("DISTRIBUIDORA", String(128)),
        )
        query = select(table.c[source_column]).where(table.c["DISTRIBUIDORA"] == distribuidora)
        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()

        seen: set[str] = set()
        installations: list[str] = []
        for row in rows:
            token = str(row[0] or "").strip()
            if token and token not in seen:
                seen.add(token)
                installations.append(token)
        return installations

    def save_meter_types(self, rows: list[dict[str, Any]]) -> int:
        normalized_rows = _normalize_medidor_rows(rows)
        metadata.create_all(self.engine, tables=[sm_dados_medidor_sp])
        with self.engine.begin() as conn:
            if normalized_rows:
                keys = [row["num_instalacao"] for row in normalized_rows]
                deleted_rows = 0
                for chunk in _chunked(keys, size=1000):
                    result = conn.execute(
                        delete(sm_dados_medidor_sp).where(sm_dados_medidor_sp.c.num_instalacao.in_(chunk))
                    )
                    deleted_rows += int(result.rowcount or 0)
                logger.info("Deleted existing MEDIDOR rows count=%s", deleted_rows)
                for chunk in _chunked(normalized_rows, size=1000):
                    conn.execute(insert(sm_dados_medidor_sp), chunk)
        logger.info("Saved %d rows to SM_DADOS_MEDIDOR_SP", len(normalized_rows))
        return len(normalized_rows)


def _chunked(values: list[Any], *, size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _normalize_medidor_rows(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        num_instalacao = str(row.get("num_instalacao") or row.get("instalacao") or "").strip()
        tp_medidor = str(row.get("tp_medidor") or row.get("tipo") or "").strip()
        if not num_instalacao or num_instalacao in seen:
            continue
        seen.add(num_instalacao)
        normalized.append({"num_instalacao": num_instalacao, "tp_medidor": tp_medidor})
    return normalized


def read_medidor_final_csv(path: Path) -> list[dict[str, str]]:
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"MEDIDOR final CSV not found: {resolved_path}")
    with resolved_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]
