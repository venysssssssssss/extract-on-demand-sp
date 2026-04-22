from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table, Text, create_engine, delete, extract, insert, inspect, select, text

logger = logging.getLogger("sap_automation.sm_repository")

# Schema definitions for existing tables
metadata = MetaData()

def get_reincidencia_table(table_name: str) -> Table:
    return Table(
        table_name,
        metadata,
        Column("ID_RECLAMAÇÃO", String(64), primary_key=True),
        Column("distribuidora", String(128)),
        Column("data", Date),
        extend_existing=True,
    )

sm_dados_fatura = Table(
    "SM_DADOS_FATURA",
    metadata,
    Column("row_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("data_extracao", DateTime, nullable=False),
    Column("mes_referencia", Integer),
    Column("ano_referencia", Integer),
    Column("distribuidora", String(128)),
    Column("chunk_index", Integer),
    Column("nota", String(128)),
    Column("doc_impr", String(128)),
    Column("montante", String(128)),
    Column("dt_fx_calc_fat", String(128)),
    Column("extraction_status", String(64)),
    Column("sqvi1_payload_json", Text),
    Column("sqvi2_payload_json", Text),
    Column("payload_json", Text),
    extend_existing=True,
)


class SmRepository:
    def __init__(self, engine_url: str) -> None:
        self.engine = create_engine(engine_url)
        import os
        self.table_reincidencia_name = os.environ.get("ENEL_SQL_TABLE", "TBL_REINCIDENCIA_SM")
        self.tbl_reincidencia = get_reincidencia_table(self.table_reincidencia_name)

    def get_installations_to_process(
        self,
        month: int | None = None,
        year: int | None = None,
        distribuidora: str = "São Paulo",
    ) -> list[str]:
        """Fetch ID_RECLAMAÇÃO from the reincidencia table for the given month/year."""
        today = date.today()
        target_month = month or today.month
        target_year = year or today.year

        query = select(self.tbl_reincidencia.c["ID_RECLAMAÇÃO"]).where(
            self.tbl_reincidencia.c.distribuidora == distribuidora,
            extract("month", self.tbl_reincidencia.c.data) == target_month,
            extract("year", self.tbl_reincidencia.c.data) == target_year,
        )

        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            seen: set[str] = set()
            installations: list[str] = []
            for row in results:
                token = str(row[0] or "").strip()
                if token and token not in seen:
                    seen.add(token)
                    installations.append(token)
            return installations

    def save_final_results(
        self,
        run_id: str,
        results: list[dict[str, Any]],
        *,
        month: int | None = None,
        year: int | None = None,
        distribuidora: str = "São Paulo",
    ) -> None:
        """Persist consolidated SQVI results into SM_DADOS_FATURA."""
        now = datetime.now()
        rows_to_insert = [
            {
                "run_id": run_id,
                "data_extracao": now,
                "mes_referencia": month,
                "ano_referencia": year,
                "distribuidora": distribuidora,
                "chunk_index": _as_int_or_none(res.get("chunk_index")),
                "nota": _as_text(res.get("nota")),
                "doc_impr": _as_text(res.get("doc_impr")),
                "montante": _as_text(res.get("montante")),
                "dt_fx_calc_fat": _as_text(res.get("dt_fx_calc_fat")),
                "extraction_status": _as_text(res.get("extraction_status")) or "success",
                "sqvi1_payload_json": json.dumps(res.get("sqvi1") or {}, ensure_ascii=False, sort_keys=True),
                "sqvi2_payload_json": json.dumps(res.get("sqvi2") or {}, ensure_ascii=False, sort_keys=True),
                "payload_json": json.dumps(res, ensure_ascii=False),
            }
            for res in results
        ]

        # Ensure table exists
        metadata.create_all(self.engine, tables=[sm_dados_fatura])
        self._ensure_final_table_columns()

        with self.engine.begin() as conn:
            conn.execute(delete(sm_dados_fatura).where(sm_dados_fatura.c.run_id == run_id))
            if rows_to_insert:
                conn.execute(insert(sm_dados_fatura), rows_to_insert)
            logger.info("Saved %d results to SM_DADOS_FATURA for run_id=%s", len(results), run_id)

    def _ensure_final_table_columns(self) -> None:
        inspector = inspect(self.engine)
        existing_columns = {
            column["name"].lower()
            for column in inspector.get_columns("SM_DADOS_FATURA")
        }
        dialect = self.engine.dialect.name
        text_type = "NVARCHAR(MAX)" if dialect == "mssql" else "TEXT"
        string_type = "NVARCHAR(128)" if dialect == "mssql" else "VARCHAR(128)"
        column_sql = {
            "mes_referencia": "INTEGER",
            "ano_referencia": "INTEGER",
            "distribuidora": string_type,
            "chunk_index": "INTEGER",
            "nota": string_type,
            "doc_impr": string_type,
            "montante": string_type,
            "dt_fx_calc_fat": string_type,
            "extraction_status": "NVARCHAR(64)" if dialect == "mssql" else "VARCHAR(64)",
            "sqvi1_payload_json": text_type,
            "sqvi2_payload_json": text_type,
        }
        missing_columns = [
            (column_name, sql_type)
            for column_name, sql_type in column_sql.items()
            if column_name.lower() not in existing_columns
        ]
        if not missing_columns:
            return
        with self.engine.begin() as conn:
            for column_name, sql_type in missing_columns:
                conn.execute(text(f"ALTER TABLE SM_DADOS_FATURA ADD {column_name} {sql_type} NULL"))
        logger.info(
            "Added missing SM_DADOS_FATURA columns: %s",
            ", ".join(column_name for column_name, _ in missing_columns),
        )


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
