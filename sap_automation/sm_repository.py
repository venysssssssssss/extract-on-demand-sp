from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import Column, Date, Integer, MetaData, String, Table, Text, create_engine, extract, insert, select
from sqlalchemy.orm import Session

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
    Column("run_id", String(64), primary_key=True),
    Column("row_id", Integer, primary_key=True, autoincrement=True),
    Column("data_extracao", Date),
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
            return [str(row[0]) for row in results if row[0]]

    def save_final_results(self, run_id: str, results: list[dict[str, Any]]) -> None:
        """Persist consolidated SQVI results into SM_DADOS_FATURA."""
        if not results:
            return

        import json
        from datetime import datetime

        now = datetime.now()
        rows_to_insert = [
            {
                "run_id": run_id,
                "data_extracao": now,
                "payload_json": json.dumps(res, ensure_ascii=False),
            }
            for res in results
        ]

        # Ensure table exists
        metadata.create_all(self.engine, tables=[sm_dados_fatura])

        with self.engine.begin() as conn:
            conn.execute(insert(sm_dados_fatura), rows_to_insert)
            logger.info("Saved %d results to SM_DADOS_FATURA for run_id=%s", len(results), run_id)
