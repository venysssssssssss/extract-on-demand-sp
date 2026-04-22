from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import Column, Date, Integer, MetaData, String, Table, Text, create_engine, extract, insert, select
from sqlalchemy.orm import Session

logger = logging.getLogger("sap_automation.sm_repository")

# Schema definitions for existing tables
metadata = MetaData()

tbl_reincidencia_sm = Table(
    "TBL_REINCIDENCIA_SM",
    metadata,
    Column("ID_RECLAMAÇÃO", String(64), primary_key=True),
    Column("distribuidora", String(128)),
    Column("data", Date),
    # Add other columns as needed or use reflect=True if connected to DB
)

sm_dados_fatura = Table(
    "SM_DADOS_FATURA",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("row_id", Integer, primary_key=True, autoincrement=True),
    Column("data_extracao", Date),
    Column("payload_json", Text),
    # This table will store the consolidated results from both SQVIs
)


class SmRepository:
    def __init__(self, engine_url: str) -> None:
        self.engine = create_engine(engine_url)
        # In a real scenario, we might want to use metadata.reflect(bind=self.engine)
        # to ensure we have the exact schema, but here we'll define minimal columns.

    def get_installations_to_process(
        self,
        month: int | None = None,
        year: int | None = None,
        distribuidora: str = "São Paulo",
    ) -> list[str]:
        """Fetch ID_RECLAMAÇÃO from TBL_REINCIDENCIA_SM for the given month/year."""
        today = date.today()
        target_month = month or today.month
        target_year = year or today.year

        query = select(tbl_reincidencia_sm.c["ID_RECLAMAÇÃO"]).where(
            tbl_reincidencia_sm.c.distribuidora == distribuidora,
            extract("month", tbl_reincidencia_sm.c.data) == target_month,
            extract("year", tbl_reincidencia_sm.c.data) == target_year,
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
