#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo ".env not found. Copy .env.example to .env first." >&2
  exit 2
fi

run_id="${1:-}"
if [[ -z "${run_id}" ]]; then
  echo "usage: $0 <run_id>" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

month="${SM_MONTH_OVERRIDE:-4}"
year="${SM_YEAR_OVERRIDE:-2026}"
distribuidora="${SM_DISTRIBUIDORA:-São Paulo}"
final_csv_path="/app/output/runs/${run_id}/sm/SM_DADOS_FATURA.csv"

docker compose exec -T db-runner python - "$run_id" "$month" "$year" "$distribuidora" "$final_csv_path" <<'PY'
import json
import os
import sys
import time
import urllib.parse
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from sap_automation.sm import ingest_sm_results

run_id = sys.argv[1]
month = int(sys.argv[2])
year = int(sys.argv[3])
distribuidora = sys.argv[4]
final_csv_path = Path(sys.argv[5])

host = os.environ["ENEL_SQL_HOST"]
user = os.environ["ENEL_SQL_USER"]
password = os.environ["ENEL_SQL_PASSWORD"]
database = os.environ.get("ENEL_SQL_DATABASE", "ENEL")
port = os.environ.get("ENEL_SQL_PORT", "1433")
driver = os.environ.get("ENEL_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

params = urllib.parse.quote_plus(
    f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
    f"UID={user};PWD={password};TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

report: dict[str, object] = {
    "run_id": run_id,
    "final_csv_path": str(final_csv_path),
    "final_csv_exists": final_csv_path.exists(),
    "sql_connectivity": {},
    "sql_context": {},
    "rows_for_run_id_before": None,
    "ingest_result": None,
    "rows_for_run_id_after": None,
}

t0 = time.time()
with engine.connect() as conn:
    report["sql_connectivity"] = {
        "select_1": conn.execute(text("SELECT 1")).scalar(),
        "elapsed_seconds": round(time.time() - t0, 2),
    }
    row = conn.execute(
        text("SELECT DB_NAME() AS db_name, SUSER_SNAME() AS login_name, SCHEMA_NAME() AS schema_name")
    ).mappings().one()
    report["sql_context"] = dict(row)
    try:
        report["rows_for_run_id_before"] = conn.execute(
            text("SELECT COUNT(*) FROM SM_DADOS_FATURA WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar()
    except SQLAlchemyError as exc:
        report["rows_for_run_id_before"] = {
            "status": "query_error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

try:
    result = ingest_sm_results(
        run_id=run_id,
        output_root=Path("/app/output"),
        config_path=Path("sap_iw69_batch_config.json"),
        final_csv_path=final_csv_path,
        month=month,
        year=year,
        distribuidora=distribuidora,
    )
    report["ingest_result"] = result.to_dict()
except Exception as exc:  # pragma: no cover - diagnosis path
    report["ingest_result"] = {
        "status": "exception",
        "error_type": type(exc).__name__,
        "error": str(exc),
    }

with engine.connect() as conn:
    try:
        report["rows_for_run_id_after"] = conn.execute(
            text("SELECT COUNT(*) FROM SM_DADOS_FATURA WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar()
    except SQLAlchemyError as exc:
        report["rows_for_run_id_after"] = {
            "status": "query_error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

print(json.dumps(report, ensure_ascii=False, indent=2))
PY
