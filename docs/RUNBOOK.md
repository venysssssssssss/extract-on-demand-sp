# Runbook

## Deployment

### API Server

```bash
# Start the FastAPI server
uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000

# With auto-reload (development)
uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000 --reload
```

### Control Plane (PostgreSQL + Redis)

```bash
# Ensure DATABASE_URL and REDIS_URL are set in .env
# Tables are auto-created on first API startup via SQLAlchemy
docker compose up -d postgres redis api scheduler db-runner
```

On Ubuntu `10.71.202.127`, the Docker control plane owns database access and artifact storage. The compose stack runs:

- `api`: FastAPI control plane and artifact HTTP API on port `8000`.
- `scheduler`: materializes scheduled workflow roots.
- `db-runner`: consumes `db-default` jobs for database and artifact steps.
- `postgres` and `redis`: control-plane state and queues.

Artifacts are stored under `output/artifacts/{run_id}/` on the host bind mount. Run-local working files remain under `output/runs/{run_id}/`.

### Windows Runner

The Windows runner stays outside Docker because SAP GUI scripting requires an interactive Windows desktop. It consumes only SAP jobs from `sap-default`; it must not be configured with database credentials for the distributed SM workflow.

```bash
# Set these in the Windows .env
DATABASE_URL=postgresql+psycopg://sap_automation:sap_automation@10.71.202.127:5432/sap_automation
REDIS_URL=redis://10.71.202.127:6379/0
SAP_QUEUE_NAME=sap-default
SAP_RUNNER_ID=windows-sap-runner-01
SAP_OUTPUT_ROOT=output

python -m sap_automation.runner
```

For SAP artifact transfer through HTTP, workflow payloads can include:

```json
{"control_plane_base_url": "http://10.71.202.127:8000"}
```

The Windows runner then downloads `SM_INSTALLATIONS.csv` and uploads `SM_DADOS_FATURA.csv`, `sm_manifest.json`, and `SM_SQVI*.txt` through the API. Minimum firewall path: Windows must reach `http://10.71.202.127:8000`; Ubuntu does not need SAP GUI or VPN access.

## Health Check

```bash
curl http://localhost:8000/health
# Returns: {"components": {"control_plane": "ok", "runner_count": N, "runners": [...]}}
```

## API Endpoints

<!-- AUTO-GENERATED:START -->

### IW69 (Batch Extraction)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/extractions/iw69/curl` | Curl example builder |
| `POST` | `/api/v1/extractions/iw69` | Synchronous batch run |
| `POST` | `/api/v1/jobs/iw69` | Queue batch job |
| `GET` | `/api/v1/extractions/iw69/{run_id}/manifest` | Retrieve batch manifest |

### IW51 (DANI Workbook)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/extractions/iw51/curl` | Curl example builder |
| `POST` | `/api/v1/extractions/iw51` | Synchronous IW51 run |
| `POST` | `/api/v1/jobs/iw51` | Queue IW51 job |
| `GET` | `/api/v1/extractions/iw51/{run_id}/manifest` | Retrieve IW51 manifest |

### IW59 (Note Extraction)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/extractions/iw59/curl` | Curl example builder |
| `POST` | `/api/v1/extractions/iw59` | Synchronous IW59 run |
| `POST` | `/api/v1/jobs/iw59` | Queue IW59 job |
| `GET` | `/api/v1/extractions/iw59/{run_id}/manifest` | Retrieve IW59 manifest |

### DW (Complaints Observations)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/extractions/dw/curl` | Curl example builder |
| `POST` | `/api/v1/extractions/dw` | Synchronous DW run |
| `POST` | `/api/v1/jobs/dw` | Queue DW job |
| `GET` | `/api/v1/extractions/dw/{run_id}/manifest` | Retrieve DW manifest |

### Job Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/jobs` | List jobs (limit 1-500, default 100) |
| `GET` | `/api/v1/jobs/{job_id}` | Get job detail |
| `POST` | `/api/v1/jobs/{job_id}/cancel` | Cancel a job |

### Workflows & Artifacts

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/workflows` | Create a workflow run, currently `sm_sala_mercado_daily` |
| `GET` | `/api/v1/workflows` | List workflow runs |
| `GET` | `/api/v1/workflows/{run_id}` | Get workflow steps and artifacts |
| `POST` | `/api/v1/artifacts/{run_id}/{artifact_name}` | Upload raw bytes for an artifact |
| `GET` | `/api/v1/artifacts/{run_id}` | List artifacts for a run |
| `GET` | `/api/v1/artifacts/{run_id}/{artifact_name}` | Download an artifact |

### Schedules & Runners

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/schedules` | List schedules |
| `POST` | `/api/v1/schedules` | Upsert schedule |
| `GET` | `/api/v1/runners` | List runners |
| `GET` | `/api/v1/runners/{runner_id}` | Get runner detail |

<!-- AUTO-GENERATED:END -->

## Supported Demandantes

| Demandante | Flows | Description |
|------------|-------|-------------|
| `IGOR` | IW69 + IW59 | CA/RL/WB extraction, IW59 with 6 ENCE* status filter (excludes plain `ENCE`), chunk_size 5000, 5th business day date transition |
| `MANU` | IW69 + IW59 | Inherits IGOR IW69 config, overrides CA OTEIL/OTGRP, IW59 with 7 ENCE* statuses (includes plain `ENCE`), chunk_size 5000 |
| `DANI` | IW51 | Workbook-driven (`projeto_Dani2.xlsm`), 3 SAP sessions, `process_parallel` mode, ledger-based resumable progress |
| `DW` | DW | Complaints CSV observation scraping, 3 SAP sessions, parallel mode with worker-session affinity |
| `KELLY` | IW59 standalone | BRS-based extraction from `brs_filtrados.csv`, multi-selects on `AENAM` field, chunk_size 100, modified date windows with 5th business day transition |
| `SALA_MERCADO` | SM workflow | Distributed DB/SAP/DB flow: `sm_prepare_input` on `db-default`, `sm_sap_extract` on `sap-default`, `sm_ingest_final` on `db-default` |

## SM/SALA_MERCADO Workflow

The default schedule `sm-sala-mercado-diario-1130` creates `workflow:sm_sala_mercado_daily` at `30 11 * * *` in `America/Bahia`.

Step order:

1. `sm_prepare_input`: Ubuntu `db-runner` queries `TBL_REINCIDENCIA_SM`, writes `SM_INSTALLATIONS.csv`, and registers it as an artifact.
2. `sm_sap_extract`: Windows runner downloads `SM_INSTALLATIONS.csv`, runs SAP with `skip_ingest=true`, and uploads `SM_DADOS_FATURA.csv`, `sm_manifest.json`, `SM_SQVI1_*.txt`, and `SM_SQVI2_*.txt`.
3. `sm_ingest_final`: Ubuntu `db-runner` reads `SM_DADOS_FATURA.csv` and writes `SM_DADOS_FATURA` with `referencia` as `YYYYMM`.

If a step fails, the workflow is marked `failed` and the next step is not enqueued.

## Common Issues

### IW59 pastes only 1 note on Windows 10

**Symptom:** Clipboard multi-select inserts only the first note instead of all 5000.

**Cause:** Windows 10 clipboard API (`win32clipboard.CF_UNICODETEXT`) truncates large payloads when consumed by SAP GUI scripting.

**Fix:** The application auto-detects this (clipboard verification + post-paste entry count check) and falls back to file upload via `btn[23]`. No manual intervention needed. Check logs for `falling back to file upload`.

### SAP session timeout during IW59 chunking

**Symptom:** SAP disconnects mid-extraction.

**Fix:** Reduce `chunk_size` in `sap_iw69_batch_config.json` (e.g. from 5000 to 2000). The adapter will create more chunks but each completes faster.

### IW51 worker hangs

**Symptom:** A process_parallel worker stops processing items.

**Fix:** The `worker_item_timeout_seconds` setting will abort hung workers. Check `sap_session.log` for the last action. Recovery can rebuild the ledger from logs.

### Tests fail on Linux (test_login / test_logon)

These tests exercise Windows COM/SAP GUI edge cases and may fail on non-Windows environments depending on the mocked COM behavior. Validate changed paths first with:

```bash
poetry run pytest tests/test_control_plane.py tests/test_api_endpoints.py tests/test_sm.py
```

## Rollback

```bash
# Revert to previous commit
git log --oneline -5  # find the target commit
git checkout <commit-hash> -- sap_automation/ sap_iw69_batch_config.json

# Re-run tests
pytest
```

## Artifact Structure

```
output/
├── artifacts/{run_id}/        # HTTP artifact exchange between Ubuntu and Windows
│   ├── SM_INSTALLATIONS.csv
│   ├── SM_DADOS_FATURA.csv
│   ├── sm_manifest.json
│   └── SM_SQVI*.txt
├── runs/{run_id}/
│   ├── CA/, RL/, WB/          # IW69 per-object raw + normalized
│   ├── sm/
│   │   ├── input/SM_INSTALLATIONS.csv
│   │   └── SM_DADOS_FATURA.csv
│   ├── iw59/
│   │   ├── raw/               # Per-chunk .txt exports
│   │   ├── normalized/        # Combined CSV per source object
│   │   └── metadata/          # Manifest JSON per source object
│   ├── consolidated/          # notes.csv, interactions.csv
│   ├── logs/sap_session.log   # Full SAP session log
│   └── batch_manifest.json    # Overall batch result
└── latest/legacy/             # BASE_AUTOMACAO_*.txt symlinks
```
