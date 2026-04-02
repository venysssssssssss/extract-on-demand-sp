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
```

### Windows Runner

The runner polls Redis for queued jobs and executes them on the local SAP GUI:

```bash
# Set SAP_RUNNER_ID in .env to identify this runner
python3 -m sap_automation.runner
```

## Health Check

```bash
curl http://localhost:8000/health
# Returns: {"status": "ok", "runner_id": "...", ...}
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

### Schedules & Runners

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/schedules` | List schedules |
| `POST` | `/api/v1/schedules` | Upsert schedule |
| `GET` | `/api/v1/runners` | List runners |
| `GET` | `/api/v1/runners/{runner_id}` | Get runner detail |

<!-- AUTO-GENERATED:END -->

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

**Expected.** These tests require Windows COM/SAP GUI mocking. The 16 failures in `test_login.py` and `test_logon.py` are pre-existing on non-Windows environments. All other tests (126+) should pass.

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
├── runs/{run_id}/
│   ├── CA/, RL/, WB/          # IW69 per-object raw + normalized
│   ├── iw59/
│   │   ├── raw/               # Per-chunk .txt exports
│   │   ├── normalized/        # Combined CSV per source object
│   │   └── metadata/          # Manifest JSON per source object
│   ├── consolidated/          # notes.csv, interactions.csv
│   ├── logs/sap_session.log   # Full SAP session log
│   └── batch_manifest.json    # Overall batch result
└── latest/legacy/             # BASE_AUTOMACAO_*.txt symlinks
```
