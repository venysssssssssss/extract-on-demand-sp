# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAP GUI batch extraction automation for Enel SP. Orchestrates extraction of IW69 object types (CA, RL, WB), runs IW59 chunked extraction as a post-processing stage, provides IW51 workbook automation for DANI, provides DW observation scraping from a complaints CSV, consolidates results into normalized CSVs, and exposes both CLI and FastAPI REST endpoints.

**Critical desktop integration module:** `sap_gui_export_compat` — a pywin32-based SAP GUI automation compatibility module shipped in this repo. Imported dynamically via `importlib.import_module("sap_gui_export_compat")`. Provides `load_config()`, `connect_sap_session()`, `run_steps()`, and recovery functions used by `legacy_runner.py` and `execution.py`.

## Commands

```bash
# Install
poetry install

# Run all tests
pytest

# Run single test file or pattern
pytest tests/test_batch.py -v
pytest -k "consolidate" -v

# CLI batch extraction (Windows only — requires SAP GUI)
python3 sap_iw69_batch.py --run-id 20260310T090000 --reference 202603 --from-date 2026-01-01 --demandante IGOR --output-root output

# IW51 DANI flow (Windows only — requires SAP GUI + openpyxl)
python3 sap_iw51_dani.py --run-id 20260326T090000 --demandante DANI --output-root output

# Start API server
uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000
```

## Architecture

**Entry points:** `sap_iw69_batch.py` (IW69 CLI), `sap_iw51_dani.py` (IW51 CLI), `sap_dw.py` (DW CLI), and `sap_automation/api.py` (FastAPI with IW69, IW51, IW59 and DW endpoints).

**Core flow (IW69):** `BatchRunPayload` → `BatchOrchestrator` → per-object `LegacyExportService.execute()` → `ObjectManifest` results → `Consolidator` merges by "nota" key → if all IW69 objects succeed, `Iw59ExportAdapter.execute()` runs chunked IW59 extraction → `BatchManifest` persisted via `ArtifactStore`.

**IW51 flow:** `sap_iw51_dani.py` or `POST /api/v1/extractions/iw51` → `run_iw51_demandante()` reads Excel workbook → for each pending row, `execute_iw51_item()` drives IW51 transaction in SAP → marks `FEITO=SIM` in workbook → `Iw51Manifest` persisted.

**Key modules:**
- `contracts.py` — Frozen dataclasses: `ExportJobSpec`, `BatchRunPayload`, `ObjectManifest`, `ConsolidationManifest`, `BatchManifest`, `ObjectArtifactPaths`
- `batch.py` — `BatchOrchestrator`: runs IW69 jobs sequentially, consolidates, then triggers IW59. Respects `stop_on_object_failure` config flag
- `consolidation.py` — `Consolidator`: merges CA/RL/WB CSVs, deduplicates by "nota", outputs `notes.csv` + `interactions.csv`
- `legacy_runner.py` — `LegacyExportService`: wraps `sap_gui_export_compat` for SAP GUI step execution. `resolve_iw69_date_range()` must respect the exact `from_date`/`to_date` supplied in the request
- `iw59.py` — `Iw59ExportAdapter`: collects notes from CA CSV, chunks them, runs IW59 extraction per chunk via clipboard-based multi-select, concatenates outputs
- `iw51.py` — IW51 workbook-driven execution module for demandante `DANI`; reads `projeto_Dani2.xlsm`, applies SAP creation flow row by row, and writes `FEITO=SIM`. Contains `Iw51Settings`, `Iw51WorkItem`, `Iw51Manifest` dataclasses
- `dw.py` — DW complaints CSV flow; reads `ID Reclamação` from `BASE RECLAMAÇÕES 2026- ATUALIZADO(BASE) (1)(1).csv`, opens 3 SAP sessions, scrapes tab `TAB02` text, and writes `OBSERVAÇÃO` back to the same CSV
- `sap_helpers.py` — Shared SAP GUI scripting helpers: `set_text()`, `set_selected()`, `resolve_first_existing()`, `set_first_existing_text()`, `wait_for_file()`, `wait_not_busy()`. Used by `iw51.py` and `iw59.py` to avoid code duplication
- `integrations.py` — Re-exports `Iw59ExportAdapter`; placeholder `Iw67ExportAdapter` (`pending_configuration`)
- `service.py` — Factory functions: `create_session_provider(config)` selects provider based on `logon_pad.enabled`, `create_batch_orchestrator()` wires all dependencies, `run_iw51_payload()`, `run_iw59_payload()` for standalone execution
- `api.py` — FastAPI with IW69, IW51, IW59 and DW endpoints (run, manifest, curl for each flow)
- `api_models.py` — Pydantic models: `BatchRunRequest`, `Iw51RunRequest`, `Iw59RunRequest`, `DwRunRequest`, response models

**Session lifecycle (logon pad flow):**
- `errors.py` — Exception hierarchy: `SapAutomationError` → `SapLogonPadError`, `SapLoginError`, `SapCredentialsError` with specific subtypes
- `credentials.py` — `CredentialsLoader` loads `SAP_USERNAME`/`SAP_PASSWORD` from `.env` via `python-dotenv`. Also accepts `SAP_USER`/`SAP_PASS` as fallbacks
- `logon.py` — `SapApplicationProvider` obtains COM Application object; `SapConnectionOpener` opens connection by description string; `SapLogonPadUiOpener` provides `pywinauto` UI fallback
- `login.py` — `SapLoginHandler` fills login screen fields, handles multiple-logon popup, verifies login success
- `execution.py` — `SessionProvider` Protocol with two implementations: `SapSessionProvider` (legacy COM-by-index) and `LogonPadSessionProvider` (full logon pad automation). `StepExecutor` wraps `run_steps()`
- `runtime_logging.py` — Per-run dual logger (stdout + file) at `output/runs/{run_id}/logs/sap_session.log`

**Design decisions:**
- **Partial success:** If one object fails, others still consolidate (status = "partial"). Configurable `stop_on_object_failure` can halt the batch early
- **Shared SAP session:** Objects execute sequentially reusing one SAP GUI connection
- **Legacy compatibility:** Raw exports copied to `output/latest/legacy/BASE_AUTOMACAO_*.txt`
- **Frozen dataclasses:** All contracts are immutable for thread-safety
- **SessionProvider abstraction:** `logon_pad.enabled` flag in config toggles between legacy COM-by-index and full logon pad automation — existing behavior untouched when disabled
- **IW59 chunking:** Large note sets split into configurable chunks (default 20k) to avoid SAP timeouts. Notes pasted via clipboard into SAP multi-select dialog

**SAP config:** `sap_iw69_batch_config.json` defines per-object step sequences with template variables (`{transaction_code}`, `{iw69_from_date_dmy}`, `{raw_dir}`, etc.). Also contains `global.logon_pad` section for connection automation, `iw59` settings with demandante-specific overrides, `iw51` settings for the DANI workbook flow, and `dw` settings for the complaints-observation flow.

**Demandante terminology:** use `demandante` everywhere in contracts, config and API. Current supported demandantes are `IGOR`, `MANU`, `DANI` and `DW`. `MANU` filters `IW59` input to `CA` notes with `statusuar=ENCE`; `IW69` must use the exact `from_date`/`to_date` supplied in the request.

**IW51 DANI flow:** uses fixed `RIWO00-QWRNUM=389496787`, reads `PN`, `INSTALAÇÃO` and `TIPOLOGIA` from `projeto_Dani2.xlsm`, processes pending rows sequentially, waits 30 seconds between successful items, and marks `FEITO=SIM` directly in the workbook after each completed SAP save.

**Credentials:** `.env` file with `SAP_USERNAME`, `SAP_PASSWORD`, `SAP_CLIENT`, `SAP_LANGUAGE`. See `.env.example` for template. Never committed to git.

## Packages

Two packages defined in `pyproject.toml`:
- `sap_automation` — Core extraction/orchestration library
- `elt_enel_sftp` — ELT utilities with SAP header field mapping profiles (`header_profiles.py`)
