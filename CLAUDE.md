# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAP GUI batch extraction automation focused on IW69 today, with IW59 and IW67 treated as the next integration targets. The current implementation orchestrates extraction of three IW69 object types (CA, RL, WB), consolidates results into normalized CSVs, and exposes both CLI and FastAPI REST endpoints.

**Critical external dependency:** `exemplo_sap_gui_export` — a pywin32-based SAP GUI automation module expected on PYTHONPATH (not in this repo). It provides `load_config()`, `connect_sap_session()`, `run_steps()`, and recovery functions used by `legacy_runner.py` and `execution.py`.

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
python3 sap_iw69_batch.py --run-id 20260310T090000 --reference 202603 --from-date 2026-01-01 --output-root output

# Start API server
uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000
```

## Architecture

**Entry points:** `sap_iw69_batch.py` (CLI) and `sap_automation/api.py` (FastAPI).

**Core flow:** `BatchRunPayload` → `BatchOrchestrator` → per-object `LegacyExportService.execute()` → `ObjectManifest` results → `Consolidator` merges by "nota" key → `BatchManifest` persisted via `ArtifactStore`.

**Key modules:**
- `contracts.py` — All data structures (frozen dataclasses): `ExportJobSpec`, `BatchRunPayload`, `ObjectManifest`, `ConsolidationManifest`, `BatchManifest`, `ObjectArtifactPaths`
- `batch.py` — `BatchOrchestrator`: creates job specs, runs them sequentially (shared SAP session), consolidates
- `consolidation.py` — `Consolidator`: merges CA/RL/WB CSVs, deduplicates by "nota", outputs `notes.csv` + `interactions.csv`
- `legacy_runner.py` — `LegacyExportService`: wraps `exemplo_sap_gui_export` for SAP GUI step execution
- `integrations.py` — Placeholder adapters for IW59 and IW67 (both currently `pending_configuration`)
- `service.py` — Factory functions for creating orchestrator and loading manifests

**Design decisions:**
- **Partial success:** If one object fails, others still consolidate (status = "partial")
- **Shared SAP session:** Objects execute sequentially to reuse one SAP GUI connection
- **Legacy compatibility:** Raw exports copied to `output/latest/legacy/BASE_AUTOMACAO_*.txt`
- **Frozen dataclasses:** All contracts are immutable for thread-safety

**SAP config:** `sap_iw69_batch_config.json` defines per-object step sequences with template variables (`{transaction_code}`, `{iw69_from_date_dmy}`, `{raw_dir}`, etc.).

**Logon pad support:** when `global.logon_pad.enabled = true`, the session provider changes from the legacy COM-by-index flow to the new logon-pad flow using `.env` credentials and named connection opening.

## Packages

Two packages defined in `pyproject.toml`:
- `sap_automation` — Core extraction/orchestration library
- `elt_enel_sftp` — ELT utilities with SAP header field mapping profiles (`header_profiles.py`)
