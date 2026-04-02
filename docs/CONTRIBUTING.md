# Contributing

## Prerequisites

- Python 3.12+
- [Poetry](https://python-poetry.org/) for dependency management
- Windows with SAP GUI installed (for SAP integration testing)
- PostgreSQL + Redis (for control plane features)

## Setup

```bash
# Clone and install dependencies
git clone <repo-url>
cd extract-on-demand-sp
poetry install

# Copy environment template
cp .env.example .env
# Fill in SAP credentials and control plane URLs in .env
```

## Available Commands

<!-- AUTO-GENERATED:START -->

| Command | Description |
|---------|-------------|
| `poetry install` | Install all dependencies (main + dev) |
| `pytest` | Run full test suite |
| `pytest tests/test_batch.py -v` | Run a specific test file |
| `pytest -k "consolidate" -v` | Run tests matching a pattern |
| `python3 sap_iw69_batch.py --run-id <id> --reference <ref> --from-date <date> --demandante IGOR --output-root output` | IW69 batch extraction (Windows) |
| `python3 sap_iw51_dani.py --run-id <id> --demandante DANI --output-root output` | IW51 DANI workbook flow (Windows) |
| `python3 sap_dw.py --run-id <id> --demandante DW --output-root output` | DW complaints observation flow (Windows) |
| `uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000` | Start FastAPI server |

<!-- AUTO-GENERATED:END -->

## Testing

All tests live in `tests/`. Run with:

```bash
# Full suite
pytest

# Specific module
pytest tests/test_iw59.py -v

# Pattern match
pytest -k "igor" -v
```

Tests mock SAP GUI interactions — no SAP connection needed to run them.

**Note:** `test_login.py` and `test_logon.py` have pre-existing failures related to SAP COM mocking on non-Windows environments. These are expected on Linux.

## Code Style

- Frozen dataclasses for all contracts (`@dataclass(frozen=True)`)
- Type hints everywhere
- Use `demandante` terminology consistently (not "requester" or "user")
- SOLID principles, high modularization
- No unnecessary abstractions — simple > clever

## PR Checklist

- [ ] All existing tests pass (`pytest`)
- [ ] New functionality has tests
- [ ] CLAUDE.md updated if architecture changed
- [ ] Config changes reflected in `sap_iw69_batch_config.json`
- [ ] No secrets in committed files
