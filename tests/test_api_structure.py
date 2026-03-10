from __future__ import annotations

from pathlib import Path


def test_fastapi_module_declares_expected_routes() -> None:
    api_path = Path("sap_automation/api.py")
    content = api_path.read_text(encoding="utf-8")

    assert '"/health"' in content
    assert '"/api/v1/extractions/iw69"' in content
    assert '"/api/v1/extractions/iw69/{run_id}/manifest"' in content
    assert '"/api/v1/extractions/iw69/curl"' in content
