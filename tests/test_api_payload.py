from __future__ import annotations

from pathlib import Path

from sap_automation.api import _build_payload
from sap_automation.api_models import BatchRunRequest


def test_build_payload_defaults_to_date_to_from_date() -> None:
    request = BatchRunRequest(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        output_root="output",
        objects=["CA"],
        config_path="sap_iw69_batch_config.json",
    )

    payload = _build_payload(request)

    assert payload.from_date == "2026-03-23"
    assert payload.to_date == "2026-03-23"
    assert payload.coordinator == "IGOR"


def test_build_payload_preserves_explicit_to_date() -> None:
    request = BatchRunRequest(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        to_date="2026-03-24",
        output_root="output",
        objects=["CA"],
        config_path=str(Path("sap_iw69_batch_config.json")),
    )

    payload = _build_payload(request)

    assert payload.to_date == "2026-03-24"


def test_build_payload_preserves_explicit_coordinator() -> None:
    request = BatchRunRequest(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        coordinator="manu",
        output_root="output",
        objects=["CA"],
        config_path="sap_iw69_batch_config.json",
    )

    payload = _build_payload(request)

    assert payload.coordinator == "MANU"
