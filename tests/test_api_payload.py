from __future__ import annotations

from pathlib import Path

from sap_automation.api import _build_iw51_kwargs, _build_payload, iw51_curl_examples
from sap_automation.api_models import BatchRunRequest, Iw51RunRequest


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
    assert payload.demandante == "IGOR"


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


def test_build_payload_preserves_explicit_demandante() -> None:
    request = BatchRunRequest(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        demandante="manu",
        output_root="output",
        objects=["CA"],
        config_path="sap_iw69_batch_config.json",
    )

    payload = _build_payload(request)

    assert payload.demandante == "MANU"


def test_build_iw51_kwargs_preserves_request_values() -> None:
    request = Iw51RunRequest(
        run_id="iw51-run-001",
        demandante="dani",
        output_root="output",
        config_path="sap_iw69_batch_config.json",
        max_rows=4,
    )

    kwargs = _build_iw51_kwargs(request)

    assert kwargs["run_id"] == "iw51-run-001"
    assert kwargs["demandante"] == "dani"
    assert kwargs["output_root"] == Path("output")
    assert kwargs["config_path"] == Path("sap_iw69_batch_config.json")
    assert kwargs["max_rows"] == 4


def test_iw51_curl_examples_include_post_command() -> None:
    response = iw51_curl_examples()

    assert any("/api/v1/extractions/iw51" in command for command in response.commands)
    assert any('"demandante":"DANI"' in command for command in response.commands)
