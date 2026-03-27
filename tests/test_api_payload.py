from __future__ import annotations

from pathlib import Path

from sap_automation.api import (
    _build_dw_kwargs,
    _build_iw51_kwargs,
    _build_iw59_kwargs,
    _build_payload,
    dw_curl_examples,
    iw51_curl_examples,
    iw59_curl_examples,
)
from sap_automation.api_models import BatchRunRequest, DwRunRequest, Iw51RunRequest, Iw59RunRequest


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


def test_build_iw59_kwargs_preserves_request_values() -> None:
    request = Iw59RunRequest(
        run_id="run-iw59-001",
        demandante="manu",
        output_root="output",
        config_path="sap_iw69_batch_config.json",
    )

    kwargs = _build_iw59_kwargs(request)

    assert kwargs["run_id"] == "run-iw59-001"
    assert kwargs["demandante"] == "manu"
    assert kwargs["output_root"] == Path("output")
    assert kwargs["config_path"] == Path("sap_iw69_batch_config.json")


def test_iw59_curl_examples_include_post_command() -> None:
    response = iw59_curl_examples()

    assert any("/api/v1/extractions/iw59" in command for command in response.commands)
    assert any('"run_id":"20260326T100000"' in command for command in response.commands)
    assert any('"demandante":"MANU"' in command for command in response.commands)


def test_build_dw_kwargs_preserves_request_values() -> None:
    request = DwRunRequest(
        run_id="dw-run-001",
        demandante="dw",
        output_root="output",
        config_path="sap_iw69_batch_config.json",
        max_rows=15,
    )

    kwargs = _build_dw_kwargs(request)

    assert kwargs["run_id"] == "dw-run-001"
    assert kwargs["demandante"] == "dw"
    assert kwargs["output_root"] == Path("output")
    assert kwargs["config_path"] == Path("sap_iw69_batch_config.json")
    assert kwargs["max_rows"] == 15


def test_dw_curl_examples_include_post_command() -> None:
    response = dw_curl_examples()

    assert any("/api/v1/extractions/dw" in command for command in response.commands)
    assert any('"demandante":"DW"' in command for command in response.commands)
