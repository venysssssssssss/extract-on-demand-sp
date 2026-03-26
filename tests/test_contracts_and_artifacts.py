from __future__ import annotations

from datetime import date
from pathlib import Path

from sap_automation.artifacts import ArtifactStore
from sap_automation.config import load_export_config, resolve_iw69_object_config, validate_iw69_objects
from sap_automation.contracts import BatchRunPayload, ExportJobSpec
from sap_automation.legacy_runner import compute_iw69_rolling_month_date_range, resolve_iw69_date_range


def test_batch_payload_builds_all_iw69_jobs(tmp_path: Path) -> None:
    payload = BatchRunPayload(
        run_id="run-001",
        reference="202603",
        from_date="2026-01-01",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
    )

    jobs = payload.build_jobs()

    assert [job.object_code for job in jobs] == ["CA", "RL", "WB"]
    assert jobs[0].export_filename == "BASE_AUTOMACAO_CA_run-001.txt"
    assert jobs[0].to_date == "2026-01-01"
    assert jobs[0].demandante == "IGOR"


def test_batch_payload_propagates_explicit_to_date(tmp_path: Path) -> None:
    payload = BatchRunPayload(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        to_date="2026-03-24",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
    )

    jobs = payload.build_jobs()

    assert [job.to_date for job in jobs] == ["2026-03-24", "2026-03-24", "2026-03-24"]


def test_artifact_store_creates_run_scoped_layout(tmp_path: Path) -> None:
    job = ExportJobSpec(
        object_code="CA",
        run_id="run-002",
        reference="202603",
        from_date="2026-01-01",
        output_root=tmp_path,
    )

    paths = ArtifactStore(tmp_path).build_object_paths(job)

    assert paths.raw_dir == tmp_path / "runs" / "run-002" / "ca" / "raw"
    assert paths.canonical_csv_path.name == "ca_202603_run-002.csv"
    assert paths.legacy_copy_path == tmp_path / "latest" / "legacy" / "BASE_AUTOMACAO_CA.txt"
    assert paths.metadata_dir.exists()


def test_repo_config_contains_required_iw69_objects() -> None:
    config = load_export_config(Path("sap_iw69_batch_config.json"))

    validate_iw69_objects(config)


def test_batch_payload_propagates_explicit_demandante(tmp_path: Path) -> None:
    payload = BatchRunPayload(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        demandante="manu",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
    )

    jobs = payload.build_jobs()

    assert [job.demandante for job in jobs] == ["MANU", "MANU", "MANU"]


def test_repo_config_resolves_manu_ca_override() -> None:
    config = load_export_config(Path("sap_iw69_batch_config.json"))

    ca_config = resolve_iw69_object_config(
        config=config,
        object_code="CA",
        demandante="MANU",
    )
    rl_config = resolve_iw69_object_config(
        config=config,
        object_code="RL",
        demandante="MANU",
    )

    manu_steps = ca_config.get("steps", [])
    labels = {step.get("label", ""): step for step in manu_steps}
    clipboard_step = labels["CA copy MANU codes to clipboard"]

    assert labels["CA open OTGRP multisel"]["ids"][0] == "wnd[0]/usr/btn%_OTGRP_%_APP_%-VALU_PUSH"
    assert labels["CA OTGRP code CA"]["value"] == "CA"
    assert labels["CA OTGRP code OSBI"]["value"] == "OSBI"
    assert clipboard_step["action"] == "set_clipboard_text"
    assert clipboard_step["values"][0] == "CBXR"
    assert clipboard_step["values"][-1] == "CDTSS"
    assert rl_config["steps"][0]["label"] == "RL maximize"


def test_compute_iw69_rolling_month_date_range_uses_three_month_window() -> None:
    start_date, end_date = compute_iw69_rolling_month_date_range(
        months=3,
        today=date(2026, 3, 25),
    )

    assert start_date == "2026-01-01"
    assert end_date == "2026-03-25"


def test_resolve_iw69_date_range_uses_manu_rolling_window(tmp_path: Path) -> None:
    config = load_export_config(Path("sap_iw69_batch_config.json"))
    job = ExportJobSpec(
        object_code="CA",
        run_id="run-003",
        reference="202603",
        from_date="2026-03-23",
        to_date="2026-03-24",
        demandante="MANU",
        output_root=tmp_path,
    )

    start_date, end_date = resolve_iw69_date_range(
        job=job,
        config=config,
        today=date(2026, 3, 25),
    )

    assert start_date == "2026-01-01"
    assert end_date == "2026-03-25"
