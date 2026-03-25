from __future__ import annotations

from pathlib import Path

from sap_automation.artifacts import ArtifactStore
from sap_automation.config import load_export_config, resolve_iw69_object_config, validate_iw69_objects
from sap_automation.contracts import BatchRunPayload, ExportJobSpec


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
    assert jobs[0].coordinator == "IGOR"


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


def test_batch_payload_propagates_explicit_coordinator(tmp_path: Path) -> None:
    payload = BatchRunPayload(
        run_id="run-001",
        reference="202603",
        from_date="2026-03-23",
        coordinator="manu",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
    )

    jobs = payload.build_jobs()

    assert [job.coordinator for job in jobs] == ["MANU", "MANU", "MANU"]


def test_repo_config_resolves_manu_ca_override() -> None:
    config = load_export_config(Path("sap_iw69_batch_config.json"))

    ca_config = resolve_iw69_object_config(
        config=config,
        object_code="CA",
        coordinator="MANU",
    )
    rl_config = resolve_iw69_object_config(
        config=config,
        object_code="RL",
        coordinator="MANU",
    )

    manu_steps = ca_config.get("steps", [])
    assert manu_steps[6]["ids"][0] == "wnd[0]/usr/btn%_OTGRP_%_APP_%-VALU_PUSH"
    assert manu_steps[7]["value"] == "CA"
    assert manu_steps[8]["value"] == "OSBI"
    assert manu_steps[14]["value"] == "CBXR"
    assert manu_steps[34]["value"] == "CDTSS"
    assert rl_config["steps"][0]["label"] == "RL maximize"
