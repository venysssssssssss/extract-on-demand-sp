from __future__ import annotations

from pathlib import Path

from sap_automation.artifacts import ArtifactStore
from sap_automation.config import load_export_config, validate_iw69_objects
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
