from __future__ import annotations

import csv
import json
from pathlib import Path

from sap_automation.artifacts import ArtifactStore
from sap_automation.batch import BatchOrchestrator
from sap_automation.consolidation import Consolidator
from sap_automation.contracts import BatchRunPayload, ObjectManifest


class _FakeSessionProvider:
    def get_session(self, config, logger=None):  # noqa: ANN001
        return "fake-session"


class _FakeExportService:
    def __init__(self) -> None:
        self.session_provider = _FakeSessionProvider()

    def execute(self, *, job, artifacts, shared_session=None):  # noqa: ANN001
        artifacts.ensure_directories()
        if job.object_code == "WB":
            return ObjectManifest(
                object_code=job.object_code,
                status="failed",
                error="simulated failure",
                canonical_csv_path=str(artifacts.canonical_csv_path),
                raw_txt_path=str(artifacts.raw_txt_path),
                metadata_path=str(artifacts.metadata_path),
            )

        artifacts.raw_txt_path.write_text(f"{job.object_code} raw", encoding="utf-8")
        with artifacts.canonical_csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["nota", "descricao"])
            writer.writeheader()
            writer.writerow({"nota": f"{job.object_code}-1", "descricao": job.object_code})
        artifacts.metadata_path.write_text(
            json.dumps({"run_id": job.run_id, "object": job.object_code}),
            encoding="utf-8",
        )
        return ObjectManifest(
            object_code=job.object_code,
            status="success",
            rows_exported=1,
            raw_txt_path=str(artifacts.raw_txt_path),
            canonical_csv_path=str(artifacts.canonical_csv_path),
            metadata_path=str(artifacts.metadata_path),
            source_txt_path=str(artifacts.raw_txt_path),
            details={"run_id": job.run_id},
        )


def test_batch_orchestrator_keeps_running_after_single_object_failure(tmp_path: Path) -> None:
    payload = BatchRunPayload(
        run_id="run-batch",
        reference="202603",
        from_date="2026-01-01",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
    )
    orchestrator = BatchOrchestrator(
        artifact_store=ArtifactStore(tmp_path),
        export_service=_FakeExportService(),
        consolidator=Consolidator(),
    )

    manifest = orchestrator.run(payload)

    assert manifest.status == "partial"
    assert manifest.consolidation["status"] == "partial"
    assert (tmp_path / "latest" / "legacy" / "BASE_AUTOMACAO_CA.txt").exists()
    assert (tmp_path / "latest" / "legacy" / "BASE_AUTOMACAO_RL.txt").exists()
    assert not (tmp_path / "latest" / "legacy" / "BASE_AUTOMACAO_WB.txt").exists()
    persisted = json.loads(
        (tmp_path / "runs" / "run-batch" / "batch_manifest.json").read_text(encoding="utf-8")
    )
    assert persisted["status"] == "partial"
