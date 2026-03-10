from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifacts import ArtifactStore
from .batch import BatchOrchestrator
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload
from .execution import SapSessionProvider
from .legacy_runner import LegacyExportService


def create_batch_orchestrator(output_root: Path) -> BatchOrchestrator:
    artifact_store = ArtifactStore(output_root)
    export_service = LegacyExportService(session_provider=SapSessionProvider())
    return BatchOrchestrator(
        artifact_store=artifact_store,
        export_service=export_service,
        consolidator=Consolidator(),
    )


def run_batch_payload(payload: BatchRunPayload) -> BatchManifest:
    orchestrator = create_batch_orchestrator(payload.output_root)
    return orchestrator.run(payload)


def load_batch_manifest(*, output_root: Path, run_id: str) -> dict[str, Any]:
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))
