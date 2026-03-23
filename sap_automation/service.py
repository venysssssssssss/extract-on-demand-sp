from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .artifacts import ArtifactStore
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload
from .credentials import CredentialsLoader
from .execution import LogonPadSessionProvider, SapSessionProvider, SessionProvider
from .legacy_runner import LegacyExportService
from .login import SapLoginHandler
from .logon import SapApplicationProvider, SapConnectionOpener

if TYPE_CHECKING:
    from .batch import BatchOrchestrator


def create_session_provider(config: dict[str, Any] | None = None) -> SessionProvider:
    global_cfg = (config or {}).get("global", {})
    logon_pad_cfg = global_cfg.get("logon_pad", {})
    if bool(logon_pad_cfg.get("enabled", False)):
        env_path_raw = str(logon_pad_cfg.get("env_path", "")).strip()
        env_path = Path(env_path_raw).expanduser() if env_path_raw else None
        app_provider = SapApplicationProvider()
        return LogonPadSessionProvider(
            credentials_loader=CredentialsLoader(env_path=env_path),
            app_provider=app_provider,
            connection_opener=SapConnectionOpener(app_provider),
            login_handler=SapLoginHandler(),
        )
    return SapSessionProvider()


def create_batch_orchestrator(
    output_root: Path,
    *,
    config: dict[str, Any] | None = None,
) -> "BatchOrchestrator":
    from .batch import BatchOrchestrator

    artifact_store = ArtifactStore(output_root)
    export_service = LegacyExportService(session_provider=create_session_provider(config))
    return BatchOrchestrator(
        artifact_store=artifact_store,
        export_service=export_service,
        consolidator=Consolidator(),
    )


def run_batch_payload(payload: BatchRunPayload) -> BatchManifest:
    from .config import load_export_config

    config = load_export_config(payload.config_path)
    orchestrator = create_batch_orchestrator(payload.output_root, config=config)
    return orchestrator.run(payload)


def load_batch_manifest(*, output_root: Path, run_id: str) -> dict[str, Any]:
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))
