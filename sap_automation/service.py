from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .artifacts import ArtifactStore
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload, ObjectManifest
from .credentials import CredentialsLoader
from .dw import DwManifest, run_dw_demandante
from .execution import LogonPadSessionProvider, SapSessionProvider, SessionProvider
from .iw51 import Iw51Manifest, run_iw51_demandante
from .iw59 import Iw59ExportResult
from .integrations import Iw59ExportAdapter
from .legacy_runner import LegacyExportService
from .login import SapLoginHandler
from .logon import SapApplicationProvider, SapConnectionOpener, SapLogonPadUiOpener
from .runtime_logging import configure_run_logger

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
            connection_opener=SapConnectionOpener(
                app_provider,
                ui_opener=SapLogonPadUiOpener(),
            ),
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


def run_iw51_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    max_rows: int | None = None,
) -> Iw51Manifest:
    return run_iw51_demandante(
        run_id=run_id,
        demandante=demandante,
        config_path=config_path,
        output_root=output_root,
        max_rows=max_rows,
    )


def run_dw_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    max_rows: int | None = None,
) -> DwManifest:
    return run_dw_demandante(
        run_id=run_id,
        demandante=demandante,
        config_path=config_path,
        output_root=output_root,
        max_rows=max_rows,
    )


def load_batch_manifest(*, output_root: Path, run_id: str) -> dict[str, Any]:
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def run_iw59_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
) -> Iw59ExportResult:
    from .config import load_export_config

    resolved_output_root = output_root.expanduser().resolve()
    config = load_export_config(config_path)
    logger, _ = configure_run_logger(output_root=resolved_output_root, run_id=run_id)
    session = create_session_provider(config).get_session(config=config, logger=logger)
    try:
        batch_manifest = load_batch_manifest(output_root=resolved_output_root, run_id=run_id)
    except FileNotFoundError:
        batch_manifest = {}

    ca_manifest: ObjectManifest | None = None
    reference = ""
    resolved_demandante = str(demandante).strip().upper() or "IGOR"
    if batch_manifest:
        ca_manifest_data = next(
            (
                item
                for item in batch_manifest.get("objects", [])
                if str(item.get("object_code", "")).strip().upper() == "CA"
            ),
            None,
        )
        if isinstance(ca_manifest_data, dict):
            ca_manifest = ObjectManifest(**ca_manifest_data)
            reference = str(batch_manifest.get("reference", "")).strip()
            resolved_demandante = str(batch_manifest.get("demandante", resolved_demandante)).strip() or resolved_demandante

    if ca_manifest is None:
        ca_manifest, reference = _load_ca_manifest_for_iw59_replay(
            output_root=resolved_output_root,
            run_id=run_id,
        )

    if ca_manifest.status != "success":
        raise RuntimeError(
            f"Run {run_id} CA manifest is not successful. status={ca_manifest.status}"
        )

    return Iw59ExportAdapter().execute(
        output_root=resolved_output_root,
        run_id=run_id,
        reference=reference,
        demandante=resolved_demandante,
        ca_manifest=ca_manifest,
        session=session,
        logger=logger,
        config=config,
    )


def _load_ca_manifest_for_iw59_replay(
    *,
    output_root: Path,
    run_id: str,
) -> tuple[ObjectManifest, str]:
    ca_root = output_root / "runs" / run_id / "ca"
    metadata_candidates = sorted((ca_root / "metadata").glob("ca_*.manifest.json"))
    if not metadata_candidates:
        raise FileNotFoundError(
            f"Could not find batch_manifest.json or CA metadata manifest for run_id={run_id}."
        )

    metadata_path = metadata_candidates[-1]
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    reference = str(metadata.get("reference", "")).strip()
    canonical_csv_path = Path(str(metadata.get("output_path", "")).strip())
    raw_csv_path = Path(str(metadata.get("raw_csv_path", "")).strip())
    source_txt_path = Path(str(metadata.get("source_txt_path", "")).strip())
    header_map_path = Path(str(metadata.get("header_map_path", "")).strip() or metadata_path.with_suffix(".header_map.csv"))
    rejects_path = Path(str(metadata.get("rejects_path", "")).strip())
    object_manifest = ObjectManifest(
        object_code="CA",
        status="success" if bool(metadata.get("csv_materialized", False) or source_txt_path.exists()) else "failed",
        rows_exported=int(metadata.get("rows_exported", 0) or 0),
        raw_txt_path=str(source_txt_path),
        canonical_csv_path=str(canonical_csv_path),
        raw_csv_path=str(raw_csv_path),
        header_map_path=str(header_map_path),
        rejects_path=str(rejects_path),
        metadata_path=str(metadata_path),
        source_txt_path=str(source_txt_path),
        details=metadata,
    )
    if not reference:
        stem_parts = metadata_path.stem.split("_")
        if len(stem_parts) >= 3:
            reference = stem_parts[1]
    if not reference:
        raise RuntimeError(f"Could not determine reference for run_id={run_id} from CA metadata.")
    return object_manifest, reference
