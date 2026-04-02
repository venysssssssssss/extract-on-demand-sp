from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .artifacts import ArtifactStore
from .control_plane import ControlPlaneService, ControlPlaneSettings, JobEnvelope
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload, ObjectManifest
from .credentials import CredentialsLoader
from .dw import DwManifest, run_dw_demandante
from .execution import LogonPadSessionProvider, SapSessionProvider, SessionProvider
from .iw51 import Iw51Manifest, run_iw51_demandante
from .iw59 import Iw59BatchResult, Iw59ExportResult
from .integrations import Iw59ExportAdapter
from .legacy_runner import LegacyExportService
from .login import SapLoginHandler
from .logon import SapApplicationProvider, SapConnectionOpener, SapLogonPadUiOpener
from .runtime_logging import configure_run_logger

if TYPE_CHECKING:
    from .batch import BatchOrchestrator


_CONTROL_PLANE_SERVICE: ControlPlaneService | None = None


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


def create_control_plane_service() -> ControlPlaneService:
    global _CONTROL_PLANE_SERVICE
    if _CONTROL_PLANE_SERVICE is None:
        _CONTROL_PLANE_SERVICE = ControlPlaneService(settings=ControlPlaneSettings.from_env())
    return _CONTROL_PLANE_SERVICE


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


def execute_control_plane_job(job: JobEnvelope) -> tuple[str, dict[str, Any], str]:
    payload = dict(job.payload)
    flow_type = str(job.flow_type).strip().lower()
    if flow_type == "iw69":
        manifest = run_batch_payload(
            BatchRunPayload(
                run_id=str(payload["run_id"]),
                reference=str(payload["reference"]),
                from_date=str(payload["from_date"]),
                to_date=str(payload.get("to_date") or payload["from_date"]),
                demandante=str(payload.get("demandante", job.demandante)),
                output_root=Path(str(payload.get("output_root", "output"))),
                objects=list(payload.get("objects", ["CA", "RL", "WB"])),
                regional=str(payload.get("regional", "SP")),
                config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
                legacy_compatibility=bool(payload.get("legacy_compatibility", True)),
                include_iw59_placeholder=bool(payload.get("include_iw59_placeholder", True)),
            )
        )
        status = str(manifest.status).strip().lower()
        manifest_path = (
            Path(str(payload.get("output_root", "output"))).expanduser().resolve()
            / "runs"
            / str(payload["run_id"])
            / "batch_manifest.json"
        )
        return status, manifest.to_dict(), str(manifest_path)
    if flow_type == "iw51":
        manifest = run_iw51_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            max_rows=int(payload.get("max_rows", 0) or 0) or None,
        )
        return str(manifest.status).strip().lower(), manifest.to_dict(), str(manifest.manifest_path)
    if flow_type == "dw":
        manifest = run_dw_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            max_rows=int(payload.get("max_rows", 0) or 0) or None,
        )
        return str(manifest.status).strip().lower(), manifest.to_dict(), str(manifest.manifest_path)
    if flow_type == "iw59":
        manifest = run_iw59_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            input_csv_path=Path(str(payload.get("input_csv_path", "")).strip()) if str(payload.get("input_csv_path", "")).strip() else None,
        )
        return str(manifest.status).strip().lower(), manifest.to_dict(), str(manifest.metadata_path)
    raise ValueError(f"Unsupported flow_type={job.flow_type}.")


def load_batch_manifest(*, output_root: Path, run_id: str) -> dict[str, Any]:
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def run_iw59_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    input_csv_path: Path | None = None,
) -> Iw59BatchResult:
    from .config import load_export_config

    resolved_output_root = output_root.expanduser().resolve()
    config = load_export_config(config_path)
    logger, _ = configure_run_logger(output_root=resolved_output_root, run_id=run_id)
    session = create_session_provider(config).get_session(config=config, logger=logger)
    resolved_demandante = str(demandante).strip().upper() or "IGOR"
    if resolved_demandante == "KELLY":
        adapter = Iw59ExportAdapter()
        result = adapter.execute_modified_by_brs(
            output_root=resolved_output_root,
            run_id=run_id,
            demandante=resolved_demandante,
            session=session,
            logger=logger,
            config=config,
            input_csv_path=input_csv_path,
        )
        metadata_dir = resolved_output_root / "runs" / run_id / "iw59" / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = metadata_dir / f"iw59_{run_id}.manifest.json"
        aggregate = Iw59BatchResult(
            status=str(result.status).strip().lower(),
            run_id=run_id,
            reference="",
            demandante=resolved_demandante,
            results=[result.to_dict()],
            metadata_path=str(metadata_path),
        )
        metadata_path.write_text(json.dumps(aggregate.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return aggregate

    try:
        batch_manifest = load_batch_manifest(output_root=resolved_output_root, run_id=run_id)
    except FileNotFoundError:
        batch_manifest = {}

    reference = ""
    source_manifests: list[ObjectManifest] = []
    if batch_manifest:
        source_manifests = _extract_iw59_source_manifests_from_batch_manifest(batch_manifest)
        reference = str(batch_manifest.get("reference", "")).strip()
        resolved_demandante = str(batch_manifest.get("demandante", resolved_demandante)).strip() or resolved_demandante

    if not source_manifests:
        source_manifests, reference = _load_object_manifests_for_iw59_replay(
            output_root=resolved_output_root,
            run_id=run_id,
        )

    if not source_manifests:
        raise RuntimeError(f"Run {run_id} did not produce successful CA/RL/WB manifests for IW59 replay.")

    adapter = Iw59ExportAdapter()
    results: list[dict[str, Any]] = []
    for source_manifest in source_manifests:
        logger.info(
            "Starting IW59 replay source_object=%s run_id=%s reference=%s",
            source_manifest.object_code,
            run_id,
            reference,
        )
        result = adapter.execute(
            output_root=resolved_output_root,
            run_id=run_id,
            reference=reference,
            demandante=resolved_demandante,
            source_manifest=source_manifest,
            session=session,
            logger=logger,
            config=config,
        )
        logger.info(
            "Finished IW59 replay source_object=%s status=%s combined_csv=%s reason=%s",
            source_manifest.object_code,
            result.status,
            result.combined_csv_path,
            result.reason,
        )
        results.append(result.to_dict())

    statuses = {str(item.get("status", "")).strip().lower() for item in results}
    overall_status = (
        "success"
        if statuses == {"success"}
        else "partial"
        if "success" in statuses or len(statuses) > 1
        else "failed"
        if statuses == {"failed"}
        else "skipped"
    )
    metadata_dir = resolved_output_root / "runs" / run_id / "iw59" / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_dir / f"iw59_{run_id}.manifest.json"
    aggregate = Iw59BatchResult(
        status=overall_status,
        run_id=run_id,
        reference=reference,
        demandante=resolved_demandante,
        results=results,
        metadata_path=str(metadata_path),
    )
    metadata_path.write_text(json.dumps(aggregate.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return aggregate


def _extract_iw59_source_manifests_from_batch_manifest(batch_manifest: dict[str, Any]) -> list[ObjectManifest]:
    source_manifests: list[ObjectManifest] = []
    for item in batch_manifest.get("objects", []):
        if not isinstance(item, dict):
            continue
        object_code = str(item.get("object_code", "")).strip().upper()
        if object_code not in {"CA", "RL", "WB"}:
            continue
        manifest = ObjectManifest(**item)
        if manifest.status == "success":
            source_manifests.append(manifest)
    return source_manifests


def _load_object_manifests_for_iw59_replay(
    *,
    output_root: Path,
    run_id: str,
) -> tuple[list[ObjectManifest], str]:
    object_manifests: list[ObjectManifest] = []
    reference = ""
    for object_code in ("CA", "RL", "WB"):
        object_slug = object_code.casefold()
        object_root = output_root / "runs" / run_id / object_slug
        metadata_candidates = sorted((object_root / "metadata").glob(f"{object_slug}_*.manifest.json"))
        if not metadata_candidates:
            continue

        metadata_path = metadata_candidates[-1]
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not reference:
            reference = str(metadata.get("reference", "")).strip()
        canonical_csv_path = Path(str(metadata.get("output_path", "")).strip())
        raw_csv_path = Path(str(metadata.get("raw_csv_path", "")).strip())
        source_txt_path = Path(str(metadata.get("source_txt_path", "")).strip())
        header_map_path = Path(
            str(metadata.get("header_map_path", "")).strip() or metadata_path.with_suffix(".header_map.csv")
        )
        rejects_path = Path(str(metadata.get("rejects_path", "")).strip())
        object_manifest = ObjectManifest(
            object_code=object_code,
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
        if object_manifest.status == "success":
            object_manifests.append(object_manifest)
        if not reference:
            stem_parts = metadata_path.stem.split("_")
            if len(stem_parts) >= 3:
                reference = stem_parts[1]

    if not object_manifests:
        raise FileNotFoundError(
            f"Could not find batch_manifest.json or successful CA/RL/WB metadata manifests for run_id={run_id}."
        )
    if not reference:
        raise RuntimeError(f"Could not determine reference for run_id={run_id} from IW69 metadata.")
    return object_manifests, reference
