from __future__ import annotations

import json
import hashlib
import shutil
import urllib.request
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
from .medidor import MedidorManifest, run_medidor_demandante
from .sm import SmManifest, prepare_sm_installations_csv, run_sm_demandante, ingest_sm_results
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


def run_medidor_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    installations_path: Path | None = None,
    group_map_path: Path | None = None,
) -> MedidorManifest:
    return run_medidor_demandante(
        run_id=run_id,
        demandante=demandante,
        config_path=config_path,
        output_root=output_root,
        installations_path=installations_path,
        group_map_path=group_map_path,
    )


def run_sm_payload(
    *,
    run_id: str,
    demandante: str,
    output_root: Path,
    config_path: Path,
    month: int | None = None,
    year: int | None = None,
    distribuidora: str = "São Paulo",
    installations: list[str] | None = None,
    installations_csv_path: Path | None = None,
    skip_ingest: bool = False,
) -> SmManifest:
    return run_sm_demandante(
        run_id=run_id,
        demandante=demandante,
        config_path=config_path,
        output_root=output_root,
        month=month,
        year=year,
        distribuidora=distribuidora,
        installations=installations,
        installations_csv_path=installations_csv_path,
        skip_ingest=skip_ingest,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_local_path(*, output_root: Path, run_id: str, artifact_name: str) -> Path:
    normalized = str(artifact_name).strip().replace("\\", "/")
    if not normalized or normalized.startswith("/") or ".." in normalized.split("/"):
        raise ValueError(f"Invalid artifact name: {artifact_name!r}")
    return output_root.expanduser().resolve() / "artifacts" / run_id / normalized


def _register_local_artifact(
    *,
    run_id: str,
    artifact_name: str,
    source_path: Path,
    output_root: Path,
    producer_job_id: str,
    kind: str = "file",
) -> dict[str, Any]:
    target_path = _artifact_local_path(output_root=output_root, run_id=run_id, artifact_name=artifact_name)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.expanduser().resolve() != target_path:
        shutil.copyfile(source_path, target_path)
    service = create_control_plane_service()
    artifact = service.register_artifact(
        run_id=run_id,
        artifact_name=artifact_name,
        path=target_path,
        kind=kind,
        producer_job_id=producer_job_id,
        sha256=_sha256_file(target_path),
        size_bytes=target_path.stat().st_size,
    )
    return artifact.__dict__


def _download_artifact_to_path(
    *,
    run_id: str,
    artifact_name: str,
    target_path: Path,
    output_root: Path,
    control_plane_base_url: str = "",
) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    base_url = str(control_plane_base_url or "").strip().rstrip("/")
    if base_url:
        url = f"{base_url}/api/v1/artifacts/{run_id}/{artifact_name}"
        with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - internal control-plane URL
            target_path.write_bytes(response.read())
        return target_path
    service = create_control_plane_service()
    artifact = service.get_artifact(run_id=run_id, artifact_name=artifact_name)
    if artifact is None:
        raise FileNotFoundError(f"Artifact not found: run_id={run_id} name={artifact_name}")
    shutil.copyfile(Path(artifact.path), target_path)
    return target_path


def _upload_artifact(
    *,
    run_id: str,
    artifact_name: str,
    source_path: Path,
    output_root: Path,
    producer_job_id: str,
    control_plane_base_url: str = "",
    kind: str = "file",
) -> dict[str, Any]:
    base_url = str(control_plane_base_url or "").strip().rstrip("/")
    if base_url:
        url = f"{base_url}/api/v1/artifacts/{run_id}/{artifact_name}?producer_job_id={producer_job_id}&kind={kind}"
        request = urllib.request.Request(
            url,
            data=source_path.read_bytes(),
            method="POST",
            headers={"Content-Type": "application/octet-stream"},
        )
        with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310 - internal control-plane URL
            payload = json.loads(response.read().decode("utf-8"))
        return dict(payload.get("data", payload))
    return _register_local_artifact(
        run_id=run_id,
        artifact_name=artifact_name,
        source_path=source_path,
        output_root=output_root,
        producer_job_id=producer_job_id,
        kind=kind,
    )


def _execute_sm_prepare_input(job: JobEnvelope, payload: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    output_root = Path(str(payload.get("output_root", "output")))
    run_id = str(payload["run_id"])
    csv_path = output_root.expanduser().resolve() / "runs" / run_id / "sm" / "input" / "SM_INSTALLATIONS.csv"
    result = prepare_sm_installations_csv(
        run_id=run_id,
        output_root=output_root,
        config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
        month=int(payload["month"]) if payload.get("month") else None,
        year=int(payload["year"]) if payload.get("year") else None,
        distribuidora=str(payload.get("distribuidora", "São Paulo")),
        csv_path=csv_path,
    )
    payload_result = result.to_dict()
    if result.status == "success":
        payload_result["artifact"] = _register_local_artifact(
            run_id=run_id,
            artifact_name="SM_INSTALLATIONS.csv",
            source_path=Path(result.csv_path),
            output_root=output_root,
            producer_job_id=job.job_id,
            kind="sm_input",
        )
    return result.status, payload_result, str(result.csv_path)


def _execute_sm_sap_extract(job: JobEnvelope, payload: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    output_root = Path(str(payload.get("output_root", "output")))
    run_id = str(payload["run_id"])
    input_path = output_root.expanduser().resolve() / "runs" / run_id / "sm" / "input" / "SM_INSTALLATIONS.csv"
    _download_artifact_to_path(
        run_id=run_id,
        artifact_name="SM_INSTALLATIONS.csv",
        target_path=input_path,
        output_root=output_root,
        control_plane_base_url=str(payload.get("control_plane_base_url", "")),
    )
    manifest = run_sm_payload(
        run_id=run_id,
        demandante=str(payload.get("demandante", job.demandante)),
        output_root=output_root,
        config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
        month=int(payload["month"]) if payload.get("month") else None,
        year=int(payload["year"]) if payload.get("year") else None,
        distribuidora=str(payload.get("distribuidora", "São Paulo")),
        installations_csv_path=input_path,
        skip_ingest=True,
    )
    result = manifest.to_dict()
    artifacts: list[dict[str, Any]] = []
    final_csv = Path(manifest.final_csv_path)
    if final_csv.exists():
        artifacts.append(
            _upload_artifact(
                run_id=run_id,
                artifact_name="SM_DADOS_FATURA.csv",
                source_path=final_csv,
                output_root=output_root,
                producer_job_id=job.job_id,
                control_plane_base_url=str(payload.get("control_plane_base_url", "")),
                kind="sm_final",
            )
        )
    manifest_path = Path(manifest.manifest_path)
    if manifest_path.exists():
        artifacts.append(
            _upload_artifact(
                run_id=run_id,
                artifact_name="sm_manifest.json",
                source_path=manifest_path,
                output_root=output_root,
                producer_job_id=job.job_id,
                control_plane_base_url=str(payload.get("control_plane_base_url", "")),
                kind="manifest",
            )
        )
    for txt_path in sorted((output_root.expanduser().resolve() / "runs" / run_id / "sm").glob("SM_SQVI*.txt")):
        artifacts.append(
            _upload_artifact(
                run_id=run_id,
                artifact_name=txt_path.name,
                source_path=txt_path,
                output_root=output_root,
                producer_job_id=job.job_id,
                control_plane_base_url=str(payload.get("control_plane_base_url", "")),
                kind="sap_raw",
            )
        )
    result["artifacts"] = artifacts
    return str(manifest.status).strip().lower(), result, str(manifest.manifest_path)


def _execute_sm_ingest_final(job: JobEnvelope, payload: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    output_root = Path(str(payload.get("output_root", "output")))
    run_id = str(payload["run_id"])
    final_csv_path = output_root.expanduser().resolve() / "runs" / run_id / "sm" / "SM_DADOS_FATURA.csv"
    _download_artifact_to_path(
        run_id=run_id,
        artifact_name="SM_DADOS_FATURA.csv",
        target_path=final_csv_path,
        output_root=output_root,
        control_plane_base_url=str(payload.get("control_plane_base_url", "")),
    )
    result = ingest_sm_results(
        run_id=run_id,
        output_root=output_root,
        config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
        final_csv_path=final_csv_path,
        month=int(payload["month"]) if payload.get("month") else None,
        year=int(payload["year"]) if payload.get("year") else None,
        distribuidora=str(payload.get("distribuidora", "São Paulo")),
    )
    return result.status, result.to_dict(), result.source_csv_path


def execute_control_plane_job(job: JobEnvelope) -> tuple[str, dict[str, Any], str]:
    payload = dict(job.payload)
    flow_type = str(job.flow_type).strip().lower()
    if flow_type == "sm_prepare_input":
        return _execute_sm_prepare_input(job, payload)
    if flow_type == "sm_sap_extract":
        return _execute_sm_sap_extract(job, payload)
    if flow_type == "sm_ingest_final":
        return _execute_sm_ingest_final(job, payload)
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
            / "runs" / str(payload["run_id"]) / "batch_manifest.json"
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
    if flow_type == "medidor":
        manifest = run_medidor_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            installations_path=Path(str(payload.get("installations_path", "")).strip()) if str(payload.get("installations_path", "")).strip() else None,
            group_map_path=Path(str(payload.get("group_map_path", "")).strip()) if str(payload.get("group_map_path", "")).strip() else None,
        )
        return str(manifest.status).strip().lower(), manifest.to_dict(), str(manifest.manifest_path)
    if flow_type == "sm":
        manifest = run_sm_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            month=int(payload["month"]) if payload.get("month") else None,
            year=int(payload["year"]) if payload.get("year") else None,
            distribuidora=str(payload.get("distribuidora", "São Paulo")),
            installations=list(payload["installations"]) if payload.get("installations") else None,
            installations_csv_path=Path(str(payload["installations_csv_path"])) if payload.get("installations_csv_path") else None,
            skip_ingest=bool(payload.get("skip_ingest", False)),
        )
        return str(manifest.status).strip().lower(), manifest.to_dict(), str(manifest.manifest_path)
    if flow_type == "iw59":
        manifest = run_iw59_payload(
            run_id=str(payload["run_id"]),
            demandante=str(payload.get("demandante", job.demandante)),
            output_root=Path(str(payload.get("output_root", "output"))),
            config_path=Path(str(payload.get("config_path", "sap_iw69_batch_config.json"))),
            reference=str(payload.get("reference", "")).strip() or None,
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
    reference: str | None = None,
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
            reference=reference,
            input_csv_path=input_csv_path,
        )
        reference = ""
        if result.metadata_path:
            metadata_candidate = Path(result.metadata_path)
            if metadata_candidate.exists():
                metadata_payload = json.loads(metadata_candidate.read_text(encoding="utf-8"))
                reference = str(metadata_payload.get("reference", "")).strip()
        metadata_dir = resolved_output_root / "runs" / run_id / "iw59" / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = metadata_dir / f"iw59_{run_id}.manifest.json"
        aggregate = Iw59BatchResult(
            status=str(result.status).strip().lower(),
            run_id=run_id,
            reference=reference,
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
