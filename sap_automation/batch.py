from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import TYPE_CHECKING

from .artifacts import ArtifactStore
from .config import load_export_config, validate_iw69_objects
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload, ObjectManifest
from .integrations import Iw59ExportAdapter, Iw67ExportAdapter
from .runtime_logging import configure_run_logger
from .service import create_batch_orchestrator

if TYPE_CHECKING:
    from .legacy_runner import LegacyExportService


_CLOSED_STATUS_VALUES: set[str] = {
    "ENCE",
    "ENCE DEFE",
    "ENCE DEFE INDE",
    "ENCE DUPL",
    "ENCE IMPR",
    "ENCE INDE",
    "ENCE PROC",
}


def _resolve_status_field(fieldnames: list[str]) -> str:
    lowered = {str(item or "").strip().casefold(): str(item or "").strip() for item in fieldnames if str(item or "").strip()}
    for candidate in ("statusuar", "status_usuario"):
        if candidate in lowered:
            return lowered[candidate]
    return ""


def _materialize_open_status_csv(canonical_csv_path: Path) -> tuple[Path | None, int, bool]:
    if not canonical_csv_path.exists():
        return None, 0, False
    with canonical_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    if not fieldnames:
        return None, 0, False

    status_field = _resolve_status_field(fieldnames)
    if not status_field:
        return None, 0, False

    output_path = canonical_csv_path.with_name(f"{canonical_csv_path.stem}_abertas.csv")
    open_rows = [
        row
        for row in rows
        if str(row.get(status_field, "") or "").strip().upper() not in _CLOSED_STATUS_VALUES
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(open_rows)
    return output_path, len(open_rows), True


class BatchOrchestrator:
    def __init__(
        self,
        *,
        artifact_store: ArtifactStore,
        export_service: LegacyExportService,
        consolidator: Consolidator,
    ) -> None:
        self.artifact_store = artifact_store
        self.export_service = export_service
        self.consolidator = consolidator

    def run(self, payload: BatchRunPayload) -> BatchManifest:
        config = load_export_config(payload.config_path)
        validate_iw69_objects(config)
        global_cfg = config.get("global", {})
        stop_on_object_failure = bool(global_cfg.get("stop_on_object_failure", True))
        logger, log_path = configure_run_logger(
            output_root=self.artifact_store.output_root,
            run_id=payload.run_id,
        )
        logger.info(
            "Starting SAP batch run run_id=%s reference=%s objects=%s session_log=%s",
            payload.run_id,
            payload.reference,
            ",".join(payload.objects),
            str(log_path),
        )
        session_provider = self.export_service.session_provider
        shared_session = session_provider.get_session(config=config, logger=logger)

        object_manifests: list[ObjectManifest] = []
        for job in payload.build_jobs():
            logger.info("Starting object extraction object=%s transaction=%s", job.object_code, job.transaction_code)
            artifacts = self.artifact_store.build_object_paths(job)
            manifest = self.export_service.execute(
                job=job,
                artifacts=artifacts,
                shared_session=shared_session,
            )
            if manifest.status == "success" and job.legacy_compatibility:
                source_path = Path(manifest.source_txt_path or manifest.raw_txt_path)
                if source_path.exists():
                    self.artifact_store.copy_legacy_export(
                        source_path=source_path,
                        destination_path=artifacts.legacy_copy_path,
                    )
                    manifest = ObjectManifest(
                        **{
                            **manifest.to_dict(),
                            "legacy_copy_path": str(artifacts.legacy_copy_path),
                        }
                    )
            if manifest.status == "success" and manifest.canonical_csv_path:
                open_csv_path, open_rows_exported, open_filter_applied = _materialize_open_status_csv(
                    Path(manifest.canonical_csv_path)
                )
                details = {
                    **manifest.details,
                    "open_filter_closed_statuses": sorted(_CLOSED_STATUS_VALUES),
                    "open_filter_applied": open_filter_applied,
                    "open_rows_exported": open_rows_exported,
                    "open_csv_path": str(open_csv_path) if open_csv_path else "",
                }
                manifest = ObjectManifest(
                    **{
                        **manifest.to_dict(),
                        "details": details,
                    }
                )
            object_manifests.append(manifest)
            logger.info(
                "Finished object extraction object=%s status=%s rows_exported=%s error=%s",
                manifest.object_code,
                manifest.status,
                manifest.rows_exported,
                manifest.error,
            )
            if manifest.status == "success" and manifest.details.get("open_csv_path"):
                logger.info(
                    "Generated open-status CSV object=%s open_rows=%s output=%s",
                    manifest.object_code,
                    manifest.details.get("open_rows_exported", 0),
                    manifest.details.get("open_csv_path", ""),
                )
            if manifest.status != "success" and stop_on_object_failure:
                logger.error(
                    "Stopping batch after object failure object=%s stop_on_object_failure=true",
                    manifest.object_code,
                )
                break

        notes_path, interactions_path = self.artifact_store.consolidated_paths(payload.run_id)
        consolidation = self.consolidator.consolidate(
            object_manifests=object_manifests,
            notes_path=notes_path,
            interactions_path=interactions_path,
        )
        object_statuses = {manifest.status for manifest in object_manifests}
        if object_statuses == {"success"} and consolidation.status == "success":
            status = "success"
        elif "success" in object_statuses or consolidation.status in {"success", "partial"}:
            status = "partial"
        else:
            status = "failed"
        pending_stages: list[dict[str, object]] = []
        iw59_result = None
        if payload.include_iw59_placeholder:
            ca_manifest = next(
                (item for item in object_manifests if item.object_code == "CA"),
                None,
            )
            if ca_manifest is not None and ca_manifest.status == "success":
                iw59_result = Iw59ExportAdapter().execute(
                    output_root=self.artifact_store.output_root,
                    run_id=payload.run_id,
                    reference=payload.reference,
                    demandante=payload.demandante,
                    ca_manifest=ca_manifest,
                    session=shared_session,
                    logger=logger,
                    config=config,
                )
            else:
                iw59_result = Iw59ExportAdapter().skip(
                    reason="IW59 skipped because CA did not complete successfully."
                )
            pending_stages.append(iw59_result.to_dict())
        pending_stages.append(Iw67ExportAdapter().to_dict())
        if iw59_result is not None and iw59_result.status == "failed" and status == "success":
            status = "partial"
        manifest = BatchManifest(
            run_id=payload.run_id,
            reference=payload.reference,
            from_date=payload.from_date,
            to_date=str(payload.to_date or payload.from_date),
            demandante=payload.demandante,
            status=status,
            output_root=str(self.artifact_store.output_root),
            objects=[item.to_dict() for item in object_manifests],
            consolidation=consolidation.to_dict(),
            pending_stages=pending_stages,
        )
        self.artifact_store.batch_manifest_path(payload.run_id).write_text(
            json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("SAP batch run completed status=%s", manifest.status)
        return manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the IW69 batch SAP GUI extraction.")
    parser.add_argument("--run-id", required=True, help="Batch run identifier.")
    parser.add_argument("--reference", required=True, help="Reference in YYYYMM or business token.")
    parser.add_argument("--from-date", required=True, help="SAP DATUV lower bound in ISO format (YYYY-MM-DD).")
    parser.add_argument(
        "--demandante",
        default="IGOR",
        help="Demandante-specific IW69 flow name. Default: IGOR.",
    )
    parser.add_argument(
        "--output-root",
        default="output",
        help="Root folder for run artifacts.",
    )
    parser.add_argument(
        "--config",
        default="sap_iw69_batch_config.json",
        help="Path to the SAP GUI export config JSON.",
    )
    parser.add_argument(
        "--objects",
        default="CA,RL,WB",
        help="Comma-separated IW69 object codes to execute.",
    )
    parser.add_argument(
        "--disable-legacy-copy",
        action="store_true",
        help="Skip writing the fixed legacy TXT copies under latest/legacy.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    payload = BatchRunPayload(
        run_id=args.run_id,
        reference=args.reference,
        from_date=args.from_date,
        demandante=args.demandante,
        output_root=Path(args.output_root),
        objects=[item.strip().upper() for item in args.objects.split(",") if item.strip()],
        config_path=Path(args.config),
        legacy_compatibility=not args.disable_legacy_copy,
    )
    config = load_export_config(payload.config_path)
    orchestrator = create_batch_orchestrator(payload.output_root, config=config)
    manifest = orchestrator.run(payload)
    return 0 if manifest.status in {"success", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
