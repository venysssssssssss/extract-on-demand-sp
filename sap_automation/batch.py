from __future__ import annotations

import argparse
import json
from pathlib import Path

from .artifacts import ArtifactStore
from .config import load_export_config, validate_iw69_objects
from .consolidation import Consolidator
from .contracts import BatchManifest, BatchRunPayload, Iw59JobSpec, ObjectManifest
from .integrations import Iw59ExportAdapter, Iw67ExportAdapter
from .runtime_logging import configure_run_logger
from .service import create_batch_orchestrator


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
            object_manifests.append(manifest)

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
        pending_stages: list[dict[str, str]] = []
        if payload.include_iw59_placeholder:
            pending_stages.append(Iw59ExportAdapter().to_dict())
        pending_stages.append(Iw67ExportAdapter().to_dict())
        manifest = BatchManifest(
            run_id=payload.run_id,
            reference=payload.reference,
            from_date=payload.from_date,
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
