from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from .contracts import ExportJobSpec, ObjectArtifactPaths, ObjectManifest
from .execution import SessionProvider, SapSessionProvider, StepExecutor


def _read_metadata(metadata_path: Path) -> dict[str, Any]:
    if not metadata_path.exists():
        return {}
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _legacy_export_module():
    return importlib.import_module("sap_gui_export_compat")


class LegacyExportService:
    def __init__(
        self,
        *,
        session_provider: SessionProvider | None = None,
        step_executor: StepExecutor | None = None,
    ) -> None:
        self.session_provider = session_provider or SapSessionProvider()
        self.step_executor = step_executor or StepExecutor()

    def execute(
        self,
        *,
        job: ExportJobSpec,
        artifacts: ObjectArtifactPaths,
        shared_session: Any | None = None,
    ) -> ObjectManifest:
        legacy_export = _legacy_export_module()
        script_dir = job.config_path.expanduser().resolve().parent
        config = legacy_export.load_config(str(job.config_path), script_dir)
        payload = legacy_export.ExportPayload(
            run_id=job.run_id,
            object_code=job.object_code,
            sqvi_name=job.sqvi_name,
            reference=job.reference,
            regional=job.regional,
            lot_ranges=[],
            period_start=job.from_date,
            period_end=job.to_date or job.from_date,
            output_path=artifacts.canonical_csv_path,
            metadata_path=artifacts.metadata_path,
            raw_csv_path=artifacts.raw_csv_path,
            header_map_path=artifacts.header_map_path,
            rejects_path=artifacts.rejects_path,
            required_fields=list(job.required_fields),
            filters=list(job.filters),
        )
        logger, log_path = legacy_export.setup_logger(
            payload=payload,
            output_path=artifacts.canonical_csv_path,
            config=config,
        )
        object_config = legacy_export.resolve_object_config(config=config, payload=payload)
        context = legacy_export.build_context(payload=payload, output_path=artifacts.canonical_csv_path)
        context.update(
            {
                "run_id": job.run_id,
                "object": job.object_code,
                "iw69_from_date_dmy": context["period_start_dmy"],
                "iw69_to_date_dmy": context.get("period_end_dmy", context["period_start_dmy"]),
                "raw_dir": str(artifacts.raw_dir),
                "raw_dir_win": str(artifacts.raw_dir),
                "raw_txt_path": str(artifacts.raw_txt_path),
                "ca_export_filename": (
                    job.export_filename if job.object_code == "CA" else context.get("ca_export_filename", "")
                ),
                "rl_export_filename": (
                    job.export_filename if job.object_code == "RL" else context.get("rl_export_filename", "")
                ),
                "wb_export_filename": (
                    job.export_filename if job.object_code == "WB" else context.get("wb_export_filename", "")
                ),
                "sap_variant": job.variant_name,
                "status_user": job.status_user,
                "transaction_code": job.transaction_code,
            }
        )
        global_cfg = config.get("global", {})
        wait_timeout_seconds = float(global_cfg.get("wait_timeout_seconds", 60.0))
        reset_transaction_code = job.transaction_code
        unwind_back_min_presses = int(global_cfg.get("unwind_back_min_presses", 4))
        unwind_back_max_presses = int(global_cfg.get("unwind_back_max_presses", 5))
        session = shared_session or self.session_provider.get_session(config=config, logger=logger)

        try:
            legacy_export.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
            try:
                self.step_executor.execute(
                    session=session,
                    steps=object_config.get("steps", []),
                    context=context,
                    default_timeout_seconds=wait_timeout_seconds,
                    logger=logger,
                )
            except Exception as step_error:
                logger.exception(
                    "SAP step execution failed object=%s error=%s",
                    job.object_code,
                    step_error,
                )
                recovered_rows = legacy_export._try_recover_export_after_step_failure(
                    payload=payload,
                    output_path=artifacts.canonical_csv_path,
                    metadata_path=artifacts.metadata_path,
                    object_config=object_config,
                    context=context,
                    required_fields=payload.required_fields,
                    logger=logger,
                    global_cfg=global_cfg,
                    source_ready_timeout_seconds=float(global_cfg.get("post_export_wait_seconds", 20.0)),
                    source_ready_poll_seconds=0.2,
                    source_ready_stable_hits=2,
                    session=session,
                    step_error=step_error,
                )
                if recovered_rows is None:
                    raise
                metadata = _read_metadata(artifacts.metadata_path)
                return ObjectManifest(
                    object_code=job.object_code,
                    status="success",
                    rows_exported=recovered_rows,
                    raw_txt_path=str(artifacts.raw_txt_path),
                    canonical_csv_path=str(artifacts.canonical_csv_path),
                    raw_csv_path=str(artifacts.raw_csv_path),
                    header_map_path=str(artifacts.header_map_path),
                    rejects_path=str(artifacts.rejects_path),
                    metadata_path=str(artifacts.metadata_path),
                    log_path=str(log_path),
                    source_txt_path=str(metadata.get("source_txt_path", "") or artifacts.raw_txt_path),
                    details=metadata,
                )
            rows_exported = legacy_export._materialize_and_persist_metadata(
                payload=payload,
                output_path=artifacts.canonical_csv_path,
                metadata_path=artifacts.metadata_path,
                object_config=object_config,
                context=context,
                required_fields=payload.required_fields,
                logger=logger,
                source_ready_timeout_seconds=float(global_cfg.get("post_export_wait_seconds", 20.0)),
                source_ready_poll_seconds=0.2,
                source_ready_stable_hits=2,
                session=session,
                allow_csv_failure=False,
                skip_csv_materialization=False,
            )
            metadata = _read_metadata(artifacts.metadata_path)
            return ObjectManifest(
                object_code=job.object_code,
                status="success",
                rows_exported=rows_exported,
                raw_txt_path=str(artifacts.raw_txt_path),
                canonical_csv_path=str(artifacts.canonical_csv_path),
                raw_csv_path=str(artifacts.raw_csv_path),
                header_map_path=str(artifacts.header_map_path),
                rejects_path=str(artifacts.rejects_path),
                metadata_path=str(artifacts.metadata_path),
                log_path=str(log_path),
                source_txt_path=str(metadata.get("source_txt_path", "") or artifacts.raw_txt_path),
                details=metadata,
            )
        except Exception as exc:
            metadata = _read_metadata(artifacts.metadata_path)
            return ObjectManifest(
                object_code=job.object_code,
                status="failed",
                error=str(exc),
                raw_txt_path=str(artifacts.raw_txt_path),
                canonical_csv_path=str(artifacts.canonical_csv_path),
                raw_csv_path=str(artifacts.raw_csv_path),
                header_map_path=str(artifacts.header_map_path),
                rejects_path=str(artifacts.rejects_path),
                metadata_path=str(artifacts.metadata_path),
                log_path=str(log_path),
                details=metadata,
            )
        finally:
            try:
                legacy_export._unwind_after_export_with_back(
                    session=session,
                    transaction_code=reset_transaction_code or job.transaction_code,
                    timeout_seconds=wait_timeout_seconds,
                    min_back_presses=unwind_back_min_presses,
                    max_back_presses=unwind_back_max_presses,
                    logger=logger,
                )
            except Exception:
                logger.exception("Post-export unwind failed.")
