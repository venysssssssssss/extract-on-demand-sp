from __future__ import annotations

import logging
import socket
import time

from .control_plane import ControlPlaneService, ControlPlaneSettings
from .service import execute_control_plane_job


def _runner_capabilities() -> list[str]:
    return ["db", "artifact", "sm_prepare_input", "sm_ingest_final"]


def run_db_runner_loop(*, service: ControlPlaneService | None = None) -> None:
    settings = ControlPlaneSettings.from_env()
    resolved_service = service or ControlPlaneService(settings=settings)
    logger = logging.getLogger("sap_automation.db_runner")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
    runner_id = settings.runner_id
    hostname = socket.gethostname()
    current_job_id = ""
    logger.info("Starting DB runner loop runner_id=%s queue=db-default", runner_id)

    while True:
        resolved_service.upsert_runner(
            runner_id=runner_id,
            hostname=hostname,
            capabilities=_runner_capabilities(),
            current_job_id=current_job_id,
            sap_available=False,
            desktop_session_ready=False,
            status_message="db runner ready",
        )
        job = resolved_service.claim_next_job(
            runner_id=runner_id,
            queue_name="db-default",
            timeout_seconds=settings.runner_poll_seconds,
        )
        if job is None:
            time.sleep(settings.runner_poll_seconds)
            continue

        current_job_id = job.job_id
        resolved_service.mark_job_running(job_id=job.job_id, runner_id=runner_id)
        try:
            status, result, manifest_path = execute_control_plane_job(job)
            resolved_service.complete_job(
                job_id=job.job_id,
                status=status if status in {"success", "partial", "failed"} else "failed",
                result=result,
                manifest_path=manifest_path,
                error_message="",
            )
            logger.info("DB runner completed job job_id=%s status=%s", job.job_id, status)
        except Exception as exc:  # pragma: no cover - runtime path
            resolved_service.complete_job(
                job_id=job.job_id,
                status="failed",
                result={},
                manifest_path="",
                error_message=str(exc),
            )
            logger.exception("DB runner failed job job_id=%s error=%s", job.job_id, exc)
        finally:
            current_job_id = ""


if __name__ == "__main__":
    run_db_runner_loop()
