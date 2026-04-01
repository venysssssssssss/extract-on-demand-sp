from __future__ import annotations

import logging
import os
import platform
import socket
import time
from pathlib import Path
from typing import Any

from .control_plane import ControlPlaneService, ControlPlaneSettings
from .service import execute_control_plane_job


def _runner_capabilities() -> list[str]:
    return ["iw69", "iw59", "iw51", "dw"]


def _check_runner_preconditions(*, output_root: Path) -> tuple[bool, bool, str]:
    desktop_session_ready = str(os.environ.get("SESSIONNAME", "")).strip().lower() not in {"", "services"}
    env_path = Path(".env")
    credentials_ready = env_path.exists() or (
        bool(str(os.environ.get("SAP_USERNAME", "")).strip()) and bool(str(os.environ.get("SAP_PASSWORD", "")).strip())
    )
    output_root.mkdir(parents=True, exist_ok=True)
    if platform.system() != "Windows":
        return False, desktop_session_ready, "runner is not on Windows"
    if not credentials_ready:
        return False, desktop_session_ready, "SAP credentials are not configured"
    try:
        import win32com.client  # type: ignore

        win32com.client.GetObject("SAPGUI")
    except Exception as exc:  # pragma: no cover - Windows runtime path
        return False, desktop_session_ready, f"SAP GUI unavailable: {exc}"
    if not desktop_session_ready:
        return False, False, "interactive desktop session is not ready"
    return True, True, "runner ready"


def run_windows_runner_loop(*, service: ControlPlaneService | None = None) -> None:
    settings = ControlPlaneSettings.from_env()
    resolved_service = service or ControlPlaneService(settings=settings)
    logger = logging.getLogger("sap_automation.runner")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
    runner_id = settings.runner_id
    hostname = socket.gethostname()
    current_job_id = ""
    logger.info("Starting Windows runner loop runner_id=%s poll_seconds=%s", runner_id, settings.runner_poll_seconds)

    while True:
        sap_available, desktop_session_ready, status_message = _check_runner_preconditions(output_root=settings.output_root)
        resolved_service.upsert_runner(
            runner_id=runner_id,
            hostname=hostname,
            capabilities=_runner_capabilities(),
            current_job_id=current_job_id,
            sap_available=sap_available,
            desktop_session_ready=desktop_session_ready,
            status_message=status_message,
        )
        if not sap_available or not desktop_session_ready:
            time.sleep(settings.runner_heartbeat_seconds)
            continue

        job = resolved_service.claim_next_job(
            runner_id=runner_id,
            queue_name=settings.queue_name,
            timeout_seconds=settings.runner_poll_seconds,
        )
        if job is None:
            time.sleep(settings.runner_poll_seconds)
            continue

        current_job_id = job.job_id
        resolved_service.mark_job_running(job_id=job.job_id, runner_id=runner_id)
        resolved_service.upsert_runner(
            runner_id=runner_id,
            hostname=hostname,
            capabilities=_runner_capabilities(),
            current_job_id=current_job_id,
            sap_available=sap_available,
            desktop_session_ready=desktop_session_ready,
            status_message="running job",
        )
        try:
            status, result, manifest_path = execute_control_plane_job(job)
            resolved_service.complete_job(
                job_id=job.job_id,
                status=status if status in {"success", "partial", "failed"} else "failed",
                result=result,
                manifest_path=manifest_path,
                error_message="",
            )
            logger.info("Runner completed job job_id=%s status=%s manifest_path=%s", job.job_id, status, manifest_path)
        except Exception as exc:  # pragma: no cover - integration/runtime path
            resolved_service.complete_job(
                job_id=job.job_id,
                status="failed",
                result={},
                manifest_path="",
                error_message=str(exc),
            )
            logger.exception("Runner failed job job_id=%s error=%s", job.job_id, exc)
        finally:
            current_job_id = ""


if __name__ == "__main__":
    run_windows_runner_loop()
