from __future__ import annotations

import logging
import time

from .control_plane import ControlPlaneService, ControlPlaneSettings


def run_scheduler_loop(*, service: ControlPlaneService | None = None) -> None:
    settings = ControlPlaneSettings.from_env()
    resolved_service = service or ControlPlaneService(settings=settings)
    logger = logging.getLogger("sap_automation.scheduler")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
    logger.info("Starting scheduler loop poll_seconds=%s", settings.scheduler_poll_seconds)
    while True:
        jobs = resolved_service.run_scheduler_tick()
        if jobs:
            logger.info("Scheduler created jobs count=%s ids=%s", len(jobs), ",".join(job.job_id for job in jobs))
        time.sleep(settings.scheduler_poll_seconds)


if __name__ == "__main__":
    run_scheduler_loop()
