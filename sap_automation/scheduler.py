from __future__ import annotations

import logging
import time

from .control_plane import ControlPlaneService, ControlPlaneSettings


def ensure_default_schedules(service: ControlPlaneService) -> None:
    service.upsert_schedule(
        schedule_id="sm-sala-mercado-diario-1130",
        enabled=True,
        flow_type="workflow:sm_sala_mercado_daily",
        demandante="SALA_MERCADO",
        cron_expression="30 11 * * *",
        timezone="America/Bahia",
        payload_template={
            "demandante": "SALA_MERCADO",
            "distribuidora": "São Paulo",
            "reference": "current_month",
            "output_root": str(service.settings.output_root),
            "config_path": "sap_iw69_batch_config.json",
        },
        misfire_policy="catch_up",
    )


def run_scheduler_loop(*, service: ControlPlaneService | None = None) -> None:
    settings = ControlPlaneSettings.from_env()
    resolved_service = service or ControlPlaneService(settings=settings)
    logger = logging.getLogger("sap_automation.scheduler")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
    ensure_default_schedules(resolved_service)
    logger.info("Starting scheduler loop poll_seconds=%s", settings.scheduler_poll_seconds)
    while True:
        jobs = resolved_service.run_scheduler_tick()
        if jobs:
            logger.info("Scheduler created jobs count=%s ids=%s", len(jobs), ",".join(job.job_id for job in jobs))
        time.sleep(settings.scheduler_poll_seconds)


if __name__ == "__main__":
    run_scheduler_loop()
