from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sap_automation.control_plane import ControlPlaneService, ControlPlaneSettings, InMemoryJobQueue
from sap_automation.scheduler import ensure_default_schedules


def _build_service(tmp_path: Path) -> ControlPlaneService:
    settings = ControlPlaneSettings(
        database_url=f"sqlite+pysqlite:///{(tmp_path / 'control_plane.db').as_posix()}",
        redis_url="",
        queue_name="sap-default",
        scheduler_poll_seconds=15.0,
        runner_poll_seconds=5.0,
        runner_heartbeat_seconds=15.0,
        runner_id="runner-test",
        output_root=tmp_path / "output",
    )
    return ControlPlaneService(settings=settings, queue=InMemoryJobQueue())


def test_create_job_persists_and_enqueues(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    job = service.create_job(
        flow_type="iw51",
        demandante="DANI",
        payload={"run_id": "20260401T120000", "output_root": str(tmp_path / "output")},
    )

    assert job.flow_type == "iw51"
    assert job.status == "queued"
    assert job.run_id == "20260401T120000"
    assert service.get_job(job_id=job.job_id) is not None


def test_claim_next_job_marks_runner_and_running_completion(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    created = service.create_job(
        flow_type="dw",
        demandante="DW",
        payload={"run_id": "20260401T130000", "output_root": str(tmp_path / "output")},
    )

    claimed = service.claim_next_job(runner_id="runner-win-01", timeout_seconds=0.1)
    assert claimed is not None
    assert claimed.job_id == created.job_id
    assert claimed.status == "claimed"

    running = service.mark_job_running(job_id=claimed.job_id, runner_id="runner-win-01")
    assert running.status == "running"

    completed = service.complete_job(
        job_id=claimed.job_id,
        status="success",
        result={"status": "success"},
        manifest_path="output/runs/20260401T130000/dw/dw_manifest.json",
    )
    assert completed.status == "success"
    assert completed.manifest_path.endswith("dw_manifest.json")


def test_scheduler_tick_materializes_due_jobs_with_idempotency(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service.upsert_schedule(
        schedule_id="schedule-iw69-01",
        flow_type="iw69",
        demandante="IGOR",
        cron_expression="*/5 * * * *",
        timezone="America/Bahia",
        payload_template={
            "reference": "202604",
            "from_date": "2026-04-01",
            "to_date": "2026-04-30",
            "output_root": str(tmp_path / "output"),
            "objects": ["CA", "RL", "WB"],
            "config_path": "sap_iw69_batch_config.json",
        },
    )

    jobs = service.run_scheduler_tick(now=datetime(2026, 12, 31, 23, 55, tzinfo=UTC))
    assert len(jobs) == 1
    assert jobs[0].flow_type == "iw69"
    assert jobs[0].payload["reference"] == "202604"


def test_default_sm_schedule_runs_daily_at_1130_current_month_payload(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    ensure_default_schedules(service)
    schedules = {schedule.schedule_id: schedule for schedule in service.list_schedules()}

    schedule = schedules["sm-sala-mercado-diario-1130"]
    assert schedule.flow_type == "workflow:sm_sala_mercado_daily"
    assert schedule.demandante == "SALA_MERCADO"
    assert schedule.cron_expression == "30 11 * * *"
    assert schedule.timezone == "America/Bahia"
    assert schedule.payload_template["distribuidora"] == "São Paulo"
    assert schedule.payload_template["reference"] == "current_month"
    assert "month" not in schedule.payload_template
    assert "year" not in schedule.payload_template


def test_upsert_runner_tracks_heartbeat(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    runner = service.upsert_runner(
        runner_id="runner-win-01",
        hostname="sap-host",
        capabilities=["iw69", "iw59", "iw51", "dw"],
        current_job_id="",
        sap_available=True,
        desktop_session_ready=True,
        status_message="ready",
    )

    assert runner.runner_id == "runner-win-01"
    assert runner.sap_available is True
    assert runner.desktop_session_ready is True
    assert runner.status_message == "ready"


def test_workflow_sm_steps_advance_only_on_success(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    workflow = service.create_workflow(
        workflow_type="sm_sala_mercado_daily",
        demandante="SALA_MERCADO",
        payload={
            "run_id": "sm-workflow-01",
            "reference": "202604",
            "output_root": str(tmp_path / "output"),
            "config_path": "sap_iw69_batch_config.json",
            "distribuidora": "São Paulo",
        },
    )

    assert workflow.status == "running"
    assert workflow.current_step == "sm_prepare_input"
    first_step = workflow.steps[0]
    assert first_step["step_name"] == "sm_prepare_input"
    assert first_step["job_id"]

    service.complete_job(
        job_id=first_step["job_id"],
        status="success",
        result={"status": "success"},
    )
    advanced = service.get_workflow(run_id="sm-workflow-01")
    assert advanced is not None
    assert advanced.current_step == "sm_sap_extract"
    second_step = advanced.steps[1]
    assert second_step["status"] == "queued"
    assert second_step["job_id"]

    service.complete_job(
        job_id=second_step["job_id"],
        status="failed",
        result={"status": "failed"},
        error_message="SAP offline",
    )
    failed = service.get_workflow(run_id="sm-workflow-01")
    assert failed is not None
    assert failed.status == "failed"
    assert failed.steps[2]["status"] == "pending"


def test_register_artifact_replaces_metadata_for_same_name(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    artifact_path = tmp_path / "SM_INSTALLATIONS.csv"
    artifact_path.write_text("ID_RECLAMAÇÃO\nA1\n", encoding="utf-8")

    first = service.register_artifact(
        run_id="run-artifact",
        artifact_name="SM_INSTALLATIONS.csv",
        kind="sm_input",
        path=artifact_path,
        producer_job_id="job-1",
        sha256="a" * 64,
        size_bytes=artifact_path.stat().st_size,
    )
    second = service.register_artifact(
        run_id="run-artifact",
        artifact_name="SM_INSTALLATIONS.csv",
        kind="sm_input",
        path=artifact_path,
        producer_job_id="job-2",
        sha256="b" * 64,
        size_bytes=artifact_path.stat().st_size,
    )

    assert second.artifact_id == first.artifact_id
    assert second.producer_job_id == "job-2"
    assert service.get_artifact(run_id="run-artifact", artifact_name="SM_INSTALLATIONS.csv") is not None
