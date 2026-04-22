from __future__ import annotations

from fastapi.testclient import TestClient

import sap_automation.api as api_module
from sap_automation.control_plane import ControlPlaneService, ControlPlaneSettings, InMemoryJobQueue


class _Manifest:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def to_dict(self) -> dict[str, object]:
        return dict(self._payload)


client = TestClient(api_module.app)


def test_legacy_iw69_endpoint_preserves_synchronous_execution(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "run_batch_payload",
        lambda payload: _Manifest({"run_id": payload.run_id, "status": "success", "kind": "sync"}),
    )

    response = client.post(
        "/api/v1/extractions/iw69",
        json={
            "run_id": "20260401T170000",
            "reference": "202604",
            "from_date": "2026-04-01",
            "to_date": "2026-04-30",
            "demandante": "IGOR",
            "output_root": "output",
            "objects": ["CA", "RL", "WB"],
            "config_path": "sap_iw69_batch_config.json",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["kind"] == "sync"
    assert response.json()["data"]["status"] == "success"


def test_legacy_iw51_endpoint_preserves_synchronous_execution(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "run_iw51_payload",
        lambda **kwargs: _Manifest({"run_id": kwargs["run_id"], "status": "success", "kind": "sync-iw51"}),
    )

    response = client.post(
        "/api/v1/extractions/iw51",
        json={
            "run_id": "20260401T171000",
            "demandante": "DANI",
            "output_root": "output",
            "config_path": "sap_iw69_batch_config.json",
            "max_rows": 3,
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["kind"] == "sync-iw51"


def test_job_iw69_endpoint_enqueues_without_running(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "_queue_iw69_job",
        lambda request: {"job_id": "job-123", "status": "queued", "flow_type": "iw69"},
    )

    response = client.post(
        "/api/v1/jobs/iw69",
        json={
            "run_id": "20260401T172000",
            "reference": "202604",
            "from_date": "2026-04-01",
            "to_date": "2026-04-30",
            "demandante": "IGOR",
            "output_root": "output",
            "objects": ["CA", "RL", "WB"],
            "config_path": "sap_iw69_batch_config.json",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["job_id"] == "job-123"
    assert response.json()["data"]["status"] == "queued"


def test_job_dw_endpoint_enqueues_without_running(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "_queue_dw_job",
        lambda request: {"job_id": "job-dw-1", "status": "queued", "flow_type": "dw"},
    )

    response = client.post(
        "/api/v1/jobs/dw",
        json={
            "run_id": "20260401T173000",
            "demandante": "DW",
            "output_root": "output",
            "config_path": "sap_iw69_batch_config.json",
            "max_rows": 10,
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["flow_type"] == "dw"
    assert response.json()["data"]["status"] == "queued"


def test_legacy_medidor_endpoint_preserves_synchronous_execution(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "run_medidor_payload",
        lambda **kwargs: _Manifest({"run_id": kwargs["run_id"], "status": "success", "kind": "sync-medidor"}),
    )

    response = client.post(
        "/api/v1/extractions/medidor",
        json={
            "run_id": "20260416T090000",
            "demandante": "MEDIDOR",
            "output_root": "output",
            "config_path": "sap_iw69_batch_config.json",
            "installations_path": "instalacaosp.xlsx",
            "group_map_path": "gruporegsap.xlsx",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["kind"] == "sync-medidor"


def test_job_medidor_endpoint_enqueues_without_running(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        api_module,
        "_queue_medidor_job",
        lambda request: {"job_id": "job-medidor-1", "status": "queued", "flow_type": "medidor"},
    )

    response = client.post(
        "/api/v1/jobs/medidor",
        json={
            "run_id": "20260416T091000",
            "demandante": "MEDIDOR",
            "output_root": "output",
            "config_path": "sap_iw69_batch_config.json",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["flow_type"] == "medidor"
    assert response.json()["data"]["status"] == "queued"


def test_artifact_upload_list_download_roundtrip(monkeypatch, tmp_path) -> None:  # noqa: ANN001
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
    service = ControlPlaneService(settings=settings, queue=InMemoryJobQueue())
    monkeypatch.setattr(api_module, "create_control_plane_service", lambda: service)

    upload = client.post(
        "/api/v1/artifacts/run-api/SM_INSTALLATIONS.csv?kind=sm_input&producer_job_id=job-1",
        content=b"ID_RECLAMACAO\nA1\n",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert upload.status_code == 200
    assert upload.json()["data"]["artifact_name"] == "SM_INSTALLATIONS.csv"
    assert upload.json()["data"]["producer_job_id"] == "job-1"

    listed = client.get("/api/v1/artifacts/run-api")
    assert listed.status_code == 200
    assert listed.json()["data"][0]["artifact_name"] == "SM_INSTALLATIONS.csv"

    downloaded = client.get("/api/v1/artifacts/run-api/SM_INSTALLATIONS.csv")
    assert downloaded.status_code == 200
    assert downloaded.content == b"ID_RECLAMACAO\nA1\n"


def test_workflow_endpoint_creates_sm_pipeline(monkeypatch, tmp_path) -> None:  # noqa: ANN001
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
    service = ControlPlaneService(settings=settings, queue=InMemoryJobQueue())
    monkeypatch.setattr(api_module, "create_control_plane_service", lambda: service)

    response = client.post(
        "/api/v1/workflows",
        json={
            "workflow_type": "sm_sala_mercado_daily",
            "demandante": "SALA_MERCADO",
            "run_id": "run-workflow-api",
            "reference": "202604",
            "payload": {"distribuidora": "São Paulo"},
        },
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["run_id"] == "run-workflow-api"
    assert data["current_step"] == "sm_prepare_input"
    assert [step["step_name"] for step in data["steps"]] == [
        "sm_prepare_input",
        "sm_sap_extract",
        "sm_ingest_final",
    ]
