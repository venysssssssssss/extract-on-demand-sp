from __future__ import annotations

from fastapi.testclient import TestClient

import sap_automation.api as api_module


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
