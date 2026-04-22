from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from dotenv import load_dotenv

load_dotenv()

from .api_models import (
    BatchManifestResponse,
    BatchRunRequest,
    CurlExamplesResponse,
    DwRunRequest,
    HealthResponse,
    Iw51RunRequest,
    Iw59RunRequest,
    JobListResponse,
    JobResponse,
    MedidorRunRequest,
    RunnerListResponse,
    ScheduleListResponse,
    ScheduleRequest,
    SmRunRequest,
    SmIngestRequest,
)
from .contracts import BatchRunPayload
from .errors import SapAutomationError
from .service import (
    create_control_plane_service,
    load_batch_manifest,
    run_batch_payload,
    run_dw_payload,
    run_iw51_payload,
    run_iw59_payload,
    run_medidor_payload,
    run_sm_payload,
)
from .sm import ingest_sm_results

app = FastAPI(
    title="SAP IW69 Batch API",
    version="0.2.1",
    description="Hybrid SAP control plane: legacy synchronous extraction endpoints plus queued jobs, schedules and Windows runner orchestration.",
)


def _build_payload(request: BatchRunRequest) -> BatchRunPayload:
    return BatchRunPayload(
        run_id=request.run_id,
        reference=request.reference,
        from_date=request.from_date,
        to_date=request.to_date,
        demandante=request.demandante,
        output_root=Path(request.output_root),
        objects=request.objects,
        regional=request.regional,
        config_path=Path(request.config_path),
        legacy_compatibility=request.legacy_compatibility,
        include_iw59_placeholder=request.include_iw59_placeholder,
    )


def _build_iw51_kwargs(request: Iw51RunRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "demandante": request.demandante,
        "output_root": Path(request.output_root),
        "config_path": Path(request.config_path),
        "max_rows": request.max_rows or None,
    }


def _build_iw59_kwargs(request: Iw59RunRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "demandante": request.demandante,
        "output_root": Path(request.output_root),
        "config_path": Path(request.config_path),
        "reference": request.reference,
        "input_csv_path": Path(request.input_csv_path) if request.input_csv_path else None,
    }


def _build_dw_kwargs(request: DwRunRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "demandante": request.demandante,
        "output_root": Path(request.output_root),
        "config_path": Path(request.config_path),
        "max_rows": request.max_rows or None,
    }


def _build_medidor_kwargs(request: MedidorRunRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "demandante": request.demandante,
        "output_root": Path(request.output_root),
        "config_path": Path(request.config_path),
        "installations_path": Path(request.installations_path) if request.installations_path else None,
        "group_map_path": Path(request.group_map_path) if request.group_map_path else None,
    }


def _build_sm_kwargs(request: SmRunRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "demandante": request.demandante,
        "output_root": Path(request.output_root),
        "config_path": Path(request.config_path),
        "month": request.month,
        "year": request.year,
        "distribuidora": request.distribuidora,
        "installations": request.installations,
        "installations_csv_path": Path(request.installations_csv_path) if request.installations_csv_path else None,
        "skip_ingest": request.skip_ingest,
    }


def _queue_iw69_job(request: BatchRunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_payload(request)
    job = service.create_job(
        flow_type="iw69",
        demandante=payload.demandante,
        payload={
            "run_id": payload.run_id,
            "reference": payload.reference,
            "from_date": payload.from_date,
            "to_date": payload.to_date,
            "demandante": payload.demandante,
            "output_root": str(payload.output_root),
            "objects": payload.objects,
            "regional": payload.regional,
            "config_path": str(payload.config_path),
            "legacy_compatibility": payload.legacy_compatibility,
            "include_iw59_placeholder": payload.include_iw59_placeholder,
        },
    )
    return job.__dict__


def _queue_iw51_job(request: Iw51RunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_iw51_kwargs(request)
    job = service.create_job(
        flow_type="iw51",
        demandante=str(payload["demandante"]),
        payload={
            "run_id": str(payload["run_id"]),
            "demandante": str(payload["demandante"]),
            "output_root": str(payload["output_root"]),
            "config_path": str(payload["config_path"]),
            "max_rows": payload["max_rows"] or 0,
        },
    )
    return job.__dict__


def _queue_iw59_job(request: Iw59RunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_iw59_kwargs(request)
    job = service.create_job(
        flow_type="iw59",
        demandante=str(payload["demandante"]),
        payload={
            "run_id": str(payload["run_id"]),
            "demandante": str(payload["demandante"]),
            "output_root": str(payload["output_root"]),
            "config_path": str(payload["config_path"]),
            "reference": str(payload["reference"] or "").strip(),
            "input_csv_path": str(payload["input_csv_path"]) if payload.get("input_csv_path") else "",
        },
    )
    return job.__dict__


def _queue_dw_job(request: DwRunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_dw_kwargs(request)
    job = service.create_job(
        flow_type="dw",
        demandante=str(payload["demandante"]),
        payload={
            "run_id": str(payload["run_id"]),
            "demandante": str(payload["demandante"]),
            "output_root": str(payload["output_root"]),
            "config_path": str(payload["config_path"]),
            "max_rows": payload["max_rows"] or 0,
        },
    )
    return job.__dict__


def _queue_medidor_job(request: MedidorRunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_medidor_kwargs(request)
    job = service.create_job(
        flow_type="medidor",
        demandante=str(payload["demandante"]),
        payload={
            "run_id": str(payload["run_id"]),
            "demandante": str(payload["demandante"]),
            "output_root": str(payload["output_root"]),
            "config_path": str(payload["config_path"]),
            "installations_path": str(payload["installations_path"]) if payload.get("installations_path") else "",
            "group_map_path": str(payload["group_map_path"]) if payload.get("group_map_path") else "",
        },
    )
    return job.__dict__


def _queue_sm_job(request: SmRunRequest) -> dict[str, Any]:
    service = create_control_plane_service()
    payload = _build_sm_kwargs(request)
    job = service.create_job(
        flow_type="sm",
        demandante=str(payload["demandante"]),
        payload={
            "run_id": str(payload["run_id"]),
            "demandante": str(payload["demandante"]),
            "output_root": str(payload["output_root"]),
            "config_path": str(payload["config_path"]),
            "month": payload["month"],
            "year": payload["year"],
            "distribuidora": str(payload["distribuidora"]),
            "installations": payload["installations"],
            "installations_csv_path": str(payload["installations_csv_path"]) if payload.get("installations_csv_path") else "",
            "skip_ingest": payload["skip_ingest"],
        },
    )
    return job.__dict__


@app.get("/health", response_model=HealthResponse, tags=["infra"])
def health() -> HealthResponse:
    service = create_control_plane_service()
    runners = service.list_runners()
    return HealthResponse(
        components={
            "control_plane": "ok",
            "runner_count": len(runners),
            "runners": [item.__dict__ for item in runners],
        }
    )


@app.get("/api/v1/extractions/iw69/curl", response_model=CurlExamplesResponse, tags=["iw69"])
def curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw69 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260310T090000\",\"reference\":\"202603\",\"from_date\":\"2026-01-01\",\"to_date\":\"2026-01-31\","
            f"\"demandante\":\"IGOR\",\"output_root\":\"{output_root}\",\"objects\":[\"CA\",\"RL\",\"WB\"],"
            f"\"config_path\":\"{config_path}\"}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw69 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260310T090000\",\"reference\":\"202603\",\"from_date\":\"2026-01-01\",\"to_date\":\"2026-01-31\","
            f"\"demandante\":\"IGOR\",\"output_root\":\"{output_root}\",\"objects\":[\"CA\",\"RL\",\"WB\"],"
            f"\"config_path\":\"{config_path}\"}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.get("/api/v1/extractions/iw51/curl", response_model=CurlExamplesResponse, tags=["iw51"])
def iw51_curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw51 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260326T090000\",\"demandante\":\"DANI\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"max_rows\":4}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw51 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260326T090000\",\"demandante\":\"DANI\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"max_rows\":4}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.get("/api/v1/extractions/iw59/curl", response_model=CurlExamplesResponse, tags=["iw59"])
def iw59_curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw59 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260326T100000\",\"demandante\":\"MANU\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\"}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw59 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260326T100000\",\"demandante\":\"MANU\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\"}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw59 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260402T090000\",\"demandante\":\"KELLY\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"input_csv_path\":\"brs_filtrados.csv\"}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw59 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260410T090000\",\"demandante\":\"KELLY\",\"reference\":\"202603\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"input_csv_path\":\"brs_filtrados.csv\"}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.get("/api/v1/extractions/dw/curl", response_model=CurlExamplesResponse, tags=["dw"])
def dw_curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/dw "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260327T160000\",\"demandante\":\"DW\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"max_rows\":30}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/dw "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260327T160000\",\"demandante\":\"DW\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"max_rows\":30}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.get("/api/v1/extractions/medidor/curl", response_model=CurlExamplesResponse, tags=["medidor"])
def medidor_curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/medidor "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260416T090000\",\"demandante\":\"MEDIDOR\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"installations_path\":\"instalacaosp.xlsx\","
            f"\"group_map_path\":\"gruporegsap.xlsx\"}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/medidor "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260416T090000\",\"demandante\":\"MEDIDOR\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"installations_path\":\"instalacaosp.xlsx\","
            f"\"group_map_path\":\"gruporegsap.xlsx\"}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.get("/api/v1/extractions/sm/curl", response_model=CurlExamplesResponse, tags=["sm"])
def sm_curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/sm "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260422T113000\",\"demandante\":\"SALA_MERCADO\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"month\":4,\"year\":2026}}'"
        ),
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/jobs/sm "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260422T113000\",\"demandante\":\"SALA_MERCADO\",\"output_root\":\"{output_root}\","
            f"\"config_path\":\"{config_path}\",\"month\":4,\"year\":2026}}'"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.post("/api/v1/extractions/iw69", response_model=BatchManifestResponse, tags=["iw69"])
async def run_iw69_batch(request: BatchRunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_batch_payload, _build_payload(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/extractions/iw51", response_model=BatchManifestResponse, tags=["iw51"])
async def run_iw51(request: Iw51RunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_iw51_payload, **_build_iw51_kwargs(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/extractions/iw59", response_model=BatchManifestResponse, tags=["iw59"])
async def run_iw59(request: Iw59RunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_iw59_payload, **_build_iw59_kwargs(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/extractions/dw", response_model=BatchManifestResponse, tags=["dw"])
async def run_dw(request: DwRunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_dw_payload, **_build_dw_kwargs(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/extractions/medidor", response_model=BatchManifestResponse, tags=["medidor"])
async def run_medidor(request: MedidorRunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_medidor_payload, **_build_medidor_kwargs(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/extractions/sm", response_model=BatchManifestResponse, tags=["sm"])
async def run_sm(request: SmRunRequest) -> BatchManifestResponse:
    try:
        manifest = await run_in_threadpool(run_sm_payload, **_build_sm_kwargs(request))
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        import logging
        logging.error("SM extraction failed with 400: %s", exc, exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.post("/api/v1/ingest/sm", response_model=BatchManifestResponse, tags=["sm"])
async def ingest_sm(request: SmIngestRequest) -> BatchManifestResponse:
    try:
        result = await run_in_threadpool(
            ingest_sm_results,
            run_id=request.run_id,
            results=request.results,
            output_root=Path("output"),
            config_path=Path(request.config_path),
            fetch_only=request.fetch_only,
            month=request.month,
            year=request.year,
            distribuidora=request.distribuidora,
        )
        if result.status == "failed":
            raise HTTPException(status_code=400, detail=result.error)
        return BatchManifestResponse(data=result.to_dict())
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/iw69", response_model=JobResponse, tags=["jobs"])
async def queue_iw69_batch(request: BatchRunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_iw69_job(request))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/iw51", response_model=JobResponse, tags=["jobs"])
async def queue_iw51(request: Iw51RunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_iw51_job(request))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/iw59", response_model=JobResponse, tags=["jobs"])
async def queue_iw59(request: Iw59RunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_iw59_job(request))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/dw", response_model=JobResponse, tags=["jobs"])
async def queue_dw(request: DwRunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_dw_job(request))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/medidor", response_model=JobResponse, tags=["jobs"])
async def queue_medidor(request: MedidorRunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_medidor_job(request))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/jobs/sm", response_model=JobResponse, tags=["jobs"])
async def queue_sm(request: SmRunRequest) -> JobResponse:
    try:
        return JobResponse(data=_queue_sm_job(request))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/v1/jobs", response_model=JobListResponse, tags=["jobs"])
def list_jobs(limit: int = Query(100, ge=1, le=500)) -> JobListResponse:
    service = create_control_plane_service()
    return JobListResponse(data=[item.__dict__ for item in service.list_jobs(limit=limit)])


@app.get("/api/v1/jobs/{job_id}", response_model=JobResponse, tags=["jobs"])
def get_job(job_id: str) -> JobResponse:
    service = create_control_plane_service()
    job = service.get_job(job_id=job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    return JobResponse(data=job.__dict__)


@app.post("/api/v1/jobs/{job_id}/cancel", response_model=JobResponse, tags=["jobs"])
def cancel_job(job_id: str) -> JobResponse:
    service = create_control_plane_service()
    job = service.cancel_job(job_id=job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    return JobResponse(data=job.__dict__)


@app.get("/api/v1/schedules", response_model=ScheduleListResponse, tags=["schedules"])
def list_schedules() -> ScheduleListResponse:
    service = create_control_plane_service()
    return ScheduleListResponse(data=[item.__dict__ for item in service.list_schedules()])


@app.post("/api/v1/schedules", response_model=JobResponse, tags=["schedules"])
def upsert_schedule(request: ScheduleRequest) -> JobResponse:
    service = create_control_plane_service()
    schedule = service.upsert_schedule(
        schedule_id=request.schedule_id,
        enabled=request.enabled,
        flow_type=request.flow_type,
        demandante=request.demandante,
        cron_expression=request.cron_expression,
        timezone=request.timezone,
        payload_template=request.payload_template,
        misfire_policy=request.misfire_policy,
        queue_name=request.queue_name,
    )
    return JobResponse(data=schedule.__dict__)


@app.get("/api/v1/runners", response_model=RunnerListResponse, tags=["infra"])
def list_runners() -> RunnerListResponse:
    service = create_control_plane_service()
    return RunnerListResponse(data=[item.__dict__ for item in service.list_runners()])


@app.get("/api/v1/runners/{runner_id}", response_model=JobResponse, tags=["infra"])
def get_runner(runner_id: str) -> JobResponse:
    service = create_control_plane_service()
    for item in service.list_runners():
        if item.runner_id == runner_id:
            return JobResponse(data=item.__dict__)
    raise HTTPException(status_code=404, detail=f"Runner not found: {runner_id}")


@app.get("/api/v1/extractions/iw69/{run_id}/manifest", response_model=BatchManifestResponse, tags=["iw69"])
def get_iw69_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    manifest_path = Path(output_root).expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"Batch manifest not found for run_id={run_id}.")
    data: dict[str, Any] = load_batch_manifest(output_root=Path(output_root), run_id=run_id)
    return BatchManifestResponse(data=data)


@app.get("/api/v1/extractions/iw51/{run_id}/manifest", response_model=BatchManifestResponse, tags=["iw51"])
def get_iw51_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    manifest_path = Path(output_root).expanduser().resolve() / "runs" / run_id / "iw51" / "iw51_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"IW51 manifest not found for run_id={run_id}.")
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return BatchManifestResponse(data=data)


@app.get("/api/v1/extractions/iw59/{run_id}/manifest", response_model=BatchManifestResponse, tags=["iw59"])
def get_iw59_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    metadata_dir = Path(output_root).expanduser().resolve() / "runs" / run_id / "iw59" / "metadata"
    aggregate_manifest_path = metadata_dir / f"iw59_{run_id}.manifest.json"
    if aggregate_manifest_path.exists():
        data = json.loads(aggregate_manifest_path.read_text(encoding="utf-8"))
        return BatchManifestResponse(data=data)
    manifest_candidates = sorted(metadata_dir.glob("iw59_*.manifest.json"))
    if not manifest_candidates:
        raise HTTPException(status_code=404, detail=f"IW59 manifest not found for run_id={run_id}.")
    data = json.loads(manifest_candidates[-1].read_text(encoding="utf-8"))
    return BatchManifestResponse(data=data)


@app.get("/api/v1/extractions/dw/{run_id}/manifest", response_model=BatchManifestResponse, tags=["dw"])
def get_dw_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    manifest_path = Path(output_root).expanduser().resolve() / "runs" / run_id / "dw" / "dw_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"DW manifest not found for run_id={run_id}.")
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return BatchManifestResponse(data=data)


@app.get("/api/v1/extractions/medidor/{run_id}/manifest", response_model=BatchManifestResponse, tags=["medidor"])
def get_medidor_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    metadata_dir = Path(output_root).expanduser().resolve() / "runs" / run_id / "medidor" / "metadata"
    manifest_candidates = sorted(metadata_dir.glob("medidor_*.manifest.json"))
    if not manifest_candidates:
        raise HTTPException(status_code=404, detail=f"MEDIDOR manifest not found for run_id={run_id}.")
    data = json.loads(manifest_candidates[-1].read_text(encoding="utf-8"))
    return BatchManifestResponse(data=data)


@app.get("/api/v1/extractions/sm/{run_id}/manifest", response_model=BatchManifestResponse, tags=["sm"])
def get_sm_manifest(run_id: str, output_root: str = Query("output")) -> BatchManifestResponse:
    manifest_path = Path(output_root).expanduser().resolve() / "runs" / run_id / "sm" / "sm_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"SM manifest not found for run_id={run_id}.")
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return BatchManifestResponse(data=data)
