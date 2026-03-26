from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from .api_models import BatchManifestResponse, BatchRunRequest, CurlExamplesResponse, HealthResponse
from .contracts import BatchRunPayload
from .errors import SapAutomationError
from .service import load_batch_manifest, run_batch_payload

app = FastAPI(
    title="SAP IW69 Batch API",
    version="0.1.0",
    description="HTTP wrapper for the IW69 CA/RL/WB SAP GUI extraction batch.",
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


@app.get("/health", response_model=HealthResponse, tags=["infra"])
def health() -> HealthResponse:
    return HealthResponse()


@app.get(
    "/api/v1/extractions/iw69/curl",
    response_model=CurlExamplesResponse,
    tags=["iw69"],
)
def curl_examples(
    output_root: str = Query("output"),
    config_path: str = Query("sap_iw69_batch_config.json"),
) -> CurlExamplesResponse:
    commands = [
        "uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000",
        (
            "curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw69 "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"run_id\":\"20260310T090000\",\"reference\":\"202603\","
            f"\"from_date\":\"2026-01-01\",\"to_date\":\"2026-01-31\",\"demandante\":\"IGOR\",\"output_root\":\"{output_root}\","
            "\"objects\":[\"CA\",\"RL\",\"WB\"],"
            f"\"config_path\":\"{config_path}\"}}'"
        ),
        (
            "curl http://127.0.0.1:8000/api/v1/extractions/iw69/"
            "20260310T090000/manifest?output_root="
            f"{output_root}"
        ),
    ]
    return CurlExamplesResponse(commands=commands)


@app.post(
    "/api/v1/extractions/iw69",
    response_model=BatchManifestResponse,
    tags=["iw69"],
)
async def run_iw69_batch(request: BatchRunRequest) -> BatchManifestResponse:
    payload = _build_payload(request)
    try:
        manifest = await run_in_threadpool(run_batch_payload, payload)
    except SapAutomationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BatchManifestResponse(data=manifest.to_dict())


@app.get(
    "/api/v1/extractions/iw69/{run_id}/manifest",
    response_model=BatchManifestResponse,
    tags=["iw69"],
)
def get_iw69_manifest(
    run_id: str,
    output_root: str = Query("output"),
) -> BatchManifestResponse:
    manifest_path = Path(output_root).expanduser().resolve() / "runs" / run_id / "batch_manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"Batch manifest not found for run_id={run_id}.")
    data: dict[str, Any] = load_batch_manifest(output_root=Path(output_root), run_id=run_id)
    return BatchManifestResponse(data=data)
