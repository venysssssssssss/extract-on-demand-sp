from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "sap-iw69-batch-api"
    components: dict[str, Any] = Field(default_factory=dict)


class BatchRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    reference: str = Field(..., min_length=1)
    from_date: str = Field(..., description="SAP DATUV lower bound in ISO format (YYYY-MM-DD).")
    to_date: str | None = Field(
        default=None,
        description="Optional SAP DATUV upper bound in ISO format (YYYY-MM-DD). Defaults to from_date.",
    )
    demandante: str = Field(default="IGOR")
    output_root: str = Field(default="output")
    objects: list[str] = Field(default_factory=lambda: ["CA", "RL", "WB"])
    config_path: str = Field(default="sap_iw69_batch_config.json")
    regional: str = Field(default="SP")
    legacy_compatibility: bool = True
    include_iw59_placeholder: bool = True

    @field_validator("objects")
    @classmethod
    def normalize_objects(cls, value: list[str]) -> list[str]:
        normalized = [item.strip().upper() for item in value if item.strip()]
        return normalized or ["CA", "RL", "WB"]


class BatchManifestResponse(BaseModel):
    data: dict[str, Any]


class JobResponse(BaseModel):
    data: dict[str, Any]


class JobListResponse(BaseModel):
    data: list[dict[str, Any]]


class ScheduleRequest(BaseModel):
    schedule_id: str = Field(..., min_length=1)
    enabled: bool = True
    flow_type: str = Field(..., min_length=1)
    demandante: str = Field(..., min_length=1)
    cron_expression: str = Field(..., min_length=1)
    timezone: str = Field(default="America/Bahia", min_length=1)
    payload_template: dict[str, Any] = Field(default_factory=dict)
    misfire_policy: str = Field(default="catch_up", min_length=1)
    queue_name: str = Field(default="sap-default", min_length=1)


class RunnerListResponse(BaseModel):
    data: list[dict[str, Any]]


class ScheduleListResponse(BaseModel):
    data: list[dict[str, Any]]


class Iw51RunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="DANI")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    max_rows: int = Field(default=0, ge=0)


class Iw59RunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="IGOR")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")


class DwRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="DW")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    max_rows: int = Field(default=0, ge=0)


class CurlExamplesResponse(BaseModel):
    commands: list[str]
