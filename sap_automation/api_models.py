from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "sap-iw69-batch-api"


class BatchRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    reference: str = Field(..., min_length=1)
    from_date: str = Field(..., description="SAP DATUV lower bound in ISO format (YYYY-MM-DD).")
    to_date: str | None = Field(
        default=None,
        description="Optional SAP DATUV upper bound in ISO format (YYYY-MM-DD). Defaults to from_date.",
    )
    coordinator: str = Field(default="IGOR")
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


class CurlExamplesResponse(BaseModel):
    commands: list[str]
