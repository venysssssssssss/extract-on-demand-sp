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


class WorkflowRequest(BaseModel):
    workflow_type: str = Field(default="sm_sala_mercado_daily", min_length=1)
    demandante: str = Field(default="SALA_MERCADO", min_length=1)
    run_id: str | None = Field(default=None)
    reference: str = Field(default="current_month")
    payload: dict[str, Any] = Field(default_factory=dict)


class WorkflowListResponse(BaseModel):
    data: list[dict[str, Any]]


class ArtifactListResponse(BaseModel):
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
    reference: str | None = Field(
        default=None,
        description="Optional YYYYMM reference month override. Used by demandantes with standalone IW59 month selection logic.",
    )
    input_csv_path: str | None = Field(
        default=None,
        description="Optional standalone CSV input path used by IW59 demandantes that do not replay CA/RL/WB outputs.",
    )

    @field_validator("reference")
    @classmethod
    def validate_reference(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if len(normalized) != 6 or not normalized.isdigit():
            raise ValueError("reference must use YYYYMM format.")
        year = int(normalized[:4])
        month = int(normalized[4:])
        if year < 2000 or year > 2040 or month < 1 or month > 12:
            raise ValueError("reference must be a valid YYYYMM between 200001 and 204012.")
        return normalized


class DwRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="DW")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    max_rows: int = Field(default=0, ge=0)


class MedidorRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="MEDIDOR")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    installations_path: str | None = Field(
        default=None,
        description="Optional XLSX path containing the INSTALACAO column. Defaults to profile config.",
    )
    group_map_path: str | None = Field(
        default=None,
        description="Optional XLSX path containing Grp.registrad. and Tipo columns. Defaults to profile config.",
    )
    installations_source: str = Field(
        default="workbook",
        description="Installation source: workbook/csv path or db. Use db to read TBL_REINCIDENCIA_SM.ALIMENTADOR.",
    )
    distribuidora: str = Field(default="São Paulo")
    source_column: str = Field(default="ALIMENTADOR")
    extract_only: bool = Field(default=False)
    ingest_only: bool = Field(default=False)
    fetch_installations_only: bool = Field(
        default=False,
        description="If true, only fetch DB installations and write output/runs/{run_id}/medidor/input/MEDIDOR_INSTALLATIONS.csv.",
    )
    final_csv_path: str | None = Field(
        default=None,
        description="Optional MEDIDOR final CSV path used when ingest_only=true.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Extraction run_id used to locate output/runs/{source_run_id}/medidor/normalized/medidor_*.csv.",
    )


class SmRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="SALA_MERCADO")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    month: int | None = Field(default=None, ge=1, le=12)
    year: int | None = Field(default=None, ge=2020, le=2040)
    distribuidora: str = Field(default="São Paulo")
    installations: list[str] | None = Field(default=None)
    installations_csv_path: str | None = Field(default=None, description="Path to a CSV file containing installations.")
    skip_ingest: bool = Field(default=False)


class SmIngestRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    results: list[dict[str, Any]] | None = Field(default=None)
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    fetch_only: bool = Field(default=False, description="If true, only fetch installations from DB and save to CSV.")
    final_csv_path: str | None = Field(
        default=None,
        description="Path to SM_DADOS_FATURA.csv generated by /api/v1/extractions/sm with skip_ingest=true.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Extraction run_id used to locate output/runs/{source_run_id}/sm/SM_DADOS_FATURA.csv.",
    )
    month: int | None = Field(default=None)
    year: int | None = Field(default=None)
    distribuidora: str = Field(default="São Paulo")


class ValidaDaniRunRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    demandante: str = Field(default="VALIDA_DANI")
    output_root: str = Field(default="output")
    config_path: str = Field(default="sap_iw69_batch_config.json")
    input_path: str = Field(
        default="output/runs/20260330T171500/iw51/working/projeto_Dani2.xlsm",
        description="Path to the original DANI spreadsheet on the machine running the API.",
    )
    chunk_size: int = Field(default=5000, ge=1, le=5000)
    created_by: str = Field(default="BR0041761455")


class CurlExamplesResponse(BaseModel):
    commands: list[str]
