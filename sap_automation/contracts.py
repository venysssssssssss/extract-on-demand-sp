from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

SUPPORTED_IW69_OBJECTS: tuple[str, ...] = ("CA", "RL", "WB")
LEGACY_FILENAME_BY_OBJECT: dict[str, str] = {
    "CA": "BASE_AUTOMACAO_CA.txt",
    "RL": "BASE_AUTOMACAO_RL.txt",
    "WB": "BASE_AUTOMACAO_WB.txt",
}


def _normalize_object_code(value: str) -> str:
    token = str(value).strip().upper()
    if not token:
        raise ValueError("object_code must be non-empty.")
    return token


@dataclass(frozen=True)
class ObjectArtifactPaths:
    object_code: str
    object_root: Path
    raw_dir: Path
    normalized_dir: Path
    metadata_dir: Path
    raw_txt_path: Path
    canonical_csv_path: Path
    raw_csv_path: Path
    header_map_path: Path
    rejects_path: Path
    metadata_path: Path
    legacy_copy_path: Path
    log_path: Path

    def ensure_directories(self) -> None:
        for path in (
            self.raw_dir,
            self.normalized_dir,
            self.metadata_dir,
            self.legacy_copy_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ExportJobSpec:
    object_code: str
    run_id: str
    reference: str
    from_date: str
    output_root: Path
    regional: str = "SP"
    transaction_code: str = "IW69"
    variant_name: str = "/BATISTAO"
    status_user: str = ""
    config_path: Path = Path("sap_iw69_batch_config.json")
    required_fields: list[str] = field(default_factory=lambda: ["Nota"])
    filters: list[dict[str, str]] = field(default_factory=list)
    legacy_compatibility: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_code", _normalize_object_code(self.object_code))
        object.__setattr__(self, "run_id", str(self.run_id).strip())
        object.__setattr__(self, "reference", str(self.reference).strip())
        object.__setattr__(self, "from_date", str(self.from_date).strip())
        if self.object_code not in SUPPORTED_IW69_OBJECTS:
            raise ValueError(
                f"Unsupported IW69 object '{self.object_code}'. "
                f"Supported: {', '.join(SUPPORTED_IW69_OBJECTS)}."
            )
        if not self.run_id:
            raise ValueError("run_id must be non-empty.")
        if not self.reference:
            raise ValueError("reference must be non-empty.")
        if not self.from_date:
            raise ValueError("from_date must be non-empty.")

    @property
    def object_slug(self) -> str:
        return self.object_code.casefold()

    @property
    def export_filename(self) -> str:
        return f"{LEGACY_FILENAME_BY_OBJECT[self.object_code][:-4]}_{self.run_id}.txt"

    @property
    def legacy_filename(self) -> str:
        return LEGACY_FILENAME_BY_OBJECT[self.object_code]

    @property
    def sqvi_name(self) -> str:
        return self.transaction_code


@dataclass(frozen=True)
class Iw59JobSpec:
    run_id: str
    reference: str
    from_date: str
    output_root: Path
    status: Literal["pending_configuration"] = "pending_configuration"
    reason: str = "IW59 SAP GUI script not provided yet."


@dataclass(frozen=True)
class BatchRunPayload:
    run_id: str
    reference: str
    from_date: str
    output_root: Path
    objects: list[str] = field(default_factory=lambda: list(SUPPORTED_IW69_OBJECTS))
    regional: str = "SP"
    config_path: Path = Path("sap_iw69_batch_config.json")
    legacy_compatibility: bool = True
    include_iw59_placeholder: bool = True

    def build_jobs(self) -> list[ExportJobSpec]:
        return [
            ExportJobSpec(
                object_code=object_code,
                run_id=self.run_id,
                reference=self.reference,
                from_date=self.from_date,
                output_root=self.output_root,
                regional=self.regional,
                config_path=self.config_path,
                legacy_compatibility=self.legacy_compatibility,
            )
            for object_code in self.objects
        ]


@dataclass(frozen=True)
class ObjectManifest:
    object_code: str
    status: Literal["success", "failed", "pending_configuration"]
    rows_exported: int = 0
    error: str = ""
    raw_txt_path: str = ""
    canonical_csv_path: str = ""
    raw_csv_path: str = ""
    header_map_path: str = ""
    rejects_path: str = ""
    metadata_path: str = ""
    legacy_copy_path: str = ""
    log_path: str = ""
    source_txt_path: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConsolidationManifest:
    status: Literal["success", "partial", "skipped"]
    notes_path: str = ""
    interactions_path: str = ""
    rows_notes: int = 0
    rows_interactions: int = 0
    missing_objects: list[str] = field(default_factory=list)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BatchManifest:
    run_id: str
    reference: str
    from_date: str
    status: Literal["success", "partial", "failed"]
    output_root: str
    objects: list[dict[str, Any]]
    consolidation: dict[str, Any]
    pending_stages: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
