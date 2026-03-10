from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from charset_normalizer import from_bytes as charset_normalizer_from_bytes
except ImportError:  # pragma: no cover - optional dependency at runtime
    charset_normalizer_from_bytes = None


def _configure_csv_field_size_limit() -> int:
    limit = sys.maxsize
    while limit > 0:
        try:
            csv.field_size_limit(limit)
            return limit
        except OverflowError:
            limit //= 10
    csv.field_size_limit(131072)
    return 131072


def _ensure_project_root_on_sys_path() -> None:
    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        package_dir = parent / "elt_enel_sftp"
        if package_dir.exists() and package_dir.is_dir():
            project_root = str(parent)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            return


_ensure_project_root_on_sys_path()

from elt_enel_sftp.sap.header_profiles import SapHeaderProfile, get_header_profile

LOGGER_NAME = "sap_gui_export"
_DEFAULT_SOURCE_READY_TIMEOUT_SECONDS = 30.0
_DEFAULT_SOURCE_READY_POLL_SECONDS = 0.2
_DEFAULT_SOURCE_READY_STABLE_HITS = 2
_DEFAULT_STRICT_REQUIRED_FIELDS = True
_DEFAULT_COERCE_EXTRA_COLUMNS = False
_DEFAULT_HEADER_SCAN_MAX_LINES = 400
_DEFAULT_NORMALIZATION_TIMEOUT_SECONDS = 900.0
_DEFAULT_WRITE_RAW_CSV = True
_DEFAULT_CANONICAL_ENABLE = True
_DEFAULT_EXACT_CONTRACT_COLUMNS_FAT = False
_DEFAULT_EXACT_CONTRACT_COLUMNS_CLI = False
_DEFAULT_HEADER_MAX_SPAN_LINES = 2
_DEFAULT_HEADER_MIN_CONFIDENCE = 0.58
_DEFAULT_CANONICAL_FAIL_ON_LOW_CONFIDENCE = True
_DEFAULT_TEMP_PROMOTION_TIMEOUT_SECONDS = 20.0
_DEFAULT_TEMP_PROMOTION_POLL_SECONDS = 0.2
_DEFAULT_ENCODING_DETECTION_SAMPLE_BYTES = 262144
_DEFAULT_HEADER_PROBE_MAX_ENCODINGS = 4
_SOURCE_TEXT_BUFFERING = 64 * 1024
_CSV_FIELD_SIZE_LIMIT = _configure_csv_field_size_limit()
_DEFAULT_SOURCE_ENCODINGS: tuple[str, ...] = (
    "cp1252",
    "utf-8-sig",
    "utf-8",
    "latin-1",
    "iso-8859-1",
    "cp850",
    "cp437",
    "utf-16",
    "utf-16-le",
    "utf-16-be",
)
_NORMALIZATION_PROGRESS_EVERY_ROWS = 50000
_NORMALIZATION_TIMEOUT_CHECK_EVERY_ROWS = 2048
_SOURCE_WAIT_LOG_INTERVAL_SECONDS = 5.0
_EXPORT_TXT_PREFIX_BY_OBJECT: dict[str, tuple[str, ...]] = {
    "FAT": ("ZFAT", "FATURAMENTO"),
    "CLI": ("BASECLIENTES", "CLIENTES", "CLI"),
    "RISK": ("RISCO", "RISK", "OPERANDO"),
    "PDA": ("LEITURAS", "PDA"),
    "NOTE": ("BASEOAR", "NOTE", "NOTA"),
    "CA": ("BASE_AUTOMACAO_CA", "CA"),
    "RL": ("BASE_AUTOMACAO_RL", "RL"),
    "WB": ("BASE_AUTOMACAO_WB", "WB"),
}


def _parse_optional_path(value: Any) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return Path(raw)


@dataclass(frozen=True)
class ExportPayload:
    run_id: str
    object_code: str
    sqvi_name: str
    reference: str
    regional: str
    lot_ranges: list[list[int]]
    period_start: str | None
    period_end: str | None
    output_path: Path
    metadata_path: Path
    required_fields: list[str] = field(default_factory=list)
    filters: list[dict[str, str]] = field(default_factory=list)
    raw_csv_path: Path | None = None
    header_map_path: Path | None = None
    rejects_path: Path | None = None

    @classmethod
    def from_json(cls, payload_path: Path) -> "ExportPayload":
        raw = json.loads(payload_path.read_text(encoding="utf-8"))
        return cls(
            run_id=str(raw["run_id"]),
            object_code=str(raw["object"]),
            sqvi_name=str(raw["sqvi_name"]),
            reference=str(raw["reference"]),
            regional=str(raw["regional"]),
            lot_ranges=[list(item) for item in raw.get("lot_ranges", [])],
            period_start=(str(raw.get("period_start", "")).strip() or None),
            period_end=(str(raw.get("period_end", "")).strip() or None),
            output_path=Path(raw["output_path"]),
            metadata_path=Path(raw["metadata_path"]),
            raw_csv_path=_parse_optional_path(raw.get("raw_csv_path")),
            header_map_path=_parse_optional_path(raw.get("header_map_path")),
            rejects_path=_parse_optional_path(raw.get("rejects_path")),
            required_fields=[str(item) for item in raw.get("required_fields", [])],
            filters=[dict(item) for item in raw.get("filters", [])],
        )


@dataclass(frozen=True)
class HeaderCandidate:
    header: list[str]
    line_start: int
    line_end: int
    span: int
    confidence: float
    required_hits: int
    optional_hits: int
    negative_hits: int
    non_empty_count: int
    unnamed_count: int
    duplicate_count: int
    score: float


class FatalNormalizationConfigurationError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run SAP GUI Scripting steps for a single object export. "
            "This script is called by sap-extract in desktop mode."
        ),
    )
    parser.add_argument(
        "payload_path",
        help="Path to payload JSON generated by the sap-extract engine.",
    )
    parser.add_argument(
        "--config",
        default="",
        help=(
            "Optional config JSON path. "
            "Default: env SAP_GUI_EXPORT_CONFIG or sap_iw69_batch_config.json"
        ),
    )
    return parser.parse_args()


def load_config(config_arg: str, script_dir: Path) -> dict[str, Any]:
    if config_arg.strip():
        config_path = Path(config_arg).expanduser()
    else:
        env_value = _safe_env("SAP_GUI_EXPORT_CONFIG")
        env_path = Path(env_value).expanduser() if env_value else None
        if env_path is not None:
            config_path = env_path
        else:
            config_path = script_dir / "sap_iw69_batch_config.json"

    if not config_path.exists() or not config_path.is_file():
        raise FileNotFoundError(
            "SAP GUI export config not found. "
            f"Expected: {config_path}. "
            "Set SAP_GUI_EXPORT_CONFIG or create sap_iw69_batch_config.json."
        )

    return json.loads(config_path.read_text(encoding="utf-8"))


def setup_logger(
    *,
    payload: ExportPayload,
    output_path: Path,
    config: dict[str, Any],
) -> tuple[logging.Logger, Path]:
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    logger.propagate = False

    global_cfg = config.get("global", {})
    level_name = str(global_cfg.get("log_level", "INFO")).strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    context = {
        "run_id": payload.run_id,
        "object": payload.object_code,
        "sqvi_name": payload.sqvi_name,
        "reference": payload.reference,
        "output_path": str(output_path),
        "output_dir": str(output_path.parent),
        "output_file_name": output_path.name,
    }
    log_path_template = str(
        global_cfg.get(
            "log_path",
            "{output_dir}/{object}_{run_id}.sap_gui_export.log",
        )
    )
    log_path = Path(render_template(log_path_template, context)).expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger, log_path


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    _load_dotenv_for_script(script_dir=script_dir)

    payload_path = Path(args.payload_path)
    if not payload_path.exists() or not payload_path.is_file():
        print(f"[ERROR] Payload file not found: {payload_path}", file=sys.stderr)
        return 2

    payload = ExportPayload.from_json(payload_path)
    config = load_config(args.config, script_dir)

    output_path = payload.output_path.expanduser().resolve()
    metadata_path = payload.metadata_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    logger, log_path = setup_logger(
        payload=payload, output_path=output_path, config=config
    )

    object_config = resolve_object_config(config=config, payload=payload)
    context = build_context(payload=payload, output_path=output_path)
    global_cfg = config.get("global", {})
    wait_timeout_seconds = float(global_cfg.get("wait_timeout_seconds", 60.0))
    reset_transaction_code = str(
        global_cfg.get("reset_transaction_code", "ZISU0128")
    ).strip()
    raise_on_unwind_failure = parse_bool(
        global_cfg.get("raise_on_unwind_failure", False)
    )
    unwind_back_min_presses = int(global_cfg.get("unwind_back_min_presses", 4))
    unwind_back_max_presses = int(global_cfg.get("unwind_back_max_presses", 5))

    logger.info(
        f"[INFO] Starting SAP GUI export: object={payload.object_code} "
        f"sqvi={payload.sqvi_name} output={output_path} log={log_path}"
    )

    session = connect_sap_session(config=config, logger=logger)
    wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
    try:
        post_wait_seconds = float(global_cfg.get("post_export_wait_seconds", 8.0))
        source_ready_timeout_seconds = max(
            post_wait_seconds,
            _safe_float_env(
                "ENEL_SAP_EXPORT_SOURCE_READY_TIMEOUT_SECONDS",
                _DEFAULT_SOURCE_READY_TIMEOUT_SECONDS,
            ),
        )
        source_ready_poll_seconds = _safe_float_env(
            "ENEL_SAP_EXPORT_SOURCE_READY_POLL_SECONDS",
            _DEFAULT_SOURCE_READY_POLL_SECONDS,
        )
        source_ready_stable_hits = max(
            1,
            _safe_int_env(
                "ENEL_SAP_EXPORT_SOURCE_READY_STABLE_HITS",
                _DEFAULT_SOURCE_READY_STABLE_HITS,
            ),
        )
        continue_txt_on_csv_error = parse_bool(
            _safe_env("ENEL_SAP_CONTINUE_TXT_ON_CSV_ERROR") or "false"
        )
        disable_csv_after_first_error = parse_bool(
            _safe_env("ENEL_SAP_DISABLE_CSV_AFTER_FIRST_ERROR") or "false"
        )
        csv_skip_marker_path = (
            output_path.parent / f".{payload.run_id}.skip_csv_materialization"
        )
        skip_csv_materialization = (
            disable_csv_after_first_error and csv_skip_marker_path.exists()
        )
        if skip_csv_materialization:
            logger.warning(
                "CSV materialization is disabled for this run by marker file. "
                "Proceeding with TXT-only extraction flow: marker=%s",
                csv_skip_marker_path,
            )

        try:
            steps_started_at = time.time()
            logger.info(
                "SAP GUI steps start: object=%s total_steps=%s",
                payload.object_code,
                (
                    len(object_config.get("steps", []))
                    if isinstance(object_config.get("steps", []), list)
                    else 0
                ),
            )
            run_steps(
                session=session,
                steps=object_config.get("steps", []),
                context=context,
                default_timeout_seconds=wait_timeout_seconds,
                logger=logger,
            )
            logger.info(
                "SAP GUI steps completed: object=%s elapsed_s=%.2f",
                payload.object_code,
                time.time() - steps_started_at,
            )
        except Exception as step_error:
            recovered_rows = _try_recover_export_after_step_failure(
                payload=payload,
                output_path=output_path,
                metadata_path=metadata_path,
                object_config=object_config,
                context=context,
                required_fields=payload.required_fields,
                logger=logger,
                global_cfg=global_cfg,
                source_ready_timeout_seconds=source_ready_timeout_seconds,
                source_ready_poll_seconds=source_ready_poll_seconds,
                source_ready_stable_hits=source_ready_stable_hits,
                session=session,
                step_error=step_error,
            )
            if recovered_rows is not None:
                return 0
            raise

        rows_exported = _materialize_and_persist_metadata(
            payload=payload,
            output_path=output_path,
            metadata_path=metadata_path,
            object_config=object_config,
            context=context,
            required_fields=payload.required_fields,
            logger=logger,
            source_ready_timeout_seconds=source_ready_timeout_seconds,
            source_ready_poll_seconds=source_ready_poll_seconds,
            source_ready_stable_hits=source_ready_stable_hits,
            session=session,
            allow_csv_failure=continue_txt_on_csv_error,
            skip_csv_materialization=skip_csv_materialization,
        )
        if (
            continue_txt_on_csv_error
            and disable_csv_after_first_error
            and not output_path.exists()
            and not csv_skip_marker_path.exists()
        ):
            csv_skip_marker_path.touch()
            logger.warning(
                "CSV materialization failed once. Created run marker to skip CSV "
                "attempts for remaining chunks/objects in this run: marker=%s",
                csv_skip_marker_path,
            )
        logger.info(
            "[OK] Export finished: rows_exported=%s metadata=%s",
            rows_exported,
            metadata_path,
        )
        return 0
    finally:
        unwind_error: Exception | None = None
        try:
            _unwind_after_export_with_back(
                session=session,
                transaction_code=reset_transaction_code or "ZISU0128",
                timeout_seconds=wait_timeout_seconds,
                logger=logger,
                min_back_presses=unwind_back_min_presses,
                max_back_presses=unwind_back_max_presses,
            )
        except Exception as exc:  # noqa: BLE001
            unwind_error = exc
            logger.exception("Post-export back navigation failed.")
        if unwind_error is not None and raise_on_unwind_failure:
            raise unwind_error
        if unwind_error is not None and not raise_on_unwind_failure:
            logger.warning(
                "Post-export unwind failure ignored (raise_on_unwind_failure=false). "
                "Next chunk/object can recover via explicit start_transaction."
            )


def _materialize_and_persist_metadata(
    *,
    payload: ExportPayload,
    output_path: Path,
    metadata_path: Path,
    object_config: dict[str, Any],
    context: dict[str, str],
    required_fields: list[str],
    logger: logging.Logger,
    source_ready_timeout_seconds: float,
    source_ready_poll_seconds: float,
    source_ready_stable_hits: int,
    session: Any | None = None,
    allow_csv_failure: bool = False,
    skip_csv_materialization: bool = False,
) -> int:
    normalization_metadata: dict[str, Any] = {}
    if skip_csv_materialization:
        source_txt_path = _try_resolve_source_txt_path(
            output_path=output_path,
            object_code=payload.object_code,
            object_config=object_config,
            context=context,
            source_ready_timeout_seconds=source_ready_timeout_seconds,
            source_ready_poll_seconds=source_ready_poll_seconds,
            source_ready_stable_hits=source_ready_stable_hits,
            session=session,
            logger=logger,
        )
        normalization_metadata.update(
            _artifact_audit_metadata("source_txt", source_txt_path)
        )
        _persist_export_metadata(
            payload=payload,
            output_path=output_path,
            metadata_path=metadata_path,
            rows_exported=0,
            csv_materialized=False,
            csv_error="csv_materialization_disabled_by_run_marker",
            source_txt_path=source_txt_path,
            normalization_metadata=normalization_metadata,
        )
        return 0

    try:
        rows_exported = _materialize_export_output_csv(
            output_path=output_path,
            object_code=payload.object_code,
            object_config=object_config,
            context=context,
            required_fields=required_fields,
            raw_csv_path=payload.raw_csv_path,
            header_map_path=payload.header_map_path,
            rejects_path=payload.rejects_path,
            logger=logger,
            source_ready_timeout_seconds=source_ready_timeout_seconds,
            source_ready_poll_seconds=source_ready_poll_seconds,
            source_ready_stable_hits=source_ready_stable_hits,
            session=session,
            normalization_metadata=normalization_metadata,
        )
        _persist_export_metadata(
            payload=payload,
            output_path=output_path,
            metadata_path=metadata_path,
            rows_exported=rows_exported,
            csv_materialized=True,
            csv_error="",
            source_txt_path=None,
            normalization_metadata=normalization_metadata,
        )
        return rows_exported
    except Exception as exc:
        if not allow_csv_failure:
            raise
        source_txt_path = _try_resolve_source_txt_path(
            output_path=output_path,
            object_code=payload.object_code,
            object_config=object_config,
            context=context,
            source_ready_timeout_seconds=source_ready_timeout_seconds,
            source_ready_poll_seconds=source_ready_poll_seconds,
            source_ready_stable_hits=source_ready_stable_hits,
            session=session,
            logger=logger,
        )
        normalization_metadata.update(
            _artifact_audit_metadata("source_txt", source_txt_path)
        )
        _persist_export_metadata(
            payload=payload,
            output_path=output_path,
            metadata_path=metadata_path,
            rows_exported=0,
            csv_materialized=False,
            csv_error=str(exc),
            source_txt_path=source_txt_path,
            normalization_metadata=normalization_metadata,
        )
        logger.warning(
            "CSV materialization failed, but TXT extraction was preserved and flow will continue: "
            "object=%s source_txt=%s error=%s",
            payload.object_code,
            source_txt_path,
            exc,
        )
        return 0


def _persist_export_metadata(
    *,
    payload: ExportPayload,
    output_path: Path,
    metadata_path: Path,
    rows_exported: int,
    csv_materialized: bool,
    csv_error: str,
    source_txt_path: Path | None,
    normalization_metadata: dict[str, Any] | None = None,
) -> None:
    details = normalization_metadata or {}
    metadata = {
        "run_id": payload.run_id,
        "object": payload.object_code,
        "sqvi_name": payload.sqvi_name,
        "reference": payload.reference,
        "regional": payload.regional,
        "rows_exported": rows_exported,
        "output_path": str(output_path),
        "csv_materialized": bool(csv_materialized),
        "csv_error": csv_error,
        "source_txt_path": str(source_txt_path) if source_txt_path else "",
        "raw_csv_path": str(details.get("raw_csv_path", "") or ""),
        "canonical_csv_path": str(details.get("canonical_csv_path", "") or ""),
        "rejects_path": str(details.get("rejects_path", "") or ""),
        "source_txt_exists": bool(details.get("source_txt_exists", False)),
        "source_txt_size_bytes": int(details.get("source_txt_size_bytes", 0) or 0),
        "source_txt_sha256": str(details.get("source_txt_sha256", "") or ""),
        "canonical_csv_exists": bool(details.get("canonical_csv_exists", False)),
        "canonical_csv_size_bytes": int(
            details.get("canonical_csv_size_bytes", 0) or 0
        ),
        "canonical_csv_sha256": str(details.get("canonical_csv_sha256", "") or ""),
        "raw_csv_exists": bool(details.get("raw_csv_exists", False)),
        "raw_csv_size_bytes": int(details.get("raw_csv_size_bytes", 0) or 0),
        "raw_csv_sha256": str(details.get("raw_csv_sha256", "") or ""),
        "header_map_exists": bool(details.get("header_map_exists", False)),
        "header_map_size_bytes": int(details.get("header_map_size_bytes", 0) or 0),
        "header_map_sha256": str(details.get("header_map_sha256", "") or ""),
        "rejects_exists": bool(details.get("rejects_exists", False)),
        "rejects_size_bytes": int(details.get("rejects_size_bytes", 0) or 0),
        "rejects_sha256": str(details.get("rejects_sha256", "") or ""),
        "temp_promotion_verified": bool(details.get("temp_promotion_verified", False)),
        "header_line_start": int(details.get("header_line_start", 0) or 0),
        "header_line_span": int(details.get("header_line_span", 0) or 0),
        "header_confidence": float(details.get("header_confidence", 0.0) or 0.0),
        "header_columns_raw": list(details.get("header_columns_raw", []) or []),
        "header_columns_canonical": list(
            details.get("header_columns_canonical", []) or []
        ),
        "duplicate_columns_count": int(details.get("duplicate_columns_count", 0) or 0),
        "required_token_hits": int(details.get("required_token_hits", 0) or 0),
        "raw_column_count": int(details.get("raw_column_count", 0) or 0),
        "canonical_column_count": int(details.get("canonical_column_count", 0) or 0),
        "projected_column_count": int(details.get("projected_column_count", 0) or 0),
        "expected_full_column_count": int(
            details.get("expected_full_column_count", 0) or 0
        ),
        "captured_expected_column_count": int(
            details.get("captured_expected_column_count", 0) or 0
        ),
        "missing_expected_columns": list(
            details.get("missing_expected_columns", []) or []
        ),
        "encoding_used": str(details.get("encoding_used", "") or ""),
        "detected_source_encoding": dict(
            details.get("detected_source_encoding", {}) or {}
        ),
        "header_probe": dict(details.get("header_probe", {}) or {}),
        "delimiter_used": str(details.get("delimiter_used", "") or ""),
        "rows_raw": int(details.get("rows_raw", 0) or 0),
        "rows_normalized": int(details.get("rows_normalized", 0) or 0),
        "rows_rejected": int(details.get("rows_rejected", 0) or 0),
        "normalization_profile_version": str(
            details.get("normalization_profile_version", "")
        ),
        "header_map_path": str(details.get("header_map_path", "") or ""),
        "generated_at_epoch": int(time.time()),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _try_resolve_source_txt_path(
    *,
    output_path: Path,
    object_code: str,
    object_config: dict[str, Any],
    context: dict[str, str],
    source_ready_timeout_seconds: float,
    source_ready_poll_seconds: float,
    source_ready_stable_hits: int,
    session: Any | None,
    logger: logging.Logger | None = None,
) -> Path | None:
    source_candidates = _build_source_candidates(
        output_path=output_path,
        object_config=object_config,
        context=context,
    )
    expected_source_names = _extract_expected_source_names(
        object_code=object_code,
        source_candidates=source_candidates,
        object_config=object_config,
        context=context,
    )
    try:
        source_path = _wait_for_source_path(
            output_path=output_path,
            object_code=object_code,
            source_candidates=source_candidates,
            expected_source_names=expected_source_names,
            timeout_seconds=source_ready_timeout_seconds,
            poll_seconds=source_ready_poll_seconds,
            stable_hits_required=source_ready_stable_hits,
            session=session,
            logger=logger,
        )
    except Exception:  # noqa: BLE001
        return None
    if source_path.suffix.lower() != ".txt":
        return None
    return source_path


def _try_recover_export_after_step_failure(
    *,
    payload: ExportPayload,
    output_path: Path,
    metadata_path: Path,
    object_config: dict[str, Any],
    context: dict[str, str],
    required_fields: list[str],
    logger: logging.Logger,
    global_cfg: dict[str, Any],
    source_ready_timeout_seconds: float,
    source_ready_poll_seconds: float,
    source_ready_stable_hits: int,
    session: Any | None,
    step_error: Exception,
) -> int | None:
    env_override = _safe_env("ENEL_SAP_RECOVER_FROM_TXT_ON_STEP_FAILURE")
    if env_override:
        recover_enabled = parse_bool(env_override)
    else:
        recover_enabled = parse_bool(
            global_cfg.get("recover_from_txt_on_step_failure", True)
        )

    if not recover_enabled:
        logger.error(
            "SAP step failed and recovery is disabled: object=%s error=%s",
            payload.object_code,
            step_error,
        )
        return None

    logger.warning(
        "SAP step failed for object=%s. Attempting recovery from exported file artifact. error=%s",
        payload.object_code,
        step_error,
    )
    try:
        rows_exported = _materialize_and_persist_metadata(
            payload=payload,
            output_path=output_path,
            metadata_path=metadata_path,
            object_config=object_config,
            context=context,
            required_fields=required_fields,
            logger=logger,
            source_ready_timeout_seconds=source_ready_timeout_seconds,
            source_ready_poll_seconds=source_ready_poll_seconds,
            source_ready_stable_hits=source_ready_stable_hits,
            session=session,
            allow_csv_failure=parse_bool(
                _safe_env("ENEL_SAP_CONTINUE_TXT_ON_CSV_ERROR") or "false"
            ),
        )
    except Exception as recovery_error:  # noqa: BLE001
        logger.error(
            "SAP recovery from exported file failed: object=%s original_error=%s recovery_error=%s",
            payload.object_code,
            step_error,
            recovery_error,
        )
        return None

    logger.warning(
        "SAP recovery succeeded after step failure: object=%s rows_exported=%s output=%s",
        payload.object_code,
        rows_exported,
        output_path,
    )
    return rows_exported


def connect_sap_session(config: dict[str, Any], logger: logging.Logger | None = None):
    try:
        import win32com.client  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "pywin32 is required for SAP GUI Scripting. Install with: pip install pywin32"
        ) from exc

    global_cfg = config.get("global", {})
    connection_index = int(global_cfg.get("connection_index", 0))
    session_index = int(global_cfg.get("session_index", 0))
    auto_login = parse_bool(global_cfg.get("auto_login", True))
    env_auto_login = _safe_env("SAP_AUTO_LOGIN")
    if env_auto_login:
        auto_login = parse_bool(env_auto_login)
    connection_name = str(global_cfg.get("connection_name", "")).strip() or _safe_env(
        "SAP_CONNECTION_NAME"
    )
    if not connection_name:
        connection_name = "BAP"
    login_timeout_seconds = float(global_cfg.get("login_timeout_seconds", 45.0))

    sap_gui = win32com.client.GetObject("SAPGUI")
    if sap_gui is None:
        raise RuntimeError("SAP GUI COM object not available. Open SAP Logon first.")

    application = sap_gui.GetScriptingEngine
    if application is None:
        raise RuntimeError(
            "Scripting engine unavailable. Enable SAP GUI Scripting on client/server."
        )

    try:
        connection = application.Children(connection_index)
    except Exception:
        if not auto_login:
            raise RuntimeError(
                f"Could not access SAP connection index {connection_index}. "
                "Open an SAP connection manually and retry."
            )
        if logger is not None:
            logger.info(
                "No active SAP connection at index %s. Opening connection '%s'.",
                connection_index,
                connection_name,
            )
        connection = _open_connection(
            application=application,
            connection_name=connection_name,
            connection_index=connection_index,
            timeout_seconds=login_timeout_seconds,
        )

    try:
        session = connection.Children(session_index)
    except Exception:
        if not auto_login:
            raise RuntimeError(
                f"Could not access SAP session index {session_index}. "
                "Open an SAP session manually and retry."
            )
        session = _wait_for_session(
            connection=connection,
            session_index=session_index,
            timeout_seconds=login_timeout_seconds,
        )

    if auto_login:
        _ensure_logged_in(
            session=session,
            logger=logger,
            timeout_seconds=login_timeout_seconds,
        )

    return session


def _open_connection(
    application,
    connection_name: str,
    connection_index: int,
    timeout_seconds: float,
):
    try:
        opened = application.OpenConnection(connection_name, True)
        if opened is not None:
            return opened
    except Exception:
        pass

    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            return application.Children(connection_index)
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(
        f"Could not open SAP connection '{connection_name}'. "
        "Confirm that this entry exists in SAP Logon and is available."
    )


def _wait_for_session(connection, session_index: int, timeout_seconds: float):
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            return connection.Children(session_index)
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(
        f"Could not access SAP session index {session_index} after opening connection."
    )


def _ensure_logged_in(
    session, logger: logging.Logger | None, timeout_seconds: float
) -> None:
    login_user_id = "wnd[0]/usr/txtRSYST-BNAME"
    login_pass_id = "wnd[0]/usr/pwdRSYST-BCODE"
    if not _exists_any_id(session=session, ids=[login_user_id, login_pass_id]):
        return

    sap_user = _safe_env("SAP_USER")
    sap_password = _safe_env("SAP_PASSWORD")
    sap_client = _safe_env("SAP_CLIENT")
    sap_language = _safe_env("SAP_LANGUAGE") or _safe_env("SAP_LANGU")

    if not sap_user or not sap_password:
        raise RuntimeError(
            "Automatic SAP login requires SAP_USER and SAP_PASSWORD in .env."
        )

    if logger is not None:
        logger.info("SAP login screen detected. Filling credentials from environment.")

    _set_first_available_text(
        session=session,
        ids=["wnd[0]/usr/txtRSYST-BNAME"],
        value=sap_user,
        required=True,
        label="SAP_USER",
    )
    _set_first_available_text(
        session=session,
        ids=["wnd[0]/usr/pwdRSYST-BCODE"],
        value=sap_password,
        required=True,
        label="SAP_PASSWORD",
    )
    _set_first_available_text(
        session=session,
        ids=["wnd[0]/usr/txtRSYST-MANDT"],
        value=sap_client,
        required=False,
        label="SAP_CLIENT",
    )
    _set_first_available_text(
        session=session,
        ids=["wnd[0]/usr/txtRSYST-LANGU", "wnd[0]/usr/txtRSYST-SPRAS"],
        value=sap_language,
        required=False,
        label="SAP_LANGUAGE",
    )

    try:
        session.findById("wnd[0]").sendVKey(0)
    except Exception as exc:
        raise RuntimeError(f"Could not submit SAP login screen: {exc}") from exc
    wait_not_busy(session=session, timeout_seconds=timeout_seconds)


def _set_first_available_text(
    session,
    ids: list[str],
    value: str,
    required: bool,
    label: str,
) -> None:
    if not value and not required:
        return
    for item_id in ids:
        try:
            control = session.findById(item_id)
            control.text = value
            return
        except Exception:
            continue
    if required:
        raise RuntimeError(f"Could not fill required login field {label}.")


def resolve_object_config(
    config: dict[str, Any], payload: ExportPayload
) -> dict[str, Any]:
    objects_map = config.get("objects", {})
    object_cfg = objects_map.get(payload.object_code)
    if object_cfg:
        return object_cfg

    sqvi_map = config.get("sqvi", {})
    sqvi_cfg = sqvi_map.get(payload.sqvi_name)
    if sqvi_cfg:
        return sqvi_cfg

    default_cfg = objects_map.get("default")
    if default_cfg:
        return default_cfg

    raise RuntimeError(
        "No object config found. Add mapping in SAP GUI export config for "
        f"object={payload.object_code} or sqvi={payload.sqvi_name}."
    )


def build_context(payload: ExportPayload, output_path: Path) -> dict[str, str]:
    lot_values = expand_lot_ranges(payload.lot_ranges)
    lot_first, lot_last = resolve_lot_bounds(payload.lot_ranges)
    period_start, period_end = _resolve_reference_period_window(
        payload.reference,
        payload_period_start=payload.period_start,
        payload_period_end=payload.period_end,
    )
    reference_ym_slash = payload.reference
    if len(payload.reference) == 6 and payload.reference.isdigit():
        reference_ym_slash = f"{payload.reference[:4]}/{payload.reference[4:]}"
    fat_tipo_fatura = (
        _extract_filter_value(
            filters=payload.filters,
            needles=("tipofatura", "tipo de fatura"),
        )
        or "FATURAMENTO"
    )
    fat_zone_temp_exclude = (
        _extract_filter_value(
            filters=payload.filters,
            needles=("zona de temperatura",),
        )
        or "Consumo Proprio - Sub-Estacao"
    )
    lot_period_token = f"{lot_first:02d}a{lot_last:02d}"
    day_period_token = f"{period_start.day:02d}a{period_end.day:02d}"
    fat_export_macro_filename = f"ZFAT{lot_period_token}.txt"
    cli_export_filename = f"BASECLIENTES{lot_period_token}.txt"
    pda_export_filename = f"LEITURAS{day_period_token}.txt"
    note_export_filename = f"BASEOAR{day_period_token}.txt"
    ca_export_filename = f"BASE_AUTOMACAO_CA_{payload.run_id}.txt"
    rl_export_filename = f"BASE_AUTOMACAO_RL_{payload.run_id}.txt"
    wb_export_filename = f"BASE_AUTOMACAO_WB_{payload.run_id}.txt"
    return {
        "run_id": payload.run_id,
        "object": payload.object_code,
        "sqvi_name": payload.sqvi_name,
        "reference": payload.reference,
        "reference_ym_slash": reference_ym_slash,
        "regional": payload.regional,
        "lot_values": ",".join(lot_values),
        "lot_ranges": serialize_lot_ranges(payload.lot_ranges),
        "lot_first": str(lot_first),
        "lot_last": str(lot_last),
        "lot_first_2d": f"{lot_first:02d}",
        "lot_last_2d": f"{lot_last:02d}",
        "period_start_dmy": period_start.strftime("%d.%m.%Y"),
        "period_end_dmy": period_end.strftime("%d.%m.%Y"),
        "period_start_ymd": period_start.strftime("%Y-%m-%d"),
        "period_end_ymd": period_end.strftime("%Y-%m-%d"),
        "fat_tipo_fatura": fat_tipo_fatura,
        "fat_zone_temp_exclude": fat_zone_temp_exclude,
        "fat_export_macro_filename": fat_export_macro_filename,
        "cli_export_filename": cli_export_filename,
        "pda_export_filename": pda_export_filename,
        "note_export_filename": note_export_filename,
        "ca_export_filename": ca_export_filename,
        "rl_export_filename": rl_export_filename,
        "wb_export_filename": wb_export_filename,
        "output_path": str(output_path),
        "output_path_win": str(output_path),
        "output_dir": str(output_path.parent),
        "output_dir_win": str(output_path.parent),
        "raw_dir": str(output_path.parent),
        "raw_dir_win": str(output_path.parent),
        "output_file_name": output_path.name,
    }


def run_steps(
    session,
    steps: list[dict[str, Any]],
    context: dict[str, str],
    default_timeout_seconds: float,
    logger: logging.Logger,
) -> None:
    if not steps:
        raise RuntimeError("No steps configured in SAP GUI export config.")

    soft_fail_variant_steps = parse_bool(
        _safe_env("ENEL_SAP_SOFT_FAIL_VARIANT_STEPS") or "false"
    )
    total_steps = len(steps)
    for index, step in enumerate(steps, start=1):
        action = str(step.get("action", "")).strip().lower()
        if not action:
            raise RuntimeError(f"Step {index} is missing 'action'.")
        started_at = time.perf_counter()

        logger.info(
            "[STEP %s/%s] action=%s details=%s",
            index,
            total_steps,
            action,
            _describe_step(step=step, context=context),
        )
        try:
            run_step(
                session=session,
                step=step,
                action=action,
                context=context,
                default_timeout_seconds=default_timeout_seconds,
                logger=logger,
            )
            elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            logger.info(
                "[STEP %s/%s] done action=%s elapsed_ms=%.2f",
                index,
                total_steps,
                action,
                elapsed_ms,
            )
        except Exception as exc:
            extra = ""
            message = str(exc)
            runtime_error = _extract_runtime_error_details(session=session)
            if runtime_error:
                extra = f" SAP runtime error details: {runtime_error}"
            if "Could not resolve SAP GUI element" in message:
                visible_ids = collect_visible_control_ids(session=session, limit=80)
                if visible_ids:
                    preview = " | ".join(visible_ids[:20])
                    extra += f" Available controls sample: {preview}"
            elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            if soft_fail_variant_steps and _is_variant_layout_step(step):
                logger.warning(
                    "[STEP %s/%s] soft-fail action=%s elapsed_ms=%.2f reason=%s",
                    index,
                    total_steps,
                    action,
                    elapsed_ms,
                    f"{exc}{extra}",
                )
                continue
            raise RuntimeError(
                f"Step {index} failed ({action}) elapsed_ms={elapsed_ms:.2f}: {exc}{extra}"
            ) from exc


def _is_variant_layout_step(step: dict[str, Any]) -> bool:
    action = str(step.get("action", "")).strip().lower()
    if not action:
        return False
    if action == "select_variant_rows":
        return True

    ids: list[str] = []
    raw_id = step.get("id")
    if isinstance(raw_id, str):
        ids.append(raw_id)
    raw_ids = step.get("ids")
    if isinstance(raw_ids, list):
        ids.extend(str(item) for item in raw_ids if isinstance(item, str))

    ids_normalized = [item.upper() for item in ids if item.strip()]
    if any("SAPLSKBH:0620" in item for item in ids_normalized):
        return True
    if any("BTNAPP_FL_SING" in item for item in ids_normalized):
        return True

    if action == "call_method":
        method = str(step.get("method", "")).strip().lower()
        if method in {"presstoolbarcontextbutton", "selectcontextmenuitem"}:
            for arg in step.get("args", []):
                if not isinstance(arg, dict):
                    continue
                token = str(arg.get("value", "")).strip().upper()
                if token in {"&MB_VARIANT", "&COL0"}:
                    return True
    return False


def run_step(
    session,
    step: dict[str, Any],
    action: str,
    context: dict[str, str],
    default_timeout_seconds: float,
    logger: logging.Logger,
) -> None:
    optional = parse_bool(step.get("optional", False))
    action_started_at = time.perf_counter()

    def trace(
        *,
        operation: str,
        status: str = "ok",
        item: Any | None = None,
        ids: list[str] | None = None,
        value: Any | None = None,
        note: str = "",
    ) -> None:
        _log_step_trace(
            logger=logger,
            action=action,
            operation=operation,
            status=status,
            item=item,
            ids=ids,
            value=value,
            optional=optional,
            started_at=action_started_at,
            note=note,
        )

    if action == "start_transaction":
        transaction = render_template(str(step.get("value", "")), context)
        if not transaction:
            raise RuntimeError("start_transaction requires a non-empty value.")
        session.StartTransaction(transaction)
        wait_not_busy(session, timeout_seconds=default_timeout_seconds)
        trace(operation="start_transaction", value=transaction)
        return

    if action == "wait_not_busy":
        timeout_seconds = float(step.get("timeout_seconds", default_timeout_seconds))
        wait_not_busy(session, timeout_seconds=timeout_seconds)
        trace(operation="wait_not_busy", value=timeout_seconds)
        return

    if action == "sleep":
        seconds = float(step.get("seconds", step.get("value", 0.5)))
        logger.info("Sleeping for %.3f seconds", seconds)
        time.sleep(seconds)
        trace(operation="sleep", value=seconds)
        return

    if action == "send_vkey":
        value = int(step.get("value", 0))
        ids = _resolve_step_ids(step=step, context=context, default_id="wnd[0]")
        target = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            default_id="wnd[0]",
            optional=optional,
        )
        if target is None:
            trace(operation="send_vkey", status="skipped", ids=ids, value=value)
            return
        target.sendVKey(value)
        trace(operation="send_vkey", item=target, ids=ids, value=value)
        return

    if action == "set_okcode":
        value = render_step_value(step, context)
        ids = _resolve_step_ids(
            step=step,
            context=context,
            default_id="wnd[0]/tbar[0]/okcd",
        )
        item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            default_id="wnd[0]/tbar[0]/okcd",
            optional=optional,
        )
        if item is None:
            trace(operation="set_okcode", status="skipped", ids=ids, value=value)
            return
        item.text = value
        if parse_bool(step.get("submit", True)):
            wnd = _resolve_item(
                session=session,
                step={"id": "wnd[0]"},
                context=context,
                action="send_vkey",
                optional=optional,
            )
            if wnd is not None:
                wnd.sendVKey(0)
                trace(
                    operation="set_okcode_submit",
                    item=wnd,
                    ids=["wnd[0]"],
                    value=0,
                )
        trace(operation="set_okcode", item=item, ids=ids, value=value)
        return

    if action == "invoke_export_dialog":
        menu_ids = _render_template_list(
            step.get("menu_ids"),
            context=context,
            default=[
                "wnd[0]/mbar/menu[0]/menu[11]/menu[3]",
                "wnd[0]/mbar/menu[0]/menu[10]/menu[3]",
            ],
        )
        for menu_id in menu_ids:
            try:
                item = session.findById(menu_id)
                item.press()
                trace(
                    operation="invoke_export_dialog_menu",
                    item=item,
                    ids=[menu_id],
                )
                return
            except Exception:  # noqa: BLE001
                continue

        okcodes = _render_template_list(
            step.get("okcodes"),
            context=context,
            default=["&XXL", "&PC"],
        )
        okcode_field = _resolve_item(
            session=session,
            step={"id": "wnd[0]/tbar[0]/okcd"},
            context=context,
            action=action,
            optional=optional,
        )
        wnd = _resolve_item(
            session=session,
            step={"id": "wnd[0]"},
            context=context,
            action="send_vkey",
            optional=optional,
        )
        if okcode_field is None or wnd is None:
            trace(
                operation="invoke_export_dialog",
                status="skipped",
                ids=menu_ids,
                note="okcode field/window not available",
            )
            return

        for code in okcodes:
            try:
                okcode_field.text = code
                wnd.sendVKey(0)
                trace(
                    operation="invoke_export_dialog_okcode",
                    item=okcode_field,
                    ids=["wnd[0]/tbar[0]/okcd"],
                    value=code,
                )
                return
            except Exception:  # noqa: BLE001
                continue

        if optional:
            trace(
                operation="invoke_export_dialog",
                status="skipped",
                ids=menu_ids,
                note="all menu/okcode attempts failed but optional=true",
            )
            return
        raise RuntimeError("Could not trigger export dialog via menu or OKCODE.")

    if action == "confirm_export_format_dialog":
        path_ids = _render_template_list(
            step.get("path_ids"),
            context=context,
            default=[
                "wnd[1]/usr/ctxtDY_PATH",
                "wnd[2]/usr/ctxtDY_PATH",
                "wnd[1]/usr/txtDY_PATH",
                "wnd[2]/usr/txtDY_PATH",
            ],
        )
        if _exists_any_id(session=session, ids=path_ids):
            trace(
                operation="confirm_export_format_dialog",
                status="skipped",
                ids=path_ids,
                note="path field already available",
            )
            return

        button_ids = _render_template_list(
            step.get("ids"),
            context=context,
            default=[
                "wnd[1]/tbar[0]/btn[0]",
                "wnd[1]/tbar[0]/btn[11]",
            ],
        )
        for button_id in button_ids:
            try:
                item = session.findById(button_id)
                item.press()
                trace(
                    operation="confirm_export_format_dialog_press",
                    item=item,
                    ids=[button_id],
                )
                return
            except Exception:  # noqa: BLE001
                continue

        if optional:
            trace(
                operation="confirm_export_format_dialog",
                status="skipped",
                ids=button_ids,
                note="optional=true and no confirm button found",
            )
            return
        raise RuntimeError("Could not confirm export format dialog.")

    if action == "scroll_to_label":
        label = render_step_value(step, context)
        if not label:
            raise RuntimeError("scroll_to_label requires a non-empty value.")
        scope_id = render_template(str(step.get("scope_id", "wnd[0]/usr")), context)
        max_scrolls = int(step.get("max_scrolls", 30))
        step_size = int(step.get("step_size", 8))
        found = _scroll_scope_until_label(
            session=session,
            scope_id=scope_id,
            label_contains=label,
            max_scrolls=max_scrolls,
            step_size=step_size,
        )
        if not found:
            if optional:
                trace(
                    operation="scroll_to_label",
                    status="skipped",
                    ids=[scope_id],
                    value=label,
                    note="label not found but optional=true",
                )
                return
            raise RuntimeError(f"Could not locate label '{label}' after scrolling.")
        trace(
            operation="scroll_to_label",
            ids=[scope_id],
            value=label,
            note=f"max_scrolls={max_scrolls} step_size={step_size}",
        )
        return

    if action == "set_text_by_label":
        label = str(step.get("label_contains", "")).strip()
        if not label:
            raise RuntimeError("set_text_by_label requires 'label_contains'.")
        value = render_step_value(step, context)
        scope_id = render_template(str(step.get("scope_id", "wnd[0]/usr")), context)
        side = str(step.get("side", "low")).strip().lower()
        if parse_bool(step.get("skip_if_empty", False)) and not value.strip():
            trace(
                operation="set_text_by_label",
                status="skipped",
                ids=[scope_id],
                value=value,
                note="skip_if_empty=true",
            )
            return
        inputs = _find_inputs_by_label(
            session=session,
            scope_id=scope_id,
            label_contains=label,
        )
        if not inputs:
            if optional:
                trace(
                    operation="set_text_by_label",
                    status="skipped",
                    ids=[scope_id],
                    value=value,
                    note=f"label='{label}' not found and optional=true",
                )
                return
            raise RuntimeError(f"Could not resolve input by label '{label}'.")
        order = _preferred_input_order(side=side, size=len(inputs))
        errors: list[str] = []
        for idx in order:
            if idx < 0 or idx >= len(inputs):
                continue
            target = inputs[idx]["node"]
            target_id = str(inputs[idx]["id"]).strip()
            try:
                _set_control_text_like(target=target, value=value)
                trace(
                    operation="set_text_by_label",
                    item=target,
                    ids=[target_id] if target_id else [scope_id],
                    value=value,
                    note=f"label='{label}' side={side}",
                )
                return
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{target_id}: {exc}")
                continue
        if optional:
            trace(
                operation="set_text_by_label",
                status="skipped",
                ids=[scope_id],
                value=value,
                note=f"label='{label}' set failed on all candidates and optional=true",
            )
            return
        raise RuntimeError(
            f"Could not set value for label '{label}'. " + " | ".join(errors)
        )

    if action == "press_multisel_by_label":
        label = str(step.get("label_contains", "")).strip()
        if not label:
            raise RuntimeError("press_multisel_by_label requires 'label_contains'.")
        scope_id = render_template(str(step.get("scope_id", "wnd[0]/usr")), context)
        button = _find_multisel_button_by_label(
            session=session,
            scope_id=scope_id,
            label_contains=label,
        )
        if button is None and parse_bool(step.get("auto_scroll", True)):
            _scroll_scope_until_label(
                session=session,
                scope_id=scope_id,
                label_contains=label,
                max_scrolls=int(step.get("max_scrolls", 45)),
                step_size=int(step.get("step_size", 8)),
            )
            button = _find_multisel_button_by_label(
                session=session,
                scope_id=scope_id,
                label_contains=label,
            )
        if button is None and _match_label(label, "texto para zona de temperatura"):
            button = _resolve_known_multisel_button(
                session=session,
                ids=[
                    "wnd[0]/usr/btn%SP$00040%APP%-VALU_PUSH",
                    "wnd[0]/usr/btn%SP$00039%APP%-VALU_PUSH",
                    "wnd[0]/usr/btn%SP$00041%APP%-VALU_PUSH",
                ],
            )
        if button is None:
            if optional:
                trace(
                    operation="press_multisel_by_label",
                    status="skipped",
                    ids=[scope_id],
                    value=label,
                    note="multisel button not found and optional=true",
                )
                return
            raise RuntimeError(
                f"Could not resolve multisel button for label '{label}'."
            )
        button.press()
        target_id = _get_control_id(button)
        trace(
            operation="press_multisel_by_label",
            item=button,
            ids=[target_id] if target_id else [scope_id],
            value=label,
        )
        return

    if action == "set_first_text_field":
        value = render_step_value(step, context)
        default_scope = render_template(
            str(step.get("scope_id", "wnd[1]/usr")), context
        )
        if parse_bool(step.get("skip_if_empty", False)) and not value.strip():
            trace(
                operation="set_first_text_field",
                status="skipped",
                ids=[default_scope],
                value=value,
                note="skip_if_empty=true",
            )
            return
        scope_ids = _render_template_list(
            step.get("scope_ids"),
            context=context,
            default=[default_scope],
        )
        for scope_id in scope_ids:
            field = _find_first_text_field(session=session, scope_id=scope_id)
            if field is None:
                continue
            field.text = value
            target_id = _get_control_id(field)
            trace(
                operation="set_first_text_field",
                item=field,
                ids=[target_id] if target_id else [scope_id],
                value=value,
            )
            return
        if optional:
            trace(
                operation="set_first_text_field",
                status="skipped",
                ids=scope_ids,
                value=value,
                note="no text field found and optional=true",
            )
            return
        raise RuntimeError(
            "Could not find first text field under any scope: " + ", ".join(scope_ids)
        )

    if action == "select_tab_by_text":
        tab_text = render_step_value(step, context)
        if not tab_text:
            raise RuntimeError("select_tab_by_text requires non-empty value.")
        default_scope = render_template(
            str(step.get("scope_id", "wnd[1]/usr")), context
        )
        scope_ids = _render_template_list(
            step.get("scope_ids"),
            context=context,
            default=[default_scope],
        )
        for scope_id in scope_ids:
            tab = _find_tab_by_text(
                session=session, scope_id=scope_id, tab_text=tab_text
            )
            if tab is None:
                continue
            tab.select()
            target_id = _get_control_id(tab)
            trace(
                operation="select_tab_by_text",
                item=tab,
                ids=[target_id] if target_id else [scope_id],
                value=tab_text,
            )
            return
        if optional:
            trace(
                operation="select_tab_by_text",
                status="skipped",
                ids=scope_ids,
                value=tab_text,
                note="tab not found and optional=true",
            )
            return
        raise RuntimeError(
            f"Could not find tab with text '{tab_text}' in scopes: "
            + ", ".join(scope_ids)
        )

    if action == "select_radio_by_text":
        radio_text = render_step_value(step, context)
        if not radio_text:
            raise RuntimeError("select_radio_by_text requires non-empty value.")
        scope_ids = _render_template_list(
            step.get("scope_ids"),
            context=context,
            default=["wnd[1]/usr", "wnd[2]/usr", "wnd[1]", "wnd[2]"],
        )
        for scope_id in scope_ids:
            radio = _find_radio_by_text(
                session=session,
                scope_id=scope_id,
                radio_text=radio_text,
            )
            if radio is None:
                continue
            radio.selected = True
            target_id = _get_control_id(radio)
            trace(
                operation="select_radio_by_text",
                item=radio,
                ids=[target_id] if target_id else [scope_id],
                value=radio_text,
            )
            return
        if optional:
            trace(
                operation="select_radio_by_text",
                status="skipped",
                ids=scope_ids,
                value=radio_text,
                note="radio not found and optional=true",
            )
            return
        raise RuntimeError(f"Could not find radio button with text '{radio_text}'.")

    if action == "maximize":
        ids = _resolve_step_ids(step=step, context=context, default_id="wnd[0]")
        item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            default_id="wnd[0]",
            optional=optional,
        )
        if item is None:
            trace(operation="maximize", status="skipped", ids=ids)
            return
        item.maximize()
        trace(operation="maximize", item=item, ids=ids)
        return

    if action == "close":
        ids = _resolve_step_ids(step=step, context=context, default_id="wnd[0]")
        item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            default_id="wnd[0]",
            optional=optional,
        )
        if item is None:
            trace(operation="close", status="skipped", ids=ids)
            return
        item.close()
        trace(operation="close", item=item, ids=ids)
        return

    if action == "set_property":
        property_name = str(step.get("property", "")).strip()
        if not property_name:
            raise RuntimeError("set_property requires non-empty 'property'.")
        ids = _resolve_step_ids(step=step, context=context)
        item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            optional=optional,
        )
        if item is None:
            trace(
                operation="set_property",
                status="skipped",
                ids=ids,
                note=f"property={property_name}",
            )
            return
        value = _resolve_typed_step_value(step=step, context=context)
        setattr(item, property_name, value)
        trace(
            operation="set_property",
            item=item,
            ids=ids,
            value=value,
            note=f"property={property_name}",
        )
        return

    if action == "select_variant_rows":
        grid_ids = _resolve_step_ids(step=step, context=context)
        if not grid_ids:
            raise RuntimeError("select_variant_rows requires 'id' or 'ids'.")
        grid_item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            optional=optional,
        )
        if grid_item is None:
            trace(operation="select_variant_rows", status="skipped", ids=grid_ids)
            return
        apply_button_id = render_template(str(step.get("apply_button_id", "")), context)
        if not apply_button_id:
            raise RuntimeError("select_variant_rows requires 'apply_button_id'.")
        apply_button = session.findById(apply_button_id)
        selected_rows = step.get("rows", [])
        if not isinstance(selected_rows, list) or not selected_rows:
            raise RuntimeError("select_variant_rows requires a non-empty 'rows' list.")
        first_visible_row = step.get("first_visible_row")
        if first_visible_row is not None:
            grid_item.firstVisibleRow = int(first_visible_row)
        current_cell_row = step.get("current_cell_row")
        if current_cell_row is not None:
            grid_item.currentCellRow = int(current_cell_row)
        for raw_row in selected_rows:
            row_value = int(raw_row)
            grid_item.selectedRows = str(row_value)
            apply_button.press()
        trace(
            operation="select_variant_rows",
            item=grid_item,
            ids=grid_ids + [apply_button_id],
            value=selected_rows,
        )
        return

    if action == "call_method":
        method_name = str(step.get("method", "")).strip()
        if not method_name:
            raise RuntimeError("call_method requires non-empty 'method'.")
        ids = _resolve_step_ids(step=step, context=context)
        item = _resolve_item(
            session=session,
            step=step,
            context=context,
            action=action,
            optional=optional,
        )
        if item is None:
            trace(
                operation="call_method",
                status="skipped",
                ids=ids,
                note=f"method={method_name}",
            )
            return
        raw_args = step.get("args", [])
        args = _resolve_call_args(raw_args=raw_args, context=context)
        method = getattr(item, method_name)
        method(*args)
        trace(
            operation="call_method",
            item=item,
            ids=ids,
            value=args,
            note=f"method={method_name}",
        )
        return

    ids = _resolve_step_ids(step=step, context=context)
    item = _resolve_item(
        session=session,
        step=step,
        context=context,
        action=action,
        optional=optional,
    )
    if item is None:
        trace(operation=action, status="skipped", ids=ids)
        return

    if action == "press":
        item.press()
        trace(operation="press", item=item, ids=ids)
        return
    if action == "select":
        item.select()
        trace(operation="select", item=item, ids=ids)
        return
    if action == "set_focus":
        item.setFocus()
        trace(operation="set_focus", item=item, ids=ids)
        return
    if action == "set_text":
        value = render_step_value(step, context)
        if parse_bool(step.get("skip_if_empty", False)) and not value.strip():
            trace(
                operation="set_text",
                status="skipped",
                item=item,
                ids=ids,
                value=value,
                note="skip_if_empty=true",
            )
            return
        item.text = value
        trace(operation="set_text", item=item, ids=ids, value=value)
        return
    if action == "set_key":
        value = render_step_value(step, context)
        if parse_bool(step.get("skip_if_empty", False)) and not value.strip():
            trace(
                operation="set_key",
                status="skipped",
                item=item,
                ids=ids,
                value=value,
                note="skip_if_empty=true",
            )
            return
        item.key = value
        trace(operation="set_key", item=item, ids=ids, value=value)
        return
    if action == "set_checkbox":
        value = parse_bool(step.get("value", True))
        item.selected = value
        trace(operation="set_checkbox", item=item, ids=ids, value=value)
        return
    if action == "set_caret":
        caret = int(step.get("value", 0))
        item.caretPosition = caret
        trace(operation="set_caret", item=item, ids=ids, value=caret)
        return

    raise RuntimeError(f"Unsupported action: {action}")


def _resolve_item(
    session,
    step: dict[str, Any],
    context: dict[str, str],
    action: str,
    default_id: str | None = None,
    optional: bool = False,
):
    ids = _resolve_step_ids(step=step, context=context, default_id=default_id)
    if not ids:
        if optional:
            return None
        raise RuntimeError(f"Action '{action}' requires 'id' or 'ids'.")

    errors: list[str] = []
    for item_id in ids:
        try:
            return session.findById(item_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{item_id}: {exc}")
            continue

    if optional:
        return None

    raise RuntimeError(
        f"Could not resolve SAP GUI element for action '{action}'. "
        + " | ".join(errors)
    )


def _resolve_step_ids(
    step: dict[str, Any],
    context: dict[str, str],
    default_id: str | None = None,
) -> list[str]:
    ids: list[str] = []
    raw_ids = step.get("ids")
    if isinstance(raw_ids, list):
        for raw in raw_ids:
            value = render_template(str(raw), context).strip()
            if value:
                ids.append(value)

    raw_id = str(step.get("id", "")).strip()
    if raw_id:
        rendered = render_template(raw_id, context).strip()
        if rendered:
            ids.append(rendered)

    if not ids and default_id:
        rendered_default = render_template(default_id, context).strip()
        if rendered_default:
            ids.append(rendered_default)

    # Preserve order while removing duplicates.
    deduped: list[str] = []
    for item in ids:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _log_step_trace(
    *,
    logger: logging.Logger,
    action: str,
    operation: str,
    status: str,
    item: Any | None,
    ids: list[str] | None,
    value: Any | None,
    optional: bool,
    started_at: float,
    note: str = "",
) -> None:
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    target_id = _get_control_id(item) if item is not None else ""
    target_type = _safe_get(item, "Type") if item is not None else ""
    target_text = _shorten(_safe_get(item, "Text"), 80) if item is not None else ""
    target_tooltip = (
        _shorten(_safe_get(item, "Tooltip"), 80) if item is not None else ""
    )
    ids_text = ",".join(ids[:6]) if ids else ""
    value_text = _shorten(str(value), 120) if value is not None else ""
    note_text = _shorten(str(note), 180) if note else ""

    logger.info(
        "[STEP TRACE] action=%s op=%s status=%s elapsed_ms=%.2f optional=%s "
        "ids=%s target_id=%s target_type=%s target_text=%s target_tooltip=%s "
        "value=%s note=%s",
        action,
        operation,
        status,
        elapsed_ms,
        optional,
        ids_text,
        target_id,
        target_type,
        target_text,
        target_tooltip,
        value_text,
        note_text,
    )


def _render_template_list(
    raw: Any,
    context: dict[str, str],
    default: list[str] | None = None,
) -> list[str]:
    values: list[str] = []
    if isinstance(raw, list):
        source = raw
    elif raw is None:
        source = default or []
    else:
        source = [raw]

    for entry in source:
        rendered = render_template(str(entry), context).strip()
        if rendered and rendered not in values:
            values.append(rendered)
    return values


def _exists_any_id(session, ids: list[str]) -> bool:
    for item_id in ids:
        try:
            session.findById(item_id)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def resolve_lot_bounds(lot_ranges: list[list[int]]) -> tuple[int, int]:
    values = [int(v) for v in expand_lot_ranges(lot_ranges) if str(v).strip()]
    if not values:
        return (1, 2)
    return (min(values), max(values))


def _resolve_reference_period_window(
    reference: str,
    *,
    payload_period_start: str | None = None,
    payload_period_end: str | None = None,
) -> tuple[date, date]:
    override_start = _parse_iso_date(payload_period_start)
    override_end = _parse_iso_date(payload_period_end)
    if override_start and override_end:
        start, end = (
            (override_start, override_end)
            if override_start <= override_end
            else (override_end, override_start)
        )
        return start, end

    if len(reference) != 6 or not reference.isdigit():
        today = date.today()
        return today, today

    year = int(reference[:4])
    month = int(reference[4:])
    period_start = date(year, month, 1)
    if month == 12:
        period_last_day = date(year, 12, 31)
    else:
        period_last_day = date(year, month + 1, 1) - timedelta(days=1)

    day_offset = int(_safe_env("ENEL_SAP_RJ_DAY_OFFSET") or "-1")
    today_override = _safe_env("ENEL_SAP_RJ_CALENDAR_TODAY")
    if today_override:
        try:
            base_today = datetime.strptime(today_override, "%Y-%m-%d").date()
        except ValueError:
            base_today = date.today()
    else:
        base_today = date.today()

    target_day = base_today + timedelta(days=day_offset)
    period_end = min(target_day, period_last_day)
    if period_end < period_start:
        period_end = period_start
    return period_start, period_end


def _parse_iso_date(raw: str | None) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _reset_session_to_transaction(
    *,
    session: Any,
    transaction_code: str,
    enabled: bool,
    timeout_seconds: float,
    logger: logging.Logger | None = None,
    attempts: int = 5,
    raise_on_failure: bool = True,
) -> bool:
    if not enabled:
        return False
    tx = transaction_code.strip().upper()
    if not tx:
        return False
    errors: list[str] = []
    tries = max(1, attempts)
    for attempt in range(1, tries + 1):
        try:
            _dismiss_modal_windows(session=session, logger=logger)
            try:
                okcd = session.findById("wnd[0]/tbar[0]/okcd")
                okcd.text = tx
                session.findById("wnd[0]").sendVKey(0)
            except Exception:
                session.StartTransaction(tx)
            wait_not_busy(session=session, timeout_seconds=timeout_seconds)
            if logger is not None:
                logger.info(
                    "Session reset to transaction: %s (attempt %s/%s)",
                    tx,
                    attempt,
                    tries,
                )
            return True
        except Exception as exc:  # noqa: BLE001
            errors.append(f"attempt={attempt}: {exc}")
            time.sleep(0.25)

    message = (
        f"Could not reset SAP session to transaction {tx} after {tries} attempts. "
        + " | ".join(errors)
    )
    if raise_on_failure:
        raise RuntimeError(message)
    if logger is not None:
        logger.warning(message)
    return False


def _unwind_after_export_with_back(
    *,
    session: Any,
    transaction_code: str,
    timeout_seconds: float,
    logger: logging.Logger | None = None,
    min_back_presses: int = 4,
    max_back_presses: int = 5,
) -> None:
    """
    After each export, navigate back with F3 repeatedly until the back button
    is no longer available/enabled, and recover the transaction screen.
    """
    tx = transaction_code.strip().upper() or "ZISU0128"
    pressed = 0
    min_presses = max(0, int(min_back_presses))
    max_presses = max(min_presses, int(max_back_presses))
    reached_limit = True
    for _ in range(max_presses):
        _dismiss_modal_windows(session=session, logger=logger)
        back_btn = _find_back_button(session=session)
        pressed_this_round = False
        press_mode = ""
        if back_btn is not None:
            try:
                back_btn.press()
                pressed_this_round = True
                press_mode = "button"
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"Could not press back button (F3) during post-export unwind: {exc}"
                ) from exc
        else:
            # Fallback: send F3 even when toolbar control is not directly resolvable.
            try:
                session.findById("wnd[0]").sendVKey(3)
                pressed_this_round = True
                press_mode = "vkey"
            except Exception:
                pressed_this_round = False

        if not pressed_this_round:
            if pressed >= min_presses:
                reached_limit = False
                break
            continue

        pressed += 1
        if logger is not None:
            logger.info(
                "Back navigation press applied: index=%s/%s via=%s",
                pressed,
                max_presses,
                press_mode or "unknown",
            )
        wait_not_busy(
            session=session,
            timeout_seconds=max(5.0, min(timeout_seconds, 20.0)),
        )

    if logger is not None:
        logger.info(
            "Back navigation completed before reset: F3_presses=%s min_required=%s max_allowed=%s reached_limit=%s",
            pressed,
            min_presses,
            max_presses,
            reached_limit,
        )

    # Reset to ZISU once, with bounded retries/time, to avoid repeated OKCODE loops.
    reset_timeout = max(10.0, min(timeout_seconds, 30.0))
    if _reset_session_to_transaction(
        session=session,
        transaction_code=tx,
        enabled=True,
        timeout_seconds=reset_timeout,
        logger=logger,
        attempts=3,
        raise_on_failure=False,
    ):
        if logger is not None:
            logger.info(
                "Back navigation unwind finished: F3_presses=%s transaction=%s",
                pressed,
                tx,
            )
        return

    raise RuntimeError(
        f"Could not recover SAP session to transaction {tx} after "
        f"F3_presses={pressed} max_presses={max_presses}."
    )


def _find_back_button(*, session: Any):
    try:
        button = session.findById("wnd[0]/tbar[0]/btn[3]")
    except Exception:
        return None
    if not _safe_get_bool(button, "Enabled", True):
        return None
    if not _safe_get_bool(button, "Visible", True):
        return None
    return button


def _dismiss_modal_windows(
    *,
    session: Any,
    logger: logging.Logger | None = None,
) -> None:
    handled = 0
    for wnd_id in ("wnd[2]", "wnd[1]"):
        try:
            wnd = session.findById(wnd_id)
        except Exception:
            continue

        for btn_id in (
            f"{wnd_id}/tbar[0]/btn[12]",
            f"{wnd_id}/tbar[0]/btn[15]",
            f"{wnd_id}/usr/btnSPOP-OPTION1",
            f"{wnd_id}/usr/btnSPOP-OPTION2",
        ):
            try:
                session.findById(btn_id).press()
                handled += 1
                break
            except Exception:
                continue
        else:
            try:
                wnd.sendVKey(12)
                handled += 1
            except Exception:
                continue

    if handled and logger is not None:
        logger.info("Dismissed %s modal SAP window(s) before reset.", handled)


def _find_scope(session, scope_id: str):
    return session.findById(scope_id)


def _descendants_with_meta(root, limit: int = 2000) -> list[dict[str, Any]]:
    stack = [root]
    rows: list[dict[str, Any]] = []
    while stack and len(rows) < limit:
        node = stack.pop()
        rows.append(
            {
                "node": node,
                "id": _safe_get(node, "Id"),
                "type": _safe_get(node, "Type"),
                "text": _safe_get(node, "Text"),
                "tooltip": _safe_get(node, "Tooltip"),
                "left": _safe_get_num(node, "Left"),
                "top": _safe_get_num(node, "Top"),
                "width": _safe_get_num(node, "Width"),
                "height": _safe_get_num(node, "Height"),
            }
        )
        for child in _get_children(node):
            stack.append(child)
    return rows


def _safe_get(node: Any, attr: str) -> str:
    try:
        return str(getattr(node, attr, "")).strip()
    except Exception:  # noqa: BLE001
        return ""


def _safe_get_num(node: Any, attr: str) -> float:
    try:
        return float(getattr(node, attr, 0))
    except Exception:  # noqa: BLE001
        return 0.0


def _safe_get_bool(node: Any, attr: str, default: bool = False) -> bool:
    try:
        value = getattr(node, attr, default)
    except Exception:  # noqa: BLE001
        return default
    try:
        return bool(value)
    except Exception:  # noqa: BLE001
        return default


def _is_text_input(control_type: str) -> bool:
    text = control_type.lower()
    return any(token in text for token in ("textfield", "ctextfield", "combobox"))


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_folded = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(ascii_folded.lower().strip().split())


def _match_label(text: str, label_contains: str) -> bool:
    return _normalize(label_contains) in _normalize(text)


def _find_inputs_by_label(
    session,
    scope_id: str,
    label_contains: str,
) -> list[dict[str, Any]]:
    scope = _find_scope(session=session, scope_id=scope_id)
    controls = _descendants_with_meta(scope)
    labels = [c for c in controls if _match_label(c["text"], label_contains)]
    if not labels:
        return []

    # Prefer the top-most visible match.
    labels.sort(key=lambda c: (c["top"], c["left"]))
    label = labels[0]
    row_tol = max(6.0, label["height"] + 6.0)

    inputs = [
        c
        for c in controls
        if _is_text_input(c["type"])
        and c["left"] > label["left"]
        and abs(c["top"] - label["top"]) <= row_tol
        and _safe_get_bool(c["node"], "Changeable", True)
    ]
    if not inputs:
        return []
    inputs.sort(key=lambda c: c["left"])
    return inputs


def _find_input_by_label(
    session,
    scope_id: str,
    label_contains: str,
    side: str = "low",
):
    inputs = _find_inputs_by_label(
        session=session,
        scope_id=scope_id,
        label_contains=label_contains,
    )
    if not inputs:
        return None
    order = _preferred_input_order(side=side, size=len(inputs))
    for idx in order:
        if idx < 0 or idx >= len(inputs):
            continue
        return inputs[idx]["node"]
    return None


def _preferred_input_order(side: str, size: int) -> list[int]:
    if size <= 0:
        return []
    if side == "high":
        primary = 1 if size > 1 else 0
    else:
        primary = 0
    order = [primary]
    for idx in range(size):
        if idx not in order:
            order.append(idx)
    return order


def _set_control_text_like(target: Any, value: str) -> None:
    errors: list[str] = []
    for attr in ("text", "key"):
        try:
            setattr(target, attr, value)
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{attr}: {exc}")
            continue
    raise RuntimeError(" ; ".join(errors))


def _find_multisel_button_by_label(session, scope_id: str, label_contains: str):
    scope = _find_scope(session=session, scope_id=scope_id)
    controls = _descendants_with_meta(scope)
    labels = [
        c
        for c in controls
        if _match_label(c["text"], label_contains)
        or _match_label(c["tooltip"], label_contains)
    ]
    if not labels:
        return None
    labels.sort(key=lambda c: (c["top"], c["left"]))
    for label in labels:
        row_tol = max(10.0, label["height"] + 10.0)
        buttons = [
            c
            for c in controls
            if "button" in c["type"].lower()
            and c["left"] > label["left"]
            and abs(c["top"] - label["top"]) <= row_tol
        ]
        if buttons:
            buttons.sort(key=lambda c: c["left"], reverse=True)
            return buttons[0]["node"]

    # Fallback: try SAP multi-select buttons by ID signature.
    by_id = [
        c
        for c in controls
        if "button" in c["type"].lower() and "%app%-valu_push" in c["id"].lower()
    ]
    if by_id:
        by_id.sort(key=lambda c: (c["top"], c["left"]))
        return by_id[-1]["node"]
    return None


def _scroll_scope_until_label(
    session,
    scope_id: str,
    label_contains: str,
    max_scrolls: int,
    step_size: int,
) -> bool:
    attempts = max(1, max_scrolls)
    step = max(1, step_size)
    for _ in range(attempts):
        scope = _find_scope(session=session, scope_id=scope_id)
        controls = _descendants_with_meta(scope)
        if _has_label(controls, label_contains):
            return True

        _focus_scope_for_scroll(scope=scope)

        # 1) Preferred strategy: use wnd[0]/usr verticalScrollbar in 25% jumps.
        scrollbar = _get_vertical_scrollbar(
            session=session, scope_id=scope_id, scope=scope
        )
        if scrollbar is not None:
            if _scrollbar_forward_screen_fraction(
                scrollbar=scrollbar,
                fraction=0.25,
                min_step=1,
            ):
                time.sleep(0.05)
                continue

        # 2) Fallback: send down arrow key to the window.
        if _send_arrow_down(session=session, repeats=step):
            time.sleep(0.05)
            continue

        # 3) Last resort: try any descendant scrollbar.
        if _move_any_vertical_scrollbar(scope=scope, step=step):
            time.sleep(0.05)
            continue

        break

    # Final check after attempts.
    scope = _find_scope(session=session, scope_id=scope_id)
    controls = _descendants_with_meta(scope)
    return _has_label(controls, label_contains)


def _move_any_vertical_scrollbar(scope, step: int) -> bool:
    # Try scope-level vertical scrollbar first (common in SAP Dynpro screens).
    for attr_name in ("VerticalScrollbar", "verticalScrollbar"):
        try:
            scrollbar = getattr(scope, attr_name, None)
        except Exception:  # noqa: BLE001
            scrollbar = None
        if _scrollbar_forward(scrollbar=scrollbar, step=step):
            return True

    # Fallback to any descendant scrollbar control.
    for control in _descendants_with_meta(scope):
        node = control["node"]
        if "scrollbar" not in control["type"].lower():
            continue
        if _scrollbar_forward(scrollbar=node, step=step):
            return True
    return False


def _get_vertical_scrollbar(session, scope_id: str, scope):
    preferred_scope_ids = [scope_id.strip(), "wnd[0]/usr"]
    seen: set[str] = set()
    for candidate in preferred_scope_ids:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            node = session.findById(candidate)
        except Exception:  # noqa: BLE001
            continue
        for attr in ("verticalScrollbar", "VerticalScrollbar"):
            try:
                sb = getattr(node, attr, None)
            except Exception:  # noqa: BLE001
                sb = None
            if sb is not None:
                return sb

    for attr in ("verticalScrollbar", "VerticalScrollbar"):
        try:
            sb = getattr(scope, attr, None)
        except Exception:  # noqa: BLE001
            sb = None
        if sb is not None:
            return sb
    return None


def _focus_scope_for_scroll(scope) -> None:
    try:
        scope.setFocus()
    except Exception:  # noqa: BLE001
        return


def _scrollbar_forward(scrollbar, step: int) -> bool:
    if scrollbar is None:
        return False
    try:
        current = int(getattr(scrollbar, "Position", 0))
        maximum = int(getattr(scrollbar, "Maximum", current))
    except Exception:  # noqa: BLE001
        return False
    if maximum <= current:
        return False
    try:
        setattr(scrollbar, "Position", min(maximum, current + max(1, step)))
        return True
    except Exception:  # noqa: BLE001
        return False


def _scrollbar_forward_screen_fraction(
    scrollbar,
    fraction: float,
    min_step: int,
) -> bool:
    if scrollbar is None:
        return False
    try:
        current = int(getattr(scrollbar, "Position", 0))
        maximum = int(getattr(scrollbar, "Maximum", current))
    except Exception:  # noqa: BLE001
        return False
    if maximum <= current:
        return False

    page_size = 0
    for attr in ("PageSize", "Pagesize", "VisibleRows"):
        try:
            page_size = int(getattr(scrollbar, attr, 0))
        except Exception:  # noqa: BLE001
            page_size = 0
        if page_size > 0:
            break

    if page_size > 0:
        delta = int(round(page_size * max(0.05, fraction)))
    else:
        delta = int(round((maximum - current) * max(0.05, fraction)))
    delta = max(int(min_step), delta)

    target = min(maximum, current + delta)
    if target <= current and current < maximum:
        target = current + 1
    if target <= current:
        return False
    try:
        setattr(scrollbar, "Position", target)
        return True
    except Exception:  # noqa: BLE001
        return False


def _has_label(controls: list[dict[str, Any]], label_contains: str) -> bool:
    return any(
        _match_label(c["text"], label_contains)
        or _match_label(c["tooltip"], label_contains)
        for c in controls
    )


def _send_arrow_down(session, repeats: int = 1) -> bool:
    if repeats <= 0:
        return False
    moved = False
    for _ in range(repeats):
        pressed = False
        for wnd_id in ("wnd[0]", "wnd[1]", "wnd[2]"):
            try:
                # SAP GUI VKey 40 matches "Arrow Down" in common desktop scripting setups.
                session.findById(wnd_id).sendVKey(40)
                pressed = True
                moved = True
                break
            except Exception:  # noqa: BLE001
                continue
        if not pressed:
            break
    return moved


def _resolve_known_multisel_button(session, ids: list[str]):
    for item_id in ids:
        try:
            return session.findById(item_id)
        except Exception:  # noqa: BLE001
            continue
    return None


def _find_first_text_field(session, scope_id: str):
    scope = _find_scope(session=session, scope_id=scope_id)
    controls = _descendants_with_meta(scope)
    inputs = [c for c in controls if _is_text_input(c["type"])]
    if not inputs:
        return None
    inputs.sort(key=lambda c: (c["top"], c["left"]))
    return inputs[0]["node"]


def _find_tab_by_text(session, scope_id: str, tab_text: str):
    scope = _find_scope(session=session, scope_id=scope_id)
    controls = _descendants_with_meta(scope)
    needle = _normalize(tab_text)
    for c in controls:
        if "tab" not in c["type"].lower():
            continue
        if needle and needle in _normalize(c["text"]):
            return c["node"]
    return None


def _find_radio_by_text(session, scope_id: str, radio_text: str):
    try:
        scope = _find_scope(session=session, scope_id=scope_id)
    except Exception:  # noqa: BLE001
        return None
    controls = _descendants_with_meta(scope)
    candidates: list[dict[str, Any]] = []
    for c in controls:
        if "radio" not in c["type"].lower():
            continue
        if _match_label(c["text"], radio_text) or _match_label(
            c["tooltip"], radio_text
        ):
            candidates.append(c)
    if not candidates:
        return None
    candidates.sort(key=lambda c: (c["top"], c["left"]))
    return candidates[0]["node"]


def collect_visible_control_ids(session, limit: int = 80) -> list[str]:
    if limit <= 0:
        return []

    roots: list[Any] = []
    try:
        roots.append(session.findById("wnd[0]"))
    except Exception:  # noqa: BLE001
        roots.append(session)

    stack: list[Any] = list(roots)
    result: list[str] = []
    seen: set[str] = set()

    while stack and len(result) < limit:
        node = stack.pop()
        node_id = _get_control_id(node)
        if node_id and node_id not in seen:
            seen.add(node_id)
            result.append(node_id)
            if len(result) >= limit:
                break

        for child in _get_children(node):
            stack.append(child)

    return result


def _get_control_id(node: Any) -> str:
    try:
        value = getattr(node, "Id", "")
        return str(value).strip()
    except Exception:  # noqa: BLE001
        return ""


def _get_children(node: Any) -> list[Any]:
    try:
        children = getattr(node, "Children", None)
    except Exception:  # noqa: BLE001
        return []

    if children is None:
        return []

    try:
        count = int(children.Count)
    except Exception:  # noqa: BLE001
        count = 0

    result: list[Any] = []
    for index in range(count):
        try:
            result.append(children(index))
        except Exception:  # noqa: BLE001
            continue
    return result


def render_step_value(step: dict[str, Any], context: dict[str, str]) -> str:
    if "value" not in step:
        return ""
    raw = str(step.get("value", ""))
    return render_template(raw, context)


def _resolve_typed_step_value(step: dict[str, Any], context: dict[str, str]) -> Any:
    raw_value = step.get("value", "")
    value_type = str(step.get("value_type", "auto")).strip().lower()
    return _coerce_typed_value(raw=raw_value, context=context, value_type=value_type)


def _resolve_call_args(raw_args: Any, context: dict[str, str]) -> list[Any]:
    if isinstance(raw_args, list):
        source = raw_args
    elif raw_args is None:
        source = []
    else:
        source = [raw_args]

    values: list[Any] = []
    for raw in source:
        if isinstance(raw, dict):
            value_type = str(raw.get("type", "auto")).strip().lower()
            value = raw.get("value", "")
            values.append(
                _coerce_typed_value(raw=value, context=context, value_type=value_type)
            )
            continue
        values.append(_coerce_typed_value(raw=raw, context=context, value_type="auto"))
    return values


def _coerce_typed_value(raw: Any, context: dict[str, str], value_type: str) -> Any:
    normalized_type = _normalize_value_type(value_type)
    if isinstance(raw, str):
        rendered: Any = render_template(raw, context)
    else:
        rendered = raw

    if normalized_type == "auto":
        return rendered
    if normalized_type == "str":
        return str(rendered)
    if normalized_type == "int":
        return int(rendered)
    if normalized_type == "float":
        return float(rendered)
    if normalized_type == "bool":
        return parse_bool(rendered)
    raise RuntimeError(f"Unsupported value_type/type: {value_type}")


def _normalize_value_type(value_type: str) -> str:
    normalized = value_type.strip().lower()
    aliases = {
        "string": "str",
        "integer": "int",
        "number": "float",
        "boolean": "bool",
    }
    return aliases.get(normalized, normalized)


def render_template(template: str, context: dict[str, str]) -> str:
    return template.format(**context)


def wait_not_busy(session, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    next_runtime_error_check = 0.0
    while time.time() < deadline:
        now = time.time()
        if now >= next_runtime_error_check:
            _raise_if_runtime_error_screen(session=session)
            next_runtime_error_check = now + 2.0
        try:
            if not bool(getattr(session, "Busy", False)):
                return
        except Exception:
            return
        time.sleep(0.1)
    raise TimeoutError(f"SAP session remained busy after {timeout_seconds} seconds.")


def wait_for_file(
    path: Path, timeout_seconds: float, session: Any | None = None
) -> None:
    deadline = time.time() + max(0.0, timeout_seconds)
    next_runtime_error_check = 0.0
    while time.time() <= deadline:
        now = time.time()
        if session is not None and now >= next_runtime_error_check:
            _raise_if_runtime_error_screen(session=session)
            next_runtime_error_check = now + 2.0
        if path.exists() and path.is_file():
            return
        time.sleep(0.2)


def _raise_if_runtime_error_screen(session) -> None:
    details = _extract_runtime_error_details(session=session)
    if details:
        raise RuntimeError(
            "SAP runtime error screen detected during extraction. " f"details={details}"
        )


def _extract_runtime_error_details(session) -> str:
    needles = (
        "abap runtime error",
        "application had to terminate",
        "terminate due to an abap runtime error",
        "cancelamento de uma aplicacao sap",
        "erro de runtime abap",
    )
    snippets: list[str] = []

    for wnd_id in ("wnd[0]", "wnd[1]", "wnd[2]"):
        try:
            wnd = session.findById(wnd_id)
        except Exception:  # noqa: BLE001
            continue

        title = _safe_get(wnd, "Text")
        if title:
            snippets.append(title)

        try:
            controls = _descendants_with_meta(wnd, limit=400)
        except Exception:  # noqa: BLE001
            controls = []

        for control in controls:
            text = str(control.get("text", "")).strip()
            if text:
                snippets.append(text)

    if not snippets:
        return ""

    normalized_joined = _normalize(" | ".join(snippets))
    if not any(needle in normalized_joined for needle in needles):
        return ""

    # Keep a compact and readable excerpt for logs/errors.
    compact: list[str] = []
    seen: set[str] = set()
    for item in snippets:
        value = " ".join(item.split())
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        compact.append(value)
        if len(compact) >= 8:
            break
    return " | ".join(compact)


def count_csv_rows(csv_path: Path) -> int:
    decode_errors: list[str] = []
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.reader(handle)
                row_count = -1
                for row_count, _ in enumerate(reader):
                    pass
                return max(0, row_count)
        except UnicodeDecodeError as exc:
            decode_errors.append(f"encoding={encoding}: {exc}")
    raise RuntimeError("Could not decode CSV file. " + " | ".join(decode_errors))


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except (FileNotFoundError, PermissionError, OSError):
        return


def _flush_and_sync(handle: Any) -> None:
    handle.flush()
    try:
        os.fsync(handle.fileno())
    except OSError:
        return


def _compute_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_audit_metadata(prefix: str, path: Path | None) -> dict[str, Any]:
    if path is None:
        return {
            f"{prefix}_path": "",
            f"{prefix}_exists": False,
            f"{prefix}_size_bytes": 0,
            f"{prefix}_sha256": "",
        }
    resolved = path.expanduser().resolve()
    exists = resolved.exists() and resolved.is_file()
    size_bytes = resolved.stat().st_size if exists else 0
    sha256 = _compute_file_sha256(resolved) if exists else ""
    return {
        f"{prefix}_path": str(resolved),
        f"{prefix}_exists": exists,
        f"{prefix}_size_bytes": size_bytes,
        f"{prefix}_sha256": sha256,
    }


def _promote_temp_file(
    *,
    temp_path: Path,
    destination_path: Path,
    artifact_label: str,
    logger: logging.Logger | None = None,
) -> None:
    timeout_seconds = max(
        1.0,
        _safe_float_env(
            "ENEL_SAP_TEMP_PROMOTION_TIMEOUT_SECONDS",
            _DEFAULT_TEMP_PROMOTION_TIMEOUT_SECONDS,
        ),
    )
    poll_seconds = max(
        0.05,
        _safe_float_env(
            "ENEL_SAP_TEMP_PROMOTION_POLL_SECONDS",
            _DEFAULT_TEMP_PROMOTION_POLL_SECONDS,
        ),
    )
    deadline = time.time() + timeout_seconds
    last_error: OSError | None = None

    while time.time() <= deadline:
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.replace(destination_path)
            if not destination_path.exists() or not destination_path.is_file():
                raise RuntimeError(
                    f"promoted {artifact_label} is missing after rename: {destination_path}"
                )
            return
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"temporary {artifact_label} vanished before promotion: {temp_path}"
            ) from exc
        except OSError as exc:
            last_error = exc
            if time.time() > deadline:
                break
            if logger is not None:
                logger.warning(
                    "Temp artifact promotion retry: artifact=%s temp=%s destination=%s error=%s",
                    artifact_label,
                    temp_path,
                    destination_path,
                    exc,
                )
            time.sleep(poll_seconds)

    raise RuntimeError(
        "could not promote temporary artifact to final path: "
        f"artifact={artifact_label} temp={temp_path} destination={destination_path} "
        f"timeout_seconds={timeout_seconds} last_error={last_error}"
    )


def _promote_optional_temp_file(
    *,
    temp_path: Path | None,
    destination_path: Path | None,
    artifact_label: str,
    logger: logging.Logger | None = None,
) -> None:
    if temp_path is None or destination_path is None:
        return
    if not temp_path.exists():
        return
    try:
        _promote_temp_file(
            temp_path=temp_path,
            destination_path=destination_path,
            artifact_label=artifact_label,
            logger=logger,
        )
    except Exception as exc:
        if logger is not None:
            logger.warning(
                "Optional artifact promotion failed; canonical CSV will be preserved: artifact=%s temp=%s destination=%s error=%s",
                artifact_label,
                temp_path,
                destination_path,
                exc,
            )


def _resolve_expected_canonical_required_fields(
    *,
    profile: SapHeaderProfile,
    required_fields: list[str],
) -> list[str]:
    expected: list[str] = []
    seen: set[str] = set()
    for raw_field in required_fields:
        token = _norm_header_token(raw_field)
        canonical = profile.known_aliases.get(token) or _sanitize_canonical_name(
            raw_field
        )
        normalized = _norm_header_token(canonical)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        expected.append(canonical)
    return expected


def _should_project_to_exact_contract_columns(*, profile: SapHeaderProfile) -> bool:
    if profile.object_code == "FAT":
        return parse_bool(
            _safe_env("ENEL_SAP_EXACT_CONTRACT_COLUMNS_FAT")
            or str(_DEFAULT_EXACT_CONTRACT_COLUMNS_FAT)
        )
    if profile.object_code == "CLI":
        return parse_bool(
            _safe_env("ENEL_SAP_EXACT_CONTRACT_COLUMNS_CLI")
            or str(_DEFAULT_EXACT_CONTRACT_COLUMNS_CLI)
        )
    return False


def _project_header_to_required_contract(
    *,
    canonical_header: list[str],
    required_fields: list[str],
    profile: SapHeaderProfile,
) -> tuple[list[str], list[int], list[str]]:
    expected_header = _resolve_expected_canonical_required_fields(
        profile=profile,
        required_fields=required_fields,
    )
    selected_indexes: list[int] = []
    seen_indexes: set[int] = set()
    for expected_name in expected_header:
        expected_token = _norm_header_token(expected_name)
        selected_index = -1
        for index, observed_name in enumerate(canonical_header):
            if index in seen_indexes:
                continue
            observed_token = _norm_header_token(observed_name)
            if (
                observed_token == expected_token
                or expected_token in observed_token
                or observed_token in expected_token
            ):
                selected_index = index
                break
        if selected_index < 0:
            raise RuntimeError(
                "required canonical field could not be projected from normalized header: "
                f"{expected_name}"
            )
        selected_indexes.append(selected_index)
        seen_indexes.add(selected_index)
    dropped_columns = [
        column_name
        for index, column_name in enumerate(canonical_header)
        if index not in seen_indexes
    ]
    return expected_header, selected_indexes, dropped_columns


def _project_row_to_selected_indexes(
    *, row: list[str], selected_indexes: list[int]
) -> list[str]:
    return [row[index] if index < len(row) else "" for index in selected_indexes]


def _materialize_export_output_csv(
    *,
    output_path: Path,
    object_code: str,
    object_config: dict[str, Any],
    context: dict[str, str],
    required_fields: list[str],
    logger: logging.Logger,
    source_ready_timeout_seconds: float,
    source_ready_poll_seconds: float,
    source_ready_stable_hits: int,
    session: Any | None = None,
    raw_csv_path: Path | None = None,
    header_map_path: Path | None = None,
    rejects_path: Path | None = None,
    normalization_metadata: dict[str, Any] | None = None,
) -> int:
    normalization_metadata = (
        normalization_metadata if normalization_metadata is not None else {}
    )
    strict_required_fields = parse_bool(
        _safe_env("ENEL_SAP_STRICT_REQUIRED_FIELDS")
        or str(_DEFAULT_STRICT_REQUIRED_FIELDS)
    )
    coerce_extra_columns = parse_bool(
        _safe_env("ENEL_SAP_COERCE_EXTRA_COLUMNS") or str(_DEFAULT_COERCE_EXTRA_COLUMNS)
    )
    header_scan_max_lines = max(
        20,
        _safe_int_env(
            "ENEL_SAP_HEADER_SCAN_MAX_LINES",
            _DEFAULT_HEADER_SCAN_MAX_LINES,
        ),
    )
    normalization_timeout_seconds = max(
        30.0,
        _safe_float_env(
            "ENEL_SAP_NORMALIZATION_TIMEOUT_SECONDS",
            _DEFAULT_NORMALIZATION_TIMEOUT_SECONDS,
        ),
    )
    write_raw_csv = parse_bool(
        _safe_env("ENEL_SAP_WRITE_RAW_CSV") or str(_DEFAULT_WRITE_RAW_CSV)
    )
    canonical_enable = parse_bool(
        _safe_env("ENEL_SAP_CANONICAL_ENABLE") or str(_DEFAULT_CANONICAL_ENABLE)
    )
    header_max_span_lines = max(
        1,
        min(
            2,
            _safe_int_env(
                "ENEL_SAP_HEADER_MAX_SPAN_LINES",
                _DEFAULT_HEADER_MAX_SPAN_LINES,
            ),
        ),
    )
    header_min_confidence = max(
        0.0,
        min(
            1.0,
            _safe_float_env(
                "ENEL_SAP_HEADER_MIN_CONFIDENCE",
                _DEFAULT_HEADER_MIN_CONFIDENCE,
            ),
        ),
    )
    canonical_fail_on_low_confidence = parse_bool(
        _safe_env("ENEL_SAP_CANONICAL_FAIL_ON_LOW_CONFIDENCE")
        or str(_DEFAULT_CANONICAL_FAIL_ON_LOW_CONFIDENCE)
    )
    source_candidates = _build_source_candidates(
        output_path=output_path,
        object_config=object_config,
        context=context,
    )
    expected_source_names = _extract_expected_source_names(
        object_code=object_code,
        source_candidates=source_candidates,
        object_config=object_config,
        context=context,
    )
    logger.info(
        "TXT->CSV materialization config: object=%s strict_required_fields=%s coerce_extra_columns=%s "
        "header_scan_max_lines=%s normalization_timeout_s=%.1f write_raw=%s canonical_enable=%s "
        "header_max_span=%s header_min_confidence=%.2f candidates=%s expected_names=%s",
        object_code,
        strict_required_fields,
        coerce_extra_columns,
        header_scan_max_lines,
        normalization_timeout_seconds,
        write_raw_csv,
        canonical_enable,
        header_max_span_lines,
        header_min_confidence,
        [str(item) for item in source_candidates],
        sorted(expected_source_names),
    )
    source_path = _wait_for_source_path(
        output_path=output_path,
        object_code=object_code,
        source_candidates=source_candidates,
        expected_source_names=expected_source_names,
        timeout_seconds=source_ready_timeout_seconds,
        poll_seconds=source_ready_poll_seconds,
        stable_hits_required=source_ready_stable_hits,
        session=session,
    )
    if source_path != output_path:
        logger.info(
            "Using alternate SAP export source for normalization: object=%s source=%s destination=%s",
            object_code,
            source_path,
            output_path,
        )

    raw_destination_path = raw_csv_path or output_path.with_suffix(".raw.csv")
    resolved_header_map_path = header_map_path or output_path.with_suffix(
        ".header_map.json"
    )
    resolved_rejects_path = rejects_path or output_path.with_suffix(".rejects.csv")
    rows_exported, encoding_used, delimiter_used = _normalize_delimited_file_to_csv(
        source_path=source_path,
        destination_path=output_path,
        required_fields=required_fields,
        object_code=object_code,
        strict_required_fields=strict_required_fields,
        coerce_extra_columns=coerce_extra_columns,
        header_scan_max_lines=header_scan_max_lines,
        normalization_timeout_seconds=normalization_timeout_seconds,
        write_raw_csv=write_raw_csv,
        raw_destination_path=raw_destination_path,
        canonical_enable=canonical_enable,
        header_map_path=resolved_header_map_path,
        rejects_path=resolved_rejects_path,
        header_max_span_lines=header_max_span_lines,
        header_min_confidence=header_min_confidence,
        canonical_fail_on_low_confidence=canonical_fail_on_low_confidence,
        normalization_metadata=normalization_metadata,
        logger=logger,
    )
    normalization_metadata.setdefault("canonical_csv_path", str(output_path))
    normalization_metadata.setdefault(
        "raw_csv_path",
        str(
            raw_destination_path
            if write_raw_csv and raw_destination_path.exists()
            else ""
        ),
    )
    normalization_metadata.setdefault(
        "header_map_path",
        str(resolved_header_map_path if resolved_header_map_path.exists() else ""),
    )
    normalization_metadata.setdefault(
        "rejects_path",
        str(resolved_rejects_path if resolved_rejects_path.exists() else ""),
    )
    normalization_metadata["encoding_used"] = encoding_used
    normalization_metadata["delimiter_used"] = delimiter_used
    logger.info(
        "Canonical CSV materialized from SAP export: source=%s destination=%s raw=%s rows=%s encoding=%s delimiter=%s",
        source_path,
        output_path,
        normalization_metadata.get("raw_csv_path", ""),
        rows_exported,
        encoding_used,
        repr(delimiter_used),
    )
    logger.info(
        "Header detection summary: object=%s line_start=%s span=%s confidence=%.4f required_hits=%s duplicates=%s",
        object_code,
        normalization_metadata.get("header_line_start", 0),
        normalization_metadata.get("header_line_span", 0),
        float(normalization_metadata.get("header_confidence", 0.0) or 0.0),
        normalization_metadata.get("required_token_hits", 0),
        normalization_metadata.get("duplicate_columns_count", 0),
    )
    if delimiter_used != "\t":
        logger.warning(
            "Unexpected delimiter detected in SAP export source: delimiter=%s source=%s (expected tab).",
            repr(delimiter_used),
            source_path,
        )
    return rows_exported


def _build_source_candidates(
    *,
    output_path: Path,
    object_config: dict[str, Any],
    context: dict[str, str],
) -> list[Path]:
    candidates: list[Path] = []
    copy_from = str(object_config.get("copy_from", "")).strip()
    if copy_from:
        rendered = render_template(copy_from, context).strip()
        if rendered:
            candidates.append(Path(rendered).expanduser().resolve())
    candidates.append(output_path)
    return candidates


def _wait_for_source_path(
    *,
    output_path: Path,
    object_code: str,
    source_candidates: list[Path],
    expected_source_names: set[str],
    timeout_seconds: float,
    poll_seconds: float,
    stable_hits_required: int,
    session: Any | None,
    logger: logging.Logger | None = None,
) -> Path:
    deadline = time.time() + max(0.0, timeout_seconds)
    poll_delay = max(0.05, poll_seconds)
    next_runtime_error_check = 0.0
    next_progress_log = time.time() + _SOURCE_WAIT_LOG_INTERVAL_SECONDS
    stable_hits_by_path: dict[Path, int] = {}
    previous_size_by_path: dict[Path, int] = {}
    observed_paths: set[Path] = set()
    object_key = str(object_code).strip().upper()
    last_seen_paths: list[Path] = []

    while time.time() <= deadline:
        now = time.time()
        if session is not None and now >= next_runtime_error_check:
            _raise_if_runtime_error_screen(session=session)
            next_runtime_error_check = now + 2.0

        scan_paths = list(source_candidates)
        scan_paths.extend(
            _discover_source_aliases(
                output_path=output_path,
                object_code=object_key,
                source_candidates=source_candidates,
            )
        )
        deduped_paths = _dedupe_paths(scan_paths)
        if expected_source_names:
            filtered_paths = [
                candidate
                for candidate in deduped_paths
                if _path_file_name_key(candidate) in expected_source_names
            ]
        else:
            filtered_paths = deduped_paths
        last_seen_paths = filtered_paths

        if now >= next_progress_log:
            logger_candidates = [
                f"{path.name}:stable={stable_hits_by_path.get(path, 0)}"
                for path in filtered_paths[:8]
            ]
            logger_message = (
                ", ".join(logger_candidates) if logger_candidates else "<none>"
            )
            message = "Waiting SAP export source: object=%s remaining_s=%.1f expected=%s candidates=%s"
            expected_tokens = (
                ",".join(sorted(expected_source_names))
                if expected_source_names
                else "<none>"
            )
            if logger is not None:
                logger.info(
                    message,
                    object_key,
                    max(0.0, deadline - now),
                    expected_tokens,
                    logger_message,
                )
            else:
                print(
                    "[INFO] Waiting SAP export source: "
                    f"object={object_key} remaining_s={max(0.0, deadline - now):.1f} "
                    f"expected={expected_tokens} candidates={logger_message}"
                )
            next_progress_log = now + _SOURCE_WAIT_LOG_INTERVAL_SECONDS

        for candidate in filtered_paths:
            try:
                if not candidate.exists() or not candidate.is_file():
                    continue
                size_bytes = candidate.stat().st_size
            except OSError:
                continue
            if size_bytes <= 0:
                continue

            if candidate not in observed_paths:
                observed_paths.add(candidate)
                if logger is not None:
                    logger.info(
                        "SAP export source detected: object=%s path=%s size=%s",
                        object_key,
                        candidate,
                        size_bytes,
                    )
                else:
                    print(
                        f"[INFO] SAP export source detected: object={object_key} "
                        f"path={candidate} size={size_bytes}"
                    )

            previous_size = previous_size_by_path.get(candidate)
            if previous_size == size_bytes:
                stable_hits_by_path[candidate] = (
                    stable_hits_by_path.get(candidate, 0) + 1
                )
            else:
                stable_hits_by_path[candidate] = 1
                previous_size_by_path[candidate] = size_bytes

            if stable_hits_by_path[candidate] >= stable_hits_required:
                if logger is not None:
                    logger.info(
                        "SAP export source stabilized: object=%s path=%s stable_hits=%s",
                        object_key,
                        candidate,
                        stable_hits_by_path[candidate],
                    )
                else:
                    print(
                        f"[INFO] SAP export source stabilized: object={object_key} "
                        f"path={candidate} stable_hits={stable_hits_by_path[candidate]}"
                    )
                return candidate
        time.sleep(poll_delay)

    attempted = ", ".join(str(path) for path in last_seen_paths) or "<none>"
    expected = (
        ",".join(sorted(expected_source_names)) if expected_source_names else "<none>"
    )
    raise RuntimeError(
        "Export file was not created or did not stabilize in time: "
        f"destination={output_path} object={object_key} expected={expected} attempted={attempted}"
    )


def _discover_source_aliases(
    *,
    output_path: Path,
    object_code: str,
    source_candidates: list[Path],
) -> list[Path]:
    directories: set[Path] = {output_path.parent.resolve()}
    prefixes: set[str] = set(_EXPORT_TXT_PREFIX_BY_OBJECT.get(object_code, ()))

    for candidate in source_candidates:
        try:
            directories.add(candidate.parent.resolve())
        except OSError:
            continue
        if candidate.suffix.lower() == ".txt":
            inferred = _extract_leading_alpha_prefix(candidate.stem)
            if inferred:
                prefixes.add(inferred)

    discovered: list[Path] = []
    for directory in sorted(directories):
        for prefix in sorted(prefixes):
            try:
                discovered.extend(directory.glob(f"{prefix}*.txt"))
                discovered.extend(directory.glob(f"{prefix}*.TXT"))
            except OSError:
                continue

    deduped = _dedupe_paths(discovered)
    deduped.sort(
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )
    return deduped


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        try:
            normalized = path.resolve()
        except OSError:
            normalized = path
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _extract_expected_source_names(
    *,
    object_code: str,
    source_candidates: list[Path],
    object_config: dict[str, Any],
    context: dict[str, str],
) -> set[str]:
    expected_names: set[str] = set()
    object_key = str(object_code).strip().upper()

    for candidate in source_candidates:
        if candidate.suffix.lower() != ".txt":
            continue
        expected_names.add(_path_file_name_key(candidate))

    if object_key == "FAT":
        forced_fat_name = str(context.get("fat_export_macro_filename", "")).strip()
        if forced_fat_name:
            expected_names.add(_extract_filename_token(forced_fat_name).casefold())

    raw_steps = object_config.get("steps", [])
    if not isinstance(raw_steps, list):
        return expected_names

    for raw_step in raw_steps:
        if not isinstance(raw_step, dict):
            continue
        action = str(raw_step.get("action", "")).strip().lower()
        if action != "set_text":
            continue
        if not _step_targets_dy_filename(raw_step):
            continue
        value_template = str(raw_step.get("value", "")).strip()
        if not value_template:
            continue
        rendered = render_template(value_template, context).strip()
        if not rendered:
            continue
        expected_name = _extract_filename_token(rendered)
        if not expected_name:
            continue
        if "." in expected_name and not expected_name.lower().endswith(".txt"):
            continue
        expected_names.add(expected_name.casefold())

    return expected_names


def _step_targets_dy_filename(step: dict[str, Any]) -> bool:
    ids: list[str] = []
    single_id = step.get("id")
    if isinstance(single_id, str) and single_id.strip():
        ids.append(single_id)
    raw_ids = step.get("ids")
    if isinstance(raw_ids, list):
        for item in raw_ids:
            if isinstance(item, str) and item.strip():
                ids.append(item)
    return any("DY_FILENAME" in item.upper() for item in ids)


def _path_file_name_key(path: Path) -> str:
    return _extract_filename_token(str(path)).casefold()


def _extract_filename_token(value: str) -> str:
    token = str(value).strip().strip('"').strip("'")
    if not token:
        return ""
    normalized = token.replace("\\", "/")
    return normalized.rsplit("/", 1)[-1].strip()


def _extract_leading_alpha_prefix(value: str) -> str:
    token = value.strip()
    if not token:
        return ""
    collected: list[str] = []
    for char in token:
        if char.isalpha() or char == "_":
            collected.append(char)
            continue
        break
    return "".join(collected).upper()


def _normalize_delimited_file_to_csv(
    *,
    source_path: Path,
    destination_path: Path,
    required_fields: list[str],
    object_code: str = "",
    strict_required_fields: bool = _DEFAULT_STRICT_REQUIRED_FIELDS,
    coerce_extra_columns: bool = _DEFAULT_COERCE_EXTRA_COLUMNS,
    header_scan_max_lines: int = _DEFAULT_HEADER_SCAN_MAX_LINES,
    normalization_timeout_seconds: float = _DEFAULT_NORMALIZATION_TIMEOUT_SECONDS,
    write_raw_csv: bool = _DEFAULT_WRITE_RAW_CSV,
    raw_destination_path: Path | None = None,
    canonical_enable: bool = _DEFAULT_CANONICAL_ENABLE,
    header_map_path: Path | None = None,
    rejects_path: Path | None = None,
    header_max_span_lines: int = _DEFAULT_HEADER_MAX_SPAN_LINES,
    header_min_confidence: float = _DEFAULT_HEADER_MIN_CONFIDENCE,
    canonical_fail_on_low_confidence: bool = _DEFAULT_CANONICAL_FAIL_ON_LOW_CONFIDENCE,
    normalization_metadata: dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> tuple[int, str, str]:
    normalization_metadata = (
        normalization_metadata if normalization_metadata is not None else {}
    )
    profile = get_header_profile(object_code)
    decode_errors: list[str] = []
    parse_errors: list[str] = []
    detected_encoding = _detect_source_encoding(source_path=source_path)
    candidate_encodings = _resolve_candidate_source_encodings(source_path=source_path)
    header_probe = _try_header_with_forced_encoding(
        source_path=source_path,
        profile=profile,
        required_fields=required_fields,
        candidate_encodings=candidate_encodings,
        header_scan_max_lines=header_scan_max_lines,
        header_max_span_lines=header_max_span_lines,
        logger=logger,
    )
    if header_probe:
        chosen_encoding = str(header_probe.get("encoding", "")).strip()
        if chosen_encoding:
            candidate_encodings = tuple(
                [chosen_encoding]
                + [item for item in candidate_encodings if item != chosen_encoding]
            )
    delimiter_candidates = ("\t", ";", ",")
    if header_probe:
        chosen_delimiter = str(header_probe.get("delimiter", "") or "")
        if chosen_delimiter in delimiter_candidates:
            delimiter_candidates = tuple(
                [chosen_delimiter]
                + [item for item in delimiter_candidates if item != chosen_delimiter]
            )
    if detected_encoding:
        normalization_metadata["detected_source_encoding"] = detected_encoding
    normalization_metadata["candidate_encodings"] = list(candidate_encodings)
    normalization_metadata["header_probe"] = dict(header_probe)
    for encoding in candidate_encodings:
        for delimiter in delimiter_candidates:
            if logger is not None:
                logger.info(
                    "CSV normalization attempt: source=%s object=%s encoding=%s delimiter=%s strict_required_fields=%s fallback_chain=%s",
                    source_path,
                    object_code,
                    encoding,
                    repr(delimiter),
                    strict_required_fields,
                    ",".join(candidate_encodings),
                )
            try:
                rows_exported = _transcode_delimited_source_to_csv(
                    source_path=source_path,
                    destination_path=destination_path,
                    required_fields=required_fields,
                    profile=profile,
                    encoding=encoding,
                    delimiter=delimiter,
                    strict_required_fields=strict_required_fields,
                    coerce_extra_columns=coerce_extra_columns,
                    header_scan_max_lines=header_scan_max_lines,
                    normalization_timeout_seconds=normalization_timeout_seconds,
                    write_raw_csv=write_raw_csv,
                    raw_destination_path=raw_destination_path,
                    canonical_enable=canonical_enable,
                    header_map_path=header_map_path,
                    rejects_path=rejects_path,
                    header_max_span_lines=header_max_span_lines,
                    header_min_confidence=header_min_confidence,
                    canonical_fail_on_low_confidence=canonical_fail_on_low_confidence,
                    normalization_metadata=normalization_metadata,
                    logger=logger,
                )
                return rows_exported, encoding, delimiter
            except FatalNormalizationConfigurationError:
                raise
            except UnicodeError as exc:
                if logger is not None:
                    logger.warning(
                        "CSV normalization decode failure: source=%s encoding=%s delimiter=%s error=%s",
                        source_path,
                        encoding,
                        repr(delimiter),
                        exc,
                    )
                decode_errors.append(
                    f"encoding={encoding} delimiter={repr(delimiter)}: {exc}"
                )
                break
            except RuntimeError as exc:
                if logger is not None:
                    logger.warning(
                        "CSV normalization parse failure: source=%s encoding=%s delimiter=%s error=%s",
                        source_path,
                        encoding,
                        repr(delimiter),
                        exc,
                    )
                parse_errors.append(
                    f"encoding={encoding} delimiter={repr(delimiter)}: {exc}"
                )
            except csv.Error as exc:
                if logger is not None:
                    logger.warning(
                        "CSV normalization csv-reader failure: source=%s encoding=%s delimiter=%s error=%s field_size_limit=%s",
                        source_path,
                        encoding,
                        repr(delimiter),
                        exc,
                        _CSV_FIELD_SIZE_LIMIT,
                    )
                parse_errors.append(
                    f"encoding={encoding} delimiter={repr(delimiter)} csv_error={exc}"
                )

    details: list[str] = []
    if parse_errors:
        details.append("parse_errors=" + " | ".join(parse_errors[:6]))
    if decode_errors:
        details.append("decode_errors=" + " | ".join(decode_errors[:4]))
    detail_message = " ".join(details) if details else "no additional details."
    raise RuntimeError(
        f"Could not normalize SAP export file into canonical CSV: source={source_path}. {detail_message}"
    )


def _resolve_candidate_source_encodings(*, source_path: Path) -> tuple[str, ...]:
    raw = (_safe_env("ENEL_SAP_SOURCE_ENCODINGS") or "").strip()
    configured = (
        [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]
        if raw
        else list(_DEFAULT_SOURCE_ENCODINGS)
    )
    detected = _detect_source_encoding(source_path=source_path)
    normalized: list[str] = []
    seen: set[str] = set()
    if detected:
        token = detected.get("encoding", "")
        confidence = float(detected.get("confidence", 0.0) or 0.0)
        if token and confidence >= 0.9:
            seen.add(str(token).casefold())
            normalized.append(str(token))
    for encoding in configured:
        token = encoding.strip()
        if not token:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(token)
    if not normalized:
        return _DEFAULT_SOURCE_ENCODINGS
    return tuple(normalized)


def _try_header_with_forced_encoding(
    *,
    source_path: Path,
    profile: SapHeaderProfile,
    required_fields: list[str],
    candidate_encodings: tuple[str, ...],
    header_scan_max_lines: int,
    header_max_span_lines: int,
    logger: logging.Logger | None = None,
) -> dict[str, object]:
    probe_limit = max(
        1,
        min(
            len(candidate_encodings),
            _safe_int_env(
                "ENEL_SAP_HEADER_PROBE_MAX_ENCODINGS",
                _DEFAULT_HEADER_PROBE_MAX_ENCODINGS,
            ),
        ),
    )
    probe_encodings = candidate_encodings[:probe_limit]
    best_choice: dict[str, object] = {}
    best_score: tuple[int, int, float, int] | None = None

    for encoding in probe_encodings:
        for delimiter in ("\t", ";", ","):
            try:
                with source_path.open(
                    "r",
                    encoding=encoding,
                    newline="",
                    buffering=_SOURCE_TEXT_BUFFERING,
                ) as source_handle:
                    reader = csv.reader(source_handle, delimiter=delimiter)
                    scan_rows: list[list[str]] = []
                    for line_number, raw_row in enumerate(reader, start=1):
                        if line_number > header_scan_max_lines:
                            break
                        scan_rows.append(
                            [_normalize_header_cell(cell) for cell in raw_row]
                        )
                if not scan_rows:
                    continue
                candidate = _locate_source_header(
                    rows=scan_rows,
                    required_fields=required_fields,
                    profile=profile,
                    strict_required_fields=False,
                    max_span_lines=header_max_span_lines,
                    min_confidence=0.0,
                    max_scan_lines=header_scan_max_lines,
                    logger=None,
                )
            except Exception:  # noqa: BLE001
                continue

            score = (
                int(candidate.required_hits),
                int(candidate.optional_hits),
                float(candidate.confidence),
                -int(candidate.negative_hits),
            )
            if best_score is not None and score <= best_score:
                continue
            best_score = score
            best_choice = {
                "encoding": encoding,
                "delimiter": delimiter,
                "confidence": round(float(candidate.confidence), 4),
                "required_hits": int(candidate.required_hits),
                "optional_hits": int(candidate.optional_hits),
                "line_start": int(candidate.line_start),
                "line_span": int(candidate.span),
            }

    if logger is not None and best_choice:
        logger.info(
            "Header encoding probe selected: source=%s encoding=%s delimiter=%s confidence=%.4f required_hits=%s optional_hits=%s",
            source_path,
            best_choice.get("encoding", ""),
            repr(best_choice.get("delimiter", "")),
            float(best_choice.get("confidence", 0.0) or 0.0),
            best_choice.get("required_hits", 0),
            best_choice.get("optional_hits", 0),
        )
    return best_choice


def _detect_source_encoding(*, source_path: Path) -> dict[str, object]:
    if charset_normalizer_from_bytes is None:
        return {}
    sample_size = max(
        4096,
        _safe_int_env(
            "ENEL_SAP_ENCODING_DETECTION_SAMPLE_BYTES",
            _DEFAULT_ENCODING_DETECTION_SAMPLE_BYTES,
        ),
    )
    try:
        with source_path.open("rb", buffering=_SOURCE_TEXT_BUFFERING) as handle:
            raw_bytes = handle.read(sample_size)
    except OSError:
        return {}
    if not raw_bytes:
        return {}
    try:
        matches = charset_normalizer_from_bytes(raw_bytes)
        best = matches.best()
    except Exception:  # noqa: BLE001
        return {}
    if best is None or not getattr(best, "encoding", ""):
        return {}
    confidence = float(getattr(best, "percent_chaos", 100.0) or 100.0)
    quality = max(0.0, min(1.0, 1.0 - (confidence / 100.0)))
    return {
        "encoding": str(best.encoding),
        "confidence": round(quality, 4),
    }


def _transcode_delimited_source_to_csv(
    *,
    source_path: Path,
    destination_path: Path,
    required_fields: list[str],
    profile: SapHeaderProfile,
    encoding: str,
    delimiter: str,
    strict_required_fields: bool,
    coerce_extra_columns: bool,
    header_scan_max_lines: int,
    normalization_timeout_seconds: float,
    write_raw_csv: bool,
    raw_destination_path: Path | None,
    canonical_enable: bool,
    header_map_path: Path | None,
    rejects_path: Path | None,
    header_max_span_lines: int,
    header_min_confidence: float,
    canonical_fail_on_low_confidence: bool,
    normalization_metadata: dict[str, Any] | None,
    logger: logging.Logger | None,
) -> int:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path = raw_destination_path or destination_path.with_suffix(".raw.csv")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_header_map_path = header_map_path or destination_path.with_suffix(
        ".header_map.json"
    )
    resolved_header_map_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_rejects_path = rejects_path or destination_path.with_suffix(".rejects.csv")
    resolved_rejects_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output = destination_path.parent / f".{destination_path.name}.tmp"
    temp_raw_output = raw_path.parent / f".{raw_path.name}.tmp"
    temp_header_map = (
        resolved_header_map_path.parent / f".{resolved_header_map_path.name}.tmp"
    )
    temp_rejects_output = (
        resolved_rejects_path.parent / f".{resolved_rejects_path.name}.tmp"
    )
    _unlink_if_exists(temp_output)
    _unlink_if_exists(temp_raw_output)
    _unlink_if_exists(temp_header_map)
    _unlink_if_exists(temp_rejects_output)

    rows_exported = 0
    rows_raw = 0
    rows_rejected = 0
    started_at = time.time()
    deadline = started_at + normalization_timeout_seconds
    try:
        if logger is not None:
            logger.info(
                "CSV transcode start: source=%s destination=%s encoding=%s delimiter=%s",
                source_path,
                destination_path,
                encoding,
                repr(delimiter),
            )
        with source_path.open(
            "r",
            encoding=encoding,
            newline="",
            buffering=_SOURCE_TEXT_BUFFERING,
        ) as source_handle:
            reader = csv.reader(source_handle, delimiter=delimiter)
            scan_rows: list[list[str]] = []
            for line_number, raw_row in enumerate(reader, start=1):
                if line_number > header_scan_max_lines:
                    break
                scan_rows.append([_normalize_header_cell(cell) for cell in raw_row])
            fatal_layout_error = _build_fat_layout_mismatch_error_from_scan_rows(
                profile=profile,
                scan_rows=scan_rows,
                required_fields=required_fields,
            )
            if fatal_layout_error is not None:
                raise FatalNormalizationConfigurationError(fatal_layout_error)
            header_candidate = _locate_source_header(
                rows=scan_rows,
                required_fields=required_fields,
                profile=profile,
                strict_required_fields=strict_required_fields,
                max_span_lines=header_max_span_lines,
                min_confidence=header_min_confidence,
                max_scan_lines=header_scan_max_lines,
                logger=logger,
            )
            if (
                canonical_fail_on_low_confidence
                and header_candidate.confidence < header_min_confidence
            ):
                raise RuntimeError(
                    "header confidence below threshold: "
                    f"confidence={header_candidate.confidence:.4f} "
                    f"threshold={header_min_confidence:.4f} "
                    f"line={header_candidate.line_start} span={header_candidate.span}"
                )
            raw_header = [
                _normalize_header_cell(item) for item in header_candidate.header
            ]
            canonical_header = _build_canonical_header(
                raw_header=raw_header,
                profile=profile,
                canonical_enable=canonical_enable,
            )
            missing_canonical_required = _required_fields_missing(
                header=canonical_header,
                required_fields=_resolve_expected_canonical_required_fields(
                    profile=profile,
                    required_fields=required_fields,
                ),
            )
            if missing_canonical_required:
                fatal_layout_error = _build_fat_layout_mismatch_error(
                    profile=profile,
                    raw_header=raw_header,
                    canonical_header=canonical_header,
                    missing_required_fields=missing_canonical_required,
                )
                if fatal_layout_error is not None:
                    raise FatalNormalizationConfigurationError(fatal_layout_error)
                raise RuntimeError(
                    "required canonical fields not present after header normalization: "
                    + ", ".join(missing_canonical_required)
                )
            projected_header = canonical_header
            projected_indexes = list(range(len(canonical_header)))
            dropped_canonical_columns: list[str] = []
            if _should_project_to_exact_contract_columns(profile=profile):
                (
                    projected_header,
                    projected_indexes,
                    dropped_canonical_columns,
                ) = _project_header_to_required_contract(
                    canonical_header=canonical_header,
                    required_fields=required_fields,
                    profile=profile,
                )
            raw_column_count = len(raw_header)
            canonical_column_count = len(canonical_header)
            projected_column_count = len(projected_header)
            expected_full_columns = list(profile.full_column_spec or ())
            captured_expected_columns = [
                column_name
                for column_name in expected_full_columns
                if any(
                    _norm_header_token(column_name) == _norm_header_token(observed)
                    for observed in canonical_header
                )
            ]
            missing_expected_columns = [
                column_name
                for column_name in expected_full_columns
                if column_name not in captured_expected_columns
            ]
            duplicate_columns_count = _count_duplicates_by_normalized_key(
                header_candidate.header
            )
            raw_occurrence_by_token: dict[str, int] = {}
            raw_total_by_token: dict[str, int] = {}
            for raw_name in raw_header:
                token = _norm_header_token(raw_name)
                if not token:
                    continue
                raw_total_by_token[token] = raw_total_by_token.get(token, 0) + 1
            header_map_rows: list[dict[str, Any]] = []
            for index, (raw_name, canonical_name) in enumerate(
                zip(raw_header, canonical_header, strict=False),
                start=1,
            ):
                token = _norm_header_token(raw_name)
                occurrence = 1
                if token:
                    occurrence = raw_occurrence_by_token.get(token, 0) + 1
                    raw_occurrence_by_token[token] = occurrence
                header_map_rows.append(
                    {
                        "index": index,
                        "raw": raw_name,
                        "raw_normalized": token,
                        "canonical": canonical_name,
                        "canonical_normalized": _norm_header_token(canonical_name),
                        "occurrence": occurrence,
                        "is_duplicate_raw": raw_total_by_token.get(token, 0) > 1,
                        "alias_used": token != _norm_header_token(canonical_name),
                        "header_confidence": round(header_candidate.confidence, 4),
                        "line_start": header_candidate.line_start,
                        "line_span": header_candidate.span,
                    }
                )
            if normalization_metadata is not None:
                normalization_metadata["raw_csv_path"] = (
                    str(raw_path) if write_raw_csv else ""
                )
                normalization_metadata["canonical_csv_path"] = str(destination_path)
                normalization_metadata["header_line_start"] = (
                    header_candidate.line_start
                )
                normalization_metadata["header_line_span"] = header_candidate.span
                normalization_metadata["header_confidence"] = (
                    header_candidate.confidence
                )
                normalization_metadata["header_columns_raw"] = raw_header
                normalization_metadata["header_columns_canonical_detected"] = (
                    canonical_header
                )
                normalization_metadata["header_columns_canonical"] = projected_header
                normalization_metadata["duplicate_columns_count"] = (
                    duplicate_columns_count
                )
                normalization_metadata["required_token_hits"] = (
                    header_candidate.required_hits
                )
                normalization_metadata["raw_column_count"] = raw_column_count
                normalization_metadata["canonical_column_count"] = (
                    canonical_column_count
                )
                normalization_metadata["projected_column_count"] = (
                    projected_column_count
                )
                normalization_metadata["expected_full_column_count"] = len(
                    expected_full_columns
                )
                normalization_metadata["captured_expected_column_count"] = len(
                    captured_expected_columns
                )
                normalization_metadata["missing_expected_columns"] = (
                    missing_expected_columns
                )
                normalization_metadata["normalization_profile_version"] = (
                    profile.profile_version
                )
                normalization_metadata["header_map_path"] = str(
                    resolved_header_map_path
                )
                normalization_metadata["canonical_contract_projection_enabled"] = (
                    projected_header != canonical_header
                )
                normalization_metadata["dropped_canonical_columns"] = (
                    dropped_canonical_columns
                )

            expected_columns = len(raw_header)
            if logger is not None:
                logger.info(
                    "CSV header selected: source=%s line=%s span=%s columns=%s confidence=%.4f preview=%s",
                    source_path,
                    header_candidate.line_start,
                    header_candidate.span,
                    raw_column_count,
                    header_candidate.confidence,
                    ", ".join(raw_header[:8]),
                )
                if (
                    canonical_column_count != raw_column_count
                    or projected_column_count != raw_column_count
                ):
                    logger.warning(
                        "Column count divergence after SAP TXT normalization: object=%s raw=%s canonical=%s projected=%s",
                        profile.object_code,
                        raw_column_count,
                        canonical_column_count,
                        projected_column_count,
                    )
                if expected_full_columns:
                    logger.info(
                        "FAT full-column coverage: captured_expected=%s expected=%s missing=%s",
                        len(captured_expected_columns),
                        len(expected_full_columns),
                        len(missing_expected_columns),
                    )
                if dropped_canonical_columns:
                    logger.warning(
                        "Canonical FAT contract projection applied with dropped columns: kept=%s dropped=%s",
                        len(projected_header),
                        dropped_canonical_columns,
                    )
            with ExitStack() as stack:
                output_handle = stack.enter_context(
                    temp_output.open("w", encoding="utf-8", newline="")
                )
                canonical_writer = csv.writer(output_handle)
                canonical_writer.writerow(projected_header)
                raw_writer = None
                rejects_writer = None
                if write_raw_csv:
                    raw_handle = stack.enter_context(
                        temp_raw_output.open("w", encoding="utf-8", newline="")
                    )
                    raw_writer = csv.writer(raw_handle)
                    raw_writer.writerow(raw_header)
                rejects_handle = stack.enter_context(
                    temp_rejects_output.open(
                        "w",
                        encoding="utf-8",
                        newline="",
                    )
                )
                rejects_writer = csv.writer(rejects_handle)
                rejects_writer.writerow(
                    [
                        "line_number",
                        "reason",
                        "expected_columns",
                        "actual_columns",
                        "raw_row",
                    ]
                )
                sanitize_cell = _strip_invisible_characters
                project_row = _project_row_to_selected_indexes
                for line_number, row in _iter_data_rows_after_header(
                    reader=reader,
                    scan_rows=scan_rows,
                    header_candidate=header_candidate,
                ):
                    rows_raw += 1
                    if (
                        rows_raw % _NORMALIZATION_TIMEOUT_CHECK_EVERY_ROWS == 0
                        and time.time() > deadline
                    ):
                        raise RuntimeError(
                            "normalization timeout exceeded: "
                            f"timeout_seconds={normalization_timeout_seconds} "
                            f"line={line_number} rows_written={rows_exported}"
                        )
                    coerced_row = _coerce_row_to_expected_columns(
                        row=row,
                        expected_columns=expected_columns,
                        coerce_extra_columns=coerce_extra_columns,
                    )
                    if coerced_row is None:
                        rows_rejected += 1
                        assert rejects_writer is not None
                        rejects_writer.writerow(
                            [
                                line_number,
                                "column_count_mismatch",
                                expected_columns,
                                len(row),
                                " | ".join(str(item) for item in row),
                            ]
                        )
                        continue
                    sanitized_row = [sanitize_cell(item) for item in coerced_row]
                    if raw_writer is not None:
                        raw_writer.writerow(sanitized_row)
                    canonical_writer.writerow(
                        project_row(
                            row=sanitized_row,
                            selected_indexes=projected_indexes,
                        )
                    )
                    rows_exported += 1
                    if (
                        logger is not None
                        and rows_exported % _NORMALIZATION_PROGRESS_EVERY_ROWS == 0
                    ):
                        logger.info(
                            "CSV normalization progress: source=%s rows_written=%s line=%s",
                            source_path,
                            rows_exported,
                            line_number,
                        )
                _flush_and_sync(output_handle)
                if write_raw_csv:
                    _flush_and_sync(raw_handle)
                _flush_and_sync(rejects_handle)
            temp_header_map.parent.mkdir(parents=True, exist_ok=True)
            with temp_header_map.open("w", encoding="utf-8") as header_map_handle:
                header_map_handle.write(
                    json.dumps(
                        {
                            "metadata": {
                                "object_code": profile.object_code,
                                "profile_version": profile.profile_version,
                                "header_line_start": header_candidate.line_start,
                                "header_line_span": header_candidate.span,
                                "header_confidence": round(
                                    header_candidate.confidence,
                                    4,
                                ),
                                "raw_column_count": raw_column_count,
                                "canonical_column_count": canonical_column_count,
                                "projected_column_count": projected_column_count,
                                "expected_full_column_count": len(
                                    expected_full_columns
                                ),
                                "captured_expected_column_count": len(
                                    captured_expected_columns
                                ),
                                "missing_expected_columns": missing_expected_columns,
                            },
                            "columns": header_map_rows,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                _flush_and_sync(header_map_handle)

        _promote_temp_file(
            temp_path=temp_output,
            destination_path=destination_path,
            artifact_label="canonical_csv",
            logger=logger,
        )
        _promote_optional_temp_file(
            temp_path=temp_raw_output if write_raw_csv else None,
            destination_path=raw_path if write_raw_csv else None,
            artifact_label="raw_csv",
            logger=logger,
        )
        _promote_optional_temp_file(
            temp_path=temp_header_map,
            destination_path=resolved_header_map_path,
            artifact_label="header_map",
            logger=logger,
        )
        _promote_optional_temp_file(
            temp_path=temp_rejects_output,
            destination_path=resolved_rejects_path,
            artifact_label="rejects_csv",
            logger=logger,
        )
        if logger is not None:
            logger.info(
                "CSV transcode finished: source=%s destination=%s rows=%s raw_rows=%s rejected_rows=%s elapsed_s=%.2f",
                source_path,
                destination_path,
                rows_exported,
                rows_raw,
                rows_rejected,
                time.time() - started_at,
            )
    except Exception:
        _unlink_if_exists(temp_output)
        _unlink_if_exists(temp_raw_output)
        _unlink_if_exists(temp_header_map)
        _unlink_if_exists(temp_rejects_output)
        raise

    normalized_rows = count_csv_rows(destination_path)
    if normalized_rows != rows_exported:
        raise RuntimeError(
            "row count mismatch after normalization: "
            f"source_rows={rows_exported} normalized_rows={normalized_rows}"
        )
    if normalization_metadata is not None:
        normalization_metadata["rows_raw"] = rows_raw
        normalization_metadata["rows_normalized"] = rows_exported
        normalization_metadata["rows_rejected"] = rows_rejected
        normalization_metadata["rejects_path"] = str(
            resolved_rejects_path if resolved_rejects_path.exists() else ""
        )
        normalization_metadata["temp_promotion_verified"] = True
        normalization_metadata.update(
            _artifact_audit_metadata("source_txt", source_path)
        )
        normalization_metadata.update(
            _artifact_audit_metadata("canonical_csv", destination_path)
        )
        normalization_metadata.update(
            _artifact_audit_metadata(
                "raw_csv",
                raw_path if write_raw_csv and raw_path.exists() else None,
            )
        )
        normalization_metadata.update(
            _artifact_audit_metadata(
                "header_map",
                resolved_header_map_path if resolved_header_map_path.exists() else None,
            )
        )
        normalization_metadata.update(
            _artifact_audit_metadata(
                "rejects",
                resolved_rejects_path if resolved_rejects_path.exists() else None,
            )
        )
    return rows_exported


def _normalize_header_cell(value: str) -> str:
    return _strip_invisible_characters(str(value)).strip()


def _normalize_header_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", _strip_invisible_characters(str(value)))
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    compact = " ".join(without_accents.strip().casefold().split())
    return compact


def _strip_invisible_characters(value: str) -> str:
    text = str(value)
    if text.isascii() and "\ufeff" not in text and "\xa0" not in text:
        return text
    if "\ufeff" in text or "\xa0" in text:
        text = text.replace("\ufeff", "").replace("\xa0", " ")
    if text.isascii():
        return text
    cleaned: list[str] = []
    for char in text:
        if unicodedata.category(char) == "Cf":
            continue
        cleaned.append(char)
    return "".join(cleaned)


def _dedupe_header_cells(header: list[str]) -> list[str]:
    deduped: list[str] = []
    used_keys: set[str] = set()

    for index, raw_name in enumerate(header, start=1):
        base_name = raw_name or f"UNNAMED_{index}"
        candidate = base_name
        suffix = 2
        while _normalize_header_key(candidate) in used_keys:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        deduped.append(candidate)
        used_keys.add(_normalize_header_key(candidate))

    return deduped


def _locate_source_header(
    *,
    rows: list[list[str]],
    required_fields: list[str],
    profile: SapHeaderProfile,
    strict_required_fields: bool,
    max_span_lines: int,
    min_confidence: float,
    max_scan_lines: int,
    logger: logging.Logger | None = None,
) -> HeaderCandidate:
    best_candidate: HeaderCandidate | None = None
    required_non_empty = [item for item in required_fields if item.strip()]

    max_line = min(max_scan_lines, len(rows))
    for line_index in range(max_line):
        row = rows[line_index]
        if not any(item.strip() for item in row):
            continue

        candidates: list[tuple[list[str], int]] = [(row, 1)]
        if (
            profile.allow_multiline_header
            and max_span_lines >= 2
            and line_index + 1 < max_line
        ):
            candidates.append((_merge_header_rows(row, rows[line_index + 1]), 2))

        for candidate_row, span in candidates:
            scored = _score_header_candidate(
                candidate_row=candidate_row,
                line_start=line_index + 1,
                span=span,
                profile=profile,
                required_fields=required_non_empty,
            )
            if best_candidate is None or scored.score > best_candidate.score:
                best_candidate = scored
                if logger is not None:
                    logger.info(
                        "Header candidate improved: line=%s span=%s score=%.4f confidence=%.4f required_hits=%s optional_hits=%s negatives=%s unnamed=%s",
                        scored.line_start,
                        scored.span,
                        scored.score,
                        scored.confidence,
                        scored.required_hits,
                        scored.optional_hits,
                        scored.negative_hits,
                        scored.unnamed_count,
                    )
            missing_fields = _required_fields_missing(
                header=scored.header,
                required_fields=required_non_empty,
            )
            if not missing_fields and scored.confidence >= min_confidence:
                return scored

    if best_candidate is None:
        raise RuntimeError(
            "source file is empty or header was not found within scan window. "
            f"max_scan_lines={max_scan_lines}"
        )

    if not required_non_empty:
        return best_candidate

    missing_fields = _required_fields_missing(
        header=best_candidate.header,
        required_fields=required_non_empty,
    )
    if missing_fields and not strict_required_fields:
        return best_candidate
    if best_candidate.confidence < min_confidence and not strict_required_fields:
        return best_candidate

    preview = ", ".join(best_candidate.header[:8])
    raise RuntimeError(
        "required fields not present in normalized export header: "
        + ", ".join(missing_fields)
        + f" | candidate_line={best_candidate.line_start}"
        + f" candidate_span={best_candidate.span}"
        + f" candidate_confidence={best_candidate.confidence:.4f}"
        + f" candidate_preview={preview}"
        + f" | max_scan_lines={max_scan_lines}"
    )


def _score_header_candidate(
    *,
    candidate_row: list[str],
    line_start: int,
    span: int,
    profile: SapHeaderProfile,
    required_fields: list[str],
) -> HeaderCandidate:
    raw_header = [_normalize_header_cell(item) for item in candidate_row]
    normalized_cells = [_norm_header_token(item) for item in raw_header if item.strip()]
    unique_normalized = set(normalized_cells)
    non_empty_count = sum(1 for item in raw_header if item.strip())
    unnamed_count = sum(1 for item in raw_header if not item.strip())
    duplicate_count = max(0, len(normalized_cells) - len(unique_normalized))

    required_tokens = set(profile.required_tokens)
    explicit_required_tokens: set[str] = set()
    for documented_field in profile.documented_fields:
        token = _norm_header_token(documented_field)
        if token:
            required_tokens.add(token)
    for field in required_fields:
        token = _norm_header_token(field)
        if token:
            required_tokens.add(token)
            explicit_required_tokens.add(token)
    optional_tokens = set(profile.optional_tokens)
    negative_tokens = set(profile.preamble_negative_tokens)

    required_hits = sum(
        1
        for token in required_tokens
        if any(token in cell for cell in normalized_cells)
    )
    confidence_required_tokens = explicit_required_tokens or required_tokens
    confidence_required_hits = sum(
        1
        for token in confidence_required_tokens
        if any(token in cell for cell in normalized_cells)
    )
    optional_hits = sum(
        1
        for token in optional_tokens
        if any(token in cell for cell in normalized_cells)
    )
    joined = " ".join(normalized_cells)
    negative_hits = sum(1 for token in negative_tokens if token and token in joined)
    unnamed_ratio = (unnamed_count / len(raw_header)) if raw_header else 1.0

    score = (
        (required_hits * 5.0)
        + (optional_hits * 1.5)
        + (non_empty_count * 0.03)
        + (len(unique_normalized) * 0.02)
        - (negative_hits * 3.0)
        - (unnamed_ratio * 2.5)
    )
    confidence = _header_confidence(
        required_hits=confidence_required_hits,
        total_required=max(1, len(confidence_required_tokens)),
        optional_hits=optional_hits,
        total_optional=max(1, len(optional_tokens)),
        unnamed_ratio=unnamed_ratio,
        negative_hits=negative_hits,
    )
    return HeaderCandidate(
        header=raw_header,
        line_start=line_start,
        line_end=line_start + span - 1,
        span=span,
        confidence=confidence,
        required_hits=required_hits,
        optional_hits=optional_hits,
        negative_hits=negative_hits,
        non_empty_count=non_empty_count,
        unnamed_count=unnamed_count,
        duplicate_count=duplicate_count,
        score=score,
    )


def _header_confidence(
    *,
    required_hits: int,
    total_required: int,
    optional_hits: int,
    total_optional: int,
    unnamed_ratio: float,
    negative_hits: int,
) -> float:
    required_score = min(1.0, required_hits / max(1, total_required))
    optional_score = min(1.0, optional_hits / max(1, total_optional))
    negative_penalty = min(0.4, negative_hits * 0.2)
    confidence = (
        (required_score * 0.7)
        + (optional_score * 0.2)
        + ((1.0 - min(1.0, unnamed_ratio)) * 0.1)
        - negative_penalty
    )
    return max(0.0, min(1.0, confidence))


def _iter_data_rows_after_header(
    *,
    reader: csv.reader,
    scan_rows: list[list[str]],
    header_candidate: HeaderCandidate,
):
    for line_number in range(header_candidate.line_end + 1, len(scan_rows) + 1):
        row = scan_rows[line_number - 1]
        if _looks_like_header_continuation_row(
            row=row,
            header_candidate=header_candidate,
        ):
            continue
        yield line_number, row
    for line_number, row in enumerate(reader, start=len(scan_rows) + 1):
        yield line_number, row


def _looks_like_header_continuation_row(
    *,
    row: list[str],
    header_candidate: HeaderCandidate,
) -> bool:
    non_empty_tokens = [
        _norm_header_token(cell) for cell in row if _normalize_header_cell(cell)
    ]
    if not non_empty_tokens:
        return False

    header_tokens = [
        _norm_header_token(cell)
        for cell in header_candidate.header
        if _normalize_header_cell(cell)
    ]
    if not header_tokens:
        return False

    matched_tokens = 0
    numeric_like_tokens = 0
    for token in non_empty_tokens:
        if token.isdigit():
            numeric_like_tokens += 1
        if any(
            token == header_token or token in header_token or header_token in token
            for header_token in header_tokens
        ):
            matched_tokens += 1

    match_ratio = matched_tokens / max(1, len(non_empty_tokens))
    numeric_ratio = numeric_like_tokens / max(1, len(non_empty_tokens))
    return match_ratio >= 0.8 and numeric_ratio <= 0.2


def _merge_header_rows(first: list[str], second: list[str]) -> list[str]:
    max_len = max(len(first), len(second))
    merged: list[str] = []
    for index in range(max_len):
        first_value = first[index] if index < len(first) else ""
        second_value = second[index] if index < len(second) else ""
        first_value = _normalize_header_cell(first_value)
        second_value = _normalize_header_cell(second_value)
        if first_value and second_value:
            first_norm = _norm_header_token(first_value)
            second_norm = _norm_header_token(second_value)
            if first_norm == second_norm:
                merged.append(first_value)
            else:
                merged.append(f"{first_value} {second_value}".strip())
            continue
        merged.append(first_value or second_value)
    return merged


def _build_canonical_header(
    *,
    raw_header: list[str],
    profile: SapHeaderProfile,
    canonical_enable: bool,
) -> list[str]:
    if not canonical_enable:
        return _dedupe_header_cells(list(raw_header))
    mapped: list[str] = []
    occurrence_by_token: dict[str, int] = {}
    for index, raw_name in enumerate(raw_header, start=1):
        base_name = raw_name or f"UNNAMED_{index}"
        token = _norm_header_token(base_name)
        occurrence = 1
        if token:
            occurrence = occurrence_by_token.get(token, 0) + 1
            occurrence_by_token[token] = occurrence
        semantic_name = _resolve_semantic_duplicate_column_name(
            raw_name=base_name,
            occurrence=occurrence,
            profile=profile,
        )
        if semantic_name:
            mapped.append(semantic_name)
            continue
        canonical = profile.known_aliases.get(token)
        if canonical:
            mapped.append(canonical)
            continue
        mapped.append(_sanitize_canonical_name(base_name))
    return _dedupe_header_cells(mapped)


def _resolve_semantic_duplicate_column_name(
    *,
    raw_name: str,
    occurrence: int,
    profile: SapHeaderProfile,
) -> str | None:
    token = _norm_header_token(raw_name)
    if not token:
        return None
    semantic_name = profile.duplicate_column_semantics.get((token, occurrence))
    if semantic_name:
        return semantic_name
    alias_name = profile.known_aliases.get(token)
    if alias_name:
        alias_token = _norm_header_token(alias_name)
        if alias_token:
            semantic_name = profile.duplicate_column_semantics.get(
                (alias_token, occurrence)
            )
            if semantic_name:
                return semantic_name
    return None


def _sanitize_canonical_name(raw_name: str) -> str:
    value = _normalize_header_cell(raw_name)
    if not value:
        return "UNNAMED"
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", ascii_text).strip("_")
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.lower() or "UNNAMED"


def _norm_header_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_text.lower())


def _count_duplicates_by_normalized_key(values: list[str]) -> int:
    seen: set[str] = set()
    duplicates = 0
    for value in values:
        key = _normalize_header_key(value)
        if not key:
            continue
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
    return duplicates


def _coerce_row_to_expected_columns(
    *,
    row: list[str],
    expected_columns: int,
    coerce_extra_columns: bool,
) -> list[str] | None:
    if len(row) == expected_columns:
        return row
    if len(row) < expected_columns:
        return row + ([""] * (expected_columns - len(row)))
    extras = row[expected_columns:]
    if coerce_extra_columns and expected_columns >= 1:
        prefix = row[: expected_columns - 1]
        overflow = row[expected_columns - 1 :]
        merged_last = " ".join(
            token for token in (str(item).strip() for item in overflow) if token
        )
        return prefix + [merged_last]
    if all(not str(cell).strip() for cell in extras):
        return row[:expected_columns]
    return None


def _assert_required_fields_present(
    *, header: list[str], required_fields: list[str]
) -> None:
    missing = _required_fields_missing(
        header=header,
        required_fields=required_fields,
    )
    if missing:
        raise RuntimeError(
            "required fields not present in normalized export header: "
            + ", ".join(missing)
        )


def _required_fields_missing(
    *,
    header: list[str],
    required_fields: list[str],
) -> list[str]:
    if not required_fields:
        return []
    normalized_header = [
        _norm_header_token(item) for item in header if str(item).strip()
    ]
    missing: list[str] = []
    for raw_field in required_fields:
        required_token = _norm_header_token(raw_field)
        if not required_token:
            continue
        if any(
            required_token == header_token
            or required_token in header_token
            or header_token in required_token
            for header_token in normalized_header
            if header_token
        ):
            continue
        missing.append(str(raw_field).strip())
    return missing


def _build_fat_layout_mismatch_error(
    *,
    profile: SapHeaderProfile,
    raw_header: list[str],
    canonical_header: list[str],
    missing_required_fields: list[str],
) -> str | None:
    if profile.object_code != "FAT":
        return None

    observed_tokens = {
        _norm_header_token(item) for item in raw_header if _normalize_header_cell(item)
    }
    mismatch_markers = {
        "nodocoficial",
        "nodoccalc",
        "tipocontrato",
        "motivofaturamento",
        "zntemp",
        "ctgtar",
    }
    detected_markers = sorted(
        marker for marker in mismatch_markers if marker in observed_tokens
    )
    critical_missing = {
        "status_da_fatura",
        "tipo_de_fatura",
        "lote_de_leitura",
        "tipo_de_leitura",
        "equipam",
        "unleit",
        "descricao_da_classe",
    }
    missing_critical = sorted(
        item for item in missing_required_fields if item in critical_missing
    )
    if len(detected_markers) < 3 or len(missing_critical) < 4:
        return None

    header_preview = ", ".join(raw_header[:12])
    return (
        "FAT export layout mismatch detected. The SAP TXT header does not match the "
        "RJ contract expected by the pipeline. This usually means the ALV column "
        "variant/layout selection failed or a different report layout was exported. "
        f"Detected markers={detected_markers}. "
        f"Missing critical canonical fields={missing_critical}. "
        f"Header preview={header_preview}. "
        "Review the SAP FAT variant steps and ensure the export includes business "
        "columns such as Status da fatura, Tipo de fatura, Lote de leitura, "
        "Tipo de leitura, UnLeit, Equipam and the numbered Quantidade de calculo fields."
    )


def _build_fat_layout_mismatch_error_from_scan_rows(
    *,
    profile: SapHeaderProfile,
    scan_rows: list[list[str]],
    required_fields: list[str],
) -> str | None:
    if profile.object_code != "FAT":
        return None

    observed_tokens = {
        _norm_header_token(cell)
        for row in scan_rows[:12]
        for cell in row
        if _normalize_header_cell(cell)
    }
    required_tokens = {
        _norm_header_token(field) for field in required_fields if str(field).strip()
    }
    detected_markers = sorted(
        marker
        for marker in (
            "nodocoficial",
            "nodoccalc",
            "tipocontrato",
            "motivofaturamento",
            "zntemp",
            "ctgtar",
        )
        if marker in observed_tokens
    )
    missing_expected_tokens = sorted(
        token
        for token in (
            "statusdafatura",
            "tipodefaturafaturamentorefaturamento",
            "lotedeleitura",
            "tipodeleitura",
            "equipam",
            "unleit",
            "descricaodaclasse",
        )
        if token not in observed_tokens and token in required_tokens
    )
    if len(detected_markers) < 3 or len(missing_expected_tokens) < 4:
        return None

    preview = ", ".join(
        cell for row in scan_rows[:6] for cell in row if _normalize_header_cell(cell)
    )
    return (
        "FAT export layout mismatch detected. The TXT contains header markers from the "
        "default Breakdown layout instead of the RJ business-column layout. "
        f"Detected markers={detected_markers}. Missing expected tokens={missing_expected_tokens}. "
        f"Scan preview={preview[:600]}. "
        "Check the SAP ALV variant/column-selection steps before the export."
    )


def expand_lot_ranges(lot_ranges: list[list[int]]) -> list[str]:
    result: list[str] = []
    for pair in lot_ranges:
        if len(pair) != 2:
            continue
        start, end = int(pair[0]), int(pair[1])
        if end < start:
            start, end = end, start
        for value in range(start, end + 1):
            result.append(str(value))
    return result


def serialize_lot_ranges(lot_ranges: list[list[int]]) -> str:
    chunks: list[str] = []
    for pair in lot_ranges:
        if len(pair) != 2:
            continue
        start, end = int(pair[0]), int(pair[1])
        if start == end:
            chunks.append(str(start))
        else:
            if end < start:
                start, end = end, start
            chunks.append(f"{start}-{end}")
    return ",".join(chunks)


def _extract_filter_value(
    filters: list[dict[str, str]], needles: tuple[str, ...]
) -> str:
    lower_needles = tuple(item.lower().strip() for item in needles if item.strip())
    if not lower_needles:
        return ""
    for item in filters:
        field = str(item.get("field", "")).strip().lower()
        if not field:
            continue
        if any(needle in field for needle in lower_needles):
            return str(item.get("value", "")).strip()
    return ""


def _load_dotenv_for_script(script_dir: Path) -> None:
    candidates = [
        Path.cwd() / ".env",
        script_dir.parent / ".env",
    ]
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        _load_dotenv_file(path=resolved)


def _load_dotenv_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, _parse_env_value(value))


def _parse_env_value(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and (
        (value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")
    ):
        return value[1:-1]
    return value


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _safe_env(name: str) -> str:
    value = os.environ.get(name, "")
    return str(value).strip()


def _safe_float_env(name: str, fallback: float) -> float:
    raw = _safe_env(name)
    if not raw:
        return fallback
    try:
        return float(raw)
    except ValueError:
        return fallback


def _safe_int_env(name: str, fallback: int) -> int:
    raw = _safe_env(name)
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _describe_step(step: dict[str, Any], context: dict[str, str]) -> str:
    parts: list[str] = []
    if str(step.get("label", "")).strip():
        parts.append(f"label={step.get('label')}")

    ids = _resolve_step_ids(step=step, context=context)
    if ids:
        parts.append(f"ids={','.join(ids[:2])}")
        if len(ids) > 2:
            parts.append(f"+{len(ids) - 2} ids")

    if "method" in step:
        parts.append(f"method={step.get('method')}")
    if "property" in step:
        parts.append(f"property={step.get('property')}")
    if "label_contains" in step:
        parts.append(f"label={step.get('label_contains')}")

    if "value" in step:
        try:
            value = _resolve_typed_step_value(step=step, context=context)
        except Exception:  # noqa: BLE001
            value = step.get("value")
        value_text = _shorten(str(value), limit=80)
        parts.append(f"value={value_text}")

    if "args" in step:
        try:
            args = _resolve_call_args(raw_args=step.get("args"), context=context)
            parts.append(f"args={_shorten(str(args), limit=80)}")
        except Exception:  # noqa: BLE001
            parts.append("args=<render-error>")

    if not parts:
        return "-"
    return " ".join(parts)


def _shorten(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logger = logging.getLogger(LOGGER_NAME)
        if logger.handlers:
            logger.exception("[ERROR] %s", exc)
        else:
            print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
