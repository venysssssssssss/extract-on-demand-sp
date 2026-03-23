from __future__ import annotations

import csv
import json
import logging
import os
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


LOGGER_NAME = "sap_gui_export_compat"
_DEFAULT_ENCODINGS: tuple[str, ...] = ("cp1252", "utf-8-sig", "utf-8", "latin-1")
_DEFAULT_DELIMITERS: tuple[str, ...] = ("\t", ";", ",")
_SUPPORTED_STEP_ACTIONS: frozenset[str] = frozenset(
    {
        "call_method",
        "press",
        "select",
        "set_caret",
        "set_focus",
        "set_text",
        "start_transaction",
    }
)


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


def load_config(config_arg: str, script_dir: Path) -> dict[str, Any]:
    config_path = Path(config_arg).expanduser() if str(config_arg).strip() else script_dir / "sap_iw69_batch_config.json"
    return json.loads(config_path.read_text(encoding="utf-8"))


def setup_logger(
    *,
    payload: ExportPayload,
    output_path: Path,
    config: dict[str, Any],
) -> tuple[logging.Logger, Path]:
    logger = logging.getLogger(f"{LOGGER_NAME}.{payload.object_code}.{payload.run_id}")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)

    log_path = output_path.with_suffix(".sap_gui_export.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger, log_path


def resolve_object_config(config: dict[str, Any], payload: ExportPayload) -> dict[str, Any]:
    objects = config.get("objects", {})
    object_config = objects.get(payload.object_code)
    if not isinstance(object_config, dict):
        raise RuntimeError(f"Missing object config for {payload.object_code}.")
    return object_config


def build_context(payload: ExportPayload, output_path: Path) -> dict[str, str]:
    period_start = _parse_iso_date(payload.period_start) or datetime.today().date()
    export_filename_by_object = {
        "CA": f"BASE_AUTOMACAO_CA_{payload.run_id}.txt",
        "RL": f"BASE_AUTOMACAO_RL_{payload.run_id}.txt",
        "WB": f"BASE_AUTOMACAO_WB_{payload.run_id}.txt",
    }
    return {
        "run_id": payload.run_id,
        "object": payload.object_code,
        "sqvi_name": payload.sqvi_name,
        "reference": payload.reference,
        "regional": payload.regional,
        "period_start_dmy": period_start.strftime("%d.%m.%Y"),
        "output_path": str(output_path),
        "output_dir": str(output_path.parent),
        "output_file_name": output_path.name,
        "ca_export_filename": export_filename_by_object["CA"],
        "rl_export_filename": export_filename_by_object["RL"],
        "wb_export_filename": export_filename_by_object["WB"],
    }


def connect_sap_session(config: dict[str, Any], logger: logging.Logger | None = None):
    try:
        import win32com.client  # type: ignore
    except Exception as exc:  # pragma: no cover - windows runtime
        raise RuntimeError("pywin32 is required for SAP GUI scripting.") from exc

    global_cfg = config.get("global", {})
    connection_index = int(global_cfg.get("connection_index", 0))
    session_index = int(global_cfg.get("session_index", 0))
    connection_name = str(global_cfg.get("connection_name", "BAP")).strip() or "BAP"

    sap_gui = win32com.client.GetObject("SAPGUI")
    application = getattr(sap_gui, "GetScriptingEngine", None)
    if application is None:
        raise RuntimeError("SAP GUI Scripting engine unavailable.")
    if callable(application):
        try:
            application = application()
        except Exception:
            pass
    if application is None:
        raise RuntimeError("SAP GUI Scripting engine unavailable.")

    try:
        connection = application.Children(connection_index)
    except Exception:
        if logger is not None:
            logger.info("Opening SAP connection via compat module name=%s", connection_name)
        connection = application.OpenConnection(connection_name, True)
    return _wait_for_session(connection=connection, session_index=session_index, timeout_seconds=45.0)


def run_steps(
    *,
    session: Any,
    steps: list[dict[str, Any]],
    context: dict[str, str],
    default_timeout_seconds: float,
    logger: logging.Logger,
) -> None:
    object_code = str(context.get("object", "")).strip().upper() or "<unknown>"
    _validate_steps(steps=steps, object_code=object_code)
    total_steps = len(steps)
    logger.info(
        "Compat step execution start object=%s total_steps=%s",
        object_code,
        total_steps,
    )
    for index, step in enumerate(steps, start=1):
        action = str(step.get("action", "")).strip().lower()
        label = str(step.get("label", "")).strip()
        logger.info(
            "[STEP %s/%s] object=%s action=%s label=%s ids=%s",
            index,
            total_steps,
            object_code,
            action,
            label or "-",
            ",".join(_resolve_ids(step=step, context=context)[:4]) or "-",
        )
        wait_not_busy(session=session, timeout_seconds=default_timeout_seconds)
        try:
            if action == "start_transaction":
                transaction_code = _render(step.get("value", ""), context)
                session.StartTransaction(transaction_code)
                wait_not_busy(session=session, timeout_seconds=default_timeout_seconds)
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s transaction=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                    transaction_code,
                )
                continue
            item = _resolve_item(session=session, step=step, context=context)
            if item is None:
                logger.info(
                    "[STEP %s/%s] object=%s skipped action=%s label=%s optional=true",
                    index,
                    total_steps,
                    object_code,
                    action,
                    label or "-",
                )
                continue
            if action == "call_method":
                method_name = str(step.get("method", "")).strip()
                args = [_render_arg(arg, context) for arg in step.get("args", [])]
                getattr(item, method_name)(*args)
                wait_not_busy(session=session, timeout_seconds=default_timeout_seconds)
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s method=%s args=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                    method_name,
                    args,
                )
                continue
            if action == "set_text":
                rendered_value = _render(step.get("value", ""), context)
                _set_control_text(item=item, value=rendered_value)
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s value_length=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                    len(rendered_value),
                )
                continue
            if action == "set_focus":
                item.setFocus()
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                )
                continue
            if action == "set_caret":
                item.caretPosition = int(step.get("value", 0))
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s caret=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                    item.caretPosition,
                )
                continue
            if action == "press":
                item.press()
                wait_not_busy(session=session, timeout_seconds=default_timeout_seconds)
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                )
                continue
            if action == "select":
                item.select()
                wait_not_busy(session=session, timeout_seconds=default_timeout_seconds)
                logger.info(
                    "[STEP %s/%s] object=%s done action=%s",
                    index,
                    total_steps,
                    object_code,
                    action,
                )
                continue
            raise RuntimeError(f"Unsupported compat action: {action}")
        except Exception as exc:
            raise RuntimeError(
                f"Compat step failed object={object_code} index={index}/{total_steps} action={action} label={label or '-'}: {exc}"
            ) from exc
    logger.info("Compat step execution finished object=%s total_steps=%s", object_code, total_steps)


def wait_not_busy(session: Any, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if not bool(getattr(session, "Busy", False)):
                return
        except Exception:
            return
        time.sleep(0.1)
    raise TimeoutError(f"SAP session remained busy after {timeout_seconds} seconds.")


def _try_recover_export_after_step_failure(**kwargs) -> int | None:
    try:
        return _materialize_and_persist_metadata(**kwargs)
    except Exception:
        return None


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
    **_: Any,
) -> int:
    source_path = _wait_for_source_txt(
        path_candidates=_build_source_candidates(output_path=output_path, object_config=object_config, context=context),
        timeout_seconds=source_ready_timeout_seconds,
        poll_seconds=source_ready_poll_seconds,
    )
    rows_exported, header_map = _txt_to_csv(
        source_path=source_path,
        destination_path=output_path,
        raw_csv_path=payload.raw_csv_path,
        rejects_path=payload.rejects_path,
        required_fields=required_fields,
    )
    if payload.header_map_path is not None:
        payload.header_map_path.parent.mkdir(parents=True, exist_ok=True)
        with payload.header_map_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["source", "canonical"])
            writer.writeheader()
            for source_name, canonical_name in header_map:
                writer.writerow({"source": source_name, "canonical": canonical_name})
    metadata = {
        "run_id": payload.run_id,
        "object": payload.object_code,
        "reference": payload.reference,
        "rows_exported": rows_exported,
        "output_path": str(output_path),
        "source_txt_path": str(source_path),
        "raw_csv_path": str(payload.raw_csv_path or ""),
        "header_map_path": str(payload.header_map_path or ""),
        "rejects_path": str(payload.rejects_path or ""),
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Compat materialization complete object=%s rows=%s source=%s", payload.object_code, rows_exported, source_path)
    return rows_exported


def _unwind_after_export_with_back(
    *,
    session: Any,
    transaction_code: str,
    timeout_seconds: float,
    logger: logging.Logger | None = None,
    min_back_presses: int = 4,
    max_back_presses: int = 5,
) -> None:
    tx = str(transaction_code).strip().upper()
    if not tx:
        return
    try:
        session.StartTransaction(tx)
        wait_not_busy(session=session, timeout_seconds=min(timeout_seconds, 15.0))
        if logger is not None:
            logger.info("Compat unwind reset session to transaction=%s", tx)
    except Exception as exc:
        if logger is not None:
            logger.warning("Compat unwind failed transaction=%s error=%s", tx, exc)
        raise


def _resolve_item(session: Any, step: dict[str, Any], context: dict[str, str]) -> Any | None:
    item_ids = _resolve_ids(step=step, context=context)
    optional = bool(step.get("optional", False))
    if not item_ids:
        if optional:
            return None
        raise RuntimeError("Step is missing id/ids.")
    errors: list[str] = []
    for item_id in item_ids:
        try:
            return session.findById(item_id)
        except Exception as exc:
            errors.append(f"{item_id}: {exc}")
    if optional:
        return None
    raise RuntimeError("Could not resolve SAP GUI element. " + " | ".join(errors))


def _validate_steps(*, steps: list[dict[str, Any]], object_code: str) -> None:
    if not steps:
        raise RuntimeError(f"Missing SAP GUI steps for object {object_code}.")
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise RuntimeError(
                f"Invalid SAP GUI step at position {index} for object {object_code}: expected object."
            )
        action = str(step.get("action", "")).strip().lower()
        if not action:
            raise RuntimeError(
                f"Invalid SAP GUI step at position {index} for object {object_code}: missing action."
            )
        if action not in _SUPPORTED_STEP_ACTIONS:
            raise RuntimeError(
                f"Unsupported SAP GUI action '{action}' at step {index} for object {object_code}."
            )
        if action == "start_transaction":
            if not str(step.get("value", "")).strip():
                raise RuntimeError(
                    f"Invalid start_transaction step at position {index} for object {object_code}: missing value."
                )
            continue
        if not _resolve_ids(step=step, context={}):
            raise RuntimeError(
                f"Invalid SAP GUI step at position {index} for object {object_code}: missing id/ids."
            )
        if action == "call_method" and not str(step.get("method", "")).strip():
            raise RuntimeError(
                f"Invalid call_method step at position {index} for object {object_code}: missing method."
            )


def _resolve_ids(step: dict[str, Any], context: dict[str, str]) -> list[str]:
    resolved: list[str] = []
    raw_ids = step.get("ids")
    if isinstance(raw_ids, list):
        for raw_id in raw_ids:
            token = _render(str(raw_id), context).strip()
            if token and token not in resolved:
                resolved.append(token)
    raw_id = str(step.get("id", "")).strip()
    if raw_id:
        token = _render(raw_id, context).strip()
        if token and token not in resolved:
            resolved.append(token)
    return resolved


def _render(value: Any, context: dict[str, str]) -> str:
    return str(value).format(**context)


def _render_arg(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, dict):
        raw = value.get("value", "")
        value_type = str(value.get("type", "str")).strip().lower()
        rendered = _render(raw, context) if isinstance(raw, str) else raw
        if value_type == "int":
            return int(rendered)
        if value_type == "float":
            return float(rendered)
        if value_type == "bool":
            return str(rendered).strip().lower() in {"1", "true", "yes", "on"}
        return rendered
    if isinstance(value, str):
        return _render(value, context)
    return value


def _set_control_text(*, item: Any, value: str) -> None:
    try:
        item.setFocus()
    except Exception:
        pass
    item.text = value
    try:
        item.caretPosition = len(value)
    except Exception:
        pass
    try:
        current_value = str(getattr(item, "text", ""))
    except Exception:
        return
    if current_value != value:
        raise RuntimeError(
            f"text write verification failed expected={value!r} actual={current_value!r}"
        )


def _build_source_candidates(
    *,
    output_path: Path,
    object_config: dict[str, Any],
    context: dict[str, str],
) -> list[Path]:
    candidates: list[Path] = []
    copy_from = str(object_config.get("copy_from", "")).strip()
    if copy_from:
        candidates.append(Path(_render(copy_from, context)).expanduser().resolve())
    raw_txt = str(context.get("raw_txt_path", "")).strip()
    if raw_txt:
        candidates.append(Path(raw_txt).expanduser().resolve())
    return candidates


def _wait_for_source_txt(
    *,
    path_candidates: list[Path],
    timeout_seconds: float,
    poll_seconds: float,
) -> Path:
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        for candidate in path_candidates:
            try:
                if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
                    return candidate
            except OSError:
                continue
        time.sleep(max(0.1, poll_seconds))
    raise RuntimeError("Export TXT file was not created in time.")


def _txt_to_csv(
    *,
    source_path: Path,
    destination_path: Path,
    raw_csv_path: Path | None,
    rejects_path: Path | None,
    required_fields: list[str],
) -> tuple[int, list[tuple[str, str]]]:
    encoding, delimiter, rows = _read_delimited_text(source_path)
    if not rows:
        raise RuntimeError(f"Source TXT is empty: {source_path}")
    source_header = [str(item).strip() for item in rows[0]]
    canonical_header = [_normalize_header_name(item) for item in source_header]
    if "nota" not in canonical_header:
        raise RuntimeError(f"Canonical export is missing required column 'nota': header={canonical_header}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    header_map = list(zip(source_header, canonical_header, strict=False))
    data_rows = rows[1:]

    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(canonical_header)
        writer.writerows(data_rows)

    if raw_csv_path is not None:
        raw_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with raw_csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(source_header)
            writer.writerows(data_rows)

    if rejects_path is not None:
        rejects_path.parent.mkdir(parents=True, exist_ok=True)
        with rejects_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["line_number", "reason", "raw_row"])

    return len(data_rows), header_map


def _read_delimited_text(source_path: Path) -> tuple[str, str, list[list[str]]]:
    errors: list[str] = []
    for encoding in _DEFAULT_ENCODINGS:
        try:
            text = source_path.read_text(encoding=encoding)
        except Exception as exc:
            errors.append(f"encoding={encoding}: {exc}")
            continue
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return encoding, "\t", []
        best_delimiter = max(_DEFAULT_DELIMITERS, key=lambda token: lines[0].count(token))
        rows = list(csv.reader(lines, delimiter=best_delimiter))
        return encoding, best_delimiter, rows
    raise RuntimeError("Could not decode TXT export. " + " | ".join(errors[:4]))


def _normalize_header_name(value: str) -> str:
    token = _strip_accents(value).strip().lower()
    token = "_".join(part for part in "".join(ch if ch.isalnum() else " " for ch in token).split())
    special_cases = {
        "nota": "nota",
        "nota_de_leitura": "nota",
        "nota_leitura": "nota",
    }
    return special_cases.get(token, token or "unnamed")


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _wait_for_session(connection: Any, session_index: int, timeout_seconds: float):
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            return connection.Children(session_index)
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"Could not access SAP session index {session_index}.")


def _parse_iso_date(value: str | None):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None
