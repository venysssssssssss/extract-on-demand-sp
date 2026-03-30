from __future__ import annotations

import argparse
import csv
import json
import os
import re
import threading
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import load_export_config
from .runtime_logging import configure_run_logger
from .sap_helpers import resolve_first_existing_with_id, set_text, wait_not_busy

_DW_MAIN_WINDOW_ID = "wnd[0]"
_DW_OKCODE_ID = "wnd[0]/tbar[0]/okcd"
_DW_QMNUM_FIELD_IDS = [
    "wnd[0]/usr/ctxtQMNUM-LOW",
    "wnd[0]/usr/ctxtRIWO00-QMNUM",
]
_DW_EXECUTE_IDS = [
    "wnd[0]/tbar[1]/btn[8]",
    "wnd[0]/tbar[1]/btn[5]",
]
_DW_BACK_BUTTON_ID = "wnd[0]/tbar[0]/btn[3]"
_DW_TAB02_ID = r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02"
_DW_TEXT_TABLE_ID = (
    r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02/ssubSUB_GROUP_10:SAPLIQS0:7235/"
    r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_2:SAPLIQS0:7710/tblSAPLIQS0TEXT"
)
_DW_TEXT_LINE_TEMPLATE = _DW_TEXT_TABLE_ID + r"/txtLTXTTAB2-TLINE[0,{row}]"
_DW_REQUIRED_ID_HEADER = "ID Reclamação"
_DW_OUTPUT_HEADER = "OBSERVAÇÃO"
_DW_SESSION_ID_PATTERN = re.compile(r"/app/con\[(?P<connection>\d+)\]/ses\[(?P<session>\d+)\]", re.IGNORECASE)
_DW_MAX_CONSECUTIVE_FAILURES = 10
_DW_FAST_FAIL_THRESHOLD_SECONDS = 0.5
_DW_PROGRESS_LOG_INTERVAL = 50
_DW_SAP_COM_LOCK = threading.RLock()


def _normalize_header(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip())
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return "".join(ch.lower() for ch in ascii_text if ch.isalnum())


def _read_text(item: Any) -> str:
    for attr_name in ("Text", "text"):
        try:
            return str(getattr(item, attr_name, "") or "")
        except Exception:
            continue
    return ""


def _set_caret(item: Any, value: int) -> None:
    try:
        item.caretPosition = value
    except Exception:
        pass


def _set_scroll_position(scrollbar: Any, value: int) -> None:
    last_error: Exception | None = None
    for attr_name in ("position", "Position"):
        try:
            setattr(scrollbar, attr_name, value)
            return
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Could not adjust SAP text-table scrollbar position: {last_error}")


def _get_int_attr(target: Any, *names: str) -> int | None:
    for attr_name in names:
        try:
            value = getattr(target, attr_name)
        except Exception:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def _read_session_id(session: Any) -> str:
    for attr_name in ("Id", "id"):
        try:
            value = str(getattr(session, attr_name, "") or "").strip()
        except Exception:
            continue
        if value:
            return value
    return ""


def _session_locator_from_session(session: Any) -> "DwSessionLocator":
    session_id = _read_session_id(session)
    match = _DW_SESSION_ID_PATTERN.search(session_id)
    if match is None:
        raise RuntimeError(f"Could not determine SAP session locator from session id: {session_id or '<empty>'}")
    return DwSessionLocator(
        connection_index=int(match.group("connection")),
        session_index=int(match.group("session")),
    )


def _normalize_transaction_code(value: str) -> str:
    code = str(value or "").strip()
    if not code:
        raise RuntimeError("DW transaction code is empty.")
    if code.startswith("/"):
        return code
    return f"/n{code}"


def _is_session_disconnected_error(exc: Exception) -> bool:
    text = str(exc).casefold()
    return (
        "desconectado de seus clientes" in text
        or "called object was disconnected" in text
        or "falha na chamada de procedimento remoto" in text
        or "remote procedure call failed" in text
        or "rpc server is unavailable" in text
        or "o servidor rpc nao esta disponivel" in text
        or "o servidor rpc não está disponível" in text
    )


@dataclass(frozen=True)
class DwSettings:
    demandante: str
    input_path: Path
    input_encoding: str
    delimiter: str
    id_column: str
    output_column: str
    transaction_code: str
    session_count: int
    max_rows_per_run: int
    wait_timeout_seconds: float
    post_login_wait_seconds: float
    parallel_mode: bool
    circuit_breaker_fast_fail_threshold: int
    circuit_breaker_slow_fail_threshold: int
    per_step_timeout_transaction_seconds: float
    per_step_timeout_query_seconds: float
    session_recovery_mode: str


@dataclass(frozen=True)
class DwWorkItem:
    row_index: int
    complaint_id: str


@dataclass(frozen=True)
class DwItemResult:
    row_index: int
    complaint_id: str
    observacao: str
    worker_index: int
    elapsed_seconds: float


@dataclass(frozen=True)
class DwSessionLocator:
    connection_index: int
    session_index: int


@dataclass
class DwWorkerState:
    worker_index: int
    session_id: str
    status: str = "idle"
    items_total: int = 0
    items_processed: int = 0
    items_ok: int = 0
    items_failed: int = 0
    last_ok_complaint_id: str = ""
    current_complaint_id: str = ""
    consecutive_failures: int = 0
    elapsed_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DwManifest:
    run_id: str
    demandante: str
    input_path: str
    processed_rows: int
    successful_rows: int
    skipped_rows: int
    session_count: int
    status: str
    log_path: str
    manifest_path: str
    failed_rows: list[dict[str, Any]] = field(default_factory=list)
    worker_states: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_dw_settings(
    *,
    config: dict[str, Any],
    config_path: Path,
    demandante: str,
) -> DwSettings:
    dw_cfg = config.get("dw", {})
    demandante_name = str(demandante or "").strip().upper() or "DW"
    if not isinstance(dw_cfg, dict):
        raise RuntimeError("Missing dw section in config JSON.")
    demandantes_cfg = dw_cfg.get("demandantes", {})
    if not isinstance(demandantes_cfg, dict):
        raise RuntimeError("Invalid dw.demandantes section in config JSON.")
    profile = demandantes_cfg.get(demandante_name)
    if not isinstance(profile, dict):
        raise RuntimeError(f"Missing DW config for demandante={demandante_name}.")

    input_path = Path(str(profile.get("input_path", "")).strip())
    if not input_path.is_absolute():
        input_path = (config_path.expanduser().resolve().parent / input_path).resolve()
    if not input_path.exists():
        raise RuntimeError(f"DW input CSV not found: {input_path}")

    return DwSettings(
        demandante=demandante_name,
        input_path=input_path,
        input_encoding=str(profile.get("input_encoding", "cp1252")).strip() or "cp1252",
        delimiter=str(profile.get("delimiter", "\t")),
        id_column=str(profile.get("id_column", _DW_REQUIRED_ID_HEADER)).strip() or _DW_REQUIRED_ID_HEADER,
        output_column=str(profile.get("output_column", _DW_OUTPUT_HEADER)).strip() or _DW_OUTPUT_HEADER,
        transaction_code=str(profile.get("transaction_code", "IW53")).strip() or "IW53",
        session_count=max(1, int(profile.get("session_count", 3) or 3)),
        max_rows_per_run=max(1, int(profile.get("max_rows_per_run", 3000) or 3000)),
        wait_timeout_seconds=float(
            profile.get("wait_timeout_seconds", config.get("global", {}).get("wait_timeout_seconds", 120.0))
        ),
        post_login_wait_seconds=float(profile.get("post_login_wait_seconds", 6.0)),
        parallel_mode=bool(profile.get("parallel_mode", True)),
        circuit_breaker_fast_fail_threshold=max(
            1,
            int(profile.get("circuit_breaker_fast_fail_threshold", _DW_MAX_CONSECUTIVE_FAILURES) or 10),
        ),
        circuit_breaker_slow_fail_threshold=max(
            1,
            int(profile.get("circuit_breaker_slow_fail_threshold", _DW_MAX_CONSECUTIVE_FAILURES * 3) or 30),
        ),
        per_step_timeout_transaction_seconds=float(
            profile.get("per_step_timeout_transaction_seconds", 30.0)
        ),
        per_step_timeout_query_seconds=float(
            profile.get("per_step_timeout_query_seconds", 180.0)
        ),
        session_recovery_mode=str(profile.get("session_recovery_mode", "soft")).strip().lower() or "soft",
    )


def load_dw_work_items(
    *,
    input_path: Path,
    encoding: str,
    delimiter: str,
    id_column: str,
    output_column: str,
    max_rows: int | None = None,
) -> tuple[list[str], list[list[str]], int, list[DwWorkItem], int]:
    with input_path.open("r", encoding=encoding, newline="") as handle:
        rows = list(csv.reader(handle, delimiter=delimiter))

    if not rows:
        raise RuntimeError(f"DW input CSV is empty: {input_path}")

    header = list(rows[0])
    data_rows = [list(row) for row in rows[1:]]
    header_index_by_key = {
        _normalize_header(value): index for index, value in enumerate(header) if str(value or "").strip()
    }
    id_column_index = header_index_by_key.get(_normalize_header(id_column))
    if id_column_index is None:
        raise RuntimeError(f"DW input CSV is missing required column: {id_column}")

    output_column_index = header_index_by_key.get(_normalize_header(output_column))
    if output_column_index is None:
        output_column_index = len(header)
        header.append(output_column)
        for row in data_rows:
            row.append("")

    items: list[DwWorkItem] = []
    skipped_rows = 0
    expected_len = len(header)
    for row_offset, row in enumerate(data_rows, start=2):
        while len(row) < expected_len:
            row.append("")
        complaint_id = str(row[id_column_index] or "").strip()
        observacao = str(row[output_column_index] or "").strip()
        if not complaint_id:
            continue
        if observacao:
            skipped_rows += 1
            continue
        items.append(DwWorkItem(row_index=row_offset, complaint_id=complaint_id))
        if max_rows is not None and max_rows > 0 and len(items) >= max_rows:
            break

    return header, data_rows, output_column_index, items, skipped_rows


def write_dw_csv(
    *,
    input_path: Path,
    encoding: str,
    delimiter: str,
    header: list[str],
    data_rows: list[list[str]],
) -> None:
    temp_path = input_path.with_name(f"{input_path.name}.tmp")
    with temp_path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.writer(handle, delimiter=delimiter, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(data_rows)
    os.replace(temp_path, input_path)


def write_dw_debug_csv(
    *,
    output_path: Path,
    rows: list[DwItemResult],
) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=",", lineterminator="\n")
        writer.writerow(["worker", "complaint_id", "observacao"])
        for row in rows:
            writer.writerow([row.worker_index, row.complaint_id, row.observacao])
    os.replace(temp_path, output_path)


def split_work_items_evenly(items: list[DwWorkItem], session_count: int) -> list[list[DwWorkItem]]:
    if session_count <= 0:
        raise ValueError("session_count must be greater than zero.")
    groups = [[] for _ in range(session_count)]
    for index, item in enumerate(items):
        groups[index % session_count].append(item)
    return [group for group in groups if group]


def _connection_from_session(session: Any) -> Any:
    for attr_name in ("Parent", "parent"):
        try:
            parent = getattr(session, attr_name)
        except Exception:
            continue
        if parent is not None:
            return parent
    raise RuntimeError("Could not resolve SAP connection from base session.")


def _list_connection_sessions(connection: Any, *, limit: int = 12) -> list[Any]:
    # Prefer Sessions collection (non-blocking) over Children (blocks when any session is busy).
    # See: SAP GUI Scripting API — "Collection vs. Children, the Busy Difference".
    sessions_coll = getattr(connection, "Sessions", None)
    if sessions_coll is not None:
        try:
            count = int(getattr(sessions_coll, "Count", 0))
            return [sessions_coll(i) for i in range(min(count, limit))]
        except Exception:
            pass
    sessions: list[Any] = []
    for index in range(limit):
        try:
            sessions.append(connection.Children(index))
        except Exception:
            break
    return sessions


def _session_identity(session: Any) -> str:
    session_id = _read_session_id(session)
    if session_id:
        return session_id
    return f"<session-object:{id(session)}>"


def _read_active_window(session: Any) -> Any | None:
    for attr_name in ("ActiveWindow", "activeWindow"):
        try:
            window = getattr(session, attr_name)
        except Exception:
            continue
        if window is not None:
            return window
    return None


def _read_window_type(window: Any) -> str:
    if window is None:
        return ""
    for attr_name in ("Type", "type"):
        try:
            value = str(getattr(window, attr_name, "") or "").strip()
        except Exception:
            continue
        if value:
            return value
    return ""


def _dismiss_popup_if_present(session: Any, logger: Any) -> bool:
    try:
        popup = session.findById("wnd[1]")
    except Exception:
        return False

    popup_type = _read_window_type(popup) or "<unknown>"
    popup_text = _read_text(popup)
    logger.warning(
        "DW popup detected type=%s text=%s session_id=%s",
        popup_type,
        popup_text or "<empty>",
        _read_session_id(session) or "<empty>",
    )

    try:
        popup.sendVKey(0)
        logger.info("DW popup dismissed via Enter type=%s", popup_type)
        return True
    except Exception:
        pass

    for item_id in ("wnd[1]/tbar[0]/btn[0]", "wnd[1]/usr/btnSPOP-OPTION1", "wnd[1]/usr/btnBUTTON_1"):
        try:
            session.findById(item_id).press()
            logger.info("DW popup dismissed via button id=%s", item_id)
            return True
        except Exception:
            continue
    logger.warning("DW popup could not be dismissed automatically type=%s", popup_type)
    return False


def _visible_controls(*, session: Any, limit: int = 40) -> list[str]:
    try:
        import importlib

        compat = importlib.import_module("sap_gui_export_compat")
        return list(compat._collect_visible_control_ids(session=session, limit=limit))
    except Exception:
        return []


def _read_status_bar_text(session: Any) -> str:
    for item_id in ("wnd[0]/sbar/pane[0]", "wnd[0]/sbar"):
        try:
            return _read_text(session.findById(item_id)).strip()
        except Exception:
            continue
    return ""


def _log_dw_snapshot(*, session: Any, logger: Any, phase: str, worker_index: int, item: DwWorkItem | None = None) -> None:
    active_window = _read_active_window(session)
    logger.info(
        "DW snapshot phase=%s worker=%s row=%s complaint_id=%s session_id=%s active_window_type=%s status_bar=%s visible_controls=%s",
        phase,
        worker_index,
        item.row_index if item is not None else "<empty>",
        item.complaint_id if item is not None else "<empty>",
        _read_session_id(session) or "<empty>",
        _read_window_type(active_window) or "<empty>",
        _read_status_bar_text(session) or "<empty>",
        _visible_controls(session=session, limit=20),
    )


def _wait_for_control(
    *,
    session: Any,
    ids: list[str],
    timeout_seconds: float,
    logger: Any,
    description: str,
    worker_index: int,
    item: DwWorkItem,
) -> Any:
    deadline = time.monotonic() + max(1.0, timeout_seconds)
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _, control = resolve_first_existing_with_id(session, ids)
            return control
        except Exception as exc:
            last_error = exc
            if _is_session_disconnected_error(exc):
                break
            _dismiss_popup_if_present(session, logger)
            time.sleep(0.2)
    _log_dw_snapshot(session=session, logger=logger, phase=f"wait_control_timeout.{description}", worker_index=worker_index, item=item)
    raise RuntimeError(
        f"DW could not resolve control for {description} worker={worker_index} row={item.row_index} "
        f"complaint_id={item.complaint_id}: {last_error}"
    )


def _wait_session_ready(session: Any, *, timeout_seconds: float, logger: Any) -> None:
    """Block until the session UI tree is ready for interaction."""
    deadline = time.monotonic() + min(timeout_seconds, 30.0)
    while time.monotonic() < deadline:
        try:
            if getattr(session, "Busy", False):
                time.sleep(0.3)
                continue
            main_window = session.findById(_DW_MAIN_WINDOW_ID)
            okcd = session.findById(_DW_OKCODE_ID)
            active_window = _read_active_window(session)
            active_window_type = _read_window_type(active_window)
            if active_window is not None and active_window_type != "GuiModalWindow" and main_window and okcd:
                return
        except Exception:
            pass
        time.sleep(0.3)
    logger.warning(
        "DW session readiness timed out session_id=%s active_window_type=%s — proceeding anyway",
        _read_session_id(session),
        _read_window_type(_read_active_window(session)) or "<empty>",
    )


def ensure_sap_sessions(
    *,
    base_session: Any,
    session_count: int,
    logger: Any,
    wait_timeout_seconds: float,
) -> list[Any]:
    if session_count <= 1:
        return [base_session]
    connection = _connection_from_session(base_session)
    sessions = [base_session]
    known_identities = {_session_identity(base_session)}
    main_window = base_session.findById(_DW_MAIN_WINDOW_ID)
    try:
        main_window.maximize()
    except Exception:
        pass
    logger.info("DW registered SAP session slot=1 session_id=%s", _read_session_id(base_session) or "<empty>")
    while len(sessions) < session_count:
        next_slot = len(sessions) + 1
        logger.info("DW opening additional SAP session slot=%s", next_slot)
        try:
            main_window.sendVKey(0)
            wait_not_busy(base_session, timeout_seconds=wait_timeout_seconds)
        except Exception:
            pass
        create_session = getattr(base_session, "CreateSession", None) or getattr(base_session, "createSession", None)
        if callable(create_session):
            create_session()
        else:
            set_text(base_session, _DW_OKCODE_ID, "/o")
            base_session.findById(_DW_MAIN_WINDOW_ID).sendVKey(0)
        deadline = time.monotonic() + max(5.0, wait_timeout_seconds)
        new_session: Any | None = None
        while time.monotonic() < deadline:
            current_sessions = _list_connection_sessions(connection)
            for candidate in current_sessions:
                identity = _session_identity(candidate)
                if identity in known_identities:
                    continue
                new_session = candidate
                known_identities.add(identity)
                break
            if new_session is not None:
                _wait_session_ready(new_session, timeout_seconds=wait_timeout_seconds, logger=logger)
                sessions.append(new_session)
                try:
                    new_session.findById(_DW_MAIN_WINDOW_ID).maximize()
                except Exception:
                    pass
                logger.info(
                    "DW registered SAP session slot=%s session_id=%s",
                    len(sessions),
                    _read_session_id(new_session) or "<empty>",
                )
                break
            time.sleep(0.25)
        else:
            raise RuntimeError(f"Could not open SAP session slot={next_slot} for DW flow.")
    return sessions


def prepare_dw_sessions(
    *,
    base_session: Any,
    settings: DwSettings,
    logger: Any,
) -> list[Any]:
    if settings.post_login_wait_seconds > 0:
        logger.info(
            "DW waiting after authenticated login seconds=%.1f before opening SAP sessions",
            settings.post_login_wait_seconds,
        )
        time.sleep(settings.post_login_wait_seconds)
    return ensure_sap_sessions(
        base_session=base_session,
        session_count=settings.session_count,
        logger=logger,
        wait_timeout_seconds=settings.wait_timeout_seconds,
    )


def _co_initialize() -> tuple[Any | None, bool]:
    try:
        import pythoncom  # type: ignore
    except Exception:
        return None, False
    pythoncom.CoInitialize()
    return pythoncom, True


def _marshal_sap_application(count: int, logger: Any) -> list[Any]:
    """Marshal the SAP scripting engine N times for worker threads.

    Must be called from the main thread where the SAP GUI ROT entry is visible.
    Each returned stream can be unmarshaled exactly once in a worker thread.
    """
    import pythoncom  # type: ignore
    import win32com.client  # type: ignore

    logger.info("DW marshal: resolving SAP GUI scripting engine via GetObject('SAPGUI') count=%s", count)
    sap_gui = win32com.client.GetObject("SAPGUI")
    application = sap_gui.GetScriptingEngine
    logger.info("DW marshal: scripting engine acquired, creating %s interthread streams", count)
    streams: list[Any] = []
    for index in range(count):
        stream = pythoncom.CoMarshalInterThreadInterfaceInStream(
            pythoncom.IID_IDispatch,
            application._oleobj_,
        )
        streams.append(stream)
        logger.info("DW marshal: stream %s/%s created", index + 1, count)
    return streams


def _unmarshal_sap_application(stream: Any, logger: Any, worker_index: int) -> Any:
    """Unmarshal a SAP scripting engine stream in a worker thread.

    Must be called after pythoncom.CoInitialize() in the worker thread.
    The stream is consumed and cannot be reused.
    """
    import pythoncom  # type: ignore
    import win32com.client  # type: ignore

    logger.info("DW unmarshal: worker=%s releasing stream into thread-local IDispatch", worker_index)
    dispatch = pythoncom.CoGetInterfaceAndReleaseStream(
        stream,
        pythoncom.IID_IDispatch,
    )
    application = win32com.client.Dispatch(dispatch)
    logger.info("DW unmarshal: worker=%s SAP application proxy ready", worker_index)
    return application


def _reattach_dw_session(
    *,
    locator: DwSessionLocator,
    logger: Any,
    application: Any | None = None,
) -> Any:
    if application is None:
        try:
            import win32com.client  # type: ignore
        except Exception as exc:  # pragma: no cover - Windows only runtime path
            raise RuntimeError("pywin32 is required to reattach SAP sessions for DW flow.") from exc
        application = win32com.client.GetObject("SAPGUI").GetScriptingEngine

    connection = application.Children(locator.connection_index)
    target_id = f"/app/con[{locator.connection_index}]/ses[{locator.session_index}]"

    # Search by stable SAP session ID using Sessions collection (non-blocking).
    sessions_coll = getattr(connection, "Sessions", None)
    if sessions_coll is not None:
        try:
            count = int(getattr(sessions_coll, "Count", 0))
            for i in range(count):
                candidate = sessions_coll(i)
                if _read_session_id(candidate) == target_id:
                    logger.info(
                        "DW reattached session by id=%s (Sessions index=%s)",
                        target_id, i,
                    )
                    return candidate
        except Exception:
            pass

    # Fallback: direct positional access via Children.
    session = connection.Children(locator.session_index)
    logger.info(
        "DW reattached SAP session (Children fallback) con=%s ses=%s session_id=%s",
        locator.connection_index, locator.session_index,
        _read_session_id(session) or "<empty>",
    )
    return session


def _merge_text_lines(collected: list[str], block: list[str]) -> list[str]:
    if not block:
        return collected
    overlap = 0
    max_overlap = min(len(collected), len(block))
    for candidate in range(max_overlap, 0, -1):
        if collected[-candidate:] == block[:candidate]:
            overlap = candidate
            break
    return collected + block[overlap:]


def _read_visible_text_block(session: Any) -> list[str]:
    lines: list[str] = []
    for row_index in range(0, 8):
        try:
            item = session.findById(_DW_TEXT_LINE_TEMPLATE.format(row=row_index))
        except Exception:
            if row_index >= 4:
                break
            continue
        value = _read_text(item).rstrip()
        if value:
            lines.append(value)
    return lines


def _get_text_table_scrollbar(session: Any) -> Any:
    table = session.findById(_DW_TEXT_TABLE_ID)
    return getattr(table, "verticalScrollbar")


def extract_observacao_text(*, session: Any, wait_timeout_seconds: float) -> str:
    collected: list[str] = []
    previous_block: list[str] = []
    stable_hits = 0

    for position in range(0, 5):
        if position > 0:
            try:
                scrollbar = _get_text_table_scrollbar(session)
                _set_scroll_position(scrollbar, position)
                wait_not_busy(session, timeout_seconds=wait_timeout_seconds)
            except Exception:
                if position >= 4:
                    break
        block = _read_visible_text_block(session)
        if block:
            collected = _merge_text_lines(collected, block)
        if block == previous_block:
            stable_hits += 1
        else:
            stable_hits = 0
        previous_block = block
        if position >= 4 and stable_hits >= 1:
            break

    return "\n".join(collected).strip()


def extract_observacao_text_with_logging(
    *,
    session: Any,
    wait_timeout_seconds: float,
    logger: Any,
    worker_index: int,
    item: DwWorkItem,
) -> str:
    try:
        _get_text_table_scrollbar(session)
    except Exception as exc:
        logger.error(
            "DW extract setup failed worker=%s row=%s complaint_id=%s error=%s",
            worker_index,
            item.row_index,
            item.complaint_id,
            exc,
        )
        raise

    collected: list[str] = []
    previous_block: list[str] = []
    stable_hits = 0

    for position in range(0, 5):
        if position > 0:
            try:
                scrollbar = _get_text_table_scrollbar(session)
                logger.info(
                    "DW extract scroll worker=%s row=%s complaint_id=%s position=%s",
                    worker_index,
                    item.row_index,
                    item.complaint_id,
                    position,
                )
                _set_scroll_position(scrollbar, position)
                wait_not_busy(session, timeout_seconds=wait_timeout_seconds)
            except Exception as exc:
                logger.error(
                    "DW extract scroll failed worker=%s row=%s complaint_id=%s position=%s error=%s",
                    worker_index,
                    item.row_index,
                    item.complaint_id,
                    position,
                    exc,
                )
                if position >= 4:
                    break
        try:
            block = _read_visible_text_block(session)
        except Exception as exc:
            logger.error(
                "DW extract read block failed worker=%s row=%s complaint_id=%s position=%s error=%s",
                worker_index,
                item.row_index,
                item.complaint_id,
                position,
                exc,
            )
            raise
        logger.info(
            "DW extract block worker=%s row=%s complaint_id=%s position=%s lines=%s",
            worker_index,
            item.row_index,
            item.complaint_id,
            position,
            len(block),
        )
        if block:
            collected = _merge_text_lines(collected, block)
        if block == previous_block:
            stable_hits += 1
        else:
            stable_hits = 0
        previous_block = block
        if position >= 4 and stable_hits >= 1:
            break

    return "\n".join(collected).strip()


def _reset_session_state_soft(*, session: Any, settings: DwSettings, logger: Any) -> None:
    _dismiss_popup_if_present(session, logger)
    main_window = session.findById(_DW_MAIN_WINDOW_ID)
    set_text(session, _DW_OKCODE_ID, "/n")
    logger.info("DW soft session reset transaction=/n session_id=%s", _read_session_id(session) or "<empty>")
    main_window.sendVKey(0)
    wait_not_busy(session, timeout_seconds=settings.per_step_timeout_transaction_seconds)
    _dismiss_popup_if_present(session, logger)


def _open_dw_selection_screen(*, session: Any, settings: DwSettings, logger: Any, worker_index: int, item: DwWorkItem) -> None:
    transaction_code = _normalize_transaction_code(settings.transaction_code)
    _dismiss_popup_if_present(session, logger)
    main_window = session.findById(_DW_MAIN_WINDOW_ID)
    set_text(session, _DW_OKCODE_ID, transaction_code)
    logger.info(
        "DW entering transaction worker=%s row=%s complaint_id=%s transaction=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
        transaction_code,
    )
    try:
        main_window.sendVKey(0)
        wait_not_busy(session, timeout_seconds=settings.per_step_timeout_transaction_seconds)
        _dismiss_popup_if_present(session, logger)
        _wait_for_control(
            session=session,
            ids=_DW_QMNUM_FIELD_IDS,
            timeout_seconds=settings.per_step_timeout_transaction_seconds,
            logger=logger,
            description="iw53.selection_screen",
            worker_index=worker_index,
            item=item,
        )
        return
    except Exception as exc:
        logger.warning(
            "DW transaction entry failed on first attempt worker=%s row=%s complaint_id=%s transaction=%s error=%s",
            worker_index,
            item.row_index,
            item.complaint_id,
            transaction_code,
            exc,
        )
    _reset_session_state_soft(session=session, settings=settings, logger=logger)
    set_text(session, _DW_OKCODE_ID, transaction_code)
    main_window.sendVKey(0)
    wait_not_busy(session, timeout_seconds=settings.per_step_timeout_transaction_seconds)
    _dismiss_popup_if_present(session, logger)
    _wait_for_control(
        session=session,
        ids=_DW_QMNUM_FIELD_IDS,
        timeout_seconds=settings.per_step_timeout_transaction_seconds,
        logger=logger,
        description="iw53.selection_screen.retry",
        worker_index=worker_index,
        item=item,
    )


def _ensure_dw_selection_screen(
    *,
    session: Any,
    settings: DwSettings,
    logger: Any,
    worker_index: int,
    item: DwWorkItem,
    allow_navigation: bool,
) -> Any:
    try:
        _, control = resolve_first_existing_with_id(session, _DW_QMNUM_FIELD_IDS)
        return control
    except Exception:
        if not allow_navigation:
            return _wait_for_control(
                session=session,
                ids=_DW_QMNUM_FIELD_IDS,
                timeout_seconds=settings.per_step_timeout_transaction_seconds,
                logger=logger,
                description="iw53.selection_screen",
                worker_index=worker_index,
                item=item,
            )
    _open_dw_selection_screen(
        session=session,
        settings=settings,
        logger=logger,
        worker_index=worker_index,
        item=item,
    )
    return _wait_for_control(
        session=session,
        ids=_DW_QMNUM_FIELD_IDS,
        timeout_seconds=settings.per_step_timeout_transaction_seconds,
        logger=logger,
        description="iw53.selection_screen.post_navigation",
        worker_index=worker_index,
        item=item,
    )


def _refresh_dw_session(
    *,
    session: Any,
    session_locator: DwSessionLocator | None,
    application: Any | None,
    settings: DwSettings,
    logger: Any,
    worker_index: int,
    item: DwWorkItem,
    reason: str,
) -> Any:
    if session_locator is None:
        return session
    refreshed_session = _reattach_dw_session(
        locator=session_locator,
        logger=logger,
        application=application,
    )
    _wait_session_ready(
        refreshed_session,
        timeout_seconds=settings.wait_timeout_seconds,
        logger=logger,
    )
    logger.info(
        "DW session refreshed worker=%s row=%s complaint_id=%s reason=%s session_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
        reason,
        _read_session_id(refreshed_session) or "<empty>",
    )
    return refreshed_session


def execute_dw_item(
    *,
    session: Any,
    session_locator: DwSessionLocator | None,
    application: Any | None,
    item: DwWorkItem,
    settings: DwSettings,
    logger: Any,
    worker_index: int,
) -> str:
    logger.info(
        "DW step selection_screen_ready worker=%s row=%s complaint_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
    )
    complaint_item = _ensure_dw_selection_screen(
        session=session,
        settings=settings,
        logger=logger,
        worker_index=worker_index,
        item=item,
        allow_navigation=False,
    )
    try:
        complaint_item.setFocus()
    except Exception:
        pass
    logger.info(
        "DW step fill_qmnum worker=%s row=%s complaint_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
    )
    complaint_item.text = item.complaint_id
    _set_caret(complaint_item, len(item.complaint_id))
    logger.info(
        "DW step execute_selection worker=%s row=%s complaint_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
    )
    _wait_for_control(
        session=session,
        ids=_DW_EXECUTE_IDS,
        timeout_seconds=settings.per_step_timeout_transaction_seconds,
        logger=logger,
        description="iw53.execute_button",
        worker_index=worker_index,
        item=item,
    ).press()
    wait_not_busy(session, timeout_seconds=settings.per_step_timeout_query_seconds)
    _dismiss_popup_if_present(session, logger)

    logger.info(
        "DW step open_tab02 worker=%s row=%s complaint_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
    )
    _wait_for_control(
        session=session,
        ids=[_DW_TAB02_ID],
        timeout_seconds=settings.per_step_timeout_query_seconds,
        logger=logger,
        description="iw53.tab02",
        worker_index=worker_index,
        item=item,
    ).select()
    wait_not_busy(session, timeout_seconds=settings.per_step_timeout_query_seconds)
    _dismiss_popup_if_present(session, logger)
    _wait_for_control(
        session=session,
        ids=[_DW_TEXT_TABLE_ID],
        timeout_seconds=settings.per_step_timeout_query_seconds,
        logger=logger,
        description="iw53.text_table",
        worker_index=worker_index,
        item=item,
    )
    logger.info(
        "DW step extract_observacao worker=%s row=%s complaint_id=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
    )
    observacao = extract_observacao_text_with_logging(
        session=session,
        wait_timeout_seconds=settings.per_step_timeout_query_seconds,
        logger=logger,
        worker_index=worker_index,
        item=item,
    )

    for back_press in range(1, 3):
        session = _refresh_dw_session(
            session=session,
            session_locator=session_locator,
            application=application,
            settings=settings,
            logger=logger,
            worker_index=worker_index,
            item=item,
            reason=f"before_back_{back_press}",
        )
        logger.info(
            "DW step back worker=%s row=%s complaint_id=%s press=%s/2",
            worker_index,
            item.row_index,
            item.complaint_id,
            back_press,
        )
        _wait_for_control(
            session=session,
            ids=[_DW_BACK_BUTTON_ID],
            timeout_seconds=settings.per_step_timeout_transaction_seconds,
            logger=logger,
            description=f"iw53.back_button.{back_press}",
            worker_index=worker_index,
            item=item,
        ).press()
        wait_not_busy(session, timeout_seconds=settings.per_step_timeout_transaction_seconds)
        _dismiss_popup_if_present(session, logger)

    logger.info(
        "DW item completed worker=%s row=%s complaint_id=%s observation_length=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
        len(observacao),
    )
    return observacao


def _process_dw_item_with_retry(
    *,
    worker_index: int,
    session: Any,
    session_locator: DwSessionLocator,
    item: DwWorkItem,
    settings: DwSettings,
    logger: Any,
    application: Any | None = None,
) -> tuple[DwItemResult | None, dict[str, Any] | None, Any]:
    """Process a single DW item. Returns (result, failure_dict, current_session).

    Uses the cached *session* on the first attempt; only reattaches on retry.
    The third return value is the (possibly refreshed) session reference.
    """
    last_error: Exception | None = None
    current_session = session
    for attempt in range(1, 3):
        started_at = time.perf_counter()
        try:
            with _DW_SAP_COM_LOCK:
                if attempt > 1:
                    logger.info(
                        "DW reattach for retry worker=%s row=%s attempt=%s con=%s ses=%s",
                        worker_index, item.row_index, attempt,
                        session_locator.connection_index, session_locator.session_index,
                    )
                    current_session = _reattach_dw_session(
                        locator=session_locator, logger=logger, application=application,
                    )
                    if settings.session_recovery_mode == "soft":
                        _reset_session_state_soft(session=current_session, settings=settings, logger=logger)
                    _ensure_dw_selection_screen(
                        session=current_session,
                        settings=settings,
                        logger=logger,
                        worker_index=worker_index,
                        item=item,
                        allow_navigation=True,
                    )
                observacao = execute_dw_item(
                    session=current_session,
                    session_locator=session_locator,
                    application=application,
                    settings=settings,
                    logger=logger,
                    worker_index=worker_index,
                    item=item,
                )
        except Exception as exc:
            elapsed_seconds = time.perf_counter() - started_at
            last_error = exc
            is_fast_fail = elapsed_seconds < _DW_FAST_FAIL_THRESHOLD_SECONDS
            if attempt < 2 and _is_session_disconnected_error(exc):
                logger.warning(
                    "DW session disconnected; retrying worker=%s row=%s complaint_id=%s attempt=%s elapsed_s=%.2f",
                    worker_index, item.row_index, item.complaint_id, attempt, elapsed_seconds,
                )
                continue
            logger.error(
                "DW item FAILED worker=%s row=%s complaint_id=%s elapsed_s=%.2f attempt=%s fast_fail=%s error=%s",
                worker_index, item.row_index, item.complaint_id,
                elapsed_seconds, attempt, is_fast_fail, exc,
            )
            return None, {
                "worker_index": worker_index,
                "row_index": item.row_index,
                "complaint_id": item.complaint_id,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "attempt": attempt,
                "fast_fail": is_fast_fail,
                "error": str(exc),
            }, current_session
        elapsed_seconds = time.perf_counter() - started_at
        logger.info(
            "DW item OK worker=%s row=%s complaint_id=%s elapsed_s=%.2f obs_len=%s",
            worker_index, item.row_index, item.complaint_id,
            elapsed_seconds, len(observacao),
        )
        return DwItemResult(
            row_index=item.row_index,
            complaint_id=item.complaint_id,
            observacao=observacao,
            worker_index=worker_index,
            elapsed_seconds=round(elapsed_seconds, 3),
        ), None, current_session
    raise RuntimeError(f"Unexpected DW retry fallthrough for row={item.row_index}: {last_error}")


def _worker_run(
    *,
    worker_index: int,
    session_locator: DwSessionLocator,
    items: list[DwWorkItem],
    settings: DwSettings,
    logger: Any,
    marshaled_app_stream: Any | None = None,
    worker_state: DwWorkerState | None = None,
    cancel_event: threading.Event | None = None,
) -> tuple[list[DwItemResult], list[dict[str, Any]]]:
    worker_started_at = time.perf_counter()
    pythoncom, initialized = _co_initialize()
    try:
        application: Any | None = None
        if worker_state is not None:
            worker_state.status = "running"
            worker_state.items_total = len(items)
        if marshaled_app_stream is not None:
            try:
                application = _unmarshal_sap_application(marshaled_app_stream, logger=logger, worker_index=worker_index)
            except Exception as exc:
                if worker_state is not None:
                    worker_state.status = "failed"
                logger.error(
                    "DW worker ABORT worker=%s unmarshal failed — no items will be processed error=%s",
                    worker_index, exc,
                )
                return [], [
                    {
                        "worker_index": worker_index,
                        "row_index": item.row_index,
                        "complaint_id": item.complaint_id,
                        "elapsed_seconds": 0.0,
                        "attempt": 0,
                        "fast_fail": True,
                        "error": f"COM unmarshal failed: {exc}",
                    }
                    for item in items
                ]
        # Obtain the session ONCE at worker start instead of per-item reattach.
        try:
            session = _reattach_dw_session(
                locator=session_locator, logger=logger, application=application,
            )
            _wait_session_ready(session, timeout_seconds=settings.wait_timeout_seconds, logger=logger)
        except Exception as exc:
            if worker_state is not None:
                worker_state.status = "failed"
            logger.error(
                "DW worker ABORT worker=%s initial session attach failed error=%s",
                worker_index, exc,
            )
            return [], [
                {
                    "worker_index": worker_index,
                    "row_index": item.row_index,
                    "complaint_id": item.complaint_id,
                    "elapsed_seconds": 0.0,
                    "attempt": 0,
                    "fast_fail": True,
                    "error": f"Session attach failed: {exc}",
                }
                for item in items
            ]
        results: list[DwItemResult] = []
        failures: list[dict[str, Any]] = []
        consecutive_failures = 0
        consecutive_fast_fails = 0
        logger.info(
            "DW worker START worker=%s assigned_items=%s con=%s ses=%s session_id=%s",
            worker_index, len(items),
            session_locator.connection_index, session_locator.session_index,
            _read_session_id(session),
        )
        if worker_state is not None:
            worker_state.session_id = _read_session_id(session)
        bootstrap_item = items[0] if items else DwWorkItem(row_index=0, complaint_id="")
        _ensure_dw_selection_screen(
            session=session,
            settings=settings,
            logger=logger,
            worker_index=worker_index,
            item=bootstrap_item,
            allow_navigation=True,
        )
        for index, item in enumerate(items, start=1):
            if cancel_event is not None and cancel_event.is_set():
                logger.warning("DW worker CANCELLED worker=%s remaining_items=%s", worker_index, len(items) - index + 1)
                for skip_item in items[index - 1 :]:
                    failures.append(
                        {
                            "worker_index": worker_index,
                            "row_index": skip_item.row_index,
                            "complaint_id": skip_item.complaint_id,
                            "elapsed_seconds": 0.0,
                            "attempt": 0,
                            "fast_fail": False,
                            "error": "Skipped: cancel event set",
                        }
                    )
                if worker_state is not None:
                    worker_state.status = "failed"
                break
            if index % _DW_PROGRESS_LOG_INTERVAL == 0 or index == 1:
                elapsed_so_far = time.perf_counter() - worker_started_at
                rate = len(results) / elapsed_so_far if elapsed_so_far > 0 else 0.0
                logger.info(
                    "DW worker PROGRESS worker=%s index=%s/%s ok=%s fail=%s elapsed_s=%.1f rate=%.2f items/s",
                    worker_index, index, len(items),
                    len(results), len(failures), elapsed_so_far, rate,
                )
            if worker_state is not None:
                worker_state.current_complaint_id = item.complaint_id
            result, failure, session = _process_dw_item_with_retry(
                worker_index=worker_index,
                session=session,
                session_locator=session_locator,
                item=item,
                settings=settings,
                logger=logger,
                application=application,
            )
            if failure is not None:
                failures.append(failure)
                consecutive_failures += 1
                if worker_state is not None:
                    worker_state.items_processed += 1
                    worker_state.items_failed += 1
                    worker_state.consecutive_failures = consecutive_failures
                if failure.get("fast_fail", False):
                    consecutive_fast_fails += 1
                else:
                    consecutive_fast_fails = 0
                if consecutive_fast_fails >= settings.circuit_breaker_fast_fail_threshold:
                    remaining = len(items) - index
                    logger.error(
                        "DW worker CIRCUIT-BREAKER worker=%s consecutive_fast_fails=%s — "
                        "aborting worker, skipping %s remaining items. "
                        "Likely systematic COM/session error.",
                        worker_index, consecutive_fast_fails, remaining,
                    )
                    if worker_state is not None:
                        worker_state.status = "circuit_breaker"
                    for skip_item in items[index:]:
                        failures.append({
                            "worker_index": worker_index,
                            "row_index": skip_item.row_index,
                            "complaint_id": skip_item.complaint_id,
                            "elapsed_seconds": 0.0,
                            "attempt": 0,
                            "fast_fail": True,
                            "error": f"Skipped: circuit breaker after {consecutive_fast_fails} consecutive fast failures",
                        })
                    break
                if consecutive_failures >= settings.circuit_breaker_slow_fail_threshold:
                    remaining = len(items) - index
                    logger.error(
                        "DW worker CIRCUIT-BREAKER worker=%s consecutive_failures=%s — "
                        "aborting worker, skipping %s remaining items.",
                        worker_index, consecutive_failures, remaining,
                    )
                    if worker_state is not None:
                        worker_state.status = "circuit_breaker"
                    for skip_item in items[index:]:
                        failures.append({
                            "worker_index": worker_index,
                            "row_index": skip_item.row_index,
                            "complaint_id": skip_item.complaint_id,
                            "elapsed_seconds": 0.0,
                            "attempt": 0,
                            "fast_fail": False,
                            "error": f"Skipped: circuit breaker after {consecutive_failures} consecutive failures",
                        })
                    break
                continue
            assert result is not None
            results.append(result)
            consecutive_failures = 0
            consecutive_fast_fails = 0
            if worker_state is not None:
                worker_state.items_processed += 1
                worker_state.items_ok += 1
                worker_state.last_ok_complaint_id = item.complaint_id
                worker_state.consecutive_failures = 0
        worker_elapsed = time.perf_counter() - worker_started_at
        if worker_state is not None:
            if worker_state.status not in {"circuit_breaker", "failed"}:
                worker_state.status = "completed"
            worker_state.elapsed_seconds = round(worker_elapsed, 3)
            worker_state.current_complaint_id = ""
        logger.info(
            "DW worker DONE worker=%s ok=%s fail=%s total=%s elapsed_s=%.1f",
            worker_index, len(results), len(failures), len(items), worker_elapsed,
        )
        return results, failures
    finally:
        if initialized and pythoncom is not None:
            pythoncom.CoUninitialize()


def _run_interleaved_workers(
    *,
    groups: list[list[DwWorkItem]],
    session_locators: list[DwSessionLocator],
    sessions: list[Any],
    settings: DwSettings,
    logger: Any,
    worker_states: list[DwWorkerState],
    apply_worker_results: Any,
    cancel_event: threading.Event,
) -> None:
    logger.info("DW starting interleaved multi-session processing workers=%s total_items=%s", len(groups), sum(len(group) for group in groups))
    worker_started_at = time.perf_counter()
    positions = {worker_index: 0 for worker_index in range(1, len(groups) + 1)}
    results_count = {worker_index: 0 for worker_index in range(1, len(groups) + 1)}
    failures_count = {worker_index: 0 for worker_index in range(1, len(groups) + 1)}
    consecutive_failures = {worker_index: 0 for worker_index in range(1, len(groups) + 1)}
    consecutive_fast_fails = {worker_index: 0 for worker_index in range(1, len(groups) + 1)}
    completed_workers: set[int] = set()
    current_sessions = {worker_index: sessions[worker_index - 1] for worker_index in range(1, len(groups) + 1)}

    for worker_index, group in enumerate(groups, start=1):
        session = current_sessions[worker_index]
        worker_state = worker_states[worker_index - 1]
        worker_state.status = "running"
        worker_state.items_total = len(group)
        worker_state.session_id = _read_session_id(session)
        logger.info(
            "DW worker START worker=%s assigned_items=%s con=%s ses=%s session_id=%s",
            worker_index,
            len(group),
            session_locators[worker_index - 1].connection_index,
            session_locators[worker_index - 1].session_index,
            _read_session_id(session),
        )
        bootstrap_item = group[0] if group else DwWorkItem(row_index=0, complaint_id="")
        _ensure_dw_selection_screen(
            session=session,
            settings=settings,
            logger=logger,
            worker_index=worker_index,
            item=bootstrap_item,
            allow_navigation=True,
        )

    while len(completed_workers) < len(groups):
        progressed = False
        for worker_index, group in enumerate(groups, start=1):
            if worker_index in completed_workers:
                continue
            if cancel_event.is_set():
                completed_workers.add(worker_index)
                continue
            index = positions[worker_index]
            if index >= len(group):
                worker_state = worker_states[worker_index - 1]
                worker_elapsed = time.perf_counter() - worker_started_at
                worker_state.status = "completed"
                worker_state.elapsed_seconds = round(worker_elapsed, 3)
                worker_state.current_complaint_id = ""
                logger.info(
                    "DW worker DONE worker=%s ok=%s fail=%s total=%s elapsed_s=%.1f",
                    worker_index, results_count[worker_index], failures_count[worker_index], len(group), worker_elapsed,
                )
                completed_workers.add(worker_index)
                continue

            item = group[index]
            if index == 0 or (index + 1) % _DW_PROGRESS_LOG_INTERVAL == 0:
                elapsed_so_far = time.perf_counter() - worker_started_at
                rate = results_count[worker_index] / elapsed_so_far if elapsed_so_far > 0 else 0.0
                logger.info(
                    "DW worker PROGRESS worker=%s index=%s/%s ok=%s fail=%s elapsed_s=%.1f rate=%.2f items/s",
                    worker_index, index + 1, len(group),
                    results_count[worker_index], failures_count[worker_index], elapsed_so_far, rate,
                )

            worker_state = worker_states[worker_index - 1]
            worker_state.current_complaint_id = item.complaint_id
            result, failure, current_session = _process_dw_item_with_retry(
                worker_index=worker_index,
                session=current_sessions[worker_index],
                session_locator=session_locators[worker_index - 1],
                item=item,
                settings=settings,
                logger=logger,
            )
            current_sessions[worker_index] = current_session
            positions[worker_index] += 1
            progressed = True

            if result is not None:
                apply_worker_results(worker_index, [result], [])
                results_count[worker_index] += 1
                consecutive_failures[worker_index] = 0
                consecutive_fast_fails[worker_index] = 0
                worker_state.items_processed += 1
                worker_state.items_ok += 1
                worker_state.last_ok_complaint_id = item.complaint_id
                worker_state.consecutive_failures = 0
                continue

            assert failure is not None
            apply_worker_results(worker_index, [], [failure])
            failures_count[worker_index] += 1
            consecutive_failures[worker_index] += 1
            worker_state.items_processed += 1
            worker_state.items_failed += 1
            worker_state.consecutive_failures = consecutive_failures[worker_index]
            if failure.get("fast_fail", False):
                consecutive_fast_fails[worker_index] += 1
            else:
                consecutive_fast_fails[worker_index] = 0

            remaining_items = group[positions[worker_index]:]
            if consecutive_fast_fails[worker_index] >= settings.circuit_breaker_fast_fail_threshold:
                logger.error(
                    "DW worker CIRCUIT-BREAKER worker=%s consecutive_fast_fails=%s — aborting worker, skipping %s remaining items. Likely systematic COM/session error.",
                    worker_index,
                    consecutive_fast_fails[worker_index],
                    len(remaining_items),
                )
                worker_state.status = "circuit_breaker"
                skipped_failures = [
                    {
                        "worker_index": worker_index,
                        "row_index": skip_item.row_index,
                        "complaint_id": skip_item.complaint_id,
                        "elapsed_seconds": 0.0,
                        "attempt": 0,
                        "fast_fail": True,
                        "error": f"Skipped: circuit breaker after {consecutive_fast_fails[worker_index]} consecutive fast failures",
                    }
                    for skip_item in remaining_items
                ]
                if skipped_failures:
                    apply_worker_results(worker_index, [], skipped_failures)
                    failures_count[worker_index] += len(skipped_failures)
                positions[worker_index] = len(group)
                continue

            if consecutive_failures[worker_index] >= settings.circuit_breaker_slow_fail_threshold:
                logger.error(
                    "DW worker CIRCUIT-BREAKER worker=%s consecutive_failures=%s — aborting worker, skipping %s remaining items.",
                    worker_index,
                    consecutive_failures[worker_index],
                    len(remaining_items),
                )
                worker_state.status = "circuit_breaker"
                skipped_failures = [
                    {
                        "worker_index": worker_index,
                        "row_index": skip_item.row_index,
                        "complaint_id": skip_item.complaint_id,
                        "elapsed_seconds": 0.0,
                        "attempt": 0,
                        "fast_fail": False,
                        "error": f"Skipped: circuit breaker after {consecutive_failures[worker_index]} consecutive failures",
                    }
                    for skip_item in remaining_items
                ]
                if skipped_failures:
                    apply_worker_results(worker_index, [], skipped_failures)
                    failures_count[worker_index] += len(skipped_failures)
                positions[worker_index] = len(group)

        if not progressed:
            break


def run_dw_demandante(
    *,
    run_id: str,
    demandante: str,
    config_path: Path,
    output_root: Path,
    max_rows: int | None = None,
) -> DwManifest:
    from .service import create_session_provider

    config = load_export_config(config_path)
    settings = load_dw_settings(
        config=config,
        config_path=config_path,
        demandante=demandante,
    )
    logger, log_path = configure_run_logger(output_root=output_root, run_id=run_id)
    logger.info(
        "Starting DW run run_id=%s demandante=%s input=%s transaction=%s session_count=%s",
        run_id,
        settings.demandante,
        settings.input_path,
        settings.transaction_code,
        settings.session_count,
    )

    header, data_rows, output_column_index, items, skipped_rows = load_dw_work_items(
        input_path=settings.input_path,
        encoding=settings.input_encoding,
        delimiter=settings.delimiter,
        id_column=settings.id_column,
        output_column=settings.output_column,
        max_rows=max_rows or settings.max_rows_per_run,
    )
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "dw" / "dw_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if not items:
        manifest = DwManifest(
            run_id=run_id,
            demandante=settings.demandante,
            input_path=str(settings.input_path),
            processed_rows=0,
            successful_rows=0,
            skipped_rows=skipped_rows,
            session_count=settings.session_count,
            status="skipped",
            log_path=str(log_path),
            manifest_path=str(manifest_path),
            worker_states=[],
        )
        manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    base_session = create_session_provider(config=config).get_session(config=config, logger=logger)
    sessions = prepare_dw_sessions(
        base_session=base_session,
        settings=settings,
        logger=logger,
    )
    session_locators = [_session_locator_from_session(session) for session in sessions]
    for slot_index, locator in enumerate(session_locators, start=1):
        logger.info(
            "DW prepared SAP session slot=%s connection_index=%s session_index=%s",
            slot_index,
            locator.connection_index,
            locator.session_index,
        )
    groups = split_work_items_evenly(items, len(sessions))
    failed_rows: list[dict[str, Any]] = []
    debug_rows: list[DwItemResult] = []
    successful_rows = 0
    row_index_to_data_index = {row_index: row_index - 2 for row_index in range(2, len(data_rows) + 2)}
    debug_csv_path = manifest_path.parent / "dw_observacoes_debug.csv"
    write_dw_debug_csv(
        output_path=debug_csv_path,
        rows=debug_rows,
    )
    worker_states = [
        DwWorkerState(
            worker_index=worker_index,
            session_id=f"/app/con[{session_locators[worker_index - 1].connection_index}]/ses[{session_locators[worker_index - 1].session_index}]",
        )
        for worker_index in range(1, len(groups) + 1)
    ]

    for worker_index, group in enumerate(groups, start=1):
        logger.info(
            "DW assigned instruction batch slot=%s items=%s first_row=%s last_row=%s",
            worker_index,
            len(group),
            group[0].row_index if group else "<none>",
            group[-1].row_index if group else "<none>",
        )

    csv_lock = threading.Lock()
    cancel_event = threading.Event()

    def _apply_worker_results(
        worker_index: int,
        worker_results: list[DwItemResult],
        worker_failures: list[dict[str, Any]],
    ) -> None:
        nonlocal successful_rows
        failed_rows.extend(worker_failures)
        with csv_lock:
            debug_rows.extend(worker_results)
            for result in worker_results:
                data_index = row_index_to_data_index[result.row_index]
                row = data_rows[data_index]
                while len(row) <= output_column_index:
                    row.append("")
                row[output_column_index] = result.observacao
                successful_rows += 1
            if worker_results:
                write_dw_csv(
                    input_path=settings.input_path,
                    encoding=settings.input_encoding,
                    delimiter=settings.delimiter,
                    header=header,
                    data_rows=data_rows,
                )
                write_dw_debug_csv(
                    output_path=debug_csv_path,
                    rows=debug_rows,
                )
        logger.info(
            "DW worker COLLECTED worker=%s successful=%s failures=%s",
            worker_index, len(worker_results), len(worker_failures),
        )

    processing_started_at = time.perf_counter()
    if settings.parallel_mode and len(groups) > 1:
        try:
            _run_interleaved_workers(
                groups=groups,
                session_locators=session_locators,
                sessions=sessions,
                settings=settings,
                logger=logger,
                worker_states=worker_states,
                apply_worker_results=_apply_worker_results,
                cancel_event=cancel_event,
            )
        except Exception as exc:
            logger.exception("DW interleaved processing CRASHED error=%s", exc)
            cancel_event.set()
            for worker_index, group in enumerate(groups, start=1):
                worker_state = worker_states[worker_index - 1]
                if worker_state.status in {"completed", "circuit_breaker"}:
                    continue
                worker_state.status = "failed"
                worker_state.elapsed_seconds = round(time.perf_counter() - processing_started_at, 3)
                worker_failures = [
                    {
                        "worker_index": worker_index,
                        "row_index": item.row_index,
                        "complaint_id": item.complaint_id,
                        "elapsed_seconds": 0.0,
                        "attempt": 0,
                        "fast_fail": True,
                        "error": f"Interleaved processing crashed: {exc}",
                    }
                    for item in group[worker_states[worker_index - 1].items_processed :]
                ]
                _apply_worker_results(worker_index, [], worker_failures)
    else:
        logger.info("DW starting sequential processing workers=%s total_items=%s", len(groups), len(items))
        for worker_index, group in enumerate(groups, start=1):
            try:
                worker_results, worker_failures = _worker_run(
                    worker_index=worker_index,
                    session_locator=session_locators[worker_index - 1],
                    items=group,
                    settings=settings,
                    logger=logger,
                    worker_state=worker_states[worker_index - 1],
                    cancel_event=cancel_event,
                )
            except Exception as exc:
                logger.exception(
                    "DW worker CRASHED worker=%s error=%s",
                    worker_index,
                    exc,
                )
                worker_state = worker_states[worker_index - 1]
                worker_state.status = "failed"
                worker_state.elapsed_seconds = round(time.perf_counter() - processing_started_at, 3)
                cancel_event.set()
                worker_results = []
                worker_failures = [
                    {
                        "worker_index": worker_index,
                        "row_index": item.row_index,
                        "complaint_id": item.complaint_id,
                        "elapsed_seconds": 0.0,
                        "attempt": 0,
                        "fast_fail": True,
                        "error": f"Worker crashed: {exc}",
                    }
                    for item in group
                ]
            _apply_worker_results(worker_index, worker_results, worker_failures)

    processing_elapsed = time.perf_counter() - processing_started_at
    logger.info(
        "DW processing phase DONE ok=%s fail=%s total=%s elapsed_s=%.1f",
        successful_rows, len(failed_rows), len(items), processing_elapsed,
    )
    status = "success" if successful_rows == len(items) and not failed_rows else "partial"
    manifest = DwManifest(
        run_id=run_id,
        demandante=settings.demandante,
        input_path=str(settings.input_path),
        processed_rows=len(items),
        successful_rows=successful_rows,
        skipped_rows=skipped_rows,
        session_count=len(groups),
        status=status,
        log_path=str(log_path),
        manifest_path=str(manifest_path),
        failed_rows=failed_rows,
        worker_states=[state.to_dict() for state in worker_states],
    )
    manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "DW run finished demandante=%s status=%s successful_rows=%s processed_rows=%s manifest=%s",
        settings.demandante,
        status,
        successful_rows,
        len(items),
        manifest_path,
    )
    return manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DW SAP GUI observation extraction from a complaints CSV.")
    parser.add_argument("--run-id", required=True, help="Run identifier.")
    parser.add_argument("--demandante", default="DW", help="Demandante profile. Default: DW.")
    parser.add_argument("--config", default="sap_iw69_batch_config.json", help="Path to config JSON.")
    parser.add_argument("--output-root", default="output", help="Root folder for run artifacts.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum number of pending complaint rows to process. Default uses config.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    manifest = run_dw_demandante(
        run_id=args.run_id,
        demandante=args.demandante,
        config_path=Path(args.config),
        output_root=Path(args.output_root),
        max_rows=args.max_rows or None,
    )
    return 0 if manifest.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
