from __future__ import annotations

import argparse
import csv
import json
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import load_export_config
from .runtime_logging import configure_run_logger
from .sap_helpers import set_text, wait_not_busy

_DW_MAIN_WINDOW_ID = "wnd[0]"
_DW_OKCODE_ID = "wnd[0]/tbar[0]/okcd"
_DW_QMNUM_LOW_ID = "wnd[0]/usr/ctxtQMNUM-LOW"
_DW_EXECUTE_ID = "wnd[0]/tbar[1]/btn[8]"
_DW_BACK_BUTTON_ID = "wnd[0]/tbar[0]/btn[3]"
_DW_TAB02_ID = r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02"
_DW_TEXT_TABLE_ID = (
    r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02/ssubSUB_GROUP_10:SAPLIQS0:7235/"
    r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_2:SAPLIQS0:7710/tblSAPLIQS0TEXT"
)
_DW_TEXT_LINE_TEMPLATE = _DW_TEXT_TABLE_ID + r"/txtLTXTTAB2-TLINE[0,{row}]"
_DW_REQUIRED_ID_HEADER = "ID Reclamação"
_DW_OUTPUT_HEADER = "OBSERVAÇÃO"


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
    for attr_name in ("position", "Position"):
        try:
            setattr(scrollbar, attr_name, value)
            return
        except Exception:
            continue
    raise RuntimeError("Could not adjust SAP text-table scrollbar position.")


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
    with input_path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.writer(handle, delimiter=delimiter, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(data_rows)


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
    sessions: list[Any] = []
    for index in range(limit):
        try:
            sessions.append(connection.Children(index))
        except Exception:
            break
    return sessions


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
    sessions = _list_connection_sessions(connection)
    while len(sessions) < session_count:
        before_count = len(sessions)
        create_session = getattr(base_session, "CreateSession", None) or getattr(base_session, "createSession", None)
        if callable(create_session):
            create_session()
        else:
            set_text(base_session, _DW_OKCODE_ID, "/o")
            base_session.findById(_DW_MAIN_WINDOW_ID).sendVKey(0)
        deadline = time.monotonic() + max(5.0, wait_timeout_seconds)
        while time.monotonic() < deadline:
            sessions = _list_connection_sessions(connection)
            if len(sessions) > before_count:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError(f"Could not open SAP session {before_count + 1} for DW flow.")
        logger.info("DW opened additional SAP session index=%s", len(sessions) - 1)
    return sessions[:session_count]


def _co_initialize() -> tuple[Any | None, bool]:
    try:
        import pythoncom  # type: ignore
    except Exception:
        return None, False
    pythoncom.CoInitialize()
    return pythoncom, True


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


def extract_observacao_text(*, session: Any, wait_timeout_seconds: float) -> str:
    table = session.findById(_DW_TEXT_TABLE_ID)
    try:
        focus_item = session.findById(_DW_TEXT_LINE_TEMPLATE.format(row=3))
        focus_item.setFocus()
        _set_caret(focus_item, 64)
    except Exception:
        pass

    scrollbar = getattr(table, "verticalScrollbar")
    max_position = max(
        4,
        min(
            50,
            _get_int_attr(scrollbar, "Maximum", "maximum", "MaxPosition", "maxPosition") or 4,
        ),
    )
    collected: list[str] = []
    previous_block: list[str] = []
    stable_hits = 0

    for position in range(0, max_position + 1):
        if position > 0:
            try:
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
        if position >= 4 and stable_hits >= 2:
            break

    try:
        final_focus = session.findById(_DW_TEXT_LINE_TEMPLATE.format(row=2))
        final_focus.setFocus()
        _set_caret(final_focus, 0)
    except Exception:
        pass

    return "\n".join(collected).strip()


def execute_dw_item(
    *,
    session: Any,
    item: DwWorkItem,
    settings: DwSettings,
    logger: Any,
    worker_index: int,
) -> str:
    main_window = session.findById(_DW_MAIN_WINDOW_ID)
    main_window.maximize()
    set_text(session, _DW_OKCODE_ID, settings.transaction_code)
    main_window.sendVKey(0)
    wait_not_busy(session, timeout_seconds=settings.wait_timeout_seconds)

    complaint_item = session.findById(_DW_QMNUM_LOW_ID)
    complaint_item.text = item.complaint_id
    _set_caret(complaint_item, len(item.complaint_id))
    session.findById(_DW_EXECUTE_ID).press()
    wait_not_busy(session, timeout_seconds=settings.wait_timeout_seconds)

    session.findById(_DW_TAB02_ID).select()
    wait_not_busy(session, timeout_seconds=settings.wait_timeout_seconds)
    observacao = extract_observacao_text(session=session, wait_timeout_seconds=settings.wait_timeout_seconds)

    for _ in range(2):
        session.findById(_DW_BACK_BUTTON_ID).press()
        wait_not_busy(session, timeout_seconds=settings.wait_timeout_seconds)

    logger.info(
        "DW item completed worker=%s row=%s complaint_id=%s observation_length=%s",
        worker_index,
        item.row_index,
        item.complaint_id,
        len(observacao),
    )
    return observacao


def _worker_run(
    *,
    worker_index: int,
    session: Any,
    items: list[DwWorkItem],
    settings: DwSettings,
    logger: Any,
) -> tuple[list[DwItemResult], list[dict[str, Any]]]:
    pythoncom, initialized = _co_initialize()
    try:
        results: list[DwItemResult] = []
        failures: list[dict[str, Any]] = []
        for index, item in enumerate(items, start=1):
            logger.info(
                "DW worker processing worker=%s index=%s/%s row=%s complaint_id=%s",
                worker_index,
                index,
                len(items),
                item.row_index,
                item.complaint_id,
            )
            started_at = time.perf_counter()
            try:
                observacao = execute_dw_item(
                    session=session,
                    item=item,
                    settings=settings,
                    logger=logger,
                    worker_index=worker_index,
                )
            except Exception as exc:
                elapsed_seconds = time.perf_counter() - started_at
                logger.exception(
                    "DW item failed worker=%s row=%s complaint_id=%s elapsed_s=%.2f",
                    worker_index,
                    item.row_index,
                    item.complaint_id,
                    elapsed_seconds,
                )
                failures.append(
                    {
                        "worker_index": worker_index,
                        "row_index": item.row_index,
                        "complaint_id": item.complaint_id,
                        "elapsed_seconds": round(elapsed_seconds, 3),
                        "error": str(exc),
                    }
                )
                continue
            elapsed_seconds = time.perf_counter() - started_at
            logger.info(
                "DW item performance worker=%s row=%s complaint_id=%s elapsed_s=%.2f",
                worker_index,
                item.row_index,
                item.complaint_id,
                elapsed_seconds,
            )
            results.append(
                DwItemResult(
                    row_index=item.row_index,
                    complaint_id=item.complaint_id,
                    observacao=observacao,
                    worker_index=worker_index,
                    elapsed_seconds=round(elapsed_seconds, 3),
                )
            )
        return results, failures
    finally:
        if initialized and pythoncom is not None:
            pythoncom.CoUninitialize()


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
        )
        manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    base_session = create_session_provider(config=config).get_session(config=config, logger=logger)
    sessions = ensure_sap_sessions(
        base_session=base_session,
        session_count=settings.session_count,
        logger=logger,
        wait_timeout_seconds=settings.wait_timeout_seconds,
    )
    groups = split_work_items_evenly(items, len(sessions))
    failed_rows: list[dict[str, Any]] = []
    successful_rows = 0
    row_index_to_data_index = {row_index: row_index - 2 for row_index in range(2, len(data_rows) + 2)}

    with ThreadPoolExecutor(max_workers=len(groups)) as executor:
        future_map = {
            executor.submit(
                _worker_run,
                worker_index=worker_index,
                session=sessions[worker_index - 1],
                items=group,
                settings=settings,
                logger=logger,
            ): worker_index
            for worker_index, group in enumerate(groups, start=1)
        }
        for future in as_completed(future_map):
            worker_results, worker_failures = future.result()
            failed_rows.extend(worker_failures)
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
