from __future__ import annotations

import argparse
import importlib
import json
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import load_export_config
from .runtime_logging import configure_run_logger
from .service import create_session_provider

_IW51_TRANSACTION_CODE = "IW51"
_IW51_FIXED_QMART = "CA"
_IW51_FIXED_QWRNUM = "389496787"
_IW51_OKCODE_ID = "wnd[0]/tbar[0]/okcd"
_IW51_MAIN_WINDOW_ID = "wnd[0]"
_IW51_QMART_ID = "wnd[0]/usr/ctxtRIWO00-QMART"
_IW51_QWRNUM_ID = "wnd[0]/usr/ctxtRIWO00-QWRNUM"
_IW51_TAB_19_ID = r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19"
_IW51_TAB_01_ID = r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB01"
_IW51_TPLNR_ID = (
    r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19/ssubSUB_GROUP_10:SAPLIQS0:7235/"
    r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_1:SAPLIQS0:7322/subOBJEKT:SAPLIWO1:0100/"
    r"ctxtRIWO1-TPLNR"
)
_IW51_INSTALACAO_ID = (
    r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19/ssubSUB_GROUP_10:SAPLIQS0:7235/"
    r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_3:SAPLIQS0:7900/subUSER0001:SAPLXQQM:0114/"
    r"ctxtQMEL-ZZANLAGE"
)
_IW51_PN_ID = (
    r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB01/ssubSUB_GROUP_10:SAPLIQS0:7235/"
    r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_1:SAPLIQS0:7515/subANSPRECH:SAPLIPAR:0700/"
    r"tabsTSTRIP_700/tabpKUND/ssubTSTRIP_SCREEN:SAPLIPAR:0130/subADRESSE:SAPLIPAR:0150/"
    r"ctxtVIQMEL-KUNUM"
)
_IW51_TIPOLOGIA_ID = (
    r"wnd[0]/usr/subSCREEN_1:SAPLIQS0:1060/subNOTIF_TYPE:SAPLIQS0:1061/txtVIQMEL-QMTXT"
)
_IW51_STATUS_BUTTON_ID = "wnd[0]/usr/subSCREEN_1:SAPLIQS0:1060/btnANWENDERSTATUS"
_IW51_STATUS_FIRST_ID = "wnd[1]/usr/tblSAPLBSVATC_E/radJ_STMAINT-ANWS[0,1]"
_IW51_STATUS_SECOND_ID = "wnd[1]/usr/tblSAPLBSVATC_E/radJ_STMAINT-ANWS[0,4]"
_IW51_STATUS_CONFIRM_ID = "wnd[1]/tbar[0]/btn[0]"
_IW51_SAVE_BUTTON_ID = "wnd[0]/tbar[1]/btn[16]"
_IW51_SAVE_CONFIRM_ID = "wnd[1]/tbar[0]/btn[0]"
_IW51_REQUIRED_HEADERS = ("pn", "instalacao", "tipologia")
_IW51_DONE_HEADER = "FEITO"
_IW51_DONE_VALUE = "SIM"


def _openpyxl_module() -> Any:
    try:
        import openpyxl
    except ImportError as exc:
        raise RuntimeError(
            "openpyxl is required to execute IW51 workbook automation. Install dependencies with poetry."
        ) from exc
    return openpyxl


def _normalize_header(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip())
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return "".join(ch.lower() for ch in ascii_text if ch.isalnum())


@dataclass(frozen=True)
class Iw51Settings:
    demandante: str
    workbook_path: Path
    sheet_name: str
    inter_item_sleep_seconds: float
    max_rows_per_run: int
    notification_type: str = _IW51_FIXED_QMART
    reference_notification_number: str = _IW51_FIXED_QWRNUM


@dataclass(frozen=True)
class Iw51WorkItem:
    row_index: int
    pn: str
    instalacao: str
    tipologia: str


@dataclass(frozen=True)
class Iw51Manifest:
    run_id: str
    demandante: str
    workbook_path: str
    sheet_name: str
    processed_rows: int
    successful_rows: int
    skipped_rows: int
    status: str
    log_path: str
    manifest_path: str
    failed_rows: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_iw51_settings(
    *,
    config: dict[str, Any],
    config_path: Path,
    demandante: str,
) -> Iw51Settings:
    iw51_cfg = config.get("iw51", {})
    demandante_name = str(demandante or "").strip().upper() or "DANI"
    if not isinstance(iw51_cfg, dict):
        raise RuntimeError("Missing iw51 section in config JSON.")
    demandantes_cfg = iw51_cfg.get("demandantes", {})
    if not isinstance(demandantes_cfg, dict):
        raise RuntimeError("Invalid iw51.demandantes section in config JSON.")
    profile = demandantes_cfg.get(demandante_name)
    if not isinstance(profile, dict):
        raise RuntimeError(f"Missing IW51 config for demandante={demandante_name}.")

    workbook_path = Path(str(profile.get("workbook_path", "")).strip())
    if not workbook_path.is_absolute():
        workbook_path = (config_path.expanduser().resolve().parent / workbook_path).resolve()
    return Iw51Settings(
        demandante=demandante_name,
        workbook_path=workbook_path,
        sheet_name=str(profile.get("sheet_name", "Macro1")).strip() or "Macro1",
        inter_item_sleep_seconds=float(profile.get("inter_item_sleep_seconds", 30.0)),
        max_rows_per_run=int(profile.get("max_rows_per_run", 4)),
        notification_type=str(profile.get("notification_type", _IW51_FIXED_QMART)).strip() or _IW51_FIXED_QMART,
        reference_notification_number=(
            str(profile.get("reference_notification_number", _IW51_FIXED_QWRNUM)).strip()
            or _IW51_FIXED_QWRNUM
        ),
    )


def load_iw51_work_items(
    *,
    workbook_path: Path,
    sheet_name: str,
    max_rows: int | None = None,
) -> tuple[Any, Any, int, list[Iw51WorkItem], int]:
    openpyxl = _openpyxl_module()
    workbook = openpyxl.load_workbook(workbook_path, keep_vba=True)
    if sheet_name not in workbook.sheetnames:
        raise RuntimeError(f"Sheet '{sheet_name}' not found in workbook {workbook_path}.")
    sheet = workbook[sheet_name]

    header_index_by_key: dict[str, int] = {}
    feito_column_index = 0
    for column_index, cell in enumerate(sheet[1], start=1):
        header_key = _normalize_header(cell.value)
        if not header_key:
            continue
        header_index_by_key[header_key] = column_index
        if header_key == "feito":
            feito_column_index = column_index

    missing_headers = [header for header in _IW51_REQUIRED_HEADERS if header not in header_index_by_key]
    if missing_headers:
        raise RuntimeError(
            "IW51 workbook is missing required columns: " + ", ".join(sorted(missing_headers))
        )

    if feito_column_index == 0:
        feito_column_index = sheet.max_column + 1
        sheet.cell(row=1, column=feito_column_index, value=_IW51_DONE_HEADER)

    items: list[Iw51WorkItem] = []
    skipped_rows = 0
    for row_index in range(2, sheet.max_row + 1):
        pn = str(sheet.cell(row=row_index, column=header_index_by_key["pn"]).value or "").strip()
        instalacao = str(sheet.cell(row=row_index, column=header_index_by_key["instalacao"]).value or "").strip()
        tipologia = str(sheet.cell(row=row_index, column=header_index_by_key["tipologia"]).value or "").strip()
        feito = str(sheet.cell(row=row_index, column=feito_column_index).value or "").strip().upper()

        if not pn and not instalacao and not tipologia:
            break
        if feito == _IW51_DONE_VALUE:
            skipped_rows += 1
            continue
        if not pn or not instalacao or not tipologia:
            raise RuntimeError(
                f"IW51 workbook row {row_index} is incomplete. Expected pn, instalacao and tipologia."
            )

        items.append(
            Iw51WorkItem(
                row_index=row_index,
                pn=pn,
                instalacao=instalacao,
                tipologia=tipologia,
            )
        )
        if max_rows is not None and max_rows > 0 and len(items) >= max_rows:
            break

    return workbook, sheet, feito_column_index, items, skipped_rows


def _set_text(session: Any, item_id: str, value: str) -> None:
    item = session.findById(item_id)
    try:
        item.setFocus()
    except Exception:
        pass
    item.text = value
    try:
        item.caretPosition = len(value)
    except Exception:
        pass


def _set_selected(item: Any, value: bool) -> None:
    for attr_name in ("selected", "Selected"):
        try:
            setattr(item, attr_name, value)
            return
        except Exception:
            continue
    raise RuntimeError("Could not select SAP radio button.")


def execute_iw51_item(
    *,
    session: Any,
    item: Iw51WorkItem,
    settings: Iw51Settings,
    logger: Any,
) -> None:
    compat = importlib.import_module("sap_gui_export_compat")

    main_window = session.findById(_IW51_MAIN_WINDOW_ID)
    main_window.maximize()
    _set_text(session, _IW51_OKCODE_ID, _IW51_TRANSACTION_CODE)
    main_window.sendVKey(0)
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    _set_text(session, _IW51_QMART_ID, settings.notification_type)
    qwrnum_item = session.findById(_IW51_QWRNUM_ID)
    qwrnum_item.text = settings.reference_notification_number
    qwrnum_item.setFocus()
    qwrnum_item.caretPosition = len(settings.reference_notification_number)
    main_window.sendVKey(0)
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    session.findById(_IW51_TAB_19_ID).select()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)
    _set_text(session, _IW51_TPLNR_ID, "")
    instalacao_item = session.findById(_IW51_INSTALACAO_ID)
    instalacao_item.text = item.instalacao
    instalacao_item.setFocus()
    instalacao_item.caretPosition = len(item.instalacao)
    main_window.sendVKey(0)
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    session.findById(_IW51_TAB_01_ID).select()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)
    pn_item = session.findById(_IW51_PN_ID)
    pn_item.text = item.pn
    pn_item.setFocus()
    pn_item.caretPosition = len(item.pn)
    main_window.sendVKey(0)
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    tipologia_item = session.findById(_IW51_TIPOLOGIA_ID)
    tipologia_item.text = item.tipologia
    tipologia_item.setFocus()
    tipologia_item.caretPosition = len(item.tipologia)

    session.findById(_IW51_STATUS_BUTTON_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)
    first_status = session.findById(_IW51_STATUS_FIRST_ID)
    _set_selected(first_status, True)
    first_status.setFocus()
    session.findById(_IW51_STATUS_CONFIRM_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    session.findById(_IW51_STATUS_BUTTON_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)
    second_status = session.findById(_IW51_STATUS_SECOND_ID)
    _set_selected(second_status, True)
    second_status.setFocus()
    session.findById(_IW51_STATUS_CONFIRM_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    session.findById(_IW51_SAVE_BUTTON_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)
    session.findById(_IW51_SAVE_CONFIRM_ID).press()
    compat.wait_not_busy(session=session, timeout_seconds=30.0)

    logger.info(
        "IW51 item completed row=%s pn=%s instalacao=%s tipologia=%s",
        item.row_index,
        item.pn,
        item.instalacao,
        item.tipologia,
    )


def run_iw51_demandante(
    *,
    run_id: str,
    demandante: str,
    config_path: Path,
    output_root: Path,
    max_rows: int | None = None,
) -> Iw51Manifest:
    config = load_export_config(config_path)
    settings = load_iw51_settings(
        config=config,
        config_path=config_path,
        demandante=demandante,
    )
    logger, log_path = configure_run_logger(output_root=output_root, run_id=run_id)
    logger.info(
        "Starting IW51 run run_id=%s demandante=%s workbook=%s sheet=%s",
        run_id,
        settings.demandante,
        settings.workbook_path,
        settings.sheet_name,
    )

    workbook, sheet, feito_column_index, items, skipped_rows = load_iw51_work_items(
        workbook_path=settings.workbook_path,
        sheet_name=settings.sheet_name,
        max_rows=max_rows or settings.max_rows_per_run,
    )
    if not items:
        manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "iw51" / "iw51_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = Iw51Manifest(
            run_id=run_id,
            demandante=settings.demandante,
            workbook_path=str(settings.workbook_path),
            sheet_name=settings.sheet_name,
            processed_rows=0,
            successful_rows=0,
            skipped_rows=skipped_rows,
            status="skipped",
            log_path=str(log_path),
            manifest_path=str(manifest_path),
        )
        manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    session_provider = create_session_provider(config=config)
    session = session_provider.get_session(config=config, logger=logger)
    failed_rows: list[dict[str, Any]] = []
    successful_rows = 0
    manifest_path = output_root.expanduser().resolve() / "runs" / run_id / "iw51" / "iw51_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    for index, item in enumerate(items, start=1):
        logger.info(
            "IW51 processing item index=%s/%s row=%s pn=%s instalacao=%s tipologia=%s",
            index,
            len(items),
            item.row_index,
            item.pn,
            item.instalacao,
            item.tipologia,
        )
        try:
            execute_iw51_item(
                session=session,
                item=item,
                settings=settings,
                logger=logger,
            )
        except Exception as exc:
            failed_rows.append(
                {
                    "row_index": item.row_index,
                    "pn": item.pn,
                    "instalacao": item.instalacao,
                    "tipologia": item.tipologia,
                    "error": str(exc),
                }
            )
            logger.exception("IW51 item failed row=%s", item.row_index)
            break

        sheet.cell(row=item.row_index, column=feito_column_index, value=_IW51_DONE_VALUE)
        workbook.save(settings.workbook_path)
        successful_rows += 1
        if index < len(items) and settings.inter_item_sleep_seconds > 0:
            logger.info(
                "Sleeping between IW51 items seconds=%.1f",
                settings.inter_item_sleep_seconds,
            )
            time.sleep(settings.inter_item_sleep_seconds)

    status = "success" if successful_rows == len(items) and not failed_rows else "partial"
    manifest = Iw51Manifest(
        run_id=run_id,
        demandante=settings.demandante,
        workbook_path=str(settings.workbook_path),
        sheet_name=settings.sheet_name,
        processed_rows=len(items),
        successful_rows=successful_rows,
        skipped_rows=skipped_rows,
        status=status,
        log_path=str(log_path),
        manifest_path=str(manifest_path),
        failed_rows=failed_rows,
    )
    manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "IW51 run finished demandante=%s status=%s successful_rows=%s processed_rows=%s manifest=%s",
        settings.demandante,
        status,
        successful_rows,
        len(items),
        manifest_path,
    )
    return manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run IW51 SAP GUI automation for a demandante workbook flow.")
    parser.add_argument("--run-id", required=True, help="Run identifier.")
    parser.add_argument("--demandante", default="DANI", help="Demandante profile. Default: DANI.")
    parser.add_argument("--config", default="sap_iw69_batch_config.json", help="Path to config JSON.")
    parser.add_argument("--output-root", default="output", help="Root folder for run artifacts.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum number of pending workbook rows to process. Default uses config.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    manifest = run_iw51_demandante(
        run_id=args.run_id,
        demandante=args.demandante,
        config_path=Path(args.config),
        output_root=Path(args.output_root),
        max_rows=args.max_rows or None,
    )
    return 0 if manifest.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
