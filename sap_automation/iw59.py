from __future__ import annotations

import csv
import importlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from .contracts import ObjectManifest
from .sap_helpers import (
    resolve_first_existing,
    resolve_first_existing_with_id,
    set_first_existing_text,
    set_text as _sap_set_text,
    wait_for_file,
    wait_not_busy as _sap_wait_not_busy,
)


def collect_iw59_notes_from_ca_csv(
    csv_path: Path,
    *,
    allowed_status_values: set[str] | None = None,
) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = [str(item or "").strip().lower() for item in (reader.fieldnames or [])]

    note_field = "nota" if "nota" in fieldnames else ""
    status_field = ""
    for candidate in ("statusuar", "status_usuario"):
        if candidate in fieldnames:
            status_field = candidate
            break
    if not note_field or not status_field:
        return []

    normalized_status_filter = {
        str(item).strip().upper() for item in (allowed_status_values or set()) if str(item).strip()
    }
    notes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        note = str(row.get(note_field, "") or "").strip()
        status_value = str(row.get(status_field, "") or "").strip()
        if (
            not note
            or not status_value
            or note in seen
            or (
                normalized_status_filter
                and status_value.strip().upper() not in normalized_status_filter
            )
        ):
            continue
        seen.add(note)
        notes.append(note)
    return notes


def chunk_iw59_notes(notes: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    return [notes[index : index + chunk_size] for index in range(0, len(notes), chunk_size)]


def concatenate_text_exports(source_paths: list[Path], destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as destination:
        for index, source_path in enumerate(source_paths):
            text = source_path.read_text(encoding="utf-8", errors="ignore")
            if index > 0 and text and not text.startswith("\n"):
                destination.write("\n")
            destination.write(text)


def build_ca_note_enrichment_map(csv_path: Path) -> dict[str, dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [str(item or "").strip() for item in (reader.fieldnames or [])]
        lowered = {name.casefold(): name for name in fieldnames if name.strip()}

        note_field = lowered.get("nota", "")
        if not note_field:
            return {}
        enrichment_fields = {
            "texto_code_parte_obj": lowered.get("texto_code_parte_obj", ""),
            "ptob": lowered.get("ptob", ""),
        }

        mapping: dict[str, dict[str, str]] = {}
        for row in reader:
            note = str(row.get(note_field, "") or "").strip()
            if not note or note in mapping:
                continue
            payload = {
                field_name: str(row.get(source_field, "") or "").strip()
                for field_name, source_field in enrichment_fields.items()
                if source_field
            }
            if any(payload.values()):
                mapping[note] = payload
        return mapping


def concatenate_delimited_exports(
    source_paths: list[Path],
    destination_path: Path,
    *,
    ca_note_enrichment_map: dict[str, dict[str, str]] | None = None,
) -> int:
    compat = importlib.import_module("sap_gui_export_compat")
    source_header: list[str] | None = None
    output_header: list[str] | None = None
    rows_written = 0
    enrichment_map = ca_note_enrichment_map or {}
    enrichment_fields = ["texto_code_parte_obj", "ptob"]
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for source_path in source_paths:
            _, delimiter, rows = compat._read_delimited_text(source_path)
            if not rows:
                continue
            current_header = [str(item).strip() for item in rows[0]]
            normalized_current_header = [item.casefold() for item in current_header]
            note_index = normalized_current_header.index("nota") if "nota" in normalized_current_header else -1
            enrichment_indexes = {
                field_name: normalized_current_header.index(field_name)
                for field_name in enrichment_fields
                if field_name in normalized_current_header
            }

            if source_header is None:
                source_header = current_header
                output_header = list(current_header)
                if enrichment_map:
                    normalized_output_header = [item.casefold() for item in output_header]
                    for field_name in enrichment_fields:
                        if field_name not in normalized_output_header:
                            output_header.append(field_name)
                writer.writerow(output_header)

            data_rows = rows[1:] if current_header == source_header else rows
            for row in data_rows:
                output_row = list(row)
                note = (
                    str(output_row[note_index]).strip()
                    if note_index >= 0 and note_index < len(output_row)
                    else ""
                )
                enrichment_values = enrichment_map.get(note, {})
                normalized_output_header = [item.casefold() for item in (output_header or [])]
                for field_name in enrichment_fields:
                    if field_name not in normalized_output_header:
                        continue
                    output_index = normalized_output_header.index(field_name)
                    while len(output_row) <= output_index:
                        output_row.append("")
                    field_value = enrichment_values.get(field_name, "")
                    if field_name in enrichment_indexes:
                        if field_value:
                            output_row[output_index] = field_value
                    else:
                        output_row[output_index] = field_value
                writer.writerow(output_row)
                rows_written += 1
    return rows_written


def compute_iw59_manu_modified_date_range(today: date | None = None) -> tuple[str, str]:
    current_day = today or date.today()
    month_start = current_day.replace(day=1)
    return month_start.strftime("%d.%m.%Y"), current_day.strftime("%d.%m.%Y")


@dataclass(frozen=True)
class Iw59ExportResult:
    status: str
    reason: str = ""
    total_notes: int = 0
    chunk_size: int = 0
    chunk_count: int = 0
    chunk_files: list[str] = field(default_factory=list)
    combined_txt_path: str = ""
    combined_csv_path: str = ""
    metadata_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Iw59ExportAdapter:
    def skip(self, *, reason: str) -> Iw59ExportResult:
        return Iw59ExportResult(status="skipped", reason=reason)

    def execute(
        self,
        *,
        output_root: Path,
        run_id: str,
        reference: str,
        demandante: str,
        ca_manifest: ObjectManifest,
        session: Any,
        logger: Any,
        config: dict[str, Any],
    ) -> Iw59ExportResult:
        ca_path = Path(ca_manifest.canonical_csv_path)
        if not ca_path.exists():
            return self.skip(reason="IW59 skipped because CA canonical CSV is missing.")

        iw59_cfg = config.get("iw59", {})
        if not bool(iw59_cfg.get("enabled", True)):
            return self.skip(reason="IW59 skipped because iw59.enabled=false.")

        transaction_code = str(iw59_cfg.get("transaction_code", "IW59")).strip() or "IW59"
        multi_select_button_id = str(
            iw59_cfg.get("multi_select_button_id", "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH")
        ).strip()
        back_button_id = str(iw59_cfg.get("back_button_id", "wnd[0]/tbar[0]/btn[3]")).strip()
        wait_timeout_seconds = float(config.get("global", {}).get("wait_timeout_seconds", 60.0))
        unwind_min_presses = int(iw59_cfg.get("pre_iw59_unwind_min_presses", 3))
        unwind_max_presses = int(iw59_cfg.get("pre_iw59_unwind_max_presses", 8))
        demandante_name = str(demandante or "").strip().upper() or "IGOR"
        demandante_cfg = (
            iw59_cfg.get("demandantes", {}).get(demandante_name, {})
            if isinstance(iw59_cfg.get("demandantes", {}), dict)
            else {}
        )
        chunk_size = int(demandante_cfg.get("chunk_size", iw59_cfg.get("chunk_size", 20000)))
        allowed_status_values = {
            str(item).strip().upper()
            for item in demandante_cfg.get("allowed_status_values", [])
            if str(item).strip()
        }
        notes = collect_iw59_notes_from_ca_csv(
            ca_path,
            allowed_status_values=allowed_status_values or None,
        )
        if not notes:
            if allowed_status_values:
                return self.skip(
                    reason=(
                        "IW59 skipped because CA did not produce notes matching statusuar filter: "
                        + ", ".join(sorted(allowed_status_values))
                    )
                )
            return self.skip(reason="IW59 skipped because CA did not produce notes with statusuar.")

        base_dir = output_root.expanduser().resolve() / "runs" / run_id / "iw59"
        raw_dir = base_dir / "raw"
        normalized_dir = base_dir / "normalized"
        metadata_dir = base_dir / "metadata"
        raw_dir.mkdir(parents=True, exist_ok=True)
        normalized_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        chunks = chunk_iw59_notes(notes, chunk_size)
        logger.info(
            "Starting IW59 extraction run_id=%s total_notes=%s chunk_size=%s chunk_count=%s status_filter=%s",
            run_id,
            len(notes),
            chunk_size,
            len(chunks),
            sorted(allowed_status_values),
        )

        chunk_paths: list[Path] = []
        for chunk_index, chunk_notes in enumerate(chunks, start=1):
            chunk_path = raw_dir / f"iw59_{reference}_{run_id}_{chunk_index:03d}.txt"
            logger.info(
                "Running IW59 chunk index=%s/%s notes=%s output=%s",
                chunk_index,
                len(chunks),
                len(chunk_notes),
                chunk_path,
            )
            self._run_chunk(
                session=session,
                notes=chunk_notes,
                output_path=chunk_path,
                logger=logger,
                demandante=demandante_name,
                demandante_cfg=demandante_cfg,
                transaction_code=transaction_code,
                multi_select_button_id=multi_select_button_id,
                back_button_id=back_button_id,
                wait_timeout_seconds=wait_timeout_seconds,
                unwind_min_presses=unwind_min_presses,
                unwind_max_presses=unwind_max_presses,
            )
            chunk_paths.append(chunk_path)

        combined_txt_path = raw_dir / f"iw59_{reference}_{run_id}_combined.txt"
        combined_csv_path = normalized_dir / f"iw59_{reference}_{run_id}.csv"
        metadata_path = metadata_dir / f"iw59_{reference}_{run_id}.manifest.json"
        ca_note_enrichment_map = build_ca_note_enrichment_map(ca_path)

        concatenate_text_exports(chunk_paths, combined_txt_path)
        rows_written = concatenate_delimited_exports(
            chunk_paths,
            combined_csv_path,
            ca_note_enrichment_map=ca_note_enrichment_map,
        )
        metadata = {
            "run_id": run_id,
            "reference": reference,
            "demandante": demandante_name,
            "status": "success",
            "total_notes": len(notes),
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "chunk_files": [str(item) for item in chunk_paths],
            "combined_txt_path": str(combined_txt_path),
            "combined_csv_path": str(combined_csv_path),
            "rows_written": rows_written,
            "ca_enrichment_mapped_notes": len(ca_note_enrichment_map),
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "IW59 extraction finished total_notes=%s chunk_count=%s combined_txt=%s combined_csv=%s",
            len(notes),
            len(chunks),
            combined_txt_path,
            combined_csv_path,
        )
        return Iw59ExportResult(
            status="success",
            total_notes=len(notes),
            chunk_size=chunk_size,
            chunk_count=len(chunks),
            chunk_files=[str(item) for item in chunk_paths],
            combined_txt_path=str(combined_txt_path),
            combined_csv_path=str(combined_csv_path),
            metadata_path=str(metadata_path),
        )

    def _run_chunk(
        self,
        *,
        session: Any,
        notes: list[str],
        output_path: Path,
        logger: Any,
        demandante: str,
        demandante_cfg: dict[str, Any],
        transaction_code: str,
        multi_select_button_id: str,
        back_button_id: str,
        wait_timeout_seconds: float,
        unwind_min_presses: int,
        unwind_max_presses: int,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        self._unwind_before_iw59(
            session=session,
            back_button_id=back_button_id,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
            min_presses=unwind_min_presses,
            max_presses=unwind_max_presses,
        )
        logger.info("IW59 entering transaction transaction=%s", transaction_code.upper())
        main_window = session.findById("wnd[0]")
        main_window.maximize()
        okcd = session.findById("wnd[0]/tbar[0]/okcd")
        okcd.text = transaction_code.lower()
        main_window.sendVKey(0)
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._prepare_selection_filters(
            session=session,
            logger=logger,
            demandante=demandante,
            demandante_cfg=demandante_cfg,
        )

        logger.info("IW59 opening notification multiselect notes=%s", len(notes))
        session.findById(multi_select_button_id).press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        self._copy_notes_to_clipboard(notes)
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        session.findById("wnd[1]/tbar[0]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        logger.info(
            "IW59 note multiselect applied notes=%s summary=%s",
            len(notes),
            self._note_selection_summary(session),
        )

        logger.info("IW59 executing selection for chunk output=%s", output_path)
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        self._ensure_results_screen_ready(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        self._open_export_dialog(
            session=session,
            logger=logger,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        local_file_radio = session.findById(
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]"
        )
        local_file_radio.select()
        local_file_radio.setFocus()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_PATH",
                "wnd[2]/usr/ctxtDY_PATH",
                "wnd[1]/usr/txtDY_PATH",
                "wnd[2]/usr/txtDY_PATH",
            ],
            value=str(output_path.parent),
        )
        self._set_first_existing_text(
            session=session,
            ids=[
                "wnd[1]/usr/ctxtDY_FILENAME",
                "wnd[2]/usr/ctxtDY_FILENAME",
                "wnd[1]/usr/txtDY_FILENAME",
                "wnd[2]/usr/txtDY_FILENAME",
            ],
            value=output_path.name,
        )
        self._resolve_first_existing(
            session=session,
            ids=[
                "wnd[1]/tbar[0]/btn[0]",
                "wnd[2]/tbar[0]/btn[0]",
            ],
        ).press()
        self._wait_for_file(output_path=output_path, timeout_seconds=wait_timeout_seconds)
        logger.info("IW59 chunk exported output=%s notes=%s", output_path, len(notes))

    def _prepare_selection_filters(
        self,
        *,
        session: Any,
        logger: Any,
        demandante: str,
        demandante_cfg: dict[str, Any],
    ) -> None:
        if str(demandante).strip().upper() != "MANU":
            return
        if not bool(demandante_cfg.get("use_modified_date_range", True)):
            return

        modified_from, modified_to = compute_iw59_manu_modified_date_range()
        logger.info(
            "IW59 MANU selection prep clearing note dates and applying modified date range from=%s to=%s",
            modified_from,
            modified_to,
        )
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUV", value="")
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtDATUB", value="")
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-LOW", value=modified_from)
        self._set_text(session=session, item_id="wnd[0]/usr/ctxtAEDAT-HIGH", value=modified_to)
        status_field = session.findById("wnd[0]/usr/ctxtSTAI1-LOW")
        status_field.setFocus()
        status_field.caretPosition = 0

    def _ensure_results_screen_ready(
        self,
        *,
        session: Any,
        logger: Any,
        wait_timeout_seconds: float,
    ) -> None:
        deadline = time.monotonic() + min(max(wait_timeout_seconds, 1.0), 8.0)
        last_status_bar = ""
        last_visible_controls: list[str] = []
        while time.monotonic() <= deadline:
            if self._has_export_surface(session):
                logger.info(
                    "IW59 execute reached exportable results screen status_bar=%s",
                    self._status_bar_text(session),
                )
                return
            last_status_bar = self._status_bar_text(session)
            last_visible_controls = self._visible_controls(session=session, limit=60)
            if self._is_selection_screen(session):
                time.sleep(0.25)
                continue
            time.sleep(0.25)

        status_bar = last_status_bar or self._status_bar_text(session)
        popup_title = self._safe_text(session, "wnd[1]")
        popup_status = self._safe_text(session, "wnd[1]/usr")
        details = [
            "IW59 execute did not leave selection screen.",
            f"status_bar={status_bar!r}",
            f"note_selection={self._note_selection_summary(session)!r}",
        ]
        if popup_title:
            details.append(f"popup_title={popup_title!r}")
        if popup_status:
            details.append(f"popup_text={popup_status!r}")
        if last_visible_controls:
            details.append(f"visible_controls={last_visible_controls}")
        raise RuntimeError(" ".join(details))

    def _has_export_surface(self, session: Any) -> bool:
        export_menu_ids = [
            "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
        ]
        return any(self._exists(session, item_id) for item_id in export_menu_ids) or any(
            self._exists(session, item_id) for item_id in self._candidate_export_shell_ids(session=session)
        )

    def _is_selection_screen(self, session: Any) -> bool:
        selection_markers = [
            "wnd[0]/usr/ctxtMONITOR",
            "wnd[0]/usr/ctxtVARIANT",
            "wnd[0]/usr/btn%_PAGESTAT_%_APP_%-VALU_PUSH",
            "wnd[0]/usr/ctxtPAGESTAT-LOW",
            "wnd[0]/usr/ctxtANLNR-LOW",
        ]
        return any(self._exists(session, item_id) for item_id in selection_markers)

    def _note_selection_summary(self, session: Any) -> str:
        values = [
            self._safe_text(session, "wnd[0]/usr/txt%_QMNUM_%_APP_%-TEXT"),
            self._safe_text(session, "wnd[0]/usr/txt%_QMNUM_%_APP_%-TO_TEXT"),
        ]
        return " | ".join([item for item in values if item]) or ""

    def _copy_notes_to_clipboard(self, notes: list[str]) -> None:
        import win32clipboard  # type: ignore

        win32clipboard.OpenClipboard()
        try:
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardText("\r\n".join(notes), win32clipboard.CF_UNICODETEXT)
        finally:
            win32clipboard.CloseClipboard()

    def _open_export_dialog(
        self,
        *,
        session: Any,
        logger: Any,
        wait_timeout_seconds: float,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        menu_ids = [
            "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
        ]
        try:
            _, export_item = self._resolve_first_existing_with_id(
                session=session,
                ids=menu_ids,
            )
        except RuntimeError as menu_error:
            logger.info(
                "IW59 export menu unavailable, trying ALV toolbar fallback error=%s",
                menu_error,
            )
        else:
            export_item.select()
            compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
            logger.info("IW59 export opened via menu")
            return

        shell_id, shell = self._resolve_first_existing_with_id(
            session=session,
            ids=self._candidate_export_shell_ids(session=session),
        )
        shell.pressToolbarContextButton("&MB_EXPORT")
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        shell.selectContextMenuItem("&PC")
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
        logger.info("IW59 export opened via ALV toolbar fallback shell_id=%s", shell_id)

    def _candidate_export_shell_ids(self, *, session: Any) -> list[str]:
        candidates = [
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID1/shellcont/shell",
            "wnd[0]/usr/cntlGRID/shellcont/shell",
            "wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell",
            "wnd[0]/usr/cntlRESULT_LIST/shellcont/shell",
            "wnd[0]/usr/cntlRESULT/shellcont/shell",
            "wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID_CONT0050/shellcont/shell",
            "wnd[0]/usr/cntlCC_ALV/shellcont/shell",
        ]
        compat = importlib.import_module("sap_gui_export_compat")
        try:
            visible_ids = compat._collect_visible_control_ids(session=session, limit=80)
        except Exception:
            visible_ids = []
        for item_id in visible_ids:
            normalized = str(item_id or "").strip()
            if not normalized or normalized in candidates:
                continue
            lowered = normalized.lower()
            if "shellcont/shell" not in lowered and not lowered.endswith("/shell"):
                continue
            if "/usr/" not in lowered:
                continue
            candidates.append(normalized)
        return candidates

    def _resolve_first_existing(self, *, session: Any, ids: list[str]) -> Any:
        return resolve_first_existing(session, ids)

    def _resolve_first_existing_with_id(self, *, session: Any, ids: list[str]) -> tuple[str, Any]:
        return resolve_first_existing_with_id(session, ids)

    def _set_first_existing_text(self, *, session: Any, ids: list[str], value: str) -> None:
        set_first_existing_text(session, ids, value)

    def _set_text(self, *, session: Any, item_id: str, value: str) -> None:
        _sap_set_text(session, item_id, value)

    def _wait_for_file(self, *, output_path: Path, timeout_seconds: float) -> None:
        wait_for_file(output_path, timeout_seconds=timeout_seconds)

    def _visible_controls(self, *, session: Any, limit: int) -> list[str]:
        compat = importlib.import_module("sap_gui_export_compat")
        try:
            return list(compat._collect_visible_control_ids(session=session, limit=limit))
        except Exception:
            return []

    def _unwind_before_iw59(
        self,
        *,
        session: Any,
        back_button_id: str,
        logger: Any,
        wait_timeout_seconds: float,
        min_presses: int,
        max_presses: int,
    ) -> None:
        compat = importlib.import_module("sap_gui_export_compat")
        previous_signature = self._session_signature(session)
        total_presses = 0
        stable_hits = 0
        logger.info(
            "IW59 pre-unwind start min_presses=%s max_presses=%s",
            min_presses,
            max_presses,
        )
        for _ in range(max(1, max_presses)):
            pressed = self._press_back(session=session, back_button_id=back_button_id)
            if not pressed:
                if total_presses >= min_presses:
                    break
                continue
            total_presses += 1
            compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)
            current_signature = self._session_signature(session)
            changed = current_signature != previous_signature
            stable_hits = 0 if changed else stable_hits + 1
            logger.info(
                "IW59 pre-unwind press=%s changed=%s stable_hits=%s signature=%s",
                total_presses,
                changed,
                stable_hits,
                current_signature,
            )
            if total_presses >= min_presses and stable_hits >= 1:
                break
            previous_signature = current_signature
        logger.info(
            "IW59 pre-unwind finished presses=%s final_signature=%s",
            total_presses,
            self._session_signature(session),
        )

    def _press_back(self, *, session: Any, back_button_id: str) -> bool:
        try:
            session.findById(back_button_id).press()
            return True
        except Exception:
            try:
                session.findById("wnd[0]").sendVKey(3)
                return True
            except Exception:
                return False

    def _session_signature(self, session: Any) -> str:
        parts = [
            self._safe_text(session, "wnd[0]"),
            self._safe_text(session, "wnd[0]/tbar[0]/okcd"),
            self._safe_text(session, "wnd[0]/sbar"),
            "qmnum=" + str(self._exists(session, "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH")),
            "otgrup=" + str(self._exists(session, "wnd[0]/usr/ctxtOTGRP-LOW")),
        ]
        return "|".join(parts)

    def _safe_text(self, session: Any, item_id: str) -> str:
        try:
            item = session.findById(item_id)
        except Exception:
            return ""
        for attr_name in ("Text", "text"):
            try:
                return str(getattr(item, attr_name, "")).strip()
            except Exception:
                continue
        return ""

    def _status_bar_text(self, session: Any) -> str:
        return self._safe_text(session, "wnd[0]/sbar")

    def _exists(self, session: Any, item_id: str) -> bool:
        try:
            session.findById(item_id)
            return True
        except Exception:
            return False
