from __future__ import annotations

import csv
import importlib
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .contracts import ObjectManifest


def collect_iw59_notes_from_ca_csv(csv_path: Path) -> list[str]:
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

    notes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        note = str(row.get(note_field, "") or "").strip()
        status_value = str(row.get(status_field, "") or "").strip()
        if not note or not status_value or note in seen:
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


def concatenate_delimited_exports(source_paths: list[Path], destination_path: Path) -> int:
    compat = importlib.import_module("sap_gui_export_compat")
    header: list[str] | None = None
    rows_written = 0
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for source_path in source_paths:
            _, delimiter, rows = compat._read_delimited_text(source_path)
            if not rows:
                continue
            current_header = [str(item).strip() for item in rows[0]]
            if header is None:
                header = current_header
                writer.writerow(header)
            data_rows = rows[1:] if current_header == header else rows
            for row in data_rows:
                writer.writerow(row)
                rows_written += 1
    return rows_written


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

        notes = collect_iw59_notes_from_ca_csv(ca_path)
        if not notes:
            return self.skip(reason="IW59 skipped because CA did not produce notes with statusuar.")

        chunk_size = int(iw59_cfg.get("chunk_size", 20000))
        transaction_code = str(iw59_cfg.get("transaction_code", "IW59")).strip() or "IW59"
        multi_select_button_id = str(
            iw59_cfg.get("multi_select_button_id", "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH")
        ).strip()
        back_button_id = str(iw59_cfg.get("back_button_id", "wnd[0]/tbar[0]/btn[3]")).strip()
        wait_timeout_seconds = float(config.get("global", {}).get("wait_timeout_seconds", 60.0))
        unwind_min_presses = int(iw59_cfg.get("pre_iw59_unwind_min_presses", 3))
        unwind_max_presses = int(iw59_cfg.get("pre_iw59_unwind_max_presses", 8))

        base_dir = output_root.expanduser().resolve() / "runs" / run_id / "iw59"
        raw_dir = base_dir / "raw"
        normalized_dir = base_dir / "normalized"
        metadata_dir = base_dir / "metadata"
        raw_dir.mkdir(parents=True, exist_ok=True)
        normalized_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        chunks = chunk_iw59_notes(notes, chunk_size)
        logger.info(
            "Starting IW59 extraction run_id=%s total_notes=%s chunk_size=%s chunk_count=%s",
            run_id,
            len(notes),
            chunk_size,
            len(chunks),
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

        concatenate_text_exports(chunk_paths, combined_txt_path)
        rows_written = concatenate_delimited_exports(chunk_paths, combined_csv_path)
        metadata = {
            "run_id": run_id,
            "reference": reference,
            "status": "success",
            "total_notes": len(notes),
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "chunk_files": [str(item) for item in chunk_paths],
            "combined_txt_path": str(combined_txt_path),
            "combined_csv_path": str(combined_csv_path),
            "rows_written": rows_written,
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

        logger.info("IW59 executing selection for chunk output=%s", output_path)
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

        export_item = self._resolve_first_existing(
            session=session,
            ids=[
                "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
                "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
            ],
        )
        export_item.select()
        compat.wait_not_busy(session=session, timeout_seconds=wait_timeout_seconds)

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

    def _copy_notes_to_clipboard(self, notes: list[str]) -> None:
        import win32clipboard  # type: ignore

        win32clipboard.OpenClipboard()
        try:
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardText("\r\n".join(notes), win32clipboard.CF_UNICODETEXT)
        finally:
            win32clipboard.CloseClipboard()

    def _resolve_first_existing(self, *, session: Any, ids: list[str]) -> Any:
        errors: list[str] = []
        for item_id in ids:
            try:
                return session.findById(item_id)
            except Exception as exc:
                errors.append(f"{item_id}: {exc}")
        raise RuntimeError("Could not resolve SAP GUI element. " + " | ".join(errors))

    def _set_first_existing_text(self, *, session: Any, ids: list[str], value: str) -> None:
        item = self._resolve_first_existing(session=session, ids=ids)
        try:
            item.setFocus()
        except Exception:
            pass
        item.text = value
        try:
            item.caretPosition = len(value)
        except Exception:
            pass

    def _wait_for_file(self, *, output_path: Path, timeout_seconds: float) -> None:
        deadline = time.time() + max(1.0, timeout_seconds)
        while time.time() < deadline:
            try:
                if output_path.exists() and output_path.is_file() and output_path.stat().st_size > 0:
                    return
            except OSError:
                pass
            time.sleep(0.2)
        raise RuntimeError(f"IW59 export file was not created in time: {output_path}")

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

    def _exists(self, session: Any, item_id: str) -> bool:
        try:
            session.findById(item_id)
            return True
        except Exception:
            return False
