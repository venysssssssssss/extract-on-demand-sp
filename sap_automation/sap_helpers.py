"""Shared SAP GUI scripting helpers used across IW51, IW59, and other transaction modules."""

from __future__ import annotations

import importlib
import time
from pathlib import Path
from typing import Any


def wait_not_busy(session: Any, *, timeout_seconds: float = 30.0) -> None:
    compat = importlib.import_module("sap_gui_export_compat")
    compat.wait_not_busy(session=session, timeout_seconds=timeout_seconds)


def set_text(session: Any, item_id: str, value: str) -> None:
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


def set_selected(item: Any, value: bool) -> None:
    for attr_name in ("selected", "Selected"):
        try:
            setattr(item, attr_name, value)
            return
        except Exception:
            continue
    raise RuntimeError("Could not select SAP radio button.")


def resolve_first_existing(session: Any, ids: list[str]) -> Any:
    _, item = resolve_first_existing_with_id(session, ids)
    return item


def resolve_first_existing_with_id(session: Any, ids: list[str]) -> tuple[str, Any]:
    errors: list[str] = []
    for item_id in ids:
        try:
            return item_id, session.findById(item_id)
        except Exception as exc:
            errors.append(f"{item_id}: {exc}")
    visible_ids: list[str] = []
    try:
        compat = importlib.import_module("sap_gui_export_compat")
        visible_ids = compat._collect_visible_control_ids(session=session, limit=40)
    except Exception:
        visible_ids = []
    raise RuntimeError(
        "Could not resolve SAP GUI element. "
        + " | ".join(errors)
        + (f" | visible_controls={visible_ids}" if visible_ids else "")
    )


def set_first_existing_text(session: Any, ids: list[str], value: str) -> None:
    item = resolve_first_existing(session, ids)
    try:
        item.setFocus()
    except Exception:
        pass
    item.text = value
    try:
        item.caretPosition = len(value)
    except Exception:
        pass


def wait_for_file(output_path: Path, *, timeout_seconds: float = 60.0) -> None:
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            if output_path.exists() and output_path.is_file() and output_path.stat().st_size > 0:
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Export file was not created in time: {output_path}")
