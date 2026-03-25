from __future__ import annotations

import csv
from pathlib import Path

from sap_automation.iw59 import (
    Iw59ExportAdapter,
    chunk_iw59_notes,
    collect_iw59_notes_from_ca_csv,
    concatenate_delimited_exports,
    concatenate_text_exports,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_collect_iw59_notes_from_ca_csv_filters_by_statusuar_and_deduplicates(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca.csv"
    _write_csv(
        csv_path,
        [
            {"nota": "100", "statusuar": "ENCE", "descricao": "a"},
            {"nota": "100", "statusuar": "ENCE", "descricao": "dup"},
            {"nota": "101", "statusuar": "", "descricao": "skip"},
            {"nota": "102", "statusuar": "PEND", "descricao": "b"},
        ],
    )

    notes = collect_iw59_notes_from_ca_csv(csv_path)

    assert notes == ["100", "102"]


def test_chunk_iw59_notes_uses_requested_chunk_size() -> None:
    chunks = chunk_iw59_notes(["1", "2", "3", "4", "5"], chunk_size=2)

    assert chunks == [["1", "2"], ["3", "4"], ["5"]]


def test_concatenate_text_and_delimited_exports(tmp_path: Path) -> None:
    txt_a = tmp_path / "a.txt"
    txt_b = tmp_path / "b.txt"
    txt_a.write_text("nota\tvalor\n1\ta\n", encoding="utf-8")
    txt_b.write_text("nota\tvalor\n2\tb\n", encoding="utf-8")

    combined_txt = tmp_path / "combined.txt"
    combined_csv = tmp_path / "combined.csv"

    concatenate_text_exports([txt_a, txt_b], combined_txt)
    rows_written = concatenate_delimited_exports([txt_a, txt_b], combined_csv)

    assert "1\ta" in combined_txt.read_text(encoding="utf-8")
    assert "2\tb" in combined_txt.read_text(encoding="utf-8")
    assert rows_written == 2
    assert "nota,valor" in combined_csv.read_text(encoding="utf-8")


def test_iw59_adapter_skip_returns_structured_result() -> None:
    result = Iw59ExportAdapter().skip(reason="missing ca")

    assert result.to_dict()["status"] == "skipped"
    assert result.to_dict()["reason"] == "missing ca"
