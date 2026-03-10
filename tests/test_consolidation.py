from __future__ import annotations

import csv
from pathlib import Path

from sap_automation.consolidation import Consolidator
from sap_automation.contracts import ObjectManifest


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_consolidator_merges_successful_objects_and_marks_missing(tmp_path: Path) -> None:
    ca_csv = tmp_path / "ca.csv"
    rl_csv = tmp_path / "rl.csv"
    _write_csv(
        ca_csv,
        [
            {"nota": "100", "descricao": "CA item", "status_usuario": "ENCE"},
            {"nota": "101", "descricao": "CA second", "status_usuario": "ENCE"},
        ],
    )
    _write_csv(
        rl_csv,
        [
            {"nota": "100", "descricao": "RL same note", "status_usuario": "ENCE"},
        ],
    )
    manifests = [
        ObjectManifest(
            object_code="CA",
            status="success",
            canonical_csv_path=str(ca_csv),
            details={"run_id": "run-100"},
        ),
        ObjectManifest(
            object_code="RL",
            status="success",
            canonical_csv_path=str(rl_csv),
            details={"run_id": "run-100"},
        ),
        ObjectManifest(object_code="WB", status="failed", error="boom"),
    ]

    consolidator = Consolidator()
    notes_path = tmp_path / "notes.csv"
    interactions_path = tmp_path / "interactions.csv"

    manifest = consolidator.consolidate(
        object_manifests=manifests,
        notes_path=notes_path,
        interactions_path=interactions_path,
    )

    assert manifest.status == "partial"
    assert manifest.rows_notes == 2
    assert manifest.rows_interactions == 3
    assert manifest.missing_objects == ["WB"]
    note_rows = list(csv.DictReader(notes_path.open("r", encoding="utf-8", newline="")))
    assert note_rows[0]["source_objects"] in {"CA,RL", "RL,CA"}
