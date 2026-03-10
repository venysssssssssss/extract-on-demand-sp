from __future__ import annotations

import csv
from pathlib import Path

from .contracts import ConsolidationManifest, ObjectManifest

_REQUIRED_CANONICAL_COLUMNS: tuple[str, ...] = ("nota",)


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: value or "" for key, value in row.items()} for row in reader]
    return fieldnames, rows


class Consolidator:
    def consolidate(
        self,
        *,
        object_manifests: list[ObjectManifest],
        notes_path: Path,
        interactions_path: Path,
    ) -> ConsolidationManifest:
        successful = [
            manifest
            for manifest in object_manifests
            if manifest.status == "success" and manifest.canonical_csv_path
        ]
        missing_objects = sorted(
            manifest.object_code for manifest in object_manifests if manifest.status != "success"
        )
        if not successful:
            return ConsolidationManifest(
                status="skipped",
                missing_objects=missing_objects,
                error="No successful object exports available for consolidation.",
            )

        union_columns: list[str] = ["run_id", "source_object"]
        notes_by_key: dict[str, dict[str, str]] = {}
        interactions: list[dict[str, str]] = []
        rows_interactions = 0

        for manifest in successful:
            fieldnames, rows = _read_rows(Path(manifest.canonical_csv_path))
            missing_columns = [
                column for column in _REQUIRED_CANONICAL_COLUMNS if column not in fieldnames
            ]
            if missing_columns:
                raise RuntimeError(
                    f"Canonical export for {manifest.object_code} is missing columns: "
                    + ", ".join(missing_columns)
                )
            for column in fieldnames:
                if column not in union_columns:
                    union_columns.append(column)
            for row in rows:
                enriched = {"run_id": manifest.details.get("run_id", ""), "source_object": manifest.object_code}
                enriched.update(row)
                note_key = enriched.get("nota", "").strip()
                if not note_key:
                    continue
                interactions.append(enriched)
                rows_interactions += 1
                current = notes_by_key.get(note_key)
                if current is None:
                    current = dict(enriched)
                    current["source_objects"] = manifest.object_code
                    current["interaction_count"] = "1"
                    notes_by_key[note_key] = current
                    continue
                source_objects = {
                    token.strip()
                    for token in current.get("source_objects", "").split(",")
                    if token.strip()
                }
                source_objects.add(manifest.object_code)
                current["source_objects"] = ",".join(sorted(source_objects))
                current["interaction_count"] = str(int(current.get("interaction_count", "0")) + 1)
                for key, value in enriched.items():
                    if value and not current.get(key):
                        current[key] = value

        note_columns = list(union_columns)
        if "source_objects" not in note_columns:
            note_columns.append("source_objects")
        if "interaction_count" not in note_columns:
            note_columns.append("interaction_count")
        self._write_csv(notes_path, note_columns, list(notes_by_key.values()))
        self._write_csv(interactions_path, union_columns, interactions)
        status = "success" if not missing_objects else "partial"
        return ConsolidationManifest(
            status=status,
            notes_path=str(notes_path),
            interactions_path=str(interactions_path),
            rows_notes=len(notes_by_key),
            rows_interactions=rows_interactions,
            missing_objects=missing_objects,
        )

    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
