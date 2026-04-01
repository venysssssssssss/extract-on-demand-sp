from __future__ import annotations

import json
from pathlib import Path

from sap_automation.service import (
    _extract_iw59_source_manifests_from_batch_manifest,
    _load_object_manifests_for_iw59_replay,
)


def test_extract_iw59_source_manifests_from_batch_manifest_keeps_successful_ca_rl_wb() -> None:
    manifests = _extract_iw59_source_manifests_from_batch_manifest(
        {
            "objects": [
                {"object_code": "CA", "status": "success", "canonical_csv_path": "ca.csv"},
                {"object_code": "RL", "status": "success", "canonical_csv_path": "rl.csv"},
                {"object_code": "WB", "status": "failed", "canonical_csv_path": "wb.csv"},
                {"object_code": "XX", "status": "success", "canonical_csv_path": "xx.csv"},
            ]
        }
    )

    assert [manifest.object_code for manifest in manifests] == ["CA", "RL"]


def test_load_object_manifests_for_iw59_replay_reads_successful_ca_rl_wb(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "run-iw59"
    reference = "202603"
    for object_code in ("ca", "rl", "wb"):
        metadata_dir = run_root / object_code / "metadata"
        normalized_dir = run_root / object_code / "normalized"
        raw_dir = run_root / object_code / "raw"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        normalized_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        canonical_csv = normalized_dir / f"{object_code}_{reference}_run-iw59.csv"
        canonical_csv.write_text("nota,statusuar\n1,ENCE DEFE\n", encoding="utf-8")
        raw_txt = raw_dir / f"{object_code}_{reference}_run-iw59.txt"
        raw_txt.write_text("raw", encoding="utf-8")
        metadata_path = metadata_dir / f"{object_code}_{reference}_run-iw59.manifest.json"
        metadata_path.write_text(
            json.dumps(
                {
                    "reference": reference,
                    "output_path": str(canonical_csv),
                    "raw_csv_path": str(canonical_csv),
                    "source_txt_path": str(raw_txt),
                    "rows_exported": 1,
                    "csv_materialized": True,
                }
            ),
            encoding="utf-8",
        )

    manifests, resolved_reference = _load_object_manifests_for_iw59_replay(
        output_root=tmp_path,
        run_id="run-iw59",
    )

    assert resolved_reference == reference
    assert [manifest.object_code for manifest in manifests] == ["CA", "RL", "WB"]
