from __future__ import annotations

import json
from pathlib import Path

import sap_automation.config as config_module
import sap_automation.service as service_module
from sap_automation.service import (
    _extract_iw59_source_manifests_from_batch_manifest,
    _load_object_manifests_for_iw59_replay,
    run_iw59_payload,
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


def test_run_iw59_payload_routes_kelly_to_standalone_executor(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        config_module,
        "load_export_config",
        lambda path: {"iw59": {"enabled": True, "demandantes": {"KELLY": {"input_csv_path": "brs_filtrados.csv"}}}},
    )
    monkeypatch.setattr(
        service_module,
        "configure_run_logger",
        lambda output_root, run_id: (type("Logger", (), {"info": lambda *args, **kwargs: None})(), None),
    )

    class _Provider:
        def get_session(self, *, config, logger):  # noqa: ANN001
            return object()

    monkeypatch.setattr(service_module, "create_session_provider", lambda config: _Provider())

    class _Adapter:
        def execute_modified_by_brs(self, **kwargs):  # noqa: ANN003
            calls.update(kwargs)
            return service_module.Iw59ExportResult(
                status="success",
                source_object_code="KELLY",
                chunk_size=200,
                chunk_count=4,
                combined_csv_path="output/runs/run-kelly/iw59/normalized/iw59_kelly_run-kelly.csv",
                metadata_path="output/runs/run-kelly/iw59/metadata/iw59_kelly_run-kelly.manifest.json",
            )

    monkeypatch.setattr(service_module, "Iw59ExportAdapter", lambda: _Adapter())

    result = run_iw59_payload(
        run_id="run-kelly",
        demandante="KELLY",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
        reference="202603",
        input_csv_path=Path("brs_filtrados.csv"),
    )

    assert calls["run_id"] == "run-kelly"
    assert calls["demandante"] == "KELLY"
    assert calls["reference"] == "202603"
    assert calls["input_csv_path"] == Path("brs_filtrados.csv")
    assert result.status == "success"
    assert result.results[0]["source_object_code"] == "KELLY"
