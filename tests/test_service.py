from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import sap_automation.config as config_module
import sap_automation.service as service_module
from sap_automation.service import (
    _extract_iw59_source_manifests_from_batch_manifest,
    _load_object_manifests_for_iw59_replay,
    run_medidor_payload,
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


def test_run_medidor_payload_delegates_to_medidor_demandante(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    calls: dict[str, object] = {}

    def _fake_run_medidor_demandante(**kwargs):  # noqa: ANN003
        calls.update(kwargs)
        return service_module.MedidorManifest(
            status="success",
            run_id=str(kwargs["run_id"]),
            demandante=str(kwargs["demandante"]),
            reference="20260416",
            period_from="01.01.2025",
            period_to="16.04.2026",
            input_installations_path="instalacaosp.xlsx",
            group_map_path="gruporegsap.xlsx",
            manifest_path="output/runs/run-medidor/medidor/metadata/manifest.json",
        )

    monkeypatch.setattr(service_module, "run_medidor_demandante", _fake_run_medidor_demandante)

    result = run_medidor_payload(
        run_id="run-medidor",
        demandante="MEDIDOR",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
        installations_path=Path("instalacaosp.xlsx"),
        group_map_path=Path("gruporegsap.xlsx"),
    )

    assert calls["run_id"] == "run-medidor"
    assert calls["demandante"] == "MEDIDOR"
    assert calls["installations_path"] == Path("instalacaosp.xlsx")
    assert calls["group_map_path"] == Path("gruporegsap.xlsx")
    assert result.status == "success"


def test_run_medidor_payload_fetch_installations_only_writes_csv(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        service_module,
        "configure_run_logger",
        lambda output_root, run_id: (type("Logger", (), {"info": lambda *args, **kwargs: None})(), None),
    )

    class _Repository:
        def __init__(self, db_url: str) -> None:
            self.db_url = db_url

        def get_installations_by_alimentador(self, *, distribuidora: str, source_column: str) -> list[str]:
            assert distribuidora == "São Paulo"
            assert source_column == "ALIMENTADOR"
            return ["123@45", "123045", "98A76", "", "98@76"]

    monkeypatch.setattr(service_module, "MedidorRepository", _Repository)

    result = run_medidor_payload(
        run_id="run-medidor-db",
        demandante="MEDIDOR",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
        installations_source="db",
        fetch_installations_only=True,
    )

    csv_path = tmp_path / "runs" / "run-medidor-db" / "medidor" / "input" / "MEDIDOR_INSTALLATIONS.csv"
    assert result.status == "success"
    assert result.total_installations == 2
    assert result.input_installations_path == str(csv_path)
    assert csv_path.read_text(encoding="utf-8").splitlines() == ["INSTALACAO", "123045", "98076"]


def test_run_medidor_payload_db_source_uses_source_run_id_for_installations(monkeypatch, tmp_path: Path) -> None:
    source_run_id = "run-source"
    target_run_id = "run-target"
    csv_path = tmp_path / "runs" / source_run_id / "medidor" / "input" / "MEDIDOR_INSTALLATIONS.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text("INSTALACAO\n123456\n", encoding="utf-8")
    
    monkeypatch.setattr(
        service_module,
        "configure_run_logger",
        lambda output_root, run_id: (type("Logger", (), {"info": lambda *args, **kwargs: None})(), None),
    )

    calls: dict[str, object] = {}
    def _fake_run_medidor_demandante(**kwargs):
        calls.update(kwargs)
        return service_module.MedidorManifest(
            status="success",
            run_id=kwargs["run_id"],
            demandante=kwargs["demandante"],
            reference="ref",
            period_from="from",
            period_to="to",
            input_installations_path=kwargs["installations_path"] or "",
            group_map_path="",
        )

    monkeypatch.setattr(service_module, "run_medidor_demandante", _fake_run_medidor_demandante)

    result = run_medidor_payload(
        run_id=target_run_id,
        demandante="MEDIDOR",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
        installations_source="db",
        source_run_id=source_run_id,
    )

    assert result.status == "success"
    assert calls["installations"] == ["123456"]
    assert calls["run_id"] == target_run_id


def test_run_medidor_payload_db_source_reuses_existing_installations_csv(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "runs" / "run-medidor-db" / "medidor" / "input" / "MEDIDOR_INSTALLATIONS.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text("INSTALACAO\n123@45\n123045\n98A76\n", encoding="utf-8")
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        service_module,
        "configure_run_logger",
        lambda output_root, run_id: (type("Logger", (), {"info": lambda *args, **kwargs: None})(), None),
    )

    class _Repository:
        def __init__(self, db_url: str) -> None:
            self.db_url = db_url

        def get_installations_by_alimentador(self, *, distribuidora: str, source_column: str) -> list[str]:
            raise AssertionError("Existing MEDIDOR installations CSV should be reused.")

    def _fake_run_medidor_demandante(**kwargs):  # noqa: ANN003
        calls.update(kwargs)
        return service_module.MedidorManifest(
            status="success",
            run_id=str(kwargs["run_id"]),
            demandante=str(kwargs["demandante"]),
            reference="20260416",
            period_from="01.01.2025",
            period_to="16.04.2026",
            input_installations_path=str(csv_path),
            group_map_path="gruporegsap.xlsx",
            manifest_path="output/runs/run-medidor-db/medidor/metadata/manifest.json",
        )

    monkeypatch.setattr(service_module, "MedidorRepository", _Repository)
    monkeypatch.setattr(service_module, "run_medidor_demandante", _fake_run_medidor_demandante)

    result = run_medidor_payload(
        run_id="run-medidor-db",
        demandante="MEDIDOR_SP",
        output_root=tmp_path,
        config_path=Path("sap_iw69_batch_config.json"),
        installations_source="db",
        fetch_installations_only=False,
    )

    assert result.status == "success"
    assert calls["installations"] == ["123045", "98076"]


def test_execute_sm_ingest_final_reads_local_artifact_without_http(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    output_root = tmp_path / "output"
    artifact_path = output_root / "artifacts" / "run-sm-ingest" / "SM_DADOS_FATURA.csv"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("nota,doc_impr\n1,2\n", encoding="utf-8")

    service = SimpleNamespace(
        get_artifact=lambda **kwargs: SimpleNamespace(path=str(artifact_path)),
    )
    monkeypatch.setattr(service_module, "create_control_plane_service", lambda: service)

    calls: dict[str, object] = {}

    def _fake_ingest_sm_results(**kwargs):  # noqa: ANN003
        calls.update(kwargs)
        return SimpleNamespace(
            status="success",
            to_dict=lambda: {"status": "success", "rows_ingested": 1},
            source_csv_path=str(kwargs["final_csv_path"]),
        )

    monkeypatch.setattr(service_module, "ingest_sm_results", _fake_ingest_sm_results)
    monkeypatch.setattr(
        service_module,
        "_download_artifact_to_path",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("HTTP/local download helper should not be called")),
    )

    status, result, manifest_path = service_module._execute_sm_ingest_final(  # type: ignore[attr-defined]
        SimpleNamespace(job_id="job-1", demandante="SALA_MERCADO"),
        {
            "run_id": "run-sm-ingest",
            "output_root": str(output_root),
            "config_path": "sap_iw69_batch_config.json",
            "control_plane_base_url": "http://10.71.202.127:18000",
            "month": 4,
            "year": 2026,
            "distribuidora": "São Paulo",
        },
    )

    assert status == "success"
    assert result["status"] == "success"
    assert manifest_path.endswith("SM_DADOS_FATURA.csv")
    assert calls["final_csv_path"] == artifact_path
    copied_path = output_root / "runs" / "run-sm-ingest" / "sm" / "SM_DADOS_FATURA.csv"
    assert copied_path.read_text(encoding="utf-8") == "nota,doc_impr\n1,2\n"


def test_resolve_medidor_final_csv_path_finds_patterned_file(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    normalized_dir = tmp_path / "runs" / run_id / "medidor" / "normalized"
    normalized_dir.mkdir(parents=True)

    # Create a patterned file
    patterned_file = normalized_dir / f"medidor_20260424_{run_id}.csv"
    patterned_file.write_text("instalacao,equipamento,grp_reg,tipo\n1,EQ,G,Digital\n", encoding="utf-8")

    # Also create a compact file to ensure patterned has priority
    compact_file = normalized_dir / "medidor_raw_compactado.csv"
    compact_file.write_text("instalacao,tipo\n2,Analogico\n", encoding="utf-8")

    resolved = service_module._resolve_medidor_final_csv_path(
        output_root=tmp_path,
        final_csv_path=None,
        source_run_id=None,
        run_id=run_id,
    )

    assert resolved == patterned_file.resolve()


def test_resolve_medidor_final_csv_path_falls_back_to_compact_file(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    normalized_dir = tmp_path / "runs" / run_id / "medidor" / "normalized"
    normalized_dir.mkdir(parents=True)

    # Create only a compact file
    compact_file = normalized_dir / "medidor_raw_compactado.csv"
    compact_file.write_text("instalacao,tipo\n1,Digital\n", encoding="utf-8")

    resolved = service_module._resolve_medidor_final_csv_path(
        output_root=tmp_path,
        final_csv_path=None,
        source_run_id=None,
        run_id=run_id,
    )

    assert resolved == compact_file.resolve()


def test_resolve_medidor_final_csv_path_falls_back_to_any_medidor_csv(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    normalized_dir = tmp_path / "runs" / run_id / "medidor" / "normalized"
    normalized_dir.mkdir(parents=True)

    # Create an arbitrary medidor CSV
    arbitrary_file = normalized_dir / "medidor_results.csv"
    arbitrary_file.write_text("instalacao,tipo\n1,Digital\n", encoding="utf-8")

    resolved = service_module._resolve_medidor_final_csv_path(
        output_root=tmp_path,
        final_csv_path=None,
        source_run_id=None,
        run_id=run_id,
    )

    assert resolved == arbitrary_file.resolve()


def test_resolve_medidor_final_csv_path_uses_manifest_final_csv_path(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    metadata_dir = tmp_path / "runs" / run_id / "medidor" / "metadata"
    metadata_dir.mkdir(parents=True)
    final_file = tmp_path / "runs" / run_id / "medidor" / "custom" / "medidor_resultado.csv"
    final_file.parent.mkdir(parents=True)
    final_file.write_text("instalacao,equipamento,grp_reg,tipo\n1,EQ,G,Digital\n", encoding="utf-8")
    manifest_file = metadata_dir / f"medidor_20260424_{run_id}.manifest.json"
    manifest_file.write_text(f'{{"final_csv_path": "{final_file}"}}', encoding="utf-8")

    resolved = service_module._resolve_medidor_final_csv_path(
        output_root=tmp_path,
        final_csv_path=None,
        source_run_id=None,
        run_id=run_id,
    )

    assert resolved == final_file.resolve()


def test_resolve_medidor_final_csv_path_ignores_installations_input_csv(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    input_dir = tmp_path / "runs" / run_id / "medidor" / "input"
    normalized_dir = tmp_path / "runs" / run_id / "medidor" / "normalized"
    input_dir.mkdir(parents=True)
    normalized_dir.mkdir(parents=True)
    (input_dir / "MEDIDOR_INSTALLATIONS.csv").write_text("INSTALACAO\n1\n", encoding="utf-8")
    final_file = normalized_dir / f"medidor_20260424_{run_id}.csv"
    final_file.write_text("instalacao,equipamento,grp_reg,tipo\n1,EQ,G,Digital\n", encoding="utf-8")

    resolved = service_module._resolve_medidor_final_csv_path(
        output_root=tmp_path,
        final_csv_path=None,
        source_run_id=None,
        run_id=run_id,
    )

    assert resolved == final_file.resolve()


def test_resolve_medidor_final_csv_path_raises_file_not_found(tmp_path: Path) -> None:
    run_id = "MEDIDOR_SP_EXTRACT"
    normalized_dir = tmp_path / "runs" / run_id / "medidor" / "normalized"
    normalized_dir.mkdir(parents=True)

    import pytest

    with pytest.raises(FileNotFoundError, match="MEDIDOR final CSV not found"):
        service_module._resolve_medidor_final_csv_path(
            output_root=tmp_path,
            final_csv_path=None,
            source_run_id=None,
            run_id=run_id,
        )
