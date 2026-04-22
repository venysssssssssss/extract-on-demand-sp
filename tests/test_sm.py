from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select

from sap_automation.sm import _build_sm_final_rows, _extract_column_from_txt, _read_sap_txt_as_dicts, _write_sm_final_csv, ingest_sm_results, run_sm_demandante
from sap_automation.sm_repository import SmRepository, sm_dados_fatura


@pytest.fixture
def mock_config():
    return {
        "global": {
            "database_url": "sqlite:///:memory:",
            "wait_timeout_seconds": 10,
        },
        "sm": {
            "default_demandante": "SALA_MERCADO",
            "transaction_code": "ZISU0128",
            "chunk_size": 2,
            "demandantes": {
                "SALA_MERCADO": {
                    "sqvi_1": {
                        "report": "REP1",
                        "export_filename": "export1_{chunk_index}.txt",
                        "steps": [{"action": "press", "id": "btn1"}],
                    },
                    "sqvi_2": {
                        "report": "REP2",
                        "export_filename": "export2_{chunk_index}.txt",
                        "steps": [{"action": "press", "id": "btn2"}],
                    },
                }
            },
        },
    }


def test_extract_column_from_txt(tmp_path):
    file_path = tmp_path / "test.txt"
    # SAP exported TXT usually uses tabs
    content = "Header1\tDoc.impr.\tHeader3\nVal1\tDOC123\tVal3\nVal4\tDOC456\tVal6"
    file_path.write_text(content, encoding="utf-8")

    with patch("sap_gui_export_compat._read_delimited_text") as mock_read:
        mock_read.return_value = ("utf-8", "\t", [["Header1", "Doc.impr.", "Header3"], ["Val1", "DOC123", "Val3"], ["Val4", "DOC456", "Val6"]])
        results = _extract_column_from_txt(file_path, "Doc.impr.")
    
    assert results == ["DOC123", "DOC456"]


def test_read_sap_txt_skips_dynamic_list_title_lines(tmp_path: Path) -> None:
    file_path = tmp_path / "SM_SQVI1_1.txt"
    file_path.write_text(
        "\n".join(
            [
                "22.04.2026                   Saída dinâmica de lista                           1",
                "",
                "22.04.2026\t\t\tZUCRM_OT138_NTFT - Faturas associadas a nota de reclamação\t\t\t\t  1",
                "",
                "ZUCRM_OT138_NTFT - Faturas associadas a nota de reclamação",
                "",
                "\t\tNota\t\tDoc.impr.\t      Montante\tDtFxCálcFat",
                "\t\t1001\t\t9001\t      50,00\t22.04.2026",
                "\t\t1002\t\t9002\t      75,00\t22.04.2026",
            ]
        ),
        encoding="cp1252",
    )

    rows = _read_sap_txt_as_dicts(file_path)

    assert rows == [
        {"Nota": "1001", "Doc.impr.": "9001", "Montante": "50,00", "DtFxCálcFat": "22.04.2026"},
        {"Nota": "1002", "Doc.impr.": "9002", "Montante": "75,00", "DtFxCálcFat": "22.04.2026"},
    ]
    assert _extract_column_from_txt(file_path, "Doc.impr.") == ["9001", "9002"]


@patch("sap_automation.sm.SmRepository")
@patch("sap_automation.sm.load_export_config")
@patch("sap_automation.sm.configure_run_logger")
@patch("sap_automation.sm._extract_column_from_txt")
@patch("sap_automation.sm._read_sap_txt_as_dicts")
def test_run_sm_demandante(
    mock_read_dicts,
    mock_extract_col,
    mock_logger,
    mock_load_config,
    mock_repo_cls,
    mock_config,
    tmp_path,
):
    run_id = "test_run"
    demandante = "SALA_MERCADO"
    output_root = tmp_path
    config_path = Path("config.json")

    mock_load_config.return_value = mock_config
    mock_logger.return_value = (MagicMock(), Path("log.txt"))
    
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_installations_to_process.return_value = ["INST1", "INST2", "INST3"]
    
    # Chunk size is 2, so 2 chunks: ["INST1", "INST2"] and ["INST3"]
    # Mock extract_column_from_txt for SQVI 1
    mock_extract_col.side_effect = [["DOC1", "DOC2"], ["DOC3"]]
    
    mock_read_dicts.side_effect = [
        [
            {"Nota": "N1", "Doc.impr.": "DOC1", "Montante": "10", "DtFxCálcFat": "01.04.2026"},
            {"Nota": "N2", "Doc.impr.": "DOC2", "Montante": "20", "DtFxCálcFat": "02.04.2026"},
        ],
        [{"Nota": "N3", "Doc.impr.": "DOC3", "Montante": "30", "DtFxCálcFat": "03.04.2026"}],
        [{"Doc.impr.": "DOC1", "Fatura": "F1"}, {"Doc.impr.": "DOC2", "Fatura": "F2"}],
        [{"Doc.impr.": "DOC3", "Fatura": "F3"}, {"Doc.impr.": "DOC4", "Fatura": "F4"}],
    ]

    mock_session = MagicMock()
    mock_session_provider = MagicMock()
    mock_session_provider.get_session.return_value = mock_session

    with patch("sap_automation.execution.StepExecutor") as mock_executor_cls:
        manifest = run_sm_demandante(
            run_id=run_id,
            demandante=demandante,
            output_root=output_root,
            config_path=config_path,
            session_provider=mock_session_provider,
        )

    assert manifest.status == "success"
    assert manifest.processed_chunks == 2
    assert manifest.rows_extracted_sqvi1 == 3
    assert manifest.rows_extracted_sqvi2 == 4
    assert manifest.final_rows == 4
    assert Path(manifest.final_csv_path).exists()
    execute_calls = mock_executor_cls.return_value.execute.call_args_list
    assert execute_calls[0].kwargs["steps"][0]["values"] == ["INST1", "INST2"]
    assert execute_calls[1].kwargs["steps"][0]["values"] == ["INST3"]
    assert execute_calls[2].kwargs["steps"][0]["values"] == ["DOC1", "DOC2"]
    assert execute_calls[3].kwargs["steps"][0]["values"] == ["DOC3"]
    
    # Verify DB save was called
    assert mock_repo.save_final_results.called
    args, kwargs = mock_repo.save_final_results.call_args
    assert args[0] == run_id
    assert len(args[1]) == 4
    assert kwargs["distribuidora"] == "São Paulo"


@patch("sap_automation.sm.SmRepository")
@patch("sap_automation.sm.load_export_config")
def test_run_sm_no_installations(mock_load_config, mock_repo_cls, mock_config, tmp_path):
    mock_load_config.return_value = mock_config
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_installations_to_process.return_value = []

    manifest = run_sm_demandante(
        run_id="run_empty",
        demandante="SALA_MERCADO",
        output_root=tmp_path,
        config_path=Path("config.json"),
    )

    assert manifest.status == "skipped"
    assert manifest.total_chunks == 0
    assert Path(manifest.manifest_path).exists()


def test_build_sm_final_rows_merges_sqvi_payloads_by_doc_impr() -> None:
    rows = _build_sm_final_rows(
        chunk_index=1,
        sqvi1_rows=[
            {"Nota": "N1", "Doc.impr.": "D1", "Montante": "100", "DtFxCálcFat": "01.04.2026"},
            {"Nota": "N2", "Doc.impr.": "D2", "Montante": "200", "DtFxCálcFat": "02.04.2026"},
        ],
        sqvi2_rows=[{"Doc.impr.": "D1", "Conta contrato": "CC1"}],
    )

    assert rows[0]["doc_impr"] == "D1"
    assert rows[0]["nota"] == "N1"
    assert rows[0]["sqvi2"]["Conta contrato"] == "CC1"
    assert rows[1]["doc_impr"] == "D2"
    assert rows[1]["extraction_status"] == "sqvi2_missing"


def test_sm_repository_saves_structured_rows_idempotently(tmp_path: Path) -> None:
    db_url = f"sqlite+pysqlite:///{(tmp_path / 'sm.db').as_posix()}"
    repository = SmRepository(db_url)

    result = {
        "chunk_index": 1,
        "nota": "N1",
        "doc_impr": "D1",
        "montante": "100",
        "dt_fx_calc_fat": "01.04.2026",
        "extraction_status": "success",
        "sqvi1": {"Nota": "N1", "Doc.impr.": "D1"},
        "sqvi2": {"Conta contrato": "CC1"},
    }
    repository.save_final_results("run-sm", [result], month=4, year=2026, distribuidora="São Paulo")
    repository.save_final_results("run-sm", [result], month=4, year=2026, distribuidora="São Paulo")

    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(select(sm_dados_fatura)).mappings().all()

    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-sm"
    assert rows[0]["doc_impr"] == "D1"
    assert rows[0]["mes_referencia"] == 4


def test_ingest_sm_results_reads_final_csv_by_source_run_id(tmp_path: Path) -> None:
    db_path = tmp_path / "sm.db"
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"global":{"database_url":"sqlite+pysqlite:///' + db_path.as_posix() + '"}}',
        encoding="utf-8",
    )
    final_csv_path = tmp_path / "runs" / "EXT_SAP_VIA_CSV" / "sm" / "SM_DADOS_FATURA.csv"
    _write_sm_final_csv(
        final_csv_path,
        [
            {
                "chunk_index": 1,
                "nota": "N1",
                "doc_impr": "D1",
                "montante": "100",
                "dt_fx_calc_fat": "01.04.2026",
                "extraction_status": "success",
                "sqvi1": {"Nota": "N1", "Doc.impr.": "D1"},
                "sqvi2": {"Conta contrato": "CC1"},
            }
        ],
    )

    result = ingest_sm_results(
        run_id="EXT_SAP_VIA_CSV",
        output_root=tmp_path,
        config_path=config_path,
        source_run_id="EXT_SAP_VIA_CSV",
        month=4,
        year=2026,
        distribuidora="São Paulo",
    )

    assert result.status == "success"
    assert result.rows_ingested == 1
    assert result.source_csv_path == str(final_csv_path)

    engine = create_engine(f"sqlite+pysqlite:///{db_path.as_posix()}")
    with engine.connect() as conn:
        rows = conn.execute(select(sm_dados_fatura)).mappings().all()

    assert len(rows) == 1
    assert rows[0]["run_id"] == "EXT_SAP_VIA_CSV"
    assert rows[0]["doc_impr"] == "D1"
