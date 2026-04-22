from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sap_automation.sm import SmManifest, run_sm_demandante, _extract_column_from_txt


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


@patch("sap_automation.sm.SmRepository")
@patch("sap_automation.sm.load_export_config")
@patch("sap_automation.sm.configure_run_logger")
@patch("sap_automation.sm._extract_column_from_txt")
@patch("sap_gui_export_compat._read_delimited_text")
def test_run_sm_demandante(
    mock_read_delim,
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
    
    # Mock read_delimited_text for SQVI 2 (final results)
    mock_read_delim.return_value = ("utf-8", "\t", [["Col1", "Col2"], ["Res1", "Res2"], ["Res3", "Res4"]])

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
    # Total rows in final results: 2 rows from chunk 1 + 2 rows from chunk 2 = 4
    assert manifest.rows_extracted_sqvi2 == 4
    
    # Verify DB save was called
    assert mock_repo.save_final_results.called
    args, _ = mock_repo.save_final_results.call_args
    assert args[0] == run_id
    assert len(args[1]) == 4


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
