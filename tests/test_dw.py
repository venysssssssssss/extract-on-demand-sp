from __future__ import annotations

import csv
from pathlib import Path

from sap_automation.dw import (
    DwSessionLocator,
    DwWorkItem,
    _session_locator_from_session,
    load_dw_settings,
    load_dw_work_items,
    prepare_dw_sessions,
    split_work_items_evenly,
    write_dw_csv,
)


def _write_tab_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="cp1252", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


class _FakeSession:
    def __init__(self, session_id: str) -> None:
        self.Id = session_id


def test_load_dw_settings_resolves_dw_profile(tmp_path: Path) -> None:
    (tmp_path / "base.csv").write_text("ID Reclamação\tAssunto\n", encoding="cp1252")
    settings = load_dw_settings(
        config={
            "global": {"wait_timeout_seconds": 120.0},
            "dw": {
                "demandantes": {
                    "DW": {
                        "input_path": "base.csv",
                        "input_encoding": "cp1252",
                        "delimiter": "\t",
                        "id_column": "ID Reclamação",
                        "output_column": "OBSERVAÇÃO",
                        "transaction_code": "IW53",
                        "session_count": 3,
                        "max_rows_per_run": 10,
                    }
                }
            },
        },
        config_path=tmp_path / "config.json",
        demandante="dw",
    )

    assert settings.demandante == "DW"
    assert settings.input_path.name == "base.csv"
    assert settings.output_column == "OBSERVAÇÃO"
    assert settings.session_count == 3
    assert settings.post_login_wait_seconds == 6.0


def test_load_dw_work_items_adds_observacao_and_skips_completed(tmp_path: Path) -> None:
    csv_path = tmp_path / "base.csv"
    _write_tab_csv(
        csv_path,
        ["ID Reclamação", "Assunto"],
        [
            ["389744244", "A"],
            ["389744245", "B"],
        ],
    )

    header, rows, output_index, items, skipped_rows = load_dw_work_items(
        input_path=csv_path,
        encoding="cp1252",
        delimiter="\t",
        id_column="ID Reclamação",
        output_column="OBSERVAÇÃO",
    )

    assert header[-1] == "OBSERVAÇÃO"
    assert output_index == 2
    assert skipped_rows == 0
    assert [item.complaint_id for item in items] == ["389744244", "389744245"]
    assert rows[0][output_index] == ""


def test_write_dw_csv_persists_observacao_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "base.csv"
    header = ["ID Reclamação", "OBSERVAÇÃO"]
    rows = [["389744244", "linha 1"], ["389744245", "linha 2"]]

    write_dw_csv(
        input_path=csv_path,
        encoding="cp1252",
        delimiter="\t",
        header=header,
        data_rows=rows,
    )

    content = csv_path.read_text(encoding="cp1252")
    assert "OBSERVAÇÃO" in content
    assert "389744244\tlinha 1" in content


def test_split_work_items_evenly_distributes_items_round_robin() -> None:
    groups = split_work_items_evenly(
        [DwWorkItem(row_index=index + 2, complaint_id=str(index)) for index in range(7)],
        session_count=3,
    )

    assert [[item.complaint_id for item in group] for group in groups] == [
        ["0", "3", "6"],
        ["1", "4"],
        ["2", "5"],
    ]


def test_prepare_dw_sessions_waits_before_opening_new_sessions(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    sleep_calls: list[float] = []
    ensure_calls: list[tuple[object, int, float]] = []
    (tmp_path / "base.csv").write_text("ID Reclamação\tAssunto\n", encoding="cp1252")

    monkeypatch.setattr("sap_automation.dw.time.sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr(
        "sap_automation.dw.ensure_sap_sessions",
        lambda *, base_session, session_count, logger, wait_timeout_seconds: (
            ensure_calls.append((base_session, session_count, wait_timeout_seconds)) or [base_session]
        ),
    )

    base_session = object()
    settings = load_dw_settings(
        config={
            "global": {"wait_timeout_seconds": 120.0},
            "dw": {
                "demandantes": {
                    "DW": {
                        "input_path": "base.csv",
                        "input_encoding": "cp1252",
                        "delimiter": "\t",
                        "id_column": "ID Reclamação",
                        "output_column": "OBSERVAÇÃO",
                        "transaction_code": "IW53",
                        "session_count": 3,
                        "post_login_wait_seconds": 6,
                        "max_rows_per_run": 10,
                    }
                }
            },
        },
        config_path=tmp_path / "config.json",
        demandante="DW",
    )

    sessions = prepare_dw_sessions(
        base_session=base_session,
        settings=settings,
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
    )

    assert sleep_calls == [6.0]
    assert ensure_calls == [(base_session, 3, 120.0)]
    assert sessions == [base_session]


def test_session_locator_from_session_parses_connection_and_session_indexes() -> None:
    locator = _session_locator_from_session(_FakeSession("/app/con[2]/ses[5]"))

    assert locator == DwSessionLocator(connection_index=2, session_index=5)
