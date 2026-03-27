from __future__ import annotations

import csv
from pathlib import Path

from sap_automation.dw import (
    DwSessionLocator,
    DwWorkItem,
    _normalize_transaction_code,
    _session_locator_from_session,
    ensure_sap_sessions,
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


class _FakeWindow:
    def maximize(self) -> None:
        return None

    def sendVKey(self, value: int) -> None:
        return None


class _FakeSessionsCollection:
    """Mimics the SAP GUI non-blocking Sessions collection."""

    def __init__(self, sessions_list: list[object]) -> None:
        self._sessions = sessions_list

    @property
    def Count(self) -> int:
        return len(self._sessions)

    def __call__(self, index: int) -> object:
        return self._sessions[index]


class _FakeConnection:
    def __init__(self, sessions: list[object]) -> None:
        self.sessions = sessions

    @property
    def Sessions(self) -> _FakeSessionsCollection:
        return _FakeSessionsCollection(self.sessions)

    def Children(self, index: int) -> object:
        return self.sessions[index]


class _ManagedFakeSession:
    def __init__(self, session_id: str, connection: _FakeConnection | None = None) -> None:
        self.Id = session_id
        self.Parent = connection
        self._window = _FakeWindow()

    def findById(self, item_id: str) -> object:
        if item_id == "wnd[0]":
            return self._window
        raise KeyError(item_id)

    def CreateSession(self) -> None:
        assert self.Parent is not None
        next_index = len(self.Parent.sessions)
        new_session = _ManagedFakeSession(f"/app/con[0]/ses[{next_index}]", self.Parent)
        self.Parent.sessions.append(new_session)


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


def test_normalize_transaction_code_forces_navigational_prefix() -> None:
    assert _normalize_transaction_code("IW53") == "/nIW53"
    assert _normalize_transaction_code("/nIW53") == "/nIW53"


def test_ensure_sap_sessions_registers_slots_in_creation_order(monkeypatch) -> None:  # noqa: ANN001
    connection = _FakeConnection([])
    base_session = _ManagedFakeSession("/app/con[0]/ses[0]", connection)
    connection.sessions.append(base_session)
    log_messages: list[str] = []

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: session)

    logger = type(
        "Logger",
        (),
        {"info": lambda self, message, *args: log_messages.append(message % args if args else message)},
    )()

    sessions = ensure_sap_sessions(
        base_session=base_session,
        session_count=3,
        logger=logger,
        wait_timeout_seconds=5.0,
    )

    assert [session.Id for session in sessions] == [
        "/app/con[0]/ses[0]",
        "/app/con[0]/ses[1]",
        "/app/con[0]/ses[2]",
    ]
    assert any("slot=1" in message for message in log_messages)
    assert any("slot=2" in message for message in log_messages)
    assert any("slot=3" in message for message in log_messages)
