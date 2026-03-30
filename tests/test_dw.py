from __future__ import annotations

import csv
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sap_automation.dw import (
    DwItemResult,
    DwSettings,
    DwSessionLocator,
    DwWorkerState,
    DwWorkItem,
    _is_session_disconnected_error,
    _ensure_dw_selection_screen,
    _dismiss_popup_if_present,
    _DW_TEXT_LINE_TEMPLATE,
    _DW_TEXT_TABLE_ID,
    _normalize_transaction_code,
    _session_locator_from_session,
    _wait_for_control,
    _worker_run,
    extract_observacao_text,
    execute_dw_item,
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
    def __init__(self) -> None:
        self.maximize_calls = 0
        self.vkeys: list[int] = []
        self.Type = "GuiMainWindow"

    def maximize(self) -> None:
        self.maximize_calls += 1

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)


class _FakePopupWindow:
    def __init__(self) -> None:
        self.Type = "GuiModalWindow"
        self.vkeys: list[int] = []

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)


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
        self.ActiveWindow = self._window
        self._okcd = _ExecuteField()

    def findById(self, item_id: str) -> object:
        if item_id == "wnd[0]":
            return self._window
        if item_id == "wnd[0]/tbar[0]/okcd":
            return self._okcd
        raise KeyError(item_id)

    def CreateSession(self) -> None:
        assert self.Parent is not None
        next_index = len(self.Parent.sessions)
        new_session = _ManagedFakeSession(f"/app/con[0]/ses[{next_index}]", self.Parent)
        self.Parent.sessions.append(new_session)


class _ExecuteField:
    def __init__(self) -> None:
        self.text = ""
        self.caretPosition = 0


class _Pressable:
    def press(self) -> None:
        return None

    def select(self) -> None:
        return None


class _ExecuteSession:
    def __init__(self) -> None:
        self.main_window = _FakeWindow()
        self.complaint_item = _ExecuteField()
        self.active_window = self.main_window

    @property
    def ActiveWindow(self) -> _FakeWindow:
        return self.active_window

    def findById(self, item_id: str) -> object:
        mapping = {
            "wnd[0]": self.main_window,
            "wnd[0]/tbar[0]/okcd": _ExecuteField(),
            "wnd[0]/usr/ctxtQMNUM-LOW": self.complaint_item,
            "wnd[0]/usr/ctxtRIWO00-QMNUM": self.complaint_item,
            "wnd[0]/tbar[1]/btn[8]": _Pressable(),
            "wnd[0]/tbar[1]/btn[5]": _Pressable(),
            r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02": _Pressable(),
            (
                r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB02/ssubSUB_GROUP_10:SAPLIQS0:7235/"
                r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_2:SAPLIQS0:7710/tblSAPLIQS0TEXT"
            ): _Pressable(),
            "wnd[0]/tbar[0]/btn[3]": _Pressable(),
        }
        if item_id not in mapping:
            raise KeyError(item_id)
        return mapping[item_id]


class _DelayedControlSession:
    def __init__(self, succeed_after: int) -> None:
        self._remaining_failures = succeed_after
        self.Id = "/app/con[0]/ses[0]"
        self.ActiveWindow = _FakeWindow()

    def findById(self, item_id: str) -> object:
        if item_id in {"wnd[0]/usr/ctxtQMNUM-LOW", "wnd[0]/usr/ctxtRIWO00-QMNUM", "wnd[0]/sbar", "wnd[0]/sbar/pane[0]"}:
            if self._remaining_failures > 0:
                self._remaining_failures -= 1
                raise KeyError(item_id)
            return _ExecuteField()
        raise KeyError(item_id)


class _SelectionBootstrapSession:
    def __init__(self) -> None:
        self.Id = "/app/con[0]/ses[0]"
        self.ActiveWindow = _FakeWindow()
        self.okcd = _ExecuteField()
        self._field_visible = False

    def findById(self, item_id: str) -> object:
        if item_id == "wnd[0]":
            return self.ActiveWindow
        if item_id == "wnd[0]/tbar[0]/okcd":
            return self.okcd
        if item_id == "wnd[0]/usr/ctxtQMNUM-LOW":
            if not self._field_visible:
                raise KeyError(item_id)
            return _ExecuteField()
        raise KeyError(item_id)


class _TextCell:
    def __init__(self, text: str) -> None:
        self.Text = text


class _ScrollRecorder:
    def __init__(self) -> None:
        self.assignments: list[int] = []
        self._position = 0
        self.Maximum = 10

    @property
    def position(self) -> int:
        return self._position

    @position.setter
    def position(self, value: int) -> None:
        self._position = value
        self.assignments.append(value)


class _TextTable:
    def __init__(self, scrollbar: _ScrollRecorder) -> None:
        self.verticalScrollbar = scrollbar


class _ObservacaoSession:
    def __init__(self) -> None:
        self.scrollbar = _ScrollRecorder()
        self.table = _TextTable(self.scrollbar)
        self.blocks = {
            0: ["linha 0", "linha 1"],
            1: ["linha 1", "linha 2"],
            2: ["linha 2", "linha 3"],
            3: ["linha 3", "linha 4"],
            4: ["linha 4", "linha 5"],
        }

    def findById(self, item_id: str) -> object:
        if item_id == _DW_TEXT_TABLE_ID:
            return self.table
        current_rows = self.blocks.get(self.scrollbar.position, [])
        for row_index, value in enumerate(current_rows):
            if item_id == _DW_TEXT_LINE_TEMPLATE.format(row=row_index):
                return _TextCell(value)
        raise KeyError(item_id)


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
    assert settings.parallel_mode is True
    assert settings.circuit_breaker_fast_fail_threshold == 10
    assert settings.circuit_breaker_slow_fail_threshold == 30
    assert settings.per_step_timeout_transaction_seconds == 30.0
    assert settings.per_step_timeout_query_seconds == 180.0
    assert settings.session_recovery_mode == "soft"


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


def test_session_disconnected_error_accepts_rpc_unavailable() -> None:
    exc = RuntimeError("(-2147023174, 'O servidor RPC não está disponível.', None, None)")

    assert _is_session_disconnected_error(exc) is True


def test_dismiss_popup_returns_false_when_no_popup() -> None:
    session = _ExecuteSession()

    assert _dismiss_popup_if_present(session, logger=type("Logger", (), {"warning": lambda *args, **kwargs: None, "info": lambda *args, **kwargs: None})()) is False


def test_dismiss_popup_dismisses_info_dialog() -> None:
    popup = _FakePopupWindow()

    class PopupSession(_ExecuteSession):
        def findById(self, item_id: str) -> object:
            if item_id == "wnd[1]":
                return popup
            return super().findById(item_id)

    session = PopupSession()
    logger = type("Logger", (), {"warning": lambda *args, **kwargs: None, "info": lambda *args, **kwargs: None})()

    assert _dismiss_popup_if_present(session, logger=logger) is True
    assert popup.vkeys == [0]


def test_wait_for_control_retries_until_control_is_available() -> None:
    session = _DelayedControlSession(succeed_after=2)
    logger = type("Logger", (), {"warning": lambda *args, **kwargs: None, "info": lambda *args, **kwargs: None})()

    control = _wait_for_control(
        session=session,
        ids=["wnd[0]/usr/ctxtQMNUM-LOW"],
        timeout_seconds=1.0,
        logger=logger,
        description="iw53.selection_screen",
        worker_index=1,
        item=DwWorkItem(row_index=2, complaint_id="389744083"),
    )

    assert isinstance(control, _ExecuteField)


def test_wait_for_control_accepts_alternate_qmnum_field_id() -> None:
    session = _DelayedControlSession(succeed_after=0)
    logger = type("Logger", (), {"warning": lambda *args, **kwargs: None, "info": lambda *args, **kwargs: None})()

    control = _wait_for_control(
        session=session,
        ids=["wnd[0]/usr/ctxtQMNUM-LOW", "wnd[0]/usr/ctxtRIWO00-QMNUM"],
        timeout_seconds=1.0,
        logger=logger,
        description="iw53.selection_screen",
        worker_index=1,
        item=DwWorkItem(row_index=2, complaint_id="389744083"),
    )

    assert isinstance(control, _ExecuteField)


def test_extract_observacao_text_reads_only_script_scroll_window(monkeypatch) -> None:  # noqa: ANN001
    session = _ObservacaoSession()

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: None)

    result = extract_observacao_text(
        session=session,
        wait_timeout_seconds=10.0,
    )

    assert result == "\n".join(["linha 0", "linha 1", "linha 2", "linha 3", "linha 4", "linha 5"])
    assert session.scrollbar.assignments == [1, 2, 3, 4]


def test_ensure_dw_selection_screen_can_navigate_once_when_field_is_missing(monkeypatch) -> None:  # noqa: ANN001
    session = _SelectionBootstrapSession()
    logger = type("Logger", (), {"warning": lambda *args, **kwargs: None, "info": lambda *args, **kwargs: None})()

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: None)
    monkeypatch.setattr("sap_automation.dw._dismiss_popup_if_present", lambda session, logger: False)
    original_send = session.ActiveWindow.sendVKey

    def _send_and_expose(value: int) -> None:
        original_send(value)
        session._field_visible = True

    session.ActiveWindow.sendVKey = _send_and_expose  # type: ignore[method-assign]

    control = _ensure_dw_selection_screen(
        session=session,
        settings=DwSettings(
            demandante="DW",
            input_path=Path("base.csv"),
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=10,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=6.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
        ),
        logger=logger,
        worker_index=1,
        item=DwWorkItem(row_index=2, complaint_id="389744083"),
        allow_navigation=True,
    )

    assert isinstance(control, _ExecuteField)
    assert session.okcd.text == "/nIW53"


def test_execute_dw_item_does_not_call_maximize(monkeypatch) -> None:  # noqa: ANN001
    session = _ExecuteSession()
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None})()
    refresh_calls: list[DwSessionLocator] = []

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: None)
    monkeypatch.setattr("sap_automation.dw._dismiss_popup_if_present", lambda session, logger: False)
    monkeypatch.setattr("sap_automation.dw.extract_observacao_text", lambda *, session, wait_timeout_seconds: "obs")
    monkeypatch.setattr("sap_automation.dw._wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr(
        "sap_automation.dw._reattach_dw_session",
        lambda *, locator, logger, application=None: (refresh_calls.append(locator) or session),
    )

    result = execute_dw_item(
        session=session,
        session_locator=DwSessionLocator(connection_index=0, session_index=0),
        application=object(),
        item=DwWorkItem(row_index=2, complaint_id="389744083"),
        settings=DwSettings(
            demandante="DW",
            input_path=Path("base.csv"),
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=10,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=6.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
        ),
        logger=logger,
        worker_index=1,
    )

    assert result == "obs"
    assert session.main_window.maximize_calls == 0
    assert refresh_calls == [
        DwSessionLocator(connection_index=0, session_index=0),
        DwSessionLocator(connection_index=0, session_index=0),
    ]


def test_worker_run_stops_when_cancel_event_is_set(monkeypatch) -> None:  # noqa: ANN001
    worker_state = DwWorkerState(worker_index=1, session_id="/app/con[0]/ses[0]")
    cancel_event = threading.Event()
    cancel_event.set()
    monkeypatch.setattr("sap_automation.dw._co_initialize", lambda: (None, False))
    monkeypatch.setattr("sap_automation.dw._reattach_dw_session", lambda locator, logger, application=None: object())
    monkeypatch.setattr("sap_automation.dw._wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr("sap_automation.dw._ensure_dw_selection_screen", lambda **kwargs: _ExecuteField())

    results, failures = _worker_run(
        worker_index=1,
        session_locator=DwSessionLocator(connection_index=0, session_index=0),
        items=[DwWorkItem(row_index=2, complaint_id="1"), DwWorkItem(row_index=3, complaint_id="2")],
        settings=DwSettings(
            demandante="DW",
            input_path=Path("base.csv"),
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=10,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=6.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
        ),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None, "error": lambda *args, **kwargs: None})(),
        worker_state=worker_state,
        cancel_event=cancel_event,
    )

    assert results == []
    assert len(failures) == 2
    assert worker_state.status == "failed"


def test_worker_state_updates_during_processing(monkeypatch) -> None:  # noqa: ANN001
    worker_state = DwWorkerState(worker_index=2, session_id="/app/con[0]/ses[1]")
    monkeypatch.setattr("sap_automation.dw._co_initialize", lambda: (None, False))
    monkeypatch.setattr("sap_automation.dw._reattach_dw_session", lambda locator, logger, application=None: object())
    monkeypatch.setattr("sap_automation.dw._wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr("sap_automation.dw._ensure_dw_selection_screen", lambda **kwargs: _ExecuteField())
    monkeypatch.setattr(
        "sap_automation.dw._process_dw_item_with_retry",
        lambda **kwargs: (
            DwItemResult(
                row_index=kwargs["item"].row_index,
                complaint_id=kwargs["item"].complaint_id,
                observacao=f"obs-{kwargs['item'].complaint_id}",
                worker_index=kwargs["worker_index"],
                elapsed_seconds=0.1,
            ),
            None,
            kwargs["session"],
        ),
    )

    results, failures = _worker_run(
        worker_index=2,
        session_locator=DwSessionLocator(connection_index=0, session_index=1),
        items=[DwWorkItem(row_index=2, complaint_id="10"), DwWorkItem(row_index=3, complaint_id="11")],
        settings=DwSettings(
            demandante="DW",
            input_path=Path("base.csv"),
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=10,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=6.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
        ),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None, "error": lambda *args, **kwargs: None})(),
        worker_state=worker_state,
    )

    assert len(results) == 2
    assert failures == []
    assert worker_state.status == "completed"
    assert worker_state.items_total == 2
    assert worker_state.items_processed == 2
    assert worker_state.items_ok == 2
    assert worker_state.items_failed == 0
    assert worker_state.last_ok_complaint_id == "11"
    assert worker_state.current_complaint_id == ""
    assert worker_state.elapsed_seconds >= 0.0


def test_parallel_workers_do_not_interfere(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("sap_automation.dw._co_initialize", lambda: (None, False))
    monkeypatch.setattr("sap_automation.dw._wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr("sap_automation.dw._ensure_dw_selection_screen", lambda **kwargs: _ExecuteField())
    monkeypatch.setattr(
        "sap_automation.dw._reattach_dw_session",
        lambda locator, logger, application=None: type("Session", (), {"Id": f"/app/con[{locator.connection_index}]/ses[{locator.session_index}]"})(),
    )
    monkeypatch.setattr(
        "sap_automation.dw._process_dw_item_with_retry",
        lambda **kwargs: (
            DwItemResult(
                row_index=kwargs["item"].row_index,
                complaint_id=kwargs["item"].complaint_id,
                observacao=f"worker-{kwargs['worker_index']}-{kwargs['item'].complaint_id}",
                worker_index=kwargs["worker_index"],
                elapsed_seconds=0.1,
            ),
            None,
            kwargs["session"],
        ),
    )

    settings = DwSettings(
        demandante="DW",
        input_path=Path("base.csv"),
        input_encoding="cp1252",
        delimiter="\t",
        id_column="ID Reclamação",
        output_column="OBSERVAÇÃO",
        transaction_code="IW53",
        session_count=3,
        max_rows_per_run=10,
        wait_timeout_seconds=120.0,
        post_login_wait_seconds=6.0,
        parallel_mode=True,
        circuit_breaker_fast_fail_threshold=10,
        circuit_breaker_slow_fail_threshold=30,
        per_step_timeout_transaction_seconds=30.0,
        per_step_timeout_query_seconds=180.0,
        session_recovery_mode="soft",
    )
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None, "error": lambda *args, **kwargs: None})()
    worker_states = [
        DwWorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
        DwWorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
        DwWorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
    ]
    groups = [
        [DwWorkItem(row_index=2, complaint_id="a"), DwWorkItem(row_index=5, complaint_id="d")],
        [DwWorkItem(row_index=3, complaint_id="b")],
        [DwWorkItem(row_index=4, complaint_id="c")],
    ]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                _worker_run,
                worker_index=index,
                session_locator=DwSessionLocator(connection_index=0, session_index=index - 1),
                items=group,
                settings=settings,
                logger=logger,
                worker_state=worker_states[index - 1],
            )
            for index, group in enumerate(groups, start=1)
        ]

    merged_results: list[DwItemResult] = []
    for future in futures:
        results, failures = future.result()
        assert failures == []
        merged_results.extend(results)

    assert sorted(result.observacao for result in merged_results) == [
        "worker-1-a",
        "worker-1-d",
        "worker-2-b",
        "worker-3-c",
    ]
    assert [state.status for state in worker_states] == ["completed", "completed", "completed"]


def test_csv_write_thread_safe(tmp_path: Path) -> None:
    csv_path = tmp_path / "base.csv"
    header = ["ID Reclamação", "OBSERVAÇÃO"]
    rows = [["1", ""], ["2", ""], ["3", ""]]
    write_dw_csv(
        input_path=csv_path,
        encoding="cp1252",
        delimiter="\t",
        header=header,
        data_rows=rows,
    )

    lock = threading.Lock()

    def _writer(row_index: int, value: str) -> None:
        with lock:
            rows[row_index][1] = value
            write_dw_csv(
                input_path=csv_path,
                encoding="cp1252",
                delimiter="\t",
                header=header,
                data_rows=rows,
            )

    threads = [
        threading.Thread(target=_writer, args=(0, "obs-1")),
        threading.Thread(target=_writer, args=(1, "obs-2")),
        threading.Thread(target=_writer, args=(2, "obs-3")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    content = csv_path.read_text(encoding="cp1252")
    assert "1\tobs-1" in content
    assert "2\tobs-2" in content
    assert "3\tobs-3" in content


def test_ensure_sap_sessions_registers_slots_in_creation_order(monkeypatch) -> None:  # noqa: ANN001
    connection = _FakeConnection([])
    base_session = _ManagedFakeSession("/app/con[0]/ses[0]", connection)
    connection.sessions.append(base_session)
    log_messages: list[str] = []

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: session)

    logger = type(
        "Logger",
        (),
        {
            "info": lambda self, message, *args: log_messages.append(message % args if args else message),
            "warning": lambda self, message, *args: log_messages.append(message % args if args else message),
        },
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
