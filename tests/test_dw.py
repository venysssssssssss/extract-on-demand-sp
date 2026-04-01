from __future__ import annotations

import csv
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sap_automation.dw import (
    DwItemResult,
    DwLedgerState,
    DwSettings,
    DwSessionLocator,
    DwWorkerState,
    DwWorkItem,
    _build_dw_ledger_row_from_result,
    _load_dw_ledger_state,
    _reconcile_dw_items_from_ledger,
    _is_session_disconnected_error,
    _normalize_observacao_text,
    _process_dw_item_with_retry,
    _run_parallel_workers,
    _run_interleaved_workers,
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
    append_dw_progress_ledger,
    load_dw_settings,
    load_dw_work_items,
    prepare_dw_sessions,
    run_dw_demandante,
    split_work_items_evenly,
    write_dw_csv,
    write_dw_debug_csv,
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


class _OneShotScrollRecorder:
    def __init__(self, position_ref: list[int]) -> None:
        self._position_ref = position_ref
        self._used = False

    @property
    def position(self) -> int:
        return self._position_ref[0]

    @position.setter
    def position(self, value: int) -> None:
        if self._used:
            raise RuntimeError("stale scrollbar proxy")
        self._position_ref[0] = value
        self._used = True


class _ReacquiringObservacaoSession:
    def __init__(self) -> None:
        self.position_ref = [0]
        self.blocks = {
            0: ["linha 0", "linha 1"],
            1: ["linha 1", "linha 2"],
            2: ["linha 2", "linha 3"],
            3: ["linha 3", "linha 4"],
            4: ["linha 4", "linha 5"],
        }

    def findById(self, item_id: str) -> object:
        if item_id == _DW_TEXT_TABLE_ID:
            return _TextTable(_OneShotScrollRecorder(self.position_ref))
        current_rows = self.blocks.get(self.position_ref[0], [])
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
    assert settings.inter_item_sleep_seconds == 0.5
    assert settings.worker_start_stagger_seconds == 2.0


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


def test_write_dw_debug_csv_persists_simple_observation_rows(tmp_path: Path) -> None:
    debug_path = tmp_path / "dw_observacoes_debug.csv"

    write_dw_debug_csv(
        output_path=debug_path,
        rows=[
            DwItemResult(
                row_index=2,
                complaint_id="389744244",
                observacao="obs worker 1",
                worker_index=1,
                elapsed_seconds=1.2,
            ),
            DwItemResult(
                row_index=3,
                complaint_id="389744396",
                observacao="obs worker 2",
                worker_index=2,
                elapsed_seconds=1.3,
            ),
        ],
    )

    content = debug_path.read_text(encoding="utf-8")
    assert "worker,complaint_id,observacao" in content
    assert "1,389744244,obs worker 1" in content
    assert "2,389744396,obs worker 2" in content


def test_append_dw_progress_ledger_roundtrip_tracks_success_rows(tmp_path: Path) -> None:
    ledger_path = tmp_path / "dw_progress.csv"

    append_dw_progress_ledger(
        ledger_path=ledger_path,
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "complaint_id": "389744244",
                "status": "success",
                "elapsed_seconds": 1.2,
                "observacao": "obs-1",
                "error": "",
            },
            {
                "row_index": 3,
                "worker_index": 2,
                "complaint_id": "389744245",
                "status": "failed",
                "elapsed_seconds": 1.3,
                "observacao": "",
                "error": "boom",
            },
            {
                "row_index": 2,
                "worker_index": 1,
                "complaint_id": "389744244",
                "status": "success",
                "elapsed_seconds": 1.4,
                "observacao": "obs-1b",
                "error": "",
            },
        ],
    )

    ledger_state = _load_dw_ledger_state(ledger_path)

    assert ledger_state.total_entries == 3
    assert ledger_state.success_observacao_by_row == {2: "obs-1b"}


def test_reconcile_dw_items_from_ledger_updates_rows_and_skips_items() -> None:
    data_rows = [
        ["389744244", ""],
        ["389744245", ""],
    ]
    items = [
        DwWorkItem(row_index=2, complaint_id="389744244"),
        DwWorkItem(row_index=3, complaint_id="389744245"),
    ]

    remaining_items, imported_rows = _reconcile_dw_items_from_ledger(
        data_rows=data_rows,
        output_column_index=1,
        items=items,
        success_observacao_by_row={2: "obs-1"},
    )

    assert imported_rows == 1
    assert data_rows[0][1] == "obs-1"
    assert remaining_items == [DwWorkItem(row_index=3, complaint_id="389744245")]


def test_normalize_observacao_text_removes_sap_headers_and_joins_wrapped_lines() -> None:
    raw_text = (
        "22.03.2026 00:02:25 GMTUK Icaro Ryan Nascimento Santos (BR0077895625)\n"
        "Cliente informa que as faturas estão muito altas e estão sendo lidas\n"
        "pela media sendo que o relógio é digital\n"
        "23.03.2026 18:00:15 GMTUK Beatriz Guimaraes Gomes (BR0153479727)\n"
        "As faturas estão sendo estimadas pela média do cliente, gentileza\n"
        "informar leitura com foto pra refaturamento\n"
    )

    assert _normalize_observacao_text(raw_text) == (
        "Cliente informa que as faturas estão muito altas e estão sendo lidas "
        "pela media sendo que o relógio é digital\n\n"
        "As faturas estão sendo estimadas pela média do cliente, gentileza "
        "informar leitura com foto pra refaturamento"
    )


def test_normalize_observacao_text_cleans_spacing_artifacts_inside_paragraph() -> None:
    raw_text = (
        "22.03.2026 03:27:54 GMTUK L. de Cassia de Oliveira Pó (BR0066490088)\n"
        "Cliente solicita através o Ofício nº 035/SMDHC/CAF/DA/DAA/2026 que trata\n"
        "da solicitação de retenção de Imposto de Renda – IR das faturas\n"
        "inerentes aos equipamentos abaixo elencados, considerando a\n"
        "imprescindibilidade de que conste em documento de cobrança o valor\n"
        "bruto, o valor da retenção e o valor líquido (código de barras)\n"
        ",solicitamos que tais retenções se deem a partir do próximo período de\n"
        "faturamento.\n"
    )

    assert _normalize_observacao_text(raw_text) == (
        "Cliente solicita através o Ofício nº 035/SMDHC/CAF/DA/DAA/2026 que trata "
        "da solicitação de retenção de Imposto de Renda – IR das faturas inerentes "
        "aos equipamentos abaixo elencados, considerando a imprescindibilidade de "
        "que conste em documento de cobrança o valor bruto, o valor da retenção e "
        "o valor líquido (código de barras), solicitamos que tais retenções se "
        "deem a partir do próximo período de faturamento."
    )


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


def test_session_disconnected_error_accepts_remote_procedure_call_failed() -> None:
    exc = RuntimeError("(-2147023170, 'Falha na chamada de procedimento remoto.', None, None)")

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


def test_extract_observacao_text_reacquires_scrollbar_each_position(monkeypatch) -> None:  # noqa: ANN001
    session = _ReacquiringObservacaoSession()

    monkeypatch.setattr("sap_automation.dw.wait_not_busy", lambda session, timeout_seconds: None)

    result = extract_observacao_text(
        session=session,
        wait_timeout_seconds=10.0,
    )

    assert result == "\n".join(["linha 0", "linha 1", "linha 2", "linha 3", "linha 4", "linha 5"])


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
    monkeypatch.setattr(
        "sap_automation.dw.extract_observacao_text_with_logging",
        lambda *, session, wait_timeout_seconds, logger, worker_index, item: "obs",
    )
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


def test_process_dw_item_with_retry_serializes_com_critical_section(monkeypatch) -> None:  # noqa: ANN001
    active_calls = 0
    max_active_calls = 0
    active_lock = threading.Lock()

    def _fake_execute_dw_item(**kwargs) -> str:
        nonlocal active_calls, max_active_calls
        with active_lock:
            active_calls += 1
            max_active_calls = max(max_active_calls, active_calls)
        try:
            threading.Event().wait(0.05)
            return f"obs-{kwargs['item'].complaint_id}"
        finally:
            with active_lock:
                active_calls -= 1

    monkeypatch.setattr("sap_automation.dw.execute_dw_item", _fake_execute_dw_item)

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

    def _run_item(index: int) -> None:
        result, failure, _ = _process_dw_item_with_retry(
            worker_index=index,
            session=object(),
            session_locator=DwSessionLocator(connection_index=0, session_index=index - 1),
            item=DwWorkItem(row_index=index + 1, complaint_id=str(index)),
            settings=settings,
            logger=logger,
            application=object(),
        )
        assert result is not None
        assert failure is None

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(_run_item, 1)
        second = executor.submit(_run_item, 2)
        first.result()
        second.result()

    assert max_active_calls == 1


def test_run_interleaved_workers_rotates_between_sessions(monkeypatch) -> None:  # noqa: ANN001
    call_order: list[tuple[int, str]] = []
    collected: list[tuple[int, str]] = []

    monkeypatch.setattr("sap_automation.dw._ensure_dw_selection_screen", lambda **kwargs: _ExecuteField())

    def _fake_process_dw_item_with_retry(**kwargs):
        worker_index = kwargs["worker_index"]
        item = kwargs["item"]
        call_order.append((worker_index, item.complaint_id))
        return (
            DwItemResult(
                row_index=item.row_index,
                complaint_id=item.complaint_id,
                observacao=f"obs-{item.complaint_id}",
                worker_index=worker_index,
                elapsed_seconds=0.1,
            ),
            None,
            kwargs["session"],
        )

    monkeypatch.setattr("sap_automation.dw._process_dw_item_with_retry", _fake_process_dw_item_with_retry)

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
    groups = [
        [DwWorkItem(row_index=2, complaint_id="A"), DwWorkItem(row_index=5, complaint_id="D")],
        [DwWorkItem(row_index=3, complaint_id="B")],
        [DwWorkItem(row_index=4, complaint_id="C")],
    ]

    _run_interleaved_workers(
        groups=groups,
        session_locators=[
            DwSessionLocator(connection_index=0, session_index=0),
            DwSessionLocator(connection_index=0, session_index=1),
            DwSessionLocator(connection_index=0, session_index=2),
        ],
        sessions=[object(), object(), object()],
        settings=settings,
        logger=logger,
        worker_states=[
            DwWorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
            DwWorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
            DwWorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
        ],
        apply_worker_results=lambda worker_index, worker_results, worker_failures: collected.extend(
            (worker_index, result.complaint_id) for result in worker_results
        ),
        cancel_event=threading.Event(),
    )

    assert call_order == [(1, "A"), (2, "B"), (3, "C"), (1, "D")]
    assert collected == [(1, "A"), (2, "B"), (3, "C"), (1, "D")]


def test_run_parallel_workers_collects_results_from_all_workers(monkeypatch) -> None:  # noqa: ANN001
    collected: list[tuple[int, str]] = []
    sleep_calls: list[float] = []

    monkeypatch.setattr("sap_automation.dw._marshal_sap_application", lambda count, logger: [f"stream-{index}" for index in range(1, count + 1)])

    def _fake_parallel_worker(**kwargs):  # noqa: ANN001
        kwargs["result_queue"].put(
            {
                "type": "results",
                "worker_index": kwargs["worker_index"],
                "results": [
                    DwItemResult(
                        row_index=kwargs["items"][0].row_index,
                        complaint_id=kwargs["items"][0].complaint_id,
                        observacao=f"obs-{kwargs['items'][0].complaint_id}",
                        worker_index=kwargs["worker_index"],
                        elapsed_seconds=0.1,
                    )
                ],
                "failures": [],
            }
        )
        kwargs["result_queue"].put({"type": "done", "worker_index": kwargs["worker_index"]})

    monkeypatch.setattr("sap_automation.dw._run_parallel_worker", _fake_parallel_worker)
    monkeypatch.setattr("sap_automation.dw.time.sleep", lambda seconds: sleep_calls.append(seconds))

    _run_parallel_workers(
        groups=[
            [DwWorkItem(row_index=2, complaint_id="A")],
            [DwWorkItem(row_index=3, complaint_id="B")],
            [DwWorkItem(row_index=4, complaint_id="C")],
        ],
        session_locators=[
            DwSessionLocator(connection_index=0, session_index=0),
            DwSessionLocator(connection_index=0, session_index=1),
            DwSessionLocator(connection_index=0, session_index=2),
        ],
        settings=DwSettings(
            demandante="DW",
            input_path=Path("base.csv"),
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=0,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=6.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
            worker_start_stagger_seconds=2.0,
        ),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None, "error": lambda *args, **kwargs: None})(),
        worker_states=[
            DwWorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
            DwWorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
            DwWorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
        ],
        apply_worker_results=lambda worker_index, worker_results, worker_failures: collected.extend(
            (worker_index, result.complaint_id) for result in worker_results
        ),
        cancel_event=threading.Event(),
    )

    assert sorted(collected) == [(1, "A"), (2, "B"), (3, "C")]
    assert sleep_calls == [2.0, 2.0]


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


def test_run_dw_demandante_batches_csv_writes_and_flushes_final(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "base.csv"
    _write_tab_csv(
        csv_path,
        ["ID Reclamação", "OBSERVAÇÃO"],
        [
            ["1", ""],
            ["2", ""],
            ["3", ""],
        ],
    )
    write_calls: list[int] = []
    debug_write_calls: list[int] = []
    manifest_path_holder = tmp_path / "runs" / "dw-run" / "dw" / "dw_manifest.json"

    monkeypatch.setattr("sap_automation.dw.load_export_config", lambda path: {"dw": {}})
    monkeypatch.setattr(
        "sap_automation.dw.load_dw_settings",
        lambda **kwargs: DwSettings(
            demandante="DW",
            input_path=csv_path,
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=3,
            max_rows_per_run=0,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=0.0,
            parallel_mode=True,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
            csv_sync_batch_size=2,
            csv_sync_min_interval_seconds=60.0,
            manifest_sync_min_interval_seconds=60.0,
        ),
    )
    monkeypatch.setattr(
        "sap_automation.dw.configure_run_logger",
        lambda **kwargs: (
            type("Logger", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "error": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
            tmp_path / "dw.log",
        ),
    )
    monkeypatch.setattr(
        "sap_automation.dw.prepare_dw_sessions",
        lambda **kwargs: [
            _FakeSession("/app/con[0]/ses[0]"),
            _FakeSession("/app/con[0]/ses[1]"),
            _FakeSession("/app/con[0]/ses[2]"),
        ],
    )
    monkeypatch.setattr(
        "sap_automation.dw._session_locator_from_session",
        lambda session: DwSessionLocator(connection_index=0, session_index=int(str(session.Id).split("[")[-1].rstrip("]"))),
    )

    def _fake_run_parallel_workers(**kwargs):  # noqa: ANN001
        for worker_index, group in enumerate(kwargs["groups"], start=1):
            for item in group:
                kwargs["apply_worker_results"](
                    worker_index,
                    [
                        DwItemResult(
                            row_index=item.row_index,
                            complaint_id=item.complaint_id,
                            observacao=f"obs-{item.complaint_id}",
                            worker_index=worker_index,
                            elapsed_seconds=0.1,
                        )
                    ],
                        [],
                    )

    monkeypatch.setattr("sap_automation.dw._run_parallel_workers", _fake_run_parallel_workers)

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: type("Provider", (), {"get_session": lambda self, config, logger=None: object()})(),
    )

    original_write_dw_csv = write_dw_csv
    original_write_dw_debug_csv = write_dw_debug_csv

    def _tracking_write_dw_csv(*, input_path: Path, encoding: str, delimiter: str, header: list[str], data_rows: list[list[str]]) -> None:
        write_calls.append(sum(1 for row in data_rows if len(row) > 1 and row[1]))
        original_write_dw_csv(
            input_path=input_path,
            encoding=encoding,
            delimiter=delimiter,
            header=header,
            data_rows=data_rows,
        )

    def _tracking_write_dw_debug_csv(*, output_path: Path, rows: list[DwItemResult]) -> None:
        debug_write_calls.append(len(rows))
        original_write_dw_debug_csv(output_path=output_path, rows=rows)

    monotonic_state = {"value": 0.0}
    monkeypatch.setattr("sap_automation.dw.write_dw_csv", _tracking_write_dw_csv)
    monkeypatch.setattr("sap_automation.dw.write_dw_debug_csv", _tracking_write_dw_debug_csv)
    monkeypatch.setattr(
        "sap_automation.dw.time.monotonic",
        lambda: monotonic_state.__setitem__("value", monotonic_state["value"] + 1.0) or monotonic_state["value"],
    )

    manifest = run_dw_demandante(
        run_id="dw-run",
        demandante="DW",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
    )

    assert manifest.successful_rows == 3
    assert write_calls == [2, 3]
    assert debug_write_calls[0] == 0
    assert debug_write_calls[-2:] == [2, 3]
    assert manifest_path_holder.exists()


def test_run_dw_demandante_reconciles_ledger_success_into_source_csv(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "base.csv"
    _write_tab_csv(
        csv_path,
        ["ID Reclamação", "OBSERVAÇÃO"],
        [
            ["1", ""],
            ["2", ""],
        ],
    )
    run_root = tmp_path / "runs" / "dw-ledger" / "dw"
    run_root.mkdir(parents=True)
    append_dw_progress_ledger(
        ledger_path=run_root / "dw_progress.csv",
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "complaint_id": "1",
                "status": "success",
                "elapsed_seconds": 0.1,
                "observacao": "obs-1",
                "error": "",
            }
        ],
    )

    monkeypatch.setattr("sap_automation.dw.load_export_config", lambda path: {"dw": {}})
    monkeypatch.setattr(
        "sap_automation.dw.load_dw_settings",
        lambda **kwargs: DwSettings(
            demandante="DW",
            input_path=csv_path,
            input_encoding="cp1252",
            delimiter="\t",
            id_column="ID Reclamação",
            output_column="OBSERVAÇÃO",
            transaction_code="IW53",
            session_count=1,
            max_rows_per_run=0,
            wait_timeout_seconds=120.0,
            post_login_wait_seconds=0.0,
            parallel_mode=False,
            circuit_breaker_fast_fail_threshold=10,
            circuit_breaker_slow_fail_threshold=30,
            per_step_timeout_transaction_seconds=30.0,
            per_step_timeout_query_seconds=180.0,
            session_recovery_mode="soft",
        ),
    )
    monkeypatch.setattr(
        "sap_automation.dw.configure_run_logger",
        lambda **kwargs: (
            type("Logger", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "error": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
            tmp_path / "dw.log",
        ),
    )
    monkeypatch.setattr(
        "sap_automation.dw.prepare_dw_sessions",
        lambda **kwargs: [_FakeSession("/app/con[0]/ses[0]")],
    )
    monkeypatch.setattr(
        "sap_automation.dw._session_locator_from_session",
        lambda session: DwSessionLocator(connection_index=0, session_index=0),
    )
    monkeypatch.setattr(
        "sap_automation.dw._worker_run",
        lambda **kwargs: (
            [
                DwItemResult(
                    row_index=3,
                    complaint_id="2",
                    observacao="obs-2",
                    worker_index=1,
                    elapsed_seconds=0.1,
                )
            ],
            [],
        ),
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: type("Provider", (), {"get_session": lambda self, config, logger=None: object()})(),
    )

    manifest = run_dw_demandante(
        run_id="dw-ledger",
        demandante="DW",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
    )

    content = csv_path.read_text(encoding="cp1252")

    assert manifest.successful_rows == 1
    assert manifest.skipped_rows == 1
    assert "1\tobs-1" in content
    assert "2\tobs-2" in content
