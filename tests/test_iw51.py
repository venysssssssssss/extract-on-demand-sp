from __future__ import annotations

import csv
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import sap_automation.iw51 as iw51_module

from sap_automation.config import load_export_config
from sap_automation.iw51 import (
    _IW51_DONE_HEADER,
    Iw51ItemResult,
    Iw51SessionLocator,
    Iw51Settings,
    Iw51WorkItem,
    Iw51WorkerState,
    _load_iw51_ledger_state,
    _compact_iw51_ledger,
    _collect_iw51_workbook_done_rows,
    _iw51_process_worker_main,
    append_iw51_progress_ledger,
    execute_iw51_item,
    load_iw51_settings,
    load_iw51_work_items,
    recover_iw51_run_artifacts_from_log,
    run_iw51_demandante,
    split_iw51_work_items_evenly,
    _is_systemic_iw51_error,
    _reattach_iw51_session,
    _run_iw51_interleaved_workers,
    _run_iw51_parallel_worker,
    _run_iw51_process_parallel_workers,
    _run_iw51_true_parallel_workers,
)


class _Logger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args) -> None:  # noqa: ANN001
        self.messages.append(message % args if args else message)

    def warning(self, message: str, *args) -> None:  # noqa: ANN001
        self.messages.append(message % args if args else message)

    def error(self, message: str, *args) -> None:  # noqa: ANN001
        self.messages.append(message % args if args else message)

    def exception(self, message: str, *args) -> None:  # noqa: ANN001
        self.messages.append(message % args if args else message)


class _FakeCell:
    def __init__(self, value=None) -> None:  # noqa: ANN001
        self.value = value


class _FakeSheet:
    def __init__(self, rows: list[list[object | None]]) -> None:
        self._cells: dict[tuple[int, int], _FakeCell] = {}
        self.max_row = 0
        self.max_column = 0
        for row_index, row_values in enumerate(rows, start=1):
            for column_index, value in enumerate(row_values, start=1):
                self._cells[(row_index, column_index)] = _FakeCell(value)
                self.max_row = max(self.max_row, row_index)
                self.max_column = max(self.max_column, column_index)

    def __getitem__(self, row_index: int):  # noqa: ANN001
        return [self.cell(row=row_index, column=index) for index in range(1, self.max_column + 1)]

    def cell(self, row: int, column: int, value=None):  # noqa: ANN001
        if value is not None:
            self._cells[(row, column)] = _FakeCell(value)
            self.max_row = max(self.max_row, row)
            self.max_column = max(self.max_column, column)
        return self._cells.setdefault((row, column), _FakeCell())


class _FakeWorkbook:
    def __init__(self, sheet: _FakeSheet) -> None:
        self.sheetnames = ["Macro1"]
        self._sheet = sheet
        self.saved_paths: list[Path] = []

    def __getitem__(self, sheet_name: str) -> _FakeSheet:
        if sheet_name != "Macro1":
            raise KeyError(sheet_name)
        return self._sheet

    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fake workbook", encoding="utf-8")
        self.saved_paths.append(path)


class _FakeWindow:
    def __init__(self) -> None:
        self.maximize_calls = 0
        self.vkeys: list[int] = []
        self.Type = "GuiMainWindow"

    def maximize(self) -> None:
        self.maximize_calls += 1

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)


class _FakeField:
    def __init__(self) -> None:
        self.text = ""
        self.caretPosition = 0

    def setFocus(self) -> None:
        return None


class _FakeSelectable:
    def __init__(self) -> None:
        self.selected = False

    def setFocus(self) -> None:
        return None


class _FakePressable:
    def press(self) -> None:
        return None

    def select(self) -> None:
        return None


class _FakeReattachWindow:
    def __init__(self) -> None:
        self.vkeys: list[int] = []

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)


class _FakeReattachSession:
    def __init__(self, session_id: str, connection) -> None:  # noqa: ANN001
        self.Id = session_id
        self._connection = connection
        self.window = _FakeReattachWindow()

    def findById(self, item_id: str) -> object:
        if item_id == "wnd[0]":
            return self.window
        if item_id == "wnd[0]/tbar[0]/okcd":
            return _FakeField()
        raise KeyError(item_id)

    @property
    def Parent(self):  # noqa: ANN201
        return self._connection


class _FakeConnection:
    def __init__(self, sessions: list[object]) -> None:
        self.sessions = sessions

    @property
    def Sessions(self):  # noqa: ANN201
        return self

    @property
    def Count(self) -> int:  # noqa: ANN201
        return len(self.sessions)

    def __call__(self, index: int) -> object:
        return self.sessions[index]

    def Children(self, index: int) -> object:  # noqa: ANN001
        return self.sessions[index]


class _FakeProcess:
    def __init__(self, *, exitcode: int | None = None, alive: bool = False) -> None:
        self.exitcode = exitcode
        self._alive = alive
        self.terminated = False

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self.terminated = True
        self._alive = False
        self.exitcode = -15

    def join(self, timeout: float | None = None) -> None:
        self._alive = False
        if self.exitcode is None:
            self.exitcode = 0


class _FakeMpContext:
    def Queue(self):  # noqa: ANN201
        return queue.Queue()


class _ExecuteSession:
    def __init__(self) -> None:
        self.Id = "/app/con[0]/ses[0]"
        self.main_window = _FakeWindow()
        self.active_window = self.main_window
        self.fields = {
            "wnd[0]/tbar[0]/okcd": _FakeField(),
            "wnd[0]/usr/ctxtRIWO00-QMART": _FakeField(),
            "wnd[0]/usr/ctxtRIWO00-QWRNUM": _FakeField(),
            (
                r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19/ssubSUB_GROUP_10:SAPLIQS0:7235/"
                r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_1:SAPLIQS0:7322/subOBJEKT:SAPLIWO1:0100/"
                r"ctxtRIWO1-TPLNR"
            ): _FakeField(),
            (
                r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19/ssubSUB_GROUP_10:SAPLIQS0:7235/"
                r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_3:SAPLIQS0:7900/subUSER0001:SAPLXQQM:0114/"
                r"ctxtQMEL-ZZANLAGE"
            ): _FakeField(),
            (
                r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB01/ssubSUB_GROUP_10:SAPLIQS0:7235/"
                r"subCUSTOM_SCREEN:SAPLIQS0:7212/subSUBSCREEN_1:SAPLIQS0:7515/subANSPRECH:SAPLIPAR:0700/"
                r"tabsTSTRIP_700/tabpKUND/ssubTSTRIP_SCREEN:SAPLIPAR:0130/subADRESSE:SAPLIPAR:0150/"
                r"ctxtVIQMEL-KUNUM"
            ): _FakeField(),
            r"wnd[0]/usr/subSCREEN_1:SAPLIQS0:1060/subNOTIF_TYPE:SAPLIQS0:1061/txtVIQMEL-QMTXT": _FakeField(),
        }
        self.selectables = {
            "wnd[1]/usr/tblSAPLBSVATC_E/radJ_STMAINT-ANWS[0,1]": _FakeSelectable(),
            "wnd[1]/usr/tblSAPLBSVATC_E/radJ_STMAINT-ANWS[0,4]": _FakeSelectable(),
        }
        self.pressables = {
            r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB19": _FakePressable(),
            r"wnd[0]/usr/tabsTAB_GROUP_10/tabp10\TAB01": _FakePressable(),
            "wnd[0]/usr/subSCREEN_1:SAPLIQS0:1060/btnANWENDERSTATUS": _FakePressable(),
            "wnd[1]/tbar[0]/btn[0]": _FakePressable(),
            "wnd[0]/tbar[1]/btn[16]": _FakePressable(),
        }

    @property
    def ActiveWindow(self) -> _FakeWindow:
        return self.active_window

    def findById(self, item_id: str) -> object:
        if item_id == "wnd[0]":
            return self.main_window
        if item_id in self.fields:
            return self.fields[item_id]
        if item_id in self.selectables:
            return self.selectables[item_id]
        if item_id in self.pressables:
            return self.pressables[item_id]
        raise KeyError(item_id)


def test_load_iw51_settings_resolves_dani_profile() -> None:
    config = load_export_config(Path("sap_iw69_batch_config.json"))

    settings = load_iw51_settings(
        config=config,
        config_path=Path("sap_iw69_batch_config.json"),
        demandante="dani",
    )

    assert settings == Iw51Settings(
        demandante="DANI",
        workbook_path=Path("projeto_Dani2.xlsm").resolve(),
        sheet_name="Macro1",
        inter_item_sleep_seconds=1.0,
        max_rows_per_run=3000,
        notification_type="CA",
        reference_notification_number="389496787",
        session_count=3,
        execution_mode="process_parallel",
        post_login_wait_seconds=6.0,
        workbook_sync_batch_size=100,
        workbook_sync_min_interval_seconds=60.0,
        workbook_sync_max_pending_rows=500,
        manifest_sync_min_interval_seconds=15.0,
        circuit_breaker_fast_fail_threshold=10,
        circuit_breaker_slow_fail_threshold=30,
        per_step_timeout_seconds=30.0,
        session_recovery_mode="soft",
        worker_stagger_seconds=1.5,
        worker_timeout_seconds=3600.0,
        worker_start_stagger_seconds=2.0,
        worker_heartbeat_seconds=5.0,
        worker_item_timeout_seconds=180.0,
        worker_restart_limit=5,
        worker_restart_backoff_seconds=15.0,
        session_rebuild_backoff_seconds=30.0,
        message_filter_retry_window_seconds=30.0,
    )


def test_load_iw51_work_items_adds_feito_skips_completed_and_collects_rejections(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "COMUNICACAO ENDERECO SP", None],
                ["10478355", "123453453", "COMUNICACAO ENDERECO SP", "SIM"],
                ["10479336", "74518828", None, None],
                ["10479999", "99887766", "COMUNICACAO ENDERECO SP", None],
            ]
        )
    )
    openpyxl_stub = type(
        "OpenPyxlStub",
        (),
        {"load_workbook": staticmethod(lambda path, keep_vba=True: workbook)},
    )
    monkeypatch.setattr("sap_automation.iw51._openpyxl_module", lambda: openpyxl_stub)

    _, sheet, feito_column_index, items, skipped_rows, rejected_rows = load_iw51_work_items(
        workbook_path=tmp_path / "projeto_Dani2.xlsm",
        sheet_name="Macro1",
        max_rows=10,
        completed_row_indices={5},
    )

    assert sheet.cell(row=1, column=feito_column_index).value == "FEITO"
    assert skipped_rows == 2
    assert [item.row_index for item in items] == [2]
    assert rejected_rows[0]["row_index"] == 4
    assert rejected_rows[0]["status"] == "rejected"


def test_load_iw51_work_items_accepts_headerless_workbook_and_normalizes_numeric_ids(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                [10478355.0, 123453453.0, "COMUNICACAO ENDERECO SP", None],
                [10479336.0, 74518828.0, "COMUNICACAO ENDERECO SP", "SIM"],
                [None, None, None, None],
            ]
        )
    )
    openpyxl_stub = type(
        "OpenPyxlStub",
        (),
        {"load_workbook": staticmethod(lambda path, keep_vba=True: workbook)},
    )
    monkeypatch.setattr("sap_automation.iw51._openpyxl_module", lambda: openpyxl_stub)

    _, sheet, feito_column_index, items, skipped_rows, rejected_rows = load_iw51_work_items(
        workbook_path=tmp_path / "projeto_Dani2.xlsm",
        sheet_name="Macro1",
        max_rows=10,
        completed_row_indices=None,
    )

    assert sheet.cell(row=1, column=1).value == 10478355.0
    assert feito_column_index == 4
    assert skipped_rows == 1
    assert rejected_rows == []
    assert [(item.row_index, item.pn, item.instalacao, item.tipologia) for item in items] == [
        (1, "10478355", "123453453", "COMUNICACAO ENDERECO SP")
    ]


def test_load_iw51_work_items_continues_after_blank_rows(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "TIPO A", "SIM"],
                [None, None, None, None],
                ["10479336", "74518828", "TIPO B", None],
                ["10479999", "99887766", None, None],
            ]
        )
    )
    openpyxl_stub = type(
        "OpenPyxlStub",
        (),
        {"load_workbook": staticmethod(lambda path, keep_vba=True: workbook)},
    )
    monkeypatch.setattr("sap_automation.iw51._openpyxl_module", lambda: openpyxl_stub)

    _, _, feito_column_index, items, skipped_rows, rejected_rows = load_iw51_work_items(
        workbook_path=tmp_path / "projeto_Dani2.xlsm",
        sheet_name="Macro1",
        max_rows=10,
        completed_row_indices=None,
    )

    assert feito_column_index == 4
    assert skipped_rows == 1
    assert [item.row_index for item in items] == [4]
    assert rejected_rows[0]["row_index"] == 5


def test_collect_iw51_workbook_done_rows_continues_after_blank_rows() -> None:
    sheet = _FakeSheet(
        [
            ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
            ["10443892", "112235352", "TIPO A", "SIM"],
            [None, None, None, None],
            ["10479336", "74518828", "TIPO B", "SIM"],
        ]
    )

    done_rows = _collect_iw51_workbook_done_rows(
        sheet=sheet,
        header_index_by_key={"pn": 1, "instalacao": 2, "tipologia": 3},
        feito_column_index=4,
        data_start_row=2,
        ledger_terminal_rows=set(),
    )

    assert [row["row_index"] for row in done_rows] == [2, 4]


def test_append_iw51_progress_ledger_roundtrip_tracks_success_and_terminal_rows(tmp_path: Path) -> None:
    ledger_path = tmp_path / "iw51_progress.csv"

    append_iw51_progress_ledger(
        ledger_path=ledger_path,
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "success",
                "attempt": 1,
                "elapsed_seconds": 1.0,
                "error": "",
            },
            {
                "row_index": 4,
                "worker_index": 0,
                "pn": "x",
                "instalacao": "",
                "tipologia": "",
                "status": "rejected",
                "attempt": 0,
                "elapsed_seconds": 0.0,
                "error": "bad row",
            },
            {
                "row_index": 6,
                "worker_index": 2,
                "pn": "y",
                "instalacao": "z",
                "tipologia": "w",
                "status": "failed",
                "attempt": 1,
                "elapsed_seconds": 2.0,
                "error": "sap fail",
            },
        ],
    )

    ledger_state = _load_iw51_ledger_state(ledger_path)

    assert ledger_state.success_rows == {2}
    assert ledger_state.terminal_rows == {2, 4}
    assert ledger_state.retriable_failed_rows == {6}


def test_load_iw51_ledger_state_uses_last_status_and_failed_terminal_is_terminal(tmp_path: Path) -> None:
    ledger_path = tmp_path / "iw51_progress.csv"

    append_iw51_progress_ledger(
        ledger_path=ledger_path,
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "failed",
                "attempt": 1,
                "elapsed_seconds": 1.0,
                "error": "timeout",
            },
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "success",
                "attempt": 2,
                "elapsed_seconds": 1.0,
                "error": "",
            },
            {
                "row_index": 4,
                "worker_index": 1,
                "pn": "x",
                "instalacao": "y",
                "tipologia": "z",
                "status": "failed_terminal",
                "attempt": 2,
                "elapsed_seconds": 0.2,
                "error": "instalacao invalida",
            },
        ],
    )

    ledger_state = _load_iw51_ledger_state(ledger_path)

    assert ledger_state.success_rows == {2}
    assert ledger_state.terminal_rows == {2, 4}
    assert ledger_state.failed_terminal_rows == {4}
    assert ledger_state.retriable_failed_rows == set()


def test_compact_iw51_ledger_keeps_last_entry_per_row(tmp_path: Path) -> None:
    ledger_path = tmp_path / "iw51_progress.csv"

    append_iw51_progress_ledger(
        ledger_path=ledger_path,
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "failed",
                "attempt": 1,
                "elapsed_seconds": 1.0,
                "error": "first",
            },
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "success",
                "attempt": 2,
                "elapsed_seconds": 1.1,
                "error": "",
            },
            {
                "row_index": 4,
                "worker_index": 2,
                "pn": "x",
                "instalacao": "y",
                "tipologia": "z",
                "status": "failed_terminal",
                "attempt": 2,
                "elapsed_seconds": 0.4,
                "error": "instalacao invalida",
            },
        ],
    )

    removed_entries = _compact_iw51_ledger(ledger_path)
    compacted_rows = list(csv.DictReader(ledger_path.open(encoding="utf-8")))

    assert removed_entries == 1
    assert [row["row_index"] for row in compacted_rows] == ["2", "4"]
    assert compacted_rows[0]["status"] == "success"
    assert compacted_rows[1]["status"] == "failed_terminal"


def test_append_iw51_progress_ledger_flushes_and_fsyncs(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    ledger_path = tmp_path / "iw51_progress.csv"
    fsync_calls: list[int] = []

    monkeypatch.setattr(iw51_module.os, "fsync", lambda fd: fsync_calls.append(fd))

    append_iw51_progress_ledger(
        ledger_path=ledger_path,
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "a",
                "instalacao": "b",
                "tipologia": "c",
                "status": "success",
                "attempt": 1,
                "elapsed_seconds": 1.0,
                "error": "",
            }
        ],
    )

    assert fsync_calls


def test_split_iw51_work_items_evenly_distributes_items_round_robin() -> None:
    groups = split_iw51_work_items_evenly(
        [Iw51WorkItem(row_index=index + 2, pn=str(index), instalacao="i", tipologia="t") for index in range(7)],
        session_count=3,
    )

    assert [[item.pn for item in group] for group in groups] == [
        ["0", "3", "6"],
        ["1", "4"],
        ["2", "5"],
    ]


def test_is_systemic_iw51_error_treats_missing_session_index_as_systemic() -> None:
    exc = RuntimeError(
        "(-2147352567, 'Exceção.', (614, 'saplogon', "
        "'The enumerator of the collection cannot find an element with the specified index.', ...), None)"
    )

    assert _is_systemic_iw51_error(exc) is True


def test_reattach_iw51_session_recreates_missing_slot(monkeypatch) -> None:  # noqa: ANN001
    connection = _FakeConnection([])
    session0 = _FakeReattachSession("/app/con[0]/ses[0]", connection)
    session1 = _FakeReattachSession("/app/con[0]/ses[1]", connection)
    connection.sessions[:] = [session0, session1]

    def _create_session() -> None:
        connection.sessions.append(_FakeReattachSession("/app/con[0]/ses[2]", connection))

    session0.CreateSession = _create_session  # type: ignore[attr-defined]
    application = SimpleNamespace(Children=lambda index: connection)

    monkeypatch.setattr(iw51_module, "wait_not_busy", lambda session, timeout_seconds: None)
    monkeypatch.setattr(iw51_module, "set_text", lambda session, item_id, value: None)

    session = _reattach_iw51_session(
        locator=Iw51SessionLocator(connection_index=0, session_index=2),
        logger=_Logger(),
        application=application,
    )

    assert session.Id == "/app/con[0]/ses[2]"


def test_execute_iw51_item_does_not_call_maximize(monkeypatch) -> None:  # noqa: ANN001
    session = _ExecuteSession()
    logger = _Logger()

    monkeypatch.setattr("sap_automation.iw51.wait_not_busy", lambda session, timeout_seconds: None)
    monkeypatch.setattr("sap_automation.iw51._dismiss_popup_if_present", lambda session, logger: False)

    execute_iw51_item(
        session=session,
        item=Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="TIPO"),
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=0.0,
            max_rows_per_run=4,
        ),
        logger=logger,
        worker_index=1,
    )

    assert session.main_window.maximize_calls == 0


def test_run_iw51_demandante_creates_working_copy_and_persists_ledger(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "COMUNICACAO ENDERECO SP", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=1,
        session_count=1,
        execution_mode="sequential",
        workbook_sync_batch_size=1,
    )
    logger = _Logger()

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="COMUNICACAO ENDERECO SP")],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [SimpleNamespace(Id="/app/con[0]/ses[0]")],
    )
    monkeypatch.setattr(
        iw51_module,
        "_process_iw51_item_with_retry",
        lambda **kwargs: (
            Iw51ItemResult(
                row_index=kwargs["item"].row_index,
                pn=kwargs["item"].pn,
                instalacao=kwargs["item"].instalacao,
                tipologia=kwargs["item"].tipologia,
                worker_index=kwargs["worker_index"],
                elapsed_seconds=1.5,
            ),
            None,
            kwargs["session"],
        ),
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    manifest = run_iw51_demandante(
        run_id="iw51-run-001",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=1,
    )

    ledger_rows = list(
        csv.DictReader((tmp_path / "runs" / "iw51-run-001" / "iw51" / "iw51_progress.csv").open(encoding="utf-8"))
    )

    assert manifest.successful_rows == 1
    assert Path(manifest.working_workbook_path).exists()
    assert ledger_rows[0]["status"] == "success"
    assert workbook.saved_paths


def test_run_iw51_demandante_passes_ledger_rows_as_completed_to_loader(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    run_root = tmp_path / "runs" / "iw51-resume" / "iw51"
    run_root.mkdir(parents=True, exist_ok=True)
    append_iw51_progress_ledger(
        ledger_path=run_root / "iw51_progress.csv",
        rows=[
            {
                "row_index": 2,
                "worker_index": 1,
                "pn": "10443892",
                "instalacao": "112235352",
                "tipologia": "TIPO",
                "status": "success",
                "attempt": 1,
                "elapsed_seconds": 1.0,
                "error": "",
            }
        ],
    )

    seen_completed_rows: list[set[int]] = []
    workbook = _FakeWorkbook(_FakeSheet([["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"]]))
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=1,
    )

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)

    def _fake_loader(**kwargs):
        seen_completed_rows.append(set(kwargs["completed_row_indices"]))
        return workbook, workbook._sheet, 4, [], 1, []

    monkeypatch.setattr(iw51_module, "load_iw51_work_items", _fake_loader)
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (_Logger(), tmp_path / "iw51.log"))

    manifest = run_iw51_demandante(
        run_id="iw51-resume",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=1,
    )

    assert seen_completed_rows == [{2}]
    assert manifest.status == "skipped"


def test_run_iw51_demandante_reconciles_workbook_feito_rows_into_ledger(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "TIPO A", "SIM"],
                ["10443893", "112235353", "TIPO B", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=2,
        session_count=1,
        execution_mode="sequential",
        workbook_sync_batch_size=1,
    )
    logger = _Logger()

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="TIPO B")],
            1,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "_resolve_iw51_sheet_layout", lambda sheet: ({"pn": 1, "instalacao": 2, "tipologia": 3}, 4, 2))
    monkeypatch.setattr(
        iw51_module,
        "_collect_iw51_workbook_done_rows",
        lambda **kwargs: [
            {
                "row_index": 2,
                "worker_index": 0,
                "pn": "10443892",
                "instalacao": "112235352",
                "tipologia": "TIPO A",
                "status": "success",
                "attempt": 0,
                "elapsed_seconds": 0.0,
                "error": "Imported from workbook FEITO state",
            }
        ],
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [SimpleNamespace(Id="/app/con[0]/ses[0]")],
    )
    monkeypatch.setattr(
        iw51_module,
        "_process_iw51_item_with_retry",
        lambda **kwargs: (
            Iw51ItemResult(
                row_index=3,
                pn="10443893",
                instalacao="112235353",
                tipologia="TIPO B",
                worker_index=1,
                elapsed_seconds=0.2,
            ),
            None,
            kwargs["session"],
        ),
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    manifest = run_iw51_demandante(
        run_id="iw51-feito-ledger",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=2,
    )

    ledger_rows = list(
        csv.DictReader((tmp_path / "runs" / "iw51-feito-ledger" / "iw51" / "iw51_progress.csv").open(encoding="utf-8"))
    )

    assert manifest.successful_rows == 1
    assert [row["row_index"] for row in ledger_rows] == ["2", "3"]
    assert ledger_rows[0]["status"] == "success"
    assert ledger_rows[0]["error"] == "Imported from workbook FEITO state"


def test_run_iw51_demandante_flushes_pending_workbook_rows_on_processing_exception(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "TIPO A", None],
                ["10443893", "112235353", "TIPO B", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=1,
        session_count=2,
        execution_mode="interleaved",
        workbook_sync_batch_size=25,
    )
    logger = _Logger()

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="TIPO A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="TIPO B"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [SimpleNamespace(Id="/app/con[0]/ses[0]"), SimpleNamespace(Id="/app/con[0]/ses[1]")],
    )

    def _crashing_interleaved_runner(**kwargs):  # noqa: ANN001
        kwargs["apply_worker_results"](
            1,
            [
                Iw51ItemResult(
                    row_index=2,
                    pn="10443892",
                    instalacao="112235352",
                    tipologia="TIPO A",
                    worker_index=1,
                    elapsed_seconds=1.25,
                )
            ],
            [],
        )
        raise RuntimeError("boom after success")

    monkeypatch.setattr(iw51_module, "_run_iw51_interleaved_workers", _crashing_interleaved_runner)

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    with pytest.raises(RuntimeError, match="boom after success"):
        run_iw51_demandante(
            run_id="iw51-crash-flush",
            demandante="DANI",
            config_path=Path("sap_iw69_batch_config.json"),
            output_root=tmp_path,
            max_rows=2,
        )

    ledger_path = tmp_path / "runs" / "iw51-crash-flush" / "iw51" / "iw51_progress.csv"
    ledger_rows = list(csv.DictReader(ledger_path.open(encoding="utf-8")))

    assert ledger_rows[0]["row_index"] == "2"
    assert ledger_rows[0]["status"] == "success"
    assert workbook._sheet.cell(row=2, column=4).value == "SIM"
    assert workbook.saved_paths


def test_run_iw51_demandante_throttles_workbook_and_manifest_sync(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "TIPO A", None],
                ["10443893", "112235353", "TIPO B", None],
                ["10443894", "112235354", "TIPO C", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=3,
        session_count=1,
        execution_mode="sequential",
        workbook_sync_batch_size=1,
        workbook_sync_min_interval_seconds=60.0,
        workbook_sync_max_pending_rows=10,
        manifest_sync_min_interval_seconds=60.0,
    )
    logger = _Logger()
    manifest_writes: list[int] = []
    monotonic_state = {"value": 0.0}

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="TIPO A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="TIPO B"),
                Iw51WorkItem(row_index=4, pn="10443894", instalacao="112235354", tipologia="TIPO C"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [SimpleNamespace(Id="/app/con[0]/ses[0]")],
    )
    monkeypatch.setattr(
        iw51_module,
        "_process_iw51_item_with_retry",
        lambda **kwargs: (
            Iw51ItemResult(
                row_index=kwargs["item"].row_index,
                pn=kwargs["item"].pn,
                instalacao=kwargs["item"].instalacao,
                tipologia=kwargs["item"].tipologia,
                worker_index=kwargs["worker_index"],
                elapsed_seconds=0.25,
            ),
            None,
            kwargs["session"],
        ),
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    def _fake_monotonic() -> float:
        monotonic_state["value"] += 1.0
        return monotonic_state["value"]

    original_write_manifest = iw51_module._write_iw51_manifest

    def _tracking_write_manifest(*, manifest_path: Path, manifest) -> None:  # noqa: ANN001
        manifest_writes.append(manifest.processed_rows)
        original_write_manifest(manifest_path=manifest_path, manifest=manifest)

    monkeypatch.setattr(iw51_module.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(iw51_module, "_write_iw51_manifest", _tracking_write_manifest)

    manifest = run_iw51_demandante(
        run_id="iw51-sync-throttle",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=3,
    )

    assert manifest.successful_rows == 3
    assert len(workbook.saved_paths) == 2
    assert manifest_writes == [1, 3, 3]


def test_run_iw51_demandante_flushes_when_pending_rows_hit_cap(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "TIPO A", None],
                ["10443893", "112235353", "TIPO B", None],
                ["10443894", "112235354", "TIPO C", None],
                ["10443895", "112235355", "TIPO D", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=4,
        session_count=1,
        execution_mode="sequential",
        workbook_sync_batch_size=1,
        workbook_sync_min_interval_seconds=60.0,
        workbook_sync_max_pending_rows=2,
        manifest_sync_min_interval_seconds=60.0,
    )
    monotonic_state = {"value": 0.0}

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="TIPO A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="TIPO B"),
                Iw51WorkItem(row_index=4, pn="10443894", instalacao="112235354", tipologia="TIPO C"),
                Iw51WorkItem(row_index=5, pn="10443895", instalacao="112235355", tipologia="TIPO D"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (_Logger(), tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [SimpleNamespace(Id="/app/con[0]/ses[0]")],
    )
    monkeypatch.setattr(
        iw51_module,
        "_process_iw51_item_with_retry",
        lambda **kwargs: (
            Iw51ItemResult(
                row_index=kwargs["item"].row_index,
                pn=kwargs["item"].pn,
                instalacao=kwargs["item"].instalacao,
                tipologia=kwargs["item"].tipologia,
                worker_index=kwargs["worker_index"],
                elapsed_seconds=0.25,
            ),
            None,
            kwargs["session"],
        ),
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )
    monkeypatch.setattr(
        iw51_module.time,
        "monotonic",
        lambda: monotonic_state.__setitem__("value", monotonic_state["value"] + 1.0) or monotonic_state["value"],
    )

    manifest = run_iw51_demandante(
        run_id="iw51-sync-cap",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=4,
    )

    assert manifest.successful_rows == 4
    assert len(workbook.saved_paths) == 3


def test_run_iw51_interleaved_workers_rotates_between_sessions(monkeypatch) -> None:  # noqa: ANN001
    call_order: list[tuple[int, str]] = []
    collected: list[tuple[int, int]] = []

    def _fake_process_iw51_item_with_retry(**kwargs):
        worker_index = kwargs["worker_index"]
        item = kwargs["item"]
        call_order.append((worker_index, item.pn))
        return (
            Iw51ItemResult(
                row_index=item.row_index,
                pn=item.pn,
                instalacao=item.instalacao,
                tipologia=item.tipologia,
                worker_index=worker_index,
                elapsed_seconds=0.1,
            ),
            None,
            kwargs["session"],
        )

    monkeypatch.setattr("sap_automation.iw51._process_iw51_item_with_retry", _fake_process_iw51_item_with_retry)

    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=Path("projeto_Dani2.xlsm"),
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=10,
        session_count=3,
        execution_mode="interleaved",
    )

    _run_iw51_interleaved_workers(
        groups=[
            [
                Iw51WorkItem(row_index=2, pn="a", instalacao="1", tipologia="t"),
                Iw51WorkItem(row_index=5, pn="d", instalacao="4", tipologia="t"),
            ],
            [Iw51WorkItem(row_index=3, pn="b", instalacao="2", tipologia="t")],
            [Iw51WorkItem(row_index=4, pn="c", instalacao="3", tipologia="t")],
        ],
        session_locators=[
            Iw51SessionLocator(connection_index=0, session_index=0),
            Iw51SessionLocator(connection_index=0, session_index=1),
            Iw51SessionLocator(connection_index=0, session_index=2),
        ],
        sessions=[
            SimpleNamespace(Id="/app/con[0]/ses[0]"),
            SimpleNamespace(Id="/app/con[0]/ses[1]"),
            SimpleNamespace(Id="/app/con[0]/ses[2]"),
        ],
        settings=settings,
        logger=_Logger(),
        worker_states=[
            Iw51WorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
            Iw51WorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
            Iw51WorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
        ],
        apply_worker_results=lambda worker_index, worker_results, worker_failures: collected.extend(
            (worker_index, result.row_index) for result in worker_results
        ),
    )

    assert call_order == [(1, "a"), (2, "b"), (3, "c"), (1, "d")]
    assert collected == [(1, 2), (2, 3), (3, 4), (1, 5)]


def test_run_iw51_true_parallel_workers_collects_results_from_worker_threads(monkeypatch) -> None:  # noqa: ANN001
    thread_names: list[str] = []
    collected: list[tuple[int, int]] = []

    def _fake_marshal(count: int, logger) -> list[str]:  # noqa: ANN001
        return [f"stream-{index}" for index in range(1, count + 1)]

    def _fake_co_initialize():  # noqa: ANN001
        return SimpleNamespace(CoUninitialize=lambda: None), True

    def _fake_unmarshal(stream, logger, worker_index):  # noqa: ANN001
        return f"app-{worker_index}-{stream}"

    def _fake_reattach(*, locator, logger, application):  # noqa: ANN001
        return SimpleNamespace(Id=f"/app/con[{locator.connection_index}]/ses[{locator.session_index}]")

    def _fake_process(**kwargs):
        thread_names.append(threading.current_thread().name)
        item = kwargs["item"]
        worker_index = kwargs["worker_index"]
        return (
            Iw51ItemResult(
                row_index=item.row_index,
                pn=item.pn,
                instalacao=item.instalacao,
                tipologia=item.tipologia,
                worker_index=worker_index,
                elapsed_seconds=0.05,
            ),
            None,
            kwargs["session"],
        )

    monkeypatch.setattr(iw51_module, "_marshal_sap_application", _fake_marshal)
    monkeypatch.setattr(iw51_module, "_co_initialize", _fake_co_initialize)
    monkeypatch.setattr(iw51_module, "_unmarshal_sap_application", _fake_unmarshal)
    monkeypatch.setattr(iw51_module, "_reattach_iw51_session", _fake_reattach)
    monkeypatch.setattr(iw51_module, "_wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr(iw51_module, "_process_iw51_item_with_retry", _fake_process)

    _run_iw51_true_parallel_workers(
        groups=[
            [Iw51WorkItem(row_index=2, pn="a", instalacao="1", tipologia="t")],
            [Iw51WorkItem(row_index=3, pn="b", instalacao="2", tipologia="t")],
            [Iw51WorkItem(row_index=4, pn="c", instalacao="3", tipologia="t")],
        ],
        session_locators=[
            Iw51SessionLocator(connection_index=0, session_index=0),
            Iw51SessionLocator(connection_index=0, session_index=1),
            Iw51SessionLocator(connection_index=0, session_index=2),
        ],
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=0.0,
            max_rows_per_run=10,
            session_count=3,
            execution_mode="true_parallel",
            worker_stagger_seconds=0.0,
        ),
        logger=_Logger(),
        worker_states=[
            Iw51WorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
            Iw51WorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
            Iw51WorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
        ],
        apply_worker_results=lambda worker_index, worker_results, worker_failures: collected.extend(
            (worker_index, result.row_index) for result in worker_results
        ),
    )

    assert sorted(collected) == [(1, 2), (2, 3), (3, 4)]
    assert len({name for name in thread_names if name.startswith("iw51-worker-")}) == 3


def test_run_iw51_true_parallel_workers_staggers_worker_start(monkeypatch) -> None:  # noqa: ANN001
    sleep_calls: list[float] = []

    monkeypatch.setattr(iw51_module, "_marshal_sap_application", lambda count, logger: [f"stream-{index}" for index in range(count)])

    def _fake_worker(**kwargs):  # noqa: ANN001
        kwargs["result_queue"].put({"type": "done", "worker_index": kwargs["worker_index"]})

    monkeypatch.setattr(iw51_module, "_run_iw51_parallel_worker", _fake_worker)
    monkeypatch.setattr(iw51_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    _run_iw51_true_parallel_workers(
        groups=[
            [Iw51WorkItem(row_index=2, pn="a", instalacao="1", tipologia="t")],
            [Iw51WorkItem(row_index=3, pn="b", instalacao="2", tipologia="t")],
            [Iw51WorkItem(row_index=4, pn="c", instalacao="3", tipologia="t")],
        ],
        session_locators=[
            Iw51SessionLocator(connection_index=0, session_index=0),
            Iw51SessionLocator(connection_index=0, session_index=1),
            Iw51SessionLocator(connection_index=0, session_index=2),
        ],
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=0.0,
            max_rows_per_run=10,
            session_count=3,
            execution_mode="true_parallel",
            worker_stagger_seconds=1.5,
        ),
        logger=_Logger(),
        worker_states=[
            Iw51WorkerState(worker_index=1, session_id="/app/con[0]/ses[0]"),
            Iw51WorkerState(worker_index=2, session_id="/app/con[0]/ses[1]"),
            Iw51WorkerState(worker_index=3, session_id="/app/con[0]/ses[2]"),
        ],
        apply_worker_results=lambda worker_index, worker_results, worker_failures: None,
    )

    assert sleep_calls == [1.5, 1.5]


def test_run_iw51_parallel_worker_timeout_aborts(monkeypatch) -> None:  # noqa: ANN001
    monotonic_values = iter([0.0, 2.0])

    monkeypatch.setattr(iw51_module, "_co_initialize", lambda: (SimpleNamespace(CoUninitialize=lambda: None), True))
    monkeypatch.setattr(iw51_module, "_unmarshal_sap_application", lambda stream, logger, worker_index: "app")
    monkeypatch.setattr(
        iw51_module,
        "_reattach_iw51_session",
        lambda *, locator, logger, application: SimpleNamespace(Id="/app/con[0]/ses[0]"),
    )
    monkeypatch.setattr(iw51_module, "_wait_session_ready", lambda session, timeout_seconds, logger: None)
    monkeypatch.setattr(iw51_module.time, "monotonic", lambda: next(monotonic_values))

    result_queue: queue.Queue[dict[str, object]] = queue.Queue()
    worker_state = Iw51WorkerState(worker_index=1, session_id="/app/con[0]/ses[0]")

    _run_iw51_parallel_worker(
        worker_index=1,
        session_locator=Iw51SessionLocator(connection_index=0, session_index=0),
        items=[Iw51WorkItem(row_index=2, pn="a", instalacao="1", tipologia="t")],
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=0.0,
            max_rows_per_run=10,
            worker_timeout_seconds=1.0,
        ),
        logger=_Logger(),
        marshaled_app_stream="stream-1",
        worker_state=worker_state,
        result_queue=result_queue,
        cancel_event=threading.Event(),
    )

    first_message = result_queue.get_nowait()
    done_message = result_queue.get_nowait()

    assert first_message["type"] == "results"
    assert first_message["failures"][0]["error"] == "Worker timeout exceeded"
    assert worker_state.status == "timeout"
    assert done_message["type"] == "done"


def test_run_iw51_demandante_dispatches_true_parallel_mode(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "A", None],
                ["10443893", "112235353", "B", None],
                ["10443894", "112235354", "C", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=3,
        session_count=3,
        execution_mode="true_parallel",
        workbook_sync_batch_size=1,
    )
    logger = _Logger()
    seen_parallel: list[tuple[int, int]] = []

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="B"),
                Iw51WorkItem(row_index=4, pn="10443894", instalacao="112235354", tipologia="C"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [
            SimpleNamespace(Id="/app/con[0]/ses[0]"),
            SimpleNamespace(Id="/app/con[0]/ses[1]"),
            SimpleNamespace(Id="/app/con[0]/ses[2]"),
        ],
    )
    monkeypatch.setattr(
        iw51_module,
        "_run_iw51_true_parallel_workers",
        lambda **kwargs: [
            kwargs["apply_worker_results"](
                worker_index,
                [
                    Iw51ItemResult(
                        row_index=group[0].row_index,
                        pn=group[0].pn,
                        instalacao=group[0].instalacao,
                        tipologia=group[0].tipologia,
                        worker_index=worker_index,
                        elapsed_seconds=0.1,
                    )
                ],
                [],
            )
            or seen_parallel.append((worker_index, group[0].row_index))
            for worker_index, group in enumerate(kwargs["groups"], start=1)
        ],
    )

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    manifest = run_iw51_demandante(
        run_id="iw51-parallel-001",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=3,
    )

    assert manifest.successful_rows == 3
    assert sorted(seen_parallel) == [(1, 2), (2, 3), (3, 4)]


def test_run_iw51_demandante_dispatches_process_parallel_mode(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "A", None],
                ["10443893", "112235353", "B", None],
                ["10443894", "112235354", "C", None],
            ]
        )
    )
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=3,
        session_count=3,
        execution_mode="process_parallel",
        workbook_sync_batch_size=1,
    )
    logger = _Logger()
    seen_parallel: list[tuple[int, int]] = []

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="B"),
                Iw51WorkItem(row_index=4, pn="10443894", instalacao="112235354", tipologia="C"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [
            SimpleNamespace(Id="/app/con[0]/ses[0]"),
            SimpleNamespace(Id="/app/con[0]/ses[1]"),
            SimpleNamespace(Id="/app/con[0]/ses[2]"),
        ],
    )

    def _fake_process_parallel_runner(**kwargs):  # noqa: ANN001
        for worker_index, group in enumerate(kwargs["groups"], start=1):
            kwargs["apply_worker_results"](
                worker_index,
                [
                    Iw51ItemResult(
                        row_index=group[0].row_index,
                        pn=group[0].pn,
                        instalacao=group[0].instalacao,
                        tipologia=group[0].tipologia,
                        worker_index=worker_index,
                        elapsed_seconds=0.1,
                    )
                ],
                [],
            )
            seen_parallel.append((worker_index, group[0].row_index))
        return {"1": 0, "2": 0, "3": 0}, {"1": 0, "2": 0, "3": 0}, {"1": 0.0, "2": 0.0, "3": 0.0}

    monkeypatch.setattr(iw51_module, "_run_iw51_process_parallel_workers", _fake_process_parallel_runner)

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    manifest = run_iw51_demandante(
        run_id="iw51-process-parallel-001",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=3,
    )

    assert manifest.successful_rows == 3
    assert manifest.supervisor_mode == "process_parallel"
    assert sorted(seen_parallel) == [(1, 2), (2, 3), (3, 4)]


def test_run_iw51_demandante_apply_worker_results_requires_main_thread(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_workbook = tmp_path / "projeto_Dani2.xlsm"
    source_workbook.write_text("source workbook", encoding="utf-8")
    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["10443892", "112235352", "A", None],
                ["10443893", "112235353", "B", None],
            ]
        )
    )
    logger = _Logger()
    assertion_messages: list[str] = []
    settings = Iw51Settings(
        demandante="DANI",
        workbook_path=source_workbook,
        sheet_name="Macro1",
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=2,
        session_count=2,
        execution_mode="true_parallel",
        workbook_sync_batch_size=1,
    )

    monkeypatch.setattr(iw51_module, "load_export_config", lambda path: {"iw51": {}})
    monkeypatch.setattr(iw51_module, "load_iw51_settings", lambda **kwargs: settings)
    monkeypatch.setattr(
        iw51_module,
        "load_iw51_work_items",
        lambda **kwargs: (
            workbook,
            workbook._sheet,
            4,
            [
                Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="A"),
                Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="B"),
            ],
            0,
            [],
        ),
    )
    monkeypatch.setattr(iw51_module, "configure_run_logger", lambda **kwargs: (logger, tmp_path / "iw51.log"))
    monkeypatch.setattr(
        iw51_module,
        "prepare_iw51_sessions",
        lambda **kwargs: [
            SimpleNamespace(Id="/app/con[0]/ses[0]"),
            SimpleNamespace(Id="/app/con[0]/ses[1]"),
        ],
    )

    def _fake_parallel_runner(**kwargs):  # noqa: ANN001
        def _call_from_worker_thread() -> None:
            try:
                kwargs["apply_worker_results"](1, [], [])
            except AssertionError as exc:
                assertion_messages.append(str(exc))

        thread = threading.Thread(target=_call_from_worker_thread, name="iw51-test-worker")
        thread.start()
        thread.join()

    monkeypatch.setattr(iw51_module, "_run_iw51_true_parallel_workers", _fake_parallel_runner)

    import sap_automation.service as service_module

    monkeypatch.setattr(
        service_module,
        "create_session_provider",
        lambda config=None: SimpleNamespace(get_session=lambda config, logger=None: object()),
    )

    run_iw51_demandante(
        run_id="iw51-main-thread-guard",
        demandante="DANI",
        config_path=Path("sap_iw69_batch_config.json"),
        output_root=tmp_path,
        max_rows=2,
    )

    assert assertion_messages
    assert "collector thread" in assertion_messages[0]


def test_run_iw51_process_parallel_workers_restarts_after_session_rebuild_request(monkeypatch) -> None:  # noqa: ANN001
    logger = _Logger()
    worker_states = [Iw51WorkerState(worker_index=1, session_id="/app/con[0]/ses[0]")]
    locator = Iw51SessionLocator(connection_index=0, session_index=0)
    group = [Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="A")]
    sessions = [SimpleNamespace(Id="/app/con[0]/ses[0]")]
    apply_calls: list[tuple[int, list[int], list[int]]] = []
    launches: list[int] = []
    rebuild_calls: list[int] = []

    monkeypatch.setattr(iw51_module, "_iw51_get_multiprocessing_context", lambda: _FakeMpContext())

    def _fake_start_worker(**kwargs):  # noqa: ANN001
        launches.append(kwargs["start_index"])
        result_queue = kwargs["result_queue"]
        if len(launches) == 1:
            result_queue.put({"type": "heartbeat", "worker_index": 1, "sent_at": time.time()})
            result_queue.put({"type": "item_started", "worker_index": 1, "resume_index": 0, "row_index": 2, "sent_at": time.time()})
            result_queue.put({"type": "needs_session_rebuild", "worker_index": 1, "resume_index": 0, "row_index": 2, "error": "rpc"})
        else:
            result_queue.put({"type": "heartbeat", "worker_index": 1, "sent_at": time.time()})
            result_queue.put({"type": "item_started", "worker_index": 1, "resume_index": 0, "row_index": 2, "sent_at": time.time()})
            result_queue.put(
                {
                    "type": "results",
                    "worker_index": 1,
                    "results": [
                        Iw51ItemResult(
                            row_index=2,
                            pn="10443892",
                            instalacao="112235352",
                            tipologia="A",
                            worker_index=1,
                            elapsed_seconds=0.5,
                        )
                    ],
                    "failures": [],
                }
            )
            result_queue.put({"type": "done", "worker_index": 1, "resume_index": 1})
        return _FakeProcess(exitcode=0, alive=False)

    monkeypatch.setattr(iw51_module, "_start_iw51_process_worker", _fake_start_worker)
    monkeypatch.setattr(
        iw51_module,
        "_rebuild_iw51_worker_session",
        lambda **kwargs: rebuild_calls.append(kwargs["worker_index"]) or SimpleNamespace(Id="/app/con[0]/ses[0]"),
    )

    worker_restarts, session_rebuilds, heartbeat_lag_seconds = _run_iw51_process_parallel_workers(
        groups=[group],
        session_locators=[locator],
        sessions=sessions,
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=0.0,
            max_rows_per_run=1,
            session_count=1,
            execution_mode="process_parallel",
            worker_restart_limit=1,
            worker_item_timeout_seconds=60.0,
            worker_restart_backoff_seconds=0.0,
            session_rebuild_backoff_seconds=0.0,
        ),
        logger=logger,
        worker_states=worker_states,
        apply_worker_results=lambda worker_index, results, failures: apply_calls.append(
            (worker_index, [item.row_index for item in results], [int(failure["row_index"]) for failure in failures])
        ),
    )

    assert launches == [0, 0]
    assert rebuild_calls == [1]
    assert apply_calls == [(1, [2], [])]
    assert worker_restarts == {"1": 1}
    assert session_rebuilds == {"1": 1}
    assert "1" in heartbeat_lag_seconds


def test_iw51_process_worker_respects_inter_item_sleep(monkeypatch) -> None:  # noqa: ANN001
    result_queue: queue.Queue[dict[str, object]] = queue.Queue()
    sleep_calls: list[float] = []
    items = [
        Iw51WorkItem(row_index=2, pn="10443892", instalacao="112235352", tipologia="A"),
        Iw51WorkItem(row_index=3, pn="10443893", instalacao="112235353", tipologia="B"),
    ]
    calls = {"count": 0}

    monkeypatch.setattr(iw51_module, "_co_initialize", lambda apartment_threaded=False: (None, False))
    monkeypatch.setattr(iw51_module, "_register_iw51_message_filter", lambda **kwargs: (None, None))
    monkeypatch.setattr(iw51_module, "_unregister_iw51_message_filter", lambda previous_filter, logger: None)
    monkeypatch.setattr(iw51_module, "_get_sap_application", lambda: object())
    monkeypatch.setattr(iw51_module, "_reattach_iw51_session", lambda **kwargs: SimpleNamespace(Id="/app/con[0]/ses[0]"))
    monkeypatch.setattr(iw51_module, "_wait_session_ready", lambda *args, **kwargs: None)

    def _fake_process_item(**kwargs):  # noqa: ANN001
        item = kwargs["item"]
        calls["count"] += 1
        return (
            Iw51ItemResult(
                row_index=item.row_index,
                pn=item.pn,
                instalacao=item.instalacao,
                tipologia=item.tipologia,
                worker_index=kwargs["worker_index"],
                elapsed_seconds=0.1,
            ),
            None,
            kwargs["session"],
        )

    monkeypatch.setattr(iw51_module, "_process_iw51_item_with_retry", _fake_process_item)
    monkeypatch.setattr(iw51_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    _iw51_process_worker_main(
        worker_index=1,
        session_locator=Iw51SessionLocator(connection_index=0, session_index=0),
        items=items,
        start_index=0,
        settings=Iw51Settings(
            demandante="DANI",
            workbook_path=Path("projeto_Dani2.xlsm"),
            sheet_name="Macro1",
            inter_item_sleep_seconds=1.0,
            max_rows_per_run=2,
            session_count=1,
            execution_mode="process_parallel",
        ),
        result_queue=result_queue,
    )

    messages = []
    while not result_queue.empty():
        messages.append(result_queue.get_nowait())

    assert calls["count"] == 2
    assert 1.0 in sleep_calls
    assert any(message["type"] == "done" for message in messages)


def test_recover_iw51_run_artifacts_from_log_rebuilds_ledger_and_syncs_workbook(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    run_root = tmp_path / "20260330T171500"
    (run_root / "logs").mkdir(parents=True)
    (run_root / "iw51").mkdir(parents=True)
    (run_root / "logs" / "sap_session.log").write_text(
        "\n".join(
            [
                "2026-03-31 09:00:00 [INFO] IW51 item completed worker=1 row=2 pn=111 instalacao=222 tipologia=TIPO A",
                "2026-03-31 09:00:01 [INFO] IW51 item OK worker=1 row=2 elapsed_s=1.23",
                "2026-03-31 09:00:02 [INFO] IW51 item completed worker=2 row=3 pn=333 instalacao=444 tipologia=TIPO B",
                "2026-03-31 09:00:03 [INFO] IW51 item OK worker=2 row=3 elapsed_s=2.34",
            ]
        ),
        encoding="utf-8",
    )
    (run_root / "iw51" / "iw51_manifest.json").write_text(
        '{"run_id":"20260330T171500","status":"partial","successful_rows":0,"failed_rows":[]}',
        encoding="utf-8",
    )

    workbook = _FakeWorkbook(
        _FakeSheet(
            [
                ["pn", "INSTALAÇÃO", "TIPOLOGIA", "FEITO"],
                ["111", "222", "TIPO A", None],
                ["333", "444", "TIPO B", None],
            ]
        )
    )
    workbook_path = tmp_path / "projeto_Dani2.xlsm"
    workbook_path.write_text("source workbook", encoding="utf-8")
    openpyxl_stub = type(
        "OpenPyxlStub",
        (),
        {"load_workbook": staticmethod(lambda path, keep_vba=True: workbook)},
    )
    monkeypatch.setattr(iw51_module, "_openpyxl_module", lambda: openpyxl_stub)

    summary = recover_iw51_run_artifacts_from_log(
        run_root=run_root,
        workbook_path=workbook_path,
        sheet_name="Macro1",
    )

    ledger_rows = list(csv.DictReader((run_root / "iw51" / "iw51_progress.csv").open(encoding="utf-8")))
    manifest_data = (run_root / "iw51" / "iw51_manifest.json").read_text(encoding="utf-8")

    assert summary["recovered_success_rows"] == 2
    assert summary["workbook_rows_synced"] == 2
    assert [row["row_index"] for row in ledger_rows] == ["2", "3"]
    assert ledger_rows[0]["status"] == "success"
    assert workbook._sheet.cell(row=2, column=4).value == "SIM"
    assert workbook._sheet.cell(row=3, column=4).value == "SIM"
    assert '"successful_rows": 2' in manifest_data
