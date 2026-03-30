from __future__ import annotations

import csv
import threading
from pathlib import Path
from types import SimpleNamespace

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
    append_iw51_progress_ledger,
    execute_iw51_item,
    load_iw51_settings,
    load_iw51_work_items,
    run_iw51_demandante,
    split_iw51_work_items_evenly,
    _run_iw51_interleaved_workers,
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
        inter_item_sleep_seconds=0.0,
        max_rows_per_run=3000,
        notification_type="CA",
        reference_notification_number="389496787",
        session_count=3,
        execution_mode="true_parallel",
        post_login_wait_seconds=6.0,
        workbook_sync_batch_size=250,
        circuit_breaker_fast_fail_threshold=10,
        circuit_breaker_slow_fail_threshold=30,
        per_step_timeout_seconds=30.0,
        session_recovery_mode="soft",
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

    success_rows, terminal_rows = _load_iw51_ledger_state(ledger_path)

    assert success_rows == {2}
    assert terminal_rows == {2, 4}


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
