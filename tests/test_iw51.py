from __future__ import annotations

from pathlib import Path

from sap_automation.config import load_export_config
from sap_automation.iw51 import (
    _IW51_DONE_HEADER,
    Iw51Settings,
    load_iw51_settings,
    load_iw51_work_items,
)


class _FakeCell:
    def __init__(self, value=None) -> None:  # noqa: ANN001
        self.value = value


class _FakeSheet:
    def __init__(self) -> None:
        self._cells: dict[tuple[int, int], _FakeCell] = {
            (1, 1): _FakeCell("pn"),
            (1, 2): _FakeCell("INSTALAÇÃO"),
            (1, 3): _FakeCell("TIPOLOGIA"),
            (2, 1): _FakeCell("10443892"),
            (2, 2): _FakeCell("112235352"),
            (2, 3): _FakeCell("COMUNICACAO ENDERECO SP"),
            (3, 1): _FakeCell("10478355"),
            (3, 2): _FakeCell("123453453"),
            (3, 3): _FakeCell("COMUNICACAO ENDERECO SP"),
            (3, 4): _FakeCell("SIM"),
            (4, 1): _FakeCell("10479336"),
            (4, 2): _FakeCell("74518828"),
            (4, 3): _FakeCell("COMUNICACAO ENDERECO SP"),
        }
        self.max_row = 4
        self.max_column = 3

    def __getitem__(self, row_index: int):  # noqa: ANN001
        return [self.cell(row=row_index, column=index) for index in range(1, self.max_column + 1)]

    def cell(self, row: int, column: int, value=None):  # noqa: ANN001
        if value is not None:
            self._cells[(row, column)] = _FakeCell(value)
            if column > self.max_column:
                self.max_column = column
            if row > self.max_row:
                self.max_row = row
        return self._cells.setdefault((row, column), _FakeCell())


class _FakeWorkbook:
    def __init__(self, sheet: _FakeSheet) -> None:
        self.sheetnames = ["Macro1"]
        self._sheet = sheet

    def __getitem__(self, sheet_name: str) -> _FakeSheet:
        if sheet_name != "Macro1":
            raise KeyError(sheet_name)
        return self._sheet


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
        inter_item_sleep_seconds=30.0,
        max_rows_per_run=4,
        notification_type="CA",
        reference_notification_number="389496787",
    )


def test_load_iw51_work_items_adds_feito_and_skips_completed(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    workbook = _FakeWorkbook(_FakeSheet())
    openpyxl_stub = type(
        "OpenPyxlStub",
        (),
        {"load_workbook": staticmethod(lambda path, keep_vba=True: workbook)},
    )
    monkeypatch.setattr("sap_automation.iw51._openpyxl_module", lambda: openpyxl_stub)

    _, sheet, feito_column_index, items, skipped_rows = load_iw51_work_items(
        workbook_path=tmp_path / "projeto_Dani2.xlsm",
        sheet_name="Macro1",
        max_rows=4,
    )

    assert sheet.cell(row=1, column=feito_column_index).value == _IW51_DONE_HEADER
    assert skipped_rows == 1
    assert [item.row_index for item in items] == [2, 4]
    assert items[0].pn == "10443892"
    assert items[0].instalacao == "112235352"
    assert items[0].tipologia == "COMUNICACAO ENDERECO SP"
