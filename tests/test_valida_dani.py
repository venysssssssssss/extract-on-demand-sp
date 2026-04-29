from __future__ import annotations

import sys
from types import SimpleNamespace
from pathlib import Path

import openpyxl
import pytest

from sap_automation.valida_dani import (
    DaniValidationRow,
    chunk_list,
    compare_dani_rows,
    execute_iw59_chunk,
    parse_sap_text_export,
    read_dani_excel,
)


class _FakeSapControl:
    def __init__(self, control_id: str, session: _FakeSapSession) -> None:
        self.control_id = control_id
        self.session = session
        self.text = ""
        self.caretPosition = 0

    def maximize(self) -> None:
        self.session.calls.append(("maximize", self.control_id))

    def sendVKey(self, key: int) -> None:
        self.session.calls.append(("sendVKey", self.control_id, key))

    def setFocus(self) -> None:
        self.session.calls.append(("setFocus", self.control_id))

    def press(self) -> None:
        self.session.calls.append(("press", self.control_id))

    def select(self) -> None:
        self.session.calls.append(("select", self.control_id))


class _FakeSapSession:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.controls: dict[str, _FakeSapControl] = {}

    def findById(self, control_id: str) -> _FakeSapControl:
        self.calls.append(("findById", control_id))
        if control_id not in self.controls:
            self.controls[control_id] = _FakeSapControl(control_id, self)
        return self.controls[control_id]


def test_read_dani_excel_uses_columns_a_b_c(tmp_path: Path) -> None:
    workbook_path = tmp_path / "projeto_Dani2.xlsm"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["CLIENTE", "INSTALACAO", "DESCRICAO", "IGNORAR"])
    sheet.append([123.0, "00045", "Ligacao improcedente", "x"])
    sheet.append([None, "skip", "skip", "x"])
    workbook.save(workbook_path)

    rows = read_dani_excel(workbook_path)

    assert rows == [
        DaniValidationRow(cliente="123", instalacao="00045", descricao="Ligacao improcedente")
    ]


def test_chunk_list_uses_five_thousand_boundaries() -> None:
    values = [str(index) for index in range(10001)]

    chunks = list(chunk_list(values, 5000))

    assert [len(chunk) for chunk in chunks] == [5000, 5000, 1]
    assert chunks[0][0] == "0"
    assert chunks[1][0] == "5000"


def test_parse_sap_text_export_reads_required_validation_columns(tmp_path: Path) -> None:
    export_path = tmp_path / "iw59.txt"
    export_path.write_text(
        "CLIENTE;INSTALAÇÃO;DESCRIÇÃO;OUTRA\n"
        "000123;00045;Ligação improcedente;x\n",
        encoding="cp1252",
    )

    header, rows, raw_lines = parse_sap_text_export(export_path)

    assert header == ["CLIENTE", "INSTALAÇÃO", "DESCRIÇÃO", "OUTRA"]
    assert rows == [
        DaniValidationRow(cliente="000123", instalacao="00045", descricao="Ligação improcedente")
    ]
    assert raw_lines == ["000123;00045;Ligação improcedente;x"]


def test_compare_dani_rows_requires_cliente_instalacao_and_descricao() -> None:
    original = [
        DaniValidationRow(cliente="123", instalacao="45", descricao="Ligacao improcedente"),
        DaniValidationRow(cliente="124", instalacao="46", descricao="Texto esperado"),
    ]
    extracted = [
        DaniValidationRow(cliente="000123", instalacao="00045", descricao="Ligação improcedente"),
        DaniValidationRow(cliente="124", instalacao="46", descricao="Texto divergente"),
    ]

    result = compare_dani_rows(original, extracted)

    assert [row["ENCONTRADO_NO_SAP"] for row in result] == ["SIM", "NAO"]


def test_execute_iw59_chunk_pastes_clientes_in_kunum_multiselect(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    clipboard_state: dict[str, str | int] = {"text": "", "opens": 0, "closes": 0}
    fake_clipboard = SimpleNamespace(
        OpenClipboard=lambda: clipboard_state.__setitem__("opens", int(clipboard_state["opens"]) + 1),
        EmptyClipboard=lambda: clipboard_state.__setitem__("text", ""),
        SetClipboardText=lambda text: clipboard_state.__setitem__("text", text),
        CloseClipboard=lambda: clipboard_state.__setitem__("closes", int(clipboard_state["closes"]) + 1),
    )
    monkeypatch.setitem(sys.modules, "win32clipboard", fake_clipboard)
    session = _FakeSapSession()

    execute_iw59_chunk(
        session,
        ["0001", "0002", "0003"],
        tmp_path / "iw59_export_chunk_1.txt",
        created_by="BR0041761455",
    )

    assert clipboard_state == {"text": "0001\r\n0002\r\n0003", "opens": 1, "closes": 1}
    assert ("press", "wnd[0]/usr/btn%_KUNUM_%_APP_%-VALU_PUSH") in session.calls
    assert session.calls.count(("press", "wnd[1]/tbar[0]/btn[24]")) == 3
    assert ("press", "wnd[1]/tbar[0]/btn[8]") in session.calls
    assert session.controls["wnd[0]/usr/ctxtERNAM-LOW"].text == "BR0041761455"
    assert session.controls["wnd[0]/usr/ctxtVARIANT"].text == "/misV"


def test_execute_iw59_chunk_rejects_more_than_five_thousand(tmp_path: Path) -> None:
    session = _FakeSapSession()

    with pytest.raises(ValueError, match="exceeds 5000"):
        execute_iw59_chunk(
            session,
            [str(index) for index in range(5001)],
            tmp_path / "iw59_export_chunk_1.txt",
        )
