from __future__ import annotations

from pathlib import Path

import openpyxl

from sap_automation.valida_dani import (
    DaniValidationRow,
    chunk_list,
    compare_dani_rows,
    parse_sap_text_export,
    read_dani_excel,
)


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
