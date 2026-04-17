from __future__ import annotations

from datetime import date
from pathlib import Path

import openpyxl

from sap_automation.medidor import (
    MedidorExtractor,
    compact_medidor_raw_exports,
    collect_equipments_from_el31_export,
    collect_iq09_grpreg_by_equipment,
    compute_medidor_el31_period,
    load_grpreg_type_map,
    load_workbook_column,
    write_medidor_final_csv,
)


def _write_xlsx(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(headers)
    for row in rows:
        sheet.append(row)
    workbook.save(path)


def test_compute_medidor_el31_period_uses_previous_year_start_to_today() -> None:
    period_from, period_to, reference = compute_medidor_el31_period(date(2026, 4, 16))

    assert period_from == "01.01.2025"
    assert period_to == "16.04.2026"
    assert reference == "20260416"


def test_load_workbook_column_deduplicates_installations(tmp_path: Path) -> None:
    workbook_path = tmp_path / "instalacaosp.xlsx"
    _write_xlsx(
        workbook_path,
        ["INSTALACAO"],
        [["ATE0000002"], ["MTE0012436"], ["ATE0000002"], [""]],
    )

    values = load_workbook_column(workbook_path, column_name="INSTALACAO")

    assert values == ["ATE0000002", "MTE0012436"]


def test_load_grpreg_type_map_reads_expected_columns(tmp_path: Path) -> None:
    workbook_path = tmp_path / "gruporegsap.xlsx"
    _write_xlsx(
        workbook_path,
        ["Grp.registrad.", "Tipo"],
        [["AD30002N", "Digital"], ["DA01010", "Analógico"]],
    )

    mapping = load_grpreg_type_map(workbook_path)

    assert mapping == {"AD30002N": "Digital", "DA01010": "Analógico"}


def test_collect_exports_and_write_final_classified_csv(tmp_path: Path) -> None:
    el31_path = tmp_path / "el31.txt"
    iq09_path = tmp_path / "iq09.txt"
    final_path = tmp_path / "final.csv"
    el31_path.write_text(
        "Instalação\tEquipamento\nATE0000002\tEQ001\nMTE0012436\tEQ002\n",
        encoding="cp1252",
    )
    iq09_path.write_text("Equipamento\tGrpReg.\nEQ001\tAD30002N\nEQ002\tDA01010\n", encoding="utf-8")

    el31_rows, equipments = collect_equipments_from_el31_export(el31_path)
    iq09_mapping = collect_iq09_grpreg_by_equipment([iq09_path])
    rows_written = write_medidor_final_csv(
        el31_rows=el31_rows,
        iq09_grpreg_by_equipment=iq09_mapping,
        grpreg_type_map={"AD30002N": "Digital", "DA01010": "Analógico"},
        output_path=final_path,
    )

    assert equipments == ["EQ001", "EQ002"]
    assert iq09_mapping == {"EQ001": "AD30002N", "EQ002": "DA01010"}
    assert rows_written == 2
    assert final_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,equipamento,grp_reg,tipo",
        "ATE0000002,EQ001,AD30002N,Digital",
        "MTE0012436,EQ002,DA01010,Analógico",
    ]


def test_compact_medidor_raw_exports_deduplicates_all_raw_txt(tmp_path: Path) -> None:
    raw_dir = tmp_path / "runs" / "run-medidor" / "medidor" / "raw"
    raw_dir.mkdir(parents=True)
    output_path = tmp_path / "compactado.csv"
    (raw_dir / "el31_medidor_20260416_run-medidor_001.txt").write_text(
        "Instalação\tEquipamento\nATE0000002\tEQ001\nMTE0012436\tEQ002\n",
        encoding="utf-8",
    )
    (raw_dir / "el31_medidor_20260416_run-medidor_002.txt").write_text(
        "Instalação\tEquipamento\nATE0000002\tEQ001\nXTE9999999\tEQ003\n",
        encoding="utf-8",
    )

    result = compact_medidor_raw_exports(
        raw_dir=raw_dir,
        output_csv_path=output_path,
    )

    assert result.el31_rows_read == 4
    assert result.deduped_rows_written == 3
    assert result.duplicate_equipments_removed == 1
    assert result.iq09_raw_paths == []
    assert result.equipments_without_iq09_group == 0
    assert result.equipments_without_type == 0
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,equipamento",
        "ATE0000002,EQ001",
        "MTE0012436,EQ002",
        "XTE9999999,EQ003",
    ]
    assert Path(result.manifest_path).exists()


def test_compact_medidor_raw_exports_reads_sap_alv_txt_with_preamble(tmp_path: Path) -> None:
    raw_dir = tmp_path / "runs" / "run-medidor" / "medidor" / "raw"
    raw_dir.mkdir(parents=True)
    output_path = tmp_path / "compactado.csv"
    (raw_dir / "el31_medidor_20260416_run-medidor_001.txt").write_text(
        "\n".join(
            [
                "Relatorio EL31",
                "-----------------------------------------------",
                "|Instalação |Unid.leit .|Equipamento|TL |DtaLeitPr.|",
                "|ATE0000002 |001        |EQ001      |A  |16.04.2026|",
                "|MTE0012436 |002        |EQ002      |B  |16.04.2026|",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = compact_medidor_raw_exports(raw_dir=raw_dir, output_csv_path=output_path)

    assert result.el31_rows_read == 2
    assert result.deduped_rows_written == 2
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,equipamento",
        "ATE0000002,EQ001",
        "MTE0012436,EQ002",
    ]


def test_compact_medidor_raw_exports_normalizes_replacement_char_installation_header(tmp_path: Path) -> None:
    raw_dir = tmp_path / "runs" / "run-medidor" / "medidor" / "raw"
    raw_dir.mkdir(parents=True)
    output_path = tmp_path / "compactado.csv"
    (raw_dir / "el31_medidor_20260416_run-medidor_001.txt").write_text(
        "Instala��o\tUnid.leit .\tEquipamento\tTL \tDtaLeitPr.\nATE0000002\t001\tEQ001\tA\t16.04.2026\n",
        encoding="utf-8",
    )

    result = compact_medidor_raw_exports(raw_dir=raw_dir, output_csv_path=output_path)

    assert result.deduped_rows_written == 1
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,equipamento",
        "ATE0000002,EQ001",
    ]


def test_compact_medidor_raw_exports_can_include_iq09_when_requested(tmp_path: Path) -> None:
    raw_dir = tmp_path / "runs" / "run-medidor" / "medidor" / "raw"
    raw_dir.mkdir(parents=True)
    group_map_path = tmp_path / "gruporegsap.xlsx"
    output_path = tmp_path / "compactado.csv"
    _write_xlsx(group_map_path, ["Grp.registrad.", "Tipo"], [["AD30002N", "Digital"], ["DA01010", "Analógico"]])
    (raw_dir / "el31_medidor_20260416_run-medidor_001.txt").write_text(
        "Instalação\tEquipamento\nATE0000002\tEQ001\nMTE0012436\tEQ002\n",
        encoding="utf-8",
    )
    (raw_dir / "iq09_medidor_20260416_run-medidor_001.txt").write_text(
        "Equipamento\tGrpReg.\nEQ001\tAD30002N\nEQ002\tDA01010\n",
        encoding="utf-8",
    )

    result = compact_medidor_raw_exports(
        raw_dir=raw_dir,
        group_map_path=group_map_path,
        output_csv_path=output_path,
        include_iq09=True,
    )

    assert result.deduped_rows_written == 2
    assert result.equipments_without_iq09_group == 0
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,equipamento,grp_reg,tipo",
        "ATE0000002,EQ001,AD30002N,Digital",
        "MTE0012436,EQ002,DA01010,Analógico",
    ]


def test_medidor_extractor_runs_el31_then_iq09_and_writes_manifest(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    installations_path = tmp_path / "instalacaosp.xlsx"
    group_map_path = tmp_path / "gruporegsap.xlsx"
    _write_xlsx(installations_path, ["INSTALACAO"], [["ATE0000002"], ["MTE0012436"]])
    _write_xlsx(group_map_path, ["Grp.registrad.", "Tipo"], [["AD30002N", "Digital"], ["DA01010", "Analógico"]])
    calls: list[str] = []

    def _fake_el31(self, **kwargs):  # noqa: ANN001, ANN003
        calls.append(f"el31:{len(kwargs['installations'])}")
        equipment_by_installation = {"ATE0000002": "EQ001", "MTE0012436": "EQ002"}
        lines = ["Instalação\tEquipamento"]
        lines.extend(
            f"{installation}\t{equipment_by_installation[installation]}"
            for installation in kwargs["installations"]
        )
        kwargs["output_path"].write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _fake_iq09(self, **kwargs):  # noqa: ANN001, ANN003
        calls.append(f"iq09:{len(kwargs['equipments'])}")
        kwargs["output_path"].write_text("Equipamento\tGrpReg.\nEQ001\tAD30002N\nEQ002\tDA01010\n", encoding="utf-8")

    monkeypatch.setattr(MedidorExtractor, "_run_el31", _fake_el31)
    monkeypatch.setattr(MedidorExtractor, "_run_iq09", _fake_iq09)

    manifest = MedidorExtractor().execute(
        output_root=tmp_path,
        run_id="run-medidor",
        demandante="MEDIDOR",
        session=object(),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        config={
            "global": {"wait_timeout_seconds": 120},
            "medidor": {
                "el31_chunk_size": 2000,
                "iq09_chunk_size": 5000,
                "demandantes": {
                    "MEDIDOR": {
                        "installations_path": str(installations_path),
                        "group_map_path": str(group_map_path),
                        "el31_chunk_size": 1,
                        "iq09_chunk_size": 5000,
                    }
                },
            },
        },
        installations_path=installations_path,
        group_map_path=group_map_path,
        today=date(2026, 4, 16),
    )

    assert calls == ["el31:1", "el31:1", "iq09:2"]
    assert manifest.status == "success"
    assert manifest.total_installations == 2
    assert manifest.total_equipments == 2
    assert manifest.el31_chunk_size == 1
    assert manifest.el31_chunk_count == 2
    assert manifest.rows_written == 2
    assert len(manifest.raw_el31_paths) == 2
    assert Path(manifest.manifest_path).exists()
    assert Path(manifest.final_csv_path).exists()
