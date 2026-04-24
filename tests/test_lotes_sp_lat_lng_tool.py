from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, select

from sap_automation.lotes_sp_lat_lng_repository import (
    LotesLatLngRepository,
    instalacoes_coordenadas_sp,
)
from sap_automation.lotes_sp_lat_lng_tool import (
    _extract_lote_number,
    _merge_coordinate_rows,
    _normalize_installation,
    _select_lote_files,
    extract_lotes_lat_lng,
)


class _FakeCell:
    def __init__(self, value):
        self.v = value


class _FakeSheet:
    def __init__(self, rows: list[list[object]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_FakeSheet":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def rows(self):
        for row in self._rows:
            yield [_FakeCell(value) for value in row]

    def iter_rows(self):
        yield from self._rows


class _FakeWorkbook:
    def __init__(self, rows: list[list[object]]) -> None:
        self.sheets = ["Planilha2"]
        self.sheet_names = ["Planilha2"]
        self._rows = rows

    def __enter__(self) -> "_FakeWorkbook":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get_sheet(self, name: str) -> _FakeSheet:
        assert name == "Planilha2"
        return _FakeSheet(self._rows)

    def get_sheet_by_name(self, name: str) -> _FakeSheet:
        assert name == "Planilha2"
        return _FakeSheet(self._rows)

    def close(self) -> None:
        return None


def test_extract_lote_number_supports_suffix_a() -> None:
    assert _extract_lote_number("Lote 10 A.xlsb") == 10
    assert _extract_lote_number("Lote 02.xlsb") == 2
    assert _extract_lote_number("outro.xlsb") is None


def test_select_lote_files_filters_requested_interval(tmp_path: Path) -> None:
    for name in ["Lote 01.xlsb", "Lote 10 A.xlsb", "Lote 17 A.xlsb", "Lote 21.xlsb"]:
        (tmp_path / name).write_text("", encoding="utf-8")

    files = _select_lote_files(tmp_path, lote_start=1, lote_end=20)

    assert [path.name for path in files] == ["Lote 01.xlsb", "Lote 10 A.xlsb", "Lote 17 A.xlsb"]


def test_normalize_installation_keeps_only_digits() -> None:
    assert _normalize_installation("204768961.0") == "204768961"
    assert _normalize_installation(" 20.476.896-1 ") == "204768961"
    assert _normalize_installation("") == ""


def test_merge_coordinate_rows_prefers_existing_and_fills_blanks() -> None:
    merged = _merge_coordinate_rows(
        {
            "instalacao": "123",
            "lat_da_instalacao": "",
            "lng_da_instalacao": "",
            "lat_da_leitura": "-23.1",
            "lng_da_leitura": "",
        },
        {
            "instalacao": "123",
            "lat_da_instalacao": "-23.9",
            "lng_da_instalacao": "-46.5",
            "lat_da_leitura": "",
            "lng_da_leitura": "-46.1",
        },
    )

    assert merged == {
        "instalacao": "123",
        "lat_da_instalacao": "-23.9",
        "lng_da_instalacao": "-46.5",
        "lat_da_leitura": "-23.1",
        "lng_da_leitura": "-46.1",
    }


def test_extract_lotes_lat_lng_deduplicates_by_installation(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_dir = tmp_path / "lotes"
    source_dir.mkdir()
    for name in ["Lote 01.xlsb", "Lote 02.xlsb"]:
        (source_dir / name).write_text("", encoding="utf-8")

    rows = [
        [
            "N°pos.enc.",
            "Nº ordem",
            "Nº rota",
            "Instalação",
            "Unidade",
            "Latitude inst",
            "Longitude inst",
            "Latitude lido",
            "Longitude lido",
        ],
        [1, 2, 3, 204768961.0, "U1", "", "", -23.7718727, -46.431551],
        [4, 5, 6, 204768961.0, "U1", -23.7718700, -46.4315500, "", ""],
        [7, 8, 9, 204768960.0, "U2", "", "", "", ""],
    ]

    monkeypatch.setattr(
        "sap_automation.lotes_sp_lat_lng_tool.load_calamine_workbook",
        lambda path: _FakeWorkbook(rows),
    )

    output_csv_path = tmp_path / "output.csv"
    manifest_path = tmp_path / "manifest.json"
    result = extract_lotes_lat_lng(
        source_dir=source_dir,
        lote_start=1,
        lote_end=20,
        output_csv_path=output_csv_path,
        manifest_path=manifest_path,
    )

    assert result.status == "success"
    assert result.rows_read == 6
    assert result.rows_written == 2
    assert result.duplicate_installations_removed == 4
    assert output_csv_path.read_text(encoding="utf-8").splitlines() == [
        "instalacao,lat_da_instalacao,lng_da_instalacao,lat_da_leitura,lng_da_leitura",
        "204768960,,,,",
        "204768961,-23.77187,-46.43155,-23.7718727,-46.431551",
    ]


def test_extract_lotes_lat_lng_finds_header_after_preamble(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    source_dir = tmp_path / "lotes"
    source_dir.mkdir()
    (source_dir / "Lote 01.xlsb").write_text("", encoding="utf-8")
    rows = [
        ["saida dinamica", None, None],
        [None, None, None],
        ["N°pos.enc.", "Instalação", "Latitude inst", "Longitude inst", "Latitude lido", "Longitude lido"],
        [1, 204768961.0, "", "", -23.7718727, -46.431551],
    ]

    monkeypatch.setattr(
        "sap_automation.lotes_sp_lat_lng_tool.load_calamine_workbook",
        lambda path: _FakeWorkbook(rows),
    )

    result = extract_lotes_lat_lng(
        source_dir=source_dir,
        lote_start=1,
        lote_end=20,
        output_csv_path=tmp_path / "output.csv",
        manifest_path=tmp_path / "manifest.json",
    )

    assert result.status == "success"
    assert result.rows_written == 1


def test_lotes_lat_lng_repository_saves_rows(tmp_path: Path) -> None:
    db_url = f"sqlite+pysqlite:///{tmp_path / 'coords.db'}"
    repository = LotesLatLngRepository(db_url)
    rows_ingested = repository.save_rows(
        [
            {
                "instalacao": "204768961",
                "lat_da_instalacao": "-23.77",
                "lng_da_instalacao": "-46.43",
                "lat_da_leitura": "-23.78",
                "lng_da_leitura": "-46.44",
            },
            {
                "instalacao": "204768960",
                "lat_da_instalacao": "",
                "lng_da_instalacao": "",
                "lat_da_leitura": "",
                "lng_da_leitura": "",
            },
        ]
    )

    engine = create_engine(db_url)
    with engine.connect() as conn:
        saved_rows = conn.execute(
            select(instalacoes_coordenadas_sp).order_by(instalacoes_coordenadas_sp.c.instalacao)
        ).fetchall()

    assert rows_ingested == 2
    assert [(row.instalacao, row.lat_da_instalacao, row.lng_da_instalacao) for row in saved_rows] == [
        ("204768960", "", ""),
        ("204768961", "-23.77", "-46.43"),
    ]


def test_lotes_lat_lng_repository_saves_csv_in_chunks(tmp_path: Path) -> None:
    db_url = f"sqlite+pysqlite:///{tmp_path / 'coords.db'}"
    csv_path = tmp_path / "coords.csv"
    csv_path.write_text(
        "\n".join(
            [
                "instalacao,lat_da_instalacao,lng_da_instalacao,lat_da_leitura,lng_da_leitura",
                "204768961,-23.77,-46.43,-23.78,-46.44",
                "204768961,-23.00,-46.00,-23.01,-46.01",
                "204768960,,,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    repository = LotesLatLngRepository(db_url)
    rows_ingested = repository.save_csv(csv_path, chunk_size=1)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        saved_rows = conn.execute(
            select(instalacoes_coordenadas_sp).order_by(instalacoes_coordenadas_sp.c.instalacao)
        ).fetchall()

    assert rows_ingested == 2
    assert [(row.instalacao, row.lat_da_instalacao, row.lng_da_instalacao) for row in saved_rows] == [
        ("204768960", "", ""),
        ("204768961", "-23.77", "-46.43"),
    ]
