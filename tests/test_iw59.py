from __future__ import annotations

import csv
import importlib
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import sap_automation.iw59 as iw59_module
from sap_automation.iw59 import (
    Iw59ExportAdapter,
    build_ca_note_enrichment_map,
    chunk_iw59_notes,
    collect_iw59_brs_from_csv,
    compute_iw59_modified_date_range,
    compute_iw59_manu_modified_date_range,
    compute_nth_business_day_of_month,
    collect_iw59_notes_from_ca_csv,
    concatenate_delimited_exports,
    concatenate_text_exports,
    is_business_day,
    partition_iw59_values,
    resolve_iw59_modified_date_policy,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_collect_iw59_notes_from_ca_csv_filters_by_statusuar_and_deduplicates(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca.csv"
    _write_csv(
        csv_path,
        [
            {"nota": "100", "statusuar": "ENCE", "descricao": "a"},
            {"nota": "100", "statusuar": "ENCE", "descricao": "dup"},
            {"nota": "101", "statusuar": "", "descricao": "skip"},
            {"nota": "102", "statusuar": "PEND", "descricao": "b"},
        ],
    )

    notes = collect_iw59_notes_from_ca_csv(csv_path)

    assert notes == ["100", "102"]


def test_collect_iw59_notes_from_ca_csv_can_filter_allowed_status_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca_filtered.csv"
    _write_csv(
        csv_path,
        [
            {"nota": "100", "statusuar": "ENCE", "descricao": "a"},
            {"nota": "101", "statusuar": "PEND", "descricao": "b"},
            {"nota": "102", "statusuar": " ence ", "descricao": "c"},
            {"nota": "103", "statusuar": "ENCE DEFE", "descricao": "d"},
            {"nota": "104", "statusuar": "ENCE PROC", "descricao": "e"},
            {"nota": "105", "statusuar": "ENCE IMPR", "descricao": "f"},
        ],
    )

    notes = collect_iw59_notes_from_ca_csv(
        csv_path,
        allowed_status_values={
            "ENCE",
            "ENCE DEFE",
            "ENCE DEFE INDE",
            "ENCE DUPL",
            "ENCE IMPR",
            "ENCE INDE",
            "ENCE PROC",
        },
    )

    assert notes == ["100", "102", "103", "104", "105"]


def test_collect_iw59_notes_from_ca_csv_filters_igor_ence_only(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca_igor_ence.csv"
    _write_csv(
        csv_path,
        [
            {"nota": "100", "statusuar": "ENCE", "descricao": "aberta"},
            {"nota": "101", "statusuar": "ENCE DEFE", "descricao": "ok"},
            {"nota": "102", "statusuar": "ENCE DEFE INDE", "descricao": "ok"},
            {"nota": "103", "statusuar": "ENCE DUPL", "descricao": "ok"},
            {"nota": "104", "statusuar": "ENCE IMPR", "descricao": "ok"},
            {"nota": "105", "statusuar": "ENCE INDE", "descricao": "ok"},
            {"nota": "106", "statusuar": "ENCE PROC", "descricao": "ok"},
        ],
    )

    notes = collect_iw59_notes_from_ca_csv(
        csv_path,
        allowed_status_values={"ENCE"},
        source_object_code="CA",
    )

    assert notes == ["100"]


def test_collect_iw59_notes_from_ca_csv_supports_alias_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca_alias.csv"
    _write_csv(
        csv_path,
        [
            {"qmnum": "200", "user_status": "ENCE", "descricao": "ok"},
            {"qmnum": "201", "user_status": "PEND", "descricao": "skip"},
        ],
    )

    notes = collect_iw59_notes_from_ca_csv(
        csv_path,
        allowed_status_values={"ENCE"},
        source_object_code="RL",
    )

    assert notes == ["200"]


def test_chunk_iw59_notes_uses_requested_chunk_size() -> None:
    chunks = chunk_iw59_notes(["1", "2", "3", "4", "5"], chunk_size=2)

    assert chunks == [["1", "2"], ["3", "4"], ["5"]]


def test_partition_iw59_values_splits_evenly_across_partitions() -> None:
    partitions = partition_iw59_values(["1", "2", "3", "4", "5", "6", "7"], partition_count=3)

    assert partitions == [["1", "2", "3"], ["4", "5"], ["6", "7"]]


def test_collect_iw59_brs_from_csv_reads_and_deduplicates_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "brs.csv"
    _write_csv(
        csv_path,
        [
            {"BRS": "br001"},
            {"BRS": "BR002"},
            {"BRS": "BR001"},
            {"BRS": ""},
        ],
    )

    brs_values = collect_iw59_brs_from_csv(csv_path)

    assert brs_values == ["BR001", "BR002"]


def test_concatenate_text_and_delimited_exports(tmp_path: Path) -> None:
    txt_a = tmp_path / "a.txt"
    txt_b = tmp_path / "b.txt"
    txt_a.write_text("nota\tvalor\n1\ta\n", encoding="utf-8")
    txt_b.write_text("nota\tvalor\n2\tb\n", encoding="utf-8")

    combined_txt = tmp_path / "combined.txt"
    combined_csv = tmp_path / "combined.csv"

    concatenate_text_exports([txt_a, txt_b], combined_txt)
    rows_written = concatenate_delimited_exports([txt_a, txt_b], combined_csv)

    assert "1\ta" in combined_txt.read_text(encoding="utf-8")
    assert "2\tb" in combined_txt.read_text(encoding="utf-8")
    assert rows_written == 2
    assert "nota,valor" in combined_csv.read_text(encoding="utf-8")


def test_build_ca_note_enrichment_map_reads_ca_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "ca.csv"
    _write_csv(
        csv_path,
        [
            {"nota": "100", "texto_code_parte_obj": "POSTE", "ptob": "P1", "statusuar": "ENCE"},
            {"nota": "101", "texto_code_parte_obj": "", "ptob": "P2", "statusuar": "ENCE"},
            {"nota": "100", "texto_code_parte_obj": "IGNORAR_DUP", "ptob": "P9", "statusuar": "ENCE"},
        ],
    )

    mapping = build_ca_note_enrichment_map(csv_path)

    assert mapping == {
        "100": {"texto_code_parte_obj": "POSTE", "ptob": "P1"},
        "101": {"texto_code_parte_obj": "", "ptob": "P2"},
    }


def test_concatenate_delimited_exports_enriches_with_ca_fields(tmp_path: Path) -> None:
    txt_a = tmp_path / "a.txt"
    txt_a.write_text("nota\tvalor\n100\ta\n101\tb\n", encoding="utf-8")
    combined_csv = tmp_path / "combined_enriched.csv"

    rows_written = concatenate_delimited_exports(
        [txt_a],
        combined_csv,
        ca_note_enrichment_map={
            "100": {"texto_code_parte_obj": "POSTE", "ptob": "P1"},
            "101": {"texto_code_parte_obj": "REDE", "ptob": "P2"},
        },
    )

    assert rows_written == 2
    combined_text = combined_csv.read_text(encoding="utf-8")
    assert "texto_code_parte_obj" in combined_text
    assert "ptob" in combined_text
    assert "100,a,POSTE,P1" in combined_text
    assert "101,b,REDE,P2" in combined_text


def test_iw59_execute_uses_demandante_specific_chunk_size(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    ca_csv = tmp_path / "ca.csv"
    _write_csv(
        ca_csv,
        [
            {"nota": "100", "statusuar": "ENCE", "texto_code_parte_obj": "A"},
            {"nota": "101", "statusuar": "ENCE", "texto_code_parte_obj": "B"},
            {"nota": "102", "statusuar": "ENCE", "texto_code_parte_obj": "C"},
            {"nota": "103", "statusuar": "ENCE", "texto_code_parte_obj": "D"},
            {"nota": "104", "statusuar": "ENCE", "texto_code_parte_obj": "E"},
        ],
    )
    executed_chunks: list[list[str]] = []

    monkeypatch.setattr(
        iw59_module,
        "concatenate_text_exports",
        lambda source_paths, destination_path: None,
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_delimited_exports",
        lambda source_paths, destination_path, ca_note_enrichment_map=None: 5,
    )
    monkeypatch.setattr(
        Iw59ExportAdapter,
        "_run_chunk",
        lambda self, **kwargs: executed_chunks.append(list(kwargs["notes"])),
    )

    result = Iw59ExportAdapter().execute(
        output_root=tmp_path,
        run_id="run-iw59",
        reference="202603",
        demandante="MANU",
        source_manifest=iw59_module.ObjectManifest(
            object_code="CA",
            status="success",
            canonical_csv_path=str(ca_csv),
        ),
        session=object(),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        config={
            "global": {"wait_timeout_seconds": 60.0},
            "iw59": {
                "enabled": True,
                "chunk_size": 10,
                "transaction_code": "IW59",
                "multi_select_button_id": "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
                "back_button_id": "wnd[0]/tbar[0]/btn[3]",
                "pre_iw59_unwind_min_presses": 3,
                "pre_iw59_unwind_max_presses": 8,
                "demandantes": {
                    "MANU": {
                        "chunk_size": 2,
                        "allowed_status_values": [
                            "ENCE",
                            "ENCE DEFE",
                            "ENCE DEFE INDE",
                            "ENCE DUPL",
                            "ENCE IMPR",
                            "ENCE INDE",
                            "ENCE PROC",
                        ],
                        "use_modified_date_range": True,
                    }
                },
            },
        },
    )

    assert result.chunk_size == 2
    assert result.chunk_count == 3
    assert executed_chunks == [["100", "101"], ["102", "103"], ["104"]]


def test_iw59_execute_uses_igor_closed_status_filter_and_chunk_size(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    ca_csv = tmp_path / "ca_igor.csv"
    _write_csv(
        ca_csv,
        [
            {"nota": "100", "statusuar": "ENCE", "texto_code_parte_obj": "A"},
            {"nota": "101", "statusuar": "ENCE DEFE", "texto_code_parte_obj": "B"},
            {"nota": "102", "statusuar": "ENCE DEFE INDE", "texto_code_parte_obj": "C"},
            {"nota": "103", "statusuar": "ENCE DUPL", "texto_code_parte_obj": "D"},
            {"nota": "104", "statusuar": "ENCE IMPR", "texto_code_parte_obj": "E"},
            {"nota": "105", "statusuar": "ENCE INDE", "texto_code_parte_obj": "F"},
            {"nota": "106", "statusuar": "ENCE PROC", "texto_code_parte_obj": "G"},
        ],
    )
    executed_chunks: list[list[str]] = []

    monkeypatch.setattr(
        iw59_module,
        "concatenate_text_exports",
        lambda source_paths, destination_path: None,
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_delimited_exports",
        lambda source_paths, destination_path, ca_note_enrichment_map=None: 6,
    )
    monkeypatch.setattr(
        Iw59ExportAdapter,
        "_run_chunk",
        lambda self, **kwargs: executed_chunks.append(list(kwargs["notes"])),
    )

    result = Iw59ExportAdapter().execute(
        output_root=tmp_path,
        run_id="run-iw59-igor",
        reference="202603",
        demandante="IGOR",
        source_manifest=iw59_module.ObjectManifest(
            object_code="CA",
            status="success",
            canonical_csv_path=str(ca_csv),
        ),
        session=object(),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        config={
            "global": {"wait_timeout_seconds": 60.0},
            "iw59": {
                "enabled": True,
                "chunk_size": 20000,
                "transaction_code": "IW59",
                "multi_select_button_id": "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
                "back_button_id": "wnd[0]/tbar[0]/btn[3]",
                "pre_iw59_unwind_min_presses": 3,
                "pre_iw59_unwind_max_presses": 8,
                "demandantes": {
                    "IGOR": {
                        "chunk_size": 5000,
                        "allowed_status_values": [
                            "ENCE",
                        ],
                    }
                },
            },
        },
    )

    assert result.chunk_size == 5000
    assert result.chunk_count == 1
    assert executed_chunks == [["100"]]


def test_iw59_execute_writes_object_specific_output_names(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    rl_csv = tmp_path / "rl.csv"
    _write_csv(
        rl_csv,
        [
            {"nota": "200", "statusuar": "ENCE DEFE", "texto_code_parte_obj": "R1"},
        ],
    )
    chunk_outputs: list[Path] = []
    combined_outputs: list[Path] = []

    monkeypatch.setattr(
        Iw59ExportAdapter,
        "_run_chunk",
        lambda self, **kwargs: chunk_outputs.append(kwargs["output_path"]),
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_text_exports",
        lambda source_paths, destination_path: combined_outputs.append(destination_path),
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_delimited_exports",
        lambda source_paths, destination_path, ca_note_enrichment_map=None: 1,
    )

    result = Iw59ExportAdapter().execute(
        output_root=tmp_path,
        run_id="run-iw59-rl",
        reference="202603",
        demandante="IGOR",
        source_manifest=iw59_module.ObjectManifest(
            object_code="RL",
            status="success",
            canonical_csv_path=str(rl_csv),
        ),
        session=object(),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        config={
            "global": {"wait_timeout_seconds": 60.0},
            "iw59": {
                "enabled": True,
                "chunk_size": 5000,
                "transaction_code": "IW59",
                "multi_select_button_id": "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
                "back_button_id": "wnd[0]/tbar[0]/btn[3]",
                "pre_iw59_unwind_min_presses": 3,
                "pre_iw59_unwind_max_presses": 8,
                "demandantes": {
                    "IGOR": {
                        "chunk_size": 5000,
                        "allowed_status_values": ["ENCE DEFE"],
                    }
                },
            },
        },
    )

    assert result.source_object_code == "RL"
    assert chunk_outputs[0].name.startswith("iw59_rl_202603_run-iw59-rl_")
    assert combined_outputs[0].name == "iw59_rl_202603_run-iw59-rl_combined.txt"
    assert Path(result.combined_csv_path).name == "iw59_rl_202603_run-iw59-rl.csv"


def test_build_ca_note_enrichment_map_supports_rl_real_column_names(tmp_path: Path) -> None:
    csv_path = tmp_path / "rl_real.csv"
    _write_csv(
        csv_path,
        [
            {
                "nota": "300",
                "descricao": "RL",
                "statusuar": "ENCE",
                "data": "2026-03-01",
                "hora": "08:00",
                "encerram": "",
                "concl_desj": "",
                "txt_cod_part_ob": "PARTE-RL",
                "ptob": "PTOB-RL",
                "texto_de_code_para_problema": "PROB",
                "dano": "D",
                "criado_por": "user",
            }
        ],
    )

    mapping = build_ca_note_enrichment_map(csv_path, source_object_code="RL")

    assert mapping == {"300": {"texto_code_parte_obj": "PARTE-RL", "ptob": "PTOB-RL"}}


def test_iw59_adapter_skip_returns_structured_result() -> None:
    result = Iw59ExportAdapter().skip(reason="missing ca")

    assert result.to_dict()["status"] == "skipped"
    assert result.to_dict()["reason"] == "missing ca"


def test_compute_iw59_manu_modified_date_range_uses_month_start_and_today() -> None:
    modified_from, modified_to = compute_iw59_manu_modified_date_range(date(2026, 3, 25))

    assert modified_from == "01.03.2026"
    assert modified_to == "25.03.2026"


def test_is_business_day_ignores_weekends() -> None:
    assert is_business_day(date(2026, 4, 1)) is True
    assert is_business_day(date(2026, 4, 4)) is False


def test_compute_nth_business_day_of_month_returns_fifth_business_day() -> None:
    fifth_business_day = compute_nth_business_day_of_month(
        current_day=date(2026, 4, 1),
        nth=5,
    )

    assert fifth_business_day == date(2026, 4, 7)


def test_compute_iw59_modified_date_range_keeps_previous_month_until_fifth_business_day() -> None:
    modified_from, modified_to = compute_iw59_modified_date_range(date(2026, 4, 1))

    assert modified_from == "01.03.2026"
    assert modified_to == "31.03.2026"


def test_compute_iw59_modified_date_range_switches_after_fifth_business_day() -> None:
    modified_from, modified_to = compute_iw59_modified_date_range(date(2026, 4, 8))

    assert modified_from == "01.04.2026"
    assert modified_to == "08.04.2026"


def test_resolve_iw59_modified_date_policy_uses_global_defaults() -> None:
    use_modified_date_range, transition_business_day = resolve_iw59_modified_date_policy(
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={},
    )

    assert use_modified_date_range is True
    assert transition_business_day == 5


def test_resolve_iw59_modified_date_policy_prefers_demandante_override() -> None:
    use_modified_date_range, transition_business_day = resolve_iw59_modified_date_policy(
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={"use_modified_date_range": False, "transition_business_day": 7},
    )

    assert use_modified_date_range is False
    assert transition_business_day == 7


class _BackButton:
    def __init__(self, session) -> None:  # noqa: ANN001
        self._session = session

    def press(self) -> None:
        self._session.press_back()


class _Window:
    def __init__(self, session) -> None:  # noqa: ANN001
        self._session = session
        self.Text = "Main"

    def sendVKey(self, value: int) -> None:
        if value == 3:
            self._session.press_back()


class _Iw59Session:
    def __init__(self) -> None:
        self.Busy = False
        self.state_index = 0
        self.states = [
            {"okcd": "IW69", "sbar": "fase1", "otgrup": True},
            {"okcd": "IW69", "sbar": "fase2", "otgrup": True},
            {"okcd": "IW69", "sbar": "fase3", "otgrup": False},
            {"okcd": "", "sbar": "", "otgrup": False},
            {"okcd": "", "sbar": "", "otgrup": False},
        ]
        self.back_presses = 0

    def press_back(self) -> None:
        self.back_presses += 1
        if self.state_index < len(self.states) - 1:
            self.state_index += 1

    def findById(self, item_id: str):  # noqa: ANN001
        state = self.states[self.state_index]
        if item_id == "wnd[0]/tbar[0]/btn[3]":
            return _BackButton(self)
        if item_id == "wnd[0]":
            return _Window(self)
        if item_id == "wnd[0]/tbar[0]/okcd":
            return type("Okcd", (), {"Text": state["okcd"]})()
        if item_id == "wnd[0]/sbar":
            return type("Sbar", (), {"Text": state["sbar"]})()
        if item_id == "wnd[0]/usr/ctxtOTGRP-LOW" and state["otgrup"]:
            return object()
        raise KeyError(item_id)


def test_unwind_before_iw59_presses_back_until_state_stabilizes() -> None:
    adapter = Iw59ExportAdapter()
    session = _Iw59Session()

    adapter._unwind_before_iw59(
        session=session,
        back_button_id="wnd[0]/tbar[0]/btn[3]",
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        wait_timeout_seconds=5.0,
        min_presses=3,
        max_presses=8,
    )

    assert session.back_presses >= 3


class _CompatStub:
    @staticmethod
    def wait_not_busy(*, session, timeout_seconds: float) -> None:  # noqa: ANN001
        assert timeout_seconds > 0
        session.wait_calls += 1

    @staticmethod
    def _collect_visible_control_ids(*, session, limit: int) -> list[str]:  # noqa: ANN001
        visible = [
            "wnd[0]",
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID1/shellcont/shell",
        ]
        return visible[:limit]


class _Control:
    def __init__(self, session, item_id: str) -> None:  # noqa: ANN001
        self._session = session
        self._item_id = item_id
        self._text = ""
        self.Text = ""

    @property
    def text(self) -> str:
        return self._text

    @text.setter
    def text(self, value: str) -> None:
        self._text = value
        self.Text = value
        if self._item_id == "wnd[0]/tbar[0]/okcd":
            self._session.current_okcd = value
        self._session.actions.append(f"text:{self._item_id}={value}")

    @property
    def caretPosition(self) -> int:
        return len(self._text)

    @caretPosition.setter
    def caretPosition(self, value: int) -> None:
        self._session.actions.append(f"caret:{self._item_id}={value}")

    def press(self) -> None:
        self._session.handle_press(self._item_id)

    def select(self) -> None:
        self._session.actions.append(f"select:{self._item_id}")

    def setFocus(self) -> None:
        self._session.actions.append(f"focus:{self._item_id}")

    def maximize(self) -> None:
        self._session.actions.append("maximize:wnd[0]")

    def sendVKey(self, value: int) -> None:
        self._session.actions.append(f"vkey:{value}")
        self._session.handle_vkey(value)

    def pressToolbarContextButton(self, value: str) -> None:  # noqa: N802
        self._session.actions.append(f"toolbar:{self._item_id}={value}")

    def selectContextMenuItem(self, value: str) -> None:  # noqa: N802
        self._session.actions.append(f"contextmenu:{self._item_id}={value}")


class _FlowSession:
    def __init__(self, *, menu_export_available: bool = True, shell_export_available: bool = False) -> None:
        self.Busy = False
        self.actions: list[str] = []
        self.wait_calls = 0
        self.back_presses = 0
        self.unwind_state = 0
        self.current_okcd = "IW69"
        self.iw59_open = False
        self.selection_executed = False
        self.file_dialog_open = False
        self.menu_export_available = menu_export_available
        self.shell_export_available = shell_export_available
        self.selection_screen_after_execute = not menu_export_available and not shell_export_available
        self.controls: dict[str, _Control] = {}

    def handle_press(self, item_id: str) -> None:
        self.actions.append(f"press:{item_id}")
        if item_id == "wnd[0]/tbar[0]/btn[3]":
            self.back_presses += 1
            if self.unwind_state < 3:
                self.unwind_state += 1
            return
        if item_id == "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH":
            return
        if item_id == "wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH":
            return
        if item_id == "wnd[0]/tbar[1]/btn[8]":
            self.selection_executed = True
            return
        if item_id == "wnd[1]/tbar[0]/btn[0]" and self.file_dialog_open:
            return
        if item_id in {"wnd[1]/tbar[0]/btn[24]", "wnd[1]/tbar[0]/btn[8]"}:
            return

    def handle_vkey(self, value: int) -> None:
        if value == 3:
            self.handle_press("wnd[0]/tbar[0]/btn[3]")
            return
        if value == 0 and self.current_okcd.lower() == "iw59":
            self.iw59_open = True

    def _control(self, item_id: str) -> _Control:
        control = self.controls.get(item_id)
        if control is None:
            control = _Control(self, item_id)
            if item_id == "wnd[0]/tbar[0]/okcd":
                control._text = self.current_okcd
                control.Text = self.current_okcd
            self.controls[item_id] = control
        return control

    def findById(self, item_id: str):  # noqa: ANN001
        if item_id == "wnd[0]/tbar[0]/btn[3]":
            return self._control(item_id)
        if item_id == "wnd[0]":
            return self._control(item_id)
        if item_id == "wnd[0]/tbar[0]/okcd":
            control = self._control(item_id)
            control._text = self.current_okcd
            control.Text = self.current_okcd
            return control
        if item_id == "wnd[0]/sbar":
            return SimpleNamespace(Text=f"state-{self.unwind_state}")
        if item_id == "wnd[0]/usr/ctxtOTGRP-LOW" and self.unwind_state < 2:
            return object()
        if item_id == "wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH" and self.iw59_open:
            return self._control(item_id)
        if item_id == "wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH" and self.iw59_open:
            return self._control(item_id)
        if item_id in {
            "wnd[0]/usr/ctxtDATUV",
            "wnd[0]/usr/ctxtDATUB",
            "wnd[0]/usr/ctxtAEDAT-LOW",
            "wnd[0]/usr/ctxtAEDAT-HIGH",
            "wnd[0]/usr/ctxtSTAI1-LOW",
            "wnd[0]/usr/ctxtAENAM-LOW",
            "wnd[0]/usr/txt%_QMNUM_%_APP_%-TEXT",
            "wnd[0]/usr/txt%_QMNUM_%_APP_%-TO_TEXT",
            "wnd[0]/usr/txt%_AENAM_%_APP_%-TEXT",
            "wnd[0]/usr/txt%_AENAM_%_APP_%-TO_TEXT",
            "wnd[1]/tbar[0]/btn[24]",
            "wnd[1]/tbar[0]/btn[23]",
            "wnd[1]/tbar[0]/btn[0]",
            "wnd[1]/tbar[0]/btn[8]",
            "wnd[0]/tbar[1]/btn[8]",
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]",
            "wnd[1]/usr/ctxtDY_PATH",
            "wnd[1]/usr/ctxtDY_FILENAME",
            "wnd[1]/usr/txtDY_PATH",
            "wnd[1]/usr/txtDY_FILENAME",
            "wnd[2]/usr/ctxtDY_PATH",
            "wnd[2]/usr/ctxtDY_FILENAME",
            "wnd[2]/usr/txtDY_PATH",
            "wnd[2]/usr/txtDY_FILENAME",
            "wnd[2]/tbar[0]/btn[0]",
        }:
            if item_id.startswith("wnd[1]/usr/ctxtDY_") or item_id.startswith("wnd[1]/usr/txtDY_"):
                self.file_dialog_open = True
            return self._control(item_id)
        if item_id in {
            "wnd[0]/usr/ctxtMONITOR",
            "wnd[0]/usr/ctxtVARIANT",
            "wnd[0]/usr/btn%_PAGESTAT_%_APP_%-VALU_PUSH",
            "wnd[0]/usr/ctxtPAGESTAT-LOW",
            "wnd[0]/usr/ctxtANLNR-LOW",
        } and self.selection_executed and self.selection_screen_after_execute:
            return self._control(item_id)
        if item_id in {
            "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
        } and self.selection_executed and self.menu_export_available:
            return self._control(item_id)
        if item_id in {
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID1/shellcont/shell",
            "wnd[0]/usr/cntlGRID/shellcont/shell",
            "wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell",
        } and self.selection_executed and self.shell_export_available:
            return self._control(item_id)
        raise KeyError(item_id)


def test_run_chunk_unwinds_before_entering_iw59(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    adapter = Iw59ExportAdapter()
    session = _FlowSession()
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()
    output_path = tmp_path / "iw59.txt"
    original_import_module = importlib.import_module

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: _CompatStub() if name == "sap_gui_export_compat" else original_import_module(name),
    )
    monkeypatch.setattr(adapter, "_copy_notes_to_clipboard", lambda notes, **kwargs: len(notes))
    monkeypatch.setattr(
        adapter,
        "_wait_for_file",
        lambda *, output_path, timeout_seconds: output_path.write_text("nota\tvalor\n1\ta\n", encoding="utf-8"),
    )

    adapter._run_chunk(
        session=session,
        notes=["1", "2"],
        output_path=output_path,
        logger=logger,
        demandante="IGOR",
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={},
        transaction_code="IW59",
        multi_select_button_id="wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
        back_button_id="wnd[0]/tbar[0]/btn[3]",
        wait_timeout_seconds=5.0,
        unwind_min_presses=3,
        unwind_max_presses=8,
    )

    assert session.back_presses >= 3
    assert "text:wnd[0]/tbar[0]/okcd=iw59" in session.actions
    assert "press:wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH" in session.actions
    assert session.actions.index("press:wnd[0]/tbar[0]/btn[3]") < session.actions.index(
        "text:wnd[0]/tbar[0]/okcd=iw59"
    )


def test_prepare_selection_filters_applies_manu_dynamic_dates(monkeypatch) -> None:  # noqa: ANN001
    adapter = Iw59ExportAdapter()
    session = _FlowSession()
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()

    monkeypatch.setattr(
        iw59_module,
        "compute_iw59_modified_date_range",
        lambda **kwargs: ("01.03.2026", "25.03.2026"),
    )

    adapter._prepare_selection_filters(
        session=session,
        logger=logger,
        demandante="MANU",
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={"use_modified_date_range": True},
    )

    assert "text:wnd[0]/usr/ctxtDATUV=" in session.actions
    assert "text:wnd[0]/usr/ctxtDATUB=" in session.actions
    assert "text:wnd[0]/usr/ctxtAEDAT-LOW=01.03.2026" in session.actions
    assert "text:wnd[0]/usr/ctxtAEDAT-HIGH=25.03.2026" in session.actions
    assert "focus:wnd[0]/usr/ctxtSTAI1-LOW" in session.actions
    assert "caret:wnd[0]/usr/ctxtSTAI1-LOW=0" in session.actions


def test_run_chunk_uses_alv_toolbar_fallback_when_export_menu_is_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:  # noqa: ANN001
    adapter = Iw59ExportAdapter()
    session = _FlowSession(menu_export_available=False, shell_export_available=True)
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()
    output_path = tmp_path / "iw59.txt"
    original_import_module = importlib.import_module

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: _CompatStub() if name == "sap_gui_export_compat" else original_import_module(name),
    )
    monkeypatch.setattr(adapter, "_copy_notes_to_clipboard", lambda notes, **kwargs: len(notes))
    monkeypatch.setattr(
        adapter,
        "_wait_for_file",
        lambda *, output_path, timeout_seconds: output_path.write_text("nota\tvalor\n1\ta\n", encoding="utf-8"),
    )

    adapter._run_chunk(
        session=session,
        notes=["1", "2"],
        output_path=output_path,
        logger=logger,
        demandante="IGOR",
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={},
        transaction_code="IW59",
        multi_select_button_id="wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
        back_button_id="wnd[0]/tbar[0]/btn[3]",
        wait_timeout_seconds=5.0,
        unwind_min_presses=3,
        unwind_max_presses=8,
    )

    assert "toolbar:wnd[0]/usr/cntlCONTAINER/shellcont/shell=&MB_EXPORT" in session.actions
    assert "contextmenu:wnd[0]/usr/cntlCONTAINER/shellcont/shell=&PC" in session.actions


def test_execute_modified_by_brs_uses_chunking_and_three_partitions(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    brs_csv = tmp_path / "brs.csv"
    _write_csv(
        brs_csv,
        [{"BRS": f"BR{i:03d}"} for i in range(1, 11)],
    )
    executed_chunks: list[list[str]] = []

    monkeypatch.setattr(
        Iw59ExportAdapter,
        "_run_chunk_modified_by",
        lambda self, **kwargs: executed_chunks.append(list(kwargs["brs_values"])),
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_text_exports",
        lambda source_paths, destination_path: None,
    )
    monkeypatch.setattr(
        iw59_module,
        "concatenate_delimited_exports",
        lambda source_paths, destination_path, ca_note_enrichment_map=None: 10,
    )

    result = Iw59ExportAdapter().execute_modified_by_brs(
        output_root=tmp_path,
        run_id="run-iw59-kelly",
        demandante="KELLY",
        session=object(),
        logger=type("Logger", (), {"info": lambda *args, **kwargs: None})(),
        config={
            "global": {"wait_timeout_seconds": 60.0},
            "iw59": {
                "enabled": True,
                "transaction_code": "IW59",
                "back_button_id": "wnd[0]/tbar[0]/btn[3]",
                "pre_iw59_unwind_min_presses": 3,
                "pre_iw59_unwind_max_presses": 8,
                "demandantes": {
                    "KELLY": {
                        "chunk_size": 2,
                        "partition_count": 3,
                        "use_modified_date_range": False,
                        "input_csv_path": str(brs_csv),
                        "multi_select_button_id": "wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH",
                        "selection_entry_field_id": "wnd[0]/usr/ctxtAENAM-LOW",
                        "selection_summary_ids": [
                            "wnd[0]/usr/txt%_AENAM_%_APP_%-TEXT",
                            "wnd[0]/usr/txt%_AENAM_%_APP_%-TO_TEXT",
                        ],
                    }
                },
            },
        },
        input_csv_path=brs_csv,
    )

    assert result.source_object_code == "KELLY"
    assert result.chunk_size == 2
    assert result.chunk_count == 6
    assert executed_chunks == [
        ["BR001", "BR002"],
        ["BR003", "BR004"],
        ["BR005", "BR006"],
        ["BR007"],
        ["BR008", "BR009"],
        ["BR010"],
    ]


def test_run_chunk_modified_by_uses_aenam_multiselect(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    adapter = Iw59ExportAdapter()
    session = _FlowSession()
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()
    output_path = tmp_path / "iw59_kelly.txt"
    original_import_module = importlib.import_module

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: _CompatStub() if name == "sap_gui_export_compat" else original_import_module(name),
    )
    monkeypatch.setattr(adapter, "_copy_notes_to_clipboard", lambda notes, **kwargs: len(notes))
    monkeypatch.setattr(
        adapter,
        "_wait_for_file",
        lambda *, output_path, timeout_seconds: output_path.write_text("nota\tvalor\n1\ta\n", encoding="utf-8"),
    )

    adapter._run_chunk_modified_by(
        session=session,
        brs_values=["BR001", "BR002"],
        output_path=output_path,
        logger=logger,
        demandante="KELLY",
        iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
        demandante_cfg={"use_modified_date_range": False},
        transaction_code="IW59",
        multi_select_button_id="wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH",
        selection_entry_field_id="wnd[0]/usr/ctxtAENAM-LOW",
        selection_summary_ids=[
            "wnd[0]/usr/txt%_AENAM_%_APP_%-TEXT",
            "wnd[0]/usr/txt%_AENAM_%_APP_%-TO_TEXT",
        ],
        back_button_id="wnd[0]/tbar[0]/btn[3]",
        wait_timeout_seconds=5.0,
        unwind_min_presses=3,
        unwind_max_presses=8,
    )

    assert "focus:wnd[0]/usr/ctxtAENAM-LOW" in session.actions
    assert "press:wnd[0]/usr/btn%_AENAM_%_APP_%-VALU_PUSH" in session.actions
    assert "text:wnd[0]/usr/ctxtDATUV=" in session.actions
    assert "text:wnd[0]/usr/ctxtDATUB=" in session.actions


def test_run_chunk_raises_clear_error_when_iw59_stays_on_selection_screen(
    monkeypatch,
    tmp_path: Path,
) -> None:  # noqa: ANN001
    adapter = Iw59ExportAdapter()
    session = _FlowSession(menu_export_available=False, shell_export_available=False)
    logger = type("Logger", (), {"info": lambda *args, **kwargs: None})()
    output_path = tmp_path / "iw59.txt"
    original_import_module = importlib.import_module

    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: _CompatStub() if name == "sap_gui_export_compat" else original_import_module(name),
    )
    monkeypatch.setattr(adapter, "_copy_notes_to_clipboard", lambda notes, **kwargs: len(notes))

    try:
            adapter._run_chunk(
                session=session,
                notes=["1", "2"],
                output_path=output_path,
                logger=logger,
                demandante="MANU",
                iw59_cfg={"use_modified_date_range": True, "transition_business_day": 5},
                demandante_cfg={"use_modified_date_range": True},
                transaction_code="IW59",
                multi_select_button_id="wnd[0]/usr/btn%_QMNUM_%_APP_%-VALU_PUSH",
                back_button_id="wnd[0]/tbar[0]/btn[3]",
            wait_timeout_seconds=1.0,
            unwind_min_presses=3,
            unwind_max_presses=8,
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected IW59 selection-screen error")

    assert "IW59 execute did not leave selection screen." in message
    assert "status_bar='state-3'" in message
