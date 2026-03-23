from __future__ import annotations

import json
import logging
from pathlib import Path

import sap_gui_export_compat as compat


class FakeControl:
    def __init__(self) -> None:
        self.text = ""
        self.caretPosition = 0
        self.focused = False
        self.pressed = False
        self.selected = False
        self.called_methods: list[tuple[str, tuple[object, ...]]] = []

    def setFocus(self) -> None:
        self.focused = True

    def press(self) -> None:
        self.pressed = True

    def select(self) -> None:
        self.selected = True

    def maximize(self) -> None:
        self.called_methods.append(("maximize", ()))


class RejectingTextControl(FakeControl):
    @property
    def text(self) -> str:
        return getattr(self, "_text", "")

    @text.setter
    def text(self, value: str) -> None:
        self._text = ""


class FakeSession:
    def __init__(self, controls: dict[str, FakeControl]) -> None:
        self.controls = controls
        self.Busy = False
        self.transactions: list[str] = []

    def StartTransaction(self, transaction_code: str) -> None:
        self.transactions.append(transaction_code)

    def findById(self, item_id: str) -> FakeControl:
        if item_id not in self.controls:
            raise KeyError(item_id)
        return self.controls[item_id]


def test_validate_steps_accepts_repo_iw69_objects() -> None:
    config = json.loads(
        Path("sap_iw69_batch_config.json").read_text(encoding="utf-8")
    )

    for object_code in ("CA", "RL", "WB"):
        compat._validate_steps(
            steps=config["objects"][object_code]["steps"],
            object_code=object_code,
        )


def test_run_steps_executes_all_actions_for_ca_flow() -> None:
    controls = {
        "wnd[0]": FakeControl(),
        "wnd[0]/usr/ctxtQMART-LOW": FakeControl(),
        "wnd[0]/usr/ctxtDATUV": FakeControl(),
        "wnd[0]/usr/ctxtOTGRP-LOW": FakeControl(),
        "wnd[0]/usr/btn%OTEIL%APP%-VALU_PUSH": FakeControl(),
        "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,0]": FakeControl(),
        "wnd[1]/tbar[0]/btn[8]": FakeControl(),
        "wnd[0]/usr/ctxtSTAE1-LOW": FakeControl(),
        "wnd[0]/usr/ctxtVARIANT": FakeControl(),
        "wnd[0]/tbar[1]/btn[8]": FakeControl(),
        "wnd[0]/mbar/menu[0]/menu[11]/menu[2]": FakeControl(),
        "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]": FakeControl(),
        "wnd[1]/tbar[0]/btn[0]": FakeControl(),
        "wnd[1]/usr/ctxtDY_PATH": FakeControl(),
        "wnd[1]/usr/ctxtDY_FILENAME": FakeControl(),
        "wnd[1]/tbar[0]/btn[11]": FakeControl(),
    }
    session = FakeSession(controls)
    steps = [
        {"action": "call_method", "id": "wnd[0]", "method": "maximize"},
        {"action": "start_transaction", "value": "{transaction_code}"},
        {"action": "set_text", "id": "wnd[0]/usr/ctxtQMART-LOW", "value": "CA"},
        {
            "action": "set_text",
            "id": "wnd[0]/usr/ctxtDATUV",
            "value": "{iw69_from_date_dmy}",
        },
        {"action": "set_focus", "id": "wnd[0]/usr/ctxtOTGRP-LOW"},
        {"action": "set_caret", "id": "wnd[0]/usr/ctxtOTGRP-LOW", "value": 2},
        {"action": "press", "id": "wnd[0]/usr/btn%OTEIL%APP%-VALU_PUSH"},
        {
            "action": "set_text",
            "id": "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,0]",
            "value": "AFAX",
        },
        {"action": "press", "id": "wnd[1]/tbar[0]/btn[8]"},
        {"action": "set_text", "id": "wnd[0]/usr/ctxtSTAE1-LOW", "value": "E0001"},
        {"action": "set_text", "id": "wnd[0]/usr/ctxtVARIANT", "value": "/OPERACAO"},
        {
            "action": "select",
            "ids": [
                "wnd[0]/mbar/menu[0]/menu[10]/menu[2]",
                "wnd[0]/mbar/menu[0]/menu[11]/menu[2]",
            ],
        },
        {
            "action": "select",
            "id": "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]",
        },
        {
            "action": "press",
            "ids": ["wnd[1]/tbar[0]/btn[9]", "wnd[1]/tbar[0]/btn[0]"],
        },
        {
            "action": "set_text",
            "ids": ["wnd[1]/usr/ctxtDY_PATH"],
            "value": "{raw_dir_win}",
        },
        {
            "action": "set_text",
            "ids": ["wnd[1]/usr/ctxtDY_FILENAME"],
            "value": "{ca_export_filename}",
        },
        {
            "action": "press",
            "ids": ["wnd[1]/tbar[0]/btn[11]", "wnd[1]/tbar[0]/btn[0]"],
        },
    ]

    compat.run_steps(
        session=session,
        steps=steps,
        context={
            "object": "CA",
            "transaction_code": "IW69",
            "iw69_from_date_dmy": "23.03.2026",
            "raw_dir_win": "C:/temp",
            "ca_export_filename": "BASE_AUTOMACAO_CA.txt",
        },
        default_timeout_seconds=5.0,
        logger=logging.getLogger("test.compat"),
    )

    assert session.transactions == ["IW69"]
    assert controls["wnd[0]"].called_methods == [("maximize", ())]
    assert controls["wnd[0]/usr/ctxtQMART-LOW"].text == "CA"
    assert controls["wnd[0]/usr/ctxtDATUV"].text == "23.03.2026"
    assert controls["wnd[0]/usr/ctxtOTGRP-LOW"].focused is True
    assert controls["wnd[0]/usr/ctxtOTGRP-LOW"].caretPosition == 2
    assert controls["wnd[0]/usr/btn%OTEIL%APP%-VALU_PUSH"].pressed is True
    assert (
        controls[
            "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,0]"
        ].text
        == "AFAX"
    )
    assert controls["wnd[1]/tbar[0]/btn[8]"].pressed is True
    assert controls["wnd[0]/usr/ctxtSTAE1-LOW"].text == "E0001"
    assert controls["wnd[0]/usr/ctxtVARIANT"].text == "/OPERACAO"
    assert controls["wnd[0]/mbar/menu[0]/menu[11]/menu[2]"].selected is True
    assert (
        controls[
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]"
        ].selected
        is True
    )
    assert controls["wnd[1]/tbar[0]/btn[0]"].pressed is True
    assert controls["wnd[1]/usr/ctxtDY_PATH"].text == "C:/temp"
    assert (
        controls["wnd[1]/usr/ctxtDY_FILENAME"].text == "BASE_AUTOMACAO_CA.txt"
    )
    assert controls["wnd[1]/tbar[0]/btn[11]"].pressed is True


def test_run_steps_skips_optional_missing_control() -> None:
    session = FakeSession({})

    compat.run_steps(
        session=session,
        steps=[{"action": "press", "id": "wnd[9]/missing", "optional": True}],
        context={"object": "CA"},
        default_timeout_seconds=5.0,
        logger=logging.getLogger("test.compat.optional"),
    )


def test_set_text_fails_when_control_does_not_persist_value() -> None:
    session = FakeSession({"wnd[0]/usr/ctxtVARIANT": RejectingTextControl()})

    try:
        compat.run_steps(
            session=session,
            steps=[
                {
                    "action": "set_text",
                    "id": "wnd[0]/usr/ctxtVARIANT",
                    "value": "/BATISTAO",
                    "label": "variant",
                }
            ],
            context={"object": "CA"},
            default_timeout_seconds=5.0,
            logger=logging.getLogger("test.compat.write"),
        )
    except RuntimeError as exc:
        assert "Compat step failed object=CA index=1/1 action=set_text label=variant" in str(exc)
        assert "text write verification failed" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for non-persisted SAP text write.")
