from __future__ import annotations

from types import SimpleNamespace

import pytest

from sap_automation.credentials import SapCredentials
from sap_automation.errors import LoginFailedError, MultipleLogonError
from sap_automation.login import SapLoginHandler
from sap_automation.logon import LogonConfig


class _FakeField:
    def __init__(self) -> None:
        self.Text = ""
        self.selected = False
        self.pressed = False

    def Select(self) -> None:
        self.selected = True

    def press(self) -> None:
        self.pressed = True


class _FakeWindow:
    def __init__(self) -> None:
        self.vkeys: list[int] = []

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)


class _FakeSession:
    def __init__(self, mapping: dict[str, object], *, busy: bool = False, system_name: str = "") -> None:
        self._mapping = mapping
        self.Busy = busy
        self.Info = SimpleNamespace(SystemName=system_name)

    def findById(self, item_id: str):
        if item_id not in self._mapping:
            raise KeyError(item_id)
        return self._mapping[item_id]


def test_login_fills_fields_and_submits() -> None:
    user = _FakeField()
    password = _FakeField()
    client = _FakeField()
    language = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]/usr/txtRSYST-MANDT": client,
            "wnd[0]/usr/txtRSYST-LANGU": language,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="PRD",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="300", language="PT"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert user.Text == "user.sap"
    assert password.Text == "secret"
    assert client.Text == "300"
    assert language.Text == "PT"
    assert window.vkeys == [0]


def test_login_handles_multiple_logon_continue() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    option = _FakeField()
    confirm = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
            "wnd[1]": object(),
            "wnd[1]/usr/radMULTI_LOGON_OPT1": option,
            "wnd[1]/tbar[0]/btn[0]": confirm,
        },
        system_name="PRD",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(
            connection_description="H181",
            workspace_name="00 SAP ERP",
            multiple_logon_action="continue",
        ),
    )

    assert option.selected is True
    assert confirm.pressed is True


def test_login_handles_multiple_logon_fail() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
            "wnd[1]": object(),
        },
        system_name="",
    )

    with pytest.raises(MultipleLogonError):
        SapLoginHandler().login(
            session,
            SapCredentials(username="user.sap", password="secret"),
            LogonConfig(
                connection_description="H181",
                workspace_name="00 SAP ERP",
                multiple_logon_action="fail",
            ),
        )


def test_login_detects_wrong_password() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    status.Text = "Senha incorreta"
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )

    with pytest.raises(LoginFailedError, match="Senha incorreta"):
        SapLoginHandler().login(
            session,
            SapCredentials(username="user.sap", password="secret"),
            LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
        )


def test_login_skips_if_already_logged_in() -> None:
    session = _FakeSession({}, system_name="PRD")

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )


def test_login_fills_client_and_language_when_provided() -> None:
    user = _FakeField()
    password = _FakeField()
    client = _FakeField()
    language = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]/usr/txtRSYST-MANDT": client,
            "wnd[0]/usr/txtRSYST-LANGU": language,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="PRD",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="300", language="PT"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert client.Text == "300"
    assert language.Text == "PT"


def test_login_skips_optional_fields_when_empty() -> None:
    user = _FakeField()
    password = _FakeField()
    client = _FakeField()
    language = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]/usr/txtRSYST-MANDT": client,
            "wnd[0]/usr/txtRSYST-LANGU": language,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="PRD",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="", language=""),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert client.Text == ""
    assert language.Text == ""
