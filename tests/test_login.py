from __future__ import annotations

from types import SimpleNamespace
from typing import Callable

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
        self.focused = False
        self.caretPosition = 0

    def Select(self) -> None:
        self.selected = True

    def press(self) -> None:
        self.pressed = True

    def SetFocus(self) -> None:
        self.focused = True


class _FakeWindow:
    def __init__(self, on_send_vkey: Callable[[int], None] | None = None) -> None:
        self.vkeys: list[int] = []
        self.on_send_vkey = on_send_vkey

    def sendVKey(self, value: int) -> None:
        self.vkeys.append(value)
        if self.on_send_vkey is not None:
            self.on_send_vkey(value)


class _FakeSession:
    def __init__(self, mapping: dict[str, object], *, busy: bool = False, system_name: str = "") -> None:
        self._mapping = mapping
        self.Busy = busy
        self.Info = SimpleNamespace(SystemName=system_name, User="")

    def findById(self, item_id: str):
        if item_id not in self._mapping:
            raise KeyError(item_id)
        return self._mapping[item_id]

    def mark_logged_in(self, system_name: str = "PRD") -> None:
        self.Info.SystemName = system_name
        self.Info.User = "user.sap"


class _DisconnectedSession:
    @property
    def Busy(self):
        raise RuntimeError("session disconnected")

    @property
    def Info(self):
        raise RuntimeError("session disconnected")


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
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

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


def test_login_accepts_ctxt_user_and_password_fallback_ids() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/ctxtRSYST-BNAME": user,
            "wnd[0]/usr/txtRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert user.Text == "user.sap"
    assert password.Text == "secret"
    assert window.vkeys == [0]


def test_login_allows_prefilled_username_when_only_password_field_exists() -> None:
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert password.Text == "secret"
    assert window.vkeys == [0]


def test_login_only_submits_with_enter_without_pressing_main_confirm_button() -> None:
    user = _FakeField()
    password = _FakeField()
    confirm = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]/usr/btnBUTTON_1": confirm,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert window.vkeys == [0]
    assert confirm.pressed is False


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
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

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


def test_login_multiple_logon_confirm_falls_back_to_enter() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    popup = _FakeWindow()
    status = _FakeField()
    option = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
            "wnd[1]": popup,
            "wnd[1]/usr/radMULTI_LOGON_OPT1": option,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

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
    assert popup.vkeys == [0]


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


def test_login_dismisses_popup_without_explicit_confirm_button() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    popup = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
            "wnd[1]": popup,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert popup.vkeys == [0]


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


def test_login_detects_backend_session_terminated_popup() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    popup = _FakeField()
    popup.Text = "RP1: Mensagem do sistema SAP: Sessão back-end encerrada pelo sistema"
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
            "wnd[1]": popup,
        },
        system_name="",
    )

    with pytest.raises(LoginFailedError, match="Sessão back-end encerrada pelo sistema"):
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


def test_login_treats_okcode_field_as_authenticated_surface() -> None:
    session = _FakeSession(
        {
            "wnd[0]/tbar[0]/okcd": _FakeField(),
        },
        system_name="",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )


def test_login_treats_non_login_window_title_as_authenticated_surface() -> None:
    window = _FakeWindow()
    window.Text = "SAP Easy Access"
    session = _FakeSession(
        {
            "wnd[0]": window,
        },
        system_name="",
    )

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )


def test_login_does_not_skip_when_system_name_exists_but_login_screen_is_visible() -> None:
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
        },
        system_name="PRD",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert user.Text == "user.sap"
    assert password.Text == "secret"
    assert window.vkeys == [0]


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
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="300", language="PT"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert client.Text == "300"


def test_login_focuses_and_sets_caret_when_filling_fields() -> None:
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
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert user.focused is True
    assert user.caretPosition == len("user.sap")
    assert password.focused is True
    assert password.caretPosition == len("secret")
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
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="", language=""),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert client.Text == ""
    assert language.Text == ""


def test_login_uses_optional_client_field_fallback() -> None:
    user = _FakeField()
    password = _FakeField()
    client = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]/usr/txtRSYST-MESSION": client,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )
    window.on_send_vkey = lambda _: session.mark_logged_in()

    SapLoginHandler().login(
        session,
        SapCredentials(username="user.sap", password="secret", client="300"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
    )

    assert client.Text == "300"


def test_login_refreshes_session_when_com_handle_disconnects_after_submit() -> None:
    user = _FakeField()
    password = _FakeField()
    window = _FakeWindow()
    status = _FakeField()
    refreshed_window = _FakeWindow()
    refreshed_session = _FakeSession(
        {
            "wnd[0]": refreshed_window,
            "wnd[0]/sbar": status,
        },
        system_name="PRD",
    )
    session = _FakeSession(
        {
            "wnd[0]/usr/txtRSYST-BNAME": user,
            "wnd[0]/usr/pwdRSYST-BCODE": password,
            "wnd[0]": window,
            "wnd[0]/sbar": status,
        },
        system_name="",
    )
    disconnected = _DisconnectedSession()
    window.on_send_vkey = lambda _: None

    handler = SapLoginHandler()

    original_wait_not_busy = handler._wait_not_busy
    call_count = {"count": 0}

    def fake_wait_not_busy(current_session, *, timeout_seconds, logger=None, session_resolver=None):
        call_count["count"] += 1
        if call_count["count"] == 1:
            return disconnected
        return original_wait_not_busy(
            current_session,
            timeout_seconds=timeout_seconds,
            logger=logger,
            session_resolver=session_resolver,
        )

    handler._wait_not_busy = fake_wait_not_busy  # type: ignore[method-assign]

    result = handler.login(
        session,
        SapCredentials(username="user.sap", password="secret"),
        LogonConfig(connection_description="H181", workspace_name="00 SAP ERP"),
        session_resolver=lambda: refreshed_session,
    )

    assert result is refreshed_session


def test_login_wait_not_busy_retries_session_refresh_until_it_recovers() -> None:
    handler = SapLoginHandler()
    refreshed_session = _FakeSession({}, busy=False, system_name="PRD")
    disconnected = _DisconnectedSession()
    resolver_calls = {"count": 0}

    def resolver():
        resolver_calls["count"] += 1
        if resolver_calls["count"] < 3:
            raise RuntimeError("session not ready yet")
        return refreshed_session

    result = handler._wait_not_busy(  # noqa: SLF001
        disconnected,
        timeout_seconds=1.0,
        session_resolver=resolver,
    )

    assert result is refreshed_session
    assert resolver_calls["count"] == 3
