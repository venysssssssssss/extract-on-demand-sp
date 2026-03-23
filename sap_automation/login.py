from __future__ import annotations

import time
from typing import Any

from .credentials import SapCredentials
from .errors import LoginFailedError, LoginTimeoutError, MultipleLogonError
from .logon import LogonConfig

_LOGIN_USER_IDS: tuple[str, ...] = (
    "wnd[0]/usr/txtRSYST-BNAME",
    "wnd[0]/usr/ctxtRSYST-BNAME",
)
_LOGIN_PASSWORD_IDS: tuple[str, ...] = (
    "wnd[0]/usr/pwdRSYST-BCODE",
    "wnd[0]/usr/txtRSYST-BCODE",
)
_LOGIN_CLIENT_IDS: tuple[str, ...] = (
    "wnd[0]/usr/txtRSYST-MANDT",
    "wnd[0]/usr/txtRSYST-MESSION",
    "wnd[0]/usr/ctxtRSYST-MANDT",
    "wnd[0]/usr/ctxtRSYST-MESSION",
)
_LOGIN_LANGUAGE_IDS: tuple[str, ...] = (
    "wnd[0]/usr/txtRSYST-LANGU",
    "wnd[0]/usr/ctxtRSYST-LANGU",
)
_STATUS_BAR_ID = "wnd[0]/sbar"
_MULTI_LOGON_IDS: dict[str, tuple[str, ...]] = {
    "continue": (
        "wnd[1]/usr/radMULTI_LOGON_OPT1",
        "wnd[1]/usr/radMULTI_LOGON_OPT1[0,0]",
    ),
    "terminate_other": (
        "wnd[1]/usr/radMULTI_LOGON_OPT2",
        "wnd[1]/usr/radMULTI_LOGON_OPT2[1,0]",
    ),
}
_MULTI_LOGON_CONFIRM_IDS: tuple[str, ...] = (
    "wnd[1]/tbar[0]/btn[0]",
    "wnd[1]/usr/btnSPOP-OPTION1",
    "wnd[1]/usr/btnBUTTON_1",
)
_INFO_POPUP_CONFIRM_IDS: tuple[str, ...] = (
    "wnd[1]/tbar[0]/btn[0]",
    "wnd[1]/usr/btnSPOP-OPTION1",
    "wnd[1]/usr/btnBUTTON_1",
)


def _safe_find(session: Any, item_id: str) -> Any | None:
    try:
        return session.findById(item_id)
    except Exception:
        return None


def _is_logged_in(session: Any) -> bool:
    try:
        system_name = str(getattr(session.Info, "SystemName", "")).strip()
    except Exception:
        system_name = ""
    return bool(system_name)


def _set_text(item: Any, value: str) -> None:
    for attr_name in ("Text", "text"):
        if hasattr(item, attr_name):
            try:
                setattr(item, attr_name, value)
                return
            except Exception:
                continue
    raise LoginFailedError("Não foi possível preencher um campo da tela de login SAP.")


class SapLoginHandler:
    def login(self, session: Any, credentials: SapCredentials, config: LogonConfig) -> None:
        self._wait_for_login_screen_or_session(session, timeout_seconds=config.logon_timeout_seconds)
        if _is_logged_in(session):
            return

        username_field = self._resolve_first(session, _LOGIN_USER_IDS)
        if username_field is not None:
            _set_text(username_field, credentials.username)

        password_field = self._resolve_first(session, _LOGIN_PASSWORD_IDS)
        if password_field is None:
            raise LoginFailedError("Campo de senha não encontrado na tela de login SAP.")
        _set_text(password_field, credentials.password)

        if credentials.client:
            client_field = self._resolve_first(session, _LOGIN_CLIENT_IDS)
            if client_field is not None:
                _set_text(client_field, credentials.client)

        if credentials.language:
            language_field = self._resolve_first(session, _LOGIN_LANGUAGE_IDS)
            if language_field is not None:
                _set_text(language_field, credentials.language)

        main_window = _safe_find(session, "wnd[0]")
        if main_window is None:
            raise LoginFailedError("Janela principal SAP não encontrada para submeter o login.")
        main_window.sendVKey(0)
        self._wait_not_busy(session, timeout_seconds=config.logon_timeout_seconds)
        self._handle_multiple_logon(session, config)
        self._wait_not_busy(session, timeout_seconds=config.logon_timeout_seconds)
        self._wait_login_resolution(session, timeout_seconds=config.logon_timeout_seconds)
        self._dismiss_information_popup(session)

    def _resolve_first(self, session: Any, item_ids: tuple[str, ...]) -> Any | None:
        for item_id in item_ids:
            item = _safe_find(session, item_id)
            if item is not None:
                return item
        return None

    def _wait_not_busy(self, session: Any, *, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if not bool(getattr(session, "Busy", False)):
                return
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _wait_for_login_screen_or_session(self, session: Any, *, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if _is_logged_in(session):
                return
            if self._resolve_first(session, _LOGIN_USER_IDS) is not None:
                return
            if self._resolve_first(session, _LOGIN_PASSWORD_IDS) is not None:
                return
            popup = _safe_find(session, "wnd[1]")
            if popup is not None:
                self._dismiss_information_popup(session)
            status_bar = _safe_find(session, _STATUS_BAR_ID)
            status_text = str(getattr(status_bar, "Text", "")).strip() if status_bar is not None else ""
            if status_text and self._looks_like_login_error(status_text):
                raise LoginFailedError(status_text)
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _handle_multiple_logon(self, session: Any, config: LogonConfig) -> None:
        popup = self._wait_for_optional_popup(session, timeout_seconds=2.5)
        if popup is None:
            return

        if config.multiple_logon_action == "fail":
            raise MultipleLogonError(
                "Foi detectado popup de logon múltiplo e a ação configurada é 'fail'."
            )

        option_ids = _MULTI_LOGON_IDS.get(config.multiple_logon_action, _MULTI_LOGON_IDS["continue"])
        radio = self._resolve_first(session, option_ids)
        if radio is None:
            return
        radio.Select()
        confirm = self._resolve_first(session, _MULTI_LOGON_CONFIRM_IDS)
        if confirm is not None:
            confirm.press()
            return
        popup_window = _safe_find(session, "wnd[1]")
        if popup_window is None:
            raise MultipleLogonError(
                "Popup de logon múltiplo apareceu, mas a confirmação não pôde ser executada."
            )
        popup_window.sendVKey(0)

    def _wait_login_resolution(self, session: Any, *, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if _is_logged_in(session):
                return
            popup = _safe_find(session, "wnd[1]")
            if popup is not None:
                self._dismiss_information_popup(session)
            status_bar = _safe_find(session, _STATUS_BAR_ID)
            status_text = str(getattr(status_bar, "Text", "")).strip() if status_bar is not None else ""
            if status_text and self._looks_like_login_error(status_text):
                raise LoginFailedError(status_text)
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _dismiss_information_popup(self, session: Any) -> None:
        popup = _safe_find(session, "wnd[1]")
        if popup is None:
            return
        confirm = self._resolve_first(session, _INFO_POPUP_CONFIRM_IDS)
        if confirm is not None:
            try:
                confirm.press()
                return
            except Exception:
                pass
        try:
            popup.sendVKey(0)
        except Exception:
            return

    def _wait_for_optional_popup(self, session: Any, *, timeout_seconds: float) -> Any | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            popup = _safe_find(session, "wnd[1]")
            if popup is not None:
                return popup
            time.sleep(0.1)
        return None

    def _looks_like_login_error(self, value: str) -> bool:
        normalized = value.casefold()
        markers = (
            "senha",
            "password",
            "bloquead",
            "nao autorizado",
            "não autorizado",
            "incorret",
            "erro",
            "logon",
            "usuario",
            "usuário",
        )
        return any(marker in normalized for marker in markers)
