from __future__ import annotations

import time
import logging
from typing import Any, Callable

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
_FIELD_WRITE_RETRIES = 5
_FIELD_WRITE_SLEEP_SECONDS = 0.2
_LOGIN_SUBMIT_ATTEMPTS = 2
_LOGIN_CONFIRM_BUTTON_IDS: tuple[str, ...] = (
    "wnd[0]/tbar[0]/btn[0]",
    "wnd[0]/usr/btnBUTTON_1",
)


def _safe_find(session: Any, item_id: str) -> Any | None:
    try:
        return session.findById(item_id)
    except Exception:
        return None


def _is_logged_in(session: Any) -> bool:
    for item_id in (*_LOGIN_USER_IDS, *_LOGIN_PASSWORD_IDS):
        if _safe_find(session, item_id) is not None:
            return False
    try:
        user_name = str(getattr(session.Info, "User", "")).strip()
    except Exception:
        user_name = ""
    if user_name:
        return True
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
    def login(
        self,
        session: Any,
        credentials: SapCredentials,
        config: LogonConfig,
        logger: logging.Logger | None = None,
        session_resolver: Callable[[], Any] | None = None,
    ) -> Any:
        self._log(
            logger,
            "SAP login started username_masked=%s client=%s language=%s",
            self._mask_username(credentials.username),
            credentials.client or "<empty>",
            credentials.language or "<empty>",
        )
        session = self._wait_for_login_screen_or_session(
            session,
            timeout_seconds=config.logon_timeout_seconds,
            logger=logger,
            session_resolver=session_resolver,
        )
        if _is_logged_in(session):
            self._log(logger, "SAP session already authenticated before login submit")
            return session

        for attempt in range(_LOGIN_SUBMIT_ATTEMPTS):
            self._log(logger, "SAP login attempt=%s", attempt + 1)
            self._fill_login_fields(session, credentials, logger=logger)
            session = self._submit_login(
                session,
                config,
                logger=logger,
                session_resolver=session_resolver,
            )
            try:
                session = self._wait_login_resolution(
                    session,
                    timeout_seconds=config.logon_timeout_seconds,
                    logger=logger,
                    session_resolver=session_resolver,
                )
                self._dismiss_information_popup(session)
                self._log(logger, "SAP login finished successfully")
                return session
            except LoginTimeoutError:
                self._log(
                    logger,
                    "SAP login attempt timed out attempt=%s login_screen_visible=%s",
                    attempt + 1,
                    self._is_login_screen_visible(session),
                )
                if attempt + 1 >= _LOGIN_SUBMIT_ATTEMPTS or not self._is_login_screen_visible(session):
                    raise
        raise LoginFailedError("A tela de login SAP permaneceu aberta apos as tentativas de autenticacao.")

    def _resolve_first(self, session: Any, item_ids: tuple[str, ...]) -> Any | None:
        for item_id in item_ids:
            item = _safe_find(session, item_id)
            if item is not None:
                return item
        return None

    def _wait_not_busy(
        self,
        session: Any,
        *,
        timeout_seconds: float,
        logger: logging.Logger | None = None,
        session_resolver: Callable[[], Any] | None = None,
    ) -> Any:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                if not bool(getattr(session, "Busy", False)):
                    return session
            except Exception as exc:
                if session_resolver is None:
                    raise
                refreshed = self._try_refresh_session(
                    session_resolver=session_resolver,
                    logger=logger,
                    reason=str(exc),
                )
                if refreshed is not None:
                    session = refreshed
                    continue
                time.sleep(0.2)
                continue
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _fill_login_fields(
        self,
        session: Any,
        credentials: SapCredentials,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        username_field_id, username_field = self._resolve_first_with_id(session, _LOGIN_USER_IDS)
        if username_field is not None:
            self._log(logger, "SAP username field resolved field_id=%s", username_field_id)
            self._write_field(username_field, credentials.username, verify=True)
            self._log(logger, "SAP username written field_id=%s length=%s", username_field_id, len(credentials.username))
        else:
            self._log(logger, "SAP username field not found via known ids")

        password_field_id, password_field = self._resolve_first_with_id(session, _LOGIN_PASSWORD_IDS)
        if password_field is None:
            raise LoginFailedError("Campo de senha não encontrado na tela de login SAP.")
        self._log(logger, "SAP password field resolved field_id=%s", password_field_id)
        self._write_field(password_field, credentials.password, verify=False)
        self._log(logger, "SAP password written field_id=%s length=%s", password_field_id, len(credentials.password))

        if credentials.client:
            client_field_id, client_field = self._resolve_first_with_id(session, _LOGIN_CLIENT_IDS)
            if client_field is not None:
                self._write_field(client_field, credentials.client, verify=True)
                self._log(logger, "SAP client written field_id=%s value=%s", client_field_id, credentials.client)

        if credentials.language:
            language_field_id, language_field = self._resolve_first_with_id(session, _LOGIN_LANGUAGE_IDS)
            if language_field is not None:
                self._write_field(language_field, credentials.language, verify=True)
                self._log(
                    logger,
                    "SAP language written field_id=%s value=%s",
                    language_field_id,
                    credentials.language,
                )

    def _write_field(self, item: Any, value: str, *, verify: bool) -> None:
        last_seen = ""
        for _ in range(_FIELD_WRITE_RETRIES):
            self._focus_item(item)
            _set_text(item, value)
            self._set_caret(item, len(value))
            if not verify:
                return
            last_seen = self._read_text(item)
            if last_seen.strip() == value.strip():
                return
            time.sleep(_FIELD_WRITE_SLEEP_SECONDS)
        raise LoginFailedError(
            "Nao foi possivel confirmar o preenchimento de um campo da tela de login SAP. "
            f"Valor atual: '{last_seen}'."
        )

    def _submit_login(
        self,
        session: Any,
        config: LogonConfig,
        *,
        logger: logging.Logger | None = None,
        session_resolver: Callable[[], Any] | None = None,
    ) -> Any:
        main_window = _safe_find(session, "wnd[0]")
        if main_window is None:
            raise LoginFailedError("Janela principal SAP não encontrada para submeter o login.")
        self._log(logger, "SAP login submit sending Enter/VKey(0)")
        main_window.sendVKey(0)
        if self._is_login_screen_visible(session):
            confirm = self._resolve_first(session, _LOGIN_CONFIRM_BUTTON_IDS)
            if confirm is not None:
                try:
                    confirm.press()
                    self._log(logger, "SAP login confirm button pressed after Enter")
                except Exception:
                    pass
        session = self._wait_not_busy(
            session,
            timeout_seconds=config.logon_timeout_seconds,
            logger=logger,
            session_resolver=session_resolver,
        )
        self._handle_multiple_logon(session, config)
        session = self._wait_not_busy(
            session,
            timeout_seconds=config.logon_timeout_seconds,
            logger=logger,
            session_resolver=session_resolver,
        )
        return session

    def _wait_for_login_screen_or_session(
        self,
        session: Any,
        *,
        timeout_seconds: float,
        logger: logging.Logger | None = None,
        session_resolver: Callable[[], Any] | None = None,
    ) -> Any:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            refreshed = self._try_refresh_session(
                session_resolver=session_resolver,
                logger=logger,
                probe_session=session,
            )
            if refreshed is not None:
                session = refreshed
            if _is_logged_in(session):
                self._log(logger, "SAP session detected as already authenticated")
                return session
            if self._resolve_first(session, _LOGIN_USER_IDS) is not None:
                self._log(logger, "SAP login screen detected via username field")
                return session
            if self._resolve_first(session, _LOGIN_PASSWORD_IDS) is not None:
                self._log(logger, "SAP login screen detected via password field")
                return session
            popup = _safe_find(session, "wnd[1]")
            if popup is not None:
                self._dismiss_information_popup(session)
            status_bar = _safe_find(session, _STATUS_BAR_ID)
            status_text = str(getattr(status_bar, "Text", "")).strip() if status_bar is not None else ""
            if status_text and self._looks_like_login_error(status_text):
                self._log(logger, "SAP login screen returned status error=%s", status_text)
                raise LoginFailedError(status_text)
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _is_login_screen_visible(self, session: Any) -> bool:
        return (
            self._resolve_first(session, _LOGIN_PASSWORD_IDS) is not None
            or self._resolve_first(session, _LOGIN_USER_IDS) is not None
        )

    def _resolve_first_with_id(self, session: Any, item_ids: tuple[str, ...]) -> tuple[str | None, Any | None]:
        for item_id in item_ids:
            item = _safe_find(session, item_id)
            if item is not None:
                return item_id, item
        return None, None

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

    def _wait_login_resolution(
        self,
        session: Any,
        *,
        timeout_seconds: float,
        logger: logging.Logger | None = None,
        session_resolver: Callable[[], Any] | None = None,
    ) -> Any:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            refreshed = self._try_refresh_session(
                session_resolver=session_resolver,
                logger=logger,
                probe_session=session,
            )
            if refreshed is not None:
                session = refreshed
            if _is_logged_in(session):
                self._log(logger, "SAP login resolution detected authenticated session")
                return session
            popup = _safe_find(session, "wnd[1]")
            if popup is not None:
                self._dismiss_information_popup(session)
            status_bar = _safe_find(session, _STATUS_BAR_ID)
            status_text = str(getattr(status_bar, "Text", "")).strip() if status_bar is not None else ""
            if status_text and self._looks_like_login_error(status_text):
                self._log(logger, "SAP post-submit status error=%s", status_text)
                raise LoginFailedError(status_text)
            time.sleep(0.2)
        raise LoginTimeoutError(timeout_seconds=timeout_seconds)

    def _try_refresh_session(
        self,
        *,
        session_resolver: Callable[[], Any] | None,
        logger: logging.Logger | None = None,
        reason: str = "",
        probe_session: Any | None = None,
    ) -> Any | None:
        if session_resolver is None:
            return None
        if probe_session is not None:
            try:
                getattr(probe_session, "Info", None)
                return None
            except Exception as exc:
                reason = reason or str(exc)
        try:
            session = session_resolver()
        except Exception:
            return None
        self._log(
            logger,
            "SAP session handle refreshed after COM disconnect reason=%s",
            reason or "<unknown>",
        )
        return session

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

    def _focus_item(self, item: Any) -> None:
        set_focus = getattr(item, "SetFocus", None)
        if callable(set_focus):
            try:
                set_focus()
            except Exception:
                return

    def _set_caret(self, item: Any, position: int) -> None:
        if not hasattr(item, "caretPosition"):
            return
        try:
            setattr(item, "caretPosition", position)
        except Exception:
            return

    def _read_text(self, item: Any) -> str:
        for attr_name in ("Text", "text"):
            if hasattr(item, attr_name):
                try:
                    return str(getattr(item, attr_name))
                except Exception:
                    continue
        return ""

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

    def _mask_username(self, value: str) -> str:
        if len(value) <= 2:
            return "*" * len(value)
        return f"{value[:2]}***{value[-1]}"

    def _log(self, logger: logging.Logger | None, message: str, *args: Any) -> None:
        if logger is None:
            return
        logger.info(message, *args)
