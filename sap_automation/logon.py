from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .errors import ConnectionNotFoundError, LogonTimeoutError, SapLogonNotRunningError, SapLogonPadError


@dataclass(frozen=True)
class LogonConfig:
    connection_description: str
    workspace_name: str
    synchronous: bool = True
    logon_timeout_seconds: float = 45.0
    multiple_logon_action: str = "continue"
    ui_fallback_enabled: bool = True

    def __post_init__(self) -> None:
        if not self.connection_description.strip():
            raise ConnectionNotFoundError(description="", available_connections=[])


class SapApplicationProvider:
    def get_application(self) -> Any:
        try:
            import win32com.client  # type: ignore
        except Exception as exc:  # pragma: no cover - Windows only runtime path
            raise SapLogonPadError(
                "pywin32 é obrigatório para acessar o SAP Logon pad via COM."
            ) from exc

        candidates: list[Any] = []
        try:
            direct_object = win32com.client.GetObject("SAPGUI")
            if direct_object is not None:
                candidates.append(direct_object)
        except Exception:
            pass

        try:
            rot_wrapper = win32com.client.Dispatch("SapROTWr.SapROTWrapper")
            rot_entry = rot_wrapper.GetROTEntry("SAPGUI")
            if rot_entry is not None:
                candidates.append(rot_entry)
        except Exception:
            pass

        if not candidates:
            raise SapLogonNotRunningError()

        for candidate in candidates:
            application = self._resolve_application(candidate)
            if application is not None:
                return application

        raise SapLogonPadError(
            "Não foi possível obter o ScriptingEngine do SAP GUI. "
            "Verifique se o SAP Logon pad está aberto e se o SAP GUI Scripting está habilitado no cliente e no servidor."
        )

    def _resolve_application(self, candidate: Any) -> Any | None:
        if candidate is None:
            return None
        if hasattr(candidate, "OpenConnection"):
            return candidate
        if hasattr(candidate, "GetScriptingEngine"):
            try:
                application = getattr(candidate, "GetScriptingEngine")
            except Exception:
                return None
            if callable(application):
                try:
                    application = application()
                except Exception:
                    return None
            if application is not None:
                return application
        return None


class SapConnectionOpener:
    def __init__(
        self,
        app_provider: SapApplicationProvider,
        *,
        ui_opener: "SapLogonPadUiOpener | None" = None,
    ) -> None:
        self._app_provider = app_provider
        self._ui_opener = ui_opener

    def open_connection(self, config: LogonConfig) -> Any:
        application: Any | None = None
        used_ui_fallback = False
        try:
            application = self._app_provider.get_application()
        except SapLogonPadError:
            if not config.ui_fallback_enabled or self._ui_opener is None:
                raise
            self._ui_opener.open_connection(config)
            used_ui_fallback = True
            application = self._wait_for_application(config)

        existing_connection = self._find_existing_matching_connection(
            application=application,
            description=config.connection_description,
        )
        if existing_connection is not None:
            return existing_connection

        started_at = time.monotonic()
        if used_ui_fallback:
            connection = self._wait_for_matching_connection(
                application=application,
                description=config.connection_description,
                timeout_seconds=config.logon_timeout_seconds,
            )
        else:
            try:
                connection = application.OpenConnection(
                    config.connection_description,
                    config.synchronous,
                )
            except Exception as exc:  # pragma: no cover - COM runtime path
                if not config.ui_fallback_enabled or self._ui_opener is None:
                    raise ConnectionNotFoundError(
                        description=config.connection_description,
                        available_connections=self._list_available_connections(application),
                    ) from exc
                self._ui_opener.open_connection(config)
                connection = self._wait_for_matching_connection(
                    application=self._wait_for_application(config),
                    description=config.connection_description,
                    timeout_seconds=config.logon_timeout_seconds,
                )

        if connection is None:
            raise LogonTimeoutError(
                description=config.connection_description,
                timeout_seconds=config.logon_timeout_seconds,
            )

        if (time.monotonic() - started_at) > config.logon_timeout_seconds:
            raise LogonTimeoutError(
                description=config.connection_description,
                timeout_seconds=config.logon_timeout_seconds,
            )
        return connection

    def _find_existing_matching_connection(self, *, application: Any, description: str) -> Any | None:
        try:
            count = int(getattr(application, "ConnectionCount", 0))
        except Exception:
            count = 0
        for index in range(count):
            try:
                connection = application.Children(index)
            except Exception:
                continue
            current_description = str(getattr(connection, "Description", "")).strip()
            if current_description == description or description in current_description:
                return connection
        return None

    def _wait_for_application(self, config: LogonConfig) -> Any:
        deadline = time.monotonic() + config.logon_timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                return self._app_provider.get_application()
            except Exception as exc:  # pragma: no cover - COM runtime path
                last_error = exc
                time.sleep(0.5)
        raise SapLogonPadError(
            "A conexão foi acionada no SAP Logon pad, mas o ScriptingEngine do SAP GUI não ficou disponível a tempo."
        ) from last_error

    def _wait_for_matching_connection(
        self,
        *,
        application: Any,
        description: str,
        timeout_seconds: float,
    ) -> Any:
        deadline = time.monotonic() + timeout_seconds
        fallback_connection: Any | None = None
        while time.monotonic() < deadline:
            try:
                count = int(getattr(application, "ConnectionCount", 0))
            except Exception:
                count = 0
            for index in range(count):
                try:
                    connection = application.Children(index)
                except Exception:
                    continue
                if fallback_connection is None:
                    fallback_connection = connection
                current_description = str(getattr(connection, "Description", "")).strip()
                if current_description == description or description in current_description:
                    return connection
            if fallback_connection is not None and count == 1:
                return fallback_connection
            time.sleep(0.5)
        raise LogonTimeoutError(
            description=description,
            timeout_seconds=timeout_seconds,
        )

    def _list_available_connections(self, application: Any) -> list[str]:
        descriptions: list[str] = []
        try:
            count = int(getattr(application, "ConnectionCount", 0))
        except Exception:
            count = 0
        for index in range(count):
            try:
                connection = application.Children(index)
                description = str(getattr(connection, "Description", "")).strip()
                if description:
                    descriptions.append(description)
            except Exception:
                continue
        return descriptions


class SapLogonPadUiOpener:
    def open_connection(self, config: LogonConfig) -> None:
        try:
            from pywinauto import Desktop  # type: ignore
        except Exception as exc:  # pragma: no cover - Windows only runtime path
            raise SapLogonPadError(
                "pywinauto é obrigatório para o fallback de UI do SAP Logon pad."
            ) from exc

        desktop = Desktop(backend="uia")
        try:
            window = desktop.window(title_re="SAP Logon.*")
            window.wait("visible", timeout=max(5.0, config.logon_timeout_seconds))
            window.set_focus()
        except Exception as exc:  # pragma: no cover - Windows only runtime path
            raise SapLogonPadError(
                "A janela do SAP Logon pad não foi encontrada para o fallback de UI."
            ) from exc

        self._click_if_found(window, config.workspace_name)
        if not self._click_if_found(window, config.connection_description, double=True):
            logon_button_clicked = self._click_if_found(window, "Logon")
            if not logon_button_clicked:
                raise ConnectionNotFoundError(
                    description=config.connection_description,
                    available_connections=[],
                )
        else:
            return

        if not self._click_if_found(window, "Logon"):
            raise SapLogonPadError(
                "A conexão foi localizada no SAP Logon pad, mas o botão 'Logon' não foi encontrado."
            )

    def _click_if_found(self, window: Any, title: str, *, double: bool = False) -> bool:
        expected = str(title).strip()
        if not expected:
            return False
        expected_norm = expected.casefold()
        for control in self._iter_controls(window):
            control_text = str(getattr(control.element_info, "name", "")).strip()
            control_norm = control_text.casefold()
            if not control_norm:
                continue
            if expected_norm != control_norm and expected_norm not in control_norm and control_norm not in expected_norm:
                continue
            try:
                if double:
                    control.double_click_input()
                else:
                    control.click_input()
                return True
            except Exception:
                continue
        return False

    def _iter_controls(self, window: Any) -> list[Any]:
        try:
            return list(window.descendants())
        except Exception:
            return []
