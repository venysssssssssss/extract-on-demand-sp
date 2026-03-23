from __future__ import annotations

import time
import logging
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
    def get_application(self, logger: logging.Logger | None = None) -> Any:
        try:
            import win32com.client  # type: ignore
        except Exception as exc:  # pragma: no cover - Windows only runtime path
            raise SapLogonPadError(
                "pywin32 é obrigatório para acessar o SAP Logon pad via COM."
            ) from exc

        strategies = (
            ("GetObject(SAPGUI)", lambda: win32com.client.GetObject("SAPGUI")),
            (
                "SapROTWr.SapROTWrapper/GetROTEntry(SAPGUI)",
                lambda: win32com.client.Dispatch("SapROTWr.SapROTWrapper").GetROTEntry("SAPGUI"),
            ),
            ("Dispatch(Sapgui.ScriptingCtrl.1)", lambda: win32com.client.Dispatch("Sapgui.ScriptingCtrl.1")),
        )

        had_runtime_candidate = False
        failures: list[str] = []
        for strategy_name, resolver in strategies:
            try:
                candidate = resolver()
            except Exception as exc:
                failures.append(f"{strategy_name}: {exc}")
                continue
            if candidate is None:
                failures.append(f"{strategy_name}: candidate unavailable")
                continue
            had_runtime_candidate = True
            application, detail = self._resolve_application(candidate)
            if application is not None:
                if logger is not None:
                    logger.info("SAP COM application resolved via %s", strategy_name)
                return application
            failures.append(f"{strategy_name}: {detail}")

        if not had_runtime_candidate:
            raise SapLogonNotRunningError()

        raise SapLogonPadError(
            "Não foi possível obter o ScriptingEngine do SAP GUI via COM. "
            "Verifique se o SAP Logon pad está aberto e se o SAP GUI Scripting está habilitado no cliente e no servidor. "
            f"Tentativas: {' | '.join(failures[:5])}"
        )

    def _resolve_application(self, candidate: Any) -> tuple[Any | None, str]:
        if candidate is None:
            return None, "candidate is None"
        if self._looks_like_application(candidate):
            return candidate, "candidate exposes application surface"

        try:
            application = getattr(candidate, "GetScriptingEngine")
        except Exception as exc:
            application = None
            get_engine_error = str(exc)
        else:
            get_engine_error = ""
        if application is not None:
            if self._looks_like_application(application):
                return application, "resolved from GetScriptingEngine attribute"
            if callable(application):
                try:
                    called_application = application()
                except Exception as exc:
                    return None, f"GetScriptingEngine callable failed: {exc}"
                if self._looks_like_application(called_application):
                    return called_application, "resolved from GetScriptingEngine callable"
                if called_application is not None:
                    return called_application, "resolved from GetScriptingEngine callable"
            return application, "resolved from GetScriptingEngine attribute"

        try:
            scripting_engine = getattr(candidate, "ScriptingEngine")
        except Exception as exc:
            scripting_engine = None
            scripting_engine_error = str(exc)
        else:
            scripting_engine_error = ""
        if scripting_engine is not None:
            if self._looks_like_application(scripting_engine):
                return scripting_engine, "resolved from ScriptingEngine attribute"
            return scripting_engine, "resolved from ScriptingEngine"

        if get_engine_error:
            return None, f"GetScriptingEngine unavailable: {get_engine_error}"
        if scripting_engine_error:
            return None, f"ScriptingEngine unavailable: {scripting_engine_error}"
        return None, "candidate did not expose OpenConnection/GetScriptingEngine/ScriptingEngine"

    def _looks_like_application(self, candidate: Any) -> bool:
        try:
            open_connection = getattr(candidate, "OpenConnection")
        except Exception:
            open_connection = None
        if callable(open_connection):
            return True
        try:
            children = getattr(candidate, "Children")
        except Exception:
            children = None
        if callable(children):
            return True
        try:
            connection_count = getattr(candidate, "ConnectionCount")
        except Exception:
            connection_count = None
        return connection_count is not None


class SapConnectionOpener:
    def __init__(
        self,
        app_provider: SapApplicationProvider,
        *,
        ui_opener: "SapLogonPadUiOpener | None" = None,
    ) -> None:
        self._app_provider = app_provider
        self._ui_opener = ui_opener

    def open_connection(self, config: LogonConfig, logger: logging.Logger | None = None) -> Any:
        application: Any | None = None
        used_ui_fallback = False
        try:
            application = self._app_provider.get_application(logger=logger)
            if logger is not None:
                logger.info("SAP scripting engine resolved via COM")
        except SapLogonPadError as exc:
            if not config.ui_fallback_enabled or self._ui_opener is None:
                raise
            if logger is not None:
                logger.warning("SAP COM open failed, switching to SAP Logon UI fallback reason=%s", exc)
            self._ui_opener.open_connection(config, logger=logger)
            used_ui_fallback = True
            application = self._wait_for_application(config, logger=logger)

        existing_connection = self._find_existing_matching_connection(
            application=application,
            description=config.connection_description,
        )
        if existing_connection is not None:
            if logger is not None:
                logger.info(
                    "Reusing existing SAP connection description=%s",
                    config.connection_description,
                )
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
                if logger is not None:
                    logger.info(
                        "Opening new SAP connection description=%s synchronous=%s",
                        config.connection_description,
                        config.synchronous,
                    )
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
                if logger is not None:
                    logger.warning(
                        "OpenConnection failed for description=%s, retrying with UI fallback",
                        config.connection_description,
                    )
                self._ui_opener.open_connection(config, logger=logger)
                connection = self._wait_for_matching_connection(
                    application=self._wait_for_application(config, logger=logger),
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

    def _wait_for_application(self, config: LogonConfig, logger: logging.Logger | None = None) -> Any:
        deadline = time.monotonic() + config.logon_timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                return self._app_provider.get_application(logger=logger)
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
    def open_connection(self, config: LogonConfig, logger: logging.Logger | None = None) -> None:
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
            if logger is not None:
                logger.info("SAP Logon window focused for UI fallback")
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
        if logger is not None:
            logger.info(
                "SAP Logon UI fallback activated connection=%s workspace=%s",
                config.connection_description,
                config.workspace_name,
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
