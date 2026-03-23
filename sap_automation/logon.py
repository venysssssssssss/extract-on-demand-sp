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

        rot_wrapper = win32com.client.Dispatch("SapROTWr.SapROTWrapper")
        sap_gui = rot_wrapper.GetROTEntry("SAPGUI")
        if sap_gui is None:
            raise SapLogonNotRunningError()
        application = sap_gui.GetScriptingEngine()
        if application is None:
            raise SapLogonPadError(
                "SAP GUI Scripting não está habilitado. Verifique as configurações do cliente e do servidor SAP."
            )
        return application


class SapConnectionOpener:
    def __init__(self, app_provider: SapApplicationProvider) -> None:
        self._app_provider = app_provider

    def open_connection(self, config: LogonConfig) -> Any:
        application = self._app_provider.get_application()
        started_at = time.monotonic()
        try:
            connection = application.OpenConnection(
                config.connection_description,
                config.synchronous,
            )
        except Exception as exc:  # pragma: no cover - COM runtime path
            raise ConnectionNotFoundError(
                description=config.connection_description,
                available_connections=self._list_available_connections(application),
            ) from exc

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
