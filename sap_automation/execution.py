from __future__ import annotations

import importlib
import logging
import time
from typing import Any, Protocol

from .credentials import CredentialsLoader
from .errors import LogonTimeoutError
from .login import SapLoginHandler
from .logon import LogonConfig, SapApplicationProvider, SapConnectionOpener


class SessionProvider(Protocol):
    def get_session(
        self,
        config: dict[str, Any],
        logger: logging.Logger | None = None,
    ) -> Any: ...


def _legacy_export_module():
    return importlib.import_module("sap_gui_export_compat")


class SapSessionProvider:
    def __init__(self) -> None:
        self._session: Any | None = None

    def get_session(self, config: dict[str, Any], logger: logging.Logger | None = None):
        if self._session is None:
            self._session = _legacy_export_module().connect_sap_session(
                config=config,
                logger=logger,
            )
        return self._session


class LogonPadSessionProvider:
    def __init__(
        self,
        *,
        credentials_loader: CredentialsLoader,
        app_provider: SapApplicationProvider,
        connection_opener: SapConnectionOpener,
        login_handler: SapLoginHandler,
    ) -> None:
        self._credentials_loader = credentials_loader
        self._app_provider = app_provider
        self._connection_opener = connection_opener
        self._login_handler = login_handler
        self._session: Any | None = None

    def get_session(
        self,
        config: dict[str, Any],
        logger: logging.Logger | None = None,
    ) -> Any:
        if self._session is not None:
            if logger is not None:
                logger.info("Reusing cached SAP session from provider")
            return self._session

        global_cfg = config.get("global", {})
        logon_pad_cfg = global_cfg.get("logon_pad", {})
        if logger is not None:
            logger.info(
                "Starting SAP session bootstrap connection=%s workspace=%s session_index=%s",
                str(logon_pad_cfg.get("connection_description", "")).strip(),
                str(logon_pad_cfg.get("workspace_name", "")).strip(),
                int(global_cfg.get("session_index", 0)),
            )
        credentials = self._credentials_loader.load(logger=logger)
        logon_config = LogonConfig(
            connection_description=str(logon_pad_cfg.get("connection_description", "")).strip(),
            workspace_name=str(logon_pad_cfg.get("workspace_name", "")).strip(),
            synchronous=bool(logon_pad_cfg.get("synchronous", True)),
            logon_timeout_seconds=float(global_cfg.get("login_timeout_seconds", 45.0)),
            multiple_logon_action=str(logon_pad_cfg.get("multiple_logon_action", "continue")).strip() or "continue",
            ui_fallback_enabled=bool(logon_pad_cfg.get("ui_fallback_enabled", True)),
        )
        connection = self._connection_opener.open_connection(logon_config, logger=logger)
        session_index = int(global_cfg.get("session_index", 0))
        session = self._wait_for_session(
            connection=connection,
            session_index=session_index,
            timeout_seconds=logon_config.logon_timeout_seconds,
        )
        if logger is not None:
            logger.info("SAP session acquired, starting interactive login")
        session = self._login_handler.login(
            session,
            credentials,
            logon_config,
            logger=logger,
            session_resolver=lambda: self._wait_for_session(
                connection=connection,
                session_index=session_index,
                timeout_seconds=min(logon_config.logon_timeout_seconds, 5.0),
            ),
        )
        if logger is not None:
            logger.info("SAP session authenticated successfully")
        self._session = session
        return self._session

    def _wait_for_session(
        self,
        *,
        connection: Any,
        session_index: int,
        timeout_seconds: float,
    ) -> Any:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                return connection.Children(session_index)
            except Exception:
                time.sleep(0.2)
        raise LogonTimeoutError(
            description=f"session_index={session_index}",
            timeout_seconds=timeout_seconds,
        )


class StepExecutor:
    def execute(
        self,
        *,
        session: Any,
        steps: list[dict[str, Any]],
        context: dict[str, str],
        default_timeout_seconds: float,
        logger: logging.Logger,
    ) -> None:
        _legacy_export_module().run_steps(
            session=session,
            steps=steps,
            context=context,
            default_timeout_seconds=default_timeout_seconds,
            logger=logger,
        )
