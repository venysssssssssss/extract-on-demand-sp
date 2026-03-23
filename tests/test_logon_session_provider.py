from __future__ import annotations

from types import SimpleNamespace

import pytest

from sap_automation.credentials import SapCredentials
from sap_automation.errors import MissingCredentialError
from sap_automation.execution import LogonPadSessionProvider, SapSessionProvider
from sap_automation.logon import LogonConfig
from sap_automation.service import create_session_provider


class _FakeCredentialsLoader:
    def __init__(self, credentials: SapCredentials | Exception) -> None:
        self._credentials = credentials
        self.calls = 0

    def load(self, logger=None) -> SapCredentials:  # noqa: ANN001
        self.calls += 1
        if isinstance(self._credentials, Exception):
            raise self._credentials
        return self._credentials


class _FakeConnection:
    def __init__(self, session) -> None:  # noqa: ANN001
        self._session = session
        self.calls = 0

    def Children(self, index: int):  # noqa: ANN001
        self.calls += 1
        return self._session


class _FakeConnectionOpener:
    def __init__(self, connection_or_error) -> None:  # noqa: ANN001
        self._connection_or_error = connection_or_error
        self.calls = 0

    def open_connection(self, config: LogonConfig, logger=None):  # noqa: ANN001
        self.calls += 1
        if isinstance(self._connection_or_error, Exception):
            raise self._connection_or_error
        return self._connection_or_error


class _FakeLoginHandler:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    def login(self, session, credentials, config, logger=None) -> None:  # noqa: ANN001
        self.calls += 1
        if self.error is not None:
            raise self.error


def test_full_flow_orchestration() -> None:
    session = SimpleNamespace()
    loader = _FakeCredentialsLoader(SapCredentials(username="user.sap", password="secret"))
    opener = _FakeConnectionOpener(_FakeConnection(session))
    login_handler = _FakeLoginHandler()
    provider = LogonPadSessionProvider(
        credentials_loader=loader,
        app_provider=SimpleNamespace(),
        connection_opener=opener,
        login_handler=login_handler,
    )

    resolved = provider.get_session(
        {
            "global": {
                "session_index": 0,
                "login_timeout_seconds": 45,
                "logon_pad": {
                    "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                    "workspace_name": "00 SAP ERP",
                },
            }
        }
    )

    assert resolved is session
    assert loader.calls == 1
    assert opener.calls == 1
    assert login_handler.calls == 1


def test_session_cached_after_first_call() -> None:
    session = SimpleNamespace()
    loader = _FakeCredentialsLoader(SapCredentials(username="user.sap", password="secret"))
    opener = _FakeConnectionOpener(_FakeConnection(session))
    login_handler = _FakeLoginHandler()
    provider = LogonPadSessionProvider(
        credentials_loader=loader,
        app_provider=SimpleNamespace(),
        connection_opener=opener,
        login_handler=login_handler,
    )
    config = {
        "global": {
            "session_index": 0,
            "login_timeout_seconds": 45,
            "logon_pad": {
                "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                "workspace_name": "00 SAP ERP",
            },
        }
    }

    first = provider.get_session(config)
    second = provider.get_session(config)

    assert first is second
    assert loader.calls == 1
    assert opener.calls == 1
    assert login_handler.calls == 1


def test_propagates_credentials_error() -> None:
    provider = LogonPadSessionProvider(
        credentials_loader=_FakeCredentialsLoader(MissingCredentialError("SAP_USERNAME")),
        app_provider=SimpleNamespace(),
        connection_opener=_FakeConnectionOpener(_FakeConnection(SimpleNamespace())),
        login_handler=_FakeLoginHandler(),
    )

    with pytest.raises(MissingCredentialError):
        provider.get_session(
            {
                "global": {
                    "session_index": 0,
                    "login_timeout_seconds": 45,
                    "logon_pad": {
                        "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                        "workspace_name": "00 SAP ERP",
                    },
                }
            }
        )


def test_propagates_connection_error() -> None:
    provider = LogonPadSessionProvider(
        credentials_loader=_FakeCredentialsLoader(SapCredentials(username="user.sap", password="secret")),
        app_provider=SimpleNamespace(),
        connection_opener=_FakeConnectionOpener(RuntimeError("boom")),
        login_handler=_FakeLoginHandler(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        provider.get_session(
            {
                "global": {
                    "session_index": 0,
                    "login_timeout_seconds": 45,
                    "logon_pad": {
                        "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                        "workspace_name": "00 SAP ERP",
                    },
                }
            }
        )


def test_propagates_login_error() -> None:
    provider = LogonPadSessionProvider(
        credentials_loader=_FakeCredentialsLoader(SapCredentials(username="user.sap", password="secret")),
        app_provider=SimpleNamespace(),
        connection_opener=_FakeConnectionOpener(_FakeConnection(SimpleNamespace())),
        login_handler=_FakeLoginHandler(RuntimeError("bad login")),
    )

    with pytest.raises(RuntimeError, match="bad login"):
        provider.get_session(
            {
                "global": {
                    "session_index": 0,
                    "login_timeout_seconds": 45,
                    "logon_pad": {
                        "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                        "workspace_name": "00 SAP ERP",
                    },
                }
            }
        )


def test_factory_returns_legacy_provider_when_logon_pad_disabled() -> None:
    provider = create_session_provider({"global": {"logon_pad": {"enabled": False}}})

    assert isinstance(provider, SapSessionProvider)


def test_factory_returns_logon_pad_provider_when_enabled() -> None:
    provider = create_session_provider(
        {
            "global": {
                "logon_pad": {
                    "enabled": True,
                    "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
                    "workspace_name": "00 SAP ERP",
                }
            }
        }
    )

    assert isinstance(provider, LogonPadSessionProvider)
