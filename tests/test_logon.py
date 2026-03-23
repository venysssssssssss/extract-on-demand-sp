from __future__ import annotations

from types import SimpleNamespace
import sys

import pytest

from sap_automation.errors import ConnectionNotFoundError, SapLogonNotRunningError
from sap_automation.logon import LogonConfig, SapApplicationProvider, SapConnectionOpener


class _FakeRotWrapper:
    def __init__(self, entry) -> None:
        self._entry = entry

    def GetROTEntry(self, name: str):
        assert name == "SAPGUI"
        return self._entry


class _FakeApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.ConnectionCount = 1
        self._children = [SimpleNamespace(Description="H181 RP1 ENEL SP CCS Produção (without SSO)")]

    def OpenConnection(self, description: str, synchronous: bool):
        self.calls.append((description, synchronous))
        return SimpleNamespace()

    def Children(self, index: int):
        return self._children[index]


class _FakeSapGuiAuto:
    def __init__(self, application) -> None:  # noqa: ANN001
        self._application = application

    def GetScriptingEngine(self):
        return self._application


class _FakeSapGuiAutoProperty:
    def __init__(self, application) -> None:  # noqa: ANN001
        self.GetScriptingEngine = application


class _FakeRotEntryWithoutGetScriptingEngine:
    OpenConnection = None


def test_application_provider_returns_engine(monkeypatch) -> None:
    engine = _FakeApplication()
    sap_gui = _FakeSapGuiAuto(engine)
    fake_client = SimpleNamespace(
        Dispatch=lambda progid: _FakeRotWrapper(sap_gui),
        GetObject=lambda name: sap_gui,
    )
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=fake_client))
    monkeypatch.setitem(sys.modules, "win32com.client", fake_client)

    provider = SapApplicationProvider()

    assert provider.get_application() is engine


def test_application_provider_accepts_property_style_scripting_engine(monkeypatch) -> None:
    engine = _FakeApplication()
    sap_gui = _FakeSapGuiAutoProperty(engine)
    fake_client = SimpleNamespace(
        Dispatch=lambda progid: _FakeRotWrapper(sap_gui),
        GetObject=lambda name: sap_gui,
    )
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=fake_client))
    monkeypatch.setitem(sys.modules, "win32com.client", fake_client)

    provider = SapApplicationProvider()

    assert provider.get_application() is engine


def test_application_provider_sap_not_running(monkeypatch) -> None:
    fake_client = SimpleNamespace(
        Dispatch=lambda progid: _FakeRotWrapper(None),
        GetObject=lambda name: None,
    )
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=fake_client))
    monkeypatch.setitem(sys.modules, "win32com.client", fake_client)

    provider = SapApplicationProvider()

    with pytest.raises(SapLogonNotRunningError):
        provider.get_application()


def test_application_provider_uses_direct_getobject_when_rot_entry_is_incomplete(monkeypatch) -> None:
    engine = _FakeApplication()
    fake_client = SimpleNamespace(
        Dispatch=lambda progid: _FakeRotWrapper(_FakeRotEntryWithoutGetScriptingEngine()),
        GetObject=lambda name: _FakeSapGuiAuto(engine),
    )
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=fake_client))
    monkeypatch.setitem(sys.modules, "win32com.client", fake_client)

    provider = SapApplicationProvider()

    assert provider.get_application() is engine


def test_application_provider_uses_scripting_control_dispatch_as_last_resort(monkeypatch) -> None:
    engine = _FakeApplication()

    def _dispatch(progid: str):
        if progid == "SapROTWr.SapROTWrapper":
            return _FakeRotWrapper(None)
        if progid == "Sapgui.ScriptingCtrl.1":
            return engine
        raise AssertionError(progid)

    fake_client = SimpleNamespace(
        Dispatch=_dispatch,
        GetObject=lambda name: None,
    )
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=fake_client))
    monkeypatch.setitem(sys.modules, "win32com.client", fake_client)

    provider = SapApplicationProvider()

    assert provider.get_application() is engine


def test_connection_opener_calls_open_connection() -> None:
    app = _FakeApplication()
    app.ConnectionCount = 0
    app._children = []
    opener = SapConnectionOpener(app_provider=SimpleNamespace(get_application=lambda: app))

    opener.open_connection(
        LogonConfig(
            connection_description="H181 RP1 ENEL SP CCS Produção (without SSO)",
            workspace_name="00 SAP ERP",
        )
    )

    assert app.calls == [("H181 RP1 ENEL SP CCS Produção (without SSO)", True)]


def test_connection_opener_reuses_existing_matching_connection() -> None:
    app = _FakeApplication()
    existing = app.Children(0)
    opener = SapConnectionOpener(app_provider=SimpleNamespace(get_application=lambda: app))

    resolved = opener.open_connection(
        LogonConfig(
            connection_description="H181 RP1 ENEL SP CCS Produção (without SSO)",
            workspace_name="00 SAP ERP",
        )
    )

    assert resolved is existing
    assert app.calls == []


def test_connection_opener_not_found() -> None:
    app = _FakeApplication()

    def _fail(*args, **kwargs):
        raise RuntimeError("not found")

    app.OpenConnection = _fail  # type: ignore[method-assign]
    opener = SapConnectionOpener(app_provider=SimpleNamespace(get_application=lambda: app))

    with pytest.raises(ConnectionNotFoundError, match="H181 RP1 ENEL SP CCS Produção"):
        opener.open_connection(
            LogonConfig(
                connection_description="H181 RP1 ENEL SP CCS Produção (without SSO)",
                workspace_name="00 SAP ERP",
            )
        )


def test_logon_config_frozen() -> None:
    config = LogonConfig(
        connection_description="H181 RP1 ENEL SP CCS Produção (without SSO)",
        workspace_name="00 SAP ERP",
    )

    with pytest.raises(Exception):
        config.workspace_name = "other"  # type: ignore[misc]
