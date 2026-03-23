from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from sap_automation.credentials import CredentialsLoader, SapCredentials
from sap_automation.errors import MissingCredentialError


def test_load_credentials_from_env(monkeypatch) -> None:
    monkeypatch.setenv("SAP_USERNAME", "user.sap")
    monkeypatch.setenv("SAP_PASSWORD", "secret")
    monkeypatch.setenv("SAP_CLIENT", "300")
    monkeypatch.setenv("SAP_LANGUAGE", "PT")

    credentials = CredentialsLoader().load()

    assert credentials.username == "user.sap"
    assert credentials.password == "secret"
    assert credentials.client == "300"
    assert credentials.language == "PT"


def test_load_credentials_missing_username(monkeypatch) -> None:
    monkeypatch.delenv("SAP_USERNAME", raising=False)
    monkeypatch.setenv("SAP_PASSWORD", "secret")

    with pytest.raises(MissingCredentialError, match="SAP_USERNAME"):
        CredentialsLoader().load()


def test_load_credentials_missing_password(monkeypatch) -> None:
    monkeypatch.setenv("SAP_USERNAME", "user.sap")
    monkeypatch.delenv("SAP_PASSWORD", raising=False)

    with pytest.raises(MissingCredentialError, match="SAP_PASSWORD"):
        CredentialsLoader().load()


def test_load_credentials_optional_client_language(monkeypatch) -> None:
    monkeypatch.setenv("SAP_USERNAME", "user.sap")
    monkeypatch.setenv("SAP_PASSWORD", "secret")
    monkeypatch.delenv("SAP_CLIENT", raising=False)
    monkeypatch.delenv("SAP_LANGUAGE", raising=False)

    credentials = CredentialsLoader().load()

    assert credentials.client == ""
    assert credentials.language == ""


def test_sap_credentials_frozen() -> None:
    credentials = SapCredentials(username="user.sap", password="secret")

    with pytest.raises(FrozenInstanceError):
        credentials.username = "other"  # type: ignore[misc]


def test_credentials_repr_hides_password() -> None:
    credentials = SapCredentials(username="user.sap", password="secret")

    assert "secret" not in repr(credentials)
    assert "secret" not in str(credentials)
