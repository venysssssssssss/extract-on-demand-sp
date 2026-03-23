from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from .errors import MissingCredentialError


@dataclass(frozen=True)
class SapCredentials:
    username: str
    password: str = field(repr=False)
    client: str = ""
    language: str = ""

    def __post_init__(self) -> None:
        if not self.username.strip():
            raise MissingCredentialError("SAP_USERNAME")
        if not self.password.strip():
            raise MissingCredentialError("SAP_PASSWORD")


class CredentialsLoader:
    def __init__(self, env_path: Path | None = None) -> None:
        self._env_path = env_path

    def load(self) -> SapCredentials:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=self._env_path)
        username = str(os.getenv("SAP_USERNAME", "")).strip()
        password = str(os.getenv("SAP_PASSWORD", "")).strip()
        if not username:
            raise MissingCredentialError("SAP_USERNAME")
        if not password:
            raise MissingCredentialError("SAP_PASSWORD")
        return SapCredentials(
            username=username,
            password=password,
            client=str(os.getenv("SAP_CLIENT", "")).strip(),
            language=str(os.getenv("SAP_LANGUAGE", "")).strip(),
        )
