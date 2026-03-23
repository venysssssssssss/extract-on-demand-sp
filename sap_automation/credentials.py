from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path

from .errors import MissingCredentialError

_USERNAME_ENV_KEYS: tuple[str, ...] = ("SAP_USERNAME", "SAP_USER")
_PASSWORD_ENV_KEYS: tuple[str, ...] = ("SAP_PASSWORD", "SAP_PASS")


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

    def load(self, logger: logging.Logger | None = None) -> SapCredentials:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=self._env_path, override=True)
        username_key, username = self._first_env_value(_USERNAME_ENV_KEYS)
        password_key, password = self._first_env_value(_PASSWORD_ENV_KEYS)
        if not username:
            raise MissingCredentialError("SAP_USERNAME")
        if not password:
            raise MissingCredentialError("SAP_PASSWORD")
        if logger is not None:
            logger.info(
                "SAP credentials loaded username_source=%s password_source=%s env_path=%s username_masked=%s",
                username_key or "missing",
                password_key or "missing",
                str(self._env_path) if self._env_path is not None else ".env auto-discovery",
                self._mask_username(username),
            )
        return SapCredentials(
            username=username,
            password=password,
            client=str(os.getenv("SAP_CLIENT", "")).strip(),
            language=str(os.getenv("SAP_LANGUAGE", "")).strip(),
        )

    def _first_env_value(self, keys: tuple[str, ...]) -> tuple[str, str]:
        for key in keys:
            value = str(os.getenv(key, "")).strip()
            if value:
                return key, value
        return "", ""

    def _mask_username(self, value: str) -> str:
        if len(value) <= 2:
            return "*" * len(value)
        return f"{value[:2]}***{value[-1]}"
