from __future__ import annotations

import logging
from typing import Any

import exemplo_sap_gui_export as legacy_export


class SapSessionProvider:
    def __init__(self) -> None:
        self._session: Any | None = None

    def get_session(self, config: dict[str, Any], logger: logging.Logger | None = None):
        if self._session is None:
            self._session = legacy_export.connect_sap_session(config=config, logger=logger)
        return self._session


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
        legacy_export.run_steps(
            session=session,
            steps=steps,
            context=context,
            default_timeout_seconds=default_timeout_seconds,
            logger=logger,
        )
