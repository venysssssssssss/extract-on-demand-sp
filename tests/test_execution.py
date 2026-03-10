from __future__ import annotations

import logging

from sap_automation.execution import StepExecutor


def test_step_executor_delegates_to_legacy_runner(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    def fake_run_steps(**kwargs) -> None:
        recorded.update(kwargs)

    monkeypatch.setattr("sap_automation.execution.legacy_export.run_steps", fake_run_steps)

    executor = StepExecutor()
    executor.execute(
        session="sap-session",
        steps=[{"action": "press"}],
        context={"object": "CA"},
        default_timeout_seconds=15.0,
        logger=logging.getLogger("test"),
    )

    assert recorded["session"] == "sap-session"
    assert recorded["steps"] == [{"action": "press"}]
    assert recorded["context"] == {"object": "CA"}
    assert recorded["default_timeout_seconds"] == 15.0
