from __future__ import annotations

from sap_automation.integrations import Iw59ExportAdapter, Iw67ExportAdapter


def test_pending_integrations_are_iw59_and_iw67() -> None:
    assert Iw59ExportAdapter().to_dict()["status"] == "pending_configuration"
    assert Iw67ExportAdapter().to_dict()["status"] == "pending_configuration"
    assert "IW59" in Iw59ExportAdapter().to_dict()["reason"]
    assert "IW67" in Iw67ExportAdapter().to_dict()["reason"]
