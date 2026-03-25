from __future__ import annotations

from sap_automation.integrations import Iw59ExportAdapter, Iw67ExportAdapter


def test_integrations_expose_iw59_and_iw67_adapters() -> None:
    assert Iw59ExportAdapter().skip(reason="test").to_dict()["status"] == "skipped"
    assert Iw67ExportAdapter().to_dict()["status"] == "pending_configuration"
    assert "IW67" in Iw67ExportAdapter().to_dict()["reason"]
