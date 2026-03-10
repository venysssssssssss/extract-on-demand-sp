from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .contracts import SUPPORTED_IW69_OBJECTS


def load_export_config(config_path: Path) -> dict[str, Any]:
    resolved = config_path.expanduser().resolve()
    return json.loads(resolved.read_text(encoding="utf-8"))


def validate_iw69_objects(config: dict[str, Any]) -> None:
    objects = config.get("objects", {})
    missing = [object_code for object_code in SUPPORTED_IW69_OBJECTS if object_code not in objects]
    if missing:
        raise ValueError(
            "Missing IW69 object configuration for: " + ", ".join(sorted(missing))
        )
