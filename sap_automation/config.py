from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .contracts import SUPPORTED_IW69_OBJECTS


def load_export_config(config_path: Path) -> dict[str, Any]:
    resolved = config_path.expanduser().resolve()
    return json.loads(resolved.read_text(encoding="utf-8"))


def _normalize_coordinator(value: str | None) -> str:
    token = str(value or "").strip().upper()
    return token or "IGOR"


def _merge_object_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for object_code, object_config in override.items():
        merged[str(object_code).strip().upper()] = object_config
    return merged


def _resolve_coordinator_profile(
    *,
    coordinators: dict[str, Any],
    coordinator: str,
    root_objects: dict[str, Any],
    stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    profile = coordinators.get(coordinator)
    if not isinstance(profile, dict):
        raise ValueError(f"Missing IW69 coordinator profile: {coordinator}")

    inherited_objects = dict(root_objects)
    raw_inherits = str(profile.get("inherits", "")).strip()
    inherits_from = _normalize_coordinator(raw_inherits) if raw_inherits else ""
    if inherits_from:
        if inherits_from == coordinator:
            raise ValueError(f"IW69 coordinator profile {coordinator} cannot inherit from itself.")
        if inherits_from in stack:
            chain = " -> ".join(stack + (coordinator, inherits_from))
            raise ValueError(f"Circular IW69 coordinator inheritance detected: {chain}")
        inherited_profile = _resolve_coordinator_profile(
            coordinators=coordinators,
            coordinator=inherits_from,
            root_objects=root_objects,
            stack=stack + (coordinator,),
        )
        inherited_objects = _merge_object_configs(
            inherited_objects,
            inherited_profile.get("objects", {}),
        )

    return {
        "objects": _merge_object_configs(
            inherited_objects,
            profile.get("objects", {}),
        )
    }


def resolve_iw69_profile(config: dict[str, Any], coordinator: str | None = None) -> dict[str, Any]:
    root_objects = config.get("objects", {})
    iw69_cfg = config.get("iw69", {})
    if not isinstance(iw69_cfg, dict) or not isinstance(iw69_cfg.get("coordinators"), dict):
        return {
            "coordinator": _normalize_coordinator(coordinator),
            "objects": root_objects,
        }

    coordinators = iw69_cfg.get("coordinators", {})
    resolved_coordinator = _normalize_coordinator(
        coordinator or iw69_cfg.get("default_coordinator")
    )
    profile = _resolve_coordinator_profile(
        coordinators=coordinators,
        coordinator=resolved_coordinator,
        root_objects=root_objects,
    )
    return {
        "coordinator": resolved_coordinator,
        "objects": profile.get("objects", {}),
    }


def resolve_iw69_object_config(
    *,
    config: dict[str, Any],
    object_code: str,
    coordinator: str | None = None,
) -> dict[str, Any]:
    profile = resolve_iw69_profile(config=config, coordinator=coordinator)
    objects = profile.get("objects", {})
    normalized_object_code = str(object_code).strip().upper()
    object_config = objects.get(normalized_object_code)
    if not isinstance(object_config, dict):
        raise ValueError(
            f"Missing IW69 object configuration for object={normalized_object_code} "
            f"coordinator={profile['coordinator']}."
        )
    return object_config


def validate_iw69_objects(config: dict[str, Any]) -> None:
    profile = resolve_iw69_profile(config=config)
    missing = [
        object_code
        for object_code in SUPPORTED_IW69_OBJECTS
        if object_code not in profile.get("objects", {})
    ]
    if missing:
        raise ValueError("Missing IW69 object configuration for: " + ", ".join(sorted(missing)))

    iw69_cfg = config.get("iw69", {})
    coordinators = iw69_cfg.get("coordinators", {}) if isinstance(iw69_cfg, dict) else {}
    for coordinator in coordinators:
        resolved_profile = resolve_iw69_profile(config=config, coordinator=coordinator)
        missing_for_coordinator = [
            object_code
            for object_code in SUPPORTED_IW69_OBJECTS
            if object_code not in resolved_profile.get("objects", {})
        ]
        if missing_for_coordinator:
            raise ValueError(
                "Missing IW69 object configuration for coordinator="
                f"{coordinator}: {', '.join(sorted(missing_for_coordinator))}"
            )
