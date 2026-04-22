from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .contracts import SUPPORTED_IW69_OBJECTS


def load_export_config(config_path: Path) -> dict[str, Any]:
    resolved = config_path.expanduser().resolve()
    return json.loads(resolved.read_text(encoding="utf-8"))


def _normalize_demandante(value: str | None) -> str:
    token = str(value or "").strip().upper()
    return token or "IGOR"


def _merge_object_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for object_code, object_config in override.items():
        merged[str(object_code).strip().upper()] = object_config
    return merged


def _merge_profile_settings(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in {"inherits", "objects"}:
            continue
        merged[str(key)] = value
    return merged


def _resolve_demandante_profile(
    *,
    demandantes: dict[str, Any],
    demandante: str,
    root_objects: dict[str, Any],
    stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    profile = demandantes.get(demandante)
    if not isinstance(profile, dict):
        raise ValueError(f"Missing IW69 demandante profile: {demandante}")

    inherited_objects = dict(root_objects)
    inherited_settings: dict[str, Any] = {}
    raw_inherits = str(profile.get("inherits", "")).strip()
    inherits_from = _normalize_demandante(raw_inherits) if raw_inherits else ""
    if inherits_from:
        if inherits_from == demandante:
            raise ValueError(f"IW69 demandante profile {demandante} cannot inherit from itself.")
        if inherits_from in stack:
            chain = " -> ".join(stack + (demandante, inherits_from))
            raise ValueError(f"Circular IW69 demandante inheritance detected: {chain}")
        inherited_profile = _resolve_demandante_profile(
            demandantes=demandantes,
            demandante=inherits_from,
            root_objects=root_objects,
            stack=stack + (demandante,),
        )
        inherited_objects = _merge_object_configs(
            inherited_objects,
            inherited_profile.get("objects", {}),
        )
        inherited_settings = _merge_profile_settings(
            inherited_settings,
            inherited_profile,
        )

    return _merge_profile_settings(
        {
            **inherited_settings,
            "objects": _merge_object_configs(
                inherited_objects,
                profile.get("objects", {}),
            ),
        },
        profile,
    ) | {
        "objects": _merge_object_configs(
            inherited_objects,
            profile.get("objects", {}),
        )
    }


def resolve_iw69_profile(config: dict[str, Any], demandante: str | None = None) -> dict[str, Any]:
    root_objects = config.get("objects", {})
    iw69_cfg = config.get("iw69", {})
    if not isinstance(iw69_cfg, dict) or not isinstance(iw69_cfg.get("demandantes"), dict):
        return {
            "demandante": _normalize_demandante(demandante),
            "objects": root_objects,
        }

    demandantes = iw69_cfg.get("demandantes", {})
    resolved_demandante = _normalize_demandante(
        demandante or iw69_cfg.get("default_demandante")
    )
    profile = _resolve_demandante_profile(
        demandantes=demandantes,
        demandante=resolved_demandante,
        root_objects=root_objects,
    )
    return {"demandante": resolved_demandante, **profile}


def resolve_iw69_object_config(
    *,
    config: dict[str, Any],
    object_code: str,
    demandante: str | None = None,
) -> dict[str, Any]:
    profile = resolve_iw69_profile(config=config, demandante=demandante)
    objects = profile.get("objects", {})
    normalized_object_code = str(object_code).strip().upper()
    object_config = objects.get(normalized_object_code)
    if not isinstance(object_config, dict):
        raise ValueError(
            f"Missing IW69 object configuration for object={normalized_object_code} "
            f"demandante={profile['demandante']}."
        )
    return object_config


def resolve_sm_config(config: dict[str, Any], demandante: str | None = None) -> dict[str, Any]:
    sm_cfg = config.get("sm", {})
    if not isinstance(sm_cfg, dict):
        raise ValueError("Missing 'sm' section in configuration.")
    demandantes = sm_cfg.get("demandantes", {})
    resolved_demandante = str(demandante or sm_cfg.get("default_demandante") or "SALA_MERCADO").strip().upper()
    profile = demandantes.get(resolved_demandante)
    if not isinstance(profile, dict):
        raise ValueError(f"Missing SM demandante profile: {resolved_demandante}")
    return {
        "demandante": resolved_demandante,
        "transaction_code": sm_cfg.get("transaction_code", "ZISU0128"),
        "chunk_size": sm_cfg.get("chunk_size", 5000),
        **profile,
    }


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
    demandantes = iw69_cfg.get("demandantes", {}) if isinstance(iw69_cfg, dict) else {}
    for demandante in demandantes:
        resolved_profile = resolve_iw69_profile(config=config, demandante=demandante)
        missing_for_demandante = [
            object_code
            for object_code in SUPPORTED_IW69_OBJECTS
            if object_code not in resolved_profile.get("objects", {})
        ]
        if missing_for_demandante:
            raise ValueError(
                "Missing IW69 object configuration for demandante="
                f"{demandante}: {', '.join(sorted(missing_for_demandante))}"
            )
