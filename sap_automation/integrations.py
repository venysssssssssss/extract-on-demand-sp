from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


@dataclass(frozen=True)
class Iw59ExportAdapter:
    status: str = "pending_configuration"
    reason: str = "IW59 SAP GUI script not provided yet."

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class MopSourceAdapter(Protocol):
    def fetch(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class DisabledMopSourceAdapter:
    status: str = "pending_configuration"
    reason: str = "MOP/fora MOP source adapter is declared but not implemented."

    def fetch(self) -> dict[str, str]:
        return asdict(self)
