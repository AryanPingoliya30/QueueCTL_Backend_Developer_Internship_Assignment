from __future__ import annotations

from typing import Dict

from .storage import Storage


class ConfigService:
    def __init__(self, storage: Storage | None = None):
        self.storage = storage or Storage()

    def list(self) -> Dict[str, str]:
        return self.storage.list_config()

    def get(self, key: str) -> str:
        return self.storage.get_config(key)

    def set(self, key: str, value: str) -> None:
        self.storage.set_config(key, value)


__all__ = ["ConfigService"]

