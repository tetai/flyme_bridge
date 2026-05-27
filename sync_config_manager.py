"""Sync config manager for Flyme MQTT bridge."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SyncConfigManager:
    """Stores device/entity sync mapping."""

    def __init__(self, hass) -> None:
        self.hass = hass
        self.config_path = Path(hass.config.path(".flyme_bridge_sync_config.json"))
        self.device_sync_config: dict[str, set[str]] = {}

    async def load_config(self) -> bool:
        """Load saved sync config."""
        if not self.config_path.exists():
            self.device_sync_config = {}
            return False

        def _load() -> dict[str, Any]:
            with self.config_path.open("r", encoding="utf-8") as file:
                return json.load(file)

        data = await self.hass.async_add_executor_job(_load)
        self.device_sync_config = {
            dev_id: set(entities)
            for dev_id, entities in data.get("device_sync_config", {}).items()
        }
        return True

    async def save_config(self) -> bool:
        """Persist sync config."""
        payload = {
            "version": "1.0",
            "device_sync_config": {
                dev_id: list(entity_set)
                for dev_id, entity_set in self.device_sync_config.items()
            },
        }

        def _save() -> None:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with self.config_path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)

        await self.hass.async_add_executor_job(_save)
        return True

    def update_config(self, config_data: dict[str, Any]) -> None:
        """Update in-memory config from mqtt command."""
        devices = config_data.get("devices")
        if not isinstance(devices, list):
            raise ValueError("devices must be a list")
        updated: dict[str, set[str]] = {}
        for device in devices:
            dev_id = device.get("devId")
            entity_ids = device.get("entityIds", [])
            if not dev_id or not isinstance(entity_ids, list):
                continue
            updated.setdefault(dev_id, set()).update(entity_ids)
        self.device_sync_config = updated

    def is_entity_synced(self, entity_id: str, device_id: str | None) -> bool:
        """Check whether entity should be synced."""
        if not self.device_sync_config or not device_id:
            return False
        allowed = self.device_sync_config.get(device_id)
        if not allowed:
            return False
        if "*" in allowed:
            return True
        return entity_id in allowed

    async def clear_config(self) -> None:
        """Clear memory and storage."""
        self.device_sync_config = {}
        if self.config_path.exists():
            await self.hass.async_add_executor_job(self.config_path.unlink)
