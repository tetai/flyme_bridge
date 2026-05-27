"""Build payload for Flyme MQTT bridge."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.helpers import entity_registry as er


class EntityInfoBuilder:
    """Builds entity/device payloads."""

    def __init__(self, hass) -> None:
        self.hass = hass
        self.entity_registry = er.async_get(hass)

    @staticmethod
    def _ts_ms(value: datetime | None) -> int | None:
        if not value:
            return None
        return int(value.timestamp() * 1000)

    def build_entity_info(self, entity_id: str, new_state, old_state=None) -> dict[str, Any]:
        """Build one entity payload."""
        entry = self.entity_registry.async_get(entity_id)
        return {
            "entity_id": entity_id,
            "name": entry.name if entry else None,
            "domain": entity_id.split(".", 1)[0],
            "state": new_state.state,
            "attributes": dict(new_state.attributes),
            "icon": entry.icon if entry else None,
            "device_class": entry.original_device_class if entry else None,
            "platform": entry.platform if entry else None,
            "old_state": old_state.state if old_state else None,
            "last_changed": self._ts_ms(new_state.last_changed),
            "changed": old_state is not None,
        }

    @staticmethod
    def build_ha_pkg(
        rpq: str, msg_type: str, data: dict[str, Any], callback_id: str | None = None
    ) -> dict[str, Any]:
        """Build standard package for MQTT."""
        return {"rpq": rpq, "type": msg_type, "data": data, "callbackId": callback_id}
