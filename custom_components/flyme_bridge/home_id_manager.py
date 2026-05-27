"""Manage stable Flyme home id."""

from __future__ import annotations

import random
import string

from homeassistant.helpers.storage import Store

from .const import DEFAULT_HOME_ID_PREFIX, STORAGE_KEY, STORAGE_VERSION


class HomeIdManager:
    """Generate and persist Flyme home id."""

    def __init__(self, hass) -> None:
        self.hass = hass
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._data: dict[str, str] = {}

    @staticmethod
    def _generate_home_id() -> str:
        random_chars = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=20)
        )
        return f"{DEFAULT_HOME_ID_PREFIX}{random_chars}"

    async def get_home_id(self) -> str:
        """Get stored home id, or create one."""
        if not self._data:
            loaded = await self._store.async_load()
            if loaded:
                self._data = loaded

        home_id = self._data.get("homeId")
        if home_id:
            return home_id

        home_id = self._generate_home_id()
        self._data["homeId"] = home_id
        await self._store.async_save(self._data)
        return home_id
