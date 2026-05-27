"""Flyme Bridge integration for Home Assistant."""

from __future__ import annotations

import logging
from typing import TypeAlias

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv

from .const import (
    CONF_DEVICE_NAME,
    CONF_HOME_ID,
    CONF_MQTT_CONFIG,
    DOMAIN,
)
from .domain_filter import DomainFilter
from .entity_info_builder import EntityInfoBuilder
from .flyme_mqtt_manager import FlymeMqttManager

_LOGGER = logging.getLogger(__name__)

PLATFORMS = []
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

FlymeBridgeConfigEntry: TypeAlias = ConfigEntry


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up domain data."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("domain_filter", DomainFilter())
    hass.data[DOMAIN].setdefault("entity_info_builder", EntityInfoBuilder(hass))
    hass.data[DOMAIN].setdefault("mqtt_manager", None)
    return True


async def async_create_and_start_manager(
    hass: HomeAssistant, entry: FlymeBridgeConfigEntry
) -> None:
    """Create and start the MQTT manager with proper HA startup check."""
    old_manager = hass.data.get(DOMAIN, {}).get("mqtt_manager")
    if old_manager:
        await old_manager.stop()
        hass.data[DOMAIN]["mqtt_manager"] = None

    mqtt_config = entry.data.get(CONF_MQTT_CONFIG)
    if mqtt_config:
        mqtt_manager = FlymeMqttManager(
            hass=hass,
            mqtt_config=mqtt_config,
            domain_filter=hass.data[DOMAIN]["domain_filter"],
            entity_info_builder=hass.data[DOMAIN]["entity_info_builder"],
            home_id=entry.data.get(CONF_HOME_ID),
            config_key=entry.data.get(CONF_DEVICE_NAME) or entry.entry_id,
        )
        if await mqtt_manager.initialize():

            async def _delayed_start(_event) -> None:
                await mqtt_manager.start()
                hass.data[DOMAIN]["mqtt_manager"] = mqtt_manager

            if hass.is_running:
                await _delayed_start(None)
            else:
                hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _delayed_start)


async def async_setup_entry(
    hass: HomeAssistant, entry: FlymeBridgeConfigEntry
) -> bool:
    """Set up from config entry."""
    await async_create_and_start_manager(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: FlymeBridgeConfigEntry
) -> bool:
    """Unload a config entry."""
    old_manager = hass.data.get(DOMAIN, {}).get("mqtt_manager")
    if old_manager:
        await old_manager.stop()
    if DOMAIN in hass.data:
        hass.data[DOMAIN]["mqtt_manager"] = None

    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
