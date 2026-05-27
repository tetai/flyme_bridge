"""Config flow for Flyme Bridge integration."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import time
from typing import Any

import httpx
import qrcode
import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_NAME
from homeassistant.helpers.selector import BooleanSelector


from .const import (
    CONF_BIND_CODE,
    CONF_BOOT_UP_REASON,
    CONF_DEVICE_NAME,
    CONF_HOME_ID,
    CONF_IMPORTANT_NOTES,
    CONF_MQTT_CLIENT_ID,
    CONF_MQTT_CONFIG,
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_TOKEN,
    DEFAULT_NAME,
    DEFAULT_TIMEOUT,
    DOMAIN,
    FLYME_NOTE_URL,
)
from . import async_create_and_start_manager
from .home_id_manager import HomeIdManager
from .smart_life_client import SmartLifeClient

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_NAME, default=DEFAULT_NAME): str,
        vol.Required(CONF_IMPORTANT_NOTES, default=False): BooleanSelector(),
    }
)




class FlymeBridgeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Main config flow."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            if not user_input.get(CONF_IMPORTANT_NOTES):
                errors["base"] = "important_notes_not_agree"
            else:
                    home_id = await HomeIdManager(self.hass).get_home_id()
                    await self.async_set_unique_id(home_id)
                    self._abort_if_unique_id_configured()
                    data = dict(user_input)
                    data[CONF_HOME_ID] = home_id
                    data[CONF_BOOT_UP_REASON] = "home_assistant"
                    return self.async_create_entry(
                        title=user_input[CONF_NAME], data=data
                    )
        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
            description_placeholders={"note_url": FLYME_NOTE_URL},
        )

    @staticmethod
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return FlymeBridgeOptionsFlow(config_entry)


class FlymeBridgeOptionsFlow(OptionsFlow):
    """Options flow for Flyme binding management."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        self._config_entry = config_entry
        self._smart_life_client: SmartLifeClient | None = None
        self._qrcode_base64: str | None = None
        self._bind_code: str | None = None
        self._bridge_data: dict[str, Any] = {}
        self._wait_task: asyncio.Task | None = None
        self._delay_seconds = 300
        self._qr_code_valid = False

    @staticmethod
    def _generate_qrcode_base64(qr_code: str) -> str:
        qr = qrcode.QRCode(
            version=5,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(qr_code)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if self._config_entry.data.get(CONF_DEVICE_NAME):
            return self.async_show_menu(
                step_id="init", menu_options={"reconfigure": "重新配置MQTT"}
            )
        return await self.async_step_qrcode_process()

    async def async_step_qrcode_process(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if self._smart_life_client is None:
            self._smart_life_client = SmartLifeClient(self.hass)
        home_id = self._config_entry.data[CONF_HOME_ID]
        response = await self._smart_life_client.bind_code(home_id)
        if not response.success or not isinstance(response.data, str):
            return self.async_show_form(
                step_id="qrcode_process",
                data_schema=vol.Schema({}),
                errors={"base": "network_error"},
            )
        bind_uri = response.data
        bind_payload = json.loads(bind_uri[9:])
        self._bind_code = bind_payload.get(CONF_BIND_CODE)
        self._qrcode_base64 = self._generate_qrcode_base64(bind_uri)
        return await self.async_step_qrcode_show()

    async def _wait_for_scanned(self) -> None:
        start = time.time()
        home_id = self._config_entry.data[CONF_HOME_ID]
        self._qr_code_valid = False
        while time.time() - start < self._delay_seconds:
            await asyncio.sleep(3)
            result = await self._smart_life_client.bind_ret(home_id, self._bind_code)
            if result.code == 600407:
                self._qr_code_valid = True
                break  # 600407 means QR code is invalid or expired, no need to continue polling
            if not result.success or not isinstance(result.data, dict):
                continue
            mqtt_config = {
                CONF_MQTT_HOST: result.data.get(CONF_MQTT_HOST),
                CONF_MQTT_PORT: result.data.get(CONF_MQTT_PORT),
                CONF_MQTT_CLIENT_ID: result.data.get(CONF_MQTT_CLIENT_ID),
                CONF_MQTT_TOKEN: result.data.get(CONF_MQTT_TOKEN),
            }
            self._bridge_data = {
                CONF_MQTT_CONFIG: mqtt_config,
                CONF_DEVICE_NAME: home_id,
                CONF_BOOT_UP_REASON: "home_assistant",
            }
            break
        return 0

    async def async_step_qrcode_show(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if self._wait_task is None:
            self._wait_task = self.hass.async_create_task(self._wait_for_scanned())
        if self._wait_task.done():
            return self.async_show_progress_done(next_step_id="qrcode_scanned")
        return self.async_show_progress(
            step_id="qrcode_show",
            progress_action="wait_for_scanned",
            progress_task=self._wait_task,
            description_placeholders={
                "img_base64": self._qrcode_base64 or "",
                "time_out": self._delay_seconds,
                "time_unit": "秒",
            },
        )

    async def async_step_qrcode_scanned(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if self._qr_code_valid is True:
            return self.async_abort(reason="QR code is invalid or expired")
        if not self._bridge_data:
            return self.async_show_form(
                step_id="qrcode_scanned",
                data_schema=vol.Schema({}),
                errors={"base": "network_error"},
            )
        updated_data = {**self._config_entry.data, **self._bridge_data}
        self.hass.config_entries.async_update_entry(
            self._config_entry, data=updated_data
        )
        await async_create_and_start_manager(self.hass, self._config_entry)
        return self.async_abort(reason="Flyme MQTT 绑定完成")

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        manager = self.hass.data.get(DOMAIN, {}).get("mqtt_manager")
        if manager:
            await manager.stop()
            self.hass.data[DOMAIN]["mqtt_manager"] = None
        new_data = {**self._config_entry.data}
        new_data.pop(CONF_MQTT_CONFIG, None)
        new_data.pop(CONF_DEVICE_NAME, None)
        self.hass.config_entries.async_update_entry(self._config_entry, data=new_data)
        return await self.async_step_qrcode_process()
