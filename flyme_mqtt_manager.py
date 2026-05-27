"""Flyme MQTT manager."""

from __future__ import annotations

import json
import logging
import time
import anyio
import asyncio
import aiohttp
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.helpers import llm
from homeassistant.components import conversation
import base64
from .const import (
    CONF_DEVICE_NAME,
    CONF_MQTT_CLIENT_ID,
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_TOKEN,
    DOMAIN,
    STATELESS_LLM_API,
)
from .flyme_mqtt_connector import FlymeMqttConnector
from .sync_config_manager import SyncConfigManager
from .mcpServer import create_server
from mcp import types
from mcp.shared.message import SessionMessage

_LOGGER = logging.getLogger(__name__)


class FlymeMqttManager:
    """Handle mqtt lifecycle and bridge behavior."""

    def __init__(
        self,
        hass,
        mqtt_config,
        domain_filter,
        entity_info_builder,
        home_id: str,
        config_key: str,
    ) -> None:
        self.hass = hass
        self.mqtt_config = mqtt_config
        self.domain_filter = domain_filter
        self.entity_info_builder = entity_info_builder
        self.home_id = home_id
        self.config_key = config_key
        self.connector: FlymeMqttConnector | None = None
        self.state_listener = None
        self.is_connected = False
        self.sync_config_manager = SyncConfigManager(hass)
        self.llm_context = llm.LLMContext(
            platform=DOMAIN,
            context={},
            language="*",
            assistant=conversation.DOMAIN,
            device_id=None,
        )
        self.server = None
        self.options = None
        self.read_stream_writer = None
        self.read_stream = None
        self.write_stream_writer = None
        self.write_stream = None
        self.message_data = None
        self._message_contexts: dict[str, dict] = {} 
        self._message_contexts_max_size = 50 
        self._message_contexts_expire_seconds = 300  # 5 minutes

    async def initialize(self) -> bool:
        """Initialize connector."""
        self.connector = FlymeMqttConnector(
            hass=self.hass,
            home_id=self.home_id,
            client_id=self.mqtt_config[CONF_MQTT_CLIENT_ID],
            token=self.mqtt_config[CONF_MQTT_TOKEN],
            broker_host=self.mqtt_config[CONF_MQTT_HOST],
            broker_port=self.mqtt_config[CONF_MQTT_PORT],
        )
        self.connector.set_message_handler(self._handle_mqtt_message)
        await self.sync_config_manager.load_config()
        self.server = await create_server(
            self.hass, STATELESS_LLM_API, self.llm_context
        )
        self.options = await self.hass.async_add_executor_job(
            self.server.create_initialization_options
        )

        self.read_stream_writer, self.read_stream = anyio.create_memory_object_stream(32)
        self.write_stream, self.write_stream_reader = anyio.create_memory_object_stream(32)

        return True

    async def start(self) -> bool:
        """Start mqtt and monitor."""
        if not self.connector:
            return False
        _LOGGER.info("Starting Flyme MQTT manager...")
        self.connector.start()
        self.hass.async_create_task(self._run_server())
        self.is_connected = True
        # self.state_listener = self.hass.bus.async_listen(
        #     EVENT_STATE_CHANGED, self._on_state_changed
        # )
        return True

    def _cleanup_message_contexts(self) -> None:
        """Clean expired or old message contexts, prevent memory leak."""
        current_time = time.time()
        original_size = len(self._message_contexts)
        removed_count = 0
        
        # clean expired contexts
        expired_keys = []
        for key, context in self._message_contexts.items():
            timestamp = context.get('timestamp', 0)
            if current_time - timestamp > self._message_contexts_expire_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self._message_contexts[key]
            removed_count += 1
        
        # if still exceeds max size, clean oldest contexts
        while len(self._message_contexts) > self._message_contexts_max_size:
            # find oldest context
            oldest_key = min(
                self._message_contexts.keys(),
                key=lambda k: self._message_contexts[k].get('timestamp', 0)
            )
            del self._message_contexts[oldest_key]
            removed_count += 1
        
        if removed_count > 0:
            _LOGGER.info("Cleaned up %d expired/old message contexts (from %d to %d)", 
                        removed_count, original_size, len(self._message_contexts))

    async def _run_server(self) -> None:
        """Run MCP server."""
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.resp_writer)
            await self.server.run(
                self.read_stream, self.write_stream, self.options, stateless=True
            )

    async def stop(self) -> None:
        """Stop mqtt."""
        _LOGGER.info("stop Flyme MQTT manager")
        if self.state_listener:
            self.state_listener()
            self.state_listener = None
        if self.read_stream_writer:
            await self.read_stream_writer.aclose()
            self.read_stream_writer = None
        if self.write_stream:
            await self.write_stream.aclose()
            self.write_stream = None
        if self.connector:
            self.connector.disconnect()
            self.connector = None
        self._message_contexts.clear()
        self.is_connected = False

    async def _handle_mqtt_message(self, msg) -> None:
        payload = msg.payload.decode()
        try:
            self.message_data = json.loads(payload)
        except json.JSONDecodeError:
            return
        msg_type = self.message_data.get("type")
        callback_id = self.message_data.get("callbackId")
        data = self.message_data.get("data")
        if msg_type == "set_sync_entity":
            await self._handle_set_sync_entity(data, callback_id)
        elif msg_type == "unbind":
            await self._handle_unbind()
        elif msg_type in ["mcp_tools_list", "mcp_tools_call", "mcp_msg"]:
            try:
                _LOGGER.info("mcp reader: %s", data)
                json_data = data if isinstance(data, dict) else json.loads(data)
                message = types.JSONRPCMessage.model_validate(json_data)
                session_message = SessionMessage(message)
                
                message_id = str(message.root.id) if hasattr(message.root, 'id') and message.root.id is not None else str(id(message))
                # 清理过期或过多的上下文
                self._cleanup_message_contexts()
                # 存储消息上下文，包含时间戳
                self._message_contexts[message_id] = {
                    'callback_id': callback_id,
                    'msg_type': msg_type,
                    'timestamp': time.time()
                }
                _LOGGER.info("mcp reader: message_id=%s, callback_id=%s, msg_type=%s", message_id, callback_id, msg_type)
                await self.read_stream_writer.send(session_message)
            except Exception as err:
                _LOGGER.error("mcp Invalid message from client: %s", err)
        else:
            _LOGGER.error("未知的消息类型: %s", msg_type)

    async def _handle_set_sync_entity(self, data, callback_id: str | None) -> None:
        req = json.loads(data) if isinstance(data, str) else data
        self.sync_config_manager.update_config(req)
        await self.sync_config_manager.save_config()
        self._send_success_response(callback_id, "set_sync_entity", {"success": True})

    async def _handle_unbind(self) -> None:
        await self.sync_config_manager.clear_config()
        await self.stop()
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if entry.data.get(CONF_DEVICE_NAME) == self.config_key:
                new_data = {**entry.data}
                new_data.pop(CONF_DEVICE_NAME, None)
                new_data.pop("mqtt_config", None)
                self.hass.config_entries.async_update_entry(entry, data=new_data)
                break

    def _on_state_changed(self, event) -> None:
        if not self.is_connected:
            return
        entity_id = event.data.get("entity_id")
        new_state = event.data.get("new_state")
        old_state = event.data.get("old_state")
        if not new_state or not self.domain_filter.is_entity_included(entity_id):
            return
        entity_registry = er.async_get(self.hass)
        entity_entry = entity_registry.async_get(entity_id)
        device_id = entity_entry.device_id if entity_entry else None
        if not self.sync_config_manager.is_entity_synced(entity_id, device_id):
            return
        payload = self.entity_info_builder.build_entity_info(
            entity_id, new_state, old_state
        )
        self._send_success_response(None, "state_change", payload, rsp_type="req")

    def _send_success_response(
        self, callback_id: str | None, msg_type: str, data: dict, rsp_type: str = "rsp"
    ) -> None:
        if not self.connector:
            return
        payload = self.entity_info_builder.build_ha_pkg(
            rsp_type, msg_type, data, callback_id
        )
        self.connector.publish(self.home_id, payload)

    def _send_error_response(self, callback_id: str | None, error_message: str) -> None:
        if not callback_id or not self.connector:
            return
        payload = self.entity_info_builder.build_ha_pkg(
            "rsp", "error_report", {"error": error_message}, callback_id
        )
        self.connector.publish(self.home_id, payload)

    async def resp_writer(self):
        async for session_message in self.write_stream_reader:
            # extract actual JSONRPCMessage from SessionMessage
            actual_message = session_message.message
            json_data = actual_message.model_dump(by_alias=True, exclude_none=True)
            _LOGGER.info("actual_message: %s", actual_message.root)
            
            message_id = str(actual_message.root.id) if hasattr(actual_message.root, 'id') and actual_message.root.id is not None else str(id(actual_message))
            context = self._message_contexts.pop(message_id, None)
            
            if context:
                callbackId = context.get('callback_id')
                msg_type = context.get('msg_type')
                _LOGGER.info("resp_writer: found context for message_id=%s, callbackId=%s, msg_type=%s", message_id, callbackId, msg_type)
            else:
                callbackId = self.message_data.get("callbackId") if self.message_data else None
                msg_type = self.message_data.get("type") if self.message_data else None
                _LOGGER.warning("resp_writer: context not found for message_id=%s, using fallback callbackId=%s, msg_type=%s", message_id, callbackId, msg_type)
            
            json_data["callbackId"] = callbackId
            _LOGGER.info(json.dumps(json_data, ensure_ascii=False))
            payload = self.entity_info_builder.build_ha_pkg(
                "rsp",
                msg_type,
                {
                    "data": base64.b64encode(
                        json.dumps(json_data, ensure_ascii=False).encode("utf-8")
                    ).decode("utf-8")
                },
                callbackId,
            )
            self.connector.publish(self.home_id, payload)
