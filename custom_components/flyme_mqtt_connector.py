"""Flyme MQTT connector."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any, Callable

import paho.mqtt.client as mqtt

from .const import FLYME_MQTT_USER_NAME

_LOGGER = logging.getLogger(__name__)


class FlymeMqttConnector:
    """MQTT client wrapper."""

    def __init__(
        self,
        hass,
        home_id: str,
        client_id: str,
        token: str,
        broker_host: str,
        broker_port: int,
    ) -> None:
        self.hass = hass
        self.home_id = home_id
        self.client_id = client_id
        self.is_connected = False
        self._loop_started = False
        self._connect_in_progress = False
        self._message_handler: Callable[[Any], Any] | None = None
        self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.username_pw_set(FLYME_MQTT_USER_NAME, token)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self._host = broker_host
        self._port = broker_port
        self._publish_lock = threading.Lock()

    def set_message_handler(self, handler: Callable[[Any], Any]) -> None:
        """Set mqtt message callback."""
        self._message_handler = handler

    def _on_connect(self, client, userdata, flags, rc) -> None:
        self._connect_in_progress = False
        self.is_connected = rc == 0
        if rc != 0:
            _LOGGER.error("Flyme MQTT connect failed: %s", rc)
        else:
            client.subscribe("flyme/to/" + self.client_id + "/command/#")
            _LOGGER.info("Flyme MQTT connected, home_id=%s", self.home_id)

    def _on_disconnect(self, client, userdata, rc) -> None:
        self.is_connected = False
        _LOGGER.warning("Flyme MQTT disconnected, rc=%s, home_id=%s", rc, self.home_id)

    def _on_message(self, client, userdata, msg) -> None:
        _LOGGER.info(
            "get mqtt message : topic=%s payload=%s",
            msg.topic,
            msg.payload.decode(),
        )
        if self._message_handler:
            self.hass.add_job(self._message_handler, msg)

    def start(self) -> None:
        """Start mqtt loop."""
        if self.is_connected or self._connect_in_progress:
            _LOGGER.debug(
                "Skip mqtt start: connected=%s connecting=%s",
                self.is_connected,
                self._connect_in_progress,
            )
            return
        self._connect_in_progress = True
        self.client.connect(self._host, self._port, 30)
        if not self._loop_started:
            self.client.loop_start()
            self._loop_started = True

    def publish(self, topic: str, payload: dict[str, Any]) -> bool:
        """Publish one message."""
        _LOGGER.info("publish mqtt message: topic=%s payload=%s", topic, payload)
        if not self.is_connected:
            return False
        with self._publish_lock:
            result = self.client.publish(
                topic,
                json.dumps(
                    payload,
                    default=lambda obj: (
                        obj.isoformat() if isinstance(obj, datetime) else None
                    ),
                ),
                qos=1,
            )
        return result.rc == mqtt.MQTT_ERR_SUCCESS

    def disconnect(self) -> None:
        """Stop mqtt loop."""
        self._connect_in_progress = False
        if self._loop_started:
            self.client.loop_stop()
            self._loop_started = False
        self.client.disconnect()
        self.is_connected = False
