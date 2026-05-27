"""Constants for the Flyme Bridge integration."""

from typing import Final

DOMAIN = "flyme_bridge"

# Shared config
CONF_NAME = "name"
CONF_IMPORTANT_NOTES = "important_notes"

# Flyme config
CONF_HOME_ID = "home_id"
CONF_BOOT_UP_REASON = "boot_up_reason"
CONF_DEVICE_NAME = "device_name"
CONF_MQTT_CONFIG = "mqtt_config"
CONF_BIND_CODE = "bindCode"
CONF_MQTT_HOST = "mqttHost"
CONF_MQTT_PORT = "mqttPort"
CONF_MQTT_CLIENT_ID = "haClientId"
CONF_MQTT_TOKEN = "mqttToken"

DEFAULT_NAME: Final = "Flyme Bridge"
DEFAULT_TIMEOUT: Final = 30
DEFAULT_HOME_ID_PREFIX: Final = "HOM:"
FLYME_NOTE_URL: Final = "http://iot.vivo.com.cn/h5/223/"
FLYME_MQTT_USER_NAME: Final = "pluginVer1.3"
FLYME_SMART_LIFE_BASE_URL: Final = "http://10.148.6.128/iot-ha"


STORAGE_KEY: Final = DOMAIN
STORAGE_VERSION: Final = 1

STATELESS_LLM_API: Final = "stateless_assist"

PLUGIN_VERSION = "1.0"
