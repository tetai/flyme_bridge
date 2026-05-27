"""Flyme SmartLife API client."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any

import aiohttp
import async_timeout
from homeassistant.helpers import aiohttp_client

from .const import FLYME_SMART_LIFE_BASE_URL, PLUGIN_VERSION


@dataclass
class ApiResponse:
    """API response."""

    success: bool
    data: dict[str, Any] | str | None = None
    message: str | None = None
    code: int | None = None


class SmartLifeClient:
    """HTTP client for bind code APIs."""

    def __init__(self, hass, timeout: int = 30) -> None:
        self.hass = hass
        self.base_url = FLYME_SMART_LIFE_BASE_URL
        self.timeout = timeout
        self._session = aiohttp_client.async_get_clientsession(hass)

    @staticmethod
    def _timestamp_ms() -> str:
        return str(int(time.time() * 1000))

    @staticmethod
    def _req_id() -> str:
        return str(uuid.uuid4())

    def _headers(self) -> dict[str, str]:
        return {
            "reqId": self._req_id(),
            "timestamp": self._timestamp_ms(),
            "pluginVer": PLUGIN_VERSION,
            "Content-Type": "application/json; charset=utf-8",
            "Host": "iot-ha.meizu.com",
        }

    async def _post(self, endpoint: str, payload: dict[str, Any]) -> ApiResponse:
        url = f"{self.base_url}{endpoint}"
        try:
            async with async_timeout.timeout(self.timeout):
                async with self._session.post(
                    url, json=payload, headers=self._headers()
                ) as resp:
                    body = await resp.json(content_type=None)
                    ok = body.get("code", 0) in (0, 200)
                    return ApiResponse(
                        success=ok,
                        data=body.get("data"),
                        message=body.get("msg"),
                        code=body.get("code"),
                    )
        except asyncio.TimeoutError:
            return ApiResponse(
                success=False, message=f"Request timeout after {self.timeout}s"
            )
        except (aiohttp.ClientError, ValueError) as err:
            return ApiResponse(success=False, message=str(err))

    async def bind_code(self, home_id: str) -> ApiResponse:
        """Request QR bind code."""
        return await self._post("/api/ha/v1/bindCode", {"haHomeId": home_id})

    async def bind_ret(self, home_id: str, bind_code: str) -> ApiResponse:
        """Poll bind result."""
        return await self._post(
            "/api/ha/v1/bindRet", {"haHomeId": home_id, "bindCode": bind_code}
        )
