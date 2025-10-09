import datetime
import logging
from typing import Optional

import httpx
import stamina


logger = logging.getLogger(__name__)


class OpenWeatherClient:
    def __init__(self, *, api_key: str, units: str = "metric", lang: str = "en"):
        self.api_key = api_key
        self.units = units
        self.lang = lang

    async def fetch_current_weather(
        self,
        *,
        lat: float,
        lon: float,
        client: Optional[httpx.AsyncClient] = None,
    ) -> dict:
        """Fetch current weather for the given lat/lon using OpenWeather current weather API.

        Retries up to 3 times with exponential backoff when httpx raises an HTTPError.
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": self.units,
            "lang": self.lang,
        }

        async def _do_request(session: httpx.AsyncClient) -> dict:
            response = await session.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
            )
            response.raise_for_status()
            return response.json()

        async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=datetime.timedelta(seconds=2),
            wait_max=datetime.timedelta(seconds=8),
        ):
            with attempt:
                if client is not None:
                    return await _do_request(client)
                async with httpx.AsyncClient(timeout=60) as session:
                    return await _do_request(session)