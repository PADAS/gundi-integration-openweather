import pytest
import respx
from httpx import Response

from app.actions.client import OpenWeatherClient


@pytest.mark.asyncio
@respx.mock
async def test_fetch_current_weather_success():
    client = OpenWeatherClient(api_key="secret", units="metric", lang="en")

    respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        return_value=Response(200, json={"dt": 1700000000, "name": "X"})
    )

    data = await client.fetch_current_weather(lat=1.23, lon=4.56)
    assert data["dt"] == 1700000000


@pytest.mark.asyncio
@respx.mock
async def test_fetch_current_weather_retry_then_success():
    client = OpenWeatherClient(api_key="secret")

    respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        side_effect=[
            Response(500, json={"error": "server"}),
            Response(200, json={"dt": 1700000001}),
        ]
    )

    data = await client.fetch_current_weather(lat=1.0, lon=2.0)
    assert data["dt"] == 1700000001