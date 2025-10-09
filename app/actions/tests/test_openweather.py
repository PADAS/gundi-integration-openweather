from unittest.mock import AsyncMock, patch, MagicMock
import pytest
from app.actions.openweather_client import OpenWeatherClient
from app.actions.openweather_transformer import transform_openweather_data, generate_source_id
from datetime import datetime
import httpx


@pytest.fixture
def sample_openweather_data():
    return {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "base": "stations",
        "main": {"temp": 16.88, "feels_like": 16.92, "temp_min": 16.88, "temp_max": 16.88, "pressure": 1016, "humidity": 88, "sea_level": 1016, "grnd_level": 841},
        "visibility": 10000,
        "wind": {"speed": 0, "deg": 0},
        "rain": {"1h": 0.91},
        "clouds": {"all": 100},
        "dt": 1760015910,
        "sys": {"type": 1, "id": 7128, "country": "MX", "sunrise": 1760013996, "sunset": 1760056454},
        "timezone": -21600,
        "id": 4005539,
        "name": "Guadalajara",
        "cod": 200
    }


@pytest.mark.asyncio
async def test_openweather_client_get_weather_data_success(sample_openweather_data):
    api_key = "test_api_key"
    client = OpenWeatherClient(api_key=api_key)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        # Ensure json() returns a direct value, not a coroutine
        mock_response.json = MagicMock(return_value=sample_openweather_data)
        mock_response.raise_for_status.return_value = None
        mock_get.return_value.__aenter__.return_value = mock_response

        lat, lon = 20.6597, -103.3496
        data = await client.get_weather_data(lat, lon)

        assert data == sample_openweather_data
        mock_get.assert_called_once_with(
            client.BASE_URL,
            params={
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "metric",
            },
        )


@pytest.mark.asyncio
async def test_openweather_client_get_weather_data_http_error():
    api_key = "test_api_key"
    client = OpenWeatherClient(api_key=api_key)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json = MagicMock(return_value={})
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=httpx.Request("GET", client.BASE_URL), response=mock_response
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        lat, lon = 20.6597, -103.3496
        data = await client.get_weather_data(lat, lon)

        assert data is None


@pytest.mark.asyncio
async def test_openweather_client_get_weather_data_request_error():
    api_key = "test_api_key"
    client = OpenWeatherClient(api_key=api_key)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_response = MagicMock()
        mock_response.json = MagicMock(return_value={})
        mock_get.return_value.__aenter__.side_effect = httpx.RequestError("Network error", request=httpx.Request("GET", client.BASE_URL))

        lat, lon = 20.6597, -103.3496
        data = await client.get_weather_data(lat, lon)

        assert data is None

def test_generate_source_id():
    lat, lon = 20.6597, -103.3496
    expected_id = generate_source_id(lat, lon)
    assert isinstance(expected_id, str)
    assert len(expected_id) == 32  # MD5 hash is 32 characters long

def test_transform_openweather_data_success(sample_openweather_data):
    location_name = "Guadalajara"
    lat, lon = 20.6597, -103.3496

    observation = transform_openweather_data(sample_openweather_data, location_name, lat, lon)

    assert observation is not None
    assert observation["type"] == "stationary-object"
    assert observation["subtype"] == "weather_station"
    assert observation["source_name"] == location_name
    assert observation["source"] == generate_source_id(lat, lon)
    assert observation["location"] == {"latitude": lat, "longitude": lon}
    assert observation["recorded_at"] == datetime.fromtimestamp(sample_openweather_data["dt"]).isoformat() + "Z"
    assert observation["temperature"] == 16.88
    assert observation["humidity"] == 88
    assert observation["pressure"] == 1016
    assert observation["wind_speed"] == 0
    assert observation["wind_direction"] == 0
    assert observation["weather_condition"] == "light rain"
    
    # Check additional fields
    additional = observation["additional"]
    assert additional["rain_1h"] == 0.91
    assert additional["clouds_all"] == 100
    assert additional["main_feels_like"] == 16.92
    assert additional["main_temp_min"] == 16.88
    assert additional["main_temp_max"] == 16.88
    assert additional["main_sea_level"] == 1016
    assert additional["main_grnd_level"] == 841
    assert additional["sunrise"] == datetime.fromtimestamp(sample_openweather_data["sys"]["sunrise"]).isoformat() + "Z"
    assert additional["sunset"] == datetime.fromtimestamp(sample_openweather_data["sys"]["sunset"]).isoformat() + "Z"
    assert "base" not in additional
    assert "visibility" not in additional # This is not explicitly added to additional
    assert "timezone" not in additional
    assert "id" not in additional
    assert "name" not in additional
    assert "cod" not in additional

def test_transform_openweather_data_empty_data():
    location_name = "Guadalajara"
    lat, lon = 20.6597, -103.3496
    observation = transform_openweather_data({}, location_name, lat, lon)
    assert observation is None
