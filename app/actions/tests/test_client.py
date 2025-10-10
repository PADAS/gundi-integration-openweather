import pytest
import httpx

from pydantic import ValidationError
from app.actions.client import get_weather_data_for_location, generate_location_source_id


@pytest.mark.asyncio
async def test_generate_location_source_id():
    """Test that source ID generation is deterministic and unique for different coordinates."""
    lat, lon = 20.659698, -103.349609
    source_id1 = generate_location_source_id(lat, lon)
    source_id2 = generate_location_source_id(lat, lon)

    # Should be the same for same coordinates
    assert source_id1 == source_id2
    assert isinstance(source_id1, str)
    assert len(source_id1) == 32  # MD5 hash length

    # Should be different for different coordinates
    different_source_id = generate_location_source_id(lat + 1, lon + 1)
    assert source_id1 != different_source_id


@pytest.mark.asyncio
async def test_get_weather_data_for_location_success(mocker):
    """Test successful weather data retrieval."""
    mock_response_data = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "base": "stations",
        "main": {
            "temp": 16.88,
            "feels_like": 16.92,
            "temp_min": 16.88,
            "temp_max": 16.88,
            "pressure": 1016,
            "humidity": 88,
            "sea_level": 1016,
            "grnd_level": 841
        },
        "visibility": 10000,
        "wind": {"speed": 0, "deg": 0},
        "rain": {"1h": 0.91},
        "clouds": {"all": 100},
        "dt": 1760015910,
        "sys": {
            "type": 1,
            "id": 7128,
            "country": "MX",
            "sunrise": 1760013996,
            "sunset": 1760056454
        },
        "timezone": -21600,
        "id": 4005539,
        "name": "Guadalajara",
        "cod": 200
    }

    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(200, json=mock_response_data)
        response._request = request
        return response

    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)

    result = await get_weather_data_for_location(
        lat=20.659698,
        lon=-103.349609,
        api_key="test_key",
        base_url="https://api.openweathermap.org/data/2.5/weather"
    )

    assert result.coord.lat == 20.6597
    assert result.coord.lon == -103.3496
    assert result.weather[0].main == "Rain"
    assert result.main.temp == 16.88
    assert result.name == "Guadalajara"


@pytest.mark.asyncio
async def test_get_weather_data_for_location_unauthorized(mocker):
    """Test handling of unauthorized API key."""
    mocker.patch('httpx.AsyncClient.get', side_effect=httpx.HTTPStatusError(
        "Unauthorized",
        request=mocker.Mock(),
        response=mocker.Mock(status_code=401)
    ))

    with pytest.raises(httpx.HTTPStatusError):
        await get_weather_data_for_location(
            lat=20.659698,
            lon=-103.349609,
            api_key="invalid_key"
        )


@pytest.mark.asyncio
async def test_get_weather_data_for_location_server_error(mocker):
    """Test handling of server errors."""
    mocker.patch('httpx.AsyncClient.get', side_effect=httpx.HTTPStatusError(
        "Server Error",
        request=mocker.Mock(),
        response=mocker.Mock(status_code=500)
    ))

    with pytest.raises(httpx.HTTPStatusError):
        await get_weather_data_for_location(
            lat=20.659698,
            lon=-103.349609,
            api_key="test_key"
        )


@pytest.mark.asyncio
async def test_get_weather_data_for_location_network_error(mocker):
    """Test handling of network errors."""
    mocker.patch('httpx.AsyncClient.get', side_effect=httpx.ConnectError("Connection failed"))

    with pytest.raises(httpx.ConnectError):
        await get_weather_data_for_location(
            lat=20.659698,
            lon=-103.349609,
            api_key="test_key"
        )


@pytest.mark.asyncio
async def test_get_weather_data_for_location_invalid_response(mocker):
    """Test handling of invalid response format."""
    mock_invalid_response = {"invalid": "data"}

    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(200, json=mock_invalid_response)
        response._request = request
        return response

    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)

    with pytest.raises(ValidationError):
        await get_weather_data_for_location(
            lat=20.659698,
            lon=-103.349609,
            api_key="test_key"
        )