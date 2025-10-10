import pytest
import httpx
from pydantic import ValidationError
from unittest.mock import Mock

from app.actions.client import (
    fetch_weather_data,
    generate_source_id,
    get_auth_config,
    get_pull_observations_config
)
from app.actions.configurations import Location
from app.services.errors import ConfigurationNotFound


def test_generate_source_id():
    """Test that source ID generation is consistent for the same coordinates."""
    lat, lon = 20.659698, -103.349609
    source_id_1 = generate_source_id(lat, lon)
    source_id_2 = generate_source_id(lat, lon)
    
    # Same coordinates should generate same ID
    assert source_id_1 == source_id_2
    
    # Different coordinates should generate different ID
    source_id_3 = generate_source_id(20.659698, -103.349610)
    assert source_id_1 != source_id_3
    
    # Should be a valid MD5 hash (32 characters)
    assert len(source_id_1) == 32


@pytest.mark.asyncio
async def test_fetch_weather_data_success(mocker):
    """Test successful weather data fetch."""
    lat, lon = 20.659698, -103.349609
    api_key = "test_api_key"
    
    mock_response_data = {
        "coord": {"lon": lon, "lat": lat},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "main": {
            "temp": 16.88,
            "feels_like": 16.92,
            "temp_min": 16.88,
            "temp_max": 16.88,
            "pressure": 1016,
            "humidity": 88
        },
        "wind": {"speed": 0, "deg": 0},
        "clouds": {"all": 100},
        "dt": 1760015910,
        "sys": {"country": "MX", "sunrise": 1760013996, "sunset": 1760056454},
        "timezone": -21600,
        "name": "Guadalajara"
    }
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(200, json=mock_response_data)
        response._request = request
        return response
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    result = await fetch_weather_data(
        lat=lat,
        lon=lon,
        api_key=api_key,
        units="metric"
    )
    
    assert result == mock_response_data
    assert result["coord"]["lat"] == lat
    assert result["coord"]["lon"] == lon


@pytest.mark.asyncio
async def test_fetch_weather_data_unauthorized(mocker):
    """Test weather data fetch with invalid API key."""
    lat, lon = 20.659698, -103.349609
    api_key = "invalid_key"
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(401, json={"cod": 401, "message": "Invalid API key"})
        response._request = request
        # Manually raise the exception
        raise httpx.HTTPStatusError(
            "Unauthorized",
            request=request,
            response=response
        )
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_weather_data(
            lat=lat,
            lon=lon,
            api_key=api_key,
            units="metric"
        )


@pytest.mark.asyncio
async def test_fetch_weather_data_http_error(mocker):
    """Test weather data fetch with server error."""
    lat, lon = 20.659698, -103.349609
    api_key = "test_api_key"
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(500, json={"error": "Internal server error"})
        response._request = request
        raise httpx.HTTPStatusError(
            "Server Error",
            request=request,
            response=response
        )
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_weather_data(
            lat=lat,
            lon=lon,
            api_key=api_key,
            units="metric"
        )


def test_get_auth_config_success(mocker):
    """Test successful retrieval of auth config."""
    integration = mocker.Mock()
    integration.id = "test_integration_id"
    
    auth_config_data = {"api_key": "test_api_key"}
    config_mock = mocker.Mock()
    config_mock.data = auth_config_data
    config_mock.action.value = "auth"
    
    integration.configurations = [config_mock]
    
    result = get_auth_config(integration)
    
    assert result.api_key.get_secret_value() == "test_api_key"


def test_get_auth_config_not_found(mocker):
    """Test auth config retrieval when config is missing."""
    integration = mocker.Mock()
    integration.id = "test_integration_id"
    integration.configurations = []
    
    with pytest.raises(ConfigurationNotFound):
        get_auth_config(integration)


def test_get_pull_observations_config_success(mocker):
    """Test successful retrieval of pull observations config."""
    integration = mocker.Mock()
    integration.id = "test_integration_id"
    
    pull_config_data = {
        "locations": [
            {"name": "Test Location", "lat": 20.659698, "lon": -103.349609}
        ],
        "units": "metric"
    }
    config_mock = mocker.Mock()
    config_mock.data = pull_config_data
    config_mock.action.value = "pull_observations"
    
    integration.configurations = [config_mock]
    
    result = get_pull_observations_config(integration)
    
    assert len(result.locations) == 1
    assert result.locations[0].name == "Test Location"
    assert result.units == "metric"


def test_get_pull_observations_config_not_found(mocker):
    """Test pull observations config retrieval when config is missing."""
    integration = mocker.Mock()
    integration.id = "test_integration_id"
    integration.configurations = []
    
    with pytest.raises(ConfigurationNotFound):
        get_pull_observations_config(integration)
