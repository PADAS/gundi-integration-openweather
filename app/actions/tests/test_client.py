import pytest
import httpx

from app.actions.client import (
    fetch_current_weather,
    validate_api_key,
    generate_source_id,
    get_auth_config,
    get_pull_observations_config
)
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, Location
from app.services.errors import ConfigurationNotFound


@pytest.mark.asyncio
async def test_fetch_current_weather_success(mocker):
    """Test successful weather data fetch."""
    lat = 20.659698
    lon = -103.349609
    api_key = "test_api_key"
    units = "metric"
    
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
    
    result = await fetch_current_weather(
        lat=lat,
        lon=lon,
        api_key=api_key,
        units=units
    )
    
    assert result == mock_response_data
    assert result['name'] == 'Guadalajara'
    assert result['main']['temp'] == 16.88


@pytest.mark.asyncio
async def test_fetch_current_weather_http_error(mocker):
    """Test handling of HTTP errors."""
    lat = 20.659698
    lon = -103.349609
    api_key = "test_api_key"
    units = "metric"
    
    mocker.patch(
        'httpx.AsyncClient.get',
        side_effect=httpx.HTTPStatusError(
            "Server Error",
            request=mocker.Mock(),
            response=mocker.Mock(status_code=500)
        )
    )
    
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_current_weather(
            lat=lat,
            lon=lon,
            api_key=api_key,
            units=units
        )


@pytest.mark.asyncio
async def test_fetch_current_weather_unauthorized(mocker):
    """Test handling of unauthorized access (invalid API key)."""
    lat = 20.659698
    lon = -103.349609
    api_key = "invalid_key"
    units = "metric"
    
    mocker.patch(
        'httpx.AsyncClient.get',
        side_effect=httpx.HTTPStatusError(
            "Unauthorized",
            request=mocker.Mock(),
            response=mocker.Mock(status_code=401)
        )
    )
    
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_current_weather(
            lat=lat,
            lon=lon,
            api_key=api_key,
            units=units
        )


@pytest.mark.asyncio
async def test_validate_api_key_valid(mocker):
    """Test API key validation with valid key."""
    api_key = "valid_test_key"
    
    mock_response_data = {
        "coord": {"lon": -0.1278, "lat": 51.5074},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "main": {"temp": 15.0, "pressure": 1013, "humidity": 72},
        "dt": 1760015910,
        "name": "London",
        "cod": 200
    }
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(200, json=mock_response_data)
        response._request = request
        return response
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    result = await validate_api_key(api_key)
    
    assert result is True


@pytest.mark.asyncio
async def test_validate_api_key_invalid(mocker):
    """Test API key validation with invalid key."""
    api_key = "invalid_test_key"
    
    mocker.patch(
        'httpx.AsyncClient.get',
        side_effect=httpx.HTTPStatusError(
            "Unauthorized",
            request=mocker.Mock(),
            response=mocker.Mock(status_code=401)
        )
    )
    
    result = await validate_api_key(api_key)
    
    assert result is False


@pytest.mark.asyncio
async def test_validate_api_key_http_error(mocker):
    """Test API key validation with other HTTP errors."""
    api_key = "test_key"
    
    mocker.patch(
        'httpx.AsyncClient.get',
        side_effect=httpx.HTTPStatusError(
            "Server Error",
            request=mocker.Mock(),
            response=mocker.Mock(status_code=500)
        )
    )
    
    with pytest.raises(httpx.HTTPStatusError):
        await validate_api_key(api_key)


def test_generate_source_id():
    """Test source ID generation from coordinates."""
    lat = 20.659698
    lon = -103.349609
    
    source_id = generate_source_id(lat, lon)
    
    # Should start with 'openweather_'
    assert source_id.startswith('openweather_')
    
    # Should be consistent for same coordinates
    source_id2 = generate_source_id(lat, lon)
    assert source_id == source_id2
    
    # Should be different for different coordinates
    source_id3 = generate_source_id(20.0, -103.0)
    assert source_id != source_id3


def test_get_auth_config_success(mocker):
    """Test successful retrieval of auth configuration."""
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    mock_config = mocker.Mock()
    mock_config.data = {
        "api_key": "test_api_key"
    }
    
    mocker.patch(
        'app.actions.client.find_config_for_action',
        return_value=mock_config
    )
    
    result = get_auth_config(integration)
    
    assert isinstance(result, AuthenticateConfig)
    assert result.api_key.get_secret_value() == "test_api_key"


def test_get_auth_config_not_found(mocker):
    """Test error when auth configuration is not found."""
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    mocker.patch(
        'app.actions.client.find_config_for_action',
        return_value=None
    )
    
    with pytest.raises(ConfigurationNotFound):
        get_auth_config(integration)


def test_get_pull_observations_config_success(mocker):
    """Test successful retrieval of pull observations configuration."""
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    mock_config = mocker.Mock()
    mock_config.data = {
        "locations": [
            {"name": "Test Location", "lat": 20.0, "lon": -103.0}
        ],
        "units": "metric"
    }
    
    mocker.patch(
        'app.actions.client.find_config_for_action',
        return_value=mock_config
    )
    
    result = get_pull_observations_config(integration)
    
    assert isinstance(result, PullObservationsConfig)
    assert len(result.locations) == 1
    assert result.locations[0].name == "Test Location"
    assert result.units == "metric"


def test_get_pull_observations_config_not_found(mocker):
    """Test error when pull observations configuration is not found."""
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    mocker.patch(
        'app.actions.client.find_config_for_action',
        return_value=None
    )
    
    with pytest.raises(ConfigurationNotFound):
        get_pull_observations_config(integration)

