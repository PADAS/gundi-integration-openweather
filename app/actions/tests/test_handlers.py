import pytest
import httpx
import datetime
from unittest.mock import AsyncMock, Mock
from httpx import Response

from app.actions.handlers import (
    action_auth,
    action_pull_observations,
    transform_weather_data
)
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, Location


def test_transform_weather_data():
    """Test transformation of OpenWeather API data to Gundi observation format."""
    weather_data = {
        "coord": {"lon": -103.349609, "lat": 20.659698},
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
    
    location_name = "Test Station"
    source_id = "test_source_123"
    
    observation = transform_weather_data(weather_data, location_name, source_id)
    
    # Check basic structure
    assert observation["source"] == source_id
    assert observation["source_name"] == location_name
    assert observation["type"] == "stationary-object"
    assert observation["subtype"] == "weather_station"
    
    # Check location
    assert observation["location"]["lat"] == 20.659698
    assert observation["location"]["lon"] == -103.349609
    
    # Check recorded_at is datetime
    assert isinstance(observation["recorded_at"], datetime.datetime)
    assert observation["recorded_at"].timestamp() == 1760015910
    
    # Check additional data
    assert "weather" in observation["additional"]
    assert "main" in observation["additional"]
    assert "wind" in observation["additional"]
    assert "rain" in observation["additional"]
    assert "clouds" in observation["additional"]
    assert "sys" in observation["additional"]
    assert "visibility" in observation["additional"]
    assert "city_name" in observation["additional"]
    assert observation["additional"]["city_name"] == "Guadalajara"


@pytest.mark.asyncio
async def test_action_auth_success(mocker):
    """Test successful authentication."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    action_config = AuthenticateConfig(api_key='test_api_key')
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(200, json={"cod": 200})
        response._request = request
        return response
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": True}


@pytest.mark.asyncio
async def test_action_auth_unauthorized(mocker):
    """Test authentication with invalid credentials."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    action_config = AuthenticateConfig(api_key='invalid_key')
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(401, json={"cod": 401, "message": "Invalid API key"})
        response._request = request
        raise httpx.HTTPStatusError(
            "Unauthorized",
            request=request,
            response=response
        )
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": False}


@pytest.mark.asyncio
async def test_action_auth_http_error(mocker):
    """Test authentication with server error."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    action_config = AuthenticateConfig(api_key='test_api_key')
    
    async def mock_get(url, *args, **kwargs):
        request = httpx.Request('GET', url)
        response = httpx.Response(500, json={"error": "Server error"})
        response._request = request
        raise httpx.HTTPStatusError(
            "Server Error",
            request=request,
            response=response
        )
    
    mocker.patch('httpx.AsyncClient.get', side_effect=mock_get)
    
    with pytest.raises(httpx.HTTPStatusError):
        await action_auth(integration, action_config)


@pytest.mark.asyncio
async def test_action_pull_observations_success(mocker):
    """Test successful pull observations."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    
    locations = [
        Location(name="Test Location", lat=20.659698, lon=-103.349609)
    ]
    action_config = PullObservationsConfig(locations=locations, units="metric")
    
    mock_weather_data = {
        "coord": {"lon": -103.349609, "lat": 20.659698},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain"}],
        "main": {"temp": 16.88, "humidity": 88, "pressure": 1016},
        "wind": {"speed": 0},
        "clouds": {"all": 100},
        "dt": 1760015910,
        "sys": {"country": "MX"},
        "name": "Guadalajara"
    }
    
    # Mock the functions
    mocker.patch(
        'app.actions.client.get_auth_config',
        return_value=AuthenticateConfig(api_key='test_api_key')
    )
    mocker.patch(
        'app.actions.client.fetch_weather_data',
        return_value=mock_weather_data
    )
    mocker.patch(
        'app.services.gundi.send_observations_to_gundi',
        return_value={"status": "success"}
    )
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    
    result = await action_pull_observations(integration, action_config)
    
    assert result["observations_extracted"] == 1
    assert len(result["details"]) == 1
    assert result["details"][0]["location"] == "Test Location"
    assert result["details"][0]["status"] == "success"


@pytest.mark.asyncio
async def test_action_pull_observations_no_locations(mocker):
    """Test pull observations with no configured locations."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    
    action_config = PullObservationsConfig(locations=[], units="metric")
    
    mocker.patch(
        'app.actions.client.get_auth_config',
        return_value=AuthenticateConfig(api_key='test_api_key')
    )
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    
    result = await action_pull_observations(integration, action_config)
    
    assert result["observations_extracted"] == 0
    assert "message" in result
    assert "No locations configured" in result["message"]


@pytest.mark.asyncio
async def test_action_pull_observations_with_errors(mocker):
    """Test pull observations when some locations fail."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    
    locations = [
        Location(name="Good Location", lat=20.659698, lon=-103.349609),
        Location(name="Bad Location", lat=50.0, lon=50.0),
        Location(name="Another Good Location", lat=30.0, lon=-100.0)
    ]
    action_config = PullObservationsConfig(locations=locations, units="metric")
    
    mock_weather_data = {
        "coord": {"lon": -103.349609, "lat": 20.659698},
        "weather": [{"id": 500, "main": "Rain"}],
        "main": {"temp": 16.88},
        "dt": 1760015910,
        "name": "Test"
    }
    
    call_count = 0
    location_call_count = {}
    
    async def mock_fetch_weather(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        lat = kwargs.get('lat', 20.659698)
        lon = kwargs.get('lon', -103.349609)
        location_key = f"{lat},{lon}"
        
        # Track calls per location
        if location_key not in location_call_count:
            location_call_count[location_key] = 0
        location_call_count[location_key] += 1
        
        # Second location (50.0, 50.0) always fails
        if lat == 50.0 and lon == 50.0:
            raise httpx.HTTPError("Network error")
        
        # Update coordinates for each location
        data = mock_weather_data.copy()
        data["coord"] = {"lon": lon, "lat": lat}
        return data
    
    mocker.patch(
        'app.actions.client.get_auth_config',
        return_value=AuthenticateConfig(api_key='test_api_key')
    )
    mocker.patch(
        'app.actions.client.fetch_weather_data',
        side_effect=mock_fetch_weather
    )
    mocker.patch(
        'app.services.gundi.send_observations_to_gundi',
        return_value={"status": "success"}
    )
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    
    result = await action_pull_observations(integration, action_config)
    
    # Should have 2 successful observations (1st and 3rd location)
    assert result["observations_extracted"] == 2
    assert len(result["details"]) == 3
    
    # Check that all locations are in details
    location_names = [detail["location"] for detail in result["details"]]
    assert "Good Location" in location_names
    assert "Bad Location" in location_names
    assert "Another Good Location" in location_names
    
    # Check statuses
    for detail in result["details"]:
        if detail["location"] == "Bad Location":
            assert detail["status"] == "error"
        else:
            assert detail["status"] == "success"


@pytest.mark.asyncio
async def test_action_pull_observations_http_error(mocker):
    """Test pull observations with unhandled exception."""
    integration = mocker.Mock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org'
    
    action_config = PullObservationsConfig(locations=[], units="metric")
    
    # Make get_auth_config raise an exception
    mocker.patch(
        'app.actions.client.get_auth_config',
        side_effect=Exception("Unexpected error")
    )
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    
    with pytest.raises(Exception):
        await action_pull_observations(integration, action_config)
