import pytest
import httpx
import respx
import datetime
from httpx import Response
from unittest.mock import AsyncMock, patch

from app.actions.handlers import (
    action_auth,
    action_pull_observations,
    transform_weather_to_observation
)
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, Location


def test_transform_weather_to_observation():
    """Test transformation of weather data to observation format."""
    weather_data = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
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
        "wind": {"speed": 5.5, "deg": 180, "gust": 8.2},
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
    source_id = "openweather_test123"
    
    observation = transform_weather_to_observation(weather_data, location_name, source_id)
    
    # Check structure
    assert observation["source"] == source_id
    assert observation["source_name"] == location_name
    assert observation["type"] == "stationary-object"
    assert observation["subtype"] == "weather_station"
    
    # Check location
    assert observation["location"]["lat"] == 20.6597
    assert observation["location"]["lon"] == -103.3496
    
    # Check recorded_at is datetime
    assert isinstance(observation["recorded_at"], datetime.datetime)
    
    # Check additional data
    additional = observation["additional"]
    assert additional["temperature"] == 16.88
    assert additional["humidity"] == 88
    assert additional["weather_main"] == "Rain"
    assert additional["weather_description"] == "light rain"
    assert additional["wind_speed"] == 5.5
    assert additional["rain_1h"] == 0.91
    assert additional["country"] == "MX"
    assert additional["city_name"] == "Guadalajara"


@pytest.mark.asyncio
@respx.mock
async def test_action_auth_success(mocker, mock_publish_event):
    """Test successful authentication."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    action_config = AuthenticateConfig(api_key="valid_test_key")
    
    # Mock the OpenWeather API endpoint used for testing
    url = "https://api.openweathermap.org/data/2.5/weather"
    respx.get(url).mock(
        return_value=Response(
            200,
            json={
                "coord": {"lon": -0.1278, "lat": 51.5074},
                "weather": [{"id": 800, "main": "Clear"}],
                "main": {"temp": 15.0},
                "dt": 1760015910,
                "name": "London"
            }
        )
    )
    
    result = await action_auth(integration, action_config)
    
    assert result == {"valid_credentials": True}


@pytest.mark.asyncio
@respx.mock
async def test_action_auth_unauthorized(mocker, mock_publish_event):
    """Test authentication with invalid credentials."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    action_config = AuthenticateConfig(api_key="invalid_key")
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    respx.get(url).mock(
        return_value=Response(401, json={"cod": 401, "message": "Invalid API key"})
    )
    
    result = await action_auth(integration, action_config)
    
    assert result == {"valid_credentials": False}


@pytest.mark.asyncio
@respx.mock
async def test_action_auth_http_error(mocker, mock_publish_event):
    """Test authentication with server error."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    
    action_config = AuthenticateConfig(api_key="test_key")
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    respx.get(url).mock(
        return_value=Response(500, json={"message": "Server Error"})
    )
    
    with pytest.raises(httpx.HTTPError):
        await action_auth(integration, action_config)


@pytest.mark.asyncio
async def test_action_pull_observations_success(mocker, mock_publish_event):
    """Test successful pulling of observations."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    integration.configurations = []
    
    location = Location(name="Test Location", lat=20.659698, lon=-103.349609)
    action_config = PullObservationsConfig(
        locations=[location],
        units="metric"
    )
    
    auth_config = AuthenticateConfig(api_key="test_key")
    
    mock_weather_data = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "main": {
            "temp": 16.88,
            "feels_like": 16.92,
            "temp_min": 16.88,
            "temp_max": 16.88,
            "pressure": 1016,
            "humidity": 88
        },
        "visibility": 10000,
        "wind": {"speed": 0, "deg": 0},
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
        "name": "Guadalajara"
    }
    
    # Mock dependencies
    mocker.patch('app.actions.client.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.fetch_current_weather', return_value=mock_weather_data)
    mocker.patch('app.services.gundi.send_observations_to_gundi', return_value={"status": "success"})
    mocker.patch('app.actions.handlers.state_manager.get_state', return_value=None)
    mocker.patch('app.actions.handlers.state_manager.set_state', return_value=None)
    
    result = await action_pull_observations(integration, action_config)
    
    assert result["observations_extracted"] == 1
    assert len(result["details"]) == 1
    assert result["details"][0]["location"] == "Test Location"


@pytest.mark.asyncio
async def test_action_pull_observations_no_new_data(mocker, mock_publish_event):
    """Test pulling observations when data is not new."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    integration.configurations = []
    
    location = Location(name="Test Location", lat=20.659698, lon=-103.349609)
    action_config = PullObservationsConfig(
        locations=[location],
        units="metric"
    )
    
    auth_config = AuthenticateConfig(api_key="test_key")
    
    mock_weather_data = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "main": {"temp": 16.88, "pressure": 1016, "humidity": 88},
        "dt": 1760015910,
        "sys": {"country": "MX"},
        "name": "Guadalajara"
    }
    
    # Mock state with newer timestamp
    existing_state = {
        "latest_timestamp": datetime.datetime.fromtimestamp(
            1760015920,  # 10 seconds later
            tz=datetime.timezone.utc
        ).isoformat()
    }
    
    # Mock dependencies
    mocker.patch('app.actions.client.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.fetch_current_weather', return_value=mock_weather_data)
    mocker.patch('app.actions.handlers.state_manager.get_state', return_value=existing_state)
    
    result = await action_pull_observations(integration, action_config)
    
    assert result["observations_extracted"] == 0
    assert "message" in result


@pytest.mark.asyncio
async def test_action_pull_observations_with_failures(mocker, mock_publish_event):
    """Test pulling observations with some locations failing."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    integration.configurations = []
    
    location1 = Location(name="Working Location", lat=20.659698, lon=-103.349609)
    location2 = Location(name="Failing Location", lat=30.0, lon=-100.0)
    action_config = PullObservationsConfig(
        locations=[location1, location2],
        units="metric"
    )
    
    auth_config = AuthenticateConfig(api_key="test_key")
    
    mock_weather_data = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "main": {"temp": 16.88, "pressure": 1016, "humidity": 88},
        "dt": 1760015910,
        "sys": {"country": "MX"},
        "name": "Guadalajara"
    }
    
    async def mock_fetch_weather(**kwargs):
        if kwargs['lat'] == 20.659698:
            return mock_weather_data
        else:
            raise httpx.HTTPError("Connection error")
    
    # Mock dependencies
    mocker.patch('app.actions.client.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.fetch_current_weather', side_effect=mock_fetch_weather)
    mocker.patch('app.services.gundi.send_observations_to_gundi', return_value={"status": "success"})
    mocker.patch('app.actions.handlers.state_manager.get_state', return_value=None)
    mocker.patch('app.actions.handlers.state_manager.set_state', return_value=None)
    
    result = await action_pull_observations(integration, action_config)
    
    # Should have one successful observation
    assert result["observations_extracted"] == 1
    # Should have failed_locations field
    assert "failed_locations" in result
    assert len(result["failed_locations"]) == 1
    assert result["failed_locations"][0]["location"] == "Failing Location"


@pytest.mark.asyncio
async def test_action_pull_observations_multiple_locations(mocker, mock_publish_event):
    """Test pulling observations from multiple locations."""
    mocker.patch('app.services.activity_logger.publish_event', mock_publish_event)
    
    integration = mocker.Mock()
    integration.id = "test-integration-id"
    integration.configurations = []
    
    location1 = Location(name="Location 1", lat=20.659698, lon=-103.349609)
    location2 = Location(name="Location 2", lat=30.0, lon=-100.0)
    action_config = PullObservationsConfig(
        locations=[location1, location2],
        units="metric"
    )
    
    auth_config = AuthenticateConfig(api_key="test_key")
    
    mock_weather_data_1 = {
        "coord": {"lon": -103.3496, "lat": 20.6597},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "main": {"temp": 16.88, "pressure": 1016, "humidity": 88},
        "dt": 1760015910,
        "sys": {"country": "MX"},
        "name": "Guadalajara"
    }
    
    mock_weather_data_2 = {
        "coord": {"lon": -100.0, "lat": 30.0},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "main": {"temp": 25.0, "pressure": 1013, "humidity": 60},
        "dt": 1760015920,
        "sys": {"country": "US"},
        "name": "Test City"
    }
    
    async def mock_fetch_weather(**kwargs):
        if kwargs['lat'] == 20.659698:
            return mock_weather_data_1
        else:
            return mock_weather_data_2
    
    # Mock dependencies
    mocker.patch('app.actions.client.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.fetch_current_weather', side_effect=mock_fetch_weather)
    mocker.patch('app.services.gundi.send_observations_to_gundi', return_value={"status": "success"})
    mocker.patch('app.actions.handlers.state_manager.get_state', return_value=None)
    mocker.patch('app.actions.handlers.state_manager.set_state', return_value=None)
    
    result = await action_pull_observations(integration, action_config)
    
    assert result["observations_extracted"] == 2
    assert len(result["details"]) == 2
    assert result["details"][0]["location"] == "Location 1"
    assert result["details"][1]["location"] == "Location 2"

