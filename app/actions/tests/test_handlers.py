import pytest
import httpx
import respx
from httpx import Response
from unittest.mock import AsyncMock, patch
from app.actions.handlers import action_auth, action_pull_observations, transform_weather_data_to_observation
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, LocationConfig
from app.actions.client import OpenWeatherResponse, Coordinates, WeatherCondition, MainWeatherData, Wind, Rain, Clouds, SystemInfo


@pytest.mark.asyncio
async def test_action_auth_success(mocker):
    """Test successful API key authentication."""
    integration = mocker.MagicMock()
    integration.base_url = 'https://api.openweathermap.org/data/2.5/weather'
    action_config = AuthenticateConfig(api_key='valid_key')

    # Mock the client function that makes the actual API call
    mock_weather_response = mocker.MagicMock()
    mock_weather_response.coord = mocker.MagicMock(lon=-103.3496, lat=20.6597)
    mock_weather_response.weather = [mocker.MagicMock(id=500, main="Rain", description="light rain", icon="10d")]
    mock_weather_response.main = mocker.MagicMock(temp=16.88)
    mock_weather_response.dt = 1760015910
    mock_weather_response.name = "Guadalajara"

    mocker.patch('app.actions.client.get_weather_data_for_location', return_value=mock_weather_response)

    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": True}


@pytest.mark.asyncio
async def test_action_auth_invalid_key(mocker):
    """Test authentication with invalid API key."""
    integration = mocker.MagicMock()
    integration.base_url = 'https://api.openweathermap.org/data/2.5/weather'
    action_config = AuthenticateConfig(api_key='invalid_key')

    # Mock the client function to raise HTTPStatusError for invalid key
    mocker.patch('app.actions.client.get_weather_data_for_location', side_effect=httpx.HTTPStatusError(
        "Unauthorized", request=mocker.MagicMock(), response=mocker.MagicMock(status_code=401)
    ))

    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": False}


@pytest.mark.asyncio
async def test_action_auth_server_error(mocker):
    """Test authentication with server error."""
    integration = mocker.MagicMock()
    integration.base_url = 'https://api.openweathermap.org/data/2.5/weather'
    action_config = AuthenticateConfig(api_key='test_key')

    # Mock the client function to raise HTTPStatusError for server error
    mocker.patch('app.actions.client.get_weather_data_for_location', side_effect=httpx.HTTPStatusError(
        "Server Error", request=mocker.MagicMock(), response=mocker.MagicMock(status_code=500)
    ))

    with pytest.raises(httpx.HTTPError):
        await action_auth(integration, action_config)


def test_transform_weather_data_to_observation():
    """Test transformation of weather data to observation format."""
    location_config = LocationConfig(name="Guadalajara", lat=20.659698, lon=-103.349609)

    weather_response = OpenWeatherResponse(
        coord=Coordinates(lon=-103.3496, lat=20.6597),
        weather=[WeatherCondition(id=500, main="Rain", description="light rain", icon="10d")],
        base="stations",
        main=MainWeatherData(
            temp=16.88,
            feels_like=16.92,
            temp_min=16.88,
            temp_max=16.88,
            pressure=1016,
            humidity=88,
            sea_level=1016,
            grnd_level=841
        ),
        visibility=10000,
        wind=Wind(speed=0, deg=0),
        rain=Rain(one_h=0.91),
        clouds=Clouds(all=100),
        dt=1760015910,
        sys=SystemInfo(country="MX", sunrise=1760013996, sunset=1760056454),
        timezone=-21600,
        id=4005539,
        name="Guadalajara",
        cod=200
    )

    weather_data = {
        "location_config": location_config,
        "weather_response": weather_response
    }

    observation = transform_weather_data_to_observation(weather_data)

    assert observation["source_name"] == "Guadalajara"
    assert observation["type"] == "stationary-object"
    assert observation["subtype"] == "weather_station"
    assert observation["location"]["lat"] == 20.659698
    assert observation["location"]["lon"] == -103.349609
    assert "recorded_at" in observation
    assert isinstance(observation["additional"], dict)
    assert observation["additional"]["temp"] == 16.88
    assert observation["additional"]["humidity"] == 88
    assert observation["additional"]["wind"]["speed"] == 0
    assert observation["additional"]["weather"][0]["main"] == "Rain"


@pytest.mark.asyncio
async def test_action_pull_observations_success(mocker):
    """Test successful pull observations."""
    integration = mocker.MagicMock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org/data/2.5/weather'

    locations = [LocationConfig(name="Guadalajara", lat=20.659698, lon=-103.349609)]
    action_config = PullObservationsConfig(locations=locations)

    # Mock auth config
    mocker.patch('app.actions.client.get_auth_config', return_value=AuthenticateConfig(api_key='test_key'))

    # Mock weather API response
    mock_weather_response = OpenWeatherResponse(
        coord=Coordinates(lon=-103.3496, lat=20.6597),
        weather=[WeatherCondition(id=500, main="Rain", description="light rain", icon="10d")],
        base="stations",
        main=MainWeatherData(
            temp=16.88, feels_like=16.92, temp_min=16.88, temp_max=16.88,
            pressure=1016, humidity=88
        ),
        dt=1760015910,
        sys=SystemInfo(country="MX"),
        timezone=-21600,
        id=4005539,
        name="Guadalajara",
        cod=200
    )

    mocker.patch('app.actions.client.get_weather_data_for_location', return_value=mock_weather_response)
    mocker.patch('app.services.gundi.send_observations_to_gundi', return_value=[{"status": "success"}])
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())

    result = await action_pull_observations(integration, action_config)

    assert result["observations_extracted"] == 1
    assert "details" in result


@pytest.mark.asyncio
async def test_action_pull_observations_no_locations(mocker):
    """Test pull observations with no locations configured."""
    integration = mocker.MagicMock()
    integration.id = "test_integration"

    action_config = PullObservationsConfig(locations=[])

    # Mock the activity logger to avoid pubsub calls
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())

    result = await action_pull_observations(integration, action_config)

    assert result["observations_extracted"] == 0
    assert result["message"] == "No locations configured"


@pytest.mark.asyncio
async def test_action_pull_observations_api_error(mocker):
    """Test pull observations with API error."""
    integration = mocker.MagicMock()
    integration.id = "test_integration"
    integration.base_url = 'https://api.openweathermap.org/data/2.5/weather'

    locations = [LocationConfig(name="Guadalajara", lat=20.659698, lon=-103.349609)]
    action_config = PullObservationsConfig(locations=locations)

    # Mock auth config
    mocker.patch('app.actions.client.get_auth_config', return_value=AuthenticateConfig(api_key='test_key'))

    # Mock API error
    mocker.patch('app.actions.client.get_weather_data_for_location', side_effect=httpx.HTTPStatusError(
        "Server Error", request=mocker.Mock(), response=mocker.Mock(status_code=500)
    ))

    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())

    result = await action_pull_observations(integration, action_config)

    assert result["observations_extracted"] == 0
    assert "details" in result