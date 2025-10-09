import pytest
import respx
from httpx import Response

from app.actions.handlers import action_pull_observations
from app.actions.configurations import PullObservationsConfiguration, NamedLocation


@pytest.mark.asyncio
@respx.mock
async def test_action_pull_observations_success(mocker):
    integration = mocker.Mock()
    integration.id = "integration-123"

    config = PullObservationsConfiguration(
        api_key="secret",
        units="metric",
        lang="en",
        locations=[
            NamedLocation(name="GDL", lat=20.659698, lon=-103.349609),
            NamedLocation(name="NYC", lat=40.7128, lon=-74.0060),
        ],
    )

    def mock_payload(dt_value):
        return {
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
            },
            "visibility": 10000,
            "wind": {"speed": 0, "deg": 0},
            "clouds": {"all": 100},
            "dt": dt_value,
            "sys": {"country": "MX"},
            "timezone": -21600,
            "id": 4005539,
            "name": "Guadalajara",
            "cod": 200,
        }

    weather_route = respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        side_effect=[
            Response(200, json=mock_payload(1760015910)),
            Response(200, json=mock_payload(1760015911)),
        ]
    )

    mocker.patch("app.actions.handlers.send_observations_to_gundi", return_value={})
    mocker.patch("app.services.activity_logger.publish_event", new_callable=mocker.AsyncMock)

    result = await action_pull_observations(integration, config)

    assert result["observations_extracted"] == 2
    assert weather_route.called


@pytest.mark.asyncio
@respx.mock
async def test_action_pull_observations_partial_fail(mocker):
    integration = mocker.Mock()
    integration.id = "integration-123"

    config = PullObservationsConfiguration(
        api_key="secret",
        units="metric",
        lang="en",
        locations=[
            NamedLocation(name="Good", lat=10.0, lon=11.0),
            NamedLocation(name="Bad", lat=11.0, lon=22.0),
        ],
    )

    respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        side_effect=[
            Response(200, json={"dt": 1700000000}),
            Response(500, json={"error": "server"}),
        ]
    )

    mocker.patch("app.actions.handlers.send_observations_to_gundi", return_value={})
    mocker.patch("app.services.activity_logger.publish_event", new_callable=mocker.AsyncMock)

    result = await action_pull_observations(integration, config)

    assert result["observations_extracted"] == 1
    assert len(result["details"]) == 1