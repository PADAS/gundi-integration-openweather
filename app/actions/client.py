import hashlib
import logging
import datetime
import httpx
import pydantic

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)


# Pydantic Models for OpenWeather API response
class Coordinates(pydantic.BaseModel):
    lon: float
    lat: float


class WeatherCondition(pydantic.BaseModel):
    id: int
    main: str
    description: str
    icon: str


class MainWeatherData(pydantic.BaseModel):
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int
    sea_level: int = None
    grnd_level: int = None


class Wind(pydantic.BaseModel):
    speed: float
    deg: int = None
    gust: float = None


class Rain(pydantic.BaseModel):
    one_h: float = pydantic.Field(None, alias="1h")

    class Config:
        allow_population_by_field_name = True


class Clouds(pydantic.BaseModel):
    all: int


class SystemInfo(pydantic.BaseModel):
    type: int = None
    id: int = None
    country: str = None
    sunrise: int = None
    sunset: int = None


class OpenWeatherResponse(pydantic.BaseModel):
    coord: Coordinates
    weather: list[WeatherCondition]
    base: str
    main: MainWeatherData
    visibility: int = None
    wind: Wind = None
    rain: Rain = None
    clouds: Clouds = None
    dt: int
    sys: SystemInfo = None
    timezone: int
    id: int
    name: str
    cod: int


def get_auth_config(integration):
    """Get authentication configuration from integration."""
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            "are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_observations_config(integration):
    """Get pull observations configuration from integration."""
    config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not config:
        raise ConfigurationNotFound(
            f"PullObservations settings for integration {str(integration.id)} "
            "are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(config.data)


def generate_location_source_id(lat: float, lon: float) -> str:
    """Generate a unique source ID from lat/lon coordinates using hash."""
    coord_string = f"{lat},{lon}"
    return hashlib.md5(coord_string.encode()).hexdigest()


async def get_weather_data_for_location(
    *,
    lat: float,
    lon: float,
    api_key: str,
    base_url: str = "https://api.openweathermap.org/data/2.5/weather"
) -> OpenWeatherResponse:
    """Fetch weather data for a specific location from OpenWeather API."""
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"
    }

    async with httpx.AsyncClient(timeout=30) as session:
        response = await session.get(base_url, params=params)
        response.raise_for_status()

        response_data = response.json()
        return OpenWeatherResponse.parse_obj(response_data)


async def get_weather_data_for_locations(
    *,
    locations: list,
    api_key: str,
    base_url: str = "https://api.openweathermap.org/data/2.5/weather",
    integration_id: str
) -> dict:
    """
    Fetch weather data for multiple locations.
    Returns a dict mapping location source_id to weather data.
    """
    weather_data = {}

    for location in locations:
        try:
            logger.info(f"Fetching weather data for location: {location.name} ({location.lat}, {location.lon})")

            weather_response = await get_weather_data_for_location(
                lat=location.lat,
                lon=location.lon,
                api_key=api_key,
                base_url=base_url
            )

            source_id = generate_location_source_id(location.lat, location.lon)
            weather_data[source_id] = {
                "location_config": location,
                "weather_response": weather_response
            }

            logger.info(f"Successfully fetched weather data for {location.name}")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error(f"Invalid API key for location {location.name}: {e}")
            else:
                logger.error(f"HTTP error fetching weather for {location.name}: {e}")
        except httpx.HTTPError as e:
            logger.error(f"Network error fetching weather for {location.name}: {e}")
        except pydantic.ValidationError as e:
            logger.error(f"Invalid response format for location {location.name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching weather for {location.name}: {e}")

    return weather_data