import hashlib
import logging
import httpx
from typing import Dict, Any

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)


def get_auth_config(integration):
    """Retrieve authentication configuration for the integration."""
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_observations_config(integration):
    """Retrieve pull observations configuration for the integration."""
    config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not config:
        raise ConfigurationNotFound(
            f"PullObservations settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(config.data)


def generate_source_id(lat: float, lon: float) -> str:
    """
    Generate a unique source ID from latitude and longitude coordinates.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
        
    Returns:
        A hash string representing the unique location
    """
    location_string = f"{lat},{lon}"
    return hashlib.md5(location_string.encode()).hexdigest()


async def fetch_weather_data(
    *,
    lat: float,
    lon: float,
    api_key: str,
    units: str = "metric",
    base_url: str = "https://api.openweathermap.org"
) -> Dict[str, Any]:
    """
    Fetch current weather data from OpenWeatherMap API for a specific location.
    
    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        api_key: OpenWeatherMap API key
        units: Units system (metric, imperial, or standard)
        base_url: Base URL for the OpenWeatherMap API
        
    Returns:
        Dict containing the weather data response
        
    Raises:
        httpx.HTTPStatusError: If the API returns an error status code
        httpx.HTTPError: For other HTTP-related errors
    """
    url = f"{base_url}/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": units
    }
    
    logger.info(f"Fetching weather data for location ({lat}, {lon})")
    
    async with httpx.AsyncClient(timeout=30) as session:
        response = await session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
    logger.info(f"Successfully fetched weather data for location ({lat}, {lon})")
    return data
