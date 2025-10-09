import hashlib
import logging
import httpx
import pydantic

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action


logger = logging.getLogger(__name__)


class OpenWeatherException(Exception):
    """Base exception for OpenWeather API errors."""
    def __init__(self, message: str, status_code=500):
        self.status_code = status_code
        self.message = message
        super().__init__(f'{self.status_code}: {self.message}')


class OpenWeatherUnauthorizedException(Exception):
    """Exception for authentication failures."""
    def __init__(self, message: str, status_code=401):
        self.status_code = status_code
        self.message = message
        super().__init__(f'{self.status_code}: {self.message}')


def get_auth_config(integration):
    """Extract authentication configuration from integration."""
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
    """Extract pull observations configuration from integration."""
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
    Generate a unique source ID from coordinates using hash.
    
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        
    Returns:
        A unique string identifier based on the coordinates
    """
    # Create a stable string representation of the coordinates
    coord_string = f"{lat:.6f},{lon:.6f}"
    # Generate a hash
    hash_obj = hashlib.sha256(coord_string.encode())
    # Return a shortened hash (first 12 characters should be unique enough)
    return f"openweather_{hash_obj.hexdigest()[:12]}"


async def fetch_current_weather(
    *,
    lat: float,
    lon: float,
    api_key: str,
    units: str = "metric"
) -> dict:
    """
    Fetch current weather data from OpenWeather API for a specific location.
    
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        api_key: OpenWeather API key
        units: Units system (metric, imperial, or standard)
        
    Returns:
        Weather data as a dictionary
        
    Raises:
        httpx.HTTPStatusError: For HTTP errors
        pydantic.ValidationError: For invalid response format
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": units
    }
    
    logger.info(f"Fetching weather data for location ({lat}, {lon}) with units={units}")
    
    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.get(url, params=params)
        response.raise_for_status()
        
    data = response.json()
    logger.debug(f"Received weather data: {data}")
    
    return data


async def validate_api_key(api_key: str) -> bool:
    """
    Validate if the provided API key is valid by making a simple API call.
    
    Args:
        api_key: OpenWeather API key to test
        
    Returns:
        True if the API key is valid, False otherwise
    """
    # Use a known location (London) to test the API key
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": 51.5074,
        "lon": -0.1278,
        "appid": api_key
    }
    
    try:
        async with httpx.AsyncClient(timeout=30) as session:
            response = await session.get(url, params=params)
            response.raise_for_status()
            return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            return False
        raise
    except httpx.HTTPError:
        raise

