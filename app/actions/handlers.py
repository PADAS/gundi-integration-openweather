import datetime
import httpx
import logging
import stamina

import app.actions.client as client
import app.services.gundi as gundi_tools

from app.services.activity_logger import activity_logger
from app.services.action_scheduler import crontab_schedule
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig

logger = logging.getLogger(__name__)


def transform_weather_data(weather_data: dict, location_name: str, source_id: str) -> dict:
    """
    Transform OpenWeatherMap API response to Gundi observation format.
    
    Args:
        weather_data: Raw weather data from OpenWeatherMap API
        location_name: Name of the location
        source_id: Unique source identifier
        
    Returns:
        Dictionary in Gundi observation format
    """
    # Extract timestamp from the API response
    recorded_at = datetime.datetime.fromtimestamp(
        weather_data["dt"],
        tz=datetime.timezone.utc
    )
    
    # Extract location coordinates
    lat = weather_data["coord"]["lat"]
    lon = weather_data["coord"]["lon"]
    
    # Build the additional data with all available fields
    additional = {}
    
    # Weather conditions
    if "weather" in weather_data and weather_data["weather"]:
        additional["weather"] = weather_data["weather"]
    
    # Main weather measurements
    if "main" in weather_data:
        additional["main"] = weather_data["main"]
    
    # Visibility
    if "visibility" in weather_data:
        additional["visibility"] = weather_data["visibility"]
    
    # Wind data
    if "wind" in weather_data:
        additional["wind"] = weather_data["wind"]
    
    # Rain data
    if "rain" in weather_data:
        additional["rain"] = weather_data["rain"]
    
    # Snow data
    if "snow" in weather_data:
        additional["snow"] = weather_data["snow"]
    
    # Clouds
    if "clouds" in weather_data:
        additional["clouds"] = weather_data["clouds"]
    
    # System data (sunrise, sunset, country)
    if "sys" in weather_data:
        additional["sys"] = weather_data["sys"]
    
    # Timezone offset
    if "timezone" in weather_data:
        additional["timezone"] = weather_data["timezone"]
    
    # Base station info
    if "base" in weather_data:
        additional["base"] = weather_data["base"]
    
    # City name from API
    if "name" in weather_data:
        additional["city_name"] = weather_data["name"]
    
    # Build the observation
    observation = {
        "source": source_id,
        "source_name": location_name,
        "type": "stationary-object",
        "subtype": "weather_station",
        "recorded_at": recorded_at,
        "location": {
            "lat": lat,
            "lon": lon
        },
        "additional": additional
    }
    
    return observation


async def action_auth(integration, action_config: AuthenticateConfig):
    """
    Validate OpenWeatherMap API credentials by making a test request.
    
    Args:
        integration: Integration object
        action_config: Authentication configuration
        
    Returns:
        Dict with validation result
    """
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        base_url = integration.base_url or 'https://api.openweathermap.org'
        
        # Make a test request to validate credentials
        # Using a simple location (latitude 0, longitude 0)
        async with httpx.AsyncClient(timeout=30) as session:
            response = await session.get(
                f"{base_url}/data/2.5/weather",
                params={
                    "lat": 0,
                    "lon": 0,
                    "appid": action_config.api_key.get_secret_value()
                }
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            logger.warning("Invalid API credentials")
            return {"valid_credentials": False}
        else:
            logger.error(f"Error authenticating to OpenWeatherMap API. status code: {e.response.status_code}")
            raise e
    except httpx.HTTPError as e:
        message = f"auth action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info(f"Authenticated with success.")
        return {"valid_credentials": True}


@crontab_schedule("*/20 * * * *")  # Run every 20 minutes
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    """
    Pull weather observations from OpenWeatherMap API for all configured locations.
    
    Args:
        integration: Integration object
        action_config: Pull observations configuration
        
    Returns:
        Dict with extraction results
    """
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")
    
    try:
        result = {"observations_extracted": 0, "details": {}}
        
        # Get authentication config
        auth_config = client.get_auth_config(integration)
        api_key = auth_config.api_key.get_secret_value()
        
        # Get base URL
        base_url = integration.base_url or 'https://api.openweathermap.org'
        
        # Get configured locations
        locations = action_config.locations
        units = action_config.units
        
        if not locations:
            msg = f"No locations configured for integration_id: {str(integration.id)}."
            logger.warning(msg)
            result["message"] = msg
            return result
        
        total_observations = 0
        response_per_location = []
        
        # Process each location
        for location in locations:
            location_name = location.name
            lat = location.lat
            lon = location.lon
            
            logger.info(f"Processing location: {location_name} ({lat}, {lon})")
            
            try:
                # Fetch weather data with retry logic
                weather_data = None
                async for attempt in stamina.retry_context(
                    on=httpx.HTTPError,
                    attempts=3,
                    wait_initial=datetime.timedelta(seconds=1),
                    wait_max=datetime.timedelta(seconds=10),
                    wait_jitter=datetime.timedelta(seconds=1),
                ):
                    with attempt:
                        weather_data = await client.fetch_weather_data(
                            lat=lat,
                            lon=lon,
                            api_key=api_key,
                            units=units,
                            base_url=base_url
                        )
                
                if weather_data:
                    # Generate source ID from coordinates
                    source_id = client.generate_source_id(lat, lon)
                    
                    # Transform to Gundi observation format
                    observation = transform_weather_data(
                        weather_data,
                        location_name,
                        source_id
                    )
                    
                    # Send to Gundi with retry
                    async for attempt in stamina.retry_context(
                        on=httpx.HTTPError,
                        attempts=3,
                        wait_initial=datetime.timedelta(seconds=2),
                        wait_max=datetime.timedelta(seconds=10),
                        wait_jitter=datetime.timedelta(seconds=1),
                    ):
                        with attempt:
                            response = await gundi_tools.send_observations_to_gundi(
                                observations=[observation],
                                integration_id=integration.id
                            )
                    
                    total_observations += 1
                    response_per_location.append({
                        "location": location_name,
                        "status": "success",
                        "response": response
                    })
                    logger.info(f"Successfully processed location: {location_name}")
                    
            except httpx.HTTPError as e:
                msg = f"Failed to fetch weather data for location '{location_name}' after retries. Exception: {e}"
                logger.exception(
                    msg,
                    extra={
                        'needs_attention': True,
                        'integration_id': str(integration.id),
                        'action_id': "pull_observations",
                        'location': location_name
                    }
                )
                response_per_location.append({
                    "location": location_name,
                    "status": "error",
                    "error": str(e)
                })
                # Continue with other locations
                continue
        
        result["observations_extracted"] = total_observations
        result["details"] = response_per_location
        return result
        
    except Exception as e:
        message = f"pull_observations action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e

