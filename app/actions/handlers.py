import datetime
import logging
import httpx
import stamina

import app.actions.client as client
import app.services.gundi as gundi_tools

from app.services.activity_logger import activity_logger
from app.services.state import IntegrationStateManager
from app.services.action_scheduler import crontab_schedule
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def transform_weather_to_observation(
    weather_data: dict,
    location_name: str,
    source_id: str
) -> dict:
    """
    Transform OpenWeather API response into Gundi observation format.
    
    Args:
        weather_data: Raw weather data from OpenWeather API
        location_name: Human-readable name for the location
        source_id: Unique identifier for the source
        
    Returns:
        Observation dictionary in Gundi format
    """
    # Extract timestamp (convert Unix timestamp to datetime)
    recorded_at = datetime.datetime.fromtimestamp(
        weather_data['dt'],
        tz=datetime.timezone.utc
    )
    
    # Extract coordinates
    lat = weather_data['coord']['lat']
    lon = weather_data['coord']['lon']
    
    # Build additional data with all available fields
    additional = {}
    
    # Weather conditions
    if 'weather' in weather_data and weather_data['weather']:
        weather_info = weather_data['weather'][0]
        additional['weather_main'] = weather_info.get('main')
        additional['weather_description'] = weather_info.get('description')
        additional['weather_id'] = weather_info.get('id')
        additional['weather_icon'] = weather_info.get('icon')
    
    # Main measurements
    if 'main' in weather_data:
        main = weather_data['main']
        additional['temperature'] = main.get('temp')
        additional['feels_like'] = main.get('feels_like')
        additional['temp_min'] = main.get('temp_min')
        additional['temp_max'] = main.get('temp_max')
        additional['pressure'] = main.get('pressure')
        additional['humidity'] = main.get('humidity')
        additional['sea_level'] = main.get('sea_level')
        additional['grnd_level'] = main.get('grnd_level')
    
    # Visibility
    if 'visibility' in weather_data:
        additional['visibility'] = weather_data['visibility']
    
    # Wind data
    if 'wind' in weather_data:
        wind = weather_data['wind']
        additional['wind_speed'] = wind.get('speed')
        additional['wind_deg'] = wind.get('deg')
        additional['wind_gust'] = wind.get('gust')
    
    # Clouds
    if 'clouds' in weather_data:
        additional['clouds_all'] = weather_data['clouds'].get('all')
    
    # Rain
    if 'rain' in weather_data:
        rain = weather_data['rain']
        additional['rain_1h'] = rain.get('1h')
        additional['rain_3h'] = rain.get('3h')
    
    # Snow
    if 'snow' in weather_data:
        snow = weather_data['snow']
        additional['snow_1h'] = snow.get('1h')
        additional['snow_3h'] = snow.get('3h')
    
    # System info
    if 'sys' in weather_data:
        sys_info = weather_data['sys']
        additional['country'] = sys_info.get('country')
        if 'sunrise' in sys_info:
            additional['sunrise'] = datetime.datetime.fromtimestamp(
                sys_info['sunrise'],
                tz=datetime.timezone.utc
            ).isoformat()
        if 'sunset' in sys_info:
            additional['sunset'] = datetime.datetime.fromtimestamp(
                sys_info['sunset'],
                tz=datetime.timezone.utc
            ).isoformat()
    
    # Timezone offset
    if 'timezone' in weather_data:
        additional['timezone_offset'] = weather_data['timezone']
    
    # City name from API
    if 'name' in weather_data:
        additional['city_name'] = weather_data['name']
    
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


@activity_logger()
async def action_auth(integration, action_config: AuthenticateConfig):
    """
    Authenticate with OpenWeather API by testing the API key.
    
    Args:
        integration: Integration object
        action_config: Authentication configuration
        
    Returns:
        Dictionary with valid_credentials boolean
    """
    logger.info(f"Executing auth action with integration {integration}...")
    try:
        api_key = action_config.api_key.get_secret_value()
        is_valid = await client.validate_api_key(api_key)
        
        if is_valid:
            logger.info("Authenticated successfully with OpenWeather API.")
            return {"valid_credentials": True}
        else:
            logger.warning("Invalid API key provided for OpenWeather API.")
            return {"valid_credentials": False}
            
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            logger.warning("Authentication failed: Invalid API key.")
            return {"valid_credentials": False}
        else:
            logger.error(
                f"Error authenticating to OpenWeather API. Status code: {e.response.status_code}"
            )
            raise e
    except httpx.HTTPError as e:
        message = "Auth action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e


@crontab_schedule("*/20 * * * *")  # Run every 20 minutes
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    """
    Pull weather observations from OpenWeather API for all configured locations.
    
    Args:
        integration: Integration object
        action_config: Pull observations configuration
        
    Returns:
        Dictionary with observations_extracted count and details
    """
    logger.info(
        f"Executing pull_observations action with integration {integration} "
        f"and action_config {action_config}..."
    )
    
    try:
        # Get authentication config
        auth_config = client.get_auth_config(integration)
        api_key = auth_config.api_key.get_secret_value()
        
        total_observations = 0
        response_per_location = []
        failed_locations = []
        
        # Process each configured location
        for location in action_config.locations:
            location_name = location.name
            lat = location.lat
            lon = location.lon
            source_id = client.generate_source_id(lat, lon)
            
            logger.info(
                f"Processing location '{location_name}' at ({lat}, {lon}), "
                f"source_id: {source_id}"
            )
            
            # Retry logic with exponential backoff
            weather_data = None
            try:
                async for attempt in stamina.retry_context(
                    on=httpx.HTTPError,
                    attempts=3,
                    wait_initial=datetime.timedelta(seconds=10),
                    wait_max=datetime.timedelta(seconds=60),
                ):
                    with attempt:
                        try:
                            weather_data = await client.fetch_current_weather(
                                lat=lat,
                                lon=lon,
                                api_key=api_key,
                                units=action_config.units
                            )
                        except httpx.HTTPError as e:
                            logger.warning(
                                f"Error fetching weather for location '{location_name}': {e}. "
                                f"Attempt {attempt.num} of 3"
                            )
                            if attempt.num >= 3:
                                # Final attempt failed
                                msg = (
                                    f"Failed to fetch weather data for location '{location_name}' "
                                    f"after 3 attempts. Error: {e}"
                                )
                                logger.error(
                                    msg,
                                    extra={
                                        'needs_attention': True,
                                        'integration_id': str(integration.id),
                                        'action_id': "pull_observations",
                                        'location_name': location_name
                                    }
                                )
                                failed_locations.append({
                                    "location": location_name,
                                    "error": str(e)
                                })
                                weather_data = None
                            raise
            except httpx.HTTPError:
                # All retries exhausted, continue with next location
                pass
            
            if weather_data:
                # Check if this is new data by comparing with state
                current_state = await state_manager.get_state(
                    str(integration.id),
                    "pull_observations",
                    source_id
                )
                
                recorded_at = datetime.datetime.fromtimestamp(
                    weather_data['dt'],
                    tz=datetime.timezone.utc
                )
                
                should_send = True
                if current_state and 'latest_timestamp' in current_state:
                    latest_timestamp = datetime.datetime.fromisoformat(
                        current_state['latest_timestamp']
                    )
                    if recorded_at <= latest_timestamp:
                        logger.info(
                            f"Skipping location '{location_name}': "
                            f"Data timestamp {recorded_at} is not newer than "
                            f"latest seen timestamp {latest_timestamp}"
                        )
                        should_send = False
                
                if should_send:
                    # Transform to observation format
                    observation = transform_weather_to_observation(
                        weather_data,
                        location_name,
                        source_id
                    )
                    
                    # Send to Gundi with retry
                    async for attempt in stamina.retry_context(
                        on=httpx.HTTPError,
                        attempts=3,
                        wait_initial=datetime.timedelta(seconds=10),
                        wait_max=datetime.timedelta(seconds=30),
                    ):
                        with attempt:
                            try:
                                response = await gundi_tools.send_observations_to_gundi(
                                    observations=[observation],
                                    integration_id=integration.id
                                )
                            except httpx.HTTPError as e:
                                msg = (
                                    f"Sensors API returned error for integration_id: "
                                    f"{str(integration.id)}. Exception: {e}"
                                )
                                logger.exception(
                                    msg,
                                    extra={
                                        'needs_attention': True,
                                        'integration_id': str(integration.id),
                                        'action_id': "pull_observations"
                                    }
                                )
                                if attempt.num >= 3:
                                    response_per_location.append({
                                        "location": location_name,
                                        "response": [msg]
                                    })
                                raise
                            else:
                                total_observations += 1
                                
                                # Update state with latest timestamp
                                state = {
                                    "latest_timestamp": recorded_at.isoformat(),
                                    "location_name": location_name
                                }
                                await state_manager.set_state(
                                    str(integration.id),
                                    "pull_observations",
                                    state,
                                    source_id
                                )
                                
                                response_per_location.append({
                                    "location": location_name,
                                    "source_id": source_id,
                                    "response": response
                                })
                                
                                logger.info(
                                    f"Successfully sent observation for location '{location_name}'"
                                )
                else:
                    response_per_location.append({
                        "location": location_name,
                        "response": "skipped - no new data"
                    })
        
        result = {
            "observations_extracted": total_observations,
            "details": response_per_location
        }
        
        if failed_locations:
            result["failed_locations"] = failed_locations
            
        if total_observations == 0:
            msg = f"No new observations extracted for integration_id: {str(integration.id)}."
            logger.warning(msg)
            result["message"] = msg
        else:
            logger.info(
                f"Successfully extracted {total_observations} observation(s) "
                f"for integration_id: {str(integration.id)}"
            )
            
        return result
        
    except Exception as e:
        message = "pull_observations action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
