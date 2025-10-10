import datetime
import logging
import stamina
import httpx

import app.actions.client as client
import app.services.gundi as gundi_tools

from app.services.activity_logger import activity_logger
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig


logger = logging.getLogger(__name__)


def transform_weather_data_to_observation(weather_data: dict) -> dict:
    """
    Transform OpenWeather API response to Gundi observation format.
    """
    location_config = weather_data["location_config"]
    weather_response = weather_data["weather_response"]

    # Convert Unix timestamp to datetime string
    recorded_at = datetime.datetime.fromtimestamp(
        weather_response.dt, tz=datetime.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Generate source ID from coordinates
    source = client.generate_location_source_id(
        location_config.lat, location_config.lon
    )

    # Prepare additional data - include all fields except those mapped to root level
    additional = {}

    # Add weather conditions
    if weather_response.weather:
        additional["weather"] = [
            {
                "id": w.id,
                "main": w.main,
                "description": w.description,
                "icon": w.icon
            } for w in weather_response.weather
        ]

    # Add main weather data
    additional.update({
        "temp": weather_response.main.temp,
        "feels_like": weather_response.main.feels_like,
        "temp_min": weather_response.main.temp_min,
        "temp_max": weather_response.main.temp_max,
        "pressure": weather_response.main.pressure,
        "humidity": weather_response.main.humidity,
    })

    if weather_response.main.sea_level is not None:
        additional["sea_level_pressure"] = weather_response.main.sea_level
    if weather_response.main.grnd_level is not None:
        additional["ground_level_pressure"] = weather_response.main.grnd_level

    # Add visibility
    if weather_response.visibility is not None:
        additional["visibility"] = weather_response.visibility

    # Add wind data
    if weather_response.wind:
        wind_data = {"speed": weather_response.wind.speed}
        if weather_response.wind.deg is not None:
            wind_data["deg"] = weather_response.wind.deg
        if weather_response.wind.gust is not None:
            wind_data["gust"] = weather_response.wind.gust
        additional["wind"] = wind_data

    # Add rain data
    if weather_response.rain and weather_response.rain.one_h is not None:
        additional["rain_1h"] = weather_response.rain.one_h

    # Add clouds data
    if weather_response.clouds:
        additional["clouds"] = weather_response.clouds.all

    # Add system info
    if weather_response.sys:
        sys_data = {}
        if weather_response.sys.country:
            sys_data["country"] = weather_response.sys.country
        if weather_response.sys.sunrise:
            sys_data["sunrise"] = datetime.datetime.fromtimestamp(
                weather_response.sys.sunrise, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        if weather_response.sys.sunset:
            sys_data["sunset"] = datetime.datetime.fromtimestamp(
                weather_response.sys.sunset, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        if sys_data:
            additional["sys"] = sys_data

    # Add other metadata
    additional.update({
        "base": weather_response.base,
        "timezone": weather_response.timezone,
        "city_id": weather_response.id,
        "city_name": weather_response.name,
        "cod": weather_response.cod
    })

    # Create observation
    observation = {
        "source": source,
        "source_name": location_config.name,
        "type": "stationary-object",
        "subtype": "weather_station",
        "recorded_at": recorded_at,
        "location": {
            "lat": location_config.lat,
            "lon": location_config.lon
        },
        "additional": additional
    }

    return observation


async def action_auth(integration, action_config: AuthenticateConfig):
    """
    Validate the OpenWeather API key by making a test request.
    """
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")

    try:
        base_url = integration.base_url or 'https://api.openweathermap.org/data/2.5/weather'

        # Use a default location to test the API key
        test_lat, test_lon = 20.659698, -103.349609  # Guadalajara coordinates

        await client.get_weather_data_for_location(
            lat=test_lat,
            lon=test_lon,
            api_key=action_config.api_key.get_secret_value(),
            base_url=base_url
        )

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            logger.error(f"Invalid API key provided: {e}")
            return {"valid_credentials": False}
        else:
            logger.error(f"Authentication failed with status code {e.response.status_code}: {e}")
            raise e
    except httpx.HTTPError as e:
        message = "Auth action returned network error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    except Exception as e:
        message = f"Unexpected error during authentication: {e}"
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info("API key authentication successful.")
        return {"valid_credentials": True}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    """
    Pull weather observations for configured locations.
    """
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")

    try:
        result = {"observations_extracted": 0, "details": {}}
        total_observations = 0

        if not action_config.locations:
            logger.warning(f"No locations configured for integration {integration.id}")
            result["message"] = "No locations configured"
            return result

        base_url = integration.base_url or 'https://api.openweathermap.org/data/2.5/weather'
        auth_config = client.get_auth_config(integration)

        # Process each location with retry logic
        for location in action_config.locations:
            location_result = {"response": []}

            try:
                async for attempt in stamina.retry_context(
                    on=httpx.HTTPError,
                    attempts=3,
                    wait_initial=datetime.timedelta(seconds=1),
                    wait_max=datetime.timedelta(seconds=30),
                    wait_jitter=True,
                ):
                    with attempt:
                        try:
                            logger.info(f"Fetching weather data for {location.name} (attempt {attempt.num})")

                            weather_data = await client.get_weather_data_for_location(
                                lat=location.lat,
                                lon=location.lon,
                                api_key=auth_config.api_key.get_secret_value(),
                                base_url=base_url
                            )

                            # Transform to observation
                            observation = transform_weather_data_to_observation({
                                "location_config": location,
                                "weather_response": weather_data
                            })

                            # Send to Gundi
                            async for send_attempt in stamina.retry_context(
                                on=httpx.HTTPError,
                                attempts=3,
                                wait_initial=datetime.timedelta(seconds=10),
                                wait_max=datetime.timedelta(seconds=60),
                            ):
                                with send_attempt:
                                    response = await gundi_tools.send_observations_to_gundi(
                                        observations=[observation],
                                        integration_id=integration.id
                                    )
                                    location_result["response"] = response
                                    total_observations += 1
                                    logger.info(f"Successfully sent observation for {location.name}")
                                    break  # Success, exit retry loop

                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 401:
                                logger.error(f"Invalid API key for location {location.name}: {e}")
                                location_result["response"] = [{"error": f"Invalid API key: {e}"}]
                                break  # Don't retry auth errors
                            else:
                                logger.warning(f"HTTP error for {location.name} (attempt {attempt.num}): {e}")
                                raise  # Will be retried by stamina
                        except httpx.HTTPError as e:
                            logger.warning(f"Network error for {location.name} (attempt {attempt.num}): {e}")
                            raise  # Will be retried by stamina
                        except Exception as e:
                            logger.error(f"Unexpected error for {location.name}: {e}")
                            location_result["response"] = [{"error": f"Unexpected error: {e}"}]
                            break  # Don't retry unexpected errors

            except httpx.HTTPError as e:
                # All retries exhausted
                logger.error(f"All retries exhausted for location {location.name}: {e}")
                location_result["response"] = [{"error": f"All retries exhausted: {e}"}]

            # Store result for this location
            source_id = client.generate_location_source_id(location.lat, location.lon)
            result["details"][source_id] = location_result

        result["observations_extracted"] = total_observations
        logger.info(f"Extracted {total_observations} observations for integration {integration.id}")

        if total_observations == 0:
            result["message"] = f"No observations extracted for integration_id: {str(integration.id)}"

        return result

    except Exception as e:
        message = "pull_observations action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e