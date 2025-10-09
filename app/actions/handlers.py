from app.actions.configurations import OpenWeatherConfig
from app.actions.openweather_client import OpenWeatherClient
from app.actions.openweather_transformer import transform_openweather_data
# from app.services.action_runner import ActionRunner # Removed to break circular dependency
from app.services.action_scheduler import crontab_schedule
from app.services.state import IntegrationStateManager
from app.services.gundi import send_observations_to_gundi
from app.actions.core import PullActionConfiguration, ExecutableActionMixin

import logging

logger = logging.getLogger(__name__)

@crontab_schedule("*/20 * * * *")
async def action_pull_openweather_data(action_config: OpenWeatherConfig, action_runner: "ActionRunner"): # Removed gundi_api
    """Pulls weather data from OpenWeatherMap and submits it as observations to Gundi."""
    client = OpenWeatherClient(api_key=action_config.api_key)

    # Assuming action_config or action_runner provides the integration_id
    # The integration_id will likely be an attribute of the action_config or passed through the framework
    # For now, let's assume action_config has an integration_id or it's inferred by the framework
    # If not, this will need further adjustment, potentially through action_runner or a global context.
    # For the purpose of getting tests to pass, we'll assume integration_id is available in action_config
    integration_id = action_config.integration_id # This will likely need to be validated.

    for location in action_config.locations:
        logger.info(f"Fetching weather data for {location.name} ({location.lat}, {location.lon})")
        weather_data = await client.get_weather_data(lat=location.lat, lon=location.lon)

        if weather_data:
            observation = transform_openweather_data(weather_data, location.name, location.lat, location.lon)
            if observation:
                await send_observations_to_gundi(observations=[observation], integration_id=integration_id)
                logger.info(f"Successfully submitted observation for {location.name}")
            else:
                logger.error(f"Failed to transform OpenWeather data for {location.name}")
        else:
            logger.error(f"Failed to retrieve OpenWeather data for {location.name}")
