import datetime
import hashlib
import logging

from app.services.activity_logger import activity_logger
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi

from .configurations import PullObservationsConfiguration
from .client import OpenWeatherClient


logger = logging.getLogger(__name__)


def _hash_source(lat: float, lon: float) -> str:
    raw = f"{lat},{lon}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _transform_openweather_response(*, payload: dict, name: str, lat: float, lon: float) -> dict:
    recorded_at = datetime.datetime.fromtimestamp(payload.get("dt"), tz=datetime.timezone.utc) if payload.get("dt") else None
    location = {"lat": lat, "lon": lon}
    additional = {
        key: value for key, value in payload.items()
        if key not in {"dt"}
    }

    return {
        "source": _hash_source(lat, lon),
        "source_name": name,
        "type": "stationary-object",
        "subtype": "weather_station",
        "recorded_at": recorded_at,
        "location": location,
        "additional": additional,
    }


@crontab_schedule("*/20 * * * *")
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfiguration):
    logger.info(
        f"Executing pull_observations for integration {integration} with config {action_config.dict(exclude={'api_key'})}"
    )

    result = {"observations_extracted": 0, "details": []}

    ow = OpenWeatherClient(
        api_key=action_config.api_key.get_secret_value(),
        units=action_config.units,
        lang=action_config.lang,
    )

    observations = []
    for loc in action_config.locations:
        try:
            payload = await ow.fetch_current_weather(lat=loc.lat, lon=loc.lon)
        except Exception as e:
            logger.warning(
                f"Failed fetching OpenWeather data for {loc.name} ({loc.lat},{loc.lon}). Error: {e}"
            )
            result["details"].append({"location": loc.name, "error": str(e)})
            continue

        transformed = _transform_openweather_response(
            payload=payload,
            name=loc.name,
            lat=loc.lat,
            lon=loc.lon,
        )
        observations.append(transformed)

    if observations:
        await send_observations_to_gundi(
            observations=observations,
            integration_id=integration.id,
        )
        result["observations_extracted"] = len(observations)
    return result


