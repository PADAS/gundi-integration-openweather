import pydantic
from typing import List, Literal

from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
from app.services.utils import GlobalUISchemaOptions, FieldWithUIOptions, UIOptions


class Location(pydantic.BaseModel):
    """A named location with coordinates."""
    name: str = pydantic.Field(..., description="Name for this location")
    lat: float = pydantic.Field(..., description="Latitude", ge=-90, le=90)
    lon: float = pydantic.Field(..., description="Longitude", ge=-180, le=180)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        format="password",
        title="API Key",
        description="API key for OpenWeatherMap API.",
        ui_options=UIOptions(
            widget="password",
        )
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "api_key",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    locations: List[Location] = FieldWithUIOptions(
        default_factory=list,
        title="Locations",
        description="List of locations to fetch weather data for. Each location needs a name, latitude, and longitude.",
    )
    units: Literal["metric", "imperial", "standard"] = FieldWithUIOptions(
        "metric",
        title="Units",
        description="Units for temperature and wind speed. Metric = Celsius/m/s, Imperial = Fahrenheit/mph, Standard = Kelvin/m/s.",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "locations",
            "units",
        ],
    )

