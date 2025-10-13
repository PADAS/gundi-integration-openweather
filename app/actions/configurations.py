import pydantic
from typing import List
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin


class Location(pydantic.BaseModel):
    """A named location with coordinates for weather data collection."""
    name: str = pydantic.Field(
        ...,
        description="A descriptive name for this location (e.g., 'Nairobi HQ', 'Serengeti Camp')"
    )
    lat: float = pydantic.Field(
        ...,
        ge=-90,
        le=90,
        description="Latitude in decimal degrees (-90 to 90)"
    )
    lon: float = pydantic.Field(
        ...,
        ge=-180,
        le=180,
        description="Longitude in decimal degrees (-180 to 180)"
    )


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    """Configuration for authenticating with OpenWeather API."""
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        format="password",
        title="OpenWeather API Key",
        description="Your OpenWeather API key. Get one at https://openweathermap.org/api",
        ui_options=UIOptions(
            widget="password",
        )
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["api_key"],
    )


class PullObservationsConfig(PullActionConfiguration):
    """Configuration for pulling weather observations from OpenWeather."""
    locations: List[Location] = FieldWithUIOptions(
        ...,
        title="Weather Monitoring Locations",
        description="List of locations to monitor for weather data",
        min_items=1,
    )
    units: str = FieldWithUIOptions(
        "metric",
        title="Units System",
        description="Units of measurement: metric (Celsius, m/s), imperial (Fahrenheit, mph), or standard (Kelvin, m/s)",
        ui_options=UIOptions(
            widget="select",
        ),
    )

    @pydantic.validator('units')
    def validate_units(cls, v):
        allowed_units = ["metric", "imperial", "standard"]
        if v not in allowed_units:
            raise ValueError(f"Units must be one of: {', '.join(allowed_units)}")
        return v

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["locations", "units"],
    )
