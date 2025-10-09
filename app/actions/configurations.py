import pydantic

from .core import PullActionConfiguration
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions


class NamedLocation(pydantic.BaseModel):
    name: str = FieldWithUIOptions(
        ...,
        title="Location name",
        description="Human-friendly name for the location",
    )
    lat: float = FieldWithUIOptions(
        ...,
        title="Latitude",
        description="Latitude in decimal degrees",
        ge=-90,
        le=90,
    )
    lon: float = FieldWithUIOptions(
        ...,
        title="Longitude",
        description="Longitude in decimal degrees",
        ge=-180,
        le=180,
    )


class PullObservationsConfiguration(PullActionConfiguration):
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="OpenWeather API Key",
        description="API key from OpenWeather",
        ui_options=UIOptions(widget="password"),
    )
    units: pydantic.constr(strip_whitespace=True) = FieldWithUIOptions(
        "metric",
        title="Units",
        description="Units system (metric, imperial, or standard)",
        regex=r"^(metric|imperial|standard)$",
    )
    lang: pydantic.constr(strip_whitespace=True) = FieldWithUIOptions(
        "en",
        title="Language",
        description="Language code (e.g., en, es)",
        min_length=2,
        max_length=5,
    )
    locations: list[NamedLocation] = FieldWithUIOptions(
        default_factory=list,
        title="Locations",
        description="List of named locations to poll",
        min_items=1,
        ui_options=UIOptions(orderable=True, addable=True, removable=True, copyable=True),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "api_key",
            "units",
            "lang",
            "locations",
        ],
    )


