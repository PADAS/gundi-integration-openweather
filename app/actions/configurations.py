import pydantic

from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
from app.services.utils import GlobalUISchemaOptions


class LocationConfig(pydantic.BaseModel):
    name: str
    lat: float
    lon: float

    class Config:
        schema_extra = {
            "example": {
                "name": "Guadalajara",
                "lat": 20.659698,
                "lon": -103.349609
            }
        }


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "api_key",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    locations: list[LocationConfig] = pydantic.Field(
        default_factory=list,
        description="List of locations to pull weather data for"
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "locations",
        ],
    )