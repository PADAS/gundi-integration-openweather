from pydantic import Field
from typing import Dict, List, Optional
from gundi_core.schemas.v2 import IntegrationActionConfiguration


class OpenWeatherLocation(IntegrationActionConfiguration):
    name: str = Field(..., description="A user-friendly name for the location")
    lat: float = Field(..., description="Latitude of the location")
    lon: float = Field(..., description="Longitude of the location")


class OpenWeatherConfig(IntegrationActionConfiguration):
    locations: List[OpenWeatherLocation] = Field(
        default_factory=list, description="List of named locations for OpenWeather data collection"
    )
    api_key: str = Field(..., description="API key for OpenWeatherMap")
