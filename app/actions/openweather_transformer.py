import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

def generate_source_id(lat: float, lon: float) -> str:
    """Generates a unique source ID based on latitude and longitude."""
    unique_string = f"{lat},{lon}".encode("utf-8")
    return hashlib.md5(unique_string).hexdigest()

def transform_openweather_data(data: Dict[str, Any], location_name: str, lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Transforms OpenWeatherMap data into a Gundi observation format."""
    if not data:
        return None

    source_id = generate_source_id(lat, lon)
    recorded_at = datetime.fromtimestamp(data["dt"]).isoformat() + "Z"

    observation = {
        "type": "stationary-object",
        "subtype": "weather_station",
        "source_name": location_name,
        "source": source_id,
        "location": {
            "latitude": lat,
            "longitude": lon,
        },
        "recorded_at": recorded_at,
        "additional": {},
    }

    # Map root fields
    if "main" in data:
        observation["temperature"] = data["main"].get("temp")
        observation["humidity"] = data["main"].get("humidity")
        observation["pressure"] = data["main"].get("pressure")

    if "wind" in data:
        observation["wind_speed"] = data["wind"].get("speed")
        observation["wind_direction"] = data["wind"].get("deg")

    if "weather" in data and data["weather"]:
        observation["weather_condition"] = data["weather"][0].get("description")

    # Place all other fields into 'additional'
    for key, value in data.items():
        if key not in ["coord", "weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod", "rain"]:
            observation["additional"][key] = value
        elif key == "coord": # special handling for coord
            if "lat" in value and "lon" in value:
                # These are already used for the main location, but can be stored in additional if needed.
                # For now, we will skip adding them again to avoid redundancy if lat/lon are primary fields
                pass
        elif key == "sys":
            if "sunrise" in value:
                observation["additional"]["sunrise"] = datetime.fromtimestamp(value["sunrise"]).isoformat() + "Z"
            if "sunset" in value:
                observation["additional"]["sunset"] = datetime.fromtimestamp(value["sunset"]).isoformat() + "Z"

    # Add remaining fields from main into additional if not already mapped as root fields
    if "main" in data:
        for k, v in data["main"].items():
            if k not in ["temp", "humidity", "pressure"]:
                observation["additional"][f"main_{k}"] = v
    
    # Add remaining fields from wind into additional if not already mapped as root fields
    if "wind" in data:
        for k, v in data["wind"].items():
            if k not in ["speed", "deg"]:
                observation["additional"][f"wind_{k}"] = v

    if "rain" in data:
        observation["additional"]["rain_1h"] = data["rain"].get("1h")
        observation["additional"]["rain_3h"] = data["rain"].get("3h")

    if "clouds" in data:
        observation["additional"]["clouds_all"] = data["clouds"].get("all")



    return observation
