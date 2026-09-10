"""Real kickoff-weather lookups for outdoor stadiums via the free Open-Meteo API.

No API key required. Domed/retractable-roof stadiums are reported as indoor
conditions (no wind/precip) rather than calling out to the weather API at all.
"""
import time
from datetime import datetime, timedelta

import requests

from ..config import logger

# lat/lon of each team's home stadium, plus roof type.
# roof: "outdoor" | "dome" | "retractable" (retractable is treated as a dome —
# games are overwhelmingly played closed in bad weather).
STADIUMS = {
    "ARI": {"lat": 33.5276, "lon": -112.2626, "roof": "retractable"},
    "ATL": {"lat": 33.7554, "lon": -84.4008, "roof": "dome"},
    "BAL": {"lat": 39.2780, "lon": -76.6227, "roof": "outdoor"},
    "BUF": {"lat": 42.7738, "lon": -78.7870, "roof": "outdoor"},
    "CAR": {"lat": 35.2258, "lon": -80.8528, "roof": "outdoor"},
    "CHI": {"lat": 41.8623, "lon": -87.6167, "roof": "outdoor"},
    "CIN": {"lat": 39.0955, "lon": -84.5160, "roof": "outdoor"},
    "CLE": {"lat": 41.5061, "lon": -81.6995, "roof": "outdoor"},
    "DAL": {"lat": 32.7473, "lon": -97.0945, "roof": "retractable"},
    "DEN": {"lat": 39.7439, "lon": -105.0201, "roof": "outdoor"},
    "DET": {"lat": 42.3400, "lon": -83.0456, "roof": "dome"},
    "GB": {"lat": 44.5013, "lon": -88.0622, "roof": "outdoor"},
    "HOU": {"lat": 29.6847, "lon": -95.4107, "roof": "retractable"},
    "IND": {"lat": 39.7601, "lon": -86.1639, "roof": "retractable"},
    "JAX": {"lat": 30.3239, "lon": -81.6373, "roof": "outdoor"},
    "KC": {"lat": 39.0489, "lon": -94.4839, "roof": "outdoor"},
    "LA": {"lat": 33.9535, "lon": -118.3392, "roof": "dome"},
    "LAC": {"lat": 33.9535, "lon": -118.3392, "roof": "dome"},
    "LV": {"lat": 36.0909, "lon": -115.1833, "roof": "dome"},
    "MIA": {"lat": 25.9580, "lon": -80.2389, "roof": "outdoor"},
    "MIN": {"lat": 44.9738, "lon": -93.2577, "roof": "dome"},
    "NE": {"lat": 42.0909, "lon": -71.2643, "roof": "outdoor"},
    "NO": {"lat": 29.9511, "lon": -90.0812, "roof": "dome"},
    "NYG": {"lat": 40.8135, "lon": -74.0745, "roof": "outdoor"},
    "NYJ": {"lat": 40.8135, "lon": -74.0745, "roof": "outdoor"},
    "PHI": {"lat": 39.9008, "lon": -75.1675, "roof": "outdoor"},
    "PIT": {"lat": 40.4468, "lon": -80.0158, "roof": "outdoor"},
    "SEA": {"lat": 47.5952, "lon": -122.3316, "roof": "outdoor"},
    "SF": {"lat": 37.4032, "lon": -121.9698, "roof": "outdoor"},
    "TB": {"lat": 27.9759, "lon": -82.5033, "roof": "outdoor"},
    "TEN": {"lat": 36.1665, "lon": -86.7713, "roof": "outdoor"},
    "WAS": {"lat": 38.9078, "lon": -76.8645, "roof": "outdoor"},
}

# WMO weather code -> short human label (Open-Meteo uses WMO codes).
_WMO_LABELS = {
    0: "Clear", 1: "Mostly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow",
    80: "Rain showers", 81: "Rain showers", 82: "Heavy rain showers",
    95: "Thunderstorms", 96: "Thunderstorms", 99: "Thunderstorms",
}

_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_SECONDS = 3600


def _dome_response(roof: str) -> dict:
    return {
        "is_dome": True,
        "roof": roof,
        "temp_f": 72,
        "wind_mph": 0,
        "condition": "Dome",
        "precip_chance": 0,
    }


def get_game_weather(home_team: str, gameday: str | None) -> dict | None:
    """Kickoff-window weather for the home team's stadium on `gameday` (YYYY-MM-DD).

    Returns None if the team/venue is unknown. Dome/retractable venues short-circuit
    to a fixed indoor reading without calling out to the API.
    """
    venue = STADIUMS.get(home_team)
    if not venue:
        return None
    if venue["roof"] != "outdoor":
        return _dome_response(venue["roof"])

    if not gameday:
        return None

    cache_key = f"{home_team}:{gameday}"
    cached = _CACHE.get(cache_key)
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    try:
        game_date = datetime.strptime(gameday, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None

    # Open-Meteo's free forecast endpoint covers ~16 days out; beyond that (or
    # for past dates) fall back to its historical-archive endpoint.
    today = datetime.utcnow().date()
    days_out = (game_date - today).days
    use_archive = days_out < -1 or days_out > 15

    base = "https://archive-api.open-meteo.com/v1/archive" if use_archive else "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": venue["lat"],
        "longitude": venue["lon"],
        "start_date": gameday,
        "end_date": gameday,
        "hourly": "temperature_2m,wind_speed_10m,precipitation_probability,weather_code",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "auto",
    }

    try:
        resp = requests.get(base, params=params, timeout=6)
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        if not times:
            return None

        # Sample the mid-afternoon/evening slot (kickoff windows cluster there)
        # rather than midnight, so single-value display is representative.
        target_hour = f"{gameday}T16:00"
        idx = times.index(target_hour) if target_hour in times else len(times) // 2

        temps = hourly.get("temperature_2m", [])
        winds = hourly.get("wind_speed_10m", [])
        precs = hourly.get("precipitation_probability", [])
        codes = hourly.get("weather_code", [])

        result = {
            "is_dome": False,
            "roof": "outdoor",
            "temp_f": round(temps[idx]) if idx < len(temps) and temps[idx] is not None else None,
            "wind_mph": round(winds[idx]) if idx < len(winds) and winds[idx] is not None else None,
            "condition": _WMO_LABELS.get(codes[idx], "—") if idx < len(codes) else "—",
            "precip_chance": precs[idx] if idx < len(precs) else None,
        }
        _CACHE[cache_key] = (now, result)
        return result
    except Exception as e:
        logger.warning("Weather lookup failed for %s on %s: %s", home_team, gameday, e)
        return None
