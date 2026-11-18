"""Resilient Open-Meteo Weather & Air Quality API extractor."""

from datetime import datetime, timezone
from typing import Any

import httpx
from pydantic import BaseModel, Field
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.ingestion.cities import MONITORED_CITIES, MonitoredCity

WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


class WeatherHourlyRecord(BaseModel):
    """Validated hourly weather telemetry row."""

    city_id: str
    city_name: str
    country: str
    region: str
    latitude: float
    longitude: float
    observed_at_utc: datetime
    temperature_c: float | None = None
    relative_humidity_pct: float | None = Field(default=None, ge=0, le=100)
    apparent_temperature_c: float | None = None
    precipitation_mm: float | None = Field(default=None, ge=0)
    wind_speed_kmh: float | None = Field(default=None, ge=0)
    surface_pressure_hpa: float | None = None
    cloud_cover_pct: float | None = Field(default=None, ge=0, le=100)
    weather_code: int | None = None
    ingested_at_utc: datetime


class AirQualityHourlyRecord(BaseModel):
    """Validated hourly air quality telemetry row."""

    city_id: str
    observed_at_utc: datetime
    pm10: float | None = Field(default=None, ge=0)
    pm2_5: float | None = Field(default=None, ge=0)
    carbon_monoxide: float | None = Field(default=None, ge=0)
    nitrogen_dioxide: float | None = Field(default=None, ge=0)
    ozone: float | None = Field(default=None, ge=0)
    us_aqi: float | None = Field(default=None, ge=0)
    uv_index: float | None = Field(default=None, ge=0)
    ingested_at_utc: datetime


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    reraise=True,
)
def _fetch_json(client: httpx.Client, url: str, params: dict[str, Any]) -> dict[str, Any]:
    """Perform an HTTP GET with exponential backoff retries."""
    response = client.get(url, params=params)
    response.raise_for_status()
    return response.json()


def _parse_utc_timestamp(ts: str) -> datetime:
    """Parse ISO-8601 timestamp string returned by Open-Meteo into UTC datetime."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_city_weather(
    client: httpx.Client,
    city: MonitoredCity,
    past_days: int = 2,
    forecast_days: int = 3,
    ingested_at: datetime | None = None,
) -> list[WeatherHourlyRecord]:
    """Fetch and validate hourly weather metrics for a single city."""
    now = ingested_at or datetime.now(timezone.utc)
    payload = _fetch_json(
        client,
        WEATHER_API_URL,
        {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "hourly": (
                "temperature_2m,relative_humidity_2m,apparent_temperature,"
                "precipitation,wind_speed_10m,surface_pressure,cloud_cover,weather_code"
            ),
            "timezone": "UTC",
            "past_days": past_days,
            "forecast_days": forecast_days,
        },
    )
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])

    records: list[WeatherHourlyRecord] = []
    for idx, ts in enumerate(times):
        records.append(
            WeatherHourlyRecord(
                city_id=city.city_id,
                city_name=city.city_name,
                country=city.country,
                region=city.region,
                latitude=city.latitude,
                longitude=city.longitude,
                observed_at_utc=_parse_utc_timestamp(ts),
                temperature_c=_safe_get(hourly, "temperature_2m", idx),
                relative_humidity_pct=_safe_get(hourly, "relative_humidity_2m", idx),
                apparent_temperature_c=_safe_get(hourly, "apparent_temperature", idx),
                precipitation_mm=_safe_get(hourly, "precipitation", idx),
                wind_speed_kmh=_safe_get(hourly, "wind_speed_10m", idx),
                surface_pressure_hpa=_safe_get(hourly, "surface_pressure", idx),
                cloud_cover_pct=_safe_get(hourly, "cloud_cover", idx),
                weather_code=_safe_get(hourly, "weather_code", idx),
                ingested_at_utc=now,
            )
        )
    return records


def extract_city_air_quality(
    client: httpx.Client,
    city: MonitoredCity,
    past_days: int = 2,
    forecast_days: int = 3,
    ingested_at: datetime | None = None,
) -> list[AirQualityHourlyRecord]:
    """Fetch and validate hourly air quality metrics for a single city."""
    now = ingested_at or datetime.now(timezone.utc)
    payload = _fetch_json(
        client,
        AIR_QUALITY_API_URL,
        {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,us_aqi,uv_index",
            "timezone": "UTC",
            "past_days": past_days,
            "forecast_days": forecast_days,
        },
    )
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])

    records: list[AirQualityHourlyRecord] = []
    for idx, ts in enumerate(times):
        records.append(
            AirQualityHourlyRecord(
                city_id=city.city_id,
                observed_at_utc=_parse_utc_timestamp(ts),
                pm10=_safe_get(hourly, "pm10", idx),
                pm2_5=_safe_get(hourly, "pm2_5", idx),
                carbon_monoxide=_safe_get(hourly, "carbon_monoxide", idx),
                nitrogen_dioxide=_safe_get(hourly, "nitrogen_dioxide", idx),
                ozone=_safe_get(hourly, "ozone", idx),
                us_aqi=_safe_get(hourly, "us_aqi", idx),
                uv_index=_safe_get(hourly, "uv_index", idx),
                ingested_at_utc=now,
            )
        )
    return records


def extract_all_cities(
    cities: tuple[MonitoredCity, ...] = MONITORED_CITIES,
    past_days: int = 2,
    forecast_days: int = 3,
    timeout_seconds: float = 20.0,
) -> tuple[list[WeatherHourlyRecord], list[AirQualityHourlyRecord]]:
    """Extract weather and air quality records for all monitored cities."""
    ingested_at = datetime.now(timezone.utc)
    weather_records: list[WeatherHourlyRecord] = []
    aq_records: list[AirQualityHourlyRecord] = []

    with httpx.Client(timeout=timeout_seconds) as client:
        for city in cities:
            weather_records.extend(
                extract_city_weather(
                    client,
                    city,
                    past_days=past_days,
                    forecast_days=forecast_days,
                    ingested_at=ingested_at,
                )
            )
            aq_records.extend(
                extract_city_air_quality(
                    client,
                    city,
                    past_days=past_days,
                    forecast_days=forecast_days,
                    ingested_at=ingested_at,
                )
            )
    return weather_records, aq_records


def _safe_get(hourly: dict[str, list[Any]], key: str, idx: int) -> Any:
    """Safely extract value at index from Open-Meteo column arrays."""
    values = hourly.get(key)
    if not values or idx >= len(values):
        return None
    return values[idx]
