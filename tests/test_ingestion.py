"""Unit tests for Open-Meteo extraction and Pydantic validation."""

from datetime import datetime, timezone

import httpx

from src.ingestion.cities import MonitoredCity
from src.ingestion.open_meteo import extract_city_air_quality, extract_city_weather


def test_extract_city_weather_parses_hourly_payload() -> None:
    city = MonitoredCity("tacna-pe", "Tacna", "Peru", "LATAM", -18.0146, -70.2536, "America/Lima")

    def handler(request: httpx.Request) -> httpx.Response:
        assert "api.open-meteo.com" in str(request.url)
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "time": ["2026-11-17T00:00", "2026-11-17T01:00"],
                    "temperature_2m": [21.4, 20.8],
                    "relative_humidity_2m": [62.0, 65.0],
                    "apparent_temperature": [21.0, 20.4],
                    "precipitation": [0.0, 0.2],
                    "wind_speed_10m": [11.5, 9.8],
                    "surface_pressure": [1014.2, 1014.5],
                    "cloud_cover": [20.0, 35.0],
                    "weather_code": [1, 2],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        rows = extract_city_weather(
            client,
            city,
            ingested_at=datetime(2026, 11, 17, 2, 0, tzinfo=timezone.utc),
        )

    assert len(rows) == 2
    assert rows[0].city_id == "tacna-pe"
    assert rows[0].temperature_c == 21.4
    assert rows[1].precipitation_mm == 0.2
    assert rows[1].weather_code == 2


def test_extract_city_air_quality_parses_hourly_payload() -> None:
    city = MonitoredCity("lima-pe", "Lima", "Peru", "LATAM", -12.0464, -77.0428, "America/Lima")

    def handler(request: httpx.Request) -> httpx.Response:
        assert "air-quality-api.open-meteo.com" in str(request.url)
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "time": ["2026-11-17T00:00"],
                    "pm10": [28.5],
                    "pm2_5": [14.2],
                    "carbon_monoxide": [210.0],
                    "nitrogen_dioxide": [19.4],
                    "ozone": [48.1],
                    "us_aqi": [55.0],
                    "uv_index": [0.0],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        rows = extract_city_air_quality(
            client,
            city,
            ingested_at=datetime(2026, 11, 17, 2, 0, tzinfo=timezone.utc),
        )

    assert len(rows) == 1
    assert rows[0].city_id == "lima-pe"
    assert rows[0].us_aqi == 55.0
    assert rows[0].pm2_5 == 14.2
