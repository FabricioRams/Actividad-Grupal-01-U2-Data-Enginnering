"""End-to-end test: synthetic Open-Meteo records -> DuckDB raw -> dbt build (run + test)."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config import Settings
from src.db import (
    ensure_raw_schema,
    query_dicts,
    upsert_air_quality_records,
    upsert_weather_records,
)
from src.ingestion.open_meteo import AirQualityHourlyRecord, WeatherHourlyRecord
from src.ingestion.run_pipeline import run_dbt_build


def test_end_to_end_duckdb_and_dbt_build(tmp_path: Path) -> None:
    duckdb_file = tmp_path / "test_warehouse.duckdb"
    cfg = Settings(
        environment="test",
        warehouse_backend="duckdb",
        duckdb_path=duckdb_file,
        dbt_project_dir=Path("dbt_project"),
        dbt_profiles_dir=Path("dbt_project"),
    )

    ensure_raw_schema(cfg)
    base_ts = datetime(2026, 11, 17, 0, 0, tzinfo=timezone.utc)

    weather_rows = []
    aq_rows = []
    for hour in range(24):
        ts = base_ts + timedelta(hours=hour)
        # Create one thermal + AQI anomaly spike at hour 14
        temp = 36.5 if hour == 14 else 20.0 + (hour % 5) * 0.4
        aqi = 125.0 if hour == 14 else 38.0
        weather_rows.append(
            WeatherHourlyRecord(
                city_id="tacna-pe",
                city_name="Tacna",
                country="Peru",
                region="LATAM",
                latitude=-18.0146,
                longitude=-70.2536,
                observed_at_utc=ts,
                temperature_c=temp,
                relative_humidity_pct=55.0,
                apparent_temperature_c=temp,
                precipitation_mm=0.0,
                wind_speed_kmh=12.0,
                surface_pressure_hpa=1013.0,
                cloud_cover_pct=15.0,
                weather_code=0,
                ingested_at_utc=base_ts,
            )
        )
        aq_rows.append(
            AirQualityHourlyRecord(
                city_id="tacna-pe",
                observed_at_utc=ts,
                pm10=32.0,
                pm2_5=18.0,
                carbon_monoxide=190.0,
                nitrogen_dioxide=14.0,
                ozone=42.0,
                us_aqi=aqi,
                uv_index=3.0,
                ingested_at_utc=base_ts,
            )
        )

    assert upsert_weather_records(weather_rows, cfg) == 24
    assert upsert_air_quality_records(aq_rows, cfg) == 24

    # Verify idempotent upsert does not duplicate rows
    assert upsert_weather_records(weather_rows, cfg) == 24

    assert run_dbt_build(cfg) is True

    cities = query_dicts("SELECT * FROM marts.dim_cities;", settings=cfg)
    assert len(cities) == 1
    assert cities[0]["city_id"] == "tacna-pe"

    daily = query_dicts("SELECT * FROM marts.mart_city_daily_summary;", settings=cfg)
    assert len(daily) == 1
    assert daily[0]["observations_count"] == 24

    anomalies = query_dicts("SELECT * FROM marts.mart_climate_anomalies;", settings=cfg)
    assert len(anomalies) >= 1
    assert anomalies[0]["anomaly_type"] == "Air Quality Alert"
