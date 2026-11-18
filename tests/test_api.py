"""API integration tests against a populated DuckDB warehouse."""

from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from src.api import main as api_main
from src.config import Settings
from src.db import (
    ensure_raw_schema,
    record_pipeline_run,
    upsert_air_quality_records,
    upsert_weather_records,
)
from src.ingestion.open_meteo import AirQualityHourlyRecord, WeatherHourlyRecord
from src.ingestion.run_pipeline import run_dbt_build


def test_api_endpoints_serve_mart_data(tmp_path: Path, monkeypatch) -> None:
    duckdb_file = tmp_path / "api_warehouse.duckdb"
    cfg = Settings(
        environment="test",
        warehouse_backend="duckdb",
        duckdb_path=duckdb_file,
        dbt_project_dir=Path("dbt_project"),
        dbt_profiles_dir=Path("dbt_project"),
    )

    ensure_raw_schema(cfg)
    ts = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
    upsert_weather_records(
        [
            WeatherHourlyRecord(
                city_id="lima-pe",
                city_name="Lima",
                country="Peru",
                region="LATAM",
                latitude=-12.0464,
                longitude=-77.0428,
                observed_at_utc=ts,
                temperature_c=22.5,
                relative_humidity_pct=70.0,
                apparent_temperature_c=22.5,
                precipitation_mm=0.0,
                wind_speed_kmh=14.0,
                surface_pressure_hpa=1012.0,
                cloud_cover_pct=40.0,
                weather_code=2,
                ingested_at_utc=ts,
            )
        ],
        cfg,
    )
    upsert_air_quality_records(
        [
            AirQualityHourlyRecord(
                city_id="lima-pe",
                observed_at_utc=ts,
                pm10=35.0,
                pm2_5=16.0,
                carbon_monoxide=220.0,
                nitrogen_dioxide=18.0,
                ozone=45.0,
                us_aqi=60.0,
                uv_index=5.5,
                ingested_at_utc=ts,
            )
        ],
        cfg,
    )
    record_pipeline_run(
        run_id="run-test-001",
        started_at_utc=ts,
        status="success",
        backend="duckdb",
        weather_rows=1,
        air_quality_rows=1,
        dbt_status="passed",
        settings=cfg,
    )
    assert run_dbt_build(cfg) is True

    monkeypatch.setattr(api_main, "get_settings", lambda: cfg)
    monkeypatch.setattr("src.db.get_settings", lambda: cfg)

    client = TestClient(api_main.app)

    health_res = client.get("/health")
    assert health_res.status_code == 200
    assert health_res.json()["warehouse_ready"] is True

    summary_res = client.get("/api/v1/summary")
    assert summary_res.status_code == 200
    assert summary_res.json()["cities_count"] == 1
    assert summary_res.json()["hourly_observations"] == 1

    cities_res = client.get("/api/v1/cities")
    assert cities_res.status_code == 200
    assert cities_res.json()["cities"][0]["city_name"] == "Lima"

    hourly_res = client.get("/api/v1/hourly?city_id=lima-pe")
    assert hourly_res.status_code == 200
    assert len(hourly_res.json()["records"]) == 1

    runs_res = client.get("/api/v1/pipeline/runs")
    assert runs_res.status_code == 200
    assert runs_res.json()["runs"][0]["run_id"] == "run-test-001"

    html_res = client.get("/")
    assert html_res.status_code == 200
    assert "SkyPulse Data Platform" in html_res.text
