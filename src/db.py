"""Warehouse database abstraction supporting DuckDB (local) and PostgreSQL (cloud/Docker)."""

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from src.config import Settings, get_settings
from src.ingestion.open_meteo import AirQualityHourlyRecord, WeatherHourlyRecord

RAW_DDL_STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS raw;",
    "CREATE SCHEMA IF NOT EXISTS analytics;",
    """
    CREATE TABLE IF NOT EXISTS raw.raw_weather_hourly (
        city_id VARCHAR NOT NULL,
        city_name VARCHAR NOT NULL,
        country VARCHAR NOT NULL,
        region VARCHAR NOT NULL,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        observed_at_utc TIMESTAMPTZ NOT NULL,
        temperature_c DOUBLE PRECISION,
        relative_humidity_pct DOUBLE PRECISION,
        apparent_temperature_c DOUBLE PRECISION,
        precipitation_mm DOUBLE PRECISION,
        wind_speed_kmh DOUBLE PRECISION,
        surface_pressure_hpa DOUBLE PRECISION,
        cloud_cover_pct DOUBLE PRECISION,
        weather_code INTEGER,
        ingested_at_utc TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (city_id, observed_at_utc)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.raw_air_quality_hourly (
        city_id VARCHAR NOT NULL,
        observed_at_utc TIMESTAMPTZ NOT NULL,
        pm10 DOUBLE PRECISION,
        pm2_5 DOUBLE PRECISION,
        carbon_monoxide DOUBLE PRECISION,
        nitrogen_dioxide DOUBLE PRECISION,
        ozone DOUBLE PRECISION,
        us_aqi DOUBLE PRECISION,
        uv_index DOUBLE PRECISION,
        ingested_at_utc TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (city_id, observed_at_utc)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.raw_pipeline_runs (
        run_id VARCHAR PRIMARY KEY,
        started_at_utc TIMESTAMPTZ NOT NULL,
        finished_at_utc TIMESTAMPTZ,
        status VARCHAR NOT NULL,
        backend VARCHAR NOT NULL,
        weather_rows INTEGER DEFAULT 0,
        air_quality_rows INTEGER DEFAULT 0,
        dbt_status VARCHAR,
        notes VARCHAR
    );
    """,
)

WEATHER_UPSERT_SQL = """
INSERT INTO raw.raw_weather_hourly (
    city_id, city_name, country, region, latitude, longitude,
    observed_at_utc, temperature_c, relative_humidity_pct,
    apparent_temperature_c, precipitation_mm, wind_speed_kmh,
    surface_pressure_hpa, cloud_cover_pct, weather_code, ingested_at_utc
)
VALUES ({placeholders})
ON CONFLICT (city_id, observed_at_utc) DO UPDATE SET
    city_name = EXCLUDED.city_name,
    country = EXCLUDED.country,
    region = EXCLUDED.region,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    temperature_c = EXCLUDED.temperature_c,
    relative_humidity_pct = EXCLUDED.relative_humidity_pct,
    apparent_temperature_c = EXCLUDED.apparent_temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
    surface_pressure_hpa = EXCLUDED.surface_pressure_hpa,
    cloud_cover_pct = EXCLUDED.cloud_cover_pct,
    weather_code = EXCLUDED.weather_code,
    ingested_at_utc = EXCLUDED.ingested_at_utc;
"""

AQ_UPSERT_SQL = """
INSERT INTO raw.raw_air_quality_hourly (
    city_id, observed_at_utc, pm10, pm2_5, carbon_monoxide,
    nitrogen_dioxide, ozone, us_aqi, uv_index, ingested_at_utc
)
VALUES ({placeholders})
ON CONFLICT (city_id, observed_at_utc) DO UPDATE SET
    pm10 = EXCLUDED.pm10,
    pm2_5 = EXCLUDED.pm2_5,
    carbon_monoxide = EXCLUDED.carbon_monoxide,
    nitrogen_dioxide = EXCLUDED.nitrogen_dioxide,
    ozone = EXCLUDED.ozone,
    us_aqi = EXCLUDED.us_aqi,
    uv_index = EXCLUDED.uv_index,
    ingested_at_utc = EXCLUDED.ingested_at_utc;
"""


def _ensure_duckdb_parent(duckdb_path: Path) -> None:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_connection(settings: Settings | None = None) -> Iterator[tuple[str, Any]]:
    """Yield (backend, connection) for the configured warehouse."""
    cfg = settings or get_settings()
    backend = cfg.resolved_backend
    if backend == "postgres":
        conn = psycopg2.connect(cfg.normalized_postgres_url)
        try:
            yield backend, conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        _ensure_duckdb_parent(cfg.duckdb_path)
        conn = duckdb.connect(str(cfg.duckdb_path))
        try:
            yield backend, conn
            conn.commit()
        finally:
            conn.close()


def ensure_raw_schema(settings: Settings | None = None) -> None:
    """Create raw and analytics schemas and raw landing tables if missing."""
    with get_connection(settings) as (backend, conn):
        if backend == "postgres":
            with conn.cursor() as cur:
                for stmt in RAW_DDL_STATEMENTS:
                    cur.execute(stmt)
        else:
            for stmt in RAW_DDL_STATEMENTS:
                conn.execute(stmt)


def upsert_weather_records(
    records: list[WeatherHourlyRecord],
    settings: Settings | None = None,
) -> int:
    """Idempotently upsert weather hourly records into raw.raw_weather_hourly."""
    if not records:
        return 0

    rows = [
        (
            r.city_id,
            r.city_name,
            r.country,
            r.region,
            r.latitude,
            r.longitude,
            r.observed_at_utc,
            r.temperature_c,
            r.relative_humidity_pct,
            r.apparent_temperature_c,
            r.precipitation_mm,
            r.wind_speed_kmh,
            r.surface_pressure_hpa,
            r.cloud_cover_pct,
            r.weather_code,
            r.ingested_at_utc,
        )
        for r in records
    ]

    with get_connection(settings) as (backend, conn):
        if backend == "postgres":
            sql = WEATHER_UPSERT_SQL.replace("VALUES ({placeholders})", "VALUES %s")
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=500)
        else:
            placeholders = ", ".join(["?"] * 16)
            sql = WEATHER_UPSERT_SQL.format(placeholders=placeholders)
            conn.executemany(sql, rows)

    return len(rows)


def upsert_air_quality_records(
    records: list[AirQualityHourlyRecord],
    settings: Settings | None = None,
) -> int:
    """Idempotently upsert air quality records into raw.raw_air_quality_hourly."""
    if not records:
        return 0

    rows = [
        (
            r.city_id,
            r.observed_at_utc,
            r.pm10,
            r.pm2_5,
            r.carbon_monoxide,
            r.nitrogen_dioxide,
            r.ozone,
            r.us_aqi,
            r.uv_index,
            r.ingested_at_utc,
        )
        for r in records
    ]

    with get_connection(settings) as (backend, conn):
        if backend == "postgres":
            sql = AQ_UPSERT_SQL.replace("VALUES ({placeholders})", "VALUES %s")
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=500)
        else:
            placeholders = ", ".join(["?"] * 10)
            sql = AQ_UPSERT_SQL.format(placeholders=placeholders)
            conn.executemany(sql, rows)

    return len(rows)


def record_pipeline_run(
    run_id: str,
    started_at_utc: datetime,
    status: str,
    backend: str,
    weather_rows: int = 0,
    air_quality_rows: int = 0,
    dbt_status: str | None = None,
    notes: str | None = None,
    finished_at_utc: datetime | None = None,
    settings: Settings | None = None,
) -> None:
    """Persist pipeline run telemetry in raw.raw_pipeline_runs."""
    finished = finished_at_utc or datetime.now(timezone.utc)
    params = (
        run_id,
        started_at_utc,
        finished,
        status,
        backend,
        weather_rows,
        air_quality_rows,
        dbt_status,
        notes,
    )
    upsert_pg = """
    INSERT INTO raw.raw_pipeline_runs (
        run_id, started_at_utc, finished_at_utc, status, backend,
        weather_rows, air_quality_rows, dbt_status, notes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id) DO UPDATE SET
        finished_at_utc = EXCLUDED.finished_at_utc,
        status = EXCLUDED.status,
        weather_rows = EXCLUDED.weather_rows,
        air_quality_rows = EXCLUDED.air_quality_rows,
        dbt_status = EXCLUDED.dbt_status,
        notes = EXCLUDED.notes;
    """
    upsert_duck = upsert_pg.replace("%s", "?")

    with get_connection(settings) as (resolved, conn):
        if resolved == "postgres":
            with conn.cursor() as cur:
                cur.execute(upsert_pg, params)
        else:
            conn.execute(upsert_duck, params)


def query_dicts(
    sql: str,
    params: tuple[Any, ...] = (),
    settings: Settings | None = None,
    read_only: bool = True,
) -> list[dict[str, Any]]:
    """Execute a SELECT query using %s placeholders and return list of dicts."""
    cfg = settings or get_settings()
    backend = cfg.resolved_backend

    if backend == "postgres":
        with get_connection(cfg) as (_, conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    _ensure_duckdb_parent(cfg.duckdb_path)
    if read_only and not cfg.duckdb_path.exists():
        return []
    conn = duckdb.connect(str(cfg.duckdb_path), read_only=read_only and cfg.duckdb_path.exists())
    try:
        duck_sql = sql.replace("%s", "?")
        cursor = conn.execute(duck_sql, params)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()
