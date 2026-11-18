"""FastAPI analytics API and interactive dashboard for SkyPulse."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from src.config import get_settings
from src.db import query_dicts
from src.ingestion.run_pipeline import run_elt_pipeline

logger = logging.getLogger("skypulse.api")

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert dates, datetimes, and Decimals to JSON-friendly primitives."""
    out: dict[str, Any] = {}
    for key, val in row.items():
        if isinstance(val, (datetime, date)):
            out[key] = val.isoformat()
        elif hasattr(val, "__float__") and not isinstance(val, (int, float, bool)):
            out[key] = float(val)
        else:
            out[key] = val
    return out


def _warehouse_has_marts() -> bool:
    """Return True if marts.dim_cities exists and has rows."""
    try:
        rows = query_dicts("SELECT COUNT(*) AS cnt FROM marts.dim_cities;")
        return bool(rows and int(rows[0]["cnt"]) > 0)
    except Exception:
        return False


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Bootstrap initial ELT run on startup if configured and warehouse is empty."""
    auto_bootstrap = os.getenv("AUTO_BOOTSTRAP_ON_STARTUP", "false").lower() == "true"
    if auto_bootstrap and not _warehouse_has_marts():
        try:
            logger.info("Warehouse empty at startup; running initial ELT bootstrap...")
            run_elt_pipeline(get_settings())
        except Exception:
            logger.exception("Startup ELT bootstrap failed; service will remain online.")
    yield


app = FastAPI(
    title="SkyPulse Data Engineering Platform",
    description=(
        "Cloud-native Weather & Air Quality ELT API powered by Apache Airflow, "
        "dbt, DuckDB/PostgreSQL, and FastAPI (Zero AWS)."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> HTMLResponse:
    """Serve the interactive SkyPulse analytics dashboard."""
    html_path = TEMPLATES_DIR / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> dict[str, Any]:
    """Liveness and warehouse readiness check."""
    cfg = get_settings()
    ready = _warehouse_has_marts()
    return {
        "status": "ok",
        "environment": cfg.environment,
        "backend": cfg.resolved_backend,
        "warehouse_ready": ready,
    }


@app.get("/api/v1/summary")
def get_summary() -> dict[str, Any]:
    """Return global KPIs and latest ELT pipeline run telemetry."""
    if not _warehouse_has_marts():
        return {
            "warehouse_ready": False,
            "cities_count": 0,
            "hourly_observations": 0,
            "anomalies_count": 0,
            "avg_temperature_c": None,
            "avg_us_aqi": None,
            "avg_comfort_index": None,
            "latest_run": None,
        }

    kpis = query_dicts(
        """
        SELECT
            COUNT(DISTINCT city_id) AS cities_count,
            COUNT(*) AS hourly_observations,
            ROUND(CAST(AVG(temperature_c) AS NUMERIC), 2) AS avg_temperature_c,
            ROUND(CAST(AVG(us_aqi) AS NUMERIC), 1) AS avg_us_aqi,
            ROUND(CAST(AVG(comfort_index_score) AS NUMERIC), 1) AS avg_comfort_index
        FROM marts.fct_city_hourly_metrics;
        """
    )
    anomalies = query_dicts("SELECT COUNT(*) AS cnt FROM marts.mart_climate_anomalies;")
    runs = query_dicts(
        """
        SELECT run_id, started_at_utc, finished_at_utc, status, backend,
               weather_rows, air_quality_rows, dbt_status
        FROM raw.raw_pipeline_runs
        ORDER BY started_at_utc DESC
        LIMIT 1;
        """
    )
    row = _serialize_row(kpis[0]) if kpis else {}
    return {
        "warehouse_ready": True,
        "cities_count": int(row.get("cities_count") or 0),
        "hourly_observations": int(row.get("hourly_observations") or 0),
        "anomalies_count": int(anomalies[0]["cnt"]) if anomalies else 0,
        "avg_temperature_c": row.get("avg_temperature_c"),
        "avg_us_aqi": row.get("avg_us_aqi"),
        "avg_comfort_index": row.get("avg_comfort_index"),
        "latest_run": _serialize_row(runs[0]) if runs else None,
    }


@app.get("/api/v1/cities")
def get_cities() -> dict[str, Any]:
    """Return monitored cities with latest aggregated daily KPIs."""
    if not _warehouse_has_marts():
        return {"cities": []}

    rows = query_dicts(
        """
        SELECT
            c.city_id,
            c.city_name,
            c.country,
            c.region,
            c.latitude,
            c.longitude,
            ROUND(CAST(AVG(d.avg_temperature_c) AS NUMERIC), 2) AS avg_temperature_c,
            ROUND(CAST(MAX(d.max_temperature_c) AS NUMERIC), 2) AS max_temperature_c,
            ROUND(CAST(MIN(d.min_temperature_c) AS NUMERIC), 2) AS min_temperature_c,
            ROUND(CAST(AVG(d.avg_us_aqi) AS NUMERIC), 1) AS avg_us_aqi,
            ROUND(CAST(AVG(d.avg_pm2_5) AS NUMERIC), 2) AS avg_pm2_5,
            ROUND(CAST(AVG(d.avg_comfort_index) AS NUMERIC), 1) AS avg_comfort_index,
            SUM(d.observations_count) AS observations_count
        FROM marts.dim_cities AS c
        LEFT JOIN marts.mart_city_daily_summary AS d
            ON c.city_id = d.city_id
        GROUP BY c.city_id, c.city_name, c.country, c.region, c.latitude, c.longitude
        ORDER BY avg_comfort_index DESC NULLS LAST;
        """
    )
    return {"cities": [_serialize_row(r) for r in rows]}


@app.get("/api/v1/hourly")
def get_hourly_metrics(
    city_id: str | None = Query(default=None, description="Filter by city_id (e.g. tacna-pe)"),
    limit: int = Query(default=120, ge=1, le=500),
) -> dict[str, Any]:
    """Return hourly weather & air quality observations from marts.fct_city_hourly_metrics."""
    if not _warehouse_has_marts():
        return {"records": []}

    if city_id:
        rows = query_dicts(
            """
            SELECT *
            FROM marts.fct_city_hourly_metrics
            WHERE city_id = %s
            ORDER BY observed_at_utc ASC
            LIMIT %s;
            """,
            (city_id, limit),
        )
    else:
        rows = query_dicts(
            """
            SELECT *
            FROM marts.fct_city_hourly_metrics
            ORDER BY observed_at_utc DESC
            LIMIT %s;
            """,
            (limit,),
        )
    return {"records": [_serialize_row(r) for r in rows]}


@app.get("/api/v1/anomalies")
def get_anomalies(limit: int = Query(default=40, ge=1, le=200)) -> dict[str, Any]:
    """Return detected thermal, wind, precipitation, and air-quality anomalies."""
    if not _warehouse_has_marts():
        return {"anomalies": []}

    rows = query_dicts(
        """
        SELECT
            observation_pk, city_id, city_name, country, region,
            observed_at_utc, temperature_c, baseline_temp_c,
            temp_delta_c, temp_z_score, us_aqi, pm2_5,
            aqi_category, wind_speed_kmh, precipitation_mm, anomaly_type
        FROM marts.mart_climate_anomalies
        ORDER BY observed_at_utc DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return {"anomalies": [_serialize_row(r) for r in rows]}


@app.get("/api/v1/pipeline/runs")
def get_pipeline_runs(limit: int = Query(default=15, ge=1, le=100)) -> dict[str, Any]:
    """Return recent ELT pipeline run telemetry."""
    try:
        rows = query_dicts(
            """
            SELECT run_id, started_at_utc, finished_at_utc, status, backend,
                   weather_rows, air_quality_rows, dbt_status, notes
            FROM raw.raw_pipeline_runs
            ORDER BY started_at_utc DESC
            LIMIT %s;
            """,
            (limit,),
        )
        return {"runs": [_serialize_row(r) for r in rows]}
    except Exception:
        return {"runs": []}


@app.post("/api/v1/pipeline/refresh")
def trigger_pipeline_refresh(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger an asynchronous ELT refresh (extract -> load -> dbt build)."""
    cfg = get_settings()
    if cfg.resolved_backend == "duckdb":
        raise HTTPException(
            status_code=409,
            detail=(
                "On-demand refresh via API is disabled on DuckDB to avoid write lock "
                "contention; use CLI or Postgres."
            ),
        )
    background_tasks.add_task(run_elt_pipeline, cfg)
    return {"status": "accepted", "message": "ELT pipeline refresh scheduled in background."}
