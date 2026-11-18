---
title: "Building an Automated, Zero-AWS Data Engineering Platform with Apache Airflow, dbt, DuckDB/PostgreSQL, and FastAPI"
published: true
description: "A complete walkthrough of designing, testing, containerizing, and automating a cloud-native ELT weather and air-quality analytics platform using open-source tools and public PaaS — without AWS."
tags: dataengineering, dbt, airflow, python
---

Modern Data Engineering doesn't require heavyweight cloud lock-in or expensive AWS infrastructure to be production-grade. With open-source tools like **Apache Airflow**, **dbt Core**, **DuckDB**, **PostgreSQL**, **FastAPI**, and **GitHub Actions**, you can build a reproducible, automatically tested, and continuously deployed ELT platform on free-tier public cloud/SaaS providers.

In this article, we walk through **SkyPulse**, an end-to-end Data Engineering solution that:
1. Ingests live hourly weather and air-quality telemetry for 10 global cities from the **Open-Meteo API** (no API key required).
2. Stores raw observations idempotently in **DuckDB** (local/CI) or **PostgreSQL** (production cloud).
3. Transforms and tests the data using **dbt Core** following a **Medallion Architecture** (`raw` → `staging` → `intermediate` → `marts`).
4. Orchestrates scheduled runs via an **Apache Airflow DAG** (local Docker stack) and **GitHub Actions** (6-hour serverless cron).
5. Serves analytics and an interactive **Plotly** dashboard via **FastAPI**, automatically deployed via **Render** (`render.yaml`).

> **Source Code (Public Repository):** [github.com/FabricioRams/Actividad-Grupal-01-U2-Data-Enginnering](https://github.com/FabricioRams/Actividad-Grupal-01-U2-Data-Enginnering)

---

## 1. Architecture Overview (100% AWS-Free)

Our goal was to keep the stack portable across laptops, CI runners, and public PaaS providers without touching AWS S3, Redshift, or MWAA:

- **Extraction & Validation (`src/ingestion/open_meteo.py`)**: `httpx` + `tenacity` retries + **Pydantic** models validate every hourly record before it touches the warehouse.
- **Dual-Backend Warehouse (`src/db.py`)**: A unified connection layer supports **DuckDB** for zero-setup local development/CI and **PostgreSQL** (Render Managed Postgres or Neon) in production.
- **Transformation & Data Quality (`dbt_project/`)**: `dbt-core` builds views and materialized tables across `staging`, `intermediate`, and `marts` schemas, backed by 20 automated schema and singular tests.
- **Orchestration (`airflow/dags/skypulse_elt_dag.py` + `.github/workflows/ci-cd.yml`)**: Runs the pipeline locally in Apache Airflow and automatically every 6 hours in GitHub Actions.
- **Serving Layer (`src/api/main.py`)**: FastAPI serves JSON endpoints (`/api/v1/summary`, `/api/v1/cities`, `/api/v1/hourly`, `/api/v1/anomalies`) and a responsive Plotly analytics dashboard at `/`.

---

## 2. Resilient Ingestion with Idempotent Upserts

One of the golden rules of Data Engineering is **idempotency**: re-running a pipeline for the same time window must never create duplicate records.

In `src/db.py`, both `raw.raw_weather_hourly` and `raw.raw_air_quality_hourly` enforce a composite primary key `(city_id, observed_at_utc)` and perform `ON CONFLICT ... DO UPDATE`:

```sql
INSERT INTO raw.raw_weather_hourly (
    city_id, city_name, country, region, latitude, longitude,
    observed_at_utc, temperature_c, relative_humidity_pct,
    apparent_temperature_c, precipitation_mm, wind_speed_kmh,
    surface_pressure_hpa, cloud_cover_pct, weather_code, ingested_at_utc
)
VALUES (...)
ON CONFLICT (city_id, observed_at_utc) DO UPDATE SET
    temperature_c = EXCLUDED.temperature_c,
    relative_humidity_pct = EXCLUDED.relative_humidity_pct,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
    ingested_at_utc = EXCLUDED.ingested_at_utc;
```

Before loading, `src/ingestion/open_meteo.py` validates constraints like `relative_humidity_pct` (`0–100`) and non-negative pollutant concentrations (`pm2_5`, `pm10`, `us_aqi`) using Pydantic.

---

## 3. Medallion Modeling & Quality Gates in dbt

Our `dbt_project/` separates raw ingestion from business-ready analytics across three layers:

### Staging (`staging` schema — views)
- `stg_weather_hourly.sql`: Deduplicates using `ROW_NUMBER()` over `(city_id, observed_at_utc)`, normalizes UTC dates with `date_trunc('day', observed_at_utc at time zone 'UTC')`, and casts numeric types consistently across DuckDB and PostgreSQL.
- `stg_air_quality_hourly.sql`: Maps raw `us_aqi` values into standard EPA categories (`Good`, `Moderate`, `Sensitive`, `Unhealthy`, `Hazardous`).

### Intermediate (`intermediate` schema — view)
- `int_city_hourly_combined.sql`: Joins hourly weather and air quality streams and calculates a composite **Comfort Index Score (0–100)** combining temperature deviation from 21°C, humidity, AQI, and wind speed.

### Marts (`marts` schema — materialized tables)
- `dim_cities`: Monitored city metadata and coordinates.
- `fct_city_hourly_metrics`: Unified hourly weather + pollutant fact table.
- `mart_city_daily_summary`: Daily min/avg/max temperature, precipitation totals, AQI, and comfort index per city.
- `mart_climate_anomalies`: Statistical anomaly detector computing per-city temperature z-scores (`|z| >= 1.75`) alongside high-AQI (`>= 100`), high-wind (`>= 40 km/h`), and heavy-precipitation (`>= 5 mm/h`) events.

Every `dbt build` runs **20 data quality tests**, including `unique`, `not_null`, `relationships`, `accepted_values`, and custom singular SQL tests such as `assert_valid_humidity_range.sql`:

```sql
select
    observation_pk,
    city_id,
    relative_humidity_pct
from {{ ref('fct_city_hourly_metrics') }}
where relative_humidity_pct < 0
   or relative_humidity_pct > 100
```

---

## 4. Orchestration with Apache Airflow & GitHub Actions

For containerized local or self-hosted execution, `airflow/dags/skypulse_elt_dag.py` defines a clear DAG using `TaskGroup`:

```python
with DAG(
    dag_id="skypulse_weather_air_quality_elt",
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["skypulse", "dbt", "postgres", "elt", "no-aws"],
) as dag:
    extract_and_load_raw = BashOperator(
        task_id="extract_and_load_open_meteo",
        bash_command="cd /opt/skypulse && python -m src.ingestion.run_pipeline --skip-dbt",
    )

    with TaskGroup(group_id="dbt_medallion_pipeline") as dbt_group:
        dbt_run_staging = BashOperator(...)
        dbt_run_marts = BashOperator(...)
        dbt_test_quality = BashOperator(...)
        dbt_run_staging >> dbt_run_marts >> dbt_test_quality

    extract_and_load_raw >> dbt_group
```

Running `docker compose up --build` spins up PostgreSQL 16, Apache Airflow 2.10, and the FastAPI analytics service together.

---

## 5. Automated CI/CD & Public Cloud Deployment (Zero AWS)

Automation doesn't stop at the data pipeline—our entire software delivery lifecycle is automated via **GitHub Actions** (`.github/workflows/ci-cd.yml`) and **Render** (`render.yaml`):

1. **Continuous Integration (on every push & PR)**:
   - Lints Python code with `ruff`.
   - Executes unit tests (`httpx.MockTransport`), end-to-end DuckDB + `dbt build` tests, and FastAPI endpoint tests via `pytest`.
   - Verifies the production `Dockerfile` builds cleanly.
2. **Continuous Deployment & Scheduled Cloud ELT**:
   - On `main` and on a `0 */6 * * *` cron schedule, GitHub Actions runs `python -m src.ingestion.run_pipeline` against our cloud PostgreSQL database (`DATABASE_URL` secret).
   - On push to `main`, it triggers `RENDER_DEPLOY_HOOK_URL` (or Render's native Git auto-deploy via `render.yaml`) to roll out the Dockerized FastAPI service and dashboard.

Using `render.yaml` Blueprints, both the managed PostgreSQL database and the Docker web service are provisioned declaratively as Infrastructure-as-Code without touching AWS.

---

## 6. Serving Insights via FastAPI & Plotly

Rather than leaving analytics trapped inside SQL tables, FastAPI exposes:
- `GET /` — An interactive dark-mode **Plotly.js** dashboard comparing hourly temperature vs. US AQI trajectories, city comfort rankings, detected climate anomalies, and pipeline run telemetry.
- `GET /docs` — Auto-generated OpenAPI/Swagger documentation.
- `GET /health` — Readiness probe verifying warehouse mart availability.

---

## Try It Yourself

Clone the repository and run the entire pipeline locally in under a minute:

```bash
git clone https://github.com/FabricioRams/Actividad-Grupal-01-U2-Data-Enginnering.git
cd Actividad-Grupal-01-U2-Data-Enginnering
python -m venv .venv
pip install -r requirements-dev.txt
python -m src.ingestion.run_pipeline
uvicorn src.api.main:app --reload
```

Thanks for reading! Feel free to fork the repository, connect your own Render or Neon PostgreSQL instance, and extend the pipeline with additional Open-Meteo metrics or streaming sources.
