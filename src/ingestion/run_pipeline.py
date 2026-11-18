"""End-to-end ELT pipeline runner (Extract -> Load -> dbt Transform & Test)."""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from dbt.cli.main import dbtRunner

from src.config import Settings, get_settings
from src.db import (
    ensure_raw_schema,
    record_pipeline_run,
    upsert_air_quality_records,
    upsert_weather_records,
)
from src.ingestion.open_meteo import extract_all_cities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("skypulse.pipeline")


@dataclass
class PipelineResult:
    """Summary returned after a pipeline execution."""

    run_id: str
    backend: str
    weather_rows: int
    air_quality_rows: int
    dbt_success: bool
    status: str


def _export_dbt_env(settings: Settings) -> str:
    """Populate environment variables expected by dbt_project/profiles.yml and return target."""
    backend = settings.resolved_backend
    if backend == "postgres":
        parsed = urlparse(settings.normalized_postgres_url)
        os.environ["DBT_PG_HOST"] = parsed.hostname or "localhost"
        os.environ["DBT_PG_PORT"] = str(parsed.port or 5432)
        os.environ["DBT_PG_USER"] = parsed.username or "postgres"
        os.environ["DBT_PG_PASSWORD"] = parsed.password or ""
        os.environ["DBT_PG_DBNAME"] = parsed.path.lstrip("/") or "skypulse"
        is_local = parsed.hostname in ("localhost", "127.0.0.1", "postgres")
        needs_ssl = "sslmode=require" in settings.normalized_postgres_url or not is_local
        os.environ["DBT_PG_SSLMODE"] = "require" if needs_ssl else "prefer"
        return "postgres"

    os.environ["DBT_DUCKDB_PATH"] = str(settings.duckdb_path.resolve())
    return "duckdb"


def run_dbt_build(settings: Settings) -> bool:
    """Execute `dbt build` (run + test) against the active warehouse target."""
    target = _export_dbt_env(settings)
    runner = dbtRunner()
    args = [
        "build",
        "--project-dir",
        str(settings.dbt_project_dir.resolve()),
        "--profiles-dir",
        str(settings.dbt_profiles_dir.resolve()),
        "--target",
        target,
    ]
    logger.info("Running dbt build (target=%s)...", target)
    res = runner.invoke(args)
    if not res.success:
        logger.error("dbt build failed: %s", res.exception)
    else:
        logger.info("dbt build succeeded.")
    return bool(res.success)


def run_elt_pipeline(
    settings: Settings | None = None,
    skip_dbt: bool = False,
) -> PipelineResult:
    """Run full Extract -> Load -> Transform -> Test pipeline."""
    cfg = settings or get_settings()
    run_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    started_at = datetime.now(timezone.utc)
    backend = cfg.resolved_backend

    logger.info("Starting SkyPulse ELT run_id=%s backend=%s", run_id, backend)
    ensure_raw_schema(cfg)

    weather_rows = 0
    aq_rows = 0
    dbt_success = True
    try:
        weather_records, aq_records = extract_all_cities(
            past_days=cfg.past_days,
            forecast_days=cfg.forecast_days,
            timeout_seconds=cfg.request_timeout_seconds,
        )
        weather_rows = upsert_weather_records(weather_records, cfg)
        aq_rows = upsert_air_quality_records(aq_records, cfg)
        logger.info(
            "Loaded raw records: weather=%d, air_quality=%d",
            weather_rows,
            aq_rows,
        )

        if not skip_dbt:
            dbt_success = run_dbt_build(cfg)
            if not dbt_success:
                raise RuntimeError("dbt build failed quality checks or model compilation.")

        record_pipeline_run(
            run_id=run_id,
            started_at_utc=started_at,
            status="success",
            backend=backend,
            weather_rows=weather_rows,
            air_quality_rows=aq_rows,
            dbt_status="skipped" if skip_dbt else "passed",
            notes="Scheduled/automated ELT run completed cleanly.",
            settings=cfg,
        )
        return PipelineResult(
            run_id=run_id,
            backend=backend,
            weather_rows=weather_rows,
            air_quality_rows=aq_rows,
            dbt_success=dbt_success,
            status="success",
        )
    except Exception as exc:
        logger.exception("Pipeline failed for run_id=%s", run_id)
        try:
            record_pipeline_run(
                run_id=run_id,
                started_at_utc=started_at,
                status="failed",
                backend=backend,
                weather_rows=weather_rows,
                air_quality_rows=aq_rows,
                dbt_status="failed" if not dbt_success else "not_run",
                notes=str(exc)[:500],
                settings=cfg,
            )
        except Exception:
            logger.warning("Could not persist failed pipeline run metadata.")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SkyPulse ELT pipeline.")
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Only run extract & load into raw tables without running dbt build.",
    )
    args = parser.parse_args()
    result = run_elt_pipeline(skip_dbt=args.skip_dbt)
    print(
        f"Pipeline {result.run_id} finished: status={result.status}, "
        f"backend={result.backend}, weather_rows={result.weather_rows}, "
        f"air_quality_rows={result.air_quality_rows}, dbt_success={result.dbt_success}"
    )


if __name__ == "__main__":
    main()
