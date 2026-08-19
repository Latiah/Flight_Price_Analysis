"""Airflow DAG: Bangladesh flight price analysis pipeline.

Orchestration only. Every task here is a thin wrapper that opens a connection,
calls into ``src/``, and logs the outcome — the business logic lives in the
modules under ``src/`` so it can be unit tested without Airflow, a scheduler, or
a database.

Flow::

    check_source_file
      -> load_csv_to_mysql          (truncate + chunked load into staging)
      -> validate_staging_data      (label rows, write report, enforce threshold)
      -> transform_and_load_fact    (clean + enrich valid rows -> analytics)
      -> [ four KPI tasks in parallel ]
      -> pipeline_summary

**Handoff between tasks is a database table, not XCom.** The dataset is 57k rows;
pushing it through XCom would serialise it into the metadata database on every
stage. Instead each task reads its input from the table the previous task wrote,
and XCom carries only small summary dicts. That also means every intermediate
result is durable and queryable after the run.

**Idempotency.** Each load truncates its target inside the same transaction as
the insert, so a rerun replaces the previous run's data rather than appending to
it, and a mid-load failure rolls back instead of leaving a half-written table. A
rerun of the whole DAG is therefore safe and produces the same result.

**Why the four KPIs are separate tasks.** They are independent, so they run in
parallel, and a failure in one is visible as one red task rather than as an
opaque failure of a combined step.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.decorators import dag, task

from src.config import settings
from src.database import mysql, postgres
from src.ingestion import csv_loader
from src.kpi import metrics
from src.transformation import transformations
from src.validation import validators

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    # Retries suit the transient case (a database not yet accepting connections);
    # they do not help a data quality failure, which fails identically on retry.
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="flight_price_pipeline",
    description="Ingest, validate, transform, and load Bangladesh flight price data with KPIs",
    default_args=default_args,
    # The source is a static CSV, so there is no schedule to track: runs are
    # triggered when the file changes.
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    # A second concurrent run would truncate the tables the first is reading.
    max_active_runs=1,
    tags=["flight-prices", "bangladesh", "etl"],
    doc_md=__doc__,
)
def flight_price_pipeline() -> None:
    @task
    def check_source_file() -> str:
        """Fail fast if the source CSV is missing or empty."""
        return csv_loader.check_source_file()

    @task
    def load_csv_to_mysql(source_path: str) -> dict:
        """Replace the MySQL staging table with the contents of the CSV."""
        engine = mysql.get_engine()
        try:
            return csv_loader.load_csv_to_staging(engine, csv_path=source_path)
        finally:
            engine.dispose()

    @task
    def validate_staging_data(load_summary: dict) -> dict:
        """Label every staged row, persist a data quality report, enforce the threshold.

        The report is written to ``PROCESSED_DATA_DIR`` as well as logged, so a
        run's verdict outlives the task log and can be diffed between runs.

        The threshold check runs last, after the labels and the report have been
        written — so when the run does fail on data quality, the evidence for why
        is already saved rather than lost with the task.
        """
        engine = mysql.get_engine()
        try:
            staged = mysql.read_staging(engine)
            results, report = validators.validate_dataframe(staged)
            mysql.write_validation_results(engine, results)
        finally:
            engine.dispose()

        settings.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        report_path = settings.PROCESSED_DATA_DIR / settings.VALIDATION_REPORT_FILENAME
        report_out = {**report, "source_file": load_summary.get("source_file")}
        report_path.write_text(json.dumps(report_out, indent=2), encoding="utf-8")
        log.info("Data quality report written to %s\n%s", report_path, json.dumps(report_out, indent=2))

        validators.enforce_quality_threshold(report)
        return report

    @task
    def transform_and_load_fact(validation_report: dict) -> dict:
        """Clean and enrich the valid staged rows, then load the analytics fact table.

        Transformation and loading share one task because separating them would
        mean materialising an intermediate copy of the same rows for no gain — the
        transformation output has exactly one consumer. The logic itself stays
        separable: ``transformations`` is pure pandas, ``postgres`` does the I/O.
        """
        source_engine = mysql.get_engine()
        try:
            # Only the rows the validation task marked valid, filtered in SQL so
            # the flagged ones are never fetched. Their reasons stay in staging.
            valid_rows = mysql.read_staging(source_engine, valid_only=True)
        finally:
            source_engine.dispose()

        # Recompute just the fare comparison, not all eight validation rules —
        # the uplift flag is the only validation output the fact table carries.
        consistency = validators.classify_fare_consistency(valid_rows)
        fact = transformations.transform(
            valid_rows, fare_uplift_flags=consistency["has_known_uplift"]
        )

        target_engine = postgres.get_engine()
        try:
            rows = postgres.replace_table_contents(target_engine, settings.FACT_TABLE, fact)
        finally:
            target_engine.dispose()

        summary = {
            "fact_rows_loaded": rows,
            "staged_rows": int(validation_report["total_rows"]),
            "excluded_invalid_rows": int(validation_report["invalid_rows"]),
            "rows_with_fare_uplift": int(fact["has_fare_uplift"].sum()),
            "distinct_airlines": int(fact["airline"].nunique()),
            "distinct_routes": int(fact["route"].nunique()),
        }
        log.info("Fact load summary: %s", summary)
        return summary

    def _load_kpi(table: str, compute) -> dict:
        """Read the fact table, compute one KPI, replace that KPI's table.

        Shared by the four KPI tasks: they differ only in which function they call
        and which table they write, so the connection handling lives in one place.
        """
        engine = postgres.get_engine()
        try:
            fact = postgres.read_fact_table(engine)
            kpi = compute(fact)
            rows = postgres.replace_table_contents(engine, table, kpi)
        finally:
            engine.dispose()
        return {"table": table, "rows": rows}

    @task
    def compute_kpi_average_fare_by_airline() -> dict:
        """KPI 1 — average fare by airline."""
        return _load_kpi(settings.KPI_AVERAGE_FARE_TABLE, metrics.average_fare_by_airline)

    @task
    def compute_kpi_seasonal_fare_variation() -> dict:
        """KPI 2 — seasonal fare variation, peak vs. non-peak."""
        return _load_kpi(settings.KPI_SEASONAL_VARIATION_TABLE, metrics.seasonal_fare_variation)

    @task
    def compute_kpi_booking_count_by_airline() -> dict:
        """KPI 3 — booking count by airline."""
        return _load_kpi(settings.KPI_BOOKING_COUNT_TABLE, metrics.booking_count_by_airline)

    @task
    def compute_kpi_popular_routes() -> dict:
        """KPI 4 — most popular routes."""
        return _load_kpi(settings.KPI_POPULAR_ROUTES_TABLE, metrics.popular_routes)

    @task
    def pipeline_summary(fact_summary: dict, kpi_results: list[dict]) -> dict:
        """Verify the loaded row counts and emit one summary of the whole run.

        Counts are read back from the database rather than taken from the upstream
        XCom values, so this is an independent check that what the tasks reported
        loading is actually present.
        """
        engine = postgres.get_engine()
        try:
            counts = {
                result["table"]: postgres.count_rows(engine, result["table"])
                for result in kpi_results
            }
            counts[settings.FACT_TABLE] = postgres.count_rows(engine, settings.FACT_TABLE)
        finally:
            engine.dispose()

        empty = [table for table, count in counts.items() if count == 0]
        if empty:
            raise ValueError(
                f"Pipeline finished but these analytics tables are empty: {empty}. "
                "Check the corresponding task logs."
            )

        summary = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "row_counts": counts,
            **fact_summary,
        }
        log.info("Pipeline complete:\n%s", json.dumps(summary, indent=2))
        return summary

    source_path = check_source_file()
    load_summary = load_csv_to_mysql(source_path)
    validation_report = validate_staging_data(load_summary)
    fact_summary = transform_and_load_fact(validation_report)

    kpi_results = [
        compute_kpi_average_fare_by_airline(),
        compute_kpi_seasonal_fare_variation(),
        compute_kpi_booking_count_by_airline(),
        compute_kpi_popular_routes(),
    ]
    # The KPI tasks take no arguments, so their dependency on the fact load is
    # declared explicitly rather than inferred from data flow.
    fact_summary >> kpi_results

    pipeline_summary(fact_summary, kpi_results)


flight_price_pipeline()
