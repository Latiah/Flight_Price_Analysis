"""Central configuration for the flight price pipeline.

Every tunable value the pipeline depends on lives here, so a change of table
name, threshold, or column mapping happens in one file rather than being hunted
down across tasks. Nothing in this module performs I/O or touches Airflow, which
keeps it importable from plain unit tests.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Only useful when running outside the containers (e.g. pytest on a host shell).
# Inside the stack the values already come from docker-compose.yml, and
# load_dotenv does not overwrite existing environment variables.
load_dotenv()

# --- Airflow connection IDs -------------------------------------------------
# Injected as AIRFLOW_CONN_* environment variables by docker-compose.yml.
MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

# --- Paths ------------------------------------------------------------------
RAW_CSV_PATH = Path(
    os.getenv("RAW_CSV_PATH", "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv")
)
PROCESSED_DATA_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "/opt/airflow/data/processed"))

# --- Table names ------------------------------------------------------------
STAGING_TABLE = "stg_flight_prices"
FACT_TABLE = "flight_prices"

KPI_AVERAGE_FARE_TABLE = "kpi_average_fare_by_airline"
KPI_SEASONAL_VARIATION_TABLE = "kpi_seasonal_fare_variation"
KPI_BOOKING_COUNT_TABLE = "kpi_booking_count_by_airline"
KPI_POPULAR_ROUTES_TABLE = "kpi_popular_routes"

# --- CSV -> staging column mapping ------------------------------------------
# Keys are the exact headers in the source CSV; values are the staging column
# names. Renaming at the boundary means the rest of the pipeline never has to
# quote a column name containing spaces, ampersands, or parentheses.
CSV_COLUMN_MAP = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    # "Class" is renamed because `class` is a Python keyword and a reserved word
    # in several SQL dialects.
    "Class": "flight_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

# Columns the project brief names as mandatory, expressed as staging names.
# Their absence is a structural failure, not a row-level data problem, so the
# pipeline stops rather than producing a partial result.
REQUIRED_COLUMNS = (
    "airline",
    "source",
    "destination",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
)

# Columns that must hold a usable value for a row to be analytically meaningful.
NOT_NULL_COLUMNS = REQUIRED_COLUMNS
NUMERIC_COLUMNS = ("base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt", "duration_hrs")
NON_EMPTY_STRING_COLUMNS = ("airline", "source", "destination")

# --- Ingestion --------------------------------------------------------------
# The source file is ~57k rows, comfortably small, but reading and inserting in
# chunks keeps memory flat regardless of how much the dataset grows.
INGESTION_CHUNK_SIZE = 10_000
# Rows per INSERT statement. 21 columns x 1000 rows stays well inside the
# placeholder limits of both drivers while still batching usefully.
INSERT_CHUNK_SIZE = 1_000

# --- Validation -------------------------------------------------------------
# IATA-style location codes: exactly three letters. The source data uses these
# for both source and destination.
AIRPORT_CODE_PATTERN = r"^[A-Z]{3}$"

# Absolute BDT tolerance when checking base + tax == total, to absorb the
# floating-point noise of a CSV round trip rather than real disagreement.
FARE_RECONCILIATION_TOLERANCE = 0.01

# A documented quirk of this dataset: on ~4.4% of rows the total fare is exactly
# (base + tax) * 1.20 rather than (base + tax). The factor is identical to
# floating-point precision across every affected row, so it is a systematic 20%
# surcharge, not corruption. Such rows are recorded and passed through, not
# "fixed" — overwriting the total would silently erase a real 20% price premium.
# Only a mismatch that is neither exact nor this factor counts as an error.
KNOWN_FARE_UPLIFT_FACTOR = 1.20
FARE_UPLIFT_RELATIVE_TOLERANCE = 1e-6

# Minimum share of rows that must pass validation for the run to continue.
# Deliberately a hard failure: a pipeline that quietly loads a fraction of its
# input is worse than one that stops and says so.
MIN_VALID_ROW_RATIO = 0.95

# Name of the data quality report the validation task writes to
# PROCESSED_DATA_DIR, so a run's verdict survives past the task log.
VALIDATION_REPORT_FILENAME = "data_quality_report.json"

# --- Transformation ---------------------------------------------------------
# Seasons treated as peak for the seasonal fare variation KPI. Taken from the
# values actually present in the source `Seasonality` column; `Regular` is the
# non-peak baseline the other seasons are compared against.
BASELINE_SEASON = "Regular"
PEAK_SEASONS = ("Eid", "Hajj", "Winter Holidays")

# --- KPIs -------------------------------------------------------------------
# How many source-destination pairs the "most popular routes" KPI keeps.
TOP_N_ROUTES = 20
