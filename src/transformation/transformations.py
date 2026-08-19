"""Cleaning and enrichment of validated staging rows.

Pure pandas, like the validation module, so each rule can be tested on a few
hand-written rows.

Two rules deserve stating up front, because both are places where a pipeline can
quietly corrupt its own output:

*Total fare is computed only when it is missing.* The brief says to calculate
``Total Fare = Base Fare + Tax & Surcharge`` "if not already present". In this
dataset it is always present, and on ~4.4% of rows it is deliberately 1.20x the
sum of its parts. Recomputing every row would erase that 20% surcharge and
understate those fares — so the stated total wins wherever there is one.

*Invalid rows never reach here.* Validation labels rows rather than deleting
them, and the transformation stage is fed only the rows marked valid. The flagged
ones stay queryable in staging with the reason attached, which is what makes a
surprising KPI traceable back to the rows that were left out.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import settings

log = logging.getLogger(__name__)

# Columns carried from staging into the analytics fact table, in order. The
# staging-only bookkeeping columns (`id`, `_ingested_at`, `is_valid`,
# `validation_errors`) stop here: they describe the staging load, not the flight.
FACT_COLUMNS = (
    "airline",
    "source",
    "source_name",
    "destination",
    "destination_name",
    "departure_datetime",
    "arrival_datetime",
    "duration_hrs",
    "stopovers",
    "aircraft_type",
    "flight_class",
    "booking_source",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
    "seasonality",
    "days_before_departure",
    "route",
    "is_peak_season",
    "has_fare_uplift",
    "_source_file",
)

# Text columns worth normalising. Trailing whitespace in a grouping key silently
# splits one airline into two rows in a KPI table.
_TEXT_COLUMNS = (
    "airline",
    "source_name",
    "destination_name",
    "stopovers",
    "aircraft_type",
    "flight_class",
    "booking_source",
    "seasonality",
)
_CODE_COLUMNS = ("source", "destination")


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise text, codes, and numeric types.

    Does not alter fare values — see :func:`reconcile_total_fare` for that.
    """
    out = df.copy()

    for column in _TEXT_COLUMNS:
        if column in out.columns:
            out[column] = out[column].astype("string").str.strip()

    # Location codes are uppercased so that a stray lowercase code cannot create
    # a second, near-duplicate route in the popular-routes KPI.
    for column in _CODE_COLUMNS:
        out[column] = out[column].astype("string").str.strip().str.upper()

    for column in settings.NUMERIC_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")

    for column in ("departure_datetime", "arrival_datetime"):
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce")

    if "days_before_departure" in out.columns:
        # Nullable integer: a missing value stays missing instead of becoming a
        # float and turning the whole column into 14.0-style values.
        out["days_before_departure"] = pd.to_numeric(
            out["days_before_departure"], errors="coerce"
        ).astype("Int64")

    log.info("Cleaned %d rows", len(out))
    return out


def reconcile_total_fare(df: pd.DataFrame) -> pd.DataFrame:
    """Fill in a missing total fare from its components.

    Only rows whose ``total_fare_bdt`` is null are computed, as ``base + tax``. A
    row that already states a total keeps it, including the rows where the total
    is the documented 1.20x uplift of its components.
    """
    out = df.copy()
    missing_total = out["total_fare_bdt"].isna()
    computed = out["base_fare_bdt"] + out["tax_surcharge_bdt"]
    out.loc[missing_total, "total_fare_bdt"] = computed[missing_total]

    filled = int((missing_total & out["total_fare_bdt"].notna()).sum())
    if filled:
        log.info("Computed total_fare_bdt for %d row(s) that were missing it", filled)
    else:
        log.info("Every row already carried a total fare; none recomputed")
    return out


def enrich(df: pd.DataFrame, fare_uplift_flags: pd.Series | None = None) -> pd.DataFrame:
    """Add the derived columns the KPIs group and compare by.

    ``route``
        ``"DAC-CGP"``. Materialised rather than grouping by the two code columns
        so the route KPI has a single primary key and joins stay simple.
    ``is_peak_season``
        Derived from the source ``Seasonality`` column against
        ``settings.PEAK_SEASONS``. This is the seasonal KPI's peak/non-peak split.
    ``has_fare_uplift``
        Whether this row's total is the documented 1.20x uplift of its
        components. Carried into analytics because those rows raise the average
        fare for their airline, and an analyst needs to be able to see or exclude
        them rather than wonder why a mean looks high.
    """
    out = df.copy()

    out["route"] = out["source"].astype("string") + "-" + out["destination"].astype("string")

    seasonality = out["seasonality"].astype("string").str.strip()
    # astype(bool) collapses pandas' nullable boolean to a plain one; the database
    # driver writes a real bool but cannot adapt a pd.NA.
    out["is_peak_season"] = seasonality.isin(settings.PEAK_SEASONS).astype(bool)

    unknown = set(seasonality.dropna().unique()) - set(settings.PEAK_SEASONS) - {
        settings.BASELINE_SEASON
    }
    if unknown:
        # Not fatal: an unrecognised season is simply treated as non-peak. Logged
        # because it means settings.PEAK_SEASONS needs a decision, and silently
        # classifying a new peak season as "Regular" would skew the seasonal KPI.
        log.warning(
            "Unrecognised seasonality value(s) %s treated as non-peak; "
            "add them to settings.PEAK_SEASONS if they are peak periods",
            sorted(unknown),
        )

    if fare_uplift_flags is not None:
        out["has_fare_uplift"] = fare_uplift_flags.reindex(out.index).fillna(False).astype(bool)
    else:
        out["has_fare_uplift"] = False

    log.info(
        "Enriched %d rows: %d distinct routes, %d peak-season rows, %d uplift rows",
        len(out),
        out["route"].nunique(),
        int(out["is_peak_season"].sum()),
        int(out["has_fare_uplift"].sum()),
    )
    return out


def select_fact_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Project onto the fact table's columns, in its column order.

    Fails loudly on a missing column rather than letting pandas insert NaNs and
    turning a coding mistake into an all-null column in the analytics database.
    """
    missing = [column for column in FACT_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Transformed data is missing fact column(s): {missing}")
    return df[list(FACT_COLUMNS)].copy()


def transform(df: pd.DataFrame, fare_uplift_flags: pd.Series | None = None) -> pd.DataFrame:
    """Run clean -> reconcile -> enrich -> project over the rows to be loaded.

    ``df`` holds the rows already selected for analytics. Excluding the invalid
    ones is the caller's job — in the DAG it is a ``WHERE is_valid = 1`` on the
    staging read, so flagged rows are never fetched in the first place.

    ``fare_uplift_flags`` is the ``has_known_uplift`` column from
    :func:`src.validation.validators.classify_fare_consistency`, aligned to ``df``
    by index.
    """
    if df.empty:
        raise ValueError(
            "No rows to transform. Every staged row was flagged invalid, or the "
            f"{settings.STAGING_TABLE} table is empty."
        )

    out = clean(df)
    out = reconcile_total_fare(out)
    out = enrich(out, fare_uplift_flags=fare_uplift_flags)
    out = select_fact_columns(out)

    log.info("Transformation produced %d rows for the fact table", len(out))
    return out
