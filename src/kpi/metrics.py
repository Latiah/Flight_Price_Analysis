"""Computation of the four required KPIs.

Pure pandas. Each function takes the enriched fact data and returns a DataFrame
whose columns match its target table in sql/postgres/create_tables.sql exactly,
so loading is a plain append with no column mapping in between.

A note on what "one booking" means: the source data has no booking identifier, so
one row is one booking (one fare quote). Every count below is therefore a row
count. This is stated in each docstring because it is the single assumption that
most affects how the count KPIs should be read.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import settings

log = logging.getLogger(__name__)


def _require_columns(df: pd.DataFrame, columns: tuple[str, ...], kpi: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{kpi} requires missing column(s): {missing}")


def average_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """KPI 1 — mean total fare per airline.

    Base and tax means are carried alongside the total so a high average can be
    attributed to fare or to surcharges without a second query. Min and max give
    the spread, which matters because a mean over mixed cabin classes is a wide
    distribution rather than a typical price.
    """
    _require_columns(
        df, ("airline", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"), "average_fare_by_airline"
    )

    out = (
        df.groupby("airline", as_index=False, dropna=True)
        .agg(
            average_base_fare=("base_fare_bdt", "mean"),
            average_tax_charge=("tax_surcharge_bdt", "mean"),
            average_total_fare=("total_fare_bdt", "mean"),
            min_total_fare=("total_fare_bdt", "min"),
            max_total_fare=("total_fare_bdt", "max"),
            flight_count=("total_fare_bdt", "size"),
        )
        .sort_values("average_total_fare", ascending=False, ignore_index=True)
    )
    out = out.round(4)
    log.info("KPI average_fare_by_airline: %d airlines", len(out))
    return out


def seasonal_fare_variation(df: pd.DataFrame) -> pd.DataFrame:
    """KPI 2 — average fare per season, against the non-peak baseline.

    ``variation_vs_baseline_pct`` is each season's average total fare expressed as
    a percentage difference from ``settings.BASELINE_SEASON`` (``Regular``), which
    is the peak vs. non-peak comparison the brief asks for. Storing the computed
    difference means the answer does not depend on the reader reproducing the
    right baseline join.

    If the baseline season is absent from the data the column is left null rather
    than silently comparing against some other season's average.
    """
    _require_columns(df, ("seasonality", "is_peak_season", "total_fare_bdt"), "seasonal_fare_variation")

    out = (
        df.groupby("seasonality", as_index=False, dropna=True)
        .agg(
            # "max" over a per-season constant simply carries the flag through
            # the aggregation; every row of a season shares the same value.
            is_peak_season=("is_peak_season", "max"),
            average_total_fare=("total_fare_bdt", "mean"),
            flight_count=("total_fare_bdt", "size"),
        )
    )

    baseline_rows = out.loc[out["seasonality"] == settings.BASELINE_SEASON, "average_total_fare"]
    if baseline_rows.empty:
        log.warning(
            "Baseline season %r not present; variation_vs_baseline_pct left null",
            settings.BASELINE_SEASON,
        )
        # float NaN rather than pd.NA: NaN is what the database driver knows how
        # to write as NULL, whereas pd.NA in an object column cannot be adapted.
        out["variation_vs_baseline_pct"] = float("nan")
    else:
        baseline = float(baseline_rows.iloc[0])
        out["variation_vs_baseline_pct"] = (
            (out["average_total_fare"] - baseline) / baseline * 100
        ).round(2)

    out["is_peak_season"] = out["is_peak_season"].astype(bool)
    out["average_total_fare"] = out["average_total_fare"].round(4)
    out = out.sort_values("average_total_fare", ascending=False, ignore_index=True)

    log.info("KPI seasonal_fare_variation: %d seasons", len(out))
    return out


def booking_count_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """KPI 3 — number of bookings per airline, one row being one booking.

    ``booking_share_pct`` is included because a raw count is only interpretable
    against the run's total: a rerun over a different extract changes every count
    but leaves the shares comparable.
    """
    _require_columns(df, ("airline",), "booking_count_by_airline")

    out = (
        df.groupby("airline", as_index=False, dropna=True)
        .agg(booking_count=("airline", "size"))
        .sort_values("booking_count", ascending=False, ignore_index=True)
    )
    total = int(out["booking_count"].sum())
    out["booking_share_pct"] = (
        (out["booking_count"] / total * 100).round(2) if total else float("nan")
    )

    log.info("KPI booking_count_by_airline: %d airlines, %d bookings", len(out), total)
    return out


def popular_routes(df: pd.DataFrame, top_n: int | None = None) -> pd.DataFrame:
    """KPI 4 — top source-destination pairs by booking count.

    ``rank_position`` is stored rather than derived at read time so the ranking
    recorded is the one this run produced. Ties are ranked by the order pandas
    returns after the count sort, which is deterministic for a given input.
    """
    _require_columns(df, ("route", "source", "destination", "total_fare_bdt"), "popular_routes")

    limit = settings.TOP_N_ROUTES if top_n is None else top_n

    out = (
        df.groupby(["route", "source", "destination"], as_index=False, dropna=True)
        .agg(
            booking_count=("route", "size"),
            average_total_fare=("total_fare_bdt", "mean"),
        )
        .sort_values(["booking_count", "route"], ascending=[False, True], ignore_index=True)
    )

    # Rank over every route, then truncate — so rank 1 is the busiest route
    # overall and not merely the busiest of whatever survived the cut.
    out["rank_position"] = range(1, len(out) + 1)
    total_routes = len(out)
    out = out.head(limit).copy()
    out["average_total_fare"] = out["average_total_fare"].round(4)

    log.info(
        "KPI popular_routes: keeping top %d of %d routes", len(out), total_routes
    )
    return out


def compute_all(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute every KPI, keyed by its destination table name.

    Convenient for tests and ad hoc use. The DAG calls the individual functions
    instead, so that one failing KPI is visible as one failing task.
    """
    return {
        settings.KPI_AVERAGE_FARE_TABLE: average_fare_by_airline(df),
        settings.KPI_SEASONAL_VARIATION_TABLE: seasonal_fare_variation(df),
        settings.KPI_BOOKING_COUNT_TABLE: booking_count_by_airline(df),
        settings.KPI_POPULAR_ROUTES_TABLE: popular_routes(df),
    }
