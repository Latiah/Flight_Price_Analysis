"""Tests for src/kpi/metrics.py — one test per KPI, plus the KPI table columns.

Each test builds a tiny fact table whose correct answer is obvious by hand, so
the expected numbers can be checked by reading them.
"""

import pandas as pd

from src.kpi import metrics


def fact_table(rows):
    """Build a fact table from a list of dicts, filling in unused columns."""
    defaults = {
        "airline": "Biman",
        "source": "DAC",
        "destination": "CGP",
        "route": "DAC-CGP",
        "base_fare_bdt": 10_000.0,
        "tax_surcharge_bdt": 200.0,
        "total_fare_bdt": 10_200.0,
        "seasonality": "Regular",
        "is_peak_season": False,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_average_fare_by_airline():
    """Biman flies for 10,000 and 20,000, so its average is 15,000."""
    df = fact_table([
        {"airline": "Biman", "total_fare_bdt": 10_000.0},
        {"airline": "Biman", "total_fare_bdt": 20_000.0},
        {"airline": "US-Bangla", "total_fare_bdt": 5_000.0},
    ])

    result = metrics.average_fare_by_airline(df).set_index("airline")

    assert result.loc["Biman", "average_total_fare"] == 15_000.0
    assert result.loc["Biman", "min_total_fare"] == 10_000.0
    assert result.loc["Biman", "max_total_fare"] == 20_000.0
    assert result.loc["Biman", "flight_count"] == 2
    assert result.loc["US-Bangla", "average_total_fare"] == 5_000.0


def test_seasonal_fare_variation():
    """Eid averages 12,000 against a Regular baseline of 10,000, so +20%."""
    df = fact_table([
        {"seasonality": "Regular", "is_peak_season": False, "total_fare_bdt": 10_000.0},
        {"seasonality": "Eid", "is_peak_season": True, "total_fare_bdt": 12_000.0},
    ])

    result = metrics.seasonal_fare_variation(df).set_index("seasonality")

    assert result.loc["Eid", "variation_vs_baseline_pct"] == 20.0
    assert result.loc["Regular", "variation_vs_baseline_pct"] == 0.0
    assert result.loc["Eid", "is_peak_season"] == True


def test_booking_count_by_airline():
    """One row is one booking: 3 of 4 bookings are Biman, so 75%."""
    df = fact_table([
        {"airline": "Biman"},
        {"airline": "Biman"},
        {"airline": "Biman"},
        {"airline": "US-Bangla"},
    ])

    result = metrics.booking_count_by_airline(df).set_index("airline")

    assert result.loc["Biman", "booking_count"] == 3
    assert result.loc["Biman", "booking_share_pct"] == 75.0
    assert result.loc["US-Bangla", "booking_count"] == 1


def test_popular_routes_are_ranked_by_bookings():
    df = fact_table([
        {"route": "DAC-CGP", "source": "DAC", "destination": "CGP"},
        {"route": "DAC-CGP", "source": "DAC", "destination": "CGP"},
        {"route": "DAC-CGP", "source": "DAC", "destination": "CGP"},
        {"route": "DAC-CXB", "source": "DAC", "destination": "CXB"},
        {"route": "DAC-CXB", "source": "DAC", "destination": "CXB"},
        {"route": "BZL-CGP", "source": "BZL", "destination": "CGP"},
    ])

    result = metrics.popular_routes(df)

    assert result["route"].tolist() == ["DAC-CGP", "DAC-CXB", "BZL-CGP"]
    assert result["booking_count"].tolist() == [3, 2, 1]
    assert result["rank_position"].tolist() == [1, 2, 3]


def test_top_n_keeps_the_overall_ranking():
    """Rank 1 is the busiest route overall, not just of the routes kept."""
    df = fact_table([
        {"route": "A-B", "source": "A", "destination": "B"},
        {"route": "A-B", "source": "A", "destination": "B"},
        {"route": "C-D", "source": "C", "destination": "D"},
    ])

    result = metrics.popular_routes(df, top_n=1)

    assert len(result) == 1
    assert result["route"].iloc[0] == "A-B"
    assert result["rank_position"].iloc[0] == 1


def test_kpi_columns_match_the_database_tables():
    """A rename in the DDL that is not mirrored in the code fails here, not on load."""
    df = fact_table([{}])

    assert set(metrics.average_fare_by_airline(df).columns) == {
        "airline", "average_base_fare", "average_tax_charge",
        "average_total_fare", "min_total_fare", "max_total_fare", "flight_count",
    }
    assert set(metrics.seasonal_fare_variation(df).columns) == {
        "seasonality", "is_peak_season", "average_total_fare",
        "flight_count", "variation_vs_baseline_pct",
    }
    assert set(metrics.booking_count_by_airline(df).columns) == {
        "airline", "booking_count", "booking_share_pct",
    }
    assert set(metrics.popular_routes(df).columns) == {
        "route", "source", "destination", "booking_count",
        "average_total_fare", "rank_position",
    }
