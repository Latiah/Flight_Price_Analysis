"""Tests for src/transformation/transformations.py.

Plain functions and small DataFrames. The transformation rules are pure pandas,
so no database is involved.
"""

import pandas as pd

from src.transformation import transformations


def a_staged_row(**changes):
    """One valid staged row carrying every column the fact table needs."""
    row = {
        "airline": "Biman Bangladesh Airlines",
        "source": "DAC",
        "source_name": "Hazrat Shahjalal International Airport",
        "destination": "CGP",
        "destination_name": "Shah Amanat International Airport",
        "departure_datetime": pd.Timestamp("2025-03-01 08:00:00"),
        "arrival_datetime": pd.Timestamp("2025-03-01 09:00:00"),
        "duration_hrs": 1.0,
        "stopovers": "Direct",
        "aircraft_type": "Airbus A320",
        "flight_class": "Economy",
        "booking_source": "Online Website",
        "base_fare_bdt": 10_000.0,
        "tax_surcharge_bdt": 200.0,
        "total_fare_bdt": 10_200.0,
        "seasonality": "Regular",
        "days_before_departure": 14,
        "_source_file": "Flight_Price_Dataset_of_Bangladesh.csv",
    }
    row.update(changes)
    return pd.DataFrame([row])


def test_codes_are_cleaned_up():
    """Stray case or spaces in a code would otherwise split one route into two."""
    messy = a_staged_row(source=" dac ", destination="cgp")

    cleaned = transformations.clean(messy)

    assert cleaned["source"].iloc[0] == "DAC"
    assert cleaned["destination"].iloc[0] == "CGP"


def test_the_route_key_is_built():
    result = transformations.transform(a_staged_row())

    assert result["route"].iloc[0] == "DAC-CGP"


def test_peak_seasons_are_marked():
    regular = transformations.transform(a_staged_row(seasonality="Regular"))
    eid = transformations.transform(a_staged_row(seasonality="Eid"))

    assert regular["is_peak_season"].iloc[0] == False
    assert eid["is_peak_season"].iloc[0] == True


def test_a_missing_total_is_calculated():
    """The brief's rule: Total Fare = Base Fare + Tax & Surcharge."""
    no_total = a_staged_row(total_fare_bdt=None)

    result = transformations.transform(no_total)

    assert result["total_fare_bdt"].iloc[0] == 10_200.0


def test_an_existing_total_is_never_overwritten():
    """The most important rule here.

    Recalculating every total would erase the 20% surcharge on the uplift rows
    and understate those fares, so a stated total always wins.
    """
    uplifted = a_staged_row(total_fare_bdt=12_240.0)  # (10000 + 200) * 1.20

    result = transformations.transform(uplifted)

    assert result["total_fare_bdt"].iloc[0] == 12_240.0


def test_uplift_rows_are_flagged_for_analysts():
    row = a_staged_row(total_fare_bdt=12_240.0)
    uplift_flags = pd.Series([True], index=row.index)

    result = transformations.transform(row, fare_uplift_flags=uplift_flags)

    assert result["has_fare_uplift"].iloc[0] == True


def test_only_fact_table_columns_come_out():
    """Staging bookkeeping columns must not reach the analytics database."""
    staged = a_staged_row()
    staged["id"] = 1
    staged["is_valid"] = True

    result = transformations.transform(staged)

    assert tuple(result.columns) == transformations.FACT_COLUMNS
    assert "id" not in result.columns
    assert "is_valid" not in result.columns
