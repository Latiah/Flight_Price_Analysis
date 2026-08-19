"""Tests for src/validation/validators.py.

Plain functions and small DataFrames, so each test can be read on its own.
No database is involved — the validation rules are pure pandas.
"""

import pandas as pd
import pytest

from src.validation import validators


def a_valid_row(**changes):
    """One row that passes every rule. Pass changes to break exactly one thing."""
    row = {
        "airline": "Biman Bangladesh Airlines",
        "source": "DAC",
        "destination": "CGP",
        "base_fare_bdt": 10_000.0,
        "tax_surcharge_bdt": 200.0,
        "total_fare_bdt": 10_200.0,  # base + tax
    }
    row.update(changes)
    return pd.DataFrame([row])


def test_a_clean_row_passes():
    results, report = validators.validate_dataframe(a_valid_row())

    assert results["is_valid"].iloc[0] == True
    assert results["validation_errors"].iloc[0] is None
    assert report["valid_rows"] == 1


def test_a_missing_airline_is_flagged():
    results, _ = validators.validate_dataframe(a_valid_row(airline=None))

    assert results["is_valid"].iloc[0] == False
    assert "null_airline" in results["validation_errors"].iloc[0]


def test_a_negative_fare_is_flagged():
    results, _ = validators.validate_dataframe(a_valid_row(base_fare_bdt=-500.0))

    assert results["is_valid"].iloc[0] == False
    assert "negative_base_fare_bdt" in results["validation_errors"].iloc[0]


def test_a_bad_airport_code_is_flagged():
    results, _ = validators.validate_dataframe(a_valid_row(source="DHAKA"))

    assert results["is_valid"].iloc[0] == False
    assert "malformed_source_code" in results["validation_errors"].iloc[0]


def test_the_20_percent_uplift_rows_are_valid():
    """The important case for this dataset.

    On ~4.4% of rows the total is exactly (base + tax) * 1.20. That is a
    systematic surcharge, not bad data, so those rows must pass validation and
    keep their stated total.
    """
    uplifted = a_valid_row(total_fare_bdt=12_240.0)  # (10000 + 200) * 1.20

    results, report = validators.validate_dataframe(uplifted)

    assert results["is_valid"].iloc[0] == True
    assert results["has_known_uplift"].iloc[0] == True
    assert report["rows_with_known_fare_uplift"] == 1


def test_a_total_that_matches_no_rule_is_flagged():
    """Neither base + tax nor the 1.20x uplift, so it really is inconsistent."""
    results, _ = validators.validate_dataframe(a_valid_row(total_fare_bdt=11_000.0))

    assert results["is_valid"].iloc[0] == False
    assert "fare_mismatch" in results["validation_errors"].iloc[0]


def test_missing_required_column_stops_the_run():
    no_total = a_valid_row().drop(columns=["total_fare_bdt"])

    with pytest.raises(validators.SchemaValidationError, match="total_fare_bdt"):
        validators.validate_dataframe(no_total)


def test_too_much_bad_data_fails_the_pipeline():
    """95% of rows must be valid; below that the DAG stops on purpose."""
    poor_quality = {"valid_ratio": 0.10, "failed_checks": {"null_airline": 9}}

    with pytest.raises(ValueError, match="below threshold"):
        validators.enforce_quality_threshold(poor_quality)
