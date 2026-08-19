"""Schema, null, numeric, and business-rule validation of staged data.

Pure pandas: every function here takes a DataFrame and returns data. No
database, no Airflow, no file I/O — which is what makes the rules testable
against a handful of hand-written rows.

The design principle is that validation *labels* rows, it does not delete them.
Each row gets an ``is_valid`` flag and a human-readable ``validation_errors``
string, both written back to staging. A flagged row therefore remains available
for inspection instead of vanishing, and the decision to exclude it happens
later and visibly, in the transformation stage.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.config import settings

log = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """Raised when the staged data is structurally unusable.

    Distinct from row-level problems: a missing required column means no row can
    be interpreted, so there is nothing to flag and the run must stop.
    """


def validate_required_columns(df: pd.DataFrame) -> None:
    """Assert that every column the brief requires is present."""
    missing = [column for column in settings.REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise SchemaValidationError(
            f"Staged data is missing required column(s): {missing}. "
            f"Present: {sorted(df.columns)}"
        )
    log.info("Schema check passed: all %d required columns present", len(settings.REQUIRED_COLUMNS))


def classify_fare_consistency(df: pd.DataFrame) -> pd.DataFrame:
    """Compare each row's total fare against base + tax.

    Public because the transformation stage needs the uplift flags too, and
    recomputing this one comparison is far cheaper than rerunning every rule.

    Returns two boolean columns:

    ``reconciles_exactly``
        base + tax == total, within ``FARE_RECONCILIATION_TOLERANCE``.
    ``has_known_uplift``
        total == (base + tax) * ``KNOWN_FARE_UPLIFT_FACTOR``.

    The uplift case exists because ~4.4% of rows in this dataset carry a total
    that is exactly 1.20x the sum of its parts, with the factor identical to
    floating-point precision on every affected row. A systematic 20% surcharge is
    a pricing rule, not a data error, so those rows pass validation and keep
    their stated total. A row matching neither pattern is genuinely inconsistent
    and is flagged.
    """
    base = pd.to_numeric(df["base_fare_bdt"], errors="coerce")
    tax = pd.to_numeric(df["tax_surcharge_bdt"], errors="coerce")
    total = pd.to_numeric(df["total_fare_bdt"], errors="coerce")
    components = base + tax

    reconciles_exactly = (total - components).abs() <= settings.FARE_RECONCILIATION_TOLERANCE

    # Masking zero denominators keeps an all-zero row from dividing by zero and
    # registering a spurious uplift match.
    ratio = total.divide(components.where(components != 0))
    has_known_uplift = np.isclose(
        ratio,
        settings.KNOWN_FARE_UPLIFT_FACTOR,
        rtol=settings.FARE_UPLIFT_RELATIVE_TOLERANCE,
        equal_nan=False,
    )

    return pd.DataFrame(
        {
            "reconciles_exactly": reconciles_exactly.fillna(False),
            "has_known_uplift": pd.Series(has_known_uplift, index=df.index).fillna(False),
        },
        index=df.index,
    )


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Apply every row-level rule and return the labelled rows plus a report.

    The returned DataFrame carries ``is_valid``, ``validation_errors``, and the
    fare-consistency flags the transformation stage reuses, so the fare
    comparison is computed once rather than repeated downstream.

    Errors accumulate: a row failing three rules reports all three, because
    fixing data is much easier with the complete list than with whichever check
    happened to run first.
    """
    validate_required_columns(df)

    if df.empty:
        raise SchemaValidationError("Staged data is empty; nothing to validate.")

    # One list of error labels per row, joined at the end.
    errors: list[list[str]] = [[] for _ in range(len(df))]
    checks: dict[str, int] = {}

    def flag(mask: pd.Series, label: str) -> int:
        """Attach ``label`` to every row where ``mask`` is True."""
        positions = np.flatnonzero(mask.fillna(False).to_numpy())
        for position in positions:
            errors[position].append(label)
        checks[label] = int(positions.size)
        return int(positions.size)

    # 1. Nulls in columns without which a row cannot be analysed.
    for column in settings.NOT_NULL_COLUMNS:
        flag(df[column].isna(), f"null_{column}")

    # 2. Numeric fields must actually parse as numbers.
    for column in settings.NUMERIC_COLUMNS:
        if column not in df.columns:
            continue
        coerced = pd.to_numeric(df[column], errors="coerce")
        flag(coerced.isna() & df[column].notna(), f"non_numeric_{column}")

    # 3. A negative fare is not a price. Flagged rather than clamped, since the
    #    correct value is unknowable from the row itself.
    for column in ("base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"):
        flag(pd.to_numeric(df[column], errors="coerce") < 0, f"negative_{column}")

    # 4. Categorical identity fields must be non-empty strings.
    for column in settings.NON_EMPTY_STRING_COLUMNS:
        blank = df[column].isna() | (df[column].astype("string").str.strip() == "")
        flag(blank, f"blank_{column}")

    # 5. Location codes must look like IATA codes. This is the brief's "invalid
    #    city names" check, expressed against the codes rather than the display
    #    names, because the codes are what the route KPI groups by.
    for column in ("source", "destination"):
        code = df[column].astype("string").str.strip().str.upper()
        malformed = code.notna() & ~code.str.fullmatch(settings.AIRPORT_CODE_PATTERN, na=False)
        flag(malformed, f"malformed_{column}_code")

    # 6. A flight cannot depart from and arrive at the same airport.
    endpoints_equal = df["source"].astype("string").str.strip().str.upper() == df[
        "destination"
    ].astype("string").str.strip().str.upper()
    flag(endpoints_equal, "source_equals_destination")

    # 7. Fare arithmetic, per the classification above.
    consistency = classify_fare_consistency(df)
    unexplained = ~(consistency["reconciles_exactly"] | consistency["has_known_uplift"])
    flag(unexplained, "fare_mismatch")

    # 8. Chronology and range checks. A negative duration or an arrival before
    #    departure means the row's timing cannot be trusted.
    if {"departure_datetime", "arrival_datetime"} <= set(df.columns):
        departure = pd.to_datetime(df["departure_datetime"], errors="coerce")
        arrival = pd.to_datetime(df["arrival_datetime"], errors="coerce")
        flag(arrival < departure, "arrival_before_departure")
    if "duration_hrs" in df.columns:
        flag(pd.to_numeric(df["duration_hrs"], errors="coerce") <= 0, "non_positive_duration")
    if "days_before_departure" in df.columns:
        flag(
            pd.to_numeric(df["days_before_departure"], errors="coerce") < 0,
            "negative_days_before_departure",
        )

    error_strings = pd.Series([";".join(row) for row in errors], index=df.index)
    is_valid = error_strings == ""

    results = pd.DataFrame(
        {
            "is_valid": is_valid,
            # NULL rather than "" for a clean row, so `validation_errors IS NULL`
            # is a meaningful query against staging.
            #
            # Built as an object column holding real ``None`` rather than via
            # ``where(..., other=None)``: on a string-dtype column that yields
            # float NaN, and neither NaN nor pd.NA is something the MySQL driver
            # can adapt to SQL NULL. ``None`` is.
            "validation_errors": pd.Series(
                [None if ok else text for ok, text in zip(is_valid, error_strings)],
                index=df.index,
                dtype=object,
            ),
            "reconciles_exactly": consistency["reconciles_exactly"],
            "has_known_uplift": consistency["has_known_uplift"],
        },
        index=df.index,
    )
    if "id" in df.columns:
        results.insert(0, "id", df["id"].to_numpy())

    valid_count = int(is_valid.sum())
    total_count = len(df)
    report = {
        "total_rows": total_count,
        "valid_rows": valid_count,
        "invalid_rows": total_count - valid_count,
        "valid_ratio": round(valid_count / total_count, 6),
        # Only non-zero checks, so the report reads as a list of findings rather
        # than a wall of zeroes.
        "failed_checks": {name: count for name, count in checks.items() if count},
        "rows_with_known_fare_uplift": int(consistency["has_known_uplift"].sum()),
        "fare_uplift_factor": settings.KNOWN_FARE_UPLIFT_FACTOR,
        "min_valid_ratio_required": settings.MIN_VALID_ROW_RATIO,
    }

    log.info(
        "Validation: %d/%d rows valid (%.2f%%); findings: %s",
        valid_count,
        total_count,
        100 * valid_count / total_count,
        report["failed_checks"] or "none",
    )
    return results, report


def enforce_quality_threshold(report: dict) -> None:
    """Fail the run if too few rows passed validation.

    Deliberately a hard stop. Loading a small, unrepresentative fraction of the
    data would produce KPIs that look plausible and are wrong; stopping makes the
    problem visible while the report is still in the task log.
    """
    ratio = float(report["valid_ratio"])
    if ratio < settings.MIN_VALID_ROW_RATIO:
        raise ValueError(
            f"Data quality below threshold: {ratio:.2%} of rows valid, "
            f"required {settings.MIN_VALID_ROW_RATIO:.2%}. "
            f"Findings: {report['failed_checks']}"
        )
    log.info("Quality threshold met: %.2f%% valid", 100 * ratio)
