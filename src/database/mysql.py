"""MySQL (staging) engine and table helpers.

The engine is built from the Airflow Connection rather than from environment
variables so that the credentials the DAG uses and the ones the UI displays are
always the same object. The driver is named explicitly (`mysql+pymysql`) because
Airflow's own hook may resolve to a different DBAPI depending on which optional
client library is installed; pinning it here keeps behaviour identical wherever
the code runs.
"""

from __future__ import annotations

import logging

import pandas as pd
from airflow.sdk import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

from src.config import settings

log = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Return a SQLAlchemy engine for the staging database."""
    conn = BaseHook.get_connection(settings.MYSQL_CONN_ID)
    url = URL.create(
        "mysql+pymysql",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        database=conn.schema,
    )
    # pre_ping avoids handing out a connection MySQL has already timed out,
    # which is a real risk for a task that sits idle between long chunked loads.
    return create_engine(url, pool_pre_ping=True)


def truncate_staging_table(engine: Engine) -> None:
    """Empty the staging table.

    Called before every load so a rerun replaces the previous run's rows instead
    of appending to them — this is what makes the ingestion task idempotent.
    TRUNCATE rather than DELETE so the AUTO_INCREMENT id restarts and the table
    is genuinely as-new.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {settings.STAGING_TABLE}"))
    log.info("Truncated %s", settings.STAGING_TABLE)


def count_staging_rows(engine: Engine) -> int:
    """Return the number of rows currently in the staging table."""
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {settings.STAGING_TABLE}")).scalar_one())


def read_staging(engine: Engine, valid_only: bool = False) -> pd.DataFrame:
    """Read the staging table into a DataFrame.

    With ``valid_only``, returns just the rows the validation stage marked as
    valid — the filter the transformation stage uses so that flagged rows stay
    in staging for inspection but never reach the analytics database.
    """
    query = f"SELECT * FROM {settings.STAGING_TABLE}"
    if valid_only:
        query += " WHERE is_valid = 1"
    df = pd.read_sql(text(query), engine)
    log.info("Read %d rows from %s (valid_only=%s)", len(df), settings.STAGING_TABLE, valid_only)
    return df


def write_validation_results(engine: Engine, results: pd.DataFrame) -> int:
    """Write per-row validation outcomes back onto the staged rows.

    ``results`` must carry ``id``, ``is_valid``, and ``validation_errors``. The
    updates are applied by joining a temporary table rather than issuing one
    UPDATE per row: at 57k rows the row-at-a-time version takes minutes and
    holds a transaction open the whole time.
    """
    required = {"id", "is_valid", "validation_errors"}
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"validation results missing columns: {sorted(missing)}")

    temp_table = f"_tmp_validation_{settings.STAGING_TABLE}"
    payload = results[["id", "is_valid", "validation_errors"]]

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE {temp_table} (
                    id BIGINT PRIMARY KEY,
                    is_valid BOOLEAN,
                    validation_errors TEXT
                )
                """
            )
        )
        payload.to_sql(
            temp_table,
            conn,
            if_exists="append",
            index=False,
            chunksize=settings.INSERT_CHUNK_SIZE,
            method="multi",
        )
        updated = conn.execute(
            text(
                f"""
                UPDATE {settings.STAGING_TABLE} s
                JOIN {temp_table} t ON t.id = s.id
                SET s.is_valid = t.is_valid,
                    s.validation_errors = t.validation_errors
                """
            )
        ).rowcount
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

    log.info("Wrote validation results for %d rows", updated)
    return int(updated)
