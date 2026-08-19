"""PostgreSQL (analytics) engine and table helpers.

Mirrors src/database/mysql.py: the engine comes from the Airflow Connection and
names its driver explicitly. The load helper is shared by the fact table and all
four KPI tables, because every one of them follows the same replace-in-place
pattern.
"""

from __future__ import annotations

import logging

import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

from src.config import settings

log = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Return a SQLAlchemy engine for the analytics database."""
    conn = BaseHook.get_connection(settings.POSTGRES_CONN_ID)
    url = URL.create(
        "postgresql+psycopg2",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        database=conn.schema,
    )
    return create_engine(url, pool_pre_ping=True)


def replace_table_contents(engine: Engine, table: str, df: pd.DataFrame) -> int:
    """Replace a table's contents with ``df`` in a single transaction.

    Truncate-then-append rather than pandas' ``if_exists="replace"``: replace
    drops and recreates the table, which would discard the DDL in
    sql/postgres/create_tables.sql — its constraints, defaults, and indexes —
    and let pandas guess the column types instead.

    Both statements share one transaction, so a failure mid-load rolls back to
    the previous contents rather than leaving the table empty or half-written.
    That is what keeps the analytics tables consistent across a failed rerun.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        df.to_sql(
            table,
            conn,
            if_exists="append",
            index=False,
            chunksize=settings.INSERT_CHUNK_SIZE,
            method="multi",
        )
    log.info("Loaded %d rows into %s", len(df), table)
    return len(df)


def read_fact_table(engine: Engine) -> pd.DataFrame:
    """Read the enriched fact table.

    The KPI tasks read from here rather than being handed a DataFrame through
    XCom: the fact table is the durable, already-loaded result of the previous
    stage, so each KPI is computed from exactly the data an analyst can query.
    """
    df = pd.read_sql(text(f"SELECT * FROM {settings.FACT_TABLE}"), engine)
    log.info("Read %d rows from %s", len(df), settings.FACT_TABLE)
    return df


def count_rows(engine: Engine, table: str) -> int:
    """Return the number of rows in ``table``."""
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())
