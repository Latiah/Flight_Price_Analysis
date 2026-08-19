"""CSV -> MySQL staging load.

Ingestion deliberately does as little as possible: rename columns to their
staging names, parse the two datetime fields, stamp provenance, insert. No
cleaning, no filtering, no fare arithmetic. Keeping it a faithful copy of the
source means the validation stage always has an unmodified record of exactly
what arrived, and a surprising KPI can be traced back to a real input row rather
than to something ingestion silently changed.

The engine is passed in rather than constructed here so the loader can be
exercised against any SQLAlchemy target, including an in-memory database.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from src.config import settings

log = logging.getLogger(__name__)

# Parsed at read time so a malformed timestamp surfaces during ingestion rather
# than as a confusing type error several tasks later.
DATETIME_COLUMNS = ("departure_datetime", "arrival_datetime")

# Written by ingestion; the validation columns are left NULL for the validation
# stage to fill in.
INGESTION_METADATA_COLUMNS = ("_source_file", "_ingested_at")


def check_source_file(csv_path: Path | None = None) -> str:
    """Confirm the source CSV exists and is not empty.

    Raising here stops the run on its first task, with a message naming the path
    that was actually checked, instead of letting a missing file surface as an
    opaque pandas error part-way through the load.
    """
    path = Path(csv_path or settings.RAW_CSV_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Source CSV not found at {path}. Place the dataset there "
            f"(host path: data/raw/{path.name}) and retrigger the DAG."
        )
    if path.stat().st_size == 0:
        raise ValueError(f"Source CSV at {path} is empty.")

    log.info("Source file OK: %s (%.1f MB)", path, path.stat().st_size / 1_048_576)
    return str(path)


def prepare_chunk(chunk: pd.DataFrame, source_file: str, ingested_at: datetime) -> pd.DataFrame:
    """Map one raw CSV chunk onto the staging table's columns.

    Split out from the load loop so the mapping can be unit tested on a handful
    of rows without a database.
    """
    missing = set(settings.CSV_COLUMN_MAP) - set(chunk.columns)
    if missing:
        raise ValueError(
            f"Source CSV is missing expected column(s): {sorted(missing)}. "
            f"Found: {sorted(chunk.columns)}"
        )

    # Reindex to the mapping's keys so any unexpected extra column in a future
    # version of the dataset is dropped rather than breaking the INSERT.
    df = chunk[list(settings.CSV_COLUMN_MAP)].rename(columns=settings.CSV_COLUMN_MAP)

    for column in DATETIME_COLUMNS:
        # errors="coerce" turns an unparseable timestamp into NaT rather than
        # aborting the load; validation reports it as a row-level problem.
        df[column] = pd.to_datetime(df[column], errors="coerce")

    df["_source_file"] = source_file
    df["_ingested_at"] = ingested_at
    return df


def load_csv_to_staging(engine: Engine, csv_path: Path | None = None) -> dict[str, object]:
    """Load the source CSV into the MySQL staging table, replacing its contents.

    Reads and inserts in chunks so peak memory stays proportional to
    ``INGESTION_CHUNK_SIZE`` rather than to the size of the file.

    Returns a summary suitable for logging and for XCom.
    """
    from src.database import mysql  # imported here to keep this module DB-agnostic

    path = Path(csv_path or settings.RAW_CSV_PATH)
    source_file = path.name
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    mysql.truncate_staging_table(engine)

    rows_loaded = 0
    chunks = 0
    for chunk in pd.read_csv(path, chunksize=settings.INGESTION_CHUNK_SIZE):
        prepared = prepare_chunk(chunk, source_file, ingested_at)
        prepared.to_sql(
            settings.STAGING_TABLE,
            engine,
            if_exists="append",
            index=False,
            chunksize=settings.INSERT_CHUNK_SIZE,
            method="multi",
        )
        rows_loaded += len(prepared)
        chunks += 1
        log.info("Loaded chunk %d (%d rows, %d total)", chunks, len(prepared), rows_loaded)

    # Read the count back from the database rather than trusting the running
    # total: this is the validation the brief asks for, that every CSV row
    # actually landed in the table.
    staged = mysql.count_staging_rows(engine)
    if staged != rows_loaded:
        raise RuntimeError(
            f"Row count mismatch after load: read {rows_loaded} from CSV but "
            f"{staged} rows are present in {settings.STAGING_TABLE}."
        )

    log.info("Ingestion complete: %d rows in %d chunks", staged, chunks)
    return {
        "source_file": source_file,
        "rows_loaded": staged,
        "chunks": chunks,
        "ingested_at": ingested_at.isoformat(),
    }
