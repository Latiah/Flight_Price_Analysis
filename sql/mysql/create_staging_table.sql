-- MySQL staging table.
--
-- Purpose: land the source CSV data with minimal transformation, so that
-- validation always has an unmodified copy of exactly what was ingested.
-- Column names mirror src/config/settings.py CSV_COLUMN_MAP.
--
-- `is_valid` and `validation_errors` start NULL at load time and are
-- populated by the validation stage (src/validation/validators.py) — they
-- are not computed during ingestion, keeping ingestion and validation as
-- separate, single-responsibility pipeline stages.

USE flight_staging;

CREATE TABLE IF NOT EXISTS stg_flight_prices (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Source columns (see settings.CSV_COLUMN_MAP for the raw CSV header mapping)
    airline                 VARCHAR(100),
    source                  VARCHAR(10),
    source_name             VARCHAR(255),
    destination             VARCHAR(10),
    destination_name        VARCHAR(255),
    departure_datetime      DATETIME,
    arrival_datetime        DATETIME,
    duration_hrs            DOUBLE,
    stopovers               VARCHAR(50),
    aircraft_type           VARCHAR(100),
    flight_class            VARCHAR(50),
    booking_source          VARCHAR(50),
    base_fare_bdt           DECIMAL(14, 4),
    tax_surcharge_bdt       DECIMAL(14, 4),
    total_fare_bdt          DECIMAL(14, 4),
    seasonality             VARCHAR(50),
    days_before_departure   INT,
    -- Ingestion metadata
    _source_file            VARCHAR(255),
    _ingested_at            DATETIME,
    -- Validation metadata (populated by the validation task)
    is_valid                BOOLEAN NULL,
    validation_errors       TEXT NULL,
    -- Supports the validation task's per-airline / per-route inspection
    -- queries and the "is_valid" filter used when reading valid rows for
    -- transformation.
    INDEX idx_stg_is_valid (is_valid),
    INDEX idx_stg_airline (airline),
    INDEX idx_stg_route (source, destination)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
