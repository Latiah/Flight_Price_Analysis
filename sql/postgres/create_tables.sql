-- PostgreSQL analytics schema: one enriched fact table plus one table per
-- required KPI.
--
-- Run automatically by the `postgres` container on first startup (mounted into
-- /docker-entrypoint-initdb.d by docker-compose.yml). The database itself is
-- created from POSTGRES_DB, so this script only defines tables and connects to
-- the analytics database already.
--
-- The KPIs are materialised as tables rather than views because the pipeline is
-- a scheduled batch job: each run recomputes them in pandas and reloads them,
-- so the values are a stored result of a specific run, not a live query over
-- the fact table.

-- Enriched fact table. Column names mirror the MySQL staging table
-- (sql/mysql/create_staging_table.sql) so a row keeps one identity end to end;
-- the trailing columns are what the transformation stage adds.
CREATE TABLE IF NOT EXISTS flight_prices (
    id                      BIGSERIAL PRIMARY KEY,
    airline                 VARCHAR(100)    NOT NULL,
    source                  VARCHAR(10)     NOT NULL,
    source_name             VARCHAR(255),
    destination             VARCHAR(10)     NOT NULL,
    destination_name        VARCHAR(255),
    departure_datetime      TIMESTAMP,
    arrival_datetime        TIMESTAMP,
    duration_hrs            DOUBLE PRECISION,
    stopovers               VARCHAR(50),
    aircraft_type           VARCHAR(100),
    flight_class            VARCHAR(50),
    booking_source          VARCHAR(50),
    base_fare_bdt           NUMERIC(14, 4)  NOT NULL,
    tax_surcharge_bdt       NUMERIC(14, 4)  NOT NULL,
    total_fare_bdt          NUMERIC(14, 4)  NOT NULL,
    seasonality             VARCHAR(50),
    days_before_departure   INTEGER,
    -- Enrichment added by the transformation stage.
    route                   VARCHAR(25),    -- 'DAC-CGP', the grouping key for the route KPI
    is_peak_season          BOOLEAN,        -- derived from seasonality; drives the seasonal KPI
    -- True where total_fare_bdt is exactly (base + tax) * 1.20 rather than
    -- (base + tax). That pattern holds on ~4.4% of source rows with the factor
    -- identical to floating-point precision, so it is a systematic 20% surcharge
    -- rather than corrupt data. The rows are loaded with their stated total, and
    -- this flag is what lets an analyst include or exclude them knowingly --
    -- they do raise the average fare for the airlines carrying them.
    has_fare_uplift         BOOLEAN,
    -- Load metadata: which file a row came from and which DAG run wrote it.
    _source_file            VARCHAR(255),
    _loaded_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes match the group-by keys of the four KPIs, so each aggregation reads
-- an index rather than sequentially scanning the whole fact table.
CREATE INDEX IF NOT EXISTS idx_flight_prices_airline     ON flight_prices (airline);
CREATE INDEX IF NOT EXISTS idx_flight_prices_route       ON flight_prices (route);
CREATE INDEX IF NOT EXISTS idx_flight_prices_seasonality ON flight_prices (seasonality);

-- KPI 1: Average Fare by Airline — mean total fare grouped by airline.
CREATE TABLE IF NOT EXISTS kpi_average_fare_by_airline (
    airline             VARCHAR(100)    PRIMARY KEY,
    average_base_fare   NUMERIC(14, 4),
    average_tax_charge  NUMERIC(14, 4),
    average_total_fare  NUMERIC(14, 4)  NOT NULL,
    min_total_fare      NUMERIC(14, 4),
    max_total_fare      NUMERIC(14, 4),
    flight_count        INTEGER         NOT NULL,
    computed_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 2: Seasonal Fare Variation — average fare per season, with the gap
-- against the non-peak baseline so peak vs. non-peak is readable from one row
-- without re-joining the table to itself.
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    seasonality             VARCHAR(50)     PRIMARY KEY,
    is_peak_season          BOOLEAN         NOT NULL,
    average_total_fare      NUMERIC(14, 4)  NOT NULL,
    flight_count            INTEGER         NOT NULL,
    -- Percentage difference from the non-peak ('Regular') average.
    variation_vs_baseline_pct NUMERIC(8, 2),
    computed_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 3: Booking Count by Airline — one source row is one booking; see the
-- project README on why, given the source data has no booking identifier.
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    airline         VARCHAR(100)    PRIMARY KEY,
    booking_count   INTEGER         NOT NULL,
    -- Share of all bookings, so airlines stay comparable across reruns whose
    -- total row count differs.
    booking_share_pct NUMERIC(8, 2),
    computed_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 4: Most Popular Routes — source/destination pairs ranked by booking count.
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    route               VARCHAR(25)     PRIMARY KEY,
    source              VARCHAR(10)     NOT NULL,
    destination         VARCHAR(10)     NOT NULL,
    booking_count       INTEGER         NOT NULL,
    average_total_fare  NUMERIC(14, 4),
    -- Stored rather than derived at query time: it records the ranking as of
    -- this run, which a later ORDER BY over changed data would not reproduce.
    rank_position       INTEGER,
    computed_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
