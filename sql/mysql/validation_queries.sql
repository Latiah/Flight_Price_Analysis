-- Inspection queries for the MySQL staging table.
--
-- Not run by the pipeline. These are the queries to reach for after a run when
-- the validation task reports findings and you need to see which rows they are.
-- The validation stage labels rows rather than deleting them precisely so that
-- these queries have something to find.

USE flight_staging;

-- Overall verdict for the current staging contents.
SELECT
    COUNT(*)                                        AS total_rows,
    SUM(is_valid = 1)                               AS valid_rows,
    SUM(is_valid = 0)                               AS invalid_rows,
    SUM(is_valid IS NULL)                           AS unlabelled_rows,
    ROUND(100 * SUM(is_valid = 1) / COUNT(*), 2)    AS valid_pct
FROM stg_flight_prices;

-- Unlabelled rows mean ingestion ran but validation did not; the pipeline
-- leaves is_valid NULL at load time and fills it in during validation.

-- Which rules fired, and how often. validation_errors holds a ';'-separated
-- list, so a row failing several rules appears under each combination — read
-- this as "distinct failure signatures" rather than as per-rule totals.
SELECT
    validation_errors,
    COUNT(*) AS rows_affected
FROM stg_flight_prices
WHERE is_valid = 0
GROUP BY validation_errors
ORDER BY rows_affected DESC;

-- The flagged rows themselves, worst first.
SELECT
    id, airline, source, destination,
    base_fare_bdt, tax_surcharge_bdt, total_fare_bdt,
    validation_errors
FROM stg_flight_prices
WHERE is_valid = 0
ORDER BY id
LIMIT 100;

-- Fare arithmetic across the whole table, split into the three cases the
-- validation stage distinguishes. The middle bucket is the documented 20%
-- surcharge: total = (base + tax) * 1.20 exactly. Those rows are valid and are
-- loaded with their stated total, because recomputing it would erase a real
-- price premium.
SELECT
    CASE
        WHEN ABS(total_fare_bdt - (base_fare_bdt + tax_surcharge_bdt)) <= 0.01
            THEN 'reconciles exactly'
        WHEN base_fare_bdt + tax_surcharge_bdt <> 0
             AND ABS(total_fare_bdt / (base_fare_bdt + tax_surcharge_bdt) - 1.20) <= 0.000001
            THEN 'known 1.20x uplift'
        ELSE 'unexplained mismatch'
    END                                             AS fare_case,
    COUNT(*)                                        AS rows_affected,
    ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM stg_flight_prices), 2) AS pct_of_table
FROM stg_flight_prices
GROUP BY fare_case
ORDER BY rows_affected DESC;

-- Null counts in the columns the brief marks as required. Uses SUM(... IS NULL)
-- rather than COUNT so every column is one row of one result set.
SELECT 'airline' AS column_name, SUM(airline IS NULL) AS null_rows FROM stg_flight_prices
UNION ALL SELECT 'source',            SUM(source IS NULL)            FROM stg_flight_prices
UNION ALL SELECT 'destination',       SUM(destination IS NULL)       FROM stg_flight_prices
UNION ALL SELECT 'base_fare_bdt',     SUM(base_fare_bdt IS NULL)     FROM stg_flight_prices
UNION ALL SELECT 'tax_surcharge_bdt', SUM(tax_surcharge_bdt IS NULL) FROM stg_flight_prices
UNION ALL SELECT 'total_fare_bdt',    SUM(total_fare_bdt IS NULL)    FROM stg_flight_prices;

-- Distinct location codes, to confirm they are all three-letter IATA codes.
SELECT source AS code, COUNT(*) AS rows_affected
FROM stg_flight_prices
GROUP BY source
ORDER BY code;

-- Per-airline validity, to show whether a data problem is concentrated in one
-- carrier's rows or spread evenly across the file.
SELECT
    airline,
    COUNT(*)            AS total_rows,
    SUM(is_valid = 0)   AS invalid_rows
FROM stg_flight_prices
GROUP BY airline
HAVING invalid_rows > 0
ORDER BY invalid_rows DESC;

-- Provenance of the current contents. Ingestion truncates before loading, so a
-- single row here confirms the table holds exactly one file's worth of data.
SELECT _source_file, _ingested_at, COUNT(*) AS rows_loaded
FROM stg_flight_prices
GROUP BY _source_file, _ingested_at;
