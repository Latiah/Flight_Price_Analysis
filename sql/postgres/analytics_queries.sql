-- Ad hoc analytical queries against the PostgreSQL analytics schema.
--
-- Not run by the pipeline. The four KPI tables answer the questions the brief
-- asks; these go past them, using the enriched fact table for the cuts the KPI
-- tables deliberately do not pre-aggregate.
--
-- One caveat applies throughout: the source data has no booking identifier, so
-- one row is one booking (one fare quote). Every count below is a row count.

-- ---------------------------------------------------------------------------
-- 1. The four KPI tables as loaded
-- ---------------------------------------------------------------------------

SELECT * FROM kpi_average_fare_by_airline  ORDER BY average_total_fare DESC;
SELECT * FROM kpi_seasonal_fare_variation  ORDER BY average_total_fare DESC;
SELECT * FROM kpi_booking_count_by_airline ORDER BY booking_count DESC;
SELECT * FROM kpi_popular_routes           ORDER BY rank_position;

-- ---------------------------------------------------------------------------
-- 2. The fare uplift rows
-- ---------------------------------------------------------------------------

-- On ~4.4% of rows the total fare is exactly (base + tax) * 1.20. The pipeline
-- keeps those rows and their stated totals, flagged with has_fare_uplift, rather
-- than "correcting" a systematic surcharge out of the data.

-- How much the uplift rows move each airline's average fare. If an airline's
-- average looks high, this is the first place to check.
SELECT
    airline,
    COUNT(*)                                                    AS total_flights,
    COUNT(*) FILTER (WHERE has_fare_uplift)                     AS uplift_flights,
    ROUND(AVG(total_fare_bdt), 2)                               AS avg_fare_all,
    ROUND(AVG(total_fare_bdt) FILTER (WHERE NOT has_fare_uplift), 2) AS avg_fare_excluding_uplift,
    ROUND(
        AVG(total_fare_bdt) - AVG(total_fare_bdt) FILTER (WHERE NOT has_fare_uplift),
        2
    )                                                           AS uplift_effect_bdt
FROM flight_prices
GROUP BY airline
ORDER BY uplift_effect_bdt DESC NULLS LAST;

-- Confirm the factor really is 1.20 on every flagged row: min and max of the
-- ratio should be identical.
SELECT
    COUNT(*)                                                            AS uplift_rows,
    MIN(total_fare_bdt / (base_fare_bdt + tax_surcharge_bdt))            AS min_ratio,
    MAX(total_fare_bdt / (base_fare_bdt + tax_surcharge_bdt))            AS max_ratio
FROM flight_prices
WHERE has_fare_uplift
  AND base_fare_bdt + tax_surcharge_bdt <> 0;

-- ---------------------------------------------------------------------------
-- 3. Seasonality beyond the KPI table
-- ---------------------------------------------------------------------------

-- Peak vs. non-peak as a single comparison, which is the headline the seasonal
-- KPI table stores per-season.
SELECT
    is_peak_season,
    COUNT(*)                        AS flights,
    ROUND(AVG(total_fare_bdt), 2)   AS avg_total_fare,
    ROUND(AVG(base_fare_bdt), 2)    AS avg_base_fare,
    ROUND(AVG(tax_surcharge_bdt), 2) AS avg_tax_surcharge
FROM flight_prices
GROUP BY is_peak_season
ORDER BY is_peak_season DESC;

-- Which airlines raise fares most in peak season. The KPI tables cover season
-- and airline separately; this is the interaction between them.
SELECT
    airline,
    ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season), 2)      AS avg_peak_fare,
    ROUND(AVG(total_fare_bdt) FILTER (WHERE NOT is_peak_season), 2)  AS avg_offpeak_fare,
    ROUND(
        100 * (AVG(total_fare_bdt) FILTER (WHERE is_peak_season)
             - AVG(total_fare_bdt) FILTER (WHERE NOT is_peak_season))
            / NULLIF(AVG(total_fare_bdt) FILTER (WHERE NOT is_peak_season), 0),
        2
    )                                                                AS peak_premium_pct
FROM flight_prices
GROUP BY airline
HAVING COUNT(*) FILTER (WHERE is_peak_season) > 0
ORDER BY peak_premium_pct DESC NULLS LAST;

-- Monthly fare trend, from the departure timestamp.
SELECT
    DATE_TRUNC('month', departure_datetime)::date   AS departure_month,
    COUNT(*)                                        AS flights,
    ROUND(AVG(total_fare_bdt), 2)                   AS avg_total_fare
FROM flight_prices
WHERE departure_datetime IS NOT NULL
GROUP BY departure_month
ORDER BY departure_month;

-- ---------------------------------------------------------------------------
-- 4. Booking lead time and cabin class
-- ---------------------------------------------------------------------------

-- Does booking earlier cost less? Buckets rather than a correlation, so the
-- shape of the relationship is visible rather than collapsed to one number.
SELECT
    CASE
        WHEN days_before_departure <= 7  THEN '1. 0-7 days'
        WHEN days_before_departure <= 14 THEN '2. 8-14 days'
        WHEN days_before_departure <= 30 THEN '3. 15-30 days'
        WHEN days_before_departure <= 60 THEN '4. 31-60 days'
        ELSE                                  '5. 61+ days'
    END                                 AS lead_time_bucket,
    COUNT(*)                            AS flights,
    ROUND(AVG(total_fare_bdt), 2)       AS avg_total_fare
FROM flight_prices
WHERE days_before_departure IS NOT NULL
GROUP BY lead_time_bucket
ORDER BY lead_time_bucket;

-- Average fare by cabin class and airline.
SELECT
    airline,
    flight_class,
    COUNT(*)                        AS flights,
    ROUND(AVG(total_fare_bdt), 2)   AS avg_total_fare
FROM flight_prices
GROUP BY airline, flight_class
ORDER BY airline, avg_total_fare DESC;

-- What share of the total fare is tax and surcharge, by class. Relevant because
-- the source data floors the tax at 200 BDT on a large share of rows.
SELECT
    flight_class,
    COUNT(*)                                                    AS flights,
    COUNT(*) FILTER (WHERE tax_surcharge_bdt = 200)             AS at_tax_floor,
    ROUND(AVG(100 * tax_surcharge_bdt / NULLIF(total_fare_bdt, 0)), 2) AS avg_tax_pct_of_total
FROM flight_prices
GROUP BY flight_class
ORDER BY avg_tax_pct_of_total DESC;

-- ---------------------------------------------------------------------------
-- 5. Routes
-- ---------------------------------------------------------------------------

-- Busiest routes with their fare spread, which the KPI table reduces to a mean.
SELECT
    route,
    COUNT(*)                        AS bookings,
    ROUND(MIN(total_fare_bdt), 2)   AS cheapest,
    ROUND(AVG(total_fare_bdt), 2)   AS average,
    ROUND(MAX(total_fare_bdt), 2)   AS dearest,
    COUNT(DISTINCT airline)         AS airlines_serving
FROM flight_prices
GROUP BY route
ORDER BY bookings DESC
LIMIT 20;

-- Routes where fares vary most between airlines — where competition or cabin mix
-- matters most to the price paid.
SELECT
    route,
    COUNT(DISTINCT airline)                                     AS airlines_serving,
    ROUND(MIN(airline_avg), 2)                                  AS cheapest_airline_avg,
    ROUND(MAX(airline_avg), 2)                                  AS dearest_airline_avg,
    ROUND(MAX(airline_avg) - MIN(airline_avg), 2)               AS spread_bdt
FROM (
    SELECT route, airline, AVG(total_fare_bdt) AS airline_avg
    FROM flight_prices
    GROUP BY route, airline
) per_airline
GROUP BY route
HAVING COUNT(DISTINCT airline) > 1
ORDER BY spread_bdt DESC
LIMIT 20;

-- Direct vs. connecting, per route.
SELECT
    stopovers,
    COUNT(*)                        AS flights,
    -- duration_hrs is DOUBLE PRECISION, and PostgreSQL's two-argument round()
    -- takes numeric only, so the cast is required here but not for the fare
    -- columns, which are already numeric.
    ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs,
    ROUND(AVG(total_fare_bdt), 2)   AS avg_total_fare
FROM flight_prices
GROUP BY stopovers
ORDER BY avg_total_fare DESC;

-- ---------------------------------------------------------------------------
-- 6. Load sanity checks
-- ---------------------------------------------------------------------------

-- Row counts and provenance. The pipeline replaces table contents on each run,
-- so one distinct _source_file / _loaded_at pair is the expected result.
SELECT
    _source_file,
    MAX(_loaded_at)                             AS loaded_at,
    COUNT(*)                                    AS rows_loaded,
    COUNT(DISTINCT airline)                     AS airlines,
    COUNT(DISTINCT route)                       AS routes,
    COUNT(*) FILTER (WHERE has_fare_uplift)     AS uplift_rows
FROM flight_prices
GROUP BY _source_file;

-- The KPI tables should agree with the fact table they were computed from.
SELECT 'fact rows'            AS metric, COUNT(*)::text AS value FROM flight_prices
UNION ALL
SELECT 'sum of KPI bookings', SUM(booking_count)::text   FROM kpi_booking_count_by_airline
UNION ALL
SELECT 'sum of KPI flights',  SUM(flight_count)::text    FROM kpi_average_fare_by_airline
UNION ALL
SELECT 'distinct fact airlines', COUNT(DISTINCT airline)::text FROM flight_prices
UNION ALL
SELECT 'KPI airline rows',    COUNT(*)::text             FROM kpi_average_fare_by_airline;
