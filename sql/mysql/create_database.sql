-- Creates the MySQL staging database.
-- Run automatically by the `mysql` container on first startup of an empty data
-- directory (mounted into /docker-entrypoint-initdb.d by docker-compose.yml).
-- Redundant with MYSQL_DATABASE, and deliberately so: it keeps the staging
-- schema fully described by the SQL in this directory rather than half of it
-- living in a compose environment variable.

CREATE DATABASE IF NOT EXISTS flight_staging
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
