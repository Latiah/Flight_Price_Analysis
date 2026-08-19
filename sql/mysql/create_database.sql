-- Creates the MySQL staging database.
-- Run once during environment setup (see scripts/setup_mysql.sh).

CREATE DATABASE IF NOT EXISTS flight_staging
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
