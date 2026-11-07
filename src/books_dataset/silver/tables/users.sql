-- 1) Valid, and typed users rows
CREATE OR REPLACE TEMPORARY VIEW users_valid
COMMENT 'Bronze users rows passing silver quality checks, with typed casts'
AS SELECT
    `User-ID`                 AS user_id,
    nullif(nullif(lower(trim(split(Location, ',')[0])), ''), 'n/a') AS city,
    nullif(nullif(lower(trim(split(Location, ',')[1])), ''), 'n/a') AS state,
    nullif(nullif(lower(trim(split(Location, ',')[2])), ''), 'n/a') AS country,
    CAST(Age AS DOUBLE)     AS age,
    _ingestion_time
FROM STREAM(bronze.users_raw)
WHERE ${catalog}.silver.valid_users_data(`User-ID`, Location, Age);

-- 2) Silver target
CREATE OR REFRESH STREAMING TABLE users
COMMENT 'Cleaned, deduplicated user records — one row per user, latest version wins.';

-- 3) CDC flow: latest entry per user
CREATE FLOW users_flow AS AUTO CDC INTO users
FROM STREAM(users_valid)
KEYS (user_id)
SEQUENCE BY _ingestion_time
STORED AS SCD TYPE 1;

-- 4) Quarantine: inverse filter, keeps invalid rows for later inspection
CREATE OR REFRESH STREAMING TABLE users_invalid
COMMENT 'Bronze users rows that failed silver quality checks.'
AS SELECT
    `User-ID`                 AS user_id,
    Location                  AS location,
    Age                       AS age,
    _rescued_data,
    _source_file_path,
    _ingestion_time
FROM STREAM(bronze.users_raw)
WHERE NOT ${catalog}.silver.valid_users_data(`User-ID`, Location, Age);