-- 1) Valid, and typed ratings rows
CREATE OR REPLACE TEMPORARY VIEW ratings_valid
COMMENT 'Bronze ratings rows passing silver quality checks, with typed casts'
AS SELECT
    ISBN                             AS isbn,
    `User-ID`                     AS user_id,
    CAST(`Book-Rating` AS INT) AS book_rating,
    _ingestion_time                
FROM STREAM(bronze.ratings_raw)
WHERE ${catalog}.silver.valid_ratings_data(`User-ID`, ISBN, `Book-Rating`);

-- 2) Silver target
CREATE OR REFRESH STREAMING TABLE ratings
COMMENT 'Cleaned, deduplicated rating records — one row per user-book pair, latest version wins.';

-- 3) CDC flow: latest entry per user-book pair
CREATE FLOW ratings_flow AS AUTO CDC INTO ratings
FROM STREAM(ratings_valid)
KEYS (user_id, isbn)
SEQUENCE BY _ingestion_time
STORED AS SCD TYPE 1;

-- 4) Quarantine: inverse filter, keeps invalid rows for later inspection
CREATE OR REFRESH STREAMING TABLE ratings_invalid
COMMENT 'Bronze ratings rows that failed silver quality checks.'
AS SELECT
    ISBN                      AS isbn,
    `User-ID`                 AS user_id,
    `Book-Rating`             AS book_rating,
    _rescued_data,
    _source_file_path,
    _ingestion_time
FROM STREAM(bronze.ratings_raw)
WHERE NOT ${catalog}.silver.valid_ratings_data(`User-ID`, ISBN, `Book-Rating`);