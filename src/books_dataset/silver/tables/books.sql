-- 1) Valid, and typed book rows: a temporary view for the CDC flow to consume
CREATE OR REPLACE TEMPORARY VIEW books_valid
COMMENT 'Bronze books rows passing silver quality checks, with typed casts'
AS SELECT
    ISBN                             AS isbn,
    `Book-Title`                     AS book_title,
    `Book-Author`                    AS book_author,
    CAST(`Year-Of-Publication` AS INT) AS year_of_publication,
    Publisher                        AS publisher,
    `Image-URL-S`                    AS image_url_s,
    `Image-URL-M`                    AS image_url_m,
    `Image-URL-L`                    AS image_url_l,
    _ingestion_time                
FROM STREAM(bronze.books_raw)
WHERE ${catalog}.silver.valid_book_data(ISBN, `Book-Title`, `Book-Author`, `Year-Of-Publication`, Publisher);

-- 2) Silver target
CREATE OR REFRESH STREAMING TABLE books
COMMENT 'Cleaned, deduplicated book records — one row per ISBN, latest version wins.';

-- 3) CDC flow: latest entry per ISBN
CREATE FLOW books_flow AS AUTO CDC INTO books
FROM STREAM(books_valid)
KEYS (isbn)
SEQUENCE BY _ingestion_time
STORED AS SCD TYPE 1;

-- 4) Quarantine: inverse filter, keeps invalid rows for later inspection
CREATE OR REFRESH STREAMING TABLE books_invalid
COMMENT 'Bronze books rows that failed silver quality checks.'
AS SELECT
    ISBN                      AS isbn,
    `Book-Title`              AS book_title,
    `Book-Author`             AS book_author,
    `Year-Of-Publication`     AS year_of_publication, 
    Publisher                 AS publisher,
    `Image-URL-S`             AS image_url_s,
    `Image-URL-M`             AS image_url_m,
    `Image-URL-L`             AS image_url_l,
    _rescued_data,
    _source_file_path,
    _ingestion_time
FROM STREAM(bronze.books_raw)
WHERE NOT ${catalog}.silver.valid_book_data(ISBN, `Book-Title`, `Book-Author`, `Year-Of-Publication`, Publisher);