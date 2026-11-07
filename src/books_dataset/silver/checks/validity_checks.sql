USE CATALOG {{catalog}};
USE SCHEMA silver;
CREATE OR REPLACE FUNCTION valid_book_data(
    isbn STRING,
    book_title STRING,
    book_author STRING,
    year_of_publication STRING,
    publisher STRING)
RETURNS BOOLEAN
DETERMINISTIC
COMMENT 'True when a bronze books row passes all silver quality rules'
RETURN
    isbn IS NOT NULL AND isbn RLIKE '^[0-9]{9}[0-9Xx]$'
    AND book_title IS NOT NULL AND trim(book_title) != ''
    AND book_author IS NOT NULL AND trim(book_author) != ''
    AND year_of_publication IS NOT NULL AND year_of_publication RLIKE '^[0-9]{4}$'
        AND try_cast(year_of_publication AS INT) BETWEEN 1400 AND 2100
    AND publisher IS NOT NULL AND trim(publisher) != '';



CREATE OR REPLACE FUNCTION valid_ratings_data(
    user_id STRING,
    isbn STRING,
    book_rating STRING)
RETURNS BOOLEAN
DETERMINISTIC
COMMENT 'True when a bronze ratings row passes all silver quality rules'
RETURN
    user_id IS NOT NULL
    AND isbn IS NOT NULL AND isbn RLIKE '^[0-9]{9}[0-9Xx]$'
    AND try_cast(book_rating AS INT) IS NOT NULL
    AND try_cast(book_rating AS INT) BETWEEN 0 AND 10;



CREATE OR REPLACE FUNCTION valid_users_data(
    user_id STRING,
    location STRING,
    age STRING)
RETURNS BOOLEAN
DETERMINISTIC
COMMENT 'True when a bronze users row passes all silver quality rules. Age is optional but checked for validity if set.'
RETURN
    user_id IS NOT NULL
    AND location IS NOT NULL AND trim(location) != ''
        AND size(split(location, ',')) = 3
    AND (age IS NULL OR (try_cast(age AS DOUBLE) IS NOT NULL AND try_cast(age AS DOUBLE) BETWEEN 5 AND 100));
