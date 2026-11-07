CREATE OR REFRESH MATERIALIZED VIEW books_rating_stats
COMMENT 'Aggregated book rating statistics, one row per ISBN'
AS
SELECT b.isbn,
    b.book_title,
    b.book_author,
    b.year_of_publication,
    b.publisher,
    COUNT(r.user_id) AS total_ratings,
    COUNT(CASE WHEN r.book_rating > 0 THEN 1 END) AS explicit_ratings,
    COUNT(CASE WHEN r.book_rating = 0 THEN 1 END) AS implicit_ratings,
    SUM(r.book_rating) AS sum_ratings
FROM silver.books b
LEFT JOIN silver.ratings r
ON r.isbn = b.isbn
GROUP BY b.isbn, b.book_title, b.book_author, b.year_of_publication, b.publisher
