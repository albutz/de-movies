WITH nyt_reviews AS (
    SELECT * FROM {{ ref('nyt_reviews_cleansed') }}
)

SELECT * 
FROM nyt_reviews
WHERE LENGTH(movie_title) = 0
