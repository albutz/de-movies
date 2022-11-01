WITH imdb_ratings AS (
    SELECT * FROM {{ ref('imdb_ratings_cleansed') }}
)

SELECT *
FROM imdb_ratings
WHERE average_rating <= 0
