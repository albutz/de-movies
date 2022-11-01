WITH imdb_ratings AS (
    SELECT * FROM {{ ref('imdb_ratings_cleansed') }}
)

SELECT *
FROM imdb_ratings
WHERE num_votes <= 0
