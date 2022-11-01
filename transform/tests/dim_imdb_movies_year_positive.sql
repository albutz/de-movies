WITH dim_imdb_movies AS (
    SELECT * FROM {{ ref('dim_imdb_movies') }}
)

SELECT * 
FROM dim_imdb_movies
WHERE year <= 0
