WITH imdb_ratings AS (
    SELECT *
    FROM {{ ref('imdb_rating_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

SELECT
    r.id AS id,
    average_rating,
    num_votes
FROM
    imdb_ratings r
    
