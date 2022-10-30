{{
    config(materialized='incremental')
}}

WITH imdb_ratings AS (
    SELECT * FROM {{ ref('imdb_ratings_cleansed') }}
),
unique_ids AS (
    SELECT id
    FROM imdb_ratings
    GROUP BY id
    HAVING COUNT(*) = 1
)

SELECT
    id, 
    average_rating AS rating,
    num_votes
FROM 
    imdb_ratings
WHERE
    id IN (SELECT * FROM unique_ids)
{% if is_incremental() %}
    AND id NOT IN (SELECT id FROM {{ this }})
{% endif %}
