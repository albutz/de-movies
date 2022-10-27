{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH imdb_ratings AS (
    SELECT *
    FROM {{ source('movies', 'imdb_ratings') }}
),
imdb_basics AS (
    SELECT *
    FROM {{ ref('imdb_basics_cleansed') }}
)

SELECT
    r.id AS id,
    CURRENT_TIMESTAMP() AS updated_at,
    average_rating,
    num_votes
FROM
    imdb_ratings r
    JOIN imdb_basics b 
    ON r.id = b.id
-- The updated_at field will be updated with every run, but all other fields
-- and new records will be updated / inserted incrementally.
{% if is_incremental() %}
WHERE
    updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
