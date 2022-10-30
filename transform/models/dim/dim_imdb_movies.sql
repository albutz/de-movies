{{
    config(materialized='incremental')
}}

WITH imdb_basics AS (
    SELECT * FROM {{ ref('imdb_basics_cleansed') }}
),
imdb_basics_unique AS (
    SELECT
        primary_title,
        year
    FROM
        imdb_basics
    GROUP BY
        primary_title,
        year
    HAVING COUNT(*) = 1
)

SELECT
    id,
    x.primary_title AS name,
    x.year AS year,
    runtime_minutes
FROM
    imdb_basics_unique x
    JOIN imdb_basics y
    ON x.primary_title = y.primary_title AND x.year = y.year
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}
