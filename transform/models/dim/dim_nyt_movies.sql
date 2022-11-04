{{
    config(materialized='incremental')
}}

WITH nyt_reviews AS (
    SELECT * FROM {{ ref('nyt_reviews_cleansed') }}
),
unique_movies AS (
    SELECT
        movie_title,
        opening_date
    FROM 
        nyt_reviews
    GROUP BY
        movie_title,
        opening_date
    HAVING
        COUNT(*) = 1
)

SELECT 
    {{ dbt_utils.surrogate_key(['title', 'opening_date']) }} AS id,
    *
FROM (
    SELECT 
        x.movie_title AS title,
        x.opening_date AS opening_date,
        mpaa_rating
    FROM 
        unique_movies x
        JOIN nyt_reviews y
        ON x.movie_title = y.movie_title AND x.opening_date = y.opening_date
)
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}
