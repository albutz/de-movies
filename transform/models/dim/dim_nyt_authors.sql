{{
    config(materialized='incremental')
}}

WITH nyt_reviews AS (
    SELECT * FROM {{ ref('nyt_reviews_cleansed') }}
),
author_names AS (
    SELECT 
        author_name,
        movie_title,
        review_date
    FROM 
        nyt_reviews
)

SELECT 
    {{ dbt_utils.surrogate_key(['author_name', 'movie_title', 'review_date']) }} AS id,
    author_name AS name
FROM 
    author_names
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}
