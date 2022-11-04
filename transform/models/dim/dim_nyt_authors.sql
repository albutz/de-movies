{{
    config(materialized='incremental')
}}

WITH nyt_reviews AS (
    SELECT * FROM {{ ref('nyt_reviews_cleansed') }}
),
author_names AS (
    SELECT 
        DISTINCT author_name AS name
    FROM 
        nyt_reviews
)

SELECT 
    {{ dbt_utils.surrogate_key(['name']) }} AS id,
    name
FROM 
    author_names
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}
