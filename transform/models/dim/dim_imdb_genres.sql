{{
    config(materialized='incremental')
}}

WITH genres AS (
    SELECT  
        id, 
        genres_array 
    FROM 
        {{ ref('imdb_basics_cleansed') }}
)

SELECT
    id,
    value::STRING AS genre
FROM
    genres,
    TABLE(FLATTEN(genres.genres_array))
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}

