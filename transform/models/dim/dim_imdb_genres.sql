{{
    config(materialized='incremental')
}}

WITH genres AS (
    SELECT DISTINCT genres_array FROM {{ ref('imdb_basics_cleansed') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['value']) }} AS id,
    value::STRING AS genre
FROM (
    SELECT
        DISTINCT value
    FROM
        genres, 
        TABLE(FLATTEN(genres.genres_array)))
{% if is_incremental() %}
WHERE
    id NOT IN (SELECT id FROM {{ this }})
{% endif %}
ORDER BY
    value

