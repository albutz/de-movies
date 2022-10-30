WITH fct AS (
    SELECT * FROM {{ ref('fct_imdb_nyt') }}
)

SELECT
    genre,
    opening_year,
    COUNT(*) AS n_movies,
    AVG(imdb_rating) AS avg_imdb_rating,
    AVG(runtime_minutes) AS avg_runtime_minutes,
    SUM(is_nyt_critics_pick::INTEGER) AS n_nyt_critics_picks
FROM
    fct
GROUP BY 
    CUBE (genre, opening_year)
ORDER BY
    genre, 
    opening_year
