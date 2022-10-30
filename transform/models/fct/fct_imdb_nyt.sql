WITH dim_imdb_movies AS (
    SELECT * FROM {{ ref('dim_imdb_movies') }}
), 
dim_imdb_ratings AS (
    SELECT * FROM {{ ref('dim_imdb_ratings') }}
),
dim_imdb_genres AS (
    SELECT * FROM {{ ref('dim_imdb_genres') }}
),
dim_nyt_movies AS (
    SELECT * FROM {{ ref('dim_nyt_movies') }}
),
dim_nyt_reviews AS (
    SELECT * FROM {{ ref('dim_nyt_reviews') }}
)

SELECT 
    im.name AS movie_name,
    rating AS imdb_rating,
    num_votes AS imdb_num_votes,
    im.year AS opening_year,
    runtime_minutes,
    mpaa_rating,
    genre,
    is_critics_pick AS is_nyt_critics_pick,
    review_date AS nyt_review_date
FROM
    dim_imdb_movies im
    LEFT JOIN dim_imdb_ratings ir
        ON im.id = ir.id
    LEFT JOIN dim_imdb_genres ig
        ON im.id = ig.id
    LEFT JOIN dim_nyt_movies nm
        ON im.name = nm.name AND im.year = YEAR(nm.opening_date)
    LEFT JOIN dim_nyt_reviews nr
        ON nm.id = nr.id
