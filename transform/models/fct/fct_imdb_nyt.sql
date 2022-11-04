WITH imdb_basics AS (
    SELECT * FROM {{ ref('imdb_basics_cleansed') }}
),
imdb_ratings AS (
    SELECT * FROM {{ ref('imdb_ratings_cleansed') }}
),
nyt AS (
    SELECT * FROM {{ ref('nyt_reviews_cleansed') }}
),
dim_imdb_genres AS (
    SELECT * FROM {{ ref('dim_imdb_genres') }}
),
dim_imdb_movies AS (
SELECT * FROM {{ ref('dim_imdb_movies') }}
),
dim_nyt_authors AS (
SELECT * FROM {{ ref('dim_nyt_authors') }}
),
dim_nyt_movies AS (
SELECT * FROM {{ ref('dim_nyt_movies') }}
),
dim_nyt_reviews AS (
SELECT * FROM {{ ref('dim_nyt_reviews') }}
),
-- Combine IMDB tables
imdb_combined AS (
    SELECT
        im.id AS imdb_movie_id,
        average_rating,
        num_votes,
        ig.id AS genre_id,
        -- Join keys with NYT
        im.year AS year,
        im.name AS movie_name
    FROM 
        imdb_ratings ir
        JOIN imdb_basics ib
            ON ir.id = ib.id
        LEFT JOIN dim_imdb_movies im
            ON ib.id = im.id
        LEFT JOIN dim_imdb_genres ig
            ON ARRAY_CONTAINS(ig.genre::VARIANT, genres_array)
),
-- Combine NYT tables
nyt_combined AS (
    SELECT 
        nm.id AS nyt_movie_id,
        na.id AS nyt_author_id,
        nr.id AS nyt_review_id,
        nr.is_critics_pick,
        -- Join keys with IMDB
        nm.title AS movie_name,
        YEAR(nm.opening_date) AS year
    FROM
        nyt n
        JOIN dim_nyt_movies nm
            ON n.movie_title = nm.title
        LEFT JOIN dim_nyt_authors na
            ON n.author_name = na.name
        LEFT JOIN dim_nyt_reviews nr
            ON n.review_headline = nr.headline AND n.review_date = nr.review_date
)

SELECT
    imdb_movie_id,
    nyt_movie_id,
    genre_id,
    nyt_author_id,
    nyt_review_id,
    average_rating,
    num_votes,
    is_critics_pick
FROM
    imdb_combined x
    LEFT JOIN nyt_combined y
        ON x.movie_name = y.movie_name AND x.movie_name = y.movie_name
