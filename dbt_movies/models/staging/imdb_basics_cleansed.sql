{% set genres_list = [
    "Comedy",
    "War",
    "Talk-Show",
    "Sci-Fi",
    "Action",
    "Horror",
    "Crime",
    "Short",
    "Drama",
    "History",
    "Musical",
    "Animation",
    "Western",
    "Game-Show",
    "Family",
    "News",
    "Adult",
    "Thriller",
    "Music",
    "Mystery",
    "Romance",
    "Biography",
    "Reality-TV",
    "Film-Noir",
    "Documentary",
    "Sport",
    "Adventure",
    "Fantasy"
    ] 
%}

WITH imdb_basics AS (
    SELECT * 
    FROM {{ source('movies', 'imdb_basics') }} 
    WHERE 
        title_type = 'movie' AND start_year <= YEAR(CURRENT_TIMESTAMP()) + 1
),
imdb_basics_cleansed AS (
    SELECT
        id,
        primary_title,
        original_title,
        is_adult,
        start_year AS year,
        runtime_minutes,
        {% for genre in genres_list -%}
            ARRAY_CONTAINS('{{ genre }}'::VARIANT, SPLIT(genres, ',')) AS is_{{ genre | replace('-', '_') }}_genre,
        {% endfor %}
        SPLIT(genres, ',') AS genres_array
    FROM
        imdb_basics
),
imdb_ratings AS (
    SELECT * FROM {{ source('movies', 'imdb_ratings') }}
)

SELECT 
    id,
    primary_title,
    original_title,
    is_adult,
    year,
    runtime_minutes,
    {% for genre in genres_list -%}
        {% set col_name = "is_" + genre.replace('-', '_') + "_genre" %}
        CASE
            WHEN {{ col_name }} IS NULL THEN FALSE 
            ELSE {{ col_name }}
        END AS {{ col_name }},
    {% endfor %}
    genres_array
FROM
    imdb_basics_cleansed
WHERE
    id IN (SELECT id FROM imdb_ratings)

