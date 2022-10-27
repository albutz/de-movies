WITH nyt_reviews AS (
    SELECT * FROM {{ source('movies', 'nyt') }}
)

SELECT
    $1:byline::STRING AS author_name,
    $1:critics_pick::INTEGER::BOOLEAN AS is_critics_pick,
    $1:date_updated::DATE AS review_date_updated,
    $1:display_title::STRING AS movie_title,
    $1:headline::STRING AS review_headline,
    $1:link.url::STRING AS review_url,
    CASE 
        WHEN $1:mpaa_rating::STRING = '' THEN NULL
        ELSE $1:mpaa_rating::STRING
    END AS mpaa_rating,
    $1:multimedia.src::STRING AS image_url,
    $1:opening_date::DATE AS opening_date,
    $1:publication_date::DATE AS review_date,
    $1:summary_short::STRING AS summary_short
FROM
    nyt_reviews
