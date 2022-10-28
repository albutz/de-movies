WITH nyt_reviews AS (
    SELECT * FROM {{ ref('nyt_snapshot') }}
)

SELECT
    id,
    author_name::STRING AS author_name,
    is_critics_pick::INTEGER::BOOLEAN AS is_critics_pick,
    review_date_updated,
    movie_title::STRING AS movie_title,
    review_headline::STRING AS review_headline,
    review_url::STRING AS review_url,
    CASE
        WHEN mpaa_rating::STRING = '' THEN NULL
        ELSE mpaa_rating::STRING
    END AS mpaa_rating,
    image_url::STRING AS image_url,
    opening_date::DATE AS opening_date,
    review_date::DATE AS review_date,
    summary_short::STRING AS summary_short
FROM 
    nyt_reviews
WHERE
    dbt_valid_to IS NULL
