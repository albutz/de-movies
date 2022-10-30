{% snapshot nyt_snapshot %}

{{
    
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='review_date_updated'
    )
    
}}

WITH nyt_reviews AS (
    SELECT * FROM {{ source('movies', 'nyt') }}
),
nyt_parsed AS (
    SELECT
        $1:byline AS author_name,
        $1:critics_pick AS is_critics_pick,
        $1:date_updated::DATE AS review_date_updated,
        $1:display_title AS movie_title,
        $1:headline AS review_headline,
        $1:link.url AS review_url,
        $1:mpaa_rating AS mpaa_rating,
        $1:multimedia.src AS image_url,
        $1:opening_date AS opening_date,
        $1:publication_date AS review_date,
        $1:summary_short AS summary_short
    FROM
        nyt_reviews
)

SELECT
    {{ dbt_utils.surrogate_key(['author_name', 'movie_title', 'review_headline']) }} AS id,
    *
FROM nyt_parsed

{% endsnapshot %}
