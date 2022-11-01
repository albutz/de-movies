{% snapshot imdb_rating_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=['average_rating', 'num_votes']
    )
}}

SELECT *
FROM {{ source('movies', 'imdb_ratings') }}

{% endsnapshot %}
