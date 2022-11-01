{% snapshot imdb_basic_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=[
            'primary_title',
            'original_title',
            'start_year',
            'runtime_minutes',
            'genres'
        ]
    )
}}

SELECT * 
FROM {{ source('movies', 'imdb_basics') }} 
WHERE title_type = 'movie'

{% endsnapshot %}
