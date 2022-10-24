"""DAG."""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from extract import (
    _branch_imdb_copy,
    _branch_imdb_tests,
    _branch_nyt_copy,
    _branch_nyt_tests,
    _extract_imdb_datasets,
    _extract_nyt_reviews,
)
from ge_extract import _runtime_batch_request
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

with DAG(
    dag_id="movie_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    max_active_runs=1,
) as dag:

    # NYT reviews
    extract_nyt_reviews = PythonOperator(
        task_id="extract_nyt_reviews",
        python_callable=_extract_nyt_reviews,
        op_kwargs={
            "url": Variable.get("nyt_url"),
            "key": Variable.get("nyt_key"),
            "left_boundary": "{{ ds }}",
            "right_boundary": "{{ ds }}",
        },
    )

    branch_test_load_raw_nyt_reviews = BranchPythonOperator(
        task_id="branch_test_load_raw_nyt_reviews",
        python_callable=_branch_nyt_tests,
    )

    skip_tests_raw_nyt_reviews = EmptyOperator(task_id="skip_tests_raw_nyt_reviews")

    run_tests_raw_nyt_reviews = GreatExpectationsOperator(
        task_id="run_tests_raw_nyt_reviews",
        data_context_root_dir="great_expectations",
        checkpoint_name="nyt_review_raw",
        checkpoint_kwargs={
            "validations": [
                {
                    "batch_request": _runtime_batch_request(
                        data_source_name="nyt_reviews_raw",
                        data_asset_name="nyt_review",
                        path="data/nyt/nyt-review.json",
                    )
                }
            ]
        },
    )

    load_nyt_reviews_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_nyt_reviews_to_s3",
        filename="data/nyt/nyt-review.json",
        dest_key="nyt/nyt-review-{{ ds }}.json",
        dest_bucket=Variable.get("s3_bucket"),
        aws_conn_id="s3_conn",
        replace=True,
    )

    combine_test_load_raw_nyt_reviews = EmptyOperator(
        task_id="combine_test_load_raw_nyt_reviews", trigger_rule="none_failed"
    )

    truncate_raw_nyt_table = SnowflakeOperator(
        task_id="truncate_raw_nyt_table",
        sql="TRUNCATE TABLE raw_nyt_reviews;",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        snowflake_conn_id="snowflake_conn",
    )

    branch_copy_nyt_table = BranchPythonOperator(
        task_id="branch_copy_nyt_table", python_callable=_branch_nyt_copy
    )

    copy_raw_nyt_table = S3ToSnowflakeOperator(
        task_id="copy_raw_nyt_table",
        snowflake_conn_id="snowflake_conn",
        s3_keys=["nyt-review-{{ ds }}.json"],
        table="raw_nyt_reviews",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        stage="MOVIES.STAGES.s3_nyt",
        file_format="(TYPE = JSON)",
    )

    skip_copy_raw_nyt_table = EmptyOperator(task_id="skip_copy_raw_nyt_table")

    combine_copy_raw_nyt_table = EmptyOperator(
        task_id="combine_copy_raw_nyt_table", trigger_rule="none_failed"
    )

    # IMDB datasets
    extract_imdb_datasets = PythonOperator(
        task_id="extract_imdb_datasets",
        python_callable=_extract_imdb_datasets,
        op_kwargs={
            "url": Variable.get("imdb_url"),
        },
    )

    branch_test_load_raw_imdb_datasets = BranchPythonOperator(
        task_id="branch_test_load_raw_imdb_datasets",
        python_callable=_branch_imdb_tests,
    )

    skip_tests_raw_imdb_basics = EmptyOperator(task_id="skip_tests_raw_imdb_basics")

    skip_tests_raw_imdb_ratings = EmptyOperator(task_id="skip_tests_raw_imdb_ratings")

    run_tests_raw_imdb_basics = GreatExpectationsOperator(
        task_id="run_tests_raw_imdb_basics",
        data_context_root_dir="great_expectations",
        checkpoint_name="imdb_basic_raw",
        checkpoint_kwargs={
            "validations": [
                {
                    "batch_request": _runtime_batch_request(
                        data_source_name="imdb_raw",
                        data_asset_name="imdb_basic",
                        path="data/imdb/tables/title.basics.csv.gz",
                    )
                }
            ]
        },
    )

    run_tests_raw_imdb_ratings = GreatExpectationsOperator(
        task_id="run_tests_raw_imdb_ratings",
        data_context_root_dir="great_expectations",
        checkpoint_name="imdb_rating_raw",
        checkpoint_kwargs={
            "validations": [
                {
                    "batch_request": _runtime_batch_request(
                        data_source_name="imdb_raw",
                        data_asset_name="imdb_rating",
                        path="data/imdb/tables/title.ratings.csv.gz",
                    )
                }
            ]
        },
    )

    load_imdb_basics_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_imdb_basics_to_s3",
        filename="data/imdb/tables/title.basics.csv.gz",
        dest_key="imdb/title.basics-{{ ds }}.csv.gz",
        dest_bucket=Variable.get("s3_bucket"),
        aws_conn_id="s3_conn",
        replace=True,
    )

    load_imdb_ratings_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_imdb_ratings_to_s3",
        filename="data/imdb/tables/title.ratings.csv.gz",
        dest_key="imdb/title.ratings-{{ ds }}.csv.gz",
        dest_bucket=Variable.get("s3_bucket"),
        aws_conn_id="s3_conn",
        replace=True,
    )

    combine_test_load_imdb_datasets = EmptyOperator(
        task_id="combine_test_load_imdb_datasets", trigger_rule="none_failed"
    )

    truncate_raw_imdb_basics_table = SnowflakeOperator(
        task_id="truncate_raw_imdb_basics_table",
        sql="TRUNCATE TABLE raw_imdb_basics;",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        snowflake_conn_id="snowflake_conn",
    )

    truncate_raw_imdb_ratings_table = SnowflakeOperator(
        task_id="truncate_raw_imdb_ratings_table",
        sql="TRUNCATE TABLE raw_imdb_ratings;",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        snowflake_conn_id="snowflake_conn",
    )

    branch_copy_imdb_datasets = BranchPythonOperator(
        task_id="branch_copy_imdb_datasets", python_callable=_branch_imdb_copy
    )

    copy_raw_imdb_basics_table = S3ToSnowflakeOperator(
        task_id="copy_raw_imdb_basics_table",
        snowflake_conn_id="snowflake_conn",
        s3_keys=["title.basics-{{ ds }}.csv.gz"],
        table="raw_imdb_basics",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        stage="MOVIES.STAGES.s3_imdb",
        file_format="MOVIES.FILE_FORMATS.csv_file",
    )

    skip_copy_raw_imdb_basics_table = EmptyOperator(task_id="skip_copy_raw_imdb_basics_table")

    copy_raw_imdb_ratings_table = S3ToSnowflakeOperator(
        task_id="copy_raw_imdb_ratings_table",
        snowflake_conn_id="snowflake_conn",
        s3_keys=["title.ratings-{{ ds }}.csv.gz"],
        table="raw_imdb_ratings",
        schema="RAW",
        database="MOVIES",
        warehouse="COMPUTE_WH",
        stage="MOVIES.STAGES.s3_imdb",
        file_format="MOVIES.FILE_FORMATS.csv_file",
    )

    skip_copy_raw_imdb_ratings_table = EmptyOperator(task_id="skip_copy_raw_imdb_ratings_table")

    combine_copy_raw_imdb_tables = EmptyOperator(
        task_id="combine_copy_raw_imdb_tables", trigger_rule="none_failed"
    )

    extract_nyt_reviews >> branch_test_load_raw_nyt_reviews
    branch_test_load_raw_nyt_reviews >> [run_tests_raw_nyt_reviews, skip_tests_raw_nyt_reviews]
    run_tests_raw_nyt_reviews >> load_nyt_reviews_to_s3
    [load_nyt_reviews_to_s3, skip_tests_raw_nyt_reviews] >> combine_test_load_raw_nyt_reviews
    combine_test_load_raw_nyt_reviews >> truncate_raw_nyt_table
    truncate_raw_nyt_table >> branch_copy_nyt_table
    branch_copy_nyt_table >> [copy_raw_nyt_table, skip_copy_raw_nyt_table]
    [copy_raw_nyt_table, skip_copy_raw_nyt_table] >> combine_copy_raw_nyt_table

    extract_imdb_datasets >> branch_test_load_raw_imdb_datasets
    branch_test_load_raw_imdb_datasets >> [
        run_tests_raw_imdb_basics,
        run_tests_raw_imdb_ratings,
        skip_tests_raw_imdb_basics,
        skip_tests_raw_imdb_ratings,
    ]
    run_tests_raw_imdb_basics >> load_imdb_basics_to_s3
    run_tests_raw_imdb_ratings >> load_imdb_ratings_to_s3
    [
        load_imdb_basics_to_s3,
        load_imdb_ratings_to_s3,
        skip_tests_raw_imdb_basics,
        skip_tests_raw_imdb_ratings,
    ] >> combine_test_load_imdb_datasets
    combine_test_load_imdb_datasets >> [
        truncate_raw_imdb_basics_table,
        truncate_raw_imdb_ratings_table,
    ]
    [truncate_raw_imdb_basics_table, truncate_raw_imdb_ratings_table] >> branch_copy_imdb_datasets
    branch_copy_imdb_datasets >> [
        copy_raw_imdb_basics_table,
        copy_raw_imdb_ratings_table,
        skip_copy_raw_imdb_basics_table,
        skip_copy_raw_imdb_ratings_table,
    ]
    [
        copy_raw_imdb_basics_table,
        copy_raw_imdb_ratings_table,
        skip_copy_raw_imdb_basics_table,
        skip_copy_raw_imdb_ratings_table,
    ] >> combine_copy_raw_imdb_tables
