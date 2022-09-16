"""DAG."""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import days_ago
from extract import _branch_tests, _extract_imdb_datasets, _extract_nyt_reviews
from ge_extract import imdb_basic_runtime, imdb_rating_runtime, nyt_raw_runtime
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

with DAG(dag_id="movie_dag", schedule_interval="@daily", start_date=days_ago(1)) as dag:

    # NYT reviews
    extract_nyt_reviews = PythonOperator(
        task_id="extract_nyt_reviews",
        python_callable=_extract_nyt_reviews,
        op_kwargs={
            "url": Variable.get("nyt_url"),
            "key": Variable.get("nyt_key"),
            "left_boundary": "2022-09-10",
            "right_boundary": "{{ ds }}",
        },
    )

    branch_raw_nyt_reviews = BranchPythonOperator(
        task_id="branch_raw_nyt_reviews",
        python_callable=_branch_tests,
        op_kwargs={"task_ids": "extract_nyt_reviews"},
    )

    skip_tests_raw_nyt_reviews = EmptyOperator(task_id="skip_tests_raw_nyt_reviews")

    run_tests_raw_nyt_reviews = GreatExpectationsOperator(
        task_id="run_tests_raw_nyt_reviews",
        data_context_root_dir="great_expectations",
        checkpoint_name="nyt_review_raw",
        checkpoint_kwargs={"validations": [{"batch_request": nyt_raw_runtime}]},
    )

    combine_raw_nyt_reviews = EmptyOperator(
        task_id="combine_raw_nyt_reviews", trigger_rule="none_failed"
    )

    load_nyt_reviews_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_nyt_reviews_to_s3",
        filename="data/nyt/nyt-review.json",
        dest_key="nyt/nyt-review-{{ ds }}.json",
        dest_bucket=Variable.get("s3_bucket"),
        aws_conn_id="s3_conn",
        replace=True,
    )

    # IMDB datasets
    extract_imdb_datasets = PythonOperator(
        task_id="extract_imdb_datasets",
        python_callable=_extract_imdb_datasets,
        op_kwargs={
            "url": Variable.get("imdb_url"),
        },
    )

    run_tests_raw_imdb_basic = GreatExpectationsOperator(
        task_id="run_tests_raw_imdb_basic",
        data_context_root_dir="great_expectations",
        checkpoint_name="imdb_basic_raw",
        checkpoint_kwargs={"validations": [{"batch_request": imdb_basic_runtime}]},
    )

    run_tests_raw_imdb_rating = GreatExpectationsOperator(
        task_id="run_tests_raw_imdb_rating",
        data_context_root_dir="great_expectations",
        checkpoint_name="imdb_rating_raw",
        checkpoint_kwargs={"validations": [{"batch_request": imdb_rating_runtime}]},
    )

    extract_nyt_reviews >> branch_raw_nyt_reviews
    branch_raw_nyt_reviews >> [run_tests_raw_nyt_reviews, skip_tests_raw_nyt_reviews]
    run_tests_raw_nyt_reviews >> load_nyt_reviews_to_s3
    [load_nyt_reviews_to_s3, skip_tests_raw_nyt_reviews] >> combine_raw_nyt_reviews

    extract_imdb_datasets >> [run_tests_raw_imdb_basic, run_tests_raw_imdb_rating]
