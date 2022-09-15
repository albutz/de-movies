"""DAG."""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from extract import _branch_test_raw_nyt_reviews, _extract_imdb_datasets, _extract_nyt_reviews
from ge_extract import nyt_raw_runtime
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

with DAG(dag_id="movie_dag", schedule_interval="@daily", start_date=days_ago(1)) as dag:

    extract_nyt_reviews = PythonOperator(
        task_id="extract_nyt_reviews",
        python_callable=_extract_nyt_reviews,
        op_kwargs={
            "url": Variable.get("nyt_url"),
            "key": Variable.get("nyt_key"),
            "left_boundary": "{{ ds }}",
            "right_boundary": "{{ ds }}",
        },
    )

    branch_test_raw_nyt_reviews = BranchPythonOperator(
        task_id="branch_test_raw_nyt_reviews",
        python_callable=_branch_test_raw_nyt_reviews,
    )

    skip_test_raw_nyt_reviews = EmptyOperator(task_id="skip_test_raw_nyt_reviews")

    run_test_raw_nyt_reviews = GreatExpectationsOperator(
        task_id="run_test_raw_nyt_reviews",
        data_context_root_dir="great_expectations",
        checkpoint_name="nyt_review_raw",
        checkpoint_kwargs={"validations": nyt_raw_runtime},
    )

    combine_test_raw_nyt_reviews = EmptyOperator(
        task_id="combine_test_raw_nyt_reviews", trigger_rule="none_failed"
    )

    extract_imdb_datasets = PythonOperator(
        task_id="extract_imdb_datasets",
        python_callable=_extract_imdb_datasets,
        op_kwargs={
            "url": Variable.get("imdb_url"),
        },
    )

    (
        extract_nyt_reviews
        >> branch_test_raw_nyt_reviews
        >> [run_test_raw_nyt_reviews, skip_test_raw_nyt_reviews]
        >> combine_test_raw_nyt_reviews
    )
