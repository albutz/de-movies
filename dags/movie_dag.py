"""DAG."""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extract import _extract_imdb_datasets, _extract_nyt_reviews

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

    extract_imdb_datasets = PythonOperator(
        task_id="extract_imdb_datasets",
        python_callable=_extract_imdb_datasets,
        op_kwargs={
            "url": Variable.get("imdb_url"),
        },
    )
