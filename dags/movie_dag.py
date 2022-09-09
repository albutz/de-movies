"""DAG."""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extract import _extract_nyt_reviews

with DAG(dag_id="movie_dag", schedule_interval="@daily", start_date=days_ago(3)) as dag:

    fetch_nyt_reviews = PythonOperator(
        task_id="fetch_nyt_reviews",
        python_callable=_extract_nyt_reviews,
        op_kwargs={
            "url": Variable.get("nyt_url"),
            "key": Variable.get("nyt_key"),
            "left_boundary": "{{ ds }}",
            "right_boundary": "{{ ds }}",
        },
    )
