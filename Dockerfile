FROM apache/airflow:2.3.4
RUN pip install --no-cache-dir airflow-provider-great-expectations==0.1.5

