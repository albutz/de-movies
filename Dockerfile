FROM apache/airflow:2.3.4

RUN pip install --no-cache-dir airflow-provider-great-expectations==0.1.5 \
    && pip install --no-cache-dir apache-airflow-providers-snowflake==3.3.0 \
    && pip install --no-cache-dir dbt-snowflake==1.3.0 \
    && pip install --no-cache-dir airflow-dbt==0.4.0

