# Movie Ratings & Review Pipeline

An ELT pipeline for movie ratings and reviews from IMDB and the New York Times. 

## Tools

- [Airflow](https://airflow.apache.org/) is used as the orchestration tool. All components run in separate [Docker](https://www.docker.com/) containers.
- [Great expectations](https://greatexpectations.io/) is used to run data quality tests after the extraction step.
- [AWS](https://aws.amazon.com/) is used to dump the raw files in an S3 bucket.
- [Snowflake](https://www.snowflake.com/en/) is used as the computation workhorse.
- [dbt](https://www.getdbt.com/) is used for transformations and tests in Snowflake.

## Pipeline

Movie ratings are fetched daily from the [IMDB database](https://www.imdb.com/interfaces/) and reviews from the [NYT Movie Review API](https://developer.nytimes.com/docs/movie-reviews-api/1/overview). The pipeline built in Airflow is shown below.

<img width="1669" alt="airflow_pipeline" src="https://user-images.githubusercontent.com/60711201/199123235-9ec93089-b05d-4848-a69b-d82bffc27f04.png">

On a high level, the individual tasks are

1. Extract the (incremental) data from the source system.
    - Download the daily updates from IMDB and dump new records as csv to the local filesystem to ensure incremental loads.
    - Fetch NYT movie reviews for the given day and dump them as JSON to the local filesystem.
2. Run a bunch of data quality tests on the raw data and save the source data to S3. Note that these tasks are skipped if there are no new records available.
3. Truncate the staging tables in Snowflake.
4. Copy the source data loaded to S3 to the respective staging tables in Snowflake. As before, this task is conditional on new IMDB ratings and / or NYT reviews being available.
5. Run the transformation pipeline.

The lineage graph in dbt is shown below.

<img width="1477" alt="dbt_lineage" src="https://user-images.githubusercontent.com/60711201/200029812-fcdd043a-136a-4b9c-b4fa-3525a167bd71.png">

- Snapshots of the source data via `dbt snapshot` allow to track ratings and reviews over time.
- The transformation step (`dbt run`) builds dimensional tables and a fact table that combines IMDB ratings and NYT reviews.
- Tables and views are covered with data quality tests via `dbt test`.

## Usage

The setup of the project requires a couple of steps.

1. Create the `nyt_url`, `nyt_key`, `s3_bucket` and `imdb_url` variables in Airflow.
2. Create the `s3_conn` and `snowflake_conn` connections in Airflow.
4. Execute the setup script `snowflake/setup.py` to configure Snowflake. This creates the necessary user, role, database, schemas, access rights, staging tables, storage integrations and file formats. Note that the setup script assumes a `snowflake.cfg` at the project root and an S3 bucket and an IAM role needs to be created upfront.
