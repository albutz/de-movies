# Movie Ratings & Review Pipeline

An ELT pipeline for movie ratings and reviews from IMDB and the New York Times. 

## Tools

- [Airflow](https://airflow.apache.org/) is used as the orchestration tool. All components run in separate [Docker](https://www.docker.com/) containers.
- [Great expectations](https://greatexpectations.io/) is used to run data quality tests after the extraction step.
- [AWS](https://aws.amazon.com/) is used to dump the raw files in an S3 bucket.
- [Snowflake](https://www.snowflake.com/en/) is used as the computation workhorse. We can scale up our virtual warehouses if data size increased by 100x (up to size 4XL) and scale out, i.e. use multi-cluster warehouses (auto-scaling), in case the database is accessed by 100+ users. 
- [dbt](https://www.getdbt.com/) is used for transformations and tests in Snowflake.

## Pipeline

Movie ratings are fetched daily from the [IMDB database](https://www.imdb.com/interfaces/) and reviews from the [NYT Movie Review API](https://developer.nytimes.com/docs/movie-reviews-api/1/overview). The exact start time for a given day can be configured with the `start_date` argument of the `DAG` class, e.g. 7am. The pipeline built in Airflow is shown below.

<img width="1669" alt="airflow_pipeline" src="https://user-images.githubusercontent.com/60711201/199123235-9ec93089-b05d-4848-a69b-d82bffc27f04.png">

On a high level, the individual tasks are

1. Extract the (incremental) data from the source system.
    - Download the daily updates from IMDB and dump new records as csv to the local filesystem to ensure an incremental ingestion.
    - Fetch NYT movie reviews for the given day and dump them as JSON to the local filesystem.
2. Run a bunch of data quality tests on the raw data and save the source data to S3. Note that these tasks are skipped if there are no new records available.
3. Truncate the staging tables in Snowflake.
4. Copy the source data loaded to S3 to the respective staging tables in Snowflake. As before, this task is conditional on new IMDB ratings and / or NYT reviews being available.
5. Run the transformation pipeline.

The lineage graph in dbt is shown below.

<img width="1812" alt="dbt_lineage" src="https://user-images.githubusercontent.com/60711201/199124679-c4752c09-186a-4b02-a03f-f6b1e68664f6.png">

- Snapshots of the source data via `dbt snapshot` allow to track ratings and reviews over time. An example for the `imdb_rating_snapshot` table is shown below.

<img width="1027" alt="snapshot_sample" src="https://user-images.githubusercontent.com/60711201/199196906-3f7368c9-42ea-4e26-80d9-597f2b65d4f0.png">

- The transformation step (`dbt run`) builds dimensional tables and a fact table in a star schema that combines IMDB ratings and NYT reviews. This gives the flexibiliy to build data marts in a very performant manner. Contrast this to third normal form where multiple joins are necessary for OLAP analyses. As an example, a data mart is built to analyze ratings and reviews by genre over time and can serve downstream BI applications. The output of the query to build the data mart (see ``)
- Tables and views are covered with data quality tests via `dbt test`.

## Usage

The setup of the project requires a couple of steps.

1. Create the `nyt_url`, `nyt_key`, `s3_bucket` and `imdb_url` variables in Airflow.
2. Create the `s3_conn` and `snowflake_conn` connections in Airflow.
4. Execute the setup script `snowflake/setup.py` to configure Snowflake. This creates the necessary user, role, database, schemas, access rights, staging tables, storage integrations and file formats. Note that the setup script assumes a `snowflake.cfg` at the project root and an S3 bucket and an IAM role needs to be created upfront.
