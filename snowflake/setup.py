"""Snowflake setup."""
from configparser import ConfigParser
from pathlib import Path
from textwrap import dedent


def main() -> None:
    """Setup configuration for Snowflake.

    - Create a role and user in Snowflake and grant the necessary access.
    - Create the raw tables for loading from S3 to Snowflake.
    - Create integration, file format and stage objects.
    """
    root_dir = Path(__file__).parent.parent
    config = ConfigParser()
    config.read(root_dir / "snowflake.cfg")

    query = f"""
        USE ROLE ACCOUNTADMIN;

        -- Role
        CREATE OR REPLACE ROLE airflow;
        GRANT ROLE airflow TO ROLE SYSADMIN;

        CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
        GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE airflow;

        -- User
        CREATE OR REPLACE USER airflow_user
            PASSWORD = '{config['airflow']['password']}'
            LOGIN_NAME = '{config['airflow']['username']}'
            MUST_CHANGE_PASSWORD = FALSE
            DEFAULT_WAREHOUSE = 'COMPUTE_WH'
            DEFAULT_ROLE = 'airflow'
            DEFAULT_NAMESPACE = 'MOVIES.RAW';

        GRANT ROLE airflow TO USER airflow_user;

        -- Database and schema
        CREATE DATABASE IF NOT EXISTS MOVIES;
        CREATE SCHEMA IF NOT EXISTS MOVIES.RAW;

        -- Grant access to role
        GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE airflow;
        GRANT ALL ON DATABASE MOVIES TO ROLE airflow;
        GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIES TO ROLE airflow;
        GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIES TO ROLE airflow;
        GRANT ALL ON ALL TABLES IN SCHEMA MOVIES.RAW TO ROLE airflow;
        GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIES.RAW TO ROLE airflow;

        USE WAREHOUSE COMPUTE_WH;
        USE DATABASE MOVIES;
        USE SCHEMA RAW;

        -- Raw tables
        CREATE OR REPLACE TABLE raw_nyt_reviews (
            content VARIANT
        );

        CREATE OR REPLACE TABLE raw_imdb_basics (
            id VARCHAR,
            title_type VARCHAR,
            primary_title VARCHAR,
            original_title VARCHAR,
            is_adult BOOLEAN,
            start_year INTEGER,
            end_year INTEGER,
            runtime_minutes NUMBER,
            genres VARCHAR
        );

        CREATE OR REPLACE TABLE raw_imdb_ratings (
            id VARCHAR,
            average_rating NUMBER,
            num_votes INTEGER
        );

        -- Integration for S3 bucket
        CREATE OR REPLACE STORAGE INTEGRATION s3_int
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = S3
        ENABLED = TRUE
        STORAGE_AWS_ROLE_ARN = '{config['airflow']['aws_role_arn']}'
        STORAGE_ALLOWED_LOCATIONS = ('{config['airflow']['s3_bucket']}');

        -- Get STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
        DESC INTEGRATION s3_int;

        -- Grant usage
        GRANT USAGE ON INTEGRATION s3_int TO ROLE airflow;

        -- Ensure ownership of objects
        DROP SCHEMA IF EXISTS MOVIES.FILE_FORMATS;
        DROP SCHEMA IF EXISTS MOVIES.STAGES;
        USE ROLE airflow;

        -- File formats
        CREATE OR REPLACE FILE FORMAT MOVIES.FILE_FORMATS.csv_file
            TYPE = CSV
            COMPRESSION = GZIP
            SKIP_HEADER = 1;

        -- Stage objects
        CREATE OR REPLACE SCHEMA MOVIES.STAGES;
        CREATE OR REPLACE STAGE MOVIES.STAGES.s3_imdb
            URL = '{config['airflow']['s3_bucket']}/imdb/'
            STORAGE_INTEGRATION = s3_int
            FILE_FORMAT = MOVIES.FILE_FORMATS.csv_file;

        CREATE OR REPLACE STAGE MANAGE_DB.STAGES.s3_nyt
            URL = '{config['airflow']['s3_bucket']}/nyt/'
            STORAGE_INTEGRATION = s3_int
            FILE_FORMAT = (TYPE = JSON);
    """

    output_file = root_dir / "snowflake" / "snowflake_setup.txt"

    if output_file.exists():
        output_file.unlink()

    output_file.touch()
    output_file.write_text(dedent(query))


if __name__ == "__main__":
    main()
