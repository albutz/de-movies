"""Extraction."""
import json
import logging
import time
from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from requests.exceptions import HTTPError

DATA_DIR = "/opt/airflow/data"
IMDB_TABLES = ["title.basics", "title.ratings"]


def _extract_nyt_reviews(url: str, key: str, left_boundary: str, right_boundary: str) -> bool:
    """Extract NYT movie reviews from movie review API.

    Fetch movie reviews in a time frame starting at left_boundary and ending
    at right_boundary. The server only allows for 10 requests per minute so,
    there will be a timeout of one minute in case a 429 status code is
    encountered. The result is dumped as json to ./data.

    Args:
        url: URL for the NYT movie review API.
        key: Key for the NYT movie review API.
        left_boundary: Start date, format must be %Y-%m-%d.
        right_boundary: End date, format must be %Y-%m-%d.

    Returns:
        Boolean indicating if reviews were dumped.
    """
    movies = []
    has_more = True
    offset = 0

    while has_more:
        try:
            response = requests.get(
                url=url + "/reviews/search.json",
                params={
                    "api-key": key,
                    "opening-date": f"{left_boundary}:{right_boundary}",
                    "offset": str(offset),
                },
            )
            response.raise_for_status()

            response_parsed = response.json()

            # Check if response has more results
            has_more = response_parsed["has_more"]
            offset += 20

            results = response_parsed["results"]
            if results is not None:
                movies += results

        except HTTPError as err:
            # Pause for 1 minute in case request limit is reached
            if err.response.status_code == 429:
                time.sleep(60)
            else:
                logging.error(err)

    file_name = "nyt-review.json"

    if movies:
        logging.info(f"Fetched {len(movies)} movie reviews. Writing to {file_name}.")
        with open(f"{DATA_DIR}/nyt/nyt-review.json", "w") as f:
            json.dump(movies, f, indent=4)
    else:
        logging.info("No reviews available.")

    return True if movies else False


def _get_download_links(url: str) -> List[str]:
    """Get download links from url.

    Parse the site and extract all hrefs that point to zipped files.

    Args:
        url: The URL for the site to parse.

    Returns:
        A list of urls.
    """
    links = []
    response = requests.get(url)

    for link in BeautifulSoup(response.content, parse_only=SoupStrainer("a"), features="lxml"):
        if hasattr(link, "href") and link["href"].endswith("gz"):
            links.append(link["href"])

    return links


def _extract_imdb_datasets(url: str) -> List[str]:
    """Extract datasets from IMDB.

    Fetch the title.basics and title.ratings datasets from IMDB and dump new
    rows as csv.gz to ./data.

    Args:
        url: URL to get download links via _get_download_links.

    Returns:
        List of dumped table names.
    """
    urls = _get_download_links(url)
    urls = [url for url in urls if any(keep_url in url for keep_url in IMDB_TABLES)]
    tbl_urls = {tbl: url for tbl, url in zip(IMDB_TABLES, urls)}

    dumped_tbls: List[str] = []

    for tbl, url in tbl_urls.items():
        df = pd.read_table(url, header=0, compression="gzip")
        ids_file = f"{DATA_DIR}/imdb/ids/ids.{tbl}.csv"

        if Path(ids_file).exists():
            existing_ids = pd.read_csv(ids_file, header=None).squeeze("columns")
            df = df.loc[~df.tconst.isin(existing_ids)]

        # Append new ids
        df.tconst.to_csv(ids_file, header=False, index=False, mode="a")

        # '\\N' encodes missing values
        df = df.where(df != "\\N", other=np.nan)

        n_rows = df.shape[0]

        file_name = f"imdb/tables/{tbl}.csv.gz"

        if n_rows > 0:
            logging.info(f"Fetched {n_rows} new rows for {tbl}. Writing to {file_name}.")

            dumped_tbls.append(tbl)

            df.to_csv(f"{DATA_DIR}/{file_name}", index=False)
        else:
            logging.info(f"No new rows for {tbl}.")

    return dumped_tbls


def _branch_nyt_tests(**context: Any) -> str:
    """Branch for testing.

    Skip the data tests if there are no new reviews available.

    Args:
        context: Airflow context.

    Returns:
        ID of task to run.
    """
    has_results = context["task_instance"].xcom_pull(
        task_ids="extract_nyt_reviews", key="return_value"
    )
    return "run_tests_raw_nyt_reviews" if has_results else "skip_tests_raw_nyt_reviews"


def _branch_nyt_copy(**context: Any) -> str:
    """Branch for copying.

    Skip the copy if there are no new reviews available.

    Args:
        context: Airflow context.

    Returns:
        ID of task to run.
    """
    has_results = context["task_instance"].xcom_pull(
        task_ids="extract_nyt_reviews", key="return_value"
    )
    return "copy_raw_nyt_table" if has_results else "skip_copy_raw_nyt_table"


def _branch_imdb_tests(**context: Any) -> List[str]:
    """Branch for testing IMDB datasets.

    Skip the data tests if there are no new records available.

    Args:
        context: Airflow context.

    Returns:
        IDs of tasks to run.
    """
    dumped_tbls = context["task_instance"].xcom_pull(
        task_ids="extract_imdb_datasets", key="return_value"
    )

    next_tasks = []
    for tbl in IMDB_TABLES:
        tbl_suffix = tbl.replace("title.", "")
        if tbl in dumped_tbls:
            next_tasks.append(f"run_tests_raw_imdb_{tbl_suffix}")
        else:
            next_tasks.append(f"skip_tests_raw_imdb_{tbl_suffix}")

    return next_tasks


def _branch_imdb_copy(**context: Any) -> List[str]:
    """Branch for copying IMDB datasets.

    Skip the copy if there are no new records available.

    Args:
        context: Airflow context.

    Returns:
        IDs of tasks to run.
    """
    dumped_tbls = context["task_instance"].xcom_pull(
        task_ids="extract_imdb_datasets", key="return_value"
    )

    next_tasks = []
    for tbl in IMDB_TABLES:
        tbl_suffix = tbl.replace("title.", "")
        if tbl in dumped_tbls:
            next_tasks.append(f"copy_raw_imdb_{tbl_suffix}_table")
        else:
            next_tasks.append(f"skip_copy_raw_imdb_{tbl_suffix}_table")

    return next_tasks
