"""Extraction."""
import json
import logging
import time
from typing import Any, List

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from requests.exceptions import HTTPError


def _extract_nyt_reviews(
    url: str, key: str, left_boundary: str, right_boundary: str, **context: Any
) -> None:
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
        **context: Airflow context variables.
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

    file_name = f"nyt-review-{context['ds']}.json"
    logging.info(f"Fetched {len(movies)} movie reviews. Writing to {file_name}.")

    with open(f"/opt/airflow/data/{file_name}", "w") as f:
        json.dump(movies, f, indent=4)


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


def _extract_imdb_datasets(url: str, ds: str) -> None:
    """Extract datasets from IMDB.

    Fetch the title.basics and title.ratings datasets from IMDB and dump them
    as csv.gz to ./data.

    Args:
        url: URL to get download links via _get_download_links.
        ds: DAG run's logical date.
    """
    tbls = ["title.basics", "title.ratings"]
    urls = _get_download_links(url)
    urls = [url for url in urls if any(keep_url in url for keep_url in tbls)]
    tbl_urls = {tbl: url for tbl, url in zip(tbls, urls)}

    for tbl, url in tbl_urls.items():
        df = pd.read_table(url, header=0, compression="gzip", low_memory=False)

        # '\\N' encodes missing values
        df = df.where(df != "\\N", other=np.nan)

        file_name = f"{tbl}-{ds}.csv.gz"
        logging.info(f"Fetched {df.shape[0]} rows for {tbl}. Writing to {file_name}.")

        df.to_csv(f"/opt/airflow/data/{file_name}", index=False)
