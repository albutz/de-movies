"""Extraction."""
import configparser
import datetime
import logging
import pathlib
import time
from typing import List

import requests
from requests.exceptions import HTTPError

parser = configparser.ConfigParser()
parser.read(pathlib.Path("..", "pipeline.conf"))

nyt_key = parser.get("nyt", "key")


def fetch_nyt_reviews(url: str, left_boundary: str, right_boundary: str) -> List:
    """Extract NYT movie reviews from movie review API.

    Fetch movie reviews in a time frame starting at left_boundary and ending
    at right_boundary. The server only allows for 10 requests per minute so,
    there will be a timeout of one minute in case a 429 status code is
    encountered.

    Args:
        url: URL for the NYT movie review API.
        left_boundary: Start date, format must be %Y-%m-%d.
        right_boundary: End date, format must be %Y-%m-%d.

    Returns:
        List: Responses from GET requests.
    """
    movies = []
    has_more = True
    offset = 0

    while has_more:
        try:
            response = requests.get(
                url=url + "/reviews/search.json",
                params={
                    "api-key": nyt_key,
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

    return movies


if __name__ == "__main__":
    movies = fetch_nyt_reviews(
        url="https://api.nytimes.com/svc/movies/v2",
        left_boundary="2022-01-01",
        right_boundary=datetime.datetime.now().strftime("%Y-%m-%d"),
    )
