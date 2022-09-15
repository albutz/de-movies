"""Runtime config."""
from typing import Any

from great_expectations.core.batch import RuntimeBatchRequest

nyt_raw_runtime = RuntimeBatchRequest(
    **{
        "datasource_name": "nyt_reviews_raw",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "nyt_review",
        "runtime_parameters": {
            "path": "data/nyt/nyt-review.json"  # can't use template inside RuntimeBatchRequest!
        },
        "batch_identifiers": {"default_identifier_name": "default_identifier"},
    }
)


def _branch_test_raw_nyt_reviews(**context: Any) -> str:
    has_results = context["task_instance"].xcom_pull(
        task_ids="extract_nyt_reviews", key="return_value"
    )
    return "run_test_raw_nyt_reviews" if has_results else "skip_test_raw_nyt_reviews"
