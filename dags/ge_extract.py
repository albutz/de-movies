"""Runtime config."""
from great_expectations.core.batch import RuntimeBatchRequest

# NOTE: need to keep file referenced in
# validations:
#   - batch_request:
#     data_asset_name: <file>
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

imdb_basic_runtime = RuntimeBatchRequest(
    **{
        "datasource_name": "imdb_raw",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "imdb_basic",
        "runtime_parameters": {"path": "data/imdb/tables/title.basics.csv.gz"},
        "batch_identifiers": {"default_identifier_name": "default_identifier"},
    }
)

imdb_rating_runtime = RuntimeBatchRequest(
    **{
        "datasource_name": "imdb_raw",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "imdb_rating",
        "runtime_parameters": {"path": "data/imdb/tables/title.ratings.csv.gz"},
        "batch_identifiers": {"default_identifier_name": "default_identifier"},
    }
)
