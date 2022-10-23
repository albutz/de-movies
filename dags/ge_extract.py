"""Runtime config."""
from great_expectations.core.batch import RuntimeBatchRequest


def _runtime_batch_request(
    data_source_name: str, data_asset_name: str, path: str
) -> RuntimeBatchRequest:
    """Get runtime config for GreatExpectationsOperator.

    Args:
        data_source_name: data source set up in great_expectations.yml.
        data_asset_name: identifier for data asset.
        path: file to use for runtime test.

    Returns:
        RuntimeBatchRequest: runtime config.
    """
    return RuntimeBatchRequest(
        **{
            "datasource_name": data_source_name,
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": data_asset_name,
            "runtime_parameters": {"path": path},  # can't use template inside RuntimeBatchRequest!
            "batch_identifiers": {"default_identifier_name": "default_identifier"},
        }
    )


# NOTE: need to keep file referenced in
# validations:
#   - batch_request:
#     data_asset_name: <file>
