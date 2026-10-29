from deephaven_massive._massive import rest
from deephaven_massive._massive.rest import batch, client, governor, response


def test_the_package_exports_the_client_the_governor_the_page_and_to_batch() -> None:
    assert sorted(rest.__all__) == [
        "BucketState",
        "Page",
        "RequestGovernor",
        "RestClient",
        "to_batch",
    ]
    assert rest.RequestGovernor is governor.RequestGovernor
    assert rest.BucketState is governor.BucketState
    assert rest.RestClient is client.RestClient
    assert rest.Page is response.Page
    assert rest.to_batch is batch.to_batch
