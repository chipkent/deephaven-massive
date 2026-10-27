from deephaven_massive._massive import rest
from deephaven_massive._massive.rest import batch, client, response


def test_the_package_exports_the_client_the_page_and_to_batch() -> None:
    assert sorted(rest.__all__) == ["Page", "RestClient", "to_batch"]
    assert rest.RestClient is client.RestClient
    assert rest.Page is response.Page
    assert rest.to_batch is batch.to_batch
