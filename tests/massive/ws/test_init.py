from deephaven_massive._massive import ws
from deephaven_massive._massive.ws import client, protocol


def test_the_package_re_exports_the_client_interface() -> None:
    assert sorted(ws.__all__) == ["Status", "StatusKind", "WsClient", "WsState"]
    assert ws.WsClient is client.WsClient
    assert ws.WsState is client.WsState
    assert ws.Status is protocol.Status
    assert ws.StatusKind is protocol.StatusKind
