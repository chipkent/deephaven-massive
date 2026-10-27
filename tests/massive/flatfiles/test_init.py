from deephaven_massive._massive import flatfiles
from deephaven_massive._massive.flatfiles import client, layout, reader


def test_the_package_re_exports_the_client_the_reader_and_the_layout() -> None:
    assert sorted(flatfiles.__all__) == [
        "FlatFile",
        "FlatFileClient",
        "ReadColumn",
        "ReadPlan",
        "object_key",
    ]
    assert flatfiles.FlatFileClient is client.FlatFileClient
    assert flatfiles.FlatFile is reader.FlatFile
    assert flatfiles.ReadPlan is reader.ReadPlan
    assert flatfiles.ReadColumn is reader.ReadColumn
    assert flatfiles.object_key is layout.object_key
