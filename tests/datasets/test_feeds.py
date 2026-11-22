"""Tests for the WebSocket hosts Massive's pages document, as the library reads them.

The hosts come from the generated channels, so these tests hold whatever a refresh brings: they
check what the library relies on (one host per FMV channel, one feed label per host, the same
hosts on every channel of a dataset) rather than the hostnames themselves.
"""

import pytest

from deephaven_massive._config import DELAYED_HOST, REALTIME_HOST
from deephaven_massive._datasets import feeds
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.feeds import (
    FMV_CHANNEL,
    channel_hosts,
    channel_only_host,
    dataset_hosts,
    fmv_host,
)
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars
from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._massive import Feed, FeedHost, Transport, sources
from deephaven_massive._session import CATALOG

STREAMS = [spec for spec in CATALOG.by_name.values() if spec.channels]
CHANNELS = sources(Transport.WS).values()


def test_a_channel_s_hosts_are_its_generated_channel_s() -> None:
    hosts = channel_hosts(AssetClass.STOCKS, "T")
    assert {h.feed for h in hosts} == {Feed.REAL_TIME, Feed.DELAYED}
    assert channel_only_host(AssetClass.STOCKS, "T") is None
    with pytest.raises(KeyError, match="stocks/NOPE"):
        channel_hosts(AssetClass.STOCKS, "NOPE")


@pytest.mark.parametrize("spec", STREAMS, ids=lambda s: s.name)
def test_every_channel_of_a_dataset_lists_the_same_hosts(spec: DatasetSpec) -> None:
    lists = {channel_hosts(spec.single_class, c) for c in spec.channels.values()}
    assert len(lists) == 1
    (hosts,) = lists
    assert hosts
    assert dataset_hosts(spec) == hosts


def test_a_dataset_without_channels_has_no_hosts() -> None:
    assert dataset_hosts(CATALOG.by_name["stocks.dividends"]) == ()


def test_every_fmv_stream_reads_the_fmv_channel_and_its_one_host() -> None:
    fmv = [spec for spec in STREAMS if spec.channel_family is ChannelFamily.FMV]
    assert {spec.single_class for spec in fmv} == {
        AssetClass.STOCKS,
        AssetClass.OPTIONS,
        AssetClass.FOREX,
        AssetClass.CRYPTO,
    }
    for spec in fmv:
        assert dict(spec.channels) == {NotBars.ANY: FMV_CHANNEL}
        (only,) = dataset_hosts(spec)
        assert fmv_host(spec.single_class) == only.host
    # No other stream reads an FMV channel.
    others = [s for s in STREAMS if s.channel_family is ChannelFamily.TICK]
    assert all(FMV_CHANNEL not in s.channels.values() for s in others)


def test_an_fmv_page_with_several_hosts_is_a_defect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(feeds, "channel_only_host", lambda *_: None)
    with pytest.raises(BrokenGuaranteeError, match="stocks FMV page does not document one host"):
        fmv_host(AssetClass.STOCKS)


def test_every_page_gives_a_host_one_feed() -> None:
    labels: dict[str, set[Feed]] = {}
    for channel in CHANNELS:
        for listed in channel.hosts:
            labels.setdefault(listed.host, set()).add(listed.feed)
    assert all(len(found) == 1 for found in labels.values()), labels


def test_the_real_time_and_delayed_hosts_are_the_pages() -> None:
    # The two hosts the library picks for a class's tick streams are typed once, in the
    # configuration; every page that lists the feed lists that host.
    hosts = [h for channel in CHANNELS for h in channel.hosts]
    assert {h.host for h in hosts if h.feed is Feed.REAL_TIME} == {REALTIME_HOST}
    assert {h.host for h in hosts if h.feed is Feed.DELAYED} == {DELAYED_HOST}


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_ticks_have_the_one_real_time_host(asset_class: AssetClass) -> None:
    # The host decision makes no request for them, and the configuration refuses a delayed
    # host for them: both rest on this.
    for spec in STREAMS:
        if spec.single_class is asset_class and spec.channel_family is ChannelFamily.TICK:
            assert dataset_hosts(spec) == (FeedHost(feed=Feed.REAL_TIME, host=REALTIME_HOST),)
