"""The WebSocket hosts Massive's pages document for each channel and each streamed dataset.

Each WebSocket channel's documentation page lists, in its "Connection" table, the feeds that
serve the channel and their hosts (`Feed`, `FeedHost`): `Real-Time` on `socket.massive.com`,
`Delayed` on `delayed.massive.com`, `Business` on `business.massive.com`. The spec refresh
copies them into the generated channel (`WsChannel.hosts`), and this module reads them there,
so no hostname of a single channel is typed by hand:

- `channel_hosts(asset_class, channel)`: one channel's documented hosts, and
  `channel_only_host(asset_class, channel)` its one host when it has one;
- `dataset_hosts(spec)`: every host a dataset's channels document, sorted by feed;
- `fmv_host(asset_class)`: the one host the class's fair-market-value channel documents,
  which every stream of the FMV channel family uses.

A feed names where a channel is served, never how recent its data is: Massive's index value
page lists only the `Business` feed, yet a plan may get those values 15 minutes late. So nothing
here says whether a stream is real-time.

The generated channels are loaded on first use (`_massive.sources`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from deephaven_massive._errors import BrokenGuaranteeError, library_bug
from deephaven_massive._massive import Transport, source

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._enums import AssetClass
    from deephaven_massive._massive import FeedHost

__all__ = [
    "FMV_CHANNEL",
    "channel_hosts",
    "channel_only_host",
    "dataset_hosts",
    "fmv_host",
]

FMV_CHANNEL: Final = "FMV"
"""The fair-market-value channel, the one channel of the FMV family in every class that has it."""


def channel_hosts(asset_class: AssetClass, channel: str) -> tuple[FeedHost, ...]:
    """Return the hosts Massive's page documents for one channel, sorted by feed.

    Args:
        asset_class: The channel's class, whose value is its cluster.
        channel: The channel, such as `"T"`.

    Returns:
        Each documented feed and its host, such as `Real-Time` on `socket.massive.com`.

    Raises:
        KeyError: If the class has no generated channel of that name.
    """
    return source(Transport.WS, f"{asset_class.value}/{channel}").hosts


def channel_only_host(asset_class: AssetClass, channel: str) -> str | None:
    """Return the one host Massive's page documents for a channel, if it documents one.

    Args:
        asset_class: The channel's class.
        channel: The channel, such as `"V"`.

    Returns:
        The host, such as `"business.massive.com"` for index values; None for a channel its
        page documents on several hosts.

    Raises:
        KeyError: If the class has no generated channel of that name.
    """
    return source(Transport.WS, f"{asset_class.value}/{channel}").only_host


def dataset_hosts(spec: DatasetSpec) -> tuple[FeedHost, ...]:
    """Return every host the pages of a dataset's channels document, sorted by feed.

    The channels of one dataset (a bar dataset's minute and second channels) list the same
    hosts, which a test checks.

    Args:
        spec: A dataset; one without channels has none.

    Returns:
        The distinct hosts, sorted by feed; empty for a dataset without channels.
    """
    found = {
        host
        for channel in spec.channels.values()
        for host in channel_hosts(spec.single_class, channel)
    }
    return tuple(sorted(found, key=lambda h: (h.feed, h.host)))


def fmv_host(asset_class: AssetClass) -> str:
    """Return the host of a class's fair-market-value streams: the one its page documents.

    Args:
        asset_class: A class with an FMV channel.

    Returns:
        The host, such as `"business.massive.com"`.

    Raises:
        BrokenGuaranteeError: If the class's FMV page documents more than one host, or none,
            which a test of the generated channels rules out.
    """
    host = channel_only_host(asset_class, FMV_CHANNEL)
    if host is None:
        raise BrokenGuaranteeError(
            library_bug(f"the {asset_class.value} {FMV_CHANNEL} page does not document one host")
        )
    return host
