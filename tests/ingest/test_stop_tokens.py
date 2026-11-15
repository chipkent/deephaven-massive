import threading

import pytest

from deephaven_massive import _massive
from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._ingest.stop_tokens import as_cancel


def test_a_cancel_or_none_is_returned_as_it_is() -> None:
    stop = _massive.Cancel()
    assert as_cancel(stop) is stop
    assert as_cancel(None) is None


def test_a_plain_event_is_a_library_defect() -> None:
    with pytest.raises(BrokenGuaranteeError, match=r"^a read's stop token is Event, not Cancel\. "):
        as_cancel(threading.Event())
