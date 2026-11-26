"""The HTTP client is pinned to the address that was checked.

The name is resolved once. Any answer that is not global, including
shared address space and multicast, is rejected. The request then uses
the first accepted address, so a later lookup cannot switch the target.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from nodeio import GraphError, http_node, load, run
from tests.support import RecordingClient


def test_non_global_and_multicast_addresses_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """CGNAT and multicast answers are restricted even though they are not private."""

    def resolve(host: str, *_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        """Return the address encoded in the host label."""
        address = "100.64.0.1" if host == "cgnat.test" else "224.0.0.1"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    for host in ("cgnat.test", "multicast.test"):
        node = http_node("h", timeout=1, client=RecordingClient(200, b"ok"))
        graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
        with pytest.raises(GraphError, match="node h failed") as caught:
            asyncio.run(run(graph, {"h": {"method": "GET", "url": f"https://{host}/"}}))
        assert isinstance(caught.value.__cause__, GraphError)
        assert "restricted" in str(caught.value.__cause__)


def test_request_uses_the_checked_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """The client is given the address that passed the check, not the name."""

    def resolve(_host: str, *_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        """Return two global answers. The first is the one that is pinned."""
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    client = _Remember()
    node = http_node("h", timeout=1, max_bytes=3, client=client)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://pin.test:8443/x"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "max_bytes" in str(caught.value.__cause__)
    assert client.url.startswith("https://8.8.8.8:8443/")


class _Remember:
    """Record the URL the node actually requests."""

    def __init__(self) -> None:
        """Start with no URL."""
        self.url = ""

    async def request(self, method: str, url: str, timeout: float) -> _Streaming:
        """Store the URL and return a body larger than the cap."""
        del method, timeout
        self.url = url
        return _Streaming(b"abcdef")


class _Streaming:
    """Response whose body arrives in chunks."""

    def __init__(self, body: bytes) -> None:
        """Store the body."""
        self.status_code = 200
        self._body = body

    async def aiter_bytes(self) -> object:
        """Yield the body in two chunks."""
        yield self._body[:2]
        yield self._body[2:]
