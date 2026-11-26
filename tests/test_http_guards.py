"""HTTP streaming, redirects, address policy, and final 5xx."""

from __future__ import annotations

import asyncio
import socket

import pytest

from nodeio import GraphError, http_node, load, run
from tests.support import RecordingClient


def test_verify_false_requires_allow_insecure() -> None:
    """Disabling TLS verification is an explicit choice."""
    with pytest.raises(GraphError, match="allow_insecure"):
        http_node("h", timeout=1, verify=False)
    node = http_node(
        "h",
        timeout=1,
        verify=False,
        allow_insecure=True,
        client=RecordingClient(200, b"ok"),
    )
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["body"] == b"ok"


def test_private_addresses_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loopback, private, and link-local answers are rejected at request time."""

    def resolve(_host: str, *_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        """Return a private address so the guard rejects the host."""
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    blocked = http_node("h", timeout=1, client=RecordingClient(200, b"ok"))
    graph = load((blocked,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://rebind.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "restricted" in str(caught.value.__cause__)
    for host in ("127.0.0.1", "10.0.0.2", "169.254.1.1"):
        literal = http_node("n", timeout=1, client=RecordingClient(200, b"x"))
        literal_graph = load((literal,), (), entries=(("n", "method"), ("n", "url")))
        with pytest.raises(GraphError, match="node n failed"):
            asyncio.run(run(literal_graph, {"n": {"method": "GET", "url": f"https://{host}/"}}))
    allowed = http_node("a", timeout=1, allow_private=True, client=RecordingClient(200, b"local"))
    allowed_graph = load((allowed,), (), entries=(("a", "method"), ("a", "url")))
    body = asyncio.run(run(allowed_graph, {"a": {"method": "GET", "url": "https://127.0.0.1/"}}))
    assert body["a"]["body"] == b"local"


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

    class Remember:
        """Record the URL the node actually requests."""

        def __init__(self) -> None:
            """Start with no URL."""
            self.url = ""

        async def request(self, method: str, url: str, timeout: float) -> object:
            """Store the URL and return a body larger than the cap."""
            del method, timeout
            self.url = url
            return _Streaming(b"abcdef")

    client = Remember()
    node = http_node("h", timeout=1, max_bytes=3, client=client)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://pin.test:8443/x"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "max_bytes" in str(caught.value.__cause__)
    assert client.url.startswith("https://8.8.8.8:8443/")


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
