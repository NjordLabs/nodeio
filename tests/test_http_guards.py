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
