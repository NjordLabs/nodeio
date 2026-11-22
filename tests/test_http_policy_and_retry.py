"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nodeio import (
    GraphError,
    http_node,
    load,
    run,
)


def test_http_policy_and_retry() -> None:
    """Run this step."""
    with pytest.raises(GraphError, match="timeout"):
        http_node("h", timeout=0)

    class Client:
        """Hold a test double."""

        def __init__(self) -> None:
            """Run this step."""
            self.calls = 0

        async def request(
            self, method: str, url: str, timeout: float
        ) -> SimpleNamespace:
            """Run this step."""
            del url, timeout
            self.calls += 1
            if method == "GET" and self.calls == 1:
                return SimpleNamespace(status_code=500, content=b"ok")
            return SimpleNamespace(status_code=200, content=b"ok")

    client = Client()
    node = http_node(
        "h",
        timeout=1,
        allow_insecure=True,
        allow_hosts=("example.test",),
        retries=1,
        client=client,
    )
    definition = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(
        run(definition, {"h": {"method": "GET", "url": "http://example.test/item"}})
    )
    assert outputs["h"]["status"] == 200
    assert client.calls == 2
    with pytest.raises(GraphError, match="node b failed") as insecure:
        blocked = http_node("b", timeout=1, client=client)
        graph = load((blocked,), (), entries=(("b", "method"), ("b", "url")))
        asyncio.run(run(graph, {"b": {"method": "GET", "url": "http://example.test"}}))
    assert isinstance(insecure.value.__cause__, GraphError)
    assert "insecure" in str(insecure.value.__cause__)
    with pytest.raises(GraphError, match="node c failed") as blocked_host:
        limited = http_node("c", timeout=1, allow_hosts=("ok.test",), client=client)
        graph = load((limited,), (), entries=(("c", "method"), ("c", "url")))
        asyncio.run(run(graph, {"c": {"method": "GET", "url": "https://no.test"}}))
    assert isinstance(blocked_host.value.__cause__, GraphError)
    assert "not allowed" in str(blocked_host.value.__cause__)
