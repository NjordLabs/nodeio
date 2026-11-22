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


def test_non_int_status_and_unsupported_scheme() -> None:
    """Run this step."""

    class Weird:
        """Hold a test double."""

        async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
            """Run this step."""
            del method, url, timeout
            return SimpleNamespace(status_code="no", content=b"")

    weird = http_node("w", timeout=1, client=Weird())
    weird_graph = load((weird,), (), entries=(("w", "method"), ("w", "url")))
    with pytest.raises(GraphError, match="node w failed"):
        asyncio.run(run(weird_graph, {"w": {"method": "GET", "url": "https://example.test"}}))
    ftp = http_node("f", timeout=1, client=Weird())
    ftp_graph = load((ftp,), (), entries=(("f", "method"), ("f", "url")))
    with pytest.raises(GraphError, match="node f failed"):
        asyncio.run(run(ftp_graph, {"f": {"method": "GET", "url": "ftp://example.test"}}))
