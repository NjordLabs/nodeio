"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nodeio import (
    http_node,
    load,
    run,
)


class _Stream:
    """Async context manager yielding one empty response."""

    async def __aenter__(self) -> SimpleNamespace:
        """Run this step."""
        return SimpleNamespace(status_code=204, aiter_bytes=_empty)

    async def __aexit__(self, *args: object) -> bool:
        """Run this step."""
        return False


async def _empty() -> object:
    """Yield no body bytes."""
    for _chunk in ():
        yield _chunk


def test_owned_client_receives_tls_material(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run this step."""

    class Owned:
        """Hold a test double."""

        async def __aenter__(self) -> object:
            """Run this step."""
            return self

        async def __aexit__(self, *args: object) -> bool:
            """Run this step."""
            return False

        def stream(self, method: str, url: str) -> _Stream:
            """Run this step."""
            del method, url
            return _Stream()

    seen: dict[str, object] = {}

    def factory(**kwargs: object) -> Owned:
        """Run this step."""
        seen.update(kwargs)
        return Owned()

    monkeypatch.setattr("httpx.AsyncClient", factory)
    plain = http_node("h", timeout=1, verify="/ca.pem", cert=("cert.pem", "key.pem"))
    graph = load((plain,), (), entries=(("h", "method"), ("h", "url")))
    status = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert status["h"]["status"] == 204
    assert status["h"]["body"] == b""
    assert seen["verify"] == "/ca.pem"
    assert seen["cert"] == ("cert.pem", "key.pem")
    assert seen["follow_redirects"] is False
