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

        async def request(self, method: str, url: str) -> SimpleNamespace:
            """Run this step."""
            del method, url
            return SimpleNamespace(status_code=204, content=b"")

    seen: dict[str, object] = {}

    def factory(**kwargs: object) -> Owned:
        """Run this step."""
        seen.update(kwargs)
        return Owned()

    monkeypatch.setattr("httpx.AsyncClient", factory)
    plain = http_node("h", timeout=1, verify="/ca.pem", cert=("cert.pem", "key.pem"))
    graph = load((plain,), (), entries=(("h", "method"), ("h", "url")))
    status = asyncio.run(
        run(graph, {"h": {"method": "GET", "url": "https://example.test"}})
    )
    assert status["h"]["status"] == 204
    assert seen["verify"] == "/ca.pem"
    assert seen["cert"] == ("cert.pem", "key.pem")
