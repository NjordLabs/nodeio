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


def test_http_backoff_waits_between_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """An idempotent retry waits with exponential backoff."""
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        """Record the backoff delay."""
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr("nodeio.pipes._jitter", lambda: 1.0)

    class Client:
        """Hold a test double."""

        def __init__(self) -> None:
            """Count calls."""
            self.calls = 0

        async def request(
            self, method: str, url: str, timeout: float
        ) -> SimpleNamespace:
            """Return a server error and then success."""
            del method, url, timeout
            self.calls += 1
            status = 500 if self.calls == 1 else 200
            return SimpleNamespace(status_code=status, content=b"ok")

    node = http_node("h", timeout=1, retries=2, backoff=0.2, client=Client())
    definition = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(
        run(definition, {"h": {"method": "GET", "url": "https://example.test/item"}})
    )
    assert outputs["h"]["status"] == 200
    assert delays == [0.2]
