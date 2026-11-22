"""Library-owned HTTP clients stream bodies and do not follow redirects."""

from __future__ import annotations

import asyncio
import ssl

import pytest

from nodeio import GraphError, http_node, load, run


class _Response:
    """Status and async byte iterator for one owned response."""

    def __init__(self, status: int, chunks: tuple[bytes, ...]) -> None:
        """Store the status and the chunks ``aiter_bytes`` will yield."""
        self.status_code = status
        self._chunks = chunks

    def aiter_bytes(self) -> object:
        """Yield the prepared body chunks."""
        return _iterate(self._chunks)


class _Stream:
    """Async context that returns one response."""

    def __init__(self, response: _Response) -> None:
        """Store the response ``__aenter__`` returns."""
        self._response = response

    async def __aenter__(self) -> _Response:
        """Return the prepared response."""
        return self._response

    async def __aexit__(self, *_args: object) -> bool:
        """Ignore the context exit."""
        return False


class _Owned:
    """Async client whose stream returns a fixed response."""

    def __init__(self, response: _Response) -> None:
        """Store the response ``stream`` will wrap."""
        self._response = response

    async def __aenter__(self) -> object:
        """Return this client."""
        return self

    async def __aexit__(self, *_args: object) -> bool:
        """Ignore the context exit."""
        return False

    def stream(self, method: str, url: str) -> _Stream:
        """Return the prepared response stream."""
        del method, url
        return _Stream(self._response)


async def _iterate(chunks: tuple[bytes, ...]) -> object:
    """Yield each prepared chunk."""
    for chunk in chunks:
        yield chunk


def _factory(status: int, chunks: tuple[bytes, ...], seen: dict[str, object] | None = None):
    """Build an httpx constructor that records keyword arguments."""

    def factory(**kwargs: object) -> _Owned:
        """Record constructor arguments and return the owned client."""
        if seen is not None:
            seen.update(kwargs)
        return _Owned(_Response(status, chunks))

    return factory


def test_owned_client_streams_until_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The httpx path stops while chunks are arriving."""
    monkeypatch.setattr("httpx.AsyncClient", _factory(200, (b"abc", b"def")))
    node = http_node("h", timeout=1, max_bytes=4)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "max_bytes" in str(caught.value.__cause__)


def test_redirect_is_not_followed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 302 from a library-owned client is returned as the result."""
    seen: dict[str, object] = {}
    monkeypatch.setattr("httpx.AsyncClient", _factory(302, (), seen))
    node = http_node("h", timeout=1)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["status"] == 302
    assert seen["follow_redirects"] is False


def test_insecure_opt_in_skips_certificate_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit insecure client receives a context that does not verify TLS."""
    seen: dict[str, object] = {}
    monkeypatch.setattr("httpx.AsyncClient", _factory(204, (), seen))
    node = http_node("h", timeout=1, verify=False, allow_insecure=True)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    setting = seen["verify"]
    assert isinstance(setting, ssl.SSLContext)
    assert setting.verify_mode == ssl.CERT_NONE
