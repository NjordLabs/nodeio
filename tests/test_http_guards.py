"""HTTP streaming, redirects, address policy, and final 5xx."""

from __future__ import annotations

import asyncio
import json
import socket
from types import SimpleNamespace

import pytest

from nodeio import GraphError, http_node, load, run


def test_verify_false_requires_allow_insecure() -> None:
    """Disabling TLS verification is an explicit choice."""
    with pytest.raises(GraphError, match="allow_insecure"):
        http_node("h", timeout=1, verify=False)
    node = http_node(
        "h",
        timeout=1,
        verify=False,
        allow_insecure=True,
        client=_Client(200, b"ok"),
    )
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["body"] == b"ok"


def test_private_addresses_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loopback, private, and link-local answers are rejected at request time."""

    def resolve(host: str, *_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    blocked = http_node("h", timeout=1, client=_Client(200, b"ok"))
    graph = load((blocked,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://rebind.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "restricted" in str(caught.value.__cause__)
    for host in ("127.0.0.1", "10.0.0.2", "169.254.1.1"):
        literal = http_node("n", timeout=1, client=_Client(200, b"x"))
        literal_graph = load((literal,), (), entries=(("n", "method"), ("n", "url")))
        with pytest.raises(GraphError, match="node n failed"):
            asyncio.run(run(literal_graph, {"n": {"method": "GET", "url": f"https://{host}/"}}))
    allowed = http_node("a", timeout=1, allow_private=True, client=_Client(200, b"local"))
    allowed_graph = load((allowed,), (), entries=(("a", "method"), ("a", "url")))
    body = asyncio.run(run(allowed_graph, {"a": {"method": "GET", "url": "https://127.0.0.1/"}}))
    assert body["a"]["body"] == b"local"


def test_final_5xx_raises_after_retries() -> None:
    """Idempotent retries that end in 5xx are a graph error."""
    client = _Client(500, b"nope")
    node = http_node("h", timeout=1, retries=1, client=client)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "after retries" in str(caught.value.__cause__)
    assert client.calls == 2
    posted = _Client(500, b"once")
    post = http_node("p", timeout=1, retries=3, client=posted)
    post_graph = load((post,), (), entries=(("p", "method"), ("p", "url")))
    outputs = asyncio.run(run(post_graph, {"p": {"method": "POST", "url": "https://example.test"}}))
    assert outputs["p"]["status"] == 500
    assert posted.calls == 1


def test_http_log_records_duration_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    """The log line has a duration and omits bodies, userinfo, and headers."""
    from mimirlog import configure

    configure()
    node = http_node("h", timeout=1, client=_Client(204, b"secret-body"))
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    asyncio.run(
        run(graph, {"h": {"method": "GET", "url": "https://user:secret@example.test/item"}})
    )
    captured = capsys.readouterr()
    text = captured.err + captured.out
    assert "secret" not in text
    assert "secret-body" not in text
    payload = json.loads(text.strip().splitlines()[-1])
    message = payload["message"]
    assert message["duration_s"] >= 0
    assert message["host"] == "example.test"
    assert set(message) == {"attempt", "duration_s", "event", "host", "method", "status"}


def test_owned_client_streams_until_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The httpx path stops while chunks are arriving."""

    class Response:
        status_code = 200

        def aiter_bytes(self) -> object:
            return _chunks()

    class Stream:
        async def __aenter__(self) -> Response:
            return Response()

        async def __aexit__(self, *_args: object) -> bool:
            return False

    class Owned:
        async def __aenter__(self) -> Owned:
            return self

        async def __aexit__(self, *_args: object) -> bool:
            return False

        def stream(self, method: str, url: str) -> Stream:
            del method, url
            return Stream()

    monkeypatch.setattr("httpx.AsyncClient", lambda **_kwargs: Owned())
    node = http_node("h", timeout=1, max_bytes=4)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "max_bytes" in str(caught.value.__cause__)


def test_redirect_is_not_followed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 302 from a library-owned client is returned as the result."""
    seen: dict[str, object] = {}

    class Response:
        status_code = 302

        def aiter_bytes(self) -> object:
            return _empty()

    class Stream:
        async def __aenter__(self) -> Response:
            return Response()

        async def __aexit__(self, *_args: object) -> bool:
            return False

    class Owned:
        async def __aenter__(self) -> Owned:
            return self

        async def __aexit__(self, *_args: object) -> bool:
            return False

        def stream(self, method: str, url: str) -> Stream:
            del method, url
            return Stream()

    def factory(**kwargs: object) -> Owned:
        seen.update(kwargs)
        return Owned()

    monkeypatch.setattr("httpx.AsyncClient", factory)
    node = http_node("h", timeout=1)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["status"] == 302
    assert seen["follow_redirects"] is False


class _Client:
    """Injected client that only exposes content."""

    def __init__(self, status: int, body: bytes) -> None:
        self.calls = 0
        self._status = status
        self._body = body

    async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
        del method, url, timeout
        self.calls += 1
        return SimpleNamespace(status_code=self._status, content=self._body)


async def _chunks() -> object:
    yield b"abc"
    yield b"def"


async def _empty() -> object:
    if False:
        yield b""
