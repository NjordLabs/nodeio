"""Branches that the main scenarios do not reach."""

from __future__ import annotations

import asyncio
import builtins
import socket
import sys
from collections.abc import Mapping
from multiprocessing import shared_memory
from types import ModuleType

import pytest

from nodeio import GraphError, Node, load, run
from nodeio.boundary import Observer, otel_tracer
from nodeio.executors import SharedBytes, _restore, _run_with_shared
from nodeio.graph import GraphDefinition
from nodeio.pipes import _Client, _jitter
from tests.support import restore_modules


def test_tracer_is_absent_when_the_import_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A blocked OpenTelemetry import leaves the tracer unset."""
    real_import = builtins.__import__

    def blocked(name: str, *args: object, **kwargs: object) -> object:
        """Refuse OpenTelemetry and forward every other import."""
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    for name in _otel_names():
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(builtins, "__import__", blocked)
    assert otel_tracer() is None


def test_uncallable_get_tracer_returns_none() -> None:
    """A tracer module whose getter is missing returns None."""
    saved = _saved_otel()
    _install_fake_otel()
    try:
        assert otel_tracer() == "nodeio"
        sys.modules["opentelemetry.trace"].get_tracer = None  # type: ignore[attr-defined]
        assert otel_tracer() is None
    finally:
        _restore_otel(saved)


def test_jitter_stays_inside_the_half_to_one_range() -> None:
    """The retry scale stays between one half and one."""
    assert 0.5 <= _jitter() <= 1.0


def test_unresolved_host_fails_the_node(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DNS failure becomes a graph error."""

    def broken(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        """Raise the resolver error getaddrinfo would raise."""
        raise socket.gaierror(1, "nope")

    monkeypatch.setattr(socket, "getaddrinfo", broken)
    graph = load((_http(),), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://missing.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)


def test_odd_address_shapes_still_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """Malformed resolver rows are skipped and a real address is used."""

    def odd(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        """Return one unusable row, one empty address, and one public address."""
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", "nope"),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ()),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", odd)
    graph = load((_http(),), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["status"] == 204


def test_unstreamable_response_is_rejected() -> None:
    """A response without aiter_bytes cannot be read."""

    class Silent:
        """Response double with no byte stream."""

        status_code = 200
        aiter_bytes = None

    with pytest.raises(GraphError, match="streamed"):
        asyncio.run(_read(Silent(), 10))


def test_observer_defaults_and_abstract_client() -> None:
    """Protocol defaults return and the base client cannot perform a request."""
    bare = _Bare()
    definition = GraphDefinition((), ())
    bare.on_start(definition, "n", run_id="1")
    bare.on_success(definition, "n", run_id="1", duration_s=0)
    bare.on_exception(definition, "n", "RuntimeError", run_id="1", duration_s=0)
    with pytest.raises(NotImplementedError):
        asyncio.run(_Client.request(object(), "GET", "https://example.test", 1.0))


def test_shared_bytes_round_trip_and_missing_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restored handles match the payload and an empty block is refused."""
    handle = SharedBytes.from_bytes(b"ab")
    try:
        assert _restore({"value": handle, "n": 1})["value"] == b"ab"
        produced = _run_with_shared(_echo_value, {"value": handle}, {})
        assert produced["value"] == b"ab"
    finally:
        handle.unlink()

    monkeypatch.setattr(shared_memory, "SharedMemory", _empty_block)
    with pytest.raises(RuntimeError, match="buffer"):
        SharedBytes.from_bytes(b"zz")
    with pytest.raises(RuntimeError, match="buffer"):
        SharedBytes("missing", 2).read()


class _Bare(Observer):
    """Use the protocol's default method bodies."""


class _Http:
    async def request(self, method: str, url: str, timeout: float) -> object:
        """Return a fixed empty 204 response."""
        del method, url, timeout
        return _Response()


class _Response:
    status_code = 204
    content = b""


class _EmptyBlock:
    """Shared-memory block whose buffer is missing."""

    buf = None
    name = "missing"

    def close(self) -> None:
        """Ignore close."""
        return None

    def unlink(self) -> None:
        """Ignore unlink."""
        return None


def _http() -> Node:
    from nodeio import http_node

    return http_node("h", timeout=1, client=_Http())


async def _read(response: object, max_bytes: int) -> bytes:
    from nodeio.boundary import _read_limited

    return await _read_limited(response, max_bytes)


def _install_fake_otel() -> None:
    otel = ModuleType("opentelemetry")
    trace = ModuleType("opentelemetry.trace")

    def get_tracer(name: str) -> str:
        """Return the tracer name so the helper can see a callable getter."""
        return name

    trace.get_tracer = get_tracer  # type: ignore[attr-defined]
    otel.trace = trace  # type: ignore[attr-defined]
    sys.modules["opentelemetry"] = otel
    sys.modules["opentelemetry.trace"] = trace


def _echo_value(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Return the value input unchanged."""
    del side
    return {"value": inputs["value"]}


def _empty_block(*_args: object, **_kwargs: object) -> _EmptyBlock:
    """Return a block with no buffer."""
    return _EmptyBlock()


def _otel_names() -> list[str]:
    prefix = "opentelemetry."
    return [key for key in sys.modules if key == "opentelemetry" or key.startswith(prefix)]


def _saved_otel() -> dict[str, ModuleType]:
    return {name: sys.modules[name] for name in _otel_names()}


def _restore_otel(saved: dict[str, ModuleType]) -> None:
    for name in _otel_names():
        if name not in saved:
            sys.modules.pop(name, None)
    restore_modules(saved)
