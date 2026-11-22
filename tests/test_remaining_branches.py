"""Branches that the main scenarios do not reach."""

from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import Mapping
from multiprocessing import shared_memory
from types import ModuleType

import pytest

from nodeio import GraphError, Node, Port, TelemetryObserver, load, register_subgraph, run
from nodeio.boundary import Observer, grpc_channel, otel_tracer
from nodeio.builtin import reset_subgraphs, subgraph_node
from nodeio.executors import SharedBytes, _drive_close, _restore, _run_with_shared
from nodeio.flow import EdgeBuffer
from nodeio.graph import GraphDefinition
from nodeio.pipes import _Client, _jitter
from nodeio.run import _raise_from_group


def test_direct_helpers_cover_rare_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Helpers reject bad sockets, empty tracers, and non-awaitable calls."""
    assert otel_tracer() is None
    _install_fake_otel()
    try:
        assert otel_tracer() == "nodeio"
        sys.modules["opentelemetry.trace"].get_tracer = None  # type: ignore[attr-defined]
        assert otel_tracer() is None
    finally:
        sys.modules.pop("opentelemetry", None)
        sys.modules.pop("opentelemetry.trace", None)
    assert 0.5 <= _jitter() <= 1.0

    def broken(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        raise socket.gaierror(1, "nope")

    monkeypatch.setattr(socket, "getaddrinfo", broken)
    node = _http()
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://missing.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)

    def odd(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", "nope"),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ()),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", odd)
    outputs = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert outputs["h"]["status"] == 204

    class Silent:
        status_code = 200
        aiter_bytes = None

    with pytest.raises(GraphError, match="streamed"):
        asyncio.run(_read(Silent(), 10))

    bare = _Bare()
    definition = GraphDefinition((), ())
    bare.on_start(definition, "n", run_id="1")
    bare.on_success(definition, "n", run_id="1", duration_s=0)
    bare.on_exception(definition, "n", "RuntimeError", run_id="1", duration_s=0)

    with pytest.raises(NotImplementedError):
        asyncio.run(_Client.request(object(), "GET", "https://example.test", 1.0))

    handle = SharedBytes.from_bytes(b"ab")
    try:
        assert _restore({"value": handle, "n": 1})["value"] == b"ab"
        produced = _run_with_shared(
            lambda inputs, side: {"value": inputs["value"]},
            {"value": handle},
            {},
        )
        assert produced["value"] == b"ab"
    finally:
        handle.unlink()

    class Empty:
        buf = None
        name = "missing"

        def close(self) -> None:
            return None

        def unlink(self) -> None:
            return None

    monkeypatch.setattr(shared_memory, "SharedMemory", lambda *args, **kwargs: Empty())
    with pytest.raises(RuntimeError, match="buffer"):
        SharedBytes.from_bytes(b"zz")
    with pytest.raises(RuntimeError, match="buffer"):
        SharedBytes("missing", 2).read()


def test_get_waits_until_a_value_arrives() -> None:
    """An empty edge queue waits instead of failing."""

    async def check() -> None:
        buffer = EdgeBuffer()
        task = asyncio.create_task(buffer.get(("b", "value")))
        await asyncio.sleep(0)
        await buffer.put(("b", "value"), 5, None)
        assert await task == 5

    asyncio.run(check())


def test_cancel_before_start_and_group_unwrapping() -> None:
    """A set event cancels before a node, and group errors unwrap."""
    cancel = asyncio.Event()
    cancel.set()
    node = Node("a", "map", (), (Port("value", "any"),), _nope)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run(load((node,), ()), {}, cancel=cancel))
    with pytest.raises(asyncio.CancelledError):
        _raise_from_group(BaseExceptionGroup("cancel", [asyncio.CancelledError()]))
    with pytest.raises(RuntimeError, match="other"):
        _raise_from_group(BaseExceptionGroup("other", [RuntimeError("other")]))


def test_close_uncancels_the_current_task() -> None:
    """close runs while the task is cancelling, then cancellation resumes."""

    async def body() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            await _drive_close(lambda: None)

    async def scenario() -> None:
        task = asyncio.create_task(body())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_subgraph_without_an_output_is_an_error() -> None:
    """A child that publishes nothing is a graph error."""
    reset_subgraphs()
    register_subgraph("empty", load((), ()))
    parent = load((subgraph_node("sub", "empty"),), (), entries=(("sub", "value"),))
    with pytest.raises(GraphError, match="node sub failed") as caught:
        asyncio.run(run(parent, {"sub": {"value": 1}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "no value" in str(caught.value.__cause__)
    reset_subgraphs()


def test_grpc_channel_and_loaded_tracer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake grpc modules exercise the channel helper and a present tracer."""
    saved = {name: sys.modules.get(name) for name in ("grpc", "grpc.aio")}
    grpc = ModuleType("grpc")
    aio = ModuleType("grpc.aio")

    def insecure_channel(target: str) -> tuple[str, str]:
        return ("insecure", target)

    def ssl_channel_credentials(**kwargs: object) -> dict[str, object]:
        return dict(kwargs)

    def secure_channel(target: str, credentials: object) -> tuple[str, str, object]:
        return ("secure", target, credentials)

    aio.insecure_channel = insecure_channel  # type: ignore[attr-defined]
    aio.secure_channel = secure_channel  # type: ignore[attr-defined]
    grpc.aio = aio  # type: ignore[attr-defined]
    grpc.ssl_channel_credentials = ssl_channel_credentials  # type: ignore[attr-defined]
    sys.modules["grpc"] = grpc
    sys.modules["grpc.aio"] = aio
    try:
        assert grpc_channel("api:1", True, None, None, None) == ("insecure", "api:1")
        opened = grpc_channel("api:1", False, b"root", b"key", b"cert")
        assert opened[0] == "secure"
        from nodeio import pipes

        pipes._require_grpc()
        opened_insecure = pipes._open_channel("api:1", True, None, None, None, None)
        assert opened_insecure == ("insecure", "api:1")
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    import nodeio.observe as observe

    monkeypatch.setattr(observe, "otel_tracer", lambda: object())
    TelemetryObserver("demo")

    class Channel:
        def unary_unary(self, method: str):
            del method

            def call(request: object, timeout: float) -> dict[str, object]:
                return {"request": request, "timeout": timeout}

            return call

        def close(self) -> None:
            return None

    from nodeio import grpc_node

    node = grpc_node(
        "g",
        method="pkg.Service/Run",
        deadline=1,
        target="api:1",
        channel_factory=lambda target, **kwargs: Channel(),
    )
    graph = load((node,), (), entries=(("g", "request"),))
    outputs = asyncio.run(run(graph, {"g": {"request": {}}}))
    assert outputs["g"]["response"]["timeout"] == 1


class _Bare(Observer):
    """Use the protocol's default method bodies."""


class _Http:
    async def request(self, method: str, url: str, timeout: float) -> object:
        del method, url, timeout
        return _Response()


class _Response:
    status_code = 204
    content = b""


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
        return name

    trace.get_tracer = get_tracer  # type: ignore[attr-defined]
    otel.trace = trace  # type: ignore[attr-defined]
    sys.modules["opentelemetry"] = otel
    sys.modules["opentelemetry.trace"] = trace


async def _nope(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    del inputs, side
    raise AssertionError("node should not start")
