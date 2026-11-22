"""Channel construction and a tracer injected through the observer."""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType

import pytest

from nodeio import Node, TelemetryObserver, load, observe, run
from nodeio.boundary import grpc_channel
from tests.support import restore_modules


def test_grpc_channel_and_loaded_tracer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake grpc modules exercise the channel helper and a present tracer."""
    saved = {name: sys.modules.get(name) for name in ("grpc", "grpc.aio")}
    grpc = ModuleType("grpc")
    aio = ModuleType("grpc.aio")
    aio.insecure_channel = _insecure_channel  # type: ignore[attr-defined]
    aio.secure_channel = _secure_channel  # type: ignore[attr-defined]
    grpc.aio = aio  # type: ignore[attr-defined]
    grpc.ssl_channel_credentials = _ssl_channel_credentials  # type: ignore[attr-defined]
    sys.modules["grpc"] = grpc
    sys.modules["grpc.aio"] = aio
    try:
        assert grpc_channel("api:1", True, None, None, None) == ("insecure", "api:1")
        opened = grpc_channel("api:1", False, b"root", b"key", b"cert")
        assert opened[0] == "secure"
    finally:
        restore_modules(saved)

    def tracer() -> object:
        """Return a tracer object with no span methods."""
        return object()

    monkeypatch.setattr(observe, "otel_tracer", tracer)
    TelemetryObserver("demo")
    graph = load((_grpc_with_channel(),), (), entries=(("g", "request"),))
    outputs = asyncio.run(run(graph, {"g": {"request": {}}}))
    assert outputs["g"]["response"]["timeout"] == 1


class _Channel:
    """Stand-in channel that echoes one unary call."""

    def unary_unary(self, method: str) -> object:
        """Return a call that echoes the request and the timeout."""
        del method

        def call(request: object, timeout: float) -> dict[str, object]:
            """Echo the request and the timeout."""
            return {"request": request, "timeout": timeout}

        return call

    def close(self) -> None:
        """Ignore close."""
        return None


def _insecure_channel(target: str) -> tuple[str, str]:
    """Record an insecure channel target."""
    return ("insecure", target)


def _ssl_channel_credentials(**kwargs: object) -> dict[str, object]:
    """Return the TLS material passed to the channel."""
    return dict(kwargs)


def _secure_channel(target: str, credentials: object) -> tuple[str, str, object]:
    """Record a secure channel target and its credentials."""
    return ("secure", target, credentials)


def _channel_factory(target: str, **kwargs: object) -> _Channel:
    """Ignore the target and return the stand-in channel."""
    del target, kwargs
    return _Channel()


def _grpc_with_channel() -> Node:
    from nodeio import grpc_node

    return grpc_node(
        "g",
        method="pkg.Service/Run",
        deadline=1,
        target="api:1",
        channel_factory=_channel_factory,
    )
