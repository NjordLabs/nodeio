"""Public gRPC node against a stand-in grpc module."""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType

from nodeio import grpc_node, load, run
from tests.support import restore_modules


class _Channel:
    """Channel double that echoes one unary call."""

    def unary_unary(self, method: str):
        """Return a call that echoes the request and the deadline."""

        def call(request: object, timeout: float) -> dict[str, object]:
            """Echo the method, request, and timeout."""
            return {"method": method, "request": request, "timeout": timeout}

        return call

    def close(self) -> None:
        """Ignore close."""
        return None


def _insecure_channel(target: str) -> _Channel:
    """Return a channel for the requested target."""
    del target
    return _Channel()


def test_fake_grpc_serves_an_insecure_call() -> None:
    """An insecure node uses the channel from the installed grpc module."""
    saved = {name: sys.modules.get(name) for name in ("grpc", "grpc.aio")}
    grpc = ModuleType("grpc")
    aio = ModuleType("grpc.aio")
    aio.insecure_channel = _insecure_channel  # type: ignore[attr-defined]
    grpc.aio = aio  # type: ignore[attr-defined]
    sys.modules["grpc"] = grpc
    sys.modules["grpc.aio"] = aio
    try:
        node = grpc_node(
            "g",
            method="pkg.Service/Run",
            deadline=1,
            target="api:1",
            allow_insecure=True,
        )
        graph = load((node,), (), entries=(("g", "request"),))
        outputs = asyncio.run(run(graph, {"g": {"request": {"n": 1}}}))
        response = outputs["g"]["response"]
        assert response["method"] == "pkg.Service/Run"
        assert response["timeout"] == 1
    finally:
        restore_modules(saved)
