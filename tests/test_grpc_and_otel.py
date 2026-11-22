"""gRPC channel construction and the optional span observer."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import GraphError, grpc_node, load, pipes, run


def test_grpc_uses_an_injected_channel_factory() -> None:
    """A factory stands in for grpc.aio so the test needs no server."""
    opened: list[object] = []

    class Channel:
        """Stand-in channel that records unary calls and close."""

        def __init__(self) -> None:
            """Start open and with no calls."""
            self.closed = False
            self.calls: list[tuple[str, object, float]] = []

        def unary_unary(self, method: str) -> object:
            """Return a call that records the request."""

            async def call(request: object, timeout: float) -> dict[str, object]:
                """Record the method, request, and timeout."""
                self.calls.append((method, request, timeout))
                return {"method": method, "request": request, "timeout": timeout}

            return call

        async def close(self) -> None:
            """Mark the channel closed."""
            self.closed = True

    def factory(target: str, **kwargs: object) -> Channel:
        """Open one channel and remember how it was constructed."""
        channel = Channel()
        opened.append((target, kwargs, channel))
        return channel

    node = grpc_node(
        "g",
        method="pkg.Service/Run",
        deadline=0.25,
        target="api.example:443",
        channel_factory=factory,
        root_certificates=b"root",
        private_key=b"key",
        certificate_chain=b"cert",
    )
    graph = load((node,), (), entries=(("g", "request"),))
    outputs = asyncio.run(run(graph, {"g": {"request": {"n": 1}}}))
    response = outputs["g"]["response"]
    assert isinstance(response, dict)
    assert response["timeout"] == 0.25
    target, kwargs, channel = opened[0]
    assert target == "api.example:443"
    assert kwargs["allow_insecure"] is False
    assert kwargs["private_key"] == b"key"
    assert channel.closed is True

    insecure = grpc_node(
        "i",
        method="pkg.Service/Run",
        deadline=1,
        target="api.example:443",
        allow_insecure=True,
        channel_factory=factory,
    )
    insecure_graph = load((insecure,), (), entries=(("i", "request"),))
    asyncio.run(run(insecure_graph, {"i": {"request": {}}}))
    assert opened[1][1]["allow_insecure"] is True


def test_installed_grpc_without_a_target_names_the_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """A present extra does not report itself missing when target is absent."""

    def allow_import() -> None:
        """Pretend the grpc extra is already installed."""
        return None

    monkeypatch.setattr(pipes, "_require_grpc", allow_import)
    node = grpc_node("g", method="pkg.Service/Run", deadline=1)
    graph = load((node,), (), entries=(("g", "request"),))
    with pytest.raises(GraphError, match="node g failed") as caught:
        asyncio.run(run(graph, {"g": {"request": {}}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "target is required" in str(caught.value.__cause__)
    assert "not installed" not in str(caught.value.__cause__)
