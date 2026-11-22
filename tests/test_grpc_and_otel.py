"""gRPC channel construction and the optional span observer."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import GraphError, TelemetryObserver, grpc_node, load, pipes, run


def test_grpc_uses_an_injected_channel_factory() -> None:
    """A factory stands in for grpc.aio so the test needs no server."""
    opened: list[object] = []

    class Channel:
        def __init__(self) -> None:
            self.closed = False
            self.calls: list[tuple[str, object, float]] = []

        def unary_unary(self, method: str):
            async def call(request: object, timeout: float) -> dict[str, object]:
                self.calls.append((method, request, timeout))
                return {"method": method, "request": request, "timeout": timeout}

            return call

        async def close(self) -> None:
            self.closed = True

    def factory(target: str, **kwargs: object) -> Channel:
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
    monkeypatch.setattr(pipes, "_require_grpc", lambda: None)
    node = grpc_node("g", method="pkg.Service/Run", deadline=1)
    graph = load((node,), (), entries=(("g", "request"),))
    with pytest.raises(GraphError, match="node g failed") as caught:
        asyncio.run(run(graph, {"g": {"request": {}}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "target is required" in str(caught.value.__cause__)
    assert "not installed" not in str(caught.value.__cause__)


def test_telemetry_spans_when_a_tracer_is_injected() -> None:
    """Each node start opens a span and success or failure ends it."""

    class Span:
        def __init__(self, name: str) -> None:
            self.name = name
            self.attributes: dict[str, str] = {}
            self.ended = False

        def set_attribute(self, key: str, value: str) -> None:
            self.attributes[key] = value

        def end(self) -> None:
            self.ended = True

    class Tracer:
        def __init__(self) -> None:
            self.spans: list[Span] = []

        def start_span(self, name: str) -> Span:
            span = Span(name)
            self.spans.append(span)
            return span

    tracer = Tracer()
    observer = TelemetryObserver("demo", tracer)
    node = _node("a", fail=False)
    asyncio.run(run(load((node,), ()), {}, observer=observer))
    assert tracer.spans[0].ended is True
    assert tracer.spans[0].attributes["nodeio.node"] == "a"
    failing = TelemetryObserver("demo", tracer)
    bad = _node("b", fail=True)
    with pytest.raises(GraphError):
        asyncio.run(run(load((bad,), ()), {}, observer=failing))
    assert tracer.spans[1].attributes["nodeio.exception"] == "RuntimeError"
    assert tracer.spans[1].ended is True


def test_telemetry_noops_without_the_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing API and an explicit None tracer do not start spans."""
    monkeypatch.setattr("nodeio.observe.otel_tracer", lambda: None)
    observer = TelemetryObserver("demo")
    node = _node("a", fail=False)
    asyncio.run(run(load((node,), ()), {}, observer=observer))
    silent = TelemetryObserver("demo", None)
    asyncio.run(run(load((node,), ()), {}, observer=silent))


def _node(node_id: str, *, fail: bool):
    async def runner(inputs: object, side: object) -> dict[str, object]:
        del inputs, side
        if fail:
            raise RuntimeError("nope")
        return {"value": 1}

    from nodeio import Node, Port

    return Node(node_id, "map", (), (Port("value", "any"),), runner)
