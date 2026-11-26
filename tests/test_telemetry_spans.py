"""OpenTelemetry observer spans for success and failure."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import GraphError, Node, Port, TelemetryObserver, load, run


def test_telemetry_spans_when_a_tracer_is_injected() -> None:
    """Each node start opens a span and success or failure ends it."""

    class Span:
        """Record attributes and whether the span was ended."""

        def __init__(self, name: str) -> None:
            """Store the span name."""
            self.name = name
            self.attributes: dict[str, str] = {}
            self.ended = False

        def set_attribute(self, key: str, value: str) -> None:
            """Store one string attribute."""
            self.attributes[key] = value

        def end(self) -> None:
            """Mark the span ended."""
            self.ended = True

    class Tracer:
        """Collect spans started by the observer."""

        def __init__(self) -> None:
            """Start with no spans."""
            self.spans: list[Span] = []

        def start_span(self, name: str) -> Span:
            """Start and remember a span."""
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

    async def cancel_run() -> None:
        """Cancel a node that is waiting and expect its span to end."""
        waiting = TelemetryObserver("demo", tracer)

        async def sleep(inputs: object, side: object) -> dict[str, object]:
            del inputs, side
            await asyncio.sleep(30)
            return {}

        paused = Node("c", "map", (), (), sleep)
        task = asyncio.create_task(run(load((paused,), ()), {}, observer=waiting))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_run())
    assert tracer.spans[-1].ended is True
    assert tracer.spans[-1].attributes["nodeio.exception"] == "CancelledError"


def test_telemetry_noops_without_the_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing API and an explicit None tracer do not start spans."""

    def no_tracer() -> None:
        """Report that the OpenTelemetry API is absent."""
        return None

    monkeypatch.setattr("nodeio.observe.otel_tracer", no_tracer)
    observer = TelemetryObserver("demo")
    node = _node("a", fail=False)
    asyncio.run(run(load((node,), ()), {}, observer=observer))
    silent = TelemetryObserver("demo", None)
    asyncio.run(run(load((node,), ()), {}, observer=silent))


def _node(node_id: str, *, fail: bool) -> Node:
    async def runner(inputs: object, side: object) -> dict[str, object]:
        """Return one value, or raise when the node should fail."""
        del inputs, side
        if fail:
            raise RuntimeError("nope")
        return {"value": 1}

    return Node(node_id, "map", (), (Port("value", "any"),), runner)
