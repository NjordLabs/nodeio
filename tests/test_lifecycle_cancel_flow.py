"""Lifecycle, cancellation, backpressure, and output callbacks."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import Edge, GraphError, Node, Port, load, run
from nodeio.flow import EdgeBuffer


def test_lifecycle_opens_once_and_closes_after_failure() -> None:
    """open runs before process, and close runs when process fails."""

    class Life:
        def __init__(self, fail: bool) -> None:
            self.fail = fail
            self.events: list[str] = []

        async def open(self) -> None:
            self.events.append("open")

        async def process(
            self, inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            del inputs, side
            self.events.append("process")
            if self.fail:
                raise RuntimeError("process")
            return {"value": 1}

        def close(self) -> None:
            self.events.append("close")

    life = Life(False)
    node = Node("a", "map", (), (Port("value", "any"),), life)
    outputs = asyncio.run(run(load((node,), ()), {}))
    assert outputs["a"]["value"] == 1
    assert life.events == ["open", "process", "close"]
    again = asyncio.run(run(load((node,), ()), {}))
    assert again["a"]["value"] == 1
    assert life.events == ["open", "process", "close", "open", "process", "close"]
    failing = Life(True)
    bad = Node("b", "map", (), (Port("value", "any"),), failing)
    with pytest.raises(GraphError, match="node b failed"):
        asyncio.run(run(load((bad,), ()), {}))
    assert failing.events == ["open", "process", "close"]


def test_cancel_event_closes_an_in_flight_node() -> None:
    """Setting cancel stops the node and still runs close."""
    cancel = asyncio.Event()
    started = asyncio.Event()

    class Life:
        def __init__(self) -> None:
            self.closed = False

        async def open(self) -> None:
            return None

        async def process(
            self, inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            del inputs, side
            started.set()
            await asyncio.Event().wait()
            return {"value": 1}

        async def close(self) -> None:
            self.closed = True

    life = Life()
    node = Node("a", "map", (), (Port("value", "any"),), life)

    async def scenario() -> None:
        task = asyncio.create_task(run(load((node,), ()), {}, cancel=cancel))
        await started.wait()
        cancel.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(asyncio.wait_for(scenario(), 2))
    assert life.closed is True


def test_task_cancellation_closes_an_in_flight_node() -> None:
    """Cancelling the run task cancels the node and still runs close."""
    started = asyncio.Event()

    class Life:
        def __init__(self) -> None:
            self.closed = False

        async def open(self) -> None:
            return None

        async def process(
            self, inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            del inputs, side
            started.set()
            await asyncio.Event().wait()
            return {"value": 1}

        async def close(self) -> None:
            self.closed = True

    life = Life()
    node = Node("a", "map", (), (Port("value", "any"),), life)

    async def scenario() -> None:
        task = asyncio.create_task(run(load((node,), ()), {}))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(asyncio.wait_for(scenario(), 2))
    assert life.closed is True


def test_bound_of_one_does_not_drop_or_grow() -> None:
    """A full edge waits and a graph bound of 1 still delivers the value."""

    async def check_buffer() -> None:
        buffer = EdgeBuffer()
        await buffer.put(("b", "value"), 1, 1)
        started = asyncio.Event()

        async def second() -> None:
            started.set()
            await buffer.put(("b", "value"), 2, 1)

        task = asyncio.create_task(second())
        await started.wait()
        await asyncio.sleep(0)
        assert buffer.high_water == 1
        assert not task.done()
        assert await buffer.get(("b", "value")) == 1
        await task
        assert await buffer.get(("b", "value")) == 2
        assert buffer.high_water == 1

    asyncio.run(check_buffer())

    async def add(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        value = inputs["value"]
        if not isinstance(value, int):
            raise TypeError("value must be an int")
        return {"value": value + 1}

    source = Node("a", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    sink = Node("b", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    definition = load(
        (source, sink),
        (Edge("a", "value", "b", "value", queue_size=1),),
        entries=(("a", "value"),),
    )
    outputs = asyncio.run(run(definition, {"a": {"value": 1}}, queue_size=8))
    assert outputs["b"]["value"] == 3
    with pytest.raises(GraphError, match="queue_size"):
        asyncio.run(run(definition, {"a": {"value": 1}}, queue_size=0))
    with pytest.raises(GraphError, match="queue_size"):
        load((source, sink), (Edge("a", "value", "b", "value", queue_size=0),))


def test_output_callback_sees_each_published_value() -> None:
    """The coordinator invokes the callback for sync and async listeners."""
    seen: list[tuple[str, str, object]] = []

    def sync(node_id: str, port: str, value: object) -> None:
        seen.append((node_id, port, value))

    async def async_cb(node_id: str, port: str, value: object) -> None:
        seen.append((node_id, port, value))

    async def add(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        value = inputs["value"]
        if not isinstance(value, int):
            raise TypeError("value must be an int")
        return {"value": value + 1}

    source = Node("a", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    definition = load((source,), (), entries=(("a", "value"),))
    asyncio.run(run(definition, {"a": {"value": 1}}, on_output=sync))
    asyncio.run(run(definition, {"a": {"value": 1}}, on_output=async_cb))
    assert seen == [("a", "value", 2), ("a", "value", 2)]
