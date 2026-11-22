"""Edge bounds wait for capacity and output callbacks see each value."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import Edge, GraphError, load, run
from nodeio.flow import EdgeBuffer
from tests.support import increment, increment_graph, int_node


def test_bound_of_one_does_not_drop_or_grow() -> None:
    """A full edge waits and a graph bound of 1 still delivers the value."""

    async def check_buffer() -> None:
        """Fill a one-slot buffer and wait until a reader frees it."""
        buffer = EdgeBuffer()
        await buffer.put(("b", "value"), 1, 1)
        started = asyncio.Event()

        async def second() -> None:
            """Publish a second value once the first slot is free."""
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
    _source, sink, definition = increment_graph(queue_size=1)
    outputs = asyncio.run(run(definition, {"a": {"value": 1}}, queue_size=8))
    assert outputs["b"]["value"] == 3
    with pytest.raises(GraphError, match="queue_size"):
        asyncio.run(run(definition, {"a": {"value": 1}}, queue_size=0))
    with pytest.raises(GraphError, match="queue_size"):
        load((int_node("a"), sink), (Edge("a", "value", "b", "value", queue_size=0),))


def test_output_callback_sees_each_published_value() -> None:
    """The coordinator invokes the callback for sync and async listeners."""
    seen: list[tuple[str, str, object]] = []

    def sync(node_id: str, port: str, value: object) -> None:
        """Record one published value from a synchronous callback."""
        seen.append((node_id, port, value))

    async def async_cb(node_id: str, port: str, value: object) -> None:
        """Record one published value from an asynchronous callback."""
        seen.append((node_id, port, value))

    definition = load((int_node("a"),), (), entries=(("a", "value"),))
    asyncio.run(run(definition, {"a": {"value": 1}}, on_output=sync))
    asyncio.run(run(definition, {"a": {"value": 1}}, on_output=async_cb))
    assert seen == [("a", "value", 2), ("a", "value", 2)]


def test_increment_rejects_a_non_int() -> None:
    """The shared increment runner rejects a non-integer value."""
    with pytest.raises(TypeError, match="int"):
        asyncio.run(increment({"value": "x"}, {}))
