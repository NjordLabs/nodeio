"""Queue waits, cancellation before start, and an empty subgraph."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import GraphError, Node, Port, load, register_subgraph, run
from nodeio.builtin import reset_subgraphs, subgraph_node
from nodeio.executors import _drive_close
from nodeio.flow import EdgeBuffer
from nodeio.run import _raise_from_group


def test_get_waits_until_a_value_arrives() -> None:
    """An empty edge queue waits instead of failing."""

    async def check() -> None:
        """Wait for a value that arrives on the next turn."""
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
        """Sleep, then drive close from the cancellation finalizer."""
        try:
            await asyncio.sleep(10)
        finally:
            await _drive_close(lambda: None)

    async def scenario() -> None:
        """Cancel the sleeping task and expect cancellation to resume."""
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


async def _nope(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    del inputs, side
    raise AssertionError("node should not start")
