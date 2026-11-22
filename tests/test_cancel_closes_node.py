"""Cancellation still runs close on an in-flight lifecycle node."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import Node, Port, load, run


class _Life:
    """Lifecycle body that waits until the run is cancelled."""

    def __init__(self, started: asyncio.Event) -> None:
        """Store the event process sets before it waits."""
        self._started = started
        self.closed = False

    async def open(self) -> None:
        """No resources to prepare."""
        return None

    async def process(
        self, inputs: Mapping[str, object], side: Mapping[str, object]
    ) -> dict[str, object]:
        """Signal start and wait until cancellation."""
        del inputs, side
        self._started.set()
        await asyncio.Event().wait()
        return {"value": 1}

    async def close(self) -> None:
        """Record that close ran."""
        self.closed = True


def test_cancel_event_closes_an_in_flight_node() -> None:
    """Setting cancel stops the node and still runs close."""
    cancel = asyncio.Event()
    started = asyncio.Event()
    life = _Life(started)
    node = Node("a", "map", (), (Port("value", "any"),), life)

    async def scenario() -> None:
        """Cancel the run after the node has started."""
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
    life = _Life(started)
    node = Node("a", "map", (), (Port("value", "any"),), life)

    async def scenario() -> None:
        """Cancel the run task after the node has started."""
        task = asyncio.create_task(run(load((node,), ()), {}))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(asyncio.wait_for(scenario(), 2))
    assert life.closed is True
