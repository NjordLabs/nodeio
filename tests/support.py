"""Shared node builders for the engine tests."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Mapping
from types import ModuleType, SimpleNamespace

from nodeio import Edge, GraphDefinition, Node, Port, load


def async_node(node_id: str, kind: str = "map") -> Node:
    """Build a node that echoes its value and the side packet."""

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Echo the input and the side packet."""
        await asyncio.sleep(0)
        seen = side.get("seen")
        return {"value": inputs.get("value"), "seen": seen}

    return Node(
        node_id,
        kind,
        (Port("value", "any"),),
        (Port("value", "any"), Port("seen", "any")),
        runner,
    )


async def number(inputs: Mapping[str, object], side: Mapping[str, object]) -> int:
    """Return a value the executor rejects."""
    del inputs, side
    return 1


async def boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Raise so the scheduler records a failure."""
    del inputs, side
    raise RuntimeError("nope")


def identity() -> Callable[..., object]:
    """Build a runner that copies ``value``."""

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Copy the input value."""
        del side
        return {"value": inputs["value"]}

    return runner


async def increment(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Add one to an integer ``value`` input."""
    del side
    value = inputs["value"]
    if not isinstance(value, int):
        raise TypeError("value must be an int")
    return {"value": value + 1}


def int_node(node_id: str) -> Node:
    """Build an integer map node that increments its value."""
    return Node(
        node_id,
        "map",
        (Port("value", "int"),),
        (Port("value", "int"),),
        increment,
    )


def increment_graph(*, queue_size: int | None = None) -> tuple[Node, Node, GraphDefinition]:
    """Load increment nodes ``a`` and ``b`` joined on ``value``."""
    source = int_node("a")
    sink = int_node("b")
    if queue_size is None:
        edge = Edge("a", "value", "b", "value")
    else:
        edge = Edge("a", "value", "b", "value", queue_size=queue_size)
    return source, sink, load((source, sink), (edge,), entries=(("a", "value"),))


def process_node(node_id: str, runner: Callable[..., object]) -> Node:
    """Build a process node with any-typed value ports."""
    return Node(
        node_id,
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        runner,
        "process",
    )


class RecordingClient:
    """Injected HTTP client that returns one status and body."""

    def __init__(self, status: int, body: bytes) -> None:
        """Store the status and body returned by :meth:`request`."""
        self.calls = 0
        self._status = status
        self._body = body

    async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
        """Count the call and return the prepared response."""
        del method, url, timeout
        self.calls += 1
        return SimpleNamespace(status_code=self._status, content=self._body)


def restore_modules(saved: Mapping[str, ModuleType | None]) -> None:
    """Put previously imported modules back and drop ones the test added."""
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module
