"""Shared node builders for the engine tests."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

from nodeio import Node, Port


def async_node(node_id: str, kind: str = "map") -> Node:
    """Build a node that echoes its value and the side packet."""

    async def runner(
        inputs: Mapping[str, object], side: Mapping[str, object]
    ) -> dict[str, object]:
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


async def boom(
    inputs: Mapping[str, object], side: Mapping[str, object]
) -> dict[str, object]:
    """Raise so the scheduler records a failure."""
    del inputs, side
    raise RuntimeError("nope")


def identity() -> Callable[..., object]:
    """Build a runner that copies ``value``."""

    async def runner(
        inputs: Mapping[str, object], side: Mapping[str, object]
    ) -> dict[str, object]:
        """Copy the input value."""
        del side
        return {"value": inputs["value"]}

    return runner
