"""Lifecycle open, process, and close around a node failure."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import GraphError, Node, Port, load, run


def test_lifecycle_opens_once_and_closes_after_failure() -> None:
    """open runs before process, and close runs when process fails."""

    class Life:
        """Record open, process, and close for one node body."""

        def __init__(self, fail: bool) -> None:
            """Store whether process should raise."""
            self.fail = fail
            self.events: list[str] = []

        async def open(self) -> None:
            """Record that the node was opened."""
            self.events.append("open")

        async def process(
            self, inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            """Record the call and optionally raise."""
            del inputs, side
            self.events.append("process")
            if self.fail:
                raise RuntimeError("process")
            return {"value": 1}

        def close(self) -> None:
            """Record that the node was closed."""
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
