"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import (
    GraphError,
    Node,
    Port,
    load,
    run,
)
from tests.support import async_node


def test_none_is_a_value_and_runs_are_isolated() -> None:
    """Run this step."""
    node = async_node("a")
    definition = load((node,), (), entries=(("a", "value"),), side={"seen": "from-side"})

    async def both() -> tuple[object, object]:
        """Run this step."""
        left, right = await asyncio.gather(
            run(definition, {"a": {"value": None}}),
            run(definition, {"a": {"value": 1}}),
        )
        return left["a"]["value"], right["a"]["value"]

    assert asyncio.run(both()) == (None, 1)
    assert asyncio.run(run(definition, {"a": {"value": None}}))["a"]["seen"] == "from-side"


def test_failure_names_the_node_and_collect_continues() -> None:
    """Run this step."""

    async def boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run this step."""
        del inputs, side
        raise RuntimeError("secret body")

    node = Node("bad", "map", (), (Port("value", "any"),), boom)
    definition = load((node,), ())
    with pytest.raises(GraphError, match="node bad failed: RuntimeError"):
        asyncio.run(run(definition, {}))
    collected = asyncio.run(run(definition, {}, policy="collect"))
    assert "bad" not in collected


def test_timeout_and_limit() -> None:
    """Run this step."""

    async def slow(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run this step."""
        del inputs, side
        await asyncio.sleep(1)
        return {"value": 1}

    node = Node("slow", "map", (), (Port("value", "any"),), slow)
    definition = load((node,), ())
    with pytest.raises(GraphError, match="slow"):
        asyncio.run(run(definition, {}, timeout=0.01))
    with pytest.raises(GraphError, match="limit"):
        asyncio.run(run(definition, {}, limit=0))
