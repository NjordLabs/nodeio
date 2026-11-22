"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import (
    Edge,
    GraphError,
    Node,
    Port,
    create,
    load,
    register,
    run,
)


def test_type_mismatch_and_unknown_node() -> None:
    """Run this step."""
    left = Node("a", "map", (), (Port("value", "int"),))
    right = Node("b", "map", (Port("value", "str"),), ())
    with pytest.raises(GraphError, match="type mismatch"):
        load((left, right), (Edge("a", "value", "b", "value"),))
    with pytest.raises(GraphError, match="unknown"):
        load((left,), (Edge("a", "value", "missing", "value"),))


def test_factory_mixes_kinds() -> None:
    """Run this step."""

    def make(node_id: str, scale: int = 1) -> Node:
        """Run this step."""

        async def runner(
            inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            """Run this step."""
            del side
            value = inputs["value"]
            if not isinstance(value, int):
                raise TypeError("value must be an int")
            return {"value": value * scale}

        return Node(node_id, "scale", (Port("value", "int"),), (Port("value", "int"),), runner)

    register("scale", make)
    built = create("scale", "n", scale=3)
    other = Node("src", "const", (), (Port("value", "int"),))

    async def source(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run this step."""
        del inputs, side
        return {"value": 2}

    source_node = Node("src", "const", (), (Port("value", "int"),), source)
    assert other.id == source_node.id
    definition = load(
        (source_node, built),
        (Edge("src", "value", "n", "value"),),
    )
    outputs = asyncio.run(run(definition, {}))
    assert outputs["n"]["value"] == 6
    lone = load((built,), (), entries=(("n", "value"),))
    with pytest.raises(GraphError, match="node n failed"):
        asyncio.run(run(lone, {"n": {"value": "x"}}))
