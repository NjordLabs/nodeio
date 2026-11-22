"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import pytest

from nodeio import (
    Edge,
    GraphError,
    Node,
    Port,
    load,
)


def test_cycle_is_named() -> None:
    """Run this step."""
    left = Node("a", "map", (Port("value", "any"),), (Port("value", "any"),))
    right = Node("b", "map", (Port("value", "any"),), (Port("value", "any"),))
    edges = (
        Edge("a", "value", "b", "value"),
        Edge("b", "value", "a", "value"),
    )
    with pytest.raises(GraphError) as caught:
        load((left, right), edges)
    assert caught.value.cycle[0] == caught.value.cycle[-1]
    assert "a" in caught.value.cycle and "b" in caught.value.cycle


def test_definitions_do_not_share_side_packets() -> None:
    """Run this step."""
    side = {"token": "secret"}
    node = Node("a", "map", (), (Port("value", "any"),))
    first = load((node,), (), side=side)
    side["token"] = "changed"
    second = load((node,), (), side={"token": "other"})
    assert first.side[0][1] == "secret"
    assert second.side != first.side
    assert first.nodes is not second.nodes


def test_duplicate_id_and_unconnected_input() -> None:
    """Run this step."""
    node = Node("a", "map", (Port("value", "any"),), ())
    with pytest.raises(GraphError, match="duplicate"):
        load((node, Node("a", "map")), ())
    with pytest.raises(GraphError, match="unconnected"):
        load((node,), ())
