"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import (
    GraphError,
    Node,
    Port,
    demux,
    gate,
    load,
    mux,
    register_subgraph,
    run,
    subgraph_node,
)
from nodeio.builtin import linear, reset_subgraphs
from nodeio.graph import GraphDefinition
from tests.support import identity


def test_gate_mux_demux_and_subgraph() -> None:
    """Run this step."""
    reset_subgraphs()
    child_in = Node(
        "in",
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        identity(),
    )
    child_out = Node(
        "out",
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        identity(),
    )
    register_subgraph("child", linear(child_in, child_out))
    parent = load(
        (subgraph_node("sub", "child"),),
        (),
        entries=(("sub", "value"),),
    )
    assert asyncio.run(run(parent, {"sub": {"value": 5}}))["sub"]["value"] == 5
    opened = load((gate("g"),), (), entries=(("g", "open"), ("g", "value")))
    closed = asyncio.run(run(opened, {"g": {"open": False, "value": 1}}))
    assert closed["g"]["value"] is None
    chosen = load((mux("m"),), (), entries=(("m", "select"), ("m", "left"), ("m", "right")))
    muxed = asyncio.run(run(chosen, {"m": {"select": "left", "left": 1, "right": 2}}))
    assert muxed["m"]["value"] == 1
    split = load((demux("d"),), (), entries=(("d", "key"), ("d", "value")))
    routed = asyncio.run(run(split, {"d": {"key": "right", "value": 9}}))
    assert routed["d"] == {"left": None, "right": 9}
    with pytest.raises(GraphError, match="node d failed"):
        asyncio.run(run(split, {"d": {"key": "middle", "value": 9}}))
    with pytest.raises(ValueError):
        register_subgraph("", GraphDefinition((), ()))
