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
    demux,
    gate,
    load,
    mux,
    run,
)


def test_open_gate_mux_right_and_demux_left() -> None:
    """Run this step."""
    opened = load((gate("g"),), (), entries=(("g", "open"), ("g", "value")))
    assert asyncio.run(run(opened, {"g": {"open": True, "value": 3}}))["g"]["value"] == 3
    chosen = load((mux("m"),), (), entries=(("m", "select"), ("m", "left"), ("m", "right")))
    muxed = asyncio.run(run(chosen, {"m": {"select": "right", "left": 1, "right": 2}}))
    assert muxed["m"]["value"] == 2
    split = load((demux("d"),), (), entries=(("d", "key"), ("d", "value")))
    routed = asyncio.run(run(split, {"d": {"key": "left", "value": 4}}))
    assert routed["d"]["left"] == 4


def test_unknown_ports_and_a_stalled_edge() -> None:
    """Run this step."""
    left = Node("a", "map", (), (Port("value", "int"),))
    right = Node("b", "map", (Port("value", "int"),), ())
    with pytest.raises(GraphError, match="no output port"):
        load((left, right), (Edge("a", "missing", "b", "value"),))
    with pytest.raises(GraphError, match="unknown node"):
        load((left,), (), entries=(("missing", "value"),))
    with pytest.raises(GraphError, match="no input port"):
        load((left,), (), entries=(("a", "value"),))
    other = Node("c", "map", (), (Port("value", "int"),))
    load(
        (left, other, right),
        (Edge("a", "value", "b", "value"), Edge("c", "value", "b", "value")),
    )

    async def empty(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run this step."""
        del inputs, side
        return {}

    source = Node("a", "map", (), (Port("value", "int"),), empty)
    sink = Node("b", "map", (Port("value", "int"),), ())
    stalled = load((source, sink), (Edge("a", "value", "b", "value"),))
    with pytest.raises(GraphError, match="still waiting on b"):
        asyncio.run(run(stalled, {}))
