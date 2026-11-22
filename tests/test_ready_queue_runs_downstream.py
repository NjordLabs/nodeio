"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping

import pytest

from nodeio import (
    Edge,
    GraphError,
    LoggingObserver,
    Node,
    Port,
    load,
    run,
)
from tests.support import async_node


def test_ready_queue_runs_downstream() -> None:
    """Run this step."""

    async def add(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run this step."""
        del side
        value = inputs["value"]
        if not isinstance(value, int):
            raise TypeError("value must be an int")
        return {"value": value + 1}

    source = Node("a", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    sink = Node("b", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    definition = load(
        (source, sink),
        (Edge("a", "value", "b", "value"),),
        entries=(("a", "value"),),
    )
    assert '"a" -> "b"' in definition.to_dot()
    assert '"a" --> "b"' in definition.to_mermaid()
    outputs = asyncio.run(run(definition, {"a": {"value": 1}}))
    assert outputs["b"]["value"] == 3
    rejected = load((source,), (), entries=(("a", "value"),))
    with pytest.raises(GraphError, match="node a failed"):
        asyncio.run(run(rejected, {"a": {"value": "x"}}))


def test_missing_seed() -> None:
    """Run this step."""
    node = async_node("a")
    definition = load((node,), (), entries=(("a", "value"),))
    with pytest.raises(GraphError, match="missing inputs"):
        asyncio.run(run(definition, {}))


def test_observer_omits_values(capsys: pytest.CaptureFixture[str]) -> None:
    """Run this step."""
    from mimirlog import configure

    configure()
    node = async_node("a")
    definition = load((node,), (), entries=(("a", "value"),))
    asyncio.run(run(definition, {"a": {"value": "secret"}}, observer=LoggingObserver("demo")))
    captured = capsys.readouterr()
    text = captured.err + captured.out
    assert "secret" not in text
    payload = json.loads(text.strip().splitlines()[-1])
    assert payload["message"]["node"] == "a"
    assert payload["message"]["event"] == "success"
    assert payload["message"]["run_id"]
    assert payload["message"]["duration_s"] >= 0
