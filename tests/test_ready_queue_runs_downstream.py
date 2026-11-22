"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
import json

import pytest

from nodeio import (
    GraphError,
    LoggingObserver,
    load,
    run,
)
from tests.support import async_node, increment_graph


def test_ready_queue_runs_downstream() -> None:
    """Run this step."""
    source, _sink, definition = increment_graph()
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
