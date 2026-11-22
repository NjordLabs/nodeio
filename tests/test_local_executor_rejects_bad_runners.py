"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import (
    GraphError,
    LoggingObserver,
    Node,
    Port,
    load,
    run,
)
from tests.support import boom, number
from tests.workers import double


def test_local_executor_rejects_bad_runners() -> None:
    """Run this step."""
    from nodeio.executors import LocalExecutor

    async def check() -> None:
        """Run this step."""
        executor = LocalExecutor()
        produced = await executor.run(
            Node(
                "p",
                "map",
                (Port("value", "any"),),
                (Port("value", "any"),),
                double,
                "process",
            ),
            {"value": 2},
            {},
        )
        assert produced["value"] == 4
        executor.close()
        executor.close()
        with pytest.raises(ValueError, match="no runner"):
            await executor.run(Node("bare", "map"), {}, {})
        with pytest.raises(TypeError):
            await executor.run(Node("bad", "map", runner=number), {}, {})
        with pytest.raises(ValueError, match="unknown executor"):
            await executor.run(
                Node("odd", "map", runner=number, executor="nope"),
                {},
                {},
            )

    asyncio.run(check())


def test_observer_records_an_exception(capsys: pytest.CaptureFixture[str]) -> None:
    """Run this step."""
    del capsys
    from mimirlog import configure

    configure()
    failing = Node("bad", "map", (), (Port("value", "any"),), boom)
    definition = load((failing,), ())
    with pytest.raises(GraphError, match="node bad failed"):
        asyncio.run(run(definition, {}, observer=LoggingObserver("demo")))
