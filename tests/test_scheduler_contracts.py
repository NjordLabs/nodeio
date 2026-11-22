"""Scheduler ownership, failure policy, and observer isolation."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Mapping

import pytest

from nodeio import Edge, GraphError, Node, Port, load, run
from nodeio.executors import LocalExecutor


def test_run_closes_an_executor_it_created(monkeypatch: pytest.MonkeyPatch) -> None:
    """An executor created by run is closed, including after success."""
    closed: list[bool] = []
    original = LocalExecutor.close

    def spy(self: LocalExecutor) -> None:
        closed.append(True)
        original(self)

    monkeypatch.setattr(LocalExecutor, "close", spy)
    node = Node("a", "map", (), (Port("value", "any"),), _value(1))
    asyncio.run(run(load((node,), ()), {}))
    assert closed == [True]


def test_caller_executor_stays_open() -> None:
    """A caller-supplied executor is not closed by run."""

    class Supplied:
        """Executor the caller owns, so run must leave it open."""

        def __init__(self) -> None:
            """Wrap a local executor and record close."""
            self.closed = False
            self._inner = LocalExecutor()

        async def run(
            self,
            node: Node,
            inputs: Mapping[str, object],
            side: Mapping[str, object],
        ) -> Mapping[str, object]:
            """Forward the node to the wrapped executor."""
            return await self._inner.run(node, inputs, side)

        def close(self) -> None:
            """Close the wrapped executor once."""
            self.closed = True
            self._inner.close()

    supplied = Supplied()
    node = Node("a", "map", (), (Port("value", "any"),), _value(1))
    asyncio.run(run(load((node,), ()), {}, executor=supplied))  # type: ignore[arg-type]
    assert supplied.closed is False
    supplied.close()


def test_fail_fast_cancels_a_running_sibling() -> None:
    """A failure cancels another node already running in the wave."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Wait until the sibling failure cancels this node."""
        del inputs, side
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return {"value": 1}

    slow_node = Node("slow", "map", (), (Port("value", "any"),), slow)
    bad = Node("bad", "map", (), (Port("value", "any"),), _boom)
    definition = load((slow_node, bad), ())

    async def scenario() -> None:
        """Run the pair and expect the failure to cancel the sibling."""
        with pytest.raises(GraphError, match="node bad failed"):
            await run(definition, {}, limit=2)

    asyncio.run(asyncio.wait_for(scenario(), 2))
    assert cancelled.is_set()


def test_unknown_policy_is_rejected() -> None:
    """policy only accepts fail and collect."""
    node = Node("a", "map", (), (Port("value", "any"),), _value(1))
    with pytest.raises(GraphError, match="policy"):
        asyncio.run(run(load((node,), ()), {}, policy="ignore"))


def test_collect_skips_dependents_and_keeps_other_outputs() -> None:
    """A collected failure does not raise a second stalled-node error."""
    failed = Node("bad", "map", (), (Port("value", "any"),), _boom)
    child = Node("child", "map", (Port("value", "any"),), (Port("value", "any"),), _echo)
    other = Node("ok", "map", (), (Port("value", "any"),), _value(7))
    definition = load(
        (failed, child, other),
        (Edge("bad", "value", "child", "value"),),
    )
    outputs = asyncio.run(run(definition, {}, policy="collect"))
    assert "bad" not in outputs
    assert "child" not in outputs
    assert outputs["ok"]["value"] == 7


def test_observer_errors_do_not_replace_the_outcome() -> None:
    """An observer failure is logged and does not hide the node result."""

    class Boom:
        """Observer whose every hook raises."""

        def on_start(self, *_args: object, **_kwargs: object) -> None:
            """Raise so the guard must swallow the start hook."""
            raise RuntimeError("observer")

        def on_success(self, *_args: object, **_kwargs: object) -> None:
            """Raise so the guard must swallow the success hook."""
            raise RuntimeError("observer")

        def on_exception(self, *_args: object, **_kwargs: object) -> None:
            """Raise so the guard must swallow the exception hook."""
            raise RuntimeError("observer")

    node = Node("a", "map", (), (Port("value", "any"),), _value(4))
    outputs = asyncio.run(run(load((node,), ()), {}, observer=Boom()))
    assert outputs["a"]["value"] == 4
    failed = Node("bad", "map", (), (Port("value", "any"),), _boom)
    with pytest.raises(GraphError, match="node bad failed: RuntimeError") as caught:
        asyncio.run(run(load((failed,), ()), {}, observer=Boom()))
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert "observer" not in str(caught.value.__cause__)


def test_logging_an_observer_failure_can_itself_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """A broken logger does not escape the observer guard."""
    node = Node("a", "map", (), (Port("value", "any"),), _value(1))

    class Boom:
        """Observer that fails only while reporting success."""

        def on_start(self, *_args: object, **_kwargs: object) -> None:
            """Allow the node to start."""
            return None

        def on_success(self, *_args: object, **_kwargs: object) -> None:
            """Force the observer guard to log a failure."""
            raise RuntimeError("observer")

        def on_exception(self, *_args: object, **_kwargs: object) -> None:
            """Ignore exception notices."""
            return None

    def broken() -> object:
        """Raise before a logger can be obtained."""
        raise RuntimeError("no logger")

    monkeypatch.setattr(sys.modules["nodeio.notify"], "_logger", broken)
    assert asyncio.run(run(load((node,), ()), {}, observer=Boom()))["a"]["value"] == 1

    class Loud:
        """Logger whose exception method fails."""

        def exception(self, *_args: object, **_kwargs: object) -> None:
            """Raise instead of writing the log record."""
            raise RuntimeError("log failed")

    def loud() -> Loud:
        """Return a logger that fails while recording the observer error."""
        return Loud()

    monkeypatch.setattr(sys.modules["nodeio.notify"], "_logger", loud)
    assert asyncio.run(run(load((node,), ()), {}, observer=Boom()))["a"]["value"] == 1


def _value(number: int):
    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        return {"value": number}

    return runner


async def _echo(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    del side
    return {"value": inputs["value"]}


async def _boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    del inputs, side
    raise RuntimeError("nope")
