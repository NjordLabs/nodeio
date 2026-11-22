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
        def __init__(self) -> None:
            self.closed = False
            self._inner = LocalExecutor()

        async def run(
            self,
            node: Node,
            inputs: Mapping[str, object],
            side: Mapping[str, object],
        ) -> Mapping[str, object]:
            return await self._inner.run(node, inputs, side)

        def close(self) -> None:
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
        def on_start(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("observer")

        def on_success(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("observer")

        def on_exception(self, *_args: object, **_kwargs: object) -> None:
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
        def on_start(self, *_args: object, **_kwargs: object) -> None:
            return None

        def on_success(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("observer")

        def on_exception(self, *_args: object, **_kwargs: object) -> None:
            return None

    def broken() -> object:
        raise RuntimeError("no logger")

    monkeypatch.setattr(sys.modules["nodeio.run"], "_logger", broken)
    assert asyncio.run(run(load((node,), ()), {}, observer=Boom()))["a"]["value"] == 1

    class Loud:
        def exception(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("log failed")

    monkeypatch.setattr(sys.modules["nodeio.run"], "_logger", lambda: Loud())
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
