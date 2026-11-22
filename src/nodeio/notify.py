"""Observer notices and output publication for one run."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from nodeio.errors import GraphError
from nodeio.graph import GraphDefinition, Node
from nodeio.observe import Observer

if TYPE_CHECKING:
    from nodeio.run import _State


def _notify_start(
    observer: Observer | None,
    definition: GraphDefinition,
    node_id: str,
    run_id: str,
) -> None:
    if observer is None:
        return
    _guard_observer(lambda: observer.on_start(definition, node_id, run_id=run_id))


def _notify_success(
    observer: Observer | None,
    definition: GraphDefinition,
    node_id: str,
    run_id: str,
    duration_s: float,
) -> None:
    if observer is None:
        return
    _guard_observer(
        lambda: observer.on_success(
            definition,
            node_id,
            run_id=run_id,
            duration_s=duration_s,
        )
    )


async def _execute(
    node: Node,
    inputs: Mapping[str, object],
    state: _State,
) -> tuple[Mapping[str, object], float] | None:
    async with state.semaphore:
        started = time.perf_counter()
        try:
            produced = await asyncio.wait_for(
                state.executor.run(node, inputs, state.side),
                state.timeout,
            )
        except Exception as exc:
            _notify_exception(state, node.id, started, exc)
            if state.policy == "fail":
                name = type(exc).__name__
                raise GraphError(f"node {node.id} failed: {name}") from exc
            return None
        return produced, time.perf_counter() - started


def _notify_exception(state: _State, node_id: str, started: float, exc: Exception) -> None:
    observer = state.observer
    if observer is None:
        return
    duration_s = time.perf_counter() - started
    name = type(exc).__name__
    _guard_observer(
        lambda: observer.on_exception(
            state.definition,
            node_id,
            name,
            run_id=state.run_id,
            duration_s=duration_s,
        )
    )


def _guard_observer(callback: Callable[[], None]) -> None:
    try:
        callback()
    except Exception:
        _log_observer_failure()


def _logger() -> logging.Logger:
    return logging.getLogger("nodeio")


def _log_observer_failure() -> None:
    try:
        logger = _logger()
    except Exception:
        return
    try:
        logger.exception("observer failed")
    except Exception:
        return
