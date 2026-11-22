"""Hooks fired by the scheduler. Bodies and headers are not included."""

from __future__ import annotations

import logging
from typing import Protocol, cast

from nodeio.boundary import Observer, named_logger, otel_tracer
from nodeio.graph import GraphDefinition

__all__ = ["LoggingObserver", "Observer", "TelemetryObserver"]

_AUTO = object()


class LoggingObserver(Observer):
    """Write one mimirlog event per notification.

    The event carries the node id and the outcome. It does not carry
    port values, request bodies, or headers.
    """

    def __init__(self, graph: str) -> None:
        """Store the graph name copied onto every event.

        Args:
            graph: Name included in the structured event.
        """
        self._graph = graph
        self._logger: logging.Logger = named_logger("nodeio")

    def on_start(self, definition: GraphDefinition, node_id: str, *, run_id: str) -> None:
        """Log that ``node_id`` is starting.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that is starting.
            run_id: Id of this run.
        """
        _ = definition
        self._logger.info({
            "event": "start",
            "graph": self._graph,
            "node": node_id,
            "run_id": run_id,
        })

    def on_success(
        self,
        definition: GraphDefinition,
        node_id: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """Log that ``node_id`` succeeded.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that finished.
            run_id: Id of this run.
            duration_s: Seconds the node spent inside the executor.
        """
        _ = definition
        self._logger.info({
            "duration_s": duration_s,
            "event": "success",
            "graph": self._graph,
            "node": node_id,
            "run_id": run_id,
        })

    def on_exception(
        self,
        definition: GraphDefinition,
        node_id: str,
        exception: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """Log the exception type for ``node_id``.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that failed.
            exception: Exception type name.
            run_id: Id of this run.
            duration_s: Seconds the node spent inside the executor.
        """
        _ = definition
        self._logger.info({
            "duration_s": duration_s,
            "event": "exception",
            "exception": exception,
            "graph": self._graph,
            "node": node_id,
            "run_id": run_id,
        })


class _Span(Protocol):
    """The span operations this package uses."""

    def set_attribute(self, key: str, value: str) -> None:
        """Attach one string attribute."""
        ...

    def end(self) -> None:
        """Finish the span."""
        ...


class _Tracer(Protocol):
    """Starts one span."""

    def start_span(self, name: str) -> _Span:
        """Start a span named ``name``."""
        ...


class TelemetryObserver(Observer):
    """Start one span per node when OpenTelemetry is installed.

    The observer does nothing when the API is missing and no tracer was
    injected. Importing :mod:`nodeio` does not import OpenTelemetry.
    """

    def __init__(self, graph: str, tracer: object | None = _AUTO) -> None:
        """Store the graph name and resolve a tracer.

        Args:
            graph: Name copied onto span attributes.
            tracer: Injected tracer. The default loads the global tracer
                when the API is installed and otherwise no-ops. ``None``
                always no-ops.
        """
        self._graph = graph
        self._tracer = _load_tracer(tracer)
        self._spans: dict[tuple[str, str], _Span] = {}

    def on_start(self, definition: GraphDefinition, node_id: str, *, run_id: str) -> None:
        """Start a span for ``node_id``.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that is starting.
            run_id: Id of this run.
        """
        _ = definition
        tracer = self._tracer
        if tracer is None:
            return
        span = tracer.start_span(f"{self._graph}/{node_id}")
        span.set_attribute("nodeio.node", node_id)
        span.set_attribute("nodeio.run_id", run_id)
        self._spans[(run_id, node_id)] = span

    def on_success(
        self,
        definition: GraphDefinition,
        node_id: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """End the span for a successful node.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that finished.
            run_id: Id of this run.
            duration_s: Unused. The span's own clock covers the interval.
        """
        _ = (definition, duration_s)
        self._finish(run_id, node_id, None)

    def on_exception(
        self,
        definition: GraphDefinition,
        node_id: str,
        exception: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """End the span and record the exception type.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that failed.
            exception: Exception type name.
            run_id: Id of this run.
            duration_s: Unused. The span's own clock covers the interval.
        """
        _ = (definition, duration_s)
        self._finish(run_id, node_id, exception)

    def _finish(self, run_id: str, node_id: str, error: str | None) -> None:
        span = self._spans.pop((run_id, node_id), None)
        if span is None:
            return
        if error is not None:
            span.set_attribute("nodeio.exception", error)
        span.end()


def _load_tracer(tracer: object | None) -> _Tracer | None:
    if tracer is None:
        return None
    if tracer is not _AUTO:
        return cast(_Tracer, tracer)
    loaded = otel_tracer()
    if loaded is None:
        return None
    return cast(_Tracer, loaded)
