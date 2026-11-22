"""Hooks fired by the scheduler. Bodies and headers are not included."""

from __future__ import annotations

import logging

from nodeio.boundary import Observer as Observer
from nodeio.boundary import named_logger
from nodeio.graph import GraphDefinition


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
