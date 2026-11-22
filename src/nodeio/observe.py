"""Hooks fired by the scheduler. Bodies and headers are not included."""

from __future__ import annotations

from typing import Protocol

from mimirlog import get_logger

from nodeio.graph import GraphDefinition


class Observer(Protocol):
    """Notifications for one node attempt."""

    def on_start(self, definition: GraphDefinition, node_id: str) -> None:
        """A node is about to run.

        Args:
            definition: Graph being run.
            node_id: Node that is starting.
        """

    def on_success(self, definition: GraphDefinition, node_id: str) -> None:
        """A node returned outputs.

        Args:
            definition: Graph being run.
            node_id: Node that finished.
        """

    def on_exception(self, definition: GraphDefinition, node_id: str, exception: str) -> None:
        """A node raised.

        Args:
            definition: Graph being run.
            node_id: Node that failed.
            exception: Exception type name. The message is omitted.
        """


class LoggingObserver:
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
        self._logger = get_logger("nodeio")

    def on_start(self, definition: GraphDefinition, node_id: str) -> None:
        """Log that ``node_id`` is starting.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that is starting.
        """
        del definition
        self._logger.info({"event": "start", "graph": self._graph, "node": node_id})

    def on_success(self, definition: GraphDefinition, node_id: str) -> None:
        """Log that ``node_id`` succeeded.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that finished.
        """
        del definition
        self._logger.info({"event": "success", "graph": self._graph, "node": node_id})

    def on_exception(self, definition: GraphDefinition, node_id: str, exception: str) -> None:
        """Log the exception type for ``node_id``.

        Args:
            definition: Unused. Present so the scheduler can pass it.
            node_id: Node that failed.
            exception: Exception type name.
        """
        del definition
        self._logger.info({
            "event": "exception",
            "exception": exception,
            "graph": self._graph,
            "node": node_id,
        })
