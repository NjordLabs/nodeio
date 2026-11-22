"""Outbound calls whose libraries mypy treats as untyped.

A sibling ``boundary.pyi`` is what type checkers read. This module keeps
``httpx`` and the logger lookup out of the Any-expression count.
"""

from __future__ import annotations

import logging
from typing import Protocol

from nodeio.graph import GraphDefinition


def named_logger(name: str) -> logging.Logger:
    """Return the process logger for ``name``.

    Args:
        name: Logger name.

    Returns:
        The standard-library logger.
    """
    from mimirlog import get_logger

    return get_logger(name)


async def http_exchange(
    method: str,
    url: str,
    timeout: float,
    verify: bool | str,
    cert: tuple[str, str] | None,
) -> tuple[int, bytes]:
    """Perform one HTTP request with an owned client.

    Args:
        method: HTTP method.
        url: Absolute URL.
        timeout: Seconds for the request.
        verify: TLS verification flag or a CA bundle path.
        cert: Certificate and key paths, or ``None``.

    Returns:
        The status code and the response body.
    """
    import httpx

    async with httpx.AsyncClient(timeout=timeout, verify=verify, cert=cert) as owned:
        response = await owned.request(method, url)
        status = response.status_code
        body = response.content
    return int(status), bytes(body)


class Observer(Protocol):
    """Notifications for one node attempt."""

    def on_start(
        self, definition: GraphDefinition, node_id: str, *, run_id: str
    ) -> None:
        """A node is about to run.

        Args:
            definition: Graph being run.
            node_id: Node that is starting.
            run_id: Id of this run.
        """
        return

    def on_success(
        self,
        definition: GraphDefinition,
        node_id: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """A node returned outputs.

        Args:
            definition: Graph being run.
            node_id: Node that finished.
            run_id: Id of this run.
            duration_s: Seconds inside the executor.
        """
        return

    def on_exception(
        self,
        definition: GraphDefinition,
        node_id: str,
        exception: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None:
        """A node raised.

        Args:
            definition: Graph being run.
            node_id: Node that failed.
            exception: Exception type name.
            run_id: Id of this run.
            duration_s: Seconds inside the executor.
        """
        return
