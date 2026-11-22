"""Outbound calls whose libraries mypy treats as untyped.

A sibling ``boundary.pyi`` is what type checkers read. This module keeps
``httpx`` and the logger lookup out of the Any-expression count.
"""

from __future__ import annotations

import logging
from typing import Protocol

from nodeio.errors import GraphError
from nodeio.graph import GraphDefinition


def otel_tracer() -> object | None:
    """Return the OpenTelemetry tracer, or ``None`` when it is absent.

    Returns:
        The ``nodeio`` tracer, or ``None`` when the API is not installed.
    """
    try:
        from opentelemetry import trace
    except ImportError:
        return None
    get_tracer = getattr(trace, "get_tracer", None)
    if not callable(get_tracer):
        return None
    return get_tracer("nodeio")


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
    max_bytes: int,
) -> tuple[int, bytes]:
    """Perform one HTTP request with an owned client.

    Redirects are not followed. The body is read in chunks and rejected
    once it grows past ``max_bytes``, before the full payload is kept.

    Args:
        method: HTTP method.
        url: Absolute URL.
        timeout: Seconds for the request.
        verify: TLS verification flag or a CA bundle path.
        cert: Certificate and key paths, or ``None``.
        max_bytes: Maximum response body size.

    Returns:
        The status code and the response body.
    """
    import httpx

    async with (
        httpx.AsyncClient(
            timeout=timeout,
            verify=verify,
            cert=cert,
            follow_redirects=False,
        ) as owned,
        owned.stream(method, url) as response,
    ):
        status = int(response.status_code)
        body = await _read_limited(response, max_bytes)
    return status, body


async def _read_limited(response: object, max_bytes: int) -> bytes:
    read = getattr(response, "aiter_bytes", None)
    if not callable(read):
        raise GraphError("response cannot be streamed")
    chunks: list[bytes] = []
    total = 0
    async for chunk in read():
        piece = bytes(chunk)
        total += len(piece)
        if total > max_bytes:
            raise GraphError("response exceeds max_bytes")
        chunks.append(piece)
    return b"".join(chunks)


def grpc_channel(
    target: str,
    allow_insecure: bool,
    root_certificates: bytes | None,
    private_key: bytes | None,
    certificate_chain: bytes | None,
) -> object:
    """Open a ``grpc.aio`` channel.

    Args:
        target: Host and port.
        allow_insecure: Open an insecure channel when true.
        root_certificates: PEM roots, or ``None``.
        private_key: PEM client key for mutual TLS, or ``None``.
        certificate_chain: PEM client certificate, or ``None``.

    Returns:
        A channel with ``unary_unary`` and ``close``.
    """
    import grpc  # pylint: disable=import-error

    if allow_insecure:
        return grpc.aio.insecure_channel(target)
    credentials = grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )
    return grpc.aio.secure_channel(target, credentials)


class Observer(Protocol):
    """Notifications for one node attempt."""

    def on_start(self, definition: GraphDefinition, node_id: str, *, run_id: str) -> None:
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
