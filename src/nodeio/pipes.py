"""Outbound HTTP and gRPC calls.

Timeouts are mandatory. TLS is on unless the caller opts into insecure
transport. Only idempotent HTTP methods are retried. Bodies are not logged.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from urllib.parse import urlparse

from mimirlog import get_logger

from nodeio.errors import GraphError
from nodeio.graph import Node, Port

_IDEMPOTENT = frozenset({"DELETE", "GET", "HEAD", "PUT"})
Unary = Callable[[str, object, float], Awaitable[object]]


def http_node(
    node_id: str,
    *,
    timeout: float,
    allow_insecure: bool = False,
    allow_hosts: tuple[str, ...] = (),
    max_bytes: int = 1_000_000,
    retries: int = 0,
    client: object | None = None,
    verify: bool | str = True,
    cert: tuple[str, str] | None = None,
    backoff: float = 0.0,
) -> Node:
    """Build a node that performs one HTTP request.

    Args:
        node_id: Node id.
        timeout: Seconds for the request. Must be positive.
        allow_insecure: Permit ``http://`` URLs when true.
        allow_hosts: When non-empty, the URL host must be in this tuple.
        max_bytes: Maximum response body size.
        retries: Extra attempts for idempotent methods.
        client: Object with an async ``request`` method. Tests inject one.
        verify: ``True``, ``False``, or a CA bundle path for the owned client.
        cert: Certificate and key paths for mutual TLS on the owned client.
        backoff: Base delay in seconds before a retry. Zero disables the wait.
            The delay doubles each retry and is scaled by a random factor
            between one half and one.

    Returns:
        A node. Inputs are ``method`` and ``url``. Output is ``status``.

    Raises:
        GraphError: ``timeout`` is not positive.
    """
    if timeout <= 0:
        raise GraphError("timeout must be positive")

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        method = str(inputs["method"]).upper()
        url = str(inputs["url"])
        _check_url(url, allow_insecure=allow_insecure, allow_hosts=allow_hosts)
        attempts = retries + 1 if method in _IDEMPOTENT else 1
        last: dict[str, object] = {"status": 0}
        for attempt in range(attempts):
            if attempt and backoff > 0:
                delay = backoff * (2 ** (attempt - 1)) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)
            last = await _once(client, method, url, timeout, max_bytes, verify, cert)
            status = last["status"]
            _log_http(method, url, status, attempt)
            if not isinstance(status, int):
                raise GraphError("status must be an int")
            if status < 500 or method not in _IDEMPOTENT:
                return last
        return last

    return Node(
        node_id,
        "http",
        (Port("method", "str"), Port("url", "str")),
        (Port("status", "int"),),
        runner,
    )


def _check_url(url: str, *, allow_insecure: bool, allow_hosts: tuple[str, ...]) -> None:
    parsed = urlparse(url)
    if parsed.scheme == "http" and not allow_insecure:
        raise GraphError("insecure HTTP requires allow_insecure")
    if parsed.scheme not in {"http", "https"}:
        raise GraphError(f"unsupported URL scheme {parsed.scheme}")
    if allow_hosts and parsed.hostname not in allow_hosts:
        raise GraphError(f"host {parsed.hostname} is not allowed")


def _log_http(method: str, url: str, status: object, attempt: int) -> None:
    host = urlparse(url).hostname or ""
    get_logger("nodeio.http").info({
        "attempt": attempt,
        "event": "http",
        "host": host,
        "method": method,
        "status": status,
    })


async def _once(
    client: object | None,
    method: str,
    url: str,
    timeout: float,
    max_bytes: int,
    verify: bool | str,
    cert: tuple[str, str] | None,
) -> dict[str, object]:
    if client is None:
        import httpx

        async with httpx.AsyncClient(timeout=timeout, verify=verify, cert=cert) as owned:
            response = await owned.request(method, url)
            body = response.content
            status = response.status_code
    else:
        response = await client.request(method, url, timeout=timeout)  # type: ignore[attr-defined]
        body = response.content
        status = response.status_code
    if len(body) > max_bytes:
        raise GraphError("response exceeds max_bytes")
    return {"status": status}


def grpc_node(
    node_id: str,
    *,
    method: str,
    deadline: float,
    insecure: bool = False,
    call: Unary | None = None,
) -> Node:
    """Build a node that performs one unary gRPC call.

    Args:
        node_id: Node id.
        method: Full method path, for example ``pkg.Service/Run``.
        deadline: Positive deadline in seconds.
        insecure: Permit an insecure channel when true.
        call: Injected unary call. The default builds a TLS channel.

    Returns:
        A node. Input is ``request``. Output is ``response``.

    Raises:
        GraphError: The deadline is not positive, or insecure is set
            without the caller passing ``insecure=True``.
    """
    if deadline <= 0:
        raise GraphError("deadline must be positive")

    if insecure and call is None:
        raise GraphError("insecure gRPC requires an explicit channel")

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        if call is None:
            raise GraphError("grpc extra is not installed")
        response = await call(method, inputs["request"], deadline)
        return {"response": response}

    return Node(
        node_id,
        "grpc",
        (Port("request", "any"),),
        (Port("response", "any"),),
        runner,
    )
