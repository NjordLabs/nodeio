"""Outbound HTTP and gRPC calls.

Timeouts are mandatory. TLS is on unless the caller opts into insecure
transport. Only idempotent HTTP methods are retried. Bodies, header
values, and URLs that contain userinfo are not logged.
"""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
import secrets
import socket
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol, cast
from urllib.parse import urlparse

from nodeio.boundary import grpc_channel, http_exchange, named_logger
from nodeio.errors import GraphError
from nodeio.graph import Node, Port

_IDEMPOTENT = frozenset({"DELETE", "GET", "HEAD", "PUT"})
Unary = Callable[[str, object, float], Awaitable[object]]
ChannelFactory = Callable[..., object]


class _Response(Protocol):
    """Status and body of one HTTP response."""

    status_code: int
    content: bytes


class _Client(Protocol):
    """Object that can perform one HTTP request."""

    async def request(self, method: str, url: str, timeout: float) -> _Response:
        """Send ``method`` to ``url`` and return the response.

        Raises:
            NotImplementedError: Callers use a concrete client.
        """
        raise NotImplementedError


class _UnaryCall(Protocol):
    """One unary RPC."""

    def __call__(self, request: object, timeout: float) -> Awaitable[object] | object:
        """Send ``request`` and return the response."""
        ...


class _Channel(Protocol):
    """The channel operations this package uses."""

    def unary_unary(self, method: str) -> _UnaryCall:
        """Return a unary callable for ``method``."""
        ...

    def close(self) -> Awaitable[None] | None:
        """Close the channel."""
        ...


def _as_client(client: object) -> _Client:
    return cast(_Client, client)


def _as_channel(channel: object) -> _Channel:
    return cast(_Channel, channel)


def http_node(
    node_id: str,
    *,
    timeout: float,
    allow_insecure: bool = False,
    allow_hosts: tuple[str, ...] = (),
    allow_private: bool = False,
    max_bytes: int = 1_000_000,
    retries: int = 0,
    client: object | None = None,
    verify: bool | str = True,
    cert: tuple[str, str] | None = None,
    backoff: float = 0.0,
) -> Node:
    """Build a node that performs one HTTP request.

    The host is resolved when the request starts. Private, loopback, and
    link-local answers are rejected unless ``allow_private`` is true.
    That check does not stop DNS rebinding after the request has started.
    Clients constructed here do not follow redirects. ``verify=False``
    requires ``allow_insecure``. A CA path string is still allowed.
    A final HTTP 5xx on an idempotent method raises after retries.

    Args:
        node_id: Node id.
        timeout: Seconds for the request. Must be positive.
        allow_insecure: Permit ``http://`` URLs and ``verify=False``.
        allow_hosts: When non-empty, the URL host must be in this tuple.
        allow_private: Permit private, loopback, and link-local addresses.
        max_bytes: Maximum response body size, enforced while streaming.
        retries: Extra attempts for idempotent methods.
        client: Object with an async ``request`` method. Tests inject one.
        verify: ``True``, ``False``, or a CA bundle path for the owned client.
        cert: Certificate and key paths for mutual TLS on the owned client.
        backoff: Base delay in seconds before a retry. Zero disables the wait.
            The delay doubles each retry and is scaled by a random factor
            between one half and one.

    Returns:
        A node. Inputs are ``method`` and ``url``. Outputs are ``status``
        and ``body``.

    Raises:
        GraphError: ``timeout`` is not positive, or TLS verification is
            disabled without ``allow_insecure``.
    """
    if timeout <= 0:
        raise GraphError("timeout must be positive")
    if verify is False and not allow_insecure:
        raise GraphError("verify=False requires allow_insecure")

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Perform the HTTP request described by the node inputs.

        Returns:
            The status and body.
        """
        _ = side
        method = str(inputs["method"]).upper()
        url = str(inputs["url"])
        _check_url(
            url,
            allow_insecure=allow_insecure,
            allow_hosts=allow_hosts,
            allow_private=allow_private,
        )
        return await _attempts(
            client, method, url, timeout, max_bytes, verify, cert, retries, backoff
        )

    return Node(
        node_id,
        "http",
        (Port("method", "str"), Port("url", "str")),
        (Port("status", "int"), Port("body", "bytes")),
        runner,
    )


def _check_url(
    url: str,
    *,
    allow_insecure: bool,
    allow_hosts: tuple[str, ...],
    allow_private: bool,
) -> None:
    parsed = urlparse(url)
    _check_scheme(parsed.scheme, allow_insecure)
    if allow_hosts and parsed.hostname not in allow_hosts:
        raise GraphError(f"host {parsed.hostname} is not allowed")
    _reject_private(parsed.hostname, allow_private=allow_private)


def _check_scheme(scheme: str, allow_insecure: bool) -> None:
    if scheme == "http" and not allow_insecure:
        raise GraphError("insecure HTTP requires allow_insecure")
    if scheme not in {"http", "https"}:
        raise GraphError(f"unsupported URL scheme {scheme}")


def _reject_private(host: str | None, *, allow_private: bool) -> None:
    if allow_private or host is None:
        return
    for address in _addresses(host):
        ip = ipaddress.ip_address(address)
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            raise GraphError(f"host {host} resolves to a restricted address")


def _addresses(host: str) -> tuple[str, ...]:
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as exc:
        raise GraphError(f"cannot resolve host {host}") from exc
    found: list[str] = []
    for info in infos:
        sockaddr = info[4]
        if isinstance(sockaddr, tuple) and sockaddr:
            found.append(str(sockaddr[0]))
    return tuple(found)


async def _attempts(
    client: object | None,
    method: str,
    url: str,
    timeout: float,
    max_bytes: int,
    verify: bool | str,
    cert: tuple[str, str] | None,
    retries: int,
    backoff: float,
) -> dict[str, object]:
    attempts = retries + 1 if method in _IDEMPOTENT else 1
    last: dict[str, object] = {"status": 0, "body": b""}
    for attempt in range(attempts):
        await _pause(attempt, backoff)
        started = time.perf_counter()
        last = await _once(client, method, url, timeout, max_bytes, verify, cert)
        _log_http(method, url, last["status"], attempt, time.perf_counter() - started)
        status = _status(last)
        if status < 500 or method not in _IDEMPOTENT:
            return last
    raise GraphError(f"HTTP {last['status']} after retries")


async def _pause(attempt: int, backoff: float) -> None:
    if attempt and backoff > 0:
        delay = backoff * (1 << (attempt - 1)) * _jitter()
        await asyncio.sleep(delay)


def _status(payload: Mapping[str, object]) -> int:
    status = payload["status"]
    if not isinstance(status, int):
        raise GraphError("status must be an int")
    return status


def _jitter() -> float:
    """Return a delay scale between one half and one.

    Returns:
        A factor in the closed range from 0.5 to 1.0.
    """
    return 0.5 + (secrets.randbelow(501) / 1000)


def _log_http(method: str, url: str, status: object, attempt: int, duration_s: float) -> None:
    host = urlparse(url).hostname or ""
    named_logger("nodeio.http").info({
        "attempt": attempt,
        "duration_s": duration_s,
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
        status, body = await http_exchange(method, url, timeout, verify, cert, max_bytes)
        return {"status": status, "body": body}
    client_status, client_body = await _read_client(client, method, url, timeout, max_bytes)
    return {"status": client_status, "body": client_body}


async def _read_client(
    client: object,
    method: str,
    url: str,
    timeout: float,
    max_bytes: int,
) -> tuple[object, bytes]:
    response = await _as_client(client).request(method, url, timeout=timeout)
    body = response.content
    if len(body) > max_bytes:
        raise GraphError("response exceeds max_bytes")
    return response.status_code, body


def grpc_node(
    node_id: str,
    *,
    method: str,
    deadline: float,
    allow_insecure: bool = False,
    insecure: bool = False,
    target: str | None = None,
    call: Unary | None = None,
    channel_factory: ChannelFactory | None = None,
    root_certificates: bytes | None = None,
    private_key: bytes | None = None,
    certificate_chain: bytes | None = None,
) -> Node:
    """Build a node that performs one unary gRPC call.

    An injected ``call`` is used as-is. Otherwise, when the ``grpc`` extra
    is installed, the node opens a ``grpc.aio`` channel to ``target``.
    The channel is secure unless ``allow_insecure`` is true. Optional PEM
    values are client credentials for mutual TLS. The deadline is the
    call timeout.

    Args:
        node_id: Node id.
        method: Full method path, for example ``pkg.Service/Run``.
        deadline: Positive deadline in seconds.
        allow_insecure: Open an insecure channel when true.
        insecure: Alias of ``allow_insecure``.
        target: ``host:port`` for the channel. Required without ``call``.
        call: Injected unary call. Skips the channel.
        channel_factory: Test double that returns a channel.
        root_certificates: PEM roots for the secure channel.
        private_key: PEM client key for mutual TLS.
        certificate_chain: PEM client certificate for mutual TLS.

    Returns:
        A node. Input is ``request``. Output is ``response``.

    Raises:
        GraphError: The deadline is not positive.
    """
    if deadline <= 0:
        raise GraphError("deadline must be positive")
    use_insecure = allow_insecure or insecure

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Perform the unary call described by the node inputs.

        Returns:
            The response mapping.
        """
        _ = side
        response = await _grpc_response(
            method,
            inputs["request"],
            deadline,
            target,
            use_insecure,
            call,
            channel_factory,
            root_certificates,
            private_key,
            certificate_chain,
        )
        return {"response": response}

    return Node(
        node_id,
        "grpc",
        (Port("request", "any"),),
        (Port("response", "any"),),
        runner,
    )


async def _grpc_response(
    method: str,
    request: object,
    deadline: float,
    target: str | None,
    allow_insecure: bool,
    call: Unary | None,
    channel_factory: ChannelFactory | None,
    root_certificates: bytes | None,
    private_key: bytes | None,
    certificate_chain: bytes | None,
) -> object:
    if call is not None:
        return await call(method, request, deadline)
    if channel_factory is None:
        _require_grpc()
    if target is None or target == "":
        raise GraphError("target is required")
    channel = _open_channel(
        target,
        allow_insecure,
        channel_factory,
        root_certificates,
        private_key,
        certificate_chain,
    )
    try:
        return await _unary(channel, method, request, deadline)
    finally:
        await _close_channel(channel)


def _require_grpc() -> None:
    try:
        module = __import__("grpc")
        aio = __import__("grpc.aio", fromlist=["aio"])
    except ImportError as exc:
        raise GraphError("grpc extra is not installed") from exc
    _ = (module, aio)


def _open_channel(
    target: str,
    allow_insecure: bool,
    channel_factory: ChannelFactory | None,
    root_certificates: bytes | None,
    private_key: bytes | None,
    certificate_chain: bytes | None,
) -> object:
    if channel_factory is not None:
        return channel_factory(
            target,
            allow_insecure=allow_insecure,
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )
    return grpc_channel(
        target,
        allow_insecure,
        root_certificates,
        private_key,
        certificate_chain,
    )


async def _unary(channel: object, method: str, request: object, deadline: float) -> object:
    call = _as_channel(channel).unary_unary(method)
    response = call(request, timeout=deadline)
    if inspect.isawaitable(response):
        return await response
    return response


async def _close_channel(channel: object) -> None:
    result = _as_channel(channel).close()
    if inspect.isawaitable(result):
        await result
