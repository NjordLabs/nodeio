"""Keep HTTP tests off the network."""

from __future__ import annotations

import ipaddress
import socket

import pytest

_PUBLIC = "93.184.216.34"


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve hostnames to a public address and leave IP literals alone."""

    def fake(host: str, *_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        address = host
        try:
            ipaddress.ip_address(host)
        except ValueError:
            address = _PUBLIC
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake)
