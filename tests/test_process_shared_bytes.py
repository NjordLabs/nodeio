"""Bytes cross the process boundary through shared memory."""

from __future__ import annotations

import asyncio

import pytest

from nodeio import GraphError, Node, Port, load, run
from nodeio.executors import SharedBytes
from tests.workers import echo_bytes, fail_process


def test_process_restores_bytes_and_unlinks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A byte input is restored in the child and the block is released."""
    released: list[str] = []
    original = SharedBytes.unlink

    def spy(self: SharedBytes) -> None:
        released.append(self.name)
        original(self)

    monkeypatch.setattr(SharedBytes, "unlink", spy)
    node = Node(
        "p",
        "map",
        (Port("value", "bytes"),),
        (Port("value", "bytes"),),
        echo_bytes,
        "process",
    )
    definition = load((node,), (), entries=(("p", "value"),), side={"blob": b"side"})
    outputs = asyncio.run(run(definition, {"p": {"value": b"pixels"}}))
    assert outputs["p"]["value"] == b"pixels"
    assert released


def test_process_unlinks_when_the_call_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shared blocks are released when the process runner raises."""
    released: list[str] = []
    original = SharedBytes.unlink

    def spy(self: SharedBytes) -> None:
        released.append(self.name)
        original(self)

    monkeypatch.setattr(SharedBytes, "unlink", spy)
    node = Node("p", "map", (Port("value", "bytes"),), (), fail_process, "process")
    definition = load((node,), (), entries=(("p", "value"),))
    with pytest.raises(GraphError, match="node p failed"):
        asyncio.run(run(definition, {"p": {"value": b"pixels"}}))
    assert released


def test_empty_bytes_skip_shared_memory() -> None:
    """An empty payload is passed through without a shared block."""
    node = Node(
        "p",
        "map",
        (Port("value", "bytes"),),
        (Port("value", "bytes"),),
        echo_bytes,
        "process",
    )
    definition = load((node,), (), entries=(("p", "value"),))
    outputs = asyncio.run(run(definition, {"p": {"value": b""}}))
    assert outputs["p"]["value"] == b""
