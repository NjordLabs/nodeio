"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

from nodeio import (
    Node,
    Port,
    load,
    run,
)
from nodeio.executors import SharedBytes
from tests.workers import double


def test_thread_and_process_executors() -> None:
    """Run this step."""

    def sync(
        inputs: Mapping[str, object], side: Mapping[str, object]
    ) -> dict[str, object]:
        """Run this step."""
        del side
        return {"value": inputs["value"]}

    threaded = Node(
        "t", "map", (Port("value", "any"),), (Port("value", "any"),), sync, "thread"
    )
    processed = Node(
        "p",
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        double,
        "process",
    )
    definition = load(
        (threaded, processed),
        (),
        entries=(("t", "value"), ("p", "value")),
    )
    outputs = asyncio.run(run(definition, {"t": {"value": "ok"}, "p": {"value": 4}}))
    assert outputs["t"]["value"] == "ok"
    assert outputs["p"]["value"] == 8


def test_shared_memory_roundtrip() -> None:
    """Run this step."""
    handle = SharedBytes.from_bytes(b"pixels")
    try:
        assert handle.read() == b"pixels"
    finally:
        handle.unlink()
