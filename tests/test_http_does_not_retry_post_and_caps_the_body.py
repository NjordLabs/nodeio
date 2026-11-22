"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nodeio import (
    GraphError,
    grpc_node,
    http_node,
    load,
    run,
)


def test_http_does_not_retry_post_and_caps_the_body() -> None:
    """Run this step."""

    class Client:
        """Hold a test double."""

        def __init__(self) -> None:
            """Run this step."""
            self.calls = 0

        async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
            """Run this step."""
            del method, url, timeout
            self.calls += 1
            return SimpleNamespace(status_code=500, content=b"12345")

    client = Client()
    node = http_node("h", timeout=1, retries=3, max_bytes=2, client=client)
    definition = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as too_big:
        asyncio.run(run(definition, {"h": {"method": "POST", "url": "https://example.test"}}))
    assert isinstance(too_big.value.__cause__, GraphError)
    assert "max_bytes" in str(too_big.value.__cause__)
    assert client.calls == 1


def test_grpc_requires_a_deadline_and_a_secure_call() -> None:
    """Run this step."""
    with pytest.raises(GraphError, match="deadline"):
        grpc_node("g", method="pkg.Service/Run", deadline=0)
    insecure = grpc_node("g", method="pkg.Service/Run", deadline=1, allow_insecure=True)
    insecure_graph = load((insecure,), (), entries=(("g", "request"),))
    with pytest.raises(GraphError, match="node g failed") as insecure_run:
        asyncio.run(run(insecure_graph, {"g": {"request": {}}}))
    assert isinstance(insecure_run.value.__cause__, GraphError)
    assert "grpc extra" in str(insecure_run.value.__cause__)

    async def unary(method: str, request: object, deadline: float) -> dict[str, object]:
        """Run this step."""
        return {"method": method, "request": request, "deadline": deadline}

    node = grpc_node("g", method="pkg.Service/Run", deadline=0.5, call=unary)
    definition = load((node,), (), entries=(("g", "request"),))
    outputs = asyncio.run(run(definition, {"g": {"request": {"n": 1}}}))
    response = outputs["g"]["response"]
    assert isinstance(response, dict)
    assert response["method"] == "pkg.Service/Run"
    missing = grpc_node("x", method="pkg.Service/Run", deadline=1)
    graph = load((missing,), (), entries=(("x", "request"),))
    with pytest.raises(GraphError, match="node x failed") as missing_extra:
        asyncio.run(run(graph, {"x": {"request": {}}}))
    assert isinstance(missing_extra.value.__cause__, GraphError)
    assert "grpc extra" in str(missing_extra.value.__cause__)
