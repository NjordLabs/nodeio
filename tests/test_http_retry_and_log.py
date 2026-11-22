"""HTTP retry exhaustion and the duration log line."""

from __future__ import annotations

import asyncio
import json

import pytest

from nodeio import GraphError, http_node, load, run
from tests.support import RecordingClient


def test_final_5xx_raises_after_retries() -> None:
    """Idempotent retries that end in 5xx are a graph error."""
    client = RecordingClient(500, b"nope")
    node = http_node("h", timeout=1, retries=1, client=client)
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    with pytest.raises(GraphError, match="node h failed") as caught:
        asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert isinstance(caught.value.__cause__, GraphError)
    assert "after retries" in str(caught.value.__cause__)
    assert client.calls == 2
    posted = RecordingClient(500, b"once")
    post = http_node("p", timeout=1, retries=3, client=posted)
    post_graph = load((post,), (), entries=(("p", "method"), ("p", "url")))
    outputs = asyncio.run(run(post_graph, {"p": {"method": "POST", "url": "https://example.test"}}))
    assert outputs["p"]["status"] == 500
    assert posted.calls == 1


def test_http_log_records_duration_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    """The log line has a duration and omits bodies, userinfo, and headers."""
    from mimirlog import configure

    configure()
    node = http_node("h", timeout=1, client=RecordingClient(204, b"secret-body"))
    graph = load((node,), (), entries=(("h", "method"), ("h", "url")))
    asyncio.run(
        run(graph, {"h": {"method": "GET", "url": "https://user:secret@example.test/item"}})
    )
    captured = capsys.readouterr()
    text = captured.err + captured.out
    assert "secret" not in text
    assert "secret-body" not in text
    payload = json.loads(text.strip().splitlines()[-1])
    message = payload["message"]
    assert message["duration_s"] >= 0
    assert message["host"] == "example.test"
    assert set(message) == {"attempt", "duration_s", "event", "host", "method", "status"}
