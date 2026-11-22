"""Regression tests for the graph engine and the client pipes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from types import SimpleNamespace

import pytest

from nodeio import (
    Edge,
    GraphError,
    LoggingObserver,
    Node,
    Port,
    create,
    demux,
    gate,
    grpc_node,
    http_node,
    load,
    mux,
    register,
    register_subgraph,
    run,
    subgraph_node,
)
from nodeio.builtin import linear, reset_subgraphs
from nodeio.executors import SharedBytes
from nodeio.graph import GraphDefinition
from tests.workers import double


def _async_node(node_id: str, kind: str = "map") -> Node:
    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        await asyncio.sleep(0)
        seen = side.get("seen")
        return {"value": inputs.get("value"), "seen": seen}

    return Node(
        node_id,
        kind,
        (Port("value", "any"),),
        (Port("value", "any"), Port("seen", "any")),
        runner,
    )


def test_cycle_is_named() -> None:
    left = Node("a", "map", (Port("value", "any"),), (Port("value", "any"),))
    right = Node("b", "map", (Port("value", "any"),), (Port("value", "any"),))
    edges = (
        Edge("a", "value", "b", "value"),
        Edge("b", "value", "a", "value"),
    )
    with pytest.raises(GraphError) as caught:
        load((left, right), edges)
    assert caught.value.cycle[0] == caught.value.cycle[-1]
    assert "a" in caught.value.cycle and "b" in caught.value.cycle


def test_definitions_do_not_share_side_packets() -> None:
    side = {"token": "secret"}
    node = Node("a", "map", (), (Port("value", "any"),))
    first = load((node,), (), side=side)
    side["token"] = "changed"
    second = load((node,), (), side={"token": "other"})
    assert first.side[0][1] == "secret"
    assert second.side != first.side
    assert first.nodes is not second.nodes


def test_duplicate_id_and_unconnected_input() -> None:
    node = Node("a", "map", (Port("value", "any"),), ())
    with pytest.raises(GraphError, match="duplicate"):
        load((node, Node("a", "map")), ())
    with pytest.raises(GraphError, match="unconnected"):
        load((node,), ())


def test_type_mismatch_and_unknown_node() -> None:
    left = Node("a", "map", (), (Port("value", "int"),))
    right = Node("b", "map", (Port("value", "str"),), ())
    with pytest.raises(GraphError, match="type mismatch"):
        load((left, right), (Edge("a", "value", "b", "value"),))
    with pytest.raises(GraphError, match="unknown"):
        load((left,), (Edge("a", "value", "missing", "value"),))


def test_factory_mixes_kinds() -> None:
    def make(node_id: str, scale: int = 1) -> Node:
        async def runner(
            inputs: Mapping[str, object], side: Mapping[str, object]
        ) -> dict[str, object]:
            del side
            value = inputs["value"]
            if not isinstance(value, int):
                raise TypeError("value must be an int")
            return {"value": value * scale}

        return Node(node_id, "scale", (Port("value", "int"),), (Port("value", "int"),), runner)

    register("scale", make)
    built = create("scale", "n", scale=3)
    other = Node("src", "const", (), (Port("value", "int"),))

    async def source(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        return {"value": 2}

    source_node = Node("src", "const", (), (Port("value", "int"),), source)
    assert other.id == source_node.id
    definition = load(
        (source_node, built),
        (Edge("src", "value", "n", "value"),),
    )
    outputs = asyncio.run(run(definition, {}))
    assert outputs["n"]["value"] == 6


def test_none_is_a_value_and_runs_are_isolated() -> None:
    node = _async_node("a")
    definition = load((node,), (), entries=(("a", "value"),), side={"seen": "from-side"})

    async def both() -> tuple[object, object]:
        left, right = await asyncio.gather(
            run(definition, {"a": {"value": None}}),
            run(definition, {"a": {"value": 1}}),
        )
        return left["a"]["value"], right["a"]["value"]

    assert asyncio.run(both()) == (None, 1)
    assert asyncio.run(run(definition, {"a": {"value": None}}))["a"]["seen"] == "from-side"


def test_failure_names_the_node_and_collect_continues() -> None:
    async def boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        raise RuntimeError("secret body")

    node = Node("bad", "map", (), (Port("value", "any"),), boom)
    definition = load((node,), ())
    with pytest.raises(GraphError, match="node bad failed: RuntimeError"):
        asyncio.run(run(definition, {}))
    collected = asyncio.run(run(definition, {}, policy="collect"))
    assert "bad" not in collected


def test_timeout_and_limit() -> None:
    async def slow(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        await asyncio.sleep(1)
        return {"value": 1}

    node = Node("slow", "map", (), (Port("value", "any"),), slow)
    definition = load((node,), ())
    with pytest.raises(GraphError, match="slow"):
        asyncio.run(run(definition, {}, timeout=0.01))
    with pytest.raises(GraphError, match="limit"):
        asyncio.run(run(definition, {}, limit=0))


def test_thread_and_process_executors() -> None:
    def sync(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        return {"value": inputs["value"]}

    threaded = Node("t", "map", (Port("value", "any"),), (Port("value", "any"),), sync, "thread")
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
    handle = SharedBytes.from_bytes(b"pixels")
    try:
        assert handle.read() == b"pixels"
    finally:
        handle.unlink()


def test_ready_queue_runs_downstream() -> None:
    async def add(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        value = inputs["value"]
        if not isinstance(value, int):
            raise TypeError("value must be an int")
        return {"value": value + 1}

    source = Node("a", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    sink = Node("b", "map", (Port("value", "int"),), (Port("value", "int"),), add)
    definition = load(
        (source, sink),
        (Edge("a", "value", "b", "value"),),
        entries=(("a", "value"),),
    )
    assert "a -> b" in definition.to_dot()
    assert "a --> b" in definition.to_mermaid()
    outputs = asyncio.run(run(definition, {"a": {"value": 1}}))
    assert outputs["b"]["value"] == 3


def test_missing_seed() -> None:
    node = _async_node("a")
    definition = load((node,), (), entries=(("a", "value"),))
    with pytest.raises(GraphError, match="missing inputs"):
        asyncio.run(run(definition, {}))


def test_observer_omits_values(capsys: pytest.CaptureFixture[str]) -> None:
    from mimirlog import configure

    configure()
    node = _async_node("a")
    definition = load((node,), (), entries=(("a", "value"),))
    asyncio.run(run(definition, {"a": {"value": "secret"}}, observer=LoggingObserver("demo")))
    captured = capsys.readouterr()
    text = captured.err + captured.out
    assert "secret" not in text
    payload = json.loads(text.strip().splitlines()[-1])
    assert payload["message"]["node"] == "a"
    assert payload["message"]["event"] == "success"


def test_gate_mux_demux_and_subgraph() -> None:
    reset_subgraphs()
    child_in = Node(
        "in",
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        _identity(),
    )
    child_out = Node(
        "out",
        "map",
        (Port("value", "any"),),
        (Port("value", "any"),),
        _identity(),
    )
    register_subgraph("child", linear(child_in, child_out))
    parent = load(
        (subgraph_node("sub", "child"),),
        (),
        entries=(("sub", "value"),),
    )
    assert asyncio.run(run(parent, {"sub": {"value": 5}}))["sub"]["value"] == 5
    opened = load((gate("g"),), (), entries=(("g", "open"), ("g", "value")))
    closed = asyncio.run(run(opened, {"g": {"open": False, "value": 1}}))
    assert closed["g"]["value"] is None
    chosen = load((mux("m"),), (), entries=(("m", "select"), ("m", "left"), ("m", "right")))
    muxed = asyncio.run(run(chosen, {"m": {"select": "left", "left": 1, "right": 2}}))
    assert muxed["m"]["value"] == 1
    split = load((demux("d"),), (), entries=(("d", "key"), ("d", "value")))
    routed = asyncio.run(run(split, {"d": {"key": "right", "value": 9}}))
    assert routed["d"] == {"left": None, "right": 9}
    with pytest.raises(ValueError):
        register_subgraph("", GraphDefinition((), ()))


def test_http_policy_and_retry() -> None:
    with pytest.raises(GraphError, match="timeout"):
        http_node("h", timeout=0)

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
            del url, timeout
            self.calls += 1
            if method == "GET" and self.calls == 1:
                return SimpleNamespace(status_code=500, content=b"ok")
            return SimpleNamespace(status_code=200, content=b"ok")

    client = Client()
    node = http_node(
        "h",
        timeout=1,
        allow_insecure=True,
        allow_hosts=("example.test",),
        retries=1,
        client=client,
    )
    definition = load((node,), (), entries=(("h", "method"), ("h", "url")))
    outputs = asyncio.run(
        run(definition, {"h": {"method": "GET", "url": "http://example.test/item"}})
    )
    assert outputs["h"]["status"] == 200
    assert client.calls == 2
    with pytest.raises(GraphError, match="node b failed") as insecure:
        blocked = http_node("b", timeout=1, client=client)
        graph = load((blocked,), (), entries=(("b", "method"), ("b", "url")))
        asyncio.run(run(graph, {"b": {"method": "GET", "url": "http://example.test"}}))
    assert isinstance(insecure.value.__cause__, GraphError)
    assert "insecure" in str(insecure.value.__cause__)
    with pytest.raises(GraphError, match="node c failed") as blocked_host:
        limited = http_node("c", timeout=1, allow_hosts=("ok.test",), client=client)
        graph = load((limited,), (), entries=(("c", "method"), ("c", "url")))
        asyncio.run(run(graph, {"c": {"method": "GET", "url": "https://no.test"}}))
    assert isinstance(blocked_host.value.__cause__, GraphError)
    assert "not allowed" in str(blocked_host.value.__cause__)


def test_http_does_not_retry_post_and_caps_the_body() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
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
    with pytest.raises(GraphError, match="deadline"):
        grpc_node("g", method="pkg.Service/Run", deadline=0)
    with pytest.raises(GraphError, match="insecure"):
        grpc_node("g", method="pkg.Service/Run", deadline=1, insecure=True)

    async def unary(method: str, request: object, deadline: float) -> dict[str, object]:
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


def test_remaining_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = load((gate("g"),), (), entries=(("g", "open"), ("g", "value")))
    assert asyncio.run(run(opened, {"g": {"open": True, "value": 3}}))["g"]["value"] == 3
    chosen = load((mux("m"),), (), entries=(("m", "select"), ("m", "left"), ("m", "right")))
    muxed = asyncio.run(run(chosen, {"m": {"select": "right", "left": 1, "right": 2}}))
    assert muxed["m"]["value"] == 2
    split = load((demux("d"),), (), entries=(("d", "key"), ("d", "value")))
    routed = asyncio.run(run(split, {"d": {"key": "left", "value": 4}}))
    assert routed["d"]["left"] == 4

    left = Node("a", "map", (), (Port("value", "int"),))
    right = Node("b", "map", (Port("value", "int"),), ())
    with pytest.raises(GraphError, match="no output port"):
        load((left, right), (Edge("a", "missing", "b", "value"),))
    with pytest.raises(GraphError, match="unknown node"):
        load((left,), (), entries=(("missing", "value"),))
    with pytest.raises(GraphError, match="no input port"):
        load((left,), (), entries=(("a", "value"),))
    other = Node("c", "map", (), (Port("value", "int"),))
    load((left, other, right), (Edge("a", "value", "b", "value"), Edge("c", "value", "b", "value")))

    async def empty(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        return {}

    source = Node("a", "map", (), (Port("value", "int"),), empty)
    sink = Node("b", "map", (Port("value", "int"),), ())
    stalled = load((source, sink), (Edge("a", "value", "b", "value"),))
    with pytest.raises(GraphError, match="still waiting on b"):
        asyncio.run(run(stalled, {}))

    async def check_executor() -> None:
        from nodeio.executors import LocalExecutor

        executor = LocalExecutor()
        produced = await executor.run(
            Node("p", "map", (Port("value", "any"),), (Port("value", "any"),), double, "process"),
            {"value": 2},
            {},
        )
        assert produced["value"] == 4
        executor.close()
        executor.close()
        with pytest.raises(ValueError, match="no runner"):
            await executor.run(Node("bare", "map"), {}, {})
        with pytest.raises(TypeError):
            await executor.run(Node("bad", "map", runner=_number), {}, {})
        with pytest.raises(ValueError, match="unknown executor"):
            await executor.run(Node("odd", "map", runner=_number, executor="nope"), {}, {})

    asyncio.run(check_executor())

    from mimirlog import configure

    configure()
    failing = Node("bad", "map", (), (Port("value", "any"),), _boom)
    definition = load((failing,), ())
    with pytest.raises(GraphError, match="node bad failed"):
        asyncio.run(run(definition, {}, observer=LoggingObserver("demo")))

    class Owned:
        async def __aenter__(self) -> Owned:
            return self

        async def __aexit__(self, *args: object) -> bool:
            return False

        async def request(self, method: str, url: str) -> SimpleNamespace:
            del method, url
            return SimpleNamespace(status_code=204, content=b"")

    monkeypatch.setattr("httpx.AsyncClient", lambda **_kwargs: Owned())
    plain = http_node("h", timeout=1)
    graph = load((plain,), (), entries=(("h", "method"), ("h", "url")))
    status = asyncio.run(run(graph, {"h": {"method": "GET", "url": "https://example.test"}}))
    assert status["h"]["status"] == 204

    class Weird:
        async def request(self, method: str, url: str, timeout: float) -> SimpleNamespace:
            del method, url, timeout
            return SimpleNamespace(status_code="no", content=b"")

    weird = http_node("w", timeout=1, client=Weird())
    weird_graph = load((weird,), (), entries=(("w", "method"), ("w", "url")))
    with pytest.raises(GraphError, match="node w failed"):
        asyncio.run(run(weird_graph, {"w": {"method": "GET", "url": "https://example.test"}}))
    ftp = http_node("f", timeout=1, client=Weird())
    ftp_graph = load((ftp,), (), entries=(("f", "method"), ("f", "url")))
    with pytest.raises(GraphError, match="node f failed"):
        asyncio.run(run(ftp_graph, {"f": {"method": "GET", "url": "ftp://example.test"}}))


async def _number(inputs: Mapping[str, object], side: Mapping[str, object]) -> int:
    del inputs, side
    return 1


async def _boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    del inputs, side
    raise RuntimeError("nope")


def _identity() -> Callable[..., object]:
    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        return {"value": inputs["value"]}

    return runner
