"""Gate, mux, exports, and subgraph option forwarding."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from nodeio import (
    Edge,
    GraphError,
    Node,
    Port,
    gate,
    load,
    mux,
    register_subgraph,
    run,
    subgraph_node,
)
from nodeio.builtin import reset_subgraphs
from nodeio.executors import LocalExecutor
from nodeio.run import current_options
from tests.support import identity


def test_exports_quote_ids() -> None:
    """Quotes and newlines stay inside one exported edge line."""
    source = Node('say "hi"\n', "map", (), (Port("value", "int"),))
    sink = Node("ok", "map", (Port("value", "int"),), ())
    graph = load((source, sink), (Edge('say "hi"\n', "value", "ok", "value"),))
    for exported in (graph.to_dot(), graph.to_mermaid()):
        edge_line = exported.splitlines()[1]
        assert "\\n" in edge_line
        assert '\\"' in edge_line
        assert "\n" not in edge_line


def test_gate_and_mux_reject_unexpected_controls() -> None:
    """open is a bool and select is left or right."""
    opened = load((gate("g"),), (), entries=(("g", "open"), ("g", "value")))
    with pytest.raises(GraphError, match="node g failed") as gate_error:
        asyncio.run(run(opened, {"g": {"open": "yes", "value": 1}}))
    assert isinstance(gate_error.value.__cause__, GraphError)
    assert "bool" in str(gate_error.value.__cause__)
    chosen = load((mux("m"),), (), entries=(("m", "select"), ("m", "left"), ("m", "right")))
    with pytest.raises(GraphError, match="node m failed") as mux_error:
        asyncio.run(run(chosen, {"m": {"select": "middle", "left": 1, "right": 2}}))
    assert isinstance(mux_error.value.__cause__, GraphError)
    assert "select" in str(mux_error.value.__cause__)


def test_subgraph_forwards_run_options() -> None:
    """The child run sees the parent's observer, executor, limit, and policy."""
    assert current_options().policy == "fail"
    reset_subgraphs()
    seen: list[str] = []
    depth = {"current": 0, "peak": 0}

    class Recorder:
        def __init__(self) -> None:
            self.ids: list[str] = []
            self._executor: object = None

        async def run(
            self,
            node: Node,
            inputs: Mapping[str, object],
            side: Mapping[str, object],
        ) -> Mapping[str, object]:
            from nodeio.executors import LocalExecutor

            self.ids.append(node.id)
            if not isinstance(self._executor, LocalExecutor):
                self._executor = LocalExecutor()
            return await self._executor.run(node, inputs, side)

        def close(self) -> None:
            if isinstance(self._executor, LocalExecutor):
                self._executor.close()

    class Watch:
        def on_start(self, _definition: object, node_id: str, *, run_id: str) -> None:
            del run_id
            seen.append(node_id)

        def on_success(self, *_args: object, **_kwargs: object) -> None:
            return None

        def on_exception(self, *_args: object, **_kwargs: object) -> None:
            return None

    async def hold(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del side
        depth["current"] += 1
        depth["peak"] = max(depth["peak"], depth["current"])
        await asyncio.sleep(0.05)
        depth["current"] -= 1
        return {"value": inputs.get("value", 1)}

    left = Node("in", "map", (), (Port("value", "any"),), hold)
    right = Node("out", "map", (), (Port("value", "any"),), hold)
    extra = Node("more", "map", (), (Port("value", "any"),), hold)
    register_subgraph("forwarded", load((left, right, extra), ()))
    parent = load((subgraph_node("sub", "forwarded"),), (), entries=(("sub", "value"),))
    recorder = Recorder()
    outputs = asyncio.run(
        run(
            parent,
            {"sub": {"value": 1}},
            executor=recorder,  # type: ignore[arg-type]
            observer=Watch(),  # type: ignore[arg-type]
            limit=1,
        )
    )
    assert outputs["sub"]["value"] == 1
    assert {"in", "out", "sub"} <= set(seen)
    assert {"in", "out"} <= set(recorder.ids)
    assert depth["peak"] == 1
    recorder.close()

    reset_subgraphs()

    async def boom(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        del inputs, side
        raise RuntimeError("child")

    child_in = Node("in", "map", (Port("value", "any"),), (Port("value", "any"),), identity())
    child_out = Node("out", "map", (Port("value", "any"),), (Port("value", "any"),), identity())
    bad = Node("bad", "map", (), (Port("value", "any"),), boom)
    register_subgraph(
        "partial",
        load(
            (child_in, child_out, bad),
            (Edge("in", "value", "out", "value"),),
            (("in", "value"),),
        ),
    )
    collected = load((subgraph_node("sub", "partial"),), (), entries=(("sub", "value"),))
    result = asyncio.run(run(collected, {"sub": {"value": 3}}, policy="collect"))
    assert result["sub"]["value"] == 3
    reset_subgraphs()
