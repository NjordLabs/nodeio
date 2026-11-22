"""Ready-queue execution. One coordinator publishes outputs."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Mapping

from nodeio.errors import GraphError
from nodeio.executors import Executor, LocalExecutor
from nodeio.graph import GraphDefinition, Node
from nodeio.observe import Observer

Values = dict[tuple[str, str], object]


async def run(
    definition: GraphDefinition,
    seeds: Mapping[str, Mapping[str, object]],
    *,
    executor: Executor | None = None,
    observer: Observer | None = None,
    limit: int = 8,
    timeout: float | None = None,
    policy: str = "fail",
) -> dict[str, Mapping[str, object]]:
    """Run ``definition`` once.

    Concurrent runs must not share a seed mapping. The definition is
    immutable and can be shared.

    Args:
        definition: Validated graph.
        seeds: External inputs, keyed by node id then port name.
        executor: Node dispatcher. ``None`` uses :class:`LocalExecutor`.
        observer: Optional start, success, and exception hook.
        limit: Maximum number of nodes running at once.
        timeout: Seconds for one node. ``None`` waits without a limit.
        policy: ``fail`` raises on the first node error. ``collect``
            records the error and leaves that node's outputs unset.

    Returns:
        Output mappings keyed by node id.

    Raises:
        GraphError: A seed is missing, a node fails under ``fail``, or
            the ready set is empty before every node has run.
    """
    if limit < 1:
        raise GraphError("limit must be at least 1")
    chosen = executor if executor is not None else LocalExecutor()
    values = _seed(definition, seeds)
    side = dict(definition.side)
    outputs: dict[str, Mapping[str, object]] = {}
    pending = {node.id: node for node in definition.nodes}
    semaphore = asyncio.Semaphore(limit)
    publish = asyncio.Lock()
    run_id = uuid.uuid4().hex
    while pending:
        ready = [node for node in pending.values() if _inputs_ready(node, values)]
        if not ready:
            names = ", ".join(sorted(pending))
            raise GraphError(f"no ready node; still waiting on {names}")
        await asyncio.gather(*[
            _launch(
                node,
                definition,
                values,
                side,
                outputs,
                chosen,
                observer,
                semaphore,
                publish,
                timeout,
                policy,
                run_id,
            )
            for node in ready
        ])
        for node in ready:
            pending.pop(node.id, None)
    return outputs


def _seed(
    definition: GraphDefinition,
    seeds: Mapping[str, Mapping[str, object]],
) -> Values:
    values: Values = {}
    provided: set[tuple[str, str]] = set()
    for node_id, ports in seeds.items():
        for port, value in ports.items():
            values[(node_id, port)] = value
            provided.add((node_id, port))
    missing = [
        f"{node}.{port}" for node, port in definition.entries if (node, port) not in provided
    ]
    if missing:
        raise GraphError("missing inputs " + ", ".join(missing))
    return values


def _inputs_ready(node: Node, values: Values) -> bool:
    return all((node.id, port.name) in values for port in node.inputs)


async def _launch(
    node: Node,
    definition: GraphDefinition,
    values: Values,
    side: Mapping[str, object],
    outputs: dict[str, Mapping[str, object]],
    executor: Executor,
    observer: Observer | None,
    semaphore: asyncio.Semaphore,
    publish: asyncio.Lock,
    timeout: float | None,
    policy: str,
    run_id: str,
) -> None:
    inputs = {port.name: values[(node.id, port.name)] for port in node.inputs}
    if observer is not None:
        observer.on_start(definition, node.id, run_id=run_id)
    async with semaphore:
        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(executor.run(node, inputs, side), timeout)
        except Exception as exc:
            duration_s = time.perf_counter() - started
            if observer is not None:
                observer.on_exception(
                    definition,
                    node.id,
                    type(exc).__name__,
                    run_id=run_id,
                    duration_s=duration_s,
                )
            if policy == "fail":
                raise GraphError(f"node {node.id} failed: {type(exc).__name__}") from exc
            return
        duration_s = time.perf_counter() - started
    if observer is not None:
        observer.on_success(definition, node.id, run_id=run_id, duration_s=duration_s)
    async with publish:
        for edge in definition.edges:
            if edge.source == node.id and edge.source_port in result:
                values[(edge.target, edge.target_port)] = result[edge.source_port]
        outputs[node.id] = dict(result)
