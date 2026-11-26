"""Ready-queue execution. One coordinator publishes outputs."""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from nodeio.errors import GraphError
from nodeio.executors import Executor, LocalExecutor
from nodeio.flow import EdgeBuffer
from nodeio.graph import Edge, GraphDefinition, Node
from nodeio.notify import _execute, _notify_start, _notify_success
from nodeio.observe import Observer
from nodeio.publish import _publish

Values = dict[tuple[str, str], object]
Policy = Literal["fail", "collect"]
OutputCallback = Callable[[str, str, object], Awaitable[None] | None]


@dataclass(frozen=True)
class RunOptions:  # pylint: disable=too-many-instance-attributes
    """Options the active :func:`run` is using.

    A subgraph copies these into the child run so limit, timeout, policy,
    observer, and executor stay the same. Thread and process workers cannot
    be stopped by cancellation; they run until they return.

    Attributes:
        limit: Maximum number of nodes running at once.
        timeout: Seconds for one node. ``None`` waits without a limit.
        policy: ``fail`` or ``collect``.
        observer: Optional start, success, and exception hook.
        executor: Dispatcher for this run. ``None`` when no run is active.
        cancel: Event that cancels in-flight async node tasks.
        queue_size: Default maximum queued values on an edge.
        on_output: Called when a node publishes an output.
    """

    limit: int = 8
    timeout: float | None = None
    policy: Policy = "fail"
    observer: Observer | None = None
    executor: Executor | None = None
    cancel: asyncio.Event | None = None
    queue_size: int | None = None
    on_output: OutputCallback | None = None


_OPTIONS: contextvars.ContextVar[RunOptions | None] = contextvars.ContextVar(
    "nodeio_run_options",
    default=None,
)


def current_options() -> RunOptions:
    """Return the options of the active run, or the defaults.

    Returns:
        The options set by :func:`run`, or defaults matching that function.
    """
    options = _OPTIONS.get()
    if options is None:
        return RunOptions()
    return options


@dataclass
class _State:  # pylint: disable=too-many-instance-attributes
    """Mutable values for one invocation of :func:`run`."""

    definition: GraphDefinition
    values: Values
    side: Mapping[str, object]
    outputs: dict[str, Mapping[str, object]]
    pending: dict[str, Node]
    failed: set[str]
    executor: Executor
    observer: Observer | None
    semaphore: asyncio.Semaphore
    publish: asyncio.Lock
    timeout: float | None
    policy: Policy
    run_id: str
    cancel: asyncio.Event | None
    queue_size: int | None
    on_output: OutputCallback | None
    queues: EdgeBuffer


async def run(
    definition: GraphDefinition,
    seeds: Mapping[str, Mapping[str, object]],
    *,
    executor: Executor | None = None,
    observer: Observer | None = None,
    limit: int = 8,
    timeout: float | None = None,
    policy: str = "fail",
    cancel: asyncio.Event | None = None,
    queue_size: int | None = None,
    on_output: OutputCallback | None = None,
) -> dict[str, Mapping[str, object]]:
    """Run ``definition`` once.

    Concurrent runs must not share a seed mapping. The definition is
    immutable and can be shared. An executor created for this call is
    closed when the call ends. A caller-supplied executor is left open.

    Cancellation of this task, or setting ``cancel``, cancels in-flight
    async node tasks. ``close`` still runs for a lifecycle node. Thread
    and process workers cannot be stopped by cancellation; they run
    until they return.

    Args:
        definition: Validated graph.
        seeds: External inputs, keyed by node id then port name.
        executor: Node dispatcher. ``None`` uses :class:`LocalExecutor`.
        observer: Optional start, success, and exception hook.
        limit: Maximum number of nodes running at once.
        timeout: Seconds for one node. ``None`` waits without a limit.
        policy: ``fail`` raises on the first node error. ``collect``
            records the error, skips dependents, and returns the rest.
        cancel: When set, in-flight async node tasks are cancelled.
        queue_size: Default bound on values waiting on an edge.
            ``None`` does not bound the edge. A full edge makes the
            producer wait instead of dropping the value.
        on_output: Called with the node id, port, and value when a node
            publishes an output.

    Returns:
        Output mappings keyed by node id. Nodes that failed or were
        skipped under ``collect`` are absent.

    Raises:
        GraphError: A seed is missing, ``policy`` is unknown, a node
            fails under ``fail``, or the ready set is empty before
            every runnable node has run.
    """
    if policy not in {"fail", "collect"}:
        raise GraphError("policy must be fail or collect")
    if limit < 1:
        raise GraphError("limit must be at least 1")
    if queue_size is not None and queue_size < 1:
        raise GraphError("queue_size must be at least 1")
    chosen_policy: Policy = "collect" if policy == "collect" else "fail"
    created: LocalExecutor | None = None
    if executor is None:
        created = LocalExecutor()
        chosen: Executor = created
    else:
        chosen = executor
    options = RunOptions(
        limit=limit,
        timeout=timeout,
        policy=chosen_policy,
        observer=observer,
        executor=chosen,
        cancel=cancel,
        queue_size=queue_size,
        on_output=on_output,
    )
    try:
        state = _state(definition, seeds, chosen, options)
        token = _OPTIONS.set(options)
        try:
            await _schedule(state)
        finally:
            _OPTIONS.reset(token)
        return state.outputs
    finally:
        if created is not None:
            created.close()


def _state(
    definition: GraphDefinition,
    seeds: Mapping[str, Mapping[str, object]],
    executor: Executor,
    options: RunOptions,
) -> _State:
    return _State(
        definition=definition,
        values=_seed(definition, seeds),
        side=dict(definition.side),
        outputs={},
        pending={node.id: node for node in definition.nodes},
        failed=set(),
        executor=executor,
        observer=options.observer,
        semaphore=asyncio.Semaphore(options.limit),
        publish=asyncio.Lock(),
        timeout=options.timeout,
        policy=options.policy,
        run_id=uuid.uuid4().hex,
        cancel=options.cancel,
        queue_size=options.queue_size,
        on_output=options.on_output,
        queues=EdgeBuffer(),
    )


def _accept_seed(
    key: tuple[str, str], known: set[tuple[str, str]], connected: set[tuple[str, str]]
) -> None:
    """Reject a seed that names an unknown port or a port an edge already fills.

    A connected port is filled by the upstream node. Seeding it as well
    made the queue and the seed disagree about which value was ready.

    Args:
        key: Node id and input port.
        known: Input ports that exist on the graph.
        connected: Input ports that already have an incoming edge.

    Raises:
        GraphError: The port does not exist, or an edge already targets it.
    """
    node_id, port = key
    if key not in known:
        raise GraphError(f"unknown input {node_id}.{port}")
    if key in connected:
        raise GraphError(f"connected input {node_id}.{port} cannot be seeded")


def _seed(definition: GraphDefinition, seeds: Mapping[str, Mapping[str, object]]) -> Values:
    known = {(node.id, port.name) for node in definition.nodes for port in node.inputs}
    connected = {(edge.target, edge.target_port) for edge in definition.edges}
    values: Values = {}
    provided: set[tuple[str, str]] = set()
    for node_id, ports in seeds.items():
        for port, value in ports.items():
            key = (node_id, port)
            _accept_seed(key, known, connected)
            values[key] = value
            provided.add(key)
    missing = [
        f"{node}.{port}" for node, port in definition.entries if (node, port) not in provided
    ]
    if missing:
        raise GraphError("missing inputs " + ", ".join(missing))
    return values


async def _schedule(state: _State) -> None:
    while state.pending:
        _ensure_running(state.cancel)
        if state.policy == "collect":
            _drop_blocked(state.pending, state.failed, state.definition.edges)
        if not state.pending:
            return
        ready = [
            node
            for node in state.pending.values()
            if _inputs_ready(node, state.values, state.queues)
        ]
        if not ready:
            names = ", ".join(sorted(state.pending))
            raise GraphError(f"no ready node; still waiting on {names}")
        await _launch_all(ready, state)
        for node in ready:
            state.pending.pop(node.id, None)


def _ensure_running(cancel: asyncio.Event | None) -> None:
    if _cancel_requested(cancel):
        raise asyncio.CancelledError


def _cancel_requested(cancel: asyncio.Event | None) -> bool:
    if cancel is not None and cancel.is_set():
        return True
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _drop_blocked(pending: dict[str, Node], failed: set[str], edges: tuple[Edge, ...]) -> None:
    changed = True
    while changed:
        changed = False
        for node_id in list(pending):
            if _source_failed(node_id, failed, edges):
                failed.add(node_id)
                pending.pop(node_id, None)
                changed = True


def _source_failed(node_id: str, failed: set[str], edges: tuple[Edge, ...]) -> bool:
    return any(edge.target == node_id and edge.source in failed for edge in edges)


def _inputs_ready(node: Node, values: Values, queues: EdgeBuffer) -> bool:
    for port in node.inputs:
        key = (node.id, port.name)
        if key not in values and not queues.ready(key):
            return False
    return True


async def _launch_all(ready: list[Node], state: _State) -> None:
    try:
        async with asyncio.TaskGroup() as group:
            for node in ready:
                group.create_task(_launch(node, state))
            if state.cancel is not None:
                group.create_task(_watch_cancel(state.cancel))
    except BaseExceptionGroup as group_exc:
        _raise_from_group(group_exc)


async def _watch_cancel(cancel: asyncio.Event) -> None:
    await cancel.wait()
    raise asyncio.CancelledError


def _raise_from_group(group_exc: BaseExceptionGroup[BaseException]) -> None:
    errors = [exc for exc in group_exc.exceptions if isinstance(exc, GraphError)]
    if errors:
        raise errors[0]
    cancelled = [exc for exc in group_exc.exceptions if isinstance(exc, asyncio.CancelledError)]
    if cancelled and len(cancelled) == len(group_exc.exceptions):
        raise cancelled[0]
    first = group_exc.exceptions[0]
    raise first


async def _launch(node: Node, state: _State) -> None:
    inputs = await _take_inputs(node, state)
    _notify_start(state.observer, state.definition, node.id, state.run_id)
    result = await _execute(node, inputs, state)
    if result is None:
        state.failed.add(node.id)
        return
    produced, duration_s = result
    _notify_success(state.observer, state.definition, node.id, state.run_id, duration_s)
    await _publish(state, node, produced)


async def _take_inputs(node: Node, state: _State) -> dict[str, object]:
    inputs: dict[str, object] = {}
    for port in node.inputs:
        key = (node.id, port.name)
        if state.queues.ready(key):
            inputs[port.name] = await state.queues.get(key)
        else:
            inputs[port.name] = state.values[key]
    return inputs
