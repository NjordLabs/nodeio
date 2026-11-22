"""Publish one node's outputs onto the edges that read them."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import TYPE_CHECKING

from nodeio.graph import Edge, Node

if TYPE_CHECKING:
    from nodeio.run import OutputCallback, _State


async def _publish(state: _State, node: Node, produced: Mapping[str, object]) -> None:
    """Copy produced values onto outgoing edges and the output callback.

    Args:
        state: Active run.
        node: Node that produced the values.
        produced: Output name to value.
    """
    deliveries: list[tuple[Edge, object]] = []
    async with state.publish:
        for edge in state.definition.edges:
            if edge.source == node.id and edge.source_port in produced:
                deliveries.append((edge, produced[edge.source_port]))
        state.outputs[node.id] = dict(produced)
    for edge, value in deliveries:
        await state.queues.put(
            (edge.target, edge.target_port),
            value,
            _bound(edge, state.queue_size),
        )
    await _emit_outputs(state.on_output, node.id, produced)


def _bound(edge: Edge, queue_size: int | None) -> int | None:
    """Return the queue bound for one edge.

    Args:
        edge: Edge that may set its own bound.
        queue_size: Run-level default bound.

    Returns:
        The edge bound when set, otherwise the run default.
    """
    if edge.queue_size is not None:
        return edge.queue_size
    return queue_size


async def _emit_outputs(
    callback: OutputCallback | None,
    node_id: str,
    produced: Mapping[str, object],
) -> None:
    """Call the output callback for each produced value.

    Args:
        callback: Optional run-level output callback.
        node_id: Node that produced the values.
        produced: Output name to value.
    """
    if callback is None:
        return
    for port, value in produced.items():
        result = callback(node_id, port, value)
        if isinstance(result, Awaitable):
            await result
