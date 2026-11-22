"""Immutable graph definitions.

A definition is checked once. Each run receives its own values.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from nodeio.errors import GraphError

Runner = Callable[..., object]


@dataclass(frozen=True)
class Port:
    """A named, typed connection point.

    Attributes:
        name: Port name, unique on its node.
        annotation: Type name checked against the other end of an edge.
    """

    name: str
    annotation: str


@dataclass(frozen=True)
class Edge:
    """A directed connection between two ports.

    Attributes:
        source: Node that produces the value.
        source_port: Output port name.
        target: Node that consumes the value.
        target_port: Input port name.
    """

    source: str
    source_port: str
    target: str
    target_port: str


@dataclass(frozen=True)
class Node:
    """One step in a graph.

    Attributes:
        id: Unique node id.
        kind: Registry key that built this node.
        inputs: Ports that must be ready before the node runs.
        outputs: Ports the runner may produce.
        runner: Called with the input mapping and the side-packet mapping.
        executor: ``async``, ``thread``, or ``process``.
    """

    id: str
    kind: str
    inputs: tuple[Port, ...] = ()
    outputs: tuple[Port, ...] = ()
    runner: Runner | None = None
    executor: str = "async"


@dataclass(frozen=True)
class GraphDefinition:
    """A validated graph. Instances share no mutable containers.

    Attributes:
        nodes: Nodes in insertion order.
        edges: Connections between ports.
        entries: ``(node id, port)`` pairs supplied by each run.
        side: Side packets copied as pairs so callers cannot mutate the graph.
    """

    nodes: tuple[Node, ...]
    edges: tuple[Edge, ...]
    entries: tuple[tuple[str, str], ...] = ()
    side: tuple[tuple[str, object], ...] = ()

    def to_dot(self) -> str:
        """Return a Graphviz digraph.

        Returns:
            A ``digraph`` body with one line per edge.
        """
        lines = ["digraph {"]
        for edge in self.edges:
            lines.append(f"  {edge.source} -> {edge.target};")
        lines.append("}")
        return "\n".join(lines)

    def to_mermaid(self) -> str:
        """Return a Mermaid flowchart.

        Returns:
            A ``flowchart LR`` body with one line per edge.
        """
        lines = ["flowchart LR"]
        for edge in self.edges:
            lines.append(f"  {edge.source} --> {edge.target}")
        return "\n".join(lines)


def load(
    nodes: tuple[Node, ...] | list[Node],
    edges: tuple[Edge, ...] | list[Edge],
    entries: tuple[tuple[str, str], ...] | list[tuple[str, str]] = (),
    side: Mapping[str, object] | None = None,
) -> GraphDefinition:
    """Validate a graph and return an immutable definition.

    Args:
        nodes: Nodes. Ids must be unique.
        edges: Connections. Port names and type names must match.
        entries: External inputs filled by each run.
        side: Side packets. Stored as pairs.

    Returns:
        A definition that later runs can share.
    """
    node_tuple = tuple(nodes)
    edge_tuple = tuple(edges)
    entry_tuple = tuple(entries)
    _unique_ids(node_tuple)
    by_id = {node.id: node for node in node_tuple}
    _check_edges(by_id, edge_tuple)
    _check_entries(by_id, entry_tuple, edge_tuple)
    _check_cycle(node_tuple, edge_tuple)
    side_pairs = tuple((key, side[key]) for key in side) if side else ()
    return GraphDefinition(node_tuple, edge_tuple, entry_tuple, side_pairs)


def _unique_ids(nodes: tuple[Node, ...]) -> None:
    seen: set[str] = set()
    for node in nodes:
        if node.id in seen:
            raise GraphError(f"duplicate node id {node.id}")
        seen.add(node.id)


def _port(node: Node, name: str, outputs: bool) -> Port:
    ports = node.outputs if outputs else node.inputs
    for port in ports:
        if port.name == name:
            return port
    side = "output" if outputs else "input"
    raise GraphError(f"{node.id} has no {side} port {name}")


def _check_edges(by_id: dict[str, Node], edges: tuple[Edge, ...]) -> None:
    for edge in edges:
        if edge.source not in by_id or edge.target not in by_id:
            raise GraphError(
                f"edge names an unknown node {edge.source} -> {edge.target}"
            )
        source = _port(by_id[edge.source], edge.source_port, outputs=True)
        target = _port(by_id[edge.target], edge.target_port, outputs=False)
        if source.annotation != target.annotation:
            message = (
                f"type mismatch {edge.source}.{edge.source_port} -> "
                f"{edge.target}.{edge.target_port}"
            )
            raise GraphError(message)


def _check_entries(
    by_id: dict[str, Node],
    entries: tuple[tuple[str, str], ...],
    edges: tuple[Edge, ...],
) -> None:
    connected = {(edge.target, edge.target_port) for edge in edges}
    declared = set(entries)
    for node_id, port in entries:
        if node_id not in by_id:
            raise GraphError(f"entry names an unknown node {node_id}")
        _port(by_id[node_id], port, outputs=False)
    for node in by_id.values():
        for input_port in node.inputs:
            key = (node.id, input_port.name)
            if key not in connected and key not in declared:
                raise GraphError(f"unconnected input {node.id}.{input_port.name}")


def _check_cycle(nodes: tuple[Node, ...], edges: tuple[Edge, ...]) -> None:
    indegree = {node.id: 0 for node in nodes}
    adjacent: dict[str, list[str]] = {node.id: [] for node in nodes}
    for edge in edges:
        indegree[edge.target] += 1
        adjacent[edge.source].append(edge.target)
    if _drain(indegree, adjacent) != len(nodes):
        remaining = {node_id for node_id, count in indegree.items() if count > 0}
        cycle = tuple(_name_cycle(adjacent, remaining))
        raise GraphError("cycle in graph", cycle=cycle)


def _drain(indegree: dict[str, int], adjacent: dict[str, list[str]]) -> int:
    ready = [node_id for node_id, count in indegree.items() if count == 0]
    seen = 0
    while ready:
        current = ready.pop()
        seen += 1
        _release(adjacent[current], indegree, ready)
    return seen


def _release(targets: list[str], indegree: dict[str, int], ready: list[str]) -> None:
    for nxt in targets:
        indegree[nxt] -= 1
        if indegree[nxt] == 0:
            ready.append(nxt)


def _name_cycle(adjacent: dict[str, list[str]], remaining: set[str]) -> list[str]:
    for start in sorted(remaining):
        path = _walk(adjacent, remaining, start, [start])
        if path:
            return path
    return sorted(remaining)


def _walk(
    adjacent: dict[str, list[str]],
    remaining: set[str],
    node: str,
    path: list[str],
) -> list[str]:
    for nxt in adjacent[node]:
        if nxt not in remaining:
            continue
        if nxt in path:
            return path[path.index(nxt) :] + [nxt]
        found = _walk(adjacent, remaining, nxt, path + [nxt])
        if found:
            return found
    return []
