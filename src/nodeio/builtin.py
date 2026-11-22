"""Built-in routing nodes and named subgraphs."""

from __future__ import annotations

from collections.abc import Mapping

from nodeio.errors import GraphError
from nodeio.graph import Edge, GraphDefinition, Node, Port, load

_SUBGRAPHS: dict[str, GraphDefinition] = {}


def register_subgraph(name: str, definition: GraphDefinition) -> None:
    """Store ``definition`` under ``name``.

    Args:
        name: Lookup key.
        definition: Graph used by :func:`subgraph_node`.

    Raises:
        ValueError: ``name`` is empty or already registered.
    """
    if name == "" or name in _SUBGRAPHS:
        raise ValueError(f"subgraph name is not available: {name!r}")
    _SUBGRAPHS[name] = definition


def subgraph(name: str) -> GraphDefinition:
    """Return a previously registered subgraph.

    Args:
        name: Key passed to :func:`register_subgraph`.

    Returns:
        The stored definition.
    """
    return _SUBGRAPHS[name]


def reset_subgraphs() -> None:
    """Drop every registered subgraph. Tests use this."""
    _SUBGRAPHS.clear()


def gate(node_id: str) -> Node:
    """Forward ``value`` when ``open`` is true.

    Args:
        node_id: Node id.

    Returns:
        A node with boolean ``open``, value ``value``, and output ``value``.
    """

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Forward the value when the gate is open.

        Returns:
            The output mapping.

        Raises:
            GraphError: ``open`` is not a bool.
        """
        _ = side
        opened = inputs["open"]
        if not isinstance(opened, bool):
            raise GraphError("open must be a bool")
        if opened:
            return {"value": inputs["value"]}
        return {"value": None}

    return Node(
        node_id,
        "gate",
        (Port("open", "bool"), Port("value", "any")),
        (Port("value", "any"),),
        runner,
    )


def mux(node_id: str) -> Node:
    """Choose ``left`` or ``right`` based on ``select``.

    Args:
        node_id: Node id.

    Returns:
        A node whose output ``value`` is one of the two inputs.
    """

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Select the left or right input.

        Returns:
            The output mapping.

        Raises:
            GraphError: ``select`` is not ``left`` or ``right``.
        """
        _ = side
        selected = inputs["select"]
        if selected not in {"left", "right"}:
            raise GraphError("select must be left or right")
        return {"value": inputs[str(selected)]}

    return Node(
        node_id,
        "mux",
        (Port("select", "str"), Port("left", "any"), Port("right", "any")),
        (Port("value", "any"),),
        runner,
    )


def demux(node_id: str) -> Node:
    """Copy ``value`` to the output named by ``key``.

    Args:
        node_id: Node id.

    Returns:
        A node with outputs ``left`` and ``right``. The other output is ``None``.
    """

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Route the value to the named output.

        Returns:
            The output mapping.
        """
        _ = side
        value = inputs["value"]
        if inputs["key"] == "left":
            return {"left": value, "right": None}
        return {"left": None, "right": value}

    return Node(
        node_id,
        "demux",
        (Port("key", "str"), Port("value", "any")),
        (Port("left", "any"), Port("right", "any")),
        runner,
    )


def subgraph_node(node_id: str, name: str) -> Node:
    """Run the subgraph registered as ``name``.

    The subgraph must have a single entry node ``in`` with port ``value``
    and a single output node ``out`` with port ``value``. The child run
    receives the parent's limit, timeout, policy, observer, and executor.

    Args:
        node_id: Node id in the parent graph.
        name: Subgraph key.

    Returns:
        A node that runs the child graph.
    """

    async def runner(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
        """Run the registered child graph once.

        Returns:
            The child output.

        Raises:
            GraphError: The child graph did not produce ``out.value``.
        """
        _ = side
        from nodeio.run import current_options, run

        options = current_options()
        child = subgraph(name)
        outputs = await run(
            child,
            {"in": {"value": inputs["value"]}},
            executor=options.executor,
            observer=options.observer,
            limit=options.limit,
            timeout=options.timeout,
            policy=options.policy,
            cancel=options.cancel,
            queue_size=options.queue_size,
            on_output=options.on_output,
        )
        if "out" not in outputs or "value" not in outputs["out"]:
            raise GraphError("subgraph produced no value")
        return {"value": outputs["out"]["value"]}

    return Node(
        node_id,
        "subgraph",
        (Port("value", "any"),),
        (Port("value", "any"),),
        runner,
    )


def linear(left: Node, right: Node) -> GraphDefinition:
    """Connect ``left.value`` to ``right.value`` for tests and subgraphs.

    Args:
        left: Upstream node. Must have an output named ``value``.
        right: Downstream node. Must have an input named ``value``.

    Returns:
        A loaded definition. ``left`` inputs are external entries.
    """
    entries = tuple((left.id, port.name) for port in left.inputs)
    edge = Edge(left.id, "value", right.id, "value")
    return load((left, right), (edge,), entries)
