"""Build nodes by kind. A graph can mix kinds."""

from __future__ import annotations

from collections.abc import Callable

from pymjolnir import Registry

from nodeio.graph import Node

Creator = Callable[..., Node]
NODES: Registry[Node] = Registry()


def register(kind: str, creator: Creator) -> None:
    """Register ``creator`` under ``kind``.

    Args:
        kind: Key stored on the node.
        creator: Called with the node id and a mapping of options.
    """
    NODES.register(kind, creator)


def create(kind: str, node_id: str, **options: object) -> Node:
    """Build the node registered at ``kind``.

    Args:
        kind: Registry key.
        node_id: Id assigned to the new node.
        options: Forwarded to the creator.

    Returns:
        The node the creator returned.
    """
    return NODES.create(kind, node_id, **options)
