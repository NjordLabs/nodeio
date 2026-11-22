"""Per-edge queues. A bound makes the producer wait."""

from __future__ import annotations

import asyncio

Key = tuple[str, str]


class EdgeBuffer:
    """Values waiting on an edge, counted per target port.

    ``bound`` is the maximum number of values queued for one key.
    ``None`` does not limit the queue. A full queue waits; it does not
    drop the value.

    Attributes:
        high_water: Largest number of values queued for any one key.
    """

    def __init__(self) -> None:
        """Start with empty queues."""
        self._items: dict[Key, list[object]] = {}
        self._conds: dict[Key, asyncio.Condition] = {}
        self.high_water = 0

    def ready(self, key: Key) -> bool:
        """Return whether ``key`` has a queued value.

        Args:
            key: Target node id and port name.

        Returns:
            True when :meth:`get` would not have to wait.
        """
        return bool(self._items.get(key))

    async def put(self, key: Key, value: object, bound: int | None) -> None:
        """Queue ``value`` on ``key``, waiting while the bound is full.

        Args:
            key: Target node id and port name.
            value: Payload. ``None`` is a real value.
            bound: Maximum queued values. ``None`` does not wait.
        """
        cond = self._condition(key)
        async with cond:
            while bound is not None and len(self._items.get(key, ())) >= bound:
                await cond.wait()
            bucket = self._items.setdefault(key, [])
            bucket.append(value)
            self.high_water = max(self.high_water, len(bucket))
            cond.notify_all()

    async def get(self, key: Key) -> object:
        """Remove and return the next value for ``key``.

        Args:
            key: Target node id and port name.

        Returns:
            The oldest queued value.
        """
        cond = self._condition(key)
        async with cond:
            while not self._items.get(key):
                await cond.wait()
            value = self._items[key].pop(0)
            cond.notify_all()
            return value

    def _condition(self, key: Key) -> asyncio.Condition:
        cond = self._conds.get(key)
        if cond is None:
            cond = asyncio.Condition()
            self._conds[key] = cond
        return cond
