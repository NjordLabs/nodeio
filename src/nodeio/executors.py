"""How a node body is invoked.

``async`` runners are awaited. Synchronous runners run in a worker
thread. ``process`` runners run in a process pool. Bytes can cross
that boundary through shared memory instead of being pickled.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import shared_memory
from typing import Protocol

from nodeio.graph import Node


class Executor(Protocol):
    """Something that can run a node body.

    ``nodeio-workers`` can provide another implementation later.
    """

    async def run(
        self,
        node: Node,
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Run ``node`` and return its outputs.

        Args:
            node: Node whose runner is invoked.
            inputs: Values for the node's input ports.
            side: Side packets for this run.

        Returns:
            Values for the node's output ports.
        """


class LocalExecutor:
    """Await async runners and shift synchronous runners off the loop."""

    def __init__(self, processes: ProcessPoolExecutor | None = None) -> None:
        """Store an optional process pool.

        Args:
            processes: Pool for ``executor="process"`` nodes. ``None``
                creates a pool on first use.
        """
        self._processes = processes

    async def run(
        self,
        node: Node,
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Dispatch ``node`` according to its executor name.

        Args:
            node: Node to run.
            inputs: Input port values.
            side: Side packets.

        Returns:
            The runner's mapping.

        Raises:
            TypeError: The runner did not return a mapping.
            ValueError: ``node.executor`` is not known.
        """
        if node.runner is None:
            raise ValueError(f"{node.id} has no runner")
        result = await self._invoke(node, inputs, side)
        if not isinstance(result, Mapping):
            raise TypeError(f"{node.id} runner must return a mapping")
        return result

    async def _invoke(
        self,
        node: Node,
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> object:
        runner = node.runner
        if runner is None:
            raise ValueError(f"{node.id} has no runner")
        if node.executor == "async":
            return await runner(inputs, side)  # type: ignore[misc]
        if node.executor == "thread":
            return await asyncio.to_thread(runner, inputs, side)
        if node.executor == "process":
            pool = self._processes or ProcessPoolExecutor(max_workers=1)
            self._processes = pool
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(pool, runner, dict(inputs), dict(side))
        raise ValueError(f"unknown executor {node.executor}")

    def close(self) -> None:
        """Shut down a process pool this executor created."""
        if self._processes is not None:
            self._processes.shutdown(wait=True)
            self._processes = None


class SharedBytes:
    """A byte string stored in ``multiprocessing.shared_memory``.

    Attributes:
        name: Shared-memory block name.
        size: Number of payload bytes.
    """

    def __init__(self, name: str, size: int) -> None:
        """Record an existing block.

        Args:
            name: Block name.
            size: Payload length.
        """
        self.name = name
        self.size = size

    @classmethod
    def from_bytes(cls, payload: bytes) -> SharedBytes:
        """Copy ``payload`` into a new shared block.

        Args:
            payload: Bytes to publish.

        Returns:
            A handle the caller must :meth:`unlink`.

        Raises:
            RuntimeError: The new block has no buffer.
        """
        block = shared_memory.SharedMemory(create=True, size=len(payload))
        buffer = block.buf
        if buffer is None:
            raise RuntimeError("shared memory has no buffer")
        buffer[: len(payload)] = payload
        name = block.name
        block.close()
        return cls(name, len(payload))

    def read(self) -> bytes:
        """Read the payload.

        Returns:
            The bytes stored by :meth:`from_bytes`.

        Raises:
            RuntimeError: The block has no buffer.
        """
        block = shared_memory.SharedMemory(name=self.name)
        try:
            buffer = block.buf
            if buffer is None:
                raise RuntimeError("shared memory has no buffer")
            return bytes(buffer[: self.size])
        finally:
            block.close()

    def unlink(self) -> None:
        """Release the shared block."""
        block = shared_memory.SharedMemory(name=self.name)
        block.close()
        block.unlink()
