"""How a node body is invoked.

``async`` runners are awaited. Synchronous runners run in a worker
thread. ``process`` runners run in a process pool. Bytes can cross
that boundary through shared memory instead of being pickled.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import shared_memory
from typing import Protocol, cast

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

        Cancellation stops this coroutine. It does not stop a thread or
        process worker that has already started; those run until they
        return. ``close`` still runs when the runner is a lifecycle.
        """
        if node.runner is None:
            raise ValueError(f"{node.id} has no runner")
        life = _as_lifecycle(node.runner)
        if life is None:
            result = await self._invoke(node, node.runner, inputs, side)
        else:
            result = await self._lifecycle(node, life, inputs, side)
        if not isinstance(result, Mapping):
            raise TypeError(f"{node.id} runner must return a mapping")
        return result

    async def _lifecycle(
        self,
        node: Node,
        life: _Lifecycle,
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> object:
        await _drive(life.open)
        try:
            return await self._invoke(node, life.process, inputs, side)
        finally:
            await _drive_close(life.close)

    async def _invoke(
        self,
        node: Node,
        runner: Callable[..., object],
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> object:
        if node.executor == "async":
            return await runner(inputs, side)  # type: ignore[misc]
        if node.executor == "thread":
            return await _cancellable(None, functools.partial(runner, inputs, side))
        if node.executor == "process":
            return await self._in_process(runner, inputs, side)
        raise ValueError(f"unknown executor {node.executor}")

    async def _in_process(
        self,
        runner: Callable[..., object],
        inputs: Mapping[str, object],
        side: Mapping[str, object],
    ) -> object:
        shared: list[SharedBytes] = []
        try:
            payload = _share_into(inputs, shared)
            side_payload = _share_into(side, shared)
            pool = self._processes or ProcessPoolExecutor(max_workers=1)
            self._processes = pool
            result = await _cancellable(
                pool,
                functools.partial(_run_with_shared, runner, payload, side_payload),
            )
            return result
        finally:
            _unlink_all(shared)

    def close(self) -> None:
        """Shut down a process pool this executor created."""
        if self._processes is not None:
            self._processes.shutdown(wait=True)
            self._processes = None


async def _cancellable(pool: ProcessPoolExecutor | None, work: Callable[[], object]) -> object:
    """Run ``work`` on ``pool`` and cancel the future if this task is cancelled.

    A worker that has already started keeps running. ``future.cancel``
    only drops work that has not started.

    Args:
        pool: Executor. ``None`` uses the default thread pool.
        work: Callable with no arguments.

    Returns:
        Whatever ``work`` returns.

    Raises:
        asyncio.CancelledError: This task was cancelled. Queued work is dropped.
    """
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(pool, work)
    try:
        return await future
    except asyncio.CancelledError:
        future.cancel()
        raise


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


class _Lifecycle(Protocol):
    """A node body with open, process, and close."""

    def open(self) -> object:
        """Prepare the node before the first process call."""
        ...

    def process(self, inputs: Mapping[str, object], side: Mapping[str, object]) -> object:
        """Run the node once.

        Args:
            inputs: Input port values.
            side: Side packets.

        Returns:
            The output mapping.
        """
        ...

    def close(self) -> object:
        """Release resources opened for this run."""
        ...


def _as_lifecycle(runner: object) -> _Lifecycle | None:
    if not all(callable(getattr(runner, name, None)) for name in ("open", "process", "close")):
        return None
    return cast(_Lifecycle, runner)


async def _drive(func: Callable[[], object]) -> None:
    result = func()
    if isinstance(result, Awaitable):
        await result


async def _drive_close(func: Callable[[], object]) -> None:
    task = asyncio.current_task()
    pending = task.cancelling() if task is not None else 0
    if task is not None:
        while task.cancelling():
            task.uncancel()
    try:
        await _drive(func)
    finally:
        if task is not None and pending:
            task.cancel()


def _share_into(values: Mapping[str, object], shared: list[SharedBytes]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in values.items():
        if isinstance(value, bytes) and value:
            handle = SharedBytes.from_bytes(value)
            shared.append(handle)
            payload[key] = handle
        else:
            payload[key] = value
    return payload


def _unlink_all(shared: list[SharedBytes]) -> None:
    for handle in shared:
        handle.unlink()


def _restore(values: Mapping[str, object]) -> dict[str, object]:
    restored: dict[str, object] = {}
    for key, value in values.items():
        restored[key] = value.read() if isinstance(value, SharedBytes) else value
    return restored


def _run_with_shared(
    runner: Callable[..., object],
    inputs: dict[str, object],
    side: dict[str, object],
) -> object:
    return runner(_restore(inputs), _restore(side))
