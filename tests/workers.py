"""Picklable node bodies for the process executor."""

from __future__ import annotations

from collections.abc import Mapping


def double(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Return twice the numeric ``value`` input.

    Args:
        inputs: Must contain ``value``.
        side: Ignored.

    Returns:
        A mapping with the doubled value.

    Raises:
        TypeError: ``value`` is not an int.
    """
    del side
    value = inputs["value"]
    if not isinstance(value, int):
        raise TypeError("value must be an int")
    return {"value": value * 2}


def echo_bytes(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Return the byte string passed as ``value``.

    Args:
        inputs: Must contain ``value``.
        side: Ignored.

    Returns:
        A mapping with that byte string.

    Raises:
        TypeError: ``value`` is not bytes.
    """
    del side
    value = inputs["value"]
    if not isinstance(value, bytes):
        raise TypeError("value must be bytes")
    return {"value": value}


def fail_process(inputs: Mapping[str, object], side: Mapping[str, object]) -> dict[str, object]:
    """Raise so the parent still releases shared memory.

    Args:
        inputs: Ignored.
        side: Ignored.

    Raises:
        RuntimeError: Always.
    """
    del inputs, side
    raise RuntimeError("process failed")
