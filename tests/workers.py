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
