"""Property tests for graph validation."""

from __future__ import annotations

import importlib.util

import pytest

if importlib.util.find_spec("hypothesis") is None:
    pytest.skip("hypothesis is not installed", allow_module_level=True)
from hypothesis import given
from hypothesis import strategies as st

from nodeio import load
from tests.support import async_node

_NAME = st.text(alphabet="abc", min_size=1, max_size=8)


@given(_NAME)
def test_a_single_node_loads(name: str) -> None:
    """Any single node id loads."""
    definition = load(
        (async_node(name),),
        (),
        entries=((name, "value"),),
    )
    assert definition.nodes[0].id == name
