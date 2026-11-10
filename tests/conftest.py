"""Shared pytest fixtures."""

from __future__ import annotations

from typing import Callable, Iterator

import pytest

from skein.clock import FakeClock
from tests.fixtures_site import FixtureSite, SiteOptions, running_site


@pytest.fixture
def site() -> Iterator[FixtureSite]:
    with running_site() as running:
        yield running


@pytest.fixture
def make_site() -> Iterator[Callable[..., FixtureSite]]:
    """Factory for sites with custom :class:`SiteOptions`; all are stopped afterwards."""
    stack: list = []

    def factory(**options: object) -> FixtureSite:
        manager = running_site(SiteOptions(**options))  # type: ignore[arg-type]
        stack.append(manager)
        return manager.__enter__()

    yield factory
    for manager in reversed(stack):
        manager.__exit__(None, None, None)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()

