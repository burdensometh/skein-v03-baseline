"""Politeness scheduling: per-host spacing, Crawl-delay and per-host concurrency across processes."""

from skein.politeness.policy import PolitenessPolicy
from skein.politeness.scheduler import HostScheduler, HostSlot, HostState

__all__ = ["HostScheduler", "HostSlot", "HostState", "PolitenessPolicy"]
