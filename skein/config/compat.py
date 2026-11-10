"""Loading version-1 configuration files through the version-2 schema.

:func:`upgrade_v1` moves every flat v1 key to its v2 group and returns the
deprecation notices the move produced. Unknown keys stay where they are,
so the v2 validator still reports them with a "did you mean" hint. Each
move is remembered in a :class:`PathMap`, which rewrites the JSON paths in
validation errors back to the location the user actually wrote, e.g.
``spiders[0].politeness.min_delay`` -> ``spiders[0].min_delay``.
"""

from __future__ import annotations

import copy
import warnings
from dataclasses import dataclass, field
from typing import Any

from skein.config.schema import ConfigError, Issue

# v1 spider key -> (v2 group, v2 key)
SPIDER_MOVES: dict[str, tuple[str, str]] = {
    "obey_robots": ("robots", "obey"),
    "use_robots_sitemaps": ("robots", "use_sitemaps"),
    "min_delay": ("politeness", "min_delay"),
    "max_crawl_delay": ("politeness", "max_crawl_delay"),
    "per_host_concurrency": ("politeness", "per_host_concurrency"),
    "max_retries": ("retry", "max_retries"),
    "retry_delay": ("retry", "backoff_base"),
    "sort_query": ("canonical", "sort_query"),
    "remove_params": ("canonical", "remove_params"),
    "keep_params": ("canonical", "keep_params"),
    "drop_empty_params": ("canonical", "drop_empty_params"),
    "keep_fragment": ("canonical", "keep_fragment"),
    "items": ("extract", "items"),
    "links": ("extract", "links"),
    "obey_nofollow": ("extract", "obey_nofollow"),
    "obey_noindex": ("extract", "obey_noindex"),
    "deny_extensions": ("extract", "deny_extensions"),
}

# v1 top-level key -> (v2 group, v2 key)
PROJECT_MOVES: dict[str, tuple[str, str]] = {"robots_ttl": ("robots", "ttl")}

# v1 retried with a fixed delay; the closest v2 behaviour is a constant back-off.
V1_RETRY_DEFAULTS = {"backoff_factor": 1.0, "jitter": 0.0}


class ConfigDeprecationWarning(DeprecationWarning):
    """A configuration file uses a deprecated version or key; it still loads."""


@dataclass
class PathMap:
    """Rewrites v2 JSON paths of validation issues to the v1 paths they came from."""

    prefixes: list[tuple[str, str]] = field(default_factory=list)

    def add(self, new: str, old: str) -> None:
        self.prefixes.append((new, old))

    def rewrite(self, path: str) -> str:
        best: tuple[str, str] | None = None
        for new, old in self.prefixes:
            if path == new or path.startswith(new + ".") or path.startswith(new + "["):
                if best is None or len(new) > len(best[0]):
                    best = (new, old)
        if best is None:
            return path
        return best[1] + path[len(best[0]):]

    def translate(self, error: ConfigError) -> ConfigError:
        issues = [Issue(self.rewrite(issue.path), issue.message) for issue in error.issues]
        return ConfigError(issues, error.source)


def _move(container: dict[str, Any], moves: dict[str, tuple[str, str]], prefix: str, paths: PathMap,
          notices: list[str]) -> None:
    for old_key, (group, new_key) in moves.items():
        if old_key not in container:
            continue
        value = container.pop(old_key)
        target = container.setdefault(group, {})
        if not isinstance(target, dict):
            # Leave the clash for the validator to report against the group.
            container[old_key] = value
            continue
        target[new_key] = value
        paths.add(f"{prefix}{group}.{new_key}", f"{prefix}{old_key}")
        notices.append(f"{prefix}{old_key} is deprecated; use {prefix}{group}.{new_key}")


def upgrade_v1(data: dict[str, Any]) -> tuple[dict[str, Any], PathMap, list[str]]:
    """Translate a decoded v1 document into v2 shape (the input is not modified)."""
    upgraded = copy.deepcopy(data)
    paths = PathMap()
    notices = ["configuration version 1 is deprecated; settings are now grouped (see README, configuration version 2)"]
    upgraded["version"] = 2
    _move(upgraded, PROJECT_MOVES, "", paths, notices)
    spiders = upgraded.get("spiders")
    if isinstance(spiders, list):
        for index, spider in enumerate(spiders):
            if not isinstance(spider, dict):
                continue
            prefix = f"spiders[{index}]."
            _move(spider, SPIDER_MOVES, prefix, paths, notices)
            retry = spider.setdefault("retry", {})
            if isinstance(retry, dict):
                for key, value in V1_RETRY_DEFAULTS.items():
                    retry.setdefault(key, value)
                base = retry.get("backoff_base")
                if isinstance(base, (int, float)) and not isinstance(base, bool):
                    retry.setdefault("backoff_max", max(300.0, float(base)))
    return upgraded, paths, notices


def upgraded_document(data: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """A version-2 document equivalent to the v1 ``data``, for writing back to disk.

    Version-2 documents pass through unchanged (with no notices).
    """
    if data.get("version") == 2:
        return copy.deepcopy(data), []
    if data.get("version") != 1:
        raise ConfigError([Issue("version", f"cannot upgrade configuration version {data.get('version')!r}")])
    upgraded, _, notices = upgrade_v1(data)
    return upgraded, notices


def emit(notices: list[str], source: str | None) -> None:
    """Issue each notice as a :class:`ConfigDeprecationWarning` naming the file."""
    where = f"{source}: " if source else ""
    for notice in notices:
        warnings.warn(f"{where}{notice}", ConfigDeprecationWarning, stacklevel=4)
