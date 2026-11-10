"""Item change history: field-level differences between versions and a change feed.

Downstream consumers usually want "what changed since T": for each item
version observed after T, whether the item was created, updated, deleted
or restored, and which fields changed. Nested objects are compared key by
key (paths like ``dims.width``); lists are compared as whole values, since
element positions rarely mean anything stable in scraped data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator

from skein.store.items import ItemStore, ItemVersion

_MISSING: Any = object()


class ChangeKind(str, Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    RESTORED = "restored"


@dataclass(frozen=True)
class FieldChange:
    """One differing field; ``before``/``after`` are ``None`` for added/removed fields."""

    path: str
    before: Any
    after: Any
    kind: str

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "kind": self.kind, "before": self.before, "after": self.after}


@dataclass(frozen=True)
class ItemChange:
    """The transition into ``version`` of one item."""

    item_type: str
    key: str
    version: int
    kind: ChangeKind
    observed_at: float
    url: str
    run_id: int | None
    fields: list[FieldChange] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    position: int = 0


def diff_values(before: Any, after: Any, prefix: str = "") -> list[FieldChange]:
    """Field-level differences between two JSON values (dicts recurse, everything else compares whole)."""
    if isinstance(before, dict) and isinstance(after, dict):
        changes: list[FieldChange] = []
        for name in sorted(set(before) | set(after), key=str):
            path = f"{prefix}.{name}" if prefix else str(name)
            old, new = before.get(name, _MISSING), after.get(name, _MISSING)
            if old is _MISSING:
                changes.append(FieldChange(path, None, new, "added"))
            elif new is _MISSING:
                changes.append(FieldChange(path, old, None, "removed"))
            else:
                changes.extend(diff_values(old, new, path))
        return changes
    if before == after and type(before) is type(after):
        return []
    return [FieldChange(prefix or "$", before, after, "changed")]


def _transition(previous: ItemVersion | None, current: ItemVersion) -> ItemChange:
    if current.deleted:
        kind, fields = ChangeKind.DELETED, []
    elif previous is None:
        kind, fields = ChangeKind.CREATED, diff_values({}, current.data)
    elif previous.deleted:
        kind, fields = ChangeKind.RESTORED, diff_values(previous.data, current.data)
    else:
        kind, fields = ChangeKind.UPDATED, diff_values(previous.data, current.data)
    return ItemChange(current.item_type, current.key, current.version, kind, current.observed_at, current.url,
                      current.run_id, fields, current.data, current.position)


def item_history(items: ItemStore, spider: str, item_type: str, key: str) -> list[ItemChange]:
    """Every transition of one item, oldest first."""
    changes = []
    previous: ItemVersion | None = None
    for version in items.versions(spider, item_type=item_type, key=key):
        changes.append(_transition(previous, version))
        previous = version
    return changes


def change_feed(
    items: ItemStore,
    spider: str,
    *,
    since: float | None = None,
    item_type: str | None = None,
    after: int | None = None,
) -> Iterator[ItemChange]:
    """Transitions observed at or after ``since`` (or stored after feed position ``after``), in storage order.

    The version preceding the first one in the window is looked up so that
    the first change of each item is diffed against what consumers already
    had.
    """
    window = list(items.versions(spider, item_type=item_type, since=since, after=after))
    window.sort(key=lambda v: v.position)
    previous: dict[tuple[str, str], ItemVersion | None] = {}
    for version in window:
        ident = (version.item_type, version.key)
        if ident not in previous:
            previous[ident] = items.version(spider, version.item_type, version.key, version.version - 1)
        yield _transition(previous[ident], version)
        previous[ident] = version
