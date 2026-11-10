"""Items and their history as a slowly changing dimension of type 2 (``item_history``).

Every version of an item is one row with a validity interval: ``valid_from``
is when the version was first observed, ``valid_to`` when the next version
replaced it (``NULL`` while it is current, together with ``is_current = 1``).
A new version is only written when the canonical JSON of an item hashes
differently from its current version, so re-processing the same page (a
retry, a redelivered lease, an unchanged revisit) never adds versions.
Items that disappear from their page get a *tombstone* version
(``deleted = 1``) whose data is the last content seen.

Writes come in batches: :meth:`ItemStore.write` takes the item sets of many
pages, loads the current rows of every affected item with a few queries,
works out all version changes in memory (in page order, so an item seen on
two pages of one batch is handled exactly as if the pages had been written
one by one) and applies them with bulk statements.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

from skein.extract.extractor import ExtractedItem, content_hash

DELETED_HASH = "deleted"
_CHUNK = 400


@dataclass
class ItemWrite:
    """How one page's items changed the store."""

    new: int = 0
    updated: int = 0
    unchanged: int = 0
    touched: int = 0
    deleted: int = 0


@dataclass
class StoredItem:
    """The current version of one item."""

    spider: str
    item_type: str
    key: str
    url: str
    data: dict[str, Any]
    first_seen: float
    last_seen: float
    version: int = 1
    content_hash: str = ""
    changed_at: float | None = None
    deleted_at: float | None = None


@dataclass
class ItemVersion:
    """One row of an item's history; versions are numbered from 1 without gaps.

    ``valid_to`` is ``None`` for the current version. ``position`` is the
    row id, which orders the change feed.
    """

    spider: str
    item_type: str
    key: str
    version: int
    content_hash: str
    data: dict[str, Any]
    url: str
    observed_at: float
    run_id: int | None
    fetch_event_id: int | None
    deleted: bool = False
    position: int = 0
    valid_to: float | None = None


@dataclass
class PageItems:
    """The items one processed page yields, as handed to :meth:`ItemStore.write`.

    ``complete_types`` names the item types whose rules ran over the whole
    page: live items of those types last seen on ``url`` but missing from
    ``items`` are tombstoned. After a 304 (``not_modified``) the page's live
    items are only marked as seen.
    """

    spider: str
    url: str
    observed_at: float
    items: list[ExtractedItem] = field(default_factory=list)
    complete_types: tuple[str, ...] = ()
    not_modified: bool = False
    run_id: int | None = None
    fetch_event_id: int | None = None


@dataclass
class _Current:
    """In-memory image of an item's current row during a batch."""

    row_id: int | None
    version: int
    content_hash: str
    data: str
    url: str
    valid_from: float
    deleted: bool
    first_seen: float
    last_seen: float
    run_id: int | None
    fetch_event_id: int | None
    touched: bool = False


class ItemStore:
    """Repository for ``item_history``; callers provide the transaction."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    # -- writing ---------------------------------------------------------------

    def write(self, pages: list[PageItems]) -> list[ItemWrite]:
        """Apply the items of ``pages`` in order; returns one :class:`ItemWrite` per page."""
        current = self._load_current(pages)
        closed: list[tuple[float, int]] = []
        inserted: list[tuple[tuple[str, str, str], _Current]] = []
        results: list[ItemWrite] = []
        for page in pages:
            summary = ItemWrite()
            if page.not_modified:
                summary.touched = self._touch_page(page, current)
                results.append(summary)
                continue
            keep: set[tuple[str, str]] = set()
            for item in page.items:
                keep.add((item.item_type, item.key))
                ident = (page.spider, item.item_type, item.key)
                payload = item.data_json()
                digest = content_hash(payload)
                state = current.get(ident)
                if state is not None and state.content_hash == digest and not state.deleted:
                    state.last_seen = max(state.last_seen, page.observed_at)
                    state.url = item.url
                    state.touched = True
                    summary.unchanged += 1
                    continue
                if state is None:
                    summary.new += 1
                else:
                    summary.updated += 1
                self._supersede(ident, state, closed, inserted, current, page, payload, digest, item.url, False)
            for ident, state in list(current.items()):
                if (ident[0] != page.spider or state.deleted or state.url != page.url
                        or ident[1] not in page.complete_types or (ident[1], ident[2]) in keep):
                    continue
                self._supersede(ident, state, closed, inserted, current, page, state.data, DELETED_HASH, page.url,
                                True)
                summary.deleted += 1
            results.append(summary)
        self._flush(current, closed, inserted)
        return results

    def _supersede(
        self,
        ident: tuple[str, str, str],
        state: _Current | None,
        closed: list[tuple[float, int]],
        inserted: list[tuple[tuple[str, str, str], _Current]],
        current: dict[tuple[str, str, str], _Current],
        page: PageItems,
        payload: str,
        digest: str,
        url: str,
        deleted: bool,
    ) -> None:
        """Close ``state`` (if any) at the page's time and make a new current version."""
        now = page.observed_at
        if state is not None and state.row_id is not None:
            closed.append((now, state.row_id))
        if state is not None:
            state.touched = False
        replacement = _Current(
            row_id=None,
            version=(state.version + 1) if state is not None else 1,
            content_hash=digest,
            data=payload,
            url=url,
            valid_from=now,
            deleted=deleted,
            first_seen=state.first_seen if state is not None else now,
            last_seen=state.last_seen if (state is not None and deleted) else now,
            run_id=page.run_id,
            fetch_event_id=page.fetch_event_id,
        )
        inserted.append((ident, replacement))
        current[ident] = replacement

    def _touch_page(self, page: PageItems, current: dict[tuple[str, str, str], _Current]) -> int:
        touched = 0
        for ident, state in current.items():
            if ident[0] == page.spider and state.url == page.url and not state.deleted:
                state.last_seen = max(state.last_seen, page.observed_at)
                state.touched = True
                touched += 1
        return touched

    def _load_current(self, pages: list[PageItems]) -> dict[tuple[str, str, str], _Current]:
        """Current rows of every item named by, or last seen on, one of the pages."""
        wanted: dict[str, set[tuple[str, str]]] = {}
        urls: dict[str, set[str]] = {}
        for page in pages:
            wanted.setdefault(page.spider, set()).update((i.item_type, i.key) for i in page.items)
            if page.not_modified or page.complete_types:
                urls.setdefault(page.spider, set()).add(page.url)
        current: dict[tuple[str, str, str], _Current] = {}
        columns = ("id, spider, item_type, item_key, version, content_hash, data, url, valid_from, deleted, "
                   "first_seen, last_seen, run_id, fetch_event_id")
        for spider, keys in wanted.items():
            ordered = sorted(keys)
            for start in range(0, len(ordered), _CHUNK):
                chunk = ordered[start : start + _CHUNK]
                pairs = " OR ".join("(item_type = ? AND item_key = ?)" for _ in chunk)
                params: list[Any] = [spider]
                for item_type, key in chunk:
                    params.extend((item_type, key))
                rows = self.db.execute(
                    f"SELECT {columns} FROM item_history WHERE is_current = 1 AND spider = ? AND ({pairs})", params)
                current.update(_current_from(row) for row in rows)
        for spider, page_urls in urls.items():
            ordered_urls = sorted(page_urls)
            for start in range(0, len(ordered_urls), _CHUNK):
                chunk_urls = ordered_urls[start : start + _CHUNK]
                rows = self.db.execute(
                    f"""SELECT {columns} FROM item_history WHERE is_current = 1 AND spider = ?
                        AND url IN ({', '.join('?' * len(chunk_urls))})""",
                    [spider, *chunk_urls])
                current.update(_current_from(row) for row in rows)
        return current

    def _flush(
        self,
        current: dict[tuple[str, str, str], _Current],
        closed: list[tuple[float, int]],
        inserted: list[tuple[tuple[str, str, str], _Current]],
    ) -> None:
        self.db.executemany(
            "UPDATE item_history SET valid_to = ?, is_current = 0 WHERE id = ? AND is_current = 1", closed)
        successors = {(ident, state.version): state for ident, state in inserted}
        rows = []
        for ident, state in inserted:
            successor = successors.get((ident, state.version + 1))
            superseded = successor is not None
            valid_to = successor.valid_from if successor is not None else None
            rows.append((*ident, state.version, state.content_hash, state.data, state.url, state.valid_from,
                         valid_to, 0 if superseded else 1, int(state.deleted), state.first_seen, state.last_seen,
                         state.run_id, state.fetch_event_id))
        self.db.executemany(
            """INSERT INTO item_history (spider, item_type, item_key, version, content_hash, data, url, valid_from,
                                         valid_to, is_current, deleted, first_seen, last_seen, run_id, fetch_event_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.db.executemany(
            "UPDATE item_history SET last_seen = ?, url = ? WHERE id = ?",
            [(s.last_seen, s.url, s.row_id) for s in current.values() if s.touched and s.row_id is not None],
        )

    # -- reading ---------------------------------------------------------------

    def iter(
        self,
        spider: str,
        *,
        item_type: str | None = None,
        since: float | None = None,
        changed_since: float | None = None,
        include_deleted: bool = False,
    ) -> Iterator[StoredItem]:
        """Current item versions ordered by type and key.

        ``since`` filters on the last sighting, ``changed_since`` on the last
        content change (including deletion). Tombstoned items are left out
        unless ``include_deleted``.
        """
        clauses, params = _filters(spider, item_type, (("last_seen", since), ("valid_from", changed_since)))
        clauses.append("is_current = 1")
        if not include_deleted:
            clauses.append("deleted = 0")
        query = f"SELECT * FROM item_history WHERE {' AND '.join(clauses)} ORDER BY item_type, item_key"
        for row in self.db.execute(query, params):
            yield StoredItem(
                row["spider"], row["item_type"], row["item_key"], row["url"], json.loads(row["data"]),
                row["first_seen"], row["last_seen"], row["version"], row["content_hash"], row["valid_from"],
                row["valid_from"] if row["deleted"] else None,
            )

    def versions(
        self,
        spider: str,
        *,
        item_type: str | None = None,
        key: str | None = None,
        since: float | None = None,
        after: int | None = None,
    ) -> Iterator[ItemVersion]:
        """Every stored version, oldest first per item; ``after`` skips positions up to and including it."""
        clauses, params = _filters(spider, item_type, (("valid_from", since),))
        if key is not None:
            clauses.append("item_key = ?")
            params.append(key)
        if after is not None:
            clauses.append("id > ?")
            params.append(after)
        yield from self._version_rows(" AND ".join(clauses), params)

    def version(self, spider: str, item_type: str, key: str, version: int) -> ItemVersion | None:
        """One specific version, or ``None`` (also for version 0)."""
        found = list(self._version_rows(
            "spider = ? AND item_type = ? AND item_key = ? AND version = ?", [spider, item_type, key, version]))
        return found[0] if found else None

    def as_of(self, spider: str, moment: float, *, item_type: str | None = None) -> Iterator[ItemVersion]:
        """The version of every item that was valid at ``moment`` (tombstones excluded)."""
        clauses, params = _filters(spider, item_type, ())
        clauses.extend(("valid_from <= ?", "(valid_to IS NULL OR valid_to > ?)", "deleted = 0"))
        params.extend((moment, moment))
        yield from self._version_rows(" AND ".join(clauses), params)

    def _version_rows(self, where: str, params: list[Any]) -> Iterator[ItemVersion]:
        query = f"SELECT * FROM item_history WHERE {where} ORDER BY item_type, item_key, version"
        for row in self.db.execute(query, params):
            yield ItemVersion(
                row["spider"], row["item_type"], row["item_key"], row["version"], row["content_hash"],
                json.loads(row["data"]), row["url"], row["valid_from"], row["run_id"], row["fetch_event_id"],
                bool(row["deleted"]), row["id"], row["valid_to"],
            )

    def count(self, spider: str) -> int:
        """Live (not tombstoned) items of ``spider``."""
        row = self.db.execute(
            "SELECT COUNT(*) FROM item_history WHERE spider = ? AND is_current = 1 AND deleted = 0", (spider,))
        return int(row.fetchone()[0])

    def count_versions(self, spider: str) -> int:
        return int(self.db.execute("SELECT COUNT(*) FROM item_history WHERE spider = ?", (spider,)).fetchone()[0])


def _current_from(row: sqlite3.Row) -> tuple[tuple[str, str, str], _Current]:
    return (row["spider"], row["item_type"], row["item_key"]), _Current(
        row["id"], row["version"], row["content_hash"], row["data"], row["url"], row["valid_from"],
        bool(row["deleted"]), row["first_seen"], row["last_seen"], row["run_id"], row["fetch_event_id"],
    )


def _filters(
    spider: str, item_type: str | None, lower_bounds: Iterable[tuple[str, float | None]]
) -> tuple[list[str], list[Any]]:
    clauses = ["spider = ?"]
    params: list[Any] = [spider]
    if item_type is not None:
        clauses.append("item_type = ?")
        params.append(item_type)
    for column, bound in lower_bounds:
        if bound is not None:
            clauses.append(f"{column} >= ?")
            params.append(bound)
    return clauses, params
