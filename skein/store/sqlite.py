"""The :class:`Store`: one SQLite database holding every spider's crawl results and state."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from skein.extract.extractor import ExtractedItem
from skein.incremental.revisit import RevisitPolicy
from skein.store.cursors import CursorStore
from skein.store.database import StoreError, Transaction, connect
from skein.store.documents import DocumentChange, DocumentStore, FetchEvent
from skein.store.items import ItemStore, ItemWrite, PageItems
from skein.store.links import LinkStore
from skein.store.migrations import Migration, Migrator
from skein.store.rekey import CanonicalStore
from skein.store.robots import RobotsStore
from skein.store.runs import RunLog
from skein.store.schema import MIGRATIONS
from skein.store.watermarks import WatermarkStore
from skein.store.workers import WorkerRegistry

DEFAULT_REVISIT = RevisitPolicy()


@dataclass(frozen=True)
class FetchWrite:
    """What recording one fetch did to documents and items."""

    document: DocumentChange
    items: ItemWrite


class Store:
    """A SQLite database in WAL mode, migrated to the current schema on open.

    Repositories are exposed as attributes (``runs``, ``documents``,
    ``items``, ``watermarks``); they share one connection, so wrapping
    several calls in :meth:`transaction` makes them atomic together. Before
    an existing database is migrated, a backup copy is written next to it
    (``<name>.schema-v<N>.bak``) unless ``backup=False``.
    """

    def __init__(self, path: str | Path, *, backup: bool = True, synchronous: str = "normal") -> None:
        self.path = str(path)
        self.db = connect(self.path, synchronous=synchronous)
        try:
            self.migrator = Migrator(self.db, MIGRATIONS)
            version = self.migrator.current_version()
            target = None
            if backup and self.path != ":memory:":
                target = Path(f"{self.path}.schema-v{version}.bak")
            self.applied: list[Migration] = self.migrator.migrate(backup_path=target)
            self.backup_path = self.migrator.last_backup
        except (StoreError, sqlite3.DatabaseError) as exc:
            self.db.close()
            if isinstance(exc, StoreError):
                raise
            raise StoreError(f"cannot open database {self.path}: {exc}") from exc
        self.runs = RunLog(self.db)
        self.documents = DocumentStore(self.db)
        self.items = ItemStore(self.db)
        self.watermarks = WatermarkStore(self.db)
        self.robots = RobotsStore(self.db)
        self.links = LinkStore(self.db)
        self.cursors = CursorStore(self.db)
        self.workers = WorkerRegistry(self.db)
        self.canonical = CanonicalStore(self.db)

    def transaction(self) -> Transaction:
        return Transaction(self.db)

    def close(self) -> None:
        """Close the connection, rolling back a transaction left open by an error."""
        if self.db.in_transaction:
            self.db.execute("ROLLBACK")
        self.db.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def record_fetch(
        self,
        event: FetchEvent,
        items: list[ExtractedItem] | None = None,
        *,
        run_id: int | None = None,
        revisit: RevisitPolicy = DEFAULT_REVISIT,
        complete_types: tuple[str, ...] = (),
        page_url: str | None = None,
    ) -> FetchWrite:
        """Persist a fetch event, update the document state and the items atomically.

        After a 304 the live items previously extracted from the URL are
        only marked as seen; otherwise the extracted ``items`` are upserted.
        ``complete_types`` names the item types whose rules ran over the
        whole page at ``page_url`` (default: the event URL): items of those
        types that the page used to yield but no longer does are retired.
        """
        with self.transaction():
            change = self.documents.apply(event, run_id, revisit)
            (written,) = self.items.write([page_items(event, change.event_id, items or [], run_id=run_id,
                                                      complete_types=complete_types, page_url=page_url)])
        return FetchWrite(change, written)

    def spiders(self) -> list[str]:
        rows = self.db.execute(
            "SELECT spider FROM item_history UNION SELECT spider FROM fetch_events UNION SELECT spider FROM runs "
            "ORDER BY 1"
        ).fetchall()
        return [row[0] for row in rows]


def page_items(
    event: FetchEvent,
    event_id: int,
    items: list[ExtractedItem],
    *,
    run_id: int | None,
    complete_types: tuple[str, ...] = (),
    page_url: str | None = None,
) -> PageItems:
    """The item write a recorded fetch implies: a 304 touches the page's items, content upserts them."""
    return PageItems(
        spider=event.spider,
        url=page_url or event.url,
        observed_at=event.fetched_at,
        items=[] if event.not_modified else items,
        complete_types=() if event.not_modified else complete_types,
        not_modified=event.not_modified,
        run_id=run_id,
        fetch_event_id=event_id,
    )
