"""Sightings of items produced by *authority* fragment rules.

A partial item rule marked ``authority: true`` (typically the rule reading a
paginated listing) owns the membership of its item type: an item is alive
while some page of a run carries a fragment of it, and gets a tombstone when
the run ends without any sighting. A single page cannot answer "is this item
still listed?" once listings paginate, so the sightings of the whole run are
collected here and reconciled once when the run closes.
"""

from __future__ import annotations

from skein.store.database import execute_script

ITEM_SIGHTINGS = """
CREATE TABLE item_sightings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    spider       TEXT    NOT NULL,
    item_type    TEXT    NOT NULL,
    item_key     TEXT    NOT NULL,
    run_id       INTEGER NOT NULL REFERENCES runs(id),
    url          TEXT    NOT NULL,
    first_seen   REAL    NOT NULL,
    last_seen    REAL    NOT NULL,
    UNIQUE (spider, item_type, item_key, run_id)
);
CREATE INDEX item_sightings_run ON item_sightings(spider, run_id, item_type)
"""


def add_item_sightings(db) -> None:  # noqa: ANN001 - sqlite3.Connection
    execute_script(db, ITEM_SIGHTINGS)
