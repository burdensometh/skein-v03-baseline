"""Item history as a slowly changing dimension of type 2.

Version 0.2 kept the current item state in ``items`` and every version in
``item_versions``. Both are replaced by ``item_history``: one row per
version with its validity interval ``[valid_from, valid_to)`` and an
``is_current`` flag (the row with ``valid_to IS NULL``). ``first_seen`` is
copied into every row of an item; ``last_seen`` advances on the current
row each time the item is seen unchanged. ``items`` survives as a view of
the current rows for ad-hoc SQL. Row ids keep the 0.2 version rowids, so
change-feed cursors stored by 0.2 stay valid.
"""

from __future__ import annotations

import sqlite3

from skein.store.database import execute_script

ITEM_HISTORY = """
CREATE TABLE item_history (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    spider         TEXT    NOT NULL,
    item_type      TEXT    NOT NULL,
    item_key       TEXT    NOT NULL,
    version        INTEGER NOT NULL,
    content_hash   TEXT    NOT NULL,
    data           TEXT    NOT NULL,
    url            TEXT    NOT NULL,
    valid_from     REAL    NOT NULL,
    valid_to       REAL,
    is_current     INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
    deleted        INTEGER NOT NULL DEFAULT 0,
    first_seen     REAL    NOT NULL,
    last_seen      REAL    NOT NULL,
    run_id         INTEGER REFERENCES runs(id),
    fetch_event_id INTEGER REFERENCES fetch_events(id),
    UNIQUE (spider, item_type, item_key, version),
    CHECK ((is_current = 1) = (valid_to IS NULL))
);
CREATE UNIQUE INDEX item_history_current ON item_history(spider, item_type, item_key) WHERE is_current = 1;
CREATE INDEX item_history_current_by_url ON item_history(spider, url) WHERE is_current = 1;
CREATE INDEX item_history_by_time ON item_history(spider, valid_from);
CREATE INDEX item_history_by_event ON item_history(fetch_event_id)
"""

ITEMS_VIEW = """
CREATE VIEW items AS
SELECT spider, item_type, item_key, url, data, first_seen, last_seen, content_hash, version,
       valid_from AS changed_at, CASE WHEN deleted = 1 THEN valid_from END AS deleted_at
FROM item_history WHERE is_current = 1
"""


def to_item_history(db: sqlite3.Connection) -> None:
    """Fold ``items`` and ``item_versions`` into ``item_history`` (validity intervals from version times)."""
    execute_script(db, ITEM_HISTORY)
    db.execute(
        """INSERT INTO item_history (id, spider, item_type, item_key, version, content_hash, data, url,
                                     valid_from, valid_to, is_current, deleted, first_seen, last_seen,
                                     run_id, fetch_event_id)
           SELECT v.rowid, v.spider, v.item_type, v.item_key, v.version, v.content_hash, v.data, v.url,
                  v.observed_at, nxt.observed_at, nxt.observed_at IS NULL, v.deleted,
                  COALESCE(i.first_seen, v.observed_at),
                  CASE WHEN nxt.observed_at IS NULL THEN COALESCE(i.last_seen, v.observed_at)
                       ELSE nxt.observed_at END,
                  v.run_id, v.fetch_event_id
           FROM item_versions v
           LEFT JOIN item_versions nxt ON nxt.spider = v.spider AND nxt.item_type = v.item_type
                AND nxt.item_key = v.item_key AND nxt.version = v.version + 1
           LEFT JOIN items i ON i.spider = v.spider AND i.item_type = v.item_type AND i.item_key = v.item_key
           ORDER BY v.rowid"""
    )
    db.execute("DROP TABLE item_versions")
    db.execute("DROP TABLE items")
    execute_script(db, ITEMS_VIEW)
