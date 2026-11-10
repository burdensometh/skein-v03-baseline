"""Schema for near-duplicate detection and WARC archiving.

``simhashes`` holds the 64-bit SimHash of every page's text and
``simhash_bands`` the same value cut into 16-bit bands: two hashes within
Hamming distance 3 agree on at least one of four bands, so candidates are
found with indexed equality lookups. Documents remember the near
duplicate they resemble, the SHA-1 payload digest of their last response
and the WARC record holding it (revisit records refer to it, also from
other URLs serving the identical payload). Fetch events point at their
WARC record by file name and offset.
"""

from __future__ import annotations

ARCHIVE = """
CREATE TABLE simhashes (
    spider      TEXT    NOT NULL,
    url         TEXT    NOT NULL,
    simhash     INTEGER NOT NULL,
    computed_at REAL    NOT NULL,
    PRIMARY KEY (spider, url)
);
CREATE TABLE simhash_bands (
    spider TEXT    NOT NULL,
    band   INTEGER NOT NULL,
    value  INTEGER NOT NULL,
    url    TEXT    NOT NULL,
    PRIMARY KEY (spider, band, value, url)
);
CREATE INDEX simhash_bands_by_url ON simhash_bands(spider, url);
ALTER TABLE documents ADD COLUMN near_duplicate_of TEXT;
ALTER TABLE documents ADD COLUMN near_distance INTEGER;
ALTER TABLE documents ADD COLUMN payload_digest TEXT;
ALTER TABLE documents ADD COLUMN warc_record_id TEXT;
ALTER TABLE documents ADD COLUMN warc_date TEXT;
ALTER TABLE fetch_events ADD COLUMN payload_digest TEXT;
ALTER TABLE fetch_events ADD COLUMN warc_file TEXT;
ALTER TABLE fetch_events ADD COLUMN warc_offset INTEGER;
ALTER TABLE fetch_events ADD COLUMN warc_length INTEGER;
ALTER TABLE fetch_events ADD COLUMN warc_revisit INTEGER NOT NULL DEFAULT 0;
CREATE INDEX fetch_events_by_warc ON fetch_events(warc_file, warc_offset);
CREATE INDEX documents_by_payload ON documents(spider, payload_digest);
CREATE TABLE warc_files (
    name      TEXT    PRIMARY KEY,
    directory TEXT    NOT NULL,
    worker_id TEXT,
    opened_at REAL    NOT NULL,
    closed_at REAL,
    records   INTEGER NOT NULL DEFAULT 0,
    bytes     INTEGER NOT NULL DEFAULT 0
)
"""
