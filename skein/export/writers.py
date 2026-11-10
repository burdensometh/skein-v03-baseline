"""Exporting items, item versions, fetch events and documents as JSON Lines or CSV."""

from __future__ import annotations

import csv
import json
from typing import Any, Iterable, TextIO

from skein.clock import isoformat
from skein.store.documents import DocumentState
from skein.store.items import ItemVersion, StoredItem

ITEM_COLUMNS = ("_type", "_key", "_url", "_first_seen", "_last_seen", "_version", "_changed_at")
VERSION_COLUMNS = ("_type", "_key", "_version", "_url", "_observed_at", "_valid_to", "_current", "_deleted",
                   "_run_id", "_hash")
EVENT_COLUMNS = (
    "url", "final_url", "kind", "depth", "status", "error", "error_class", "attempt", "conditional", "changed",
    "content_type", "charset", "content_length", "fetched_at", "elapsed", "redirects", "etag", "last_modified",
    "items_found", "links_found", "run_id",
)
DOCUMENT_COLUMNS = (
    "url", "kind", "status", "final_url", "etag", "last_modified", "fingerprint", "first_fetched", "last_fetched",
    "last_success", "last_changed", "fetch_count", "check_count", "change_count", "revisit_interval", "next_visit",
    "sitemap_lastmod", "error_class", "last_error",
)
FORMATS = ("jsonl", "csv")
_TIME_COLUMNS = frozenset({"fetched_at", "first_fetched", "last_fetched", "last_success", "last_changed",
                           "next_visit", "sitemap_lastmod"})


def _with_data(record: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    for name, value in data.items():
        record[name if name not in record else f"data.{name}"] = value
    return record


def item_record(item: StoredItem) -> dict[str, Any]:
    """Flat export record: metadata columns prefixed with ``_`` followed by the item data."""
    record: dict[str, Any] = {
        "_type": item.item_type,
        "_key": item.key,
        "_url": item.url,
        "_first_seen": isoformat(item.first_seen),
        "_last_seen": isoformat(item.last_seen),
        "_version": item.version,
        "_changed_at": isoformat(item.changed_at),
    }
    return _with_data(record, item.data)


def version_record(version: ItemVersion) -> dict[str, Any]:
    """One historical item version with its data and validity interval (SCD type 2 row)."""
    record: dict[str, Any] = {
        "_type": version.item_type,
        "_key": version.key,
        "_version": version.version,
        "_url": version.url,
        "_observed_at": isoformat(version.observed_at),
        "_valid_to": isoformat(version.valid_to),
        "_current": version.valid_to is None,
        "_deleted": version.deleted,
        "_run_id": version.run_id,
        "_hash": version.content_hash,
    }
    return _with_data(record, version.data)


def _timestamps(record: dict[str, Any]) -> dict[str, Any]:
    for column in _TIME_COLUMNS & record.keys():
        record[column] = isoformat(record[column])
    return record


def event_record(row: dict[str, Any]) -> dict[str, Any]:
    """Export form of a fetch event from :meth:`DocumentStore.events`."""
    record = {column: row.get(column) for column in EVENT_COLUMNS}
    if row.get("sitemap_lastmod") is not None:
        record["sitemap_lastmod"] = row["sitemap_lastmod"]
    return _timestamps(record)


def document_record(document: DocumentState) -> dict[str, Any]:
    data = document.as_dict()
    return _timestamps({column: data[column] for column in DOCUMENT_COLUMNS})


def write_jsonl(records: Iterable[dict[str, Any]], out: TextIO) -> int:
    """Write one JSON object per line; returns the number of records written."""
    count = 0
    for record in records:
        out.write(json.dumps(record, ensure_ascii=False))
        out.write("\n")
        count += 1
    return count


def flatten(value: Any, prefix: str, out: dict[str, str], list_separator: str) -> None:
    """Flatten nested objects into dotted columns; scalar lists are joined."""
    if isinstance(value, dict):
        for key, inner in value.items():
            flatten(inner, f"{prefix}.{key}" if prefix else str(key), out, list_separator)
    elif isinstance(value, list):
        if all(not isinstance(v, (dict, list)) for v in value):
            out[prefix] = list_separator.join("" if v is None else _scalar(v) for v in value)
        else:
            out[prefix] = json.dumps(value, ensure_ascii=False, sort_keys=True)
    elif value is not None:
        out[prefix] = _scalar(value)
    else:
        out.setdefault(prefix, "")


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_csv(
    records: Iterable[dict[str, Any]],
    out: TextIO,
    *,
    leading: tuple[str, ...] = (),
    fields: list[str] | None = None,
    list_separator: str = " | ",
) -> int:
    """Write records as CSV with a header row.

    Columns are ``leading`` followed by the sorted union of all other
    (flattened) columns, unless ``fields`` names them explicitly. Because the
    union needs every row, rows are materialised before writing.
    """
    rows: list[dict[str, str]] = []
    other_columns: set[str] = set()
    for record in records:
        row: dict[str, str] = {}
        flatten(record, "", row, list_separator)
        rows.append(row)
        other_columns.update(k for k in row if k not in leading)
    columns = list(leading) + (list(fields) if fields is not None else sorted(other_columns))
    writer = csv.DictWriter(out, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in columns})
    return len(rows)
