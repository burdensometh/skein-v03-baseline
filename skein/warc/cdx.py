"""CDX indexes of WARC files (the classic 11-field ``CDX N b a m s k r M S V g`` layout).

One line per response or revisit record::

    urlkey timestamp original mimetype status digest redirect meta length offset filename

``urlkey`` is the SURT form of the URL (host labels reversed, ``www.``
dropped, lower-cased: ``com,example)/path?q``); ``timestamp`` is the
14-digit capture time; ``digest`` the base32 SHA-1 payload digest;
``length``/``offset`` locate the gzip member. Lines are sorted, so lookups
by URL are binary searches.
"""

from __future__ import annotations

import bisect
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from skein.checkpoint.snapshot import atomic_write
from skein.urls.parse import DEFAULT_PORTS, URLError, parse_url
from skein.warc.http import parse_http_head
from skein.warc.reader import WarcReader
from skein.warc.record import WarcRecord, parse_warc_date

CDX_HEADER = " CDX N b a m s k r M S V g"
INDEXED_TYPES = ("response", "revisit")
MATCH_TYPES = ("exact", "prefix", "host", "domain")


def surt(url: str) -> str:
    """Sort-friendly URI Reordering Transform of ``url`` (scheme dropped, default ports dropped)."""
    try:
        parsed = parse_url(url)
    except URLError:
        return url.lower()
    host = (parsed.host or "").lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    labels = ",".join(reversed(host.split("."))) if host else ""
    port = f":{parsed.port}" if parsed.port is not None and parsed.port != DEFAULT_PORTS.get(parsed.scheme or "") else ""
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{labels}{port}){path.lower()}{query.lower()}"


def timestamp14(warc_date: str) -> str:
    moment = datetime.fromtimestamp(parse_warc_date(warc_date), tz=timezone.utc)
    return moment.strftime("%Y%m%d%H%M%S")


@dataclass(frozen=True, order=True)
class CdxLine:
    urlkey: str
    timestamp: str
    original: str
    mimetype: str
    status: str
    digest: str
    redirect: str
    meta: str
    length: int
    offset: int
    filename: str

    def to_line(self) -> str:
        return " ".join(str(v) for v in (self.urlkey, self.timestamp, self.original, self.mimetype, self.status,
                                           self.digest, self.redirect, self.meta, self.length, self.offset,
                                           self.filename))

    @classmethod
    def parse(cls, line: str) -> "CdxLine":
        parts = line.split(" ")
        if len(parts) != 11:
            raise ValueError(f"expected 11 CDX fields, got {len(parts)}: {line[:80]!r}")
        return cls(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], int(parts[8]),
                   int(parts[9]), parts[10])


def cdx_line(record: WarcRecord, offset: int, length: int, filename: str) -> CdxLine | None:
    """The index line of a response or revisit record (``None`` for other record types)."""
    if record.type not in INDEXED_TYPES or not record.target_uri:
        return None
    mimetype, status, redirect = "-", "-", "-"
    if record.is_http:
        head, _ = record.http_split()
        start, fields = parse_http_head(head)
        parts = start.split(" ", 2)
        status = parts[1] if len(parts) > 1 else "-"
        for field_name, value in fields:
            lowered = field_name.lower()
            if lowered == "content-type":
                mimetype = value.split(";")[0].strip().lower() or "-"
            elif lowered == "location":
                redirect = value.replace(" ", "%20") or "-"
    if record.type == "revisit":
        mimetype = "warc/revisit"
    digest = (record.get("WARC-Payload-Digest") or "-").split(":", 1)[-1]
    return CdxLine(surt(record.target_uri), timestamp14(record.date), record.target_uri.replace(" ", "%20"),
                   mimetype, status, digest, redirect, "-", length, offset, filename)


def sidecar_path(warc: Path) -> Path:
    """Where the writer leaves the per-file index of a finished WARC file."""
    return warc.with_name(warc.name + ".cdx")


def index_file(path: Path, *, name: str | None = None) -> Iterator[CdxLine]:
    """CDX lines for the response and revisit records of one WARC file (tolerates a truncated tail).

    A sidecar index written by the WARC writer when it closed the file is
    used instead of reading the file, when it is at least as new.
    """
    path = Path(path)
    filename = name or path.name
    sidecar = sidecar_path(path)
    if sidecar.exists() and sidecar.stat().st_mtime >= path.stat().st_mtime:
        for raw in sidecar.read_text(encoding="utf-8").splitlines():
            if raw.strip() and not raw.startswith(" CDX"):
                yield replace(CdxLine.parse(raw), filename=filename)
        return
    for entry in WarcReader(path):
        line = cdx_line(entry.record, entry.offset, entry.length, filename)
        if line is not None:
            yield line


def cdxj_line(line: CdxLine) -> str:
    """The same capture in the CDXJ layout (``urlkey timestamp {json}``) used by pywb."""
    fields = {"url": line.original, "mime": line.mimetype, "status": line.status, "digest": line.digest,
              "length": str(line.length), "offset": str(line.offset), "filename": line.filename}
    if line.redirect != "-":
        fields["redirect"] = line.redirect
    return f"{line.urlkey} {line.timestamp} {json.dumps(fields, sort_keys=True)}"


def build_index(paths: Iterable[Path]) -> list[CdxLine]:
    lines: list[CdxLine] = []
    for path in paths:
        lines.extend(index_file(path))
    return sorted(lines)


def write_index(lines: list[CdxLine], destination: Path, *, cdxj: bool = False) -> None:
    ordered = sorted(lines)
    if cdxj:
        text = "".join(cdxj_line(line) + "\n" for line in ordered)
    else:
        text = CDX_HEADER + "\n" + "".join(line.to_line() + "\n" for line in ordered)
    atomic_write(destination, text.encode("utf-8"))


def database_lines(db: sqlite3.Connection, spider: str | None = None) -> Iterator[CdxLine]:
    """Index lines for every archived fetch recorded in a skein database (no WARC file is read).

    Fetch events remember the file, offset and length of their response or
    revisit record and the payload digest, which is all an index needs.
    """
    query = """SELECT url, final_url, fetched_at, status, content_type, payload_digest, warc_file, warc_offset,
                      warc_length, warc_revisit
               FROM fetch_events WHERE warc_file IS NOT NULL"""
    params: tuple[str, ...] = ()
    if spider is not None:
        query += " AND spider = ?"
        params = (spider,)
    for row in db.execute(query, params):
        original = row[1] or row[0]
        stamp = datetime.fromtimestamp(row[2], tz=timezone.utc).strftime("%Y%m%d%H%M%S")
        mime = "warc/revisit" if row[9] else ((row[4] or "-").split(";")[0].strip().lower() or "-")
        digest = (row[5] or "-").split(":", 1)[-1]
        yield CdxLine(surt(original), stamp, original, mime, str(row[3] or "-"), digest, "-", "-", row[8] or 0,
                      row[7] or 0, row[6])


class CdxIndex:
    """A sorted, in-memory CDX index with URL lookups."""

    def __init__(self, lines: Iterable[CdxLine]) -> None:
        self.lines = sorted(lines)
        self._keys = [(line.urlkey, line.timestamp) for line in self.lines]

    @classmethod
    def load(cls, path: Path) -> "CdxIndex":
        lines = []
        for raw in Path(path).read_text(encoding="utf-8").splitlines():
            if raw.startswith(" CDX") or not raw.strip():
                continue
            lines.append(CdxLine.parse(raw))
        return cls(lines)

    def captures(self, url: str) -> list[CdxLine]:
        """Every capture of ``url``, oldest first."""
        key = surt(url)
        start = bisect.bisect_left(self._keys, (key, ""))
        found = []
        for line in self.lines[start:]:
            if line.urlkey != key:
                break
            found.append(line)
        return found

    def closest(self, url: str, timestamp: str | None = None) -> CdxLine | None:
        """The capture of ``url`` closest to ``timestamp`` (latest when ``None``)."""
        captures = self.captures(url)
        if not captures:
            return None
        if timestamp is None:
            return captures[-1]
        target = int(timestamp.ljust(14, "0")[:14])
        return min(captures, key=lambda line: (abs(int(line.timestamp) - target), line.timestamp))

    def query(
        self,
        url: str,
        *,
        match: str = "exact",
        since: str | None = None,
        until: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[CdxLine]:
        """Captures matching ``url`` the way CDX servers do.

        ``exact``: this URL; ``prefix``: URLs starting with it; ``host``:
        every URL of its host (and port); ``domain``: its host and all
        subdomains. ``since``/``until`` bound the 14-digit timestamp
        (shorter values are prefixes: ``2024`` means all of 2024), ``status``
        keeps one HTTP status.
        """
        if match not in MATCH_TYPES:
            raise ValueError(f"match must be one of {MATCH_TYPES}")
        key = surt(url)
        host_part = key.split(")", 1)[0]
        if match == "exact":
            start, accept = key, (lambda k: k == key)
        elif match == "prefix":
            start, accept = key, (lambda k: k.startswith(key))
        elif match == "host":
            start, accept = host_part + ")", (lambda k: k.startswith(host_part + ")"))
        else:
            domain = host_part.split(":", 1)[0]
            start = domain
            accept = (lambda k: k.startswith(domain) and k[len(domain): len(domain) + 1] in (",", ")", ":"))
        lower = since.ljust(14, "0") if since else None
        upper = until.ljust(14, "9") if until else None
        found: list[CdxLine] = []
        for line in self.lines[bisect.bisect_left(self._keys, (start, "")):]:
            if not line.urlkey.startswith(start if match != "exact" else key):
                break
            if not accept(line.urlkey):
                continue
            if (lower and line.timestamp < lower) or (upper and line.timestamp > upper):
                continue
            if status is not None and line.status != status:
                continue
            found.append(line)
            if limit is not None and len(found) >= limit:
                break
        return found

    def by_digest(self, url: str, digest: str) -> CdxLine | None:
        """The newest non-revisit capture of ``url`` with payload digest ``digest``."""
        for line in reversed(self.captures(url)):
            if line.digest == digest and line.mimetype != "warc/revisit":
                return line
        return None
