"""Parsing of XML sitemaps, sitemap indexes, RSS/Atom feeds, gzip-compressed and text sitemaps."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime

from skein.net.content import gunzip, looks_gzipped
from skein.net.errors import BodyTooLarge, DecodingError
from skein.sitemaps.dates import parse_w3c_datetime
from skein.urls.parse import URLError, parse_url, resolve

MAX_SITEMAP_BYTES = 50 * 1024 * 1024
MAX_SITEMAP_ENTRIES = 50_000
CHANGEFREQS = frozenset({"always", "hourly", "daily", "weekly", "monthly", "yearly", "never"})


class SitemapError(ValueError):
    """The document is not a usable sitemap (bad XML, wrong root element, too large)."""


@dataclass(frozen=True)
class SitemapEntry:
    """A ``<url>`` of a urlset or a ``<sitemap>`` of an index."""

    loc: str
    lastmod: float | None = None
    changefreq: str | None = None
    priority: float | None = None


@dataclass
class Sitemap:
    """A parsed sitemap. ``kind`` is ``"urlset"``, ``"index"``, ``"feed"`` (RSS/Atom) or ``"text"``."""

    kind: str
    entries: list[SitemapEntry] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    truncated: bool = False

    @property
    def is_index(self) -> bool:
        return self.kind == "index"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower() if tag.startswith("{") else tag.lower()


def _clean_loc(raw: str, base_url: str | None) -> str | None:
    loc = raw.strip()
    if not loc:
        return None
    try:
        parsed = parse_url(loc)
        if not parsed.is_absolute and base_url:
            parsed = resolve(base_url, parsed)
    except URLError:
        return None
    if parsed.scheme not in ("http", "https") or not parsed.host:
        return None
    return str(parsed)


def decompress_if_needed(data: bytes, limit: int = MAX_SITEMAP_BYTES) -> bytes:
    """Gunzip ``data`` when it carries the gzip magic number, regardless of headers."""
    if not looks_gzipped(data):
        return data
    try:
        return gunzip(data, limit)
    except BodyTooLarge as exc:
        raise SitemapError(f"decompressed sitemap exceeds {limit} bytes") from exc
    except DecodingError as exc:
        raise SitemapError(f"corrupt gzip sitemap: {exc}") from exc


def parse_sitemap(
    data: bytes,
    base_url: str | None = None,
    *,
    max_entries: int = MAX_SITEMAP_ENTRIES,
    max_bytes: int = MAX_SITEMAP_BYTES,
) -> Sitemap:
    """Parse sitemap bytes (optionally gzip-compressed).

    XML documents must have a ``urlset``, ``sitemapindex``, RSS 2.0 ``rss`` or
    Atom ``feed`` root (the feed formats the sitemaps protocol accepts);
    documents that do not start with ``<`` are treated as plain-text sitemaps
    with one URL per line. DTDs are refused outright to rule out entity
    expansion.
    """
    data = decompress_if_needed(data, max_bytes)
    if len(data) > max_bytes:
        raise SitemapError(f"sitemap exceeds {max_bytes} bytes")
    stripped = data.lstrip(b"\xef\xbb\xbf \t\r\n")
    if not stripped.startswith(b"<"):
        return _parse_text(stripped, base_url, max_entries)
    if b"<!DOCTYPE" in stripped[:2048].upper() or b"<!ENTITY" in stripped.upper():
        raise SitemapError("sitemaps with a DTD are not accepted")
    return _parse_xml(stripped, base_url, max_entries)


def _parse_text(data: bytes, base_url: str | None, max_entries: int) -> Sitemap:
    sitemap = Sitemap("text")
    for number, line in enumerate(data.decode("utf-8", "replace").splitlines(), start=1):
        if not line.strip():
            continue
        loc = _clean_loc(line, base_url)
        if loc is None:
            sitemap.warnings.append(f"line {number}: not an http(s) URL")
            continue
        if len(sitemap.entries) >= max_entries:
            sitemap.truncated = True
            break
        sitemap.entries.append(SitemapEntry(loc))
    return sitemap


def _children(element: ET.Element, tag: str) -> list[ET.Element]:
    return [child for child in element if _local(child.tag) == tag]


def _text_of(element: ET.Element) -> dict[str, str]:
    """Map of child tag -> stripped text for the direct children of ``element``."""
    out: dict[str, str] = {}
    for child in element:
        out.setdefault(_local(child.tag), (child.text or "").strip())
    return out


def _parse_xml(data: bytes, base_url: str | None, max_entries: int) -> Sitemap:
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise SitemapError(f"invalid sitemap XML: {exc}") from exc
    tag = _local(root.tag)
    if tag == "urlset":
        sitemap, records = Sitemap("urlset"), [_text_of(e) for e in _children(root, "url")]
    elif tag == "sitemapindex":
        sitemap, records = Sitemap("index"), [_text_of(e) for e in _children(root, "sitemap")]
    elif tag == "rss":
        sitemap, records = Sitemap("feed"), _rss_records(root)
    elif tag == "feed":
        sitemap, records = Sitemap("feed"), _atom_records(root)
    else:
        raise SitemapError(f"unexpected root element <{tag}>")
    for record in records:
        entry = _make_entry(record, base_url, sitemap)
        if entry is None:
            continue
        if len(sitemap.entries) >= max_entries:
            sitemap.truncated = True
            sitemap.warnings.append(f"more than {max_entries} entries; rest ignored")
            break
        sitemap.entries.append(entry)
    return sitemap


def _rss_records(root: ET.Element) -> list[dict[str, str]]:
    """RSS 2.0 items as sitemap records (``<link>`` and ``<pubDate>``)."""
    records = []
    for channel in _children(root, "channel"):
        for item in _children(channel, "item"):
            fields = _text_of(item)
            record = {"loc": fields.get("link", "")}
            published = _rfc822_to_w3c(fields.get("pubdate", ""))
            if published:
                record["lastmod"] = published
            records.append(record)
    return records


def _atom_records(root: ET.Element) -> list[dict[str, str]]:
    """Atom entries as sitemap records (alternate ``<link href>`` and ``<updated>``)."""
    records = []
    for entry in _children(root, "entry"):
        links = _children(entry, "link")
        chosen = next((l for l in links if l.get("rel", "alternate") == "alternate"), links[0] if links else None)
        record = {"loc": chosen.get("href", "") if chosen is not None else ""}
        fields = _text_of(entry)
        if fields.get("updated"):
            record["lastmod"] = fields["updated"]
        records.append(record)
    return records


def _rfc822_to_w3c(text: str) -> str | None:
    if not text:
        return None
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.isoformat()


def _make_entry(record: dict[str, str], base_url: str | None, sitemap: Sitemap) -> SitemapEntry | None:
    raw_loc = record.get("loc", "")
    loc = _clean_loc(raw_loc, base_url)
    if loc is None:
        sitemap.warnings.append(f"skipping entry with unusable <loc> {raw_loc[:100]!r}")
        return None
    lastmod = parse_w3c_datetime(record.get("lastmod"))
    if record.get("lastmod") and lastmod is None:
        sitemap.warnings.append(f"unparseable <lastmod> {record['lastmod']!r} for {loc}")
    changefreq = record.get("changefreq", "").lower() or None
    if changefreq is not None and changefreq not in CHANGEFREQS:
        changefreq = None
    priority: float | None = None
    if record.get("priority"):
        try:
            priority = min(1.0, max(0.0, float(record["priority"])))
        except ValueError:
            sitemap.warnings.append(f"invalid <priority> for {loc}")
    return SitemapEntry(loc, lastmod, changefreq, priority)
