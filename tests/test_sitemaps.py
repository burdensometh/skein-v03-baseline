"""Sitemap parsing: urlset, index, gzip, text, dates and hostile input."""

from __future__ import annotations

import gzip
from datetime import datetime, timezone

import pytest

from skein.sitemaps import SitemapError, parse_sitemap, parse_w3c_datetime

NS = 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'


def _ts(*args: int) -> float:
    return datetime(*args, tzinfo=timezone.utc).timestamp()


def test_urlset_with_metadata() -> None:
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <urlset {NS}>
      <url><loc> https://example.com/a </loc><lastmod>2024-05-01</lastmod>
           <changefreq>Weekly</changefreq><priority>0.8</priority></url>
      <url><loc>https://example.com/b?x=1&amp;y=2</loc></url>
    </urlset>""".encode()
    sitemap = parse_sitemap(xml)
    assert sitemap.kind == "urlset" and not sitemap.is_index
    first, second = sitemap.entries
    assert first.loc == "https://example.com/a"
    assert first.lastmod == _ts(2024, 5, 1)
    assert first.changefreq == "weekly"
    assert first.priority == 0.8
    assert second.loc == "https://example.com/b?x=1&y=2"
    assert second.lastmod is None


def test_sitemap_index_gzipped() -> None:
    xml = f"""<sitemapindex {NS}>
      <sitemap><loc>https://example.com/s1.xml.gz</loc><lastmod>2024-01-02T03:04:05Z</lastmod></sitemap>
      <sitemap><loc>https://example.com/s2.xml</loc></sitemap>
    </sitemapindex>""".encode()
    sitemap = parse_sitemap(gzip.compress(xml))
    assert sitemap.is_index
    assert [e.loc for e in sitemap.entries] == ["https://example.com/s1.xml.gz", "https://example.com/s2.xml"]
    assert sitemap.entries[0].lastmod == _ts(2024, 1, 2, 3, 4, 5)


def test_namespace_free_and_relative_locations() -> None:
    xml = b"<urlset><url><loc>/relative/page</loc></url><url><loc>javascript:alert(1)</loc></url></urlset>"
    sitemap = parse_sitemap(xml, base_url="https://example.com/sitemap.xml")
    assert [e.loc for e in sitemap.entries] == ["https://example.com/relative/page"]
    assert any("unusable" in w for w in sitemap.warnings)


def test_text_sitemap() -> None:
    sitemap = parse_sitemap(b"https://example.com/1\n\nhttps://example.com/2\nnot a url\n")
    assert sitemap.kind == "text"
    assert [e.loc for e in sitemap.entries] == ["https://example.com/1", "https://example.com/2"]
    assert sitemap.warnings == ["line 4: not an http(s) URL"]


def test_rss_feed() -> None:
    xml = b"""<?xml version="1.0"?><rss version="2.0"><channel><title>News</title>
      <item><title>One</title><link>https://example.com/news/1</link>
            <pubDate>Tue, 02 Jan 2024 10:00:00 +0100</pubDate></item>
      <item><title>Two</title><link>/news/2</link><pubDate>garbage</pubDate></item>
    </channel></rss>"""
    sitemap = parse_sitemap(xml, base_url="https://example.com/feed.xml")
    assert sitemap.kind == "feed"
    assert [e.loc for e in sitemap.entries] == ["https://example.com/news/1", "https://example.com/news/2"]
    assert sitemap.entries[0].lastmod == _ts(2024, 1, 2, 9, 0)
    assert sitemap.entries[1].lastmod is None


def test_atom_feed() -> None:
    xml = b"""<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>
      <entry><link rel="self" href="https://example.com/self"/><link href="https://example.com/a"/>
             <updated>2024-03-04T05:06:07Z</updated></entry>
      <entry><title>no link</title></entry>
    </feed>"""
    sitemap = parse_sitemap(xml)
    assert [e.loc for e in sitemap.entries] == ["https://example.com/a"]
    assert sitemap.entries[0].lastmod == _ts(2024, 3, 4, 5, 6, 7)
    assert len(sitemap.warnings) == 1


def test_entry_limit_truncates() -> None:
    urls = "".join(f"<url><loc>https://example.com/{i}</loc></url>" for i in range(20))
    sitemap = parse_sitemap(f"<urlset {NS}>{urls}</urlset>".encode(), max_entries=5)
    assert len(sitemap.entries) == 5
    assert sitemap.truncated


def test_bad_values_become_warnings() -> None:
    xml = f"""<urlset {NS}><url><loc>https://example.com/</loc><lastmod>yesterday</lastmod>
      <priority>high</priority><changefreq>sometimes</changefreq></url></urlset>""".encode()
    entry = parse_sitemap(xml).entries[0]
    assert entry.lastmod is None and entry.priority is None and entry.changefreq is None


@pytest.mark.parametrize(
    "data,message",
    [
        (b"<urlset><url><loc>x</loc>", "invalid sitemap XML"),
        (b"<html><body>not a sitemap</body></html>", "unexpected root"),
        (b'<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol">]><urlset/>', "DTD"),
        (b"\x1f\x8b\x08\x00garbage", "corrupt gzip"),
    ],
)
def test_rejected_documents(data: bytes, message: str) -> None:
    with pytest.raises(SitemapError, match=message):
        parse_sitemap(data)


def test_decompressed_size_limit() -> None:
    with pytest.raises(SitemapError, match="exceeds"):
        parse_sitemap(gzip.compress(b" " * 10_000), max_bytes=1000)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2024", _ts(2024, 1, 1)),
        ("2024-07", _ts(2024, 7, 1)),
        ("2024-07-16", _ts(2024, 7, 16)),
        ("2024-07-16T19:20+01:00", _ts(2024, 7, 16, 18, 20)),
        ("2024-07-16T19:20:30Z", _ts(2024, 7, 16, 19, 20, 30)),
        ("2024-07-16T19:20:30.45-05:00", _ts(2024, 7, 17, 0, 20, 30) + 0.45),
        ("2024-07-16 19:20:30", _ts(2024, 7, 16, 19, 20, 30)),
        ("2024-13-01", None),
        ("July 2024", None),
        ("", None),
    ],
)
def test_w3c_datetime(text: str, expected: float | None) -> None:
    result = parse_w3c_datetime(text)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)
