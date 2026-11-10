"""WARC 1.1: records, the rolling gzip writer, the tolerant reader, CDX indexes and replay."""

from __future__ import annotations

import asyncio
import gzip
import json
import zlib
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.store import Store
from skein.warc import (
    CdxIndex,
    Exchange,
    ReplayError,
    Replayer,
    WarcFormatError,
    WarcReader,
    WarcRecord,
    WarcWriter,
    build_index,
    exchange_records,
    make_record,
    read_record,
    revisit_profile,
    sha1_digest,
    surt,
    warc_date,
)
from skein.warc.cdx import CdxLine, cdxj_line, database_lines, sidecar_path, write_index
from skein.warc.http import ORIG_PREFIX, RevisitOf
from skein.warc.record import REVISIT_IDENTICAL, REVISIT_NOT_MODIFIED, digest_matches, parse_warc_date
from skein.warc.rewrite import archived_link, rewrite_html
from skein.warc.server import ReplayServer
from skein.warc.writer import seal
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl

T0 = 1_700_000_000.0


def exchange(url: str = "http://www.shop.test/p?id=1", body: bytes = b"<p>hello</p>" * 20, status: int = 200,
             at: float = T0, headers: list[tuple[str, str]] | None = None) -> Exchange:
    return Exchange(url, at, "GET", "/p?id=1", [("Host", "www.shop.test"), ("User-Agent", "skein")], "HTTP/1.1",
                    status, "OK" if status == 200 else "Not Modified",
                    headers or [("Content-Type", "text/html; charset=utf-8")], body,
                    metadata=[("via", "http://www.shop.test/")])


def test_record_roundtrip_and_digests() -> None:
    block = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nbody bytes"
    record = make_record("response", block, date=warc_date(T0), content_type="application/http;msgtype=response",
                         target_uri="http://a/", payload_digest=sha1_digest(b"body bytes"))
    data = record.serialize()
    assert data.startswith(b"WARC/1.1\r\nWARC-Type: response\r\n") and data.endswith(b"body bytes\r\n\r\n")
    parsed = WarcRecord.parse(data)
    assert parsed.headers[:3] == record.headers[:3] and parsed.block == block
    assert parsed.payload == b"body bytes" and parsed.verify() == []
    assert parsed.get("content-length") == str(len(block))
    assert digest_matches(sha1_digest(b"x"), b"x") and not digest_matches("md5:abc", b"x")
    tampered = WarcRecord(parsed.headers, block.replace(b"body", b"BODY"))
    assert tampered.verify() == ["WARC-Block-Digest mismatch", "WARC-Payload-Digest mismatch"]
    assert parse_warc_date(warc_date(T0 + 0.25)) == T0 + 0.25
    for bad, message in ((b"WARC/0.9\r\n\r\n", "version"), (data.replace(b"Content-Length", b"X-Length"),
                                                             "Content-Length"),
                         (data[:-20], "truncated"), (data + b"junk", "unexpected")):
        with pytest.raises(WarcFormatError, match=message):
            WarcRecord.parse(bad)
    with pytest.raises(ValueError):
        make_record("bogus", b"", date="x")


def test_exchange_records_request_response_metadata_and_revisits() -> None:
    chunked = exchange(headers=[("Content-Type", "text/html"), ("Transfer-Encoding", "chunked")])
    request, response, metadata = exchange_records(chunked, revisit=None, software="skein/test")
    assert request.type == "request" and request.get("WARC-Concurrent-To") == response.record_id
    assert response.get("WARC-Payload-Digest") == sha1_digest(chunked.payload)
    assert f"{ORIG_PREFIX}Transfer-Encoding: chunked".encode() in response.block
    assert metadata.type == "metadata" and metadata.get("WARC-Refers-To") == response.record_id
    assert b"via: http://www.shop.test/" in metadata.block
    origin = RevisitOf(REVISIT_IDENTICAL, "<urn:uuid:x>", "http://www.shop.test/p?id=1", warc_date(T0))
    _, revisit, _ = exchange_records(exchange(at=T0 + 60), revisit=origin, software="s")
    assert revisit.type == "revisit" and revisit.get("WARC-Profile") == REVISIT_IDENTICAL
    assert revisit.payload == b"" and revisit.get("WARC-Refers-To") == "<urn:uuid:x>"
    assert revisit_profile(304, "sha1:A", "sha1:B") == REVISIT_NOT_MODIFIED
    assert revisit_profile(200, "sha1:A", "sha1:A") == REVISIT_IDENTICAL
    assert revisit_profile(200, "sha1:A", "sha1:B") is None and revisit_profile(404, "sha1:A", "sha1:A") is None


def write_archive(directory: Path, count: int, max_size: int = 4096) -> WarcWriter:
    writer = WarcWriter(directory, FakeClock(T0), max_size=max_size, worker="w1", info={"isPartOf": "test"})
    for n in range(count):
        writer.write(exchange_records(exchange(f"http://www.shop.test/p{n}", body=f"<p>{n}</p>".encode() * 50,
                                               at=T0 + n), revisit=None, software="s"))
    writer.close()
    return writer


def test_writer_rolls_over_and_every_record_is_its_own_gzip_member(tmp_path: Path) -> None:
    writer = write_archive(tmp_path, 12)
    files = sorted(tmp_path.glob("*.warc.gz"))
    assert len(files) == len(writer.files_written) > 1
    assert all(f.name.startswith("skein-20231114221320-") and f.name.endswith("-w1.warc.gz") for f in files)
    for path in files:
        entries = list(WarcReader(path))
        assert entries[0].record.type == "warcinfo" and b"isPartOf: test" in entries[0].record.block
        raw = path.read_bytes()
        for entry in entries:
            member = raw[entry.offset: entry.offset + entry.length]
            assert WarcRecord.parse(gzip.decompress(member)).record_id == entry.record.record_id
            assert read_record(path, entry.offset).record_id == entry.record.record_id
        assert sidecar_path(path).exists()
    responses = [e for path in files for e in WarcReader(path) if e.record.type == "response"]
    assert len(responses) == 12 and all(e.record.verify() == [] for e in responses)
    with pytest.raises(ValueError):
        WarcWriter(tmp_path, FakeClock(), max_size=10)


def test_reader_tolerates_a_truncated_final_member_but_not_damage(tmp_path: Path) -> None:
    write_archive(tmp_path, 3, max_size=10**6)
    (path,) = tmp_path.glob("*.warc.gz")
    raw = path.read_bytes()
    entries = list(WarcReader(path))
    last = [e for e in entries if e.record.type == "response"][-1]
    path.write_bytes(raw[: last.offset + last.length // 2])
    reader = WarcReader(path)
    survivors = list(reader)
    assert len(survivors) == entries.index(last)
    assert reader.truncated is not None and reader.truncated.offset == last.offset
    assert len(build_index([path])) == 2, "indexing skips the torn record (the stale sidecar is ignored)"
    corrupt = bytearray(raw)
    corrupt[entries[1].offset + 20] ^= 0xFF
    path.write_bytes(bytes(corrupt))
    with pytest.raises(WarcFormatError):
        list(WarcReader(path))
    plain = tmp_path / "plain.warc"
    records = [e.record.serialize() for e in entries]
    plain.write_bytes(b"".join(records) + records[0][:40])
    plain_reader = WarcReader(plain)
    assert len(list(plain_reader)) == len(entries) and plain_reader.truncated is not None
    with pytest.raises(WarcFormatError):
        read_record(path, entries[1].offset)
    assert isinstance(zlib.error(), Exception)


def test_cdx_index_lookup_and_formats(tmp_path: Path) -> None:
    write_archive(tmp_path, 6)
    files = sorted(tmp_path.glob("*.warc.gz"))
    lines = build_index(files)
    assert len(lines) == 6 and lines == sorted(lines)
    assert surt("http://www.Shop.test:80/p0") == "test,shop)/p0" and surt("https://a.b:8443/x?Q=1") == "b,a:8443)/x?q=1"
    index_path = tmp_path / "index.cdx"
    write_index(lines, index_path)
    loaded = CdxIndex.load(index_path)
    capture = loaded.closest("http://www.shop.test/p3")
    assert capture is not None and capture.status == "200" and capture.mimetype == "text/html"
    assert loaded.closest("http://nowhere/") is None
    assert CdxLine.parse(capture.to_line()) == capture
    assert cdxj_line(capture).startswith("test,shop)/p3 20231114221323 {")
    write_index(lines, tmp_path / "index.cdxj", cdxj=True)
    assert (tmp_path / "index.cdxj").read_text().count("\n") == 6
    with pytest.raises(ValueError):
        CdxLine.parse("too few fields")


def test_replay_follows_revisits(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6)
    gzipped = gzip.compress(b"<h1>archived</h1>", mtime=0)
    first = exchange(body=gzipped, headers=[("Content-Type", "text/html"), ("Content-Encoding", "gzip")])
    request, response, _ = exchange_records(first, revisit=None, software="s")
    writer.write([request, response])
    origin = RevisitOf(REVISIT_IDENTICAL, response.record_id, first.url, response.date)
    writer.write(exchange_records(exchange(body=gzipped, at=T0 + 3600,
                                           headers=[("Content-Type", "text/html"), ("Content-Encoding", "gzip")]),
                                  revisit=origin, software="s"))
    not_modified = RevisitOf(REVISIT_NOT_MODIFIED, response.record_id, first.url, response.date)
    writer.write(exchange_records(exchange(body=b"", status=304, at=T0 + 7200), revisit=not_modified,
                                  software="s"))
    writer.close()
    replayer = Replayer(CdxIndex(build_index(tmp_path.glob("*.warc.gz"))), tmp_path)
    latest = replayer.replay(first.url)
    assert latest.status == 200 and latest.body == b"<h1>archived</h1>" and len(latest.revisit_chain) == 1
    middle = replayer.replay(first.url, "20231114231320")
    assert middle.body == b"<h1>archived</h1>" and middle.header("Content-Encoding") == "gzip"
    original = replayer.replay(first.url, "2023")
    assert original.revisit_chain == []
    with pytest.raises(ReplayError):
        replayer.replay("http://www.shop.test/missing")


def test_crawl_archives_responses_revisits_and_robots(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time)
    db = tmp_path / "shop.db"
    project = {"warc": {"enabled": True, "max_size": 4096}}
    first = crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project)
    archive = tmp_path / "shop.db.warc"
    assert first.stats["archive"]["records"] > 0 and first.stats["archive"]["revisits"] == 0
    clock.advance(10 * 86400)
    second = crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project,
                   incremental={"mode": "full"})
    assert second.stats["not_modified"] > 0
    assert second.stats["archive"]["revisits"] > second.stats["not_modified"], \
        "304s and unchanged payloads without validators both become revisits"
    files = sorted(archive.glob("*.warc.gz"))
    assert len(files) > 2, "small max_size forces rollover"
    records = [entry.record for path in files for entry in WarcReader(path)]
    types = {record.type for record in records}
    assert types == {"warcinfo", "request", "response", "metadata", "revisit"}
    assert any(r.target_uri and r.target_uri.endswith("/robots.txt") for r in records if r.type == "response")
    assert all(r.verify() == [] for r in records)
    with Store(db) as store:
        located = store.db.execute(
            "SELECT url, warc_file, warc_offset FROM fetch_events WHERE warc_file IS NOT NULL").fetchall()
        assert located and all((archive / row[1]).exists() for row in located)
        url, name, offset = located[0]
        assert read_record(archive / name, offset).target_uri == url
        closed = store.db.execute("SELECT COUNT(*) FROM warc_files WHERE closed_at IS NOT NULL").fetchone()[0]
        assert closed == len(files)
    index = CdxIndex(build_index(files))
    replayer = Replayer(index, archive)
    replayed = replayer.replay(site.url("/product/1"))
    assert replayed.status == 200 and b"Widget 1" in replayed.body
    revisited = [line for line in index.lines if line.mimetype == "warc/revisit"]
    assert revisited
    for line in revisited:
        again = replayer.replay(line.original, line.timestamp)
        assert again.revisit_chain and again.status == 200 and again.body


def test_replay_server(tmp_path: Path) -> None:
    write_archive(tmp_path, 2, max_size=10**6)
    server = ReplayServer(Replayer(CdxIndex(build_index(tmp_path.glob("*.warc.gz"))), tmp_path))
    reply = server.answer("GET", "/web/latest/http://www.shop.test/p1")
    assert reply.status == 200 and reply.body == b"<p>1</p>" * 50
    names = dict(reply.headers)
    assert names["Memento-Datetime"] == "Tue, 14 Nov 2023 22:13:21 GMT" and "#" in names["X-Archive-Src"]
    assert server.answer("GET", "/cdx?url=http://www.shop.test/p0").body.count(b"\n") == 1
    assert server.answer("GET", "/web/latest/http://nowhere/").status == 404
    assert server.answer("GET", "/web/yesterday/http://www.shop.test/p1").status == 400
    assert server.answer("POST", "/web/latest/x").status == 405 and server.answer("GET", "/other").status == 404

    async def fetch() -> bytes:
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"GET /web/20231114221320/http://www.shop.test/p0 HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            data = await reader.read()
            writer.close()
            return data
        finally:
            await server.stop()

    raw = asyncio.run(fetch())
    assert raw.startswith(b"HTTP/1.1 200 OK\r\n") and raw.endswith(b"<p>0</p>" * 50)


def test_site_fixture_is_used(site: FixtureSite) -> None:
    assert site.port > 0


def test_cdx_queries_by_prefix_host_domain_and_time(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6)
    for n, url in enumerate(("http://www.shop.test/a", "http://www.shop.test/a/b", "http://shop.test/c",
                             "http://img.shop.test/x.png", "http://othershop.test/a", "http://shop.test:8080/z")):
        writer.write(exchange_records(exchange(url, at=T0 + n * 86400 * 40), revisit=None, software="s"))
    writer.close()
    index = CdxIndex(build_index(tmp_path.glob("*.warc.gz")))
    urls = lambda lines: [line.original for line in lines]  # noqa: E731
    assert urls(index.query("http://shop.test/a")) == ["http://www.shop.test/a"]
    assert urls(index.query("http://shop.test/a", match="prefix")) == ["http://www.shop.test/a",
                                                                      "http://www.shop.test/a/b"]
    assert len(index.query("http://shop.test/", match="host")) == 3
    assert len(index.query("http://shop.test/", match="domain")) == 5, "subdomains and other ports, not othershop"
    assert set(urls(index.query("http://shop.test/", match="domain", since="2024", until="202404"))) == {
        "http://shop.test/c", "http://img.shop.test/x.png"}, "captured in February and March 2024"
    assert len(index.query("http://shop.test/", match="domain", limit=2)) == 2
    assert index.query("http://shop.test/a", status="404") == []
    with pytest.raises(ValueError):
        index.query("http://shop.test/", match="regex")
    server = ReplayServer(Replayer(index, tmp_path))
    reply = server.answer("GET", "/cdx?url=http://shop.test/&matchType=host&output=json&filter=status:200")
    rows = [json.loads(line) for line in reply.body.decode().splitlines()]
    assert len(rows) == 3 and {"timestamp", "url", "offset"} <= rows[0].keys()
    assert server.answer("GET", "/cdx?url=x&matchType=nope").status == 400


def test_replayed_html_links_point_into_the_archive() -> None:
    page = ('<a href="/b?x=1&amp;y=2">b</a> <img src=\'img/p.png\'> <a href="#top">top</a> '
            '<a href="mailto:x@y">m</a> <form action="https://other.test/post"></form> <a href=c.html>c</a>')
    text, count = rewrite_html(page, "http://shop.test/dir/a", "20240101000000")
    assert count == 4
    assert 'href="/web/20240101000000/http://shop.test/b?x=1&amp;y=2"' in text
    assert "src='/web/20240101000000/http://shop.test/dir/img/p.png'" in text
    assert 'href="#top"' in text and 'href="mailto:x@y"' in text
    assert 'action="/web/20240101000000/https://other.test/post"' in text
    assert 'href="/web/20240101000000/http://shop.test/dir/c.html"' in text
    assert archived_link("http://shop.test/", "javascript:void(0)", "1") is None


def test_files_left_open_by_a_killed_writer_are_sealed(tmp_path: Path) -> None:
    write_archive(tmp_path, 3, max_size=10**6)
    (path,) = tmp_path.glob("*.warc.gz")
    sidecar_path(path).unlink()
    entries = list(WarcReader(path))
    complete = path.read_bytes()
    path.write_bytes(complete + complete[entries[2].offset: entries[2].offset + 40])
    report = seal(path)
    assert report.records == len(entries) and report.torn_bytes == 40 and report.size == len(complete)
    assert path.read_bytes() == complete and WarcReader(path).truncated is None
    assert len(CdxIndex.load(sidecar_path(path)).lines) == 3


def _exchange_records(n: int, at: float = T0) -> list[WarcRecord]:
    return exchange_records(exchange(f"http://www.shop.test/p{n}", body=f"<p>{n}</p>".encode() * 50, at=at),
                            revisit=None, software="s")


def test_rolled_back_records_disappear_and_the_file_stays_usable(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6, worker="w1")
    writer.begin()
    writer.write(_exchange_records(0))
    (path,) = tmp_path.glob("*.warc.gz")
    size_after_first = path.stat().st_size
    assert len(build_index([path])) == 1
    writer.write(_exchange_records(1, at=T0 + 1))
    assert path.stat().st_size > size_after_first and len(build_index([path])) == 2
    writer.rollback()
    # Nothing was committed: the file created inside the transaction is removed with its sidecar.
    assert not path.exists() and not sidecar_path(path).exists() and writer.files_written == []
    # The next transaction starts clean and its records land at offset 0.
    writer.begin()
    locations = writer.write(_exchange_records(2, at=T0 + 2))
    writer.commit()
    writer.close()
    (path,) = tmp_path.glob("*.warc.gz")
    entries = list(WarcReader(path))
    assert entries[0].record.type == "warcinfo" and locations[0].offset == entries[1].offset
    assert [line.original for line in build_index([path])] == ["http://www.shop.test/p2"]


def test_rollback_truncates_only_the_uncommitted_tail(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6, worker="w1")
    writer.begin()
    writer.write(_exchange_records(0))
    writer.commit()
    (path,) = tmp_path.glob("*.warc.gz")
    committed_size = path.stat().st_size
    writer.begin()
    writer.write(_exchange_records(1, at=T0 + 1))
    assert path.stat().st_size > committed_size
    writer.rollback()
    assert path.stat().st_size == committed_size and len(build_index([path])) == 1
    # The writer is still attached to the same file and appends after the committed bytes.
    writer.begin()
    locations = writer.write(_exchange_records(2, at=T0 + 2))
    writer.commit()
    writer.close()
    assert locations[0].filename == path.name and locations[0].offset == committed_size
    assert sorted(line.original for line in build_index([path])) == [
        "http://www.shop.test/p0", "http://www.shop.test/p2"]
    assert WarcReader(path).truncated is None


def test_rollback_of_a_rollover_reopens_the_finished_file(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=2048, worker="w1")
    writer.begin()
    first_locations = writer.write(_exchange_records(0))
    writer.commit()
    first_file = tmp_path / first_locations[0].filename
    writer.begin()
    # Enough exchanges to force at least one rollover inside this transaction.
    for n in range(1, 8):
        writer.write(_exchange_records(n, at=T0 + n))
    assert writer.current is not None and writer.current.name != first_file.name
    rolled_back = sorted(p.name for p in tmp_path.glob("*.warc.gz"))
    writer.rollback()
    # Files created inside the rolled-back transaction disappear; the committed file is active again.
    assert sorted(p.name for p in tmp_path.glob("*.warc.gz")) == [first_file.name]
    assert writer.current is not None and writer.current.name == first_file.name
    writer.begin()
    locations = writer.write(_exchange_records(9, at=T0 + 9))
    writer.commit()
    writer.close()
    assert locations[0].filename == first_file.name
    entries = list(WarcReader(first_file))
    assert [e.record.type for e in entries].count("response") == 2 and WarcReader(first_file).truncated is None
    assert rolled_back  # the rollover really happened before the rollback


def test_repeated_rollbacks_and_rollovers_keep_the_archive_consistent(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=1500, worker="w1")
    committed_pages = 0
    for n in range(30):
        writer.begin()
        writer.write(_exchange_records(n, at=T0 + n))
        if n % 3 == 1:
            # Roll back a third of the transactions, some of which roll over.
            writer.rollback()
            continue
        writer.commit()
        committed_pages += 1
    writer.close()
    paths = sorted(tmp_path.glob("*.warc.gz"))
    assert paths
    for path in paths:
        reader = WarcReader(path)
        entries = list(reader)
        assert entries and reader.truncated is None, f"{path.name} is whole"
        # Every gzip member sits exactly where the previous one ended.
        offset = 0
        for entry in entries:
            assert entry.offset == offset
            offset += entry.length
        assert path.stat().st_size == offset
    responses = [line for path in paths for line in build_index([path])]
    assert len(responses) == committed_pages
    # No sidecar points at a record that is no longer in its file.
    for path in paths:
        assert [line.to_line() for line in build_index([path])] == \
               [line.to_line() for line in CdxIndex.load(sidecar_path(path)).lines]


def test_implicit_writes_commit_on_their_own(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6, worker="w1")
    writer.write(_exchange_records(0))
    writer.rollback()  # no open window: nothing to undo, and the call is a no-op
    writer.close()
    (path,) = tmp_path.glob("*.warc.gz")
    assert len(build_index([path])) == 1 and sidecar_path(path).exists()


def test_crawl_seals_its_abandoned_file_and_indexes_from_the_database(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    project = {"warc": {"enabled": True}}
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project)
    archive = tmp_path / "shop.db.warc"
    (first,) = archive.glob("*.warc.gz")
    with Store(db) as store:
        store.db.execute("UPDATE warc_files SET closed_at = NULL")
        file_lines = {(line.original, line.offset, line.mimetype) for line in build_index([first])}
        db_lines = {(line.original, line.offset, line.mimetype) for line in database_lines(store.db)}
    assert db_lines and db_lines <= file_lines
    assert {line for line in file_lines if not line[0].endswith("robots.txt")} == db_lines
    with first.open("ab") as handle:
        handle.write(b"\x1f\x8b\x08\x00torn")
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project, incremental={"mode": "full"})
    assert WarcReader(first).truncated is None and list(WarcReader(first))
    with Store(db) as store:
        assert store.db.execute("SELECT COUNT(*) FROM warc_files WHERE closed_at IS NULL").fetchone()[0] == 0
        revisits = [line for line in database_lines(store.db) if line.mimetype == "warc/revisit"]
    assert revisits, "the second crawl's unchanged pages are revisits"


def test_an_unregistered_torn_warc_file_is_sealed_and_registered(site: FixtureSite, tmp_path: Path) -> None:
    """A file whose registration was lost with a rolled-back transaction still gets recovered.

    This is the half-written file nobody claimed: it sits in the archive
    directory, has no ``warc_files`` row and ends in a torn gzip member.
    """
    db = tmp_path / "shop.db"
    project = {"warc": {"enabled": True}}
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project)
    archive = tmp_path / "shop.db.warc"
    (first,) = archive.glob("*.warc.gz")
    orphan = archive / "skein-20200101000000-99999-ghost.warc.gz"
    raw = first.read_bytes()
    # A real crash leaves a half-written final gzip member: a whole member cut
    # in half, not arbitrary garbage (the reader recognises it as truncated).
    entries = list(WarcReader(first))
    last_offset = entries[-1].offset
    torn_member = raw[last_offset:]
    orphan.write_bytes(raw[:last_offset] + torn_member[: len(torn_member) // 2])
    expected = raw[:last_offset]
    with Store(db) as store:
        store.db.execute("DELETE FROM warc_files WHERE name = ?", (orphan.name,))
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project, incremental={"mode": "full"})
    assert WarcReader(orphan).truncated is None, "the torn tail was cut"
    assert orphan.read_bytes() == expected, "only the torn bytes were removed, the old records stay replayable"
    with Store(db) as store:
        row = store.db.execute("SELECT closed_at, records, worker_id FROM warc_files WHERE name = ?",
                               (orphan.name,)).fetchone()
    assert row is not None and row["closed_at"] is not None and row["worker_id"] is None
    assert sidecar_path(orphan).exists()


def test_a_file_left_open_by_a_dead_worker_is_sealed_on_any_restart(site: FixtureSite, tmp_path: Path) -> None:
    """Sealing no longer depends on the same worker id coming back (a restarted worker gets a new id)."""
    db = tmp_path / "shop.db"
    project = {"warc": {"enabled": True}}
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project)
    archive = tmp_path / "shop.db.warc"
    (first,) = archive.glob("*.warc.gz")
    with first.open("ab") as handle:
        handle.write(b"\x1f\x8b\x08\x00torn")
    with Store(db) as store:
        # Simulate a crashed worker whose id is never reused, with an open file.
        store.db.execute("UPDATE warc_files SET worker_id = ?, closed_at = NULL WHERE name = ?",
                         ("dead-and-gone-w7", first.name))
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1, project=project, incremental={"mode": "full"})
    assert WarcReader(first).truncated is None
    with Store(db) as store:
        assert store.db.execute(
            "SELECT COUNT(*) FROM warc_files WHERE closed_at IS NULL").fetchone()[0] == 0


def test_memento_timemap(tmp_path: Path) -> None:
    writer = WarcWriter(tmp_path, FakeClock(T0), max_size=10**6)
    for hours in (0, 5):
        writer.write(exchange_records(exchange(at=T0 + hours * 3600), revisit=None, software="s"))
    writer.close()
    server = ReplayServer(Replayer(CdxIndex(build_index(tmp_path.glob("*.warc.gz"))), tmp_path))
    reply = server.answer("GET", "/timemap/link/http://www.shop.test/p?id=1")
    assert reply.status == 200 and dict(reply.headers)["Content-Type"] == "application/link-format"
    entries = reply.body.decode().split(",\n")
    assert entries[0] == '<http://www.shop.test/p?id=1>; rel="original"'
    assert 'rel="first memento"' in entries[2] and "</web/20231114221320/" in entries[2]
    assert 'rel="last memento"; datetime="Wed, 15 Nov 2023 03:13:20 GMT"' in entries[3]
    assert server.answer("GET", "/timemap/link/http://nowhere/").status == 404
